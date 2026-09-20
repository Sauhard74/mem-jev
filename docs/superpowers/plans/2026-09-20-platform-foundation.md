# Platform Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver the production foundation that accepts an authenticated trace, sanitizes and canonicalizes it without persisting raw input, writes a content-addressed canonical archive object, and atomically records the tenant ledger entry plus outbox job in SurrealDB.

**Architecture:** A Go Connect API delegates to a transport-independent ingest service. The service enforces a credential-derived tenant and consent policy, sanitizes into a typed canonical batch, writes that batch through an archive capability, and commits the corresponding receipt, trace, canonical events, and outbox job through a storage capability. SurrealDB and S3 are adapters; deterministic domain behavior is tested without either, then verified against pinned local services.

**Tech Stack:** Go 1.23, Protocol Buffers, Buf v2 generation, Connect-Go, Protovalidate, SurrealDB 3.2.4 with `surrealdb.go` v1.7.0, AWS SDK for Go v2 S3, OpenTelemetry, Docker Compose, and GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-09-20-procedural-memory-platform-design.md`

## Global Constraints

- The service is advisory and must not execute recalled procedures.
- Raw trace bodies are transient: they may exist only in request memory and must never enter logs, metrics, database records, archive objects, or error text.
- Tenant identity comes from authenticated credentials and cannot be supplied or overridden by request fields.
- Consent is fail-closed with exactly `deny`, `recall_only`, and `learn_and_recall`; ingest requires `learn_and_recall`.
- Every public mutation requires an idempotency key.
- Canonical identities use SHA-256 over RFC 8785 JSON canonicalization after Unicode NFC normalization and schema validation.
- Canonical event and outcome evidence records are append-only except for a later legally required erasure workflow.
- The archive object must exist before the SurrealDB commit; an unreferenced object is safe to collect after a grace period.
- The trace, canonical events, ingest receipt, archive reference, and outbox job commit in one SurrealDB transaction.
- Work is at least once and all durable mutations are idempotent.
- SurrealDB integration tests pin `surrealdb/surrealdb:v3.2.4`; production upgrades require a compatibility run.
- Logs contain identifiers, versions, sizes, timings, and result codes only.
- The foundation must run without Jev, an embedding provider, or a workflow provider.

## Review Focus

- A request body containing an API key in an allowed-looking tool field must redact the value before hashing or persistence; Task 5 pins this.
- Reuse of one idempotency key with different canonical content must return a conflict without modifying the prior receipt; Task 9 pins this.
- A caller that includes another tenant's identifier in request metadata must remain bound to the authenticated tenant; Tasks 4 and 10 pin this.
- An archive write that succeeds before a database failure must leave an unreferenced, content-addressed object and make a retry converge on one receipt; Task 9 pins this.
- Two concurrent identical requests must produce one receipt, one trace, one event set, and one outbox job; Task 8 pins this against SurrealDB.

---

## Plan sequence

This is plan 1 of 6 from the approved architecture. Later plans cover the evidence pipeline, core retrieval, composition and revision learning, Jev enhancement, and production hardening. This plan owns the stable interfaces those plans consume.

### Task 1: Repository contract and reproducible toolchain

**Files:**
- Create: `.gitignore`
- Create: `.golangci.yml`
- Create: `go.mod`
- Create: `Makefile`
- Create: `buf.yaml`
- Create: `buf.gen.yaml`
- Create: `tools/tools.go`
- Create: `scripts/check-generated.sh`
- Create: `internal/buildinfo/buildinfo.go`
- Test: `internal/buildinfo/buildinfo_test.go`

**Interfaces:**
- Consumes: Go 1.23.3, protoc 29.3, and Docker 29 already available in the workspace.
- Produces: module `github.com/sauhard74/mem-jev`; `make generate`, `make test`, `make lint`, and `make verify`; `buildinfo.Info` for health reporting.

- [ ] **Step 1: Write the failing build-info test**

```go
package buildinfo

import "testing"

func TestCurrentHasNonEmptyVersionFields(t *testing.T) {
	got := Current()
	if got.Version == "" || got.Commit == "" || got.BuiltAt == "" {
		t.Fatalf("Current() = %#v; all fields must be non-empty", got)
	}
}
```

- [ ] **Step 2: Run the test and verify the package does not exist**

Run: `go test ./internal/buildinfo`

Expected: FAIL because `go.mod` and `internal/buildinfo` do not exist.

- [ ] **Step 3: Create the module and pinned tool configuration**

Create `go.mod` with `module github.com/sauhard74/mem-jev` and `go 1.23.0`. Pin direct runtime dependencies through normal imports and pin command-only dependencies in `tools/tools.go` behind `//go:build tools`:

```go
package tools

import (
	_ "github.com/bufbuild/buf/cmd/buf"
	_ "github.com/golangci/golangci-lint/cmd/golangci-lint"
)
```

Configure Buf v2 with managed Go package prefix `github.com/sauhard74/mem-jev/gen`, remote plugins `buf.build/protocolbuffers/go:v1.36.11` and `buf.build/connectrpc/go:v1.18.1`, both using `paths=source_relative`. `scripts/check-generated.sh` must run `go run github.com/bufbuild/buf/cmd/buf@v1.57.2 generate` and fail when `git diff --exit-code -- gen` is non-zero.

- [ ] **Step 4: Implement build information**

```go
package buildinfo

type Info struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
	BuiltAt string `json:"built_at"`
}

var (
	version = "dev"
	commit  = "unknown"
	builtAt = "unknown"
)

func Current() Info {
	return Info{Version: version, Commit: commit, BuiltAt: builtAt}
}
```

- [ ] **Step 5: Add verification targets and run them**

`make verify` must run, in order: Buf lint, generated-code drift check, `go test -race ./...`, `go vet ./...`, and golangci-lint. Generated files are committed so production builds do not depend on the Buf registry.

Run: `make test`

Expected: PASS for `internal/buildinfo`.

- [ ] **Step 6: Commit the toolchain contract**

```bash
git add .gitignore .golangci.yml go.mod go.sum Makefile buf.yaml buf.gen.yaml tools scripts internal/buildinfo
git commit -m "build: establish reproducible Go toolchain"
```

### Task 2: Versioned public ingest and health contracts

**Files:**
- Create: `proto/memjev/v1/common.proto`
- Create: `proto/memjev/v1/ingest.proto`
- Create: `proto/memjev/v1/health.proto`
- Create: `proto/memjev/v1/error.proto`
- Create: `gen/memjev/v1/*.pb.go`
- Create: `gen/memjev/v1/memjevv1connect/*.connect.go`
- Create: `internal/contracts/validate.go`
- Test: `internal/contracts/contracts_test.go`

**Interfaces:**
- Consumes: Buf generation contract from Task 1.
- Produces: `memjev.v1.IngestService/IngestTrace`, `memjev.v1.HealthService/Check`, generated Go messages, Connect handlers, and `contracts.ValidateIngest(*memjevv1.IngestTraceRequest) error`.

- [ ] **Step 1: Write the descriptor test**

```go
package contracts_test

import (
	"testing"
	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
)

func TestIngestRequestDoesNotExposeTenantID(t *testing.T) {
	fields := (&memjevv1.IngestTraceRequest{}).ProtoReflect().Descriptor().Fields()
	if fields.ByName("tenant_id") != nil {
		t.Fatal("tenant_id must come from authentication, never the request")
	}
	if fields.ByName("idempotency_key") != nil {
		t.Fatal("idempotency_key belongs to authenticated transport metadata")
	}
}
```

- [ ] **Step 2: Run the test and verify generation is missing**

Run: `go test ./internal/contracts -run TestIngestRequestDoesNotExposeTenantID -v`

Expected: FAIL because generated messages do not exist.

- [ ] **Step 3: Define the public messages**

Define enums `ConsentMode`, `EventKind`, `ToolResultState`, and `IngestDisposition`. Define messages with these exact semantic fields:

```protobuf
message TraceEvent {
  string client_event_id = 1;
  google.protobuf.Timestamp occurred_at = 2;
  EventKind kind = 3;
  string tool_name = 4;
  string tool_version = 5;
  repeated Field fields = 6;
  ToolResult result = 7;
}

message Field {
  string name = 1;
  string string_value = 2;
}

message ToolResult {
  ToolResultState state = 1;
  optional int32 exit_code = 2;
  repeated Field evidence = 3;
}

message IngestTraceRequest {
  string client_trace_id = 1;
  string harness = 2;
  string harness_version = 3;
  string task = 4;
  repeated TraceEvent events = 5;
  repeated Field environment = 6;
}

message IngestTraceResponse {
  string receipt_id = 1;
  string trace_id = 2;
  string canonical_hash = 3;
  IngestDisposition disposition = 4;
  uint32 accepted_events = 5;
}

service IngestService {
  rpc IngestTrace(IngestTraceRequest) returns (IngestTraceResponse) {}
}
```

Use Protovalidate annotations to require 1–256 character identifiers, a 1–128 character harness, and 1–5,000 events. `contracts.ValidateIngest` calls Protovalidate, rejects duplicate `client_event_id` values with a stable field path, and rejects `proto.Size(request) > 1<<20`. Fields are repeated name/value pairs instead of maps so canonical ordering is explicit.

- [ ] **Step 4: Generate, format, and verify descriptors**

Run: `make generate && gofmt -w gen && go test ./internal/contracts -v`

Expected: PASS, with no `tenant_id` or idempotency field in the request body.

- [ ] **Step 5: Commit the public contract**

```bash
git add proto gen internal/contracts
git commit -m "feat(api): define versioned ingest contract"
```

### Task 3: Domain identifiers and deterministic canonicalization

**Files:**
- Create: `internal/domain/ids.go`
- Create: `internal/domain/trace.go`
- Create: `internal/canonical/normalize.go`
- Create: `internal/canonical/marshal.go`
- Test: `internal/canonical/normalize_test.go`
- Test: `internal/canonical/testdata/canonical_trace.golden.json`

**Interfaces:**
- Consumes: generated `IngestTraceRequest` and authenticated tenant context.
- Produces: `canonical.Build(tenantID domain.TenantID, request *memjevv1.IngestTraceRequest) (domain.CanonicalBatch, error)` and `domain.CanonicalBatch.Hash`.

- [ ] **Step 1: Write canonicalization invariance tests**

```go
func TestBuildNormalizesOrderUnicodeAndPaths(t *testing.T) {
	a := requestWithFields([]field{{"z", "e\u0301"}, {"path", "./src/../src/main.go"}})
	b := requestWithFields([]field{{"path", "src/main.go"}, {"z", "é"}})

	gotA, err := Build(domain.TenantID("tenant_a"), a)
	if err != nil { t.Fatal(err) }
	gotB, err := Build(domain.TenantID("tenant_a"), b)
	if err != nil { t.Fatal(err) }
	if gotA.Hash != gotB.Hash { t.Fatalf("hashes differ: %s != %s", gotA.Hash, gotB.Hash) }
}

func TestBuildIncludesTenantInIdentity(t *testing.T) {
	req := requestWithFields(nil)
	a, _ := Build(domain.TenantID("tenant_a"), req)
	b, _ := Build(domain.TenantID("tenant_b"), req)
	if a.Hash == b.Hash { t.Fatal("cross-tenant canonical hashes must differ") }
}
```

In the same test file define `type field struct { Name, Value string }`. `requestWithFields([]field)` constructs one execute event at a fixed UTC timestamp, assigns each pair to a protobuf `Field`, and deep-copies its input so shuffle tests cannot alias messages.

- [ ] **Step 2: Run the tests and verify `Build` is missing**

Run: `go test ./internal/canonical -v`

Expected: FAIL with undefined `Build`.

- [ ] **Step 3: Define immutable domain types**

```go
type TenantID string
type TraceID string
type EventID string
type ReceiptID string
type ArchiveKey string

type CanonicalBatch struct {
	SchemaVersion string           `json:"schema_version"`
	TenantID      TenantID         `json:"tenant_id"`
	Trace         CanonicalTrace   `json:"trace"`
	Events        []CanonicalEvent `json:"events"`
	Hash          string           `json:"-"`
}
```

IDs derived from content use lowercase hexadecimal SHA-256 with type prefixes: `tr_`, `ev_`, `rc_`, and `ar_`. Event identity includes tenant ID, trace identity, canonical event position, and normalized event content.

- [ ] **Step 4: Implement normalization and RFC 8785 hashing**

Use Unicode NFC, slash-normalized clean relative paths, trimmed field names, sorted repeated fields by `(name, value)`, UTC RFC3339Nano timestamps, and rejection of duplicate field names where the schema requires uniqueness. Serialize with `encoding/json`, transform with `jcs.Transform`, and hash the transformed bytes:

```go
func MarshalAndHash(v any) ([]byte, string, error) {
	raw, err := json.Marshal(v)
	if err != nil { return nil, "", fmt.Errorf("marshal canonical value: %w", err) }
	canonical, err := jcs.Transform(raw)
	if err != nil { return nil, "", fmt.Errorf("canonicalize JSON: %w", err) }
	sum := sha256.Sum256(canonical)
	return canonical, hex.EncodeToString(sum[:]), nil
}
```

- [ ] **Step 5: Add golden and randomized ordering tests**

Run 500 deterministic shuffles of event fields and environment fields from a fixed PRNG seed. All outputs must equal `canonical_trace.golden.json` byte for byte and produce the same hash.

Run: `go test -race ./internal/canonical -count=1 -v`

Expected: PASS.

- [ ] **Step 6: Commit canonicalization**

```bash
git add internal/domain internal/canonical go.mod go.sum
git commit -m "feat: add deterministic canonical trace model"
```

### Task 4: Credential-bound tenancy, consent, and idempotency metadata

**Files:**
- Create: `internal/security/principal.go`
- Create: `internal/security/authenticator.go`
- Create: `internal/policy/consent.go`
- Create: `internal/api/interceptors/auth.go`
- Create: `internal/api/interceptors/idempotency.go`
- Test: `internal/api/interceptors/auth_test.go`
- Test: `internal/api/interceptors/idempotency_test.go`

**Interfaces:**
- Consumes: `Authorization: Bearer <opaque token>` and `Idempotency-Key` headers.
- Produces: `security.PrincipalFromContext(ctx)`, `security.Principal`, and `security.RequestMetadataFromContext(ctx)`.

- [ ] **Step 1: Write authentication and spoofing tests**

```go
func TestAuthenticatorIgnoresRequestTenantMetadata(t *testing.T) {
	auth := StaticAuthenticator{"token-a": {TenantID: "tenant_a", Consent: policy.LearnAndRecall}}
	req := connect.NewRequest(&memjevv1.IngestTraceRequest{})
	req.Header().Set("Authorization", "Bearer token-a")
	req.Header().Set("X-Tenant-ID", "tenant_b")
	principal, err := auth.Authenticate(req.Header())
	if err != nil { t.Fatal(err) }
	if principal.TenantID != "tenant_a" { t.Fatalf("tenant = %q", principal.TenantID) }
}

func TestIngestRejectsRecallOnlyConsent(t *testing.T) {
	if err := policy.AuthorizeIngest(policy.RecallOnly); !errors.Is(err, policy.ErrConsentDenied) {
		t.Fatalf("got %v", err)
	}
}
```

- [ ] **Step 2: Run the tests and verify the security types are missing**

Run: `go test ./internal/api/interceptors ./internal/policy -v`

Expected: FAIL with missing packages or symbols.

- [ ] **Step 3: Implement fail-closed principal resolution**

```go
type Principal struct {
	TenantID domain.TenantID
	Region   string
	Scopes   map[string]struct{}
	Consent  policy.ConsentMode
}

type Authenticator interface {
	Authenticate(http.Header) (Principal, error)
}
```

Missing, malformed, expired, unknown, or scope-incomplete credentials return Connect `CodeUnauthenticated` or `CodePermissionDenied` without revealing whether a tenant exists. `X-Tenant-ID` is rejected if present rather than tolerated.

- [ ] **Step 4: Implement strict idempotency metadata**

Accept 16–128 printable ASCII characters, hash the value with SHA-256 before storing or logging, and reject missing or malformed keys with `CodeInvalidArgument`. Store only `IdempotencyKeyHash` in context.

- [ ] **Step 5: Run interceptor tests**

Run: `go test -race ./internal/api/interceptors ./internal/policy -v`

Expected: PASS for missing token, tenant spoofing, deny, recall-only, learning consent, missing key, malformed key, and valid key cases.

- [ ] **Step 6: Commit the request boundary**

```bash
git add internal/security internal/policy internal/api/interceptors
git commit -m "feat(security): bind tenancy and consent to credentials"
```

### Task 5: Streaming trace sanitization and secret rejection

**Files:**
- Create: `internal/ingest/sanitize.go`
- Create: `internal/ingest/limits.go`
- Create: `internal/ingest/errors.go`
- Test: `internal/ingest/sanitize_test.go`
- Test: `internal/ingest/testdata/secret-corpus.json`

**Interfaces:**
- Consumes: validated `IngestTraceRequest` in memory and `SanitizerPolicy`.
- Produces: `Sanitize(*memjevv1.IngestTraceRequest, SanitizerPolicy) (*memjevv1.IngestTraceRequest, SanitizationReport, error)`.

- [ ] **Step 1: Write allow-list and credential tests**

```go
func TestSanitizeRedactsCredentialInsideAllowedCommand(t *testing.T) {
	req := traceWithField("command", "curl -H 'Authorization: Bearer sk-secret-value' https://example.test")
	clean, report, err := Sanitize(req, DefaultPolicy())
	if err != nil { t.Fatal(err) }
	got := clean.Events[0].Fields[0].StringValue
	if strings.Contains(got, "sk-secret-value") { t.Fatalf("secret survived: %q", got) }
	if report.Redactions != 1 { t.Fatalf("redactions = %d", report.Redactions) }
}

func TestSanitizeRejectsUnknownField(t *testing.T) {
	req := traceWithField("raw_stdout", "sensitive")
	_, _, err := Sanitize(req, DefaultPolicy())
	if !errors.Is(err, ErrForbiddenField) { t.Fatalf("got %v", err) }
}
```

In the same test file, `traceWithField(name, value string) *memjevv1.IngestTraceRequest` creates a one-event request with fixed identifiers and timestamp. `DefaultPolicy() SanitizerPolicy` is the production allow-list with the limits declared in Step 3; tests may clone it but cannot mutate the package-level default.

- [ ] **Step 2: Run the tests and verify sanitization is missing**

Run: `go test ./internal/ingest -run Sanitize -v`

Expected: FAIL with undefined `Sanitize`.

- [ ] **Step 3: Implement schema allow-listing and bounded values**

Allow only `command`, `path`, `url_host`, `query_shape`, `resource_type`, `resource_id`, `assertion`, and declared tool-contract fields. Reject unknown names. Apply per-value and total-size limits before pattern scanning. Strip C0/C1 control characters except tab and newline; normalize line endings.

- [ ] **Step 4: Implement layered secret handling**

Redact authorization headers, common API-key assignments, PEM blocks, JWTs, cloud access keys, and high-confidence entropy matches. Return stable reason codes and counts, never the matched value. Task descriptions receive the same scan. The output is a deep copy so raw request messages cannot later be archived accidentally.

- [ ] **Step 5: Verify the secret corpus and no-sensitive-error invariant**

For every corpus item, assert the secret is absent from sanitized protobuf JSON, the error string, and the structured `SanitizationReport`.

Run: `go test -race ./internal/ingest -v`

Expected: PASS.

- [ ] **Step 6: Commit the privacy boundary**

```bash
git add internal/ingest
git commit -m "feat(ingest): sanitize traces before persistence"
```

### Task 6: Content-addressed archive capability and S3 adapter

**Files:**
- Create: `internal/archive/archive.go`
- Create: `internal/archive/memory.go`
- Create: `internal/archive/s3.go`
- Test: `internal/archive/archive_test.go`
- Test: `internal/archive/s3_test.go`

**Interfaces:**
- Consumes: canonical JSON bytes, SHA-256 hash, tenant ID, schema version.
- Produces: `archive.Store.PutCanonical(ctx, archive.PutRequest) (archive.Object, error)` and `archive.Store.Get(ctx, archive.Key) ([]byte, error)`.

- [ ] **Step 1: Write idempotent archive tests**

```go
func TestPutCanonicalIsContentAddressedAndIdempotent(t *testing.T) {
	store := NewMemoryStore()
	body := []byte(`{"a":1}`)
	sum := sha256.Sum256(body)
	req := PutRequest{TenantID: "tenant_a", SchemaVersion: "v1", Hash: hex.EncodeToString(sum[:]), Body: body}
	first, err := store.PutCanonical(context.Background(), req)
	if err != nil { t.Fatal(err) }
	second, err := store.PutCanonical(context.Background(), req)
	if err != nil { t.Fatal(err) }
	if first.Key != second.Key || store.PutCount() != 1 { t.Fatalf("first=%#v second=%#v puts=%d", first, second, store.PutCount()) }
}
```

- [ ] **Step 2: Run the test and verify archive types are missing**

Run: `go test ./internal/archive -v`

Expected: FAIL with undefined `NewMemoryStore`.

- [ ] **Step 3: Define the capability and key format**

```go
type Store interface {
	PutCanonical(context.Context, PutRequest) (Object, error)
	Get(context.Context, Key) ([]byte, error)
}

type PutRequest struct {
	TenantID     domain.TenantID
	SchemaVersion string
	Hash         string
	Body         []byte
}
```

Keys use `canonical/<tenant-sha256-prefix>/<schema-version>/<content-hash>.json`; never place the raw tenant ID in an object key. Verify that `Hash` equals SHA-256 of `Body` before any network call.

- [ ] **Step 4: Implement memory and S3 adapters**

The S3 adapter uses `PutObject` with `IfNoneMatch: "*"`, server-side encryption configuration, `ContentType: application/json`, and metadata containing only schema version and content hash. Treat precondition failure as successful idempotent reuse after a `HeadObject` size/hash check. Map timeouts and throttling to retryable typed errors.

- [ ] **Step 5: Test S3 request construction without credentials**

Use a fake `S3API` interface exposing `PutObject`, `HeadObject`, and `GetObject`. Assert bucket, key, conditional header, encryption fields, metadata allow-list, and that tenant content or raw task text never appears in metadata.

Run: `go test -race ./internal/archive -v`

Expected: PASS.

- [ ] **Step 6: Commit archive capability**

```bash
git add internal/archive go.mod go.sum
git commit -m "feat(storage): add content-addressed canonical archive"
```

### Task 7: Schema-full SurrealDB foundation and migration runner

**Files:**
- Create: `db/migrations/0001_foundation.surql`
- Create: `internal/store/migrations.go`
- Create: `internal/store/surreal/client.go`
- Create: `internal/store/surreal/migrations.go`
- Create: `internal/testinfra/surreal.go`
- Test: `internal/store/surreal/migrations_test.go`

**Interfaces:**
- Consumes: SurrealDB WebSocket endpoint, namespace, database, and scoped credentials.
- Produces: `store.Migrator.Apply(ctx) error`, schema version record, and schema-full foundation tables.

- [ ] **Step 1: Write the migration idempotency integration test**

```go
func TestFoundationMigrationIsIdempotent(t *testing.T) {
	db := testinfra.StartSurreal(t, "surrealdb/surrealdb:v3.2.4")
	m := NewMigrator(db)
	if err := m.Apply(context.Background()); err != nil { t.Fatal(err) }
	if err := m.Apply(context.Background()); err != nil { t.Fatal(err) }
	if got := schemaVersion(t, db); got != 1 { t.Fatalf("schema version = %d", got) }
}
```

`schemaVersion(t, db) int` performs a parameter-free `SELECT version FROM schema_migration ORDER BY version DESC LIMIT 1`, fails the test on query/decode errors, and returns zero when the table is empty.

- [ ] **Step 2: Run the integration test and verify migration support is missing**

Run: `go test -tags=integration ./internal/store/surreal -run FoundationMigration -v`

Expected: FAIL with missing migration code.

- [ ] **Step 3: Define schema-full foundation tables**

`0001_foundation.surql` must define `tenant`, `consent_policy`, `trace_run`, `canonical_event`, `ingest_receipt`, `archive_object`, `outbox_job`, `policy_bundle`, and `schema_migration`. Every tenant-owned table requires `tenant_id`, `created_at`, `schema_version`, and `content_hash` where applicable. Define unique indexes:

- `(tenant_id, idempotency_key_hash)` on `ingest_receipt`;
- `(tenant_id, trace_id)` on `trace_run`;
- `(tenant_id, event_id)` on `canonical_event`;
- `(tenant_id, workflow_id)` on `outbox_job`;
- `(tenant_id, archive_key)` on `archive_object`.

Permissions must deny anonymous table access. Application queries still include explicit tenant predicates.

- [ ] **Step 4: Implement checksum-verified forward-only migrations**

Embed `.surql` files with `//go:embed`, SHA-256 each migration, and store version plus checksum. Refuse startup when an applied migration's checksum differs. Apply each migration in one transaction; never edit an applied migration.

- [ ] **Step 5: Test schema rejection and checksum drift**

Insert a `canonical_event` without `tenant_id` and assert schema rejection. Replace the embedded checksum through a test seam and assert `ErrMigrationChecksum` without running new statements.

Run: `go test -race -tags=integration ./internal/store/surreal -v`

Expected: PASS.

- [ ] **Step 6: Commit database foundation**

```bash
git add db internal/store internal/testinfra go.mod go.sum
git commit -m "feat(db): add schema-full SurrealDB foundation"
```

### Task 8: Atomic ingest ledger and outbox repository

**Files:**
- Create: `internal/store/ingest.go`
- Create: `internal/store/surreal/ingest.go`
- Create: `internal/store/memory/ingest.go`
- Test: `internal/store/memory/ingest_test.go`
- Test: `internal/store/surreal/ingest_test.go`

**Interfaces:**
- Consumes: `store.CommitIngestRequest` with principal tenant, canonical batch, archive object, and idempotency-key hash.
- Produces: `store.IngestRepository.Commit(ctx, request) (store.IngestReceipt, error)` with `ErrIdempotencyConflict` and deterministic workflow ID.

- [ ] **Step 1: Write repository contract tests**

```go
func TestConcurrentIdenticalCommitCreatesOneAggregate(t *testing.T) {
	repo := newRepository(t)
	req := validCommitRequest()
	var wg sync.WaitGroup
	receipts := make(chan store.IngestReceipt, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); got, err := repo.Commit(context.Background(), req); if err != nil { t.Error(err); return }; receipts <- got }()
	}
	wg.Wait()
	close(receipts)
	assertOneReceiptID(t, receipts)
	assertAggregateCounts(t, repo, 1, len(req.Batch.Events), 1)
}
```

Run this contract suite twice: `newRepository(t)` returns the in-memory adapter in the unit suite and a fresh SurrealDB-backed adapter in the integration suite. `validCommitRequest()` uses fixed canonical and archive hashes. `assertOneReceiptID` drains every result and requires one distinct ID. `assertAggregateCounts` queries adapter-owned test inspection methods and requires exactly the supplied receipt, event, and outbox counts.

- [ ] **Step 2: Run memory and Surreal contract tests and verify missing implementation**

Run: `go test -race ./internal/store/memory ./internal/store/surreal -run Commit -v`

Expected: FAIL with undefined repository implementations.

- [ ] **Step 3: Define commit semantics**

```go
type IngestRepository interface {
	Commit(context.Context, CommitIngestRequest) (IngestReceipt, error)
}

type CommitIngestRequest struct {
	TenantID          domain.TenantID
	IdempotencyKeyHash string
	Batch             domain.CanonicalBatch
	Archive           archive.Object
}
```

On an existing `(tenant, idempotency key)` with the same canonical hash, return the original receipt with disposition `duplicate`. With a different hash, return `ErrIdempotencyConflict` and change nothing.

- [ ] **Step 4: Implement memory repository then SurrealDB transaction**

Use an interactive WebSocket transaction. Insert or resolve the receipt invariant, then create the trace, archive reference, events, and one outbox job. The workflow ID is `synthesize/<tenant-hash>/<trace-id>/v1`. Retry recognized write conflicts with bounded exponential backoff and jitter; context cancellation stops retries.

- [ ] **Step 5: Run concurrency and rollback tests**

Add tests for identical duplicates, conflicting duplicates, injected failure after event insertion, tenant mismatch, archive hash mismatch, and 16 concurrent commits. After injected failure, all table counts must remain unchanged.

Run: `go test -race -tags=integration ./internal/store/... -count=1 -v`

Expected: PASS.

- [ ] **Step 6: Commit atomic persistence**

```bash
git add internal/store
git commit -m "feat(db): commit trace ledger and outbox atomically"
```

### Task 9: Ingest application service and two-system convergence

**Files:**
- Create: `internal/ingest/service.go`
- Create: `internal/ingest/result.go`
- Test: `internal/ingest/service_test.go`

**Interfaces:**
- Consumes: authenticated `security.Principal`, idempotency hash, raw in-memory protobuf request, `archive.Store`, and `store.IngestRepository`.
- Produces: `ingest.Service.Ingest(ctx, ingest.Command) (ingest.Result, error)`.

- [ ] **Step 1: Write the archive-before-ledger and retry-convergence tests**

```go
func TestIngestArchiveSuccessDatabaseFailureConvergesOnRetry(t *testing.T) {
	archives := archive.NewMemoryStore()
	repo := newFailOnceRepository()
	svc := NewService(archives, repo, DefaultPolicy())
	cmd := validCommand()
	if _, err := svc.Ingest(context.Background(), cmd); err == nil { t.Fatal("first call must fail") }
	got, err := svc.Ingest(context.Background(), cmd)
	if err != nil { t.Fatal(err) }
	if archives.PutCount() != 1 { t.Fatalf("archive writes = %d", archives.PutCount()) }
	if got.Disposition != DispositionAccepted { t.Fatalf("disposition = %v", got.Disposition) }
}
```

`validCommand()` creates a principal with `learn_and_recall`, a valid idempotency hash, and a fixed valid trace request. `newFailOnceRepository()` records no state and returns a retryable unavailable error on its first `Commit`, then delegates to the in-memory repository on later calls.

- [ ] **Step 2: Run service tests and verify service is missing**

Run: `go test ./internal/ingest -run Ingest -v`

Expected: FAIL with undefined `NewService`.

- [ ] **Step 3: Implement the ordered ingest use case**

The service must authorize consent, validate Protovalidate constraints, deep-copy and sanitize, canonicalize, write the archive, then commit the repository. No external call occurs inside the database transaction. Wrap errors with stable operation names but never with request values.

- [ ] **Step 4: Implement idempotency conflict and cancellation behavior**

Reusing a key with different content maps to `ErrIdempotencyConflict`. Cancellation before archive write creates no state. Cancellation after archive write but before commit may leave one orphan object and creates no ledger record. Retry with the same request must converge.

- [ ] **Step 5: Run all use-case tests with race detection**

Run: `go test -race ./internal/ingest -count=1 -v`

Expected: PASS for consent, sanitization, canonicalization, archive failure, repository failure, retry convergence, duplicates, conflict, and cancellation.

- [ ] **Step 6: Commit the ingest use case**

```bash
git add internal/ingest
git commit -m "feat(ingest): orchestrate durable canonical trace capture"
```

### Task 10: Connect API with safe errors and body limits

**Files:**
- Create: `internal/api/server.go`
- Create: `internal/api/ingest_handler.go`
- Create: `internal/api/health_handler.go`
- Create: `internal/api/errors.go`
- Test: `internal/api/ingest_handler_test.go`
- Test: `internal/api/server_test.go`

**Interfaces:**
- Consumes: generated Connect handlers, security interceptors, `ingest.Service`, `buildinfo.Info`.
- Produces: `api.NewHandler(api.Dependencies) http.Handler` and public Connect/JSON endpoints.

- [ ] **Step 1: Write black-box HTTP tests**

```go
func TestIngestRejectsTenantHeaderAndDoesNotEchoIt(t *testing.T) {
	server := httptest.NewServer(newTestHandler(t))
	defer server.Close()
	req := validHTTPRequest(t, server.URL)
	req.Header.Set("Authorization", "Bearer token-a")
	req.Header.Set("Idempotency-Key", "0123456789abcdef")
	req.Header.Set("X-Tenant-ID", "tenant_b_secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil { t.Fatal(err) }
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusBadRequest { t.Fatalf("status=%d body=%s", resp.StatusCode, body) }
	if bytes.Contains(body, []byte("tenant_b_secret")) { t.Fatalf("response leaked header: %s", body) }
}
```

`newTestHandler(t)` wires a static authenticator, in-memory archive, in-memory ingest repository, and fixed build info. `validHTTPRequest(t, baseURL)` marshals a minimal valid `IngestTraceRequest` as Connect JSON and posts it to the generated procedure path.

- [ ] **Step 2: Run HTTP tests and verify handlers are missing**

Run: `go test ./internal/api -v`

Expected: FAIL with undefined handler construction.

- [ ] **Step 3: Implement generated Connect handlers**

Convert generated messages into `ingest.Command` only after auth and idempotency interceptors populate context. Map domain failures to stable Connect codes: invalid input, unauthenticated, permission denied, conflict, resource exhausted, unavailable, and internal. Include a generated request ID, retryability, and safe reason code; never echo submitted values.

- [ ] **Step 4: Add transport protections**

Enforce 1 MiB decoded request size, 5,000-event limit, 10-second server timeout, panic recovery, secure response headers, and explicit method/content-type handling. Health has liveness and readiness; readiness verifies database and archive configuration without writing.

- [ ] **Step 5: Run black-box tests**

Cover success, duplicate, idempotency conflict, missing key, invalid token, spoofed tenant header, consent failure, oversized body, malformed protobuf JSON, downstream timeout, and redacted internal error.

Run: `go test -race ./internal/api -count=1 -v`

Expected: PASS.

- [ ] **Step 6: Commit the API boundary**

```bash
git add internal/api
git commit -m "feat(api): expose authenticated trace ingestion"
```

### Task 11: Configuration, telemetry, and runnable API binary

**Files:**
- Create: `internal/config/config.go`
- Create: `internal/observability/otel.go`
- Create: `internal/observability/logging.go`
- Create: `cmd/api/main.go`
- Test: `internal/config/config_test.go`
- Test: `internal/observability/logging_test.go`
- Test: `cmd/api/main_test.go`

**Interfaces:**
- Consumes: environment variables and dependency constructors from earlier tasks.
- Produces: runnable `memjev-api`, OpenTelemetry spans/metrics, and structured safe logs.

- [ ] **Step 1: Write strict configuration and log-redaction tests**

```go
func TestLoadRejectsUnknownEnvironmentVariable(t *testing.T) {
	t.Setenv("MEMJEV_UNKNOWN", "secret")
	_, err := Load()
	if !errors.Is(err, ErrUnknownVariable) { t.Fatalf("got %v", err) }
}

func TestRequestLogContainsNoSubmittedText(t *testing.T) {
	buf := new(bytes.Buffer)
	logger := NewJSONLogger(buf)
	LogRequest(logger, RequestFacts{RequestID: "req_1", TenantHash: "abc", Bytes: 42, ResultCode: "accepted"})
	if strings.Contains(buf.String(), "task") || strings.Contains(buf.String(), "command") { t.Fatalf("unsafe log: %s", buf) }
}
```

- [ ] **Step 2: Run tests and verify config/telemetry packages are missing**

Run: `go test ./internal/config ./internal/observability ./cmd/api -v`

Expected: FAIL with missing packages.

- [ ] **Step 3: Implement strict typed configuration**

Support exact `MEMJEV_` variables for listen address, shutdown timeout, Surreal endpoint/namespace/database/user/password, archive bucket/region/endpoint/encryption key, OTLP endpoint, environment, and build metadata. Require TLS endpoints outside `development`. Return field names but never values in errors.

- [ ] **Step 4: Instrument the request and storage path**

Emit spans for sanitize, canonicalize, archive put, database commit, and handler total. Metrics include request count, result code, latency, event count, bytes, archive reuse, transaction retry count, and outbox creation. Tenant is represented only by an irreversible bounded-cardinality hash when explicitly enabled.

- [ ] **Step 5: Wire graceful startup and shutdown**

`cmd/api/main.go` loads config, initializes telemetry, connects SurrealDB over WebSocket, applies migrations, constructs S3/archive and repositories, starts one `http.Server`, handles SIGTERM/SIGINT, stops accepting requests, waits for the configured drain period, closes clients, flushes telemetry, and exits non-zero on failed startup or forced shutdown.

- [ ] **Step 6: Run unit and startup tests**

Run: `go test -race ./internal/config ./internal/observability ./cmd/api -v`

Expected: PASS, including a subprocess test that starts the binary with fake adapters, observes readiness, sends SIGTERM, and observes exit code 0.

- [ ] **Step 7: Commit the runnable service**

```bash
git add internal/config internal/observability cmd/api go.mod go.sum
git commit -m "feat: add observable ingest API service"
```

### Task 12: Local production-shaped environment and CI release gate

**Files:**
- Create: `Dockerfile`
- Create: `compose.yaml`
- Create: `.dockerignore`
- Create: `.github/workflows/ci.yml`
- Create: `scripts/integration.sh`
- Create: `scripts/smoke.sh`
- Create: `docs/runbooks/local-development.md`
- Create: `docs/runbooks/ingest-failure.md`
- Test: `tests/e2e/ingest_test.go`

**Interfaces:**
- Consumes: runnable API, SurrealDB migration, S3 adapter, public protobuf contract.
- Produces: reproducible local stack, end-to-end ingest proof, and CI release gate for foundation changes.

- [ ] **Step 1: Write the failing end-to-end test**

The test must start the Compose stack, call health readiness, ingest a trace containing shuffled fields and a redacted credential, retry the same request, then inspect SurrealDB and the object store through test-only credentials. Assert one receipt, one trace, exact event count, one outbox job, one archive object, byte-identical canonical content, and no secret occurrence in any persisted representation.

Run: `go test -tags=e2e ./tests/e2e -run TestDurableIdempotentIngest -v`

Expected: FAIL because the production-shaped stack is not configured.

- [ ] **Step 2: Build a locked-down container image**

Use a multi-stage Go build with `CGO_ENABLED=0`, `-trimpath`, and build metadata ldflags. Runtime uses a non-root numeric UID, read-only root filesystem, no shell, and only the API binary plus CA certificates. Add OCI source, revision, and version labels.

- [ ] **Step 3: Define the local stack**

Pin SurrealDB `v3.2.4` and MinIO `RELEASE.2025-04-22T22-12-26Z`, and build the local API image from the committed Dockerfile. Use health checks, a private network, named data volumes, and development-only credentials generated by `scripts/integration.sh` into an untracked `.env.local`. Do not put secrets in `compose.yaml` or committed example files.

- [ ] **Step 4: Add CI jobs**

CI runs generated-code drift, Buf lint/breaking checks, unit tests with race detection, static analysis, SurrealDB integration tests, end-to-end Compose test, vulnerability scan, container build, and SBOM generation. Cancel superseded branch runs. Pin GitHub Actions by commit SHA.

- [ ] **Step 5: Document failure handling and execute the full gate**

The ingest runbook must distinguish archive failures, orphan objects, transaction conflicts, idempotency conflicts, database unavailability, and outbox backlog. Each entry specifies observable symptoms, safe retry behavior, and whether operator action is required.

Run: `make verify && ./scripts/integration.sh && ./scripts/smoke.sh`

Expected: all checks PASS; smoke output contains request/receipt identifiers and result codes only.

- [ ] **Step 6: Commit the production foundation gate**

```bash
git add Dockerfile compose.yaml .dockerignore .github scripts docs/runbooks tests/e2e
git commit -m "ci: qualify the production ingest foundation"
```

## Completion gate for Plan 1

Before beginning the evidence-pipeline plan:

1. `make verify` passes from a clean checkout.
2. SurrealDB integration and end-to-end tests pass twice consecutively.
3. `git diff --exit-code` is clean after code generation and tests.
4. A stored archive object can rebuild the trace and event IDs exactly.
5. The concurrency test proves one durable aggregate for identical requests.
6. The secret corpus is absent from logs, responses, SurrealDB, archive data, and object metadata.
7. The API succeeds with Jev, embeddings, and workflow execution disabled.
8. A reviewer confirms every tenant-owned query includes credential-derived tenant scope.
