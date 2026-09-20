package surreal

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sauhard74/mem-jev/internal/archive"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/rebuild"
	"github.com/sauhard74/mem-jev/internal/store"
	"github.com/sauhard74/mem-jev/internal/toolcontract"
	memworkflow "github.com/sauhard74/mem-jev/internal/workflow"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

type StageArtifactRepository struct {
	db *surrealdb.DB
}

func NewStageArtifactRepository(db *surrealdb.DB) *StageArtifactRepository {
	return &StageArtifactRepository{db: db}
}

func (r *StageArtifactRepository) PutStageArtifact(ctx context.Context, artifact memworkflow.StageArtifact) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil || r.db == nil {
		return errors.New("SurrealDB stage artifact repository is not configured")
	}
	if err := memworkflow.ValidateStageArtifact(artifact); err != nil {
		return memworkflow.ErrStageArtifactConflict
	}
	existing, found, err := r.getStageArtifact(ctx, artifact.TenantID, artifact.WorkflowID, artifact.Stage)
	if err != nil {
		return err
	}
	if found {
		if existing.ContentHash == artifact.ContentHash && existing.InputHash == artifact.InputHash {
			return nil
		}
		return memworkflow.ErrStageArtifactConflict
	}
	id := models.NewRecordID("pipeline_stage_artifact", stageArtifactID(artifact.TenantID, artifact.WorkflowID, artifact.Stage))
	record := map[string]any{
		"tenant_id": string(artifact.TenantID), "workflow_id": artifact.WorkflowID, "stage": string(artifact.Stage),
		"input_hash": artifact.InputHash, "payload": string(artifact.Payload), "status": string(artifact.Status),
		"created_at": artifact.CreatedAt, "expires_at": artifact.ExpiresAt,
		"schema_version": "pipeline-stage.v1", "content_hash": artifact.ContentHash,
	}
	if artifact.Code != "" {
		record["code"] = artifact.Code
	}
	_, err = surrealdb.Query[any](ctx, r.db, `CREATE ONLY $id CONTENT $record`, map[string]any{"id": id, "record": record})
	if err == nil {
		return nil
	}
	resolved, resolvedFound, lookupErr := r.getStageArtifact(ctx, artifact.TenantID, artifact.WorkflowID, artifact.Stage)
	if lookupErr == nil && resolvedFound && resolved.ContentHash == artifact.ContentHash && resolved.InputHash == artifact.InputHash {
		return nil
	}
	if lookupErr != nil {
		return databaseFailure("resolve stage artifact write", lookupErr)
	}
	return databaseFailure("write stage artifact", err)
}

func (r *StageArtifactRepository) GetStageArtifact(ctx context.Context, tenantID domain.TenantID, workflowID string, stage memworkflow.Stage) (memworkflow.StageArtifact, error) {
	artifact, found, err := r.getStageArtifact(ctx, tenantID, workflowID, stage)
	if err != nil {
		return memworkflow.StageArtifact{}, databaseFailure("read stage artifact", err)
	}
	if !found {
		return memworkflow.StageArtifact{}, memworkflow.ErrStageArtifactNotFound
	}
	if err := memworkflow.ValidateStageArtifact(artifact); err != nil {
		return memworkflow.StageArtifact{}, err
	}
	return artifact, nil
}

func (r *StageArtifactRepository) getStageArtifact(ctx context.Context, tenantID domain.TenantID, workflowID string, stage memworkflow.Stage) (memworkflow.StageArtifact, bool, error) {
	if r == nil || r.db == nil {
		return memworkflow.StageArtifact{}, false, errors.New("SurrealDB stage artifact repository is not configured")
	}
	type row struct {
		TenantID    string    `json:"tenant_id"`
		WorkflowID  string    `json:"workflow_id"`
		Stage       string    `json:"stage"`
		InputHash   string    `json:"input_hash"`
		Payload     string    `json:"payload"`
		Status      string    `json:"status"`
		Code        *string   `json:"code"`
		CreatedAt   time.Time `json:"created_at"`
		ExpiresAt   time.Time `json:"expires_at"`
		ContentHash string    `json:"content_hash"`
	}
	results, err := surrealdb.Query[[]row](ctx, r.db, `SELECT * FROM pipeline_stage_artifact
		WHERE tenant_id = $tenant_id AND workflow_id = $workflow_id AND stage = $stage LIMIT 1`, map[string]any{
		"tenant_id": string(tenantID), "workflow_id": workflowID, "stage": string(stage),
	})
	if err != nil || results == nil || len(*results) == 0 || len((*results)[0].Result) == 0 {
		return memworkflow.StageArtifact{}, false, err
	}
	value := (*results)[0].Result[0]
	artifact := memworkflow.StageArtifact{
		TenantID: domain.TenantID(value.TenantID), WorkflowID: value.WorkflowID, Stage: memworkflow.Stage(value.Stage),
		InputHash: value.InputHash, ContentHash: value.ContentHash, Payload: []byte(value.Payload), Status: memworkflow.StageStatus(value.Status),
		CreatedAt: value.CreatedAt.UTC(), ExpiresAt: value.ExpiresAt.UTC(),
	}
	if value.Code != nil {
		artifact.Code = *value.Code
	}
	return artifact, true, nil
}

func stageArtifactID(tenantID domain.TenantID, workflowID string, stage memworkflow.Stage) string {
	return "stage_" + hashString(string(tenantID)+"\x00"+workflowID+"\x00"+string(stage))
}

type WorkflowSourceLoader struct {
	db     *surrealdb.DB
	loader *rebuild.Loader
}

func NewWorkflowSourceLoader(db *surrealdb.DB, loader *rebuild.Loader) *WorkflowSourceLoader {
	return &WorkflowSourceLoader{db: db, loader: loader}
}

type traceSourceRow struct {
	SchemaVersion string `json:"schema_version"`
	ContentHash   string `json:"content_hash"`
	ArchiveKey    string `json:"archive_key"`
}

func (l *WorkflowSourceLoader) Load(ctx context.Context, input memworkflow.SynthesisInput) (memworkflow.LoadedSource, error) {
	if l == nil || l.db == nil || l.loader == nil {
		return memworkflow.LoadedSource{}, errors.New("workflow source loader is not configured")
	}
	trace, found, err := findTraceSource(ctx, l.db, input.TenantID, input.TraceID)
	if err != nil {
		return memworkflow.LoadedSource{}, err
	}
	if !found {
		return memworkflow.LoadedSource{}, &memworkflow.StageError{Code: "trace_not_found", Err: store.ErrOutcomeTraceNotFound}
	}
	batch, err := l.loader.Load(ctx, rebuild.Request{
		TenantID: input.TenantID, TraceID: input.TraceID, SchemaVersion: trace.SchemaVersion,
		ContentHash: trace.ContentHash, ArchiveKey: archive.Key(trace.ArchiveKey),
	})
	if err != nil {
		return memworkflow.LoadedSource{}, classifySourceError(err)
	}
	result := memworkflow.LoadedSource{SchemaVersion: "loaded-source.v1", Batch: batch, ArchiveHash: trace.ContentHash}
	if input.JobType == store.OutboxJobSynthesizeOutcome {
		outcome, outcomeFound, outcomeErr := loadStoredOutcome(ctx, l.db, input.TenantID, input.OutcomeID)
		if outcomeErr != nil {
			return memworkflow.LoadedSource{}, outcomeErr
		}
		if !outcomeFound {
			return memworkflow.LoadedSource{}, &memworkflow.StageError{Code: "outcome_not_found", Err: store.ErrOutboxJobNotFound}
		}
		result.Outcome = &outcome
	}
	return result, nil
}

func findTraceSource(ctx context.Context, db *surrealdb.DB, tenantID domain.TenantID, traceID domain.TraceID) (traceSourceRow, bool, error) {
	results, err := surrealdb.Query[[]traceSourceRow](ctx, db, `SELECT schema_version, archive_key FROM trace_run
		WHERE tenant_id = $tenant_id AND trace_id = $trace_id LIMIT 1`, map[string]any{"tenant_id": string(tenantID), "trace_id": string(traceID)})
	if err != nil || results == nil || len(*results) == 0 || len((*results)[0].Result) == 0 {
		return traceSourceRow{}, false, err
	}
	row := (*results)[0].Result[0]
	type archiveRow struct {
		ContentHash string `json:"content_hash"`
	}
	archives, err := surrealdb.Query[[]archiveRow](ctx, db, `SELECT content_hash FROM archive_object
		WHERE tenant_id = $tenant_id AND archive_key = $archive_key LIMIT 1`, map[string]any{
		"tenant_id": string(tenantID), "archive_key": row.ArchiveKey,
	})
	if err != nil || archives == nil || len(*archives) == 0 || len((*archives)[0].Result) == 0 {
		return traceSourceRow{}, false, err
	}
	row.ContentHash = (*archives)[0].Result[0].ContentHash
	return row, true, nil
}

func loadStoredOutcome(ctx context.Context, db *surrealdb.DB, tenantID domain.TenantID, outcomeID domain.OutcomeID) (memworkflow.StoredOutcome, bool, error) {
	type outcomeRow struct {
		ID                string `json:"outcome_id"`
		ContentHash       string `json:"content_hash"`
		State             string `json:"state"`
		PromotionEligible bool   `json:"promotion_eligible"`
		PolicyVersion     string `json:"policy_version"`
		EvidenceCount     int    `json:"evidence_count"`
	}
	results, err := surrealdb.Query[[]outcomeRow](ctx, db, `SELECT outcome_id, content_hash, state, promotion_eligible, policy_version, evidence_count
		FROM outcome_evidence WHERE tenant_id = $tenant_id AND outcome_id = $outcome_id LIMIT 1`, map[string]any{
		"tenant_id": string(tenantID), "outcome_id": string(outcomeID),
	})
	if err != nil || results == nil || len(*results) == 0 || len((*results)[0].Result) == 0 {
		return memworkflow.StoredOutcome{}, false, err
	}
	row := (*results)[0].Result[0]
	evidenceRows, err := surrealdb.Query[[]verificationRow](ctx, db, `SELECT evidence_id, class, verdict, predicate_id, verifier_id,
		verifier_version, observed_at, payload.fields AS fields FROM verification_result
		WHERE tenant_id = $tenant_id AND outcome_id = $outcome_id ORDER BY evidence_id ASC`, map[string]any{
		"tenant_id": string(tenantID), "outcome_id": string(outcomeID),
	})
	if err != nil || evidenceRows == nil || len(*evidenceRows) == 0 {
		return memworkflow.StoredOutcome{}, false, err
	}
	facts := make([]domain.OutcomeEvidence, len((*evidenceRows)[0].Result))
	for index, evidenceRow := range (*evidenceRows)[0].Result {
		facts[index] = evidenceRow.domainValue()
	}
	if len(facts) != row.EvidenceCount {
		return memworkflow.StoredOutcome{}, false, &memworkflow.StageError{Code: "evidence_mismatch", Err: memworkflow.ErrStageArtifactConflict}
	}
	return memworkflow.StoredOutcome{
		ID: domain.OutcomeID(row.ID), ContentHash: row.ContentHash, State: domain.OutcomeState(row.State),
		PromotionEligible: row.PromotionEligible, PolicyVersion: row.PolicyVersion, Evidence: facts,
	}, true, nil
}

type verificationRow struct {
	EvidenceID      string                  `json:"evidence_id"`
	Class           string                  `json:"class"`
	Verdict         string                  `json:"verdict"`
	PredicateID     string                  `json:"predicate_id"`
	VerifierID      string                  `json:"verifier_id"`
	VerifierVersion *string                 `json:"verifier_version"`
	ObservedAt      time.Time               `json:"observed_at"`
	Fields          []domain.CanonicalField `json:"fields"`
}

func (r verificationRow) domainValue() domain.OutcomeEvidence {
	value := domain.OutcomeEvidence{
		ID: domain.EvidenceID(r.EvidenceID), Class: domain.EvidenceClass(r.Class), Verdict: domain.EvidenceVerdict(r.Verdict),
		PredicateID: r.PredicateID, VerifierID: r.VerifierID, ObservedAt: r.ObservedAt.UTC().Format(time.RFC3339Nano), Fields: r.Fields,
	}
	if r.VerifierVersion != nil {
		value.VerifierVersion = *r.VerifierVersion
	}
	return value
}

func classifySourceError(err error) error {
	if errors.Is(err, rebuild.ErrIdentityMismatch) || errors.Is(err, archive.ErrHashMismatch) || errors.Is(err, archive.ErrArchiveConflict) {
		return &memworkflow.StageError{Code: "archive_corrupt", Err: err}
	}
	if errors.Is(err, rebuild.ErrUnsupportedSchema) {
		return &memworkflow.StageError{Code: "unsupported_schema", Err: err}
	}
	var archiveError *archive.OpError
	if errors.As(err, &archiveError) {
		return &memworkflow.StageError{Code: "archive_unavailable", Retryable: archiveError.Retryable, Err: err}
	}
	return err
}

type TenantRegistryProvider struct {
	db *surrealdb.DB
}

func NewTenantRegistryProvider(db *surrealdb.DB) *TenantRegistryProvider {
	return &TenantRegistryProvider{db: db}
}

func (p *TenantRegistryProvider) ForTenant(tenantID domain.TenantID) toolcontract.Registry {
	return &tenantToolRegistry{db: p.db, tenantID: tenantID}
}

type tenantToolRegistry struct {
	db       *surrealdb.DB
	tenantID domain.TenantID
	mu       sync.Mutex
	loaded   bool
	memory   *toolcontract.MemoryRegistry
	version  string
	err      error
}

func (r *tenantToolRegistry) Resolve(ctx context.Context, query toolcontract.Query) (toolcontract.Resolution, error) {
	if r == nil || r.db == nil || r.tenantID == "" {
		return toolcontract.Resolution{}, toolcontract.ErrInvalidQuery
	}
	if err := r.load(ctx); err != nil {
		return toolcontract.Resolution{}, err
	}
	return r.memory.Resolve(ctx, query)
}

func (r *tenantToolRegistry) SnapshotVersion() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.version
}

func (r *tenantToolRegistry) load(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.loaded {
		return r.err
	}
	r.loaded = true
	type row struct {
		ContractVersionID string         `json:"contract_version_id"`
		Manifest          map[string]any `json:"manifest"`
		ContentHash       string         `json:"content_hash"`
	}
	results, err := surrealdb.Query[[]row](ctx, r.db, `SELECT contract_version_id, manifest, content_hash FROM tool_contract_version
		WHERE tenant_id = $tenant_id ORDER BY contract_version_id ASC`, map[string]any{
		"tenant_id": string(r.tenantID),
	})
	if err != nil {
		r.err = err
		return err
	}
	memory := toolcontract.NewMemoryRegistry()
	identities := make([]string, 0)
	if results != nil && len(*results) > 0 {
		for _, value := range (*results)[0].Result {
			encoded, marshalErr := json.Marshal(value.Manifest)
			if marshalErr != nil {
				r.err = fmt.Errorf("encode stored tool contract: %w", marshalErr)
				return r.err
			}
			var manifest toolcontract.Manifest
			if unmarshalErr := json.Unmarshal(encoded, &manifest); unmarshalErr != nil {
				r.err = fmt.Errorf("decode stored tool contract: %w", unmarshalErr)
				return r.err
			}
			manifest.ContentHash = value.ContentHash
			if err := memory.Register(ctx, manifest); err != nil {
				r.err = fmt.Errorf("load stored tool contract: %w", err)
				return r.err
			}
			identities = append(identities, manifest.ID+"\x00"+manifest.ContentHash)
		}
	}
	sort.Strings(identities)
	r.memory = memory
	r.version = "registry_" + hashString(strings.Join(identities, "\x00"))
	return nil
}

func hashString(value string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(value)))
}

var _ memworkflow.StageArtifactRepository = (*StageArtifactRepository)(nil)
var _ memworkflow.SourceLoader = (*WorkflowSourceLoader)(nil)
var _ memworkflow.RegistryProvider = (*TenantRegistryProvider)(nil)
var _ memworkflow.VersionedRegistry = (*tenantToolRegistry)(nil)
