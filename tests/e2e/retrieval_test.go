//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/gen/memjev/v1/memjevv1connect"
	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/eligibility"
	"github.com/sauhard74/mem-jev/internal/ranking"
	"github.com/sauhard74/mem-jev/internal/retrieval"
	storesurreal "github.com/sauhard74/mem-jev/internal/store/surreal"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"google.golang.org/protobuf/proto"
)

func TestDeterministicHostedRetrieval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	db := openSurreal(t, ctx)
	defer func() { _ = db.Close(context.Background()) }()
	if err := storesurreal.NewMigrator(db).Apply(ctx); err != nil {
		t.Fatalf("first migration replay: %v", err)
	}
	if err := storesurreal.NewMigrator(db).Apply(ctx); err != nil {
		t.Fatalf("second migration replay: %v", err)
	}
	seedRetrievalServingState(t, ctx, db)

	client := memjevv1connect.NewRetrievalServiceClient(httpClient(), requiredEnv(t, "MEMJEV_E2E_API_URL"))
	first := callRetrieve(t, ctx, client, "retrieval-e2e-key-0001", "write output", nil)
	seedTieRetrievalDocument(t, ctx, db)
	second := callRetrieve(t, ctx, client, "retrieval-e2e-key-0001", "write output", nil)
	if first.Msg.GetRetrievalRunId() == "" || first.Msg.GetRetrievalRunId() != second.Msg.GetRetrievalRunId() || first.Msg.GetProvenance().GetQueryHash() != second.Msg.GetProvenance().GetQueryHash() {
		t.Fatalf("non-deterministic replay: first=%#v second=%#v", first.Msg, second.Msg)
	}
	firstWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(first.Msg)
	if err != nil {
		t.Fatal(err)
	}
	secondWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(second.Msg)
	if err != nil || !bytes.Equal(firstWire, secondWire) {
		t.Fatalf("replay response changed: equal=%v error=%v", bytes.Equal(firstWire, secondWire), err)
	}
	if first.Msg.GetDisposition() != memjevv1.RetrievalDisposition_RETRIEVAL_DISPOSITION_SELECTED || len(first.Msg.GetCandidates()) != 1 || first.Msg.GetCandidates()[0].GetProcedureVersionId() != "pv_retrieval_e2e" || first.Msg.GetCandidates()[0].GetAdvisoryOnly() {
		t.Fatalf("selected response = %#v", first.Msg)
	}
	if first.Msg.GetProvenance().GetProjectionEpoch() == 0 || len(first.Msg.GetProvenance().GetIndexManifestIds()) != 4 || first.Msg.GetProvenance().GetApproximateCandidates() {
		t.Fatalf("provenance = %#v", first.Msg.GetProvenance())
	}
	tied := callRetrieve(t, ctx, client, "retrieval-e2e-key-tie", "write output", nil)
	if tied.Msg.GetProvenance().GetProjectionEpoch() <= first.Msg.GetProvenance().GetProjectionEpoch() || len(tied.Msg.GetCandidates()) != 2 || tied.Msg.GetCandidates()[0].GetProcedureVersionId() != "pv_retrieval_e2e" || tied.Msg.GetCandidates()[1].GetProcedureVersionId() != "pv_retrieval_e2e_z" {
		t.Fatalf("stable tied ranking = %#v", tied.Msg)
	}

	explainRequest := connect.NewRequest(&memjevv1.ExplainRetrievalRequest{RetrievalRunId: first.Msg.GetRetrievalRunId()})
	explainRequest.Header().Set("Authorization", "Bearer "+requiredEnv(t, "MEMJEV_E2E_TOKEN"))
	explanation, err := client.ExplainRetrieval(ctx, explainRequest)
	if err != nil {
		t.Fatalf("explanation error = %v", err)
	}
	eligible := false
	for _, candidate := range explanation.Msg.GetCandidates() {
		eligible = eligible || candidate.GetProcedureVersionId() == "pv_retrieval_e2e" && candidate.GetEligible()
	}
	if explanation.Msg.GetProvenance().GetQueryHash() != first.Msg.GetProvenance().GetQueryHash() || len(explanation.Msg.GetCandidates()) < 1 || !eligible {
		t.Fatalf("explanation message=%s err=%v", explanation.Msg.String(), err)
	}

	foreign := connect.NewRequest(&memjevv1.ExplainRetrievalRequest{RetrievalRunId: first.Msg.GetRetrievalRunId()})
	foreign.Header().Set("Authorization", "Bearer "+requiredEnv(t, "MEMJEV_E2E_OTHER_TOKEN"))
	if _, err = client.ExplainRetrieval(ctx, foreign); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("cross-tenant explanation error = %v", err)
	}

	abstained := callRetrieve(t, ctx, client, "retrieval-e2e-key-0002", "write output", []string{"filesystem.write"})
	if abstained.Msg.GetDisposition() != memjevv1.RetrievalDisposition_RETRIEVAL_DISPOSITION_ABSTAINED || abstained.Msg.GetAbstentionCode() != "no_eligible_candidates" || len(abstained.Msg.GetCandidates()) != 0 {
		t.Fatalf("gate abstention = %#v", abstained.Msg)
	}

	changed := retrievalRequest("different task", nil)
	changed.Header().Set("Authorization", "Bearer "+requiredEnv(t, "MEMJEV_E2E_TOKEN"))
	changed.Header().Set("Idempotency-Key", "retrieval-e2e-key-0001")
	if _, err = client.Retrieve(ctx, changed); connect.CodeOf(err) != connect.CodeAlreadyExists {
		t.Fatalf("idempotency conflict error = %v", err)
	}
	regionChanged := retrievalRequest("write output", nil)
	regionChanged.Header().Set("Authorization", "Bearer "+requiredEnv(t, "MEMJEV_E2E_OTHER_REGION_TOKEN"))
	regionChanged.Header().Set("Idempotency-Key", "retrieval-e2e-key-0001")
	if _, err = client.Retrieve(ctx, regionChanged); connect.CodeOf(err) != connect.CodeAlreadyExists {
		t.Fatalf("residency-context conflict error = %v", err)
	}

	concurrentResponses := make(chan *memjevv1.RetrieveResponse, 16)
	concurrentErrors := make(chan error, 16)
	retrievalToken := requiredEnv(t, "MEMJEV_E2E_TOKEN")
	var workers sync.WaitGroup
	workers.Add(16)
	for range 16 {
		go func() {
			defer workers.Done()
			request := retrievalRequest("write output", nil)
			request.Header().Set("Authorization", "Bearer "+retrievalToken)
			request.Header().Set("Idempotency-Key", "retrieval-e2e-key-concurrent")
			response, callErr := client.Retrieve(ctx, request)
			if callErr != nil {
				concurrentErrors <- callErr
				return
			}
			concurrentResponses <- response.Msg
		}()
	}
	workers.Wait()
	close(concurrentResponses)
	close(concurrentErrors)
	for callErr := range concurrentErrors {
		t.Errorf("concurrent replay error = %v", callErr)
	}
	var concurrentWire []byte
	var concurrentRunID string
	for response := range concurrentResponses {
		wire, marshalErr := proto.MarshalOptions{Deterministic: true}.Marshal(response)
		if marshalErr != nil || response.GetRetrievalRunId() == "" {
			t.Errorf("invalid concurrent response run=%q marshal_error=%v", response.GetRetrievalRunId(), marshalErr)
			continue
		}
		if concurrentWire == nil {
			concurrentWire, concurrentRunID = wire, response.GetRetrievalRunId()
			continue
		}
		if response.GetRetrievalRunId() != concurrentRunID || !bytes.Equal(wire, concurrentWire) {
			t.Errorf("concurrent responses diverged: run=%q want=%q bytes_equal=%v", response.GetRetrievalRunId(), concurrentRunID, bytes.Equal(wire, concurrentWire))
		}
	}

	rows, err := surrealdb.Query[[]map[string]any](ctx, db, `SELECT retrieval_run_id, canonical_query_envelope FROM retrieval_run WHERE tenant_id = "tenant_e2e"`, nil)
	if err != nil || rows == nil || len(*rows) == 0 || len((*rows)[0].Result) != 4 {
		t.Fatalf("retrieval run rows=%#v err=%v", rows, err)
	}
	for _, row := range (*rows)[0].Result {
		envelope := row["canonical_query_envelope"]
		if !strings.HasPrefix(envelope.(string), "enc.v1.") || strings.Contains(envelope.(string), "write output") {
			t.Fatalf("unsafe query envelope = %#v", envelope)
		}
	}
}

func seedTieRetrievalDocument(t *testing.T, ctx context.Context, db *surrealdb.DB) {
	t.Helper()
	now := time.Now().UTC().Add(-30 * time.Second)
	intentHash, err := retrieval.CanonicalIntentHash("write output", retrieval.Harness{Name: "e2e", Version: "1"})
	if err != nil {
		t.Fatal(err)
	}
	_, environmentHash, err := canonical.MarshalAndHash([]retrieval.Fact{})
	if err != nil {
		t.Fatal(err)
	}
	document, err := retrieval.BuildDocument(retrieval.DocumentInput{
		TenantID: "tenant_e2e", ProcedureVersionID: "pv_retrieval_e2e_z", ProcedureID: "proc_retrieval_e2e_z", TaskText: "write output",
		IntentHash: intentHash, EffectSignatureHash: strings.Repeat("e", 64), Tools: []retrieval.ToolRequirement{{Name: "writer", ContractVersionID: "tcv_retrieval_e2e"}},
		OrderedStepContractIDs: []string{"tcv_retrieval_e2e"}, Effects: []string{"filesystem.write"}, EnvironmentScopeHash: environmentHash,
		Harness: retrieval.Harness{Name: "e2e", Version: "1"}, Lifecycle: "active", ObservedEndToEnd: true, VerificationStrength: 5,
		VerifiedSuccessCount: 3, ValidatedAt: now, ValidationPolicyVersion: "evidence.v1", LearnedWithRecallConsent: true, ResidencyRegion: "local", RiskClass: "low",
		Interface: e2eProcedureInterface(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	seedRetrievalDocument(t, ctx, db, document, now)
}

func callRetrieve(t *testing.T, ctx context.Context, client memjevv1connect.RetrievalServiceClient, key, task string, forbidden []string) *connect.Response[memjevv1.RetrieveResponse] {
	t.Helper()
	request := retrievalRequest(task, forbidden)
	request.Header().Set("Authorization", "Bearer "+requiredEnv(t, "MEMJEV_E2E_TOKEN"))
	request.Header().Set("Idempotency-Key", key)
	response, err := client.Retrieve(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func retrievalRequest(task string, forbidden []string) *connect.Request[memjevv1.RetrieveRequest] {
	return connect.NewRequest(&memjevv1.RetrieveRequest{
		Task: task, Tools: []*memjevv1.AvailableTool{{Name: "writer", ContractVersionId: "tcv_retrieval_e2e"}},
		Harness: &memjevv1.HarnessIdentity{Name: "e2e", Version: "1"}, ForbiddenEffects: forbidden,
		RiskClass: memjevv1.RiskClass_RISK_CLASS_LOW, LatencyClass: memjevv1.LatencyClass_LATENCY_CLASS_INTERACTIVE, MaxCandidates: 10,
	})
}

func seedRetrievalServingState(t *testing.T, ctx context.Context, db *surrealdb.DB) {
	t.Helper()
	seedTenantRetrievalServingState(t, ctx, db, "tenant_e2e", "pv_retrieval_e2e", "proc_retrieval_e2e")
	seedTenantRetrievalServingState(t, ctx, db, "tenant_other", "pv_retrieval_other", "proc_retrieval_other")
}

func seedTenantRetrievalServingState(t *testing.T, ctx context.Context, db *surrealdb.DB, tenantID domain.TenantID, versionID, procedureID string) {
	t.Helper()
	now := time.Now().UTC().Add(-time.Minute)
	intentHash, err := retrieval.CanonicalIntentHash("write output", retrieval.Harness{Name: "e2e", Version: "1"})
	if err != nil {
		t.Fatal(err)
	}
	_, environmentHash, err := canonical.MarshalAndHash([]retrieval.Fact{})
	if err != nil {
		t.Fatal(err)
	}
	document, err := retrieval.BuildDocument(retrieval.DocumentInput{
		TenantID: tenantID, ProcedureVersionID: versionID, ProcedureID: procedureID, TaskText: "write output",
		IntentHash: intentHash, EffectSignatureHash: strings.Repeat("e", 64), Tools: []retrieval.ToolRequirement{{Name: "writer", ContractVersionID: "tcv_retrieval_e2e"}},
		OrderedStepContractIDs: []string{"tcv_retrieval_e2e"}, Effects: []string{"filesystem.write"}, EnvironmentScopeHash: environmentHash,
		Harness: retrieval.Harness{Name: "e2e", Version: "1"}, Lifecycle: "active", ObservedEndToEnd: true, VerificationStrength: 5,
		VerifiedSuccessCount: 3, ValidatedAt: now, ValidationPolicyVersion: "evidence.v1", LearnedWithRecallConsent: true, ResidencyRegion: "local", RiskClass: "low",
		Interface: e2eProcedureInterface(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	seedRetrievalDocument(t, ctx, db, document, now)
	policyManifest, err := eligibility.NewPolicy(eligibility.PolicySpec{Version: "retrieval-e2e.v1", AllowedLifecycle: []eligibility.Lifecycle{eligibility.LifecycleActive}, MaximumRisk: eligibility.RiskLow, MaximumValidationAgeSeconds: 3600, CompatibleValidationPolicies: []string{"evidence.v1"}, AllowedResidencyRegions: []string{"local"}})
	if err != nil {
		t.Fatal(err)
	}
	rankerManifest, err := ranking.NewManifest(ranking.ManifestSpec{Version: "retrieval-e2e.v1", RRFK: 60, RRFCoefficient: 1, MaxCandidates: 100, Channels: []ranking.ChannelWeight{{Channel: "exact", WeightMicros: 1_000_000}, {Channel: "lexical", WeightMicros: 700_000}, {Channel: "facet", WeightMicros: 500_000}, {Channel: "graph", WeightMicros: 300_000}}, Features: []ranking.FeatureSpec{{Name: "verification_strength", Minimum: 0, Maximum: 10, Missing: 0, Coefficient: 1000}}})
	if err != nil {
		t.Fatal(err)
	}
	seedRetrievalManifests(t, ctx, db, tenantID, policyManifest, rankerManifest, now)
	config, err := retrieval.BuildServingConfig(tenantID, policyManifest.ID, rankerManifest.ID, []retrieval.SnapshotIndex{{Channel: retrieval.ChannelExact, ManifestID: "idx_exact.v1"}, {Channel: retrieval.ChannelLexical, ManifestID: "idx_lexical.v1"}, {Channel: retrieval.ChannelFacet, ManifestID: "idx_facet.v1"}, {Channel: retrieval.ChannelGraph, ManifestID: "idx_graph.v1"}})
	if err != nil {
		t.Fatal(err)
	}
	if err = storesurreal.NewRetrievalRunRepository(db).ActivateServingConfig(ctx, config); err != nil {
		t.Fatal(err)
	}
}

func e2eProcedureInterface(t *testing.T) *domain.ProcedureInterface {
	t.Helper()
	value, err := retrieval.NewProcedureInterface(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return &value
}

func seedRetrievalDocument(t *testing.T, ctx context.Context, db *surrealdb.DB, document retrieval.Document, now time.Time) {
	t.Helper()
	tenantID := string(document.TenantID)
	row, err := surrealdb.Query[[]struct {
		Epoch uint64 `json:"current_epoch"`
	}](ctx, db, `SELECT current_epoch FROM projection_epoch_head WHERE tenant_id = $tenant_id LIMIT 1`, map[string]any{"tenant_id": tenantID})
	if err != nil {
		t.Fatal(err)
	}
	epoch := uint64(1)
	if row != nil && len(*row) > 0 && len((*row)[0].Result) > 0 {
		epoch = (*row)[0].Result[0].Epoch + 1
	}
	toolNames, toolIDs := []string{}, []string{}
	for _, tool := range document.Tools {
		toolNames, toolIDs = append(toolNames, tool.Name), append(toolIDs, tool.ContractVersionID)
	}
	environmentJSON, _, _ := canonical.MarshalAndHash(document.Environment)
	record := map[string]any{"tenant_id": tenantID, "retrieval_document_id": document.ID, "procedure_version_id": document.ProcedureVersionID, "procedure_id": document.ProcedureID, "projection_epoch": epoch, "task_text": document.TaskText, "intent_hash": document.IntentHash, "effect_signature_hash": document.EffectSignatureHash, "tool_names": toolNames, "tool_contract_version_ids": toolIDs, "resource_types": []string{}, "resource_namespaces": []string{}, "resource_identity_hashes": []string{}, "resource_schema_versions": []string{}, "effects": document.Effects, "environment_scope_hash": document.EnvironmentScopeHash, "harness_name": document.Harness.Name, "harness_version": document.Harness.Version, "environment_facts": string(environmentJSON), "prefix_hashes": document.PrefixHashes, "lifecycle_state": document.Lifecycle, "observed_end_to_end": document.ObservedEndToEnd, "verification_strength": document.VerificationStrength, "verified_success_count": document.VerifiedSuccessCount, "unsafe_outcome_count": document.UnsafeOutcomeCount, "validated_at": now, "validation_policy_version": document.ValidationPolicyVersion, "learned_with_recall_consent": true, "residency_region": document.ResidencyRegion, "risk_class": document.RiskClass, "canonical_document": string(document.CanonicalJSON), "created_at": now, "schema_version": document.SchemaVersion, "content_hash": document.ContentHash}
	if _, err = surrealdb.Query[any](ctx, db, `CREATE ONLY type::record("retrieval_document", $id) CONTENT $record`, map[string]any{"id": document.ID, "record": record}); err != nil {
		t.Fatal(err)
	}
	setHash := strings.Repeat("a", 63) + "b"
	if _, err = surrealdb.Query[any](ctx, db, `CREATE ONLY projection_epoch CONTENT {tenant_id: $tenant_id, epoch: $epoch, document_set_hash: $hash, created_at: $at, schema_version: "projection-epoch.v1", content_hash: $hash}`, map[string]any{"tenant_id": tenantID, "epoch": epoch, "hash": setHash, "at": now}); err != nil {
		t.Fatal(err)
	}
	if _, err = surrealdb.Query[any](ctx, db, `UPSERT projection_epoch_head SET tenant_id = $tenant_id, current_epoch = $epoch, document_set_hash = $hash, updated_at = $at, schema_version = "projection-epoch-head.v1", content_hash = $hash WHERE tenant_id = $tenant_id`, map[string]any{"tenant_id": tenantID, "epoch": epoch, "hash": setHash, "at": now}); err != nil {
		t.Fatal(err)
	}
}

func seedRetrievalManifests(t *testing.T, ctx context.Context, db *surrealdb.DB, tenantID domain.TenantID, policyManifest eligibility.Policy, rankerManifest ranking.Manifest, now time.Time) {
	t.Helper()
	records := []struct {
		statement string
		record    map[string]any
	}{
		{`CREATE ONLY eligibility_policy_manifest CONTENT $record`, map[string]any{"tenant_id": string(tenantID), "policy_manifest_id": policyManifest.ID, "version": policyManifest.Version, "manifest": string(policyManifest.CanonicalJSON), "created_at": now, "schema_version": "eligibility-policy.v1", "content_hash": strings.TrimPrefix(policyManifest.ID, "egp_")}},
		{`CREATE ONLY ranker_manifest CONTENT $record`, map[string]any{"tenant_id": string(tenantID), "ranker_manifest_id": rankerManifest.ID, "version": rankerManifest.Version, "manifest": string(rankerManifest.CanonicalJSON), "created_at": now, "schema_version": "ranker.v1", "content_hash": strings.TrimPrefix(rankerManifest.ID, "rnk_")}},
	}
	for _, channel := range []string{"exact", "lexical", "facet", "graph"} {
		records = append(records, struct {
			statement string
			record    map[string]any
		}{`CREATE ONLY retrieval_index_manifest CONTENT $record`, map[string]any{"tenant_id": string(tenantID), "index_manifest_id": "idx_" + channel + ".v1", "channel": channel, "generation": 1, "index_name": "retrieval_document_" + channel, "approximate": false, "configuration": map[string]any{}, "created_at": now, "schema_version": "retrieval-index.v1", "content_hash": strings.Repeat(string(channel[0]), 64)}})
	}
	for _, item := range records {
		if _, err := surrealdb.Query[any](ctx, db, item.statement, map[string]any{"record": item.record}); err != nil {
			t.Fatal(err)
		}
	}
	encoded, _ := json.Marshal(records)
	if len(encoded) == 0 {
		t.Fatal("manifest seed unexpectedly empty")
	}
}
