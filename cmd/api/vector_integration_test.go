//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/config"
	"github.com/sauhard74/mem-jev/internal/embedding"
	"github.com/sauhard74/mem-jev/internal/retrieval"
	"github.com/sauhard74/mem-jev/internal/store/storetest"
	storesurreal "github.com/sauhard74/mem-jev/internal/store/surreal"
	"github.com/sauhard74/mem-jev/internal/testinfra"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

func TestHostedRetrievalBuildsProductionVectorChannelAsOptional(t *testing.T) {
	db := testinfra.StartSurreal(t, "surrealdb/surrealdb:v3.2.4")
	if err := storesurreal.NewMigrator(db).Apply(context.Background()); err != nil {
		t.Fatal(err)
	}
	seedVectorProjectionDependencies(t, db)
	projectionRepository := storesurreal.NewProjectionRepository(db)
	projectionValue := storetest.ValidProjection(t, 1, false)
	receipt, err := projectionRepository.Publish(context.Background(), projectionValue)
	if err != nil {
		t.Fatal(err)
	}
	document, err := projectionRepository.RetrievalDocument(context.Background(), "tenant_a", projectionValue.Version.ID, receipt.ProjectionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	var providerCalls atomic.Int64
	provider := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer opaque-provider-token" {
			t.Errorf("provider authorization = %q", request.Header.Get("Authorization"))
		}
		var payload struct {
			Provider      string   `json:"provider"`
			Model         string   `json:"model"`
			ModelRevision string   `json:"model_revision"`
			Inputs        []string `json:"inputs"`
		}
		if decodeErr := json.NewDecoder(request.Body).Decode(&payload); decodeErr != nil {
			t.Errorf("decode provider request: %v", decodeErr)
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		providerCalls.Add(1)
		vectors := make([][]float64, len(payload.Inputs))
		for index := range vectors {
			vectors[index] = []float64{1, 0, 0}
		}
		_ = json.NewEncoder(response).Encode(embedding.ProviderResponse{Provider: payload.Provider, Model: payload.Model, ModelRevision: payload.ModelRevision, Vectors: vectors})
	}))
	defer provider.Close()
	directory := t.TempDir()
	vectorPath := filepath.Join(directory, "vector.json")
	tokenPath := filepath.Join(directory, "provider-token")
	writeTestFile(t, vectorPath, `{
  "schema_version":"retrieval-vector-config.v1",
  "embedding":{"provider":"gateway","model":"embed-v1","model_revision":"2026-09-01","dimension":3,"distance":"cosine_normalized","normalization":"l2","quantization_scale":1000000},
  "tuning":{"degree":32,"build_search":64,"alpha_milli":1200,"overfetch":4,"search_effort":40}
}`)
	writeTestFile(t, tokenPath, "opaque-provider-token")
	configuration := config.RetrievalConfig{
		ChannelTimeout: 150 * time.Millisecond, Retention: time.Hour, MaximumSelections: 5,
		ExactManifestID: "idx_exact.v1", LexicalManifestID: "idx_lexical.v1", FacetManifestID: "idx_facet.v1", GraphManifestID: "idx_graph.v1",
		VectorConfigFile: vectorPath, EmbeddingProviderEndpoint: provider.URL, EmbeddingProviderTokenFile: tokenPath,
	}
	manifest, generation, err := loadVectorRuntimeConfig(vectorPath)
	if err != nil {
		t.Fatal(err)
	}
	tenantGeneration, err := storesurreal.ProvisionVectorGeneration(context.Background(), db, "tenant_a", manifest, generation.Tuning)
	if err != nil {
		t.Fatal(err)
	}
	httpProvider, err := embedding.NewHTTPProvider(provider.URL, "opaque-provider-token", true)
	if err != nil {
		t.Fatal(err)
	}
	embeddingStore, err := storesurreal.NewEmbeddingStore(db)
	if err != nil {
		t.Fatal(err)
	}
	embeddingService, err := embedding.NewService(httpProvider, embeddingStore)
	if err != nil {
		t.Fatal(err)
	}
	documentInput, err := embedding.InputFromDocument(document)
	if err != nil {
		t.Fatal(err)
	}
	documentEmbedding, err := embeddingService.Generate(context.Background(), "tenant_a", manifest, []embedding.Input{documentInput})
	if err != nil {
		t.Fatal(err)
	}
	if err = storesurreal.PutVectorDocument(context.Background(), db, tenantGeneration, manifest, document, receipt.ProjectionEpoch, documentEmbedding[0]); err != nil {
		t.Fatal(err)
	}
	channels, required, err := buildRetrievalChannels(db, configuration, "development")
	if err != nil {
		t.Fatal(err)
	}
	if len(channels) != 5 || channels[4].Name() != retrieval.ChannelVector || !channels[4].Approximate() {
		t.Fatalf("channels = %#v", channels)
	}
	if len(required) != 4 {
		t.Fatalf("required channels = %#v", required)
	}
	for _, channel := range required {
		if channel == retrieval.ChannelVector {
			t.Fatal("vector channel must remain optional")
		}
	}
	query, err := retrieval.BuildQuery("tenant_a", "policy.v1", retrieval.AliasSet{}, retrieval.Input{
		Task: "perform repository verification", Tools: []retrieval.Tool{{Name: "write", ContractVersionID: "tcv_write"}, {Name: "verify", ContractVersionID: "tcv_verify"}},
		Harness: retrieval.Harness{Name: "test-harness", Version: "1"}, RiskClass: retrieval.RiskMedium, LatencyClass: retrieval.LatencyInteractive, MaxCandidates: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := retrieval.ChannelRequest{TenantID: "tenant_a", ProjectionEpoch: receipt.ProjectionEpoch, Query: query, Limit: 10}
	for attempt := 0; attempt < 2; attempt++ {
		hits, searchErr := channels[4].Search(context.Background(), request)
		if searchErr != nil || len(hits) != 1 || hits[0].VersionID != document.ProcedureVersionID || !hits[0].Approximate || hits[0].IndexManifestID != tenantGeneration.IndexManifestID {
			t.Fatalf("attempt=%d hits=%#v error=%v", attempt, hits, searchErr)
		}
	}
	if providerCalls.Load() != 2 {
		t.Fatalf("provider calls=%d, want document plus one cached query", providerCalls.Load())
	}
	if err := os.Remove(tokenPath); err != nil {
		t.Fatal(err)
	}
	if _, _, err := buildRetrievalChannels(db, configuration, "development"); err == nil {
		t.Fatal("missing provider token did not fail startup")
	}
}

func seedVectorProjectionDependencies(t *testing.T, db *surrealdb.DB) {
	t.Helper()
	_, err := surrealdb.Query[any](context.Background(), db, `CREATE ONLY outcome_evidence CONTENT $record`, map[string]any{"record": map[string]any{
		"tenant_id": "tenant_a", "outcome_id": fmt.Sprintf("out_%064x", 1), "trace_id": fmt.Sprintf("tr_%064x", 1),
		"state": "verified_success", "promotion_eligible": true, "policy_version": "policy.v1", "evidence_count": 1,
		"created_at": time.Date(2026, 9, 20, 0, 0, 1, 0, time.UTC), "schema_version": "outcome.v1", "content_hash": fmt.Sprintf("%064x", 201),
	}})
	if err != nil {
		t.Fatal(err)
	}
	for index, contractID := range []string{"tcv_write", "tcv_verify"} {
		_, err = surrealdb.Query[any](context.Background(), db, `CREATE ONLY type::record("tool_contract_version", $id) CONTENT $record`, map[string]any{
			"id": contractID, "record": map[string]any{
				"tenant_id": "tenant_a", "contract_version_id": contractID, "tool_id": "tool_" + contractID,
				"tool_version": "1.0.0", "manifest": map[string]any{}, "created_at": time.Date(2026, 9, 20, 0, 0, index+1, 0, time.UTC),
				"schema_version": "tool-contract.v1", "content_hash": fmt.Sprintf("%064x", index+500),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
