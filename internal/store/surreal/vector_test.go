//go:build integration

package surreal

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/sauhard74/mem-jev/internal/embedding"
	"github.com/sauhard74/mem-jev/internal/retrieval"
	"github.com/sauhard74/mem-jev/internal/store/storetest"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

func TestSurrealVectorGenerationAndExactRerank(t *testing.T) {
	db := projectionDatabase(t)
	seedProjectionOutcomes(t, db)
	projectionRepository := NewProjectionRepository(db)
	first := storetest.ValidProjection(t, 1, false)
	firstReceipt, err := projectionRepository.Publish(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	second := storetest.ValidProjection(t, 2, true)
	secondReceipt, err := projectionRepository.Publish(context.Background(), second)
	if err != nil {
		t.Fatal(err)
	}

	manifest := vectorManifest(t, 3)
	generation, err := ProvisionVectorGeneration(context.Background(), db, "tenant_a", manifest, VectorTuning{Degree: 32, BuildSearch: 64, AlphaMilli: 1200, Overfetch: 4, SearchEffort: 40})
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := ProvisionVectorGeneration(context.Background(), db, "tenant_a", manifest, generation.Tuning)
	if err != nil || duplicate.IndexManifestID != generation.IndexManifestID {
		t.Fatalf("duplicate generation = %#v, %v", duplicate, err)
	}

	provider := &vectorProvider{manifest: manifest, vector: []float64{1, 0, 0}}
	embeddingStore, err := NewEmbeddingStore(db)
	if err != nil {
		t.Fatal(err)
	}
	service, err := embedding.NewService(provider, embeddingStore)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		valueID string
		epoch   uint64
	}{
		{first.Version.ID, firstReceipt.ProjectionEpoch},
		{second.Version.ID, secondReceipt.ProjectionEpoch},
	} {
		document, readErr := projectionRepository.RetrievalDocument(context.Background(), "tenant_a", item.valueID, item.epoch)
		if readErr != nil {
			t.Fatal(readErr)
		}
		input, inputErr := embedding.InputFromDocument(document)
		if inputErr != nil {
			t.Fatal(inputErr)
		}
		values, generateErr := service.Generate(context.Background(), "tenant_a", manifest, []embedding.Input{input})
		if generateErr != nil {
			t.Fatal(generateErr)
		}
		if putErr := PutVectorDocument(context.Background(), db, generation, manifest, document, item.epoch, values[0]); putErr != nil {
			t.Fatal(putErr)
		}
	}

	query, err := retrieval.BuildQuery("tenant_a", "policy.v1", retrieval.AliasSet{}, retrieval.Input{
		Task: "write and verify", Tools: []retrieval.Tool{{Name: "write", ContractVersionID: "tcv_write"}, {Name: "verify", ContractVersionID: "tcv_verify"}},
		Resources: []retrieval.Resource{{Type: "repository", Namespace: "repo", Identity: "workspace"}},
		Harness:   retrieval.Harness{Name: "test-harness", Version: "1"}, RiskClass: retrieval.RiskMedium,
		LatencyClass: retrieval.LatencyInteractive, MaxCandidates: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	channel, err := NewVectorChannel(db, manifest, generation, service)
	if err != nil {
		t.Fatal(err)
	}
	hits, err := channel.Search(context.Background(), retrieval.ChannelRequest{TenantID: "tenant_a", ProjectionEpoch: secondReceipt.ProjectionEpoch, Query: query, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 2 || hits[0].VersionID > hits[1].VersionID || !hits[0].Approximate || hits[0].IndexManifestID != generation.IndexManifestID || hits[0].RawScoreQuantized != 1_000_000_000_000 {
		t.Fatalf("unexpected exact rerank: %#v", hits)
	}
	if provider.calls != 2 {
		t.Fatalf("content-addressed query embedding should be reused, calls=%d", provider.calls)
	}
	otherTenantQuery, err := retrieval.BuildQuery("tenant_b", "policy.v1", retrieval.AliasSet{}, retrieval.Input{
		Task: "write and verify", Tools: []retrieval.Tool{{Name: "write", ContractVersionID: "tcv_write"}, {Name: "verify", ContractVersionID: "tcv_verify"}},
		Resources: []retrieval.Resource{{Type: "repository", Namespace: "repo", Identity: "workspace"}},
		Harness:   retrieval.Harness{Name: "test-harness", Version: "1"}, RiskClass: retrieval.RiskMedium,
		LatencyClass: retrieval.LatencyInteractive, MaxCandidates: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	otherTenantHits, err := channel.Search(context.Background(), retrieval.ChannelRequest{TenantID: "tenant_b", ProjectionEpoch: secondReceipt.ProjectionEpoch, Query: otherTenantQuery, Limit: 10})
	if err != nil || len(otherTenantHits) != 0 {
		t.Fatalf("shared channel leaked tenant_a rows: hits=%#v error=%v", otherTenantHits, err)
	}
	_, err = surrealdb.Query[any](context.Background(), db, `CREATE ONLY outcome_evidence CONTENT $record`, map[string]any{"record": map[string]any{
		"tenant_id": "tenant_a", "outcome_id": fmt.Sprintf("out_%064x", 3), "trace_id": fmt.Sprintf("tr_%064x", 3),
		"state": "verified_success", "promotion_eligible": true, "policy_version": "policy.v1", "evidence_count": 1,
		"created_at": first.CreatedAt.Add(2), "schema_version": "outcome.v1", "content_hash": fmt.Sprintf("%064x", 203),
	}})
	if err != nil {
		t.Fatal(err)
	}
	refresh := storetest.ValidProjection(t, 3, false)
	refreshReceipt, err := projectionRepository.Publish(context.Background(), refresh)
	if err != nil {
		t.Fatal(err)
	}
	staleFiltered, err := channel.Search(context.Background(), retrieval.ChannelRequest{TenantID: "tenant_a", ProjectionEpoch: refreshReceipt.ProjectionEpoch, Query: query, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(staleFiltered) != 1 || staleFiltered[0].VersionID != second.Version.ID {
		t.Fatalf("stale embedding was served: %#v", staleFiltered)
	}

	statement := vectorCandidateStatement(generation, 8)
	plan, err := surrealdb.Query[any](context.Background(), db, statement+" EXPLAIN FULL", map[string]any{
		"tenant_id": "tenant_a", "epoch": secondReceipt.ProjectionEpoch, "vector": []float64{1_000_000, 0, 0},
		"environment_hash": query.EnvironmentHash, "harness": query.Harness.Name, "contracts": []string{"tcv_write", "tcv_verify"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if rendered := strings.ToLower(fmt.Sprintf("%#v", plan)); !strings.Contains(rendered, strings.ToLower(generation.IndexName)) || !strings.Contains(rendered, "knnscan") {
		t.Fatalf("vector plan does not use %s: %s", generation.IndexName, rendered)
	}
}

func TestSurrealVectorRejectsManifestAndEmbeddingMismatch(t *testing.T) {
	db := projectionDatabase(t)
	manifest := vectorManifest(t, 3)
	generation, err := ProvisionVectorGeneration(context.Background(), db, "tenant_a", manifest, VectorTuning{Degree: 32, BuildSearch: 64, AlphaMilli: 1200, Overfetch: 4, SearchEffort: 40})
	if err != nil {
		t.Fatal(err)
	}
	other := vectorManifest(t, 4)
	if _, err := ProvisionVectorGeneration(context.Background(), db, "tenant_a", other, generation.Tuning); err != nil {
		t.Fatal(err)
	}
	if _, err := NewVectorChannel(db, other, generation, nil); err == nil {
		t.Fatal("expected generation/manifest mismatch")
	}
}

func vectorManifest(t *testing.T, dimension uint32) embedding.Manifest {
	t.Helper()
	manifest, err := embedding.NewManifest(embedding.ManifestSpec{Provider: "test-provider", Model: "test-model-v1", ModelRevision: "revision-1", Dimension: dimension, Distance: embedding.DistanceCosineNormalized, Normalization: embedding.NormalizationL2, QuantizationScale: 1_000_000})
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}

type vectorProvider struct {
	manifest embedding.Manifest
	vector   []float64
	calls    int
}

func (p *vectorProvider) Embed(_ context.Context, request embedding.ProviderRequest) (embedding.ProviderResponse, error) {
	p.calls++
	vectors := make([][]float64, len(request.Inputs))
	for index := range vectors {
		vectors[index] = append([]float64(nil), p.vector...)
	}
	return embedding.ProviderResponse{Provider: p.manifest.Provider, Model: p.manifest.Model, ModelRevision: p.manifest.ModelRevision, Vectors: vectors}, nil
}
