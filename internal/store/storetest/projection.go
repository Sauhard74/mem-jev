package storetest

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/projection"
	"github.com/sauhard74/mem-jev/internal/retrieval"
	"github.com/sauhard74/mem-jev/internal/synthesis"
)

type ProjectionRepository interface {
	projection.Repository
	Counts(context.Context) (projection.Counts, error)
	Canonical(context.Context, domain.TenantID, string) ([]byte, error)
	RetrievalDocument(context.Context, domain.TenantID, string, uint64) (retrieval.Document, error)
}

type ProjectionFactory func(*testing.T) ProjectionRepository

func RunProjectionContract(t *testing.T, factory ProjectionFactory) {
	t.Helper()
	t.Run("publish and duplicate", func(t *testing.T) {
		repository := factory(t)
		value := ValidProjection(t, 1, false)
		first, err := repository.Publish(context.Background(), value)
		if err != nil {
			t.Fatal(err)
		}
		if !first.NewFamily || !first.NewVersion || !first.EvidenceAdded {
			t.Fatalf("receipt = %#v", first)
		}
		second, err := repository.Publish(context.Background(), value)
		if err != nil {
			t.Fatal(err)
		}
		if second.Disposition != projection.DispositionDuplicate {
			t.Fatalf("receipt = %#v", second)
		}
		assertProjectionCounts(t, repository, projection.Counts{Families: 1, Versions: 1, Steps: 2, Edges: 1, Manifests: 1, EvidenceLinks: 1, RetrievalDocuments: 1, ProjectionEpochs: 1})
		document, err := repository.RetrievalDocument(context.Background(), value.TenantID, value.Version.ID, first.ProjectionEpoch)
		if err != nil || document.ProcedureVersionID != value.Version.ID || document.VerifiedSuccessCount != 1 || first.RetrievalDocumentID != document.ID ||
			document.TaskText != "write and verify" || len(document.Tools) != 2 || len(document.Resources) != 1 || len(document.Effects) != 1 {
			t.Fatalf("retrieval document = %#v, receipt = %#v, err = %v", document, first, err)
		}
	})

	t.Run("same graph refreshes evidence", func(t *testing.T) {
		repository := factory(t)
		first := ValidProjection(t, 1, false)
		second := ValidProjection(t, 2, false)
		if first.Version.ID != second.Version.ID || first.Manifest.ID == second.Manifest.ID {
			t.Fatal("invalid fixture")
		}
		if _, err := repository.Publish(context.Background(), first); err != nil {
			t.Fatal(err)
		}
		receipt, err := repository.Publish(context.Background(), second)
		if err != nil {
			t.Fatal(err)
		}
		if receipt.NewVersion || !receipt.EvidenceAdded {
			t.Fatalf("receipt = %#v", receipt)
		}
		assertProjectionCounts(t, repository, projection.Counts{Families: 1, Versions: 1, Steps: 2, Edges: 1, Manifests: 2, EvidenceLinks: 2, RetrievalDocuments: 2, ProjectionEpochs: 2})
		document, err := repository.RetrievalDocument(context.Background(), second.TenantID, second.Version.ID, receipt.ProjectionEpoch)
		if err != nil || document.VerifiedSuccessCount != 2 {
			t.Fatalf("refreshed retrieval document = %#v, err = %v", document, err)
		}
	})

	t.Run("different graph creates challenger", func(t *testing.T) {
		repository := factory(t)
		first := ValidProjection(t, 1, false)
		challenger := ValidProjection(t, 2, true)
		if _, err := repository.Publish(context.Background(), first); err != nil {
			t.Fatal(err)
		}
		receipt, err := repository.Publish(context.Background(), challenger)
		if err != nil {
			t.Fatal(err)
		}
		if receipt.NewFamily || !receipt.NewVersion {
			t.Fatalf("receipt = %#v", receipt)
		}
		assertProjectionCounts(t, repository, projection.Counts{Families: 1, Versions: 2, Steps: 4, Edges: 2, Manifests: 2, EvidenceLinks: 2, RetrievalDocuments: 2, ProjectionEpochs: 2})
	})

	t.Run("abstention stores only manifest", func(t *testing.T) {
		repository := factory(t)
		value := ValidAbstention(t, 1)
		if _, err := repository.Publish(context.Background(), value); err != nil {
			t.Fatal(err)
		}
		assertProjectionCounts(t, repository, projection.Counts{Manifests: 1})
	})

	t.Run("concurrent duplicate converges", func(t *testing.T) {
		repository := factory(t)
		value := ValidProjection(t, 1, false)
		const callers = 12
		start := make(chan struct{})
		errorsSeen := make(chan error, callers)
		var wait sync.WaitGroup
		for range callers {
			wait.Add(1)
			go func() {
				defer wait.Done()
				<-start
				_, err := repository.Publish(context.Background(), value)
				errorsSeen <- err
			}()
		}
		close(start)
		wait.Wait()
		close(errorsSeen)
		for err := range errorsSeen {
			if err != nil {
				t.Errorf("Publish() error = %v", err)
			}
		}
		assertProjectionCounts(t, repository, projection.Counts{Families: 1, Versions: 1, Steps: 2, Edges: 1, Manifests: 1, EvidenceLinks: 1, RetrievalDocuments: 1, ProjectionEpochs: 1})
	})

	t.Run("fresh rebuild is byte identical", func(t *testing.T) {
		value := ValidProjection(t, 1, false)
		first := factory(t)
		if _, err := first.Publish(context.Background(), value); err != nil {
			t.Fatal(err)
		}
		firstBytes, err := first.Canonical(context.Background(), value.TenantID, value.Version.ID)
		if err != nil {
			t.Fatal(err)
		}
		second := factory(t)
		if _, err := second.Publish(context.Background(), value); err != nil {
			t.Fatal(err)
		}
		secondBytes, err := second.Canonical(context.Background(), value.TenantID, value.Version.ID)
		if err != nil {
			t.Fatal(err)
		}
		if string(firstBytes) != string(secondBytes) || string(firstBytes) != string(value.CanonicalProjectionJSON) {
			t.Fatalf("rebuilt canonical projection differs\nfirst: %s\nsecond: %s\nwant: %s", firstBytes, secondBytes, value.CanonicalProjectionJSON)
		}
	})

	t.Run("concurrent corpus changes allocate unique epochs", func(t *testing.T) {
		repository := factory(t)
		first := ValidProjection(t, 1, false)
		if _, err := repository.Publish(context.Background(), first); err != nil {
			t.Fatal(err)
		}
		values := []projection.Projection{ValidProjection(t, 2, false), ValidProjection(t, 2, true)}
		start := make(chan struct{})
		receipts := make(chan projection.PublishReceipt, len(values))
		errorsSeen := make(chan error, len(values))
		for _, value := range values {
			go func() {
				<-start
				receipt, err := repository.Publish(context.Background(), value)
				receipts <- receipt
				errorsSeen <- err
			}()
		}
		close(start)
		epochs := make(map[uint64]struct{}, 2)
		for range values {
			if err := <-errorsSeen; err != nil {
				t.Fatal(err)
			}
			receipt := <-receipts
			epochs[receipt.ProjectionEpoch] = struct{}{}
		}
		if len(epochs) != 2 {
			t.Fatalf("epochs = %v", epochs)
		}
		if _, ok := epochs[2]; !ok {
			t.Fatalf("epoch 2 missing: %v", epochs)
		}
		if _, ok := epochs[3]; !ok {
			t.Fatalf("epoch 3 missing: %v", epochs)
		}
		assertProjectionCounts(t, repository, projection.Counts{Families: 1, Versions: 2, Steps: 4, Edges: 2, Manifests: 3, EvidenceLinks: 3, RetrievalDocuments: 3, ProjectionEpochs: 3})
	})

	t.Run("negative paths are idempotent and version scoped", func(t *testing.T) {
		repository := factory(t)
		first := withNegativePath(t, ValidProjection(t, 1, false))
		second := withNegativePath(t, ValidProjection(t, 2, false))
		challenger := withNegativePath(t, ValidProjection(t, 2, true))
		if _, err := repository.Publish(context.Background(), first); err != nil {
			t.Fatal(err)
		}
		if _, err := repository.Publish(context.Background(), second); err != nil {
			t.Fatal(err)
		}
		if _, err := repository.Publish(context.Background(), challenger); err != nil {
			t.Fatal(err)
		}
		assertProjectionCounts(t, repository, projection.Counts{Families: 1, Versions: 2, Steps: 4, Edges: 2, NegativePaths: 2, Manifests: 3, EvidenceLinks: 3, RetrievalDocuments: 3, ProjectionEpochs: 3})
	})
}

func withNegativePath(t *testing.T, value projection.Projection) projection.Projection {
	t.Helper()
	path := domain.NegativePath{
		FailurePredicateID: "predicate.failed", EventIDs: []domain.EventID{"failed-event"},
		Scope: domain.CompatibilityScope{EnvironmentHash: "environment", ToolHash: "tool", ResourceHash: "resource"},
	}
	_, hash, err := canonical.MarshalAndHash(path)
	if err != nil {
		t.Fatal(err)
	}
	path.ID = "neg_" + hash
	value.NegativePaths = []domain.NegativePath{path}
	return value
}

func ValidAbstention(t *testing.T, evidenceNumber int) projection.Projection {
	t.Helper()
	result := synthesis.Result{
		SchemaVersion: "synthesis.v1", Status: synthesis.StatusAbstained, AbstentionCode: "opaque_tool",
		TraceID: domain.TraceID(fmt.Sprintf("tr_%064x", evidenceNumber)), CausalGraphHash: strings.Repeat("7", 64),
		Hash:     fmt.Sprintf("%064x", evidenceNumber+100),
		Versions: synthesis.Versions{Sanitizer: "sanitizer.v1", Registry: "registry.v1", Policy: "policy.v1", GraphBuilder: "graph.v1", Synthesizer: "synth.v1"},
	}
	value, err := projection.Build(projection.BuildRequest{
		TenantID: "tenant_a", OutcomeID: domain.OutcomeID(fmt.Sprintf("out_%064x", evidenceNumber)),
		IntentHash: strings.Repeat("a", 64), EffectSignatureHash: strings.Repeat("b", 64), EnvironmentScopeHash: strings.Repeat("c", 64),
		ArchiveHash: strings.Repeat("d", 64), CreatedAt: time.Date(2026, 9, 20, 0, 0, evidenceNumber, 0, time.UTC), Synthesis: result,
	})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func ValidProjection(t *testing.T, evidenceNumber int, challenger bool) projection.Projection {
	return ValidProjectionForTenant(t, "tenant_a", evidenceNumber, challenger)
}

func ValidProjectionForTenant(t *testing.T, tenantID domain.TenantID, evidenceNumber int, challenger bool) projection.Projection {
	t.Helper()
	toolContract := "tcv_verify"
	if challenger {
		toolContract = "tcv_verify_v2"
	}
	result := synthesis.Result{
		SchemaVersion: "synthesis.v1", Status: synthesis.StatusSynthesized,
		TraceID: domain.TraceID(fmt.Sprintf("tr_%064x", evidenceNumber)), CausalGraphHash: strings.Repeat("7", 64),
		GoalPredicates: []string{"goal"}, ObservedEndToEnd: true, Hash: fmt.Sprintf("%064x", evidenceNumber+100),
		Versions: synthesis.Versions{Sanitizer: "sanitizer.v1", Registry: "registry.v1", Policy: "policy.v1", GraphBuilder: "graph.v1", Synthesizer: "synth.v1"},
		Steps: []domain.ProcedureStep{
			{EventID: domain.EventID(fmt.Sprintf("event-%d-a", evidenceNumber)), Ordinal: 0, ToolName: "write", ToolVersion: "1.0.0", ToolContractVersionID: "tcv_write"},
			{EventID: domain.EventID(fmt.Sprintf("event-%d-b", evidenceNumber)), Ordinal: 1, ToolName: "verify", ToolVersion: "1.0.0", ToolContractVersionID: toolContract},
		},
	}
	result.Edges = []synthesis.Edge{{From: result.Steps[0].EventID, To: result.Steps[1].EventID, Type: synthesis.EdgeResourceFlow, ResourceName: "workspace", ResourceType: "repository", ResourceNamespace: "repo"}}
	_, intentHash, err := canonical.MarshalAndHash(struct {
		Task           string `json:"task"`
		Harness        string `json:"harness"`
		HarnessVersion string `json:"harness_version,omitempty"`
	}{"write and verify", "test-harness", "1"})
	if err != nil {
		t.Fatal(err)
	}
	value, err := projection.Build(projection.BuildRequest{
		TenantID: tenantID, OutcomeID: domain.OutcomeID(fmt.Sprintf("out_%064x", evidenceNumber)),
		IntentHash: intentHash, EffectSignatureHash: strings.Repeat("b", 64), EnvironmentScopeHash: strings.Repeat("c", 64),
		ArchiveHash: strings.Repeat("d", 64), CanonicalEventStart: 0, CanonicalEventEnd: 1,
		CreatedAt: time.Date(2026, 9, 20, 0, 0, evidenceNumber, 0, time.UTC), Synthesis: result,
		Serving: projection.ServingMetadata{
			TaskText: "write and verify", Harness: retrieval.Harness{Name: "test-harness", Version: "1"},
			Resources: []retrieval.ResourceRequirement{{Type: "repository", Namespace: "repo", IdentityHash: strings.Repeat("9", 64), SchemaVersion: "v1"}},
			Effects:   []string{"filesystem.write"}, RiskClass: "medium", VerificationStrength: 5,
			LearnedWithRecallConsent: true, ResidencyRegion: "local",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func assertProjectionCounts(t *testing.T, repository ProjectionRepository, want projection.Counts) {
	t.Helper()
	got, err := repository.Counts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("counts = %#v, want %#v", got, want)
	}
}
