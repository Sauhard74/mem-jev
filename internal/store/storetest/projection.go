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
	"github.com/sauhard74/mem-jev/internal/synthesis"
)

type ProjectionRepository interface {
	projection.Repository
	Counts(context.Context) (projection.Counts, error)
	Canonical(context.Context, domain.TenantID, string) ([]byte, error)
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
		assertProjectionCounts(t, repository, projection.Counts{Families: 1, Versions: 1, Steps: 2, Edges: 1, Manifests: 1, EvidenceLinks: 1})
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
		assertProjectionCounts(t, repository, projection.Counts{Families: 1, Versions: 1, Steps: 2, Edges: 1, Manifests: 2, EvidenceLinks: 2})
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
		assertProjectionCounts(t, repository, projection.Counts{Families: 1, Versions: 2, Steps: 4, Edges: 2, Manifests: 2, EvidenceLinks: 2})
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
		assertProjectionCounts(t, repository, projection.Counts{Families: 1, Versions: 1, Steps: 2, Edges: 1, Manifests: 1, EvidenceLinks: 1})
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
		assertProjectionCounts(t, repository, projection.Counts{Families: 1, Versions: 2, Steps: 4, Edges: 2, NegativePaths: 2, Manifests: 3, EvidenceLinks: 3})
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
	value, err := projection.Build(projection.BuildRequest{
		TenantID: tenantID, OutcomeID: domain.OutcomeID(fmt.Sprintf("out_%064x", evidenceNumber)),
		IntentHash: strings.Repeat("a", 64), EffectSignatureHash: strings.Repeat("b", 64), EnvironmentScopeHash: strings.Repeat("c", 64),
		ArchiveHash: strings.Repeat("d", 64), CanonicalEventStart: 0, CanonicalEventEnd: 1,
		CreatedAt: time.Date(2026, 9, 20, 0, 0, evidenceNumber, 0, time.UTC), Synthesis: result,
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
