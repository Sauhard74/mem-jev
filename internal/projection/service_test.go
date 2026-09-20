package projection_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/projection"
	"github.com/sauhard74/mem-jev/internal/retrieval"
	"github.com/sauhard74/mem-jev/internal/synthesis"
)

func TestBuildUsesLogicalStepGraphNotTraceSpecificIDsForVersionIdentity(t *testing.T) {
	first := buildRequest()
	second := buildRequest()
	second.OutcomeID = domain.OutcomeID("out_" + strings.Repeat("e", 64))
	second.Synthesis.TraceID = "tr_second"
	second.Synthesis.Steps[0].EventID = "event-x"
	second.Synthesis.Steps[1].EventID = "event-y"
	second.Synthesis.Edges[0].From = "event-x"
	second.Synthesis.Edges[0].To = "event-y"
	second.Synthesis.Hash = strings.Repeat("9", 64)

	gotFirst, err := projection.Build(first)
	if err != nil {
		t.Fatal(err)
	}
	gotSecond, err := projection.Build(second)
	if err != nil {
		t.Fatal(err)
	}
	if gotFirst.Family.ID != gotSecond.Family.ID || gotFirst.Version.ID != gotSecond.Version.ID || gotFirst.Version.GraphHash != gotSecond.Version.GraphHash || !bytes.Equal(gotFirst.CanonicalProjectionJSON, gotSecond.CanonicalProjectionJSON) {
		t.Fatalf("trace-specific values changed projection identity:\n%#v\n%#v", gotFirst, gotSecond)
	}
	if gotFirst.Manifest.ID == gotSecond.Manifest.ID {
		t.Fatal("distinct evidence inputs must retain distinct synthesis manifests")
	}
	if len(gotFirst.Interface.Requirements) != 1 || len(gotFirst.Interface.Provisions) != 1 || gotFirst.Version.InterfaceHash != gotFirst.Interface.ContentHash || gotFirst.RetrievalDocument.Interface == nil || gotFirst.RetrievalDocument.SchemaVersion != "retrieval-document.v2" {
		t.Fatalf("typed interface was not published: %#v", gotFirst)
	}
}

func TestBuildCreatesChallengerIdentityForDifferentLogicalGraph(t *testing.T) {
	first, err := projection.Build(buildRequest())
	if err != nil {
		t.Fatal(err)
	}
	changed := buildRequest()
	changed.Synthesis.Steps[1].ToolContractVersionID = "tcv_changed"
	second, err := projection.Build(changed)
	if err != nil {
		t.Fatal(err)
	}
	if first.Family.ID != second.Family.ID || first.Version.ID == second.Version.ID || first.Version.GraphHash == second.Version.GraphHash {
		t.Fatalf("first=%#v second=%#v", first.Version, second.Version)
	}
}

func TestBuildCreatesDifferentVersionAcrossPolicyBoundaries(t *testing.T) {
	first, err := projection.Build(buildRequest())
	if err != nil {
		t.Fatal(err)
	}
	changed := buildRequest()
	changed.Synthesis.Versions.Policy = "policy.v2"
	second, err := projection.Build(changed)
	if err != nil {
		t.Fatal(err)
	}
	if first.Family.ID != second.Family.ID || first.Version.ID == second.Version.ID {
		t.Fatalf("first=%#v second=%#v", first.Version, second.Version)
	}
}

func TestBuildInterfaceIsOrderIndependentForCausallyIndependentReads(t *testing.T) {
	firstRequest := buildRequest()
	firstRequest.Synthesis.Steps = append(firstRequest.Synthesis.Steps,
		domain.ProcedureStep{EventID: "event-c", Ordinal: 2, ToolName: "inspect-a", ToolContractVersionID: "tcv_inspect_a", SideEffect: "read", Risk: "low", Reads: []domain.ProcedureResource{{ID: "res_" + strings.Repeat("4", 64), Name: "input-a", Type: "dataset", Namespace: "data", IdentityHash: strings.Repeat("4", 64), SchemaVersion: "v1"}}},
		domain.ProcedureStep{EventID: "event-d", Ordinal: 3, ToolName: "inspect-b", ToolContractVersionID: "tcv_inspect_b", SideEffect: "read", Risk: "low", Reads: []domain.ProcedureResource{{ID: "res_" + strings.Repeat("5", 64), Name: "input-b", Type: "dataset", Namespace: "data", IdentityHash: strings.Repeat("5", 64), SchemaVersion: "v1"}}},
	)
	secondRequest := firstRequest
	secondRequest.Synthesis.Steps = append([]domain.ProcedureStep(nil), firstRequest.Synthesis.Steps...)
	secondRequest.Synthesis.Steps[2], secondRequest.Synthesis.Steps[3] = secondRequest.Synthesis.Steps[3], secondRequest.Synthesis.Steps[2]
	secondRequest.Synthesis.Steps[2].Ordinal, secondRequest.Synthesis.Steps[3].Ordinal = 2, 3
	first, err := projection.Build(firstRequest)
	if err != nil {
		t.Fatal(err)
	}
	second, err := projection.Build(secondRequest)
	if err != nil {
		t.Fatal(err)
	}
	if first.Interface.ID != second.Interface.ID || first.Version.ID == second.Version.ID {
		t.Fatalf("interface must ignore independent event order while graph identity retains it: first=%#v second=%#v", first, second)
	}
}

func TestBuildInterfaceDoesNotClaimReadOnlyResourceAsProvision(t *testing.T) {
	value, err := projection.Build(buildRequest())
	if err != nil {
		t.Fatal(err)
	}
	if len(value.Interface.Requirements) != 1 || value.Interface.Requirements[0].IdentityHash != strings.Repeat("9", 64) || len(value.Interface.Provisions) != 1 || value.Interface.Provisions[0].IdentityHash != strings.Repeat("7", 64) {
		t.Fatalf("interface = %#v", value.Interface)
	}
}

func TestBuildPersistsAbstentionManifestWithoutProcedure(t *testing.T) {
	request := buildRequest()
	request.Synthesis.Status = synthesis.StatusAbstained
	request.Synthesis.AbstentionCode = "opaque_tool"
	request.Synthesis.Steps = nil
	request.Synthesis.Edges = nil
	projection, err := projection.Build(request)
	if err != nil {
		t.Fatal(err)
	}
	if projection.Manifest.Status != synthesis.StatusAbstained || projection.Manifest.AbstentionCode != "opaque_tool" || projection.Version.ID != "" || len(projection.CanonicalProjectionJSON) != 0 {
		t.Fatalf("projection = %#v", projection)
	}
}

func buildRequest() projection.BuildRequest {
	_, intentHash, _ := canonical.MarshalAndHash(struct {
		Task           string `json:"task"`
		Harness        string `json:"harness"`
		HarnessVersion string `json:"harness_version,omitempty"`
	}{"write and verify", "test-harness", "1"})
	source := domain.ProcedureResource{ID: "res_" + strings.Repeat("9", 64), Name: "source", Type: "repository", Namespace: "repo", IdentityHash: strings.Repeat("9", 64), SchemaVersion: "v1"}
	workspace := domain.ProcedureResource{ID: "res_" + strings.Repeat("a", 64), Name: "workspace", Type: "repository", Namespace: "repo", IdentityHash: strings.Repeat("a", 64), SchemaVersion: "v1"}
	artifact := domain.ProcedureResource{ID: "res_" + strings.Repeat("7", 64), Name: "artifact", Type: "artifact", Namespace: "build", IdentityHash: strings.Repeat("7", 64), SchemaVersion: "v1"}
	return projection.BuildRequest{
		TenantID: "tenant_a", OutcomeID: domain.OutcomeID("out_" + strings.Repeat("d", 64)),
		IntentHash: intentHash, EffectSignatureHash: strings.Repeat("b", 64), EnvironmentScopeHash: strings.Repeat("c", 64),
		ArchiveHash: strings.Repeat("f", 64), CanonicalEventStart: 0, CanonicalEventEnd: 1,
		CreatedAt: time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC),
		Serving: projection.ServingMetadata{
			TaskText: "write and verify", Harness: retrieval.Harness{Name: "test-harness", Version: "1"},
			Resources: []retrieval.ResourceRequirement{{Type: "repository", Namespace: "repo", IdentityHash: strings.Repeat("9", 64), SchemaVersion: "v1"}},
			Effects:   []string{"filesystem.write"}, RiskClass: "medium", VerificationStrength: 5,
			LearnedWithRecallConsent: true, ResidencyRegion: "local",
		},
		Synthesis: synthesis.Result{
			SchemaVersion: "synthesis.v2", Status: synthesis.StatusSynthesized, TraceID: "tr_first",
			CausalGraphHash: "source-graph", ObservedEndToEnd: true, Hash: strings.Repeat("8", 64),
			GoalPredicates: []string{"goal"},
			Versions:       synthesis.Versions{Sanitizer: "sanitizer.v1", Registry: "registry.v1", Policy: "policy.v1", GraphBuilder: "graph.v1", Synthesizer: "synth.v1"},
			Steps: []domain.ProcedureStep{
				{EventID: "event-a", Ordinal: 0, OriginalPosition: 0, ToolName: "write", ToolVersion: "1.0.0", ToolContractVersionID: "tcv_write", SideEffect: "write", Risk: "medium", Reads: []domain.ProcedureResource{source}, Writes: []domain.ProcedureResource{workspace}, Preconditions: []domain.ProcedurePredicate{{ID: "source.exists", ResourceName: "source"}}},
				{EventID: "event-b", Ordinal: 1, OriginalPosition: 1, ToolName: "verify", ToolVersion: "1.0.0", ToolContractVersionID: "tcv_verify", SideEffect: "write", Risk: "low", Reads: []domain.ProcedureResource{workspace}, Writes: []domain.ProcedureResource{artifact}, Effects: []string{"artifact.write"}, SuccessPredicates: []string{"artifact.exists"}},
			},
			Edges: []synthesis.Edge{{From: "event-a", To: "event-b", Type: synthesis.EdgeResourceFlow, ResourceID: workspace.ID, ResourceName: "workspace", ResourceType: "repository", ResourceNamespace: "repo"}},
		},
	}
}
