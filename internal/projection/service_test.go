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
			SchemaVersion: "synthesis.v1", Status: synthesis.StatusSynthesized, TraceID: "tr_first",
			CausalGraphHash: "source-graph", ObservedEndToEnd: true, Hash: strings.Repeat("8", 64),
			GoalPredicates: []string{"goal"},
			Versions:       synthesis.Versions{Sanitizer: "sanitizer.v1", Registry: "registry.v1", Policy: "policy.v1", GraphBuilder: "graph.v1", Synthesizer: "synth.v1"},
			Steps: []domain.ProcedureStep{
				{EventID: "event-a", Ordinal: 0, OriginalPosition: 0, ToolName: "write", ToolVersion: "1.0.0", ToolContractVersionID: "tcv_write"},
				{EventID: "event-b", Ordinal: 1, OriginalPosition: 1, ToolName: "verify", ToolVersion: "1.0.0", ToolContractVersionID: "tcv_verify"},
			},
			Edges: []synthesis.Edge{{From: "event-a", To: "event-b", Type: synthesis.EdgeResourceFlow, ResourceID: "res_instance", ResourceName: "workspace", ResourceType: "repository", ResourceNamespace: "repo"}},
		},
	}
}
