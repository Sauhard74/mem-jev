package synthesis_test

import (
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/synthesis"
)

func TestSynthesizerRetainsProvenAndUncertainStepsAndScopesFailures(t *testing.T) {
	graph := synthesisGraph()
	result, err := synthesis.Synthesize(synthesis.Request{
		Graph: graph, OutcomeState: domain.OutcomeStateVerifiedSuccess,
		Goals:           []synthesis.GoalRequirement{{PredicateID: "goal", VerifierNodes: []domain.EventID{"verify"}}},
		IrrelevantNodes: []domain.EventID{"dead"},
		FailedBranches: []synthesis.FailedBranch{{
			FailurePredicateID: "compile-failed", EventIDs: []domain.EventID{"failed"},
			Scope: domain.CompatibilityScope{EnvironmentHash: "env-1", ToolHash: "tools-1", ResourceHash: "resources-1"},
		}},
		Versions: synthesis.Versions{Synthesizer: "synth.v1", GraphBuilder: "graph.v1", Policy: "policy.v1", Registry: "registry.v1", Sanitizer: "sanitizer.v1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != synthesis.StatusSynthesized || result.Hash == "" || len(result.Steps) != 4 {
		t.Fatalf("result = %#v", result)
	}
	wantIDs := []domain.EventID{"prepare", "execute", "verify", "ambiguous"}
	for index, step := range result.Steps {
		if step.EventID != wantIDs[index] {
			t.Fatalf("steps = %#v", result.Steps)
		}
		if step.EventID == "ambiguous" && !step.UncertainNecessity {
			t.Fatal("unproven but not irrelevant step must be marked uncertain")
		}
	}
	if len(result.NegativePaths) != 1 || !result.NegativePaths[0].CompatibleWith(domain.CompatibilityScope{EnvironmentHash: "env-1", ToolHash: "tools-1", ResourceHash: "resources-1"}) || result.NegativePaths[0].CompatibleWith(domain.CompatibilityScope{EnvironmentHash: "other", ToolHash: "tools-1", ResourceHash: "resources-1"}) {
		t.Fatalf("negative paths = %#v", result.NegativePaths)
	}
	if result.SchemaVersion != "synthesis.v2" || len(result.Steps[0].Writes) != 1 || result.Steps[0].Writes[0].IdentityHash != strings.Repeat("a", 64) || len(result.Steps[1].Reads) != 1 || len(result.Steps[1].Preconditions) != 1 || result.Steps[1].Preconditions[0].ID != "workspace.exists" || len(result.Steps[1].Effects) != 1 || result.Steps[1].Effects[0] != "filesystem.write" || result.Steps[1].Risk != "medium" || result.Steps[1].SideEffect != "write" || len(result.Steps[2].SuccessPredicates) != 1 {
		t.Fatalf("typed step metadata = %#v", result.Steps)
	}
	for _, step := range result.Steps {
		for _, resource := range append(append([]domain.ProcedureResource(nil), step.Reads...), step.Writes...) {
			if resource.IdentityHash == strings.Repeat("f", 64) {
				t.Fatal("dead-branch resource leaked into synthesized procedure")
			}
		}
	}
}

func TestSynthesizerAbstainsWithoutVerifiedCompletePromotableEvidence(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*synthesis.Request)
		code   string
	}{
		{name: "provisional", mutate: func(request *synthesis.Request) { request.OutcomeState = domain.OutcomeStateProvisionalSuccess }, code: "outcome_not_verified"},
		{name: "opaque", mutate: func(request *synthesis.Request) { request.Graph.AutoPromotable = false }, code: "opaque_tool"},
		{name: "uncovered goal", mutate: func(request *synthesis.Request) { request.Goals[0].VerifierNodes = nil }, code: "uncovered_goal"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := validSynthesisRequest()
			tt.mutate(&request)
			result, err := synthesis.Synthesize(request)
			if err != nil {
				t.Fatal(err)
			}
			if result.Status != synthesis.StatusAbstained || result.AbstentionCode != tt.code || len(result.Steps) != 0 {
				t.Fatalf("result = %#v", result)
			}
		})
	}
}

func TestSynthesizerIsStableAcrossSetInputOrder(t *testing.T) {
	request := validSynthesisRequest()
	request.IrrelevantNodes = []domain.EventID{"dead", "failed"}
	request.Goals = append(request.Goals, synthesis.GoalRequirement{PredicateID: "also-goal", VerifierNodes: []domain.EventID{"verify"}})
	first, err := synthesis.Synthesize(request)
	if err != nil {
		t.Fatal(err)
	}
	slices.Reverse(request.IrrelevantNodes)
	slices.Reverse(request.Goals)
	slices.Reverse(request.Graph.Edges)
	second, err := synthesis.Synthesize(request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Hash != second.Hash || !reflect.DeepEqual(first.Steps, second.Steps) || !slices.Equal(first.Edges, second.Edges) {
		t.Fatalf("outputs differ: %#v %#v", first, second)
	}
}

func TestSynthesizerRejectsConflictingRetainedResourceMetadata(t *testing.T) {
	request := validSynthesisRequest()
	request.Graph.Nodes[0].Writes = append(request.Graph.Nodes[0].Writes, synthesis.Resource{ID: "res_" + strings.Repeat("a", 64), Name: "workspace", Type: "repository", Namespace: "repo", IdentityHash: strings.Repeat("a", 64), SchemaVersion: "v2"})
	if _, err := synthesis.Synthesize(request); !errors.Is(err, synthesis.ErrInvalidSynthesisRequest) {
		t.Fatalf("conflicting resource error = %v", err)
	}
}

func TestSynthesizerRejectsConflictingResourceMetadataAcrossRetainedSteps(t *testing.T) {
	request := validSynthesisRequest()
	request.Graph.Nodes[1].Reads[0].SchemaVersion = "v2"
	if _, err := synthesis.Synthesize(request); !errors.Is(err, synthesis.ErrInvalidSynthesisRequest) {
		t.Fatalf("cross-step resource conflict error = %v", err)
	}
}

func validSynthesisRequest() synthesis.Request {
	return synthesis.Request{
		Graph: synthesisGraph(), OutcomeState: domain.OutcomeStateVerifiedSuccess,
		Goals:           []synthesis.GoalRequirement{{PredicateID: "goal", VerifierNodes: []domain.EventID{"verify"}}},
		IrrelevantNodes: []domain.EventID{"dead", "failed"},
		Versions:        synthesis.Versions{Synthesizer: "synth.v1", GraphBuilder: "graph.v1", Policy: "policy.v1", Registry: "registry.v1", Sanitizer: "sanitizer.v1"},
	}
}

func synthesisGraph() synthesis.Graph {
	workspace := synthesis.Resource{ID: "res_" + strings.Repeat("a", 64), Name: "workspace", Type: "repository", Namespace: "repo", IdentityHash: strings.Repeat("a", 64), SchemaVersion: "v1"}
	nodes := []synthesis.Node{
		{ID: "prepare", Position: 0, ToolName: "prepare", ToolContractVersionID: "tcv_prepare", Succeeded: true, SideEffect: "none", Risk: "low", Writes: []synthesis.Resource{workspace}},
		{ID: "execute", Position: 1, ToolName: "execute", ToolContractVersionID: "tcv_execute", Succeeded: true, SideEffect: "write", Risk: "medium", Reads: []synthesis.Resource{workspace}, Writes: []synthesis.Resource{workspace}, Effects: []string{"filesystem.write"}, Preconditions: []synthesis.PredicateReference{{ID: "workspace.exists", ResourceName: "workspace"}}},
		{ID: "verify", Position: 2, ToolName: "verify", ToolContractVersionID: "tcv_verify", Succeeded: true, SideEffect: "read", Risk: "low", Reads: []synthesis.Resource{workspace}, SuccessPredicates: []string{"output.exists"}},
		{ID: "ambiguous", Position: 3, ToolName: "inspect", ToolContractVersionID: "tcv_inspect", Succeeded: true, SideEffect: "read", Risk: "low", Reads: []synthesis.Resource{workspace}},
		{ID: "dead", Position: 4, ToolName: "dead", ToolContractVersionID: "tcv_dead", Succeeded: true, SideEffect: "write", Risk: "low", Writes: []synthesis.Resource{{ID: "res_" + strings.Repeat("f", 64), Name: "dead", Type: "file", Namespace: "tmp", IdentityHash: strings.Repeat("f", 64)}}},
		{ID: "failed", Position: 5, ToolName: "failed", ToolContractVersionID: "tcv_failed", Succeeded: false, SideEffect: "write", Risk: "low"},
	}
	return synthesis.Graph{
		SchemaVersion: "causal-graph.v1", TraceID: "tr_one", Nodes: nodes, AutoPromotable: true, Hash: "graph-hash",
		Edges: []synthesis.Edge{
			{From: "prepare", To: "execute", Type: synthesis.EdgeControl},
			{From: "execute", To: "verify", Type: synthesis.EdgeVerifier},
		},
	}
}
