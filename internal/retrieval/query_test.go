package retrieval_test

import (
	"strings"
	"testing"

	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/retrieval"
)

func TestBuildCanonicalQueryNormalizesAliasesPathsAndSets(t *testing.T) {
	aliases := retrieval.AliasSet{
		Tools:     map[string]string{" Sh ": "shell"},
		Resources: map[string]string{"repo": "repository"},
	}
	left := validInput()
	left.Task = "  decompose\u0301\r\nrelease   "
	left.Tools = []retrieval.Tool{{Name: "Sh", ContractVersionID: "tc_2"}, {Name: "deploy", ContractVersionID: "tc_1"}, {Name: "shell", ContractVersionID: "tc_2"}}
	left.Resources = []retrieval.Resource{{Type: "repo", Namespace: " git ", Identity: `src\\service/../app`}, {Type: "bucket", Identity: "artifacts"}}
	left.ForbiddenEffects = []string{" network.write ", "filesystem.delete", "network.write"}
	left.Environment = []retrieval.Fact{{Name: "arch", Value: " arm64 "}, {Name: "os", Value: "linux"}}

	right := validInput()
	right.Task = "decompos\u00e9\nrelease"
	right.Tools = []retrieval.Tool{{Name: "deploy", ContractVersionID: "tc_1"}, {Name: "shell", ContractVersionID: "tc_2"}}
	right.Resources = []retrieval.Resource{{Type: "bucket", Identity: "artifacts"}, {Type: "repository", Namespace: "git", Identity: "src/app"}}
	right.ForbiddenEffects = []string{"filesystem.delete", "network.write"}
	right.Environment = []retrieval.Fact{{Name: "os", Value: "linux"}, {Name: "arch", Value: "arm64"}}

	q1, err := retrieval.BuildQuery(domain.TenantID("tenant-a"), "policy.v1", aliases, left)
	if err != nil {
		t.Fatalf("BuildQuery(left): %v", err)
	}
	q2, err := retrieval.BuildQuery(domain.TenantID("tenant-a"), "policy.v1", aliases, right)
	if err != nil {
		t.Fatalf("BuildQuery(right): %v", err)
	}
	if q1.Hash != q2.Hash || string(q1.CanonicalJSON) != string(q2.CanonicalJSON) {
		t.Fatalf("equivalent inputs differ:\n%s\n%s", q1.CanonicalJSON, q2.CanonicalJSON)
	}
	if !strings.Contains(string(q1.CanonicalJSON), `"identity":"src/app"`) {
		t.Fatalf("canonical JSON does not expose normalized path: %s", q1.CanonicalJSON)
	}
	if q1.SchemaVersion != "retrieval-query.v1" {
		t.Fatalf("SchemaVersion = %q", q1.SchemaVersion)
	}
}

func TestBuildCanonicalQueryBindsServerContextAndMeaning(t *testing.T) {
	base, err := retrieval.BuildQuery("tenant-a", "policy.v1", retrieval.AliasSet{}, validInput())
	if err != nil {
		t.Fatal(err)
	}
	mutations := []struct {
		name string
		fn   func(*retrieval.Input) (domain.TenantID, string)
	}{
		{name: "task", fn: func(in *retrieval.Input) (domain.TenantID, string) {
			in.Task = "another task"
			return "tenant-a", "policy.v1"
		}},
		{name: "tenant", fn: func(_ *retrieval.Input) (domain.TenantID, string) { return "tenant-b", "policy.v1" }},
		{name: "policy", fn: func(_ *retrieval.Input) (domain.TenantID, string) { return "tenant-a", "policy.v2" }},
		{name: "tool-version", fn: func(in *retrieval.Input) (domain.TenantID, string) {
			in.Tools[0].ContractVersionID = "tc_2"
			return "tenant-a", "policy.v1"
		}},
		{name: "risk", fn: func(in *retrieval.Input) (domain.TenantID, string) {
			in.RiskClass = RiskHigh
			return "tenant-a", "policy.v1"
		}},
	}
	for _, tt := range mutations {
		t.Run(tt.name, func(t *testing.T) {
			input := validInput()
			tenant, policy := tt.fn(&input)
			got, buildErr := retrieval.BuildQuery(tenant, policy, retrieval.AliasSet{}, input)
			if buildErr != nil {
				t.Fatal(buildErr)
			}
			if got.Hash == base.Hash {
				t.Fatalf("hash unchanged for meaningful mutation")
			}
		})
	}
}

func TestBuildCanonicalQueryRejectsAmbiguityAndUnsafePaths(t *testing.T) {
	tests := []struct {
		name    string
		aliases retrieval.AliasSet
		mutate  func(*retrieval.Input)
	}{
		{name: "empty task", mutate: func(in *retrieval.Input) { in.Task = " \t " }},
		{name: "duplicate fact conflict", mutate: func(in *retrieval.Input) {
			in.Environment = append(in.Environment, retrieval.Fact{Name: "os", Value: "darwin"})
		}},
		{name: "tool contract conflict", mutate: func(in *retrieval.Input) {
			in.Tools = append(in.Tools, retrieval.Tool{Name: "shell", ContractVersionID: "tc_2"})
		}},
		{name: "absolute resource", mutate: func(in *retrieval.Input) { in.Resources[0].Identity = "/etc/passwd" }},
		{name: "escaping resource", mutate: func(in *retrieval.Input) { in.Resources[0].Identity = "../outside" }},
		{name: "alias collision", aliases: retrieval.AliasSet{Tools: map[string]string{"shell": "one", " shell ": "two"}}, mutate: func(_ *retrieval.Input) {}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := validInput()
			tt.mutate(&input)
			if _, err := retrieval.BuildQuery("tenant-a", "policy.v1", tt.aliases, input); err == nil {
				t.Fatal("BuildQuery() error = nil")
			}
		})
	}
}

func TestInputFromProtoPreservesTypedRequest(t *testing.T) {
	req := &memjevv1.RetrieveRequest{
		Task:             "ship release",
		Tools:            []*memjevv1.AvailableTool{{Name: "shell", ContractVersionId: "tc_1"}},
		Harness:          &memjevv1.HarnessIdentity{Name: "codex", Version: "1"},
		Environment:      []*memjevv1.QueryFact{{Name: "os", Value: "linux"}},
		Resources:        []*memjevv1.AccessibleResource{{Type: "repository", Namespace: "git", Identity: "src/app"}},
		ForbiddenEffects: []string{"filesystem.delete"},
		Constraints:      []*memjevv1.QueryFact{{Name: "region", Value: "eu"}},
		RiskClass:        memjevv1.RiskClass_RISK_CLASS_MEDIUM,
		LatencyClass:     memjevv1.LatencyClass_LATENCY_CLASS_INTERACTIVE,
		MaxCandidates:    12,
	}
	input := retrieval.InputFromProto(req)
	if input.Task != req.Task || len(input.Tools) != 1 || input.Tools[0].ContractVersionID != "tc_1" || input.MaxCandidates != 12 {
		t.Fatalf("InputFromProto() = %#v", input)
	}
}

func validInput() retrieval.Input {
	return retrieval.Input{
		Task:             "decompose release",
		Tools:            []retrieval.Tool{{Name: "shell", ContractVersionID: "tc_1"}},
		Harness:          retrieval.Harness{Name: "codex", Version: "1"},
		Environment:      []retrieval.Fact{{Name: "os", Value: "linux"}},
		Resources:        []retrieval.Resource{{Type: "repository", Namespace: "git", Identity: "src/app"}},
		Constraints:      []retrieval.Fact{{Name: "region", Value: "eu"}},
		ForbiddenEffects: []string{"filesystem.delete"},
		RiskClass:        RiskMedium,
		LatencyClass:     LatencyInteractive,
		MaxCandidates:    20,
	}
}

const (
	RiskMedium         = retrieval.RiskClass("medium")
	RiskHigh           = retrieval.RiskClass("high")
	LatencyInteractive = retrieval.LatencyClass("interactive")
)
