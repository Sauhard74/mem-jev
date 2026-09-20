package retrieval_test

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/retrieval"
)

func TestBuildDocumentCanonicalizesServingMetadata(t *testing.T) {
	left := validDocumentInput()
	left.Tools = []retrieval.ToolRequirement{{Name: "verify", ContractVersionID: "tc_2"}, {Name: "write", ContractVersionID: "tc_1"}, {Name: "write", ContractVersionID: "tc_1"}}
	left.Resources = []retrieval.ResourceRequirement{{Type: "repository", Namespace: "git", IdentityHash: strings.Repeat("a", 64), SchemaVersion: "v1"}, {Type: "bucket", IdentityHash: strings.Repeat("b", 64)}}
	left.Effects = []string{"filesystem.write", "network.read", "filesystem.write"}
	left.Environment = []retrieval.Fact{{Name: "os", Value: "linux"}, {Name: "arch", Value: "arm64"}}
	right := validDocumentInput()
	right.Tools = []retrieval.ToolRequirement{{Name: "write", ContractVersionID: "tc_1"}, {Name: "verify", ContractVersionID: "tc_2"}}
	right.Resources = slices.Clone(left.Resources)
	slices.Reverse(right.Resources)
	right.Effects = []string{"network.read", "filesystem.write"}
	right.Environment = []retrieval.Fact{{Name: "arch", Value: "arm64"}, {Name: "os", Value: "linux"}}

	first, err := retrieval.BuildDocument(left)
	if err != nil {
		t.Fatal(err)
	}
	second, err := retrieval.BuildDocument(right)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID || string(first.CanonicalJSON) != string(second.CanonicalJSON) {
		t.Fatalf("canonical documents differ:\n%s\n%s", first.CanonicalJSON, second.CanonicalJSON)
	}
	if len(first.PrefixHashes) != 2 || first.ContentHash == "" || !strings.HasPrefix(first.ID, "rdoc_") {
		t.Fatalf("document = %#v", first)
	}
}

func TestBuildDocumentRejectsIncompleteOrConflictingMetadata(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*retrieval.DocumentInput)
	}{
		{name: "tenant", mutate: func(in *retrieval.DocumentInput) { in.TenantID = "" }},
		{name: "task", mutate: func(in *retrieval.DocumentInput) { in.TaskText = " " }},
		{name: "hash", mutate: func(in *retrieval.DocumentInput) { in.IntentHash = "short" }},
		{name: "tool conflict", mutate: func(in *retrieval.DocumentInput) {
			in.Tools = append(in.Tools, retrieval.ToolRequirement{Name: "write", ContractVersionID: "other"})
		}},
		{name: "resource hash", mutate: func(in *retrieval.DocumentInput) { in.Resources[0].IdentityHash = "secret/path" }},
		{name: "risk", mutate: func(in *retrieval.DocumentInput) { in.RiskClass = "unknown" }},
		{name: "verification", mutate: func(in *retrieval.DocumentInput) { in.VerificationStrength = 0 }},
		{name: "consent", mutate: func(in *retrieval.DocumentInput) { in.LearnedWithRecallConsent = false }},
		{name: "residency", mutate: func(in *retrieval.DocumentInput) { in.ResidencyRegion = "" }},
		{name: "time", mutate: func(in *retrieval.DocumentInput) { in.ValidatedAt = time.Time{} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := validDocumentInput()
			tt.mutate(&input)
			if _, err := retrieval.BuildDocument(input); err == nil {
				t.Fatal("BuildDocument() error = nil")
			}
		})
	}
}

func validDocumentInput() retrieval.DocumentInput {
	return retrieval.DocumentInput{
		TenantID: "tenant-a", ProcedureVersionID: "pv_1", ProcedureID: "proc_1",
		TaskText: "ship release", IntentHash: strings.Repeat("1", 64), EffectSignatureHash: strings.Repeat("2", 64),
		Tools:                  []retrieval.ToolRequirement{{Name: "write", ContractVersionID: "tc_1"}, {Name: "verify", ContractVersionID: "tc_2"}},
		OrderedStepContractIDs: []string{"tc_1", "tc_2"},
		Resources:              []retrieval.ResourceRequirement{{Type: "repository", Namespace: "git", IdentityHash: strings.Repeat("a", 64), SchemaVersion: "v1"}},
		Effects:                []string{"filesystem.write"}, EnvironmentScopeHash: strings.Repeat("3", 64),
		Harness: retrieval.Harness{Name: "codex", Version: "1"}, Environment: []retrieval.Fact{{Name: "os", Value: "linux"}},
		Lifecycle: "candidate", ObservedEndToEnd: true, VerificationStrength: 5, VerifiedSuccessCount: 1,
		ValidatedAt: time.Date(2026, 9, 20, 1, 2, 3, 0, time.UTC), ValidationPolicyVersion: "evidence.v1",
		LearnedWithRecallConsent: true, ResidencyRegion: "eu", RiskClass: "medium",
	}
}
