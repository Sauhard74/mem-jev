package retrieval_test

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
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

func TestProcedureInterfaceKeepsResourceSchemasDistinct(t *testing.T) {
	first, err := retrieval.NewProcedureInterface([]domain.ProcedureRequirement{{ResourceType: "dataset", Namespace: "data", IdentityHash: strings.Repeat("c", 64), SchemaVersion: "v1", AccessMode: "read"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := retrieval.NewProcedureInterface([]domain.ProcedureRequirement{{ResourceType: "dataset", Namespace: "data", IdentityHash: strings.Repeat("c", 64), SchemaVersion: "v2", AccessMode: "read"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == second.ID || first.Requirements[0].ID == second.Requirements[0].ID {
		t.Fatalf("schema mismatch collapsed: first=%#v second=%#v", first, second)
	}
}

func TestLegacyDocumentIsReadableButCannotBeRevised(t *testing.T) {
	value, err := retrieval.BuildDocument(validDocumentInput())
	if err != nil {
		t.Fatal(err)
	}
	value.SchemaVersion = "retrieval-document.v1"
	value.Interface = nil
	value.ID, value.ContentHash, value.CanonicalJSON = "", "", nil
	canonicalJSON, hash, err := canonical.MarshalAndHash(value)
	if err != nil {
		t.Fatal(err)
	}
	value.ID, value.ContentHash, value.CanonicalJSON = "rdoc_"+hash, hash, canonicalJSON
	if err := retrieval.ValidateDocument(value); err != nil {
		t.Fatalf("legacy document is not readable: %v", err)
	}
	if _, err := retrieval.ReviseEvidence(value, 2, 0, time.Now().UTC()); err == nil {
		t.Fatal("legacy document was mutated")
	}
}

func TestReviseLifecycleCreatesAuthenticatedImmutableDocument(t *testing.T) {
	original, err := retrieval.BuildDocument(validDocumentInput())
	if err != nil {
		t.Fatal(err)
	}
	revisedAt := time.Date(2026, 9, 21, 1, 2, 3, 0, time.UTC)
	revised, err := retrieval.ReviseLifecycle(original, "quarantined", 20, 2, revisedAt, "lifecycle.v1")
	if err != nil {
		t.Fatal(err)
	}
	if revised.ID == original.ID || revised.Lifecycle != "quarantined" || revised.VerifiedSuccessCount != 20 || revised.UnsafeOutcomeCount != 2 || revised.ValidationPolicyVersion != "lifecycle.v1" || retrieval.ValidateDocument(revised) != nil {
		t.Fatalf("revised document = %#v", revised)
	}
	if original.Lifecycle != "candidate" || original.VerifiedSuccessCount != 1 {
		t.Fatalf("original document mutated = %#v", original)
	}
	if _, err := retrieval.ReviseLifecycle(original, "unknown", 20, 0, revisedAt, "lifecycle.v1"); err == nil {
		t.Fatal("invalid lifecycle accepted")
	}
}

func validDocumentInput() retrieval.DocumentInput {
	procedureInterface, err := retrieval.NewProcedureInterface(
		[]domain.ProcedureRequirement{{ResourceType: "repository", Namespace: "git", IdentityHash: strings.Repeat("a", 64), SchemaVersion: "v1", AccessMode: "read", PredicateIDs: []string{"workspace.exists"}}},
		[]domain.ProcedureProvision{{ResourceType: "artifact", Namespace: "release", IdentityHash: strings.Repeat("b", 64), SchemaVersion: "v1", ProducedEffects: []string{"filesystem.write"}, SuccessPredicateIDs: []string{"artifact.exists"}}},
	)
	if err != nil {
		panic(err)
	}
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
		Interface: &procedureInterface,
	}
}
