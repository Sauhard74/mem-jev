package toolcontract_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/sauhard74/mem-jev/internal/toolcontract"
)

func TestCanonicalizeManifestIsStableAcrossDeclarationOrder(t *testing.T) {
	first := validManifest()
	second := validManifest()
	slices.Reverse(second.Aliases)
	slices.Reverse(second.Inputs)
	slices.Reverse(second.Outputs)
	slices.Reverse(second.Reads)
	slices.Reverse(second.VerificationMethods)

	gotFirst, err := toolcontract.Canonicalize(first)
	if err != nil {
		t.Fatal(err)
	}
	gotSecond, err := toolcontract.Canonicalize(second)
	if err != nil {
		t.Fatal(err)
	}
	if gotFirst.ID != gotSecond.ID || gotFirst.ContentHash != gotSecond.ContentHash {
		t.Fatalf("canonical manifests differ: %#v != %#v", gotFirst, gotSecond)
	}
}

func TestCanonicalizeRejectsAcceptedFieldWithoutSanitizer(t *testing.T) {
	manifest := validManifest()
	manifest.Inputs[0].Sanitizer = ""
	if _, err := toolcontract.Canonicalize(manifest); !errors.Is(err, toolcontract.ErrInvalidManifest) {
		t.Fatalf("Canonicalize() error = %v; want ErrInvalidManifest", err)
	}
}

func TestCanonicalizeRejectsResourceReferencingUnknownField(t *testing.T) {
	manifest := validManifest()
	manifest.Reads[0].Field = "missing"
	if _, err := toolcontract.Canonicalize(manifest); !errors.Is(err, toolcontract.ErrInvalidManifest) {
		t.Fatalf("Canonicalize() error = %v; want ErrInvalidManifest", err)
	}
}

func TestRegistryResolvesCanonicalNameAliasAndCompatibleVersion(t *testing.T) {
	registry := toolcontract.NewMemoryRegistry()
	manifest, err := toolcontract.Canonicalize(validManifest())
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(context.Background(), manifest); err != nil {
		t.Fatal(err)
	}

	for _, query := range []toolcontract.Query{
		{Name: "shell", Version: "1.2.0"},
		{Name: "bash", Version: "1.2.0"},
		{Name: "sh", Version: "1.8.4"},
	} {
		resolved, err := registry.Resolve(context.Background(), query)
		if err != nil {
			t.Fatal(err)
		}
		if resolved.Opaque || !resolved.AutoPromotable || resolved.Manifest.ID != manifest.ID {
			t.Fatalf("Resolve(%#v) = %#v", query, resolved)
		}
	}
}

func TestRegistryReturnsNonPromotableOpaqueContractForUnknownTool(t *testing.T) {
	resolved, err := toolcontract.NewMemoryRegistry().Resolve(context.Background(), toolcontract.Query{Name: "custom-tool", Version: "9"})
	if err != nil {
		t.Fatal(err)
	}
	if !resolved.Opaque || resolved.AutoPromotable || len(resolved.Manifest.Reads) != 0 || len(resolved.Manifest.Writes) != 0 {
		t.Fatalf("Resolve() = %#v; opaque tools must infer no resources and cannot promote", resolved)
	}
}

func TestRegistryRejectsAliasCollisionAndVersionMutation(t *testing.T) {
	registry := toolcontract.NewMemoryRegistry()
	first, err := toolcontract.Canonicalize(validManifest())
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(context.Background(), first); err != nil {
		t.Fatal(err)
	}

	mutatedSource := validManifest()
	mutatedSource.Risk = toolcontract.RiskHigh
	mutated, err := toolcontract.Canonicalize(mutatedSource)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(context.Background(), mutated); !errors.Is(err, toolcontract.ErrVersionConflict) {
		t.Fatalf("Register(mutated) error = %v; want ErrVersionConflict", err)
	}

	collisionSource := validManifest()
	collisionSource.ToolID = "other"
	collisionSource.Version = "2.0.0"
	collisionSource.Aliases = []string{"bash"}
	collision, err := toolcontract.Canonicalize(collisionSource)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(context.Background(), collision); !errors.Is(err, toolcontract.ErrAliasConflict) {
		t.Fatalf("Register(collision) error = %v; want ErrAliasConflict", err)
	}
}

func TestRegistryResolutionCannotMutateStoredManifest(t *testing.T) {
	registry := toolcontract.NewMemoryRegistry()
	manifest, err := toolcontract.Canonicalize(validManifest())
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(context.Background(), manifest); err != nil {
		t.Fatal(err)
	}
	first, err := registry.Resolve(context.Background(), toolcontract.Query{Name: "shell", Version: "1.2.0"})
	if err != nil {
		t.Fatal(err)
	}
	first.Manifest.Aliases[0] = "mutated"
	first.Manifest.Inputs[0].Name = "mutated"
	first.Manifest.Compensation.ToolID = "mutated"

	second, err := registry.Resolve(context.Background(), toolcontract.Query{Name: "shell", Version: "1.2.0"})
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(second.Manifest.Aliases, "mutated") || second.Manifest.Inputs[0].Name == "mutated" || second.Manifest.Compensation.ToolID == "mutated" {
		t.Fatalf("stored immutable manifest was changed through resolution: %#v", second.Manifest)
	}
}

func validManifest() toolcontract.Manifest {
	return toolcontract.Manifest{
		SchemaVersion: "tool-contract.v1",
		ToolID:        "shell",
		Version:       "1.2.0",
		Aliases:       []string{"sh", "bash"},
		Inputs: []toolcontract.FieldSpec{
			{Name: "command", Type: "string", Required: true, Sanitizer: toolcontract.SanitizerRedactSecrets},
			{Name: "cwd", Type: "string", Sanitizer: toolcontract.SanitizerRelativePath},
		},
		Outputs: []toolcontract.FieldSpec{
			{Name: "stdout", Type: "string", Sanitizer: toolcontract.SanitizerRedactSecrets},
			{Name: "exit_code", Type: "integer", Sanitizer: toolcontract.SanitizerInteger},
		},
		Reads: []toolcontract.ResourceSpec{
			{Name: "working_tree", Type: "repository", Namespace: "repository", Field: "cwd"},
		},
		Writes: []toolcontract.ResourceSpec{
			{Name: "working_tree", Type: "repository", Namespace: "repository", Field: "cwd"},
		},
		SideEffect:        toolcontract.SideEffectWrite,
		Risk:              toolcontract.RiskMedium,
		Idempotency:       toolcontract.IdempotencySpec{Mode: toolcontract.IdempotencyConditional, KeyField: "command"},
		Retry:             toolcontract.RetrySpec{Mode: toolcontract.RetryOnDeclaredTransient, MaximumAttempts: 3},
		Preconditions:     []toolcontract.PredicateSpec{{ID: "cwd-exists", Resource: "working_tree"}},
		SuccessPredicates: []toolcontract.PredicateSpec{{ID: "exit-zero", Field: "exit_code", Operator: "equals", Value: "0"}},
		Compensation:      &toolcontract.CompensationSpec{ToolID: "shell", PredicateID: "restore-working-tree"},
		VerificationMethods: []toolcontract.VerificationMethod{
			{ID: "exit-zero", EvidenceClass: "tool_postcondition"},
			{ID: "tests-pass", EvidenceClass: "harness_assertion"},
		},
		Compatibility: []toolcontract.CompatibilityRange{{MinimumInclusive: "1.0.0", MaximumExclusive: "2.0.0"}},
	}
}
