package eligibility_test

import (
	"slices"
	"testing"

	"github.com/sauhard74/mem-jev/internal/eligibility"
)

func TestEvaluateAcceptsActiveCompatibleCandidate(t *testing.T) {
	decision := eligibility.Evaluate(validPolicy(), validContext(), validCandidate())
	if !decision.Eligible || decision.AdvisoryOnly || len(decision.Rejections) != 0 {
		t.Fatalf("decision = %#v", decision)
	}
}

func TestEvaluateEveryHardGateFailsClosed(t *testing.T) {
	tests := []struct {
		name   string
		code   eligibility.ReasonCode
		mutate func(*eligibility.Context, *eligibility.Candidate)
	}{
		{name: "tenant", code: eligibility.ReasonTenantScopeDenied, mutate: func(_ *eligibility.Context, c *eligibility.Candidate) { c.OwnerTenantID = "tenant-b" }},
		{name: "sharing", code: eligibility.ReasonSharingScopeDenied, mutate: func(_ *eligibility.Context, c *eligibility.Candidate) {
			c.OwnerTenantID = "tenant-b"
			c.SharedTenantIDs = []string{"tenant-c"}
		}},
		{name: "lifecycle", code: eligibility.ReasonLifecycleIneligible, mutate: func(_ *eligibility.Context, c *eligibility.Candidate) { c.Lifecycle = eligibility.LifecycleStale }},
		{name: "superseded", code: eligibility.ReasonSuperseded, mutate: func(_ *eligibility.Context, c *eligibility.Candidate) { c.SupersededBy = "pv_new" }},
		{name: "tool", code: eligibility.ReasonToolUnavailable, mutate: func(ctx *eligibility.Context, _ *eligibility.Candidate) { ctx.Tools = nil }},
		{name: "contract", code: eligibility.ReasonToolContractIncompatible, mutate: func(ctx *eligibility.Context, _ *eligibility.Candidate) { ctx.Tools[0].ContractVersionID = "tc_2" }},
		{name: "resource", code: eligibility.ReasonResourcePreconditionMissing, mutate: func(ctx *eligibility.Context, _ *eligibility.Candidate) { ctx.Resources = nil }},
		{name: "schema", code: eligibility.ReasonResourceSchemaIncompatible, mutate: func(ctx *eligibility.Context, _ *eligibility.Candidate) { ctx.Resources[0].SchemaVersion = "schema.v2" }},
		{name: "forbidden effect", code: eligibility.ReasonForbiddenEffect, mutate: func(ctx *eligibility.Context, _ *eligibility.Candidate) {
			ctx.ForbiddenEffects = []string{"filesystem.write"}
		}},
		{name: "risk", code: eligibility.ReasonRiskBudgetExceeded, mutate: func(_ *eligibility.Context, c *eligibility.Candidate) { c.Risk = eligibility.RiskHigh }},
		{name: "environment", code: eligibility.ReasonEnvironmentIncompatible, mutate: func(ctx *eligibility.Context, _ *eligibility.Candidate) { ctx.Environment[0].Value = "darwin" }},
		{name: "harness", code: eligibility.ReasonHarnessIncompatible, mutate: func(ctx *eligibility.Context, _ *eligibility.Candidate) {
			ctx.Harness = eligibility.Harness{Name: "other", Version: "1"}
		}},
		{name: "validation missing", code: eligibility.ReasonValidationMissing, mutate: func(_ *eligibility.Context, c *eligibility.Candidate) { c.ValidatedAtUnix = 0 }},
		{name: "validation stale", code: eligibility.ReasonValidationStale, mutate: func(_ *eligibility.Context, c *eligibility.Candidate) { c.ValidatedAtUnix = 1 }},
		{name: "policy", code: eligibility.ReasonPolicyIncompatible, mutate: func(_ *eligibility.Context, c *eligibility.Candidate) { c.ValidationPolicyVersion = "other" }},
		{name: "consent", code: eligibility.ReasonConsentDenied, mutate: func(_ *eligibility.Context, c *eligibility.Candidate) { c.RecallAllowed = false }},
		{name: "residency", code: eligibility.ReasonResidencyDenied, mutate: func(_ *eligibility.Context, c *eligibility.Candidate) { c.ResidencyRegion = "us" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, candidate := validContext(), validCandidate()
			tt.mutate(&ctx, &candidate)
			decision := eligibility.Evaluate(validPolicy(), ctx, candidate)
			if decision.Eligible || !hasReason(decision, tt.code) {
				t.Fatalf("decision = %#v; want %s", decision, tt.code)
			}
		})
	}
}

func TestEvaluateUnknownRequiredFactsFailClosed(t *testing.T) {
	ctx, candidate := validContext(), validCandidate()
	candidate.RequiredEnvironment = append(candidate.RequiredEnvironment, eligibility.Fact{Name: "gpu", Value: "required"})
	candidate.RequiredResources = append(candidate.RequiredResources, eligibility.Resource{Type: "secret", IdentityHash: "sha256:missing", SchemaVersion: "v1"})
	decision := eligibility.Evaluate(validPolicy(), ctx, candidate)
	if decision.Eligible || !hasReason(decision, eligibility.ReasonEnvironmentIncompatible) || !hasReason(decision, eligibility.ReasonResourcePreconditionMissing) {
		t.Fatalf("decision = %#v", decision)
	}
}

func TestEvaluateMissingServerAndCandidateFactsFailClosed(t *testing.T) {
	ctx, candidate := validContext(), validCandidate()
	ctx.TenantID = ""
	ctx.AsOfUnix = 0
	candidate.VersionID = ""
	candidate.ResidencyRegion = ""
	decision := eligibility.Evaluate(validPolicy(), ctx, candidate)
	if decision.Eligible || !hasReason(decision, eligibility.ReasonContextInvalid) || !hasReason(decision, eligibility.ReasonCandidateInvalid) {
		t.Fatalf("decision = %#v", decision)
	}
}

func TestCandidateLifecycleIsAdvisoryOnlyWhenExplicitlyAllowed(t *testing.T) {
	candidate := validCandidate()
	candidate.Lifecycle = eligibility.LifecycleCandidate
	denied := eligibility.Evaluate(validPolicy(), validContext(), candidate)
	if denied.Eligible || !hasReason(denied, eligibility.ReasonLifecycleIneligible) {
		t.Fatalf("default candidate decision = %#v", denied)
	}
	policy := validPolicy()
	policy.AllowAdvisoryCandidates = true
	allowed := eligibility.Evaluate(policy, validContext(), candidate)
	if !allowed.Eligible || !allowed.AdvisoryOnly {
		t.Fatalf("advisory decision = %#v", allowed)
	}
}

func TestRejectionsAreCanonicalAndDoNotExposeResourceIdentity(t *testing.T) {
	ctx, candidate := validContext(), validCandidate()
	ctx.Resources = nil
	ctx.Tools = nil
	candidate.Lifecycle = eligibility.LifecycleRetired
	decision := eligibility.Evaluate(validPolicy(), ctx, candidate)
	if decision.Eligible {
		t.Fatal("decision unexpectedly eligible")
	}
	codes := make([]string, len(decision.Rejections))
	for index, rejection := range decision.Rejections {
		codes[index] = string(rejection.Code)
		if rejection.Observed == candidate.RequiredResources[0].IdentityHash || rejection.Expected == candidate.RequiredResources[0].IdentityHash {
			t.Fatalf("resource identity hash exposed: %#v", rejection)
		}
	}
	if !slices.IsSorted(codes) {
		t.Fatalf("reason codes not sorted: %v", codes)
	}
	permuted := candidate
	permuted.RequiredTools = slices.Clone(candidate.RequiredTools)
	slices.Reverse(permuted.RequiredTools)
	other := eligibility.Evaluate(validPolicy(), ctx, permuted)
	if len(other.Rejections) != len(decision.Rejections) {
		t.Fatalf("permutation changed result: %#v %#v", decision, other)
	}
	for index := range decision.Rejections {
		if decision.Rejections[index] != other.Rejections[index] {
			t.Fatalf("permutation changed reasons: %#v %#v", decision, other)
		}
	}
}

func TestPolicyVersionAndRulesChangeIdentity(t *testing.T) {
	left, err := eligibility.NewPolicy(validPolicy())
	if err != nil {
		t.Fatal(err)
	}
	rightSpec := validPolicy()
	rightSpec.Version = "eligibility.v2"
	right, err := eligibility.NewPolicy(rightSpec)
	if err != nil {
		t.Fatal(err)
	}
	if left.ID == right.ID || len(left.CanonicalJSON) == 0 {
		t.Fatalf("policy identities = %q %q", left.ID, right.ID)
	}
}

func validPolicy() eligibility.PolicySpec {
	return eligibility.PolicySpec{
		Version: "eligibility.v1", AllowedLifecycle: []eligibility.Lifecycle{eligibility.LifecycleActive, eligibility.LifecycleTrial},
		MaximumRisk: eligibility.RiskMedium, MaximumValidationAgeSeconds: 3600,
		CompatibleValidationPolicies: []string{"evidence.v1"}, AllowedResidencyRegions: []string{"eu"},
	}
}

func validContext() eligibility.Context {
	return eligibility.Context{
		TenantID: "tenant-a", AsOfUnix: 10_000, RecallAllowed: true,
		Tools:       []eligibility.Tool{{Name: "shell", ContractVersionID: "tc_1"}},
		Harness:     eligibility.Harness{Name: "codex", Version: "1"},
		Environment: []eligibility.Fact{{Name: "os", Value: "linux"}},
		Resources:   []eligibility.Resource{{Type: "repository", Namespace: "git", IdentityHash: "sha256:repo", SchemaVersion: "schema.v1"}},
		MaximumRisk: eligibility.RiskMedium, AllowedResidencyRegions: []string{"eu"},
	}
}

func validCandidate() eligibility.Candidate {
	return eligibility.Candidate{
		VersionID: "pv_1", OwnerTenantID: "tenant-a", Lifecycle: eligibility.LifecycleActive,
		RequiredTools:       []eligibility.Tool{{Name: "shell", ContractVersionID: "tc_1"}},
		RequiredHarness:     eligibility.Harness{Name: "codex", Version: "1"},
		RequiredEnvironment: []eligibility.Fact{{Name: "os", Value: "linux"}},
		RequiredResources:   []eligibility.Resource{{Type: "repository", Namespace: "git", IdentityHash: "sha256:repo", SchemaVersion: "schema.v1"}},
		Effects:             []string{"filesystem.write"}, Risk: eligibility.RiskMedium,
		ValidatedAtUnix: 9_000, ValidationPolicyVersion: "evidence.v1", RecallAllowed: true, ResidencyRegion: "eu",
	}
}

func hasReason(decision eligibility.Decision, code eligibility.ReasonCode) bool {
	return slices.ContainsFunc(decision.Rejections, func(reason eligibility.Rejection) bool { return reason.Code == code })
}
