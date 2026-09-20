package eligibility

import (
	"fmt"
	"slices"
	"sort"
	"strings"
)

type ReasonCode string

const (
	ReasonPolicyInvalid               ReasonCode = "policy_invalid"
	ReasonContextInvalid              ReasonCode = "context_invalid"
	ReasonCandidateInvalid            ReasonCode = "candidate_invalid"
	ReasonTenantScopeDenied           ReasonCode = "tenant_scope_denied"
	ReasonSharingScopeDenied          ReasonCode = "sharing_scope_denied"
	ReasonLifecycleIneligible         ReasonCode = "lifecycle_ineligible"
	ReasonSuperseded                  ReasonCode = "superseded"
	ReasonToolUnavailable             ReasonCode = "tool_unavailable"
	ReasonToolContractIncompatible    ReasonCode = "tool_contract_incompatible"
	ReasonResourcePreconditionMissing ReasonCode = "resource_precondition_missing"
	ReasonResourceSchemaIncompatible  ReasonCode = "resource_schema_incompatible"
	ReasonForbiddenEffect             ReasonCode = "forbidden_effect"
	ReasonRiskBudgetExceeded          ReasonCode = "risk_budget_exceeded"
	ReasonEnvironmentIncompatible     ReasonCode = "environment_incompatible"
	ReasonHarnessIncompatible         ReasonCode = "harness_incompatible"
	ReasonValidationMissing           ReasonCode = "validation_missing"
	ReasonValidationStale             ReasonCode = "validation_stale"
	ReasonPolicyIncompatible          ReasonCode = "policy_incompatible"
	ReasonConsentDenied               ReasonCode = "consent_denied"
	ReasonResidencyDenied             ReasonCode = "residency_denied"
)

type Tool struct {
	Name              string
	ContractVersionID string
}

type Harness struct {
	Name    string
	Version string
}

type Fact struct {
	Name  string
	Value string
}

type Resource struct {
	Type          string
	Namespace     string
	IdentityHash  string
	SchemaVersion string
}

type Context struct {
	TenantID                string
	AsOfUnix                int64
	Tools                   []Tool
	Harness                 Harness
	Environment             []Fact
	Resources               []Resource
	ForbiddenEffects        []string
	MaximumRisk             Risk
	AllowedResidencyRegions []string
}

type Candidate struct {
	VersionID               string
	OwnerTenantID           string
	SharedTenantIDs         []string
	Lifecycle               Lifecycle
	SupersededBy            string
	RequiredTools           []Tool
	RequiredHarness         Harness
	RequiredEnvironment     []Fact
	RequiredResources       []Resource
	Effects                 []string
	Risk                    Risk
	ValidatedAtUnix         int64
	ValidationPolicyVersion string
	RecallAllowed           bool
	ResidencyRegion         string
}

type Rejection struct {
	Code      ReasonCode
	SubjectID string
	Field     string
	Expected  string
	Observed  string
}

type Decision struct {
	Eligible     bool
	AdvisoryOnly bool
	PolicyID     string
	Rejections   []Rejection
}

func Evaluate(policySpec PolicySpec, context Context, candidate Candidate) Decision {
	policy, err := NewPolicy(policySpec)
	if err != nil {
		return Decision{Rejections: []Rejection{{Code: ReasonPolicyInvalid, Field: "policy", Expected: "valid", Observed: "invalid"}}}
	}
	decision := Decision{PolicyID: policy.ID}
	reject := func(code ReasonCode, subject, field, expected, observed string) {
		decision.Rejections = append(decision.Rejections, Rejection{Code: code, SubjectID: subject, Field: field, Expected: expected, Observed: observed})
	}
	if context.TenantID == "" || context.AsOfUnix <= 0 || !validRisk(context.MaximumRisk) {
		reject(ReasonContextInvalid, "request", "server_context", "complete", "invalid")
	}
	if candidate.VersionID == "" || candidate.OwnerTenantID == "" || candidate.Lifecycle == "" || !validRisk(candidate.Risk) || candidate.ResidencyRegion == "" {
		reject(ReasonCandidateInvalid, "candidate", "metadata", "complete", "invalid")
	}

	if candidate.OwnerTenantID != context.TenantID {
		if len(candidate.SharedTenantIDs) == 0 {
			reject(ReasonTenantScopeDenied, candidate.VersionID, "tenant_scope", "owner_or_explicit_share", "different_owner")
		} else if !contains(candidate.SharedTenantIDs, context.TenantID) {
			reject(ReasonSharingScopeDenied, candidate.VersionID, "sharing_scope", "tenant_grant", "grant_absent")
		}
	}
	allowedLifecycle := slices.Contains(policy.AllowedLifecycle, candidate.Lifecycle)
	if candidate.Lifecycle == LifecycleCandidate {
		allowedLifecycle = allowedLifecycle || policy.AllowAdvisoryCandidates
		decision.AdvisoryOnly = allowedLifecycle
	}
	if !allowedLifecycle {
		reject(ReasonLifecycleIneligible, candidate.VersionID, "lifecycle", "active_or_trial", string(candidate.Lifecycle))
	}
	if candidate.SupersededBy != "" || candidate.Lifecycle == LifecycleSuperseded {
		reject(ReasonSuperseded, candidate.VersionID, "supersession", "current", "superseded")
	}

	availableTools := toolMap(context.Tools)
	for _, required := range candidate.RequiredTools {
		version, exists := availableTools[required.Name]
		if !exists {
			reject(ReasonToolUnavailable, required.Name, "tool", "available", "missing")
		} else if version != required.ContractVersionID {
			reject(ReasonToolContractIncompatible, required.Name, "contract_version", "compatible", "incompatible")
		}
	}

	availableResources := resourceMap(context.Resources)
	for _, required := range candidate.RequiredResources {
		key := resourceKey(required)
		available, exists := availableResources[key]
		subject := strings.Join([]string{required.Type, required.Namespace}, ":")
		if !exists {
			reject(ReasonResourcePreconditionMissing, subject, "resource", "present", "missing")
		} else if required.SchemaVersion != "" && available.SchemaVersion != required.SchemaVersion {
			reject(ReasonResourceSchemaIncompatible, subject, "schema_version", "compatible", "incompatible")
		}
	}

	forbidden := stringSet(context.ForbiddenEffects)
	for _, effect := range candidate.Effects {
		if forbidden[effect] {
			reject(ReasonForbiddenEffect, effect, "effect", "allowed", "forbidden")
		}
	}
	effectiveRisk := context.MaximumRisk
	if !validRisk(effectiveRisk) || policy.MaximumRisk < effectiveRisk {
		effectiveRisk = policy.MaximumRisk
	}
	if !validRisk(candidate.Risk) || candidate.Risk > effectiveRisk {
		reject(ReasonRiskBudgetExceeded, candidate.VersionID, "risk", riskName(effectiveRisk), riskName(candidate.Risk))
	}

	environment := factMap(context.Environment)
	for _, required := range candidate.RequiredEnvironment {
		if value, exists := environment[required.Name]; !exists || value != required.Value {
			reject(ReasonEnvironmentIncompatible, required.Name, "environment", "compatible", "missing_or_different")
		}
	}
	if candidate.RequiredHarness.Name != "" && (candidate.RequiredHarness.Name != context.Harness.Name || (candidate.RequiredHarness.Version != "" && candidate.RequiredHarness.Version != context.Harness.Version)) {
		reject(ReasonHarnessIncompatible, candidate.RequiredHarness.Name, "harness", "compatible", "missing_or_different")
	}

	if candidate.ValidatedAtUnix <= 0 {
		reject(ReasonValidationMissing, candidate.VersionID, "validation", "present", "missing")
	} else if context.AsOfUnix <= 0 || candidate.ValidatedAtUnix > context.AsOfUnix || context.AsOfUnix-candidate.ValidatedAtUnix > policy.MaximumValidationAgeSeconds {
		reject(ReasonValidationStale, candidate.VersionID, "validation_age", "within_policy", "stale_or_future")
	}
	if !contains(policy.CompatibleValidationPolicies, candidate.ValidationPolicyVersion) {
		reject(ReasonPolicyIncompatible, candidate.VersionID, "validation_policy", "compatible", "incompatible")
	}
	if !candidate.RecallAllowed {
		reject(ReasonConsentDenied, candidate.VersionID, "recall_consent", "allowed", "denied")
	}
	if !contains(policy.AllowedResidencyRegions, candidate.ResidencyRegion) || !contains(context.AllowedResidencyRegions, candidate.ResidencyRegion) {
		reject(ReasonResidencyDenied, candidate.VersionID, "residency", "allowed_region", "denied_or_unknown")
	}

	sort.Slice(decision.Rejections, func(i, j int) bool {
		left, right := decision.Rejections[i], decision.Rejections[j]
		return fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%s", left.Code, left.SubjectID, left.Field, left.Expected, left.Observed) <
			fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%s", right.Code, right.SubjectID, right.Field, right.Expected, right.Observed)
	})
	decision.Eligible = len(decision.Rejections) == 0
	if !decision.Eligible {
		decision.AdvisoryOnly = false
	}
	return decision
}

func toolMap(source []Tool) map[string]string {
	result := make(map[string]string, len(source))
	for _, tool := range source {
		if prior, exists := result[tool.Name]; exists && prior != tool.ContractVersionID {
			result[tool.Name] = ""
		} else {
			result[tool.Name] = tool.ContractVersionID
		}
	}
	return result
}

func factMap(source []Fact) map[string]string {
	result := make(map[string]string, len(source))
	for _, fact := range source {
		if prior, exists := result[fact.Name]; exists && prior != fact.Value {
			result[fact.Name] = ""
		} else {
			result[fact.Name] = fact.Value
		}
	}
	return result
}

func resourceMap(source []Resource) map[string]Resource {
	result := make(map[string]Resource, len(source))
	for _, resource := range source {
		key := resourceKey(resource)
		if prior, exists := result[key]; exists && prior.SchemaVersion != resource.SchemaVersion {
			resource.SchemaVersion = ""
		}
		result[key] = resource
	}
	return result
}

func resourceKey(resource Resource) string {
	return strings.Join([]string{resource.Type, resource.Namespace, resource.IdentityHash}, "\x00")
}
func stringSet(source []string) map[string]bool {
	result := make(map[string]bool, len(source))
	for _, value := range source {
		result[value] = true
	}
	return result
}
func contains(source []string, target string) bool { return slices.Contains(source, target) }
func riskName(value Risk) string {
	return map[Risk]string{RiskLow: "low", RiskMedium: "medium", RiskHigh: "high", RiskCritical: "critical"}[value]
}
