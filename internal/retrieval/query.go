package retrieval

import (
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"unicode"

	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/contracts"
	"github.com/sauhard74/mem-jev/internal/domain"
	"golang.org/x/text/unicode/norm"
)

const querySchemaVersion = "retrieval-query.v1"

var ErrInvalidQuery = errors.New("invalid retrieval query")

type RiskClass string

const (
	RiskLow      RiskClass = "low"
	RiskMedium   RiskClass = "medium"
	RiskHigh     RiskClass = "high"
	RiskCritical RiskClass = "critical"
)

type LatencyClass string

const (
	LatencyInteractive LatencyClass = "interactive"
	LatencyStandard    LatencyClass = "standard"
	LatencyBatch       LatencyClass = "batch"
)

type Tool struct {
	Name              string `json:"name"`
	ContractVersionID string `json:"contract_version_id"`
}

type Harness struct {
	Name    string `json:"name"`
	Version string `json:"version,omitempty"`
}

type Fact struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type Resource struct {
	Type          string `json:"type"`
	Namespace     string `json:"namespace,omitempty"`
	Identity      string `json:"identity"`
	SchemaVersion string `json:"schema_version,omitempty"`
}

type Input struct {
	Task             string
	Tools            []Tool
	Harness          Harness
	Environment      []Fact
	Resources        []Resource
	Constraints      []Fact
	ForbiddenEffects []string
	RiskClass        RiskClass
	LatencyClass     LatencyClass
	MaxCandidates    uint32
}

type AliasSet struct {
	Tools     map[string]string
	Resources map[string]string
}

type Query struct {
	SchemaVersion    string          `json:"schema_version"`
	TenantID         domain.TenantID `json:"tenant_id"`
	PolicyVersion    string          `json:"policy_version"`
	Task             string          `json:"task"`
	IntentHash       string          `json:"intent_hash"`
	Tools            []Tool          `json:"tools"`
	Harness          Harness         `json:"harness"`
	Environment      []Fact          `json:"environment,omitempty"`
	EnvironmentHash  string          `json:"environment_hash"`
	Resources        []Resource      `json:"resources,omitempty"`
	Constraints      []Fact          `json:"constraints,omitempty"`
	ForbiddenEffects []string        `json:"forbidden_effects,omitempty"`
	RiskClass        RiskClass       `json:"risk_class"`
	LatencyClass     LatencyClass    `json:"latency_class"`
	MaxCandidates    uint32          `json:"max_candidates"`
	Hash             string          `json:"-"`
	CanonicalJSON    []byte          `json:"-"`
}

func InputFromProto(request *memjevv1.RetrieveRequest) Input {
	if request == nil {
		return Input{}
	}
	input := Input{
		Task: request.GetTask(), RiskClass: riskFromProto(request.GetRiskClass()),
		LatencyClass: latencyFromProto(request.GetLatencyClass()), MaxCandidates: request.GetMaxCandidates(),
		ForbiddenEffects: append([]string(nil), request.GetForbiddenEffects()...),
	}
	if request.GetHarness() != nil {
		input.Harness = Harness{Name: request.GetHarness().GetName(), Version: request.GetHarness().GetVersion()}
	}
	for _, item := range request.GetTools() {
		if item != nil {
			input.Tools = append(input.Tools, Tool{Name: item.GetName(), ContractVersionID: item.GetContractVersionId()})
		}
	}
	input.Environment = factsFromProto(request.GetEnvironment())
	input.Constraints = factsFromProto(request.GetConstraints())
	for _, item := range request.GetResources() {
		if item != nil {
			input.Resources = append(input.Resources, Resource{Type: item.GetType(), Namespace: item.GetNamespace(), Identity: item.GetIdentity(), SchemaVersion: item.GetSchemaVersion()})
		}
	}
	return input
}

func BuildQueryFromProto(tenantID domain.TenantID, policyVersion string, aliases AliasSet, request *memjevv1.RetrieveRequest) (Query, error) {
	if err := contracts.ValidateRetrieval(request); err != nil {
		return Query{}, err
	}
	return BuildQuery(tenantID, policyVersion, aliases, InputFromProto(request))
}

func BuildQuery(tenantID domain.TenantID, policyVersion string, aliases AliasSet, input Input) (Query, error) {
	toolAliases, err := normalizeAliases("tool", aliases.Tools)
	if err != nil {
		return Query{}, err
	}
	resourceAliases, err := normalizeAliases("resource", aliases.Resources)
	if err != nil {
		return Query{}, err
	}
	query := Query{
		SchemaVersion: querySchemaVersion,
		TenantID:      tenantID,
		PolicyVersion: token(policyVersion),
		Task:          text(input.Task),
		Harness:       Harness{Name: token(input.Harness.Name), Version: token(input.Harness.Version)},
		RiskClass:     input.RiskClass,
		LatencyClass:  input.LatencyClass,
		MaxCandidates: input.MaxCandidates,
	}
	if tenantID == "" || query.PolicyVersion == "" || query.Task == "" || query.Harness.Name == "" || input.MaxCandidates < 1 || input.MaxCandidates > 100 || !validRisk(input.RiskClass) || !validLatency(input.LatencyClass) {
		return Query{}, fmt.Errorf("%w: required server context or request field missing", ErrInvalidQuery)
	}
	query.Tools, err = normalizeTools(input.Tools, toolAliases)
	if err != nil {
		return Query{}, err
	}
	query.Environment, err = normalizeFacts("environment", input.Environment)
	if err != nil {
		return Query{}, err
	}
	_, query.EnvironmentHash, err = canonical.MarshalAndHash(query.Environment)
	if err != nil {
		return Query{}, err
	}
	query.Constraints, err = normalizeFacts("constraints", input.Constraints)
	if err != nil {
		return Query{}, err
	}
	query.Resources, err = normalizeResources(input.Resources, resourceAliases)
	if err != nil {
		return Query{}, err
	}
	query.ForbiddenEffects, err = normalizeSet("forbidden_effects", input.ForbiddenEffects)
	if err != nil {
		return Query{}, err
	}
	query.IntentHash, err = CanonicalIntentHash(query.Task, query.Harness)
	if err != nil {
		return Query{}, err
	}
	canonicalJSON, hash, err := canonical.MarshalAndHash(query)
	if err != nil {
		return Query{}, fmt.Errorf("canonicalize retrieval query: %w", err)
	}
	query.Hash, query.CanonicalJSON = hash, canonicalJSON
	return query, nil
}

func CanonicalIntentHash(taskValue string, harnessValue Harness) (string, error) {
	_, hash, err := canonical.MarshalAndHash(struct {
		Task           string `json:"task"`
		Harness        string `json:"harness"`
		HarnessVersion string `json:"harness_version,omitempty"`
	}{text(taskValue), token(harnessValue.Name), token(harnessValue.Version)})
	return hash, err
}

func normalizeAliases(kind string, source map[string]string) (map[string]string, error) {
	result := make(map[string]string, len(source))
	for raw, target := range source {
		key, value := token(raw), token(target)
		if key == "" || value == "" {
			return nil, fmt.Errorf("%w: empty %s alias", ErrInvalidQuery, kind)
		}
		if prior, exists := result[key]; exists && prior != value {
			return nil, fmt.Errorf("%w: conflicting %s alias %q", ErrInvalidQuery, kind, key)
		}
		result[key] = value
	}
	return result, nil
}

func normalizeTools(source []Tool, aliases map[string]string) ([]Tool, error) {
	if len(source) == 0 {
		return nil, fmt.Errorf("%w: tools required", ErrInvalidQuery)
	}
	byName := make(map[string]string, len(source))
	for _, item := range source {
		name, version := token(item.Name), token(item.ContractVersionID)
		if alias, ok := aliases[name]; ok {
			name = alias
		}
		if name == "" || version == "" {
			return nil, fmt.Errorf("%w: tool name and contract version required", ErrInvalidQuery)
		}
		if prior, exists := byName[name]; exists && prior != version {
			return nil, fmt.Errorf("%w: conflicting versions for tool %q", ErrInvalidQuery, name)
		}
		byName[name] = version
	}
	result := make([]Tool, 0, len(byName))
	for name, version := range byName {
		result = append(result, Tool{Name: name, ContractVersionID: version})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

func normalizeFacts(location string, source []Fact) ([]Fact, error) {
	byName := make(map[string]string, len(source))
	for _, item := range source {
		name, value := token(item.Name), text(item.Value)
		if name == "" {
			return nil, fmt.Errorf("%w: %s fact name required", ErrInvalidQuery, location)
		}
		if prior, exists := byName[name]; exists && prior != value {
			return nil, fmt.Errorf("%w: conflicting %s fact %q", ErrInvalidQuery, location, name)
		}
		byName[name] = value
	}
	result := make([]Fact, 0, len(byName))
	for name, value := range byName {
		result = append(result, Fact{Name: name, Value: value})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

func normalizeResources(source []Resource, aliases map[string]string) ([]Resource, error) {
	seen := make(map[string]Resource, len(source))
	for _, item := range source {
		item.Type, item.Namespace, item.SchemaVersion = token(item.Type), token(item.Namespace), token(item.SchemaVersion)
		if alias, ok := aliases[item.Type]; ok {
			item.Type = alias
		}
		identity, err := resourceIdentity(item.Identity)
		if err != nil {
			return nil, err
		}
		item.Identity = identity
		if item.Type == "" || item.Identity == "" {
			return nil, fmt.Errorf("%w: resource type and identity required", ErrInvalidQuery)
		}
		key := strings.Join([]string{item.Type, item.Namespace, item.Identity, item.SchemaVersion}, "\x00")
		seen[key] = item
	}
	result := make([]Resource, 0, len(seen))
	for _, item := range seen {
		result = append(result, item)
	}
	sort.Slice(result, func(i, j int) bool {
		left := strings.Join([]string{result[i].Type, result[i].Namespace, result[i].Identity, result[i].SchemaVersion}, "\x00")
		right := strings.Join([]string{result[j].Type, result[j].Namespace, result[j].Identity, result[j].SchemaVersion}, "\x00")
		return left < right
	})
	return result, nil
}

func normalizeSet(location string, source []string) ([]string, error) {
	seen := make(map[string]struct{}, len(source))
	for _, value := range source {
		value = token(value)
		if value == "" {
			return nil, fmt.Errorf("%w: %s contains empty value", ErrInvalidQuery, location)
		}
		seen[value] = struct{}{}
	}
	result := make([]string, 0, len(seen))
	for value := range seen {
		result = append(result, value)
	}
	sort.Strings(result)
	return result, nil
}

func resourceIdentity(value string) (string, error) {
	value = strings.ReplaceAll(token(value), "\\", "/")
	if value == "" || strings.HasPrefix(value, "/") || (len(value) > 1 && value[1] == ':') {
		return "", fmt.Errorf("%w: resource identity must be relative", ErrInvalidQuery)
	}
	cleaned := path.Clean(value)
	if cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", fmt.Errorf("%w: resource identity escapes its root", ErrInvalidQuery)
	}
	return cleaned, nil
}

func factsFromProto(source []*memjevv1.QueryFact) []Fact {
	result := make([]Fact, 0, len(source))
	for _, item := range source {
		if item != nil {
			result = append(result, Fact{Name: item.GetName(), Value: item.GetValue()})
		}
	}
	return result
}

func riskFromProto(value memjevv1.RiskClass) RiskClass {
	return map[memjevv1.RiskClass]RiskClass{
		memjevv1.RiskClass_RISK_CLASS_LOW: RiskLow, memjevv1.RiskClass_RISK_CLASS_MEDIUM: RiskMedium,
		memjevv1.RiskClass_RISK_CLASS_HIGH: RiskHigh, memjevv1.RiskClass_RISK_CLASS_CRITICAL: RiskCritical,
	}[value]
}

func latencyFromProto(value memjevv1.LatencyClass) LatencyClass {
	return map[memjevv1.LatencyClass]LatencyClass{
		memjevv1.LatencyClass_LATENCY_CLASS_INTERACTIVE: LatencyInteractive,
		memjevv1.LatencyClass_LATENCY_CLASS_STANDARD:    LatencyStandard,
		memjevv1.LatencyClass_LATENCY_CLASS_BATCH:       LatencyBatch,
	}[value]
}

func validRisk(value RiskClass) bool {
	return value == RiskLow || value == RiskMedium || value == RiskHigh || value == RiskCritical
}
func validLatency(value LatencyClass) bool {
	return value == LatencyInteractive || value == LatencyStandard || value == LatencyBatch
}
func token(value string) string { return norm.NFC.String(strings.TrimSpace(value)) }
func text(value string) string {
	value = norm.NFC.String(strings.ReplaceAll(value, "\r\n", "\n"))
	return strings.TrimFunc(value, unicode.IsSpace)
}
