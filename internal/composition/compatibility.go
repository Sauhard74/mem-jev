package composition

import (
	"context"
	"sort"
	"strings"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
)

const maximumCompatibilityRules = 4096

var defaultCompatibilityLimits = CompatibilityLimits{
	MaximumCandidates: 512, MaximumInterfaceEntries: 128, MaximumTotalInterfaceEntries: 16_384,
	MaximumProvidersPerResource: 128, MaximumEdges: 65_536, MaximumMatchComparisons: 1_000_000,
}

func BuildCompatibilityGraph(request GraphRequest) (CompatibilityGraph, error) {
	return BuildCompatibilityGraphContext(context.Background(), request)
}

func BuildCompatibilityGraphContext(ctx context.Context, request GraphRequest) (CompatibilityGraph, error) {
	if err := ctx.Err(); err != nil {
		return CompatibilityGraph{}, err
	}
	if request.TenantID == "" || request.ProjectionEpoch == 0 || strings.TrimSpace(request.PlannerManifestID) == "" || strings.TrimSpace(request.PolicyManifestID) == "" || ValidateCompatibilityMatrix(request.Matrix) != nil {
		return CompatibilityGraph{}, ErrInvalidCompatibilityInput
	}
	rules := append([]SchemaCompatibility(nil), request.Matrix.Rules...)
	candidates := append([]Candidate(nil), request.Candidates...)
	if len(candidates) > int(request.Matrix.Limits.MaximumCandidates) {
		return CompatibilityGraph{}, ErrInvalidCompatibilityInput
	}
	seenVersions := make(map[string]struct{}, len(candidates))
	totalInterfaceEntries := 0
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return CompatibilityGraph{}, err
		}
		entries := len(candidate.Interface.Requirements) + len(candidate.Interface.Provisions)
		totalInterfaceEntries += entries
		if !validCandidate(candidate, request.TenantID) || entries > int(request.Matrix.Limits.MaximumInterfaceEntries) || totalInterfaceEntries > int(request.Matrix.Limits.MaximumTotalInterfaceEntries) {
			return CompatibilityGraph{}, ErrInvalidCompatibilityInput
		}
		if _, duplicate := seenVersions[candidate.ProcedureVersionID]; duplicate {
			return CompatibilityGraph{}, ErrInvalidCompatibilityInput
		}
		seenVersions[candidate.ProcedureVersionID] = struct{}{}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].ProcedureVersionID < candidates[j].ProcedureVersionID })
	candidateIdentity := make([]struct {
		VersionID     string `json:"procedure_version_id"`
		InterfaceHash string `json:"interface_hash"`
		Lifecycle     string `json:"lifecycle"`
	}, len(candidates))
	for index, candidate := range candidates {
		candidateIdentity[index].VersionID, candidateIdentity[index].InterfaceHash, candidateIdentity[index].Lifecycle = candidate.ProcedureVersionID, candidate.Interface.ContentHash, candidate.Lifecycle
	}
	_, candidateSetHash, err := canonical.MarshalAndHash(candidateIdentity)
	if err != nil {
		return CompatibilityGraph{}, err
	}
	graph := CompatibilityGraph{
		SchemaVersion: "compatibility-graph.v1", TenantID: request.TenantID, ProjectionEpoch: request.ProjectionEpoch,
		PlannerManifestID: strings.TrimSpace(request.PlannerManifestID), PolicyManifestID: strings.TrimSpace(request.PolicyManifestID), MatrixHash: request.Matrix.ContentHash, CandidateSetHash: candidateSetHash,
	}
	matches, err := indexCompatibilityMatches(ctx, candidates, rules, request.Matrix.Limits)
	if err != nil {
		return CompatibilityGraph{}, err
	}
	pairKeys := make([]versionPair, 0, len(matches))
	for key := range matches {
		if len(pairKeys)%256 == 0 {
			if err := ctx.Err(); err != nil {
				return CompatibilityGraph{}, err
			}
		}
		pairKeys = append(pairKeys, key)
	}
	sort.Slice(pairKeys, func(i, j int) bool {
		return pairKeys[i].source < pairKeys[j].source || pairKeys[i].source == pairKeys[j].source && pairKeys[i].target < pairKeys[j].target
	})
	if err := ctx.Err(); err != nil {
		return CompatibilityGraph{}, err
	}
	for index, key := range pairKeys {
		if index%256 == 0 {
			if err := ctx.Err(); err != nil {
				return CompatibilityGraph{}, err
			}
		}
		match := matches[key]
		provisionIDs, requirementIDs := sortedKeys(match.provisionIDs), sortedKeys(match.requirementIDs)
		edge := CompatibilityEdge{
			TenantID: request.TenantID, ProjectionEpoch: request.ProjectionEpoch, SourceVersionID: match.source.ProcedureVersionID, TargetVersionID: match.target.ProcedureVersionID,
			SourceInterfaceHash: match.source.Interface.ContentHash, TargetInterfaceHash: match.target.Interface.ContentHash,
			SourceProvisionIDs: provisionIDs, SatisfiedRequirementIDs: requirementIDs,
			PlannerManifestID: graph.PlannerManifestID, PolicyManifestID: graph.PolicyManifestID, SchemaVersion: "compatibility-edge.v1",
		}
		_, hash, hashErr := canonical.MarshalAndHash(edge)
		if hashErr != nil {
			return CompatibilityGraph{}, hashErr
		}
		edge.ID, edge.ContentHash = "cedge_"+hash, hash
		graph.Edges = append(graph.Edges, edge)
	}
	sort.Slice(graph.Edges, func(i, j int) bool { return graph.Edges[i].ID < graph.Edges[j].ID })
	if err := ctx.Err(); err != nil {
		return CompatibilityGraph{}, err
	}
	identity := graph
	identity.ID, identity.ContentHash, identity.CanonicalJSON = "", "", nil
	canonicalJSON, hash, err := canonical.MarshalAndHash(identity)
	if err != nil {
		return CompatibilityGraph{}, err
	}
	if err := ctx.Err(); err != nil {
		return CompatibilityGraph{}, err
	}
	graph.ID, graph.ContentHash, graph.CanonicalJSON = "cgraph_"+hash, hash, canonicalJSON
	return graph, nil
}

func BuildCompatibilityMatrix(source []SchemaCompatibility) (CompatibilityMatrix, error) {
	return BuildCompatibilityMatrixWithLimits(source, defaultCompatibilityLimits)
}

func BuildCompatibilityMatrixWithLimits(source []SchemaCompatibility, limits CompatibilityLimits) (CompatibilityMatrix, error) {
	if !validCompatibilityLimits(limits) {
		return CompatibilityMatrix{}, ErrInvalidCompatibilityInput
	}
	rules, _, err := normalizeCompatibility(source)
	if err != nil {
		return CompatibilityMatrix{}, err
	}
	matrix := CompatibilityMatrix{SchemaVersion: "compatibility-matrix.v1", Rules: rules, Limits: limits}
	canonicalJSON, hash, err := canonical.MarshalAndHash(matrix)
	if err != nil {
		return CompatibilityMatrix{}, err
	}
	matrix.ID, matrix.ContentHash, matrix.CanonicalJSON = "cmat_"+hash, hash, canonicalJSON
	return matrix, nil
}

func normalizeCompatibility(source []SchemaCompatibility) ([]SchemaCompatibility, string, error) {
	if len(source) > maximumCompatibilityRules {
		return nil, "", ErrInvalidCompatibilityInput
	}
	rules := append([]SchemaCompatibility(nil), source...)
	for index := range rules {
		rules[index].ResourceType = strings.TrimSpace(rules[index].ResourceType)
		rules[index].Namespace = strings.TrimSpace(rules[index].Namespace)
		rules[index].ProvidedSchema = strings.TrimSpace(rules[index].ProvidedSchema)
		rules[index].RequiredSchema = strings.TrimSpace(rules[index].RequiredSchema)
		if rules[index].ResourceType == "" || rules[index].Namespace == "" || rules[index].ProvidedSchema == "" || rules[index].RequiredSchema == "" ||
			strings.ContainsAny(rules[index].ResourceType+rules[index].Namespace+rules[index].ProvidedSchema+rules[index].RequiredSchema, "*") ||
			strings.ContainsRune(rules[index].ResourceType, '\x00') || strings.ContainsRune(rules[index].Namespace, '\x00') || strings.ContainsRune(rules[index].ProvidedSchema, '\x00') || strings.ContainsRune(rules[index].RequiredSchema, '\x00') {
			return nil, "", ErrInvalidCompatibilityInput
		}
	}
	sort.Slice(rules, func(i, j int) bool { return compareCompatibility(rules[i], rules[j]) < 0 })
	for index := 1; index < len(rules); index++ {
		if compareCompatibility(rules[index-1], rules[index]) == 0 {
			return nil, "", ErrInvalidCompatibilityInput
		}
	}
	_, hash, err := canonical.MarshalAndHash(rules)
	return rules, hash, err
}

type provisionReference struct {
	source    Candidate
	provision domain.ProcedureProvision
}

type resourceIdentity struct {
	resourceType string
	namespace    string
	identityHash string
}

type versionPair struct {
	source string
	target string
}

type compatibilityMatch struct {
	source         Candidate
	target         Candidate
	provisionIDs   map[string]struct{}
	requirementIDs map[string]struct{}
}

func indexCompatibilityMatches(ctx context.Context, candidates []Candidate, rules []SchemaCompatibility, limits CompatibilityLimits) (map[versionPair]*compatibilityMatch, error) {
	byResource := make(map[resourceIdentity][]provisionReference)
	for _, source := range candidates {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for _, provision := range source.Interface.Provisions {
			if provision.SchemaVersion == "" {
				continue
			}
			key := resourceIdentity{provision.ResourceType, provision.Namespace, provision.IdentityHash}
			byResource[key] = append(byResource[key], provisionReference{source: source, provision: provision})
			if len(byResource[key]) > int(limits.MaximumProvidersPerResource) {
				return nil, ErrInvalidCompatibilityInput
			}
		}
	}
	matches := make(map[versionPair]*compatibilityMatch)
	comparisons := uint64(0)
	for _, target := range candidates {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for _, requirement := range target.Interface.Requirements {
			if requirement.SchemaVersion == "" {
				continue
			}
			key := resourceIdentity{requirement.ResourceType, requirement.Namespace, requirement.IdentityHash}
			for _, reference := range byResource[key] {
				comparisons++
				if comparisons%256 == 0 {
					if err := ctx.Err(); err != nil {
						return nil, err
					}
				}
				if comparisons > limits.MaximumMatchComparisons {
					return nil, ErrInvalidCompatibilityInput
				}
				if reference.source.ProcedureVersionID == target.ProcedureVersionID || reference.provision.SchemaVersion != requirement.SchemaVersion && !explicitlyCompatible(reference.provision, requirement, rules) || !predicateGuaranteesCover(reference.provision.SuccessPredicateIDs, requirement.PredicateIDs) {
					continue
				}
				pair := versionPair{reference.source.ProcedureVersionID, target.ProcedureVersionID}
				match := matches[pair]
				if match == nil {
					if len(matches) >= int(limits.MaximumEdges) {
						return nil, ErrInvalidCompatibilityInput
					}
					match = &compatibilityMatch{source: reference.source, target: target, provisionIDs: make(map[string]struct{}), requirementIDs: make(map[string]struct{})}
					matches[pair] = match
				}
				match.provisionIDs[reference.provision.ID] = struct{}{}
				match.requirementIDs[requirement.ID] = struct{}{}
			}
		}
	}
	return matches, nil
}

func sortedKeys(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func explicitlyCompatible(provision domain.ProcedureProvision, requirement domain.ProcedureRequirement, rules []SchemaCompatibility) bool {
	key := SchemaCompatibility{ResourceType: provision.ResourceType, Namespace: provision.Namespace, ProvidedSchema: provision.SchemaVersion, RequiredSchema: requirement.SchemaVersion}
	index := sort.Search(len(rules), func(index int) bool { return compareCompatibility(rules[index], key) >= 0 })
	return index < len(rules) && compareCompatibility(rules[index], key) == 0
}

func compareCompatibility(left, right SchemaCompatibility) int {
	leftFields := [...]string{left.ResourceType, left.Namespace, left.ProvidedSchema, left.RequiredSchema}
	rightFields := [...]string{right.ResourceType, right.Namespace, right.ProvidedSchema, right.RequiredSchema}
	for index := range leftFields {
		if leftFields[index] < rightFields[index] {
			return -1
		}
		if leftFields[index] > rightFields[index] {
			return 1
		}
	}
	return 0
}

func predicateGuaranteesCover(provided, required []string) bool {
	providedIndex := 0
	for _, predicate := range required {
		for providedIndex < len(provided) && provided[providedIndex] < predicate {
			providedIndex++
		}
		if providedIndex == len(provided) || provided[providedIndex] != predicate {
			return false
		}
	}
	return true
}
