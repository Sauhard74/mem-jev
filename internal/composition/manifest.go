package composition

import (
	"bytes"
	"errors"
	"math"
	"strings"

	"github.com/sauhard74/mem-jev/internal/canonical"
)

var ErrInvalidPlannerManifest = errors.New("invalid planner manifest")

type PlannerManifestSpec struct {
	Version                      string `json:"version"`
	BeamWidth                    uint32 `json:"beam_width"`
	MaximumCandidates            uint32 `json:"maximum_candidates"`
	MaximumGoalPredicates        uint32 `json:"maximum_goal_predicates"`
	MaximumTotalCandidateGoals   uint32 `json:"maximum_total_candidate_goals"`
	MaximumParallelEntries       uint32 `json:"maximum_parallel_entries_per_candidate"`
	MaximumTotalParallelEntries  uint32 `json:"maximum_total_parallel_entries"`
	MaximumSatisfiedRequirements uint32 `json:"maximum_satisfied_requirements"`
	MaximumDepth                 uint32 `json:"maximum_depth"`
	MaximumProcedures            uint32 `json:"maximum_procedures"`
	MaximumBridges               uint32 `json:"maximum_bridges"`
	MaximumTotalToolCost         uint32 `json:"maximum_total_tool_cost"`
	MaximumExpansions            uint32 `json:"maximum_expansions"`
	MaximumWorkUnits             uint64 `json:"maximum_work_units"`
	NoveltyPenalty               int64  `json:"novelty_penalty"`
	UnobservedEdgePenalty        int64  `json:"unobserved_edge_penalty"`
}

type PlannerManifest struct {
	SchemaVersion string `json:"schema_version"`
	PlannerManifestSpec
	ID            string `json:"-"`
	ContentHash   string `json:"-"`
	CanonicalJSON []byte `json:"-"`
}

func NewPlannerManifest(spec PlannerManifestSpec) (PlannerManifest, error) {
	spec.Version = strings.TrimSpace(spec.Version)
	manifest := PlannerManifest{SchemaVersion: "planner-manifest.v1", PlannerManifestSpec: spec}
	if !validPlannerSpec(spec) {
		return PlannerManifest{}, ErrInvalidPlannerManifest
	}
	canonicalJSON, hash, err := canonical.MarshalAndHash(manifest)
	if err != nil {
		return PlannerManifest{}, err
	}
	manifest.ID, manifest.ContentHash, manifest.CanonicalJSON = "pman_"+hash, hash, canonicalJSON
	return manifest, nil
}

func ValidatePlannerManifest(manifest PlannerManifest) error {
	rebuilt, err := NewPlannerManifest(manifest.PlannerManifestSpec)
	if err != nil || manifest.SchemaVersion != rebuilt.SchemaVersion || manifest.ID != rebuilt.ID || manifest.ContentHash != rebuilt.ContentHash || !bytes.Equal(manifest.CanonicalJSON, rebuilt.CanonicalJSON) {
		return ErrInvalidPlannerManifest
	}
	return nil
}

func validPlannerSpec(spec PlannerManifestSpec) bool {
	return spec.Version != "" && !strings.ContainsRune(spec.Version, '\x00') &&
		spec.BeamWidth > 0 && spec.BeamWidth <= 1_024 && spec.MaximumDepth > 0 && spec.MaximumDepth <= 32 &&
		spec.MaximumCandidates > 0 && spec.MaximumCandidates <= 10_000 &&
		spec.MaximumGoalPredicates > 0 && spec.MaximumGoalPredicates <= 1_024 && spec.MaximumTotalCandidateGoals >= spec.MaximumGoalPredicates && spec.MaximumTotalCandidateGoals <= 1_000_000 &&
		spec.MaximumParallelEntries > 0 && spec.MaximumParallelEntries <= 16_384 && spec.MaximumTotalParallelEntries >= spec.MaximumParallelEntries && spec.MaximumTotalParallelEntries <= 1_000_000 && spec.MaximumSatisfiedRequirements > 0 && spec.MaximumSatisfiedRequirements <= 100_000 &&
		spec.MaximumProcedures > 0 && spec.MaximumProcedures <= 32 && spec.MaximumBridges < spec.MaximumProcedures &&
		spec.MaximumTotalToolCost > 0 && spec.MaximumTotalToolCost <= 1_000_000 &&
		spec.MaximumExpansions >= spec.BeamWidth && spec.MaximumExpansions <= 100_000 && spec.MaximumWorkUnits >= uint64(spec.MaximumExpansions) && spec.MaximumWorkUnits <= 10_000_000 &&
		spec.NoveltyPenalty >= 0 && spec.NoveltyPenalty <= math.MaxInt32 && spec.UnobservedEdgePenalty >= 0 && spec.UnobservedEdgePenalty <= math.MaxInt32
}
