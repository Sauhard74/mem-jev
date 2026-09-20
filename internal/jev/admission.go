package jev

import (
	"errors"
	"sort"
	"strings"
)

var ErrInvalidAdmission = errors.New("invalid Jev admission input")

type AdmissionPolicy struct {
	MaximumCandidates      uint32
	AmbiguityScoreDistance int64
}

type RankedCandidate struct {
	VersionID  string
	Rank       uint32
	FinalScore int64
}

// AdmitAmbiguousCandidates returns no candidates when the preliminary winner
// is separated from the runner-up by more than the configured ambiguity band.
// Otherwise it admits the bounded top prefix that remains inside that band.
func AdmitAmbiguousCandidates(policy AdmissionPolicy, source []RankedCandidate) ([]string, error) {
	if policy.MaximumCandidates < 2 || policy.MaximumCandidates > 32 || policy.AmbiguityScoreDistance < 0 || len(source) > 10_000 {
		return nil, ErrInvalidAdmission
	}
	candidates := append([]RankedCandidate(nil), source...)
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Rank < candidates[j].Rank })
	for index, candidate := range candidates {
		if strings.TrimSpace(candidate.VersionID) == "" || candidate.Rank != uint32(index+1) || index > 0 && candidate.FinalScore > candidates[index-1].FinalScore {
			return nil, ErrInvalidAdmission
		}
	}
	if len(candidates) < 2 || scoreDistance(candidates[0].FinalScore, candidates[1].FinalScore) > uint64(policy.AmbiguityScoreDistance) {
		return nil, nil
	}
	limit := min(len(candidates), int(policy.MaximumCandidates))
	admitted := make([]string, 0, limit)
	for _, candidate := range candidates[:limit] {
		if scoreDistance(candidates[0].FinalScore, candidate.FinalScore) > uint64(policy.AmbiguityScoreDistance) {
			break
		}
		admitted = append(admitted, candidate.VersionID)
	}
	return admitted, nil
}

func scoreDistance(higher, lower int64) uint64 {
	// Inputs are ordered, but subtraction in signed space can overflow.
	return uint64(higher) - uint64(lower)
}
