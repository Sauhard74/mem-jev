package ranking

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
)

var ErrInvalidCandidate = errors.New("invalid ranking candidate")

type ChannelRank struct {
	Channel string
	Rank    uint32
}

type FeatureValue struct {
	Name  string
	Value int32
}

type Candidate struct {
	VersionID            string
	ChannelRanks         []ChannelRank
	Features             []FeatureValue
	VerificationStrength int32
	ObservedEndToEnd     bool
}

type Result struct {
	VersionID            string
	Rank                 uint32
	RRFScore             int64
	FinalScore           int64
	VerificationStrength int32
	ObservedEndToEnd     bool
	MissingFeatures      map[string]bool
}

func Rank(manifest Manifest, candidates []Candidate) ([]Result, error) {
	validated, err := NewManifest(manifest.ManifestSpec)
	if err != nil || manifest.ID != validated.ID || !bytes.Equal(manifest.CanonicalJSON, validated.CanonicalJSON) {
		return nil, ErrInvalidManifest
	}
	channelWeights := make(map[string]int32, len(manifest.Channels))
	for _, item := range manifest.Channels {
		channelWeights[item.Channel] = item.WeightMicros
	}
	featureSpecs := make(map[string]FeatureSpec, len(manifest.Features))
	for _, item := range manifest.Features {
		featureSpecs[item.Name] = item
	}
	seenVersions := make(map[string]struct{}, len(candidates))
	results := make([]Result, 0, len(candidates))
	for _, candidate := range candidates {
		if strings.TrimSpace(candidate.VersionID) == "" {
			return nil, fmt.Errorf("%w: version ID required", ErrInvalidCandidate)
		}
		if _, exists := seenVersions[candidate.VersionID]; exists {
			return nil, fmt.Errorf("%w: duplicate version %q", ErrInvalidCandidate, candidate.VersionID)
		}
		seenVersions[candidate.VersionID] = struct{}{}
		result, err := scoreCandidate(manifest, candidate, channelWeights, featureSpecs)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].FinalScore != results[j].FinalScore {
			return results[i].FinalScore > results[j].FinalScore
		}
		if results[i].VerificationStrength != results[j].VerificationStrength {
			return results[i].VerificationStrength > results[j].VerificationStrength
		}
		if results[i].ObservedEndToEnd != results[j].ObservedEndToEnd {
			return results[i].ObservedEndToEnd
		}
		return results[i].VersionID < results[j].VersionID
	})
	for index := range results {
		results[index].Rank = uint32(index + 1)
	}
	return results, nil
}

func scoreCandidate(manifest Manifest, candidate Candidate, channelWeights map[string]int32, featureSpecs map[string]FeatureSpec) (Result, error) {
	rrfScore := int64(0)
	seenChannels := make(map[string]struct{}, len(candidate.ChannelRanks))
	for _, channelRank := range candidate.ChannelRanks {
		weight, ok := channelWeights[channelRank.Channel]
		if !ok || channelRank.Rank == 0 || channelRank.Rank > manifest.MaxCandidates {
			return Result{}, fmt.Errorf("%w: invalid channel rank", ErrInvalidCandidate)
		}
		if _, exists := seenChannels[channelRank.Channel]; exists {
			return Result{}, fmt.Errorf("%w: duplicate channel %q", ErrInvalidCandidate, channelRank.Channel)
		}
		seenChannels[channelRank.Channel] = struct{}{}
		product, ok := checkedMul(int64(weight), rrfScale)
		if !ok {
			return Result{}, fmt.Errorf("%w: RRF overflow", ErrInvalidManifest)
		}
		contribution := product / (int64(manifest.RRFK) + int64(channelRank.Rank))
		rrfScore, ok = checkedAdd(rrfScore, contribution)
		if !ok {
			return Result{}, fmt.Errorf("%w: RRF overflow", ErrInvalidManifest)
		}
	}
	provided := make(map[string]int32, len(candidate.Features))
	for _, feature := range candidate.Features {
		spec, ok := featureSpecs[feature.Name]
		if !ok || feature.Value < spec.Minimum || feature.Value > spec.Maximum {
			return Result{}, fmt.Errorf("%w: invalid feature %q", ErrInvalidCandidate, feature.Name)
		}
		if _, exists := provided[feature.Name]; exists {
			return Result{}, fmt.Errorf("%w: duplicate feature %q", ErrInvalidCandidate, feature.Name)
		}
		provided[feature.Name] = feature.Value
	}
	finalScore := manifest.Intercept
	rrfTerm, ok := checkedMul(rrfScore, int64(manifest.RRFCoefficient))
	if !ok {
		return Result{}, fmt.Errorf("%w: score overflow", ErrInvalidManifest)
	}
	finalScore, ok = checkedAdd(finalScore, rrfTerm)
	if !ok {
		return Result{}, fmt.Errorf("%w: score overflow", ErrInvalidManifest)
	}
	missing := make(map[string]bool, len(manifest.Features))
	for _, spec := range manifest.Features {
		value, exists := provided[spec.Name]
		if !exists {
			value = spec.Missing
			missing[spec.Name] = true
		}
		term, multiplyOK := checkedMul(int64(value), int64(spec.Coefficient))
		if !multiplyOK {
			return Result{}, fmt.Errorf("%w: feature overflow", ErrInvalidManifest)
		}
		finalScore, ok = checkedAdd(finalScore, term)
		if !ok {
			return Result{}, fmt.Errorf("%w: score overflow", ErrInvalidManifest)
		}
	}
	return Result{
		VersionID: candidate.VersionID, RRFScore: rrfScore, FinalScore: finalScore,
		VerificationStrength: candidate.VerificationStrength, ObservedEndToEnd: candidate.ObservedEndToEnd,
		MissingFeatures: missing,
	}, nil
}

func checkedAdd(left, right int64) (int64, bool) {
	if right > 0 && left > math.MaxInt64-right || right < 0 && left < math.MinInt64-right {
		return 0, false
	}
	return left + right, true
}

func checkedMul(left, right int64) (int64, bool) {
	if left == 0 || right == 0 {
		return 0, true
	}
	if left == -1 && right == math.MinInt64 || right == -1 && left == math.MinInt64 {
		return 0, false
	}
	result := left * right
	if result/right != left {
		return 0, false
	}
	return result, true
}
