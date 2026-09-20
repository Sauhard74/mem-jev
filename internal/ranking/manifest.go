package ranking

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strings"

	"github.com/sauhard74/mem-jev/internal/canonical"
)

const rrfScale int64 = 1_000_000

var ErrInvalidManifest = errors.New("invalid ranker manifest")

type ChannelWeight struct {
	Channel      string `json:"channel"`
	WeightMicros int32  `json:"weight_micros"`
}

type FeatureSpec struct {
	Name        string `json:"name"`
	Minimum     int32  `json:"minimum"`
	Maximum     int32  `json:"maximum"`
	Missing     int32  `json:"missing"`
	Coefficient int32  `json:"coefficient"`
}

type ManifestSpec struct {
	Version        string          `json:"version"`
	RRFK           uint32          `json:"rrf_k"`
	RRFCoefficient int32           `json:"rrf_coefficient"`
	MaxCandidates  uint32          `json:"max_candidates"`
	Intercept      int64           `json:"intercept"`
	Channels       []ChannelWeight `json:"channels"`
	Features       []FeatureSpec   `json:"features,omitempty"`
}

type Manifest struct {
	ManifestSpec
	ID            string `json:"-"`
	CanonicalJSON []byte `json:"-"`
}

func NewManifest(source ManifestSpec) (Manifest, error) {
	spec := source
	spec.Version = strings.TrimSpace(spec.Version)
	spec.Channels = append([]ChannelWeight(nil), source.Channels...)
	spec.Features = append([]FeatureSpec(nil), source.Features...)
	if spec.Version == "" || spec.RRFK == 0 || spec.RRFK > 1_000_000 || spec.MaxCandidates == 0 || spec.MaxCandidates > 10_000 || spec.RRFCoefficient == 0 || len(spec.Channels) == 0 {
		return Manifest{}, fmt.Errorf("%w: required value missing or out of bounds", ErrInvalidManifest)
	}
	sort.Slice(spec.Channels, func(i, j int) bool { return spec.Channels[i].Channel < spec.Channels[j].Channel })
	for index := range spec.Channels {
		spec.Channels[index].Channel = strings.TrimSpace(spec.Channels[index].Channel)
		if spec.Channels[index].Channel == "" || spec.Channels[index].WeightMicros <= 0 {
			return Manifest{}, fmt.Errorf("%w: channel and positive weight required", ErrInvalidManifest)
		}
		if index > 0 && spec.Channels[index-1].Channel == spec.Channels[index].Channel {
			return Manifest{}, fmt.Errorf("%w: duplicate channel %q", ErrInvalidManifest, spec.Channels[index].Channel)
		}
	}
	sort.Slice(spec.Features, func(i, j int) bool { return spec.Features[i].Name < spec.Features[j].Name })
	for index := range spec.Features {
		feature := &spec.Features[index]
		feature.Name = strings.TrimSpace(feature.Name)
		if feature.Name == "" || feature.Minimum > feature.Maximum || feature.Missing < feature.Minimum || feature.Missing > feature.Maximum {
			return Manifest{}, fmt.Errorf("%w: invalid feature bounds", ErrInvalidManifest)
		}
		if index > 0 && spec.Features[index-1].Name == feature.Name {
			return Manifest{}, fmt.Errorf("%w: duplicate feature %q", ErrInvalidManifest, feature.Name)
		}
	}
	if !scoreBoundFits(spec) {
		return Manifest{}, fmt.Errorf("%w: score can overflow int64", ErrInvalidManifest)
	}
	canonicalJSON, hash, err := canonical.MarshalAndHash(spec)
	if err != nil {
		return Manifest{}, fmt.Errorf("canonicalize ranker manifest: %w", err)
	}
	return Manifest{ManifestSpec: spec, ID: "rnk_" + hash, CanonicalJSON: canonicalJSON}, nil
}

func scoreBoundFits(spec ManifestSpec) bool {
	bound := new(big.Int).Abs(big.NewInt(spec.Intercept))
	rrfBound := new(big.Int)
	for _, channel := range spec.Channels {
		term := new(big.Int).Mul(big.NewInt(int64(channel.WeightMicros)), big.NewInt(rrfScale))
		term.Div(term, big.NewInt(int64(spec.RRFK)+1))
		rrfBound.Add(rrfBound, term)
	}
	rrfBound.Mul(rrfBound, big.NewInt(abs32(spec.RRFCoefficient)))
	bound.Add(bound, rrfBound)
	for _, feature := range spec.Features {
		maximum := max64(abs32(feature.Minimum), abs32(feature.Maximum))
		term := new(big.Int).Mul(big.NewInt(maximum), big.NewInt(abs32(feature.Coefficient)))
		bound.Add(bound, term)
	}
	return bound.Cmp(big.NewInt(math.MaxInt64)) <= 0
}

func abs32(value int32) int64 {
	if value < 0 {
		return -int64(value)
	}
	return int64(value)
}

func max64(left, right int64) int64 {
	if left > right {
		return left
	}
	return right
}
