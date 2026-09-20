package ranking_test

import (
	"bytes"
	"math"
	"math/rand"
	"os"
	"slices"
	"testing"

	"github.com/sauhard74/mem-jev/internal/ranking"
)

func TestManifestCanonicalGolden(t *testing.T) {
	manifest := mustManifest(t, validSpec())
	want, err := os.ReadFile("testdata/ranker.golden.json")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(manifest.CanonicalJSON, bytes.TrimSpace(want)) {
		t.Fatalf("canonical manifest changed:\nwant %s\n got %s", want, manifest.CanonicalJSON)
	}
}

func TestManifestIsCanonicalAndContentAddressed(t *testing.T) {
	left, err := ranking.NewManifest(ranking.ManifestSpec{
		Version: "ranker.v1", RRFK: 60, RRFCoefficient: 3, MaxCandidates: 100,
		Channels: []ranking.ChannelWeight{{Channel: "vector", WeightMicros: 500_000}, {Channel: "exact", WeightMicros: 2_000_000}},
		Features: []ranking.FeatureSpec{
			{Name: "freshness", Minimum: -100, Maximum: 100, Missing: -100, Coefficient: 2},
			{Name: "success", Minimum: 0, Maximum: 1_000_000, Missing: 0, Coefficient: 5},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	right, err := ranking.NewManifest(ranking.ManifestSpec{
		Version: "ranker.v1", RRFK: 60, RRFCoefficient: 3, MaxCandidates: 100,
		Channels: []ranking.ChannelWeight{{Channel: "exact", WeightMicros: 2_000_000}, {Channel: "vector", WeightMicros: 500_000}},
		Features: []ranking.FeatureSpec{
			{Name: "success", Minimum: 0, Maximum: 1_000_000, Missing: 0, Coefficient: 5},
			{Name: "freshness", Minimum: -100, Maximum: 100, Missing: -100, Coefficient: 2},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if left.ID != right.ID || string(left.CanonicalJSON) != string(right.CanonicalJSON) {
		t.Fatalf("manifest order changed identity:\n%s\n%s", left.CanonicalJSON, right.CanonicalJSON)
	}
	if left.ID[:4] != "rnk_" || len(left.ID) != 68 {
		t.Fatalf("manifest ID = %q", left.ID)
	}
}

func TestManifestRejectsInvalidAndOverflowingModels(t *testing.T) {
	tests := []struct {
		name string
		spec ranking.ManifestSpec
	}{
		{name: "no version", spec: mutateSpec(func(s *ranking.ManifestSpec) { s.Version = "" })},
		{name: "zero k", spec: mutateSpec(func(s *ranking.ManifestSpec) { s.Version = "v"; s.RRFK = 0 })},
		{name: "duplicate channel", spec: mutateSpec(func(s *ranking.ManifestSpec) { s.Channels = append(s.Channels, s.Channels[0]) })},
		{name: "duplicate feature", spec: mutateSpec(func(s *ranking.ManifestSpec) { s.Features = append(s.Features, s.Features[0]) })},
		{name: "missing outside range", spec: mutateSpec(func(s *ranking.ManifestSpec) { s.Features[0].Missing = 11 })},
		{name: "overflow coefficient", spec: mutateSpec(func(s *ranking.ManifestSpec) {
			for index := range s.Features {
				s.Features[index].Minimum = 0
				s.Features[index].Maximum = math.MaxInt32
				s.Features[index].Coefficient = math.MaxInt32
			}
		})},
		{name: "overflow intercept", spec: mutateSpec(func(s *ranking.ManifestSpec) { s.Intercept = math.MaxInt64 })},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ranking.NewManifest(tt.spec); err == nil {
				t.Fatal("NewManifest() error = nil")
			}
		})
	}
}

func TestRankUsesWeightedRRFCalibratedFeaturesAndStableTies(t *testing.T) {
	manifest := mustManifest(t, validSpec())
	inputs := []ranking.Candidate{
		{VersionID: "pv_b", ChannelRanks: []ranking.ChannelRank{{Channel: "lexical", Rank: 1}}, Features: []ranking.FeatureValue{{Name: "success", Value: 700}}, VerificationStrength: 4, ObservedEndToEnd: false},
		{VersionID: "pv_a", ChannelRanks: []ranking.ChannelRank{{Channel: "exact", Rank: 2}, {Channel: "lexical", Rank: 1}}, Features: []ranking.FeatureValue{{Name: "success", Value: 700}}, VerificationStrength: 4, ObservedEndToEnd: true},
		{VersionID: "pv_c", ChannelRanks: []ranking.ChannelRank{{Channel: "exact", Rank: 1}}, Features: nil, VerificationStrength: 5, ObservedEndToEnd: true},
	}
	got, err := ranking.Rank(manifest, inputs)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].VersionID != "pv_a" || got[1].VersionID != "pv_c" || got[2].VersionID != "pv_b" {
		t.Fatalf("order = %#v", got)
	}
	if !got[2].MissingFeatures["freshness"] || got[2].MissingFeatures["success"] {
		t.Fatalf("missing features = %#v", got[2].MissingFeatures)
	}
}

func TestRankIsPermutationInvariant(t *testing.T) {
	manifest := mustManifest(t, validSpec())
	base := []ranking.Candidate{
		{VersionID: "pv_1", ChannelRanks: []ranking.ChannelRank{{Channel: "exact", Rank: 1}, {Channel: "lexical", Rank: 3}}, Features: []ranking.FeatureValue{{Name: "success", Value: 400}}},
		{VersionID: "pv_2", ChannelRanks: []ranking.ChannelRank{{Channel: "lexical", Rank: 1}}, Features: []ranking.FeatureValue{{Name: "freshness", Value: -4}}},
		{VersionID: "pv_3", ChannelRanks: []ranking.ChannelRank{{Channel: "exact", Rank: 2}}, Features: nil},
	}
	want, err := ranking.Rank(manifest, base)
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(7))
	for iteration := 0; iteration < 100; iteration++ {
		shuffled := slices.Clone(base)
		rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		for i := range shuffled {
			rng.Shuffle(len(shuffled[i].ChannelRanks), func(a, b int) {
				shuffled[i].ChannelRanks[a], shuffled[i].ChannelRanks[b] = shuffled[i].ChannelRanks[b], shuffled[i].ChannelRanks[a]
			})
			rng.Shuffle(len(shuffled[i].Features), func(a, b int) {
				shuffled[i].Features[a], shuffled[i].Features[b] = shuffled[i].Features[b], shuffled[i].Features[a]
			})
		}
		got, rankErr := ranking.Rank(manifest, shuffled)
		if rankErr != nil {
			t.Fatal(rankErr)
		}
		if !sameResults(want, got) {
			t.Fatalf("iteration %d changed results:\n%#v\n%#v", iteration, want, got)
		}
	}
}

func TestRankRejectsMalformedCandidates(t *testing.T) {
	manifest := mustManifest(t, validSpec())
	tests := []ranking.Candidate{
		{},
		{VersionID: "pv", ChannelRanks: []ranking.ChannelRank{{Channel: "unknown", Rank: 1}}},
		{VersionID: "pv", ChannelRanks: []ranking.ChannelRank{{Channel: "exact", Rank: 0}}},
		{VersionID: "pv", Features: []ranking.FeatureValue{{Name: "unknown", Value: 1}}},
		{VersionID: "pv", Features: []ranking.FeatureValue{{Name: "success", Value: 1001}}},
	}
	for i, candidate := range tests {
		if _, err := ranking.Rank(manifest, []ranking.Candidate{candidate}); err == nil {
			t.Fatalf("case %d: Rank() error = nil", i)
		}
	}
}

func validSpec() ranking.ManifestSpec {
	return ranking.ManifestSpec{
		Version: "ranker.v1", RRFK: 60, RRFCoefficient: 1, MaxCandidates: 100,
		Channels: []ranking.ChannelWeight{{Channel: "exact", WeightMicros: 2_000_000}, {Channel: "lexical", WeightMicros: 1_000_000}},
		Features: []ranking.FeatureSpec{
			{Name: "freshness", Minimum: -10, Maximum: 10, Missing: -10, Coefficient: 2},
			{Name: "success", Minimum: 0, Maximum: 1000, Missing: 0, Coefficient: 3},
		},
	}
}

func mutateSpec(fn func(*ranking.ManifestSpec)) ranking.ManifestSpec {
	spec := validSpec()
	fn(&spec)
	return spec
}

func mustManifest(t *testing.T, spec ranking.ManifestSpec) ranking.Manifest {
	t.Helper()
	manifest, err := ranking.NewManifest(spec)
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}

func sameResults(left, right []ranking.Result) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].VersionID != right[index].VersionID || left[index].FinalScore != right[index].FinalScore || left[index].RRFScore != right[index].RRFScore {
			return false
		}
	}
	return true
}
