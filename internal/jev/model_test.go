package jev

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/sauhard74/mem-jev/internal/domain"
)

func TestDefaultRubricV1IsCanonicalAndRequiresPinnedModel(t *testing.T) {
	first, err := DefaultRubricV1("jev-1.13.0")
	if err != nil {
		t.Fatal(err)
	}
	second, err := DefaultRubricV1(" jev-1.13.0 ")
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID || first.ContentHash != second.ContentHash || !bytes.Equal(first.CanonicalJSON, second.CanonicalJSON) {
		t.Fatal("equivalent pinned model produced different rubric identity")
	}
	if first.ID != "jevr_"+first.ContentHash || len(first.Questions) != 5 {
		t.Fatalf("unexpected rubric: %#v", first)
	}
	for _, model := range []string{"", "jev-latest", "jev-preview"} {
		if _, buildErr := DefaultRubricV1(model); !errors.Is(buildErr, ErrInvalidRubric) {
			t.Fatalf("DefaultRubricV1(%q) error = %v", model, buildErr)
		}
	}
}

func TestJudgmentKeyIsTenantAndManifestBound(t *testing.T) {
	rubric, err := DefaultRubricV1("jev-1.13.0")
	if err != nil {
		t.Fatal(err)
	}
	input := JudgmentKeyInput{
		TenantID: "tenant_a", QueryHash: strings.Repeat("a", 64), ProcedureVersionID: "pver_1",
		DocumentHash: strings.Repeat("b", 64), EnvironmentHash: strings.Repeat("c", 64),
		PolicyManifestID: "epol_1", RubricManifestID: rubric.ID, Provider: ProviderTypeSafe, Model: rubric.Model,
	}
	first, err := NewJudgmentKey(input)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewJudgmentKey(input)
	if err != nil || first != second || !strings.HasPrefix(first, "jevj_") {
		t.Fatalf("unstable key: %q %q %v", first, second, err)
	}
	input.TenantID = domain.TenantID("tenant_b")
	otherTenant, err := NewJudgmentKey(input)
	if err != nil || first == otherTenant {
		t.Fatal("judgment key must be tenant isolated")
	}
	input.QueryHash = "bad"
	if _, err = NewJudgmentKey(input); !errors.Is(err, ErrInvalidJudgment) {
		t.Fatalf("invalid query hash error = %v", err)
	}
}

func TestProbabilityMicrosUsesExactDecimalRounding(t *testing.T) {
	tests := map[string]int32{
		"0": 0, "1": 1_000_000, "0.1234564": 123_456, "0.1234565": 123_457,
		"1e-6": 1, "5e-7": 1, "4.9e-7": 0,
	}
	for raw, want := range tests {
		got, err := ProbabilityMicros(raw)
		if err != nil || got != want {
			t.Fatalf("ProbabilityMicros(%q) = %d, %v; want %d", raw, got, err, want)
		}
	}
	for _, raw := range []string{"", "-0.1", "1.000001", "NaN", "Inf", "1/2"} {
		if _, err := ProbabilityMicros(raw); !errors.Is(err, ErrInvalidProbability) {
			t.Fatalf("ProbabilityMicros(%q) error = %v", raw, err)
		}
	}
}

func TestJudgmentFeaturesAreBoundedAndDeterministic(t *testing.T) {
	judgment := Judgment{
		IntentFit:                    ScoreAnswer{ScoreMicros: 3_500_000, ConfidenceMicros: 800_000, ProbabilitiesMicros: []int32{0, 50_000, 100_000, 250_000, 600_000}},
		PreconditionsLikelySatisfied: NoulAnswer{NoulMicros: 900_000},
		TaskCoverage:                 ScoreAnswer{ScoreMicros: 2_000_000, ConfidenceMicros: 700_000, ProbabilitiesMicros: []int32{50_000, 100_000, 550_000, 250_000, 50_000}},
		ContradictsRequest:           NoulAnswer{NoulMicros: 125_000},
		UsefulAsPartialPlan:          NoulAnswer{NoulMicros: 600_000},
	}
	want := []Feature{
		{Name: FeatureIntentFit, Value: 875_000},
		{Name: FeatureNonContradiction, Value: 875_000},
		{Name: FeaturePartialPlan, Value: 600_000},
		{Name: FeaturePreconditions, Value: 900_000},
		{Name: FeatureTaskCoverage, Value: 500_000},
	}
	got, err := judgment.Features()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("features = %#v; want %#v", got, want)
	}
	judgment.IntentFit.ScoreMicros = 4_000_001
	if _, err = judgment.Features(); !errors.Is(err, ErrInvalidJudgment) {
		t.Fatalf("out-of-range score error = %v", err)
	}
}
