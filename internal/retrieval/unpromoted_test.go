package retrieval

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/canonical"
)

type fakeStepReader struct {
	steps map[string][]PlanStep
	calls int
}

func (r *fakeStepReader) ProcedureSteps(_ context.Context, _ string, versionIDs []string) (map[string][]PlanStep, error) {
	r.calls++
	result := map[string][]PlanStep{}
	for _, id := range versionIDs {
		if steps, ok := r.steps[id]; ok {
			result[id] = steps
		}
	}
	return result, nil
}

func learnedDocument(t *testing.T, versionID, lifecycle, risk string) Document {
	t.Helper()
	intentHash, err := CanonicalIntentHash("release", Harness{Name: "ci", Version: "1"})
	if err != nil {
		t.Fatal(err)
	}
	_, environmentHash, err := canonical.MarshalAndHash([]Fact{})
	if err != nil {
		t.Fatal(err)
	}
	procedureInterface, err := NewProcedureInterface(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	document, err := BuildDocument(DocumentInput{TenantID: "tenant_a", ProcedureVersionID: versionID, ProcedureID: "proc_" + versionID, TaskText: "release", IntentHash: intentHash, EffectSignatureHash: strings.Repeat("b", 64), Tools: []ToolRequirement{{Name: "shell", ContractVersionID: "tcv_shell"}}, OrderedStepContractIDs: []string{"tcv_shell"}, Effects: []string{"filesystem.write"}, EnvironmentScopeHash: environmentHash, Harness: Harness{Name: "ci", Version: "1"}, Lifecycle: lifecycle, ObservedEndToEnd: true, VerificationStrength: 5, VerifiedSuccessCount: 1, ValidatedAt: time.Now().UTC().Add(-time.Hour), ValidationPolicyVersion: "outcome-policy.v1", LearnedWithRecallConsent: true, ResidencyRegion: "local", RiskClass: risk, Interface: &procedureInterface})
	if err != nil {
		t.Fatal(err)
	}
	return document
}

func defaultServingService(t *testing.T, documents []Document, steps StepReader, unpromoted bool) *Service {
	t.Helper()
	defaults, err := BuildDefaultServing(DefaultServingInput{AllowedResidencyRegions: []string{"local"}})
	if err != nil {
		t.Fatal(err)
	}
	hits := make([]Hit, len(documents))
	for index, document := range documents {
		hits[index] = Hit{VersionID: document.ProcedureVersionID, RawScoreQuantized: int64(1_000_000 - index), IndexManifestID: "idx_exact"}
	}
	exact := &serviceChannel{name: ChannelExact, manifest: "idx_exact", hits: hits}
	config, err := BuildServingConfig("tenant_a", defaults.Policy.ID, defaults.Ranker.ID, []SnapshotIndex{{Channel: ChannelExact, ManifestID: "idx_exact"}})
	if err != nil {
		t.Fatal(err)
	}
	repository := &fakeDecisionRepository{snapshot: ServingSnapshot{ProjectionEpoch: 1, DocumentSetHash: strings.Repeat("a", 64), ServingConfigID: config.ID, PolicyManifestID: defaults.Policy.ID, RankerManifestID: defaults.Ranker.ID, Indexes: config.Indexes}}
	service, err := NewService(repository, fakeDocumentReader{documents: documents}, fakeManifestResolver{policy: defaults.Policy, ranker: defaults.Ranker}, fixedEffectInferer{hash: documents[0].EffectSignatureHash}, fixedCipher{}, fixedIDs{}, ServiceConfig{
		Channels: []Channel{exact}, RequiredChannels: []ChannelName{ChannelExact}, ChannelTimeout: time.Second, Retention: 24 * time.Hour, MaximumSelections: 1,
		Steps: steps, UnpromotedPlans: unpromoted,
	}, &fakePlanIssuer{})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

var safeSteps = []PlanStep{{Ordinal: 0, ToolName: "shell", ToolVersion: "1.0.0", ContractVersionID: "tcv_shell", SideEffect: "write", Risk: "low"}}

func TestUnpromotedRoutineIsReturnedAsAdvisoryPlanWithSteps(t *testing.T) {
	candidate := learnedDocument(t, "pv_a", "candidate", "low")
	steps := &fakeStepReader{steps: map[string][]PlanStep{"pv_a": safeSteps}}
	response, err := defaultServingService(t, []Document{candidate}, steps, true).Retrieve(context.Background(), newServiceFixture(t, false).request)
	if err != nil {
		t.Fatal(err)
	}
	if response.Disposition != RunSelected || response.Plan == nil || response.Plan.InjectionID == "" || !response.PlanAdvisoryOnly {
		t.Fatalf("response = %#v", response)
	}
	if got := response.PlanSteps["pv_a"]; len(got) != 1 || got[0].ToolName != "shell" {
		t.Fatalf("plan steps = %#v", response.PlanSteps)
	}
	if len(response.Candidates) != 1 || !response.Candidates[0].AdvisoryOnly {
		t.Fatalf("candidates = %#v", response.Candidates)
	}
}

func TestUnpromotedPlanIsWithheldWhenUnsafeOrDisabled(t *testing.T) {
	cases := map[string]struct {
		document   Document
		steps      []PlanStep
		unpromoted bool
	}{
		"disabled":              {learnedDocument(t, "pv_a", "candidate", "low"), safeSteps, false},
		"irreversible step":     {learnedDocument(t, "pv_a", "candidate", "low"), []PlanStep{{ToolName: "shell", SideEffect: "irreversible", Risk: "low"}}, true},
		"critical step":         {learnedDocument(t, "pv_a", "candidate", "low"), []PlanStep{{ToolName: "shell", SideEffect: "write", Risk: "critical"}}, true},
		"steps unknown":         {learnedDocument(t, "pv_a", "candidate", "low"), nil, true},
		"contract side unknown": {learnedDocument(t, "pv_a", "candidate", "low"), []PlanStep{{ToolName: "shell", Risk: "low"}}, true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			reader := &fakeStepReader{steps: map[string][]PlanStep{}}
			if tc.steps != nil {
				reader.steps["pv_a"] = tc.steps
			}
			response, err := defaultServingService(t, []Document{tc.document}, reader, tc.unpromoted).Retrieve(context.Background(), newServiceFixture(t, false).request)
			if err != nil {
				t.Fatal(err)
			}
			if response.Disposition != RunAbstained || response.DecisionCode != "advisory_only" || response.Plan != nil {
				t.Fatalf("response = %#v", response)
			}
		})
	}
}

func TestPromotedRoutineWinsOverUnpromoted(t *testing.T) {
	candidate := learnedDocument(t, "pv_a", "candidate", "low")
	trial := learnedDocument(t, "pv_b", "trial", "low")
	steps := &fakeStepReader{steps: map[string][]PlanStep{"pv_a": safeSteps, "pv_b": safeSteps}}
	response, err := defaultServingService(t, []Document{candidate, trial}, steps, true).Retrieve(context.Background(), newServiceFixture(t, false).request)
	if err != nil {
		t.Fatal(err)
	}
	if response.Disposition != RunSelected || response.Plan == nil || response.PlanAdvisoryOnly || response.Plan.Nodes[0].VersionID != "pv_b" {
		t.Fatalf("response = %#v", response)
	}
}

func TestUnpromotedPlanKeepsRequestEligibilityChecks(t *testing.T) {
	candidate := learnedDocument(t, "pv_a", "candidate", "low")
	steps := &fakeStepReader{steps: map[string][]PlanStep{"pv_a": safeSteps}}
	request := newServiceFixture(t, false).request
	request.Input.ForbiddenEffects = []string{"filesystem.write"}
	response, err := defaultServingService(t, []Document{candidate}, steps, true).Retrieve(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if response.Disposition != RunAbstained || response.Plan != nil || response.DecisionCode != "no_eligible_candidates" {
		t.Fatalf("forbidden effect still produced a plan: %#v", response)
	}
}
