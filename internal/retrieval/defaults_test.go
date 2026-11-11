package retrieval

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
)

func TestBuildDefaultServingRequiresRegion(t *testing.T) {
	if _, err := BuildDefaultServing(DefaultServingInput{AllowedResidencyRegions: []string{" "}}); err == nil {
		t.Fatal("expected error without a residency region")
	}
}

func TestBuildDefaultServingIsDeterministicAndMarkedAsDefault(t *testing.T) {
	first, err := BuildDefaultServing(DefaultServingInput{AllowedResidencyRegions: []string{"local"}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuildDefaultServing(DefaultServingInput{AllowedResidencyRegions: []string{"local"}})
	if err != nil {
		t.Fatal(err)
	}
	if first.Policy.ID != second.Policy.ID || first.Ranker.ID != second.Ranker.ID {
		t.Fatal("default manifests are not deterministic")
	}
	if !IsDefaultManifestVersion(first.Policy.Version) || !IsDefaultManifestVersion(first.Ranker.Version) {
		t.Fatalf("default versions not marked: %q %q", first.Policy.Version, first.Ranker.Version)
	}
	if supportsSemanticFeatures(first.Ranker) {
		t.Fatal("no-Jev default ranker must not declare Jev features")
	}
}

func TestBuildDefaultServingWithJevDeclaresSemanticFeatures(t *testing.T) {
	plain, err := BuildDefaultServing(DefaultServingInput{AllowedResidencyRegions: []string{"local"}})
	if err != nil {
		t.Fatal(err)
	}
	semantic, err := BuildDefaultServing(DefaultServingInput{AllowedResidencyRegions: []string{"local"}, Semantic: true})
	if err != nil {
		t.Fatal(err)
	}
	if !supportsSemanticFeatures(semantic.Ranker) || semantic.Ranker.ID == plain.Ranker.ID || !IsDefaultManifestVersion(semantic.Ranker.Version) {
		t.Fatalf("Jev default ranker = %#v", semantic.Ranker)
	}
}

// A freshly learned routine is stored as a "candidate" validated by the
// worker's outcome policy. The default policy must be able to recall it.
func TestDefaultServingRecallsFreshlyLearnedRoutine(t *testing.T) {
	now := time.Now().UTC()
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
	document, err := BuildDocument(DocumentInput{TenantID: "tenant_a", ProcedureVersionID: "pv_a", ProcedureID: "proc_a", TaskText: "release", IntentHash: intentHash, EffectSignatureHash: strings.Repeat("b", 64), Tools: []ToolRequirement{{Name: "shell", ContractVersionID: "tcv_shell"}}, OrderedStepContractIDs: []string{"tcv_shell"}, Effects: []string{"filesystem.write"}, EnvironmentScopeHash: environmentHash, Harness: Harness{Name: "ci", Version: "1"}, Lifecycle: "candidate", ObservedEndToEnd: true, VerificationStrength: 5, VerifiedSuccessCount: 1, ValidatedAt: now.Add(-time.Hour), ValidationPolicyVersion: "outcome-policy.v1", LearnedWithRecallConsent: true, ResidencyRegion: "local", RiskClass: "low", Interface: &procedureInterface})
	if err != nil {
		t.Fatal(err)
	}
	defaults, err := BuildDefaultServing(DefaultServingInput{AllowedResidencyRegions: []string{"local"}})
	if err != nil {
		t.Fatal(err)
	}
	exact := &serviceChannel{name: ChannelExact, manifest: "idx_exact", hits: []Hit{{VersionID: document.ProcedureVersionID, RawScoreQuantized: 1_000_000, IndexManifestID: "idx_exact"}}}
	config, err := BuildServingConfig("tenant_a", defaults.Policy.ID, defaults.Ranker.ID, []SnapshotIndex{{Channel: ChannelExact, ManifestID: "idx_exact"}})
	if err != nil {
		t.Fatal(err)
	}
	repository := &fakeDecisionRepository{snapshot: ServingSnapshot{ProjectionEpoch: 1, DocumentSetHash: strings.Repeat("a", 64), ServingConfigID: config.ID, PolicyManifestID: defaults.Policy.ID, RankerManifestID: defaults.Ranker.ID, Indexes: config.Indexes}}
	var bootstrapped []domain.TenantID
	service, err := NewService(repository, fakeDocumentReader{documents: []Document{document}}, fakeManifestResolver{policy: defaults.Policy, ranker: defaults.Ranker}, fixedEffectInferer{hash: document.EffectSignatureHash}, fixedCipher{}, fixedIDs{}, ServiceConfig{
		Channels: []Channel{exact}, RequiredChannels: []ChannelName{ChannelExact}, ChannelTimeout: time.Second, Retention: 24 * time.Hour, MaximumSelections: 1,
		ServingBootstrap: func(_ context.Context, tenantID domain.TenantID) { bootstrapped = append(bootstrapped, tenantID) },
	}, &fakePlanIssuer{})
	if err != nil {
		t.Fatal(err)
	}
	request := newServiceFixture(t, false).request
	response, err := service.Retrieve(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(bootstrapped) != 1 || bootstrapped[0] != "tenant_a" {
		t.Fatalf("bootstrap calls = %v", bootstrapped)
	}
	// Unpromoted routines come back as advisory guidance rather than a plan.
	if len(response.Candidates) != 1 || response.Candidates[0].VersionID != document.ProcedureVersionID || !response.Candidates[0].AdvisoryOnly || response.DecisionCode != "advisory_only" {
		t.Fatalf("default serving did not recall the learned routine: %#v", response)
	}
}

func TestDefaultServingTrustsPromotionPolicies(t *testing.T) {
	plain, err := BuildDefaultServing(DefaultServingInput{AllowedResidencyRegions: []string{"local"}})
	if err != nil {
		t.Fatal(err)
	}
	withPromotion, err := BuildDefaultServing(DefaultServingInput{AllowedResidencyRegions: []string{"local"}, PromotionPolicies: []string{"lcman_test"}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(withPromotion.Policy.CanonicalJSON), `"lcman_test"`) || withPromotion.Policy.ID == plain.Policy.ID || !IsDefaultManifestVersion(withPromotion.Policy.Version) {
		t.Fatalf("promotion policy not admitted: %s", withPromotion.Policy.CanonicalJSON)
	}
}
