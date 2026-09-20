package retrieval

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/eligibility"
	"github.com/sauhard74/mem-jev/internal/ranking"
)

func TestServiceSelectsEligibleCandidateAndPersistsDecision(t *testing.T) {
	fixture := newServiceFixture(t, false)
	response, err := fixture.service.Retrieve(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	if response.Disposition != RunSelected || len(response.Candidates) != 1 || response.Candidates[0].VersionID != fixture.document.ProcedureVersionID || len(fixture.repository.runs) != 1 {
		t.Fatalf("response = %#v, runs = %#v", response, fixture.repository.runs)
	}
	run := fixture.repository.runs[0]
	if run.QueryHash != response.QueryHash || run.Snapshot.ProjectionEpoch != 1 || len(run.Gates) != 1 || !run.Gates[0].Eligible || len(run.Ranked) != 1 || len(run.SelectedVersionIDs) != 1 {
		t.Fatalf("persisted run = %#v", run)
	}
	if fixture.exact.seenEffectHash != fixture.document.EffectSignatureHash {
		t.Fatalf("server did not bind inferred effect signature: %s", fixture.exact.seenEffectHash)
	}
	explanation, err := fixture.service.Explain(context.Background(), "tenant_a", response.RunID)
	if err != nil || explanation.QueryHash != response.QueryHash || len(explanation.Candidates) != 1 || explanation.Candidates[0].VersionID != fixture.document.ProcedureVersionID {
		t.Fatalf("explanation = %#v, err = %v", explanation, err)
	}
}

func TestServiceAllowsPolicyBoundVectorDegradation(t *testing.T) {
	fixture := newServiceFixture(t, true)
	response, err := fixture.service.Retrieve(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	if response.Disposition != RunSelected || len(response.Degraded) != 1 || response.Degraded[0].Channel != ChannelVector || response.Approximate {
		t.Fatalf("response = %#v", response)
	}
	run := fixture.repository.runs[0]
	if len(run.ChannelExecutions) != 2 || run.ChannelExecutions[1].Channel != ChannelVector || run.ChannelExecutions[1].Complete || run.ChannelExecutions[1].DegradationCode != "index_unavailable" {
		t.Fatalf("channel records = %#v", run.ChannelExecutions)
	}
}

func TestServiceFailsAndAuditsWhenRequiredChannelIsUnavailable(t *testing.T) {
	fixture := newServiceFixture(t, false)
	fixture.exact.err = &ChannelError{Code: "index_unavailable", Err: errors.New("down")}
	_, err := fixture.service.Retrieve(context.Background(), fixture.request)
	var serviceErr *ServiceError
	if !errors.As(err, &serviceErr) || serviceErr.Code != "required_channel_unavailable" || len(fixture.repository.runs) != 1 || fixture.repository.runs[0].Disposition != RunFailed {
		t.Fatalf("error = %v, runs = %#v", err, fixture.repository.runs)
	}
}

func TestServiceAbstainsOnHardGateAndThreshold(t *testing.T) {
	t.Run("hard gate", func(t *testing.T) {
		fixture := newServiceFixture(t, false)
		fixture.request.Input.ForbiddenEffects = []string{"filesystem.write"}
		response, err := fixture.service.Retrieve(context.Background(), fixture.request)
		if err != nil {
			t.Fatal(err)
		}
		if response.Disposition != RunAbstained || response.DecisionCode != "no_eligible_candidates" || fixture.repository.runs[0].Gates[0].Eligible {
			t.Fatalf("response = %#v run = %#v", response, fixture.repository.runs[0])
		}
	})
	t.Run("threshold", func(t *testing.T) {
		fixture := newServiceFixture(t, false)
		fixture.service.config.MinimumScore = math.MaxInt64
		response, err := fixture.service.Retrieve(context.Background(), fixture.request)
		if err != nil {
			t.Fatal(err)
		}
		if response.Disposition != RunAbstained || response.DecisionCode != "below_confidence_threshold" || len(fixture.repository.runs[0].Ranked) != 1 {
			t.Fatalf("response = %#v", response)
		}
	})
}

func TestServiceNeverServesUnpersistedDecisionOrCallsDormantJudge(t *testing.T) {
	fixture := newServiceFixture(t, false)
	judge := &countingJudge{}
	fixture.service.semantic = judge
	fixture.repository.saveErr = errors.New("database down")
	response, err := fixture.service.Retrieve(context.Background(), fixture.request)
	var serviceErr *ServiceError
	if response.Disposition != "" || !errors.As(err, &serviceErr) || serviceErr.Code != "run_persistence_failed" || judge.calls != 0 {
		t.Fatalf("response=%#v error=%v judge_calls=%d", response, err, judge.calls)
	}
}

func TestServiceDeniesRecallBeforeCandidateAccessWhenCurrentConsentIsFalse(t *testing.T) {
	fixture := newServiceFixture(t, false)
	fixture.request.RecallAllowed = false
	if _, err := fixture.service.Retrieve(context.Background(), fixture.request); !errors.Is(err, ErrRecallDenied) || fixture.exact.calls != 0 {
		t.Fatalf("error=%v channel_calls=%d", err, fixture.exact.calls)
	}
}

func TestServiceReplaysIdempotentRequestWithoutRerunningChannels(t *testing.T) {
	fixture := newServiceFixture(t, false)
	fixture.request.RequestIdentityHash = strings.Repeat("e", 64)
	first, err := fixture.service.Retrieve(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := fixture.service.Retrieve(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	if first.RunID != second.RunID || fixture.exact.calls != 1 || len(fixture.repository.runs) != 1 || len(second.Candidates) != 1 {
		t.Fatalf("first=%#v second=%#v calls=%d runs=%d", first, second, fixture.exact.calls, len(fixture.repository.runs))
	}
	changed := fixture.request
	changed.Input.Task = "different task"
	if _, err = fixture.service.Retrieve(context.Background(), changed); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed request conflict = %v", err)
	}
}

type serviceFixture struct {
	service    *Service
	repository *fakeDecisionRepository
	document   Document
	exact      *serviceChannel
	request    ServiceRequest
}

func newServiceFixture(t *testing.T, degradedVector bool) serviceFixture {
	t.Helper()
	now := time.Date(2026, 9, 20, 8, 0, 0, 0, time.UTC)
	intentHash, err := CanonicalIntentHash("release", Harness{Name: "ci", Version: "1"})
	if err != nil {
		t.Fatal(err)
	}
	_, environmentHash, err := canonical.MarshalAndHash([]Fact{})
	if err != nil {
		t.Fatal(err)
	}
	document, err := BuildDocument(DocumentInput{TenantID: "tenant_a", ProcedureVersionID: "pv_a", ProcedureID: "proc_a", TaskText: "release", IntentHash: intentHash, EffectSignatureHash: strings.Repeat("b", 64), Tools: []ToolRequirement{{Name: "shell", ContractVersionID: "tcv_shell"}}, OrderedStepContractIDs: []string{"tcv_shell"}, Effects: []string{"filesystem.write"}, EnvironmentScopeHash: environmentHash, Harness: Harness{Name: "ci", Version: "1"}, Lifecycle: "active", ObservedEndToEnd: true, VerificationStrength: 5, VerifiedSuccessCount: 3, ValidatedAt: now.Add(-time.Minute), ValidationPolicyVersion: "evidence.v1", LearnedWithRecallConsent: true, ResidencyRegion: "local", RiskClass: "medium"})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := eligibility.NewPolicy(eligibility.PolicySpec{Version: "eligibility.v1", AllowedLifecycle: []eligibility.Lifecycle{eligibility.LifecycleActive}, MaximumRisk: eligibility.RiskMedium, MaximumValidationAgeSeconds: 3600, CompatibleValidationPolicies: []string{"evidence.v1"}, AllowedResidencyRegions: []string{"local"}})
	if err != nil {
		t.Fatal(err)
	}
	ranker, err := ranking.NewManifest(ranking.ManifestSpec{Version: "ranker.v1", RRFK: 60, RRFCoefficient: 1, MaxCandidates: 100, Channels: []ranking.ChannelWeight{{Channel: "exact", WeightMicros: 1_000_000}, {Channel: "vector", WeightMicros: 100_000}}})
	if err != nil {
		t.Fatal(err)
	}
	exact := &serviceChannel{name: ChannelExact, manifest: "idx_exact", hits: []Hit{{VersionID: document.ProcedureVersionID, RawScoreQuantized: 1_000_000, IndexManifestID: "idx_exact"}}}
	channels := []Channel{exact}
	indexes := []SnapshotIndex{{Channel: ChannelExact, ManifestID: "idx_exact"}}
	if degradedVector {
		vector := &serviceChannel{name: ChannelVector, manifest: "idx_vector", approximate: true, err: &ChannelError{Code: "index_unavailable", Err: errors.New("down")}}
		channels = append(channels, vector)
		indexes = append(indexes, SnapshotIndex{Channel: ChannelVector, ManifestID: "idx_vector", Approximate: true})
	}
	config, err := BuildServingConfig("tenant_a", policy.ID, ranker.ID, indexes)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := ServingSnapshot{ProjectionEpoch: 1, DocumentSetHash: strings.Repeat("a", 64), ServingConfigID: config.ID, PolicyManifestID: policy.ID, RankerManifestID: ranker.ID, Indexes: config.Indexes}
	repository := &fakeDecisionRepository{snapshot: snapshot}
	service, err := NewService(repository, fakeDocumentReader{documents: []Document{document}}, fakeManifestResolver{policy: policy, ranker: ranker}, fixedEffectInferer{hash: document.EffectSignatureHash}, fixedCipher{}, fixedIDs{}, ServiceConfig{Channels: channels, RequiredChannels: []ChannelName{ChannelExact}, ChannelTimeout: time.Second, Retention: 24 * time.Hour, MaximumSelections: 1})
	if err != nil {
		t.Fatal(err)
	}
	service.clock = func() time.Time { return now }
	return serviceFixture{service: service, repository: repository, document: document, exact: exact, request: ServiceRequest{TenantID: "tenant_a", CurrentPolicyVersion: "policy.v1", RecallAllowed: true, AllowedResidencyRegions: []string{"local"}, Input: Input{Task: "release", Tools: []Tool{{Name: "shell", ContractVersionID: "tcv_shell"}}, Harness: Harness{Name: "ci", Version: "1"}, RiskClass: RiskMedium, LatencyClass: LatencyInteractive, MaxCandidates: 10}}}
}

type serviceChannel struct {
	name           ChannelName
	manifest       string
	approximate    bool
	hits           []Hit
	err            error
	calls          int
	seenEffectHash string
}

func (c *serviceChannel) Name() ChannelName  { return c.name }
func (c *serviceChannel) ManifestID() string { return c.manifest }
func (c *serviceChannel) Approximate() bool  { return c.approximate }
func (c *serviceChannel) Search(_ context.Context, request ChannelRequest) ([]Hit, error) {
	c.calls++
	c.seenEffectHash = request.Query.EffectSignatureHash
	return c.hits, c.err
}

type fakeDecisionRepository struct {
	snapshot ServingSnapshot
	runs     []Run
	saveErr  error
}

func (r *fakeDecisionRepository) AcquireServingSnapshot(context.Context, domain.TenantID) (ServingSnapshot, error) {
	return r.snapshot, nil
}
func (r *fakeDecisionRepository) SaveRetrievalRun(_ context.Context, run Run) error {
	if r.saveErr != nil {
		return r.saveErr
	}
	if ValidateRun(run) != nil {
		return ErrInvalidRun
	}
	r.runs = append(r.runs, run)
	return nil
}
func (r *fakeDecisionRepository) RetrievalRun(_ context.Context, _ domain.TenantID, id string) (Run, error) {
	for _, run := range r.runs {
		if run.ID == id {
			return run, nil
		}
	}
	return Run{}, ErrRunNotFound
}

type fakeDocumentReader struct {
	documents []Document
	err       error
}

func (r fakeDocumentReader) RetrievalDocuments(context.Context, domain.TenantID, []string, uint64) ([]Document, error) {
	return r.documents, r.err
}

type fakeManifestResolver struct {
	policy eligibility.Policy
	ranker ranking.Manifest
}

func (r fakeManifestResolver) EligibilityPolicy(context.Context, domain.TenantID, string) (eligibility.Policy, error) {
	return r.policy, nil
}
func (r fakeManifestResolver) Ranker(context.Context, domain.TenantID, string) (ranking.Manifest, error) {
	return r.ranker, nil
}

type fixedEffectInferer struct{ hash string }

func (r fixedEffectInferer) InferEffectSignature(context.Context, domain.TenantID, Query) (string, error) {
	return r.hash, nil
}

type fixedCipher struct{}

func (fixedCipher) Encrypt(context.Context, domain.TenantID, []byte) (string, error) {
	return "enc.v1.key.nonce.ciphertext", nil
}

type fixedIDs struct{}

func (fixedIDs) NewRunID() (string, error) { return "rrun_" + strings.Repeat("d", 64), nil }

type countingJudge struct{ calls int }

func (j *countingJudge) Judge(context.Context, SemanticJudgmentRequest) (SemanticJudgment, error) {
	j.calls++
	return SemanticJudgment{}, nil
}
