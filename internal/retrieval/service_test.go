package retrieval

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
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
	if response.Disposition != RunSelected || response.Plan == nil || len(response.Candidates) != 1 || response.Candidates[0].VersionID != fixture.document.ProcedureVersionID || len(fixture.repository.runs) != 1 {
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

func TestServiceDoesNotExecuteConfiguredChannelAbsentFromServingSnapshot(t *testing.T) {
	fixture := newServiceFixture(t, false)
	vector := &serviceChannel{name: ChannelVector, manifest: "idx_vector", approximate: true, err: errors.New("must not execute")}
	fixture.service.config.Channels = append(fixture.service.config.Channels, vector)
	response, err := fixture.service.Retrieve(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	if response.Disposition != RunSelected || vector.calls != 0 || len(fixture.repository.runs[0].ChannelExecutions) != 1 {
		t.Fatalf("response=%#v vector_calls=%d run=%#v", response, vector.calls, fixture.repository.runs[0])
	}
}

func TestServiceFailsClosedWhenSnapshotChannelManifestIsNotConfigured(t *testing.T) {
	fixture := newServiceFixture(t, false)
	fixture.repository.snapshot.Indexes[0].ManifestID = "idx_exact_unavailable"
	_, err := fixture.service.Retrieve(context.Background(), fixture.request)
	var serviceErr *ServiceError
	if !errors.As(err, &serviceErr) || serviceErr.Code != "serving_config_mismatch" || fixture.exact.calls != 0 {
		t.Fatalf("error=%v channel_calls=%d", err, fixture.exact.calls)
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

func TestServiceNeverServesUnpersistedDecisionOrCallsSemanticJudgeWithoutTenantConsent(t *testing.T) {
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

func TestServiceAppliesSemanticFeaturesOnlyAfterEligibilityAndReplaysWithoutCallingJudge(t *testing.T) {
	fixture := newSemanticServiceFixture(t)
	fixture.request.ExternalInferenceAllowed = true
	fixture.request.RequestIdentityHash = strings.Repeat("e", 64)
	first, err := fixture.service.Retrieve(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Disposition != RunSelected || len(first.Candidates) != 1 || first.Candidates[0].VersionID != "pv_b" || fixture.judge.calls != 2 {
		t.Fatalf("response=%#v judge_calls=%d", first, fixture.judge.calls)
	}
	run := fixture.repository.runs[0]
	if len(run.SemanticJudgments) != 2 || len(run.Ranked) != 2 || run.Ranked[0].VersionID != "pv_b" {
		t.Fatalf("run=%#v", run)
	}
	second, err := fixture.service.Retrieve(context.Background(), fixture.request)
	if err != nil || !second.Replayed || fixture.judge.calls != 2 || second.Candidates[0].VersionID != "pv_b" {
		t.Fatalf("replay=%#v err=%v judge_calls=%d", second, err, fixture.judge.calls)
	}

	blocked := newSemanticServiceFixture(t)
	blocked.request.Input.ForbiddenEffects = []string{"filesystem.write"}
	blocked.request.ExternalInferenceAllowed = true
	result, err := blocked.service.Retrieve(context.Background(), blocked.request)
	if err != nil || result.Disposition != RunAbstained || blocked.judge.calls != 0 {
		t.Fatalf("blocked response=%#v err=%v calls=%d", result, err, blocked.judge.calls)
	}
}

func TestServiceCommitsRetrievalBeforePlanAndNeverServesUncommittedPlan(t *testing.T) {
	fixture := newServiceFixture(t, false)
	fixture.plans.onIssue = func() {
		if len(fixture.repository.runs) != 1 {
			t.Fatal("plan issuer ran before retrieval decision committed")
		}
	}
	fixture.plans.issueErr = errors.New("selection store unavailable")
	response, err := fixture.service.Retrieve(context.Background(), fixture.request)
	var serviceErr *ServiceError
	if response.Disposition != "" || !errors.As(err, &serviceErr) || serviceErr.Code != "plan_persistence_failed" || len(fixture.repository.runs) != 1 {
		t.Fatalf("response=%#v error=%v runs=%d", response, err, len(fixture.repository.runs))
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

func TestServiceRejectsReplayWhenEligibilityContextChanges(t *testing.T) {
	fixture := newServiceFixture(t, false)
	fixture.request.RequestIdentityHash = strings.Repeat("e", 64)
	if _, err := fixture.service.Retrieve(context.Background(), fixture.request); err != nil {
		t.Fatal(err)
	}
	changed := fixture.request
	changed.AllowedResidencyRegions = []string{"other"}
	if _, err := fixture.service.Retrieve(context.Background(), changed); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("eligibility-context conflict = %v", err)
	}
	if fixture.exact.calls != 1 {
		t.Fatalf("channel calls = %d, want 1", fixture.exact.calls)
	}
}

func TestServiceConvergesConcurrentIdempotentRequestsOnPersistedWinner(t *testing.T) {
	fixture := newServiceFixture(t, false)
	fixture.request.RequestIdentityHash = strings.Repeat("e", 64)
	repository := &racingDecisionRepository{snapshot: fixture.repository.snapshot, initialLookups: make(chan struct{})}
	fixture.service.repository = repository

	responses := make(chan ServiceResponse, 2)
	errorsSeen := make(chan error, 2)
	var workers sync.WaitGroup
	workers.Add(2)
	for range 2 {
		go func() {
			defer workers.Done()
			response, err := fixture.service.Retrieve(context.Background(), fixture.request)
			responses <- response
			errorsSeen <- err
		}()
	}
	workers.Wait()
	close(responses)
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Fatalf("concurrent retrieve: %v", err)
		}
	}
	var runID string
	for response := range responses {
		if response.Disposition != RunSelected || len(response.Candidates) != 1 {
			t.Fatalf("response = %#v", response)
		}
		if runID != "" && response.RunID != runID {
			t.Fatalf("run ids differ: %s != %s", response.RunID, runID)
		}
		runID = response.RunID
	}
	if repository.saveConflicts != 1 {
		t.Fatalf("save conflicts = %d, want 1", repository.saveConflicts)
	}
}

type serviceFixture struct {
	service    *Service
	repository *fakeDecisionRepository
	document   Document
	exact      *serviceChannel
	plans      *fakePlanIssuer
	request    ServiceRequest
}

type semanticServiceFixture struct {
	service    *Service
	repository *fakeDecisionRepository
	judge      *countingJudge
	request    ServiceRequest
}

func newSemanticServiceFixture(t *testing.T) semanticServiceFixture {
	t.Helper()
	base := newServiceFixture(t, false)
	now := time.Date(2026, 9, 20, 8, 0, 0, 0, time.UTC)
	procedureInterface, err := NewProcedureInterface(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuildDocument(DocumentInput{TenantID: "tenant_a", ProcedureVersionID: "pv_b", ProcedureID: "proc_b", TaskText: "release", IntentHash: base.document.IntentHash, EffectSignatureHash: base.document.EffectSignatureHash, Tools: []ToolRequirement{{Name: "shell", ContractVersionID: "tcv_shell"}}, OrderedStepContractIDs: []string{"tcv_shell"}, Effects: []string{"filesystem.write"}, EnvironmentScopeHash: base.document.EnvironmentScopeHash, Harness: Harness{Name: "ci", Version: "1"}, Lifecycle: "active", ObservedEndToEnd: true, VerificationStrength: 5, VerifiedSuccessCount: 3, ValidatedAt: now.Add(-time.Minute), ValidationPolicyVersion: "evidence.v1", LearnedWithRecallConsent: true, ResidencyRegion: "local", RiskClass: "medium", Interface: &procedureInterface})
	if err != nil {
		t.Fatal(err)
	}
	ranker, err := ranking.NewManifest(ranking.ManifestSpec{Version: "ranker.semantic.v1", RRFK: 60, RRFCoefficient: 1, MaxCandidates: 100, Channels: []ranking.ChannelWeight{{Channel: "exact", WeightMicros: 1_000_000}}, Features: []ranking.FeatureSpec{
		{Name: "jev_intent_fit_micros", Minimum: 0, Maximum: 1_000_000, Missing: 0, Coefficient: 100_000},
		{Name: "jev_non_contradiction_micros", Minimum: 0, Maximum: 1_000_000, Missing: 0, Coefficient: 1},
		{Name: "jev_partial_plan_micros", Minimum: 0, Maximum: 1_000_000, Missing: 0, Coefficient: 1},
		{Name: "jev_preconditions_micros", Minimum: 0, Maximum: 1_000_000, Missing: 0, Coefficient: 1},
		{Name: "jev_task_coverage_micros", Minimum: 0, Maximum: 1_000_000, Missing: 0, Coefficient: 1},
	}})
	if err != nil {
		t.Fatal(err)
	}
	base.service.manifests = fakeManifestResolver{policy: base.service.manifests.(fakeManifestResolver).policy, ranker: ranker}
	base.repository.snapshot.RankerManifestID = ranker.ID
	base.service.documents = fakeDocumentReader{documents: []Document{base.document, second}}
	base.exact.hits = []Hit{{VersionID: base.document.ProcedureVersionID, RawScoreQuantized: 1_000_000, IndexManifestID: "idx_exact"}, {VersionID: second.ProcedureVersionID, RawScoreQuantized: 900_000, IndexManifestID: "idx_exact"}}
	judge := &countingJudge{admitted: []string{"pv_a", "pv_b"}}
	base.service.semantic = judge
	return semanticServiceFixture{service: base.service, repository: base.repository, judge: judge, request: base.request}
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
	procedureInterface, err := NewProcedureInterface(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	document, err := BuildDocument(DocumentInput{TenantID: "tenant_a", ProcedureVersionID: "pv_a", ProcedureID: "proc_a", TaskText: "release", IntentHash: intentHash, EffectSignatureHash: strings.Repeat("b", 64), Tools: []ToolRequirement{{Name: "shell", ContractVersionID: "tcv_shell"}}, OrderedStepContractIDs: []string{"tcv_shell"}, Effects: []string{"filesystem.write"}, EnvironmentScopeHash: environmentHash, Harness: Harness{Name: "ci", Version: "1"}, Lifecycle: "active", ObservedEndToEnd: true, VerificationStrength: 5, VerifiedSuccessCount: 3, ValidatedAt: now.Add(-time.Minute), ValidationPolicyVersion: "evidence.v1", LearnedWithRecallConsent: true, ResidencyRegion: "local", RiskClass: "medium", Interface: &procedureInterface})
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
	issuer := &fakePlanIssuer{}
	service, err := NewService(repository, fakeDocumentReader{documents: []Document{document}}, fakeManifestResolver{policy: policy, ranker: ranker}, fixedEffectInferer{hash: document.EffectSignatureHash}, fixedCipher{}, fixedIDs{}, ServiceConfig{Channels: channels, RequiredChannels: []ChannelName{ChannelExact}, ChannelTimeout: time.Second, Retention: 24 * time.Hour, MaximumSelections: 1}, issuer)
	if err != nil {
		t.Fatal(err)
	}
	service.clock = func() time.Time { return now }
	return serviceFixture{service: service, repository: repository, document: document, exact: exact, plans: issuer, request: ServiceRequest{TenantID: "tenant_a", CurrentPolicyVersion: "policy.v1", RecallAllowed: true, AllowedResidencyRegions: []string{"local"}, Input: Input{Task: "release", Tools: []Tool{{Name: "shell", ContractVersionID: "tcv_shell"}}, Harness: Harness{Name: "ci", Version: "1"}, RiskClass: RiskMedium, LatencyClass: LatencyInteractive, MaxCandidates: 10}}}
}

type serviceChannel struct {
	mu             sync.Mutex
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
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	c.seenEffectHash = request.Query.EffectSignatureHash
	return c.hits, c.err
}

type racingDecisionRepository struct {
	mu             sync.Mutex
	snapshot       ServingSnapshot
	run            *Run
	lookupCount    int
	initialLookups chan struct{}
	saveConflicts  int
}

func (r *racingDecisionRepository) AcquireServingSnapshot(context.Context, domain.TenantID) (ServingSnapshot, error) {
	return r.snapshot, nil
}

func (r *racingDecisionRepository) RetrievalRun(_ context.Context, _ domain.TenantID, _ string) (Run, error) {
	r.mu.Lock()
	if r.run != nil {
		run := *r.run
		r.mu.Unlock()
		return run, nil
	}
	r.lookupCount++
	if r.lookupCount == 2 {
		close(r.initialLookups)
	}
	barrier := r.initialLookups
	r.mu.Unlock()
	<-barrier
	return Run{}, ErrRunNotFound
}

func (r *racingDecisionRepository) SaveRetrievalRun(_ context.Context, run Run) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.run == nil {
		r.run = &run
		return nil
	}
	r.saveConflicts++
	return errors.New("concurrent insert conflict")
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

func (r fakeDocumentReader) RetrievalDocuments(_ context.Context, _ domain.TenantID, ids []string, _ uint64) ([]Document, error) {
	if r.err != nil {
		return nil, r.err
	}
	requested := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		requested[id] = struct{}{}
	}
	result := make([]Document, 0, len(ids))
	for _, document := range r.documents {
		if _, ok := requested[document.ProcedureVersionID]; ok {
			result = append(result, document)
		}
	}
	return result, nil
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

type countingJudge struct {
	mu       sync.Mutex
	calls    int
	admitted []string
}

func (j *countingJudge) Admit([]ranking.Result) ([]string, error) {
	return append([]string(nil), j.admitted...), nil
}

func (j *countingJudge) Judge(_ context.Context, request SemanticJudgmentRequest) SemanticJudgment {
	j.mu.Lock()
	j.calls++
	j.mu.Unlock()
	value := int32(0)
	if request.Document.ProcedureVersionID == "pv_b" {
		value = 1_000_000
	}
	return SemanticJudgment{VersionID: request.Document.ProcedureVersionID, JudgmentKey: "jevj_" + strings.Repeat("a", 64), Disposition: "committed", ContentHash: strings.Repeat("b", 64), Provider: "typesafe", Model: "jev-1.13.0", RubricManifestID: "jevr_test", Features: []ranking.FeatureValue{{Name: "jev_task_coverage_micros", Value: value}, {Name: "jev_preconditions_micros", Value: 1_000_000}, {Name: "jev_partial_plan_micros", Value: 500_000}, {Name: "jev_non_contradiction_micros", Value: 1_000_000}, {Name: "jev_intent_fit_micros", Value: value}}}
}

type fakePlanIssuer struct {
	mu       sync.Mutex
	plan     *PlanArtifact
	onIssue  func()
	issueErr error
}

func (r *fakePlanIssuer) FindByRetrievalRunID(_ context.Context, tenantID domain.TenantID, runID string) (PlanArtifact, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.plan == nil || r.plan.TenantID != tenantID || r.plan.RetrievalRunID != runID {
		return PlanArtifact{}, ErrPlanNotFound
	}
	return *r.plan, nil
}

func (r *fakePlanIssuer) Issue(_ context.Context, run Run, _ []Document) (PlanArtifact, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.onIssue != nil {
		r.onIssue()
	}
	if r.issueErr != nil {
		return PlanArtifact{}, r.issueErr
	}
	if r.plan != nil {
		return *r.plan, nil
	}
	plan := PlanArtifact{InjectionID: "inj_test", TaskExecutionID: "texec_test", SelectionHash: strings.Repeat("1", 64), PlanHash: strings.Repeat("2", 64), TenantID: run.TenantID, RetrievalRunID: run.ID, QueryHash: run.QueryHash, RequestContextHash: run.RequestContextHash, ProjectionEpoch: run.Snapshot.ProjectionEpoch, Complete: true, NoveltyClass: "exact", Nodes: []PlanNode{{VersionID: run.SelectedVersionIDs[0], InterfaceHash: strings.Repeat("3", 64)}}}
	r.plan = &plan
	return plan, nil
}
