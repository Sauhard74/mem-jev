//go:build e2e

package e2e

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/gen/memjev/v1/memjevv1connect"
	"github.com/sauhard74/mem-jev/internal/api"
	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/eligibility"
	"github.com/sauhard74/mem-jev/internal/planning"
	"github.com/sauhard74/mem-jev/internal/policy"
	"github.com/sauhard74/mem-jev/internal/ranking"
	"github.com/sauhard74/mem-jev/internal/retrieval"
	"github.com/sauhard74/mem-jev/internal/security"
	"github.com/sauhard74/mem-jev/internal/selection"
	storememory "github.com/sauhard74/mem-jev/internal/store/memory"
)

func TestHostedFiveChannelRetrievalAndVectorQuarantine(t *testing.T) {
	now := time.Now().UTC()
	intentHash, err := retrieval.CanonicalIntentHash("write output", retrieval.Harness{Name: "e2e", Version: "1"})
	if err != nil {
		t.Fatal(err)
	}
	_, environmentHash, err := canonical.MarshalAndHash([]retrieval.Fact{})
	if err != nil {
		t.Fatal(err)
	}
	document, err := retrieval.BuildDocument(retrieval.DocumentInput{
		TenantID: "tenant_vector", ProcedureVersionID: "pv_vector", ProcedureID: "proc_vector", TaskText: "write output",
		IntentHash: intentHash, EffectSignatureHash: strings.Repeat("e", 64), Tools: []retrieval.ToolRequirement{{Name: "writer", ContractVersionID: "tcv_vector"}},
		OrderedStepContractIDs: []string{"tcv_vector"}, Effects: []string{"filesystem.write"}, EnvironmentScopeHash: environmentHash,
		Harness: retrieval.Harness{Name: "e2e", Version: "1"}, Lifecycle: "active", ObservedEndToEnd: true, VerificationStrength: 5,
		VerifiedSuccessCount: 3, ValidatedAt: now.Add(-time.Minute), ValidationPolicyVersion: "evidence.v1", LearnedWithRecallConsent: true, ResidencyRegion: "local", RiskClass: "low",
		Interface: e2eProcedureInterface(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	policyManifest, err := eligibility.NewPolicy(eligibility.PolicySpec{Version: "vector-hosted.v1", AllowedLifecycle: []eligibility.Lifecycle{eligibility.LifecycleActive}, MaximumRisk: eligibility.RiskLow, MaximumValidationAgeSeconds: 3600, CompatibleValidationPolicies: []string{"evidence.v1"}, AllowedResidencyRegions: []string{"local"}})
	if err != nil {
		t.Fatal(err)
	}
	weights := []ranking.ChannelWeight{{Channel: "exact", WeightMicros: 1_000_000}, {Channel: "lexical", WeightMicros: 800_000}, {Channel: "facet", WeightMicros: 600_000}, {Channel: "graph", WeightMicros: 400_000}, {Channel: "vector", WeightMicros: 200_000}}
	rankerManifest, err := ranking.NewManifest(ranking.ManifestSpec{Version: "vector-hosted.v1", RRFK: 60, RRFCoefficient: 1, MaxCandidates: 100, Channels: weights})
	if err != nil {
		t.Fatal(err)
	}
	indexes := []retrieval.SnapshotIndex{{Channel: retrieval.ChannelExact, ManifestID: "idx_exact"}, {Channel: retrieval.ChannelLexical, ManifestID: "idx_lexical"}, {Channel: retrieval.ChannelFacet, ManifestID: "idx_facet"}, {Channel: retrieval.ChannelGraph, ManifestID: "idx_graph"}, {Channel: retrieval.ChannelVector, ManifestID: "idx_vector", Approximate: true}}
	config, err := retrieval.BuildServingConfig("tenant_vector", policyManifest.ID, rankerManifest.ID, indexes)
	if err != nil {
		t.Fatal(err)
	}
	repository := &hostedRunRepository{snapshot: retrieval.ServingSnapshot{ProjectionEpoch: 7, ProjectionCreatedAt: now.Add(-time.Minute), VectorIndexCreatedAt: now.Add(-2 * time.Minute), DocumentSetHash: strings.Repeat("a", 64), ServingConfigID: config.ID, PolicyManifestID: policyManifest.ID, RankerManifestID: rankerManifest.ID, Indexes: config.Indexes}, runs: make(map[string]retrieval.Run)}
	vector := &hostedVectorChannel{mode: "success"}
	channels := []retrieval.Channel{
		hostedChannel{name: retrieval.ChannelExact, manifest: "idx_exact", versionID: document.ProcedureVersionID},
		hostedChannel{name: retrieval.ChannelLexical, manifest: "idx_lexical", versionID: document.ProcedureVersionID},
		hostedChannel{name: retrieval.ChannelFacet, manifest: "idx_facet", versionID: document.ProcedureVersionID},
		hostedChannel{name: retrieval.ChannelGraph, manifest: "idx_graph", versionID: document.ProcedureVersionID},
		vector,
	}
	selectionRepository, err := storememory.NewSelectionRepository(selection.RetentionPolicy{TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	planIssuer, err := planning.NewDeterministicIssuer(selectionRepository)
	if err != nil {
		t.Fatal(err)
	}
	service, err := retrieval.NewService(repository, hostedDocumentReader{document}, hostedManifests{policyManifest, rankerManifest}, hostedEffectInferer{document.EffectSignatureHash}, hostedCipher{}, hostedIDs{}, retrieval.ServiceConfig{Channels: channels, RequiredChannels: []retrieval.ChannelName{retrieval.ChannelExact, retrieval.ChannelLexical, retrieval.ChannelFacet, retrieval.ChannelGraph}, ChannelTimeout: time.Second, Retention: time.Hour, MaximumSelections: 1}, planIssuer)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(api.NewHandler(api.Dependencies{Retrieval: service, RetrievalPolicyVersion: "policy.v1", Authenticator: hostedAuthenticator{}, Timeout: 5 * time.Second}))
	defer server.Close()
	client := memjevv1connect.NewRetrievalServiceClient(server.Client(), server.URL)

	success := hostedVectorRetrieve(t, client, "vector-success-key-0001")
	if !success.Msg.GetProvenance().GetApproximateCandidates() || len(success.Msg.GetProvenance().GetIndexManifestIds()) != 5 || len(success.Msg.GetProvenance().GetDegradedChannels()) != 0 {
		t.Fatalf("five-channel success provenance = %#v", success.Msg.GetProvenance())
	}
	explain := connect.NewRequest(&memjevv1.ExplainRetrievalRequest{RetrievalRunId: success.Msg.GetRetrievalRunId()})
	explain.Header().Set("Authorization", "Bearer hosted-vector-token")
	explanation, err := client.ExplainRetrieval(context.Background(), explain)
	if err != nil || len(explanation.Msg.GetCandidates()) != 1 || !containsString(explanation.Msg.GetCandidates()[0].GetSourceChannels(), "vector") {
		t.Fatalf("vector explanation=%#v error=%v", explanation, err)
	}

	vector.setMode("outage")
	outage := hostedVectorRetrieve(t, client, "vector-outage-key-0002")
	if outage.Msg.GetProvenance().GetApproximateCandidates() || !containsString(outage.Msg.GetProvenance().GetDegradedChannels(), "vector:provider_unavailable") {
		t.Fatalf("vector outage provenance = %#v", outage.Msg.GetProvenance())
	}

	vector.setMode("corrupt")
	corrupt := hostedVectorRetrieve(t, client, "vector-corrupt-key-0003")
	if corrupt.Msg.GetProvenance().GetApproximateCandidates() || !containsString(corrupt.Msg.GetProvenance().GetDegradedChannels(), "vector:malformed_result") {
		t.Fatalf("vector corruption provenance = %#v", corrupt.Msg.GetProvenance())
	}

	repository.mu.Lock()
	defer repository.mu.Unlock()
	if len(repository.runs) != 3 {
		t.Fatalf("persisted runs = %d", len(repository.runs))
	}
	for _, run := range repository.runs {
		if len(run.Snapshot.Indexes) != 5 || len(run.ChannelExecutions) != 5 {
			t.Fatalf("persisted five-channel provenance = %#v", run)
		}
	}
}

func hostedVectorRetrieve(t *testing.T, client memjevv1connect.RetrievalServiceClient, key string) *connect.Response[memjevv1.RetrieveResponse] {
	t.Helper()
	request := connect.NewRequest(&memjevv1.RetrieveRequest{Task: "write output", Tools: []*memjevv1.AvailableTool{{Name: "writer", ContractVersionId: "tcv_vector"}}, Harness: &memjevv1.HarnessIdentity{Name: "e2e", Version: "1"}, RiskClass: memjevv1.RiskClass_RISK_CLASS_LOW, LatencyClass: memjevv1.LatencyClass_LATENCY_CLASS_INTERACTIVE, MaxCandidates: 10})
	request.Header().Set("Authorization", "Bearer hosted-vector-token")
	request.Header().Set("Idempotency-Key", key)
	response, err := client.Retrieve(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if response.Msg.GetDisposition() != memjevv1.RetrievalDisposition_RETRIEVAL_DISPOSITION_SELECTED || len(response.Msg.GetCandidates()) != 1 {
		t.Fatalf("response = %#v", response.Msg)
	}
	return response
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

type hostedAuthenticator struct{}

func (hostedAuthenticator) Authenticate(http.Header) (security.Principal, error) {
	return security.Principal{TenantID: "tenant_vector", Region: "local", Scopes: map[string]struct{}{security.ScopeRetrievalRead: {}}, Consent: policy.LearnAndRecall}, nil
}

type hostedRunRepository struct {
	mu       sync.Mutex
	snapshot retrieval.ServingSnapshot
	runs     map[string]retrieval.Run
}

func (r *hostedRunRepository) AcquireServingSnapshot(context.Context, domain.TenantID) (retrieval.ServingSnapshot, error) {
	return r.snapshot, nil
}
func (r *hostedRunRepository) SaveRetrievalRun(_ context.Context, run retrieval.Run) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if retrieval.ValidateRun(run) != nil {
		return retrieval.ErrInvalidRun
	}
	if existing, ok := r.runs[run.ID]; ok && existing.ContentHash != run.ContentHash {
		return errors.New("run conflict")
	}
	r.runs[run.ID] = run
	return nil
}
func (r *hostedRunRepository) RetrievalRun(_ context.Context, _ domain.TenantID, id string) (retrieval.Run, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	run, ok := r.runs[id]
	if !ok {
		return retrieval.Run{}, retrieval.ErrRunNotFound
	}
	return retrieval.DecodeRun(run.CanonicalJSON, run.ContentHash)
}

type hostedDocumentReader struct{ document retrieval.Document }

func (r hostedDocumentReader) RetrievalDocuments(context.Context, domain.TenantID, []string, uint64) ([]retrieval.Document, error) {
	return []retrieval.Document{r.document}, nil
}

type hostedManifests struct {
	policy eligibility.Policy
	ranker ranking.Manifest
}

func (m hostedManifests) EligibilityPolicy(context.Context, domain.TenantID, string) (eligibility.Policy, error) {
	return m.policy, nil
}
func (m hostedManifests) Ranker(context.Context, domain.TenantID, string) (ranking.Manifest, error) {
	return m.ranker, nil
}

type hostedChannel struct {
	name                retrieval.ChannelName
	manifest, versionID string
}

func (c hostedChannel) Name() retrieval.ChannelName { return c.name }
func (c hostedChannel) ManifestID() string          { return c.manifest }
func (c hostedChannel) Approximate() bool           { return false }
func (c hostedChannel) Search(context.Context, retrieval.ChannelRequest) ([]retrieval.Hit, error) {
	return []retrieval.Hit{{VersionID: c.versionID, RawScoreQuantized: 1_000_000, IndexManifestID: c.manifest}}, nil
}

type hostedVectorChannel struct {
	mu   sync.Mutex
	mode string
}

func (c *hostedVectorChannel) Name() retrieval.ChannelName { return retrieval.ChannelVector }
func (c *hostedVectorChannel) ManifestID() string          { return "idx_vector" }
func (c *hostedVectorChannel) Approximate() bool           { return true }
func (c *hostedVectorChannel) setMode(mode string) {
	c.mu.Lock()
	c.mode = mode
	c.mu.Unlock()
}
func (c *hostedVectorChannel) Search(context.Context, retrieval.ChannelRequest) ([]retrieval.Hit, error) {
	c.mu.Lock()
	mode := c.mode
	c.mu.Unlock()
	switch mode {
	case "outage":
		return nil, &retrieval.ChannelError{Code: "provider_unavailable", Err: errors.New("provider unavailable")}
	case "corrupt":
		return []retrieval.Hit{{VersionID: "pv_vector", IndexManifestID: "idx_corrupt", Approximate: true}}, nil
	default:
		return []retrieval.Hit{{VersionID: "pv_vector", RawScoreQuantized: 900_000, IndexManifestID: "idx_vector", Approximate: true}}, nil
	}
}

type hostedEffectInferer struct{ hash string }

func (i hostedEffectInferer) InferEffectSignature(context.Context, domain.TenantID, retrieval.Query) (string, error) {
	return i.hash, nil
}

type hostedCipher struct{}

func (hostedCipher) Encrypt(context.Context, domain.TenantID, []byte) (string, error) {
	return "enc.v1.test.nonce.ciphertext", nil
}

type hostedIDs struct{}

func (hostedIDs) NewRunID() (string, error) { return "rrun_" + strings.Repeat("f", 64), nil }
