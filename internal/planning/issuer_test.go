package planning_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/planning"
	"github.com/sauhard74/mem-jev/internal/retrieval"
	"github.com/sauhard74/mem-jev/internal/selection"
	storememory "github.com/sauhard74/mem-jev/internal/store/memory"
)

func TestDeterministicIssuerCommitsAndReplaysAgentPlan(t *testing.T) {
	now := time.Date(2026, 9, 20, 3, 0, 0, 0, time.UTC)
	procedureInterface, err := retrieval.NewProcedureInterface(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	intentHash, err := retrieval.CanonicalIntentHash("deploy", retrieval.Harness{Name: "codex", Version: "1"})
	if err != nil {
		t.Fatal(err)
	}
	_, environmentHash, err := canonical.MarshalAndHash([]retrieval.Fact{})
	if err != nil {
		t.Fatal(err)
	}
	document, err := retrieval.BuildDocument(retrieval.DocumentInput{
		TenantID: "tenant_a", ProcedureVersionID: "pv_a", ProcedureID: "proc_a", TaskText: "deploy", IntentHash: intentHash,
		EffectSignatureHash: strings.Repeat("e", 64), Tools: []retrieval.ToolRequirement{{Name: "shell", ContractVersionID: "v1"}},
		OrderedStepContractIDs: []string{"v1"}, EnvironmentScopeHash: environmentHash, Harness: retrieval.Harness{Name: "codex", Version: "1"},
		Lifecycle: "active", ObservedEndToEnd: true, VerificationStrength: 5, VerifiedSuccessCount: 1, ValidatedAt: now.Add(-time.Minute),
		ValidationPolicyVersion: "evidence.v1", LearnedWithRecallConsent: true, ResidencyRegion: "local", RiskClass: "low", Interface: &procedureInterface,
	})
	if err != nil {
		t.Fatal(err)
	}
	query, err := retrieval.BuildQuery("tenant_a", "policy.v1", retrieval.AliasSet{}, retrieval.Input{Task: "deploy", Tools: []retrieval.Tool{{Name: "shell", ContractVersionID: "v1"}}, Harness: retrieval.Harness{Name: "codex", Version: "1"}, RiskClass: retrieval.RiskLow, LatencyClass: retrieval.LatencyInteractive, MaxCandidates: 10})
	if err != nil {
		t.Fatal(err)
	}
	serving, err := retrieval.BuildServingConfig("tenant_a", "policy_1", "ranker_1", []retrieval.SnapshotIndex{{Channel: retrieval.ChannelExact, ManifestID: "idx_exact"}})
	if err != nil {
		t.Fatal(err)
	}
	run, err := retrieval.BuildRun(retrieval.RunInput{
		ID: "rrun_" + strings.Repeat("a", 64), TenantID: "tenant_a", Query: query, RequestContextHash: strings.Repeat("b", 64), QueryEnvelope: "enc.v1.test",
		Snapshot:          retrieval.ServingSnapshot{ProjectionEpoch: 1, DocumentSetHash: strings.Repeat("c", 64), ServingConfigID: serving.ID, PolicyManifestID: "policy_1", RankerManifestID: "ranker_1", Indexes: serving.Indexes},
		ChannelExecutions: []retrieval.ChannelExecution{{Channel: retrieval.ChannelExact, IndexManifestID: "idx_exact", HitCount: 1, Complete: true}},
		Hits:              []retrieval.PersistedHit{{Channel: retrieval.ChannelExact, VersionID: "pv_a", Rank: 1, IndexManifestID: "idx_exact"}},
		Gates:             []retrieval.PersistedGate{{VersionID: "pv_a", Eligible: true, CanonicalFacts: "[]"}},
		Ranked:            []retrieval.PersistedRank{{VersionID: "pv_a", Rank: 1, VerificationStrength: 5, ObservedEndToEnd: true}},
		Disposition:       retrieval.RunSelected, SelectedVersionIDs: []string{"pv_a"}, CreatedAt: now, CompletedAt: now, ExpiresAt: now.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	repository, err := storememory.NewSelectionRepository(selection.RetentionPolicy{TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	issuer, err := planning.NewDeterministicIssuer(repository)
	if err != nil {
		t.Fatal(err)
	}
	first, err := issuer.Issue(context.Background(), run, []retrieval.Document{document})
	if err != nil {
		t.Fatal(err)
	}
	second, err := issuer.Issue(context.Background(), run, []retrieval.Document{document})
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := issuer.FindByRetrievalRunID(context.Background(), run.TenantID, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if first.InjectionID == "" || first.SelectionHash != second.SelectionHash || first.SelectionHash != replayed.SelectionHash || !first.Complete || first.NoveltyClass != "exact" || len(first.Nodes) != 1 || len(first.ParallelGroups) != 1 {
		t.Fatalf("first=%#v second=%#v replayed=%#v", first, second, replayed)
	}
}
