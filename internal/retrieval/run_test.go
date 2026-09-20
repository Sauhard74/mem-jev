package retrieval_test

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/retrieval"
)

func TestBuildRunCanonicalizesAllDecisionRecords(t *testing.T) {
	input := validRunInput(t)
	input.ChannelExecutions = []retrieval.ChannelExecution{input.ChannelExecutions[1], input.ChannelExecutions[0]}
	input.Hits = []retrieval.PersistedHit{input.Hits[1], input.Hits[0]}
	first, err := retrieval.BuildRun(input)
	if err != nil {
		t.Fatal(err)
	}
	second, err := retrieval.BuildRun(validRunInput(t))
	if err != nil {
		t.Fatal(err)
	}
	if first.ContentHash != second.ContentHash || !reflect.DeepEqual(first.CanonicalJSON, second.CanonicalJSON) {
		t.Fatalf("run canonicalization differs:\n%s\n%s", first.CanonicalJSON, second.CanonicalJSON)
	}
	if first.CandidateVersionIDs[0] != "pv_a" || first.IndexManifestIDs[0] != "idx_exact" || first.IndexManifestIDs[1] != "idx_vector" {
		t.Fatalf("derived run fields are unstable: %#v", first)
	}
}

func TestBuildRunRejectsIncompleteOrContradictoryDecisions(t *testing.T) {
	tests := []func(*retrieval.RunInput){
		func(value *retrieval.RunInput) { value.QueryEnvelope = "plaintext" },
		func(value *retrieval.RunInput) { value.Hits[0].Rank = 2 },
		func(value *retrieval.RunInput) { value.Hits[0].IndexManifestID = "wrong" },
		func(value *retrieval.RunInput) { value.ChannelExecutions[0].HitCount = 99 },
		func(value *retrieval.RunInput) { value.Gates[0].Eligible = false },
		func(value *retrieval.RunInput) { value.Ranked[0].Rank = 2 },
		func(value *retrieval.RunInput) { value.SelectedVersionIDs = []string{"missing"} },
		func(value *retrieval.RunInput) { value.ExpiresAt = value.CompletedAt.Add(31 * 24 * time.Hour) },
		func(value *retrieval.RunInput) { value.Snapshot.ProjectionEpoch = 0 },
		func(value *retrieval.RunInput) {
			value.Disposition = retrieval.RunAbstained
			value.SelectedVersionIDs = nil
			value.DecisionCode = ""
		},
	}
	for index, mutate := range tests {
		input := validRunInput(t)
		mutate(&input)
		if _, err := retrieval.BuildRun(input); !errors.Is(err, retrieval.ErrInvalidRun) {
			t.Fatalf("case %d: expected invalid run, got %v", index, err)
		}
	}
}

func TestBuildFailedRunRequiresAuditCodeButNoCandidates(t *testing.T) {
	input := validRunInput(t)
	input.Disposition = retrieval.RunFailed
	input.DecisionCode = "required_channel_unavailable"
	input.ChannelExecutions = nil
	input.Hits = nil
	input.Gates = nil
	input.Ranked = nil
	input.SelectedVersionIDs = nil
	run, err := retrieval.BuildRun(input)
	if err != nil {
		t.Fatal(err)
	}
	if run.Disposition != retrieval.RunFailed || !strings.Contains(string(run.CanonicalJSON), "required_channel_unavailable") {
		t.Fatalf("failed audit run = %#v", run)
	}
}

func validRunInput(t *testing.T) retrieval.RunInput {
	t.Helper()
	query, err := retrieval.BuildQuery("tenant_a", "policy.v1", retrieval.AliasSet{}, retrieval.Input{
		Task: "release", Tools: []retrieval.Tool{{Name: "shell", ContractVersionID: "tcv_shell"}}, Harness: retrieval.Harness{Name: "ci", Version: "1"},
		RiskClass: retrieval.RiskMedium, LatencyClass: retrieval.LatencyInteractive, MaxCandidates: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	created := time.Date(2026, 9, 20, 1, 2, 3, 0, time.UTC)
	return retrieval.RunInput{
		ID: "rrun_" + strings.Repeat("a", 64), TenantID: "tenant_a", Query: query, RequestContextHash: strings.Repeat("d", 64), QueryEnvelope: "enc.v1.key-a.nonce.ciphertext",
		Snapshot:          retrieval.ServingSnapshot{ProjectionEpoch: 7, DocumentSetHash: strings.Repeat("b", 64), ServingConfigID: "rsc_" + strings.Repeat("c", 64), PolicyManifestID: "pol_1", RankerManifestID: "rnk_1", Indexes: []retrieval.SnapshotIndex{{Channel: retrieval.ChannelVector, ManifestID: "idx_vector", Approximate: true}, {Channel: retrieval.ChannelExact, ManifestID: "idx_exact"}}},
		ChannelExecutions: []retrieval.ChannelExecution{{Channel: retrieval.ChannelExact, IndexManifestID: "idx_exact", HitCount: 1, Complete: true}, {Channel: retrieval.ChannelVector, IndexManifestID: "idx_vector", HitCount: 1, Approximate: true, Complete: true}},
		Hits:              []retrieval.PersistedHit{{Channel: retrieval.ChannelExact, VersionID: "pv_a", Rank: 1, RawScoreQuantized: 100, IndexManifestID: "idx_exact"}, {Channel: retrieval.ChannelVector, VersionID: "pv_a", Rank: 1, RawScoreQuantized: 90, IndexManifestID: "idx_vector", Approximate: true}},
		Gates:             []retrieval.PersistedGate{{VersionID: "pv_a", Eligible: true, CanonicalFacts: `{"policy":"allow"}`}},
		Ranked:            []retrieval.PersistedRank{{VersionID: "pv_a", RRFScore: 10, FinalScore: 20, Rank: 1, VerificationStrength: 5, ObservedEndToEnd: true, Features: []retrieval.PersistedFeature{{Name: "success", Value: 1}}}},
		Disposition:       retrieval.RunSelected, SelectedVersionIDs: []string{"pv_a"}, CreatedAt: created, CompletedAt: created.Add(time.Millisecond), ExpiresAt: created.Add(7 * 24 * time.Hour),
	}
}
