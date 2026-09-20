package storetest

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/retrieval"
	"github.com/sauhard74/mem-jev/internal/store"
)

type RetrievalRunFactory func(*testing.T) store.RetrievalRunRepository

func RunRetrievalRunContract(t *testing.T, factory RetrievalRunFactory) {
	t.Helper()
	t.Run("snapshot save replay and duplicate", func(t *testing.T) {
		repository := factory(t)
		config := ValidServingConfig(t, "tenant_a")
		if err := repository.ActivateServingConfig(context.Background(), config); err != nil {
			t.Fatal(err)
		}
		snapshot, err := repository.AcquireServingSnapshot(context.Background(), "tenant_a")
		if err != nil || snapshot.ServingConfigID != config.ID || snapshot.ProjectionEpoch == 0 {
			t.Fatalf("snapshot = %#v, err = %v", snapshot, err)
		}
		run := ValidRetrievalRun(t, snapshot, 'a')
		if err = repository.SaveRetrievalRun(context.Background(), run); err != nil {
			t.Fatal(err)
		}
		if err = repository.SaveRetrievalRun(context.Background(), run); err != nil {
			t.Fatalf("duplicate save: %v", err)
		}
		loaded, err := repository.RetrievalRun(context.Background(), "tenant_a", run.ID)
		if err != nil || loaded.ContentHash != run.ContentHash || string(loaded.CanonicalJSON) != string(run.CanonicalJSON) {
			t.Fatalf("loaded = %#v, err = %v", loaded, err)
		}
		if _, err = repository.RetrievalRun(context.Background(), "tenant_b", run.ID); !errors.Is(err, store.ErrRetrievalRunNotFound) {
			t.Fatalf("cross-tenant read = %v", err)
		}
	})

	t.Run("conflict and snapshot mismatch fail closed", func(t *testing.T) {
		repository := factory(t)
		config := ValidServingConfig(t, "tenant_a")
		if err := repository.ActivateServingConfig(context.Background(), config); err != nil {
			t.Fatal(err)
		}
		snapshot, err := repository.AcquireServingSnapshot(context.Background(), "tenant_a")
		if err != nil {
			t.Fatal(err)
		}
		first := ValidRetrievalRun(t, snapshot, 'b')
		if err = repository.SaveRetrievalRun(context.Background(), first); err != nil {
			t.Fatal(err)
		}
		conflictingInput := ValidRetrievalRunInput(t, snapshot, 'b')
		conflictingInput.CompletedAt = conflictingInput.CompletedAt.Add(time.Second)
		conflictingInput.ExpiresAt = conflictingInput.ExpiresAt.Add(time.Second)
		conflicting, err := retrieval.BuildRun(conflictingInput)
		if err != nil {
			t.Fatal(err)
		}
		if err = repository.SaveRetrievalRun(context.Background(), conflicting); !errors.Is(err, store.ErrRetrievalRunConflict) {
			t.Fatalf("conflict error = %v", err)
		}
		badSnapshot := snapshot
		badSnapshot.DocumentSetHash = strings.Repeat("f", 64)
		mismatch := ValidRetrievalRun(t, badSnapshot, 'c')
		if err = repository.SaveRetrievalRun(context.Background(), mismatch); !errors.Is(err, store.ErrRetrievalSnapshotMismatch) {
			t.Fatalf("snapshot error = %v", err)
		}
	})
}

func ValidServingConfig(t *testing.T, tenantID domain.TenantID) retrieval.ServingConfig {
	t.Helper()
	config, err := retrieval.BuildServingConfig(tenantID, "policy.v1", "ranker.v1", []retrieval.SnapshotIndex{{Channel: retrieval.ChannelExact, ManifestID: "index.exact.v1"}})
	if err != nil {
		t.Fatal(err)
	}
	return config
}

func ValidRetrievalRun(t *testing.T, snapshot retrieval.ServingSnapshot, idByte byte) retrieval.Run {
	t.Helper()
	run, err := retrieval.BuildRun(ValidRetrievalRunInput(t, snapshot, idByte))
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func ValidRetrievalRunInput(t *testing.T, snapshot retrieval.ServingSnapshot, idByte byte) retrieval.RunInput {
	t.Helper()
	query, err := retrieval.BuildQuery("tenant_a", "policy.v1", retrieval.AliasSet{}, retrieval.Input{Task: "release", Tools: []retrieval.Tool{{Name: "shell", ContractVersionID: "tcv_shell"}}, Harness: retrieval.Harness{Name: "ci", Version: "1"}, RiskClass: retrieval.RiskMedium, LatencyClass: retrieval.LatencyInteractive, MaxCandidates: 10})
	if err != nil {
		t.Fatal(err)
	}
	created := time.Date(2026, 9, 20, 1, 2, 3, 0, time.UTC)
	return retrieval.RunInput{
		ID: "rrun_" + strings.Repeat(string(idByte), 64), TenantID: "tenant_a", Query: query, QueryEnvelope: "enc.v1.key.nonce.ciphertext", Snapshot: snapshot,
		ChannelExecutions: []retrieval.ChannelExecution{{Channel: retrieval.ChannelExact, IndexManifestID: "index.exact.v1", HitCount: 1, Complete: true}},
		Hits:              []retrieval.PersistedHit{{Channel: retrieval.ChannelExact, VersionID: "pv_a", Rank: 1, RawScoreQuantized: 100, IndexManifestID: "index.exact.v1"}},
		Gates:             []retrieval.PersistedGate{{VersionID: "pv_a", Eligible: true, CanonicalFacts: `{}`}},
		Ranked:            []retrieval.PersistedRank{{VersionID: "pv_a", RRFScore: 10, FinalScore: 20, Rank: 1, VerificationStrength: 5, ObservedEndToEnd: true}},
		Disposition:       retrieval.RunSelected, SelectedVersionIDs: []string{"pv_a"}, CreatedAt: created, CompletedAt: created.Add(time.Millisecond), ExpiresAt: created.Add(7 * 24 * time.Hour),
	}
}
