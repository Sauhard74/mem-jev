package jev

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/domain"
)

func TestJudgmentRecordIsCanonicalAndTamperEvident(t *testing.T) {
	record := validRecord(t, "tenant_a", 875_000)
	rebuilt := validRecord(t, "tenant_a", 875_000)
	if record.Key != rebuilt.Key || record.ContentHash != rebuilt.ContentHash || !bytes.Equal(record.CanonicalJSON, rebuilt.CanonicalJSON) {
		t.Fatal("same judgment produced different canonical record")
	}
	if err := ValidateJudgmentRecord(record); err != nil {
		t.Fatal(err)
	}
	record.Judgment.IntentFit.ScoreMicros--
	if err := ValidateJudgmentRecord(record); !errors.Is(err, ErrInvalidJudgment) {
		t.Fatalf("tampered record error = %v", err)
	}
}

func TestMemoryJudgmentRepositoryContract(t *testing.T) {
	repository := NewMemoryRepository()
	now := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	record := validRecordAt(t, "tenant_a", 875_000, now)

	winner, created, err := repository.Commit(context.Background(), record)
	if err != nil || !created || winner.ContentHash != record.ContentHash {
		t.Fatalf("first Commit() = %#v, %v, %v", winner, created, err)
	}
	winner, created, err = repository.Commit(context.Background(), record)
	if err != nil || created || winner.ContentHash != record.ContentHash {
		t.Fatalf("idempotent Commit() = %#v, %v, %v", winner, created, err)
	}
	loaded, err := repository.LookupReusable(context.Background(), "tenant_a", record.Key, now.Add(time.Minute))
	if err != nil || loaded.ContentHash != record.ContentHash {
		t.Fatalf("LookupReusable() = %#v, %v", loaded, err)
	}
	if _, err = repository.LookupReusable(context.Background(), "tenant_b", record.Key, now); !errors.Is(err, ErrJudgmentNotFound) {
		t.Fatalf("cross-tenant lookup error = %v", err)
	}
	if _, err = repository.LookupReusable(context.Background(), "tenant_a", record.Key, record.ReusableUntil); !errors.Is(err, ErrJudgmentNotFound) {
		t.Fatalf("expired lookup error = %v", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = repository.LookupReusable(cancelled, "tenant_a", record.Key, now); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled lookup error = %v", err)
	}
}

func TestMemoryJudgmentRepositorySelectsOneConcurrentWinner(t *testing.T) {
	repository := NewMemoryRepository()
	now := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	const writers = 32
	winners := make(chan JudgmentRecord, writers)
	errorsFound := make(chan error, writers)
	records := make([]JudgmentRecord, writers)
	for index := range writers {
		records[index] = validRecordAt(t, "tenant_a", int32(700_000+index), now)
	}
	var group sync.WaitGroup
	for index := range writers {
		group.Add(1)
		go func(value int) {
			defer group.Done()
			winner, _, err := repository.Commit(context.Background(), records[value])
			if err != nil {
				errorsFound <- err
				return
			}
			winners <- winner
		}(index)
	}
	group.Wait()
	close(winners)
	close(errorsFound)
	for err := range errorsFound {
		t.Fatal(err)
	}
	contentHash := ""
	for winner := range winners {
		if contentHash == "" {
			contentHash = winner.ContentHash
		}
		if winner.ContentHash != contentHash {
			t.Fatalf("multiple authoritative winners: %s and %s", contentHash, winner.ContentHash)
		}
	}
}

func validRecord(t *testing.T, tenant domain.TenantID, intent int32) JudgmentRecord {
	t.Helper()
	return validRecordAt(t, tenant, intent, time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC))
}

func validRecordAt(t *testing.T, tenant domain.TenantID, intent int32, now time.Time) JudgmentRecord {
	t.Helper()
	rubric, err := DefaultRubricV1("jev-1.13.0")
	if err != nil {
		t.Fatal(err)
	}
	keyInput := JudgmentKeyInput{
		TenantID: tenant, QueryHash: fmt.Sprintf("%064x", 1), ProcedureVersionID: "pver_1",
		DocumentHash: fmt.Sprintf("%064x", 2), EnvironmentHash: fmt.Sprintf("%064x", 3),
		PolicyManifestID: "epol_1", RubricManifestID: rubric.ID, Provider: ProviderTypeSafe, Model: rubric.Model,
	}
	judgment := Judgment{
		IntentFit:                    ScoreAnswer{ScoreMicros: intent * 4, ConfidenceMicros: 800_000, ProbabilitiesMicros: []int32{0, 50_000, 100_000, 250_000, 600_000}},
		PreconditionsLikelySatisfied: NoulAnswer{NoulMicros: 900_000},
		TaskCoverage:                 ScoreAnswer{ScoreMicros: 2_000_000, ConfidenceMicros: 700_000, ProbabilitiesMicros: []int32{50_000, 100_000, 550_000, 250_000, 50_000}},
		ContradictsRequest:           NoulAnswer{NoulMicros: 125_000}, UsefulAsPartialPlan: NoulAnswer{NoulMicros: 600_000},
	}
	record, err := NewJudgmentRecord(JudgmentRecordInput{KeyInput: keyInput, Judgment: judgment, Usage: Usage{InputTokens: 321, OutputTokens: 44}, CreatedAt: now, ReusableUntil: now.Add(24 * time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	return record
}
