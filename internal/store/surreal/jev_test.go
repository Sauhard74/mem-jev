//go:build integration

package surreal

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/jev"
)

func TestSurrealJevRepositoryContract(t *testing.T) {
	db := projectionDatabase(t)
	repository := NewJevRepository(db)
	now := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	first := surrealJevRecord(t, 875_000, now)
	winner, created, err := repository.Commit(context.Background(), first)
	if err != nil || !created || winner.ContentHash != first.ContentHash {
		t.Fatalf("first commit = %#v, %v, %v", winner, created, err)
	}
	competing := surrealJevRecord(t, 700_000, now)
	winner, created, err = repository.Commit(context.Background(), competing)
	if err != nil || created || winner.ContentHash != first.ContentHash {
		t.Fatalf("competing commit = %#v, %v, %v", winner, created, err)
	}
	loaded, err := repository.LookupReusable(context.Background(), "tenant_a", first.Key, now.Add(time.Minute))
	if err != nil || loaded.ContentHash != first.ContentHash {
		t.Fatalf("lookup = %#v, %v", loaded, err)
	}
	if _, err = repository.LookupReusable(context.Background(), "tenant_b", first.Key, now); !errors.Is(err, jev.ErrJudgmentNotFound) {
		t.Fatalf("cross-tenant lookup error = %v", err)
	}
	if _, err = repository.LookupReusable(context.Background(), "tenant_a", first.Key, first.ReusableUntil); !errors.Is(err, jev.ErrJudgmentNotFound) {
		t.Fatalf("expired lookup error = %v", err)
	}
}

func surrealJevRecord(t *testing.T, intent int32, now time.Time) jev.JudgmentRecord {
	t.Helper()
	rubric, err := jev.DefaultRubricV1("jev-1.13.0")
	if err != nil {
		t.Fatal(err)
	}
	input := jev.JudgmentRecordInput{
		KeyInput: jev.JudgmentKeyInput{TenantID: "tenant_a", QueryHash: fmt.Sprintf("%064x", 1), ProcedureVersionID: "pver_1", DocumentHash: fmt.Sprintf("%064x", 2), EnvironmentHash: fmt.Sprintf("%064x", 3), PolicyManifestID: "epol_1", RubricManifestID: rubric.ID, Provider: jev.ProviderTypeSafe, Model: rubric.Model},
		Judgment: jev.Judgment{
			IntentFit:                    jev.ScoreAnswer{ScoreMicros: intent * 4, ConfidenceMicros: 800_000, ProbabilitiesMicros: []int32{0, 50_000, 100_000, 250_000, 600_000}},
			PreconditionsLikelySatisfied: jev.NoulAnswer{NoulMicros: 900_000},
			TaskCoverage:                 jev.ScoreAnswer{ScoreMicros: 2_000_000, ConfidenceMicros: 700_000, ProbabilitiesMicros: []int32{50_000, 100_000, 550_000, 250_000, 50_000}},
			ContradictsRequest:           jev.NoulAnswer{NoulMicros: 125_000}, UsefulAsPartialPlan: jev.NoulAnswer{NoulMicros: 600_000},
		},
		Usage: jev.Usage{InputTokens: 321, OutputTokens: 44}, CreatedAt: now, ReusableUntil: now.Add(time.Hour),
	}
	record, err := jev.NewJudgmentRecord(input)
	if err != nil {
		t.Fatal(err)
	}
	return record
}
