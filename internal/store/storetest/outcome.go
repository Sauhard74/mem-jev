package storetest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/evidence"
	"github.com/sauhard74/mem-jev/internal/store"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type OutcomeRepository interface {
	store.OutcomeRepository
	OutcomeCounts(context.Context) (store.OutcomeCounts, error)
}

type OutcomeFactory func(*testing.T) (store.IngestRepository, OutcomeRepository)

func RunOutcomeContract(t *testing.T, factory OutcomeFactory) {
	t.Helper()
	t.Run("accepted and duplicate", func(t *testing.T) {
		ingestRepository, repository := factory(t)
		seedTrace(t, ingestRepository)
		request := ValidOutcomeCommitRequest(t)
		accepted, err := repository.CommitOutcome(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		if accepted.Disposition != store.OutcomeDispositionAccepted || accepted.State != domain.OutcomeStateVerifiedSuccess {
			t.Fatalf("receipt = %#v", accepted)
		}
		duplicate, err := repository.CommitOutcome(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		if duplicate.ID != accepted.ID || duplicate.Disposition != store.OutcomeDispositionDuplicate {
			t.Fatalf("accepted=%#v duplicate=%#v", accepted, duplicate)
		}
		assertOutcomeCounts(t, repository, store.OutcomeCounts{Receipts: 1, Outcomes: 1, VerificationResults: 1, OutboxJobs: 1})
	})

	t.Run("idempotency conflict", func(t *testing.T) {
		ingestRepository, repository := factory(t)
		seedTrace(t, ingestRepository)
		request := ValidOutcomeCommitRequest(t)
		if _, err := repository.CommitOutcome(context.Background(), request); err != nil {
			t.Fatal(err)
		}
		request.Outcome = canonicalOutcomeForTest(t, "different")
		if _, err := repository.CommitOutcome(context.Background(), request); !errors.Is(err, store.ErrIdempotencyConflict) {
			t.Fatalf("error = %v, want idempotency conflict", err)
		}
		assertOutcomeCounts(t, repository, store.OutcomeCounts{Receipts: 1, Outcomes: 1, VerificationResults: 1, OutboxJobs: 1})
	})

	t.Run("same outcome with new idempotency key reuses aggregate", func(t *testing.T) {
		ingestRepository, repository := factory(t)
		seedTrace(t, ingestRepository)
		request := ValidOutcomeCommitRequest(t)
		if _, err := repository.CommitOutcome(context.Background(), request); err != nil {
			t.Fatal(err)
		}
		request.IdempotencyKeyHash = strings.Repeat("d", 64)
		second, err := repository.CommitOutcome(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		if second.Disposition != store.OutcomeDispositionAccepted {
			t.Fatalf("disposition = %q", second.Disposition)
		}
		assertOutcomeCounts(t, repository, store.OutcomeCounts{Receipts: 2, Outcomes: 1, VerificationResults: 1, OutboxJobs: 1})
	})

	t.Run("missing and cross tenant trace", func(t *testing.T) {
		ingestRepository, repository := factory(t)
		seedTrace(t, ingestRepository)
		request := ValidOutcomeCommitRequest(t)
		request.TenantID = "tenant_b"
		request.Outcome = canonicalOutcomeForTenant(t, "tenant_b", "execution-1")
		if _, err := repository.CommitOutcome(context.Background(), request); !errors.Is(err, store.ErrOutcomeTraceNotFound) {
			t.Fatalf("error = %v, want trace not found", err)
		}
		assertOutcomeCounts(t, repository, store.OutcomeCounts{})
	})

	t.Run("unverified selection linkage fails closed", func(t *testing.T) {
		ingestRepository, repository := factory(t)
		seedTrace(t, ingestRepository)
		request := ValidOutcomeCommitRequest(t)
		request.Outcome = canonicalOutcomeForTenantSelection(t, "tenant_a", "execution-1", "sel_unknown")
		if _, err := repository.CommitOutcome(context.Background(), request); !errors.Is(err, store.ErrOutcomeSelectionNotFound) {
			t.Fatalf("error = %v, want selection not found", err)
		}
		assertOutcomeCounts(t, repository, store.OutcomeCounts{})
	})

	t.Run("unknown superseded outcome fails closed", func(t *testing.T) {
		ingestRepository, repository := factory(t)
		seedTrace(t, ingestRepository)
		request := ValidOutcomeCommitRequest(t)
		request.Outcome = canonicalCorrectionForTest(t, domain.OutcomeID("out_"+strings.Repeat("f", 64)))
		if _, err := repository.CommitOutcome(context.Background(), request); !errors.Is(err, store.ErrOutcomeSupersessionInvalid) {
			t.Fatalf("error = %v, want invalid supersession", err)
		}
		assertOutcomeCounts(t, repository, store.OutcomeCounts{})
	})

	t.Run("conflict writes audit atomically", func(t *testing.T) {
		ingestRepository, repository := factory(t)
		seedTrace(t, ingestRepository)
		request := ValidOutcomeCommitRequest(t)
		request.Evaluation.State = domain.OutcomeStateInconclusive
		request.Evaluation.PromotionEligible = false
		request.Evaluation.ConflictPredicates = []string{"goal"}
		if _, err := repository.CommitOutcome(context.Background(), request); err != nil {
			t.Fatal(err)
		}
		assertOutcomeCounts(t, repository, store.OutcomeCounts{Receipts: 1, Outcomes: 1, VerificationResults: 1, AuditEvents: 1, OutboxJobs: 1})
	})

	t.Run("concurrent duplicate converges", func(t *testing.T) {
		ingestRepository, repository := factory(t)
		seedTrace(t, ingestRepository)
		request := ValidOutcomeCommitRequest(t)
		const callers = 12
		start := make(chan struct{})
		errorsSeen := make(chan error, callers)
		var wait sync.WaitGroup
		for range callers {
			wait.Add(1)
			go func() {
				defer wait.Done()
				<-start
				_, err := repository.CommitOutcome(context.Background(), request)
				errorsSeen <- err
			}()
		}
		close(start)
		wait.Wait()
		close(errorsSeen)
		for err := range errorsSeen {
			if err != nil {
				t.Errorf("CommitOutcome() error = %v", err)
			}
		}
		assertOutcomeCounts(t, repository, store.OutcomeCounts{Receipts: 1, Outcomes: 1, VerificationResults: 1, OutboxJobs: 1})
	})

	t.Run("concurrent aggregate reuse with distinct keys converges", func(t *testing.T) {
		ingestRepository, repository := factory(t)
		seedTrace(t, ingestRepository)
		const callers = 8
		start := make(chan struct{})
		errorsSeen := make(chan error, callers)
		var wait sync.WaitGroup
		for index := range callers {
			wait.Add(1)
			go func(index int) {
				defer wait.Done()
				request := ValidOutcomeCommitRequest(t)
				request.IdempotencyKeyHash = fmt.Sprintf("%064x", index+1)
				<-start
				_, err := repository.CommitOutcome(context.Background(), request)
				errorsSeen <- err
			}(index)
		}
		close(start)
		wait.Wait()
		close(errorsSeen)
		for err := range errorsSeen {
			if err != nil {
				t.Errorf("CommitOutcome() error = %v", err)
			}
		}
		assertOutcomeCounts(t, repository, store.OutcomeCounts{Receipts: callers, Outcomes: 1, VerificationResults: 1, OutboxJobs: 1})
	})

	t.Run("invalid request and cancellation write nothing", func(t *testing.T) {
		ingestRepository, repository := factory(t)
		seedTrace(t, ingestRepository)
		request := ValidOutcomeCommitRequest(t)
		request.Evaluation.PromotionEligible = false
		if _, err := repository.CommitOutcome(context.Background(), request); !errors.Is(err, store.ErrInvalidOutcomeCommit) {
			t.Fatalf("error = %v, want invalid outcome commit", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := repository.CommitOutcome(ctx, ValidOutcomeCommitRequest(t)); !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want canceled", err)
		}
		assertOutcomeCounts(t, repository, store.OutcomeCounts{})
	})
}

func ValidOutcomeCommitRequest(t *testing.T) store.CommitOutcomeRequest {
	t.Helper()
	outcome := canonicalOutcomeForTest(t, "execution-1")
	request := store.CommitOutcomeRequest{
		TenantID: "tenant_a", IdempotencyKeyHash: strings.Repeat("c", 64), Outcome: outcome,
		Evaluation: evidence.Result{State: domain.OutcomeStateVerifiedSuccess, PromotionEligible: true, PolicyVersion: "outcome-policy.v1", StrongestSatisfied: domain.EvidenceClassGoalPredicate},
	}
	return request
}

func canonicalOutcomeForTest(t *testing.T, executionID string) domain.CanonicalOutcome {
	return canonicalOutcomeForTenant(t, "tenant_a", executionID)
}

func canonicalOutcomeForTenant(t *testing.T, tenantID domain.TenantID, executionID string) domain.CanonicalOutcome {
	return canonicalOutcomeForTenantSelection(t, tenantID, executionID, "")
}

func canonicalOutcomeForTenantSelection(t *testing.T, tenantID domain.TenantID, executionID, selectionID string) domain.CanonicalOutcome {
	t.Helper()
	outcome, err := evidence.Canonicalize(tenantID, &memjevv1.RecordOutcomeRequest{
		TraceId:     "tr_" + strings.Repeat("a", 64),
		ExecutionId: executionID,
		SelectionId: selectionID,
		Evidence: []*memjevv1.OutcomeEvidence{{
			ClientEvidenceId: "evidence-1",
			Class:            memjevv1.EvidenceClass_EVIDENCE_CLASS_GOAL_PREDICATE,
			Verdict:          memjevv1.EvidenceVerdict_EVIDENCE_VERDICT_SATISFIED,
			PredicateId:      "goal", VerifierId: "ci",
			ObservedAt: timestamppb.New(time.Date(2026, 9, 20, 0, 0, 2, 0, time.UTC)),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return outcome
}

func canonicalCorrectionForTest(t *testing.T, previous domain.OutcomeID) domain.CanonicalOutcome {
	t.Helper()
	outcome, err := evidence.Canonicalize("tenant_a", &memjevv1.RecordOutcomeRequest{
		TraceId: "tr_" + strings.Repeat("a", 64), ExecutionId: "execution-1",
		SupersedesOutcomeId: string(previous), CorrectionReason: "verifier correction",
		Evidence: []*memjevv1.OutcomeEvidence{{
			ClientEvidenceId: "evidence-2", Class: memjevv1.EvidenceClass_EVIDENCE_CLASS_GOAL_PREDICATE,
			Verdict: memjevv1.EvidenceVerdict_EVIDENCE_VERDICT_FAILED, PredicateId: "goal", VerifierId: "ci",
			ObservedAt: timestamppb.New(time.Date(2026, 9, 20, 0, 0, 3, 0, time.UTC)),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return outcome
}

func seedTrace(t *testing.T, repository store.IngestRepository) {
	t.Helper()
	if _, err := repository.Commit(context.Background(), ValidCommitRequest(t)); err != nil {
		t.Fatal(err)
	}
}

func assertOutcomeCounts(t *testing.T, repository OutcomeRepository, want store.OutcomeCounts) {
	t.Helper()
	got, err := repository.OutcomeCounts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("counts = %#v, want %#v", got, want)
	}
}
