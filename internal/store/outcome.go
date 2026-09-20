package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/sauhard74/mem-jev/internal/credit"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/evidence"
)

var (
	ErrInvalidOutcomeCommit       = errors.New("invalid outcome commit")
	ErrOutcomeTraceNotFound       = errors.New("outcome trace was not found")
	ErrOutcomeSelectionNotFound   = errors.New("outcome selection was not found")
	ErrOutcomeSupersessionInvalid = errors.New("outcome supersession is invalid")
	ErrOutcomeCreditClaimed       = errors.New("injected plan already has outcome credit")
)

type OutcomeDisposition string

const (
	OutcomeDispositionAccepted  OutcomeDisposition = "accepted"
	OutcomeDispositionDuplicate OutcomeDisposition = "duplicate"
)

type OutcomeRepository interface {
	CommitOutcome(context.Context, CommitOutcomeRequest) (OutcomeReceipt, error)
}

type CommitOutcomeRequest struct {
	TenantID           domain.TenantID
	IdempotencyKeyHash string
	Outcome            domain.CanonicalOutcome
	Evaluation         evidence.Result
	Credit             *credit.Record
}

type OutcomeReceipt struct {
	ID                string
	TenantID          domain.TenantID
	OutcomeID         domain.OutcomeID
	TraceID           domain.TraceID
	ContentHash       string
	WorkflowID        string
	State             domain.OutcomeState
	PromotionEligible bool
	PolicyVersion     string
	Disposition       OutcomeDisposition
	CreatedAt         time.Time
	OutcomeCreditID   string
	CreditClass       credit.Class
}

type OutcomeCounts struct {
	Receipts            int
	Outcomes            int
	VerificationResults int
	AuditEvents         int
	OutboxJobs          int
}

func ValidateOutcomeCommit(request CommitOutcomeRequest) error {
	if request.TenantID == "" || request.Outcome.TenantID != request.TenantID ||
		!lowercaseSHA256.MatchString(request.IdempotencyKeyHash) || request.Evaluation.PolicyVersion == "" ||
		request.Evaluation.State == "" || request.Outcome.TraceID == "" || request.Outcome.Hash == "" {
		return ErrInvalidOutcomeCommit
	}
	if err := evidence.VerifyCanonical(request.Outcome); err != nil {
		return ErrInvalidOutcomeCommit
	}
	if request.Evaluation.PromotionEligible != (request.Evaluation.State == domain.OutcomeStateVerifiedSuccess) {
		return ErrInvalidOutcomeCommit
	}
	if request.Evaluation.State != domain.OutcomeStateVerifiedSuccess &&
		request.Evaluation.State != domain.OutcomeStateProvisionalSuccess &&
		request.Evaluation.State != domain.OutcomeStateInconclusive &&
		request.Evaluation.State != domain.OutcomeStateVerifiedFailure {
		return ErrInvalidOutcomeCommit
	}
	if request.Outcome.SupersedesOutcomeID == request.Outcome.ID {
		return ErrInvalidOutcomeCommit
	}
	if (request.Outcome.InjectionID == "") != (request.Outcome.TaskExecutionID == "") || (request.Credit == nil) != (request.Outcome.InjectionID == "") {
		return ErrInvalidOutcomeCommit
	}
	if request.Credit != nil && (credit.Validate(*request.Credit) != nil || request.Credit.TenantID != request.TenantID || request.Credit.OutcomeID != request.Outcome.ID || request.Credit.InjectionID != request.Outcome.InjectionID || request.Credit.TaskExecutionID != request.Outcome.TaskExecutionID) {
		return ErrInvalidOutcomeCommit
	}
	conflicts := append([]string(nil), request.Evaluation.ConflictPredicates...)
	sort.Strings(conflicts)
	if !equalStrings(conflicts, request.Evaluation.ConflictPredicates) {
		return ErrInvalidOutcomeCommit
	}
	return nil
}

func OutcomeReceiptID(tenantID domain.TenantID, idempotencyKeyHash string) string {
	return "orcpt_" + hashIdentity(string(tenantID)+"\x00"+idempotencyKeyHash)
}

func OutcomeWorkflowID(tenantID domain.TenantID, traceID domain.TraceID, outcomeID domain.OutcomeID) string {
	return fmt.Sprintf("synthesize/%s/%s/%s/v1", hashIdentity(string(tenantID))[:16], traceID, outcomeID)
}

func OutcomeAuditEventID(tenantID domain.TenantID, outcomeID domain.OutcomeID) string {
	return "audit_" + hashIdentity(strings.Join([]string{string(tenantID), string(outcomeID), "evidence_conflict", "v1"}, "\x00"))
}

func equalStrings(first, second []string) bool {
	if len(first) != len(second) {
		return false
	}
	for index := range first {
		if first[index] != second[index] {
			return false
		}
	}
	return true
}
