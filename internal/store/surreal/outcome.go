package surreal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/observability"
	"github.com/sauhard74/mem-jev/internal/store"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

type OutcomeRepository struct {
	db  *surrealdb.DB
	now func() time.Time
}

func NewOutcomeRepository(db *surrealdb.DB) *OutcomeRepository {
	return &OutcomeRepository{db: db, now: time.Now}
}

func (r *OutcomeRepository) CommitOutcome(ctx context.Context, request store.CommitOutcomeRequest) (store.OutcomeReceipt, error) {
	if err := ctx.Err(); err != nil {
		return store.OutcomeReceipt{}, err
	}
	if r == nil || r.db == nil {
		return store.OutcomeReceipt{}, errors.New("SurrealDB outcome repository is not configured")
	}
	if err := store.ValidateOutcomeCommit(request); err != nil {
		return store.OutcomeReceipt{}, err
	}
	const maxAttempts = 8
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		receipt, err := r.commitOutcomeOnce(ctx, request)
		if err == nil {
			return receipt, nil
		}
		lastErr = err
		existing, found, lookupErr := findOutcomeReceipt(ctx, r.db, request.TenantID, request.IdempotencyKeyHash)
		if lookupErr != nil {
			return store.OutcomeReceipt{}, databaseFailure("resolve failed outcome commit", lookupErr)
		}
		if found {
			return resolveExistingOutcome(existing, request.Outcome.Hash)
		}
		if !surrealdb.IsTransactionConflict(err) {
			return store.OutcomeReceipt{}, databaseFailure("commit outcome ledger", err)
		}
		observability.RecordTransactionRetry(ctx)
		if err := waitForRetry(ctx, attempt); err != nil {
			return store.OutcomeReceipt{}, err
		}
	}
	return store.OutcomeReceipt{}, &store.OpError{Operation: "commit outcome ledger after conflict retries", Retryable: true, Err: lastErr}
}

func (r *OutcomeRepository) commitOutcomeOnce(ctx context.Context, request store.CommitOutcomeRequest) (_ store.OutcomeReceipt, err error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return store.OutcomeReceipt{}, fmt.Errorf("begin outcome commit: %w", err)
	}
	defer func() {
		if !tx.IsClosed() {
			_ = tx.Cancel(context.Background())
		}
	}()
	if existing, found, err := findOutcomeReceipt(ctx, tx, request.TenantID, request.IdempotencyKeyHash); err != nil {
		return store.OutcomeReceipt{}, fmt.Errorf("find outcome receipt: %w", err)
	} else if found {
		if cancelErr := tx.Cancel(ctx); cancelErr != nil {
			return store.OutcomeReceipt{}, fmt.Errorf("cancel duplicate outcome transaction: %w", cancelErr)
		}
		return resolveExistingOutcome(existing, request.Outcome.Hash)
	}
	traceFound, err := traceExists(ctx, tx, request.TenantID, request.Outcome.TraceID)
	if err != nil {
		return store.OutcomeReceipt{}, fmt.Errorf("find outcome trace: %w", err)
	}
	if !traceFound {
		return store.OutcomeReceipt{}, store.ErrOutcomeTraceNotFound
	}
	if request.Outcome.SelectionID != "" {
		// Selection records are introduced by the retrieval slice. Until that
		// tenant-scoped lookup exists, fail closed rather than accepting an
		// unattributable causal link.
		return store.OutcomeReceipt{}, store.ErrOutcomeSelectionNotFound
	}
	if request.Outcome.SupersedesOutcomeID != "" {
		previousTraceID, found, findErr := findOutcomeTrace(ctx, tx, request.TenantID, request.Outcome.SupersedesOutcomeID)
		if findErr != nil {
			return store.OutcomeReceipt{}, fmt.Errorf("find superseded outcome: %w", findErr)
		}
		if !found || previousTraceID != request.Outcome.TraceID {
			return store.OutcomeReceipt{}, store.ErrOutcomeSupersessionInvalid
		}
	}
	createdAt := r.now().UTC()
	receipt := store.OutcomeReceipt{
		ID: store.OutcomeReceiptID(request.TenantID, request.IdempotencyKeyHash), TenantID: request.TenantID,
		OutcomeID: request.Outcome.ID, TraceID: request.Outcome.TraceID, ContentHash: request.Outcome.Hash,
		WorkflowID: store.OutcomeWorkflowID(request.TenantID, request.Outcome.TraceID, request.Outcome.ID),
		State:      request.Evaluation.State, PromotionEligible: request.Evaluation.PromotionEligible,
		PolicyVersion: request.Evaluation.PolicyVersion, Disposition: store.OutcomeDispositionAccepted, CreatedAt: createdAt,
	}
	if err := createOutcomeReceipt(ctx, tx, request, receipt); err != nil {
		return store.OutcomeReceipt{}, err
	}
	if existingHash, found, err := findOutcomeHash(ctx, tx, request.TenantID, request.Outcome.ID); err != nil {
		return store.OutcomeReceipt{}, fmt.Errorf("find outcome aggregate: %w", err)
	} else if found {
		if existingHash != request.Outcome.Hash {
			return store.OutcomeReceipt{}, store.ErrInvalidOutcomeCommit
		}
		if err := tx.Commit(ctx); err != nil {
			return store.OutcomeReceipt{}, fmt.Errorf("commit outcome aggregate reuse receipt: %w", err)
		}
		return receipt, nil
	}
	if err := createOutcome(ctx, tx, request, createdAt); err != nil {
		return store.OutcomeReceipt{}, err
	}
	for _, fact := range request.Outcome.Evidence {
		if err := createVerificationResult(ctx, tx, request, fact, createdAt); err != nil {
			return store.OutcomeReceipt{}, err
		}
	}
	if len(request.Evaluation.ConflictPredicates) > 0 {
		if err := createOutcomeConflictAudit(ctx, tx, request, createdAt); err != nil {
			return store.OutcomeReceipt{}, err
		}
	}
	if err := createOutcomeOutbox(ctx, tx, request, receipt, createdAt); err != nil {
		return store.OutcomeReceipt{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return store.OutcomeReceipt{}, fmt.Errorf("commit outcome transaction: %w", err)
	}
	observability.RecordOutboxCreated(ctx)
	return receipt, nil
}

type outcomeReceiptRow struct {
	ReceiptID         string    `json:"receipt_id"`
	TenantID          string    `json:"tenant_id"`
	OutcomeID         string    `json:"outcome_id"`
	TraceID           string    `json:"trace_id"`
	ContentHash       string    `json:"content_hash"`
	State             string    `json:"state"`
	PromotionEligible bool      `json:"promotion_eligible"`
	PolicyVersion     string    `json:"policy_version"`
	CreatedAt         time.Time `json:"created_at"`
}

func findOutcomeReceipt[S interface {
	*surrealdb.DB | *surrealdb.Transaction
}](ctx context.Context, sender S, tenantID domain.TenantID, keyHash string) (store.OutcomeReceipt, bool, error) {
	results, err := surrealdb.Query[[]outcomeReceiptRow](ctx, sender, `
		SELECT receipt_id, tenant_id, outcome_id, trace_id, content_hash, state, promotion_eligible, policy_version, created_at
		FROM outcome_receipt WHERE tenant_id = $tenant_id AND idempotency_key_hash = $key_hash LIMIT 1`,
		map[string]any{"tenant_id": string(tenantID), "key_hash": keyHash})
	if err != nil || results == nil || len(*results) == 0 || len((*results)[0].Result) == 0 {
		return store.OutcomeReceipt{}, false, err
	}
	row := (*results)[0].Result[0]
	receipt := store.OutcomeReceipt{
		ID: row.ReceiptID, TenantID: domain.TenantID(row.TenantID), OutcomeID: domain.OutcomeID(row.OutcomeID),
		TraceID: domain.TraceID(row.TraceID), ContentHash: row.ContentHash,
		WorkflowID: store.OutcomeWorkflowID(domain.TenantID(row.TenantID), domain.TraceID(row.TraceID), domain.OutcomeID(row.OutcomeID)),
		State:      domain.OutcomeState(row.State), PromotionEligible: row.PromotionEligible, PolicyVersion: row.PolicyVersion,
		Disposition: store.OutcomeDispositionAccepted, CreatedAt: row.CreatedAt,
	}
	return receipt, true, nil
}

func resolveExistingOutcome(receipt store.OutcomeReceipt, contentHash string) (store.OutcomeReceipt, error) {
	if receipt.ContentHash != contentHash {
		return store.OutcomeReceipt{}, store.ErrIdempotencyConflict
	}
	receipt.Disposition = store.OutcomeDispositionDuplicate
	return receipt, nil
}

func traceExists(ctx context.Context, tx *surrealdb.Transaction, tenantID domain.TenantID, traceID domain.TraceID) (bool, error) {
	results, err := surrealdb.Query[[]struct {
		TraceID string `json:"trace_id"`
	}](ctx, tx,
		"SELECT trace_id FROM trace_run WHERE tenant_id = $tenant_id AND trace_id = $trace_id LIMIT 1",
		map[string]any{"tenant_id": string(tenantID), "trace_id": string(traceID)})
	return err == nil && results != nil && len(*results) > 0 && len((*results)[0].Result) > 0, err
}

func findOutcomeTrace(ctx context.Context, tx *surrealdb.Transaction, tenantID domain.TenantID, outcomeID domain.OutcomeID) (domain.TraceID, bool, error) {
	results, err := surrealdb.Query[[]struct {
		TraceID string `json:"trace_id"`
	}](ctx, tx,
		"SELECT trace_id FROM outcome_evidence WHERE tenant_id = $tenant_id AND outcome_id = $outcome_id LIMIT 1",
		map[string]any{"tenant_id": string(tenantID), "outcome_id": string(outcomeID)})
	if err != nil || results == nil || len(*results) == 0 || len((*results)[0].Result) == 0 {
		return "", false, err
	}
	return domain.TraceID((*results)[0].Result[0].TraceID), true, nil
}

func findOutcomeHash(ctx context.Context, tx *surrealdb.Transaction, tenantID domain.TenantID, outcomeID domain.OutcomeID) (string, bool, error) {
	results, err := surrealdb.Query[[]struct {
		ContentHash string `json:"content_hash"`
	}](ctx, tx,
		"SELECT content_hash FROM outcome_evidence WHERE tenant_id = $tenant_id AND outcome_id = $outcome_id LIMIT 1",
		map[string]any{"tenant_id": string(tenantID), "outcome_id": string(outcomeID)})
	if err != nil || results == nil || len(*results) == 0 || len((*results)[0].Result) == 0 {
		return "", false, err
	}
	return (*results)[0].Result[0].ContentHash, true, nil
}

func createOutcomeReceipt(ctx context.Context, tx *surrealdb.Transaction, request store.CommitOutcomeRequest, receipt store.OutcomeReceipt) error {
	return execute(ctx, tx, "create outcome receipt", "CREATE ONLY outcome_receipt CONTENT $record", map[string]any{"record": map[string]any{
		"tenant_id": string(request.TenantID), "receipt_id": receipt.ID, "outcome_id": string(receipt.OutcomeID),
		"trace_id": string(receipt.TraceID), "idempotency_key_hash": request.IdempotencyKeyHash,
		"state": string(receipt.State), "promotion_eligible": receipt.PromotionEligible, "policy_version": receipt.PolicyVersion,
		"created_at": receipt.CreatedAt, "schema_version": request.Outcome.SchemaVersion, "content_hash": request.Outcome.Hash,
	}})
}

func createOutcome(ctx context.Context, tx *surrealdb.Transaction, request store.CommitOutcomeRequest, createdAt time.Time) error {
	record := map[string]any{
		"tenant_id": string(request.TenantID), "outcome_id": string(request.Outcome.ID), "trace_id": string(request.Outcome.TraceID),
		"state": string(request.Evaluation.State), "promotion_eligible": request.Evaluation.PromotionEligible,
		"policy_version": request.Evaluation.PolicyVersion, "evidence_count": len(request.Outcome.Evidence),
		"created_at": createdAt, "schema_version": request.Outcome.SchemaVersion, "content_hash": request.Outcome.Hash,
	}
	if request.Outcome.ExecutionID != "" {
		record["execution_id"] = request.Outcome.ExecutionID
	}
	if request.Outcome.SelectionID != "" {
		record["selection_id"] = request.Outcome.SelectionID
	}
	if request.Outcome.SupersedesOutcomeID != "" {
		record["supersedes_outcome_id"] = string(request.Outcome.SupersedesOutcomeID)
	}
	if request.Outcome.CorrectionReason != "" {
		record["correction_reason"] = request.Outcome.CorrectionReason
	}
	return execute(ctx, tx, "create outcome evidence", "CREATE ONLY outcome_evidence CONTENT $record", map[string]any{"record": record})
}

func createVerificationResult(ctx context.Context, tx *surrealdb.Transaction, request store.CommitOutcomeRequest, fact domain.OutcomeEvidence, createdAt time.Time) error {
	observedAt, err := time.Parse(time.RFC3339Nano, fact.ObservedAt)
	if err != nil {
		return fmt.Errorf("parse verification observed_at: %w", err)
	}
	record := map[string]any{
		"tenant_id": string(request.TenantID), "outcome_id": string(request.Outcome.ID), "evidence_id": string(fact.ID),
		"class": string(fact.Class), "verdict": string(fact.Verdict), "predicate_id": fact.PredicateID, "verifier_id": fact.VerifierID,
		"observed_at": observedAt, "payload": map[string]any{"fields": fact.Fields}, "created_at": createdAt,
		"schema_version": request.Outcome.SchemaVersion, "content_hash": string(fact.ID)[3:],
	}
	if fact.VerifierVersion != "" {
		record["verifier_version"] = fact.VerifierVersion
	}
	return execute(ctx, tx, "create verification result", "CREATE ONLY verification_result CONTENT $record", map[string]any{"record": record})
}

func createOutcomeConflictAudit(ctx context.Context, tx *surrealdb.Transaction, request store.CommitOutcomeRequest, createdAt time.Time) error {
	id := store.OutcomeAuditEventID(request.TenantID, request.Outcome.ID)
	return execute(ctx, tx, "create outcome conflict audit", "CREATE ONLY audit_event CONTENT $record", map[string]any{"record": map[string]any{
		"tenant_id": string(request.TenantID), "audit_event_id": id, "event_type": "outcome_evidence_conflict",
		"subject_id": string(request.Outcome.ID), "reason_code": "conflicting_strong_evidence",
		"details": map[string]any{"predicate_ids": request.Evaluation.ConflictPredicates}, "created_at": createdAt,
		"schema_version": "audit.v1", "content_hash": id[6:],
	}})
}

func createOutcomeOutbox(ctx context.Context, tx *surrealdb.Transaction, request store.CommitOutcomeRequest, receipt store.OutcomeReceipt, createdAt time.Time) error {
	return execute(ctx, tx, "create outcome outbox job", "CREATE ONLY outbox_job CONTENT $record", map[string]any{"record": map[string]any{
		"tenant_id": string(request.TenantID), "workflow_id": receipt.WorkflowID, "trace_id": string(request.Outcome.TraceID),
		"outcome_id": string(request.Outcome.ID), "job_type": "synthesize_outcome", "state": "pending", "attempt_count": 0, "lease_generation": 0,
		"available_at": createdAt, "created_at": createdAt, "schema_version": request.Outcome.SchemaVersion, "content_hash": request.Outcome.Hash,
	}})
}

func (r *OutcomeRepository) OutcomeCounts(ctx context.Context) (store.OutcomeCounts, error) {
	if r == nil || r.db == nil {
		return store.OutcomeCounts{}, errors.New("SurrealDB outcome repository is not configured")
	}
	counts := store.OutcomeCounts{}
	var err error
	if counts.Receipts, err = tableCount(ctx, r.db, "outcome_receipt"); err != nil {
		return counts, err
	}
	if counts.Outcomes, err = tableCount(ctx, r.db, "outcome_evidence"); err != nil {
		return counts, err
	}
	if counts.VerificationResults, err = tableCount(ctx, r.db, "verification_result"); err != nil {
		return counts, err
	}
	if counts.AuditEvents, err = tableCount(ctx, r.db, "audit_event"); err != nil {
		return counts, err
	}
	type countRow struct {
		Count int `json:"count"`
	}
	results, queryErr := surrealdb.Query[[]countRow](ctx, r.db, `SELECT count() AS count FROM outbox_job WHERE job_type = "synthesize_outcome" GROUP ALL`, nil)
	if queryErr != nil {
		return counts, queryErr
	}
	if results != nil && len(*results) > 0 && len((*results)[0].Result) > 0 {
		counts.OutboxJobs = (*results)[0].Result[0].Count
	}
	return counts, nil
}
