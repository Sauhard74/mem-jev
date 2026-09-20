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
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

type OutboxRepository struct {
	db *surrealdb.DB
}

func NewOutboxRepository(db *surrealdb.DB) *OutboxRepository {
	return &OutboxRepository{db: db}
}

type outboxRow struct {
	ID              models.RecordID `json:"id"`
	TenantID        string          `json:"tenant_id"`
	WorkflowID      string          `json:"workflow_id"`
	TraceID         string          `json:"trace_id"`
	OutcomeID       *string         `json:"outcome_id"`
	JobType         string          `json:"job_type"`
	State           string          `json:"state"`
	ContentHash     string          `json:"content_hash"`
	AttemptCount    int             `json:"attempt_count"`
	LeaseOwner      *string         `json:"lease_owner"`
	LeaseGeneration int             `json:"lease_generation"`
	LeaseExpiresAt  *time.Time      `json:"lease_expires_at"`
	LastErrorCode   *string         `json:"last_error_code"`
	CreatedAt       time.Time       `json:"created_at"`
}

func (r *OutboxRepository) ClaimOutbox(ctx context.Context, request store.ClaimOutboxRequest) ([]store.OutboxLease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r == nil || r.db == nil {
		return nil, errors.New("SurrealDB outbox repository is not configured")
	}
	if err := store.ValidateClaimOutbox(request); err != nil {
		return nil, err
	}
	const attempts = 8
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		leases, err := r.claimOnce(ctx, request)
		if err == nil {
			return leases, nil
		}
		lastErr = err
		if !surrealdb.IsTransactionConflict(err) {
			return nil, databaseFailure("claim outbox jobs", err)
		}
		observability.RecordTransactionRetry(ctx)
		if err := waitForRetry(ctx, attempt); err != nil {
			return nil, err
		}
	}
	return nil, &store.OpError{Operation: "claim outbox jobs after conflict retries", Retryable: true, Err: lastErr}
}

func (r *OutboxRepository) claimOnce(ctx context.Context, request store.ClaimOutboxRequest) (_ []store.OutboxLease, err error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin outbox claim: %w", err)
	}
	defer func() {
		if !tx.IsClosed() {
			_ = tx.Cancel(context.Background())
		}
	}()
	now, err := databaseNow(ctx, tx)
	if err != nil {
		return nil, err
	}
	jobTypes := make([]string, len(request.JobTypes))
	for index, jobType := range request.JobTypes {
		jobTypes[index] = string(jobType)
	}
	candidates, err := queryOutboxRows(ctx, tx, `SELECT * FROM outbox_job
		WHERE job_type IN $job_types AND (
			(state = "pending" AND available_at <= $now) OR
			(state = "leased" AND lease_expires_at != NONE AND lease_expires_at <= $now)
		)
		ORDER BY available_at ASC, created_at ASC, workflow_id ASC
		LIMIT $limit`, map[string]any{"job_types": jobTypes, "now": now, "limit": request.Limit})
	if err != nil {
		return nil, err
	}
	leases := make([]store.OutboxLease, 0, len(candidates))
	leaseAges := make([]time.Duration, 0, len(candidates))
	leaseExpiry := now.Add(request.LeaseDuration)
	for _, candidate := range candidates {
		updated, updateErr := queryOutboxRows(ctx, tx, `UPDATE $id SET
			state = "leased",
			lease_owner = $worker_id,
			lease_generation += 1,
			lease_expires_at = $lease_expires_at,
			attempt_count += 1
		WHERE (state = "pending" AND available_at <= $now) OR
			(state = "leased" AND lease_expires_at != NONE AND lease_expires_at <= $now)
		RETURN AFTER`, map[string]any{
			"id": candidate.ID, "worker_id": request.WorkerID, "lease_expires_at": leaseExpiry, "now": now,
		})
		if updateErr != nil {
			return nil, updateErr
		}
		if len(updated) == 1 {
			leases = append(leases, leaseFromRow(updated[0]))
			if !updated[0].CreatedAt.IsZero() {
				leaseAges = append(leaseAges, now.Sub(updated[0].CreatedAt))
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	for _, age := range leaseAges {
		observability.RecordOutboxLeaseAge(ctx, age)
	}
	return leases, nil
}

func (r *OutboxRepository) CompleteOutbox(ctx context.Context, request store.CompleteOutboxRequest) error {
	if err := store.ValidateCompleteOutbox(request); err != nil {
		return err
	}
	return r.transition(ctx, request.TenantID, request.WorkflowID, request.WorkerID, request.FencingToken, func(ctx context.Context, tx *surrealdb.Transaction, row outboxRow, now time.Time) error {
		if row.State == string(store.OutboxStateCompleted) {
			return nil
		}
		if row.State != string(store.OutboxStateLeased) {
			return store.ErrStaleLease
		}
		updated, err := queryOutboxRows(ctx, tx, `UPDATE $id SET state = "completed", lease_expires_at = NONE, completed_at = $now
			WHERE state = "leased" AND lease_owner = $worker_id AND lease_generation = $generation RETURN AFTER`, map[string]any{
			"id": row.ID, "worker_id": request.WorkerID, "generation": int(request.FencingToken), "now": now,
		})
		if err == nil && len(updated) != 1 {
			return store.ErrStaleLease
		}
		return err
	})
}

func (r *OutboxRepository) ReleaseOutbox(ctx context.Context, request store.ReleaseOutboxRequest) error {
	if err := store.ValidateReleaseOutbox(request); err != nil {
		return err
	}
	return r.transition(ctx, request.TenantID, request.WorkflowID, request.WorkerID, request.FencingToken, func(ctx context.Context, tx *surrealdb.Transaction, row outboxRow, now time.Time) error {
		target := string(store.OutboxStatePending)
		if request.DeadLetter {
			target = string(store.OutboxStateDeadLetter)
		}
		if row.State == target {
			return nil
		}
		if row.State != string(store.OutboxStateLeased) {
			return store.ErrStaleLease
		}
		updated, err := queryOutboxRows(ctx, tx, `UPDATE $id SET
			state = $state,
			available_at = $available_at,
			lease_expires_at = NONE,
			last_error_code = $error_code
		WHERE state = "leased" AND lease_owner = $worker_id AND lease_generation = $generation RETURN AFTER`, map[string]any{
			"id": row.ID, "state": target, "available_at": now.Add(request.RetryDelay), "error_code": request.ErrorCode,
			"worker_id": request.WorkerID, "generation": int(request.FencingToken),
		})
		if err == nil && len(updated) != 1 {
			return store.ErrStaleLease
		}
		return err
	})
}

func (r *OutboxRepository) transition(
	ctx context.Context,
	tenantID domain.TenantID,
	workflowID string,
	workerID string,
	fencingToken uint64,
	apply func(context.Context, *surrealdb.Transaction, outboxRow, time.Time) error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil || r.db == nil {
		return errors.New("SurrealDB outbox repository is not configured")
	}
	const attempts = 8
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		err := r.transitionOnce(ctx, tenantID, workflowID, workerID, fencingToken, apply)
		if err == nil || errors.Is(err, store.ErrStaleLease) || errors.Is(err, store.ErrOutboxJobNotFound) {
			return err
		}
		lastErr = err
		if !surrealdb.IsTransactionConflict(err) {
			return databaseFailure("transition outbox job", err)
		}
		observability.RecordTransactionRetry(ctx)
		if err := waitForRetry(ctx, attempt); err != nil {
			return err
		}
	}
	return &store.OpError{Operation: "transition outbox job after conflict retries", Retryable: true, Err: lastErr}
}

func (r *OutboxRepository) transitionOnce(
	ctx context.Context,
	tenantID domain.TenantID,
	workflowID string,
	workerID string,
	fencingToken uint64,
	apply func(context.Context, *surrealdb.Transaction, outboxRow, time.Time) error,
) (err error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if !tx.IsClosed() {
			_ = tx.Cancel(context.Background())
		}
	}()
	rows, err := queryOutboxRows(ctx, tx, `SELECT * FROM outbox_job
		WHERE tenant_id = $tenant_id AND workflow_id = $workflow_id LIMIT 1`, map[string]any{
		"tenant_id": string(tenantID), "workflow_id": workflowID,
	})
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return store.ErrOutboxJobNotFound
	}
	row := rows[0]
	if row.LeaseGeneration != int(fencingToken) || row.LeaseOwner == nil || *row.LeaseOwner != workerID ||
		(row.State != string(store.OutboxStateLeased) && row.State != string(store.OutboxStateCompleted) && row.State != string(store.OutboxStatePending) && row.State != string(store.OutboxStateDeadLetter)) {
		return store.ErrStaleLease
	}
	now, err := databaseNow(ctx, tx)
	if err != nil {
		return err
	}
	if row.State == string(store.OutboxStateLeased) && (row.LeaseExpiresAt == nil || !row.LeaseExpiresAt.After(now)) {
		return store.ErrStaleLease
	}
	if err := apply(ctx, tx, row, now); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func databaseNow[S interface {
	*surrealdb.DB | *surrealdb.Transaction
}](ctx context.Context, sender S) (time.Time, error) {
	results, err := surrealdb.Query[time.Time](ctx, sender, `RETURN time::now()`, nil)
	if err != nil || results == nil || len(*results) == 0 {
		return time.Time{}, err
	}
	return (*results)[0].Result.UTC(), nil
}

func queryOutboxRows(ctx context.Context, tx *surrealdb.Transaction, statement string, variables map[string]any) ([]outboxRow, error) {
	results, err := surrealdb.Query[[]outboxRow](ctx, tx, statement, variables)
	if err != nil || results == nil || len(*results) == 0 {
		return nil, err
	}
	return (*results)[0].Result, nil
}

func leaseFromRow(row outboxRow) store.OutboxLease {
	lease := store.OutboxLease{
		TenantID: domain.TenantID(row.TenantID), WorkflowID: row.WorkflowID, TraceID: domain.TraceID(row.TraceID),
		JobType: store.OutboxJobType(row.JobType), ContentHash: row.ContentHash, AttemptCount: row.AttemptCount,
		FencingToken: uint64(row.LeaseGeneration), LeaseOwner: valueOrEmpty(row.LeaseOwner),
	}
	if row.OutcomeID != nil {
		lease.OutcomeID = domain.OutcomeID(*row.OutcomeID)
	}
	if row.LeaseExpiresAt != nil {
		lease.LeaseExpiresAt = row.LeaseExpiresAt.UTC()
	}
	return lease
}

func valueOrEmpty(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
