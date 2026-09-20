package surreal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/maintenance"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

type MaintenanceRepository struct{ db *surrealdb.DB }

func NewMaintenanceRepository(db *surrealdb.DB) *MaintenanceRepository {
	return &MaintenanceRepository{db: db}
}

type maintenanceDBRow struct {
	ID              models.RecordID   `json:"id"`
	TenantID        string            `json:"tenant_id"`
	JobID           string            `json:"maintenance_job_id"`
	Kind            maintenance.Kind  `json:"job_kind"`
	IdempotencyHash string            `json:"idempotency_key_hash"`
	CanonicalJob    string            `json:"canonical_job"`
	State           maintenance.State `json:"state"`
	Attempt         uint32            `json:"attempt_count"`
	Fencing         uint64            `json:"fencing_token"`
	Owner           *string           `json:"lease_owner"`
	LeaseExpires    *time.Time        `json:"lease_expires_at"`
	Available       time.Time         `json:"available_at"`
	Created         time.Time         `json:"created_at"`
	ContentHash     string            `json:"content_hash"`
}

func (r *MaintenanceRepository) Enqueue(ctx context.Context, spec maintenance.Spec) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil || r.db == nil {
		return errors.New("SurrealDB maintenance repository is not configured")
	}
	if maintenance.ValidateSpec(spec) != nil {
		return maintenance.ErrInvalidJob
	}
	if existing, found, err := findMaintenanceByIdempotency(ctx, r.db, spec.TenantID, spec.Kind, spec.IdempotencyKeyHash); err != nil {
		return databaseFailure("find maintenance idempotency", err)
	} else if found {
		if existing.ContentHash != spec.ContentHash {
			return maintenance.ErrJobConflict
		}
		return nil
	}
	record := map[string]any{"tenant_id": string(spec.TenantID), "maintenance_job_id": spec.ID, "job_kind": string(spec.Kind), "idempotency_key_hash": spec.IdempotencyKeyHash, "canonical_payload": string(spec.CanonicalPayload), "canonical_job": string(spec.CanonicalJSON), "state": string(maintenance.Pending), "attempt_count": 0, "fencing_token": 0, "available_at": spec.CreatedAt, "created_at": spec.CreatedAt, "updated_at": spec.CreatedAt, "schema_version": spec.SchemaVersion, "content_hash": spec.ContentHash}
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return databaseFailure("begin maintenance enqueue", err)
	}
	err = createRecord(ctx, tx, models.NewRecordID("maintenance_job", spec.ID), record)
	if err == nil {
		err = tx.Commit(ctx)
	} else {
		_ = tx.Cancel(context.Background())
	}
	if !tx.IsClosed() {
		_ = tx.Cancel(context.Background())
	}
	if err != nil {
		if existing, found, lookupErr := findMaintenanceByIdempotency(ctx, r.db, spec.TenantID, spec.Kind, spec.IdempotencyKeyHash); lookupErr == nil && found {
			if existing.ContentHash != spec.ContentHash {
				return maintenance.ErrJobConflict
			}
			return nil
		}
		return databaseFailure("enqueue maintenance job", err)
	}
	return nil
}

func (r *MaintenanceRepository) Claim(ctx context.Context, request maintenance.ClaimRequest) ([]maintenance.Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r == nil || r.db == nil || request.WorkerID == "" || request.Limit < 1 || request.Limit > 100 || request.LeaseDuration < time.Second || maintenance.ValidateKinds(request.Kinds) != nil {
		return nil, maintenance.ErrInvalidJob
	}
	const attempts = 8
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		leases, err := r.claimMaintenanceOnce(ctx, request)
		if err == nil {
			return leases, nil
		}
		lastErr = err
		if !surrealdb.IsTransactionConflict(err) {
			return nil, databaseFailure("claim maintenance jobs", err)
		}
		if err := waitForRetry(ctx, attempt); err != nil {
			return nil, err
		}
	}
	return nil, databaseFailure("claim maintenance jobs after conflict retries", lastErr)
}

func (r *MaintenanceRepository) claimMaintenanceOnce(ctx context.Context, request maintenance.ClaimRequest) (_ []maintenance.Lease, err error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return nil, err
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
	kinds := make([]string, len(request.Kinds))
	for index, kind := range request.Kinds {
		kinds[index] = string(kind)
	}
	rows, err := queryMaintenanceRows(ctx, tx, `SELECT * FROM maintenance_job WHERE job_kind IN $kinds AND ((state = "pending" AND available_at <= $now) OR (state = "leased" AND lease_expires_at != NONE AND lease_expires_at <= $now)) ORDER BY available_at ASC, created_at ASC, maintenance_job_id ASC LIMIT $limit`, map[string]any{"kinds": kinds, "now": now, "limit": request.Limit})
	if err != nil {
		return nil, err
	}
	leases := make([]maintenance.Lease, 0, len(rows))
	expiry := now.Add(request.LeaseDuration)
	for _, row := range rows {
		updated, err := queryMaintenanceRows(ctx, tx, `UPDATE $id SET state = "leased", lease_owner = $worker, fencing_token += 1, lease_expires_at = $expiry, attempt_count += 1, updated_at = $now WHERE (state = "pending" AND available_at <= $now) OR (state = "leased" AND lease_expires_at != NONE AND lease_expires_at <= $now) RETURN AFTER`, map[string]any{"id": row.ID, "worker": request.WorkerID, "expiry": expiry, "now": now})
		if err != nil {
			return nil, err
		}
		if len(updated) == 1 {
			lease, err := maintenanceLease(updated[0])
			if err != nil {
				return nil, err
			}
			leases = append(leases, lease)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return leases, nil
}

func (r *MaintenanceRepository) Complete(ctx context.Context, request maintenance.FinishRequest) error {
	return r.transitionMaintenance(ctx, request, func(ctx context.Context, tx *surrealdb.Transaction, row maintenanceDBRow, now time.Time) error {
		updated, err := queryMaintenanceRows(ctx, tx, `UPDATE $id SET state = "completed", lease_owner = NONE, lease_expires_at = NONE, updated_at = $now WHERE state = "leased" AND lease_owner = $worker AND fencing_token = $fencing RETURN AFTER`, map[string]any{"id": row.ID, "worker": request.WorkerID, "fencing": request.FencingToken, "now": now})
		if err == nil && len(updated) != 1 {
			return maintenance.ErrStaleLease
		}
		return err
	})
}

func (r *MaintenanceRepository) Fail(ctx context.Context, request maintenance.FailRequest) error {
	if request.ErrorCode == "" || request.RetryAt.IsZero() || !request.RetryAt.Equal(request.RetryAt.UTC()) {
		return maintenance.ErrInvalidJob
	}
	return r.transitionMaintenance(ctx, request.FinishRequest, func(ctx context.Context, tx *surrealdb.Transaction, row maintenanceDBRow, now time.Time) error {
		state := maintenance.Pending
		if request.DeadLetter {
			state = maintenance.DeadLetter
		}
		updated, err := queryMaintenanceRows(ctx, tx, `UPDATE $id SET state = $state, available_at = $available, last_error_code = $code, lease_owner = NONE, lease_expires_at = NONE, updated_at = $now WHERE state = "leased" AND lease_owner = $worker AND fencing_token = $fencing RETURN AFTER`, map[string]any{"id": row.ID, "state": string(state), "available": request.RetryAt, "code": request.ErrorCode, "worker": request.WorkerID, "fencing": request.FencingToken, "now": now})
		if err == nil && len(updated) != 1 {
			return maintenance.ErrStaleLease
		}
		return err
	})
}

func (r *MaintenanceRepository) Stats(ctx context.Context) ([]maintenance.QueueStats, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r == nil || r.db == nil {
		return nil, maintenance.ErrInvalidJob
	}
	now, err := databaseNow(ctx, r.db)
	if err != nil {
		return nil, databaseFailure("read maintenance clock", err)
	}
	rows, err := queryMaintenanceRows(ctx, r.db, `SELECT * FROM maintenance_job WHERE state = "pending" OR (state = "leased" AND lease_expires_at != NONE AND lease_expires_at <= $now)`, map[string]any{"now": now})
	if err != nil {
		return nil, databaseFailure("read maintenance backlog", err)
	}
	byKind := make(map[maintenance.Kind]maintenance.QueueStats)
	for _, row := range rows {
		value := byKind[row.Kind]
		value.Kind, value.Pending = row.Kind, value.Pending+1
		age := now.Sub(row.Available)
		if age < 0 {
			age = 0
		}
		if age > value.OldestPendingAge {
			value.OldestPendingAge = age
		}
		byKind[row.Kind] = value
	}
	result := make([]maintenance.QueueStats, 0, len(byKind))
	for _, value := range byKind {
		result = append(result, value)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Kind < result[j].Kind })
	return result, nil
}

// ExpireTransient removes bounded batches of tenant-owned replay and decision
// artifacts. Child tables are deleted before their parent records. The cutoff
// is supplied by the authenticated job, so retries select the same time range.
func (r *MaintenanceRepository) ExpireTransient(ctx context.Context, tenantID domain.TenantID, cutoff time.Time, maximumRows uint32) (_ error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil || r.db == nil || tenantID == "" || cutoff.IsZero() || !cutoff.Equal(cutoff.UTC()) || maximumRows == 0 || maximumRows > 100_000 {
		return maintenance.ErrInvalidJob
	}
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return databaseFailure("begin transient expiry", err)
	}
	defer func() {
		if !tx.IsClosed() {
			_ = tx.Cancel(context.Background())
		}
	}()
	statements := []string{
		`DELETE (SELECT id FROM selection_plan_gap WHERE tenant_id = $tenant_id AND expires_at <= $cutoff LIMIT $limit)`,
		`DELETE (SELECT id FROM selection_plan_edge WHERE tenant_id = $tenant_id AND expires_at <= $cutoff LIMIT $limit)`,
		`DELETE (SELECT id FROM selection_plan_node WHERE tenant_id = $tenant_id AND expires_at <= $cutoff LIMIT $limit)`,
		`DELETE (SELECT id FROM outcome_credit WHERE tenant_id = $tenant_id AND expires_at <= $cutoff LIMIT $limit)`,
		`DELETE (SELECT id FROM selection_record WHERE tenant_id = $tenant_id AND expires_at <= $cutoff LIMIT $limit)`,
		`DELETE (SELECT id FROM retrieval_ranked_candidate WHERE tenant_id = $tenant_id AND expires_at <= $cutoff LIMIT $limit)`,
		`DELETE (SELECT id FROM retrieval_gate_decision WHERE tenant_id = $tenant_id AND expires_at <= $cutoff LIMIT $limit)`,
		`DELETE (SELECT id FROM retrieval_channel_hit WHERE tenant_id = $tenant_id AND expires_at <= $cutoff LIMIT $limit)`,
		`DELETE (SELECT id FROM retrieval_channel_result WHERE tenant_id = $tenant_id AND expires_at <= $cutoff LIMIT $limit)`,
		`DELETE (SELECT id FROM retrieval_run WHERE tenant_id = $tenant_id AND expires_at <= $cutoff LIMIT $limit)`,
		`DELETE (SELECT id FROM experiment_assignment WHERE tenant_id = $tenant_id AND expires_at <= $cutoff LIMIT $limit)`,
		`DELETE (SELECT id FROM pipeline_stage_artifact WHERE tenant_id = $tenant_id AND expires_at <= $cutoff LIMIT $limit)`,
	}
	variables := map[string]any{"tenant_id": string(tenantID), "cutoff": cutoff, "limit": maximumRows}
	for _, statement := range statements {
		if _, err := surrealdb.Query[any](ctx, tx, statement, variables); err != nil {
			return databaseFailure("expire transient records", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return databaseFailure("commit transient expiry", err)
	}
	return nil
}

// VerifyExposureBudgets independently counts durable causal challenger
// failures and proves that synchronous safety heads have not under-counted
// them. Heads may be higher because an external safety feed can conservatively
// advance the same counters.
func (r *MaintenanceRepository) VerifyExposureBudgets(ctx context.Context, tenantID domain.TenantID, windowStart time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil || r.db == nil || tenantID == "" || windowStart.IsZero() || !windowStart.Equal(windowStart.UTC()) {
		return maintenance.ErrInvalidJob
	}
	tenantCount, err := countCausalChallengerFailures(ctx, r.db, string(tenantID), windowStart)
	if err != nil {
		return databaseFailure("aggregate tenant experiment exposure", err)
	}
	globalCount, err := countCausalChallengerFailures(ctx, r.db, "", windowStart)
	if err != nil {
		return databaseFailure("aggregate global experiment exposure", err)
	}
	tenantBudget, err := readExperimentBudget(ctx, r.db, experimentBudgetID("experiment_tenant_budget_head", string(tenantID), windowStart))
	if err != nil {
		return databaseFailure("read tenant experiment budget", err)
	}
	globalBudget, err := readExperimentBudget(ctx, r.db, experimentBudgetID("experiment_global_budget_head", "_global", windowStart))
	if err != nil {
		return databaseFailure("read global experiment budget", err)
	}
	if tenantBudget.Unsafe < tenantCount || globalBudget.Unsafe < globalCount {
		return maintenance.ErrJobConflict
	}
	return nil
}

func countCausalChallengerFailures(ctx context.Context, db *surrealdb.DB, tenantID string, windowStart time.Time) (uint64, error) {
	type row struct {
		Count uint64 `json:"count"`
	}
	tenantAssignmentFilter := ""
	tenantSelectionFilter := ""
	tenantCreditFilter := ""
	if tenantID != "" {
		tenantAssignmentFilter = "tenant_id = $tenant_id AND "
		tenantSelectionFilter = "tenant_id = $tenant_id AND "
		tenantCreditFilter = "tenant_id = $tenant_id AND "
	}
	statement := `SELECT count() AS count FROM outcome_credit WHERE ` + tenantCreditFilter + `credit_class = "causal_failure" AND injection_id IN (
		SELECT VALUE injection_id FROM selection_record WHERE ` + tenantSelectionFilter + `experiment_assignment_id IN (
			SELECT VALUE experiment_assignment_id FROM experiment_assignment WHERE ` + tenantAssignmentFilter + `challenger = true AND window_start = $window_start
		)
	) GROUP ALL`
	rows, err := surrealdb.Query[[]row](ctx, db, statement, map[string]any{"tenant_id": tenantID, "window_start": windowStart})
	if err != nil || rows == nil || len(*rows) == 0 || len((*rows)[0].Result) == 0 {
		return 0, err
	}
	return (*rows)[0].Result[0].Count, nil
}

func (r *MaintenanceRepository) transitionMaintenance(ctx context.Context, request maintenance.FinishRequest, apply func(context.Context, *surrealdb.Transaction, maintenanceDBRow, time.Time) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil || r.db == nil || request.TenantID == "" || request.JobID == "" || request.WorkerID == "" || request.FencingToken == 0 {
		return maintenance.ErrInvalidJob
	}
	const attempts = 8
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		tx, err := r.db.Begin(ctx)
		if err != nil {
			return err
		}
		rows, err := queryMaintenanceRows(ctx, tx, `SELECT * FROM maintenance_job WHERE tenant_id = $tenant_id AND maintenance_job_id = $job_id LIMIT 1`, map[string]any{"tenant_id": string(request.TenantID), "job_id": request.JobID})
		if err == nil && len(rows) == 0 {
			err = maintenance.ErrJobNotFound
		}
		var now time.Time
		if err == nil {
			now, err = databaseNow(ctx, tx)
		}
		if err == nil {
			row := rows[0]
			if row.State != maintenance.Leased || row.Owner == nil || *row.Owner != request.WorkerID || row.Fencing != request.FencingToken || row.LeaseExpires == nil || !row.LeaseExpires.After(now) {
				err = maintenance.ErrStaleLease
			} else {
				err = apply(ctx, tx, row, now)
			}
		}
		if err == nil {
			err = tx.Commit(ctx)
		} else if !tx.IsClosed() {
			_ = tx.Cancel(context.Background())
		}
		if err == nil || errors.Is(err, maintenance.ErrJobNotFound) || errors.Is(err, maintenance.ErrStaleLease) {
			return err
		}
		lastErr = err
		if !surrealdb.IsTransactionConflict(err) {
			return databaseFailure("transition maintenance job", err)
		}
		if err := waitForRetry(ctx, attempt); err != nil {
			return err
		}
	}
	return databaseFailure("transition maintenance job after conflict retries", lastErr)
}

func maintenanceLease(row maintenanceDBRow) (maintenance.Lease, error) {
	var spec maintenance.Spec
	err := json.Unmarshal([]byte(row.CanonicalJob), &spec)
	spec.ID, spec.ContentHash, spec.CanonicalJSON = row.JobID, row.ContentHash, []byte(row.CanonicalJob)
	if err != nil || maintenance.ValidateSpec(spec) != nil || spec.TenantID != domain.TenantID(row.TenantID) || spec.Kind != row.Kind || spec.IdempotencyKeyHash != row.IdempotencyHash || row.Owner == nil || row.LeaseExpires == nil {
		return maintenance.Lease{}, fmt.Errorf("%w: invalid authenticated row", maintenance.ErrJobConflict)
	}
	return maintenance.Lease{Spec: spec, AttemptCount: row.Attempt, FencingToken: row.Fencing, LeaseOwner: *row.Owner, LeaseExpiresAt: *row.LeaseExpires}, nil
}

func findMaintenanceByIdempotency[S interface {
	*surrealdb.DB | *surrealdb.Transaction
}](ctx context.Context, sender S, tenantID domain.TenantID, kind maintenance.Kind, keyHash string) (maintenanceDBRow, bool, error) {
	rows, err := queryMaintenanceRows(ctx, sender, `SELECT * FROM maintenance_job WHERE tenant_id = $tenant_id AND job_kind = $kind AND idempotency_key_hash = $key LIMIT 1`, map[string]any{"tenant_id": string(tenantID), "kind": string(kind), "key": keyHash})
	if err != nil || len(rows) == 0 {
		return maintenanceDBRow{}, false, err
	}
	return rows[0], true, nil
}

func queryMaintenanceRows[S interface {
	*surrealdb.DB | *surrealdb.Transaction
}](ctx context.Context, sender S, statement string, vars map[string]any) ([]maintenanceDBRow, error) {
	results, err := surrealdb.Query[[]maintenanceDBRow](ctx, sender, statement, vars)
	if err != nil {
		return nil, err
	}
	if results == nil || len(*results) == 0 {
		return nil, nil
	}
	return (*results)[0].Result, nil
}
