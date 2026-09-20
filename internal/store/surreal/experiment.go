package surreal

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/experiment"
	"github.com/sauhard74/mem-jev/internal/observability"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

type ExperimentRepository struct{ db *surrealdb.DB }

func NewExperimentRepository(db *surrealdb.DB) *ExperimentRepository {
	return &ExperimentRepository{db: db}
}

func (r *ExperimentRepository) Reserve(ctx context.Context, request experiment.AssignmentRequest) (experiment.Reservation, error) {
	if err := ctx.Err(); err != nil {
		return experiment.Reservation{}, err
	}
	if r == nil || r.db == nil {
		return experiment.Reservation{}, errors.New("SurrealDB experiment repository is not configured")
	}
	candidate, err := experiment.Assign(request)
	if err != nil {
		return experiment.Reservation{}, err
	}
	request.Exposure.WindowStart = candidate.WindowStart
	const attempts = 16
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		reservation, err := r.reserveOnce(ctx, request, candidate)
		if err == nil {
			if reservation.Disposition == experiment.DispositionReserved {
				recordExperimentBudgetFallback(ctx, reservation.Assignment)
			}
			return reservation, nil
		}
		lastErr = err
		if existing, found, lookupErr := findExperimentAssignment(ctx, r.db, candidate.TenantID, candidate.ManifestID, candidate.AssignmentKeyHash); lookupErr != nil {
			return experiment.Reservation{}, databaseFailure("resolve experiment reservation", lookupErr)
		} else if found {
			if existing.QueryBucketHash != candidate.QueryBucketHash || existing.Risk != candidate.Risk {
				return experiment.Reservation{}, experiment.ErrExperimentConflict
			}
			return experiment.Reservation{Assignment: existing, Disposition: experiment.DispositionDuplicate}, nil
		}
		if errors.Is(err, experiment.ErrExperimentConflict) {
			return experiment.Reservation{}, err
		}
		if !surrealdb.IsTransactionConflict(err) {
			return experiment.Reservation{}, databaseFailure("reserve experiment assignment", err)
		}
		if err := waitForRetry(ctx, attempt); err != nil {
			return experiment.Reservation{}, err
		}
	}
	return experiment.Reservation{}, databaseFailure("reserve experiment assignment after conflict retries", lastErr)
}

func recordExperimentBudgetFallback(ctx context.Context, assignment experiment.Assignment) {
	if assignment.Challenger {
		return
	}
	for _, reason := range assignment.ReasonCodes {
		scope := ""
		switch reason {
		case experiment.ReasonTenantExposureCap, experiment.ReasonTenantUnsafeCeiling:
			scope = "tenant"
		case experiment.ReasonGlobalExposureCap, experiment.ReasonGlobalUnsafeCeiling:
			scope = "global"
		case experiment.ReasonConcurrentTrialCap:
			scope = "concurrent_trials"
		}
		if scope != "" {
			observability.RecordExperimentBudgetRejection(ctx, scope)
		}
	}
}

func (r *ExperimentRepository) reserveOnce(ctx context.Context, request experiment.AssignmentRequest, candidate experiment.Assignment) (_ experiment.Reservation, err error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return experiment.Reservation{}, err
	}
	defer func() {
		if !tx.IsClosed() {
			_ = tx.Cancel(context.Background())
		}
	}()
	if existing, found, err := findExperimentAssignment(ctx, tx, candidate.TenantID, candidate.ManifestID, candidate.AssignmentKeyHash); err != nil {
		return experiment.Reservation{}, err
	} else if found {
		_ = tx.Cancel(ctx)
		if existing.QueryBucketHash != candidate.QueryBucketHash || existing.Risk != candidate.Risk {
			return experiment.Reservation{}, experiment.ErrExperimentConflict
		}
		return experiment.Reservation{Assignment: existing, Disposition: experiment.DispositionDuplicate}, nil
	}
	if err := ensureExperimentManifest(ctx, tx, request.Manifest, request.AssignedAt); err != nil {
		return experiment.Reservation{}, err
	}
	tenantID := experimentBudgetID("experiment_tenant_budget_head", string(request.Manifest.TenantID), request.Exposure.WindowStart)
	globalID := experimentBudgetID("experiment_global_budget_head", "_global", request.Exposure.WindowStart)
	tenantBudget, err := readExperimentBudget(ctx, tx, tenantID)
	if err != nil {
		return experiment.Reservation{}, err
	}
	globalBudget, err := readExperimentBudget(ctx, tx, globalID)
	if err != nil {
		return experiment.Reservation{}, err
	}
	storedTenantUnsafe, storedGlobalUnsafe := tenantBudget.Unsafe, globalBudget.Unsafe
	request.Exposure.TenantExposureCount = max(request.Exposure.TenantExposureCount, tenantBudget.Exposure)
	request.Exposure.GlobalExposureCount = max(request.Exposure.GlobalExposureCount, globalBudget.Exposure)
	request.Exposure.TenantUnsafeCount = max(request.Exposure.TenantUnsafeCount, tenantBudget.Unsafe)
	request.Exposure.GlobalUnsafeCount = max(request.Exposure.GlobalUnsafeCount, globalBudget.Unsafe)
	assignment, err := experiment.Assign(request)
	if err != nil {
		return experiment.Reservation{}, err
	}
	if err := createExperimentAssignment(ctx, tx, request.Manifest, assignment); err != nil {
		return experiment.Reservation{}, err
	}
	if assignment.Challenger {
		tenantBudget.Exposure++
		globalBudget.Exposure++
	}
	tenantBudget.Unsafe, globalBudget.Unsafe = request.Exposure.TenantUnsafeCount, request.Exposure.GlobalUnsafeCount
	if assignment.Challenger || request.Exposure.TenantUnsafeCount > storedTenantUnsafe {
		if err := writeExperimentBudget(ctx, tx, tenantID, string(request.Manifest.TenantID), request.Exposure.WindowStart, tenantBudget, request.AssignedAt, "experiment-tenant-budget-head.v1"); err != nil {
			return experiment.Reservation{}, err
		}
	}
	if assignment.Challenger || request.Exposure.GlobalUnsafeCount > storedGlobalUnsafe {
		if err := writeExperimentBudget(ctx, tx, globalID, "_global", request.Exposure.WindowStart, globalBudget, request.AssignedAt, "experiment-global-budget-head.v1"); err != nil {
			return experiment.Reservation{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return experiment.Reservation{}, err
	}
	return experiment.Reservation{Assignment: assignment, Disposition: experiment.DispositionReserved}, nil
}

func (r *ExperimentRepository) Find(ctx context.Context, tenantID, manifestID, assignmentKeyHash string) (experiment.Assignment, error) {
	if err := ctx.Err(); err != nil {
		return experiment.Assignment{}, err
	}
	if r == nil || r.db == nil {
		return experiment.Assignment{}, experiment.ErrExperimentNotFound
	}
	value, found, err := findExperimentAssignment(ctx, r.db, domain.TenantID(tenantID), manifestID, assignmentKeyHash)
	if err != nil {
		return experiment.Assignment{}, databaseFailure("find experiment assignment", err)
	}
	if !found {
		return experiment.Assignment{}, experiment.ErrExperimentNotFound
	}
	return value, nil
}

func ensureExperimentManifest(ctx context.Context, tx *surrealdb.Transaction, manifest experiment.Manifest, createdAt time.Time) error {
	type row struct {
		ID   string `json:"experiment_manifest_id"`
		Hash string `json:"content_hash"`
	}
	rows, err := surrealdb.Query[[]row](ctx, tx, `SELECT experiment_manifest_id, content_hash FROM experiment_manifest WHERE tenant_id = $tenant_id AND procedure_id = $procedure_id AND experiment_epoch = $epoch LIMIT 1`, map[string]any{"tenant_id": string(manifest.TenantID), "procedure_id": manifest.ProcedureID, "epoch": manifest.ExperimentEpoch})
	if err != nil {
		return err
	}
	if rows != nil && len(*rows) > 0 && len((*rows)[0].Result) > 0 {
		stored := (*rows)[0].Result[0]
		if stored.ID != manifest.ID || stored.Hash != manifest.ContentHash {
			return experiment.ErrExperimentConflict
		}
		return nil
	}
	record := map[string]any{"tenant_id": string(manifest.TenantID), "experiment_manifest_id": manifest.ID, "procedure_id": manifest.ProcedureID, "champion_version_id": manifest.ChampionVersionID, "challenger_version_ids": manifest.ChallengerVersionIDs, "experiment_epoch": manifest.ExperimentEpoch, "exposure_limits": map[string]any{"challenger_exposure_ppm": manifest.ChallengerExposurePPM, "maximum_concurrent_trials": manifest.MaximumConcurrentTrials, "tenant_exposure_limit": manifest.TenantExposureLimit, "global_exposure_limit": manifest.GlobalExposureLimit, "window_seconds": manifest.ExposureWindowSeconds, "assignment_ttl_seconds": manifest.AssignmentTTLSeconds}, "risk_policy": map[string]any{"tenant_unsafe_outcome_ceiling": manifest.TenantUnsafeOutcomeCeiling, "global_unsafe_outcome_ceiling": manifest.GlobalUnsafeOutcomeCeiling, "tenant_high_risk_opt_in": manifest.TenantHighRiskOptIn}, "canonical_manifest": string(manifest.CanonicalJSON), "created_at": createdAt, "schema_version": manifest.SchemaVersion, "content_hash": manifest.ContentHash}
	return createRecord(ctx, tx, experimentRecordID("experiment_manifest", string(manifest.TenantID), manifest.ID), record)
}

func createExperimentAssignment(ctx context.Context, tx *surrealdb.Transaction, manifest experiment.Manifest, assignment experiment.Assignment) error {
	record := map[string]any{"tenant_id": string(assignment.TenantID), "experiment_assignment_id": assignment.ID, "experiment_manifest_id": assignment.ManifestID, "assignment_key_hash": assignment.AssignmentKeyHash, "assigned_procedure_version_id": assignment.AssignedProcedureVersionID, "bucket": assignment.Bucket, "canonical_assignment": string(assignment.CanonicalJSON), "created_at": assignment.CreatedAt, "expires_at": assignment.ExpiresAt, "schema_version": assignment.SchemaVersion, "content_hash": assignment.ContentHash, "procedure_id": manifest.ProcedureID, "experiment_epoch": manifest.ExperimentEpoch, "query_bucket_hash": assignment.QueryBucketHash, "challenger": assignment.Challenger, "risk": string(assignment.Risk), "reason_codes": assignment.ReasonCodes, "window_start": assignment.WindowStart}
	return createRecord(ctx, tx, models.NewRecordID("experiment_assignment", assignment.ID), record)
}

type experimentAssignmentRow struct {
	ID        string `json:"experiment_assignment_id"`
	Hash      string `json:"content_hash"`
	Canonical string `json:"canonical_assignment"`
}

func findExperimentAssignment[S interface {
	*surrealdb.DB | *surrealdb.Transaction
}](ctx context.Context, sender S, tenantID domain.TenantID, manifestID, keyHash string) (experiment.Assignment, bool, error) {
	rows, err := surrealdb.Query[[]experimentAssignmentRow](ctx, sender, `SELECT experiment_assignment_id, content_hash, canonical_assignment FROM experiment_assignment WHERE tenant_id = $tenant_id AND experiment_manifest_id = $manifest_id AND assignment_key_hash = $key_hash LIMIT 1`, map[string]any{"tenant_id": string(tenantID), "manifest_id": manifestID, "key_hash": keyHash})
	if err != nil || rows == nil || len(*rows) == 0 || len((*rows)[0].Result) == 0 {
		return experiment.Assignment{}, false, err
	}
	stored := (*rows)[0].Result[0]
	var assignment experiment.Assignment
	if err := json.Unmarshal([]byte(stored.Canonical), &assignment); err != nil {
		return experiment.Assignment{}, false, err
	}
	assignment.ID, assignment.ContentHash, assignment.CanonicalJSON = stored.ID, stored.Hash, []byte(stored.Canonical)
	if experiment.ValidateAssignment(assignment) != nil {
		return experiment.Assignment{}, false, experiment.ErrExperimentConflict
	}
	return assignment, true, nil
}

type experimentBudget struct {
	Exposure uint64 `json:"exposure_count"`
	Unsafe   uint64 `json:"unsafe_count"`
}

func readExperimentBudget[S interface {
	*surrealdb.DB | *surrealdb.Transaction
}](ctx context.Context, sender S, id models.RecordID) (experimentBudget, error) {
	rows, err := surrealdb.Query[[]experimentBudget](ctx, sender, `SELECT exposure_count, unsafe_count FROM $id`, map[string]any{"id": id})
	if err != nil || rows == nil || len(*rows) == 0 || len((*rows)[0].Result) == 0 {
		return experimentBudget{}, err
	}
	return (*rows)[0].Result[0], nil
}

func writeExperimentBudget(ctx context.Context, tx *surrealdb.Transaction, id models.RecordID, tenantID string, windowStart time.Time, budget experimentBudget, updatedAt time.Time, schemaVersion string) error {
	record := map[string]any{"tenant_id": tenantID, "window_start": windowStart, "exposure_count": budget.Exposure, "unsafe_count": budget.Unsafe, "updated_at": updatedAt, "schema_version": schemaVersion}
	_, hash, err := canonical.MarshalAndHash(record)
	if err != nil {
		return err
	}
	record["content_hash"] = hash
	_, err = surrealdb.Query[any](ctx, tx, `UPSERT $id CONTENT $record`, map[string]any{"id": id, "record": record})
	return err
}

func experimentBudgetID(table, scope string, windowStart time.Time) models.RecordID {
	return experimentRecordID(table, scope, windowStart.UTC().Format(time.RFC3339Nano))
}

func experimentRecordID(table string, values ...string) models.RecordID {
	hasher := sha256.New()
	var length [8]byte
	for _, value := range values {
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = hasher.Write(length[:])
		_, _ = hasher.Write([]byte(value))
	}
	return models.NewRecordID(table, "exp_"+hex.EncodeToString(hasher.Sum(nil)))
}
