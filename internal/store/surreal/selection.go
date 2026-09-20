package surreal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/selection"
	"github.com/sauhard74/mem-jev/internal/store"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

type SelectionRepository struct {
	db      *surrealdb.DB
	policy  selection.RetentionPolicy
	now     func() time.Time
	failure bool
}

func NewSelectionRepository(db *surrealdb.DB, policy selection.RetentionPolicy) (*SelectionRepository, error) {
	if db == nil || policy.Validate() != nil {
		return nil, selection.ErrInvalidSelection
	}
	return &SelectionRepository{db: db, policy: policy, now: time.Now}, nil
}

func (r *SelectionRepository) Commit(ctx context.Context, request selection.CommitRequest) (selection.Receipt, error) {
	if err := ctx.Err(); err != nil {
		return selection.Receipt{}, err
	}
	if r == nil || r.db == nil || selection.ValidateCommitRequest(request) != nil {
		return selection.Receipt{}, selection.ErrInvalidSelection
	}
	const maximumAttempts = 8
	var lastErr error
	for attempt := 0; attempt < maximumAttempts; attempt++ {
		receipt, err := r.commitOnce(ctx, request)
		if err == nil || errors.Is(err, selection.ErrIdempotencyConflict) || errors.Is(err, selection.ErrRetrievalRunConflict) || errors.Is(err, selection.ErrSelectionExpired) {
			return receipt, err
		}
		lastErr = err
		existing, found, lookupErr := findSelectionByIdempotency(ctx, r.db, request.TenantID, request.IdempotencyIdentityHash)
		if lookupErr != nil {
			return selection.Receipt{}, databaseFailure("resolve selection commit", lookupErr)
		}
		if found && r.now().UTC().Before(existing.ExpiresAt) {
			return resolveSelection(existing, request)
		}
		if found {
			return selection.Receipt{}, selection.ErrSelectionExpired
		}
		if !surrealdb.IsTransactionConflict(err) {
			return selection.Receipt{}, databaseFailure("commit selection", err)
		}
		if err = waitForRetry(ctx, attempt); err != nil {
			return selection.Receipt{}, err
		}
	}
	return selection.Receipt{}, &store.OpError{Operation: "commit selection after conflict retries", Retryable: true, Err: lastErr}
}

func (r *SelectionRepository) commitOnce(ctx context.Context, request selection.CommitRequest) (_ selection.Receipt, err error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return selection.Receipt{}, err
	}
	defer func() {
		if !tx.IsClosed() {
			_ = tx.Cancel(context.Background())
		}
	}()
	now := r.now().UTC()
	if existing, found, findErr := findSelectionByIdempotency(ctx, tx, request.TenantID, request.IdempotencyIdentityHash); findErr != nil {
		return selection.Receipt{}, findErr
	} else if found {
		if now.Before(existing.ExpiresAt) {
			if cancelErr := tx.Cancel(ctx); cancelErr != nil {
				return selection.Receipt{}, cancelErr
			}
			return resolveSelection(existing, request)
		}
		return selection.Receipt{}, selection.ErrSelectionExpired
	}
	if existing, found, findErr := findSelectionByRun(ctx, tx, request.TenantID, request.Draft.RetrievalRunID); findErr != nil {
		return selection.Receipt{}, findErr
	} else if found {
		if now.Before(existing.ExpiresAt) {
			if cancelErr := tx.Cancel(ctx); cancelErr != nil {
				return selection.Receipt{}, cancelErr
			}
			if existing.DraftHash != request.Draft.ContentHash || existing.IdempotencyIdentityHash != request.IdempotencyIdentityHash {
				return selection.Receipt{}, selection.ErrRetrievalRunConflict
			}
			return selection.Receipt{Record: existing, Disposition: selection.DispositionDuplicate}, nil
		}
		return selection.Receipt{}, selection.ErrSelectionExpired
	}
	createdAt := now
	record, err := selection.NewRecord(request.Draft, request.IdempotencyIdentityHash, createdAt, createdAt.Add(r.policy.TTL))
	if err != nil {
		return selection.Receipt{}, err
	}
	if err = createSelectionRecord(ctx, tx, record); err != nil {
		return selection.Receipt{}, err
	}
	if err = createSelectionChildren(ctx, tx, record); err != nil {
		return selection.Receipt{}, err
	}
	if r.failure {
		return selection.Receipt{}, errInjectedFailure
	}
	if err = tx.Commit(ctx); err != nil {
		return selection.Receipt{}, err
	}
	return selection.Receipt{Record: record, Disposition: selection.DispositionCommitted}, nil
}

func (r *SelectionRepository) FindByInjectionID(ctx context.Context, tenantID domain.TenantID, injectionID string) (selection.Record, error) {
	if err := ctx.Err(); err != nil {
		return selection.Record{}, err
	}
	if r == nil || r.db == nil || tenantID == "" || injectionID == "" {
		return selection.Record{}, selection.ErrInvalidSelection
	}
	record, found, err := findSelection(ctx, r.db, `tenant_id = $tenant_id AND injection_id = $identity`, map[string]any{"tenant_id": string(tenantID), "identity": injectionID})
	if err != nil {
		return selection.Record{}, databaseFailure("find selection by injection", err)
	}
	if !found || !r.now().UTC().Before(record.ExpiresAt) {
		return selection.Record{}, selection.ErrSelectionNotFound
	}
	return record, nil
}

func (r *SelectionRepository) FindByRetrievalRunID(ctx context.Context, tenantID domain.TenantID, runID string) (selection.Record, error) {
	if err := ctx.Err(); err != nil {
		return selection.Record{}, err
	}
	if r == nil || r.db == nil || tenantID == "" || runID == "" {
		return selection.Record{}, selection.ErrInvalidSelection
	}
	record, found, err := findSelectionByRun(ctx, r.db, tenantID, runID)
	if err != nil {
		return selection.Record{}, databaseFailure("find selection by retrieval run", err)
	}
	if !found || !r.now().UTC().Before(record.ExpiresAt) {
		return selection.Record{}, selection.ErrSelectionNotFound
	}
	return record, nil
}

func createSelectionRecord(ctx context.Context, tx *surrealdb.Transaction, record selection.Record) error {
	value := map[string]any{
		"tenant_id": string(record.TenantID), "injection_id": record.InjectionID, "idempotency_identity_hash": record.IdempotencyIdentityHash,
		"query_hash": record.QueryHash, "request_context_hash": record.RequestContextHash, "retrieval_run_id": record.RetrievalRunID,
		"projection_epoch": record.ProjectionEpoch, "document_set_hash": record.DocumentSetHash, "serving_config_id": record.ServingConfigID,
		"policy_manifest_id": record.PolicyManifestID, "ranker_manifest_id": record.RankerManifestID, "planner_manifest_id": record.PlannerManifestID,
		"lifecycle_policy_manifest_id": record.LifecyclePolicyManifestID,
		"novelty_class":                string(record.NoveltyClass), "plan_hash": record.Plan.ContentHash, "draft_hash": record.DraftHash,
		"canonical_selection": string(record.CanonicalJSON), "created_at": record.CreatedAt, "expires_at": record.ExpiresAt,
		"schema_version": record.SchemaVersion, "content_hash": record.ContentHash,
	}
	if record.ExperimentManifestID != "" {
		value["experiment_manifest_id"], value["experiment_assignment_id"] = record.ExperimentManifestID, record.ExperimentAssignmentID
	}
	return createRecord(ctx, tx, models.NewRecordID("selection_record", record.InjectionID), value)
}

func createSelectionChildren(ctx context.Context, tx *surrealdb.Transaction, record selection.Record) error {
	groupByVersion := make(map[string]string)
	if record.ParallelSchedule != nil {
		for _, group := range record.ParallelSchedule.Groups {
			for _, versionID := range group.NodeVersionIDs {
				groupByVersion[versionID] = group.ID
			}
		}
	}
	position := make(map[string]uint32, len(record.Plan.Nodes))
	for _, node := range record.Plan.Nodes {
		position[node.VersionID] = node.Ordinal
		role := "primary"
		if node.Bridge {
			role = "bridge"
		}
		identity := struct {
			InjectionID     string `json:"injection_id"`
			Ordinal         uint32 `json:"ordinal"`
			VersionID       string `json:"procedure_version_id"`
			InterfaceHash   string `json:"interface_hash"`
			Role            string `json:"role"`
			ParallelGroupID string `json:"parallel_group_id,omitempty"`
		}{record.InjectionID, node.Ordinal, node.VersionID, node.InterfaceHash, role, groupByVersion[node.VersionID]}
		_, hash, err := canonical.MarshalAndHash(identity)
		if err != nil {
			return err
		}
		nodeRecord := map[string]any{
			"tenant_id": string(record.TenantID), "injection_id": record.InjectionID, "ordinal": node.Ordinal, "procedure_version_id": node.VersionID, "interface_hash": node.InterfaceHash,
			"role": role, "created_at": record.CreatedAt, "expires_at": record.ExpiresAt, "schema_version": "selection-plan-node.v1", "content_hash": hash,
		}
		if groupID := groupByVersion[node.VersionID]; groupID != "" {
			nodeRecord["parallel_group_id"] = groupID
		}
		if err = createRecord(ctx, tx, models.NewRecordID("selection_plan_node", fmt.Sprintf("%s_%d", record.InjectionID, node.Ordinal)), nodeRecord); err != nil {
			return err
		}
	}
	for _, edge := range record.Plan.Dependencies {
		identity := struct {
			InjectionID             string   `json:"injection_id"`
			EdgeID                  string   `json:"compatibility_edge_id"`
			SourceOrdinal           uint32   `json:"source_ordinal"`
			TargetOrdinal           uint32   `json:"target_ordinal"`
			SourceProvisionIDs      []string `json:"source_provision_ids"`
			SatisfiedRequirementIDs []string `json:"satisfied_requirement_ids"`
		}{record.InjectionID, edge.CompatibilityEdgeID, position[edge.SourceVersionID], position[edge.TargetVersionID], edge.SourceProvisionIDs, edge.SatisfiedRequirementIDs}
		_, hash, err := canonical.MarshalAndHash(identity)
		if err != nil {
			return err
		}
		if err = createRecord(ctx, tx, models.NewRecordID("selection_plan_edge", edge.CompatibilityEdgeID+"_"+record.InjectionID), map[string]any{
			"tenant_id": string(record.TenantID), "injection_id": record.InjectionID, "plan_edge_id": edge.CompatibilityEdgeID,
			"source_ordinal": position[edge.SourceVersionID], "target_ordinal": position[edge.TargetVersionID], "compatibility_edge_id": edge.CompatibilityEdgeID,
			"source_provision_ids": edge.SourceProvisionIDs, "satisfied_requirement_ids": edge.SatisfiedRequirementIDs,
			"created_at": record.CreatedAt, "expires_at": record.ExpiresAt, "schema_version": "selection-plan-edge.v1", "content_hash": hash,
		}); err != nil {
			return err
		}
	}
	for ordinal, gap := range record.Plan.Gaps {
		identity := struct {
			InjectionID string `json:"injection_id"`
			Ordinal     int    `json:"ordinal"`
			Gap         any    `json:"gap"`
		}{record.InjectionID, ordinal, gap}
		_, hash, err := canonical.MarshalAndHash(identity)
		if err != nil {
			return err
		}
		gapRecord := map[string]any{
			"tenant_id": string(record.TenantID), "injection_id": record.InjectionID, "ordinal": ordinal, "code": gap.Code,
			"created_at": record.CreatedAt, "expires_at": record.ExpiresAt, "schema_version": "selection-plan-gap.v1", "content_hash": hash,
		}
		if gap.VersionID != "" {
			gapRecord["procedure_version_id"] = gap.VersionID
		}
		if gap.RequirementID != "" {
			gapRecord["requirement_id"] = gap.RequirementID
		}
		if gap.GoalPredicateID != "" {
			gapRecord["goal_predicate_id"] = gap.GoalPredicateID
		}
		if err = createRecord(ctx, tx, models.NewRecordID("selection_plan_gap", fmt.Sprintf("%s_%d", record.InjectionID, ordinal)), gapRecord); err != nil {
			return err
		}
	}
	return nil
}

func findSelectionByIdempotency[T interface {
	*surrealdb.DB | *surrealdb.Transaction
}](ctx context.Context, sender T, tenantID domain.TenantID, identity string) (selection.Record, bool, error) {
	return findSelection(ctx, sender, `tenant_id = $tenant_id AND idempotency_identity_hash = $identity`, map[string]any{"tenant_id": string(tenantID), "identity": identity})
}

func findSelectionByRun[T interface {
	*surrealdb.DB | *surrealdb.Transaction
}](ctx context.Context, sender T, tenantID domain.TenantID, runID string) (selection.Record, bool, error) {
	return findSelection(ctx, sender, `tenant_id = $tenant_id AND retrieval_run_id = $identity`, map[string]any{"tenant_id": string(tenantID), "identity": runID})
}

func findSelection[T interface {
	*surrealdb.DB | *surrealdb.Transaction
}](ctx context.Context, sender T, predicate string, params map[string]any) (selection.Record, bool, error) {
	type row struct {
		Canonical   string `json:"canonical_selection"`
		ContentHash string `json:"content_hash"`
	}
	query := `SELECT canonical_selection, content_hash FROM selection_record WHERE ` + predicate + ` LIMIT 1`
	rows, err := surrealdb.Query[[]row](ctx, sender, query, params)
	if err != nil || rows == nil || len(*rows) == 0 || len((*rows)[0].Result) == 0 {
		return selection.Record{}, false, err
	}
	stored := (*rows)[0].Result[0]
	record, err := selection.DecodeRecord([]byte(stored.Canonical), stored.ContentHash)
	if err != nil {
		return selection.Record{}, false, selection.ErrInvalidSelection
	}
	return record, true, nil
}

func resolveSelection(existing selection.Record, request selection.CommitRequest) (selection.Receipt, error) {
	if existing.DraftHash != request.Draft.ContentHash {
		return selection.Receipt{}, selection.ErrIdempotencyConflict
	}
	return selection.Receipt{Record: existing, Disposition: selection.DispositionDuplicate}, nil
}
