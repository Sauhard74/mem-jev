package surreal

import (
	"context"
	"errors"
	"fmt"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/projection"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

type ProjectionRepository struct {
	db      *surrealdb.DB
	failure failurePoint
}

func NewProjectionRepository(db *surrealdb.DB) *ProjectionRepository {
	return newProjectionRepository(db, failureNone)
}

func newProjectionRepository(db *surrealdb.DB, failure failurePoint) *ProjectionRepository {
	return &ProjectionRepository{db: db, failure: failure}
}

func (r *ProjectionRepository) Publish(ctx context.Context, value projection.Projection) (projection.PublishReceipt, error) {
	if err := ctx.Err(); err != nil {
		return projection.PublishReceipt{}, err
	}
	if r == nil || r.db == nil {
		return projection.PublishReceipt{}, errors.New("SurrealDB projection repository is not configured")
	}
	if err := projection.Validate(value); err != nil {
		return projection.PublishReceipt{}, err
	}
	const attempts = 8
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		receipt, err := r.publishOnce(ctx, value)
		if err == nil {
			return receipt, nil
		}
		lastErr = err
		if existing, found, lookupErr := findManifest(ctx, r.db, value.TenantID, value.Manifest.ID); lookupErr != nil {
			return projection.PublishReceipt{}, databaseFailure("resolve failed projection publish", lookupErr)
		} else if found {
			if existing.ContentHash != value.Manifest.ContentHash {
				return projection.PublishReceipt{}, projection.ErrProjectionConflict
			}
			return projection.PublishReceipt{ManifestID: value.Manifest.ID, ProcedureID: value.Family.ID, ProcedureVersionID: value.Version.ID, Disposition: projection.DispositionDuplicate}, nil
		}
		if !surrealdb.IsTransactionConflict(err) {
			return projection.PublishReceipt{}, databaseFailure("publish projection", err)
		}
		if err := waitForRetry(ctx, attempt); err != nil {
			return projection.PublishReceipt{}, err
		}
	}
	return projection.PublishReceipt{}, fmt.Errorf("publish projection after conflict retries: %w", lastErr)
}

func (r *ProjectionRepository) publishOnce(ctx context.Context, value projection.Projection) (_ projection.PublishReceipt, err error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return projection.PublishReceipt{}, fmt.Errorf("begin projection transaction: %w", err)
	}
	defer func() {
		if !tx.IsClosed() {
			_ = tx.Cancel(context.Background())
		}
	}()
	if existing, found, findErr := findManifest(ctx, tx, value.TenantID, value.Manifest.ID); findErr != nil {
		return projection.PublishReceipt{}, findErr
	} else if found {
		if existing.ContentHash != value.Manifest.ContentHash {
			return projection.PublishReceipt{}, projection.ErrProjectionConflict
		}
		_ = tx.Cancel(ctx)
		return projection.PublishReceipt{ManifestID: value.Manifest.ID, ProcedureID: value.Family.ID, ProcedureVersionID: value.Version.ID, Disposition: projection.DispositionDuplicate}, nil
	}
	receipt := projection.PublishReceipt{ManifestID: value.Manifest.ID, ProcedureID: value.Family.ID, ProcedureVersionID: value.Version.ID, Disposition: projection.DispositionPublished}
	if value.Manifest.Status == "abstained" {
		if err := createManifest(ctx, tx, value); err != nil {
			return projection.PublishReceipt{}, err
		}
		if err := updateCheckpoint(ctx, tx, value); err != nil {
			return projection.PublishReceipt{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return projection.PublishReceipt{}, err
		}
		return receipt, nil
	}
	familyID := models.NewRecordID("procedure", value.Family.ID)
	if hash, found, findErr := findContentHash(ctx, tx, "procedure", "procedure_id", value.TenantID, value.Family.ID); findErr != nil {
		return projection.PublishReceipt{}, findErr
	} else if found {
		if hash != value.Family.ContentHash {
			return projection.PublishReceipt{}, projection.ErrProjectionConflict
		}
	} else {
		if err := createRecord(ctx, tx, familyID, map[string]any{
			"tenant_id": string(value.TenantID), "procedure_id": value.Family.ID, "intent_hash": value.Family.IntentHash,
			"effect_signature_hash": value.Family.EffectSignatureHash, "lifecycle_state": "candidate",
			"created_at": value.CreatedAt, "updated_at": value.CreatedAt, "schema_version": "procedure.v1", "content_hash": value.Family.ContentHash,
		}); err != nil {
			return projection.PublishReceipt{}, err
		}
		receipt.NewFamily = true
	}
	versionID := models.NewRecordID("procedure_version", value.Version.ID)
	versionNew := false
	if hash, found, findErr := findContentHash(ctx, tx, "procedure_version", "procedure_version_id", value.TenantID, value.Version.ID); findErr != nil {
		return projection.PublishReceipt{}, findErr
	} else if found {
		if hash != value.Version.ContentHash {
			return projection.PublishReceipt{}, projection.ErrProjectionConflict
		}
	} else {
		if err := createRecord(ctx, tx, versionID, map[string]any{
			"tenant_id": string(value.TenantID), "procedure_version_id": value.Version.ID, "procedure_id": value.Family.ID,
			"graph_hash": value.Version.GraphHash, "environment_scope_hash": value.Version.EnvironmentScopeHash,
			"policy_version": value.Version.PolicyVersion, "goal_predicates": value.GoalPredicates,
			"canonical_projection": string(value.CanonicalProjectionJSON), "lifecycle_state": "candidate",
			"observed_end_to_end": value.Version.ObservedEndToEnd, "opaque_step_count": 0,
			"verified_success_count": 0, "unsafe_outcome_count": 0, "last_evidence_at": value.CreatedAt,
			"created_at": value.CreatedAt, "schema_version": "procedure.v1", "content_hash": value.Version.ContentHash,
		}); err != nil {
			return projection.PublishReceipt{}, err
		}
		if err := createProjectionGraph(ctx, tx, value, versionID); err != nil {
			return projection.PublishReceipt{}, err
		}
		if r.failure == failureAfterEvents {
			return projection.PublishReceipt{}, errInjectedFailure
		}
		versionNew, receipt.NewVersion = true, true
	}
	if err := createNegativePaths(ctx, tx, value); err != nil {
		return projection.PublishReceipt{}, err
	}
	evidenceAdded, err := linkEvidence(ctx, tx, value, versionID)
	if err != nil {
		return projection.PublishReceipt{}, err
	}
	receipt.EvidenceAdded = evidenceAdded
	if evidenceAdded && !versionNew {
		if _, err := surrealdb.Query[any](ctx, tx, `UPDATE $id SET verified_success_count += 1, last_evidence_at = $at`, map[string]any{"id": versionID, "at": value.CreatedAt}); err != nil {
			return projection.PublishReceipt{}, err
		}
	} else if evidenceAdded {
		if _, err := surrealdb.Query[any](ctx, tx, `UPDATE $id SET verified_success_count = 1`, map[string]any{"id": versionID}); err != nil {
			return projection.PublishReceipt{}, err
		}
	}
	if err := createManifest(ctx, tx, value); err != nil {
		return projection.PublishReceipt{}, err
	}
	if err := updateCheckpoint(ctx, tx, value); err != nil {
		return projection.PublishReceipt{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return projection.PublishReceipt{}, err
	}
	return receipt, nil
}

type manifestRow struct {
	ContentHash string `json:"content_hash"`
}

func findManifest[S interface {
	*surrealdb.DB | *surrealdb.Transaction
}](ctx context.Context, sender S, tenantID domain.TenantID, id string) (manifestRow, bool, error) {
	results, err := surrealdb.Query[[]manifestRow](ctx, sender, `SELECT content_hash FROM synthesis_manifest WHERE tenant_id = $tenant_id AND synthesis_manifest_id = $id LIMIT 1`, map[string]any{"tenant_id": string(tenantID), "id": id})
	if err != nil || results == nil || len(*results) == 0 || len((*results)[0].Result) == 0 {
		return manifestRow{}, false, err
	}
	return (*results)[0].Result[0], true, nil
}

func findContentHash(ctx context.Context, tx *surrealdb.Transaction, table, idField string, tenantID domain.TenantID, id string) (string, bool, error) {
	type row struct {
		ContentHash string `json:"content_hash"`
	}
	statement := fmt.Sprintf("SELECT content_hash FROM %s WHERE tenant_id = $tenant_id AND %s = $id LIMIT 1", table, idField)
	results, err := surrealdb.Query[[]row](ctx, tx, statement, map[string]any{"tenant_id": string(tenantID), "id": id})
	if err != nil || results == nil || len(*results) == 0 || len((*results)[0].Result) == 0 {
		return "", false, err
	}
	return (*results)[0].Result[0].ContentHash, true, nil
}

func createRecord(ctx context.Context, tx *surrealdb.Transaction, id models.RecordID, record map[string]any) error {
	_, err := surrealdb.Query[any](ctx, tx, `CREATE ONLY $id CONTENT $record`, map[string]any{"id": id, "record": record})
	return err
}

func createProjectionGraph(ctx context.Context, tx *surrealdb.Transaction, value projection.Projection, versionID models.RecordID) error {
	stepIDs := make([]models.RecordID, len(value.Steps))
	for index, step := range value.Steps {
		stepID := models.NewRecordID("step", step.ID)
		stepIDs[index] = stepID
		if err := createRecord(ctx, tx, stepID, map[string]any{
			"tenant_id": string(value.TenantID), "step_id": step.ID, "procedure_version_id": value.Version.ID,
			"ordinal": step.Ordinal, "event_id": "projection:" + step.ID, "tool_contract_version_id": step.ToolContractVersionID,
			"opaque": false, "uncertain_necessity": step.UncertainNecessity,
			"payload":    map[string]any{"tool_name": step.ToolName, "tool_version": step.ToolVersion, "compensation_boundary": step.CompensationBoundary},
			"created_at": value.CreatedAt, "schema_version": "step.v1", "content_hash": step.ContentHash,
		}); err != nil {
			return err
		}
		if err := relate(ctx, tx, versionID, "contains", stepID, map[string]any{
			"tenant_id": string(value.TenantID), "created_at": value.CreatedAt, "schema_version": "edge.v1", "content_hash": step.ContentHash,
		}); err != nil {
			return err
		}
	}
	for _, edge := range value.Edges {
		record := map[string]any{
			"tenant_id": string(value.TenantID), "dependency_type": string(edge.Type), "created_at": value.CreatedAt,
			"schema_version": "edge.v1", "content_hash": edge.ContentHash,
		}
		if edge.ResourceName != "" {
			record["resource_name"] = edge.ResourceName
		}
		if edge.ResourceType != "" {
			record["resource_type"] = edge.ResourceType
		}
		if edge.ResourceNamespace != "" {
			record["resource_namespace"] = edge.ResourceNamespace
		}
		// Causal edges point prerequisite -> dependent in the synthesis model.
		// The graph relation reads dependent -> depends_on -> prerequisite.
		if err := relate(ctx, tx, stepIDs[edge.ToOrdinal], "depends_on", stepIDs[edge.FromOrdinal], record); err != nil {
			return err
		}
	}
	return nil
}

func relate(ctx context.Context, tx *surrealdb.Transaction, in models.RecordID, table string, out models.RecordID, record map[string]any) error {
	statement := fmt.Sprintf("RELATE $in->%s->$out CONTENT $record", table)
	_, err := surrealdb.Query[any](ctx, tx, statement, map[string]any{"in": in, "out": out, "record": record})
	return err
}

func createNegativePaths(ctx context.Context, tx *surrealdb.Transaction, value projection.Projection) error {
	for _, path := range value.NegativePaths {
		identity := struct {
			ProcedureVersionID string `json:"procedure_version_id"`
			PathID             string `json:"path_id"`
		}{value.Version.ID, path.ID}
		_, recordHash, err := canonical.MarshalAndHash(identity)
		if err != nil {
			return err
		}
		recordID := "negpv_" + recordHash
		if hash, found, findErr := findContentHash(ctx, tx, "negative_path", "negative_path_id", value.TenantID, recordID); findErr != nil {
			return findErr
		} else if found {
			if hash != recordHash {
				return projection.ErrProjectionConflict
			}
			continue
		}
		id := models.NewRecordID("negative_path", recordID)
		events := make([]string, len(path.EventIDs))
		for index, eventID := range path.EventIDs {
			events[index] = string(eventID)
		}
		if err := createRecord(ctx, tx, id, map[string]any{
			"tenant_id": string(value.TenantID), "negative_path_id": recordID, "procedure_version_id": value.Version.ID,
			"failure_predicate_id": path.FailurePredicateID, "environment_scope_hash": path.Scope.EnvironmentHash,
			"tool_scope_hash": path.Scope.ToolHash, "resource_scope_hash": path.Scope.ResourceHash, "event_ids": events,
			"created_at": value.CreatedAt, "schema_version": "negative-path.v1", "content_hash": recordHash,
		}); err != nil {
			return err
		}
	}
	return nil
}

func linkEvidence(ctx context.Context, tx *surrealdb.Transaction, value projection.Projection, versionID models.RecordID) (bool, error) {
	type row struct {
		ID models.RecordID `json:"id"`
	}
	results, err := surrealdb.Query[[]row](ctx, tx, `SELECT id FROM outcome_evidence WHERE tenant_id = $tenant_id AND outcome_id = $outcome_id LIMIT 1`, map[string]any{"tenant_id": string(value.TenantID), "outcome_id": string(value.Manifest.OutcomeID)})
	if err != nil {
		return false, err
	}
	if results == nil || len(*results) == 0 || len((*results)[0].Result) == 0 {
		return false, projection.ErrInvalidProjection
	}
	outcomeID := (*results)[0].Result[0].ID
	type countRow struct {
		Count int `json:"count"`
	}
	existing, err := surrealdb.Query[[]countRow](ctx, tx, `SELECT count() AS count FROM supported_by WHERE tenant_id = $tenant_id AND in = $in AND out = $out GROUP ALL`, map[string]any{"tenant_id": string(value.TenantID), "in": versionID, "out": outcomeID})
	if err != nil {
		return false, err
	}
	if existing != nil && len(*existing) > 0 && len((*existing)[0].Result) > 0 && (*existing)[0].Result[0].Count > 0 {
		return false, nil
	}
	hash := value.Version.ContentHash
	if err := relate(ctx, tx, versionID, "supported_by", outcomeID, map[string]any{"tenant_id": string(value.TenantID), "created_at": value.CreatedAt, "schema_version": "edge.v1", "content_hash": hash}); err != nil {
		return false, err
	}
	return true, nil
}

func createManifest(ctx context.Context, tx *surrealdb.Transaction, value projection.Projection) error {
	record := map[string]any{
		"tenant_id": string(value.TenantID), "synthesis_manifest_id": value.Manifest.ID, "trace_id": string(value.Manifest.TraceID),
		"outcome_id": string(value.Manifest.OutcomeID), "archive_hash": value.Manifest.ArchiveHash,
		"canonical_event_start": value.Manifest.CanonicalEventStart, "canonical_event_end": value.Manifest.CanonicalEventEnd,
		"sanitizer_version": value.Manifest.Versions.Sanitizer, "registry_version": value.Manifest.Versions.Registry,
		"policy_version": value.Manifest.Versions.Policy, "graph_builder_version": value.Manifest.Versions.GraphBuilder,
		"synthesizer_version": value.Manifest.Versions.Synthesizer, "synthesis_hash": value.Manifest.SynthesisHash,
		"status": string(value.Manifest.Status), "created_at": value.CreatedAt, "schema_version": "synthesis-manifest.v1", "content_hash": value.Manifest.ContentHash,
	}
	if value.Manifest.ProcedureVersionID != "" {
		record["procedure_version_id"] = value.Manifest.ProcedureVersionID
	}
	if value.Manifest.AbstentionCode != "" {
		record["abstention_code"] = value.Manifest.AbstentionCode
	}
	return createRecord(ctx, tx, models.NewRecordID("synthesis_manifest", value.Manifest.ID), record)
}

func updateCheckpoint(ctx context.Context, tx *surrealdb.Transaction, value projection.Projection) error {
	id := models.NewRecordID("projection_checkpoint", "evidence_"+string(value.TenantID))
	if _, found, err := findCheckpoint(ctx, tx, id); err != nil {
		return err
	} else if !found {
		return createRecord(ctx, tx, id, map[string]any{
			"tenant_id": string(value.TenantID), "projection_name": "evidence", "generation": 1,
			"input_cursor": value.Manifest.ID, "manifest_hash": value.Manifest.ContentHash, "updated_at": value.CreatedAt,
			"schema_version": "projection-checkpoint.v1", "content_hash": value.Manifest.ContentHash,
		})
	}
	_, err := surrealdb.Query[any](ctx, tx, `UPDATE $id SET
		generation += 1,
		input_cursor = $input_cursor,
		manifest_hash = $manifest_hash,
		updated_at = $updated_at,
		content_hash = $content_hash`, map[string]any{
		"id": id, "input_cursor": value.Manifest.ID, "manifest_hash": value.Manifest.ContentHash,
		"updated_at": value.CreatedAt, "content_hash": value.Manifest.ContentHash,
	})
	return err
}

func findCheckpoint(ctx context.Context, tx *surrealdb.Transaction, id models.RecordID) (int, bool, error) {
	type row struct {
		Generation int `json:"generation"`
	}
	results, err := surrealdb.Query[[]row](ctx, tx, `SELECT generation FROM $id LIMIT 1`, map[string]any{"id": id})
	if err != nil || results == nil || len(*results) == 0 || len((*results)[0].Result) == 0 {
		return 0, false, err
	}
	return (*results)[0].Result[0].Generation, true, nil
}

func (r *ProjectionRepository) Counts(ctx context.Context) (projection.Counts, error) {
	if r == nil || r.db == nil {
		return projection.Counts{}, errors.New("SurrealDB projection repository is not configured")
	}
	tables := []string{"procedure", "procedure_version", "step", "depends_on", "negative_path", "synthesis_manifest", "supported_by"}
	values := make([]int, len(tables))
	for index, table := range tables {
		count, err := tableCount(ctx, r.db, table)
		if err != nil {
			return projection.Counts{}, err
		}
		values[index] = count
	}
	return projection.Counts{Families: values[0], Versions: values[1], Steps: values[2], Edges: values[3], NegativePaths: values[4], Manifests: values[5], EvidenceLinks: values[6]}, nil
}

func (r *ProjectionRepository) Canonical(ctx context.Context, tenantID domain.TenantID, versionID string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r == nil || r.db == nil {
		return nil, errors.New("SurrealDB projection repository is not configured")
	}
	type row struct {
		CanonicalProjection string `json:"canonical_projection"`
	}
	results, err := surrealdb.Query[[]row](ctx, r.db, `SELECT canonical_projection FROM procedure_version
		WHERE tenant_id = $tenant_id AND procedure_version_id = $version_id LIMIT 1`, map[string]any{
		"tenant_id": string(tenantID), "version_id": versionID,
	})
	if err != nil {
		return nil, databaseFailure("read canonical projection", err)
	}
	if results == nil || len(*results) == 0 || len((*results)[0].Result) == 0 {
		return nil, projection.ErrProjectionNotFound
	}
	return []byte((*results)[0].Result[0].CanonicalProjection), nil
}
