package surreal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/projection"
	"github.com/sauhard74/mem-jev/internal/retrieval"
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
	verifiedCount, unsafeCount, err := versionEvidenceCounts(ctx, tx, versionID)
	if err != nil {
		return projection.PublishReceipt{}, err
	}
	document, err := retrieval.ReviseEvidence(value.RetrievalDocument, uint64(verifiedCount), uint64(unsafeCount), value.CreatedAt)
	if err != nil {
		return projection.PublishReceipt{}, err
	}
	epoch, err := publishRetrievalDocument(ctx, tx, value, document)
	if err != nil {
		return projection.PublishReceipt{}, err
	}
	receipt.ProjectionEpoch, receipt.RetrievalDocumentID = epoch, document.ID
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

func versionEvidenceCounts(ctx context.Context, tx *surrealdb.Transaction, id models.RecordID) (int, int, error) {
	type row struct {
		Verified int `json:"verified_success_count"`
		Unsafe   int `json:"unsafe_outcome_count"`
	}
	results, err := surrealdb.Query[[]row](ctx, tx, `SELECT verified_success_count, unsafe_outcome_count FROM $id LIMIT 1`, map[string]any{"id": id})
	if err != nil || results == nil || len(*results) == 0 || len((*results)[0].Result) == 0 {
		if err == nil {
			err = projection.ErrInvalidProjection
		}
		return 0, 0, err
	}
	rowValue := (*results)[0].Result[0]
	return rowValue.Verified, rowValue.Unsafe, nil
}

func publishRetrievalDocument(ctx context.Context, tx *surrealdb.Transaction, value projection.Projection, document retrieval.Document) (uint64, error) {
	type headRow struct {
		CurrentEpoch    uint64 `json:"current_epoch"`
		DocumentSetHash string `json:"document_set_hash"`
	}
	headID := models.NewRecordID("projection_epoch_head", "retrieval_"+string(value.TenantID))
	results, err := surrealdb.Query[[]headRow](ctx, tx, `SELECT current_epoch, document_set_hash FROM $id`, map[string]any{"id": headID})
	if err != nil {
		return 0, err
	}
	var current uint64
	previousHash := strings.Repeat("0", 64)
	if results != nil && len(*results) > 0 && len((*results)[0].Result) > 0 {
		current = (*results)[0].Result[0].CurrentEpoch
		previousHash = (*results)[0].Result[0].DocumentSetHash
	}
	epoch := current + 1
	_, documentSetHash, err := canonical.MarshalAndHash(struct {
		TenantID           domain.TenantID `json:"tenant_id"`
		Epoch              uint64          `json:"epoch"`
		PreviousHash       string          `json:"previous_hash"`
		ProcedureVersionID string          `json:"procedure_version_id"`
		DocumentHash       string          `json:"document_hash"`
	}{value.TenantID, epoch, previousHash, value.Version.ID, document.ContentHash})
	if err != nil {
		return 0, err
	}
	if current == 0 {
		if err := createRecord(ctx, tx, headID, map[string]any{
			"tenant_id": string(value.TenantID), "current_epoch": epoch, "document_set_hash": documentSetHash,
			"updated_at": value.CreatedAt, "schema_version": "projection-epoch-head.v1", "content_hash": documentSetHash,
		}); err != nil {
			return 0, err
		}
	} else {
		if _, err := surrealdb.Query[any](ctx, tx, `UPDATE $id SET current_epoch = $epoch, document_set_hash = $hash, updated_at = $at, content_hash = $hash`, map[string]any{"id": headID, "epoch": epoch, "hash": documentSetHash, "at": value.CreatedAt}); err != nil {
			return 0, err
		}
	}
	_, epochIDHash, err := canonical.MarshalAndHash(struct {
		TenantID domain.TenantID `json:"tenant_id"`
		Epoch    uint64          `json:"epoch"`
	}{value.TenantID, epoch})
	if err != nil {
		return 0, err
	}
	epochRecord := map[string]any{
		"tenant_id": string(value.TenantID), "epoch": epoch, "document_set_hash": documentSetHash,
		"created_at": value.CreatedAt, "schema_version": "projection-epoch.v1", "content_hash": documentSetHash,
	}
	if current > 0 {
		epochRecord["previous_epoch"] = current
	}
	if err := createRecord(ctx, tx, models.NewRecordID("projection_epoch", "pe_"+epochIDHash), epochRecord); err != nil {
		return 0, err
	}
	environmentJSON, _, err := canonical.MarshalAndHash(document.Environment)
	if err != nil {
		return 0, err
	}
	toolNames := make([]string, len(document.Tools))
	toolVersions := make([]string, len(document.Tools))
	for index, tool := range document.Tools {
		toolNames[index], toolVersions[index] = tool.Name, tool.ContractVersionID
	}
	resourceTypes := make([]string, len(document.Resources))
	resourceNamespaces := make([]string, len(document.Resources))
	resourceHashes := make([]string, len(document.Resources))
	resourceSchemas := make([]string, len(document.Resources))
	for index, resource := range document.Resources {
		resourceTypes[index], resourceNamespaces[index], resourceHashes[index] = resource.Type, resource.Namespace, resource.IdentityHash
		resourceSchemas[index] = resource.SchemaVersion
	}
	record := map[string]any{
		"tenant_id": string(value.TenantID), "retrieval_document_id": document.ID,
		"procedure_version_id": document.ProcedureVersionID, "procedure_id": document.ProcedureID, "projection_epoch": epoch,
		"task_text": document.TaskText, "intent_hash": document.IntentHash, "effect_signature_hash": document.EffectSignatureHash,
		"tool_names": toolNames, "tool_contract_version_ids": toolVersions, "resource_types": resourceTypes,
		"resource_namespaces": resourceNamespaces, "resource_identity_hashes": resourceHashes, "effects": document.Effects,
		"resource_schema_versions": resourceSchemas,
		"environment_scope_hash":   document.EnvironmentScopeHash, "harness_name": document.Harness.Name,
		"environment_facts": string(environmentJSON), "prefix_hashes": document.PrefixHashes,
		"lifecycle_state": document.Lifecycle, "observed_end_to_end": document.ObservedEndToEnd,
		"verification_strength": document.VerificationStrength, "verified_success_count": document.VerifiedSuccessCount,
		"unsafe_outcome_count": document.UnsafeOutcomeCount, "validated_at": value.CreatedAt,
		"validation_policy_version": document.ValidationPolicyVersion, "learned_with_recall_consent": document.LearnedWithRecallConsent,
		"residency_region": document.ResidencyRegion, "risk_class": document.RiskClass,
		"canonical_document": string(document.CanonicalJSON), "created_at": value.CreatedAt,
		"schema_version": document.SchemaVersion, "content_hash": document.ContentHash,
	}
	if document.Harness.Version != "" {
		record["harness_version"] = document.Harness.Version
	}
	documentID := models.NewRecordID("retrieval_document", document.ID)
	if err := createRecord(ctx, tx, documentID, record); err != nil {
		return 0, err
	}
	if err := createRetrievalRelations(ctx, tx, value, document, documentID); err != nil {
		return 0, err
	}
	return epoch, nil
}

func createRetrievalRelations(ctx context.Context, tx *surrealdb.Transaction, value projection.Projection, document retrieval.Document, documentID models.RecordID) error {
	type idRow struct {
		ID models.RecordID `json:"id"`
	}
	for _, tool := range document.Tools {
		rows, err := surrealdb.Query[[]idRow](ctx, tx, `SELECT id FROM tool_contract_version WHERE tenant_id = $tenant_id AND contract_version_id = $version LIMIT 1`, map[string]any{"tenant_id": string(value.TenantID), "version": tool.ContractVersionID})
		if err != nil || rows == nil || len(*rows) == 0 || len((*rows)[0].Result) == 0 {
			if err == nil {
				err = projection.ErrInvalidProjection
			}
			return err
		}
		if err := relate(ctx, tx, documentID, "retrieval_uses_tool", (*rows)[0].Result[0].ID, edgeRecord(value, document.ContentHash)); err != nil {
			return err
		}
	}
	for _, resource := range document.Resources {
		rows, err := surrealdb.Query[[]idRow](ctx, tx, `SELECT id FROM resource_ref WHERE tenant_id = $tenant_id AND namespace = $namespace AND resource_type = $type AND logical_identity_hash = $hash LIMIT 1`, map[string]any{"tenant_id": string(value.TenantID), "namespace": resource.Namespace, "type": resource.Type, "hash": resource.IdentityHash})
		if err != nil {
			return err
		}
		var resourceID models.RecordID
		if rows != nil && len(*rows) > 0 && len((*rows)[0].Result) > 0 {
			resourceID = (*rows)[0].Result[0].ID
		} else {
			_, hash, hashErr := canonical.MarshalAndHash(struct {
				TenantID domain.TenantID               `json:"tenant_id"`
				Resource retrieval.ResourceRequirement `json:"resource"`
			}{value.TenantID, resource})
			if hashErr != nil {
				return hashErr
			}
			resourceID = models.NewRecordID("resource_ref", "rr_"+hash)
			if err := createRecord(ctx, tx, resourceID, map[string]any{"tenant_id": string(value.TenantID), "resource_id": "rr_" + hash, "namespace": resource.Namespace, "resource_type": resource.Type, "logical_identity_hash": resource.IdentityHash, "created_at": value.CreatedAt, "schema_version": "resource-ref.v1", "content_hash": hash}); err != nil {
				return err
			}
		}
		if err := relate(ctx, tx, documentID, "retrieval_requires_resource", resourceID, edgeRecord(value, document.ContentHash)); err != nil {
			return err
		}
	}
	for _, effect := range document.Effects {
		_, hash, err := canonical.MarshalAndHash(struct {
			TenantID domain.TenantID `json:"tenant_id"`
			Effect   string          `json:"effect"`
		}{value.TenantID, effect})
		if err != nil {
			return err
		}
		effectID := models.NewRecordID("effect_ref", "eff_"+hash)
		if _, found, findErr := findContentHash(ctx, tx, "effect_ref", "effect_id", value.TenantID, "eff_"+hash); findErr != nil {
			return findErr
		} else if !found {
			if err := createRecord(ctx, tx, effectID, map[string]any{"tenant_id": string(value.TenantID), "effect_id": "eff_" + hash, "effect_name": effect, "risk_class": document.RiskClass, "created_at": value.CreatedAt, "schema_version": "effect-ref.v1", "content_hash": hash}); err != nil {
				return err
			}
		}
		if err := relate(ctx, tx, documentID, "retrieval_has_effect", effectID, edgeRecord(value, document.ContentHash)); err != nil {
			return err
		}
	}
	for index, prefixHash := range document.PrefixHashes {
		prefixID := models.NewRecordID("procedure_prefix", "pfx_"+prefixHash)
		if _, found, findErr := findContentHash(ctx, tx, "procedure_prefix", "prefix_hash", value.TenantID, prefixHash); findErr != nil {
			return findErr
		} else if !found {
			if err := createRecord(ctx, tx, prefixID, map[string]any{"tenant_id": string(value.TenantID), "prefix_hash": prefixHash, "length": index + 1, "created_at": value.CreatedAt, "schema_version": "procedure-prefix.v1", "content_hash": prefixHash}); err != nil {
				return err
			}
		}
		if err := relate(ctx, tx, documentID, "retrieval_has_prefix", prefixID, edgeRecord(value, document.ContentHash)); err != nil {
			return err
		}
	}
	return nil
}

func edgeRecord(value projection.Projection, hash string) map[string]any {
	return map[string]any{"tenant_id": string(value.TenantID), "created_at": value.CreatedAt, "schema_version": "retrieval-edge.v1", "content_hash": hash}
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
	tables := []string{"procedure", "procedure_version", "step", "depends_on", "negative_path", "synthesis_manifest", "supported_by", "retrieval_document", "projection_epoch"}
	values := make([]int, len(tables))
	for index, table := range tables {
		count, err := tableCount(ctx, r.db, table)
		if err != nil {
			return projection.Counts{}, err
		}
		values[index] = count
	}
	return projection.Counts{Families: values[0], Versions: values[1], Steps: values[2], Edges: values[3], NegativePaths: values[4], Manifests: values[5], EvidenceLinks: values[6], RetrievalDocuments: values[7], ProjectionEpochs: values[8]}, nil
}

func (r *ProjectionRepository) RetrievalDocument(ctx context.Context, tenantID domain.TenantID, versionID string, epoch uint64) (retrieval.Document, error) {
	if err := ctx.Err(); err != nil {
		return retrieval.Document{}, err
	}
	if r == nil || r.db == nil {
		return retrieval.Document{}, errors.New("SurrealDB projection repository is not configured")
	}
	type row struct {
		ID        string `json:"retrieval_document_id"`
		Hash      string `json:"content_hash"`
		Canonical string `json:"canonical_document"`
	}
	results, err := surrealdb.Query[[]row](ctx, r.db, `SELECT retrieval_document_id, content_hash, canonical_document FROM retrieval_document WHERE tenant_id = $tenant_id AND procedure_version_id = $version_id AND projection_epoch = $epoch LIMIT 1`, map[string]any{"tenant_id": string(tenantID), "version_id": versionID, "epoch": epoch})
	if err != nil {
		return retrieval.Document{}, databaseFailure("read retrieval document", err)
	}
	if results == nil || len(*results) == 0 || len((*results)[0].Result) == 0 {
		return retrieval.Document{}, projection.ErrProjectionNotFound
	}
	stored := (*results)[0].Result[0]
	var document retrieval.Document
	if err := json.Unmarshal([]byte(stored.Canonical), &document); err != nil {
		return retrieval.Document{}, databaseFailure("decode retrieval document", err)
	}
	document.ID, document.ContentHash, document.CanonicalJSON = stored.ID, stored.Hash, []byte(stored.Canonical)
	if err := retrieval.ValidateDocument(document); err != nil {
		return retrieval.Document{}, projection.ErrProjectionConflict
	}
	return document, nil
}

func (r *ProjectionRepository) RetrievalDocuments(ctx context.Context, tenantID domain.TenantID, versionIDs []string, epoch uint64) ([]retrieval.Document, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r == nil || r.db == nil || tenantID == "" || epoch == 0 || len(versionIDs) == 0 || len(versionIDs) > 1000 {
		return nil, projection.ErrProjectionNotFound
	}
	type row struct {
		ID        string `json:"retrieval_document_id"`
		VersionID string `json:"procedure_version_id"`
		Hash      string `json:"content_hash"`
		Canonical string `json:"canonical_document"`
		Epoch     uint64 `json:"projection_epoch"`
	}
	results, err := surrealdb.Query[[]row](ctx, r.db, `SELECT retrieval_document_id, procedure_version_id, content_hash, canonical_document, projection_epoch FROM retrieval_document WHERE tenant_id = $tenant_id AND procedure_version_id IN $version_ids AND projection_epoch <= $epoch ORDER BY procedure_version_id ASC, projection_epoch DESC`, map[string]any{"tenant_id": string(tenantID), "version_ids": versionIDs, "epoch": epoch})
	if err != nil {
		return nil, databaseFailure("read retrieval documents", err)
	}
	if results == nil || len(*results) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(versionIDs))
	documents := make([]retrieval.Document, 0, len(versionIDs))
	for _, stored := range (*results)[0].Result {
		if _, ok := seen[stored.VersionID]; ok {
			continue
		}
		var document retrieval.Document
		if err := json.Unmarshal([]byte(stored.Canonical), &document); err != nil {
			return nil, databaseFailure("decode retrieval document", err)
		}
		document.ID, document.ContentHash, document.CanonicalJSON = stored.ID, stored.Hash, []byte(stored.Canonical)
		if retrieval.ValidateDocument(document) != nil || document.ProcedureVersionID != stored.VersionID {
			return nil, projection.ErrProjectionConflict
		}
		seen[stored.VersionID] = struct{}{}
		documents = append(documents, document)
	}
	sort.Slice(documents, func(i, j int) bool { return documents[i].ProcedureVersionID < documents[j].ProcedureVersionID })
	return documents, nil
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
