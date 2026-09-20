package surreal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/sauhard74/mem-jev/internal/erasure"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

type ErasureRepository struct{ db *surrealdb.DB }

func NewErasureRepository(db *surrealdb.DB) *ErasureRepository { return &ErasureRepository{db: db} }

var tenantErasureTables = []string{
	"retrieval_uses_tool", "retrieval_requires_resource", "retrieval_has_effect", "retrieval_has_prefix",
	"contains", "depends_on", "consumes", "produces", "verified_by", "failed_under", "supersedes", "derived_from", "supported_by",
	"selection_plan_gap", "selection_plan_edge", "selection_plan_node", "retrieval_semantic_judgment", "retrieval_ranked_candidate", "retrieval_gate_decision", "retrieval_channel_hit", "retrieval_channel_result",
	"outcome_credit", "experiment_assignment", "experiment_exposure_counter", "experiment_tenant_budget_head", "experiment_global_budget_head", "promotion_evaluation", "experiment_manifest",
	"lifecycle_champion_head", "lifecycle_version_head", "lifecycle_decision", "lifecycle_policy_manifest", "planner_serving_head", "planner_manifest", "procedure_compatibility_edge",
	"maintenance_job", "derived_activation_permit", "derived_projection_snapshot", "compatibility_graph_projection", "pipeline_stage_artifact",
	"jev_judgment_head", "jev_judgment", "selection_record", "retrieval_run", "retrieval_serving_head", "retrieval_serving_config", "retrieval_index_manifest",
	"embedding_value", "embedding_index_generation", "embedding_manifest", "retrieval_document", "projection_epoch_head", "projection_epoch",
	"procedure_prefix", "effect_ref", "ranker_manifest", "eligibility_policy_manifest",
	"negative_path", "step", "procedure_version", "procedure", "resource_ref", "tool_contract_version", "tool_contract", "verification_result", "outcome_evidence", "outcome_receipt",
	"synthesis_manifest", "projection_checkpoint", "audit_event", "outbox_job", "archive_object", "ingest_receipt", "canonical_event", "trace_run", "policy_bundle", "consent_policy", "tenant",
}

func (repository *ErasureRepository) CommitErasure(ctx context.Context, tenantID string, receipt erasure.Receipt) (_ erasure.Receipt, err error) {
	if err := ctx.Err(); err != nil {
		return erasure.Receipt{}, err
	}
	if repository == nil || repository.db == nil || strings.TrimSpace(tenantID) == "" || erasure.ValidateReceipt(receipt) != nil {
		return erasure.Receipt{}, erasure.ErrInvalidRequest
	}
	if existing, found, lookupErr := readErasureReceipt(ctx, repository.db, receipt.RequestID); lookupErr != nil {
		return erasure.Receipt{}, databaseFailure("read erasure receipt", lookupErr)
	} else if found {
		if existing.ContentHash != receipt.ContentHash {
			return erasure.Receipt{}, erasure.ErrConflict
		}
		return existing, nil
	}
	tx, err := repository.db.Begin(ctx)
	if err != nil {
		return erasure.Receipt{}, databaseFailure("begin tenant erasure", err)
	}
	defer func() {
		if !tx.IsClosed() {
			_ = tx.Cancel(context.Background())
		}
	}()
	for _, table := range tenantErasureTables {
		statement := "DELETE " + table + " WHERE tenant_id = $tenant_id"
		if _, err = surrealdb.Query[any](ctx, tx, statement, map[string]any{"tenant_id": tenantID}); err != nil {
			return erasure.Receipt{}, databaseFailure("delete tenant table "+table, err)
		}
	}
	row := map[string]any{
		"request_id": receipt.RequestID, "tenant_hash": receipt.TenantHash, "archive_objects_deleted": receipt.ArchiveObjectsDeleted,
		"completed_at": receipt.CompletedAt, "canonical_receipt": string(receipt.CanonicalJSON), "schema_version": receipt.SchemaVersion, "content_hash": receipt.ContentHash,
	}
	if err = createRecord(ctx, tx, models.NewRecordID("tenant_erasure_receipt", receipt.RequestID), row); err != nil {
		return erasure.Receipt{}, databaseFailure("create erasure receipt", err)
	}
	if err = tx.Commit(ctx); err != nil {
		if existing, found, lookupErr := readErasureReceipt(ctx, repository.db, receipt.RequestID); lookupErr == nil && found && existing.ContentHash == receipt.ContentHash {
			return existing, nil
		}
		return erasure.Receipt{}, databaseFailure("commit tenant erasure", err)
	}
	return receipt, nil
}

func readErasureReceipt(ctx context.Context, db *surrealdb.DB, requestID string) (erasure.Receipt, bool, error) {
	type row struct {
		Canonical string `json:"canonical_receipt"`
		Hash      string `json:"content_hash"`
	}
	results, err := surrealdb.Query[[]row](ctx, db, `SELECT canonical_receipt, content_hash FROM tenant_erasure_receipt WHERE request_id = $request_id LIMIT 1`, map[string]any{"request_id": requestID})
	if err != nil || results == nil || len(*results) == 0 || len((*results)[0].Result) == 0 {
		return erasure.Receipt{}, false, err
	}
	stored := (*results)[0].Result[0]
	var receipt erasure.Receipt
	if err = json.Unmarshal([]byte(stored.Canonical), &receipt); err != nil {
		return erasure.Receipt{}, false, fmt.Errorf("decode erasure receipt: %w", err)
	}
	receipt.ContentHash, receipt.CanonicalJSON = stored.Hash, []byte(stored.Canonical)
	if err = erasure.ValidateReceipt(receipt); err != nil {
		return erasure.Receipt{}, false, errors.New("invalid stored erasure receipt")
	}
	return receipt, true, nil
}

var _ erasure.Repository = (*ErasureRepository)(nil)
