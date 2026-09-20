package surreal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/lifecycle"
	"github.com/sauhard74/mem-jev/internal/projection"
	"github.com/sauhard74/mem-jev/internal/retrieval"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

type LifecycleRepository struct{ db *surrealdb.DB }

func NewLifecycleRepository(db *surrealdb.DB) *LifecycleRepository {
	return &LifecycleRepository{db: db}
}

func (r *LifecycleRepository) Commit(ctx context.Context, value lifecycle.Publication) (lifecycle.Receipt, error) {
	if err := ctx.Err(); err != nil {
		return lifecycle.Receipt{}, err
	}
	if r == nil || r.db == nil {
		return lifecycle.Receipt{}, errors.New("SurrealDB lifecycle repository is not configured")
	}
	if lifecycle.ValidatePublication(value) != nil {
		return lifecycle.Receipt{}, lifecycle.ErrInvalidLifecycle
	}
	const attempts = 8
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		receipt, err := r.commitOnce(ctx, value)
		if err == nil {
			return receipt, nil
		}
		lastErr = err
		if existing, found, lookupErr := findLifecycleReceipt(ctx, r.db, value.Decision.TenantID, value.Decision.ID); lookupErr != nil {
			return lifecycle.Receipt{}, databaseFailure("resolve lifecycle publication", lookupErr)
		} else if found {
			existing.Disposition = lifecycle.DispositionDuplicate
			return existing, nil
		}
		if errors.Is(err, lifecycle.ErrChampionConflict) || errors.Is(err, lifecycle.ErrLifecycleConflict) {
			return lifecycle.Receipt{}, err
		}
		if !surrealdb.IsTransactionConflict(err) {
			return lifecycle.Receipt{}, databaseFailure("commit lifecycle publication", err)
		}
		if err := waitForRetry(ctx, attempt); err != nil {
			return lifecycle.Receipt{}, err
		}
	}
	return lifecycle.Receipt{}, databaseFailure("commit lifecycle publication after conflict retries", lastErr)
}

func (r *LifecycleRepository) commitOnce(ctx context.Context, value lifecycle.Publication) (_ lifecycle.Receipt, err error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return lifecycle.Receipt{}, err
	}
	defer func() {
		if !tx.IsClosed() {
			_ = tx.Cancel(context.Background())
		}
	}()
	if existing, found, err := findLifecycleReceipt(ctx, tx, value.Decision.TenantID, value.Decision.ID); err != nil {
		return lifecycle.Receipt{}, err
	} else if found {
		_ = tx.Cancel(ctx)
		existing.Disposition = lifecycle.DispositionDuplicate
		return existing, nil
	}
	if err := ensureLifecycleManifest(ctx, tx, value); err != nil {
		return lifecycle.Receipt{}, err
	}
	stored, latestEpoch, err := currentLifecycleDocument(ctx, tx, value.Decision.TenantID, value.Decision.ProcedureVersionID)
	if err != nil || latestEpoch != value.SourceEpoch || stored.ContentHash != value.SourceDocument.ContentHash {
		return lifecycle.Receipt{}, lifecycle.ErrLifecycleConflict
	}
	head, found, err := findVersionHead(ctx, tx, value.Decision.TenantID, value.Decision.ProcedureVersionID, value.Manifest.ID)
	if err != nil {
		return lifecycle.Receipt{}, err
	}
	if found && (head.State != value.Decision.PriorState || head.ProjectionEpoch != value.SourceEpoch) {
		return lifecycle.Receipt{}, lifecycle.ErrLifecycleConflict
	}
	champion, championFound, err := findChampion(ctx, tx, value.Decision.TenantID, value.Decision.ProcedureID, value.Manifest.ID)
	if err != nil {
		return lifecycle.Receipt{}, err
	}
	if value.Decision.NextState == lifecycle.Active && championFound && champion.ProcedureVersionID != value.Decision.ProcedureVersionID {
		return lifecycle.Receipt{}, lifecycle.ErrChampionConflict
	}
	receipt := lifecycle.Receipt{DecisionID: value.Decision.ID, RetrievalDocumentID: stored.ID, ProjectionEpoch: value.SourceEpoch, Disposition: lifecycle.DispositionPublished}
	if value.Decision.NextState != value.Decision.PriorState {
		document, err := retrieval.ReviseLifecycle(stored, string(value.Decision.NextState), value.Decision.CausalSuccessCount, value.Decision.UnsafeOutcomeCount, value.Decision.EvaluatedAt, value.Manifest.ID)
		if err != nil {
			return lifecycle.Receipt{}, err
		}
		epoch, err := publishRetrievalDocument(ctx, tx, projection.Projection{TenantID: value.Decision.TenantID, Version: projection.Version{ID: value.Decision.ProcedureVersionID}, CreatedAt: value.Decision.EvaluatedAt}, document)
		if err != nil {
			return lifecycle.Receipt{}, err
		}
		receipt.RetrievalDocumentID, receipt.ProjectionEpoch = document.ID, epoch
	}
	if err := createLifecycleDecision(ctx, tx, value.Decision, receipt); err != nil {
		return lifecycle.Receipt{}, err
	}
	if err := upsertVersionHead(ctx, tx, value.Decision, receipt); err != nil {
		return lifecycle.Receipt{}, err
	}
	championID := lifecycleHeadID("lifecycle_champion_head", value.Decision.TenantID, value.Decision.ProcedureID, value.Manifest.ID)
	if value.Decision.NextState == lifecycle.Active {
		if err := upsertChampion(ctx, tx, championID, value.Decision, receipt); err != nil {
			return lifecycle.Receipt{}, err
		}
	} else if value.Decision.PriorState == lifecycle.Active && championFound && champion.ProcedureVersionID == value.Decision.ProcedureVersionID {
		if _, err := surrealdb.Query[any](ctx, tx, `DELETE $id`, map[string]any{"id": championID}); err != nil {
			return lifecycle.Receipt{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return lifecycle.Receipt{}, err
	}
	return receipt, nil
}

func (r *LifecycleRepository) VersionHead(ctx context.Context, tenantID domain.TenantID, versionID, policyID string) (lifecycle.VersionHead, error) {
	if err := ctx.Err(); err != nil {
		return lifecycle.VersionHead{}, err
	}
	if r == nil || r.db == nil {
		return lifecycle.VersionHead{}, lifecycle.ErrLifecycleNotFound
	}
	value, found, err := findVersionHead(ctx, r.db, tenantID, versionID, policyID)
	if err != nil {
		return lifecycle.VersionHead{}, databaseFailure("read lifecycle version head", err)
	}
	if !found {
		return lifecycle.VersionHead{}, lifecycle.ErrLifecycleNotFound
	}
	return value, nil
}

func (r *LifecycleRepository) Champion(ctx context.Context, tenantID domain.TenantID, procedureID, policyID string) (lifecycle.ChampionHead, error) {
	if err := ctx.Err(); err != nil {
		return lifecycle.ChampionHead{}, err
	}
	if r == nil || r.db == nil {
		return lifecycle.ChampionHead{}, lifecycle.ErrLifecycleNotFound
	}
	value, found, err := findChampion(ctx, r.db, tenantID, procedureID, policyID)
	if err != nil {
		return lifecycle.ChampionHead{}, databaseFailure("read lifecycle champion", err)
	}
	if !found {
		return lifecycle.ChampionHead{}, lifecycle.ErrLifecycleNotFound
	}
	return value, nil
}

func ensureLifecycleManifest(ctx context.Context, tx *surrealdb.Transaction, value lifecycle.Publication) error {
	type row struct {
		Hash string `json:"content_hash"`
	}
	rows, err := surrealdb.Query[[]row](ctx, tx, `SELECT content_hash FROM lifecycle_policy_manifest WHERE tenant_id = $tenant_id AND lifecycle_policy_manifest_id = $manifest_id LIMIT 1`, map[string]any{"tenant_id": string(value.Decision.TenantID), "manifest_id": value.Manifest.ID})
	if err != nil {
		return err
	}
	if rows != nil && len(*rows) > 0 && len((*rows)[0].Result) > 0 {
		if (*rows)[0].Result[0].Hash != value.Manifest.ContentHash {
			return lifecycle.ErrLifecycleConflict
		}
		return nil
	}
	record := map[string]any{"tenant_id": string(value.Decision.TenantID), "lifecycle_policy_manifest_id": value.Manifest.ID, "version": value.Manifest.Version, "thresholds": value.Manifest.PolicySpec, "canonical_manifest": string(value.Manifest.CanonicalJSON), "created_at": value.Decision.EvaluatedAt, "schema_version": value.Manifest.SchemaVersion, "content_hash": value.Manifest.ContentHash}
	return createRecord(ctx, tx, lifecycleHeadID("lifecycle_policy_manifest", value.Decision.TenantID, value.Manifest.ID), record)
}

func currentLifecycleDocument(ctx context.Context, tx *surrealdb.Transaction, tenantID domain.TenantID, versionID string) (retrieval.Document, uint64, error) {
	type row struct {
		ID        string `json:"retrieval_document_id"`
		Hash      string `json:"content_hash"`
		Canonical string `json:"canonical_document"`
		Epoch     uint64 `json:"projection_epoch"`
	}
	rows, err := surrealdb.Query[[]row](ctx, tx, `SELECT retrieval_document_id, content_hash, canonical_document, projection_epoch FROM retrieval_document WHERE tenant_id = $tenant_id AND procedure_version_id = $version_id ORDER BY projection_epoch DESC LIMIT 1`, map[string]any{"tenant_id": string(tenantID), "version_id": versionID})
	if err != nil || rows == nil || len(*rows) == 0 || len((*rows)[0].Result) == 0 {
		if err == nil {
			err = lifecycle.ErrLifecycleConflict
		}
		return retrieval.Document{}, 0, err
	}
	stored := (*rows)[0].Result[0]
	var document retrieval.Document
	if err := json.Unmarshal([]byte(stored.Canonical), &document); err != nil {
		return retrieval.Document{}, 0, err
	}
	document.ID, document.ContentHash, document.CanonicalJSON = stored.ID, stored.Hash, []byte(stored.Canonical)
	if retrieval.ValidateDocument(document) != nil {
		return retrieval.Document{}, 0, lifecycle.ErrLifecycleConflict
	}
	return document, stored.Epoch, nil
}

func createLifecycleDecision(ctx context.Context, tx *surrealdb.Transaction, value lifecycle.Decision, receipt lifecycle.Receipt) error {
	record := map[string]any{"tenant_id": string(value.TenantID), "lifecycle_decision_id": value.ID, "procedure_id": value.ProcedureID, "procedure_version_id": value.ProcedureVersionID, "lifecycle_policy_manifest_id": value.PolicyManifestID, "prior_state": string(value.PriorState), "next_state": string(value.NextState), "causal_success_count": value.CausalSuccessCount, "causal_failure_count": value.CausalFailureCount, "associated_success_count": value.AssociatedSuccessCount, "associated_failure_count": value.AssociatedFailureCount, "unsafe_outcome_count": value.UnsafeOutcomeCount, "wilson_lower_bound_ppm": value.WilsonLowerBoundPPM, "reason_codes": value.ReasonCodes, "evidence_cutoff_at": value.EvidenceCutoffAt, "canonical_decision": string(value.CanonicalJSON), "created_at": value.EvaluatedAt, "schema_version": value.SchemaVersion, "content_hash": value.ContentHash, "retrieval_document_id": receipt.RetrievalDocumentID, "projection_epoch": receipt.ProjectionEpoch}
	return createRecord(ctx, tx, models.NewRecordID("lifecycle_decision", value.ID), record)
}

func upsertVersionHead(ctx context.Context, tx *surrealdb.Transaction, decision lifecycle.Decision, receipt lifecycle.Receipt) error {
	id := lifecycleHeadID("lifecycle_version_head", decision.TenantID, decision.ProcedureVersionID, decision.PolicyManifestID)
	record := map[string]any{"tenant_id": string(decision.TenantID), "procedure_id": decision.ProcedureID, "procedure_version_id": decision.ProcedureVersionID, "lifecycle_policy_manifest_id": decision.PolicyManifestID, "lifecycle_state": string(decision.NextState), "lifecycle_decision_id": decision.ID, "projection_epoch": receipt.ProjectionEpoch, "updated_at": decision.EvaluatedAt, "schema_version": "lifecycle-version-head.v1"}
	_, hash, err := canonical.MarshalAndHash(record)
	if err != nil {
		return err
	}
	record["content_hash"] = hash
	if _, err := surrealdb.Query[any](ctx, tx, `UPSERT $id CONTENT $record`, map[string]any{"id": id, "record": record}); err != nil {
		return err
	}
	return nil
}

func upsertChampion(ctx context.Context, tx *surrealdb.Transaction, id models.RecordID, decision lifecycle.Decision, receipt lifecycle.Receipt) error {
	record := map[string]any{"tenant_id": string(decision.TenantID), "procedure_id": decision.ProcedureID, "lifecycle_policy_manifest_id": decision.PolicyManifestID, "procedure_version_id": decision.ProcedureVersionID, "lifecycle_decision_id": decision.ID, "projection_epoch": receipt.ProjectionEpoch, "updated_at": decision.EvaluatedAt, "schema_version": "lifecycle-champion-head.v1"}
	_, hash, err := canonical.MarshalAndHash(record)
	if err != nil {
		return err
	}
	record["content_hash"] = hash
	_, err = surrealdb.Query[any](ctx, tx, `UPSERT $id CONTENT $record`, map[string]any{"id": id, "record": record})
	return err
}

type lifecycleReceiptRow struct {
	DecisionID string `json:"lifecycle_decision_id"`
	DocumentID string `json:"retrieval_document_id"`
	Epoch      uint64 `json:"projection_epoch"`
}

func findLifecycleReceipt[S interface {
	*surrealdb.DB | *surrealdb.Transaction
}](ctx context.Context, sender S, tenantID domain.TenantID, decisionID string) (lifecycle.Receipt, bool, error) {
	rows, err := surrealdb.Query[[]lifecycleReceiptRow](ctx, sender, `SELECT lifecycle_decision_id, retrieval_document_id, projection_epoch FROM lifecycle_decision WHERE tenant_id = $tenant_id AND lifecycle_decision_id = $decision_id LIMIT 1`, map[string]any{"tenant_id": string(tenantID), "decision_id": decisionID})
	if err != nil || rows == nil || len(*rows) == 0 || len((*rows)[0].Result) == 0 {
		return lifecycle.Receipt{}, false, err
	}
	row := (*rows)[0].Result[0]
	return lifecycle.Receipt{DecisionID: row.DecisionID, RetrievalDocumentID: row.DocumentID, ProjectionEpoch: row.Epoch, Disposition: lifecycle.DispositionPublished}, true, nil
}

type lifecycleVersionHeadRow struct {
	TenantID    string                `json:"tenant_id"`
	ProcedureID string                `json:"procedure_id"`
	VersionID   string                `json:"procedure_version_id"`
	PolicyID    string                `json:"lifecycle_policy_manifest_id"`
	State       lifecycle.State       `json:"lifecycle_state"`
	DecisionID  string                `json:"lifecycle_decision_id"`
	Epoch       uint64                `json:"projection_epoch"`
	UpdatedAt   models.CustomDateTime `json:"updated_at"`
}

func findVersionHead[S interface {
	*surrealdb.DB | *surrealdb.Transaction
}](ctx context.Context, sender S, tenantID domain.TenantID, versionID, policyID string) (lifecycle.VersionHead, bool, error) {
	rows, err := surrealdb.Query[[]lifecycleVersionHeadRow](ctx, sender, `SELECT tenant_id, procedure_id, procedure_version_id, lifecycle_policy_manifest_id, lifecycle_state, lifecycle_decision_id, projection_epoch, updated_at FROM lifecycle_version_head WHERE tenant_id = $tenant_id AND procedure_version_id = $version_id AND lifecycle_policy_manifest_id = $policy_id LIMIT 1`, map[string]any{"tenant_id": string(tenantID), "version_id": versionID, "policy_id": policyID})
	if err != nil || rows == nil || len(*rows) == 0 || len((*rows)[0].Result) == 0 {
		return lifecycle.VersionHead{}, false, err
	}
	row := (*rows)[0].Result[0]
	return lifecycle.VersionHead{TenantID: domain.TenantID(row.TenantID), ProcedureID: row.ProcedureID, ProcedureVersionID: row.VersionID, PolicyManifestID: row.PolicyID, State: row.State, DecisionID: row.DecisionID, ProjectionEpoch: row.Epoch, UpdatedAt: row.UpdatedAt.Time}, true, nil
}

type lifecycleChampionRow struct {
	TenantID    string                `json:"tenant_id"`
	ProcedureID string                `json:"procedure_id"`
	PolicyID    string                `json:"lifecycle_policy_manifest_id"`
	VersionID   string                `json:"procedure_version_id"`
	DecisionID  string                `json:"lifecycle_decision_id"`
	Epoch       uint64                `json:"projection_epoch"`
	UpdatedAt   models.CustomDateTime `json:"updated_at"`
}

func findChampion[S interface {
	*surrealdb.DB | *surrealdb.Transaction
}](ctx context.Context, sender S, tenantID domain.TenantID, procedureID, policyID string) (lifecycle.ChampionHead, bool, error) {
	rows, err := surrealdb.Query[[]lifecycleChampionRow](ctx, sender, `SELECT tenant_id, procedure_id, lifecycle_policy_manifest_id, procedure_version_id, lifecycle_decision_id, projection_epoch, updated_at FROM lifecycle_champion_head WHERE tenant_id = $tenant_id AND procedure_id = $procedure_id AND lifecycle_policy_manifest_id = $policy_id LIMIT 1`, map[string]any{"tenant_id": string(tenantID), "procedure_id": procedureID, "policy_id": policyID})
	if err != nil || rows == nil || len(*rows) == 0 || len((*rows)[0].Result) == 0 {
		return lifecycle.ChampionHead{}, false, err
	}
	row := (*rows)[0].Result[0]
	return lifecycle.ChampionHead{TenantID: domain.TenantID(row.TenantID), ProcedureID: row.ProcedureID, PolicyManifestID: row.PolicyID, ProcedureVersionID: row.VersionID, DecisionID: row.DecisionID, ProjectionEpoch: row.Epoch, UpdatedAt: row.UpdatedAt.Time}, true, nil
}

func lifecycleHeadID(table string, values ...any) models.RecordID {
	_, hash, err := canonical.MarshalAndHash(values)
	if err != nil {
		panic(fmt.Sprintf("canonical lifecycle record id: %v", err))
	}
	return models.NewRecordID(table, "lc_"+hash)
}
