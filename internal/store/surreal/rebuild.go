package surreal

import (
	"context"
	"errors"

	"github.com/sauhard74/mem-jev/internal/rebuild"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

type RebuildRepository struct{ db *surrealdb.DB }

func NewRebuildRepository(db *surrealdb.DB) *RebuildRepository { return &RebuildRepository{db: db} }

// PublishActivation stores both authenticated comparison inputs and the permit
// in one transaction. Duplicate content is idempotent; any conflicting record
// fails the transaction without creating activation authority.
func (r *RebuildRepository) PublishActivation(ctx context.Context, stored, rebuilt rebuild.DerivedSnapshot, permit rebuild.ActivationPermit) (_ error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil || r.db == nil {
		return errors.New("SurrealDB rebuild repository is not configured")
	}
	if rebuild.ValidateDerivedSnapshot(stored) != nil || rebuild.ValidateDerivedSnapshot(rebuilt) != nil || rebuild.ValidateActivationPermit(permit) != nil || stored.TenantID != rebuilt.TenantID || stored.TenantID != permit.TenantID || stored.ProjectionEpoch != rebuilt.ProjectionEpoch || stored.ProjectionEpoch != permit.ProjectionEpoch || stored.ID != permit.StoredSnapshotID || rebuilt.ID != permit.RebuiltSnapshotID {
		return rebuild.ErrInvalidDerivedSnapshot
	}
	if hash, found, err := findActivationPermitHash(ctx, r.db, string(permit.TenantID), permit.ID); err != nil {
		return databaseFailure("find rebuild activation permit", err)
	} else if found {
		if hash != permit.ContentHash {
			return rebuild.ErrProjectionMismatch
		}
		return nil
	}
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return databaseFailure("begin rebuild activation", err)
	}
	defer func() {
		if !tx.IsClosed() {
			_ = tx.Cancel(context.Background())
		}
	}()
	for _, item := range []struct {
		role  string
		value rebuild.DerivedSnapshot
	}{{"stored", stored}, {"rebuilt", rebuilt}} {
		record := map[string]any{"tenant_id": string(item.value.TenantID), "derived_snapshot_id": item.value.ID, "projection_epoch": item.value.ProjectionEpoch, "role": item.role, "canonical_snapshot": string(item.value.CanonicalJSON), "created_at": permit.AuthorizedAt, "schema_version": item.value.SchemaVersion, "content_hash": item.value.ContentHash}
		if err := createRecord(ctx, tx, models.NewRecordID("derived_projection_snapshot", item.role+"_"+item.value.ID), record); err != nil {
			return databaseFailure("publish derived snapshot", err)
		}
	}
	record := map[string]any{"tenant_id": string(permit.TenantID), "activation_permit_id": permit.ID, "projection_epoch": permit.ProjectionEpoch, "stored_snapshot_id": permit.StoredSnapshotID, "rebuilt_snapshot_id": permit.RebuiltSnapshotID, "canonical_permit": string(permit.CanonicalJSON), "authorized_at": permit.AuthorizedAt, "schema_version": permit.SchemaVersion, "content_hash": permit.ContentHash}
	if err := createRecord(ctx, tx, models.NewRecordID("derived_activation_permit", permit.ID), record); err != nil {
		return databaseFailure("publish rebuild activation permit", err)
	}
	if err := tx.Commit(ctx); err != nil {
		if hash, found, lookupErr := findActivationPermitHash(ctx, r.db, string(permit.TenantID), permit.ID); lookupErr == nil && found && hash == permit.ContentHash {
			return nil
		}
		return databaseFailure("commit rebuild activation", err)
	}
	return nil
}

func findActivationPermitHash[S interface {
	*surrealdb.DB | *surrealdb.Transaction
}](ctx context.Context, sender S, tenantID, permitID string) (string, bool, error) {
	type row struct {
		Hash string `json:"content_hash"`
	}
	result, err := surrealdb.Query[[]row](ctx, sender, `SELECT content_hash FROM derived_activation_permit WHERE tenant_id = $tenant_id AND activation_permit_id = $permit_id LIMIT 1`, map[string]any{"tenant_id": tenantID, "permit_id": permitID})
	if err != nil || result == nil || len(*result) == 0 || len((*result)[0].Result) == 0 {
		return "", false, err
	}
	return (*result)[0].Result[0].Hash, true, nil
}
