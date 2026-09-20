package surreal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/jev"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

type JevRepository struct{ db *surrealdb.DB }

func NewJevRepository(db *surrealdb.DB) *JevRepository { return &JevRepository{db: db} }

func (repository *JevRepository) LookupReusable(ctx context.Context, tenantID domain.TenantID, key string, now time.Time) (jev.JudgmentRecord, error) {
	if err := ctx.Err(); err != nil {
		return jev.JudgmentRecord{}, err
	}
	if repository == nil || repository.db == nil || tenantID == "" || !strings.HasPrefix(key, "jevj_") || now.IsZero() {
		return jev.JudgmentRecord{}, jev.ErrJudgmentNotFound
	}
	record, found, err := readJevJudgment(ctx, repository.db, tenantID, key)
	if err != nil {
		return jev.JudgmentRecord{}, databaseFailure("read Jev judgment", err)
	}
	if !found || !now.UTC().Before(record.ReusableUntil) {
		return jev.JudgmentRecord{}, jev.ErrJudgmentNotFound
	}
	return record, nil
}

func (repository *JevRepository) Commit(ctx context.Context, record jev.JudgmentRecord) (jev.JudgmentRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return jev.JudgmentRecord{}, false, err
	}
	if repository == nil || repository.db == nil || jev.ValidateJudgmentRecord(record) != nil {
		return jev.JudgmentRecord{}, false, jev.ErrInvalidJudgment
	}
	const maximumAttempts = 8
	var lastErr error
	for attempt := 0; attempt < maximumAttempts; attempt++ {
		winner, created, err := repository.commitOnce(ctx, record)
		if err == nil {
			return winner, created, nil
		}
		lastErr = err
		winner, found, lookupErr := readJevJudgment(ctx, repository.db, record.TenantID, record.Key)
		if lookupErr != nil {
			return jev.JudgmentRecord{}, false, databaseFailure("resolve Jev judgment winner", lookupErr)
		}
		if found {
			return winner, false, nil
		}
		if !surrealdb.IsTransactionConflict(err) {
			return jev.JudgmentRecord{}, false, databaseFailure("commit Jev judgment", err)
		}
		if err = waitForRetry(ctx, attempt); err != nil {
			return jev.JudgmentRecord{}, false, err
		}
	}
	return jev.JudgmentRecord{}, false, databaseFailure("commit Jev judgment after conflicts", lastErr)
}

func (repository *JevRepository) commitOnce(ctx context.Context, record jev.JudgmentRecord) (_ jev.JudgmentRecord, _ bool, err error) {
	tx, err := repository.db.Begin(ctx)
	if err != nil {
		return jev.JudgmentRecord{}, false, err
	}
	defer func() {
		if !tx.IsClosed() {
			_ = tx.Cancel(context.Background())
		}
	}()
	if winner, found, readErr := readJevJudgment(ctx, tx, record.TenantID, record.Key); readErr != nil {
		return jev.JudgmentRecord{}, false, readErr
	} else if found {
		if cancelErr := tx.Cancel(ctx); cancelErr != nil {
			return jev.JudgmentRecord{}, false, cancelErr
		}
		return winner, false, nil
	}
	row := map[string]any{
		"tenant_id": string(record.TenantID), "judgment_key": record.Key, "query_hash": record.QueryHash,
		"procedure_version_id": record.ProcedureVersionID, "document_hash": record.DocumentHash, "environment_hash": record.EnvironmentHash,
		"policy_manifest_id": record.PolicyManifestID, "rubric_manifest_id": record.RubricManifestID,
		"provider": record.Provider, "model": record.Model, "canonical_judgment": string(record.CanonicalJSON),
		"created_at": record.CreatedAt, "reusable_until": record.ReusableUntil,
		"schema_version": record.SchemaVersion, "content_hash": record.ContentHash,
	}
	id := models.NewRecordID("jev_judgment", strings.TrimPrefix(record.Key, "jevj_"))
	if err = createRecord(ctx, tx, id, row); err != nil {
		return jev.JudgmentRecord{}, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return jev.JudgmentRecord{}, false, err
	}
	return record, true, nil
}

func readJevJudgment[T interface {
	*surrealdb.DB | *surrealdb.Transaction
}](ctx context.Context, sender T, tenantID domain.TenantID, key string) (jev.JudgmentRecord, bool, error) {
	type row struct {
		Canonical string `json:"canonical_judgment"`
		Hash      string `json:"content_hash"`
	}
	results, err := surrealdb.Query[[]row](ctx, sender, `SELECT canonical_judgment, content_hash FROM jev_judgment WHERE tenant_id = $tenant_id AND judgment_key = $key LIMIT 1`, map[string]any{"tenant_id": string(tenantID), "key": key})
	if err != nil {
		return jev.JudgmentRecord{}, false, err
	}
	if results == nil || len(*results) == 0 || len((*results)[0].Result) == 0 {
		return jev.JudgmentRecord{}, false, nil
	}
	stored := (*results)[0].Result[0]
	var record jev.JudgmentRecord
	if err = json.Unmarshal([]byte(stored.Canonical), &record); err != nil {
		return jev.JudgmentRecord{}, false, fmt.Errorf("decode Jev judgment: %w", err)
	}
	record.ContentHash, record.CanonicalJSON = stored.Hash, []byte(stored.Canonical)
	if record.TenantID != tenantID || record.Key != key || jev.ValidateJudgmentRecord(record) != nil {
		return jev.JudgmentRecord{}, false, errors.New("invalid stored Jev judgment")
	}
	return record, true, nil
}

var _ jev.Repository = (*JevRepository)(nil)
