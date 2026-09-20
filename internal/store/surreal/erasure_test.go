//go:build integration

package surreal

import (
	"context"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/erasure"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

func TestTenantErasureDeletesOnlyTargetAndCommitsHashedReceipt(t *testing.T) {
	db := projectionDatabase(t)
	for _, tenantID := range []string{"tenant_a", "tenant_b"} {
		_, err := surrealdb.Query[any](context.Background(), db, `CREATE ONLY tenant CONTENT { tenant_id: $tenant, status: "active", created_at: $now, schema_version: "tenant.v1" }`, map[string]any{"tenant": tenantID, "now": time.Now().UTC()})
		if err != nil {
			t.Fatal(err)
		}
	}
	receipt, err := erasure.NewReceipt("erase_01JABCDE1234567890", "tenant_a", 2, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	repository := NewErasureRepository(db)
	if err = repository.BeginErasure(context.Background(), "tenant_a", receipt.RequestID); err != nil {
		t.Fatal(err)
	}
	writeResults, writeErr := surrealdb.Query[any](context.Background(), db, `UPDATE tenant SET status = "inactive" WHERE tenant_id = "tenant_a"`, nil)
	if writeErr == nil {
		t.Fatalf("write for fenced tenant succeeded: %#v", writeResults)
	}
	winner, err := repository.CommitErasure(context.Background(), "tenant_a", receipt)
	if err != nil || winner.ContentHash != receipt.ContentHash {
		t.Fatalf("CommitErasure() = %#v, %v", winner, err)
	}

	type countRow struct {
		Count int `json:"count"`
	}
	for tenantID, want := range map[string]int{"tenant_a": 0, "tenant_b": 1} {
		rows, queryErr := surrealdb.Query[[]countRow](context.Background(), db, `SELECT count() AS count FROM tenant WHERE tenant_id = $tenant GROUP ALL`, map[string]any{"tenant": tenantID})
		if queryErr != nil || rows == nil || len(*rows) == 0 || len((*rows)[0].Result) == 0 || (*rows)[0].Result[0].Count != want {
			t.Fatalf("tenant %s rows=%#v err=%v want=%d", tenantID, rows, queryErr, want)
		}
	}
	replayed, err := repository.CommitErasure(context.Background(), "tenant_a", receipt)
	if err != nil || replayed.ContentHash != receipt.ContentHash {
		t.Fatalf("idempotent erasure = %#v, %v", replayed, err)
	}
}
