//go:build integration

package surreal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/selection"
	"github.com/sauhard74/mem-jev/internal/store/storetest"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

func TestSurrealSelectionRepositoryCommitsAndReplays(t *testing.T) {
	db := projectionDatabase(t)
	repository, err := NewSelectionRepository(db, selection.RetentionPolicy{TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 20, 1, 2, 3, 0, time.UTC)
	repository.now = func() time.Time { return now }
	draft := storetest.SelectionDraft(t, "tenant_a", 'a')
	request := selection.CommitRequest{TenantID: draft.TenantID, IdempotencyIdentityHash: repeated('d'), Draft: draft}
	first, err := repository.Commit(context.Background(), request)
	if err != nil || first.Disposition != selection.DispositionCommitted {
		t.Fatalf("receipt=%#v error=%v", first, err)
	}
	second, err := repository.Commit(context.Background(), request)
	if err != nil || second.Disposition != selection.DispositionDuplicate || string(second.Record.CanonicalJSON) != string(first.Record.CanonicalJSON) {
		t.Fatalf("duplicate=%#v error=%v", second, err)
	}
	stored, err := repository.FindByInjectionID(context.Background(), draft.TenantID, first.Record.InjectionID)
	if err != nil || stored.ContentHash != first.Record.ContentHash {
		t.Fatalf("stored=%#v error=%v", stored, err)
	}
	assertSelectionCounts(t, db, first.Record.InjectionID, map[string]int{"selection_record": 1, "selection_plan_node": 1, "selection_plan_edge": 0, "selection_plan_gap": 0})
	now = now.Add(time.Hour)
	_, err = repository.Commit(context.Background(), request)
	if !errors.Is(err, selection.ErrSelectionExpired) {
		t.Fatalf("expired replay error=%v", err)
	}
	assertSelectionCounts(t, db, first.Record.InjectionID, map[string]int{"selection_record": 1, "selection_plan_node": 1, "selection_plan_edge": 0, "selection_plan_gap": 0})
}

func TestSurrealSelectionRepositoryRollsBackChildren(t *testing.T) {
	db := projectionDatabase(t)
	repository, err := NewSelectionRepository(db, selection.RetentionPolicy{TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	repository.failure = true
	draft := storetest.SelectionDraft(t, "tenant_a", 'b')
	injectionID := selection.InjectionID(draft.TenantID, repeated('e'))
	_, err = repository.Commit(context.Background(), selection.CommitRequest{TenantID: draft.TenantID, IdempotencyIdentityHash: repeated('e'), Draft: draft})
	if !errors.Is(err, errInjectedFailure) {
		t.Fatalf("error=%v", err)
	}
	assertSelectionCounts(t, db, injectionID, map[string]int{"selection_record": 0, "selection_plan_node": 0, "selection_plan_edge": 0, "selection_plan_gap": 0})
}

func assertSelectionCounts(t *testing.T, db *surrealdb.DB, injectionID string, want map[string]int) {
	t.Helper()
	for table, expected := range want {
		rows, err := surrealdb.Query[[]struct {
			Count int `json:"count"`
		}](context.Background(), db, "SELECT count() AS count FROM "+table+" WHERE injection_id = $id GROUP ALL", map[string]any{"id": injectionID})
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		if rows != nil && len(*rows) > 0 && len((*rows)[0].Result) > 0 {
			count = (*rows)[0].Result[0].Count
		}
		if count != expected {
			t.Fatalf("%s count=%d want=%d", table, count, expected)
		}
	}
}

func repeated(value byte) string {
	bytes := make([]byte, 64)
	for index := range bytes {
		bytes[index] = value
	}
	return string(bytes)
}
