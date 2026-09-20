//go:build integration

package surreal

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/sauhard74/mem-jev/internal/store"
	"github.com/sauhard74/mem-jev/internal/testinfra"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

const surrealImage = "surrealdb/surrealdb:v3.2.4"

func TestFoundationMigrationIsIdempotent(t *testing.T) {
	db := testinfra.StartSurreal(t, surrealImage)
	m := NewMigrator(db)
	if err := m.Apply(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := schemaVersion(t, db); got != 1 {
		t.Fatalf("schema version = %d, want 1", got)
	}
}

func TestOpenAuthenticatesDatabaseScopedCredentials(t *testing.T) {
	instance := testinfra.StartSurrealInstance(t, surrealImage)
	if _, err := surrealdb.Query[any](context.Background(), instance.DB,
		`DEFINE USER app_user ON DATABASE PASSWORD 'app_password' ROLES OWNER`, nil); err != nil {
		t.Fatal(err)
	}
	db, err := Open(context.Background(), Config{
		Endpoint: instance.Endpoint, Namespace: instance.Namespace, Database: instance.Database,
		Username: "app_user", Password: "app_password", AuthScope: AuthScopeDatabase,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close(context.Background()) })
	if _, err := surrealdb.Query[any](context.Background(), db, "INFO FOR DB", nil); err != nil {
		t.Fatal(err)
	}
}

func TestFoundationSchemaRejectsMissingTenant(t *testing.T) {
	db := testinfra.StartSurreal(t, surrealImage)
	if err := NewMigrator(db).Apply(context.Background()); err != nil {
		t.Fatal(err)
	}

	_, err := surrealdb.Query[any](context.Background(), db, `
		CREATE canonical_event CONTENT {
			trace_id: "trace_1",
			event_id: "event_1",
			sequence: 0,
			kind: "tool",
			payload: {},
			created_at: time::now(),
			schema_version: "v1",
			content_hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		}`, nil)
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "tenant_id") {
		t.Fatalf("error = %v, want missing tenant_id rejection", err)
	}
}

func TestFoundationMigrationRejectsChecksumDrift(t *testing.T) {
	db := testinfra.StartSurreal(t, surrealImage)
	m := NewMigrator(db)
	if err := m.Apply(context.Background()); err != nil {
		t.Fatal(err)
	}
	tampered := append([]store.Migration(nil), m.migrations...)
	tampered[0] = store.NewMigration(
		tampered[0].Version,
		tampered[0].Name,
		tampered[0].Statements+"\n-- changed after application\n",
	)

	err := newMigrator(db, tampered).Apply(context.Background())
	if !errors.Is(err, store.ErrMigrationChecksum) {
		t.Fatalf("error = %v, want %v", err, store.ErrMigrationChecksum)
	}
}

func TestConcurrentFoundationMigrationConverges(t *testing.T) {
	db := testinfra.StartSurreal(t, surrealImage)
	const replicas = 8
	start := make(chan struct{})
	errorsSeen := make(chan error, replicas)
	var wait sync.WaitGroup
	for range replicas {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			errorsSeen <- NewMigrator(db).Apply(context.Background())
		}()
	}
	close(start)
	wait.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Errorf("concurrent migration: %v", err)
		}
	}
	if got := schemaVersion(t, db); got != 1 {
		t.Fatalf("schema version = %d, want 1", got)
	}
}

func schemaVersion(t *testing.T, db *surrealdb.DB) int {
	t.Helper()
	type row struct {
		Version int `json:"version"`
	}
	results, err := surrealdb.Query[[]row](context.Background(), db,
		"SELECT version FROM schema_migration ORDER BY version DESC LIMIT 1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(*results) == 0 || len((*results)[0].Result) == 0 {
		return 0
	}
	return (*results)[0].Result[0].Version
}
