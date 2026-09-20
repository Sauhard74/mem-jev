package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	storesurreal "github.com/sauhard74/mem-jev/internal/store/surreal"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

type preflightReport struct {
	CanonicalEvents int `json:"canonical_events"`
	Tenants         int `json:"tenants_with_serving_state"`
	RetrievalDocs   int `json:"retrieval_documents"`
}

func main() {
	endpoint := flag.String("endpoint", os.Getenv("MEMJEV_E2E_SURREAL_URL"), "SurrealDB websocket endpoint")
	username := flag.String("username", os.Getenv("SURREAL_USER"), "SurrealDB username")
	password := flag.String("password", os.Getenv("SURREAL_PASS"), "SurrealDB password")
	namespace := flag.String("namespace", "memjev", "SurrealDB namespace")
	database := flag.String("database", "memjev", "SurrealDB database")
	minimumEvents := flag.Int("minimum-events", 10_000_000, "minimum canonical event count")
	minimumTenants := flag.Int("minimum-tenants", 2, "minimum tenants with active serving state")
	flag.Parse()
	if *endpoint == "" || *username == "" || *password == "" || *minimumEvents < 1 || *minimumTenants < 2 {
		fmt.Fprintln(os.Stderr, "endpoint, credentials, and production qualification minima are required")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db, err := storesurreal.Open(ctx, storesurreal.Config{Endpoint: *endpoint, Namespace: *namespace, Database: *database, Username: *username, Password: *password, AuthScope: storesurreal.AuthScopeRoot})
	if err != nil {
		fatal(err)
	}
	defer func() { _ = db.Close(context.Background()) }()
	report := preflightReport{CanonicalEvents: countTable(ctx, db, "canonical_event"), RetrievalDocs: countTable(ctx, db, "retrieval_document"), Tenants: servingTenants(ctx, db)}
	_ = json.NewEncoder(os.Stdout).Encode(report)
	if report.CanonicalEvents < *minimumEvents || report.Tenants < *minimumTenants || report.RetrievalDocs < *minimumTenants {
		fmt.Fprintf(os.Stderr, "production corpus preflight failed: events=%d/%d tenants=%d/%d documents=%d\n", report.CanonicalEvents, *minimumEvents, report.Tenants, *minimumTenants, report.RetrievalDocs)
		os.Exit(1)
	}
}

func countTable(ctx context.Context, db *surrealdb.DB, table string) int {
	type countRow struct {
		Count int `json:"count"`
	}
	allowed := map[string]bool{"canonical_event": true, "retrieval_document": true}
	if !allowed[table] {
		fatal(fmt.Errorf("unsupported table %q", table))
	}
	rows, err := surrealdb.Query[[]countRow](ctx, db, "SELECT count() AS count FROM "+table+" GROUP ALL", nil)
	if err != nil || rows == nil || len(*rows) == 0 || len((*rows)[0].Result) == 0 {
		fatal(fmt.Errorf("count %s: %w", table, err))
	}
	return (*rows)[0].Result[0].Count
}

func servingTenants(ctx context.Context, db *surrealdb.DB) int {
	type tenantRow struct {
		TenantID string `json:"tenant_id"`
	}
	rows, err := surrealdb.Query[[]tenantRow](ctx, db, `SELECT tenant_id FROM retrieval_serving_head GROUP BY tenant_id`, nil)
	if err != nil || rows == nil || len(*rows) == 0 {
		fatal(fmt.Errorf("count serving tenants: %w", err))
	}
	return len((*rows)[0].Result)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
