package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"regexp"
	"time"

	"github.com/sauhard74/mem-jev/internal/retrieval"
	storesurreal "github.com/sauhard74/mem-jev/internal/store/surreal"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

type preflightReport struct {
	CanonicalEvents   int `json:"canonical_events"`
	Tenants           int `json:"tenants_with_serving_state"`
	RetrievalDocs     int `json:"retrieval_documents"`
	VectorTenants     int `json:"tenants_with_active_vector"`
	PopulatedVectors  int `json:"tenants_with_populated_vector"`
	VectorGenerations int `json:"active_vector_generations"`
	VectorDocuments   int `json:"active_vector_documents"`
}

var physicalTablePattern = regexp.MustCompile(`^embedding_g_[0-9a-f]{32}$`)

func main() {
	endpoint := flag.String("endpoint", os.Getenv("MEMJEV_E2E_SURREAL_URL"), "SurrealDB websocket endpoint")
	username := flag.String("username", os.Getenv("SURREAL_USER"), "SurrealDB username")
	password := flag.String("password", os.Getenv("SURREAL_PASS"), "SurrealDB password")
	namespace := flag.String("namespace", "memjev", "SurrealDB namespace")
	database := flag.String("database", "memjev", "SurrealDB database")
	minimumEvents := flag.Int("minimum-events", 10_000_000, "minimum canonical event count")
	minimumDocuments := flag.Int("minimum-retrieval-documents", 1_000_000, "minimum immutable retrieval document revisions")
	minimumTenants := flag.Int("minimum-tenants", 2, "minimum tenants with active serving state")
	minimumVectorDocuments := flag.Int("minimum-vector-documents", 1_000_000, "minimum documents in active vector generations")
	requireVector := flag.Bool("require-vector", false, "require every serving tenant to have a ready, populated vector generation")
	flag.Parse()
	if *endpoint == "" || *username == "" || *password == "" || *minimumEvents < 1 || *minimumDocuments < 1 || *minimumTenants < 2 || *minimumVectorDocuments < 1 {
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
	vectorState, err := inspectServingState(ctx, db)
	if err != nil {
		fatal(err)
	}
	report := preflightReport{
		CanonicalEvents: countTable(ctx, db, "canonical_event"), RetrievalDocs: countTable(ctx, db, "retrieval_document"),
		Tenants: vectorState.tenants, VectorTenants: vectorState.vectorTenants,
		VectorGenerations: len(vectorState.tableTenants),
	}
	report.VectorDocuments, report.PopulatedVectors = countVectorDocuments(ctx, db, vectorState.tableTenants)
	_ = json.NewEncoder(os.Stdout).Encode(report)
	if report.CanonicalEvents < *minimumEvents || report.Tenants < *minimumTenants || report.RetrievalDocs < *minimumDocuments {
		fmt.Fprintf(os.Stderr, "production corpus preflight failed: events=%d/%d tenants=%d/%d documents=%d/%d\n", report.CanonicalEvents, *minimumEvents, report.Tenants, *minimumTenants, report.RetrievalDocs, *minimumDocuments)
		os.Exit(1)
	}
	if *requireVector && (report.VectorTenants != report.Tenants || report.PopulatedVectors != report.Tenants || report.VectorGenerations == 0 || report.VectorDocuments < *minimumVectorDocuments) {
		fmt.Fprintf(os.Stderr, "production vector preflight failed: vector_tenants=%d/%d populated_vector_tenants=%d/%d generations=%d vector_documents=%d/%d\n", report.VectorTenants, report.Tenants, report.PopulatedVectors, report.Tenants, report.VectorGenerations, report.VectorDocuments, *minimumVectorDocuments)
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

type servingState struct {
	tenants       int
	vectorTenants int
	tableTenants  map[string][]string
}

func inspectServingState(ctx context.Context, db *surrealdb.DB) (servingState, error) {
	type headRow struct {
		TenantID string `json:"tenant_id"`
		ConfigID string `json:"serving_config_id"`
	}
	rows, err := surrealdb.Query[[]headRow](ctx, db, `SELECT tenant_id, serving_config_id FROM retrieval_serving_head`, nil)
	if err != nil || rows == nil || len(*rows) == 0 {
		return servingState{}, fmt.Errorf("read serving tenants: %w", err)
	}
	state := servingState{tenants: len((*rows)[0].Result), tableTenants: make(map[string][]string)}
	for _, head := range (*rows)[0].Result {
		type configRow struct {
			Canonical string `json:"canonical_config"`
		}
		configs, queryErr := surrealdb.Query[[]configRow](ctx, db, `SELECT canonical_config FROM retrieval_serving_config WHERE tenant_id = $tenant_id AND serving_config_id = $config_id LIMIT 1`, map[string]any{"tenant_id": head.TenantID, "config_id": head.ConfigID})
		if queryErr != nil || configs == nil || len(*configs) == 0 || len((*configs)[0].Result) != 1 {
			return servingState{}, fmt.Errorf("read serving config for tenant %q: %w", head.TenantID, queryErr)
		}
		var config retrieval.ServingConfig
		if err = json.Unmarshal([]byte((*configs)[0].Result[0].Canonical), &config); err != nil {
			return servingState{}, fmt.Errorf("decode serving config for tenant %q: %w", head.TenantID, err)
		}
		vectorManifest := ""
		for _, index := range config.Indexes {
			if index.Channel == retrieval.ChannelVector {
				vectorManifest = index.ManifestID
				break
			}
		}
		if vectorManifest == "" {
			continue
		}
		type generationRow struct {
			TableName string `json:"table_name"`
		}
		generations, generationErr := surrealdb.Query[[]generationRow](ctx, db, `SELECT table_name FROM embedding_index_generation WHERE tenant_id = $tenant_id AND index_manifest_id = $manifest_id AND state = "ready" LIMIT 1`, map[string]any{"tenant_id": head.TenantID, "manifest_id": vectorManifest})
		if generationErr != nil || generations == nil || len(*generations) == 0 || len((*generations)[0].Result) != 1 {
			continue
		}
		table := (*generations)[0].Result[0].TableName
		if !physicalTablePattern.MatchString(table) {
			return servingState{}, fmt.Errorf("unsafe vector generation table %q", table)
		}
		state.vectorTenants++
		state.tableTenants[table] = append(state.tableTenants[table], head.TenantID)
	}
	return state, nil
}

func countVectorDocuments(ctx context.Context, db *surrealdb.DB, tableTenants map[string][]string) (total, populatedTenants int) {
	for table, tenants := range tableTenants {
		if !physicalTablePattern.MatchString(table) {
			fatal(fmt.Errorf("unsafe vector generation table %q", table))
		}
		for _, tenant := range tenants {
			count := countPhysicalTable(ctx, db, table, tenant)
			total += count
			if count > 0 {
				populatedTenants++
			}
		}
	}
	return total, populatedTenants
}

func countPhysicalTable(ctx context.Context, db *surrealdb.DB, table, tenantID string) int {
	type countRow struct {
		Count int `json:"count"`
	}
	rows, err := surrealdb.Query[[]countRow](ctx, db, "SELECT count() AS count FROM "+table+" WHERE tenant_id = $tenant_id GROUP ALL", map[string]any{"tenant_id": tenantID})
	if err != nil || rows == nil || len(*rows) == 0 {
		fatal(fmt.Errorf("count vector table %s: %w", table, err))
	}
	if len((*rows)[0].Result) == 0 {
		return 0
	}
	return (*rows)[0].Result[0].Count
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
