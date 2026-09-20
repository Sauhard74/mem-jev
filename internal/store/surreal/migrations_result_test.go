package surreal

import (
	"testing"

	surrealdb "github.com/surrealdb/surrealdb.go"
)

func TestQueryResultErrorRejectsEmbeddedStatementFailure(t *testing.T) {
	results := []surrealdb.QueryResult[any]{{Status: "OK"}, {Status: "ERR", Error: &surrealdb.QueryError{Message: "definition rejected"}}}
	if err := queryResultError(&results); err == nil {
		t.Fatal("embedded query failure accepted")
	}
	if err := queryResultError(&[]surrealdb.QueryResult[any]{{Status: "OK"}}); err != nil {
		t.Fatalf("successful query rejected: %v", err)
	}
}
