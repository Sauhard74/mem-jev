//go:build integration

package surreal

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/sauhard74/mem-jev/internal/retrieval"
	"github.com/sauhard74/mem-jev/internal/store/storetest"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

func TestSurrealRetrievalChannelsMatchImmutableSnapshot(t *testing.T) {
	db := projectionDatabase(t)
	seedProjectionOutcomes(t, db)
	repository := NewProjectionRepository(db)
	value := storetest.ValidProjection(t, 1, false)
	receipt, err := repository.Publish(context.Background(), value)
	if err != nil {
		t.Fatal(err)
	}
	refresh := storetest.ValidProjection(t, 2, false)
	refreshReceipt, err := repository.Publish(context.Background(), refresh)
	if err != nil {
		t.Fatal(err)
	}
	query, err := retrieval.BuildQuery("tenant_a", "policy.v1", retrieval.AliasSet{}, retrieval.Input{
		Task: "write and verify", Tools: []retrieval.Tool{{Name: "write", ContractVersionID: "tcv_write"}, {Name: "verify", ContractVersionID: "tcv_verify"}},
		Harness: retrieval.Harness{Name: "test-harness", Version: "1"}, RiskClass: retrieval.RiskMedium,
		LatencyClass: retrieval.LatencyInteractive, MaxCandidates: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	query, err = retrieval.BindEffectSignature(query, value.RetrievalDocument.EffectSignatureHash)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []retrieval.ChannelName{retrieval.ChannelExact, retrieval.ChannelLexical, retrieval.ChannelFacet, retrieval.ChannelGraph} {
		t.Run(string(name), func(t *testing.T) {
			channel, channelErr := NewRetrievalChannel(db, name, "idx_"+string(name)+".v1")
			if channelErr != nil {
				t.Fatal(channelErr)
			}
			for _, epoch := range []uint64{receipt.ProjectionEpoch, refreshReceipt.ProjectionEpoch} {
				hits, searchErr := channel.Search(context.Background(), retrieval.ChannelRequest{TenantID: "tenant_a", ProjectionEpoch: epoch, Query: query, Limit: 10})
				if searchErr != nil || len(hits) != 1 || hits[0].VersionID != value.Version.ID {
					t.Fatalf("epoch %d hits = %#v, err = %v", epoch, hits, searchErr)
				}
			}
		})
	}
	channel, _ := NewRetrievalChannel(db, retrieval.ChannelExact, "idx_exact.v1")
	hits, err := channel.Search(context.Background(), retrieval.ChannelRequest{TenantID: "tenant_b", ProjectionEpoch: receipt.ProjectionEpoch, Query: query, Limit: 10})
	if err != nil || len(hits) != 0 {
		t.Fatalf("cross-tenant hits = %#v, err = %v", hits, err)
	}
}

func TestSurrealRetrievalQueriesUseDeclaredIndexes(t *testing.T) {
	db := projectionDatabase(t)
	checks := []struct{ query, index string }{
		{`SELECT id FROM retrieval_document WHERE tenant_id = "tenant_a" AND intent_hash = "` + strings.Repeat("a", 64) + `" AND effect_signature_hash = "` + strings.Repeat("b", 64) + `" AND projection_epoch = 1 EXPLAIN FULL`, "retrieval_document_exact"},
		{`SELECT id FROM retrieval_document WHERE task_text @0@ "release" EXPLAIN FULL`, "retrieval_document_task_text"},
	}
	for _, check := range checks {
		result, err := surrealdb.Query[any](context.Background(), db, check.query, nil)
		if err != nil {
			t.Fatal(err)
		}
		if rendered := strings.ToLower(fmt.Sprintf("%#v", result)); !strings.Contains(rendered, check.index) {
			t.Fatalf("plan does not use %s: %s", check.index, rendered)
		}
	}
}
