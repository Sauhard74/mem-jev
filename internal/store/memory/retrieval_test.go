package memory_test

import (
	"context"
	"testing"

	"github.com/sauhard74/mem-jev/internal/retrieval"
	"github.com/sauhard74/mem-jev/internal/store/memory"
	"github.com/sauhard74/mem-jev/internal/store/storetest"
)

func TestRetrievalChannelsUseLatestDocumentAtRequestedEpoch(t *testing.T) {
	repository := memory.NewProjectionRepository()
	first := storetest.ValidProjection(t, 1, false)
	firstReceipt, err := repository.Publish(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	second := storetest.ValidProjection(t, 2, false)
	secondReceipt, err := repository.Publish(context.Background(), second)
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
	for _, name := range []retrieval.ChannelName{retrieval.ChannelExact, retrieval.ChannelLexical, retrieval.ChannelFacet, retrieval.ChannelGraph, retrieval.ChannelVector} {
		t.Run(string(name), func(t *testing.T) {
			channel := memory.NewRetrievalChannel(repository, name)
			for _, epoch := range []uint64{firstReceipt.ProjectionEpoch, secondReceipt.ProjectionEpoch} {
				hits, searchErr := channel.Search(context.Background(), retrieval.ChannelRequest{TenantID: "tenant_a", ProjectionEpoch: epoch, Query: query, Limit: 10})
				if searchErr != nil || len(hits) != 1 || hits[0].VersionID != first.Version.ID {
					t.Fatalf("epoch %d hits = %#v, err = %v", epoch, hits, searchErr)
				}
			}
		})
	}
}

func TestRetrievalChannelsEnforceTenantAndEpoch(t *testing.T) {
	repository := memory.NewProjectionRepository()
	value := storetest.ValidProjection(t, 1, false)
	if _, err := repository.Publish(context.Background(), value); err != nil {
		t.Fatal(err)
	}
	channel := memory.NewRetrievalChannel(repository, retrieval.ChannelLexical)
	query := retrieval.Query{Task: "write verify"}
	for _, request := range []retrieval.ChannelRequest{
		{TenantID: "tenant_b", ProjectionEpoch: 1, Query: query, Limit: 10},
		{TenantID: "tenant_a", ProjectionEpoch: 99, Query: retrieval.Query{Task: "unrelated"}, Limit: 10},
	} {
		hits, err := channel.Search(context.Background(), request)
		if err != nil || len(hits) != 0 {
			t.Fatalf("hits = %#v, err = %v", hits, err)
		}
	}
}
