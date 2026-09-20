package memory

import (
	"context"
	"strings"
	"testing"

	"github.com/sauhard74/mem-jev/internal/retrieval"
	"github.com/sauhard74/mem-jev/internal/store/storetest"
)

func TestExactChannelAbstainsFromAmbiguousCorpusEffectInference(t *testing.T) {
	repository := NewProjectionRepository()
	first := storetest.ValidProjection(t, 1, false).RetrievalDocument
	second := first
	second.ProcedureVersionID = "pv_other"
	second.EffectSignatureHash = strings.Repeat("f", 64)
	repository.documents[documentKey(first.TenantID, first.ProcedureVersionID, 1)] = first
	repository.documentEpoch[documentKey(first.TenantID, first.ProcedureVersionID, 1)] = 1
	repository.documents[documentKey(second.TenantID, second.ProcedureVersionID, 1)] = second
	repository.documentEpoch[documentKey(second.TenantID, second.ProcedureVersionID, 1)] = 1

	query := retrieval.Query{IntentHash: first.IntentHash}
	hits, err := NewRetrievalChannel(repository, retrieval.ChannelExact).Search(context.Background(), retrieval.ChannelRequest{TenantID: first.TenantID, ProjectionEpoch: 1, Query: query, Limit: 10})
	if err != nil || len(hits) != 0 {
		t.Fatalf("ambiguous exact inference hits=%#v error=%v", hits, err)
	}
}
