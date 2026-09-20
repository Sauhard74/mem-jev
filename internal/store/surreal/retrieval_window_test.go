package surreal

import (
	"errors"
	"fmt"
	"testing"

	"github.com/sauhard74/mem-jev/internal/retrieval"
)

func TestCandidateWindowDistinguishesCorpusSizeFromRevisionCrowding(t *testing.T) {
	request := retrieval.ChannelRequest{TenantID: "tenant_a", ProjectionEpoch: 1, Limit: 1}
	distinct := make([]retrievalRow, revisionScanFactor+1)
	for index := range distinct {
		distinct[index] = retrievalRow{VersionID: fmt.Sprintf("pv_%03d", index), Epoch: 1, Score: 1}
	}
	hits, err := finalizeRetrievalRows(distinct, request, "idx_facet.v1")
	if err != nil || len(hits) != 1 || hits[0].VersionID != "pv_000" {
		t.Fatalf("normal corpus growth failed: hits=%#v error=%v", hits, err)
	}

	crowded := make([]retrievalRow, revisionScanFactor*2+1)
	for index := range crowded {
		crowded[index] = retrievalRow{VersionID: "pv_only", Epoch: uint64(index + 1), Score: 1}
	}
	_, err = finalizeRetrievalRows(crowded, retrieval.ChannelRequest{TenantID: "tenant_a", ProjectionEpoch: revisionScanFactor*2 + 1, Limit: 2}, "idx_facet.v1")
	var channelErr *retrieval.ChannelError
	if !errors.As(err, &channelErr) || channelErr.Code != "candidate_window_exhausted" {
		t.Fatalf("revision crowding error=%v", err)
	}
}
