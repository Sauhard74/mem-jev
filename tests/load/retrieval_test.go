package main

import (
	"testing"

	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
)

func TestHealthyVectorProvenance(t *testing.T) {
	indexes := []string{"exact", "lexical", "facet", "graph", "vector"}
	if !healthyVectorProvenance(&memjevv1.RetrievalProvenance{IndexManifestIds: indexes}) {
		t.Fatal("complete five-channel provenance was rejected")
	}
	for name, provenance := range map[string]*memjevv1.RetrievalProvenance{
		"missing vector": {IndexManifestIds: indexes[:4]},
		"vector outage":  {IndexManifestIds: indexes, DegradedChannels: []string{"vector:index_unavailable"}},
		"nil":            nil,
	} {
		t.Run(name, func(t *testing.T) {
			if healthyVectorProvenance(provenance) {
				t.Fatal("unhealthy provenance was accepted")
			}
		})
	}
}
