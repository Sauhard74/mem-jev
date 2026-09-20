package retrieval_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/sauhard74/mem-jev/internal/retrieval"
)

type fakeChannel struct {
	name retrieval.ChannelName
	hits []retrieval.Hit
	err  error
	wait bool
}

func (f fakeChannel) Name() retrieval.ChannelName { return f.name }
func (f fakeChannel) ManifestID() string {
	if len(f.hits) > 0 {
		return f.hits[0].IndexManifestID
	}
	return "idx_" + string(f.name)
}
func (f fakeChannel) Approximate() bool {
	return len(f.hits) > 0 && f.hits[0].Approximate
}
func (f fakeChannel) Search(ctx context.Context, _ retrieval.ChannelRequest) ([]retrieval.Hit, error) {
	if f.wait {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return f.hits, f.err
}

func TestCollectCandidatesRunsChannelsAndBuildsStableUnion(t *testing.T) {
	channels := []retrieval.Channel{
		fakeChannel{name: retrieval.ChannelVector, hits: []retrieval.Hit{{VersionID: "pv_b", RawScoreQuantized: 8, IndexManifestID: "idx_vec", Approximate: true}, {VersionID: "pv_a", RawScoreQuantized: 7, IndexManifestID: "idx_vec", Approximate: true}}},
		fakeChannel{name: retrieval.ChannelExact, hits: []retrieval.Hit{{VersionID: "pv_a", RawScoreQuantized: 100, IndexManifestID: "idx_exact"}}},
		fakeChannel{name: retrieval.ChannelLexical, hits: []retrieval.Hit{{VersionID: "pv_c", RawScoreQuantized: 20, IndexManifestID: "idx_lex"}, {VersionID: "pv_a", RawScoreQuantized: 10, IndexManifestID: "idx_lex"}}},
	}
	result, err := retrieval.CollectCandidates(context.Background(), retrieval.CollectRequest{
		Request:  retrieval.ChannelRequest{TenantID: "tenant-a", ProjectionEpoch: 3, Limit: 10},
		Channels: channels, Timeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := []string{result.Candidates[0].VersionID, result.Candidates[1].VersionID, result.Candidates[2].VersionID}; !reflect.DeepEqual(got, []string{"pv_a", "pv_b", "pv_c"}) {
		t.Fatalf("candidate order = %v", got)
	}
	first := result.Candidates[0]
	if len(first.Hits) != 3 || first.Hits[0].Channel != retrieval.ChannelExact || first.Hits[0].Rank != 1 || first.Hits[1].Channel != retrieval.ChannelLexical || first.Hits[1].Rank != 2 || first.Hits[2].Channel != retrieval.ChannelVector || first.Hits[2].Rank != 2 {
		t.Fatalf("pv_a hits = %#v", first.Hits)
	}
	if !result.Approximate || len(result.Results) != 3 {
		t.Fatalf("result = %#v", result)
	}
}

func TestCollectCandidatesIsIndependentOfChannelAndHitInputOrder(t *testing.T) {
	first := fakeChannel{name: retrieval.ChannelExact, hits: []retrieval.Hit{{VersionID: "pv_b", IndexManifestID: "idx"}, {VersionID: "pv_a", IndexManifestID: "idx"}}}
	second := fakeChannel{name: retrieval.ChannelFacet, hits: []retrieval.Hit{{VersionID: "pv_a", IndexManifestID: "idx_f"}}}
	left, err := retrieval.CollectCandidates(context.Background(), retrieval.CollectRequest{Request: validChannelRequest(), Channels: []retrieval.Channel{first, second}, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	right, err := retrieval.CollectCandidates(context.Background(), retrieval.CollectRequest{Request: validChannelRequest(), Channels: []retrieval.Channel{second, first}, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(left, right) {
		t.Fatalf("order changed output:\n%#v\n%#v", left, right)
	}
}

func TestCollectCandidatesRecordsStableDegradation(t *testing.T) {
	result, err := retrieval.CollectCandidates(context.Background(), retrieval.CollectRequest{
		Request: validChannelRequest(), Timeout: 10 * time.Millisecond,
		Channels: []retrieval.Channel{
			fakeChannel{name: retrieval.ChannelExact, hits: []retrieval.Hit{{VersionID: "pv_a", IndexManifestID: "idx"}}},
			fakeChannel{name: retrieval.ChannelVector, wait: true},
			fakeChannel{name: retrieval.ChannelLexical, err: &retrieval.ChannelError{Code: "index_unavailable", Err: errors.New("down")}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Degraded) != 2 || result.Degraded[0].Channel != retrieval.ChannelLexical || result.Degraded[0].Code != "index_unavailable" || result.Degraded[1].Channel != retrieval.ChannelVector || result.Degraded[1].Code != "deadline_exceeded" {
		t.Fatalf("degraded = %#v", result.Degraded)
	}
}

func TestCollectCandidatesRejectsMalformedChannelsAndHits(t *testing.T) {
	tests := []retrieval.CollectRequest{
		{Request: validChannelRequest(), Timeout: time.Second, Channels: []retrieval.Channel{fakeChannel{name: retrieval.ChannelExact}, fakeChannel{name: retrieval.ChannelExact}}},
		{Request: validChannelRequest(), Timeout: time.Second, Channels: []retrieval.Channel{fakeChannel{name: "made_up"}}},
		{Request: validChannelRequest(), Timeout: time.Second, Channels: []retrieval.Channel{fakeChannel{name: retrieval.ChannelExact, hits: []retrieval.Hit{{IndexManifestID: "idx"}}}}},
		{Request: validChannelRequest(), Timeout: time.Second, Channels: []retrieval.Channel{fakeChannel{name: retrieval.ChannelExact, hits: []retrieval.Hit{{VersionID: "pv", IndexManifestID: "idx"}, {VersionID: "pv", IndexManifestID: "idx"}}}}},
		{Request: retrieval.ChannelRequest{}, Timeout: time.Second, Channels: []retrieval.Channel{fakeChannel{name: retrieval.ChannelExact}}},
	}
	for index, request := range tests {
		if _, err := retrieval.CollectCandidates(context.Background(), request); err == nil {
			t.Fatalf("case %d: error = nil", index)
		}
	}
}

func validChannelRequest() retrieval.ChannelRequest {
	return retrieval.ChannelRequest{TenantID: "tenant-a", ProjectionEpoch: 1, Limit: 10}
}
