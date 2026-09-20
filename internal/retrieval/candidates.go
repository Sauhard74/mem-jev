package retrieval

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

var ErrInvalidChannels = errors.New("invalid candidate channels")

type RankedHit struct {
	Channel           ChannelName
	Rank              uint32
	RawScoreQuantized int64
	IndexManifestID   string
	Approximate       bool
}

type CandidateSet struct {
	VersionID string
	Hits      []RankedHit
}

type ChannelResult struct {
	Channel         ChannelName
	IndexManifestID string
	HitCount        uint32
	Approximate     bool
	LatencyMicros   int64
}

type DegradedChannel struct {
	Channel         ChannelName
	Code            string
	IndexManifestID string
	Approximate     bool
	LatencyMicros   int64
}

type CandidateCollection struct {
	Candidates  []CandidateSet
	Results     []ChannelResult
	Degraded    []DegradedChannel
	Approximate bool
}

type channelResponse struct {
	name          ChannelName
	manifestID    string
	approximate   bool
	hits          []Hit
	err           error
	latencyMicros int64
}

func CollectCandidates(ctx context.Context, request CollectRequest) (CandidateCollection, error) {
	if err := validateCollectRequest(request); err != nil {
		return CandidateCollection{}, err
	}
	channelCtx, cancel := context.WithTimeout(ctx, request.Timeout)
	defer cancel()
	responses := make(chan channelResponse, len(request.Channels))
	for _, channel := range request.Channels {
		go func() {
			started := time.Now()
			hits, err := channel.Search(channelCtx, request.Request)
			responses <- channelResponse{name: channel.Name(), manifestID: channel.ManifestID(), approximate: channel.Approximate(), hits: hits, err: err, latencyMicros: max(0, time.Since(started).Microseconds())}
		}()
	}
	byChannel := make(map[ChannelName]channelResponse, len(request.Channels))
	pending := make(map[ChannelName]struct{}, len(request.Channels))
	for _, channel := range request.Channels {
		pending[channel.Name()] = struct{}{}
	}
	for len(pending) > 0 {
		select {
		case response := <-responses:
			if _, exists := pending[response.name]; !exists {
				continue
			}
			byChannel[response.name] = response
			delete(pending, response.name)
		case <-channelCtx.Done():
			if err := ctx.Err(); err != nil {
				return CandidateCollection{}, err
			}
			for _, channel := range request.Channels {
				if _, exists := pending[channel.Name()]; exists {
					byChannel[channel.Name()] = channelResponse{name: channel.Name(), manifestID: channel.ManifestID(), approximate: channel.Approximate(), err: context.DeadlineExceeded, latencyMicros: request.Timeout.Microseconds()}
				}
			}
			clear(pending)
		}
	}
	if err := ctx.Err(); err != nil {
		return CandidateCollection{}, err
	}
	names := make([]ChannelName, 0, len(byChannel))
	for name := range byChannel {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool { return names[i] < names[j] })
	collection := CandidateCollection{}
	union := make(map[string][]RankedHit)
	for _, name := range names {
		response := byChannel[name]
		if response.err != nil {
			collection.Degraded = append(collection.Degraded, DegradedChannel{Channel: name, Code: channelFailure(response.err), IndexManifestID: response.manifestID, Approximate: response.approximate, LatencyMicros: response.latencyMicros})
			continue
		}
		result, err := validateHits(name, response.manifestID, response.approximate, response.hits, request.Request.Limit)
		if err != nil {
			return CandidateCollection{}, err
		}
		collection.Results = append(collection.Results, result)
		collection.Results[len(collection.Results)-1].LatencyMicros = response.latencyMicros
		collection.Approximate = collection.Approximate || result.Approximate
		for index, hit := range response.hits {
			union[hit.VersionID] = append(union[hit.VersionID], RankedHit{Channel: name, Rank: uint32(index + 1), RawScoreQuantized: hit.RawScoreQuantized, IndexManifestID: hit.IndexManifestID, Approximate: hit.Approximate})
		}
	}
	versionIDs := make([]string, 0, len(union))
	for versionID := range union {
		versionIDs = append(versionIDs, versionID)
	}
	sort.Strings(versionIDs)
	for _, versionID := range versionIDs {
		hits := union[versionID]
		sort.Slice(hits, func(i, j int) bool { return hits[i].Channel < hits[j].Channel })
		collection.Candidates = append(collection.Candidates, CandidateSet{VersionID: versionID, Hits: hits})
	}
	return collection, nil
}

func validateCollectRequest(request CollectRequest) error {
	if request.Request.TenantID == "" || request.Request.ProjectionEpoch == 0 || request.Request.Limit == 0 || request.Request.Limit > 1000 || request.Timeout <= 0 || len(request.Channels) == 0 || len(request.Channels) > 5 {
		return ErrInvalidChannels
	}
	seen := make(map[ChannelName]struct{}, len(request.Channels))
	for _, channel := range request.Channels {
		if channel == nil || !validChannel(channel.Name()) || strings.TrimSpace(channel.ManifestID()) == "" || (channel.Approximate() && channel.Name() != ChannelVector) {
			return ErrInvalidChannels
		}
		if _, exists := seen[channel.Name()]; exists {
			return ErrInvalidChannels
		}
		seen[channel.Name()] = struct{}{}
	}
	return nil
}

func validateHits(channel ChannelName, manifest string, channelApproximate bool, hits []Hit, limit uint32) (ChannelResult, error) {
	if len(hits) > int(limit) {
		return ChannelResult{}, fmt.Errorf("%w: channel %s exceeded limit", ErrInvalidChannels, channel)
	}
	seen := make(map[string]struct{}, len(hits))
	approximate := channelApproximate
	for _, hit := range hits {
		if strings.TrimSpace(hit.VersionID) == "" || strings.TrimSpace(hit.IndexManifestID) == "" {
			return ChannelResult{}, ErrInvalidChannels
		}
		if _, exists := seen[hit.VersionID]; exists {
			return ChannelResult{}, ErrInvalidChannels
		}
		seen[hit.VersionID] = struct{}{}
		if manifest != hit.IndexManifestID || hit.Approximate != channelApproximate || (hit.Approximate && channel != ChannelVector) {
			return ChannelResult{}, ErrInvalidChannels
		}
		approximate = approximate || hit.Approximate
	}
	return ChannelResult{Channel: channel, IndexManifestID: manifest, HitCount: uint32(len(hits)), Approximate: approximate}, nil
}
