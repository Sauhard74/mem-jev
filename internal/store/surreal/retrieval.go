package surreal

import (
	"context"
	"errors"
	"math"
	"sort"
	"strings"

	"github.com/sauhard74/mem-jev/internal/retrieval"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

const revisionScanFactor = 64

type RetrievalChannel struct {
	db         *surrealdb.DB
	name       retrieval.ChannelName
	manifestID string
}

func NewRetrievalChannel(db *surrealdb.DB, name retrieval.ChannelName, manifestID string) (*RetrievalChannel, error) {
	if db == nil || strings.TrimSpace(manifestID) == "" || (name != retrieval.ChannelExact && name != retrieval.ChannelLexical && name != retrieval.ChannelFacet && name != retrieval.ChannelGraph) {
		return nil, errors.New("invalid SurrealDB retrieval channel configuration")
	}
	return &RetrievalChannel{db: db, name: name, manifestID: manifestID}, nil
}

func (c *RetrievalChannel) Name() retrieval.ChannelName { return c.name }
func (c *RetrievalChannel) ManifestID() string          { return c.manifestID }
func (c *RetrievalChannel) Approximate() bool           { return false }

func (c *RetrievalChannel) Search(ctx context.Context, request retrieval.ChannelRequest) ([]retrieval.Hit, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c == nil || c.db == nil || request.TenantID == "" || request.ProjectionEpoch == 0 || request.Limit == 0 || request.Limit > 1000 {
		return nil, &retrieval.ChannelError{Code: "invalid_request", Err: retrieval.ErrInvalidChannels}
	}
	var rows []retrievalRow
	var err error
	switch c.name {
	case retrieval.ChannelExact:
		effectHash := request.Query.EffectSignatureHash
		if effectHash == "" {
			inferred, inferErr := c.query(ctx, exactEffectInferenceQuery, request, map[string]any{"intent_hash": request.Query.IntentHash})
			if inferErr != nil {
				return nil, &retrieval.ChannelError{Code: "query_failed", Err: inferErr}
			}
			if len(inferred) != 1 || inferred[0].EffectSignatureHash == "" {
				return nil, nil
			}
			effectHash = inferred[0].EffectSignatureHash
		}
		rows, err = c.query(ctx, exactCandidateQuery, request, map[string]any{"intent_hash": request.Query.IntentHash, "effect_hash": effectHash})
	case retrieval.ChannelLexical:
		rows, err = c.query(ctx, lexicalCandidateQuery, request, map[string]any{"task": request.Query.Task})
	case retrieval.ChannelFacet:
		contracts := make([]string, len(request.Query.Tools))
		for index, tool := range request.Query.Tools {
			contracts[index] = tool.ContractVersionID
		}
		rows, err = c.query(ctx, facetCandidateQuery, request, map[string]any{"contracts": contracts, "environment_hash": request.Query.EnvironmentHash, "harness": request.Query.Harness.Name})
	case retrieval.ChannelGraph:
		contracts := make([]string, len(request.Query.Tools))
		for index, tool := range request.Query.Tools {
			contracts[index] = tool.ContractVersionID
		}
		rows, err = c.query(ctx, graphCandidateQuery, request, map[string]any{"contracts": contracts})
	}
	if err != nil {
		return nil, &retrieval.ChannelError{Code: "query_failed", Err: err}
	}
	capRows := int(request.Limit)*revisionScanFactor + 1
	if len(rows) >= capRows {
		return nil, &retrieval.ChannelError{Code: "candidate_window_exhausted", Err: errors.New("revision scan bound reached")}
	}
	latest := make(map[string]retrievalRow)
	for _, row := range rows {
		if row.VersionID == "" || row.Epoch == 0 || row.Epoch > request.ProjectionEpoch || math.IsNaN(row.Score) || math.IsInf(row.Score, 0) {
			return nil, &retrieval.ChannelError{Code: "malformed_result", Err: errors.New("invalid indexed row")}
		}
		if prior, exists := latest[row.VersionID]; !exists || row.Epoch > prior.Epoch {
			latest[row.VersionID] = row
		}
	}
	ordered := make([]retrievalRow, 0, len(latest))
	for _, row := range latest {
		ordered = append(ordered, row)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Score != ordered[j].Score {
			return ordered[i].Score > ordered[j].Score
		}
		return ordered[i].VersionID < ordered[j].VersionID
	})
	if len(ordered) > int(request.Limit) {
		ordered = ordered[:request.Limit]
	}
	hits := make([]retrieval.Hit, len(ordered))
	for index, row := range ordered {
		score := int64(math.Round(row.Score * 1_000_000))
		hits[index] = retrieval.Hit{VersionID: row.VersionID, RawScoreQuantized: score, IndexManifestID: c.manifestID}
	}
	return hits, nil
}

type retrievalRow struct {
	VersionID           string  `json:"procedure_version_id"`
	EffectSignatureHash string  `json:"effect_signature_hash"`
	Epoch               uint64  `json:"projection_epoch"`
	Score               float64 `json:"score"`
}

func (c *RetrievalChannel) query(ctx context.Context, statement string, request retrieval.ChannelRequest, extra map[string]any) ([]retrievalRow, error) {
	variables := map[string]any{
		"tenant_id": string(request.TenantID), "epoch": request.ProjectionEpoch,
		"scan_limit": int(request.Limit)*revisionScanFactor + 1,
	}
	for key, value := range extra {
		variables[key] = value
	}
	results, err := surrealdb.Query[[]retrievalRow](ctx, c.db, statement, variables)
	if err != nil {
		return nil, err
	}
	if results == nil || len(*results) == 0 {
		return nil, nil
	}
	return (*results)[0].Result, nil
}

var _ retrieval.Channel = (*RetrievalChannel)(nil)
