package memory

import (
	"context"
	"sort"
	"strings"
	"unicode"

	"github.com/sauhard74/mem-jev/internal/retrieval"
)

type RetrievalChannel struct {
	repository *ProjectionRepository
	name       retrieval.ChannelName
}

func NewRetrievalChannel(repository *ProjectionRepository, name retrieval.ChannelName) *RetrievalChannel {
	return &RetrievalChannel{repository: repository, name: name}
}

func (c *RetrievalChannel) Name() retrieval.ChannelName { return c.name }
func (c *RetrievalChannel) ManifestID() string          { return "memory-" + string(c.name) + ".v1" }
func (c *RetrievalChannel) Approximate() bool           { return false }

func (c *RetrievalChannel) Search(ctx context.Context, request retrieval.ChannelRequest) ([]retrieval.Hit, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c == nil || c.repository == nil || request.TenantID == "" || request.ProjectionEpoch == 0 || request.Limit == 0 {
		return nil, &retrieval.ChannelError{Code: "invalid_request", Err: retrieval.ErrInvalidChannels}
	}
	c.repository.mu.RLock()
	latestEpoch := make(map[string]uint64)
	documents := make(map[string]retrieval.Document)
	for key, document := range c.repository.documents {
		epoch := c.repository.documentEpoch[key]
		if document.TenantID != request.TenantID || epoch > request.ProjectionEpoch || epoch <= latestEpoch[document.ProcedureVersionID] {
			continue
		}
		latestEpoch[document.ProcedureVersionID] = epoch
		documents[document.ProcedureVersionID] = document
	}
	c.repository.mu.RUnlock()
	hits := make([]retrieval.Hit, 0, len(documents))
	for versionID, document := range documents {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		score := c.score(request.Query, document)
		if score <= 0 {
			continue
		}
		hits = append(hits, retrieval.Hit{VersionID: versionID, RawScoreQuantized: score, IndexManifestID: c.ManifestID()})
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].RawScoreQuantized != hits[j].RawScoreQuantized {
			return hits[i].RawScoreQuantized > hits[j].RawScoreQuantized
		}
		return hits[i].VersionID < hits[j].VersionID
	})
	if len(hits) > int(request.Limit) {
		hits = hits[:request.Limit]
	}
	return hits, nil
}

func (c *RetrievalChannel) score(query retrieval.Query, document retrieval.Document) int64 {
	switch c.name {
	case retrieval.ChannelExact:
		if query.IntentHash != "" && query.IntentHash == document.IntentHash {
			return 1_000_000
		}
	case retrieval.ChannelLexical:
		return setSimilarity(words(query.Task), words(document.TaskText))
	case retrieval.ChannelFacet:
		return facetScore(query, document)
	case retrieval.ChannelGraph:
		return graphScore(query, document)
	case retrieval.ChannelVector:
		return setSimilarity(trigrams(query.Task), trigrams(document.TaskText))
	}
	return 0
}

func facetScore(query retrieval.Query, document retrieval.Document) int64 {
	matched, possible := int64(0), int64(0)
	possible++
	if query.Harness.Name == document.Harness.Name && (document.Harness.Version == "" || query.Harness.Version == document.Harness.Version) {
		matched++
	}
	docTools := make(map[string]string, len(document.Tools))
	for _, tool := range document.Tools {
		docTools[tool.Name] = tool.ContractVersionID
	}
	for _, tool := range query.Tools {
		possible++
		if docTools[tool.Name] == tool.ContractVersionID {
			matched++
		}
	}
	docEnvironment := make(map[string]string, len(document.Environment))
	for _, fact := range document.Environment {
		docEnvironment[fact.Name] = fact.Value
	}
	for _, fact := range query.Environment {
		possible++
		if docEnvironment[fact.Name] == fact.Value {
			matched++
		}
	}
	if possible == 0 || matched == 0 {
		return 0
	}
	return matched * 1_000_000 / possible
}

func graphScore(query retrieval.Query, document retrieval.Document) int64 {
	docTools := make(map[string]bool, len(document.Tools))
	for _, tool := range document.Tools {
		docTools[tool.Name+"\x00"+tool.ContractVersionID] = true
	}
	docResources := make(map[string]bool, len(document.Resources))
	for _, resource := range document.Resources {
		docResources[resource.Type+"\x00"+resource.Namespace] = true
	}
	matched := int64(0)
	for _, tool := range query.Tools {
		if docTools[tool.Name+"\x00"+tool.ContractVersionID] {
			matched += 2
		}
	}
	for _, resource := range query.Resources {
		if docResources[resource.Type+"\x00"+resource.Namespace] {
			matched++
		}
	}
	return matched * 100_000
}

func words(value string) map[string]struct{} {
	result := make(map[string]struct{})
	for _, field := range strings.FieldsFunc(strings.ToLower(value), func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsNumber(r) }) {
		if field != "" {
			result[field] = struct{}{}
		}
	}
	return result
}

func trigrams(value string) map[string]struct{} {
	runes := []rune(strings.ToLower(value))
	result := make(map[string]struct{})
	if len(runes) < 3 {
		if len(runes) > 0 {
			result[string(runes)] = struct{}{}
		}
		return result
	}
	for index := 0; index+3 <= len(runes); index++ {
		result[string(runes[index:index+3])] = struct{}{}
	}
	return result
}

func setSimilarity(left, right map[string]struct{}) int64 {
	if len(left) == 0 || len(right) == 0 {
		return 0
	}
	intersection := 0
	for value := range left {
		if _, ok := right[value]; ok {
			intersection++
		}
	}
	if intersection == 0 {
		return 0
	}
	return int64(intersection) * 1_000_000 / int64(len(left)+len(right)-intersection)
}

var _ retrieval.Channel = (*RetrievalChannel)(nil)
