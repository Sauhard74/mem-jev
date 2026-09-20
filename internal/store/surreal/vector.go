package surreal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/embedding"
	"github.com/sauhard74/mem-jev/internal/retrieval"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

var (
	ErrInvalidVectorGeneration = errors.New("invalid vector index generation")
	identifierPattern          = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)
)

type VectorTuning struct {
	Degree       uint32 `json:"degree"`
	BuildSearch  uint32 `json:"build_search"`
	AlphaMilli   uint32 `json:"alpha_milli"`
	Overfetch    uint32 `json:"overfetch"`
	SearchEffort uint32 `json:"search_effort"`
}

type VectorGeneration struct {
	SchemaVersion   string             `json:"schema_version"`
	TenantID        string             `json:"tenant_id"`
	IndexManifestID string             `json:"index_manifest_id"`
	EmbeddingID     string             `json:"embedding_manifest_id"`
	TableName       string             `json:"table_name"`
	IndexName       string             `json:"index_name"`
	Dimension       uint32             `json:"dimension"`
	Distance        embedding.Distance `json:"distance"`
	Algorithm       string             `json:"algorithm"`
	Tuning          VectorTuning       `json:"tuning"`
	State           string             `json:"state"`
	ContentHash     string             `json:"-"`
}

func ProvisionVectorGeneration(ctx context.Context, db *surrealdb.DB, tenantID string, manifest embedding.Manifest, tuning VectorTuning) (VectorGeneration, error) {
	if db == nil {
		return VectorGeneration{}, ErrInvalidVectorGeneration
	}
	generation, err := BuildVectorGeneration(tenantID, manifest, tuning)
	if err != nil {
		return VectorGeneration{}, err
	}
	if existing, found, findErr := findVectorGeneration(ctx, db, tenantID, generation.IndexManifestID); findErr != nil {
		return VectorGeneration{}, findErr
	} else if found {
		if existing.ContentHash != generation.ContentHash {
			return VectorGeneration{}, ErrInvalidVectorGeneration
		}
		return existing, nil
	}
	if !identifierPattern.MatchString(generation.TableName) || !identifierPattern.MatchString(generation.IndexName) {
		return VectorGeneration{}, ErrInvalidVectorGeneration
	}
	distance := strings.ToUpper(string(manifest.Distance))
	alpha := fmt.Sprintf("%d.%03d", tuning.AlphaMilli/1000, tuning.AlphaMilli%1000)
	ddl := fmt.Sprintf(`
DEFINE TABLE IF NOT EXISTS %s SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD IF NOT EXISTS tenant_id ON TABLE %s TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS procedure_version_id ON TABLE %s TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS projection_epoch ON TABLE %s TYPE int READONLY ASSERT $value > 0;
DEFINE FIELD IF NOT EXISTS embedding_id ON TABLE %s TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS input_hash ON TABLE %s TYPE string READONLY ASSERT string::len($value) = 64;
DEFINE FIELD IF NOT EXISTS vector ON TABLE %s TYPE array<float> READONLY;
DEFINE FIELD IF NOT EXISTS vector_quantized ON TABLE %s TYPE array<int> READONLY;
DEFINE FIELD IF NOT EXISTS environment_scope_hash ON TABLE %s TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS harness_name ON TABLE %s TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS tool_contract_version_ids ON TABLE %s TYPE array<string> READONLY;
DEFINE FIELD IF NOT EXISTS created_at ON TABLE %s TYPE datetime READONLY;
DEFINE FIELD IF NOT EXISTS schema_version ON TABLE %s TYPE string READONLY;
DEFINE FIELD IF NOT EXISTS content_hash ON TABLE %s TYPE string READONLY;
DEFINE INDEX IF NOT EXISTS %s ON TABLE %s FIELDS vector DISKANN DIMENSION %d DIST %s TYPE F32 DEGREE %d L_BUILD %d ALPHA %s;
DEFINE INDEX IF NOT EXISTS %s_snapshot ON TABLE %s FIELDS tenant_id, procedure_version_id, projection_epoch UNIQUE;`,
		generation.TableName, generation.TableName, generation.TableName, generation.TableName, generation.TableName,
		generation.TableName, generation.TableName, generation.TableName, generation.TableName, generation.TableName,
		generation.TableName, generation.TableName, generation.TableName, generation.TableName,
		generation.IndexName, generation.TableName, manifest.Dimension, distance, tuning.Degree, tuning.BuildSearch, alpha,
		generation.IndexName, generation.TableName)
	if _, err = surrealdb.Query[any](ctx, db, ddl, nil); err != nil {
		return VectorGeneration{}, databaseFailure("provision vector index", err)
	}
	now := time.Now().UTC()
	manifestRecord := map[string]any{
		"tenant_id": tenantID, "embedding_manifest_id": manifest.ID, "provider": manifest.Provider, "model": manifest.Model,
		"model_revision": manifest.ModelRevision, "dimension": manifest.Dimension, "distance": string(manifest.Distance),
		"normalization": string(manifest.Normalization), "created_at": now, "schema_version": manifest.SchemaVersion,
		"content_hash": manifest.ContentHash,
	}
	if err = createOrVerifyRecord(ctx, db, models.NewRecordID("embedding_manifest", tenantID+"_"+manifest.ID), manifestRecord, "embedding_manifest", "embedding_manifest_id", tenantID, manifest.ID, manifest.ContentHash); err != nil {
		return VectorGeneration{}, err
	}
	generationRecord := map[string]any{
		"tenant_id": tenantID, "index_manifest_id": generation.IndexManifestID, "embedding_manifest_id": manifest.ID,
		"table_name": generation.TableName, "index_name": generation.IndexName, "dimension": generation.Dimension,
		"algorithm": generation.Algorithm, "distance": string(generation.Distance), "tuning": generation.Tuning,
		"state": generation.State, "created_at": now, "schema_version": generation.SchemaVersion, "content_hash": generation.ContentHash,
	}
	if err = createOrVerifyRecord(ctx, db, models.NewRecordID("embedding_index_generation", tenantID+"_"+generation.IndexManifestID), generationRecord, "embedding_index_generation", "index_manifest_id", tenantID, generation.IndexManifestID, generation.ContentHash); err != nil {
		return VectorGeneration{}, err
	}
	return generation, nil
}

// BuildVectorGeneration derives every physical identifier from the immutable
// embedding manifest and tuning. Runtime configuration never supplies SQL
// identifiers directly.
func BuildVectorGeneration(tenantID string, manifest embedding.Manifest, tuning VectorTuning) (VectorGeneration, error) {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" || embedding.ValidateManifest(manifest) != nil || !validVectorTuning(tuning) {
		return VectorGeneration{}, ErrInvalidVectorGeneration
	}
	physical := struct {
		EmbeddingID string             `json:"embedding_manifest_id"`
		Dimension   uint32             `json:"dimension"`
		Distance    embedding.Distance `json:"distance"`
		Algorithm   string             `json:"algorithm"`
		Tuning      VectorTuning       `json:"tuning"`
	}{manifest.ID, manifest.Dimension, manifest.Distance, "diskann", tuning}
	_, physicalHash, err := canonical.MarshalAndHash(physical)
	if err != nil {
		return VectorGeneration{}, err
	}
	generation := VectorGeneration{
		SchemaVersion: "embedding-index-generation.v1", TenantID: tenantID,
		IndexManifestID: "vidx_" + physicalHash, EmbeddingID: manifest.ID,
		TableName: "embedding_g_" + physicalHash[:32], IndexName: "diskann_" + physicalHash[:32],
		Dimension: manifest.Dimension, Distance: manifest.Distance, Algorithm: "diskann", Tuning: tuning, State: "ready",
	}
	_, generation.ContentHash, err = canonical.MarshalAndHash(generation)
	if err != nil {
		return VectorGeneration{}, err
	}
	return generation, nil
}

func validVectorTuning(value VectorTuning) bool {
	return value.Degree >= 8 && value.Degree <= 256 && value.BuildSearch >= value.Degree && value.BuildSearch <= 2000 &&
		value.AlphaMilli >= 1000 && value.AlphaMilli <= 2000 && value.Overfetch >= 2 && value.Overfetch <= 64 &&
		value.SearchEffort >= value.Overfetch && value.SearchEffort <= 2000
}

func findVectorGeneration(ctx context.Context, db *surrealdb.DB, tenantID, id string) (VectorGeneration, bool, error) {
	type generationRow struct {
		SchemaVersion   string             `json:"schema_version"`
		TenantID        string             `json:"tenant_id"`
		IndexManifestID string             `json:"index_manifest_id"`
		EmbeddingID     string             `json:"embedding_manifest_id"`
		TableName       string             `json:"table_name"`
		IndexName       string             `json:"index_name"`
		Dimension       uint32             `json:"dimension"`
		Distance        embedding.Distance `json:"distance"`
		Algorithm       string             `json:"algorithm"`
		Tuning          VectorTuning       `json:"tuning"`
		State           string             `json:"state"`
		ContentHash     string             `json:"content_hash"`
	}
	results, err := surrealdb.Query[[]generationRow](ctx, db, `SELECT tenant_id, index_manifest_id, embedding_manifest_id, table_name, index_name, dimension, distance, algorithm, tuning, state, schema_version, content_hash FROM embedding_index_generation WHERE tenant_id = $tenant_id AND index_manifest_id = $id LIMIT 1`, map[string]any{"tenant_id": tenantID, "id": id})
	if err != nil {
		return VectorGeneration{}, false, databaseFailure("read vector generation", err)
	}
	if results == nil || len(*results) == 0 || len((*results)[0].Result) == 0 {
		return VectorGeneration{}, false, nil
	}
	row := (*results)[0].Result[0]
	return VectorGeneration(row), true, nil
}

func createOrVerifyRecord(ctx context.Context, db *surrealdb.DB, id models.RecordID, record map[string]any, table, idField, tenantID, logicalID, contentHash string) error {
	if !identifierPattern.MatchString(table) || !identifierPattern.MatchString(idField) {
		return ErrInvalidVectorGeneration
	}
	if _, err := surrealdb.Query[any](ctx, db, `CREATE ONLY $id CONTENT $record`, map[string]any{"id": id, "record": record}); err == nil {
		return nil
	}
	type hashRow struct {
		Hash string `json:"content_hash"`
	}
	statement := fmt.Sprintf("SELECT content_hash FROM %s WHERE tenant_id = $tenant_id AND %s = $logical_id LIMIT 1", table, idField)
	rows, lookupErr := surrealdb.Query[[]hashRow](ctx, db, statement, map[string]any{"tenant_id": tenantID, "logical_id": logicalID})
	if lookupErr != nil || rows == nil || len(*rows) == 0 || len((*rows)[0].Result) == 0 || (*rows)[0].Result[0].Hash != contentHash {
		return ErrInvalidVectorGeneration
	}
	return nil
}

type EmbeddingStore struct{ db *surrealdb.DB }

func NewEmbeddingStore(db *surrealdb.DB) (*EmbeddingStore, error) {
	if db == nil {
		return nil, errors.New("embedding store is not configured")
	}
	return &EmbeddingStore{db: db}, nil
}

func (s *EmbeddingStore) Get(ctx context.Context, tenantID, manifestID, inputHash string) (embedding.Embedding, bool, error) {
	if s == nil || s.db == nil {
		return embedding.Embedding{}, false, errors.New("embedding store is not configured")
	}
	type row struct{ ID, Hash, Canonical string }
	rows, err := surrealdb.Query[[]struct {
		ID        string `json:"embedding_id"`
		Hash      string `json:"content_hash"`
		Canonical string `json:"canonical_embedding"`
	}](ctx, s.db, `SELECT embedding_id, content_hash, canonical_embedding FROM embedding_value WHERE tenant_id = $tenant_id AND embedding_manifest_id = $manifest_id AND input_hash = $input_hash LIMIT 1`, map[string]any{"tenant_id": tenantID, "manifest_id": manifestID, "input_hash": inputHash})
	_ = row{}
	if err != nil {
		return embedding.Embedding{}, false, databaseFailure("read embedding", err)
	}
	if rows == nil || len(*rows) == 0 || len((*rows)[0].Result) == 0 {
		return embedding.Embedding{}, false, nil
	}
	stored := (*rows)[0].Result[0]
	var value embedding.Embedding
	if err := json.Unmarshal([]byte(stored.Canonical), &value); err != nil {
		return embedding.Embedding{}, false, databaseFailure("decode embedding", err)
	}
	value.ID, value.ContentHash, value.CanonicalJSON = stored.ID, stored.Hash, []byte(stored.Canonical)
	return value, true, nil
}

func (s *EmbeddingStore) Put(ctx context.Context, value embedding.Embedding) (embedding.Embedding, error) {
	if s == nil || s.db == nil || embedding.ValidateContent(value) != nil {
		return embedding.Embedding{}, embedding.ErrStoreConflict
	}
	record := map[string]any{
		"tenant_id": value.TenantID, "embedding_id": value.ID, "embedding_manifest_id": value.ManifestID, "input_hash": value.InputHash,
		"vector_quantized": value.Vector.Values, "canonical_embedding": string(value.CanonicalJSON), "created_at": time.Now().UTC(),
		"schema_version": value.SchemaVersion, "content_hash": value.ContentHash,
	}
	_, err := surrealdb.Query[any](ctx, s.db, `CREATE ONLY $id CONTENT $record`, map[string]any{"id": models.NewRecordID("embedding_value", value.ID), "record": record})
	if err == nil {
		return value, nil
	}
	existing, found, lookupErr := s.Get(ctx, value.TenantID, value.ManifestID, value.InputHash)
	if lookupErr != nil {
		return embedding.Embedding{}, lookupErr
	}
	if !found || existing.ID != value.ID {
		return embedding.Embedding{}, embedding.ErrStoreConflict
	}
	return existing, nil
}

var _ embedding.Store = (*EmbeddingStore)(nil)

func PutVectorDocument(ctx context.Context, db *surrealdb.DB, generation VectorGeneration, manifest embedding.Manifest, document retrieval.Document, epoch uint64, value embedding.Embedding) error {
	if db == nil || validateGeneration(generation, manifest) != nil || retrieval.ValidateDocument(document) != nil || epoch == 0 || generation.TenantID != string(document.TenantID) {
		return ErrInvalidVectorGeneration
	}
	input, err := embedding.InputFromDocument(document)
	if err != nil || embedding.ValidateEmbedding(value, string(document.TenantID), manifest, input.Hash) != nil {
		return embedding.ErrStoreConflict
	}
	if !identifierPattern.MatchString(generation.TableName) {
		return ErrInvalidVectorGeneration
	}
	tools := make([]string, len(document.Tools))
	for index, tool := range document.Tools {
		tools[index] = tool.ContractVersionID
	}
	rowHashSource := struct {
		Tenant, Version, Embedding string
		Epoch                      uint64
	}{string(document.TenantID), document.ProcedureVersionID, value.ID, epoch}
	_, rowHash, err := canonical.MarshalAndHash(rowHashSource)
	if err != nil {
		return err
	}
	record := map[string]any{
		"tenant_id": string(document.TenantID), "procedure_version_id": document.ProcedureVersionID, "projection_epoch": epoch,
		"embedding_id": value.ID, "input_hash": input.Hash, "vector": value.Vector.StorageValues(), "vector_quantized": value.Vector.Values,
		"environment_scope_hash": document.EnvironmentScopeHash, "harness_name": document.Harness.Name,
		"tool_contract_version_ids": tools, "created_at": time.Now().UTC(), "schema_version": "vector-document.v1", "content_hash": rowHash,
	}
	statement := fmt.Sprintf(`CREATE ONLY type::record("%s", $id) CONTENT $record`, generation.TableName)
	if _, err = surrealdb.Query[any](ctx, db, statement, map[string]any{"id": rowHash, "record": record}); err != nil {
		return databaseFailure("store vector document", err)
	}
	return nil
}

func validateGeneration(generation VectorGeneration, manifest embedding.Manifest) error {
	expected, err := BuildVectorGeneration(generation.TenantID, manifest, generation.Tuning)
	if err != nil || generation.SchemaVersion != expected.SchemaVersion || generation.IndexManifestID != expected.IndexManifestID || generation.EmbeddingID != expected.EmbeddingID || generation.TableName != expected.TableName || generation.IndexName != expected.IndexName || generation.Dimension != expected.Dimension || generation.Distance != expected.Distance || generation.Algorithm != expected.Algorithm || generation.State != expected.State || generation.ContentHash != expected.ContentHash {
		return ErrInvalidVectorGeneration
	}
	return nil
}

type VectorChannel struct {
	db         *surrealdb.DB
	manifest   embedding.Manifest
	generation VectorGeneration
	service    *embedding.Service
}

func NewVectorChannel(db *surrealdb.DB, manifest embedding.Manifest, generation VectorGeneration, service *embedding.Service) (*VectorChannel, error) {
	if validateGeneration(generation, manifest) != nil {
		return nil, ErrInvalidVectorGeneration
	}
	if db == nil || service == nil {
		return nil, errors.New("vector channel is not configured")
	}
	return &VectorChannel{db: db, manifest: manifest, generation: generation, service: service}, nil
}

func (c *VectorChannel) Name() retrieval.ChannelName { return retrieval.ChannelVector }
func (c *VectorChannel) ManifestID() string          { return c.generation.IndexManifestID }
func (c *VectorChannel) Approximate() bool           { return true }

type vectorRow struct {
	VersionID string  `json:"procedure_version_id"`
	Epoch     uint64  `json:"projection_epoch"`
	Values    []int32 `json:"vector_quantized"`
}

func (c *VectorChannel) Search(ctx context.Context, request retrieval.ChannelRequest) ([]retrieval.Hit, error) {
	if c == nil || c.db == nil || c.service == nil || request.TenantID == "" || request.ProjectionEpoch == 0 || request.Limit == 0 || request.Limit > 1000 {
		return nil, &retrieval.ChannelError{Code: "invalid_request", Err: retrieval.ErrInvalidChannels}
	}
	input, err := embedding.InputFromQuery(request.Query)
	if err != nil {
		return nil, &retrieval.ChannelError{Code: "query_input_invalid", Err: err}
	}
	values, err := c.service.Generate(ctx, string(request.TenantID), c.manifest, []embedding.Input{input})
	if err != nil {
		return nil, &retrieval.ChannelError{Code: "query_embedding_failed", Err: err}
	}
	queryVector := values[0].Vector
	poolSize := int(request.Limit) * int(c.generation.Tuning.Overfetch)
	if poolSize > 4096 {
		poolSize = 4096
	}
	contracts := make([]string, len(request.Query.Tools))
	for index, tool := range request.Query.Tools {
		contracts[index] = tool.ContractVersionID
	}
	rows, err := surrealdb.Query[[]vectorRow](ctx, c.db, vectorCandidateStatement(c.generation, poolSize), map[string]any{
		"tenant_id": string(request.TenantID), "epoch": request.ProjectionEpoch, "vector": queryVector.StorageValues(),
		"environment_hash": request.Query.EnvironmentHash, "harness": request.Query.Harness.Name, "contracts": contracts,
	})
	if err != nil {
		return nil, &retrieval.ChannelError{Code: "query_failed", Err: err}
	}
	if rows == nil || len(*rows) == 0 {
		return nil, nil
	}
	latestEpochs, err := currentDocumentEpochs(ctx, c.db, request, (*rows)[0].Result)
	if err != nil {
		return nil, &retrieval.ChannelError{Code: "snapshot_validation_failed", Err: err}
	}
	type scored struct {
		version string
		score   int64
	}
	latest := make(map[string]scored)
	for _, row := range (*rows)[0].Result {
		if row.VersionID == "" || row.Epoch == 0 || row.Epoch > request.ProjectionEpoch || latestEpochs[row.VersionID] != row.Epoch {
			continue
		}
		score, scoreErr := embedding.ExactScore(c.manifest, queryVector, embedding.Vector{Values: row.Values})
		if scoreErr != nil {
			return nil, &retrieval.ChannelError{Code: "malformed_result", Err: scoreErr}
		}
		if prior, found := latest[row.VersionID]; !found || score > prior.score {
			latest[row.VersionID] = scored{row.VersionID, score}
		}
	}
	ordered := make([]scored, 0, len(latest))
	for _, item := range latest {
		ordered = append(ordered, item)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].score != ordered[j].score {
			return ordered[i].score > ordered[j].score
		}
		return ordered[i].version < ordered[j].version
	})
	if len(ordered) > int(request.Limit) {
		ordered = ordered[:request.Limit]
	}
	hits := make([]retrieval.Hit, len(ordered))
	for index, item := range ordered {
		hits[index] = retrieval.Hit{VersionID: item.version, RawScoreQuantized: item.score, IndexManifestID: c.generation.IndexManifestID, Approximate: true}
	}
	return hits, nil
}

func vectorCandidateStatement(generation VectorGeneration, poolSize int) string {
	if poolSize < 1 || poolSize > 4096 || !identifierPattern.MatchString(generation.TableName) {
		return ""
	}
	return fmt.Sprintf(`SELECT procedure_version_id, projection_epoch, vector_quantized FROM %s WHERE vector <|%d,%d|> $vector AND tenant_id = $tenant_id AND projection_epoch <= $epoch AND (environment_scope_hash = $environment_hash OR harness_name = $harness OR tool_contract_version_ids CONTAINSANY $contracts)`, generation.TableName, poolSize, generation.Tuning.SearchEffort)
}

func currentDocumentEpochs(ctx context.Context, db *surrealdb.DB, request retrieval.ChannelRequest, rows []vectorRow) (map[string]uint64, error) {
	ids := make([]string, 0, len(rows))
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		if _, ok := seen[row.VersionID]; !ok {
			seen[row.VersionID] = struct{}{}
			ids = append(ids, row.VersionID)
		}
	}
	if len(ids) == 0 {
		return map[string]uint64{}, nil
	}
	type epochRow struct {
		VersionID string `json:"procedure_version_id"`
		Epoch     uint64 `json:"epoch"`
	}
	results, err := surrealdb.Query[[]epochRow](ctx, db, `SELECT procedure_version_id, math::max(projection_epoch) AS epoch FROM retrieval_document WHERE tenant_id = $tenant_id AND projection_epoch <= $epoch AND procedure_version_id IN $ids GROUP BY procedure_version_id`, map[string]any{"tenant_id": string(request.TenantID), "epoch": request.ProjectionEpoch, "ids": ids})
	if err != nil {
		return nil, err
	}
	result := make(map[string]uint64, len(ids))
	if results == nil || len(*results) == 0 {
		return result, nil
	}
	for _, row := range (*results)[0].Result {
		if row.Epoch == 0 {
			return nil, errors.New("invalid snapshot epoch")
		}
		result[row.VersionID] = row.Epoch
	}
	return result, nil
}

var _ retrieval.Channel = (*VectorChannel)(nil)
