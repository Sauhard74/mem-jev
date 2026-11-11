package surreal

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/security"
	"github.com/sauhard74/mem-jev/internal/toolcontract"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

// ToolContractRepository stores the immutable tool contract versions that let
// the worker turn verified traces into procedures. A tool without a contract
// is opaque, and traces that use it are never learned.
type ToolContractRepository struct {
	db *surrealdb.DB
}

func NewToolContractRepository(db *surrealdb.DB) *ToolContractRepository {
	return &ToolContractRepository{db: db}
}

// RegisteredContract reports the stored version of one registered contract.
type RegisteredContract struct {
	ToolID            string `json:"tool_id"`
	Version           string `json:"version"`
	ContractVersionID string `json:"contract_version_id"`
	Created           bool   `json:"created"`
}

// Register stores a contract version for a tenant. Registering the same
// contract again is a no-op. A different contract for an already registered
// tool version is rejected with toolcontract.ErrVersionConflict, because
// stored procedures refer to versions by content.
//
// Contracts registered for a tenant also apply to that tenant's per-person
// scopes ("tenant__person").
func (r *ToolContractRepository) Register(ctx context.Context, tenantID domain.TenantID, source toolcontract.Manifest) (RegisteredContract, error) {
	if r == nil || r.db == nil || tenantID == "" || security.OrganizationOf(tenantID) != tenantID {
		return RegisteredContract{}, errors.New("tool contracts are registered for a tenant, not a per-person scope")
	}
	manifest, err := toolcontract.Canonicalize(source)
	if err != nil {
		return RegisteredContract{}, err
	}
	result := RegisteredContract{ToolID: manifest.ToolID, Version: manifest.Version, ContractVersionID: manifest.ID}
	existing, found, err := r.storedVersionHash(ctx, tenantID, manifest)
	if err != nil {
		return RegisteredContract{}, err
	}
	if found {
		if existing != manifest.ContentHash {
			return RegisteredContract{}, toolcontract.ErrVersionConflict
		}
		return result, nil
	}
	// Check aliases and versions against everything the tenant already has.
	registry := NewTenantRegistryProvider(r.db).ForTenant(tenantID).(*tenantToolRegistry)
	if err = registry.load(ctx); err != nil {
		return RegisteredContract{}, err
	}
	if err = registry.memory.Register(ctx, manifest); err != nil {
		return RegisteredContract{}, err
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return RegisteredContract{}, err
	}
	var stored map[string]any
	if err = json.Unmarshal(encoded, &stored); err != nil {
		return RegisteredContract{}, err
	}
	_, recordHash, err := canonical.MarshalAndHash(struct {
		TenantID          domain.TenantID `json:"tenant_id"`
		ContractVersionID string          `json:"contract_version_id"`
	}{tenantID, manifest.ID})
	if err != nil {
		return RegisteredContract{}, err
	}
	_, createErr := surrealdb.Query[any](ctx, r.db, `CREATE ONLY $id CONTENT $record`, map[string]any{
		"id": models.NewRecordID("tool_contract_version", "tcv_"+recordHash),
		"record": map[string]any{
			"tenant_id": string(tenantID), "contract_version_id": manifest.ID, "tool_id": manifest.ToolID, "tool_version": manifest.Version,
			"manifest": stored, "created_at": time.Now().UTC(), "schema_version": manifest.SchemaVersion, "content_hash": manifest.ContentHash,
		},
	})
	// A concurrent registration of the same contract is not an error.
	existing, found, err = r.storedVersionHash(ctx, tenantID, manifest)
	if err != nil {
		return RegisteredContract{}, err
	}
	if !found {
		if createErr != nil {
			return RegisteredContract{}, databaseFailure("register tool contract", createErr)
		}
		return RegisteredContract{}, errors.New("tool contract was not stored")
	}
	if existing != manifest.ContentHash {
		return RegisteredContract{}, toolcontract.ErrVersionConflict
	}
	result.Created = createErr == nil
	return result, nil
}

func (r *ToolContractRepository) storedVersionHash(ctx context.Context, tenantID domain.TenantID, manifest toolcontract.Manifest) (string, bool, error) {
	rows, err := surrealdb.Query[[]struct {
		ContentHash string `json:"content_hash"`
	}](ctx, r.db, `SELECT content_hash FROM tool_contract_version WHERE tenant_id = $tenant_id AND tool_id = $tool_id AND tool_version = $version LIMIT 1`,
		map[string]any{"tenant_id": string(tenantID), "tool_id": manifest.ToolID, "version": manifest.Version})
	if err != nil {
		return "", false, databaseFailure("read tool contract", err)
	}
	if rows == nil || len(*rows) == 0 || len((*rows)[0].Result) == 0 {
		return "", false, nil
	}
	return (*rows)[0].Result[0].ContentHash, true, nil
}

// contractTenants lists the tenants whose contracts apply to a storage scope:
// the scope itself and, for a per-person scope, its organization.
func contractTenants(tenantID domain.TenantID) []string {
	organization := security.OrganizationOf(tenantID)
	if organization == tenantID {
		return []string{string(tenantID)}
	}
	return []string{string(tenantID), string(organization)}
}
