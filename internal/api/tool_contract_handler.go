package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	"connectrpc.com/connect"
	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/security"
	"github.com/sauhard74/mem-jev/internal/toolcontract"
)

// ToolContractRegistrar stores tool contract versions for a tenant.
type ToolContractRegistrar interface {
	Register(ctx context.Context, tenantID domain.TenantID, manifest toolcontract.Manifest) (toolcontract.Registration, error)
}

const maxContractsPerRequest = 256

type toolContractHandler struct {
	registrar ToolContractRegistrar
}

func (h *toolContractHandler) RegisterToolContracts(ctx context.Context, request *connect.Request[memjevv1.RegisterToolContractsRequest]) (*connect.Response[memjevv1.RegisterToolContractsResponse], error) {
	principal, ok := security.PrincipalFromContext(ctx)
	if !ok {
		return nil, safeConnectError(ctx, connect.CodeUnauthenticated, "authentication_required", false)
	}
	if h.registrar == nil {
		return nil, safeConnectError(ctx, connect.CodeUnimplemented, "tool_contracts_unavailable", false)
	}
	// Contracts belong to the tenant; a person's credential may not change them.
	if principal.OrganizationID == "" || principal.TenantID != principal.OrganizationID {
		return nil, safeConnectError(ctx, connect.CodePermissionDenied, "tenant_credential_required", false)
	}
	var document struct {
		Contracts []toolcontract.Manifest `json:"contracts"`
	}
	decoder := json.NewDecoder(bytes.NewReader([]byte(request.Msg.GetContractsJson())))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil || !errors.Is(decoder.Decode(&struct{}{}), io.EOF) || len(document.Contracts) == 0 || len(document.Contracts) > maxContractsPerRequest {
		return nil, safeConnectError(ctx, connect.CodeInvalidArgument, "invalid_contract_document", false)
	}
	response := &memjevv1.RegisterToolContractsResponse{}
	for _, contract := range document.Contracts {
		registered, err := h.registrar.Register(ctx, principal.OrganizationID, contract)
		switch {
		case errors.Is(err, toolcontract.ErrInvalidManifest):
			return nil, safeConnectError(ctx, connect.CodeInvalidArgument, "invalid_contract", false)
		case errors.Is(err, toolcontract.ErrVersionConflict):
			return nil, safeConnectError(ctx, connect.CodeAlreadyExists, "contract_version_conflict", false)
		case errors.Is(err, toolcontract.ErrNotTenantScope):
			return nil, safeConnectError(ctx, connect.CodePermissionDenied, "tenant_credential_required", false)
		case err != nil:
			return nil, mapDomainError(ctx, err)
		}
		response.Contracts = append(response.Contracts, &memjevv1.RegisteredToolContract{
			ToolId: registered.ToolID, Version: registered.Version, ContractVersionId: registered.ContractVersionID, Created: registered.Created,
		})
	}
	return connect.NewResponse(response), nil
}
