package api

import (
	"context"
	"net/http/httptest"
	"os"
	"testing"

	"connectrpc.com/connect"
	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/gen/memjev/v1/memjevv1connect"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/policy"
	"github.com/sauhard74/mem-jev/internal/security"
	"github.com/sauhard74/mem-jev/internal/toolcontract"
)

type recordingRegistrar struct {
	tenants []domain.TenantID
}

func (r *recordingRegistrar) Register(_ context.Context, tenantID domain.TenantID, manifest toolcontract.Manifest) (toolcontract.Registration, error) {
	canonical, err := toolcontract.Canonicalize(manifest)
	if err != nil {
		return toolcontract.Registration{}, err
	}
	r.tenants = append(r.tenants, tenantID)
	return toolcontract.Registration{ToolID: canonical.ToolID, Version: canonical.Version, ContractVersionID: canonical.ID, Created: true}, nil
}

func TestRegisterToolContractsOverTheAPI(t *testing.T) {
	const (
		adminToken  = "contract-admin-token"
		personToken = "contract-person-token"
	)
	admin := testPrincipal(policy.LearnAndRecall)
	admin.Scopes = map[string]struct{}{security.ScopeContractWrite: {}}
	admin.OrganizationID = admin.TenantID
	person := admin
	person.TenantID, person.SubjectID = admin.TenantID+"__alice", "alice"
	registrar := &recordingRegistrar{}
	server := httptest.NewServer(NewHandler(Dependencies{
		Authenticator: security.NewBearerAuthenticator(credentialResolver{
			tokenHash(adminToken): admin, tokenHash(personToken): person, tokenHash(testToken): testPrincipal(policy.LearnAndRecall),
		}),
		ToolContracts: registrar,
	}))
	defer server.Close()
	client := memjevv1connect.NewToolContractServiceClient(server.Client(), server.URL)
	contracts, err := os.ReadFile("../../examples/tool-contracts/quickstart.json")
	if err != nil {
		t.Fatal(err)
	}
	call := func(token, document string) (*connect.Response[memjevv1.RegisterToolContractsResponse], error) {
		request := connect.NewRequest(&memjevv1.RegisterToolContractsRequest{ContractsJson: document})
		request.Header().Set("Authorization", "Bearer "+token)
		return client.RegisterToolContracts(context.Background(), request)
	}
	response, err := call(adminToken, string(contracts))
	if err != nil || len(response.Msg.GetContracts()) != 1 || response.Msg.GetContracts()[0].GetToolId() != "quickstart_print" || response.Msg.GetContracts()[0].GetContractVersionId() == "" {
		t.Fatalf("register = %v, %v", response, err)
	}
	if len(registrar.tenants) != 1 || registrar.tenants[0] != admin.TenantID {
		t.Fatalf("registered for %v", registrar.tenants)
	}
	if _, err := call(testToken, string(contracts)); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("credential without contracts:write: %v", err)
	}
	if _, err := call(personToken, string(contracts)); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("per-person credential: %v", err)
	}
	if _, err := call(adminToken, `{"contracts":[{"tool_id":"x"}]}`); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("invalid contract: %v", err)
	}
	if _, err := call(adminToken, `{"tools":[]}`); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("invalid document: %v", err)
	}
}

// The handler refuses a tenant principal without contracts:write even when
// called without the auth interceptor's scope check.
func TestRegisterToolContractsHandlerChecksScope(t *testing.T) {
	principal := testPrincipal(policy.LearnAndRecall)
	principal.OrganizationID = principal.TenantID
	ctx := security.WithPrincipal(context.Background(), principal)
	registrar := &recordingRegistrar{}
	_, err := (&toolContractHandler{registrar: registrar}).RegisterToolContracts(ctx, connect.NewRequest(&memjevv1.RegisterToolContractsRequest{ContractsJson: `{"contracts":[]}`}))
	if connect.CodeOf(err) != connect.CodePermissionDenied || len(registrar.tenants) != 0 {
		t.Fatalf("error = %v, registered %v", err, registrar.tenants)
	}
}
