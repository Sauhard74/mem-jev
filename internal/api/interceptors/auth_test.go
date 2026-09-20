package interceptors

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"connectrpc.com/connect"
	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/policy"
	"github.com/sauhard74/mem-jev/internal/security"
)

func TestBearerAuthenticatorRejectsTenantHeader(t *testing.T) {
	auth := security.NewBearerAuthenticator(staticResolver{
		principal: security.Principal{TenantID: domain.TenantID("tenant_a"), Consent: policy.LearnAndRecall},
	})
	header := make(http.Header)
	header.Set("Authorization", "Bearer 0123456789abcdef")
	header.Set("X-Tenant-ID", "tenant_b")

	_, err := auth.Authenticate(header)
	if !errors.Is(err, security.ErrTenantOverride) {
		t.Fatalf("Authenticate() error = %v; want ErrTenantOverride", err)
	}
}

func TestAuthInterceptorBindsCredentialPrincipal(t *testing.T) {
	want := security.Principal{
		TenantID: domain.TenantID("tenant_a"),
		Consent:  policy.LearnAndRecall,
		Scopes:   map[string]struct{}{security.ScopeIngestWrite: {}},
	}
	interceptor := NewAuth(security.NewBearerAuthenticator(staticResolver{principal: want}), security.ScopeIngestWrite)
	next := func(ctx context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
		got, ok := security.PrincipalFromContext(ctx)
		if !ok || got.TenantID != want.TenantID {
			t.Fatalf("PrincipalFromContext() = %#v, %v", got, ok)
		}
		return connect.NewResponse(&memjevv1.IngestTraceResponse{}), nil
	}
	req := connect.NewRequest(&memjevv1.IngestTraceRequest{})
	req.Header().Set("Authorization", "Bearer 0123456789abcdef")

	if _, err := interceptor.WrapUnary(next)(context.Background(), req); err != nil {
		t.Fatal(err)
	}
}

func TestAuthInterceptorUsesSafeConnectCodes(t *testing.T) {
	tests := []struct {
		name     string
		auth     security.Authenticator
		header   string
		wantCode connect.Code
		required string
	}{
		{name: "missing", auth: security.NewBearerAuthenticator(staticResolver{}), wantCode: connect.CodeUnauthenticated},
		{name: "unknown", auth: security.NewBearerAuthenticator(staticResolver{err: security.ErrUnknownCredential}), header: "Bearer 0123456789abcdef", wantCode: connect.CodeUnauthenticated},
		{name: "missing scope", auth: security.NewBearerAuthenticator(staticResolver{principal: security.Principal{TenantID: "tenant_a"}}), header: "Bearer 0123456789abcdef", required: security.ScopeIngestWrite, wantCode: connect.CodePermissionDenied},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := connect.NewRequest(&memjevv1.IngestTraceRequest{})
			req.Header().Set("Authorization", test.header)
			_, err := NewAuth(test.auth, test.required).WrapUnary(successUnary)(context.Background(), req)
			if connect.CodeOf(err) != test.wantCode {
				t.Fatalf("code = %v, error = %v; want %v", connect.CodeOf(err), err, test.wantCode)
			}
			if test.header != "" && err != nil && contains(err.Error(), test.header) {
				t.Fatalf("error leaked credential: %v", err)
			}
		})
	}
}

type staticResolver struct {
	principal security.Principal
	err       error
}

func (r staticResolver) ResolveCredentialHash(string) (security.Principal, error) {
	return r.principal, r.err
}

func successUnary(context.Context, connect.AnyRequest) (connect.AnyResponse, error) {
	return connect.NewResponse(&memjevv1.IngestTraceResponse{}), nil
}

func contains(value, fragment string) bool {
	for index := 0; index+len(fragment) <= len(value); index++ {
		if value[index:index+len(fragment)] == fragment {
			return true
		}
	}
	return false
}
