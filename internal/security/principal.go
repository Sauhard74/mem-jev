package security

import (
	"context"

	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/policy"
)

const (
	ScopeIngestWrite  = "ingest:write"
	ScopeOutcomeWrite = "outcomes:write"
)

type Principal struct {
	TenantID domain.TenantID
	Region   string
	Scopes   map[string]struct{}
	Consent  policy.ConsentMode
}

func (p Principal) HasScope(scope string) bool {
	if scope == "" {
		return true
	}
	_, ok := p.Scopes[scope]
	return ok
}

type RequestMetadata struct {
	IdempotencyKeyHash string
}

type principalContextKey struct{}
type requestMetadataContextKey struct{}

func WithPrincipal(ctx context.Context, principal Principal) context.Context {
	return context.WithValue(ctx, principalContextKey{}, principal)
}

func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	principal, ok := ctx.Value(principalContextKey{}).(Principal)
	return principal, ok
}

func WithRequestMetadata(ctx context.Context, metadata RequestMetadata) context.Context {
	return context.WithValue(ctx, requestMetadataContextKey{}, metadata)
}

func RequestMetadataFromContext(ctx context.Context) (RequestMetadata, bool) {
	metadata, ok := ctx.Value(requestMetadataContextKey{}).(RequestMetadata)
	return metadata, ok
}
