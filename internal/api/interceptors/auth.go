package interceptors

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	"github.com/sauhard74/mem-jev/internal/security"
)

func NewAuth(authenticator security.Authenticator, requiredScope string) connect.Interceptor {
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, request connect.AnyRequest) (connect.AnyResponse, error) {
			principal, err := authenticator.Authenticate(request.Header())
			if err != nil {
				return nil, authenticationError(err)
			}
			if !principal.HasScope(requiredScope) {
				return nil, connect.NewError(connect.CodePermissionDenied, errors.New("credential lacks required scope"))
			}
			return next(security.WithPrincipal(ctx, principal), request)
		}
	})
}

func authenticationError(err error) error {
	if errors.Is(err, security.ErrTenantOverride) {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("tenant header is not accepted"))
	}
	if errors.Is(err, security.ErrInvalidPrincipal) {
		return connect.NewError(connect.CodePermissionDenied, errors.New("credential is not authorized"))
	}
	return connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
}
