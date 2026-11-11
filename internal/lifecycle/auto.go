package lifecycle

import (
	"context"
	"time"

	"github.com/sauhard74/mem-jev/internal/domain"
)

// ScopeEvaluator evaluates stored procedures one tenant scope at a time.
type ScopeEvaluator interface {
	LifecycleScopes(context.Context) ([]domain.TenantID, error)
	EvaluateScope(context.Context, domain.TenantID, PolicyManifest, time.Time) ([]Decision, error)
}

// EvaluateAll evaluates every scope accepted by include (all scopes when nil)
// and calls record for each committed decision. Scopes are evaluated
// independently, so one person's outcomes never decide another person's
// procedure. It returns the first error after trying every scope.
func EvaluateAll(ctx context.Context, evaluator ScopeEvaluator, manifest PolicyManifest, now time.Time, include func(domain.TenantID) bool, record func(Decision)) error {
	scopes, err := evaluator.LifecycleScopes(ctx)
	if err != nil {
		return err
	}
	var firstErr error
	for _, scope := range scopes {
		if include != nil && !include(scope) {
			continue
		}
		decisions, err := evaluator.EvaluateScope(ctx, scope, manifest, now)
		for _, decision := range decisions {
			if record != nil {
				record(decision)
			}
		}
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
