package security

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"sync/atomic"
	"time"
)

// ReloadingCredentialResolver serves credentials from a file and picks up
// changes to it without a restart. A changed file that fails to load is
// ignored (the previous credentials stay in effect) and retried.
type ReloadingCredentialResolver struct {
	path    string
	current atomic.Pointer[FileCredentialResolver]
	content []byte
}

// NewReloadingCredentialResolver loads path once; it fails if the file is
// invalid, like NewFileCredentialResolver.
func NewReloadingCredentialResolver(path string) (*ReloadingCredentialResolver, error) {
	resolver := &ReloadingCredentialResolver{path: path}
	if _, err := resolver.Reload(); err != nil {
		return nil, err
	}
	return resolver, nil
}

func (r *ReloadingCredentialResolver) ResolveCredentialHash(hash string) (Principal, error) {
	return r.current.Load().ResolveCredentialHash(hash)
}

// Reload reads the file again and reports whether the credentials changed.
// Watch is the only concurrent caller, so content needs no lock.
func (r *ReloadingCredentialResolver) Reload() (bool, error) {
	content, err := os.ReadFile(r.path)
	if err != nil {
		return false, err
	}
	if r.current.Load() != nil && bytes.Equal(content, r.content) {
		return false, nil
	}
	next, err := NewFileCredentialResolver(r.path)
	if err != nil {
		return false, err
	}
	r.current.Store(next)
	r.content = content
	return true, nil
}

// Watch reloads the file every interval until ctx ends.
func (r *ReloadingCredentialResolver) Watch(ctx context.Context, interval time.Duration, logger *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		changed, err := r.Reload()
		if err != nil {
			logger.Warn("credential file reload failed; keeping the loaded credentials", "error", err)
			continue
		}
		if changed {
			logger.Info("credential file reloaded")
		}
	}
}
