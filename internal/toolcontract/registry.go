package toolcontract

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"

	"github.com/sauhard74/mem-jev/internal/canonical"
	"golang.org/x/mod/semver"
)

var (
	ErrVersionConflict = errors.New("tool contract version is immutable")
	ErrAliasConflict   = errors.New("tool alias belongs to another contract")
	ErrInvalidQuery    = errors.New("invalid tool contract query")
)

type Registry interface {
	Resolve(context.Context, Query) (Resolution, error)
}

type MemoryRegistry struct {
	mu       sync.RWMutex
	byKey    map[string]Manifest
	names    map[string]string
	versions map[string][]string
}

func NewMemoryRegistry() *MemoryRegistry {
	return &MemoryRegistry{byKey: make(map[string]Manifest), names: make(map[string]string), versions: make(map[string][]string)}
}

func (r *MemoryRegistry) Register(ctx context.Context, manifest Manifest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	canonicalManifest, err := Canonicalize(manifest)
	if err != nil {
		return err
	}
	if manifest.ID != "" && (manifest.ID != canonicalManifest.ID || manifest.ContentHash != canonicalManifest.ContentHash) {
		return ErrInvalidManifest
	}
	key := contractKey(canonicalManifest.ToolID, canonicalManifest.Version)
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, exists := r.byKey[key]; exists {
		if existing.ID == canonicalManifest.ID {
			return nil
		}
		return ErrVersionConflict
	}
	for _, name := range append([]string{canonicalManifest.ToolID}, canonicalManifest.Aliases...) {
		if owner, exists := r.names[name]; exists && owner != canonicalManifest.ToolID {
			return ErrAliasConflict
		}
	}
	r.byKey[key] = cloneManifest(canonicalManifest)
	for _, name := range append([]string{canonicalManifest.ToolID}, canonicalManifest.Aliases...) {
		r.names[name] = canonicalManifest.ToolID
	}
	r.versions[canonicalManifest.ToolID] = append(r.versions[canonicalManifest.ToolID], canonicalManifest.Version)
	sort.Slice(r.versions[canonicalManifest.ToolID], func(i, j int) bool {
		return semver.Compare("v"+r.versions[canonicalManifest.ToolID][i], "v"+r.versions[canonicalManifest.ToolID][j]) > 0
	})
	return nil
}

func (r *MemoryRegistry) Resolve(ctx context.Context, query Query) (Resolution, error) {
	if err := ctx.Err(); err != nil {
		return Resolution{}, err
	}
	name := token(query.Name)
	version := canonicalVersion(query.Version)
	if name == "" {
		return Resolution{}, ErrInvalidQuery
	}
	r.mu.RLock()
	toolID, known := r.names[name]
	if known && version != "" {
		if manifest, exact := r.byKey[contractKey(toolID, version)]; exact {
			r.mu.RUnlock()
			return Resolution{Manifest: cloneManifest(manifest), AutoPromotable: true}, nil
		}
		for _, registeredVersion := range r.versions[toolID] {
			manifest := r.byKey[contractKey(toolID, registeredVersion)]
			if supports(manifest, version) {
				r.mu.RUnlock()
				return Resolution{Manifest: cloneManifest(manifest), AutoPromotable: true}, nil
			}
		}
	}
	r.mu.RUnlock()
	return opaqueResolution(name, token(query.Version))
}

func supports(manifest Manifest, version string) bool {
	for _, item := range manifest.Compatibility {
		if semver.Compare("v"+version, "v"+item.MinimumInclusive) >= 0 && semver.Compare("v"+version, "v"+item.MaximumExclusive) < 0 {
			return true
		}
	}
	return false
}

func opaqueResolution(name, version string) (Resolution, error) {
	manifest := Manifest{SchemaVersion: "tool-contract.opaque.v1", ToolID: name, Version: version, SideEffect: SideEffectIrreversible, Risk: RiskCritical, Idempotency: IdempotencySpec{Mode: IdempotencyNone}, Retry: RetrySpec{Mode: RetryNever, MaximumAttempts: 1}}
	_, hash, err := canonical.MarshalAndHash(manifest)
	if err != nil {
		return Resolution{}, fmt.Errorf("canonicalize opaque tool: %w", err)
	}
	manifest.ID = "opaque_" + hash
	manifest.ContentHash = hash
	return Resolution{Manifest: manifest, Opaque: true, AutoPromotable: false}, nil
}

func contractKey(toolID, version string) string { return toolID + "\x00" + version }

func cloneManifest(source Manifest) Manifest {
	result := source
	result.Aliases = slices.Clone(source.Aliases)
	result.Inputs = slices.Clone(source.Inputs)
	result.Outputs = slices.Clone(source.Outputs)
	result.Reads = slices.Clone(source.Reads)
	result.Writes = slices.Clone(source.Writes)
	result.Effects = slices.Clone(source.Effects)
	result.Preconditions = slices.Clone(source.Preconditions)
	result.SuccessPredicates = slices.Clone(source.SuccessPredicates)
	result.VerificationMethods = slices.Clone(source.VerificationMethods)
	result.Compatibility = slices.Clone(source.Compatibility)
	if source.Compensation != nil {
		compensation := *source.Compensation
		result.Compensation = &compensation
	}
	return result
}
