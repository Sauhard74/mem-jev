package embedding

import "context"

// Provider receives only an immutable, fully-qualified manifest. Implementations
// must not resolve aliases such as "latest" or silently substitute a model.
type Provider interface {
	Embed(context.Context, ProviderRequest) (ProviderResponse, error)
}

type ProviderRequest struct {
	Manifest Manifest
	Inputs   []Input
}

type ProviderResponse struct {
	Provider      string      `json:"provider"`
	Model         string      `json:"model"`
	ModelRevision string      `json:"model_revision"`
	Vectors       [][]float64 `json:"vectors"`
}

// Store is an immutable, content-addressed embedding store. Put must converge
// concurrent identical writes and reject a conflicting value for the same key.
type Store interface {
	Get(ctx context.Context, tenantID, manifestID, inputHash string) (Embedding, bool, error)
	Put(ctx context.Context, value Embedding) (Embedding, error)
}
