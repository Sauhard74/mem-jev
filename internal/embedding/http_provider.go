package embedding

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const maximumProviderResponseBytes = 64 << 20

type HTTPProvider struct {
	endpoint string
	token    string
	client   *http.Client
}

type httpProviderRequest struct {
	Provider      string   `json:"provider"`
	Model         string   `json:"model"`
	ModelRevision string   `json:"model_revision"`
	Inputs        []string `json:"inputs"`
}

func NewHTTPProvider(endpoint, token string, allowInsecure bool) (*HTTPProvider, error) {
	parsed, err := url.Parse(strings.TrimSpace(endpoint))
	allowedScheme := err == nil && (parsed.Scheme == "https" || allowInsecure && parsed.Scheme == "http")
	if err != nil || !allowedScheme || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || strings.TrimSpace(token) == "" {
		return nil, errors.New("invalid embedding provider configuration")
	}
	return &HTTPProvider{endpoint: parsed.String(), token: token, client: &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func (p *HTTPProvider) Embed(ctx context.Context, request ProviderRequest) (ProviderResponse, error) {
	if p == nil || p.client == nil || ValidateManifest(request.Manifest) != nil || len(request.Inputs) == 0 || len(request.Inputs) > 256 {
		return ProviderResponse{}, ErrInvalidInput
	}
	payload := httpProviderRequest{Provider: request.Manifest.Provider, Model: request.Manifest.Model, ModelRevision: request.Manifest.ModelRevision, Inputs: make([]string, len(request.Inputs))}
	for index, input := range request.Inputs {
		if err := validateInput(input); err != nil {
			return ProviderResponse{}, err
		}
		payload.Inputs[index] = string(input.CanonicalJSON)
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return ProviderResponse{}, err
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, p.endpoint, bytes.NewReader(body))
	if err != nil {
		return ProviderResponse{}, err
	}
	httpRequest.Header.Set("Authorization", "Bearer "+p.token)
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "application/json")
	response, err := p.client.Do(httpRequest)
	if err != nil {
		return ProviderResponse{}, fmt.Errorf("embedding provider request: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return ProviderResponse{}, fmt.Errorf("embedding provider status %d", response.StatusCode)
	}
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, maximumProviderResponseBytes+1))
	if err != nil {
		return ProviderResponse{}, fmt.Errorf("read embedding provider response: %w", err)
	}
	if len(responseBody) > maximumProviderResponseBytes {
		return ProviderResponse{}, errors.New("embedding provider response exceeds size limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(responseBody))
	decoder.DisallowUnknownFields()
	var result ProviderResponse
	if err = decoder.Decode(&result); err != nil {
		return ProviderResponse{}, fmt.Errorf("decode embedding provider response: %w", err)
	}
	if err = decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return ProviderResponse{}, errors.New("embedding provider returned trailing content")
	}
	if result.Provider != request.Manifest.Provider || result.Model != request.Manifest.Model || result.ModelRevision != request.Manifest.ModelRevision || len(result.Vectors) != len(request.Inputs) {
		return ProviderResponse{}, ErrProviderMismatch
	}
	return result, nil
}

var _ Provider = (*HTTPProvider)(nil)
