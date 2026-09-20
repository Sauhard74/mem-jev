package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/sauhard74/mem-jev/internal/embedding"
	storesurreal "github.com/sauhard74/mem-jev/internal/store/surreal"
)

const (
	vectorRuntimeConfigSchema = "retrieval-vector-config.v1"
	maximumRuntimeConfigBytes = 64 << 10
	maximumProviderTokenBytes = 64 << 10
)

type vectorRuntimeConfig struct {
	SchemaVersion string                    `json:"schema_version"`
	Embedding     embedding.ManifestSpec    `json:"embedding"`
	Tuning        storesurreal.VectorTuning `json:"tuning"`
}

func loadVectorRuntimeConfig(path string) (embedding.Manifest, storesurreal.VectorGeneration, error) {
	body, err := readBoundedFile(path, maximumRuntimeConfigBytes)
	if err != nil {
		return embedding.Manifest{}, storesurreal.VectorGeneration{}, fmt.Errorf("read vector runtime configuration: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var source vectorRuntimeConfig
	if err = decoder.Decode(&source); err != nil {
		return embedding.Manifest{}, storesurreal.VectorGeneration{}, fmt.Errorf("decode vector runtime configuration: %w", err)
	}
	if err = decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return embedding.Manifest{}, storesurreal.VectorGeneration{}, errors.New("vector runtime configuration contains trailing content")
	}
	if source.SchemaVersion != vectorRuntimeConfigSchema {
		return embedding.Manifest{}, storesurreal.VectorGeneration{}, errors.New("unsupported vector runtime configuration schema")
	}
	manifest, err := embedding.NewManifest(source.Embedding)
	if err != nil {
		return embedding.Manifest{}, storesurreal.VectorGeneration{}, err
	}
	generation, err := storesurreal.BuildVectorGeneration("runtime", manifest, source.Tuning)
	if err != nil {
		return embedding.Manifest{}, storesurreal.VectorGeneration{}, err
	}
	return manifest, generation, nil
}

func loadProviderToken(path string) (string, error) {
	body, err := readBoundedFile(path, maximumProviderTokenBytes)
	if err != nil {
		return "", fmt.Errorf("read embedding provider token: %w", err)
	}
	token := strings.TrimSpace(string(body))
	if token == "" || strings.ContainsAny(token, "\r\n") {
		return "", errors.New("embedding provider token is invalid")
	}
	return token, nil
}

func readBoundedFile(path string, limit int64) ([]byte, error) {
	if strings.TrimSpace(path) == "" || limit <= 0 {
		return nil, errors.New("file path is not configured")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	body, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, errors.New("file exceeds size limit")
	}
	return body, nil
}
