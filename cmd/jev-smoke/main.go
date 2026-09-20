package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/sauhard74/mem-jev/internal/jev"
)

const (
	typeSafeEndpoint = "https://api.typesafe.ai/v1/systemone"
	typeSafeHost     = "api.typesafe.ai"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout)) }

func run(arguments []string, output io.Writer) int {
	flags := flag.NewFlagSet("jev-smoke", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	model := flags.String("model", "", "exact pinned Jev model")
	keyFile := flags.String("key-file", "", "path to mounted TypeSafe API key")
	if flags.Parse(arguments) != nil || flags.NArg() != 0 || *model == "" || *keyFile == "" {
		_, _ = fmt.Fprintln(output, "configuration rejected")
		return 2
	}
	rubric, err := jev.DefaultRubricV1(*model)
	if err != nil {
		_, _ = fmt.Fprintln(output, "configuration rejected")
		return 2
	}
	secret, err := jev.NewFileSecret(*keyFile, 64<<10)
	if err != nil {
		_, _ = fmt.Fprintln(output, "secret unavailable")
		return 2
	}
	if _, err = secret.Token(); err != nil {
		_, _ = fmt.Fprintln(output, "secret unavailable")
		return 2
	}
	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		Proxy: nil, DialContext: dialer.DialContext, ForceAttemptHTTP2: true,
		MaxIdleConns: 2, MaxIdleConnsPerHost: 2, MaxConnsPerHost: 2, IdleConnTimeout: 30 * time.Second,
		TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 10 * time.Second,
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
	}
	defer transport.CloseIdleConnections()
	client, err := jev.NewClient(jev.ClientConfig{
		Endpoint: typeSafeEndpoint, AllowedHost: typeSafeHost, Model: *model, Rubric: rubric,
		Secret: secret, HTTPClient: &http.Client{Transport: transport}, Timeout: 15 * time.Second,
		MaximumRequestBytes: 256 << 10, MaximumResponseBytes: 1 << 20,
	})
	if err != nil {
		_, _ = fmt.Fprintln(output, "configuration rejected")
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	evaluation, err := client.Evaluate(ctx, map[string]any{
		"request":   map[string]any{"task": "Return the health status of a service without changing it."},
		"procedure": map[string]any{"task_text": "Call the read-only health endpoint and return its status."},
	})
	if err != nil {
		_, _ = fmt.Fprintf(output, "provider smoke failed: %s\n", safeProviderCode(err))
		return 1
	}
	features, err := evaluation.Judgment.Features()
	if err != nil {
		_, _ = fmt.Fprintln(output, "provider smoke failed: invalid_response")
		return 1
	}
	result := struct {
		Status       string        `json:"status"`
		Model        string        `json:"model"`
		InputTokens  int64         `json:"input_tokens"`
		OutputTokens int64         `json:"output_tokens"`
		Features     []jev.Feature `json:"features"`
	}{Status: "ok", Model: evaluation.Model, InputTokens: evaluation.Usage.InputTokens, OutputTokens: evaluation.Usage.OutputTokens, Features: features}
	if json.NewEncoder(output).Encode(result) != nil {
		return 1
	}
	return 0
}

func safeProviderCode(err error) string {
	var providerError *jev.ProviderError
	if errors.As(err, &providerError) {
		return providerError.Code
	}
	return "unknown"
}
