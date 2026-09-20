package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	ErrUnknownVariable = errors.New("unknown MEMJEV environment variable")
	ErrInvalidConfig   = errors.New("invalid configuration")
)

var allowedVariables = map[string]struct{}{
	"MEMJEV_LISTEN_ADDR": {}, "MEMJEV_SHUTDOWN_TIMEOUT": {}, "MEMJEV_ENVIRONMENT": {},
	"MEMJEV_REQUEST_TIMEOUT": {},
	"MEMJEV_ADAPTER_MODE":    {}, "MEMJEV_CREDENTIALS_FILE": {}, "MEMJEV_TENANT_METRICS": {},
	"MEMJEV_SURREAL_ENDPOINT": {}, "MEMJEV_SURREAL_NAMESPACE": {}, "MEMJEV_SURREAL_DATABASE": {},
	"MEMJEV_SURREAL_USER": {}, "MEMJEV_SURREAL_PASSWORD": {}, "MEMJEV_SURREAL_AUTH_SCOPE": {},
	"MEMJEV_ARCHIVE_BUCKET": {}, "MEMJEV_ARCHIVE_REGION": {}, "MEMJEV_ARCHIVE_ENDPOINT": {},
	"MEMJEV_ARCHIVE_SSE": {}, "MEMJEV_ARCHIVE_KMS_KEY_ID": {},
	"MEMJEV_OTLP_ENDPOINT": {}, "MEMJEV_BUILD_VERSION": {}, "MEMJEV_BUILD_COMMIT": {}, "MEMJEV_BUILD_AT": {},
}

type Config struct {
	ListenAddress   string
	ShutdownTimeout time.Duration
	RequestTimeout  time.Duration
	Environment     string
	AdapterMode     string
	CredentialsFile string
	TenantMetrics   bool
	Surreal         SurrealConfig
	Archive         ArchiveConfig
	OTLPEndpoint    string
	Build           BuildConfig
}

type SurrealConfig struct {
	Endpoint, Namespace, Database, Username, Password, AuthScope string
}

type ArchiveConfig struct {
	Bucket, Region, Endpoint, SSE, KMSKeyID string
}

type BuildConfig struct {
	Version, Commit, BuiltAt string
}

func Load() (Config, error) {
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(name, "MEMJEV_") {
			if _, ok := allowedVariables[name]; !ok {
				return Config{}, fmt.Errorf("%w: %s", ErrUnknownVariable, name)
			}
		}
	}
	config := Config{
		ListenAddress:   valueOr("MEMJEV_LISTEN_ADDR", ":8080"),
		ShutdownTimeout: 15 * time.Second,
		RequestTimeout:  10 * time.Second,
		Environment:     valueOr("MEMJEV_ENVIRONMENT", "development"),
		AdapterMode:     valueOr("MEMJEV_ADAPTER_MODE", "surreal-s3"),
		CredentialsFile: os.Getenv("MEMJEV_CREDENTIALS_FILE"),
		OTLPEndpoint:    os.Getenv("MEMJEV_OTLP_ENDPOINT"),
		Surreal: SurrealConfig{
			Endpoint: os.Getenv("MEMJEV_SURREAL_ENDPOINT"), Namespace: os.Getenv("MEMJEV_SURREAL_NAMESPACE"),
			Database: os.Getenv("MEMJEV_SURREAL_DATABASE"), Username: os.Getenv("MEMJEV_SURREAL_USER"), Password: os.Getenv("MEMJEV_SURREAL_PASSWORD"),
			AuthScope: valueOr("MEMJEV_SURREAL_AUTH_SCOPE", "root"),
		},
		Archive: ArchiveConfig{
			Bucket: os.Getenv("MEMJEV_ARCHIVE_BUCKET"), Region: os.Getenv("MEMJEV_ARCHIVE_REGION"), Endpoint: os.Getenv("MEMJEV_ARCHIVE_ENDPOINT"),
			SSE: valueOr("MEMJEV_ARCHIVE_SSE", "AES256"), KMSKeyID: os.Getenv("MEMJEV_ARCHIVE_KMS_KEY_ID"),
		},
		Build: BuildConfig{Version: valueOr("MEMJEV_BUILD_VERSION", "dev"), Commit: valueOr("MEMJEV_BUILD_COMMIT", "unknown"), BuiltAt: valueOr("MEMJEV_BUILD_AT", "unknown")},
	}
	if raw := os.Getenv("MEMJEV_SHUTDOWN_TIMEOUT"); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil || parsed <= 0 {
			return Config{}, fieldError("MEMJEV_SHUTDOWN_TIMEOUT")
		}
		config.ShutdownTimeout = parsed
	}
	if raw := os.Getenv("MEMJEV_REQUEST_TIMEOUT"); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil || parsed <= 0 {
			return Config{}, fieldError("MEMJEV_REQUEST_TIMEOUT")
		}
		config.RequestTimeout = parsed
	}
	if raw := os.Getenv("MEMJEV_TENANT_METRICS"); raw != "" {
		parsed, err := strconv.ParseBool(raw)
		if err != nil {
			return Config{}, fieldError("MEMJEV_TENANT_METRICS")
		}
		config.TenantMetrics = parsed
	}
	if err := validate(config); err != nil {
		return Config{}, err
	}
	return config, nil
}

func validate(config Config) error {
	if config.ListenAddress == "" || config.CredentialsFile == "" {
		return fieldError("MEMJEV_LISTEN_ADDR or MEMJEV_CREDENTIALS_FILE")
	}
	if config.Environment != "development" && config.Environment != "staging" && config.Environment != "production" {
		return fieldError("MEMJEV_ENVIRONMENT")
	}
	if config.AdapterMode == "memory" {
		if config.Environment != "development" {
			return fieldError("MEMJEV_ADAPTER_MODE")
		}
		return nil
	}
	if config.AdapterMode != "surreal-s3" {
		return fieldError("MEMJEV_ADAPTER_MODE")
	}
	if config.Surreal.AuthScope != "root" && config.Surreal.AuthScope != "namespace" && config.Surreal.AuthScope != "database" {
		return fieldError("MEMJEV_SURREAL_AUTH_SCOPE")
	}
	if config.Environment == "production" && config.Surreal.AuthScope == "root" {
		return fieldError("MEMJEV_SURREAL_AUTH_SCOPE")
	}
	required := map[string]string{
		"MEMJEV_SURREAL_ENDPOINT": config.Surreal.Endpoint, "MEMJEV_SURREAL_NAMESPACE": config.Surreal.Namespace,
		"MEMJEV_SURREAL_DATABASE": config.Surreal.Database, "MEMJEV_SURREAL_USER": config.Surreal.Username,
		"MEMJEV_SURREAL_PASSWORD": config.Surreal.Password, "MEMJEV_ARCHIVE_BUCKET": config.Archive.Bucket,
		"MEMJEV_ARCHIVE_REGION": config.Archive.Region,
	}
	for field, value := range required {
		if value == "" {
			return fieldError(field)
		}
	}
	if config.Environment != "development" {
		if !hasScheme(config.Surreal.Endpoint, "wss") ||
			(config.Archive.Endpoint != "" && !hasScheme(config.Archive.Endpoint, "https")) ||
			(config.OTLPEndpoint != "" && !hasScheme(config.OTLPEndpoint, "https")) {
			return fieldError("TLS endpoint")
		}
	}
	if config.Archive.SSE != "AES256" && config.Archive.SSE != "aws:kms" {
		return fieldError("MEMJEV_ARCHIVE_SSE")
	}
	if config.Archive.SSE == "aws:kms" && config.Archive.KMSKeyID == "" {
		return fieldError("MEMJEV_ARCHIVE_KMS_KEY_ID")
	}
	return nil
}

func fieldError(field string) error { return fmt.Errorf("%w: %s", ErrInvalidConfig, field) }
func valueOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
func hasScheme(value, scheme string) bool {
	parsed, err := url.Parse(value)
	return err == nil && parsed.Scheme == scheme
}
