package config

import (
	"errors"
	"testing"
	"time"
)

func TestLoadRejectsUnknownEnvironmentVariable(t *testing.T) {
	t.Setenv("MEMJEV_UNKNOWN", "secret")
	_, err := Load()
	if !errors.Is(err, ErrUnknownVariable) {
		t.Fatalf("error = %v, want %v", err, ErrUnknownVariable)
	}
}

func TestLoadDevelopmentMemoryConfiguration(t *testing.T) {
	clearKnownEnvironment(t)
	t.Setenv("MEMJEV_ENVIRONMENT", "development")
	t.Setenv("MEMJEV_ADAPTER_MODE", "memory")
	t.Setenv("MEMJEV_LISTEN_ADDR", "127.0.0.1:0")
	t.Setenv("MEMJEV_CREDENTIALS_FILE", "/tmp/credentials.json")
	got, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.ShutdownTimeout != 15*time.Second || got.RequestTimeout != 10*time.Second || got.ListenAddress != "127.0.0.1:0" {
		t.Fatalf("config = %#v", got)
	}
}

func TestLoadProductionRequiresTLSAndDurableAdapters(t *testing.T) {
	clearKnownEnvironment(t)
	t.Setenv("MEMJEV_ENVIRONMENT", "production")
	t.Setenv("MEMJEV_ADAPTER_MODE", "memory")
	t.Setenv("MEMJEV_CREDENTIALS_FILE", "/tmp/credentials.json")
	_, err := Load()
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("error = %v, want %v", err, ErrInvalidConfig)
	}
}

func TestLoadProductionRejectsRootSurrealAuthentication(t *testing.T) {
	setValidDurableEnvironment(t)
	t.Setenv("MEMJEV_SURREAL_AUTH_SCOPE", "root")
	if _, err := Load(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("error = %v, want %v", err, ErrInvalidConfig)
	}
}

func TestLoadProductionAcceptsDatabaseScopedSurrealAuthentication(t *testing.T) {
	setValidDurableEnvironment(t)
	t.Setenv("MEMJEV_SURREAL_AUTH_SCOPE", "database")
	got, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.Surreal.AuthScope != "database" {
		t.Fatalf("auth scope = %q", got.Surreal.AuthScope)
	}
}

func TestLoadWorkerParsesDurableLeaseAndTemporalConfiguration(t *testing.T) {
	setValidDurableEnvironment(t)
	t.Setenv("MEMJEV_SURREAL_AUTH_SCOPE", "database")
	t.Setenv("MEMJEV_TEMPORAL_ADDRESS", "temporal.example.test:7233")
	t.Setenv("MEMJEV_TEMPORAL_NAMESPACE", "production")
	t.Setenv("MEMJEV_TEMPORAL_TASK_QUEUE", "synthesis")
	t.Setenv("MEMJEV_TEMPORAL_TLS_SERVER_NAME", "temporal.example.test")
	t.Setenv("MEMJEV_WORKER_BATCH_SIZE", "64")
	t.Setenv("MEMJEV_WORKER_LEASE_DURATION", "45s")
	t.Setenv("MEMJEV_STAGE_RETENTION", "48h")
	got, err := LoadWorker()
	if err != nil {
		t.Fatal(err)
	}
	if got.Worker.BatchSize != 64 || got.Worker.LeaseDuration != 45*time.Second || got.Worker.StageRetention != 48*time.Hour || got.Temporal.TaskQueue != "synthesis" {
		t.Fatalf("config = %#v", got)
	}
}

func TestLoadWorkerRejectsIncompleteMTLSConfiguration(t *testing.T) {
	setValidDurableEnvironment(t)
	t.Setenv("MEMJEV_SURREAL_AUTH_SCOPE", "database")
	t.Setenv("MEMJEV_TEMPORAL_ADDRESS", "temporal.example.test:7233")
	t.Setenv("MEMJEV_TEMPORAL_NAMESPACE", "production")
	t.Setenv("MEMJEV_TEMPORAL_TASK_QUEUE", "synthesis")
	t.Setenv("MEMJEV_TEMPORAL_TLS_SERVER_NAME", "temporal.example.test")
	t.Setenv("MEMJEV_TEMPORAL_TLS_CERT_FILE", "/run/secrets/client.crt")
	if _, err := LoadWorker(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("error = %v", err)
	}
}

func setValidDurableEnvironment(t *testing.T) {
	t.Helper()
	clearKnownEnvironment(t)
	t.Setenv("MEMJEV_ENVIRONMENT", "production")
	t.Setenv("MEMJEV_ADAPTER_MODE", "surreal-s3")
	t.Setenv("MEMJEV_CREDENTIALS_FILE", "/run/secrets/credentials.json")
	t.Setenv("MEMJEV_SURREAL_ENDPOINT", "wss://database.example.test")
	t.Setenv("MEMJEV_SURREAL_NAMESPACE", "tenant")
	t.Setenv("MEMJEV_SURREAL_DATABASE", "memjev")
	t.Setenv("MEMJEV_SURREAL_USER", "app")
	t.Setenv("MEMJEV_SURREAL_PASSWORD", "secret")
	t.Setenv("MEMJEV_ARCHIVE_BUCKET", "archive")
	t.Setenv("MEMJEV_ARCHIVE_REGION", "us-east-1")
}

func clearKnownEnvironment(t *testing.T) {
	t.Helper()
	for name := range allowedVariables {
		t.Setenv(name, "")
	}
}
