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
	if got.ShutdownTimeout != 15*time.Second || got.ListenAddress != "127.0.0.1:0" {
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

func clearKnownEnvironment(t *testing.T) {
	t.Helper()
	for name := range allowedVariables {
		t.Setenv(name, "")
	}
}
