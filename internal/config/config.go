package config

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	ErrUnknownVariable = errors.New("unknown MEMJEV environment variable")
	ErrInvalidConfig   = errors.New("invalid configuration")
	pinnedJevModel     = regexp.MustCompile(`^jev-[0-9]+\.[0-9]+\.[0-9]+$`)
)

var allowedVariables = map[string]struct{}{
	"MEMJEV_LISTEN_ADDR": {}, "MEMJEV_SHUTDOWN_TIMEOUT": {}, "MEMJEV_ENVIRONMENT": {},
	"MEMJEV_REQUEST_TIMEOUT": {},
	"MEMJEV_ADAPTER_MODE":    {}, "MEMJEV_CREDENTIALS_FILE": {}, "MEMJEV_TENANT_METRICS": {},
	"MEMJEV_SURREAL_ENDPOINT": {}, "MEMJEV_SURREAL_NAMESPACE": {}, "MEMJEV_SURREAL_DATABASE": {},
	"MEMJEV_SURREAL_USER": {}, "MEMJEV_SURREAL_PASSWORD": {}, "MEMJEV_SURREAL_AUTH_SCOPE": {},
	"MEMJEV_ARCHIVE_BUCKET": {}, "MEMJEV_ARCHIVE_REGION": {}, "MEMJEV_ARCHIVE_ENDPOINT": {},
	"MEMJEV_ARCHIVE_SSE": {}, "MEMJEV_ARCHIVE_KMS_KEY_ID": {},
	"MEMJEV_TEMPORAL_ADDRESS": {}, "MEMJEV_TEMPORAL_NAMESPACE": {}, "MEMJEV_TEMPORAL_TASK_QUEUE": {},
	"MEMJEV_TEMPORAL_API_KEY": {}, "MEMJEV_TEMPORAL_TLS_SERVER_NAME": {}, "MEMJEV_TEMPORAL_TLS_CA_FILE": {},
	"MEMJEV_TEMPORAL_TLS_CERT_FILE": {}, "MEMJEV_TEMPORAL_TLS_KEY_FILE": {},
	"MEMJEV_WORKER_ID": {}, "MEMJEV_WORKER_HEALTH_ADDR": {}, "MEMJEV_WORKER_POLL_INTERVAL": {},
	"MEMJEV_WORKER_LEASE_DURATION": {}, "MEMJEV_WORKER_BATCH_SIZE": {}, "MEMJEV_WORKER_MAX_ATTEMPTS": {},
	"MEMJEV_WORKER_MAX_BACKOFF": {}, "MEMJEV_STAGE_RETENTION": {}, "MEMJEV_ARCHIVE_MAX_BYTES": {},
	"MEMJEV_OTLP_ENDPOINT": {}, "MEMJEV_BUILD_VERSION": {}, "MEMJEV_BUILD_COMMIT": {}, "MEMJEV_BUILD_AT": {},
	"MEMJEV_RETRIEVAL_POLICY_VERSION": {}, "MEMJEV_RETRIEVAL_QUERY_KEY_ID": {}, "MEMJEV_RETRIEVAL_QUERY_KEY_BASE64": {},
	"MEMJEV_RETRIEVAL_CHANNEL_TIMEOUT": {}, "MEMJEV_RETRIEVAL_RETENTION": {}, "MEMJEV_RETRIEVAL_MINIMUM_SCORE": {}, "MEMJEV_RETRIEVAL_MAX_SELECTIONS": {},
	"MEMJEV_RETRIEVAL_EXACT_MANIFEST_ID": {}, "MEMJEV_RETRIEVAL_LEXICAL_MANIFEST_ID": {}, "MEMJEV_RETRIEVAL_FACET_MANIFEST_ID": {}, "MEMJEV_RETRIEVAL_GRAPH_MANIFEST_ID": {},
	"MEMJEV_RETRIEVAL_VECTOR_CONFIG_FILE": {}, "MEMJEV_EMBEDDING_PROVIDER_ENDPOINT": {}, "MEMJEV_EMBEDDING_PROVIDER_TOKEN_FILE": {},
	"MEMJEV_JEV_ENABLED": {}, "MEMJEV_JEV_MODEL": {}, "MEMJEV_JEV_API_KEY_FILE": {}, "MEMJEV_JEV_TIMEOUT": {},
	"MEMJEV_JEV_MAX_REQUEST_BYTES": {}, "MEMJEV_JEV_MAX_RESPONSE_BYTES": {}, "MEMJEV_JEV_REUSE_DURATION": {}, "MEMJEV_JEV_MINIMUM_REMAINING": {},
	"MEMJEV_JEV_MAX_CONCURRENT": {}, "MEMJEV_JEV_GLOBAL_TOKEN_RATE": {}, "MEMJEV_JEV_GLOBAL_TOKEN_BURST": {},
	"MEMJEV_JEV_TENANT_TOKEN_RATE": {}, "MEMJEV_JEV_TENANT_TOKEN_BURST": {}, "MEMJEV_JEV_MAX_TENANT_LIMITERS": {},
	"MEMJEV_JEV_CIRCUIT_FAILURE_THRESHOLD": {}, "MEMJEV_JEV_CIRCUIT_OPEN_DURATION": {}, "MEMJEV_JEV_MAX_CANDIDATES": {}, "MEMJEV_JEV_AMBIGUITY_SCORE_DISTANCE": {},
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
	Temporal        TemporalConfig
	Worker          WorkerConfig
	Retrieval       RetrievalConfig
	Jev             JevConfig
	OTLPEndpoint    string
	Build           BuildConfig
}

type SurrealConfig struct {
	Endpoint, Namespace, Database, Username, Password, AuthScope string
}

type ArchiveConfig struct {
	Bucket, Region, Endpoint, SSE, KMSKeyID string
}

type TemporalConfig struct {
	Address, Namespace, TaskQueue, APIKey, TLSServerName, TLSCAFile, TLSCertFile, TLSKeyFile string
}

type WorkerConfig struct {
	ID, HealthAddress                       string
	PollInterval, LeaseDuration, MaxBackoff time.Duration
	BatchSize, MaximumAttempts              int
	StageRetention                          time.Duration
	ArchiveMaximumBytes                     int64
}

type RetrievalConfig struct {
	PolicyVersion, QueryKeyID, QueryKeyBase64 string
	ChannelTimeout, Retention                 time.Duration
	MinimumScore                              int64
	MaximumSelections                         uint32
	ExactManifestID, LexicalManifestID        string
	FacetManifestID, GraphManifestID          string
	VectorConfigFile                          string
	EmbeddingProviderEndpoint                 string
	EmbeddingProviderTokenFile                string
}

type JevConfig struct {
	Enabled                                   bool
	Endpoint, AllowedHost, Model              string
	APIKeyFile                                string
	Timeout, ReuseDuration                    time.Duration
	MinimumRemaining, CircuitOpenDuration     time.Duration
	MaximumRequestBytes, MaximumResponseBytes int64
	MaximumConcurrent                         int
	GlobalTokenRate, TenantTokenRate          float64
	GlobalTokenBurst, TenantTokenBurst        int
	MaximumTenantLimiters                     int
	CircuitFailureThreshold                   uint32
	MaximumCandidates                         uint32
	AmbiguityScoreDistance                    int64
}

func (c RetrievalConfig) VectorEnabled() bool {
	return c.VectorConfigFile != "" && c.EmbeddingProviderEndpoint != "" && c.EmbeddingProviderTokenFile != ""
}

type BuildConfig struct {
	Version, Commit, BuiltAt string
}

func Load() (Config, error) {
	config, err := load()
	if err != nil {
		return Config{}, err
	}
	if err := validate(config, true); err != nil {
		return Config{}, err
	}
	return config, nil
}

func LoadWorker() (Config, error) {
	config, err := load()
	if err != nil {
		return Config{}, err
	}
	if err := validate(config, false); err != nil {
		return Config{}, err
	}
	if err := validateWorker(config); err != nil {
		return Config{}, err
	}
	return config, nil
}

func load() (Config, error) {
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
		Temporal: TemporalConfig{
			Address: os.Getenv("MEMJEV_TEMPORAL_ADDRESS"), Namespace: os.Getenv("MEMJEV_TEMPORAL_NAMESPACE"),
			TaskQueue: os.Getenv("MEMJEV_TEMPORAL_TASK_QUEUE"), APIKey: os.Getenv("MEMJEV_TEMPORAL_API_KEY"),
			TLSServerName: os.Getenv("MEMJEV_TEMPORAL_TLS_SERVER_NAME"), TLSCAFile: os.Getenv("MEMJEV_TEMPORAL_TLS_CA_FILE"),
			TLSCertFile: os.Getenv("MEMJEV_TEMPORAL_TLS_CERT_FILE"), TLSKeyFile: os.Getenv("MEMJEV_TEMPORAL_TLS_KEY_FILE"),
		},
		Worker: WorkerConfig{
			ID: valueOr("MEMJEV_WORKER_ID", "worker-1"), HealthAddress: valueOr("MEMJEV_WORKER_HEALTH_ADDR", ":8081"),
			PollInterval: time.Second, LeaseDuration: 30 * time.Second, MaxBackoff: time.Minute,
			BatchSize: 32, MaximumAttempts: 10, StageRetention: 24 * time.Hour, ArchiveMaximumBytes: 16 << 20,
		},
		Retrieval: RetrievalConfig{
			PolicyVersion: valueOr("MEMJEV_RETRIEVAL_POLICY_VERSION", "retrieval-policy.v1"),
			QueryKeyID:    valueOr("MEMJEV_RETRIEVAL_QUERY_KEY_ID", "development-only"), QueryKeyBase64: os.Getenv("MEMJEV_RETRIEVAL_QUERY_KEY_BASE64"),
			ChannelTimeout: 150 * time.Millisecond, Retention: 24 * time.Hour, MaximumSelections: 5,
			ExactManifestID: valueOr("MEMJEV_RETRIEVAL_EXACT_MANIFEST_ID", "idx_exact.v1"), LexicalManifestID: valueOr("MEMJEV_RETRIEVAL_LEXICAL_MANIFEST_ID", "idx_lexical.v1"),
			FacetManifestID: valueOr("MEMJEV_RETRIEVAL_FACET_MANIFEST_ID", "idx_facet.v1"), GraphManifestID: valueOr("MEMJEV_RETRIEVAL_GRAPH_MANIFEST_ID", "idx_graph.v1"),
			VectorConfigFile: os.Getenv("MEMJEV_RETRIEVAL_VECTOR_CONFIG_FILE"), EmbeddingProviderEndpoint: os.Getenv("MEMJEV_EMBEDDING_PROVIDER_ENDPOINT"),
			EmbeddingProviderTokenFile: os.Getenv("MEMJEV_EMBEDDING_PROVIDER_TOKEN_FILE"),
		},
		Jev: JevConfig{
			Endpoint: "https://api.typesafe.ai/v1/systemone", AllowedHost: "api.typesafe.ai", Model: os.Getenv("MEMJEV_JEV_MODEL"), APIKeyFile: os.Getenv("MEMJEV_JEV_API_KEY_FILE"),
			Timeout: 500 * time.Millisecond, ReuseDuration: 7 * 24 * time.Hour, MinimumRemaining: 650 * time.Millisecond, CircuitOpenDuration: 30 * time.Second,
			MaximumRequestBytes: 256 << 10, MaximumResponseBytes: 1 << 20, MaximumConcurrent: 64,
			GlobalTokenRate: 200_000, GlobalTokenBurst: 250_000, TenantTokenRate: 20_000, TenantTokenBurst: 25_000,
			MaximumTenantLimiters: 100_000, CircuitFailureThreshold: 5, MaximumCandidates: 3, AmbiguityScoreDistance: 100_000_000,
		},
		Build: BuildConfig{Version: valueOr("MEMJEV_BUILD_VERSION", "dev"), Commit: valueOr("MEMJEV_BUILD_COMMIT", "unknown"), BuiltAt: valueOr("MEMJEV_BUILD_AT", "unknown")},
	}
	if config.Environment == "development" && config.Retrieval.QueryKeyBase64 == "" {
		config.Retrieval.QueryKeyBase64 = base64.StdEncoding.EncodeToString([]byte("memjev-development-key-32-byte!!"))
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
	if raw := os.Getenv("MEMJEV_JEV_ENABLED"); raw != "" {
		parsed, parseErr := strconv.ParseBool(raw)
		if parseErr != nil {
			return Config{}, fieldError("MEMJEV_JEV_ENABLED")
		}
		config.Jev.Enabled = parsed
	}
	jevDurations := []struct {
		name   string
		target *time.Duration
	}{{"MEMJEV_JEV_TIMEOUT", &config.Jev.Timeout}, {"MEMJEV_JEV_REUSE_DURATION", &config.Jev.ReuseDuration}, {"MEMJEV_JEV_MINIMUM_REMAINING", &config.Jev.MinimumRemaining}, {"MEMJEV_JEV_CIRCUIT_OPEN_DURATION", &config.Jev.CircuitOpenDuration}}
	for _, item := range jevDurations {
		if raw := os.Getenv(item.name); raw != "" {
			parsed, parseErr := time.ParseDuration(raw)
			if parseErr != nil || parsed <= 0 {
				return Config{}, fieldError(item.name)
			}
			*item.target = parsed
		}
	}
	jevInt64s := []struct {
		name   string
		target *int64
	}{{"MEMJEV_JEV_MAX_REQUEST_BYTES", &config.Jev.MaximumRequestBytes}, {"MEMJEV_JEV_MAX_RESPONSE_BYTES", &config.Jev.MaximumResponseBytes}, {"MEMJEV_JEV_AMBIGUITY_SCORE_DISTANCE", &config.Jev.AmbiguityScoreDistance}}
	for _, item := range jevInt64s {
		if raw := os.Getenv(item.name); raw != "" {
			parsed, parseErr := strconv.ParseInt(raw, 10, 64)
			if parseErr != nil || parsed <= 0 {
				return Config{}, fieldError(item.name)
			}
			*item.target = parsed
		}
	}
	jevInts := []struct {
		name   string
		target *int
	}{{"MEMJEV_JEV_MAX_CONCURRENT", &config.Jev.MaximumConcurrent}, {"MEMJEV_JEV_GLOBAL_TOKEN_BURST", &config.Jev.GlobalTokenBurst}, {"MEMJEV_JEV_TENANT_TOKEN_BURST", &config.Jev.TenantTokenBurst}, {"MEMJEV_JEV_MAX_TENANT_LIMITERS", &config.Jev.MaximumTenantLimiters}}
	for _, item := range jevInts {
		if raw := os.Getenv(item.name); raw != "" {
			parsed, parseErr := strconv.Atoi(raw)
			if parseErr != nil || parsed <= 0 {
				return Config{}, fieldError(item.name)
			}
			*item.target = parsed
		}
	}
	jevRates := []struct {
		name   string
		target *float64
	}{{"MEMJEV_JEV_GLOBAL_TOKEN_RATE", &config.Jev.GlobalTokenRate}, {"MEMJEV_JEV_TENANT_TOKEN_RATE", &config.Jev.TenantTokenRate}}
	for _, item := range jevRates {
		if raw := os.Getenv(item.name); raw != "" {
			parsed, parseErr := strconv.ParseFloat(raw, 64)
			if parseErr != nil || parsed <= 0 {
				return Config{}, fieldError(item.name)
			}
			*item.target = parsed
		}
	}
	if raw := os.Getenv("MEMJEV_JEV_CIRCUIT_FAILURE_THRESHOLD"); raw != "" {
		parsed, parseErr := strconv.ParseUint(raw, 10, 32)
		if parseErr != nil || parsed == 0 {
			return Config{}, fieldError("MEMJEV_JEV_CIRCUIT_FAILURE_THRESHOLD")
		}
		config.Jev.CircuitFailureThreshold = uint32(parsed)
	}
	if raw := os.Getenv("MEMJEV_JEV_MAX_CANDIDATES"); raw != "" {
		parsed, parseErr := strconv.ParseUint(raw, 10, 32)
		if parseErr != nil || parsed < 2 {
			return Config{}, fieldError("MEMJEV_JEV_MAX_CANDIDATES")
		}
		config.Jev.MaximumCandidates = uint32(parsed)
	}
	retrievalDurations := []struct {
		name   string
		target *time.Duration
	}{{"MEMJEV_RETRIEVAL_CHANNEL_TIMEOUT", &config.Retrieval.ChannelTimeout}, {"MEMJEV_RETRIEVAL_RETENTION", &config.Retrieval.Retention}}
	for _, item := range retrievalDurations {
		if raw := os.Getenv(item.name); raw != "" {
			parsed, err := time.ParseDuration(raw)
			if err != nil || parsed <= 0 {
				return Config{}, fieldError(item.name)
			}
			*item.target = parsed
		}
	}
	if raw := os.Getenv("MEMJEV_RETRIEVAL_MINIMUM_SCORE"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed < 0 {
			return Config{}, fieldError("MEMJEV_RETRIEVAL_MINIMUM_SCORE")
		}
		config.Retrieval.MinimumScore = parsed
	}
	if raw := os.Getenv("MEMJEV_RETRIEVAL_MAX_SELECTIONS"); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 32)
		if err != nil || parsed == 0 || parsed > 100 {
			return Config{}, fieldError("MEMJEV_RETRIEVAL_MAX_SELECTIONS")
		}
		config.Retrieval.MaximumSelections = uint32(parsed)
	}
	workerDurations := []struct {
		name   string
		target *time.Duration
	}{
		{"MEMJEV_WORKER_POLL_INTERVAL", &config.Worker.PollInterval},
		{"MEMJEV_WORKER_LEASE_DURATION", &config.Worker.LeaseDuration},
		{"MEMJEV_WORKER_MAX_BACKOFF", &config.Worker.MaxBackoff},
		{"MEMJEV_STAGE_RETENTION", &config.Worker.StageRetention},
	}
	for _, item := range workerDurations {
		if raw := os.Getenv(item.name); raw != "" {
			parsed, err := time.ParseDuration(raw)
			if err != nil || parsed <= 0 {
				return Config{}, fieldError(item.name)
			}
			*item.target = parsed
		}
	}
	workerIntegers := []struct {
		name   string
		target *int
	}{
		{"MEMJEV_WORKER_BATCH_SIZE", &config.Worker.BatchSize},
		{"MEMJEV_WORKER_MAX_ATTEMPTS", &config.Worker.MaximumAttempts},
	}
	for _, item := range workerIntegers {
		if raw := os.Getenv(item.name); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil || parsed <= 0 {
				return Config{}, fieldError(item.name)
			}
			*item.target = parsed
		}
	}
	if raw := os.Getenv("MEMJEV_ARCHIVE_MAX_BYTES"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed <= 0 {
			return Config{}, fieldError("MEMJEV_ARCHIVE_MAX_BYTES")
		}
		config.Worker.ArchiveMaximumBytes = parsed
	}
	return config, nil
}

func validate(config Config, requireAPICredentials bool) error {
	if config.ListenAddress == "" || (requireAPICredentials && config.CredentialsFile == "") {
		return fieldError("MEMJEV_LISTEN_ADDR or MEMJEV_CREDENTIALS_FILE")
	}
	if config.Environment != "development" && config.Environment != "staging" && config.Environment != "production" {
		return fieldError("MEMJEV_ENVIRONMENT")
	}
	if config.Retrieval.PolicyVersion == "" || config.Retrieval.QueryKeyID == "" || config.Retrieval.ChannelTimeout <= 0 || config.Retrieval.ChannelTimeout > config.RequestTimeout || config.Retrieval.Retention <= 0 || config.Retrieval.Retention > 30*24*time.Hour || config.Retrieval.MaximumSelections == 0 || config.Retrieval.MaximumSelections > 100 || config.Retrieval.ExactManifestID == "" || config.Retrieval.LexicalManifestID == "" || config.Retrieval.FacetManifestID == "" || config.Retrieval.GraphManifestID == "" {
		return fieldError("retrieval configuration")
	}
	if !config.Jev.Enabled && (config.Jev.Model != "" || config.Jev.APIKeyFile != "") {
		return fieldError("Jev configuration while disabled")
	}
	if config.Jev.Enabled {
		if config.Jev.Endpoint != "https://api.typesafe.ai/v1/systemone" || config.Jev.AllowedHost != "api.typesafe.ai" || !pinnedJevModel.MatchString(config.Jev.Model) || config.Jev.APIKeyFile == "" || config.Jev.Timeout <= 0 || config.Jev.Timeout >= config.RequestTimeout || config.Jev.MinimumRemaining <= config.Jev.Timeout || config.Jev.MinimumRemaining >= config.RequestTimeout || config.Jev.ReuseDuration <= 0 || config.Jev.ReuseDuration > 365*24*time.Hour || config.Jev.MaximumRequestBytes < 1_024 || config.Jev.MaximumRequestBytes > 16<<20 || config.Jev.MaximumResponseBytes < 1_024 || config.Jev.MaximumResponseBytes > 16<<20 || config.Jev.MaximumConcurrent <= 0 || config.Jev.MaximumConcurrent > 10_000 || config.Jev.GlobalTokenRate <= 0 || config.Jev.TenantTokenRate <= 0 || config.Jev.GlobalTokenBurst <= 0 || config.Jev.TenantTokenBurst <= 0 || config.Jev.TenantTokenBurst > config.Jev.GlobalTokenBurst || config.Jev.MaximumTenantLimiters <= 0 || config.Jev.CircuitFailureThreshold == 0 || config.Jev.CircuitOpenDuration <= 0 || config.Jev.MaximumCandidates < 2 || config.Jev.MaximumCandidates > 32 || config.Jev.AmbiguityScoreDistance <= 0 {
			return fieldError("Jev configuration")
		}
	}
	if config.Environment != "development" && config.Retrieval.QueryKeyBase64 == "" {
		return fieldError("MEMJEV_RETRIEVAL_QUERY_KEY_BASE64")
	}
	if config.Retrieval.QueryKeyBase64 != "" {
		key, err := base64.StdEncoding.DecodeString(config.Retrieval.QueryKeyBase64)
		if err != nil || len(key) != 32 {
			return fieldError("MEMJEV_RETRIEVAL_QUERY_KEY_BASE64")
		}
	}
	vectorFields := 0
	for _, value := range []string{config.Retrieval.VectorConfigFile, config.Retrieval.EmbeddingProviderEndpoint, config.Retrieval.EmbeddingProviderTokenFile} {
		if value != "" {
			vectorFields++
		}
	}
	if vectorFields != 0 && vectorFields != 3 {
		return fieldError("vector retrieval configuration")
	}
	if config.Retrieval.VectorEnabled() && config.Environment != "development" && !hasScheme(config.Retrieval.EmbeddingProviderEndpoint, "https") {
		return fieldError("MEMJEV_EMBEDDING_PROVIDER_ENDPOINT")
	}
	if config.AdapterMode == "memory" {
		if config.Environment != "development" || config.Retrieval.VectorEnabled() {
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

func validateWorker(config Config) error {
	if config.AdapterMode != "surreal-s3" || config.Worker.ID == "" || config.Worker.HealthAddress == "" ||
		config.Worker.BatchSize > 100 || config.Worker.MaximumAttempts > 100 || config.Worker.LeaseDuration < time.Second || config.Worker.StageRetention < time.Hour ||
		config.Temporal.Address == "" || config.Temporal.Namespace == "" || config.Temporal.TaskQueue == "" ||
		(config.Temporal.TLSCertFile == "") != (config.Temporal.TLSKeyFile == "") {
		return fieldError("worker configuration")
	}
	if config.Environment != "development" && config.Temporal.TLSServerName == "" {
		return fieldError("MEMJEV_TEMPORAL_TLS_SERVER_NAME")
	}
	if config.Temporal.APIKey != "" && config.Temporal.TLSServerName == "" {
		return fieldError("MEMJEV_TEMPORAL_TLS_SERVER_NAME")
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
