package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/sauhard74/mem-jev/internal/archive"
	"github.com/sauhard74/mem-jev/internal/config"
	"github.com/sauhard74/mem-jev/internal/observability"
	"github.com/sauhard74/mem-jev/internal/rebuild"
	storesurreal "github.com/sauhard74/mem-jev/internal/store/surreal"
	"github.com/sauhard74/mem-jev/internal/synthesis"
	memworkflow "github.com/sauhard74/mem-jev/internal/workflow"
	surrealdb "github.com/surrealdb/surrealdb.go"
	temporalclient "go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"
	temporalworkflow "go.temporal.io/sdk/workflow"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheck())
	}
	os.Exit(run())
}

func healthcheck() int {
	address := os.Getenv("MEMJEV_WORKER_HEALTH_ADDR")
	if address == "" {
		address = ":8081"
	}
	if strings.HasPrefix(address, ":") {
		address = "127.0.0.1" + address
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+"/readyz", nil)
	if err != nil {
		return 1
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 1
	}
	closeErr := response.Body.Close()
	if response.StatusCode != http.StatusOK || closeErr != nil {
		return 1
	}
	return 0
}

func run() int {
	logger := observability.NewJSONLogger(os.Stdout)
	configuration, err := config.LoadWorker()
	if err != nil {
		logger.Error("worker configuration rejected", "error", err)
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	shutdownTelemetry, err := observability.Setup(ctx, observability.TelemetryConfig{
		Endpoint: configuration.OTLPEndpoint, ServiceName: "memjev-worker",
		ServiceVersion: configuration.Build.Version, Environment: configuration.Environment,
	})
	if err != nil {
		logger.Error("telemetry startup failed", "error", err)
		return 1
	}
	exitCode := runWorker(ctx, logger, configuration)
	return shutdownAndCode(logger, shutdownTelemetry, configuration.ShutdownTimeout, exitCode)
}

func runWorker(ctx context.Context, logger *slog.Logger, configuration config.Config) int {
	db, err := storesurreal.Open(ctx, storesurreal.Config{
		Endpoint: configuration.Surreal.Endpoint, Namespace: configuration.Surreal.Namespace, Database: configuration.Surreal.Database,
		Username: configuration.Surreal.Username, Password: configuration.Surreal.Password, AuthScope: storesurreal.AuthScope(configuration.Surreal.AuthScope),
	})
	if err != nil {
		logger.Error("database startup failed", "error", err)
		return 1
	}
	defer func() { _ = db.Close(context.Background()) }()
	if err := storesurreal.NewMigrator(db).Apply(ctx); err != nil {
		logger.Error("database migration failed", "error", err)
		return 1
	}
	archiveStore, err := buildArchiveStore(ctx, configuration)
	if err != nil {
		logger.Error("archive startup failed", "error", err)
		return 1
	}
	tlsConfig, err := buildTemporalTLS(configuration.Temporal)
	if err != nil {
		logger.Error("temporal TLS configuration rejected", "error", err)
		return 1
	}
	temporalClient, err := memworkflow.DialTemporal(ctx, memworkflow.TemporalConnectionConfig{
		Address: configuration.Temporal.Address, Namespace: configuration.Temporal.Namespace,
		APIKey: configuration.Temporal.APIKey, TLS: tlsConfig,
	})
	if err != nil {
		logger.Error("temporal startup failed", "error", err)
		return 1
	}
	defer temporalClient.Close()
	processor, err := memworkflow.NewProcessor(
		storesurreal.NewWorkflowSourceLoader(db, rebuild.NewLoader(archiveStore, rebuild.Config{
			MaximumBytes: configuration.Worker.ArchiveMaximumBytes, AcceptedSchemas: []string{"canonical.v1"},
		})),
		storesurreal.NewTenantRegistryProvider(db), storesurreal.NewStageArtifactRepository(db), storesurreal.NewProjectionRepository(db),
		memworkflow.ProcessorConfig{ArtifactRetention: configuration.Worker.StageRetention, Versions: synthesisVersions(), ResidencyRegion: configuration.Archive.Region, LearnedWithRecallConsent: true},
	)
	if err != nil {
		logger.Error("pipeline startup failed", "error", err)
		return 1
	}
	activities, err := memworkflow.NewPipelineActivities(processor)
	if err != nil {
		logger.Error("activity startup failed", "error", err)
		return 1
	}
	worker := temporalworker.New(temporalClient, configuration.Temporal.TaskQueue, temporalworker.Options{WorkerStopTimeout: configuration.ShutdownTimeout})
	worker.RegisterWorkflowWithOptions(memworkflow.SynthesisWorkflow, temporalworkflow.RegisterOptions{Name: memworkflow.DefaultSynthesisWorkflowName})
	worker.RegisterActivity(activities)
	if err := worker.Start(); err != nil {
		logger.Error("temporal worker failed to start", "error", err)
		return 1
	}
	defer worker.Stop()
	starter, err := memworkflow.NewTemporalStarter(temporalClient, memworkflow.TemporalStarterConfig{TaskQueue: configuration.Temporal.TaskQueue})
	if err != nil {
		logger.Error("workflow starter configuration rejected", "error", err)
		return 1
	}
	relay, err := memworkflow.NewRelay(storesurreal.NewOutboxRepository(db), starter, memworkflow.RelayConfig{
		WorkerID: configuration.Worker.ID, BatchSize: configuration.Worker.BatchSize,
		LeaseDuration: configuration.Worker.LeaseDuration, PollInterval: configuration.Worker.PollInterval,
		MaximumAttempts: configuration.Worker.MaximumAttempts, MaximumBackoff: configuration.Worker.MaxBackoff,
	})
	if err != nil {
		logger.Error("outbox relay configuration rejected", "error", err)
		return 1
	}
	workerCtx, cancelWorker := context.WithCancel(ctx)
	defer cancelWorker()
	relayErrors := make(chan error, 1)
	go func() { relayErrors <- relay.Run(workerCtx) }()
	var ready atomic.Bool
	ready.Store(true)
	healthServer := newHealthServer(configuration.Worker.HealthAddress, &ready, db, temporalClient)
	healthErrors := make(chan error, 1)
	go func() { healthErrors <- healthServer.ListenAndServe() }()
	logger.Info("worker ready", "worker_id", configuration.Worker.ID, "task_queue", configuration.Temporal.TaskQueue)
	exitCode := 0
	select {
	case <-ctx.Done():
	case relayErr := <-relayErrors:
		if !errors.Is(relayErr, context.Canceled) {
			logger.Error("outbox relay failed", "error", relayErr)
			exitCode = 1
		}
	case healthErr := <-healthErrors:
		if !errors.Is(healthErr, http.ErrServerClosed) {
			logger.Error("worker health server failed", "error", healthErr)
			exitCode = 1
		}
	}
	ready.Store(false)
	cancelWorker()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), configuration.ShutdownTimeout)
	if err := healthServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("worker health shutdown failed", "error", err)
		exitCode = 1
	}
	cancel()
	return exitCode
}

func buildArchiveStore(ctx context.Context, configuration config.Config) (*archive.S3Store, error) {
	awsConfig, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(configuration.Archive.Region))
	if err != nil {
		return nil, fmt.Errorf("load archive client configuration: %w", err)
	}
	client := s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		if configuration.Archive.Endpoint != "" {
			options.BaseEndpoint = aws.String(configuration.Archive.Endpoint)
			options.UsePathStyle = true
		}
	})
	encryption := types.ServerSideEncryptionAes256
	if configuration.Archive.SSE == "aws:kms" {
		encryption = types.ServerSideEncryptionAwsKms
	}
	return archive.NewS3Store(client, archive.S3Config{
		Bucket: configuration.Archive.Bucket, ServerSideEncryption: encryption, KMSKeyID: configuration.Archive.KMSKeyID,
	})
}

func buildTemporalTLS(configuration config.TemporalConfig) (*tls.Config, error) {
	if configuration.TLSServerName == "" && configuration.TLSCAFile == "" && configuration.TLSCertFile == "" {
		return nil, nil
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: configuration.TLSServerName}
	if configuration.TLSCAFile != "" {
		contents, err := os.ReadFile(configuration.TLSCAFile)
		if err != nil {
			return nil, err
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(contents) {
			return nil, errors.New("temporal CA bundle contains no certificates")
		}
		tlsConfig.RootCAs = roots
	}
	if configuration.TLSCertFile != "" {
		certificate, err := tls.LoadX509KeyPair(configuration.TLSCertFile, configuration.TLSKeyFile)
		if err != nil {
			return nil, err
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}
	return tlsConfig, nil
}

func synthesisVersions() synthesis.Versions {
	return synthesis.Versions{
		Sanitizer: "sanitizer.v1", Registry: "registry.v1", Policy: "outcome-policy.v1",
		GraphBuilder: "causal-graph.v3", Synthesizer: "synthesis.v2",
	}
}

func newHealthServer(address string, ready *atomic.Bool, db *surrealdb.DB, temporalClient temporalclient.Client) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", func(writer http.ResponseWriter, _ *http.Request) { writer.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", func(writer http.ResponseWriter, request *http.Request) {
		if !ready.Load() {
			http.Error(writer, "not ready", http.StatusServiceUnavailable)
			return
		}
		ctx, cancel := context.WithTimeout(request.Context(), 2*time.Second)
		defer cancel()
		if _, err := surrealdb.Query[any](ctx, db, "INFO FOR DB", nil); err != nil {
			http.Error(writer, "database unavailable", http.StatusServiceUnavailable)
			return
		}
		if _, err := temporalClient.CheckHealth(ctx, &temporalclient.CheckHealthRequest{}); err != nil {
			http.Error(writer, "workflow service unavailable", http.StatusServiceUnavailable)
			return
		}
		writer.WriteHeader(http.StatusOK)
	})
	return &http.Server{Addr: address, Handler: mux, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second, IdleTimeout: 30 * time.Second}
}

func shutdownAndCode(logger *slog.Logger, shutdown observability.Shutdown, timeout time.Duration, code int) int {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		logger.Error("telemetry shutdown failed", "error", err)
		return 1
	}
	return code
}
