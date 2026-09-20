package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/sauhard74/mem-jev/internal/api"
	"github.com/sauhard74/mem-jev/internal/archive"
	"github.com/sauhard74/mem-jev/internal/buildinfo"
	"github.com/sauhard74/mem-jev/internal/config"
	"github.com/sauhard74/mem-jev/internal/ingest"
	"github.com/sauhard74/mem-jev/internal/observability"
	"github.com/sauhard74/mem-jev/internal/security"
	"github.com/sauhard74/mem-jev/internal/store"
	storememory "github.com/sauhard74/mem-jev/internal/store/memory"
	storesurreal "github.com/sauhard74/mem-jev/internal/store/surreal"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

func main() {
	os.Exit(run())
}

func run() int {
	logger := observability.NewJSONLogger(os.Stdout)
	configuration, err := config.Load()
	if err != nil {
		logger.Error("configuration rejected", "error", err)
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	shutdownTelemetry, err := observability.Setup(ctx, observability.TelemetryConfig{
		Endpoint: configuration.OTLPEndpoint, ServiceName: "memjev-api",
		ServiceVersion: configuration.Build.Version, Environment: configuration.Environment,
	})
	if err != nil {
		logger.Error("telemetry startup failed", "error", err)
		return 1
	}
	resolver, err := security.NewFileCredentialResolver(configuration.CredentialsFile)
	if err != nil {
		logger.Error("credential configuration rejected", "error", err)
		return shutdownAndCode(logger, shutdownTelemetry, configuration.ShutdownTimeout, 1)
	}

	archives, repository, readiness, closeDatabase, err := buildAdapters(ctx, configuration)
	if err != nil {
		logger.Error("dependency startup failed", "error", err)
		return shutdownAndCode(logger, shutdownTelemetry, configuration.ShutdownTimeout, 1)
	}
	if closeDatabase != nil {
		defer closeDatabase()
	}
	service := ingest.NewService(archives, repository, ingest.DefaultPolicy())
	info := buildinfo.Info{Version: configuration.Build.Version, Commit: configuration.Build.Commit, BuiltAt: configuration.Build.BuiltAt}
	server := &http.Server{
		Addr:              configuration.ListenAddress,
		Handler:           api.NewHandler(api.Dependencies{Ingest: service, Authenticator: security.NewBearerAuthenticator(resolver), BuildInfo: info, Readiness: readiness}),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	serveErrors := make(chan error, 1)
	go func() {
		logger.Info("API listening", "address", configuration.ListenAddress, "version", info.Version)
		serveErrors <- server.ListenAndServe()
	}()

	exitCode := 0
	select {
	case serveErr := <-serveErrors:
		if !errors.Is(serveErr, http.ErrServerClosed) {
			logger.Error("API server failed", "error", serveErr)
			exitCode = 1
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), configuration.ShutdownTimeout)
		if shutdownErr := server.Shutdown(shutdownCtx); shutdownErr != nil {
			logger.Error("forced API shutdown", "error", shutdownErr)
			exitCode = 1
		}
		cancel()
	}
	return shutdownAndCode(logger, shutdownTelemetry, configuration.ShutdownTimeout, exitCode)
}

func buildAdapters(ctx context.Context, configuration config.Config) (archive.Store, store.IngestRepository, func(context.Context) error, func(), error) {
	if configuration.AdapterMode == "memory" {
		return archive.NewMemoryStore(), storememory.NewIngestRepository(), func(context.Context) error { return nil }, nil, nil
	}
	db, err := storesurreal.Open(ctx, storesurreal.Config{
		Endpoint: configuration.Surreal.Endpoint, Namespace: configuration.Surreal.Namespace, Database: configuration.Surreal.Database,
		Username: configuration.Surreal.Username, Password: configuration.Surreal.Password,
	})
	if err != nil {
		return nil, nil, nil, nil, err
	}
	closeDB := func() { _ = db.Close(context.Background()) }
	if err := storesurreal.NewMigrator(db).Apply(ctx); err != nil {
		closeDB()
		return nil, nil, nil, nil, err
	}
	awsConfig, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(configuration.Archive.Region))
	if err != nil {
		closeDB()
		return nil, nil, nil, nil, fmt.Errorf("load archive client configuration: %w", err)
	}
	s3Client := s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		if configuration.Archive.Endpoint != "" {
			options.BaseEndpoint = aws.String(configuration.Archive.Endpoint)
			options.UsePathStyle = true
		}
	})
	encryption := types.ServerSideEncryptionAes256
	if configuration.Archive.SSE == "aws:kms" {
		encryption = types.ServerSideEncryptionAwsKms
	}
	archiveStore, err := archive.NewS3Store(s3Client, archive.S3Config{
		Bucket: configuration.Archive.Bucket, ServerSideEncryption: encryption, KMSKeyID: configuration.Archive.KMSKeyID,
	})
	if err != nil {
		closeDB()
		return nil, nil, nil, nil, err
	}
	readiness := func(ctx context.Context) error {
		if _, err := surrealdb.Query[any](ctx, db, "INFO FOR DB", nil); err != nil {
			return err
		}
		_, err := s3Client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(configuration.Archive.Bucket)})
		return err
	}
	return archiveStore, storesurreal.NewIngestRepository(db), readiness, closeDB, nil
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
