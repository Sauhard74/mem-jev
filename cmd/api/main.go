package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
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
	"github.com/sauhard74/mem-jev/internal/credit"
	"github.com/sauhard74/mem-jev/internal/embedding"
	"github.com/sauhard74/mem-jev/internal/ingest"
	"github.com/sauhard74/mem-jev/internal/observability"
	"github.com/sauhard74/mem-jev/internal/outcome"
	"github.com/sauhard74/mem-jev/internal/planning"
	"github.com/sauhard74/mem-jev/internal/retrieval"
	"github.com/sauhard74/mem-jev/internal/security"
	"github.com/sauhard74/mem-jev/internal/selection"
	"github.com/sauhard74/mem-jev/internal/store"
	storememory "github.com/sauhard74/mem-jev/internal/store/memory"
	storesurreal "github.com/sauhard74/mem-jev/internal/store/surreal"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheck())
	}
	os.Exit(run())
}

func healthcheck() int {
	address := os.Getenv("MEMJEV_LISTEN_ADDR")
	if address == "" {
		address = ":8080"
	}
	if strings.HasPrefix(address, ":") {
		address = "127.0.0.1" + address
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+address+"/memjev.v1.HealthService/Check", bytes.NewBufferString("{}"))
	if err != nil {
		return 1
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 1
	}
	var body struct {
		Status string `json:"status"`
	}
	decodeErr := json.NewDecoder(response.Body).Decode(&body)
	closeErr := response.Body.Close()
	if response.StatusCode != http.StatusOK || decodeErr != nil || closeErr != nil || body.Status != "STATUS_SERVING" {
		return 1
	}
	return 0
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

	archives, ingestRepository, outcomeRepository, selectionRepository, retrievalService, readiness, closeDatabase, err := buildAdapters(ctx, configuration)
	if err != nil {
		logger.Error("dependency startup failed", "error", err)
		return shutdownAndCode(logger, shutdownTelemetry, configuration.ShutdownTimeout, 1)
	}
	if closeDatabase != nil {
		defer closeDatabase()
	}
	ingestService := ingest.NewService(archives, ingestRepository, ingest.DefaultPolicy())
	creditRules, err := credit.NewRuleManifest("outcome-credit.v1", 30*time.Second)
	if err != nil {
		return shutdownAndCode(logger, shutdownTelemetry, configuration.ShutdownTimeout, 1)
	}
	outcomeService := outcome.NewService(outcomeRepository, outcome.DefaultPolicy(), outcome.Attribution{Selections: selectionRepository, Rules: creditRules})
	info := buildinfo.Info{Version: configuration.Build.Version, Commit: configuration.Build.Commit, BuiltAt: configuration.Build.BuiltAt}
	server := &http.Server{
		Addr:              configuration.ListenAddress,
		Handler:           api.NewHandler(api.Dependencies{Ingest: ingestService, Outcome: outcomeService, Retrieval: retrievalService, RetrievalPolicyVersion: configuration.Retrieval.PolicyVersion, Authenticator: security.NewBearerAuthenticator(resolver), BuildInfo: info, Readiness: readiness, Timeout: configuration.RequestTimeout, Logger: logger}),
		ReadHeaderTimeout: configuration.RequestTimeout,
		ReadTimeout:       configuration.RequestTimeout,
		WriteTimeout:      configuration.RequestTimeout + time.Second,
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

func buildAdapters(ctx context.Context, configuration config.Config) (archive.Store, store.IngestRepository, store.OutcomeRepository, selection.Repository, *retrieval.Service, func(context.Context) error, func(), error) {
	if configuration.AdapterMode == "memory" {
		repository := storememory.NewIngestRepository()
		selections, err := storememory.NewSelectionRepository(selection.RetentionPolicy{TTL: configuration.Retrieval.Retention})
		return archive.NewMemoryStore(), repository, repository, selections, nil, func(context.Context) error { return nil }, nil, err
	}
	db, err := storesurreal.Open(ctx, storesurreal.Config{
		Endpoint: configuration.Surreal.Endpoint, Namespace: configuration.Surreal.Namespace, Database: configuration.Surreal.Database,
		Username: configuration.Surreal.Username, Password: configuration.Surreal.Password, AuthScope: storesurreal.AuthScope(configuration.Surreal.AuthScope),
	})
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, err
	}
	closeDB := func() { _ = db.Close(context.Background()) }
	if err := storesurreal.NewMigrator(db).Apply(ctx); err != nil {
		closeDB()
		return nil, nil, nil, nil, nil, nil, nil, err
	}
	awsConfig, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(configuration.Archive.Region))
	if err != nil {
		closeDB()
		return nil, nil, nil, nil, nil, nil, nil, fmt.Errorf("load archive client configuration: %w", err)
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
		return nil, nil, nil, nil, nil, nil, nil, err
	}
	selectionRepository, err := storesurreal.NewSelectionRepository(db, selection.RetentionPolicy{TTL: configuration.Retrieval.Retention})
	if err != nil {
		closeDB()
		return nil, nil, nil, nil, nil, nil, nil, err
	}
	retrievalService, err := buildRetrievalService(db, selectionRepository, configuration.Retrieval, configuration.Environment)
	if err != nil {
		closeDB()
		return nil, nil, nil, nil, nil, nil, nil, err
	}
	readiness := func(ctx context.Context) error {
		if _, err := surrealdb.Query[any](ctx, db, "INFO FOR DB", nil); err != nil {
			return err
		}
		_, err := s3Client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(configuration.Archive.Bucket)})
		return err
	}
	return archiveStore, storesurreal.NewIngestRepository(db), storesurreal.NewOutcomeRepository(db), selectionRepository, retrievalService, readiness, closeDB, nil
}

func buildRetrievalService(db *surrealdb.DB, selectionRepository selection.Repository, configuration config.RetrievalConfig, environment string) (*retrieval.Service, error) {
	key, err := base64.StdEncoding.DecodeString(configuration.QueryKeyBase64)
	if err != nil || len(key) != 32 {
		return nil, fmt.Errorf("retrieval encryption configuration is invalid")
	}
	cipher, err := retrieval.NewAESGCMEnvelopeCipher(configuration.QueryKeyID, key)
	if err != nil {
		return nil, err
	}
	manifestRepository, err := storesurreal.NewRetrievalManifestRepository(db)
	if err != nil {
		return nil, err
	}
	channels, required, err := buildRetrievalChannels(db, configuration, environment)
	if err != nil {
		return nil, err
	}
	projectionRepository := storesurreal.NewProjectionRepository(db)
	planIssuer, err := planning.NewDeterministicIssuer(selectionRepository)
	if err != nil {
		return nil, err
	}
	return retrieval.NewService(storesurreal.NewRetrievalRunRepository(db), projectionRepository, manifestRepository, nil, cipher, retrieval.RandomRunIDSource{}, retrieval.ServiceConfig{
		Channels: channels, RequiredChannels: required, ChannelTimeout: configuration.ChannelTimeout, Retention: configuration.Retention,
		MinimumScore: configuration.MinimumScore, MaximumSelections: configuration.MaximumSelections,
	}, planIssuer)
}

func buildRetrievalChannels(db *surrealdb.DB, configuration config.RetrievalConfig, environment string) ([]retrieval.Channel, []retrieval.ChannelName, error) {
	channelSpecs := []struct {
		name       retrieval.ChannelName
		manifestID string
	}{{retrieval.ChannelExact, configuration.ExactManifestID}, {retrieval.ChannelLexical, configuration.LexicalManifestID}, {retrieval.ChannelFacet, configuration.FacetManifestID}, {retrieval.ChannelGraph, configuration.GraphManifestID}}
	channels := make([]retrieval.Channel, 0, len(channelSpecs)+1)
	required := make([]retrieval.ChannelName, 0, len(channelSpecs))
	for _, spec := range channelSpecs {
		channel, channelErr := storesurreal.NewRetrievalChannel(db, spec.name, spec.manifestID)
		if channelErr != nil {
			return nil, nil, channelErr
		}
		channels = append(channels, channel)
		required = append(required, spec.name)
	}
	if configuration.VectorEnabled() {
		manifest, generation, vectorErr := loadVectorRuntimeConfig(configuration.VectorConfigFile)
		if vectorErr != nil {
			return nil, nil, vectorErr
		}
		token, tokenErr := loadProviderToken(configuration.EmbeddingProviderTokenFile)
		if tokenErr != nil {
			return nil, nil, tokenErr
		}
		provider, providerErr := embedding.NewHTTPProvider(configuration.EmbeddingProviderEndpoint, token, environment == "development")
		if providerErr != nil {
			return nil, nil, providerErr
		}
		embeddingStore, storeErr := storesurreal.NewEmbeddingStore(db)
		if storeErr != nil {
			return nil, nil, storeErr
		}
		embeddingService, serviceErr := embedding.NewService(provider, embeddingStore)
		if serviceErr != nil {
			return nil, nil, serviceErr
		}
		vectorChannel, channelErr := storesurreal.NewVectorChannel(db, manifest, generation, embeddingService)
		if channelErr != nil {
			return nil, nil, channelErr
		}
		channels = append(channels, vectorChannel)
	}
	return channels, required, nil
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
