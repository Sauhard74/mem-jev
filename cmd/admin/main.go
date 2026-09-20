package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/sauhard74/mem-jev/internal/archive"
	"github.com/sauhard74/mem-jev/internal/erasure"
	storesurreal "github.com/sauhard74/mem-jev/internal/store/surreal"
)

func main() { os.Exit(run()) }

func surrealAuthScope() storesurreal.AuthScope {
	if scope := os.Getenv("MEMJEV_SURREAL_AUTH_SCOPE"); scope != "" {
		return storesurreal.AuthScope(scope)
	}
	return storesurreal.AuthScopeRoot
}

func journalErasureIntent(request erasure.Request) error {
	if os.Getenv("MEMJEV_ERASURE_LEDGER_REPLAY") == "true" {
		return nil
	}
	directory := os.Getenv("MEMJEV_ERASURE_LEDGER_DIR")
	if directory == "" {
		return errors.New("erasure ledger directory is required")
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
		return errors.New("erasure ledger must be a mode-0700 directory")
	}
	target := directory + string(os.PathSeparator) + request.RequestID + ".json"
	if existing, readErr := os.ReadFile(target); readErr == nil {
		var stored erasure.Request
		if json.Unmarshal(existing, &stored) != nil || stored != request {
			return erasure.ErrConflict
		}
		return nil
	} else if !os.IsNotExist(readErr) {
		return readErr
	}
	temporary, err := os.CreateTemp(directory, ".intent-*")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer func() { _ = os.Remove(temporaryName) }()
	if err = temporary.Chmod(0o600); err == nil {
		err = json.NewEncoder(temporary).Encode(request)
	}
	if err == nil {
		err = temporary.Sync()
	}
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Link(temporaryName, target); err != nil {
		if !os.IsExist(err) {
			return err
		}
		existing, readErr := os.ReadFile(target)
		var stored erasure.Request
		if readErr != nil || json.Unmarshal(existing, &stored) != nil || stored != request {
			return erasure.ErrConflict
		}
		return nil
	}
	directoryHandle, err := os.Open(directory)
	if err != nil {
		return err
	}
	syncErr := directoryHandle.Sync()
	closeErr := directoryHandle.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func run() int {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: memjev-admin erase-tenant|verify-archive-object")
		return 2
	}
	switch os.Args[1] {
	case "erase-tenant":
		return runEraseTenant()
	case "verify-archive-object":
		return runVerifyArchiveObject()
	default:
		fmt.Fprintln(os.Stderr, "usage: memjev-admin erase-tenant|verify-archive-object")
		return 2
	}
}

func runEraseTenant() int {
	var request erasure.Request
	decoder := json.NewDecoder(os.Stdin)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		fmt.Fprintln(os.Stderr, "erasure request rejected")
		return 2
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF || erasure.ValidateRequest(request) != nil {
		fmt.Fprintln(os.Stderr, "erasure request rejected")
		return 2
	}
	if err := journalErasureIntent(request); err != nil {
		fmt.Fprintln(os.Stderr, "erasure ledger unavailable")
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	db, err := storesurreal.Open(ctx, storesurreal.Config{
		Endpoint: os.Getenv("MEMJEV_SURREAL_ENDPOINT"), Namespace: os.Getenv("MEMJEV_SURREAL_NAMESPACE"), Database: os.Getenv("MEMJEV_SURREAL_DATABASE"),
		Username: os.Getenv("MEMJEV_SURREAL_USER"), Password: os.Getenv("MEMJEV_SURREAL_PASSWORD"), AuthScope: surrealAuthScope(),
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "erasure dependency unavailable")
		return 1
	}
	defer func() { _ = db.Close(context.Background()) }()
	if err = storesurreal.NewMigrator(db).Apply(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "erasure schema unavailable")
		return 1
	}
	client, bucket, err := buildS3Client(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "erasure archive unavailable")
		return 1
	}
	archives, err := archive.NewS3Eraser(client, bucket)
	if err != nil {
		fmt.Fprintln(os.Stderr, "erasure archive unavailable")
		return 1
	}
	service, err := erasure.NewService(archives, storesurreal.NewErasureRepository(db), time.Now)
	if err != nil {
		fmt.Fprintln(os.Stderr, "erasure service unavailable")
		return 1
	}
	receipt, err := service.Erase(ctx, request)
	if err != nil {
		fmt.Fprintln(os.Stderr, "tenant erasure failed")
		return 1
	}
	if err = json.NewEncoder(os.Stdout).Encode(map[string]any{"request_id": receipt.RequestID, "tenant_hash": receipt.TenantHash, "completed_at": receipt.CompletedAt, "content_hash": receipt.ContentHash}); err != nil {
		return 1
	}
	return 0
}

func buildS3Client(ctx context.Context) (*s3.Client, string, error) {
	region, bucket := os.Getenv("MEMJEV_ARCHIVE_REGION"), os.Getenv("MEMJEV_ARCHIVE_BUCKET")
	awsConfiguration, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil || bucket == "" || region == "" {
		return nil, "", errors.New("archive configuration unavailable")
	}
	client := s3.NewFromConfig(awsConfiguration, func(options *s3.Options) {
		if endpoint := os.Getenv("MEMJEV_ARCHIVE_ENDPOINT"); endpoint != "" {
			options.BaseEndpoint, options.UsePathStyle = aws.String(endpoint), true
		}
	})
	return client, bucket, nil
}

func runVerifyArchiveObject() int {
	var request struct {
		Key archive.Key `json:"key"`
	}
	decoder := json.NewDecoder(os.Stdin)
	decoder.DisallowUnknownFields()
	if decoder.Decode(&request) != nil || decoder.Decode(&struct{}{}) != io.EOF || request.Key == "" {
		fmt.Fprintln(os.Stderr, "archive verification request rejected")
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	client, bucket, err := buildS3Client(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "archive verification unavailable")
		return 1
	}
	encryption := types.ServerSideEncryptionAes256
	if os.Getenv("MEMJEV_ARCHIVE_SSE") == "aws:kms" {
		encryption = types.ServerSideEncryptionAwsKms
	}
	store, err := archive.NewS3Store(client, archive.S3Config{Bucket: bucket, ServerSideEncryption: encryption, KMSKeyID: os.Getenv("MEMJEV_ARCHIVE_KMS_KEY_ID")})
	if err != nil {
		fmt.Fprintln(os.Stderr, "archive verification unavailable")
		return 1
	}
	if _, err = store.GetBounded(ctx, request.Key, 64<<20); err != nil {
		fmt.Fprintln(os.Stderr, "archive object verification failed")
		return 1
	}
	return 0
}
