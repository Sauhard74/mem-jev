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

func openErasureLedger() (*os.File, error) {
	if os.Getenv("MEMJEV_ERASURE_LEDGER_REPLAY") == "true" {
		return nil, nil
	}
	path := os.Getenv("MEMJEV_ERASURE_LEDGER_FILE")
	if path == "" {
		return nil, errors.New("erasure ledger path is required")
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		_ = file.Close()
		return nil, errors.New("erasure ledger must be a mode-0600 regular file")
	}
	return file, nil
}

func run() int {
	if len(os.Args) != 2 || os.Args[1] != "erase-tenant" {
		fmt.Fprintln(os.Stderr, "usage: memjev-admin erase-tenant < request.json")
		return 2
	}
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
	ledger, err := openErasureLedger()
	if err != nil {
		fmt.Fprintln(os.Stderr, "erasure ledger unavailable")
		return 1
	}
	if ledger != nil {
		defer func() { _ = ledger.Close() }()
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
	region, bucket := os.Getenv("MEMJEV_ARCHIVE_REGION"), os.Getenv("MEMJEV_ARCHIVE_BUCKET")
	awsConfiguration, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil || bucket == "" {
		fmt.Fprintln(os.Stderr, "erasure archive unavailable")
		return 1
	}
	client := s3.NewFromConfig(awsConfiguration, func(options *s3.Options) {
		if endpoint := os.Getenv("MEMJEV_ARCHIVE_ENDPOINT"); endpoint != "" {
			options.BaseEndpoint, options.UsePathStyle = aws.String(endpoint), true
		}
	})
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
	if ledger != nil {
		if err = json.NewEncoder(ledger).Encode(request); err != nil || ledger.Sync() != nil {
			fmt.Fprintln(os.Stderr, "erasure ledger write failed")
			return 1
		}
	}
	if err = json.NewEncoder(os.Stdout).Encode(map[string]any{"request_id": receipt.RequestID, "tenant_hash": receipt.TenantHash, "completed_at": receipt.CompletedAt, "content_hash": receipt.ContentHash}); err != nil {
		return 1
	}
	return 0
}
