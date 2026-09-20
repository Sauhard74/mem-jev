//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	memjevv1 "github.com/sauhard74/mem-jev/gen/memjev/v1"
	"github.com/sauhard74/mem-jev/gen/memjev/v1/memjevv1connect"
	"github.com/sauhard74/mem-jev/internal/archive"
	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/ingest"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestDurableIdempotentIngest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const secret = "sk-e2e-secret-value"
	requestMessage := &memjevv1.IngestTraceRequest{
		ClientTraceId: "e2e-trace", Harness: "e2e", Task: "use " + secret,
		Events: []*memjevv1.TraceEvent{{
			ClientEventId: "event-1", OccurredAt: timestamppb.New(time.Unix(1, 0).UTC()),
			Kind: memjevv1.EventKind_EVENT_KIND_EXECUTE, ToolName: "shell",
			Fields: []*memjevv1.Field{{Name: "resource_type", StringValue: "file"}, {Name: "command", StringValue: "Authorization: Bearer " + secret}, {Name: "path", StringValue: "./a/../b"}},
			Result: &memjevv1.ToolResult{State: memjevv1.ToolResultState_TOOL_RESULT_STATE_SUCCESS},
		}},
	}
	clean, _, err := ingest.Sanitize(requestMessage, ingest.DefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	wantBatch, err := canonical.Build(domain.TenantID("tenant_e2e"), clean)
	if err != nil {
		t.Fatal(err)
	}
	s3Client := openS3(t, ctx)
	client := memjevv1connect.NewIngestServiceClient(httpClient(), requiredEnv(t, "MEMJEV_E2E_API_URL"))
	first := callIngest(t, ctx, client, requestMessage)
	second := callIngest(t, ctx, client, requestMessage)
	if first.Msg.GetDisposition() != memjevv1.IngestDisposition_INGEST_DISPOSITION_ACCEPTED ||
		second.Msg.GetDisposition() != memjevv1.IngestDisposition_INGEST_DISPOSITION_DUPLICATE ||
		first.Msg.GetReceiptId() != second.Msg.GetReceiptId() || first.Msg.GetTraceId() != string(wantBatch.Trace.ID) {
		t.Fatalf("first=%#v second=%#v", first.Msg, second.Msg)
	}

	db := openSurreal(t, ctx)
	defer func() {
		if err := db.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	persisted := make([]byte, 0)
	for table, want := range map[string]int{"ingest_receipt": 1, "trace_run": 1, "canonical_event": len(wantBatch.Events), "archive_object": 1, "outbox_job": 1} {
		rows, queryErr := surrealdb.Query[[]map[string]any](ctx, db, "SELECT * FROM "+table, nil)
		if queryErr != nil {
			t.Fatal(queryErr)
		}
		if len(*rows) == 0 || len((*rows)[0].Result) != want {
			t.Fatalf("%s count = %d, want %d", table, len((*rows)[0].Result), want)
		}
		encoded, _ := json.Marshal((*rows)[0].Result)
		persisted = append(persisted, encoded...)
	}
	if bytes.Contains(persisted, []byte(secret)) {
		t.Fatal("SurrealDB contains submitted secret")
	}

	objectKey, err := archive.KeyFor("tenant_e2e", wantBatch.SchemaVersion, wantBatch.Hash)
	if err != nil {
		t.Fatal(err)
	}
	object, err := s3Client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(requiredEnv(t, "MEMJEV_E2E_S3_BUCKET")), Key: aws.String(string(objectKey))})
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(object.Body)
	closeErr := object.Body.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("read=%v close=%v", readErr, closeErr)
	}
	if !bytes.Equal(body, wantBatch.CanonicalJSON) || bytes.Contains(body, []byte(secret)) {
		t.Fatal("archive is not the expected redacted canonical content")
	}
	head, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(requiredEnv(t, "MEMJEV_E2E_S3_BUCKET")), Key: aws.String(string(objectKey))})
	if err != nil {
		t.Fatal(err)
	}
	metadata, _ := json.Marshal(head.Metadata)
	if bytes.Contains(metadata, []byte(secret)) || len(head.Metadata) != 2 {
		t.Fatalf("unsafe metadata: %#v", head.Metadata)
	}
	if head.ServerSideEncryption != "AES256" {
		t.Fatalf("server-side encryption = %q, want AES256", head.ServerSideEncryption)
	}
}

func callIngest(t *testing.T, ctx context.Context, client memjevv1connect.IngestServiceClient, message *memjevv1.IngestTraceRequest) *connect.Response[memjevv1.IngestTraceResponse] {
	t.Helper()
	request := connect.NewRequest(message)
	request.Header().Set("Authorization", "Bearer "+requiredEnv(t, "MEMJEV_E2E_TOKEN"))
	request.Header().Set("Idempotency-Key", "e2e-idempotency-key-0001")
	response, err := client.IngestTrace(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func openSurreal(t *testing.T, ctx context.Context) *surrealdb.DB {
	t.Helper()
	db, err := surrealdb.FromEndpointURLString(ctx, requiredEnv(t, "MEMJEV_E2E_SURREAL_URL"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.SignIn(ctx, map[string]any{"user": requiredEnv(t, "SURREAL_USER"), "pass": requiredEnv(t, "SURREAL_PASS")}); err != nil {
		t.Fatal(err)
	}
	if err = db.Use(ctx, "memjev", "memjev"); err != nil {
		t.Fatal(err)
	}
	return db
}

func openS3(t *testing.T, ctx context.Context) *s3.Client {
	t.Helper()
	configuration, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(requiredEnv(t, "MINIO_ROOT_USER"), requiredEnv(t, "MINIO_ROOT_PASSWORD"), "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	return s3.NewFromConfig(configuration, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(requiredEnv(t, "MEMJEV_E2E_S3_URL"))
		options.UsePathStyle = true
	})
}

func requiredEnv(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s is required", name)
	}
	return value
}

func httpClient() *http.Client { return http.DefaultClient }
