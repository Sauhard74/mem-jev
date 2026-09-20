package archive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"maps"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/sauhard74/mem-jev/internal/domain"
)

type fakeS3 struct {
	putInput   *s3.PutObjectInput
	headInput  *s3.HeadObjectInput
	getInput   *s3.GetObjectInput
	putErr     error
	headErr    error
	getErr     error
	headOutput *s3.HeadObjectOutput
	getOutput  *s3.GetObjectOutput
	putCalls   int
	headCalls  int
	getCalls   int
}

func (f *fakeS3) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	f.putCalls++
	f.putInput = in
	return &s3.PutObjectOutput{}, f.putErr
}

func (f *fakeS3) HeadObject(_ context.Context, in *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	f.headCalls++
	f.headInput = in
	return f.headOutput, f.headErr
}

func (f *fakeS3) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	f.getCalls++
	f.getInput = in
	return f.getOutput, f.getErr
}

func canonicalRequest() PutRequest {
	body := []byte(`{"task":"safe canonical body"}`)
	sum := sha256.Sum256(body)
	return PutRequest{
		TenantID:      domain.TenantID("tenant-secret-name"),
		SchemaVersion: "v1",
		Hash:          hex.EncodeToString(sum[:]),
		Body:          body,
	}
}

const testKMSKeyARN = "arn:aws:kms:us-east-1:123456789012:key/11111111-2222-3333-4444-555555555555"

func TestS3StoreConstructsConditionalEncryptedPut(t *testing.T) {
	t.Parallel()

	fake := &fakeS3{}
	store, err := NewS3Store(fake, S3Config{
		Bucket:               "canonical-bucket",
		ServerSideEncryption: types.ServerSideEncryptionAwsKms,
		KMSKeyID:             testKMSKeyARN,
	})
	if err != nil {
		t.Fatal(err)
	}
	req := canonicalRequest()
	object, err := store.PutCanonical(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	if fake.putCalls != 1 || fake.putInput == nil {
		t.Fatalf("put calls = %d", fake.putCalls)
	}
	if got := aws.ToString(fake.putInput.Bucket); got != "canonical-bucket" {
		t.Fatalf("bucket = %q", got)
	}
	if got := aws.ToString(fake.putInput.Key); got != string(object.Key) {
		t.Fatalf("key = %q, want %q", got, object.Key)
	}
	if got := aws.ToString(fake.putInput.IfNoneMatch); got != "*" {
		t.Fatalf("If-None-Match = %q", got)
	}
	if fake.putInput.ServerSideEncryption != types.ServerSideEncryptionAwsKms {
		t.Fatalf("encryption = %q", fake.putInput.ServerSideEncryption)
	}
	if got := aws.ToString(fake.putInput.SSEKMSKeyId); got != testKMSKeyARN {
		t.Fatalf("KMS key = %q", got)
	}
	if got := aws.ToString(fake.putInput.ContentType); got != "application/json" {
		t.Fatalf("content type = %q", got)
	}
	wantMetadata := map[string]string{"schema-version": "v1", "content-sha256": req.Hash}
	if !maps.Equal(fake.putInput.Metadata, wantMetadata) {
		t.Fatalf("metadata = %#v, want %#v", fake.putInput.Metadata, wantMetadata)
	}
	for key, value := range fake.putInput.Metadata {
		joined := key + "=" + value
		if strings.Contains(joined, string(req.TenantID)) || strings.Contains(joined, string(req.Body)) {
			t.Fatalf("metadata leaks tenant or content: %q", joined)
		}
	}
	readBody, err := io.ReadAll(fake.putInput.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readBody, req.Body) {
		t.Fatalf("body = %q, want %q", readBody, req.Body)
	}
}

func TestS3StoreVerifiesHashBeforeCallingS3(t *testing.T) {
	t.Parallel()

	fake := &fakeS3{}
	store, err := NewS3Store(fake, S3Config{Bucket: "bucket", ServerSideEncryption: types.ServerSideEncryptionAes256})
	if err != nil {
		t.Fatal(err)
	}
	req := canonicalRequest()
	req.Body = []byte("different")

	_, err = store.PutCanonical(context.Background(), req)
	if !errors.Is(err, ErrHashMismatch) {
		t.Fatalf("error = %v, want %v", err, ErrHashMismatch)
	}
	if fake.putCalls != 0 || fake.headCalls != 0 {
		t.Fatalf("S3 called for invalid content: put=%d head=%d", fake.putCalls, fake.headCalls)
	}
}

func TestS3StoreTreatsVerifiedPreconditionFailureAsReuse(t *testing.T) {
	t.Parallel()

	req := canonicalRequest()
	fake := &fakeS3{
		putErr: &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "exists"},
		headOutput: &s3.HeadObjectOutput{
			ContentLength:        aws.Int64(int64(len(req.Body))),
			Metadata:             map[string]string{"content-sha256": req.Hash, "schema-version": req.SchemaVersion},
			ServerSideEncryption: types.ServerSideEncryptionAes256,
		},
	}
	store, err := NewS3Store(fake, S3Config{Bucket: "bucket", ServerSideEncryption: types.ServerSideEncryptionAes256})
	if err != nil {
		t.Fatal(err)
	}

	object, err := store.PutCanonical(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if !object.Reused || fake.headCalls != 1 {
		t.Fatalf("object=%#v head calls=%d", object, fake.headCalls)
	}
	if aws.ToString(fake.headInput.Key) != string(object.Key) {
		t.Fatalf("head key = %q, want %q", aws.ToString(fake.headInput.Key), object.Key)
	}
}

func TestS3StoreRejectsExistingObjectWithWrongEncryption(t *testing.T) {
	t.Parallel()
	req := canonicalRequest()
	fake := &fakeS3{
		putErr: &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "exists"},
		headOutput: &s3.HeadObjectOutput{
			ContentLength: aws.Int64(int64(len(req.Body))),
			Metadata:      map[string]string{"content-sha256": req.Hash, "schema-version": req.SchemaVersion},
		},
	}
	store, err := NewS3Store(fake, S3Config{Bucket: "bucket", ServerSideEncryption: types.ServerSideEncryptionAes256})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutCanonical(context.Background(), req); !errors.Is(err, ErrArchiveConflict) {
		t.Fatalf("error = %v, want %v", err, ErrArchiveConflict)
	}
}

func TestS3StoreRejectsConflictingExistingObject(t *testing.T) {
	t.Parallel()

	req := canonicalRequest()
	fake := &fakeS3{
		putErr: &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "exists"},
		headOutput: &s3.HeadObjectOutput{
			ContentLength: aws.Int64(int64(len(req.Body))),
			Metadata:      map[string]string{"content-sha256": strings.Repeat("0", 64)},
		},
	}
	store, err := NewS3Store(fake, S3Config{Bucket: "bucket", ServerSideEncryption: types.ServerSideEncryptionAes256})
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.PutCanonical(context.Background(), req)
	if !errors.Is(err, ErrArchiveConflict) {
		t.Fatalf("error = %v, want %v", err, ErrArchiveConflict)
	}
}

func TestS3StoreClassifiesRetryableErrors(t *testing.T) {
	t.Parallel()

	fake := &fakeS3{putErr: &smithy.GenericAPIError{Code: "SlowDown", Message: "retry later"}}
	store, err := NewS3Store(fake, S3Config{Bucket: "bucket", ServerSideEncryption: types.ServerSideEncryptionAes256})
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.PutCanonical(context.Background(), canonicalRequest())
	var opErr *OpError
	if !errors.As(err, &opErr) || !opErr.Retryable {
		t.Fatalf("error = %#v, want retryable OpError", err)
	}
}

func TestS3StoreGetVerifiesDownloadedContent(t *testing.T) {
	t.Parallel()

	req := canonicalRequest()
	key, err := KeyFor(req.TenantID, req.SchemaVersion, req.Hash)
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeS3{getOutput: &s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(req.Body)), Metadata: map[string]string{"content-sha256": req.Hash, "schema-version": req.SchemaVersion},
		ServerSideEncryption: types.ServerSideEncryptionAes256,
	}}
	store, err := NewS3Store(fake, S3Config{Bucket: "bucket", ServerSideEncryption: types.ServerSideEncryptionAes256})
	if err != nil {
		t.Fatal(err)
	}

	got, err := store.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, req.Body) {
		t.Fatalf("body = %q, want %q", got, req.Body)
	}
	if aws.ToString(fake.getInput.Key) != string(key) {
		t.Fatalf("get key = %q, want %q", aws.ToString(fake.getInput.Key), key)
	}

	fake.getOutput = &s3.GetObjectOutput{
		Body: io.NopCloser(strings.NewReader("tampered")), Metadata: map[string]string{"content-sha256": req.Hash, "schema-version": req.SchemaVersion},
		ServerSideEncryption: types.ServerSideEncryptionAes256,
	}
	_, err = store.Get(context.Background(), key)
	if !errors.Is(err, ErrHashMismatch) {
		t.Fatalf("error = %v, want %v", err, ErrHashMismatch)
	}
}

func TestS3StoreGetRejectsWrongEncryptionEnvelope(t *testing.T) {
	t.Parallel()
	req := canonicalRequest()
	key, err := KeyFor(req.TenantID, req.SchemaVersion, req.Hash)
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeS3{getOutput: &s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(req.Body)), Metadata: map[string]string{"content-sha256": req.Hash, "schema-version": req.SchemaVersion},
	}}
	store, err := NewS3Store(fake, S3Config{Bucket: "bucket", ServerSideEncryption: types.ServerSideEncryptionAes256})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetBounded(context.Background(), key, 1<<20); !errors.Is(err, ErrArchiveConflict) {
		t.Fatalf("GetBounded() error = %v; want archive conflict", err)
	}
}

func TestS3StoreGetRejectsWrongKMSKey(t *testing.T) {
	t.Parallel()
	req := canonicalRequest()
	key, err := KeyFor(req.TenantID, req.SchemaVersion, req.Hash)
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeS3{getOutput: &s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(req.Body)), Metadata: map[string]string{"content-sha256": req.Hash, "schema-version": req.SchemaVersion},
		ServerSideEncryption: types.ServerSideEncryptionAwsKms, SSEKMSKeyId: aws.String("alias/wrong"),
	}}
	store, err := NewS3Store(fake, S3Config{Bucket: "bucket", ServerSideEncryption: types.ServerSideEncryptionAwsKms, KMSKeyID: testKMSKeyARN})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetBounded(context.Background(), key, 1<<20); !errors.Is(err, ErrArchiveConflict) {
		t.Fatalf("GetBounded() error = %v; want archive conflict", err)
	}
}

func TestNewS3StoreRejectsUnsafeEncryptionConfiguration(t *testing.T) {
	t.Parallel()

	_, err := NewS3Store(&fakeS3{}, S3Config{Bucket: "bucket"})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("error = %v, want %v", err, ErrInvalidRequest)
	}
	_, err = NewS3Store(&fakeS3{}, S3Config{
		Bucket:               "bucket",
		ServerSideEncryption: types.ServerSideEncryptionAwsKms,
	})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("error = %v, want %v", err, ErrInvalidRequest)
	}
}
