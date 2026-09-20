package archive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

type S3API interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

type S3Config struct {
	Bucket               string
	ServerSideEncryption types.ServerSideEncryption
	KMSKeyID             string
}

type S3Store struct {
	client S3API
	config S3Config
}

func NewS3Store(client S3API, config S3Config) (*S3Store, error) {
	if client == nil || strings.TrimSpace(config.Bucket) == "" {
		return nil, ErrInvalidRequest
	}
	switch config.ServerSideEncryption {
	case types.ServerSideEncryptionAes256:
		if config.KMSKeyID != "" {
			return nil, ErrInvalidRequest
		}
	case types.ServerSideEncryptionAwsKms:
		if strings.TrimSpace(config.KMSKeyID) == "" {
			return nil, ErrInvalidRequest
		}
	default:
		return nil, ErrInvalidRequest
	}
	return &S3Store{client: client, config: config}, nil
}

func (s *S3Store) PutCanonical(ctx context.Context, req PutRequest) (Object, error) {
	key, err := validatePutRequest(req)
	if err != nil {
		return Object{}, err
	}
	input := &s3.PutObjectInput{
		Bucket:               aws.String(s.config.Bucket),
		Key:                  aws.String(string(key)),
		Body:                 bytes.NewReader(req.Body),
		ContentLength:        aws.Int64(int64(len(req.Body))),
		ContentType:          aws.String("application/json"),
		IfNoneMatch:          aws.String("*"),
		ServerSideEncryption: s.config.ServerSideEncryption,
		Metadata: map[string]string{
			"schema-version": req.SchemaVersion,
			"content-sha256": req.Hash,
		},
	}
	if s.config.ServerSideEncryption == types.ServerSideEncryptionAwsKms {
		input.SSEKMSKeyId = aws.String(s.config.KMSKeyID)
	}

	if _, err := s.client.PutObject(ctx, input); err != nil {
		if isPreconditionFailure(err) {
			return s.verifyExisting(ctx, key, req)
		}
		return Object{}, classifyError("put", err)
	}
	return Object{Key: key, Hash: req.Hash, Size: int64(len(req.Body))}, nil
}

func (s *S3Store) verifyExisting(ctx context.Context, key Key, req PutRequest) (Object, error) {
	output, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(string(key)),
	})
	if err != nil {
		return Object{}, classifyError("head", err)
	}
	if output == nil || aws.ToInt64(output.ContentLength) != int64(len(req.Body)) ||
		output.Metadata["content-sha256"] != req.Hash ||
		output.Metadata["schema-version"] != req.SchemaVersion {
		return Object{}, fmt.Errorf("%w: key %q failed size or metadata verification", ErrArchiveConflict, key)
	}
	return Object{Key: key, Hash: req.Hash, Size: int64(len(req.Body)), Reused: true}, nil
}

func (s *S3Store) Get(ctx context.Context, key Key) ([]byte, error) {
	wantHash, err := hashFromKey(key)
	if err != nil {
		return nil, err
	}
	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(string(key)),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, ErrNotFound
		}
		return nil, classifyError("get", err)
	}
	if output == nil || output.Body == nil {
		return nil, &OpError{Op: "get", Err: errors.New("empty S3 response")}
	}
	body, err := io.ReadAll(output.Body)
	closeErr := output.Body.Close()
	if err != nil {
		return nil, classifyError("read", err)
	}
	if closeErr != nil {
		return nil, classifyError("close", closeErr)
	}
	sum := sha256.Sum256(body)
	if hex.EncodeToString(sum[:]) != wantHash {
		return nil, ErrHashMismatch
	}
	return body, nil
}

func classifyError(op string, err error) error {
	return &OpError{Op: op, Retryable: isRetryable(err), Err: err}
}

func isPreconditionFailure(err error) bool {
	return apiErrorCode(err) == "PreconditionFailed" || httpStatusCode(err) == 412
}

func isNotFound(err error) bool {
	code := apiErrorCode(err)
	return code == "NotFound" || code == "NoSuchKey" || httpStatusCode(err) == 404
}

func isRetryable(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	switch apiErrorCode(err) {
	case "SlowDown", "Throttling", "ThrottlingException", "RequestTimeout", "RequestTimeoutException", "ServiceUnavailable", "InternalError":
		return true
	default:
		status := httpStatusCode(err)
		return status == 408 || status == 429 || status >= 500
	}
}

func apiErrorCode(err error) string {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode()
	}
	return ""
}

func httpStatusCode(err error) int {
	var statusErr interface{ HTTPStatusCode() int }
	if errors.As(err, &statusErr) {
		return statusErr.HTTPStatusCode()
	}
	return 0
}
