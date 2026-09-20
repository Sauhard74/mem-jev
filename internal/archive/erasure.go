package archive

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type S3EraseAPI interface {
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	DeleteObjects(context.Context, *s3.DeleteObjectsInput, ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
}

type S3Eraser struct {
	client S3EraseAPI
	bucket string
}

func NewS3Eraser(client S3EraseAPI, bucket string) (*S3Eraser, error) {
	if client == nil || strings.TrimSpace(bucket) == "" {
		return nil, ErrInvalidRequest
	}
	return &S3Eraser{client: client, bucket: strings.TrimSpace(bucket)}, nil
}

func TenantPrefix(tenantID string) (string, error) {
	if !regexpTenant.MatchString(tenantID) {
		return "", ErrInvalidRequest
	}
	sum := sha256.Sum256([]byte(tenantID))
	return "canonical/" + hex.EncodeToString(sum[:8]) + "/", nil
}

var regexpTenant = schemaVersionPattern

func (eraser *S3Eraser) EraseTenant(ctx context.Context, tenantID string) (uint64, error) {
	prefix, err := TenantPrefix(tenantID)
	if err != nil {
		return 0, err
	}
	var deleted uint64
	var continuation *string
	for {
		output, listErr := eraser.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(eraser.bucket), Prefix: aws.String(prefix), ContinuationToken: continuation})
		if listErr != nil {
			return deleted, classifyError("list tenant objects", listErr)
		}
		if output == nil {
			return deleted, errors.New("archive list tenant objects: empty response")
		}
		identifiers := make([]types.ObjectIdentifier, 0, len(output.Contents))
		for _, object := range output.Contents {
			if key := aws.ToString(object.Key); strings.HasPrefix(key, prefix) {
				identifiers = append(identifiers, types.ObjectIdentifier{Key: aws.String(key)})
			}
		}
		if len(identifiers) > 0 {
			result, deleteErr := eraser.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(eraser.bucket), Delete: &types.Delete{Objects: identifiers, Quiet: aws.Bool(true)}})
			if deleteErr != nil {
				return deleted, classifyError("delete tenant objects", deleteErr)
			}
			if result == nil || len(result.Errors) > 0 {
				return deleted, errors.New("archive delete tenant objects: incomplete response")
			}
			deleted += uint64(len(identifiers))
		}
		if !aws.ToBool(output.IsTruncated) {
			return deleted, nil
		}
		if output.NextContinuationToken == nil || *output.NextContinuationToken == "" {
			return deleted, errors.New("archive list tenant objects: missing continuation token")
		}
		continuation = output.NextContinuationToken
	}
}
