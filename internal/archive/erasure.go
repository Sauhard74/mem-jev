package archive

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type S3EraseAPI interface {
	ListObjectVersions(context.Context, *s3.ListObjectVersionsInput, ...func(*s3.Options)) (*s3.ListObjectVersionsOutput, error)
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

var regexpTenant = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)

func (eraser *S3Eraser) EraseTenant(ctx context.Context, tenantID string) (uint64, error) {
	prefix, err := TenantPrefix(tenantID)
	if err != nil {
		return 0, err
	}
	var deleted uint64
	var keyMarker, versionMarker *string
	for {
		output, listErr := eraser.client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(eraser.bucket), Prefix: aws.String(prefix), KeyMarker: keyMarker, VersionIdMarker: versionMarker})
		if listErr != nil {
			return deleted, classifyError("list tenant object versions", listErr)
		}
		if output == nil {
			return deleted, errors.New("archive list tenant object versions: empty response")
		}
		identifiers := make([]types.ObjectIdentifier, 0, len(output.Versions)+len(output.DeleteMarkers))
		for _, object := range output.Versions {
			if key := aws.ToString(object.Key); strings.HasPrefix(key, prefix) {
				identifiers = append(identifiers, types.ObjectIdentifier{Key: aws.String(key), VersionId: object.VersionId})
			}
		}
		for _, marker := range output.DeleteMarkers {
			if key := aws.ToString(marker.Key); strings.HasPrefix(key, prefix) {
				identifiers = append(identifiers, types.ObjectIdentifier{Key: aws.String(key), VersionId: marker.VersionId})
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
			verification, verifyErr := eraser.client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(eraser.bucket), Prefix: aws.String(prefix)})
			if verifyErr != nil {
				return deleted, classifyError("verify tenant object erasure", verifyErr)
			}
			if verification == nil || len(verification.Versions) != 0 || len(verification.DeleteMarkers) != 0 || aws.ToBool(verification.IsTruncated) {
				return deleted, errors.New("archive tenant object versions remain after deletion")
			}
			return deleted, nil
		}
		if output.NextKeyMarker == nil || *output.NextKeyMarker == "" {
			return deleted, errors.New("archive list tenant object versions: missing key marker")
		}
		keyMarker, versionMarker = output.NextKeyMarker, output.NextVersionIdMarker
	}
}
