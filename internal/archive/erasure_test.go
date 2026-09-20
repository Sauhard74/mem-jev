package archive

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestS3EraserDeletesOnlyHashedTenantPrefix(t *testing.T) {
	client := &fakeEraseAPI{pages: []*s3.ListObjectVersionsOutput{
		{Versions: []types.ObjectVersion{{Key: aws.String("canonical/ea7c68e607dbd8ae/trace.v1/one.json"), VersionId: aws.String("v1")}, {Key: aws.String("canonical/ea7c68e607dbd8ae/outcome.v1/two.json"), VersionId: aws.String("v2")}}, DeleteMarkers: []types.DeleteMarkerEntry{{Key: aws.String("canonical/ea7c68e607dbd8ae/trace.v1/old.json"), VersionId: aws.String("d1")}}},
		{},
	}}
	eraser, err := NewS3Eraser(client, "canonical")
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := eraser.EraseTenant(context.Background(), "tenant_a")
	if err != nil || deleted != 3 || len(client.prefixes) != 2 || client.prefixes[0] != "canonical/ea7c68e607dbd8ae/" || len(client.deleted) != 3 || aws.ToString(client.deleted[0].VersionId) != "v1" || aws.ToString(client.deleted[2].VersionId) != "d1" {
		t.Fatalf("deleted=%d prefixes=%#v keys=%#v err=%v", deleted, client.prefixes, client.deleted, err)
	}
}

type fakeEraseAPI struct {
	pages    []*s3.ListObjectVersionsOutput
	prefixes []string
	deleted  []types.ObjectIdentifier
}

func (f *fakeEraseAPI) ListObjectVersions(_ context.Context, input *s3.ListObjectVersionsInput, _ ...func(*s3.Options)) (*s3.ListObjectVersionsOutput, error) {
	f.prefixes = append(f.prefixes, aws.ToString(input.Prefix))
	page := f.pages[0]
	f.pages = f.pages[1:]
	return page, nil
}

func (f *fakeEraseAPI) DeleteObjects(_ context.Context, input *s3.DeleteObjectsInput, _ ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	f.deleted = append(f.deleted, input.Delete.Objects...)
	return &s3.DeleteObjectsOutput{}, nil
}
