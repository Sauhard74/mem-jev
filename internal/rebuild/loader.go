package rebuild

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/sauhard74/mem-jev/internal/archive"
	"github.com/sauhard74/mem-jev/internal/canonical"
	"github.com/sauhard74/mem-jev/internal/domain"
)

var (
	ErrInvalidRequest    = errors.New("invalid rebuild request")
	ErrUnsupportedSchema = errors.New("unsupported canonical schema")
	ErrIdentityMismatch  = errors.New("canonical archive identity mismatch")
)

type Config struct {
	MaximumBytes    int64
	AcceptedSchemas []string
}

type Request struct {
	TenantID      domain.TenantID
	TraceID       domain.TraceID
	SchemaVersion string
	ContentHash   string
	ArchiveKey    archive.Key
}

type Loader struct {
	reader  archive.Reader
	maximum int64
	schemas map[string]struct{}
}

func NewLoader(reader archive.Reader, config Config) *Loader {
	schemas := make(map[string]struct{}, len(config.AcceptedSchemas))
	for _, schema := range config.AcceptedSchemas {
		schemas[schema] = struct{}{}
	}
	return &Loader{reader: reader, maximum: config.MaximumBytes, schemas: schemas}
}

func (l *Loader) Load(ctx context.Context, request Request) (domain.CanonicalBatch, error) {
	if err := ctx.Err(); err != nil {
		return domain.CanonicalBatch{}, err
	}
	if l == nil || l.reader == nil || l.maximum <= 0 || len(l.schemas) == 0 || request.TenantID == "" || request.TraceID == "" {
		return domain.CanonicalBatch{}, ErrInvalidRequest
	}
	if _, accepted := l.schemas[request.SchemaVersion]; !accepted {
		return domain.CanonicalBatch{}, ErrUnsupportedSchema
	}
	expectedKey, err := archive.KeyFor(request.TenantID, request.SchemaVersion, request.ContentHash)
	if err != nil || expectedKey != request.ArchiveKey {
		return domain.CanonicalBatch{}, ErrInvalidRequest
	}
	body, err := l.reader.GetBounded(ctx, request.ArchiveKey, l.maximum)
	if err != nil {
		return domain.CanonicalBatch{}, fmt.Errorf("read canonical archive: %w", err)
	}
	sum := sha256.Sum256(body)
	if hex.EncodeToString(sum[:]) != request.ContentHash {
		return domain.CanonicalBatch{}, fmt.Errorf("%w: content hash", ErrIdentityMismatch)
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var batch domain.CanonicalBatch
	if err := decoder.Decode(&batch); err != nil {
		return domain.CanonicalBatch{}, fmt.Errorf("%w: decode", ErrIdentityMismatch)
	}
	if err := ensureEOF(decoder); err != nil {
		return domain.CanonicalBatch{}, fmt.Errorf("%w: trailing content", ErrIdentityMismatch)
	}
	if _, accepted := l.schemas[batch.SchemaVersion]; !accepted {
		return domain.CanonicalBatch{}, ErrUnsupportedSchema
	}
	if batch.SchemaVersion != request.SchemaVersion || batch.TenantID != request.TenantID || batch.Trace.ID != request.TraceID {
		return domain.CanonicalBatch{}, ErrIdentityMismatch
	}
	batch.Hash = request.ContentHash
	batch.CanonicalJSON = append([]byte(nil), body...)
	if err := canonical.VerifyBatch(batch, body); err != nil {
		return domain.CanonicalBatch{}, fmt.Errorf("%w: %w", ErrIdentityMismatch, err)
	}
	return batch, nil
}

func ensureEOF(decoder *json.Decoder) error {
	var trailing any
	err := decoder.Decode(&trailing)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("additional JSON value")
	}
	return err
}
