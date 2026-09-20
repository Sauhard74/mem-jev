package archive

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/sauhard74/mem-jev/internal/domain"
)

var (
	ErrInvalidRequest  = errors.New("invalid archive request")
	ErrHashMismatch    = errors.New("archive content hash mismatch")
	ErrNotFound        = errors.New("archive object not found")
	ErrArchiveConflict = errors.New("archive object conflicts with canonical content")
	ErrTooLarge        = errors.New("archive object exceeds the configured limit")
)

var (
	schemaVersionPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	sha256Pattern        = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

type Key = domain.ArchiveKey

type Writer interface {
	PutCanonical(context.Context, PutRequest) (Object, error)
}

type Reader interface {
	GetBounded(context.Context, Key, int64) ([]byte, error)
}

type Store interface {
	Writer
	Reader
	Get(context.Context, Key) ([]byte, error)
}

type PutRequest struct {
	TenantID      domain.TenantID
	SchemaVersion string
	Hash          string
	Body          []byte
}

type Object struct {
	Key    Key
	Hash   string
	Size   int64
	Reused bool
}

type OpError struct {
	Op        string
	Retryable bool
	Err       error
}

func (e *OpError) Error() string {
	return fmt.Sprintf("archive %s: %v", e.Op, e.Err)
}

func (e *OpError) Unwrap() error { return e.Err }

func KeyFor(tenantID domain.TenantID, schemaVersion, contentHash string) (Key, error) {
	if strings.TrimSpace(string(tenantID)) == "" ||
		!schemaVersionPattern.MatchString(schemaVersion) ||
		!sha256Pattern.MatchString(contentHash) {
		return "", ErrInvalidRequest
	}
	tenantSum := sha256.Sum256([]byte(tenantID))
	prefix := hex.EncodeToString(tenantSum[:8])
	return Key(fmt.Sprintf("canonical/%s/%s/%s.json", prefix, schemaVersion, contentHash)), nil
}

func validatePutRequest(req PutRequest) (Key, error) {
	key, err := KeyFor(req.TenantID, req.SchemaVersion, req.Hash)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(req.Body)
	if hex.EncodeToString(sum[:]) != req.Hash {
		return "", ErrHashMismatch
	}
	return key, nil
}

func hashFromKey(key Key) (string, error) {
	_, hash, err := metadataFromKey(key)
	return hash, err
}

func metadataFromKey(key Key) (string, string, error) {
	value := string(key)
	if !strings.HasPrefix(value, "canonical/") || !strings.HasSuffix(value, ".json") {
		return "", "", ErrInvalidRequest
	}
	parts := strings.Split(value, "/")
	if len(parts) != 4 || !schemaVersionPattern.MatchString(parts[2]) {
		return "", "", ErrInvalidRequest
	}
	hash := strings.TrimSuffix(parts[3], ".json")
	if !sha256Pattern.MatchString(hash) {
		return "", "", ErrInvalidRequest
	}
	return parts[2], hash, nil
}
