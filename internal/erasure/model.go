package erasure

import (
	"errors"
	"regexp"
	"time"

	"github.com/sauhard74/mem-jev/internal/canonical"
)

var (
	ErrInvalidRequest = errors.New("invalid erasure request")
	ErrConflict       = errors.New("erasure request conflicts with committed receipt")
	tenantPattern     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)
	requestPattern    = regexp.MustCompile(`^erase_[A-Za-z0-9_-]{16,96}$`)
)

type Request struct {
	TenantID     string `json:"tenant_id"`
	RequestID    string `json:"request_id"`
	Confirmation string `json:"confirmation"`
}

type Receipt struct {
	SchemaVersion         string    `json:"schema_version"`
	RequestID             string    `json:"request_id"`
	TenantHash            string    `json:"tenant_hash"`
	ArchiveObjectsDeleted uint64    `json:"archive_objects_deleted"`
	CompletedAt           time.Time `json:"completed_at"`
	ContentHash           string    `json:"-"`
	CanonicalJSON         []byte    `json:"-"`
}

func NewReceipt(requestID, tenantID string, deleted uint64, completedAt time.Time) (Receipt, error) {
	if !requestPattern.MatchString(requestID) || !tenantPattern.MatchString(tenantID) || completedAt.IsZero() {
		return Receipt{}, ErrInvalidRequest
	}
	tenantHash, err := TenantHash(tenantID)
	if err != nil {
		return Receipt{}, err
	}
	receipt := Receipt{SchemaVersion: "tenant-erasure-receipt.v1", RequestID: requestID, TenantHash: tenantHash, ArchiveObjectsDeleted: deleted, CompletedAt: completedAt.UTC()}
	encoded, hash, err := canonical.MarshalAndHash(receipt)
	if err != nil {
		return Receipt{}, err
	}
	receipt.CanonicalJSON, receipt.ContentHash = encoded, hash
	return receipt, nil
}

func TenantHash(tenantID string) (string, error) {
	if !tenantPattern.MatchString(tenantID) {
		return "", ErrInvalidRequest
	}
	_, hash, err := canonical.MarshalAndHash(tenantID)
	return hash, err
}

func ValidateReceipt(receipt Receipt) error {
	if receipt.SchemaVersion != "tenant-erasure-receipt.v1" || !requestPattern.MatchString(receipt.RequestID) || receipt.TenantHash == "" || receipt.CompletedAt.IsZero() || len(receipt.CanonicalJSON) == 0 || receipt.ContentHash == "" {
		return ErrInvalidRequest
	}
	copyOfReceipt := receipt
	copyOfReceipt.ContentHash, copyOfReceipt.CanonicalJSON = "", nil
	encoded, hash, err := canonical.MarshalAndHash(copyOfReceipt)
	if err != nil || hash != receipt.ContentHash || string(encoded) != string(receipt.CanonicalJSON) {
		return ErrInvalidRequest
	}
	return nil
}

func validRequest(request Request) bool {
	return tenantPattern.MatchString(request.TenantID) && requestPattern.MatchString(request.RequestID) && request.Confirmation == "erase:"+request.TenantID+":"+request.RequestID
}

func ValidateRequest(request Request) error {
	if !validRequest(request) {
		return ErrInvalidRequest
	}
	return nil
}
