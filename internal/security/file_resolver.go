package security

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/sauhard74/mem-jev/internal/domain"
	"github.com/sauhard74/mem-jev/internal/policy"
)

type credentialFile struct {
	Credentials []credentialEntry `json:"credentials"`
}

type credentialEntry struct {
	CredentialSHA256       string             `json:"credential_sha256"`
	TenantID               domain.TenantID    `json:"tenant_id"`
	Region                 string             `json:"region"`
	Scopes                 []string           `json:"scopes"`
	Consent                policy.ConsentMode `json:"consent"`
	AllowExternalInference bool               `json:"allow_external_inference"`
	// SubjectID optionally names the person this credential belongs to. When
	// set, everything the credential records and recalls is kept separate from
	// other people in the same tenant.
	SubjectID string `json:"subject_id,omitempty"`
}

// SubjectSeparator joins a tenant and a person into the storage scope. Tenant
// IDs used with subject_id may not contain it, and subject IDs cannot contain
// underscores, so every scope maps back to exactly one tenant and person.
const SubjectSeparator = "__"

var (
	subjectPattern      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9-]{0,62}$`)
	scopedTenantPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)
)

// ScopedTenantID returns the storage scope for a tenant and optional person.
func ScopedTenantID(tenantID domain.TenantID, subjectID string) (domain.TenantID, error) {
	if subjectID == "" {
		return tenantID, nil
	}
	if !subjectPattern.MatchString(subjectID) || strings.Contains(string(tenantID), SubjectSeparator) {
		return "", errors.New("invalid subject")
	}
	scoped := domain.TenantID(string(tenantID) + SubjectSeparator + subjectID)
	if !scopedTenantPattern.MatchString(string(scoped)) {
		return "", errors.New("invalid subject")
	}
	return scoped, nil
}

type FileCredentialResolver struct {
	principals map[string]Principal
}

func NewFileCredentialResolver(path string) (*FileCredentialResolver, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open credential file: %w", err)
	}
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var document credentialFile
	decodeErr := decoder.Decode(&document)
	var trailing any
	trailingErr := decoder.Decode(&trailing)
	closeErr := file.Close()
	if decodeErr != nil {
		return nil, fmt.Errorf("decode credential file: %w", decodeErr)
	}
	if !errors.Is(trailingErr, io.EOF) {
		return nil, errors.New("credential file contains trailing content")
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close credential file: %w", closeErr)
	}
	if len(document.Credentials) == 0 {
		return nil, errors.New("credential file contains no credentials")
	}
	resolver := &FileCredentialResolver{principals: make(map[string]Principal, len(document.Credentials))}
	type owner struct {
		tenant  domain.TenantID
		subject string
	}
	scopes := make(map[domain.TenantID]owner, len(document.Credentials))
	for _, entry := range document.Credentials {
		decodedHash, decodeHashErr := hex.DecodeString(entry.CredentialSHA256)
		if decodeHashErr != nil || len(decodedHash) != 32 || entry.TenantID == "" || entry.Region == "" || !validConsent(entry.Consent) {
			return nil, errors.New("credential file contains an invalid entry")
		}
		scope, scopeErr := ScopedTenantID(entry.TenantID, entry.SubjectID)
		if scopeErr != nil {
			return nil, errors.New("credential file contains an invalid subject_id")
		}
		if existing, seen := scopes[scope]; seen && existing != (owner{entry.TenantID, entry.SubjectID}) {
			return nil, errors.New("credential file contains a tenant_id that collides with a tenant and subject_id pair")
		}
		scopes[scope] = owner{entry.TenantID, entry.SubjectID}
		hash := strings.ToLower(entry.CredentialSHA256)
		if _, exists := resolver.principals[hash]; exists {
			return nil, errors.New("credential file contains a duplicate hash")
		}
		permissions := make(map[string]struct{}, len(entry.Scopes))
		for _, permission := range entry.Scopes {
			if permission == "" {
				return nil, errors.New("credential file contains an invalid scope")
			}
			permissions[permission] = struct{}{}
		}
		resolver.principals[hash] = Principal{TenantID: scope, OrganizationID: entry.TenantID, SubjectID: entry.SubjectID, Region: entry.Region, Scopes: permissions, Consent: entry.Consent, AllowExternalInference: entry.AllowExternalInference}
	}
	return resolver, nil
}

func validConsent(mode policy.ConsentMode) bool {
	return mode == policy.Deny || mode == policy.RecallOnly || mode == policy.LearnAndRecall
}

func (r *FileCredentialResolver) ResolveCredentialHash(hash string) (Principal, error) {
	principal, ok := r.principals[hash]
	if !ok {
		return Principal{}, ErrUnknownCredential
	}
	return principal, nil
}
