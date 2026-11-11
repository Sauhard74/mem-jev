package security

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

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
	// MayAssertSubject lets a credential without subject_id name the person
	// on each request with the Memjev-Subject header. Give it only to a
	// trusted service that authenticates its own users.
	MayAssertSubject bool `json:"may_assert_subject,omitempty"`
}

// SubjectSeparator joins a tenant and a person into the storage scope. Tenant
// IDs used with subjects may not contain it, so every scope maps back to
// exactly one tenant and person.
const SubjectSeparator = "__"

// MaximumSubjectLength is the longest person ID accepted, in bytes.
const MaximumSubjectLength = 256

var (
	// Short person IDs without underscores are used in the scope as they are.
	plainSubjectPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9-]{0,62}$`)
	scopedTenantPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)
)

// ScopedTenantID returns the storage scope for a tenant and optional person.
//
// Person IDs of up to 63 letters, digits and dashes are used as they are
// ("tenant__alice"). Any other ID of up to 256 printable characters (for
// example one with underscores, dots or "@") is replaced by "h_" and a hash
// of the ID ("tenant__h_3f2a..."). Plain IDs cannot contain "_", so the two
// forms never collide.
func ScopedTenantID(tenantID domain.TenantID, subjectID string) (domain.TenantID, error) {
	if subjectID == "" {
		return tenantID, nil
	}
	if strings.Contains(string(tenantID), SubjectSeparator) || !validSubject(subjectID) {
		return "", ErrInvalidSubject
	}
	key := subjectID
	if !plainSubjectPattern.MatchString(subjectID) {
		sum := sha256.Sum256([]byte("memjev-subject.v1\x00" + subjectID))
		key = "h_" + hex.EncodeToString(sum[:20])
	}
	scoped := domain.TenantID(string(tenantID) + SubjectSeparator + key)
	if !scopedTenantPattern.MatchString(string(scoped)) {
		return "", ErrInvalidSubject
	}
	return scoped, nil
}

// OrganizationOf returns the tenant a storage scope belongs to: the part
// before SubjectSeparator, or the scope itself when it names no person.
func OrganizationOf(scope domain.TenantID) domain.TenantID {
	organization, _, _ := strings.Cut(string(scope), SubjectSeparator)
	return domain.TenantID(organization)
}

func validSubject(subjectID string) bool {
	if len(subjectID) == 0 || len(subjectID) > MaximumSubjectLength || !utf8.ValidString(subjectID) || strings.TrimSpace(subjectID) != subjectID {
		return false
	}
	for _, character := range subjectID {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
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
	personOrganizations := make(map[domain.TenantID]struct{})
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
		if entry.MayAssertSubject && entry.SubjectID != "" {
			return nil, errors.New("credential file contains an entry with both subject_id and may_assert_subject")
		}
		if entry.MayAssertSubject {
			if _, scopeErr = ScopedTenantID(entry.TenantID, "x"); scopeErr != nil {
				return nil, errors.New("credential file contains an invalid tenant_id for may_assert_subject")
			}
		}
		scopes[scope] = owner{entry.TenantID, entry.SubjectID}
		if entry.SubjectID != "" || entry.MayAssertSubject {
			personOrganizations[entry.TenantID] = struct{}{}
		}
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
		resolver.principals[hash] = Principal{TenantID: scope, OrganizationID: entry.TenantID, SubjectID: entry.SubjectID, MayAssertSubject: entry.MayAssertSubject, Region: entry.Region, Scopes: permissions, Consent: entry.Consent, AllowExternalInference: entry.AllowExternalInference}
	}
	// A plain tenant_id such as "acme__alice" would share storage with a person
	// of tenant "acme", so it is rejected once "acme" has per-person scopes.
	for scope, entry := range scopes {
		if entry.subject != "" {
			continue
		}
		if _, ok := personOrganizations[OrganizationOf(scope)]; ok && OrganizationOf(scope) != scope {
			return nil, errors.New("credential file contains a tenant_id that collides with a tenant and subject_id pair")
		}
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
