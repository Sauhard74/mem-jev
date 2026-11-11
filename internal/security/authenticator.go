package security

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
)

var (
	ErrMissingCredential   = errors.New("credential is missing")
	ErrMalformedCredential = errors.New("credential is malformed")
	ErrUnknownCredential   = errors.New("credential is unknown")
	ErrTenantOverride      = errors.New("tenant override header is forbidden")
	ErrInvalidPrincipal    = errors.New("credential resolved to an invalid principal")
	ErrInvalidSubject      = errors.New("subject is invalid")
	ErrSubjectNotAllowed   = errors.New("credential may not name a subject")
)

// SubjectHeader names the person a request is for. Only credentials with
// may_assert_subject may send it; the request is then stored and recalled in
// that person's scope (see ScopedTenantID).
const SubjectHeader = "Memjev-Subject"

type Authenticator interface {
	Authenticate(http.Header) (Principal, error)
}

type CredentialResolver interface {
	ResolveCredentialHash(string) (Principal, error)
}

type BearerAuthenticator struct {
	resolver CredentialResolver
}

func NewBearerAuthenticator(resolver CredentialResolver) *BearerAuthenticator {
	return &BearerAuthenticator{resolver: resolver}
}

func (a *BearerAuthenticator) Authenticate(header http.Header) (Principal, error) {
	if len(header.Values("X-Tenant-ID")) > 0 {
		return Principal{}, ErrTenantOverride
	}
	value := header.Get("Authorization")
	if value == "" {
		return Principal{}, ErrMissingCredential
	}
	scheme, token, ok := strings.Cut(value, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || !validOpaqueToken(token) {
		return Principal{}, ErrMalformedCredential
	}
	sum := sha256.Sum256([]byte(token))
	principal, err := a.resolver.ResolveCredentialHash(hex.EncodeToString(sum[:]))
	if err != nil {
		return Principal{}, err
	}
	if principal.TenantID == "" {
		return Principal{}, ErrInvalidPrincipal
	}
	subjects := header.Values(SubjectHeader)
	if len(subjects) == 0 {
		return principal, nil
	}
	if !principal.MayAssertSubject || principal.SubjectID != "" {
		return Principal{}, ErrSubjectNotAllowed
	}
	if len(subjects) != 1 || subjects[0] == "" {
		return Principal{}, ErrInvalidSubject
	}
	scope, err := ScopedTenantID(principal.OrganizationID, subjects[0])
	if err != nil {
		return Principal{}, ErrInvalidSubject
	}
	principal.TenantID, principal.SubjectID = scope, subjects[0]
	return principal, nil
}

func validOpaqueToken(token string) bool {
	if len(token) < 16 || len(token) > 512 {
		return false
	}
	for index := 0; index < len(token); index++ {
		if token[index] < 0x21 || token[index] > 0x7e {
			return false
		}
	}
	return true
}
