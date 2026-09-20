package security

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
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
	for _, entry := range document.Credentials {
		decodedHash, decodeHashErr := hex.DecodeString(entry.CredentialSHA256)
		if decodeHashErr != nil || len(decodedHash) != 32 || entry.TenantID == "" || entry.Region == "" || !validConsent(entry.Consent) {
			return nil, errors.New("credential file contains an invalid entry")
		}
		hash := strings.ToLower(entry.CredentialSHA256)
		if _, exists := resolver.principals[hash]; exists {
			return nil, errors.New("credential file contains a duplicate hash")
		}
		scopes := make(map[string]struct{}, len(entry.Scopes))
		for _, scope := range entry.Scopes {
			if scope == "" {
				return nil, errors.New("credential file contains an invalid scope")
			}
			scopes[scope] = struct{}{}
		}
		resolver.principals[hash] = Principal{TenantID: entry.TenantID, Region: entry.Region, Scopes: scopes, Consent: entry.Consent, AllowExternalInference: entry.AllowExternalInference}
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
