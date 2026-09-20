package retrieval

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"regexp"
	"strings"

	"github.com/sauhard74/mem-jev/internal/domain"
)

var keyIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

type AESGCMEnvelopeCipher struct {
	keyID  string
	aead   cipher.AEAD
	random io.Reader
}

func NewAESGCMEnvelopeCipher(keyID string, key []byte) (*AESGCMEnvelopeCipher, error) {
	alias := strings.ToLower(keyID)
	if !keyIDPattern.MatchString(keyID) || len(key) != 32 || alias == "latest" || alias == "current" || alias == "default" {
		return nil, errors.New("invalid retrieval envelope key")
	}
	block, err := aes.NewCipher(append([]byte(nil), key...))
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &AESGCMEnvelopeCipher{keyID: keyID, aead: aead, random: rand.Reader}, nil
}

func (c *AESGCMEnvelopeCipher) Encrypt(ctx context.Context, tenantID domain.TenantID, plaintext []byte) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if c == nil || c.aead == nil || tenantID == "" || len(plaintext) == 0 {
		return "", errors.New("retrieval envelope cipher is not configured")
	}
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := io.ReadFull(c.random, nonce); err != nil {
		return "", err
	}
	ciphertext := c.aead.Seal(nil, nonce, plaintext, []byte(tenantID))
	encoding := base64.RawURLEncoding
	return "enc.v1." + c.keyID + "." + encoding.EncodeToString(nonce) + "." + encoding.EncodeToString(ciphertext), nil
}
