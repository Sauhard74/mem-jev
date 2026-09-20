package canonical

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/gowebpki/jcs"
)

func MarshalAndHash(value any) ([]byte, string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, "", fmt.Errorf("marshal canonical value: %w", err)
	}
	canonical, err := jcs.Transform(raw)
	if err != nil {
		return nil, "", fmt.Errorf("canonicalize JSON: %w", err)
	}
	sum := sha256.Sum256(canonical)
	return canonical, hex.EncodeToString(sum[:]), nil
}

func deriveID(prefix string, value any) (string, error) {
	_, hash, err := MarshalAndHash(value)
	if err != nil {
		return "", err
	}
	return prefix + "_" + hash, nil
}
