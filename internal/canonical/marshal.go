package canonical

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"

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

// MarshalAndHashRaw validates that raw contains exactly one JSON value, then
// returns its JCS representation and digest. It is used at trust boundaries
// where callers supply an already-canonical payload together with its hash.
func MarshalAndHashRaw(raw []byte) ([]byte, string, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, "", fmt.Errorf("decode canonical JSON: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, "", fmt.Errorf("decode canonical JSON: trailing value")
		}
		return nil, "", fmt.Errorf("decode canonical JSON: %w", err)
	}
	return MarshalAndHash(value)
}

func deriveID(prefix string, value any) (string, error) {
	_, hash, err := MarshalAndHash(value)
	if err != nil {
		return "", err
	}
	return prefix + "_" + hash, nil
}
