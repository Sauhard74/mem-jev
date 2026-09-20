package retrieval

import (
	"context"
	"strings"
	"testing"
)

func TestAESGCMEnvelopeCipherUsesFreshNonceAndHidesPlaintext(t *testing.T) {
	cipher, err := NewAESGCMEnvelopeCipher("key-2026-09", []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	plaintext := []byte(`{"task":"sensitive release"}`)
	first, err := cipher.Encrypt(context.Background(), "tenant_a", plaintext)
	if err != nil {
		t.Fatal(err)
	}
	second, err := cipher.Encrypt(context.Background(), "tenant_a", plaintext)
	if err != nil {
		t.Fatal(err)
	}
	if first == second || !strings.HasPrefix(first, "enc.v1.key-2026-09.") || strings.Contains(first, "sensitive") {
		t.Fatalf("unsafe envelope: %q %q", first, second)
	}
}

func TestAESGCMEnvelopeCipherRejectsUnpinnedKey(t *testing.T) {
	for _, test := range []struct {
		id  string
		key []byte
	}{{"latest", make([]byte, 32)}, {"bad key", make([]byte, 32)}} {
		if _, err := NewAESGCMEnvelopeCipher(test.id, test.key); err == nil {
			t.Fatalf("accepted key %#v", test)
		}
	}
}
