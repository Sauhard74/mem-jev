package archive

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/sauhard74/mem-jev/internal/domain"
)

func TestPutCanonicalIsContentAddressedAndIdempotent(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	body := []byte(`{"a":1}`)
	sum := sha256.Sum256(body)
	req := PutRequest{
		TenantID:      domain.TenantID("tenant_a"),
		SchemaVersion: "v1",
		Hash:          hex.EncodeToString(sum[:]),
		Body:          body,
	}

	first, err := store.PutCanonical(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.PutCanonical(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	if first.Key != second.Key || store.PutCount() != 1 {
		t.Fatalf("first=%#v second=%#v puts=%d", first, second, store.PutCount())
	}
	if first.Reused {
		t.Fatal("first write reported as reused")
	}
	if !second.Reused {
		t.Fatal("second write did not report reuse")
	}
	if strings.Contains(string(first.Key), string(req.TenantID)) {
		t.Fatalf("archive key leaks raw tenant ID: %q", first.Key)
	}

	got, err := store.Get(context.Background(), first.Key)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(body) {
		t.Fatalf("got %q, want %q", got, body)
	}
	got[0] = 'x'
	again, err := store.Get(context.Background(), first.Key)
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != string(body) {
		t.Fatal("stored body was mutated by caller")
	}
}

func TestPutCanonicalRejectsInvalidInputWithoutStoring(t *testing.T) {
	t.Parallel()

	validBody := []byte(`{"a":1}`)
	validSum := sha256.Sum256(validBody)
	validHash := hex.EncodeToString(validSum[:])
	tests := []struct {
		name string
		req  PutRequest
		err  error
	}{
		{name: "missing tenant", req: PutRequest{SchemaVersion: "v1", Hash: validHash, Body: validBody}, err: ErrInvalidRequest},
		{name: "unsafe schema", req: PutRequest{TenantID: "tenant", SchemaVersion: "../v1", Hash: validHash, Body: validBody}, err: ErrInvalidRequest},
		{name: "invalid hash", req: PutRequest{TenantID: "tenant", SchemaVersion: "v1", Hash: "not-a-hash", Body: validBody}, err: ErrInvalidRequest},
		{name: "hash mismatch", req: PutRequest{TenantID: "tenant", SchemaVersion: "v1", Hash: strings.Repeat("0", 64), Body: validBody}, err: ErrHashMismatch},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewMemoryStore()
			_, err := store.PutCanonical(context.Background(), tt.req)
			if !errors.Is(err, tt.err) {
				t.Fatalf("error = %v, want %v", err, tt.err)
			}
			if store.PutCount() != 0 {
				t.Fatalf("invalid request stored %d objects", store.PutCount())
			}
		})
	}
}

func TestKeyForIsStableAndTenantScoped(t *testing.T) {
	t.Parallel()

	body := []byte(`{"a":1}`)
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	one, err := KeyFor(domain.TenantID("tenant_a"), "v1", hash)
	if err != nil {
		t.Fatal(err)
	}
	two, err := KeyFor(domain.TenantID("tenant_b"), "v1", hash)
	if err != nil {
		t.Fatal(err)
	}
	again, err := KeyFor(domain.TenantID("tenant_a"), "v1", hash)
	if err != nil {
		t.Fatal(err)
	}

	if one == two {
		t.Fatal("different tenants received the same key")
	}
	if one != again {
		t.Fatalf("unstable key: %q != %q", one, again)
	}
	if !strings.HasSuffix(string(one), "/v1/"+hash+".json") {
		t.Fatalf("unexpected key format: %q", one)
	}
}
