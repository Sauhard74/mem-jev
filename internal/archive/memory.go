package archive

import (
	"context"
	"sync"
)

type MemoryStore struct {
	mu      sync.RWMutex
	objects map[Key][]byte
	puts    int
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{objects: make(map[Key][]byte)}
}

func (s *MemoryStore) PutCanonical(ctx context.Context, req PutRequest) (Object, error) {
	if err := ctx.Err(); err != nil {
		return Object{}, err
	}
	key, err := validatePutRequest(req)
	if err != nil {
		return Object{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.objects[key]; ok {
		return Object{Key: key, Hash: req.Hash, Size: int64(len(existing)), Reused: true}, nil
	}
	s.objects[key] = append([]byte(nil), req.Body...)
	s.puts++
	return Object{Key: key, Hash: req.Hash, Size: int64(len(req.Body))}, nil
}

func (s *MemoryStore) Get(ctx context.Context, key Key) ([]byte, error) {
	return s.GetBounded(ctx, key, 64<<20)
}

func (s *MemoryStore) GetBounded(ctx context.Context, key Key, maximumBytes int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := hashFromKey(key); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	body, ok := s.objects[key]
	if !ok {
		return nil, ErrNotFound
	}
	if maximumBytes <= 0 || int64(len(body)) > maximumBytes {
		return nil, ErrTooLarge
	}
	return append([]byte(nil), body...), nil
}

func (s *MemoryStore) PutCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.puts
}
