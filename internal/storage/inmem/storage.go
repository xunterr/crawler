package inmem

import (
	"sync"

	"github.com/xunterr/crawler/internal/storage"
)

type InMemoryStorage[V any] struct {
	store   map[string]V
	storeMu sync.Mutex
}

func (s *InMemoryStorage[V]) Get(key string) (V, error) {
	s.storeMu.Lock()
	defer s.storeMu.Unlock()

	val, ok := s.store[key]
	if !ok {
		return *new(V), storage.NoSuchKeyError
	}

	return val, nil
}

func (s *InMemoryStorage[V]) Put(key string, value V) error {
	s.storeMu.Lock()
	defer s.storeMu.Unlock()
	s.store[key] = value
	return nil
}

func (s *InMemoryStorage[V]) Delete(key string) error {
	s.storeMu.Lock()
	delete(s.store, key)
	s.storeMu.Unlock()
	return nil
}
