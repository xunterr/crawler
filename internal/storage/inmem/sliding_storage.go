package inmem

import (
	"github.com/xunterr/aracno/internal/storage"
)

type SlidingStorage[V any] struct {
	storage storage.Storage[V]
	cache   map[string]V

	windowSize uint
	queue      InMemoryQueue[string]
}

func NewSlidingStorage[V any](storage storage.Storage[V], windowSize uint) *SlidingStorage[V] {
	return &SlidingStorage[V]{
		storage:    storage,
		cache:      make(map[string]V, windowSize),
		windowSize: windowSize,
		queue:      InMemoryQueue[string]{},
	}
}

func (s *SlidingStorage[V]) Get(key string) (V, error) {
	if val, ok := s.cache[key]; ok {
		return val, nil
	} else {
		return s.storage.Get(key)
	}
}

func (s *SlidingStorage[V]) Put(key string, val V) error {
	if len(s.cache) >= int(s.windowSize) {
		if err := s.offload(); err != nil {
			return err
		}
	}

	s.queue.Push(key)
	s.cache[key] = val
	return nil
}

func (s *SlidingStorage[V]) offload() error {
	oldKey, err := s.queue.Pop()
	if err != nil {
		return err
	}

	if oldVal, ok := s.cache[oldKey]; ok {
		err := s.storage.Put(oldKey, oldVal)
		if err != nil {
			return err
		}
		delete(s.cache, oldKey)
	}
	return nil
}

func (s *SlidingStorage[V]) Delete(key string) error {
	if _, ok := s.cache[key]; ok {
		delete(s.cache, key)
		return nil
	} else {
		return s.storage.Delete(key)
	}
}
