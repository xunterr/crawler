package inmem

import "github.com/xunterr/aracno/internal/storage"

type InMemoryQueue[T any] []T

func NewQueue[T any]() *InMemoryQueue[T] {
	return &InMemoryQueue[T]{}
}

func (q *InMemoryQueue[T]) Push(el T) error {
	*q = append(*q, el)
	return nil
}

func (q *InMemoryQueue[T]) Peek() (T, error) {
	if len(*q) == 0 {
		return *new(T), storage.NoNextItem
	}
	return (*q)[0], nil
}

func (q *InMemoryQueue[T]) Pop() (T, error) {
	if len(*q) == 0 {
		return *new(T), storage.NoNextItem
	}

	val := (*q)[0]

	*q = (*q)[1:len(*q)]

	return val, nil
}

func (q *InMemoryQueue[T]) Len() int {
	return len(*q)
}
