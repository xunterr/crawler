package queues

import (
	"log"

	"github.com/beeker1121/goque"
)

type Queue[T any] interface {
	Push(T)
	Peek(*T) bool
	Pop(*T) bool
	Len() int
}

type InMemoryQueue[T any] []T

func NewQueue[T any]() *InMemoryQueue[T] {
	return &InMemoryQueue[T]{}
}

func (q *InMemoryQueue[T]) Push(el T) {
	*q = append(*q, el)
}

func (q *InMemoryQueue[T]) Peek(to *T) bool {
	if len(*q) == 0 {
		return false
	}
	*to = (*q)[0]
	return true
}

func (q *InMemoryQueue[T]) Pop(to *T) bool {
	if len(*q) == 0 {
		return false
	}

	*to = (*q)[0]

	*q = (*q)[1:len(*q)]

	return true
}

func (q *InMemoryQueue[T]) Len() int {
	return len(*q)
}

type PersistentQueue[T any] struct {
	queue *goque.Queue
}

func NewPersistentQueue[T any](from string) (*PersistentQueue[T], error) {
	queue, err := goque.OpenQueue(from)
	return &PersistentQueue[T]{queue: queue}, err
}

func (q *PersistentQueue[T]) Push(el T) {
	_, err := q.queue.EnqueueObject(el)

	if err != nil {
		log.Printf("Failed to push to queue: %s", err.Error())
	}
}

func (q *PersistentQueue[T]) Peek(to *T) bool {
	item, err := q.queue.Peek()
	if err != nil {
		return false
	}

	return q.toObject(item, to)
}

func (q *PersistentQueue[T]) Pop(to *T) bool {
	item, err := q.queue.Dequeue()
	if err != nil {
		return false
	}

	return q.toObject(item, to)
}

func (q *PersistentQueue[T]) Len() int {
	return int(q.queue.Length())
}

func (q *PersistentQueue[T]) toObject(item *goque.Item, to *T) bool {
	if err := item.ToObject(to); err != nil {
		log.Printf("Failed to convert item to object: %s", err.Error())
		return false
	}

	return true
}
