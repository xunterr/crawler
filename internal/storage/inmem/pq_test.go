package inmem

import (
	"container/heap"
	"testing"
)

func TestPq(t *testing.T) {
	priorityQueue := pq[int]{}
	heap.Init(&priorityQueue)

	heap.Push(&priorityQueue, &item[int]{
		value:    10,
		priority: 1,
	})

	heap.Push(&priorityQueue, &item[int]{
		value:    12,
		priority: 2,
	})

	val := heap.Pop(&priorityQueue).(*item[int])
	if val.priority != 1 {
		t.Fatalf("Wrong priority")
	}
}
