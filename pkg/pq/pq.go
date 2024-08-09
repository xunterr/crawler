package pq

import "container/heap"

// A PriorityQueue[T] provides abstraction for working with heap
type PriorityQueue[T any] struct {
	pq *pq[T]
}

func NewPriorityQueue[T any]() *PriorityQueue[T] {
	pq := &pq[T]{}
	heap.Init(pq)
	return &PriorityQueue[T]{
		pq: pq,
	}
}

func (p *PriorityQueue[T]) Push(value T, priority int) {
	heap.Push(p.pq, &item[T]{
		value:    value,
		priority: priority,
	})
}

func (p *PriorityQueue[T]) Pop() (T, int, bool) {
	item := heap.Pop(p.pq).(*item[T])
	return item.value, item.priority, true
}

func (p *PriorityQueue[T]) Length() int {
	return p.pq.Len()
}

type item[T any] struct {
	value    T   // The value of the item; arbitrary.
	priority int // The priority of the item in the queue.
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

// A pq implements heap.Interface and holds Items.
type pq[T any] []*item[T]

func (pq pq[T]) Len() int { return len(pq) }

func (pq pq[T]) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].priority < pq[j].priority
}

func (pq pq[T]) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *pq[T]) Push(x any) {
	n := len(*pq)
	item := x.(*item[T])
	item.index = n
	*pq = append(*pq, item)
}

func (pq *pq[T]) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority and value of an item in the queue.
func (pq *pq[T]) update(item *item[T], value T, priority int) {
	item.value = value
	item.priority = priority
	heap.Fix(pq, item.index)
}
