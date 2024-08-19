package queues

import "testing"

func TestQueue(t *testing.T) {
	queue := NewQueue[int]()

	queue.Push(1)

	el, ok := queue.Pop()

	if !ok || el != 1 {
		t.Fatal("Unexpected .Pop() result")
	}
}
