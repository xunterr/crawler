package frontier

import (
	"github.com/xunterr/crawler/pkg/queues"
)

type Url struct {
	Url    string
	Weight uint32
}

type FrontierQueue struct {
	queue         queues.Queue[Url]
	isActive      bool
	isLocked      bool
	sessionBudget uint64
}

func NewFrontierQueue(from queues.Queue[Url], isActive bool, sessionBudget uint64) *FrontierQueue {
	return &FrontierQueue{
		queue:         from,
		isActive:      isActive,
		sessionBudget: sessionBudget,
	}
}

func (q *FrontierQueue) Enqueue(value Url) {
	q.queue.Push(value)
}

func (q *FrontierQueue) Dequeue() (Url, bool) {

	if !q.isActive || q.IsLocked() {
		return Url{}, false
	}

	url := Url{}
	ok := q.queue.Pop(&url)

	if !ok {
		q.isActive = false
		return Url{}, false
	}

	if q.sessionBudget < uint64(url.Weight) {
		q.sessionBudget = 0
	} else {
		q.sessionBudget -= uint64(url.Weight)
	}

	if q.sessionBudget == 0 || q.IsEmpty() {
		q.isActive = false
	}

	return url, true
}

func (q *FrontierQueue) IsEmpty() bool {
	return q.queue.Len() == 0
}

func (q *FrontierQueue) IsActive() bool {
	return q.isActive
}

func (q *FrontierQueue) IsLocked() bool {
	return q.isLocked
}

func (q *FrontierQueue) Lock() {
	q.isLocked = true
}

func (q *FrontierQueue) Unlock() {
	q.isLocked = false
}

func (q *FrontierQueue) Reset(sessionBudget uint64) {
	q.isActive = true
	q.sessionBudget = sessionBudget
}
