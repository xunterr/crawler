package frontier

import (
	"net/url"
	"time"

	"github.com/xunterr/crawler/pkg/queues"
)

type Url struct {
	*url.URL
	accessAt time.Time
	weight   uint32
}

type FrontierQueue struct {
	queue         *queues.Queue[Url]
	isActive      bool
	sessionBudget uint64
}

func NewFrontierQueue(isActive bool, sessionBudget uint64) *FrontierQueue {
	return &FrontierQueue{
		queue:         queues.NewQueue[Url](),
		isActive:      isActive,
		sessionBudget: sessionBudget,
	}
}

func (q *FrontierQueue) Enqueue(value Url) {
	q.queue.Push(value)
}

func (q *FrontierQueue) Dequeue() (Url, bool) {

	if !q.isActive {
		return Url{}, false
	}

	url, ok := q.queue.Pop()

	if !ok {
		return Url{}, false
	}

	if q.sessionBudget < uint64(url.weight) {
		q.sessionBudget = 0
	} else {
		q.sessionBudget -= uint64(url.weight)
	}

	if q.sessionBudget == 0 {
		q.isActive = false
	}

	return url, true
}

func (q *FrontierQueue) IsEmpty() bool {
	return len(*q.queue) == 0
}

func (q *FrontierQueue) Reset(sessionBudget uint64) {
	q.isActive = true
	q.sessionBudget = sessionBudget
}
