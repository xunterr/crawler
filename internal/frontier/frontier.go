package frontier

import (
	"errors"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/beeker1121/goque"
	"github.com/xunterr/crawler/pkg/queues"
)

type Frontier interface {
	Put(url url.URL)
	Results() chan url.URL
}

type DefaultFrontier struct {
	results chan url.URL
}

func NewDefaultFrontier() *DefaultFrontier {
	return &DefaultFrontier{
		results: make(chan url.URL, 69),
	}
}

func (f *DefaultFrontier) Put(url url.URL) {
	//blah blah
	log.Printf("Putting %s...", url.String())
	f.results <- url
}

func (f *DefaultFrontier) Results() chan url.URL {
	return f.results
}

type BfFrontier struct {
	activeQueues uint32
	aqMu         sync.Mutex

	queueMap map[string]*FrontierQueue

	prefixQueue     *goque.PrefixQueue
	maxActiveQueues int
	nextQueue       *queues.PriorityQueue[string]
	block           *sync.Cond

	responseTime map[string]time.Duration

	inactiveQueues *queues.Queue[string]
}

func NewBfFrontier() (*BfFrontier, error) {
	pq := queues.NewPriorityQueue[string]()

	f := &BfFrontier{
		activeQueues: 0,

		queueMap:        make(map[string]*FrontierQueue),
		block:           sync.NewCond(new(sync.Mutex)),
		maxActiveQueues: 256,
		responseTime:    make(map[string]time.Duration),
		nextQueue:       pq,
		inactiveQueues:  queues.NewQueue[string](),
	}

	return f, nil
}

func (f *BfFrontier) Get() (Url, time.Time, error) {
	queueIndex, accessAt, ok := f.getNextQueue()
	if !ok {
		return Url{}, time.Time{}, errors.New("Failed to get new queue index")
	}

	queue, ok := f.queueMap[queueIndex]

	if !ok {
		log.Printf("Unable to retreive url queue %s", queueIndex)
		return f.Get()
	}

	url, ok := queue.Dequeue()

	if !ok {
		f.setInactive(queueIndex)
		return f.Get()
	}

	return url, accessAt, nil
}

func (f *BfFrontier) Processed(url url.URL, ttr time.Duration) {
	f.responseTime[url.Hostname()] = ttr
	queue, ok := f.queueMap[url.Hostname()]
	if !ok {
		return
	}

	if queue.isActive {
		after := f.getNextRequestTime(url.Hostname())
		f.setNextQueue(url.Hostname(), after)
	} else {
		f.setInactive(url.Hostname())
		f.wakeInactiveQueue()
	}

	//add to bloom filter
}

func (f *BfFrontier) setInactive(queueID string) {
	f.aqMu.Lock()
	f.activeQueues -= 1
	f.aqMu.Unlock()

	f.inactiveQueues.Push(queueID)
}

func (f *BfFrontier) wakeInactiveQueue() {
	if !f.canAddNewQueue() {
		return
	}

	inactiveQueueID, ok := f.inactiveQueues.Pop()
	if !ok {
		return
	}

	inactiveQueue, ok := f.queueMap[inactiveQueueID]
	if !ok {
		return
	}

	inactiveQueue.Reset(20)
	f.queueMap[inactiveQueueID] = inactiveQueue

	f.setNextQueue(inactiveQueueID, time.Now().UTC())

	f.aqMu.Lock()
	f.activeQueues += 1
	f.aqMu.Unlock()
}

func (f *BfFrontier) canAddNewQueue() bool {
	f.aqMu.Lock()
	defer f.aqMu.Unlock()

	return f.activeQueues < uint32(f.maxActiveQueues)
}

func (f *BfFrontier) getNextRequestTime(host string) time.Time {
	after := time.Duration(1 * time.Second)
	if responseTime, ok := f.responseTime[host]; ok {
		after = responseTime * 10
	}

	return time.Now().UTC().Add(after)

}

func (f *BfFrontier) getNextQueue() (string, time.Time, bool) {
	f.block.L.Lock()
	for f.nextQueue.Length() == 0 {
		f.block.Wait()
	}
	index, priority, ok := f.nextQueue.Pop()
	f.block.L.Unlock()

	accessAt := time.Unix(0, int64(priority)*int64(time.Millisecond))

	return index, accessAt, ok
}

func (f *BfFrontier) setNextQueue(queueIndex string, at time.Time) {
	f.block.L.Lock()
	f.nextQueue.Push(queueIndex, int(at.UnixMilli()))
	f.block.Signal()
	f.block.L.Unlock()
}

func (f *BfFrontier) Stop() {
	f.prefixQueue.Close()
	//TODO: stop all processes using context
}

func (f *BfFrontier) Put(url url.URL) {
	queue, ok := f.queueMap[url.Hostname()]
	if !ok {

		//no mapping, create one

		active := f.canAddNewQueue()

		queue = NewFrontierQueue(active, 20)
		f.queueMap[url.Hostname()] = queue

		if active {
			f.aqMu.Lock()
			f.activeQueues++
			f.aqMu.Unlock()

			nextRequest := f.getNextRequestTime(url.Hostname())
			f.setNextQueue(url.Hostname(), nextRequest)
		} else {
			f.inactiveQueues.Push(url.Hostname())
		}
	}

	queue.Enqueue(Url{
		URL:    &url,
		weight: 1,
	})

}
