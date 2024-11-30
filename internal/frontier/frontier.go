package frontier

import (
	"errors"
	"fmt"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	boom "github.com/tylertreat/BoomFilters"
	"github.com/xunterr/crawler/pkg/queues"
)

type Frontier interface {
	Get() (*url.URL, time.Time, error)
	MarkProcessed(*url.URL, time.Duration)
	Put(*url.URL) error
}

type QueueProvider interface {
	Get(string) (queues.Queue[Url], error)
}

type InMemoryQueueProvider struct{}

func (qp InMemoryQueueProvider) Get(host string) (queues.Queue[Url], error) {
	return queues.NewQueue[Url](), nil
}

type PersistentQueueProvider struct {
	base string
	db   *leveldb.DB
}

func NewPersistentQueueProvider(dbPath string) (*PersistentQueueProvider, error) {
	db, err := leveldb.OpenFile(dbPath, nil)

	return &PersistentQueueProvider{
		base: dbPath,
		db:   db,
	}, err
}

func (qp *PersistentQueueProvider) Get(id string) (queues.Queue[Url], error) {
	name := fmt.Sprintf("%s-%s", qp.base, id)
	err := qp.db.Put([]byte(id), []byte(name), nil)
	if err != nil {
		return nil, err
	}
	log.Printf("Opening %s", name)
	return queues.NewPersistentQueue[Url](name)
}

func (qp *PersistentQueueProvider) GetAll() (map[string]queues.Queue[Url], error) {

	queueMap := make(map[string]queues.Queue[Url])
	iter := qp.db.NewIterator(nil, nil)

	for iter.Next() {
		log.Printf("Opening %s", iter.Value())
		queue, err := queues.NewPersistentQueue[Url](string(iter.Value()))
		if err != nil {
			return queueMap, err
		}
		queueMap[string(iter.Key())] = queue
	}
	return queueMap, nil
}

type BfFrontier struct {
	activeQueues uint32
	aqMu         sync.Mutex

	queueProvider QueueProvider

	queueMap map[string]*FrontierQueue
	qmMu     sync.Mutex

	bloom map[string]*boom.ScalableBloomFilter
	blMu  sync.Mutex

	maxActiveQueues int
	nextQueue       *queues.PriorityQueue[string]
	block           *sync.Cond

	responseTime map[string]time.Duration
	rtMu         sync.Mutex

	inactiveQueues queues.Queue[string]
}

func NewBfFrontier(qp QueueProvider) *BfFrontier {
	pq := queues.NewPriorityQueue[string]()

	f := &BfFrontier{
		activeQueues: 0,

		queueProvider: qp,

		queueMap:        make(map[string]*FrontierQueue),
		block:           sync.NewCond(new(sync.Mutex)),
		bloom:           make(map[string]*boom.ScalableBloomFilter),
		maxActiveQueues: 256,
		responseTime:    make(map[string]time.Duration),
		nextQueue:       pq,
		inactiveQueues:  queues.NewQueue[string](),
	}

	return f
}

func (f *BfFrontier) Get() (*url.URL, time.Time, error) {
	queueIndex, accessAt, ok := f.getNextQueue()
	if !ok {
		return nil, time.Time{}, errors.New("Failed to get new queue index")
	}

	f.qmMu.Lock()
	queue, ok := f.queueMap[queueIndex]
	f.qmMu.Unlock()

	if !ok {
		log.Printf("Unable to retreive url queue %s", queueIndex)
		return f.Get()
	}

	u, ok := queue.Dequeue()

	if !ok {
		f.setInactive(queueIndex)
		return f.Get()
	}

	url, err := url.Parse(u.Url)
	if err != nil {
		return f.Get()
	}

	return url, accessAt, nil
}

func (f *BfFrontier) MarkProcessed(url *url.URL, ttr time.Duration) {
	f.rtMu.Lock()
	f.responseTime[url.Hostname()] = ttr
	f.rtMu.Unlock()

	f.qmMu.Lock()
	queue, ok := f.queueMap[url.Hostname()]
	f.qmMu.Unlock()

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

	f.addBloom(url.Hostname(), []byte(url.String()))
}

func (f *BfFrontier) addBloom(key string, entry []byte) {
	f.blMu.Lock()
	defer f.blMu.Unlock()
	b, ok := f.bloom[key]
	if !ok {
		b = boom.NewDefaultScalableBloomFilter(0.01)
		f.bloom[key] = b
	}
	b.Add(entry)
}

func (f *BfFrontier) checkBloom(key string, entry []byte) bool {
	f.blMu.Lock()
	defer f.blMu.Unlock()

	b, ok := f.bloom[key]
	if !ok {
		return false
	}

	return b.Test(entry)
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

	var inactiveQueueID string
	ok := f.inactiveQueues.Pop(&inactiveQueueID)
	if !ok {
		return
	}

	f.qmMu.Lock()
	inactiveQueue, ok := f.queueMap[inactiveQueueID]
	f.qmMu.Unlock()

	if !ok {
		return
	}

	inactiveQueue.Reset(20)

	f.qmMu.Lock()
	f.queueMap[inactiveQueueID] = inactiveQueue
	f.qmMu.Unlock()

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

	f.rtMu.Lock()
	if responseTime, ok := f.responseTime[host]; ok {
		after = responseTime * 10
	}
	f.rtMu.Unlock()

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

func (f *BfFrontier) Put(url *url.URL) error {
	if f.checkBloom(url.Hostname(), []byte(url.String())) {
		return nil
	}

	f.qmMu.Lock()
	queue, ok := f.queueMap[url.Hostname()]
	f.qmMu.Unlock()

	if !ok {
		//no mapping, create one
		q, err := f.queueProvider.Get(url.Hostname())
		if err != nil {
			return err
		}

		queue = f.addNewQueue(url.Hostname(), q, 20)
	}

	queue.Enqueue(Url{
		Url:    url.String(),
		Weight: 1,
	})
	return nil
}

func (f *BfFrontier) addNewQueue(host string, q queues.Queue[Url], limit uint64) *FrontierQueue {

	active := f.canAddNewQueue()
	queue := NewFrontierQueue(q, active, 20)

	f.qmMu.Lock()
	f.queueMap[host] = queue
	f.qmMu.Unlock()

	if active {
		f.aqMu.Lock()
		f.activeQueues++
		f.aqMu.Unlock()

		nextRequest := f.getNextRequestTime(host)
		f.setNextQueue(host, nextRequest)
	} else {
		f.inactiveQueues.Push(host)
	}

	return queue
}
func (f *BfFrontier) LoadQueues(queues map[string]queues.Queue[Url]) {
	for k, queue := range queues {
		f.addNewQueue(k, queue, 20)
	}
}
