package frontier

import (
	"errors"
	"fmt"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
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

	bloom *bloom

	maxActiveQueues int
	nextQueue       *queues.PriorityQueue[string]
	block           *sync.Cond

	responseTime map[string]time.Duration
	rtMu         sync.Mutex

	inactiveQueues queues.Queue[string]
	iqMu           sync.Mutex

	qeMu       sync.Mutex
	onQueueEnd map[string][]chan struct{}
}

func NewBfFrontier(qp QueueProvider) *BfFrontier {
	pq := queues.NewPriorityQueue[string]()

	f := &BfFrontier{
		activeQueues: 0,

		queueProvider: qp,

		queueMap: make(map[string]*FrontierQueue),
		block:    sync.NewCond(new(sync.Mutex)),

		bloom: newBloom(),

		maxActiveQueues: 256,
		responseTime:    make(map[string]time.Duration),
		nextQueue:       pq,
		inactiveQueues:  queues.NewQueue[string](),
		onQueueEnd:      make(map[string][]chan struct{}),
	}

	go func() {
		t := time.Tick(time.Duration(time.Second * 2))
		for range t {
			f.displayDebug()
		}
	}()

	return f
}

func (f *BfFrontier) displayDebug() {
	fmt.Printf("Active queues: %d\n", f.activeQueues)
	fmt.Printf("Actual active queues: %d\n", f.calculateActiveQueues())
	fmt.Printf("Ready queues: %d\n", f.nextQueue.Length())
	fmt.Printf("Inactive queues: %d\n", f.inactiveQueues.Len())
}

func (f *BfFrontier) calculateActiveQueues() int {
	f.qmMu.Lock()
	f.qmMu.Unlock()
	var counter int
	for _, v := range f.queueMap {
		if v.isActive {
			counter++
		}
	}

	return counter
}

func (f *BfFrontier) Get() (*url.URL, time.Time, error) {
	queueIndex, accessAt, ok := f.getNextQueue()
	if !ok {
		return nil, time.Time{}, errors.New("Failed to get new queue index")
	}

	u, ok := f.dequeueFrom(queueIndex)
	if !ok {
		return nil, time.Time{}, errors.New("Failed to dequeue from queue")
	}

	url, err := url.Parse(u.Url)
	if err != nil {
		return url, time.Time{}, err
	}

	return url, accessAt, nil
}

func (f *BfFrontier) dequeueFrom(queueId string) (Url, bool) {
	f.qmMu.Lock()
	queue, ok := f.queueMap[queueId]
	f.qmMu.Unlock()

	if !ok {
		return Url{}, false
	}

	u, ok := queue.Dequeue()
	if !ok {
		f.swapQueue(queueId)
		return Url{}, false
	}

	if !queue.isActive {
		f.swapQueue(queueId)
	}

	return u, true
}

func (f *BfFrontier) swapQueue(queueId string) {
	f.decreaseActiveCount()
	f.enqueueInactiveId(queueId)
	f.wakeInactiveQueue()
}

func (f *BfFrontier) Put(url *url.URL) error {
	if f.bloom.checkBloom(url.Hostname(), []byte(url.String())) {
		return nil
	}

	f.qmMu.Lock()
	queue, ok := f.queueMap[url.Hostname()]
	f.qmMu.Unlock()

	if !ok {
		var err error
		queue, err = f.addNewDefaultQueue(url.Hostname())
		if err != nil {
			return err
		}
	}

	queue.Enqueue(Url{
		Url:    url.String(),
		Weight: 1,
	})
	f.bloom.addBloom(url.Hostname(), []byte(url.String()))
	return nil
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

	if queue.IsEmpty() {
		go f.notifyAllOnEnd(url.Hostname())
	}

	if queue.isActive {
		after := f.getNextRequestTime(url.Hostname())
		f.setNextQueue(url.Hostname(), after)
	}
}

func (f *BfFrontier) notifyAllOnEnd(queueID string) {
	f.qeMu.Lock()
	defer f.qeMu.Unlock()

	ls, ok := f.onQueueEnd[queueID]
	if !ok {
		return
	}

	for _, l := range ls {
		l <- struct{}{}
		close(l)
	}
	delete(f.onQueueEnd, queueID)
}

func (f *BfFrontier) NotifyOnEnd(queueID string) chan struct{} {
	f.qmMu.Lock()
	defer f.qmMu.Unlock()

	f.qeMu.Lock()
	defer f.qeMu.Unlock()

	listeners := []chan struct{}{}
	if l, ok := f.onQueueEnd[queueID]; ok {
		listeners = l
	}

	ch := make(chan struct{})
	listeners = append(listeners, ch)

	f.onQueueEnd[queueID] = listeners

	if q, ok := f.queueMap[queueID]; ok && q.IsEmpty() {
		go f.notifyAllOnEnd(queueID)
	}

	return ch
}

func (f *BfFrontier) setQueueLock(queueID string, locked bool) bool {
	f.qmMu.Lock()
	queue, ok := f.queueMap[queueID]
	f.qmMu.Unlock()

	if !ok {
		var err error
		queue, err = f.addNewDefaultQueue(queueID)
		if err != nil {
			return false
		}
	}

	if locked {
		queue.Lock()
	} else {
		queue.Unlock()
	}

	return true
}

func (f *BfFrontier) wakeInactiveQueue() {
	if !f.canAddNewQueue() {
		return
	}

	id, queue, ok := f.findAvailableInactiveQueue(f.inactiveQueues.Len() % 512)
	if !ok {
		return
	}

	queue.Reset(20)

	f.qmMu.Lock()
	f.queueMap[id] = queue
	f.qmMu.Unlock()

	f.setNextQueue(id, time.Now().UTC())
	f.increaseActiveCount()
}

func (f *BfFrontier) increaseActiveCount() uint32 {
	f.aqMu.Lock()
	f.activeQueues += 1
	defer f.aqMu.Unlock()
	return f.activeQueues
}

func (f *BfFrontier) decreaseActiveCount() uint32 {
	f.aqMu.Lock()
	f.activeQueues -= 1
	defer f.aqMu.Unlock()
	return f.activeQueues
}

func (f *BfFrontier) findAvailableInactiveQueue(depth int) (string, *FrontierQueue, bool) {
	for i := depth; i > 0; i-- {
		inactiveQueueID, ok := f.dequeueInactiveId()
		if !ok {
			return "", nil, false
		}

		f.qmMu.Lock()
		inactiveQueue, ok := f.queueMap[inactiveQueueID]
		f.qmMu.Unlock()
		if !ok {
			continue
		}

		if !inactiveQueue.IsLocked() && !inactiveQueue.IsEmpty() {
			return inactiveQueueID, inactiveQueue, true
		} else {
			f.enqueueInactiveId(inactiveQueueID)
		}
	}

	return "", nil, false
}

func (f *BfFrontier) enqueueInactiveId(queueId string) {
	f.iqMu.Lock()
	f.inactiveQueues.Push(queueId)
	f.iqMu.Unlock()
}

func (f *BfFrontier) dequeueInactiveId() (id string, ok bool) {
	f.iqMu.Lock()
	ok = f.inactiveQueues.Pop(&id)
	f.iqMu.Unlock()
	return
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

func (f *BfFrontier) addNewDefaultQueue(queueID string) (*FrontierQueue, error) {
	q, err := f.queueProvider.Get(queueID)
	if err != nil {
		return nil, err
	}

	return f.addNewQueue(queueID, q), nil
}

func (f *BfFrontier) addNewQueue(host string, q queues.Queue[Url]) *FrontierQueue {
	active := f.canAddNewQueue()
	queue := NewFrontierQueue(q, active, 20)

	f.qmMu.Lock()
	f.queueMap[host] = queue
	f.qmMu.Unlock()

	if active {
		f.increaseActiveCount()
		nextRequest := f.getNextRequestTime(host)
		f.setNextQueue(host, nextRequest)
	} else {
		f.enqueueInactiveId(host)
	}

	return queue
}
func (f *BfFrontier) LoadQueues(queues map[string]queues.Queue[Url]) {
	for k, queue := range queues {
		f.addNewQueue(k, queue)
	}
}
