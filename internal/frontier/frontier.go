package frontier

import (
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/xunterr/crawler/internal/storage"
	"github.com/xunterr/crawler/internal/storage/inmem"
)

type Frontier interface {
	Get() (*url.URL, time.Time, error)
	MarkSuccessful(*url.URL, time.Duration) error
	MarkFailed(*url.URL) error
	Put(*url.URL) error
}

type QueueProvider interface {
	Get(string) (storage.Queue[Url], error)
}

type InMemoryQueueProvider struct{}

func (qp InMemoryQueueProvider) Get(host string) (storage.Queue[Url], error) {
	return inmem.NewQueue[Url](), nil
}

type PersistentQueueProvider struct {
	base string
	db   *leveldb.DB
}

type bfFrontierOpts struct {
	maxActiveQueues      int
	politenessMultiplier int
	defaultSessionBudget int
}

type BfFrontierOption func(*bfFrontierOpts)

func defaultOpts() bfFrontierOpts {
	return bfFrontierOpts{
		maxActiveQueues:      256,
		politenessMultiplier: 10,
		defaultSessionBudget: 20,
	}
}

func WithMaxActiveQueues(maxAq int) BfFrontierOption {
	return func(fo *bfFrontierOpts) {
		fo.maxActiveQueues = maxAq
	}
}

func WithPolitenessMultiplier(politeness int) BfFrontierOption {
	return func(fo *bfFrontierOpts) {
		fo.politenessMultiplier = politeness
	}
}

func WithSessionBudget(sb int) BfFrontierOption {
	return func(fo *bfFrontierOpts) {
		fo.defaultSessionBudget = sb
	}
}

type BfFrontier struct {
	opts bfFrontierOpts

	activeQueues uint32
	aqMu         sync.Mutex

	queueProvider QueueProvider

	queueMap map[string]*FrontierQueue
	qmMu     sync.Mutex

	bloom *bloom

	nextQueue *inmem.PriorityQueue[string]
	block     *sync.Cond

	responseTime map[string]time.Duration
	rtMu         sync.Mutex

	inactiveQueues storage.Queue[string]
	iqMu           sync.Mutex

	qeMu       sync.Mutex
	onQueueEnd map[string][]chan struct{}
}

func NewBfFrontier(qp QueueProvider, bloomStorage BloomStorage, opts ...BfFrontierOption) *BfFrontier {
	pq := inmem.NewPriorityQueue[string]()

	defaultOpts := defaultOpts()

	for _, fn := range opts {
		fn(&defaultOpts)
	}

	f := &BfFrontier{
		opts: defaultOpts,

		activeQueues: 0,

		queueProvider: qp,

		queueMap: make(map[string]*FrontierQueue),
		block:    sync.NewCond(new(sync.Mutex)),

		bloom: newBloom(bloomStorage),

		responseTime:   make(map[string]time.Duration),
		nextQueue:      pq,
		inactiveQueues: inmem.NewQueue[string](),
		onQueueEnd:     make(map[string][]chan struct{}),
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
	f.aqMu.Lock()
	defer f.aqMu.Unlock()
	fmt.Printf("Active queues: %d\n", f.activeQueues)
	fmt.Printf("Actual active queues: %d\n", f.calculateActiveQueues())
	fmt.Printf("Ready queues: %d\n", f.nextQueue.Length())
	fmt.Printf("Inactive queues: %d\n", f.inactiveQueues.Len())
}

func (f *BfFrontier) calculateActiveQueues() int {
	f.qmMu.Lock()
	defer f.qmMu.Unlock()
	var counter int
	for _, v := range f.queueMap {
		if v.isActive {
			counter++
		}
	}

	return counter
}

func (f *BfFrontier) Get() (*url.URL, time.Time, error) {
	for {
		url, accessAt, err := f.getNextUrl()
		if err != nil {
			return nil, time.Time{}, err
		}

		id := toId(url)

		hit, err := f.bloom.checkBloom(id, []byte(url.String()))
		if err != nil {
			return nil, time.Time{}, err
		}

		if hit {
			f.setNextQueue(id, f.getNextRequestTime(id))
		} else {
			return url, accessAt, nil
		}
	}
}

func toId(url *url.URL) string {
	if len(url.String()) == 0 {
		return ""
	}

	return url.Hostname()
}

func (f *BfFrontier) getNextUrl() (*url.URL, time.Time, error) {
	queueIndex, accessAt, ok := f.getNextQueue()
	if !ok {
		return nil, time.Time{}, errors.New("Failed to get new queue index")
	}

	u, ok := f.dequeueFrom(queueIndex)
	if !ok {
		return nil, time.Time{}, errors.New(fmt.Sprintf("Failed to dequeue from queue: %s", queueIndex))
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

	return u, true
}

func (f *BfFrontier) swapQueue(queueId string) {
	f.decreaseActiveCount()
	f.enqueueInactiveId(queueId)
	f.wakeInactiveQueue()
}

func (f *BfFrontier) Put(url *url.URL) error {
	id := toId(url)
	ok, err := f.bloom.checkBloom(id, []byte(url.String()))
	if err != nil {
		return err
	}

	if ok {
		return nil
	}

	f.qmMu.Lock()
	queue, ok := f.queueMap[id]
	f.qmMu.Unlock()

	if !ok {
		var err error
		queue, err = f.addNewDefaultQueue(id)
		if err != nil {
			return err
		}
	}

	queue.Enqueue(Url{
		Url:    url.String(),
		Weight: 1,
	})
	return nil
}

func (f *BfFrontier) MarkSuccessful(url *url.URL, ttr time.Duration) error {
	f.rtMu.Lock()
	f.responseTime[toId(url)] = ttr
	f.rtMu.Unlock()

	id := toId(url)
	f.qmMu.Lock()
	queue, ok := f.queueMap[id]
	f.qmMu.Unlock()

	if !ok {
		return errors.New("No such queue")
	}

	queue.sessionBudget = f.calculateSessionBudget(id)

	return f.markProcessed(url)
}

func (f *BfFrontier) MarkFailed(url *url.URL) error {
	return f.markProcessed(url)
}

func (f *BfFrontier) markProcessed(url *url.URL) error {
	id := toId(url)
	f.qmMu.Lock()
	queue, ok := f.queueMap[id]
	f.qmMu.Unlock()

	if !ok {
		return errors.New("No such queue")
	}

	if queue.IsEmpty() {
		go f.notifyAllOnEnd(id)
	}

	after := f.getNextRequestTime(id)
	f.setNextQueue(id, after)

	return f.bloom.addBloom(id, []byte(url.String()))
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
	if !f.incActiveCountIfCan() {
		return
	}

	id, queue, ok := f.findAvailableInactiveQueue(f.inactiveQueues.Len())
	if !ok {
		f.decreaseActiveCount()
		return
	}

	queue.Reset(f.calculateSessionBudget(id))

	f.qmMu.Lock()
	f.queueMap[id] = queue
	f.qmMu.Unlock()

	f.setNextQueue(id, time.Now().UTC())
}

func (f *BfFrontier) calculateSessionBudget(queueId string) uint64 {
	f.rtMu.Lock()
	rt, ok := f.responseTime[queueId]
	f.rtMu.Unlock()

	if !ok {
		return uint64(f.opts.defaultSessionBudget)
	}

	if rt.Milliseconds() < 0 {
		rt = time.Duration(0)
	}

	normalizedTTR := rt.Milliseconds() / 5000
	sessionBudget := (1 - normalizedTTR) * int64(f.opts.defaultSessionBudget)
	return uint64(sessionBudget)
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
	id, err := f.inactiveQueues.Pop()
	if err != nil {
		return "", false
	}
	f.iqMu.Unlock()
	return id, true
}

func (f *BfFrontier) canAddNewQueue() bool {
	f.aqMu.Lock()
	defer f.aqMu.Unlock()

	return f.activeQueues < uint32(f.opts.maxActiveQueues)
}

func (f *BfFrontier) incActiveCountIfCan() bool {
	f.aqMu.Lock()
	defer f.aqMu.Unlock()
	if f.activeQueues < uint32(f.opts.maxActiveQueues) {
		f.activeQueues += 1
		return true
	} else {
		return false
	}
}

func (f *BfFrontier) getNextRequestTime(id string) time.Time {
	after := time.Duration(1 * time.Second)

	f.rtMu.Lock()
	if responseTime, ok := f.responseTime[id]; ok {
		after = responseTime * time.Duration(f.opts.politenessMultiplier)
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

func (f *BfFrontier) addNewQueue(id string, q storage.Queue[Url]) *FrontierQueue {
	active := f.incActiveCountIfCan()
	queue := NewFrontierQueue(q, active, uint64(f.opts.defaultSessionBudget))

	f.qmMu.Lock()
	f.queueMap[id] = queue
	f.qmMu.Unlock()

	if active {
		f.setNextQueue(id, time.Now().UTC())
	} else {
		f.enqueueInactiveId(id)
	}

	return queue
}
func (f *BfFrontier) LoadQueues(queues map[string]storage.Queue[Url]) {
	for k, queue := range queues {
		f.addNewQueue(k, queue)
	}
}
