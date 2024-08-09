package main

import (
	"errors"
	"fmt"
	"log"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/beeker1121/goque"
	"github.com/google/uuid"
	"github.com/vishalkuo/bimap"
	"github.com/xunterr/crawler/pkg/pq"
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
	queueMap *bimap.BiMap[int, string] //todo: make own implementation to persist values on disk

	lastQueueIndex int
	lastQueueMu    sync.Mutex

	maxQueues int
	nextQueue *pq.PriorityQueue[string]
	block     *sync.Cond

	prefixQueue *goque.PrefixQueue

	responseTime map[string]time.Duration
}

func NewBfFrontier() (*BfFrontier, error) {
	crawlID, err := uuid.NewUUID()
	if err != nil {
		return nil, err
	}

	prefixQueue, err := goque.OpenPrefixQueue(fmt.Sprintf("urlQueue-%s", crawlID.String()))
	if err != nil {
		return nil, err
	}

	pq := pq.NewPriorityQueue[string]()

	f := &BfFrontier{
		queueMap:     bimap.NewBiMap[int, string](),
		prefixQueue:  prefixQueue,
		block:        sync.NewCond(new(sync.Mutex)),
		maxQueues:    20,
		responseTime: make(map[string]time.Duration),
		nextQueue:    pq,
	}

	return f, nil
}

func (f *BfFrontier) Get() ([]url.URL, error) {
	f.block.L.Lock()
	for f.nextQueue.Length() == 0 {
		f.block.Wait()
	}
	index, priority, ok := f.nextQueue.Pop()
	f.block.L.Unlock()

	if !ok {
		return nil, errors.New("Failed to get new queue index")
	}

	queueIndex, err := strconv.Atoi(index)
	if err != nil {
		return nil, err
	}

	sleep := time.Duration(int64(priority)-time.Now().UTC().UnixMilli()) * time.Millisecond
	time.Sleep(sleep)

	urlStr, err := f.prefixQueue.DequeueString(index)
	if err != nil {
		if err == goque.ErrEmpty { //delete mapping (queue is now pending)
			f.queueMap.Delete(queueIndex)
			log.Println("Queue is empty, deleting queue mapping")
			return f.Get()
		} else {
			log.Println(err.Error())
			return nil, err
		}
	}

	urlParsed, err := url.Parse(urlStr.ToString())

	f.block.L.Lock()
	after := time.Duration(10 * time.Second)
	if responseTime, ok := f.responseTime[urlParsed.Hostname()]; ok {
		after = responseTime
	}

	nextRequest := time.Now().UTC().Add(after)
	f.nextQueue.Push(index, int(nextRequest.UnixMilli()))
	f.block.Signal()
	f.block.L.Unlock()

	return []url.URL{*urlParsed}, err
}

func (f *BfFrontier) UpdateResponseTime(host string, time time.Duration) {
	f.responseTime[host] = time
}

func (f *BfFrontier) Stop() {
	f.prefixQueue.Close()
	//TODO: stop all processes using context
}

func (f *BfFrontier) Put(url url.URL) {
	queueIndex, ok := f.queueMap.GetInverse(url.Hostname())
	if !ok {
		//no mapping, create one
		log.Printf("last queue == %d; max queues == %d", f.lastQueueIndex, f.maxQueues)
		f.lastQueueMu.Lock()
		if f.lastQueueIndex < f.maxQueues {
			log.Println("Creating new mapping...")
			f.lastQueueIndex++
			f.queueMap.Insert(f.lastQueueIndex, url.Hostname())
			queueIndex = f.lastQueueIndex

			f.nextQueue.Push(strconv.Itoa(queueIndex), int(time.Now().UTC().UnixMilli()))
			f.block.L.Lock()
			f.block.Signal()
			f.block.L.Unlock()
		}
		f.lastQueueMu.Unlock()
	}

	log.Println("Enqueuing...")
	_, err := f.prefixQueue.EnqueueString(strconv.Itoa(queueIndex), url.String())
	if err != nil {
		log.Println(err.Error())
		return
	}
}
