package main

import (
	"fmt"
	"log"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/beeker1121/goque"
	"github.com/google/uuid"
	"github.com/vishalkuo/bimap"
)

type UrlRecord struct {
	from struct {
		url          url.URL
		timeToLoadMs int
	}
	url url.URL
}

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
	nextQueue *goque.PriorityQueue
	block     *sync.Cond

	prefixQueue *goque.PrefixQueue
	results     chan url.URL
}

func NewBfFrontier() (*BfFrontier, error) {
	crawlID, err := uuid.NewUUID()
	if err != nil {
		return nil, err
	}

	queue, err := goque.OpenPriorityQueue(fmt.Sprintf("nextQueue-%s", crawlID.String()), goque.ASC)
	if err != nil {
		return nil, err
	}

	prefixQueue, err := goque.OpenPrefixQueue(fmt.Sprintf("urlQueue-%s", crawlID.String()))
	if err != nil {
		return nil, err
	}

	f := &BfFrontier{
		queueMap:    bimap.NewBiMap[int, string](),
		nextQueue:   queue,
		prefixQueue: prefixQueue,
		block:       sync.NewCond(new(sync.Mutex)),
		maxQueues:   20,
		results:     make(chan url.URL, 69),
	}

	return f, nil
}

func (f *BfFrontier) Start() error {
	for {
		f.block.L.Lock()
		for f.nextQueue.Length() == 0 {
			f.block.Wait()
		}
		item, err := f.nextQueue.Dequeue()
		f.block.L.Unlock()
		if err != nil {
			log.Println(err.Error())
			continue
		}

		queueIndexStr := item.ToString()
		queueIndex, err := strconv.Atoi(queueIndexStr)
		if err != nil {
			continue
		}

		urlStr, err := f.prefixQueue.DequeueString(queueIndexStr)
		if err != nil {
			if err == goque.ErrEmpty { //delete mapping (queue is now pending)
				f.queueMap.Delete(queueIndex)
			} else {
				log.Println(err.Error())
			}
			continue
		}

		urlParsed, err := url.Parse(urlStr.ToString())
		if err != nil {
			continue
		}
		println("here")
		f.results <- *urlParsed
	}
}

func (f *BfFrontier) Stop() {
	f.prefixQueue.Close()
	f.nextQueue.Close() //TODO: stop all processes using context
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

			f.nextQueue.Enqueue(uint8(time.Now().UTC().UnixMilli()), []byte(strconv.Itoa(queueIndex)))
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

func (f *BfFrontier) Results() chan url.URL {
	return f.results
}
