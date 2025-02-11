package rocksdb

import (
	"encoding/binary"
	"math"
	"sync"

	"github.com/linxGnu/grocksdb"
	"github.com/xunterr/crawler/internal/storage"
)

type RocksdbQueue[V any] struct {
	qMu     sync.Mutex
	storage *RocksdbStorage[V]
	head    uint32
	tail    uint32
	queueId []byte
}

func NewRocksdbQueue[V any](storage *RocksdbStorage[V], queueId []byte) *RocksdbQueue[V] {
	rq := &RocksdbQueue[V]{
		storage: storage,
		head:    0,
		tail:    0,
		queueId: queueId,
	}

	rq.initQueue()
	return rq
}

func (r *RocksdbQueue[V]) getKey(seq uint32) []byte {
	buff := r.queueId
	buff = binary.BigEndian.AppendUint32(buff, seq)
	return buff
}

func (r *RocksdbQueue[V]) initQueue() {
	ro := grocksdb.NewDefaultReadOptions()
	ro.SetIterateUpperBound(r.getKey(math.MaxUint32))
	it := r.storage.getIter(ro)
	defer it.Close()
	defer ro.Destroy()
	it.Seek(r.getKey(0))

	var first []byte
	var last []byte

	for it = it; it.Valid(); it.Next() {
		key := it.Key()
		if first == nil {
			first = append([]byte{}, key.Data()...)
		}

		last = append([]byte{}, key.Data()...)

		key.Free()
	}

	if first != nil {
		r.head = binary.BigEndian.Uint32(first[len(first)-4:])
	}
	if last != nil {
		r.tail = binary.BigEndian.Uint32(last[len(last)-4:]) + 1
	}
}

func (r *RocksdbQueue[V]) Push(entry V) error {
	r.qMu.Lock()
	defer r.qMu.Unlock()

	id := r.getKey(r.tail)
	err := r.storage.Put(string(id), entry)
	if err != nil {
		return err
	}
	r.tail++
	return nil
}

func (r *RocksdbQueue[V]) Pop() (V, error) {
	id := r.getKey(r.head)
	value, err := r.storage.Get(string(id))
	if err != nil {
		if err == storage.NoSuchKeyError {
			return *new(V), storage.NoNextItem
		}
		return *new(V), err
	}
	if r.head < r.tail {
		r.head++
	}
	if err := r.storage.Delete(string(id)); err != nil {
		return *new(V), err
	}

	return value, nil
}

func (r *RocksdbQueue[V]) Peek() (V, error) {
	r.qMu.Lock()
	defer r.qMu.Unlock()

	id := r.getKey(r.head)
	value, err := r.storage.Get(string(id))
	if err != nil {
		if err == storage.NoSuchKeyError {
			return *new(V), storage.NoNextItem
		}
		return *new(V), err
	}
	return value, nil
}

func (r *RocksdbQueue[V]) Len() int {
	r.qMu.Lock()
	defer r.qMu.Unlock()
	return int(r.tail - r.head)
}
