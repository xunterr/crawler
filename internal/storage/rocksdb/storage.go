package rocksdb

import (
	"bytes"
	"encoding/gob"

	"github.com/linxGnu/grocksdb"
	"github.com/xunterr/crawler/internal/storage"
)

type RocksdbStorage[V any] struct {
	db *grocksdb.DB
}

func NewRocksdbStorage[V any](db *grocksdb.DB) *RocksdbStorage[V] {
	return &RocksdbStorage[V]{
		db: db,
	}
}

func (s *RocksdbStorage[V]) Get(key string) (V, error) {
	value, err := s.db.Get(grocksdb.NewDefaultReadOptions(), []byte(key))
	if err != nil {
		return *new(V), err
	}
	defer value.Free()

	if !value.Exists() {
		return *new(V), storage.NoSuchKeyError
	}

	var res V
	err = s.gobDecode(value.Data(), &res)
	return res, err
}

func (s *RocksdbStorage[V]) Put(key string, value V) error {
	bytes, err := s.gobEncode(value)
	if err != nil {
		return err
	}
	err = s.db.Put(grocksdb.NewDefaultWriteOptions(), []byte(key), bytes)
	return err
}

func (s *RocksdbStorage[V]) Delete(key string) error {
	return s.db.Delete(grocksdb.NewDefaultWriteOptions(), []byte(key))
}

func (s *RocksdbStorage[V]) gobEncode(from any) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(from); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (s *RocksdbStorage[V]) gobDecode(from []byte, to any) error {
	buf := bytes.NewBuffer(from)
	dec := gob.NewDecoder(buf)
	return dec.Decode(to)
}
