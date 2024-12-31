package rocksdb

import (
	"bytes"
	"encoding/gob"

	"github.com/linxGnu/grocksdb"
	"github.com/xunterr/crawler/internal/storage"
)

type Encoder[T any] func(T) ([]byte, error)
type Decoder[T any] func([]byte) (T, error)

type RocksdbStorage[V any] struct {
	db     *grocksdb.DB
	encode Encoder[V]
	decode Decoder[V]
}

func NewRocksdbStorage[V any](db *grocksdb.DB, encoder Encoder[V], decoder Decoder[V]) *RocksdbStorage[V] {
	return &RocksdbStorage[V]{
		encode: encoder,
		decode: decoder,
		db:     db,
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

	return s.decode(value.Data())
}

func (s *RocksdbStorage[V]) Put(key string, value V) error {
	bytes, err := s.encode(value)
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
	gob.Register(from)
	if err := enc.Encode(&from); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (s *RocksdbStorage[V]) gobDecode(from []byte, to any) error {
	buf := bytes.NewBuffer(from)
	dec := gob.NewDecoder(buf)
	return dec.Decode(to)
}
