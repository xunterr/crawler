package rocksdb

import (
	"bytes"
	"encoding/gob"

	"github.com/linxGnu/grocksdb"
	"github.com/xunterr/crawler/internal/storage"
)

type Encoder[T any] func(T) ([]byte, error)
type Decoder[T any] func([]byte) (T, error)

type RocksdbStorageOption func(*rocksdbStorageOptions)

type RocksdbStorage[V any] struct {
	db       *grocksdb.DB
	encode   Encoder[V]
	decode   Decoder[V]
	cfHandle *grocksdb.ColumnFamilyHandle

	ro *grocksdb.ReadOptions
	wo *grocksdb.WriteOptions
}

type rocksdbStorageOptions struct {
	cfHandle *grocksdb.ColumnFamilyHandle
}

func defaultOptions(db *grocksdb.DB) *rocksdbStorageOptions {
	return &rocksdbStorageOptions{
		cfHandle: db.GetDefaultColumnFamily(),
	}
}

func WithCF(cf *grocksdb.ColumnFamilyHandle) RocksdbStorageOption {
	return func(opts *rocksdbStorageOptions) {
		opts.cfHandle = cf
	}
}

func gobEncode[V any](from V) ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(from); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func gobDecode[V any](from []byte) (V, error) {
	var to V
	buf := bytes.NewBuffer(from)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&to)
	return to, err
}

func NewRocksdbStorage[V any](db *grocksdb.DB, opts ...RocksdbStorageOption) *RocksdbStorage[V] {
	defaultOpts := defaultOptions(db)
	for _, opt := range opts {
		opt(defaultOpts)
	}

	return &RocksdbStorage[V]{
		encode:   gobEncode[V],
		decode:   gobDecode[V],
		db:       db,
		cfHandle: defaultOpts.cfHandle,
		ro:       grocksdb.NewDefaultReadOptions(),
		wo:       grocksdb.NewDefaultWriteOptions(),
	}
}

func NewRocksdbStorageWithEncoderDecoder[V any](db *grocksdb.DB, encoder Encoder[V], decoder Decoder[V], opts ...RocksdbStorageOption) *RocksdbStorage[V] { //...
	storage := NewRocksdbStorage[V](db, opts...)
	storage.encode = encoder
	storage.decode = decoder
	return storage
}

func (s *RocksdbStorage[V]) Get(key string) (V, error) {
	value, err := s.db.GetCF(s.ro, s.cfHandle, []byte(key))
	if err != nil {
		return *new(V), err
	}
	defer value.Free()

	if !value.Exists() {
		return *new(V), storage.NoSuchKeyError
	}

	return s.decode(value.Data())
}

func (s *RocksdbStorage[V]) GetAll() (map[string]V, error) {
	all := make(map[string]V)
	iter := s.getIter(s.ro)
	defer iter.Close()
	iter.SeekToFirst()
	for iter = iter; iter.Valid(); iter.Next() {
		key := iter.Key().Data()
		value := iter.Value().Data()
		valueDecoded, err := s.decode(value)
		if err != nil {
			return nil, err
		}
		all[string(key)] = valueDecoded
	}
	return all, nil
}

func (s *RocksdbStorage[V]) Put(key string, value V) error {
	bytes, err := s.encode(value)
	if err != nil {
		return err
	}
	err = s.db.PutCF(s.wo, s.cfHandle, []byte(key), bytes)
	return err
}

func (s *RocksdbStorage[V]) Delete(key string) error {
	return s.db.DeleteCF(s.wo, s.cfHandle, []byte(key))
}

func (s *RocksdbStorage[V]) Close() {
	s.ro.Destroy()
	s.wo.Destroy()
}

func (s *RocksdbStorage[V]) getIter(ro *grocksdb.ReadOptions) *grocksdb.Iterator {
	return s.db.NewIteratorCF(ro, s.cfHandle)
}
