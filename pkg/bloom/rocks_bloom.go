package bloom

import (
	"bytes"
	"errors"
	"io"

	"github.com/linxGnu/grocksdb"
	boom "github.com/tylertreat/BoomFilters"
)

type RocksdbBloom struct {
	db *grocksdb.DB
}

func NewRocksdbBloom(db *grocksdb.DB) *RocksdbBloom {
	return &RocksdbBloom{db: db}
}

func (b *RocksdbBloom) AddBloom(key string, entry []byte) error {
	bloom, err := b.readBloom(key)
	if err != nil {
		bloom = boom.NewDefaultScalableBloomFilter(0.01)
	}

	bloom.Add(entry)

	bytes := b.toBytes(bloom)
	err = b.db.Put(grocksdb.NewDefaultWriteOptions(), []byte(key), bytes)
	if err != nil {
		return err
	}

	return nil
}

func (b *RocksdbBloom) CheckBloom(key string, entry []byte) (bool, error) {
	bloom, err := b.readBloom(key)
	if err != nil {
		return false, err
	}

	return bloom.Test(entry), nil
}

func (b *RocksdbBloom) GetBloom(key string) ([]byte, error) {
	value, err := b.db.Get(grocksdb.NewDefaultReadOptions(), []byte(key))
	if err != nil {
		return []byte{}, err
	}

	if !value.Exists() {
		bloom := boom.NewDefaultScalableBloomFilter(0.01)
		return b.toBytes(bloom), nil
	}
	defer value.Free()
	return value.Data(), nil
}

func (b *RocksdbBloom) SetBloom(key string, bloom []byte) (bool, error) {
	bl, err := b.fromBytes(bloom)
	if err != nil {
		return false, err
	}

	err = b.db.Put(grocksdb.NewDefaultWriteOptions(), []byte(key), b.toBytes(bl))
	if err != nil {
		return false, err
	}
	return true, nil
}

func (b *RocksdbBloom) readBloom(key string) (*boom.ScalableBloomFilter, error) {
	value, err := b.db.Get(grocksdb.NewDefaultReadOptions(), []byte(key))
	if err != nil {
		return nil, err
	}
	defer value.Free()

	if !value.Exists() {
		return nil, errors.New("No such key")
	}

	return b.fromBytes(value.Data())
}

func (b *RocksdbBloom) toBytes(bloom *boom.ScalableBloomFilter) []byte {
	var bytes bytes.Buffer
	writer := io.Writer(&bytes)
	bloom.WriteTo(writer)
	return bytes.Bytes()
}

func (b *RocksdbBloom) fromBytes(data []byte) (*boom.ScalableBloomFilter, error) {
	bloom := boom.NewDefaultScalableBloomFilter(0.01)
	buf := bytes.NewReader(data)
	_, err := bloom.ReadFrom(buf)

	if err != nil {
		return nil, err
	}
	return bloom, err
}
