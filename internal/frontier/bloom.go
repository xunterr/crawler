package frontier

import (
	"bufio"
	"bytes"
	"sync"

	boom "github.com/tylertreat/BoomFilters"
	"github.com/xunterr/crawler/internal/storage"
)

type BloomStorage storage.Storage[*boom.ScalableBloomFilter]

type bloom struct {
	mu      sync.Mutex
	storage BloomStorage
}

func newBloom(storage BloomStorage) *bloom {
	return &bloom{
		storage: storage,
	}
}

func (b *bloom) addBloom(key string, entry []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	bloom, err := b.storage.Get(key)
	if err != nil {
		if err != storage.NoSuchKeyError {
			return err
		}
		bloom = boom.NewDefaultScalableBloomFilter(0.01)
	}
	bloom.Add(entry)
	return b.storage.Put(key, bloom)
}

func (b *bloom) checkBloom(key string, entry []byte) (bool, error) {
	b.mu.Lock()
	bloom, err := b.storage.Get(key)
	b.mu.Unlock()
	if err != nil {
		if err != storage.NoSuchKeyError {
			return false, err
		}
		return false, nil
	}

	return bloom.Test(entry), nil
}

func (b *bloom) setBloom(key string, bloom []byte) error { //this library doesn't allow for merging two bloom filters
	r := bytes.NewReader(bloom)
	bl := boom.NewDefaultScalableBloomFilter(0.1)
	_, err := bl.ReadFrom(r)
	if err != nil {
		return err
	}

	b.mu.Lock()
	err = b.storage.Put(key, bl)
	b.mu.Unlock()
	if err != nil {
		return err
	}
	return nil
}

func (b *bloom) getBloom(key string) ([]byte, error) {
	b.mu.Lock()
	bloom, err := b.storage.Get(key)
	b.mu.Unlock()
	if err != nil {
		if err != storage.NoSuchKeyError {
			return []byte{}, err
		}

		bloom = boom.NewDefaultScalableBloomFilter(0.01)
	}

	var buff bytes.Buffer
	writer := bufio.NewWriter(&buff)
	bloom.WriteTo(writer)
	return buff.Bytes(), nil
}
