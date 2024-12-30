package bloom

import (
	"bufio"
	"bytes"
	"sync"

	boom "github.com/tylertreat/BoomFilters"
)

type Bloom interface {
	AddBloom(key string, entry []byte) error
	CheckBloom(key string, entry []byte) (bool, error)
	SetBloom(key string, entry []byte) (bool, error)
	GetBloom(key string) ([]byte, error)
}

type InMemoryBloom struct {
	blooms map[string]*boom.ScalableBloomFilter
	blMu   sync.Mutex
}

func NewInMemoryBloom() *InMemoryBloom {
	return &InMemoryBloom{
		blooms: make(map[string]*boom.ScalableBloomFilter),
	}
}

func (b *InMemoryBloom) AddBloom(key string, entry []byte) error {
	b.blMu.Lock()
	defer b.blMu.Unlock()
	bloom, ok := b.blooms[key]
	if !ok {
		bloom = boom.NewDefaultScalableBloomFilter(0.01)
		b.blooms[key] = bloom
	}
	bloom.Add(entry)
	return nil
}

func (b *InMemoryBloom) CheckBloom(key string, entry []byte) (bool, error) {
	b.blMu.Lock()
	defer b.blMu.Unlock()

	bloom, ok := b.blooms[key]
	if !ok {
		return false, nil
	}

	return bloom.Test(entry), nil
}

func (b *InMemoryBloom) SetBloom(key string, bloom []byte) (bool, error) { //this library doesn't allow for merging two bloom filters
	b.blMu.Lock()
	defer b.blMu.Unlock()

	r := bytes.NewReader(bloom)
	bl := boom.NewDefaultScalableBloomFilter(0.1)
	_, err := bl.ReadFrom(r)
	if err != nil {
		return false, nil
	}

	b.blooms[key] = bl
	return true, nil
}

func (b *InMemoryBloom) GetBloom(key string) ([]byte, error) {
	b.blMu.Lock()
	defer b.blMu.Unlock()

	bloom, ok := b.blooms[key]
	if !ok {
		return []byte{}, nil
	}

	var buff bytes.Buffer
	writer := bufio.NewWriter(&buff)
	bloom.WriteTo(writer)
	return buff.Bytes(), nil
}
