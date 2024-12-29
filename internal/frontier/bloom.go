package frontier

import (
	"bufio"
	"bytes"
	"sync"

	boom "github.com/tylertreat/BoomFilters"
)

type bloom struct {
	blooms map[string]*boom.ScalableBloomFilter
	blMu   sync.Mutex
}

func newBloom() *bloom {
	return &bloom{
		blooms: make(map[string]*boom.ScalableBloomFilter),
	}
}

func (b *bloom) addBloom(key string, entry []byte) {
	b.blMu.Lock()
	defer b.blMu.Unlock()
	bloom, ok := b.blooms[key]
	if !ok {
		bloom = boom.NewDefaultScalableBloomFilter(0.01)
		b.blooms[key] = bloom
	}
	bloom.Add(entry)
}

func (b *bloom) checkBloom(key string, entry []byte) bool {
	b.blMu.Lock()
	defer b.blMu.Unlock()

	bloom, ok := b.blooms[key]
	if !ok {
		return false
	}

	return bloom.Test(entry)
}

func (b *bloom) setBloom(key string, bloom []byte) { //this library doesn't allow for merging two bloom filters
	b.blMu.Lock()
	defer b.blMu.Unlock()

	r := bytes.NewReader(bloom)
	bl := boom.NewDefaultScalableBloomFilter(0.1)
	_, err := bl.ReadFrom(r)
	if err != nil {
		return
	}

	b.blooms[key] = bl
}

func (b *bloom) getBloom(key string) []byte {
	b.blMu.Lock()
	defer b.blMu.Unlock()

	bloom, ok := b.blooms[key]
	if !ok {
		return []byte{}
	}

	var buff bytes.Buffer
	writer := bufio.NewWriter(&buff)
	bloom.WriteTo(writer)
	return buff.Bytes()
}
