package dedup

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"math"
	"sync"
)

// MemoryBloom is an in-memory, thread-safe Bloom filter implementing BloomStore.
type MemoryBloom struct {
	mu      sync.RWMutex
	filters map[string]*bitsetBloom
	n       uint64  // capacity
	p       float64 // false positive probability
}

// bitsetBloom holds the bit slice and parameters for a single key.
type bitsetBloom struct {
	bits []uint64
	m    uint64 // total bit count
	k    uint   // number of hash functions
}

// NewMemoryBloom creates an in-memory Bloom store.
// If n <= 0, defaults to 1,000,000. If p <= 0 || p >= 1, defaults to 0.01 (1%).
func NewMemoryBloom(n uint64, p float64) *MemoryBloom {
	if n <= 0 {
		n = 1000000
	}
	if p <= 0 || p >= 1.0 {
		p = 0.01
	}

	return &MemoryBloom{
		filters: make(map[string]*bitsetBloom),
		n:       n,
		p:       p,
	}
}

func newBitsetBloom(n uint64, p float64) *bitsetBloom {
	// m = - (n * ln(p)) / (ln(2)^2)
	m := uint64(math.Ceil(-float64(n) * math.Log(p) / (math.Ln2 * math.Ln2)))
	if m < 64 {
		m = 64
	}

	// k = (m / n) * ln(2)
	k := uint(math.Round(float64(m) / float64(n) * math.Ln2))
	if k < 1 {
		k = 1
	}

	wordCount := (m + 63) / 64
	return &bitsetBloom{
		bits: make([]uint64, wordCount),
		m:    m,
		k:    k,
	}
}

// hashes generates two 64-bit base hashes for Kirsch-Mitzenmacher double hashing.
func hashes(data string) (uint64, uint64) {
	sum := sha256.Sum256([]byte(data))
	h1 := binary.BigEndian.Uint64(sum[0:8])
	h2 := binary.BigEndian.Uint64(sum[8:16])
	if h2 == 0 {
		h2 = 1
	}
	return h1, h2
}

// CheckOrAdd checks if hash is present. If not present, sets all k bits and returns false.
func (mb *MemoryBloom) CheckOrAdd(ctx context.Context, key, hash string) (bool, error) {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	b, exists := mb.filters[key]
	if !exists {
		b = newBitsetBloom(mb.n, mb.p)
		mb.filters[key] = b
	}

	h1, h2 := hashes(hash)
	allSet := true

	for i := uint(0); i < b.k; i++ {
		idx := (h1 + uint64(i)*h2) % b.m
		word := idx / 64
		bit := idx % 64

		if (b.bits[word] & (1 << bit)) == 0 {
			allSet = false
			b.bits[word] |= (1 << bit)
		}
	}

	return allSet, nil
}

// Reset clears the filter for key.
func (mb *MemoryBloom) Reset(ctx context.Context, key string) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	delete(mb.filters, key)
	return nil
}
