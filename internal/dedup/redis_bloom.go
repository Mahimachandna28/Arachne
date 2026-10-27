package dedup

import (
	"context"
	"math"
	"strings"

	"github.com/redis/go-redis/v9"
)

// RedisBloom implements BloomStore backed by RedisBloom module (BF.ADD) with automatic
// transparent fallback to Redis BitSet (SETBIT/GETBIT) for vanilla Redis servers.
type RedisBloom struct {
	client       *redis.Client
	n            uint64
	p            float64
	m            uint64 // bitset size for fallback
	k            uint   // hash function count for fallback
	supportsBf   bool
	checkedModule bool
}

// NewRedisBloom creates a Redis-backed Bloom store.
func NewRedisBloom(client *redis.Client, n uint64, p float64) *RedisBloom {
	if n <= 0 {
		n = 1000000
	}
	if p <= 0 || p >= 1.0 {
		p = 0.01
	}

	m := uint64(math.Ceil(-float64(n) * math.Log(p) / (math.Ln2 * math.Ln2)))
	if m < 64 {
		m = 64
	}
	k := uint(math.Round(float64(m) / float64(n) * math.Ln2))
	if k < 1 {
		k = 1
	}

	return &RedisBloom{
		client: client,
		n:      n,
		p:      p,
		m:      m,
		k:      k,
	}
}

// CheckOrAdd executes BF.ADD or falls back to Redis bitwise operations.
func (rb *RedisBloom) CheckOrAdd(ctx context.Context, key, hash string) (bool, error) {
	// 1. Try native RedisBloom command if not confirmed unsupported
	if !rb.checkedModule || rb.supportsBf {
		res, err := rb.client.Do(ctx, "BF.ADD", key, hash).Int()
		if err == nil {
			rb.checkedModule = true
			rb.supportsBf = true
			// BF.ADD returns 1 if item was added (did NOT exist before), 0 if it already existed
			alreadyExisted := (res == 0)
			return alreadyExisted, nil
		}

		if strings.Contains(strings.ToLower(err.Error()), "unknown command") {
			rb.checkedModule = true
			rb.supportsBf = false
			// Fall through to BitSet fallback
		} else {
			return false, err
		}
	}

	// 2. Fallback: Universal Redis BitSet with double hashing
	h1, h2 := hashes(hash)
	offsets := make([]int64, rb.k)
	for i := uint(0); i < rb.k; i++ {
		offsets[i] = int64((h1 + uint64(i)*h2) % rb.m)
	}

	// Check if all bits are already 1
	pipe := rb.client.Pipeline()
	getCmds := make([]*redis.IntCmd, len(offsets))
	for i, offset := range offsets {
		getCmds[i] = pipe.GetBit(ctx, key, offset)
	}
	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return false, err
	}

	allSet := true
	for _, cmd := range getCmds {
		if cmd.Val() == 0 {
			allSet = false
			break
		}
	}

	if allSet {
		return true, nil
	}

	// Set bits that were not 1
	setPipe := rb.client.Pipeline()
	for _, offset := range offsets {
		setPipe.SetBit(ctx, key, offset, 1)
	}
	_, err = setPipe.Exec(ctx)
	if err != nil {
		return false, err
	}

	return false, nil
}

// Reset deletes the Bloom filter key in Redis.
func (rb *RedisBloom) Reset(ctx context.Context, key string) error {
	return rb.client.Del(ctx, key).Err()
}

// Close closes the underlying Redis client.
func (rb *RedisBloom) Close() error {
	return rb.client.Close()
}
