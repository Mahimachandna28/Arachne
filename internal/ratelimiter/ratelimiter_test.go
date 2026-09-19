package ratelimiter

import (
	"sort"
	"sync"
	"testing"
	"time"
)

func TestRateLimiter_SingleDomain(t *testing.T) {
	interval := 100 * time.Millisecond
	rl := New(interval)

	start := time.Now()
	rl.Wait("example.com")
	rl.Wait("example.com") // Second call should be delayed
	elapsed := time.Since(start)

	if elapsed < interval {
		t.Errorf("Expected at least %v delay, got %v", interval, elapsed)
	}
}

func TestRateLimiter_DifferentDomains(t *testing.T) {
	interval := 200 * time.Millisecond
	rl := New(interval)

	start := time.Now()
	rl.Wait("example.com")
	rl.Wait("other.com") // Different domain — should NOT be delayed
	elapsed := time.Since(start)

	// Should complete well within the interval since domains are independent
	if elapsed >= interval {
		t.Errorf("Different domains should not rate-limit each other, elapsed: %v", elapsed)
	}
}

func TestRateLimiter_ConcurrentAccess(t *testing.T) {
	// Tests that the mutex prevents race conditions under concurrent access
	rl := New(10 * time.Millisecond)
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rl.Wait("concurrent.com")
		}()
	}

	wg.Wait() // Should complete without race conditions (run with -race to verify)
}

func TestRateLimiter_ConcurrentIntervalGuarantee(t *testing.T) {
	// Verifies that under concurrent calls to the same domain,
	// each goroutine's exit is spaced by at least the interval.
	interval := 40 * time.Millisecond
	rl := New(interval)

	const n = 5
	timestamps := make([]time.Time, 0, n)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rl.Wait("same-domain.com")
			mu.Lock()
			timestamps = append(timestamps, time.Now())
			mu.Unlock()
		}()
	}

	wg.Wait()

	if len(timestamps) != n {
		t.Fatalf("Expected %d timestamps, got %d", n, len(timestamps))
	}

	sort.Slice(timestamps, func(i, j int) bool {
		return timestamps[i].Before(timestamps[j])
	})

	tolerance := 5 * time.Millisecond
	for i := 1; i < len(timestamps); i++ {
		diff := timestamps[i].Sub(timestamps[i-1])
		if diff < interval-tolerance {
			t.Errorf("Interval between requests %d and %d was %v, expected at least %v", i-1, i, diff, interval-tolerance)
		}
	}
}
