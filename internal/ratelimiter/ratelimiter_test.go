package ratelimiter

import (
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
