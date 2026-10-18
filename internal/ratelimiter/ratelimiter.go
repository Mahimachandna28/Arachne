// Package ratelimiter provides a thread-safe token bucket rate limiter.
// It ensures workers don't hammer the same domain with too many requests.
package ratelimiter

import (
	"context"
	"sync"
	"time"
)

// RateLimiter controls how frequently requests can be made per domain.
// It uses a mutex to safely handle concurrent access from multiple goroutines.
type RateLimiter struct {
	mu             sync.Mutex
	limits         map[string]time.Time     // last request time per domain
	customInterval map[string]time.Duration // per-domain intervals (e.g. from robots.txt Crawl-delay or Retry-After)
	interval       time.Duration            // default minimum time between requests to same domain
}

// New creates a new RateLimiter with the given interval between requests.
func New(interval time.Duration) *RateLimiter {
	return &RateLimiter{
		limits:         make(map[string]time.Time),
		customInterval: make(map[string]time.Duration),
		interval:       interval,
	}
}

// SetInterval configures a custom rate limit interval for a specific domain.
func (r *RateLimiter) SetInterval(domain string, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.customInterval == nil {
		r.customInterval = make(map[string]time.Duration)
	}
	r.customInterval[domain] = d
}

// GetInterval returns the rate limit interval for a specific domain.
func (r *RateLimiter) GetInterval(domain string) time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	if d, ok := r.customInterval[domain]; ok {
		return d
	}
	return r.interval
}

// Wait blocks until it is safe to make a request to the given domain according
// to rate limit rules, or until the provided context is canceled.
func (r *RateLimiter) Wait(ctx context.Context, domain string) error {
	if ctx == nil {
		ctx = context.Background()
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		last, exists := r.limits[domain]
		interval := r.interval
		if d, ok := r.customInterval[domain]; ok {
			interval = d
		}

		now := time.Now()
		if !exists {
			r.limits[domain] = now
			return nil
		}

		next := last.Add(interval)
		if !now.Before(next) {
			r.limits[domain] = now
			return nil
		}

		// Interval not yet elapsed — release lock while sleeping,
		// cancellable via ctx.Done()
		waitDuration := next.Sub(now)
		r.mu.Unlock()

		select {
		case <-ctx.Done():
			r.mu.Lock()
			return ctx.Err()
		case <-time.After(waitDuration):
		}

		r.mu.Lock()
	}
}
