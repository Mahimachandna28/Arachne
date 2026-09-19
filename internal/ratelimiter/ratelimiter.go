// Package ratelimiter provides a thread-safe token bucket rate limiter.
// It ensures workers don't hammer the same domain with too many requests.
package ratelimiter

import (
	"sync"
	"time"
)

// RateLimiter controls how frequently requests can be made per domain.
// It uses a mutex to safely handle concurrent access from multiple goroutines.
type RateLimiter struct {
	mu             sync.Mutex
	limits         map[string]time.Time          // last request time per domain
	customInterval map[string]time.Duration      // per-domain intervals (e.g. from robots.txt Crawl-delay)
	interval       time.Duration                 // default minimum time between requests to same domain
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

// Wait blocks the calling goroutine until it is safe to make a request
// to the given domain, enforcing the rate limit.
func (r *RateLimiter) Wait(domain string) {
	r.mu.Lock()
	last, exists := r.limits[domain]
	interval := r.interval
	if d, ok := r.customInterval[domain]; ok {
		interval = d
	}
	now := time.Now()

	if exists {
		next := last.Add(interval)
		if now.Before(next) {
			// Need to wait — release lock while sleeping to avoid blocking others
			waitDuration := next.Sub(now)
			r.mu.Unlock()
			time.Sleep(waitDuration)
			r.mu.Lock()
		}
	}

	r.limits[domain] = time.Now()
	r.mu.Unlock()
}
