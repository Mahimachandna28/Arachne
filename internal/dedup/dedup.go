// Package dedup implements two-tier URL deduplication to eliminate database read bottlenecks
// while mathematically preventing URL loss from Bloom filter false positives.
package dedup

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/mahimachandna28/webcrawler/internal/storage"
)

// Metrics records performance counters for the two-tier deduplicator.
type Metrics struct {
	TotalChecked            int64 `json:"total_checked"`
	Tier1Negatives          int64 `json:"tier1_negatives"`           // URLs definitively not seen (bypassed DB)
	Tier2Collisions         int64 `json:"tier2_collisions"`          // Bloom filter hits requiring DB verification
	Tier2Duplicates         int64 `json:"tier2_duplicates"`          // True duplicates confirmed by DB and dropped
	FalsePositivesRecovered int64 `json:"false_positives_recovered"` // Bloom false positives saved from drop
}

// ExactVerifier checks persistent storage (PostgreSQL) for confirmed URL existence.
type ExactVerifier interface {
	IsVisited(ctx context.Context, jobID, normalizedHash string) (bool, error)
}

// BloomStore defines the contract for Tier 1 fast negative filters (RedisBloom or In-Memory BitSet).
type BloomStore interface {
	// CheckOrAdd checks if hash is present in the bloom filter.
	// If NOT present, it adds the hash and returns alreadyExisted = false.
	// If present (or potential false positive), it returns alreadyExisted = true.
	CheckOrAdd(ctx context.Context, key, hash string) (alreadyExisted bool, err error)

	// Reset clears the filter for the given key.
	Reset(ctx context.Context, key string) error
}

// Deduplicator decides whether a newly discovered URL should be admitted to the crawler frontier.
type Deduplicator interface {
	// ShouldCrawl evaluates a raw URL. Returns true if the URL should be crawled, false if it is a duplicate.
	ShouldCrawl(ctx context.Context, jobID, rawURL string) (bool, error)

	// Metrics returns the real-time deduplication metrics.
	Metrics() Metrics

	// Reset resets the deduplicator state for a job.
	Reset(ctx context.Context, jobID string) error

	// Close cleans up resources.
	Close() error
}

// TwoTierDeduplicator implements high-throughput deduplication with Bloom filter + database verification.
type TwoTierDeduplicator struct {
	bloom    BloomStore
	verifier ExactVerifier

	totalChecked            atomic.Int64
	tier1Negatives          atomic.Int64
	tier2Collisions         atomic.Int64
	tier2Duplicates         atomic.Int64
	falsePositivesRecovered atomic.Int64
}

// NewTwoTier creates a new TwoTierDeduplicator with a BloomStore (Tier 1) and an ExactVerifier (Tier 2).
func NewTwoTier(bloom BloomStore, verifier ExactVerifier) *TwoTierDeduplicator {
	return &TwoTierDeduplicator{
		bloom:    bloom,
		verifier: verifier,
	}
}

// BloomKey formats the Redis/Bloom filter key for a specific crawl job.
func BloomKey(jobID string) string {
	if jobID == "" {
		jobID = "default"
	}
	return fmt.Sprintf("frontier:bloom:%s", jobID)
}

// ShouldCrawl checks rawURL against Tier 1 (Bloom Filter). If Bloom returns negative (0),
// the URL is guaranteed unvisited and admitted immediately. If Bloom returns positive (1),
// it queries Tier 2 (PostgreSQL) to differentiate true duplicates from Bloom false positives.
func (d *TwoTierDeduplicator) ShouldCrawl(ctx context.Context, jobID, rawURL string) (bool, error) {
	d.totalChecked.Add(1)

	urlHash := storage.HashURL(rawURL)
	key := BloomKey(jobID)

	// Tier 1: Fast Negative Filter
	alreadyExisted, err := d.bloom.CheckOrAdd(ctx, key, urlHash)
	if err != nil {
		// If bloom filter fails, fail-safe to Tier 2 verifier if available
		if d.verifier != nil {
			visited, vErr := d.verifier.IsVisited(ctx, jobID, urlHash)
			if vErr != nil {
				return false, fmt.Errorf("bloom failed (%v) and verifier failed: %w", err, vErr)
			}
			return !visited, nil
		}
		return false, fmt.Errorf("bloom filter check failed: %w", err)
	}

	// Case 1: Bloom says NOT seen -> Definitely unvisited (Tier 1 Negative)
	if !alreadyExisted {
		d.tier1Negatives.Add(1)
		return true, nil
	}

	// Case 2: Bloom says MIGHT be seen -> Collision detected (Tier 2 Verification)
	d.tier2Collisions.Add(1)

	// If no Tier 2 verifier is configured, assume duplicate
	if d.verifier == nil {
		d.tier2Duplicates.Add(1)
		return false, nil
	}

	// Query PostgreSQL for exact verification
	visited, err := d.verifier.IsVisited(ctx, jobID, urlHash)
	if err != nil {
		return false, fmt.Errorf("tier 2 verifier error: %w", err)
	}

	if visited {
		// Confirmed true duplicate -> Discard
		d.tier2Duplicates.Add(1)
		return false, nil
	}

	// Bloom False Positive recovered! The URL was NEVER visited.
	// Admit to frontier so link starvation is prevented!
	d.falsePositivesRecovered.Add(1)
	return true, nil
}

// Metrics returns snapshot counters.
func (d *TwoTierDeduplicator) Metrics() Metrics {
	return Metrics{
		TotalChecked:            d.totalChecked.Load(),
		Tier1Negatives:          d.tier1Negatives.Load(),
		Tier2Collisions:         d.tier2Collisions.Load(),
		Tier2Duplicates:         d.tier2Duplicates.Load(),
		FalsePositivesRecovered: d.falsePositivesRecovered.Load(),
	}
}

// Reset resets the bloom filter for the job.
func (d *TwoTierDeduplicator) Reset(ctx context.Context, jobID string) error {
	return d.bloom.Reset(ctx, BloomKey(jobID))
}

// Close closes the deduplicator.
func (d *TwoTierDeduplicator) Close() error {
	return nil
}
