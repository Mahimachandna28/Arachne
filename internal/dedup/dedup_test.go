package dedup

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/mahimachandna28/webcrawler/internal/storage"
)

// mockVerifier implements ExactVerifier with an in-memory map.
type mockVerifier struct {
	mu         sync.Mutex
	visited    map[string]bool
	queryCount atomic.Int64
}

func newMockVerifier() *mockVerifier {
	return &mockVerifier{
		visited: make(map[string]bool),
	}
}

func (m *mockVerifier) IsVisited(ctx context.Context, jobID, hash string) (bool, error) {
	m.queryCount.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.visited[hash], nil
}

func (m *mockVerifier) MarkVisited(hash string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.visited[hash] = true
}

func TestMemoryBloom_Accuracy(t *testing.T) {
	ctx := context.Background()
	bloom := NewMemoryBloom(5000, 0.01)
	key := "test:bloom"

	// 1. Insert 1,000 items, ensure none existed initially
	for i := 0; i < 1000; i++ {
		hash := fmt.Sprintf("hash_%d", i)
		existed, err := bloom.CheckOrAdd(ctx, key, hash)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if existed {
			t.Errorf("expected hash %s to not exist initially", hash)
		}
	}

	// 2. Query the same 1,000 items, all must exist
	for i := 0; i < 1000; i++ {
		hash := fmt.Sprintf("hash_%d", i)
		existed, err := bloom.CheckOrAdd(ctx, key, hash)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !existed {
			t.Errorf("expected hash %s to exist", hash)
		}
	}

	// 3. Test false positive rate on 2,000 unseen items
	falsePositives := 0
	for i := 1000; i < 3000; i++ {
		hash := fmt.Sprintf("unseen_hash_%d", i)
		existed, err := bloom.CheckOrAdd(ctx, key, hash)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if existed {
			falsePositives++
		}
	}

	fpRate := float64(falsePositives) / 2000.0
	t.Logf("Observed false positive count: %d / 2000 (rate: %.4f)", falsePositives, fpRate)
	// Target is 1% (0.01), allow slight statistical variance under 3%
	if fpRate > 0.03 {
		t.Errorf("false positive rate too high: got %.4f, want <= 0.03", fpRate)
	}
}

func TestTwoTier_NegativeBypassesDB(t *testing.T) {
	ctx := context.Background()
	bloom := NewMemoryBloom(1000, 0.01)
	verifier := newMockVerifier()

	dedup := NewTwoTier(bloom, verifier)

	// Crawl brand new URL
	shouldCrawl, err := dedup.ShouldCrawl(ctx, "job-1", "https://example.com/fresh-page")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !shouldCrawl {
		t.Errorf("expected shouldCrawl = true for fresh URL")
	}

	// DB should NOT have been touched at all!
	if count := verifier.queryCount.Load(); count != 0 {
		t.Errorf("expected 0 DB queries for Tier 1 negative, got %d", count)
	}

	metrics := dedup.Metrics()
	if metrics.Tier1Negatives != 1 {
		t.Errorf("expected Tier1Negatives = 1, got %d", metrics.Tier1Negatives)
	}
	if metrics.Tier2Collisions != 0 {
		t.Errorf("expected Tier2Collisions = 0, got %d", metrics.Tier2Collisions)
	}
}

func TestTwoTier_DuplicateConfirmedByDB(t *testing.T) {
	ctx := context.Background()
	bloom := NewMemoryBloom(1000, 0.01)
	verifier := newMockVerifier()

	dedup := NewTwoTier(bloom, verifier)
	targetURL := "https://example.com/visited-page"

	// First crawl
	shouldCrawl, err := dedup.ShouldCrawl(ctx, "job-1", targetURL)
	if err != nil || !shouldCrawl {
		t.Fatalf("expected first check to admit URL")
	}

	// Record in DB as completed
	verifier.MarkVisited(storage.HashURL(targetURL))

	// Second crawl of the same URL
	shouldCrawl2, err := dedup.ShouldCrawl(ctx, "job-1", targetURL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if shouldCrawl2 {
		t.Errorf("expected duplicate URL to be rejected")
	}

	// Verified DB was queried once for the collision
	if count := verifier.queryCount.Load(); count != 1 {
		t.Errorf("expected 1 DB query, got %d", count)
	}

	metrics := dedup.Metrics()
	if metrics.Tier2Collisions != 1 {
		t.Errorf("expected Tier2Collisions = 1, got %d", metrics.Tier2Collisions)
	}
	if metrics.Tier2Duplicates != 1 {
		t.Errorf("expected Tier2Duplicates = 1, got %d", metrics.Tier2Duplicates)
	}
}

// forcedCollisionBloom simulates a Bloom filter that always returns a collision (alreadyExisted = true).
type forcedCollisionBloom struct{}

func (f *forcedCollisionBloom) CheckOrAdd(ctx context.Context, key, hash string) (bool, error) {
	return true, nil // Always report positive (simulate false positive)
}

func (f *forcedCollisionBloom) Reset(ctx context.Context, key string) error {
	return nil
}

func TestTwoTier_FalsePositiveRecovered(t *testing.T) {
	ctx := context.Background()
	bloom := &forcedCollisionBloom{} // Always triggers collision
	verifier := newMockVerifier()    // DB is empty, so URL is NOT visited

	dedup := NewTwoTier(bloom, verifier)

	// URL has never been visited, but Bloom suffered a false positive
	shouldCrawl, err := dedup.ShouldCrawl(ctx, "job-1", "https://example.com/unvisited-url")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should be recovered and admitted to crawl!
	if !shouldCrawl {
		t.Errorf("expected false positive to be recovered and admitted to crawl")
	}

	metrics := dedup.Metrics()
	if metrics.Tier2Collisions != 1 {
		t.Errorf("expected Tier2Collisions = 1, got %d", metrics.Tier2Collisions)
	}
	if metrics.FalsePositivesRecovered != 1 {
		t.Errorf("expected FalsePositivesRecovered = 1, got %d", metrics.FalsePositivesRecovered)
	}
	if metrics.Tier2Duplicates != 0 {
		t.Errorf("expected Tier2Duplicates = 0, got %d", metrics.Tier2Duplicates)
	}
}

func TestTwoTier_Concurrency(t *testing.T) {
	ctx := context.Background()
	bloom := NewMemoryBloom(10000, 0.01)
	verifier := newMockVerifier()
	dedup := NewTwoTier(bloom, verifier)

	var wg sync.WaitGroup
	workers := 10
	urlsPerWorker := 50

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < urlsPerWorker; i++ {
				u := fmt.Sprintf("https://domain%d.com/page/%d", workerID, i)
				_, _ = dedup.ShouldCrawl(ctx, "concurrency-job", u)
			}
		}(w)
	}

	wg.Wait()

	metrics := dedup.Metrics()
	if metrics.TotalChecked != int64(workers*urlsPerWorker) {
		t.Errorf("expected total checked %d, got %d", workers*urlsPerWorker, metrics.TotalChecked)
	}
}

func TestBloomKey(t *testing.T) {
	k := BloomKey("job-123")
	if k != "frontier:bloom:job-123" {
		t.Errorf("unexpected key: %s", k)
	}
	if BloomKey("") != "frontier:bloom:default" {
		t.Errorf("unexpected default key: %s", BloomKey(""))
	}
}
