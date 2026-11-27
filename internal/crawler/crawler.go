// Package crawler implements the core concurrent web crawling logic.
//
// Architecture:
//   - enqueue() normalizes, filters (spider traps, robots, dedup, page caps)
//     and pushes jobs into a per-host scheduler. It never blocks.
//   - The scheduler (internal/scheduler) hands a worker only a URL whose host
//     may be contacted now, so politeness never stalls other hosts.
//   - N worker goroutines fetch, parse, emit results and enqueue children.
//   - Transient failures (network errors, 5xx, 429) are rescheduled with
//     exponential backoff instead of sleeping inside the worker.
//   - The crawl ends exactly when nothing is queued, in flight, or held by a
//     producer (seeding, sitemap discovery).
package crawler

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/Mahimachandna28/Arachne/internal/blobstore"
	"github.com/Mahimachandna28/Arachne/internal/dedup"
	"github.com/Mahimachandna28/Arachne/internal/netguard"
	"github.com/Mahimachandna28/Arachne/internal/parser"
	"github.com/Mahimachandna28/Arachne/internal/ratelimiter"
	"github.com/Mahimachandna28/Arachne/internal/result"
	"github.com/Mahimachandna28/Arachne/internal/robots"
	"github.com/Mahimachandna28/Arachne/internal/scheduler"
	"github.com/Mahimachandna28/Arachne/internal/sitemap"
	"github.com/Mahimachandna28/Arachne/internal/storage"
	"github.com/Mahimachandna28/Arachne/internal/urlnorm"
)

// DefaultUserAgent is the default User-Agent header identifying the crawler and repository.
const DefaultUserAgent = "ArachneCrawlerBot/1.0 (+https://github.com/Mahimachandna28/Arachne)"

// Config holds the configuration for a crawl run.
type Config struct {
	Seeds             []string           // Starting URLs
	Workers           int                // Number of concurrent worker goroutines
	MaxDepth          int                // Maximum link depth to follow
	RateLimit         time.Duration      // Minimum time between requests to the same host
	Timeout           time.Duration      // HTTP request timeout
	MaxPages          int                // Maximum total pages to crawl (0 = unlimited)
	MaxPagesPerDomain int                // Maximum pages to crawl per host (0 = unlimited)
	MaxRedirects      int                // Maximum redirects to follow (default: 10)
	MaxBodyBytes      int64              // Maximum bytes to read per page (default: 1MB)
	StayOnDomain      bool               // If true, only follow links on the same host as the parent page
	Retries           int                // Maximum retry attempts on transient failure (network error, 5xx, 429)
	UserAgent         string             // User-Agent request header (defaults to DefaultUserAgent)
	AllowPrivateIPs   bool               // If true, disables the SSRF guard (e.g. for tests against localhost)
	JobID             string             // Optional Job ID in PostgreSQL storage
	DB                *storage.DB        // Optional database connection for streaming persistence
	BlobStore         blobstore.Store    // Optional object store for archiving raw compressed HTML payloads
	Dedup             dedup.Deduplicator // Optional shared deduplicator (Bloom + DB), consulted after the in-memory set

	// OnPage, if set, receives every result as it is produced and Run returns
	// nil instead of accumulating results in memory. Called from a single
	// goroutine, so it need not be thread-safe.
	OnPage func(result.Page)

	// Quiet suppresses per-URL progress output.
	Quiet bool
}

// Crawler orchestrates the worker pool and manages shared state.
type Crawler struct {
	config   Config
	sched    *scheduler.Scheduler
	resultCh chan result.Page
	limiter  *ratelimiter.RateLimiter // per-host interval registry (default, Crawl-delay, Retry-After)
	client   *http.Client

	visitedMu sync.Mutex
	visited   map[uint64]struct{} // 64-bit fingerprints of normalized URLs

	countMu     sync.Mutex
	pageCount   int
	domainCount map[string]int

	robotsMu    sync.RWMutex
	robotsCache map[string]*robots.Policy
	robotsGroup singleflight.Group
}

// New creates a new Crawler with the given configuration.
func New(cfg Config) *Crawler {
	if cfg.UserAgent == "" {
		cfg.UserAgent = DefaultUserAgent
	}
	if cfg.MaxRedirects <= 0 {
		cfg.MaxRedirects = 10
	}
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = 1 << 20 // 1MB default
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}

	guard := netguard.New(netguard.Config{
		AllowPrivate: cfg.AllowPrivateIPs,
		DialTimeout:  cfg.Timeout,
	})

	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           guard.DialContext,
		MaxIdleConns:          2000,
		MaxIdleConnsPerHost:   50,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	maxRedirects := cfg.MaxRedirects
	client := &http.Client{
		Timeout:   cfg.Timeout,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= maxRedirects {
				return fmt.Errorf("stopped after %d redirects", maxRedirects)
			}
			return nil
		},
	}

	limiter := ratelimiter.New(cfg.RateLimit)
	return &Crawler{
		config:      cfg,
		sched:       scheduler.New(limiter.GetInterval),
		resultCh:    make(chan result.Page, cfg.Workers*4),
		limiter:     limiter,
		client:      client,
		visited:     make(map[uint64]struct{}),
		domainCount: make(map[string]int),
		robotsCache: make(map[string]*robots.Policy),
	}
}

func (c *Crawler) logf(format string, args ...any) {
	if !c.config.Quiet {
		fmt.Printf(format, args...)
	}
}

// Run starts the crawl and blocks until it completes or ctx is cancelled.
// It returns all results, unless Config.OnPage is set (then it returns nil).
func (c *Crawler) Run(ctx context.Context) []result.Page {
	if ctx == nil {
		ctx = context.Background()
	}
	c.logf("🚀 Starting crawler with %d workers\n", c.config.Workers)
	c.logf("📋 Seeds: %v\n\n", c.config.Seeds)

	var results []result.Page
	var total int
	collectorDone := make(chan struct{})

	var batchWriter *storage.BatchWriter
	if c.config.DB != nil && c.config.JobID != "" {
		batchWriter = storage.NewBatchWriter(c.config.DB, c.config.JobID, storage.BatchWriterConfig{})
	}

	go func() {
		defer close(collectorDone)
		for page := range c.resultCh {
			total++
			if c.config.OnPage != nil {
				c.config.OnPage(page)
			} else {
				results = append(results, page)
			}
			if batchWriter != nil {
				batchWriter.Submit(page)
			}
		}
		if batchWriter != nil {
			closeErr := batchWriter.Close()
			saved, errs := batchWriter.Stats()
			status := storage.JobStatusCompleted
			stats := map[string]interface{}{"pages_crawled": saved, "errors": errs}
			switch {
			case closeErr != nil:
				status = storage.JobStatusFailed
				stats["persist_error"] = closeErr.Error()
				c.logf("⚠️  %v\n", closeErr)
			case ctx.Err() != nil:
				status = storage.JobStatusStopped
			}
			if err := c.config.DB.FinishJob(context.Background(), c.config.JobID, status, stats); err != nil {
				c.logf("⚠️  failed to record job completion: %v\n", err)
			}
		}
	}()

	var workerWg sync.WaitGroup
	for i := 0; i < c.config.Workers; i++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			c.worker(ctx)
		}()
	}

	// Hold the scheduler open while seeding so idle workers don't conclude
	// the crawl is finished before the first URL arrives.
	c.sched.Hold()
	for _, seedURL := range c.config.Seeds {
		if ctx.Err() != nil {
			break
		}
		c.enqueue(ctx, seedURL, 0)
	}
	c.sched.Release()

	workerWg.Wait()
	close(c.resultCh)
	<-collectorDone

	if ctx.Err() != nil {
		c.logf("\n⚠️  Crawl stopped: %v\n", ctx.Err())
	}
	c.logf("\n✅ Crawl complete. Total pages crawled: %d\n", total)
	return results
}

// worker pulls ready jobs from the scheduler until the crawl ends.
func (c *Crawler) worker(ctx context.Context) {
	for {
		j, ok := c.sched.Next(ctx)
		if !ok {
			return
		}
		c.process(ctx, j)
	}
}

func (c *Crawler) process(ctx context.Context, j scheduler.Job) {
	u, err := url.Parse(j.URL)
	if err != nil {
		c.sched.Done(j)
		return
	}

	// robots.txt is resolved lazily, once per host, by the first worker that
	// is scheduled for that host. URLs admitted before the policy was known
	// are filtered here and their page-budget slot is returned.
	if !c.policyFor(ctx, u).IsAllowed(u.Path) {
		c.releaseSlot(j.Host)
		c.sched.Done(j)
		return
	}

	if j.Attempt == 0 {
		c.logf("  [depth %d] Fetching: %s\n", j.Depth, j.URL)
	} else {
		c.logf("  [depth %d] Retrying (%d/%d): %s\n", j.Depth, j.Attempt, c.config.Retries, j.URL)
	}

	out := c.fetch(ctx, j.URL)
	if out.retryable && j.Attempt < c.config.Retries && ctx.Err() == nil {
		delay := out.retryAfter
		if delay <= 0 {
			delay = backoff(j.Attempt + 1)
		}
		c.sched.Retry(j, delay)
		return
	}

	page := out.page
	page.Depth = j.Depth
	select {
	case c.resultCh <- page:
	case <-ctx.Done():
		c.sched.Done(j)
		return
	}

	// Children are pushed before Done so the scheduler never sees an empty
	// frontier while this job's descendants are still being discovered.
	if page.Error == "" && !out.noFollow && j.Depth < c.config.MaxDepth {
		for _, link := range page.Links {
			if ctx.Err() != nil {
				break
			}
			if resolved := c.resolveURL(j.URL, link); resolved != "" {
				c.enqueue(ctx, resolved, j.Depth+1)
			}
		}
	}
	c.sched.Done(j)
}

// backoff returns a full-jitter exponential delay: rand(0, min(3s, 50ms*2^(attempt-1))).
func backoff(attempt int) time.Duration {
	const base, ceiling = 50 * time.Millisecond, 3 * time.Second
	limit := base << (attempt - 1)
	if limit > ceiling || limit <= 0 {
		limit = ceiling
	}
	return time.Duration(rand.Int63n(int64(limit)))
}

type fetchOutcome struct {
	page       result.Page
	noFollow   bool
	retryable  bool
	retryAfter time.Duration
}

// fetch performs a single HTTP GET and classifies the outcome. Retry policy
// is applied by the caller through the scheduler.
func (c *Crawler) fetch(ctx context.Context, rawURL string) fetchOutcome {
	fail := func(code int, msg string, retryable bool) fetchOutcome {
		return fetchOutcome{page: result.Page{URL: rawURL, StatusCode: code, Error: msg}, retryable: retryable}
	}

	if err := ctx.Err(); err != nil {
		return fail(0, err.Error(), false)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return fail(0, err.Error(), false)
	}
	req.Header.Set("User-Agent", c.config.UserAgent)

	resp, err := c.client.Do(req)
	if err != nil {
		blocked := errors.Is(err, netguard.ErrBlockedPrivateIP) || errors.Is(err, netguard.ErrNoSafeIPFound)
		return fail(0, err.Error(), !blocked && ctx.Err() == nil)
	}
	defer resp.Body.Close()

	host := urlnorm.Host(rawURL)

	// 429 Too Many Requests / 503 Service Unavailable: honour Retry-After and
	// slow the host down for the rest of the crawl.
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable {
		header := resp.Header.Get("Retry-After")
		retryAfter := parseRetryAfter(header)
		if retryAfter > 0 {
			c.limiter.SetInterval(host, retryAfter)
		}
		out := fail(resp.StatusCode, fmt.Sprintf("HTTP %d: %s (Retry-After: %s)", resp.StatusCode, http.StatusText(resp.StatusCode), header), true)
		out.retryAfter = retryAfter
		return out
	}

	if resp.StatusCode >= 500 && resp.StatusCode <= 599 {
		return fail(resp.StatusCode, fmt.Sprintf("HTTP %d: %s", resp.StatusCode, http.StatusText(resp.StatusCode)), true)
	}

	contentType := strings.TrimSpace(resp.Header.Get("Content-Type"))
	lower := strings.ToLower(contentType)
	if !strings.HasPrefix(lower, "text/html") && !strings.HasPrefix(lower, "application/xhtml+xml") {
		return fail(resp.StatusCode, fmt.Sprintf("skipped non-HTML content type: %q", contentType), false)
	}

	if resp.ContentLength > c.config.MaxBodyBytes {
		return fail(resp.StatusCode, fmt.Sprintf("skipped response exceeding max size: %d bytes (limit %d bytes)", resp.ContentLength, c.config.MaxBodyBytes), false)
	}

	htmlStr, err := decodeHTMLBody(resp.Body, contentType, c.config.MaxBodyBytes)
	if err != nil {
		return fail(resp.StatusCode, err.Error(), false)
	}

	meta := parser.ExtractMetadata(htmlStr)

	var payloadPath string
	if c.config.BlobStore != nil {
		if compressed, compErr := blobstore.CompressGzip([]byte(htmlStr)); compErr == nil {
			key := blobstore.FormatPayloadKey(c.config.JobID, storage.HashURL(rawURL))
			if putErr := c.config.BlobStore.Put(ctx, key, compressed, "application/gzip"); putErr == nil {
				payloadPath = key
			}
		}
	}

	return fetchOutcome{
		page: result.Page{
			URL:          rawURL,
			Title:        meta.Title,
			CanonicalURL: meta.Canonical,
			Description:  meta.Description,
			PayloadPath:  payloadPath,
			Links:        meta.Links,
			StatusCode:   resp.StatusCode,
		},
		noFollow: meta.NoFollow,
	}
}

// enqueue normalizes rawURL, applies admission rules and hands the job to
// the scheduler. It never blocks on the network or on queue capacity.
func (c *Crawler) enqueue(ctx context.Context, rawURL string, depth int) {
	if ctx.Err() != nil {
		return
	}

	norm, err := urlnorm.Normalize(rawURL)
	if err != nil {
		return
	}
	u, err := url.Parse(norm)
	if err != nil {
		return
	}
	if isSpiderTrap(u) {
		return
	}
	host := u.Host

	// Fast-path robots check when the host's policy is already cached.
	c.robotsMu.RLock()
	policy, known := c.robotsCache[host]
	c.robotsMu.RUnlock()
	if known && !policy.IsAllowed(u.Path) {
		return
	}

	// Deduplication, tier 0: in-process set of 64-bit fingerprints. This is
	// the authority for URLs discovered but not yet persisted; the external
	// deduplicator's DB tier only sees URLs after the batch writer flushes,
	// so it cannot catch a URL rediscovered while its first fetch is pending.
	fp := urlnorm.Hash64(norm)
	c.visitedMu.Lock()
	if _, seen := c.visited[fp]; seen {
		c.visitedMu.Unlock()
		return
	}
	c.visited[fp] = struct{}{}
	c.visitedMu.Unlock()

	// Tiers 1+2: shared Bloom filter + exact DB check, which survive restarts
	// and are shared across crawler processes.
	if c.config.Dedup != nil {
		ok, err := c.config.Dedup.ShouldCrawl(ctx, c.config.JobID, norm)
		if err != nil || !ok {
			return
		}
	}

	if !c.reserveSlot(host) {
		return
	}

	c.sched.Push(scheduler.Job{URL: norm, Host: host, Depth: depth})
}

// reserveSlot claims one unit of the global and per-host page budgets.
func (c *Crawler) reserveSlot(host string) bool {
	c.countMu.Lock()
	defer c.countMu.Unlock()
	if c.config.MaxPagesPerDomain > 0 && c.domainCount[host] >= c.config.MaxPagesPerDomain {
		return false
	}
	if c.config.MaxPages > 0 && c.pageCount >= c.config.MaxPages {
		return false
	}
	c.domainCount[host]++
	c.pageCount++
	return true
}

// releaseSlot returns a budget unit for a job that was admitted but never fetched.
func (c *Crawler) releaseSlot(host string) {
	c.countMu.Lock()
	c.domainCount[host]--
	c.pageCount--
	c.countMu.Unlock()
}

// policyFor returns the cached robots.txt policy for u's host, fetching it
// exactly once per host even under concurrent demand (singleflight).
func (c *Crawler) policyFor(ctx context.Context, u *url.URL) *robots.Policy {
	host := strings.ToLower(u.Host)
	c.robotsMu.RLock()
	p, ok := c.robotsCache[host]
	c.robotsMu.RUnlock()
	if ok {
		return p
	}

	v, _, _ := c.robotsGroup.Do(host, func() (any, error) {
		c.robotsMu.RLock()
		p, ok := c.robotsCache[host]
		c.robotsMu.RUnlock()
		if ok {
			return p, nil
		}

		p = c.fetchRobotsPolicy(ctx, u.Scheme, host)
		if ctx.Err() != nil {
			return p, nil // don't cache a policy produced by an aborted fetch
		}
		if p.CrawlDelay > 0 {
			c.limiter.SetInterval(host, p.CrawlDelay)
		}
		c.robotsMu.Lock()
		c.robotsCache[host] = p
		c.robotsMu.Unlock()

		if len(p.Sitemaps) > 0 {
			c.sched.Hold()
			go func() {
				defer c.sched.Release()
				c.discoverSitemapURLs(ctx, p.Sitemaps)
			}()
		}
		return p, nil
	})
	return v.(*robots.Policy)
}

func (c *Crawler) fetchRobotsPolicy(ctx context.Context, scheme, host string) *robots.Policy {
	if scheme == "" {
		scheme = "http"
	}
	robotsURL := fmt.Sprintf("%s://%s/robots.txt", scheme, host)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, robotsURL, nil)
	if err != nil {
		return &robots.Policy{}
	}
	req.Header.Set("User-Agent", c.config.UserAgent)

	resp, err := c.client.Do(req)
	if err != nil {
		return &robots.Policy{}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return &robots.Policy{}
	}

	body, err := readLimited(resp.Body, 512*1024)
	if err != nil {
		return &robots.Policy{}
	}
	return robots.Parse(body, c.config.UserAgent)
}

// discoverSitemapURLs fetches declared sitemaps and enqueues discovered URLs.
func (c *Crawler) discoverSitemapURLs(ctx context.Context, sitemapURLs []string) {
	for _, smURL := range sitemapURLs {
		if ctx.Err() != nil {
			return
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, smURL, nil)
		if err != nil {
			continue
		}
		req.Header.Set("User-Agent", c.config.UserAgent)

		resp, err := c.client.Do(req)
		if err != nil {
			continue
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			continue
		}

		res, err := sitemap.Parse(resp.Body)
		resp.Body.Close()
		if err == nil && res != nil {
			for _, pageURL := range res.PageURLs {
				c.enqueue(ctx, pageURL, 1)
			}
		}
	}
}

// resolveURL converts a link found on base into a normalized absolute URL,
// or "" if it should not be followed.
func (c *Crawler) resolveURL(base, link string) string {
	baseURL, err := url.Parse(base)
	if err != nil {
		return ""
	}
	linkURL, err := url.Parse(strings.TrimSpace(link))
	if err != nil {
		return ""
	}
	resolved := baseURL.ResolveReference(linkURL)

	norm, err := urlnorm.NormalizeURL(resolved)
	if err != nil {
		return ""
	}
	if c.config.StayOnDomain {
		baseNorm, err := urlnorm.NormalizeURL(baseURL)
		if err != nil || urlnorm.Host(norm) != urlnorm.Host(baseNorm) {
			return ""
		}
	}
	return norm
}
