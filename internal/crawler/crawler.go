// Package crawler implements the core concurrent web crawling logic.
// It uses a worker-pool pattern:
//   - A fixed pool of N goroutines (workers) reads URLs from a shared job channel
//   - A visited map (protected by a mutex) prevents duplicate crawling
//   - A rate limiter (mutex-backed) throttles requests per domain
//   - A sync.WaitGroup tracks in-flight jobs (not workers), so jobCh closes
//     automatically when the queue drains — no deadlock possible
package crawler

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/mahimachandna28/webcrawler/internal/parser"
	"github.com/mahimachandna28/webcrawler/internal/ratelimiter"
	"github.com/mahimachandna28/webcrawler/internal/result"
	"github.com/mahimachandna28/webcrawler/internal/robots"
)

// DefaultUserAgent is the default User-Agent header identifying the crawler and repository.
const DefaultUserAgent = "ArachneCrawlerBot/1.0 (+https://github.com/mahimachandna28/Arachne)"

// Config holds the configuration for a crawl run.
type Config struct {
	Seeds        []string      // Starting URLs
	Workers      int           // Number of concurrent worker goroutines
	MaxDepth     int           // Maximum link depth to follow
	RateLimit    time.Duration // Minimum time between requests to the same domain
	Timeout      time.Duration // HTTP request timeout
	MaxPages     int           // Maximum total pages to crawl (0 = unlimited)
	StayOnDomain bool          // If true, only follow links on the same domain as seed
	Retries      int           // Maximum retry attempts on transient failure (network error or 5xx)
	UserAgent    string        // User-Agent request header (defaults to DefaultUserAgent)
}

// job represents a single crawl task sent through the job channel.
type job struct {
	url   string
	depth int
}

// Crawler orchestrates the worker pool and manages shared state.
type Crawler struct {
	config      Config
	jobCh       chan job                  // Channel workers read jobs from
	resultCh    chan result.Page          // Channel workers send results to
	jobWg       sync.WaitGroup           // Tracks in-flight jobs (not workers)
	visited     map[string]bool          // Tracks visited URLs
	visitedMu   sync.Mutex               // Protects the visited map
	pageCount   int                      // Total pages crawled so far
	pageCountMu sync.Mutex               // Protects pageCount
	limiter     *ratelimiter.RateLimiter
	client      *http.Client
	robotsCache map[string]*robots.Policy // Thread-safe cache of robots.txt policy per domain
	robotsMu    sync.Mutex               // Protects robotsCache
}

// New creates a new Crawler with the given configuration.
func New(cfg Config) *Crawler {
	if cfg.UserAgent == "" {
		cfg.UserAgent = DefaultUserAgent
	}
	return &Crawler{
		config:      cfg,
		jobCh:       make(chan job, cfg.Workers*20), // Buffered to reduce blocking
		resultCh:    make(chan result.Page, cfg.Workers*20),
		visited:     make(map[string]bool),
		limiter:     ratelimiter.New(cfg.RateLimit),
		client: &http.Client{
			Timeout: cfg.Timeout,
		},
		robotsCache: make(map[string]*robots.Policy),
	}
}

// Run starts the crawl and returns all results when complete.
// It respects cancellation and deadlines via the provided ctx.
func (c *Crawler) Run(ctx context.Context) []result.Page {
	if ctx == nil {
		ctx = context.Background()
	}
	fmt.Printf("🚀 Starting crawler with %d workers\n", c.config.Workers)
	fmt.Printf("📋 Seeds: %v\n\n", c.config.Seeds)

	// Drain resultCh concurrently with a dedicated collector goroutine.
	// This prevents workers from blocking on resultCh <- page when the total
	// crawled pages exceed the bounded buffer size (Workers * 20).
	var results []result.Page
	collectorDone := make(chan struct{})
	go func() {
		for page := range c.resultCh {
			results = append(results, page)
		}
		close(collectorDone)
	}()

	// Launch worker pool — each worker is a goroutine
	var workerWg sync.WaitGroup
	for i := 1; i <= c.config.Workers; i++ {
		workerWg.Add(1)
		go func(id int) {
			defer workerWg.Done()
			c.worker(ctx, id)
		}(i)
	}

	// Seed the job channel with starting URLs
	for _, seedURL := range c.config.Seeds {
		if ctx.Err() != nil {
			break
		}
		c.enqueue(ctx, seedURL, 0)
	}

	// Closer goroutine: waits until all in-flight jobs are done or ctx is canceled, then closes jobCh.
	// This causes all workers to exit their loops cleanly without deadlocking.
	go func() {
		waitDone := make(chan struct{})
		go func() {
			c.jobWg.Wait()
			close(waitDone)
		}()

		select {
		case <-waitDone:
		case <-ctx.Done():
		}
		close(c.jobCh)
	}()

	// Wait for all workers to finish processing and exiting
	workerWg.Wait()

	// All workers have finished sending results; closing resultCh allows the collector to terminate
	close(c.resultCh)

	// Wait for the collector goroutine to finish draining any remaining pages
	<-collectorDone

	if ctx.Err() != nil {
		fmt.Printf("\n⚠️  Crawl stopped: %v\n", ctx.Err())
	}
	fmt.Printf("\n✅ Crawl complete. Total pages crawled: %d\n", len(results))
	return results
}

// worker is the function run by each goroutine in the pool.
// It reads jobs from jobCh until the channel is closed or ctx is canceled.
func (c *Crawler) worker(ctx context.Context, id int) {
	for {
		select {
		case <-ctx.Done():
			return
		case j, ok := <-c.jobCh:
			if !ok {
				return
			}
			page := c.fetch(ctx, j.url, j.depth)
			select {
			case c.resultCh <- page:
			case <-ctx.Done():
				c.jobWg.Done()
				return
			}

			// Enqueue child links before marking this job done
			if page.Error == "" && j.depth < c.config.MaxDepth {
				for _, link := range page.Links {
					if ctx.Err() != nil {
						break
					}
					resolved := c.resolveURL(j.url, link)
					if resolved != "" {
						c.enqueue(ctx, resolved, j.depth+1)
					}
				}
			}

			// Mark job complete — jobWg counter decrements here
			c.jobWg.Done()
		}
	}
}

// fetch performs the HTTP GET for a single URL using http.NewRequestWithContext,
// retrying transient errors (network failures and 5xx responses) with exponential backoff.
func (c *Crawler) fetch(ctx context.Context, rawURL string, depth int) result.Page {
	maxAttempts := c.config.Retries + 1
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var lastPage result.Page

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			// Exponential backoff: 50ms * 2^(attempt-1)
			backoff := time.Duration(50*(1<<(attempt-1))) * time.Millisecond
			select {
			case <-ctx.Done():
				return result.Page{URL: rawURL, Error: ctx.Err().Error()}
			case <-time.After(backoff):
			}
		}

		if err := ctx.Err(); err != nil {
			return result.Page{URL: rawURL, Error: err.Error()}
		}

		domain := extractDomain(rawURL)

		// Rate limit per domain — this call may block to enforce the interval
		c.limiter.Wait(domain)

		if err := ctx.Err(); err != nil {
			return result.Page{URL: rawURL, Error: err.Error()}
		}

		if attempt == 0 {
			fmt.Printf("  [depth %d] Fetching: %s\n", depth, rawURL)
		} else {
			fmt.Printf("  [depth %d] Retrying (%d/%d): %s\n", depth, attempt, c.config.Retries, rawURL)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			return result.Page{URL: rawURL, Error: err.Error()}
		}
		req.Header.Set("User-Agent", c.config.UserAgent)

		resp, err := c.client.Do(req)
		if err != nil {
			lastPage = result.Page{URL: rawURL, Error: err.Error()}
			continue // transient network error -> retry
		}

		// Check for transient 5xx server error
		if resp.StatusCode >= 500 && resp.StatusCode <= 599 {
			resp.Body.Close()
			lastPage = result.Page{
				URL:        rawURL,
				StatusCode: resp.StatusCode,
				Error:      fmt.Sprintf("HTTP %d: %s", resp.StatusCode, http.StatusText(resp.StatusCode)),
			}
			continue // transient server error -> retry
		}

		// Content-Type guard: ensure response is HTML before attempting extraction
		contentType := strings.TrimSpace(resp.Header.Get("Content-Type"))
		if !strings.HasPrefix(strings.ToLower(contentType), "text/html") {
			resp.Body.Close()
			return result.Page{
				URL:        rawURL,
				StatusCode: resp.StatusCode,
				Error:      fmt.Sprintf("skipped non-HTML content type: %q", contentType),
			}
		}

		// Size guard: check Content-Length header to avoid downloading huge files
		const maxBodyBytes = 1 << 20 // 1MB limit
		if resp.ContentLength > maxBodyBytes {
			resp.Body.Close()
			return result.Page{
				URL:        rawURL,
				StatusCode: resp.StatusCode,
				Error:      fmt.Sprintf("skipped response exceeding max size: %d bytes (limit %d bytes)", resp.ContentLength, maxBodyBytes),
			}
		}

		// Read up to 1MB of body
		body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
		resp.Body.Close()
		if err != nil {
			return result.Page{URL: rawURL, StatusCode: resp.StatusCode, Error: err.Error()}
		}

		html := string(body)
		title := parser.ExtractTitle(html)
		links := parser.ExtractLinks(html)

		return result.Page{
			URL:        rawURL,
			Title:      title,
			Links:      links,
			StatusCode: resp.StatusCode,
		}
	}

	return lastPage
}

// enqueue adds a URL to the job channel if it hasn't been visited yet,
// is permitted by robots.txt, and we haven't hit the max page limit.
func (c *Crawler) enqueue(ctx context.Context, rawURL string, depth int) {
	if ctx.Err() != nil {
		return
	}

	// Check robots.txt Disallow rules before admitting URL as a job
	if !c.isAllowedByRobots(ctx, rawURL) {
		return
	}

	// Check and update visited set — mutex protects concurrent map access
	c.visitedMu.Lock()
	if c.visited[rawURL] {
		c.visitedMu.Unlock()
		return
	}
	c.visited[rawURL] = true
	c.visitedMu.Unlock()

	// Check max pages limit
	if c.config.MaxPages > 0 {
		c.pageCountMu.Lock()
		if c.pageCount >= c.config.MaxPages {
			c.pageCountMu.Unlock()
			return
		}
		c.pageCount++
		c.pageCountMu.Unlock()
	}

	if ctx.Err() != nil {
		return
	}

	// Increment job counter BEFORE sending — ensures closer goroutine
	// cannot close jobCh until this job has been processed
	c.jobWg.Add(1)
	select {
	case c.jobCh <- job{url: rawURL, depth: depth}:
	case <-ctx.Done():
		c.jobWg.Done()
	}
}

// isAllowedByRobots checks if the URL path is allowed by the domain's robots.txt rules.
// It fetches and caches the robots.txt per domain thread-safely.
func (c *Crawler) isAllowedByRobots(ctx context.Context, rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" {
		return false
	}
	domain := strings.ToLower(u.Host)

	c.robotsMu.Lock()
	policy, exists := c.robotsCache[domain]
	c.robotsMu.Unlock()

	if !exists {
		policy = c.fetchRobotsPolicy(ctx, u.Scheme, domain)
		c.robotsMu.Lock()
		if existing, ok := c.robotsCache[domain]; ok {
			policy = existing
		} else {
			c.robotsCache[domain] = policy
			if policy != nil && policy.CrawlDelay > 0 {
				c.limiter.SetInterval(domain, policy.CrawlDelay)
			}
		}
		c.robotsMu.Unlock()
	}

	if policy == nil {
		return true
	}
	return policy.IsAllowed(u.Path)
}

func (c *Crawler) fetchRobotsPolicy(ctx context.Context, scheme, domain string) *robots.Policy {
	if scheme == "" {
		scheme = "http"
	}
	robotsURL := fmt.Sprintf("%s://%s/robots.txt", scheme, domain)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, robotsURL, nil)
	if err != nil {
		return &robots.Policy{}
	}
	req.Header.Set("User-Agent", c.config.UserAgent)

	resp, err := c.client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		return &robots.Policy{}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 512*1024))
	if err != nil {
		return &robots.Policy{}
	}

	return robots.Parse(string(body), c.config.UserAgent)
}

// resolveURL converts a relative link to an absolute URL based on the base page URL,
// applying canonicalization: stripping fragments, sorting query parameters, and
// removing trailing slashes from non-root paths so equivalent URLs are not crawled twice.
func (c *Crawler) resolveURL(base, link string) string {
	baseURL, err := url.Parse(base)
	if err != nil {
		return ""
	}
	linkURL, err := url.Parse(link)
	if err != nil {
		return ""
	}

	resolved := baseURL.ResolveReference(linkURL)

	// Only crawl http/https schemes
	if resolved.Scheme != "http" && resolved.Scheme != "https" {
		return ""
	}

	// StayOnDomain: only follow links to the same host as the seed
	if c.config.StayOnDomain && resolved.Host != baseURL.Host {
		return ""
	}

	// 1. Strip URL fragments (#section)
	resolved.Fragment = ""
	resolved.RawFragment = ""

	// 2. Sort query parameters alphabetically so key order differences don't produce duplicate crawls
	if resolved.RawQuery != "" {
		resolved.RawQuery = resolved.Query().Encode()
	}

	// 3. Strip trailing slash (except for bare root path "/")
	if len(resolved.Path) > 1 && strings.HasSuffix(resolved.Path, "/") {
		resolved.Path = strings.TrimRight(resolved.Path, "/")
	}

	return resolved.String()
}

// extractDomain returns just the hostname from a URL string.
func extractDomain(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	return strings.ToLower(u.Host)
}
