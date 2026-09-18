// Package crawler implements the core concurrent web crawling logic.
// It uses a worker-pool pattern:
//   - A fixed pool of N goroutines (workers) reads URLs from a shared job channel
//   - A visited map (protected by a mutex) prevents duplicate crawling
//   - A rate limiter (mutex-backed) throttles requests per domain
//   - A sync.WaitGroup tracks in-flight jobs (not workers), so jobCh closes
//     automatically when the queue drains — no deadlock possible
package crawler

import (
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
)

// Config holds the configuration for a crawl run.
type Config struct {
	Seeds        []string      // Starting URLs
	Workers      int           // Number of concurrent worker goroutines
	MaxDepth     int           // Maximum link depth to follow
	RateLimit    time.Duration // Minimum time between requests to the same domain
	Timeout      time.Duration // HTTP request timeout
	MaxPages     int           // Maximum total pages to crawl (0 = unlimited)
	StayOnDomain bool          // If true, only follow links on the same domain as seed
}

// job represents a single crawl task sent through the job channel.
type job struct {
	url   string
	depth int
}

// Crawler orchestrates the worker pool and manages shared state.
type Crawler struct {
	config      Config
	jobCh       chan job         // Channel workers read jobs from
	resultCh    chan result.Page // Channel workers send results to
	jobWg       sync.WaitGroup  // Tracks in-flight jobs (not workers)
	visited     map[string]bool // Tracks visited URLs
	visitedMu   sync.Mutex      // Protects the visited map
	pageCount   int             // Total pages crawled so far
	pageCountMu sync.Mutex      // Protects pageCount
	limiter     *ratelimiter.RateLimiter
	client      *http.Client
}

// New creates a new Crawler with the given configuration.
func New(cfg Config) *Crawler {
	return &Crawler{
		config:   cfg,
		jobCh:    make(chan job, cfg.Workers*20), // Buffered to reduce blocking
		resultCh: make(chan result.Page, cfg.Workers*20),
		visited:  make(map[string]bool),
		limiter:  ratelimiter.New(cfg.RateLimit),
		client: &http.Client{
			Timeout: cfg.Timeout,
		},
	}
}

// Run starts the crawl and returns all results when complete.
//
// Key design: jobWg tracks in-flight jobs, not workers.
//   - enqueue() calls jobWg.Add(1) before sending to jobCh
//   - worker() calls jobWg.Done() after processing each job
//   - A closer goroutine waits for jobWg to reach zero, then closes jobCh
//   - Workers exit their range loop when jobCh closes
//
// This ensures jobCh closes exactly when the queue is drained — no deadlock,
// no need for an external "stop" signal.
func (c *Crawler) Run() []result.Page {
	fmt.Printf("🚀 Starting crawler with %d workers\n", c.config.Workers)
	fmt.Printf("📋 Seeds: %v\n\n", c.config.Seeds)

	// Launch worker pool — each worker is a goroutine
	var workerWg sync.WaitGroup
	for i := 1; i <= c.config.Workers; i++ {
		workerWg.Add(1)
		go func(id int) {
			defer workerWg.Done()
			c.worker(id)
		}(i)
	}

	// Seed the job channel with starting URLs
	for _, seedURL := range c.config.Seeds {
		c.enqueue(seedURL, 0)
	}

	// Closer goroutine: waits until all in-flight jobs are done, then closes jobCh.
	// This causes all workers to exit their `for job := range c.jobCh` loops cleanly.
	go func() {
		c.jobWg.Wait()
		close(c.jobCh)
	}()

	// Wait for all workers to finish, then signal result collector
	workerWg.Wait()
	close(c.resultCh)

	// Collect all results
	var results []result.Page
	for page := range c.resultCh {
		results = append(results, page)
	}

	fmt.Printf("\n✅ Crawl complete. Total pages crawled: %d\n", len(results))
	return results
}

// worker is the function run by each goroutine in the pool.
// It reads jobs from jobCh until the channel is closed by the closer goroutine.
func (c *Crawler) worker(id int) {
	for j := range c.jobCh {
		page := c.fetch(j.url, j.depth)
		c.resultCh <- page

		// Enqueue child links before marking this job done
		if page.Error == "" && j.depth < c.config.MaxDepth {
			for _, link := range page.Links {
				resolved := c.resolveURL(j.url, link)
				if resolved != "" {
					c.enqueue(resolved, j.depth+1)
				}
			}
		}

		// Mark job complete — jobWg counter decrements here
		c.jobWg.Done()
	}
}

// fetch performs the HTTP GET for a single URL and returns a result.Page.
func (c *Crawler) fetch(rawURL string, depth int) result.Page {
	domain := extractDomain(rawURL)

	// Rate limit per domain — this call may block to enforce the interval
	c.limiter.Wait(domain)

	fmt.Printf("  [depth %d] Fetching: %s\n", depth, rawURL)

	resp, err := c.client.Get(rawURL)
	if err != nil {
		return result.Page{URL: rawURL, Error: err.Error()}
	}
	defer resp.Body.Close()

	// Read only up to 1MB of body to avoid huge pages
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
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

// enqueue adds a URL to the job channel if it hasn't been visited yet
// and we haven't hit the max page limit.
// IMPORTANT: jobWg.Add(1) is called BEFORE sending to the channel,
// so the closer goroutine never sees a zero count prematurely.
func (c *Crawler) enqueue(rawURL string, depth int) {
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

	// Increment job counter BEFORE sending — ensures closer goroutine
	// cannot close jobCh until this job has been processed
	c.jobWg.Add(1)
	c.jobCh <- job{url: rawURL, depth: depth}
}

// resolveURL converts a relative link to an absolute URL based on the base page URL.
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

	// Strip fragment (#section) to avoid duplicate URLs
	resolved.Fragment = ""

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
