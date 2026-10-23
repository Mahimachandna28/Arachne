// Package crawler implements the core concurrent web crawling logic.
// It uses a worker-pool pattern:
//   - A fixed pool of N goroutines (workers) reads URLs from a shared job channel
//   - A visited map (protected by a mutex) prevents duplicate crawling
//   - A rate limiter (mutex-backed) throttles requests per domain
//   - A sync.WaitGroup tracks in-flight jobs (not workers), so jobCh closes
//     automatically when the queue drains — no deadlock possible
package crawler

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/text/encoding/htmlindex"
	"golang.org/x/text/transform"

	"github.com/mahimachandna28/webcrawler/internal/blobstore"
	"github.com/mahimachandna28/webcrawler/internal/netguard"
	"github.com/mahimachandna28/webcrawler/internal/parser"
	"github.com/mahimachandna28/webcrawler/internal/ratelimiter"
	"github.com/mahimachandna28/webcrawler/internal/result"
	"github.com/mahimachandna28/webcrawler/internal/robots"
	"github.com/mahimachandna28/webcrawler/internal/sitemap"
	"github.com/mahimachandna28/webcrawler/internal/storage"
)

// DefaultUserAgent is the default User-Agent header identifying the crawler and repository.
const DefaultUserAgent = "ArachneCrawlerBot/1.0 (+https://github.com/mahimachandna28/Arachne)"

// Config holds the configuration for a crawl run.
type Config struct {
	Seeds             []string        // Starting URLs
	Workers           int             // Number of concurrent worker goroutines
	MaxDepth          int             // Maximum link depth to follow
	RateLimit         time.Duration   // Minimum time between requests to the same domain
	Timeout           time.Duration   // HTTP request timeout
	MaxPages          int             // Maximum total pages to crawl (0 = unlimited)
	MaxPagesPerDomain int             // Maximum pages to crawl per domain (0 = unlimited)
	MaxRedirects      int             // Maximum redirects to follow (default: 10)
	MaxBodyBytes      int64           // Maximum bytes to read per page (default: 2MB)
	StayOnDomain      bool            // If true, only follow links on the same domain as seed
	Retries           int             // Maximum retry attempts on transient failure (network error or 5xx)
	UserAgent         string          // User-Agent request header (defaults to DefaultUserAgent)
	AllowPrivateIPs   bool            // If true, allows connections to private/loopback IP addresses (disables SSRF guard, e.g. for testing)
	JobID             string          // Optional Job ID in PostgreSQL storage
	DB                *storage.DB     // Optional database connection for streaming persistence
	BlobStore         blobstore.Store // Optional object store for archiving raw compressed HTML payloads
}

// job represents a single crawl task sent through the job channel.
type job struct {
	url   string
	depth int
}

// Crawler orchestrates the worker pool and manages shared state.
type Crawler struct {
	config        Config
	jobCh         chan job                  // Channel workers read jobs from
	resultCh      chan result.Page          // Channel workers send results to
	jobWg         sync.WaitGroup           // Tracks in-flight jobs (not workers)
	visited       map[string]bool          // Tracks visited URLs
	visitedMu     sync.Mutex               // Protects the visited map
	pageCount     int                      // Total pages crawled so far
	pageCountMu   sync.Mutex               // Protects pageCount
	domainCount   map[string]int           // Total pages crawled per domain
	domainCountMu sync.Mutex               // Protects domainCount
	limiter       *ratelimiter.RateLimiter
	client        *http.Client
	robotsCache   map[string]*robots.Policy // Thread-safe cache of robots.txt policy per domain
	robotsMu      sync.Mutex               // Protects robotsCache
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

	guard := netguard.New(netguard.Config{
		AllowPrivate: cfg.AllowPrivateIPs,
		DialTimeout:  cfg.Timeout,
	})

	// 1.3 Transport & Connection Pool Tuning
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           guard.DialContext,
		MaxIdleConns:          2000,
		MaxIdleConnsPerHost:   50,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    false,
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

	return &Crawler{
		config:      cfg,
		jobCh:       make(chan job, cfg.Workers*20),
		resultCh:    make(chan result.Page, cfg.Workers*20),
		visited:     make(map[string]bool),
		domainCount: make(map[string]int),
		limiter:     ratelimiter.New(cfg.RateLimit),
		client:      client,
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

	var results []result.Page
	collectorDone := make(chan struct{})

	var batchWriter *storage.BatchWriter
	if c.config.DB != nil && c.config.JobID != "" {
		batchWriter = storage.NewBatchWriter(c.config.DB, c.config.JobID, storage.BatchWriterConfig{})
	}

	go func() {
		for page := range c.resultCh {
			results = append(results, page)
			if batchWriter != nil {
				batchWriter.Submit(page)
			}
		}
		if batchWriter != nil {
			_ = batchWriter.Close()
			saved, errs := batchWriter.Stats()
			_ = c.config.DB.FinishJob(context.Background(), c.config.JobID, storage.JobStatusCompleted, map[string]interface{}{
				"pages_crawled": saved,
				"errors":        errs,
			})
		}
		close(collectorDone)
	}()

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

	// Closer goroutine: waits until all in-flight jobs are done or ctx is canceled
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

	workerWg.Wait()
	close(c.resultCh)
	<-collectorDone

	if ctx.Err() != nil {
		fmt.Printf("\n⚠️  Crawl stopped: %v\n", ctx.Err())
	}
	fmt.Printf("\n✅ Crawl complete. Total pages crawled: %d\n", len(results))
	return results
}

// worker reads jobs from jobCh until closed or ctx canceled.
func (c *Crawler) worker(ctx context.Context, id int) {
	for {
		select {
		case <-ctx.Done():
			return
		case j, ok := <-c.jobCh:
			if !ok {
				return
			}
			page, noFollow := c.fetch(ctx, j.url, j.depth)
			select {
			case c.resultCh <- page:
			case <-ctx.Done():
				c.jobWg.Done()
				return
			}

			// Enqueue child links if permitted by depth, error status, and robots nofollow
			if page.Error == "" && !noFollow && j.depth < c.config.MaxDepth {
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

			c.jobWg.Done()
		}
	}
}

// fetch performs the HTTP GET for a single URL using exponential backoff with full jitter,
// handling 429/503 status codes with Retry-After headers, and safe charset decoding.
func (c *Crawler) fetch(ctx context.Context, rawURL string, depth int) (result.Page, bool) {
	maxAttempts := c.config.Retries + 1
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var lastPage result.Page

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			// 1.4 Full Jitter Exponential Backoff:
			// Sleep = rand(0, min(maxBackoff, baseBackoff * 2^(attempt-1)))
			baseBackoff := 50 * time.Millisecond
			maxBackoff := 3 * time.Second
			backoffLimit := baseBackoff * (1 << (attempt - 1))
			if backoffLimit > maxBackoff || backoffLimit <= 0 {
				backoffLimit = maxBackoff
			}
			jitter := time.Duration(rand.Int63n(int64(backoffLimit)))
			select {
			case <-ctx.Done():
				return result.Page{URL: rawURL, Error: ctx.Err().Error()}, false
			case <-time.After(jitter):
			}
		}

		if err := ctx.Err(); err != nil {
			return result.Page{URL: rawURL, Error: err.Error()}, false
		}

		domain := extractDomain(rawURL)

		// Context-aware Rate Limiter Wait (can be aborted immediately by ctx)
		if err := c.limiter.Wait(ctx, domain); err != nil {
			return result.Page{URL: rawURL, Error: err.Error()}, false
		}

		if attempt == 0 {
			fmt.Printf("  [depth %d] Fetching: %s\n", depth, rawURL)
		} else {
			fmt.Printf("  [depth %d] Retrying (%d/%d): %s\n", depth, attempt, c.config.Retries, rawURL)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			return result.Page{URL: rawURL, Error: err.Error()}, false
		}
		req.Header.Set("User-Agent", c.config.UserAgent)

		resp, err := c.client.Do(req)
		if err != nil {
			lastPage = result.Page{URL: rawURL, Error: err.Error()}
			continue
		}

		// 1.4 Rate Limit & Throttling: Handle 429 Too Many Requests & 503 Service Unavailable
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable {
			retryAfterHeader := resp.Header.Get("Retry-After")
			retryAfter := parseRetryAfter(retryAfterHeader)
			if retryAfter > 0 {
				// Dynamically throttle domain rate limiter
				c.limiter.SetInterval(domain, retryAfter)
			}
			resp.Body.Close()
			lastPage = result.Page{
				URL:        rawURL,
				StatusCode: resp.StatusCode,
				Error:      fmt.Sprintf("HTTP %d: %s (Retry-After: %s)", resp.StatusCode, http.StatusText(resp.StatusCode), retryAfterHeader),
			}
			continue
		}

		// Check for transient 5xx server error
		if resp.StatusCode >= 500 && resp.StatusCode <= 599 {
			resp.Body.Close()
			lastPage = result.Page{
				URL:        rawURL,
				StatusCode: resp.StatusCode,
				Error:      fmt.Sprintf("HTTP %d: %s", resp.StatusCode, http.StatusText(resp.StatusCode)),
			}
			continue
		}

		// 1.5 Content-Type Guard
		contentType := strings.TrimSpace(resp.Header.Get("Content-Type"))
		lowerContentType := strings.ToLower(contentType)
		if !strings.HasPrefix(lowerContentType, "text/html") &&
			!strings.HasPrefix(lowerContentType, "application/xhtml+xml") {
			resp.Body.Close()
			return result.Page{
				URL:        rawURL,
				StatusCode: resp.StatusCode,
				Error:      fmt.Sprintf("skipped non-HTML content type: %q", contentType),
			}, false
		}

		// Size guard: check Content-Length header to avoid downloading huge files
		if resp.ContentLength > c.config.MaxBodyBytes {
			resp.Body.Close()
			return result.Page{
				URL:        rawURL,
				StatusCode: resp.StatusCode,
				Error:      fmt.Sprintf("skipped response exceeding max size: %d bytes (limit %d bytes)", resp.ContentLength, c.config.MaxBodyBytes),
			}, false
		}

		// 1.5 Safe Body Size Cap & Charset Decoding
		htmlStr, err := decodeHTMLBody(resp.Body, contentType, c.config.MaxBodyBytes)
		resp.Body.Close()
		if err != nil {
			return result.Page{URL: rawURL, StatusCode: resp.StatusCode, Error: err.Error()}, false
		}

		// Extract metadata, canonical URL, and links using WHATWG parser
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

		return result.Page{
			URL:          rawURL,
			Title:        meta.Title,
			CanonicalURL: meta.Canonical,
			Description:  meta.Description,
			PayloadPath:  payloadPath,
			Links:        meta.Links,
			StatusCode:   resp.StatusCode,
		}, meta.NoFollow
	}

	return lastPage, false
}

// enqueue adds a URL to the job channel if permitted by spider-trap rules, robots.txt,
// domain caps, and global max page limits.
func (c *Crawler) enqueue(ctx context.Context, rawURL string, depth int) {
	if ctx.Err() != nil {
		return
	}

	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" {
		return
	}

	// 1.7 Spider-Trap & Runaway Protections
	if isSpiderTrap(u) {
		return
	}

	// Check robots.txt rules before admitting URL
	if !c.isAllowedByRobots(ctx, rawURL) {
		return
	}

	// Check and update visited set
	c.visitedMu.Lock()
	if c.visited[rawURL] {
		c.visitedMu.Unlock()
		return
	}
	c.visited[rawURL] = true
	c.visitedMu.Unlock()

	domain := strings.ToLower(u.Host)

	// 1.7 Per-Domain Page Cap
	if c.config.MaxPagesPerDomain > 0 {
		c.domainCountMu.Lock()
		if c.domainCount[domain] >= c.config.MaxPagesPerDomain {
			c.domainCountMu.Unlock()
			return
		}
		c.domainCount[domain]++
		c.domainCountMu.Unlock()
	}

	// Global Max Pages Limit
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

	c.jobWg.Add(1)
	select {
	case c.jobCh <- job{url: rawURL, depth: depth}:
	case <-ctx.Done():
		c.jobWg.Done()
	}
}

// isAllowedByRobots checks if the URL path is allowed by robots.txt and triggers sitemap discovery.
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

		// 1.6 Sitemap discovery: Enqueue document URLs found in sitemaps
		if policy != nil && len(policy.Sitemaps) > 0 {
			go c.discoverSitemapURLs(ctx, policy.Sitemaps)
		}
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
		if err != nil || resp.StatusCode != http.StatusOK {
			if resp != nil && resp.Body != nil {
				resp.Body.Close()
			}
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

// resolveURL converts a relative link to an absolute URL with canonicalization.
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

	if resolved.Scheme != "http" && resolved.Scheme != "https" {
		return ""
	}

	if c.config.StayOnDomain && resolved.Host != baseURL.Host {
		return ""
	}

	resolved.Fragment = ""
	resolved.RawFragment = ""

	if resolved.RawQuery != "" {
		resolved.RawQuery = resolved.Query().Encode()
	}

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

// 1.7 isSpiderTrap checks for repetitive directory patterns and query parameter bloat.
func isSpiderTrap(u *url.URL) bool {
	// Guard against excessively long URLs
	if len(u.String()) > 2048 {
		return true
	}

	// Guard against query parameter explosions (e.g. tracking parameters, dynamic loops)
	if len(u.Query()) > 10 {
		return true
	}

	// Guard against recursive directory traps (e.g., /dir/dir/dir/...)
	cleanPath := strings.Trim(u.Path, "/")
	if cleanPath != "" {
		segments := strings.Split(cleanPath, "/")
		seen := make(map[string]int)
		for _, s := range segments {
			if s != "" {
				seen[s]++
				if seen[s] >= 3 {
					return true
				}
			}
		}
	}

	return false
}

// parseRetryAfter parses the Retry-After header as either seconds or an RFC 1123 HTTP-date.
func parseRetryAfter(header string) time.Duration {
	header = strings.TrimSpace(header)
	if header == "" {
		return 0
	}

	if secs, err := strconv.Atoi(header); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}

	if t, err := http.ParseTime(header); err == nil {
		diff := time.Until(t)
		if diff > 0 {
			return diff
		}
	}

	return 0
}

// 1.5 decodeHTMLBody reads up to maxBytes and converts character encoding to UTF-8.
func decodeHTMLBody(r io.Reader, contentType string, maxBytes int64) (string, error) {
	bodyBytes, err := io.ReadAll(io.LimitReader(r, maxBytes))
	if err != nil {
		return "", err
	}

	charset := extractCharset(contentType)
	if charset == "" {
		charset = sniffCharsetFromHTML(bodyBytes)
	}

	if charset == "" || strings.EqualFold(charset, "utf-8") || strings.EqualFold(charset, "utf8") {
		return string(bodyBytes), nil
	}

	enc, err := htmlindex.Get(charset)
	if err != nil {
		// Fallback to raw string if charset is unrecognized
		return string(bodyBytes), nil
	}

	decoded, err := io.ReadAll(transform.NewReader(bytes.NewReader(bodyBytes), enc.NewDecoder()))
	if err != nil {
		return string(bodyBytes), nil
	}

	return string(decoded), nil
}

func extractCharset(contentType string) string {
	parts := strings.Split(contentType, ";")
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		lower := strings.ToLower(trimmed)
		if strings.HasPrefix(lower, "charset=") {
			val := strings.TrimPrefix(lower, "charset=")
			return strings.Trim(val, `"' `)
		}
	}
	return ""
}

func sniffCharsetFromHTML(body []byte) string {
	limit := 1024
	if len(body) < limit {
		limit = len(body)
	}
	preview := strings.ToLower(string(body[:limit]))
	if idx := strings.Index(preview, "charset="); idx != -1 {
		rest := preview[idx+8:]
		end := strings.IndexAny(rest, `"' >/`)
		if end != -1 {
			return strings.TrimSpace(rest[:end])
		}
	}
	return ""
}
