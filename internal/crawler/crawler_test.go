package crawler

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mahimachandna28/webcrawler/internal/blobstore"
	"github.com/mahimachandna28/webcrawler/internal/dedup"
	"github.com/mahimachandna28/webcrawler/internal/result"
)

// mockServer creates a local test HTTP server with predefined pages.
// This lets us test the crawler without making real network requests.
func mockServer() *httptest.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<html>
			<head><title>Home Page</title></head>
			<body>
				<a href="/about">About</a>
				<a href="/contact">Contact</a>
			</body>
		</html>`)
	})

	mux.HandleFunc("/about", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<html>
			<head><title>About Page</title></head>
			<body><a href="/">Home</a></body>
		</html>`)
	})

	mux.HandleFunc("/contact", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<html>
			<head><title>Contact Page</title></head>
			<body><a href="/">Home</a></body>
		</html>`)
	})

	return httptest.NewServer(mux)
}

func TestCrawler_BasicCrawl(t *testing.T) {
	server := mockServer()
	defer server.Close()

	cfg := Config{
		Seeds:        []string{server.URL + "/"},
		Workers:      3,
		MaxDepth:     1,
		RateLimit:    10 * time.Millisecond,
		Timeout:      5 * time.Second,
		MaxPages:     10,
		StayOnDomain:    true,
		AllowPrivateIPs: true,
	}

	results := New(cfg).Run(context.Background())

	if len(results) == 0 {
		t.Fatal("Expected at least one crawled page, got none")
	}

	// Check home page was crawled
	found := false
	for _, page := range results {
		if page.URL == server.URL+"/" {
			found = true
			if page.Title != "Home Page" {
				t.Errorf("Expected title 'Home Page', got %q", page.Title)
			}
			if page.StatusCode != 200 {
				t.Errorf("Expected status 200, got %d", page.StatusCode)
			}
		}
	}
	if !found {
		t.Error("Home page was not crawled")
	}
}

func TestCrawler_WorkerPool_Concurrency(t *testing.T) {
	// Verifies that multiple workers process jobs concurrently
	// by timing a crawl and ensuring it's faster than sequential would be
	server := mockServer()
	defer server.Close()

	cfg := Config{
		Seeds:        []string{server.URL + "/"},
		Workers:      5, // 5 concurrent workers
		MaxDepth:     1,
		RateLimit:    0, // No rate limiting for speed test
		Timeout:      5 * time.Second,
		MaxPages:     10,
		StayOnDomain:    true,
		AllowPrivateIPs: true,
	}

	start := time.Now()
	results := New(cfg).Run(context.Background())
	elapsed := time.Since(start)

	if len(results) == 0 {
		t.Fatal("Expected results, got none")
	}

	t.Logf("Crawled %d pages in %v with %d workers", len(results), elapsed, cfg.Workers)
}

func TestCrawler_NoDuplicates(t *testing.T) {
	server := mockServer()
	defer server.Close()

	cfg := Config{
		Seeds:        []string{server.URL + "/"},
		Workers:      3,
		MaxDepth:     2, // Depth 2 means we'll encounter / multiple times via links
		RateLimit:    10 * time.Millisecond,
		Timeout:      5 * time.Second,
		MaxPages:     50,
		StayOnDomain:    true,
		AllowPrivateIPs: true,
	}

	results := New(cfg).Run(context.Background())

	// Check no URL appears twice in results
	seen := make(map[string]int)
	for _, page := range results {
		seen[page.URL]++
	}
	for u, count := range seen {
		if count > 1 {
			t.Errorf("URL %q was crawled %d times (expected 1) — mutex not working correctly", u, count)
		}
	}
}

func TestCrawler_MaxPages(t *testing.T) {
	server := mockServer()
	defer server.Close()

	maxPages := 2
	cfg := Config{
		Seeds:        []string{server.URL + "/"},
		Workers:      3,
		MaxDepth:     3,
		RateLimit:    10 * time.Millisecond,
		Timeout:      5 * time.Second,
		MaxPages:     maxPages,
		StayOnDomain:    true,
		AllowPrivateIPs: true,
	}

	results := New(cfg).Run(context.Background())

	if len(results) > maxPages {
		t.Errorf("Expected at most %d pages, got %d", maxPages, len(results))
	}
}

func TestCrawler_ResultCh_ExceedBufferDeadlock(t *testing.T) {
	// Buffer size for resultCh is Workers * 20.
	// With Workers = 2, buffer size is 40.
	// We crawl a chain of 45 pages: page 0 -> page 1 -> ... -> page 44.
	// At any moment jobCh holds at most 1 job, but resultCh receives 45 pages.
	// If resultCh were not drained concurrently while workers run, the 41st send
	// to resultCh would block the worker while Run() waits at workerWg.Wait(),
	// resulting in a permanent deadlock.
	const totalPages = 45
	mux := http.NewServeMux()
	for i := 0; i < totalPages; i++ {
		curr := i
		mux.HandleFunc(fmt.Sprintf("/p%d", curr), func(w http.ResponseWriter, r *http.Request) {
			var nextLink string
			if curr+1 < totalPages {
				nextLink = fmt.Sprintf(`<a href="/p%d">Next</a>`, curr+1)
			}
			fmt.Fprintf(w, "<html><head><title>Page %d</title></head><body>%s</body></html>", curr, nextLink)
		})
	}
	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := Config{
		Seeds:        []string{server.URL + "/p0"},
		Workers:      2, // resultCh buffer is 2*20 = 40
		MaxDepth:     totalPages + 1,
		RateLimit:    0,
		Timeout:      5 * time.Second,
		MaxPages:     totalPages, // 45 > 40 (exceeds resultCh buffer)
		StayOnDomain:    true,
		AllowPrivateIPs: true,
	}

	done := make(chan []result.Page)
	go func() {
		done <- New(cfg).Run(context.Background())
	}()

	select {
	case res := <-done:
		if len(res) != totalPages {
			t.Fatalf("Expected %d pages (exceeding buffer 40), got %d", totalPages, len(res))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Deadlock detected: Run() did not complete within 5 seconds when results exceeded resultCh buffer")
	}
}

func TestCrawler_RobotsCompliance(t *testing.T) {
	mux := http.NewServeMux()

	mux.HandleFunc("/robots.txt", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `
User-agent: *
Disallow: /admin
Disallow: /secret/
Crawl-delay: 0.05
`)
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<html><body>
			<a href="/allowed">Allowed</a>
			<a href="/admin">Admin Blocked</a>
			<a href="/secret/doc.html">Secret Blocked</a>
		</body></html>`)
	})

	mux.HandleFunc("/allowed", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<html><body><a href="/admin/nested">Admin Subpage</a></body></html>`)
	})

	mux.HandleFunc("/admin", func(w http.ResponseWriter, r *http.Request) {
		t.Error("Hit disallowed path /admin")
		fmt.Fprint(w, `<html><body>Admin</body></html>`)
	})

	mux.HandleFunc("/admin/nested", func(w http.ResponseWriter, r *http.Request) {
		t.Error("Hit disallowed path /admin/nested")
		fmt.Fprint(w, `<html><body>Admin Nested</body></html>`)
	})

	mux.HandleFunc("/secret/doc.html", func(w http.ResponseWriter, r *http.Request) {
		t.Error("Hit disallowed path /secret/doc.html")
		fmt.Fprint(w, `<html><body>Secret Doc</body></html>`)
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := Config{
		Seeds:        []string{server.URL + "/"},
		Workers:      2,
		MaxDepth:     3,
		RateLimit:    10 * time.Millisecond,
		Timeout:      5 * time.Second,
		MaxPages:     10,
		StayOnDomain:    true,
		AllowPrivateIPs: true,
	}

	crawlerInstance := New(cfg)
	results := crawlerInstance.Run(context.Background())

	for _, page := range results {
		if page.URL == server.URL+"/admin" || page.URL == server.URL+"/admin/nested" || page.URL == server.URL+"/secret/doc.html" {
			t.Errorf("Disallowed URL was crawled: %s", page.URL)
		}
	}

	if len(results) != 2 {
		t.Errorf("Expected exactly 2 allowed pages (/ and /allowed), got %d: %+v", len(results), results)
	}

	// Verify Crawl-delay was applied to the rate limiter (0.05s = 50ms)
	u, _ := url.Parse(server.URL)
	domain := strings.ToLower(u.Host)
	appliedInterval := crawlerInstance.limiter.GetInterval(domain)
	if appliedInterval != 50*time.Millisecond {
		t.Errorf("Expected rate limiter interval for %s to be 50ms from robots.txt, got %v", domain, appliedInterval)
	}
}

func TestCrawler_ContextCancellation(t *testing.T) {
	// Infinite chain of pages: each page links to the next
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `<html><body><a href="%s">Next</a></body></html>`, r.URL.Path+"/next")
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := Config{
		Seeds:        []string{server.URL + "/"},
		Workers:      3,
		MaxDepth:     100,
		RateLimit:    10 * time.Millisecond,
		Timeout:      5 * time.Second,
		MaxPages:     1000,
		StayOnDomain:    true,
		AllowPrivateIPs: true,
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan []result.Page)
	go func() {
		done <- New(cfg).Run(ctx)
	}()

	// Wait briefly then cancel the crawl via context
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case res := <-done:
		if len(res) >= 1000 {
			t.Errorf("Expected crawler to cancel early, but got %d pages", len(res))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Crawler did not stop within 2s after context cancellation — goroutine leak or deadlock")
	}
}

func TestCrawler_ContextTimeout(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(20 * time.Millisecond)
		fmt.Fprintf(w, `<html><body><a href="%s">Next</a></body></html>`, r.URL.Path+"/next")
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := Config{
		Seeds:        []string{server.URL + "/"},
		Workers:      2,
		MaxDepth:     100,
		RateLimit:    0,
		Timeout:      5 * time.Second,
		MaxPages:     1000,
		StayOnDomain:    true,
		AllowPrivateIPs: true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	start := time.Now()
	res := New(cfg).Run(ctx)
	elapsed := time.Since(start)

	if len(res) >= 1000 {
		t.Errorf("Expected crawl to be limited by timeout, but crawled all %d pages", len(res))
	}
	if elapsed > 2*time.Second {
		t.Errorf("Crawl took too long to terminate on timeout: %v", elapsed)
	}
}

func TestCrawler_RetriesWithBackoff(t *testing.T) {
	var attempts int
	var mu sync.Mutex

	mux := http.NewServeMux()
	mux.HandleFunc("/flaky", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		attempts++
		curr := attempts
		mu.Unlock()

		if curr < 3 {
			// Fail first 2 attempts with 500 Internal Server Error
			http.Error(w, "Temporary Server Error", http.StatusInternalServerError)
			return
		}
		// Succeed on 3rd attempt
		fmt.Fprint(w, `<html><head><title>Recovered Page</title></head><body>Success</body></html>`)
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := Config{
		Seeds:        []string{server.URL + "/flaky"},
		Workers:      1,
		MaxDepth:     1,
		RateLimit:    0,
		Timeout:      5 * time.Second,
		MaxPages:     1,
		StayOnDomain:    true,
		AllowPrivateIPs: true,
		Retries:      2, // Should retry twice and succeed on 3rd attempt
	}

	results := New(cfg).Run(context.Background())

	if len(results) != 1 {
		t.Fatalf("Expected 1 result page, got %d", len(results))
	}

	page := results[0]
	if page.StatusCode != 200 {
		t.Errorf("Expected status 200 after retries, got %d (error: %s)", page.StatusCode, page.Error)
	}
	if page.Title != "Recovered Page" {
		t.Errorf("Expected title 'Recovered Page', got %q", page.Title)
	}

	mu.Lock()
	totalAttempts := attempts
	mu.Unlock()

	if totalAttempts != 3 {
		t.Errorf("Expected exactly 3 attempts (1 initial + 2 retries), got %d", totalAttempts)
	}
}

func TestCrawler_RetriesExhausted(t *testing.T) {
	var attempts int
	var mu sync.Mutex

	mux := http.NewServeMux()
	mux.HandleFunc("/broken", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		attempts++
		mu.Unlock()
		http.Error(w, "Persistent Error", http.StatusBadGateway) // 502
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := Config{
		Seeds:        []string{server.URL + "/broken"},
		Workers:      1,
		MaxDepth:     1,
		RateLimit:    0,
		Timeout:      5 * time.Second,
		MaxPages:     1,
		StayOnDomain:    true,
		AllowPrivateIPs: true,
		Retries:      2,
	}

	results := New(cfg).Run(context.Background())

	if len(results) != 1 {
		t.Fatalf("Expected 1 result page, got %d", len(results))
	}

	page := results[0]
	if page.StatusCode != 502 {
		t.Errorf("Expected status 502, got %d", page.StatusCode)
	}
	if page.Error == "" {
		t.Error("Expected page to be marked with an error after exhausted retries")
	}

	mu.Lock()
	totalAttempts := attempts
	mu.Unlock()

	if totalAttempts != 3 { // 1 initial + 2 retries
		t.Errorf("Expected 3 attempts before failure, got %d", totalAttempts)
	}
}

func TestCrawler_ContentTypeGuard(t *testing.T) {
	mux := http.NewServeMux()

	mux.HandleFunc("/image.png", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		w.Write([]byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A}) // PNG magic bytes
	})

	mux.HandleFunc("/data.json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"title": "Not an HTML page", "links": ["/hidden"]}`)
	})

	mux.HandleFunc("/valid.html", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, `<html><head><title>Valid</title></head><body><a href="/image.png">Image</a><a href="/data.json">JSON</a></body></html>`)
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := Config{
		Seeds:        []string{server.URL + "/valid.html"},
		Workers:      2,
		MaxDepth:     1,
		RateLimit:    0,
		Timeout:      5 * time.Second,
		MaxPages:     10,
		StayOnDomain:    true,
		AllowPrivateIPs: true,
	}

	results := New(cfg).Run(context.Background())

	if len(results) != 3 {
		t.Fatalf("Expected 3 results, got %d: %+v", len(results), results)
	}

	for _, page := range results {
		switch page.URL {
		case server.URL + "/valid.html":
			if page.Title != "Valid" {
				t.Errorf("Expected title 'Valid', got %q", page.Title)
			}
			if page.Error != "" {
				t.Errorf("Unexpected error for valid page: %s", page.Error)
			}
		case server.URL + "/image.png":
			if page.Error == "" || !strings.Contains(page.Error, "skipped non-HTML content type") {
				t.Errorf("Expected non-HTML content type error for image, got %q", page.Error)
			}
			if len(page.Links) != 0 {
				t.Errorf("Expected 0 links from binary image, got %d", len(page.Links))
			}
		case server.URL + "/data.json":
			if page.Error == "" || !strings.Contains(page.Error, "skipped non-HTML content type") {
				t.Errorf("Expected non-HTML content type error for JSON, got %q", page.Error)
			}
			if len(page.Links) != 0 {
				t.Errorf("Expected 0 links from JSON, got %d", len(page.Links))
			}
		}
	}
}

func TestCrawler_SizeGuard(t *testing.T) {
	mux := http.NewServeMux()

	mux.HandleFunc("/huge.html", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Content-Length", "2000000") // 2MB declared
		w.WriteHeader(http.StatusOK)
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := Config{
		Seeds:        []string{server.URL + "/huge.html"},
		Workers:      1,
		MaxDepth:     1,
		RateLimit:    0,
		Timeout:      5 * time.Second,
		MaxPages:     1,
		StayOnDomain:    true,
		AllowPrivateIPs: true,
	}

	results := New(cfg).Run(context.Background())

	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}

	page := results[0]
	if page.Error == "" || !strings.Contains(page.Error, "exceeding max size") {
		t.Errorf("Expected size guard error, got %q", page.Error)
	}
}

func TestCrawler_UserAgentHeader(t *testing.T) {
	var receivedUA string
	var mu sync.Mutex

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		receivedUA = r.Header.Get("User-Agent")
		mu.Unlock()
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, `<html><head><title>UA Test</title></head><body>Hello</body></html>`)
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := Config{
		Seeds:        []string{server.URL + "/"},
		Workers:      1,
		MaxDepth:     1,
		RateLimit:    0,
		Timeout:      5 * time.Second,
		MaxPages:     1,
		StayOnDomain:    true,
		AllowPrivateIPs: true,
	}

	results := New(cfg).Run(context.Background())
	if len(results) != 1 {
		t.Fatalf("Expected 1 page, got %d", len(results))
	}

	mu.Lock()
	ua := receivedUA
	mu.Unlock()

	if ua != DefaultUserAgent {
		t.Errorf("Expected User-Agent %q, got %q", DefaultUserAgent, ua)
	}
}

func TestCrawler_CustomUserAgentHeader(t *testing.T) {
	const customUA = "CustomBot/2.0 (+https://example.com/bot)"
	var receivedUA string
	var mu sync.Mutex

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		receivedUA = r.Header.Get("User-Agent")
		mu.Unlock()
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, `<html><head><title>Custom UA Test</title></head><body>Hello</body></html>`)
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := Config{
		Seeds:        []string{server.URL + "/"},
		Workers:      1,
		MaxDepth:     1,
		RateLimit:    0,
		Timeout:      5 * time.Second,
		MaxPages:     1,
		StayOnDomain:    true,
		AllowPrivateIPs: true,
		UserAgent:    customUA,
	}

	results := New(cfg).Run(context.Background())
	if len(results) != 1 {
		t.Fatalf("Expected 1 page, got %d", len(results))
	}

	mu.Lock()
	ua := receivedUA
	mu.Unlock()

	if ua != customUA {
		t.Errorf("Expected User-Agent %q, got %q", customUA, ua)
	}
}

func TestCrawler_ResolveURL_Canonicalization(t *testing.T) {
	c := New(Config{StayOnDomain: true})

	tests := []struct {
		base     string
		link     string
		expected string
	}{
		// Trailing slash removed on non-root paths
		{"http://example.com", "/about/", "http://example.com/about"},
		{"http://example.com", "/docs/intro/", "http://example.com/docs/intro"},
		// Bare root path preserved
		{"http://example.com", "/", "http://example.com/"},
		// Query parameters sorted alphabetically
		{"http://example.com", "/search?z=3&a=1&m=2", "http://example.com/search?a=1&m=2&z=3"},
		{"http://example.com", "/search?a=1&m=2&z=3", "http://example.com/search?a=1&m=2&z=3"},
		// Fragment stripped
		{"http://example.com", "/page#overview", "http://example.com/page"},
		{"http://example.com", "/page/#overview", "http://example.com/page"},
		// Combined trailing slash, unsorted query params, and fragment
		{"http://example.com", "/items/?sort=desc&filter=active#results", "http://example.com/items?filter=active&sort=desc"},
	}

	for _, tc := range tests {
		got := c.resolveURL(tc.base, tc.link)
		if got != tc.expected {
			t.Errorf("resolveURL(%q, %q) = %q, expected %q", tc.base, tc.link, got, tc.expected)
		}
	}
}

func TestCrawler_DuplicateCanonicalURLsNotCrawled(t *testing.T) {
	var requestCount int
	var mu sync.Mutex

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		// Offer multiple equivalent links to /about and /search
		fmt.Fprint(w, `<html><body>
			<a href="/about/">About Slash</a>
			<a href="/about">About NoSlash</a>
			<a href="/about#team">About Fragment</a>
			<a href="/search?b=2&a=1">Search 1</a>
			<a href="/search?a=1&b=2">Search 2</a>
		</body></html>`)
	})

	mux.HandleFunc("/about", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requestCount++
		mu.Unlock()
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, `<html><head><title>About</title></head><body>About Us</body></html>`)
	})

	mux.HandleFunc("/search", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, `<html><head><title>Search</title></head><body>Search Results</body></html>`)
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := Config{
		Seeds:        []string{server.URL + "/"},
		Workers:      2,
		MaxDepth:     2,
		RateLimit:    0,
		Timeout:      5 * time.Second,
		MaxPages:     10,
		StayOnDomain:    true,
		AllowPrivateIPs: true,
	}

	results := New(cfg).Run(context.Background())

	// We expect 3 distinct pages crawled:
	// 1. /
	// 2. /about
	// 3. /search?a=1&b=2
	if len(results) != 3 {
		t.Errorf("Expected exactly 3 canonical pages crawled, got %d: %+v", len(results), results)
	}

	mu.Lock()
	aboutHits := requestCount
	mu.Unlock()

	if aboutHits != 1 {
		t.Errorf("Expected /about to be crawled exactly once due to canonicalization, got %d times", aboutHits)
	}
}

func TestCrawler_SSRFProtection(t *testing.T) {
	server := mockServer()
	defer server.Close()

	// Default Config has AllowPrivateIPs: false
	cfg := Config{
		Seeds:           []string{server.URL + "/"},
		Workers:         1,
		MaxDepth:        1,
		RateLimit:       0,
		Timeout:         5 * time.Second,
		MaxPages:        1,
		StayOnDomain:    true,
		AllowPrivateIPs: false, // Default: SSRF guard active
	}

	results := New(cfg).Run(context.Background())
	if len(results) == 0 {
		t.Fatal("Expected at least one result recording SSRF failure")
	}

	if results[0].Error == "" {
		t.Fatalf("Expected SSRF error when crawling loopback address, got successful response: %+v", results[0])
	}
	if !strings.Contains(results[0].Error, "blocked by SSRF policy") {
		t.Errorf("Expected error to mention 'blocked by SSRF policy', got: %q", results[0].Error)
	}
}

func TestCrawler_RateLimit429RetryAfter(t *testing.T) {
	var attempts int
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		attempts++
		curr := attempts
		mu.Unlock()

		if curr == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprint(w, "Rate limit exceeded")
			return
		}

		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, "<html><head><title>Success After 429</title></head><body>OK</body></html>")
	}))
	defer server.Close()

	cfg := Config{
		Seeds:           []string{server.URL + "/"},
		Workers:         1,
		MaxDepth:        1,
		RateLimit:       10 * time.Millisecond,
		Timeout:         5 * time.Second,
		MaxPages:        1,
		StayOnDomain:    true,
		Retries:         2,
		AllowPrivateIPs: true,
	}

	results := New(cfg).Run(context.Background())
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}
	if results[0].Title != "Success After 429" {
		t.Errorf("Expected Title 'Success After 429', got %q (Error: %s)", results[0].Title, results[0].Error)
	}
}

func TestCrawler_SpiderTrapRepetition(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, `<html><body>
			<a href="/a/a/a/trap.html">Repetitive Trap</a>
			<a href="/safe/page.html">Safe Page</a>
		</body></html>`)
	}))
	defer server.Close()

	cfg := Config{
		Seeds:           []string{server.URL + "/"},
		Workers:         1,
		MaxDepth:        2,
		RateLimit:       0,
		Timeout:         5 * time.Second,
		MaxPages:        10,
		StayOnDomain:    true,
		AllowPrivateIPs: true,
	}

	results := New(cfg).Run(context.Background())
	for _, page := range results {
		if strings.Contains(page.URL, "trap.html") {
			t.Errorf("Crawler crawled spider trap URL %q, should have rejected it", page.URL)
		}
	}
}

func TestCrawler_MaxPagesPerDomain(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, `<html><body>
			<a href="/p1">P1</a>
			<a href="/p2">P2</a>
			<a href="/p3">P3</a>
			<a href="/p4">P4</a>
		</body></html>`)
	})
	for i := 1; i <= 4; i++ {
		curr := i
		mux.HandleFunc(fmt.Sprintf("/p%d", curr), func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/html")
			fmt.Fprintf(w, "<html><head><title>P%d</title></head></html>", curr)
		})
	}
	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := Config{
		Seeds:             []string{server.URL + "/"},
		Workers:           1,
		MaxDepth:          2,
		RateLimit:         0,
		Timeout:           5 * time.Second,
		MaxPages:          10,
		MaxPagesPerDomain: 2, // Hard domain cap
		StayOnDomain:      true,
		AllowPrivateIPs:   true,
	}

	results := New(cfg).Run(context.Background())
	if len(results) > 2 {
		t.Errorf("Expected at most 2 pages crawled under MaxPagesPerDomain=2, got %d", len(results))
	}
}

func TestCrawler_CharsetDecoding(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=windows-1252")
		// Windows-1252 bytes for: <title>Café & Résumé</title>
		// 'é' is 0xE9 in windows-1252 (not valid 2-byte UTF-8)
		body := []byte("<html><head><title>Caf\xe9 & R\xe9sum\xe9</title></head><body>Hello</body></html>")
		w.Write(body)
	}))
	defer server.Close()

	cfg := Config{
		Seeds:           []string{server.URL + "/"},
		Workers:         1,
		MaxDepth:        1,
		RateLimit:       0,
		Timeout:         5 * time.Second,
		MaxPages:        1,
		StayOnDomain:    true,
		AllowPrivateIPs: true,
	}

	results := New(cfg).Run(context.Background())
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}
	if results[0].Title != "Café & Résumé" {
		t.Errorf("Expected decoded UTF-8 title 'Café & Résumé', got %q", results[0].Title)
	}
}

func TestCrawler_CanonicalExtraction(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, `<html><head>
			<title>Page With Canonical</title>
			<link rel="canonical" href="https://example.com/canonical-master">
			<meta name="description" content="Canonical test description">
		</head><body>Content</body></html>`)
	}))
	defer server.Close()

	cfg := Config{
		Seeds:           []string{server.URL + "/"},
		Workers:         1,
		MaxDepth:        1,
		RateLimit:       0,
		Timeout:         5 * time.Second,
		MaxPages:        1,
		StayOnDomain:    true,
		AllowPrivateIPs: true,
	}

	results := New(cfg).Run(context.Background())
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}
	if results[0].CanonicalURL != "https://example.com/canonical-master" {
		t.Errorf("Expected CanonicalURL 'https://example.com/canonical-master', got %q", results[0].CanonicalURL)
	}
	if results[0].Description != "Canonical test description" {
		t.Errorf("Expected Description 'Canonical test description', got %q", results[0].Description)
	}
}

func TestCrawler_BlobStoreArchiving(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "blobstore_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	store, err := blobstore.NewFileStore(tempDir)
	if err != nil {
		t.Fatalf("Failed to create file store: %v", err)
	}

	htmlContent := "<html><head><title>Blobstore Test</title></head><body><p>Hello Blobstore</p></body></html>"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, htmlContent)
	}))
	defer server.Close()

	cfg := Config{
		Seeds:           []string{server.URL + "/"},
		Workers:         1,
		MaxDepth:        1,
		RateLimit:       0,
		Timeout:         5 * time.Second,
		MaxPages:        1,
		StayOnDomain:    true,
		AllowPrivateIPs: true,
		JobID:           "job-unit-test",
		BlobStore:       store,
	}

	ctx := context.Background()
	results := New(cfg).Run(ctx)
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}

	page := results[0]
	if page.PayloadPath == "" {
		t.Fatal("Expected PayloadPath to be populated, got empty string")
	}

	// Verify the object exists in the store
	exists, err := store.Exists(ctx, page.PayloadPath)
	if err != nil || !exists {
		t.Fatalf("Payload does not exist in store at %q: %v", page.PayloadPath, err)
	}

	// Read and decompress
	compressed, err := store.Get(ctx, page.PayloadPath)
	if err != nil {
		t.Fatalf("Failed to get payload from store: %v", err)
	}

	decompressed, err := blobstore.DecompressGzip(compressed)
	if err != nil {
		t.Fatalf("Failed to decompress stored payload: %v", err)
	}

	if string(decompressed) != htmlContent {
		t.Errorf("Decompressed content mismatch: got %q, want %q", string(decompressed), htmlContent)
	}
}

func TestCrawler_TwoTierDeduplication(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/a", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, `<html><body><a href="/b">Go to B</a></body></html>`)
	})
	mux.HandleFunc("/b", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, `<html><body><a href="/a">Back to A</a></body></html>`)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	bloom := dedup.NewMemoryBloom(1000, 0.01)
	dd := dedup.NewTwoTier(bloom, nil)

	cfg := Config{
		Seeds:           []string{server.URL + "/a"},
		Workers:         1,
		MaxDepth:        2,
		RateLimit:       0,
		Timeout:         5 * time.Second,
		MaxPages:        10,
		StayOnDomain:    true,
		AllowPrivateIPs: true,
		Dedup:           dd,
	}

	results := New(cfg).Run(context.Background())
	if len(results) != 2 {
		t.Fatalf("Expected 2 crawled pages (/a and /b), got %d", len(results))
	}

	metrics := dd.Metrics()
	if metrics.TotalChecked < 2 {
		t.Errorf("Expected at least 2 checked URLs, got %d", metrics.TotalChecked)
	}
	if metrics.Tier1Negatives < 2 {
		t.Errorf("Expected at least 2 Tier1Negatives, got %d", metrics.Tier1Negatives)
	}
}


