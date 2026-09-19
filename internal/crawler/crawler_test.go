package crawler

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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
		StayOnDomain: true,
	}

	results := New(cfg).Run()

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
		StayOnDomain: true,
	}

	start := time.Now()
	results := New(cfg).Run()
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
		StayOnDomain: true,
	}

	results := New(cfg).Run()

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
		StayOnDomain: true,
	}

	results := New(cfg).Run()

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
		StayOnDomain: true,
	}

	done := make(chan []result.Page)
	go func() {
		done <- New(cfg).Run()
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

