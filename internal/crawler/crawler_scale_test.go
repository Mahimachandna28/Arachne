package crawler

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mahimachandna28/Arachne/internal/result"
)

// fanOutServer serves a synthetic site shaped like a tree: page /n links to
// /(n*fanout+1) ... /(n*fanout+fanout). With fanout=10 and depth 3 there are
// 1 + 10 + 100 + 1000 = 1111 reachable pages.
func fanOutServer(fanout int, hits *atomic.Int64) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			http.NotFound(w, r)
			return
		}
		if hits != nil {
			hits.Add(1)
		}
		n, err := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/"))
		if err != nil {
			n = 0
		}
		var b strings.Builder
		fmt.Fprintf(&b, "<html><head><title>Page %d</title></head><body>", n)
		for i := 1; i <= fanout; i++ {
			fmt.Fprintf(&b, `<a href="/%d">child</a>`, n*fanout+i)
		}
		b.WriteString("</body></html>")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, b.String())
	}))
}

// TestCrawler_WideFanOutDoesNotDeadlock is a regression test for the bounded
// job-channel deadlock: workers used to push discovered links into the same
// bounded channel they consume from. Once the frontier outgrew the buffer
// (Workers*20) every worker blocked on send and nobody was left to receive.
func TestCrawler_WideFanOutDoesNotDeadlock(t *testing.T) {
	var hits atomic.Int64
	server := fanOutServer(10, &hits)
	defer server.Close()

	cfg := Config{
		Seeds:           []string{server.URL + "/0"},
		Workers:         2, // old channel buffer = 40, frontier peaks near 1000
		MaxDepth:        3,
		Timeout:         5 * time.Second,
		StayOnDomain:    true,
		AllowPrivateIPs: true,
		Quiet:           true,
	}

	done := make(chan int, 1)
	go func() { done <- len(New(cfg).Run(context.Background())) }()

	select {
	case n := <-done:
		if n != 1111 {
			t.Fatalf("expected 1111 pages, got %d", n)
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("deadlock: crawl stalled after %d fetches", hits.Load())
	}
}

// TestCrawler_PolitenessDoesNotStallOtherHosts checks that a slow-rate host
// does not occupy every worker: requests to a second, unthrottled host must
// keep flowing while the first host is waiting out its crawl delay.
func TestCrawler_PolitenessDoesNotStallOtherHosts(t *testing.T) {
	slow := fanOutServer(5, nil)
	defer slow.Close()
	fast := fanOutServer(5, nil)
	defer fast.Close()

	slowHost := strings.TrimPrefix(slow.URL, "http://")

	cfg := Config{
		Seeds:           []string{slow.URL + "/0", fast.URL + "/0"},
		Workers:         2,
		MaxDepth:        2, // 1 + 5 + 25 = 31 pages per host
		RateLimit:       0,
		Timeout:         5 * time.Second,
		StayOnDomain:    true,
		AllowPrivateIPs: true,
		Quiet:           true,
	}
	c := New(cfg)
	c.limiter.SetInterval(slowHost, 100*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	results := c.Run(ctx)

	var fastCount, slowCount int
	for _, p := range results {
		if strings.HasPrefix(p.URL, fast.URL) {
			fastCount++
		} else {
			slowCount++
		}
	}
	if fastCount != 31 {
		t.Errorf("fast host should be fully crawled while slow host waits; got %d/31 (slow got %d)", fastCount, slowCount)
	}
	if slowCount > 17 {
		t.Errorf("slow host crawled %d pages in 1.5s despite 100ms delay", slowCount)
	}
}

// TestCrawler_RobotsFetchedOncePerHost verifies singleflight: many workers
// discovering a new host at the same moment trigger a single robots.txt fetch.
func TestCrawler_RobotsFetchedOncePerHost(t *testing.T) {
	var robotsHits atomic.Int64
	inner := fanOutServer(20, nil)
	defer inner.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			robotsHits.Add(1)
			time.Sleep(30 * time.Millisecond) // widen the race window
			fmt.Fprint(w, "User-agent: *\nDisallow: /private\n")
			return
		}
		inner.Config.Handler.ServeHTTP(w, r)
	}))
	defer server.Close()

	seeds := make([]string, 0, 20)
	for i := 1; i <= 20; i++ {
		seeds = append(seeds, fmt.Sprintf("%s/%d", server.URL, i))
	}
	New(Config{
		Seeds: seeds, Workers: 16, MaxDepth: 0, Timeout: 5 * time.Second,
		StayOnDomain: true, AllowPrivateIPs: true, Quiet: true,
	}).Run(context.Background())

	if n := robotsHits.Load(); n != 1 {
		t.Fatalf("robots.txt fetched %d times, want 1", n)
	}
}

func TestCrawler_RecordsDepth(t *testing.T) {
	server := fanOutServer(2, nil)
	defer server.Close()
	results := New(Config{
		Seeds: []string{server.URL + "/0"}, Workers: 3, MaxDepth: 2, Timeout: 5 * time.Second,
		StayOnDomain: true, AllowPrivateIPs: true, Quiet: true,
	}).Run(context.Background())

	byDepth := map[int]int{}
	for _, p := range results {
		byDepth[p.Depth]++
	}
	if byDepth[0] != 1 || byDepth[1] != 2 || byDepth[2] != 4 {
		t.Fatalf("unexpected depth histogram: %v", byDepth)
	}
}

func TestCrawler_OnPageStreamsWithoutAccumulating(t *testing.T) {
	server := fanOutServer(3, nil)
	defer server.Close()
	var streamed int
	res := New(Config{
		Seeds: []string{server.URL + "/0"}, Workers: 4, MaxDepth: 2, Timeout: 5 * time.Second,
		StayOnDomain: true, AllowPrivateIPs: true, Quiet: true,
		OnPage: func(result.Page) { streamed++ },
	}).Run(context.Background())
	if res != nil {
		t.Fatalf("Run should return nil when OnPage is set, got %d results", len(res))
	}
	if streamed != 13 {
		t.Fatalf("streamed %d pages, want 13", streamed)
	}
}

// BenchmarkCrawl_FanOut measures end-to-end throughput against a local
// synthetic site (1111 pages) with no politeness delay.
func BenchmarkCrawl_FanOut(b *testing.B) {
	server := fanOutServer(10, nil)
	defer server.Close()
	for i := 0; i < b.N; i++ {
		n := len(New(Config{
			Seeds: []string{server.URL + "/0"}, Workers: 32, MaxDepth: 3, Timeout: 5 * time.Second,
			StayOnDomain: true, AllowPrivateIPs: true, Quiet: true,
		}).Run(context.Background()))
		if n != 1111 {
			b.Fatalf("got %d pages", n)
		}
	}
	b.ReportMetric(float64(1111*b.N)/b.Elapsed().Seconds(), "pages/s")
}
