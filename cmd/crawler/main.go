// Command crawler is a concurrent web crawler built with Go's goroutines,
// channels, and sync primitives. It uses a worker-pool pattern to fetch
// multiple pages in parallel while respecting per-domain rate limits.
//
// Usage:
//
//	go run ./cmd/crawler [flags]
//
// Flags:
//
//	-workers    Number of concurrent goroutines (default: 5)
//	-depth      Maximum link depth to follow (default: 2)
//	-maxpages   Maximum total pages to crawl (default: 20)
//	-ratelimit  Milliseconds between requests to same domain (default: 500)
//	-timeout    HTTP request timeout in seconds (default: 10)
//	-seed       Comma-separated seed URLs (default: uses seeds/seeds.txt)
//	-output     Output file path (default: results.json)
//	-samedomain Only follow links on the same domain as the seed (default: true)
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mahimachandna28/webcrawler/internal/crawler"
)

func main() {
	// --- CLI Flags ---
	workers     := flag.Int("workers", 5, "Number of concurrent worker goroutines")
	depth       := flag.Int("depth", 2, "Maximum link-follow depth")
	maxPages    := flag.Int("maxpages", 20, "Maximum total pages to crawl (0 = unlimited)")
	rateLimit   := flag.Int("ratelimit", 500, "Milliseconds between requests to same domain")
	timeout     := flag.Int("timeout", 10, "HTTP request timeout in seconds")
	seedFlag    := flag.String("seed", "", "Comma-separated seed URLs (overrides seeds/seeds.txt)")
	outputFile  := flag.String("output", "results.json", "Output file path for crawl results")
	sameDomain  := flag.Bool("samedomain", true, "Only follow links on the same domain as seed")
	maxDuration := flag.Duration("maxduration", 0, "Maximum crawl duration (e.g. 10s, 1m; default: 0 = unlimited)")
	retries     := flag.Int("retries", 2, "Maximum retry attempts on transient errors (network error or 5xx)")
	flag.Parse()

	// --- Resolve seed URLs ---
	var seeds []string
	if *seedFlag != "" {
		// Use seeds from CLI flag
		for _, s := range strings.Split(*seedFlag, ",") {
			s = strings.TrimSpace(s)
			if s != "" {
				seeds = append(seeds, s)
			}
		}
	} else {
		// Fall back to seeds/seeds.txt
		seeds = loadSeedsFromFile("seeds/seeds.txt")
	}

	if len(seeds) == 0 {
		fmt.Println("❌ No seed URLs provided. Use -seed flag or add URLs to seeds/seeds.txt")
		os.Exit(1)
	}

	// --- Setup graceful shutdown and timeout via context ---
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *maxDuration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *maxDuration)
		defer cancel()
	}

	// --- Build config and run ---
	cfg := crawler.Config{
		Seeds:        seeds,
		Workers:      *workers,
		MaxDepth:     *depth,
		RateLimit:    time.Duration(*rateLimit) * time.Millisecond,
		Timeout:      time.Duration(*timeout) * time.Second,
		MaxPages:     *maxPages,
		StayOnDomain: *sameDomain,
		Retries:      *retries,
	}

	results := crawler.New(cfg).Run(ctx)

	// --- Print summary to stdout ---
	fmt.Println("\n📊 Crawl Summary:")
	fmt.Println(strings.Repeat("─", 60))
	for _, page := range results {
		if page.Error != "" {
			fmt.Printf("  ❌ [%d] %s\n     Error: %s\n", page.StatusCode, page.URL, page.Error)
		} else {
			fmt.Printf("  ✅ [%d] %s\n     Title: %s | Links found: %d\n",
				page.StatusCode, page.URL, page.Title, len(page.Links))
		}
	}
	fmt.Println(strings.Repeat("─", 60))

	// --- Save results to JSON ---
	if err := saveResults(*outputFile, results); err != nil {
		fmt.Printf("⚠️  Failed to save results: %v\n", err)
	} else {
		fmt.Printf("\n💾 Results saved to: %s\n", *outputFile)
	}
}

// loadSeedsFromFile reads one URL per line from a file.
func loadSeedsFromFile(path string) []string {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var seeds []string
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "#") {
			seeds = append(seeds, line)
		}
	}
	return seeds
}

// saveResults serializes the crawl results to a JSON file.
func saveResults(path string, results interface{}) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(results)
}
