// Command crawler runs an Arachne crawl from the command line.
//
// Minimal (in-memory, results written to results.json):
//
//	go run ./cmd/crawler -seed=https://go.dev -depth=2 -maxpages=100
//
// With persistence and shared deduplication:
//
//	go run ./cmd/crawler -seed=https://go.dev \
//	    -db-url=postgres://postgres@localhost:5432/arachne?sslmode=disable \
//	    -redis-addr=localhost:6379
//
// -db-url and -redis-addr can also be set via ARACHNE_DATABASE_URL and
// ARACHNE_REDIS_ADDR. Run with -h for all flags.
package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Mahimachandna28/Arachne/internal/blobstore"
	"github.com/Mahimachandna28/Arachne/internal/crawler"
	"github.com/Mahimachandna28/Arachne/internal/dedup"
	"github.com/Mahimachandna28/Arachne/internal/result"
	"github.com/Mahimachandna28/Arachne/internal/storage"
)

func main() {
	// --- Crawl flags ---
	workers := flag.Int("workers", 5, "Number of concurrent worker goroutines")
	depth := flag.Int("depth", 2, "Maximum link-follow depth")
	maxPages := flag.Int("maxpages", 20, "Maximum total pages to crawl (0 = unlimited)")
	rateLimit := flag.Int("ratelimit", 500, "Milliseconds between requests to the same host")
	timeout := flag.Int("timeout", 10, "HTTP request timeout in seconds")
	seedFlag := flag.String("seed", "", "Comma-separated seed URLs (overrides seeds/seeds.txt)")
	outputFile := flag.String("output", "results.json", "Output file for results (JSON array, streamed); empty to disable")
	sameDomain := flag.Bool("samedomain", true, "Only follow links on the same host as the page they were found on")
	maxDuration := flag.Duration("maxduration", 0, "Maximum crawl duration (e.g. 10s, 1m; 0 = unlimited)")
	retries := flag.Int("retries", 2, "Maximum retry attempts on transient errors (network error, 5xx, 429)")
	userAgent := flag.String("useragent", crawler.DefaultUserAgent, "User-Agent header identifying the crawler")
	allowPrivate := flag.Bool("allowprivate", false, "Allow crawling private/local network addresses (disables SSRF protection)")
	maxPerDomain := flag.Int("maxpagesperdomain", 0, "Maximum pages per host (0 = unlimited)")
	payloadDir := flag.String("payload-dir", "", "Directory to store gzipped raw HTML payloads (enables blob storage)")
	quiet := flag.Bool("quiet", false, "Suppress per-URL progress output")

	// --- Infrastructure flags ---
	dbURL := flag.String("db-url", os.Getenv("ARACHNE_DATABASE_URL"), "PostgreSQL connection string (enables persistence)")
	initSchema := flag.Bool("init-schema", true, "Create tables if missing when -db-url is set")
	redisAddr := flag.String("redis-addr", os.Getenv("ARACHNE_REDIS_ADDR"), "Redis address host:port (enables shared Bloom-filter dedup)")
	bloomCapacity := flag.Uint64("bloom-capacity", 10_000_000, "Expected number of URLs for Bloom filter sizing")
	bloomFP := flag.Float64("bloom-fp", 0.001, "Target Bloom filter false-positive rate")
	flag.Parse()

	seeds := parseSeeds(*seedFlag)
	if len(seeds) == 0 {
		fmt.Fprintln(os.Stderr, "❌ No seed URLs provided. Use -seed or add URLs to seeds/seeds.txt")
		os.Exit(1)
	}

	// --- Graceful shutdown and optional deadline ---
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *maxDuration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *maxDuration)
		defer cancel()
	}

	cfg := crawler.Config{
		Seeds:             seeds,
		Workers:           *workers,
		MaxDepth:          *depth,
		RateLimit:         time.Duration(*rateLimit) * time.Millisecond,
		Timeout:           time.Duration(*timeout) * time.Second,
		MaxPages:          *maxPages,
		MaxPagesPerDomain: *maxPerDomain,
		StayOnDomain:      *sameDomain,
		Retries:           *retries,
		UserAgent:         *userAgent,
		AllowPrivateIPs:   *allowPrivate,
		Quiet:             *quiet,
		JobID:             newUUID(),
	}

	// --- Blob store ---
	if *payloadDir != "" {
		fs, err := blobstore.NewFileStore(*payloadDir)
		if err != nil {
			fatalf("payload store at %s: %v", *payloadDir, err)
		}
		cfg.BlobStore = fs
		fmt.Printf("📦 Raw payload archiving enabled -> %s\n", *payloadDir)
	}

	// --- PostgreSQL ---
	var db *storage.DB
	if *dbURL != "" {
		var err error
		db, err = storage.Connect(ctx, *dbURL, nil)
		if err != nil {
			fatalf("postgres: %v", err)
		}
		defer db.Close()
		if *initSchema {
			if err := db.InitSchema(ctx); err != nil {
				fatalf("postgres schema: %v", err)
			}
		}
		cfgJSON, _ := json.Marshal(map[string]any{
			"seeds": seeds, "workers": *workers, "depth": *depth, "max_pages": *maxPages,
			"rate_limit_ms": *rateLimit, "same_domain": *sameDomain,
		})
		now := time.Now().UTC()
		if err := db.CreateJob(ctx, &storage.CrawlJob{
			ID: cfg.JobID, Status: storage.JobStatusRunning, Config: cfgJSON, StartedAt: &now,
		}); err != nil {
			fatalf("create job: %v", err)
		}
		cfg.DB = db
		fmt.Printf("🗄️  Persisting to PostgreSQL (job %s)\n", cfg.JobID)
	}

	// --- Shared deduplication (Bloom filter + exact DB check) ---
	if *redisAddr != "" || db != nil {
		var bloom dedup.BloomStore
		if *redisAddr != "" {
			rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
			if err := rdb.Ping(ctx).Err(); err != nil {
				fatalf("redis %s: %v", *redisAddr, err)
			}
			rb := dedup.NewRedisBloom(rdb, *bloomCapacity, *bloomFP)
			defer rb.Close()
			bloom = rb
			fmt.Printf("🧮 Redis Bloom filter dedup -> %s\n", *redisAddr)
		} else {
			bloom = dedup.NewMemoryBloom(*bloomCapacity, *bloomFP)
		}
		var verifier dedup.ExactVerifier
		if db != nil { // avoid a non-nil interface wrapping a nil *DB
			verifier = db
		}
		cfg.Dedup = dedup.NewTwoTier(bloom, verifier)
	}

	// --- Stream results to disk instead of holding them in memory ---
	var out *jsonArrayWriter
	if *outputFile != "" {
		f, err := os.Create(*outputFile)
		if err != nil {
			fatalf("output file: %v", err)
		}
		out = newJSONArrayWriter(f)
	}
	var sum summary
	cfg.OnPage = func(p result.Page) {
		sum.add(p)
		if out != nil {
			if err := out.Write(p); err != nil {
				fmt.Fprintf(os.Stderr, "⚠️  write result: %v\n", err)
			}
		}
	}

	start := time.Now()
	crawler.New(cfg).Run(ctx)
	elapsed := time.Since(start)

	if out != nil {
		if err := out.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "⚠️  failed to save results: %v\n", err)
		} else {
			fmt.Printf("💾 Results saved to: %s\n", *outputFile)
		}
	}
	sum.print(elapsed)
	if cfg.Dedup != nil {
		m := cfg.Dedup.Metrics()
		fmt.Printf("   Dedup: checked=%d bloom-negatives=%d db-checks=%d duplicates=%d false-positives-recovered=%d\n",
			m.TotalChecked, m.Tier1Negatives, m.Tier2Collisions, m.Tier2Duplicates, m.FalsePositivesRecovered)
	}
}

func parseSeeds(flagValue string) []string {
	if flagValue == "" {
		return loadSeedsFromFile("seeds/seeds.txt")
	}
	var seeds []string
	for _, s := range strings.Split(flagValue, ",") {
		if s = strings.TrimSpace(s); s != "" {
			seeds = append(seeds, s)
		}
	}
	return seeds
}

// loadSeedsFromFile reads one URL per line, ignoring blanks and # comments.
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

func newUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "❌ "+format+"\n", args...)
	os.Exit(1)
}

// jsonArrayWriter streams values as a JSON array so memory stays flat no
// matter how many pages are crawled. The output is still a valid array, so
// the dashboard can read it unchanged.
type jsonArrayWriter struct {
	f     io.WriteCloser
	w     *bufio.Writer
	enc   *json.Encoder
	count int
}

func newJSONArrayWriter(f io.WriteCloser) *jsonArrayWriter {
	w := bufio.NewWriterSize(f, 64*1024)
	_, _ = w.WriteString("[\n")
	enc := json.NewEncoder(w)
	enc.SetIndent("  ", "  ")
	return &jsonArrayWriter{f: f, w: w, enc: enc}
}

func (a *jsonArrayWriter) Write(v any) error {
	if a.count > 0 {
		if _, err := a.w.WriteString(",\n"); err != nil {
			return err
		}
	}
	a.count++
	_, _ = a.w.WriteString("  ")
	return a.enc.Encode(v) // Encode appends '\n'
}

func (a *jsonArrayWriter) Close() error {
	if _, err := a.w.WriteString("]\n"); err != nil {
		return err
	}
	if err := a.w.Flush(); err != nil {
		return err
	}
	return a.f.Close()
}

type summary struct {
	total, ok, failed int
	byStatus          map[int]int
}

func (s *summary) add(p result.Page) {
	if s.byStatus == nil {
		s.byStatus = map[int]int{}
	}
	s.total++
	s.byStatus[p.StatusCode]++
	if p.Error == "" {
		s.ok++
	} else {
		s.failed++
	}
}

func (s *summary) print(elapsed time.Duration) {
	fmt.Println("\n📊 Crawl Summary:")
	fmt.Println(strings.Repeat("─", 60))
	rate := 0.0
	if elapsed > 0 {
		rate = float64(s.total) / elapsed.Seconds()
	}
	fmt.Printf("   Pages: %d (ok %d, failed %d) in %v — %.1f pages/s\n", s.total, s.ok, s.failed, elapsed.Round(time.Millisecond), rate)
	if len(s.byStatus) > 0 {
		codes := make([]int, 0, len(s.byStatus))
		for code := range s.byStatus {
			codes = append(codes, code)
		}
		sort.Ints(codes)
		var parts []string
		for _, code := range codes {
			n := s.byStatus[code]
			label := fmt.Sprint(code)
			if code == 0 {
				label = "net-error"
			}
			parts = append(parts, fmt.Sprintf("%s×%d", label, n))
		}
		fmt.Printf("   Status: %s\n", strings.Join(parts, "  "))
	}
	fmt.Println(strings.Repeat("─", 60))
}
