
# Concurrent Web Crawler in Go
A worker-pool based web crawler built with Go's goroutines, channels, and sync primitives demonstrating real-world concurrency patterns.

---

## Overview

This project implements a production-style concurrent web crawler from scratch using **only Go's standard library** (no external dependencies). It crawls websites in parallel using a **worker-pool pattern**, respects per-domain rate limits to avoid hammering servers, and prevents duplicate URL crawling using a mutex-protected visited set.

Built to demonstrate core systems concepts: **concurrency, synchronization, goroutine lifecycle management, and channel-based communication**.

---

## Architecture & Concurrency Design

```
                        ┌─────────────────────────────────────┐
  Seed URLs ──▶  enqueue()  ──▶  jobCh (buffered channel)     │
                    │                    │                      │
               jobWg.Add(1)    ┌────────┴────────┐            │
                           Worker 1  Worker 2 ... Worker N     │
                           (goroutines reading from jobCh)     │
                                    │                          │
                              fetch() + parse()                │
                                    │                          │
                             resultCh ──▶ results[]            │
                                    │                          │
                          enqueue child links                   │
                          jobWg.Done()                         │
                                    │                          │
                    jobWg reaches 0 ──▶ close(jobCh)           │
                    workers exit range loop                     │
                    close(resultCh) ──▶ collect results        │
                        └─────────────────────────────────────┘
```

### Key Concurrency Primitives Used

| Primitive | Where | Why |
|-----------|-------|-----|
| `goroutine` | Worker pool | Parallel URL fetching |
| `chan job` (buffered) | Job queue | Decouples producers from consumers |
| `chan result.Page` (buffered) | Result collection | Non-blocking worker output |
| `sync.Mutex` | Visited map, page counter | Safe concurrent map/int access |
| `sync.WaitGroup` (jobWg) | Job lifecycle | Closes jobCh when queue drains |
| `sync.WaitGroup` (workerWg) | Worker lifecycle | Waits for all workers to exit |

### Channel Closing Strategy (No Deadlock)

The trickiest part of a worker-pool crawler is knowing **when to close the job channel**. This project uses a two-WaitGroup approach:

- `jobWg` tracks **in-flight jobs** (not workers)
- `enqueue()` calls `jobWg.Add(1)` **before** sending to `jobCh`
- `worker()` calls `jobWg.Done()` **after** processing each job (including enqueueing children)
- A closer goroutine calls `jobWg.Wait()` then `close(jobCh)` — workers exit their `range` loop cleanly

This guarantees `jobCh` closes exactly when the queue is empty with no in-flight work — no deadlock, no missed URLs.

---

## Project Structure

```
webcrawler/
├── cmd/
│   └── crawler/
│       └── main.go              # CLI entry point with flags
├── internal/
│   ├── crawler/
│   │   ├── crawler.go           # Worker pool, job queue, visited set
│   │   └── crawler_test.go      # Integration tests with mock HTTP server
│   ├── parser/
│   │   ├── parser.go            # HTML title + link extraction (stdlib only)
│   │   └── parser_test.go       # Unit tests for parser
│   ├── ratelimiter/
│   │   ├── ratelimiter.go       # Token-bucket rate limiter (mutex-backed)
│   │   └── ratelimiter_test.go  # Concurrency tests for rate limiter
│   └── result/
│       └── result.go            # Page result data structure
├── seeds/
│   └── seeds.txt                # Default seed URLs (one per line)
├── go.mod
└── README.md
```

---

## Getting Started

### Prerequisites
- **Go 1.21+** — [Download here](https://go.dev/dl/)

### Clone & Run

```bash
git clone https://github.com/MahimaChandna28/webcrawler.git
cd Arachne

# Run with default seeds from seeds/seeds.txt
go run ./cmd/crawler

# Or pass seed URLs directly via flag
go run ./cmd/crawler -seed="https://example.com,https://go.dev"
```

### CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-workers` | `5` | Number of concurrent goroutines |
| `-depth` | `2` | Maximum link-follow depth |
| `-maxpages` | `20` | Maximum total pages to crawl |
| `-ratelimit` | `500` | Milliseconds between requests to same domain |
| `-timeout` | `10` | HTTP request timeout in seconds |
| `-seed` | *(seeds.txt)* | Comma-separated seed URLs |
| `-output` | `results.json` | Output file path for JSON results |
| `-samedomain` | `true` | Only follow links on the same domain |

### Example

```bash
# Crawl go.dev with 10 workers, depth 3, max 50 pages
go run ./cmd/crawler \
  -seed="https://go.dev" \
  -workers=10 \
  -depth=3 \
  -maxpages=50 \
  -ratelimit=300 \
  -output=go_dev_results.json
```

### Sample Output

```
🚀 Starting crawler with 5 workers
📋 Seeds: [https://example.com]

  [depth 0] Fetching: https://example.com
  [depth 1] Fetching: https://example.com/about
  [depth 1] Fetching: https://example.com/contact
  ...

✅ Crawl complete. Total pages crawled: 18

📊 Crawl Summary:
────────────────────────────────────────────────────────────
  ✅ [200] https://example.com
     Title: Example Domain | Links found: 4
  ✅ [200] https://example.com/about
     Title: About Us | Links found: 7
  ...
────────────────────────────────────────────────────────────

💾 Results saved to: results.json
```

---

## Running Tests

```bash
# Run all tests
go test ./...

# Run with race detector (detects data races between goroutines)
go test ./... -race

# Run with verbose output
go test ./... -v

# Run a specific package
go test ./internal/crawler/... -v
```

### Test Coverage

| Package | Tests | What's Tested |
|---------|-------|---------------|
| `parser` | 9 cases | Title extraction, link extraction, edge cases |
| `ratelimiter` | 3 cases | Per-domain throttling, concurrent access safety |
| `crawler` | 4 cases | Basic crawl, concurrency, no duplicates, max pages |

The crawler tests use Go's `net/http/httptest` package to spin up a **local mock HTTP server** — no real network requests needed.

---

## What I Learned / Design Decisions

**Why jobWg tracks jobs, not workers?**
If WaitGroup tracked workers, we'd need to close `jobCh` externally after some timeout — fragile. Tracking in-flight jobs lets the system self-terminate naturally when the queue drains.

**Why buffered channels?**
A buffered `jobCh` decouples producers (enqueue) from consumers (workers). Without buffering, every `enqueue()` call would block until a worker picks it up — serializing what should be parallel work.

**Why mutex over channel for visited set?**
The visited map is a shared, randomly-accessed data structure — mutex is the idiomatic Go choice here. A channel-based approach would require a dedicated goroutine, adding unnecessary complexity for a simple read/write guard.

**Why no external dependencies?**
Using only `net/http`, `sync`, `io`, and `encoding/json` from the standard library makes the project portable, auditable, and a better demonstration of Go fundamentals.

---

## Contributing

Contributions and suggestions welcome! Feel free to open an issue or PR.

Ideas for extension:
- [ ] Robots.txt compliance
- [ ] Sitemap.xml support
- [ ] Persistent storage (SQLite) for results
- [ ] Prometheus metrics for crawl stats
- [ ] Context-based cancellation for graceful shutdown
=======
# Arachne
>>>>>>> b7c7c2fc041629ef4975e74c6bbb047176302a42
