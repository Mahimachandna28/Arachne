# 🕷️ Arachne — a polite, concurrent web crawler in Go

Arachne is a web crawler built around the problems real crawlers have to solve:
**politeness** (robots.txt, crawl-delay, per-host rate limits, `Retry-After`),
**correctness** (URL canonicalization, deduplication, exact termination),
**safety** (SSRF protection, size and content-type guards, spider-trap detection)
and **throughput** (non-blocking per-host scheduling, batched persistence).

It runs as a single binary with zero infrastructure, and optionally persists to
PostgreSQL and shares a Bloom-filter dedup set through Redis.

```bash
go run ./cmd/crawler -seed=https://go.dev -depth=2 -maxpages=100
```

---

## Architecture

```mermaid
flowchart LR
    Seeds([Seeds / sitemaps]) --> E
    subgraph Admission ["enqueue() — never blocks"]
        E[urlnorm.Normalize] --> T{spider trap?}
        T -->|no| R{robots cached<br/>& disallowed?}
        R -->|no| V{seen?<br/>64-bit FNV set}
        V -->|new| D{Bloom + DB<br/>optional}
        D -->|new| B{page budget<br/>global / per-host}
    end
    B --> S
    subgraph S ["Scheduler (per-host back queues)"]
        direction TB
        Q1[host A: u1 u2 u3]
        Q2[host B: u4]
        Q3[host C: u5 u6]
        H[[min-heap by<br/>nextAllowed]]
    end
    S -->|Next: only hosts<br/>ready *now*| W[Worker pool<br/>N goroutines]
    W -->|first visit to host| RB[robots.txt<br/>singleflight]
    W -->|5xx / 429 / net err| S
    W -->|children| E
    W --> C[Collector]
    C --> J[(results.json<br/>streamed)]
    C --> BW[BatchWriter] --> PG[(PostgreSQL)]
    D -.-> RD[(Redis Bloom)]
```

### The scheduler (why it is not just a channel)

The first version of Arachne used a classic worker pool: workers read jobs from
a buffered channel and pushed discovered links back into **the same channel**.
That design has two flaws that show up as soon as you crawl a real site:

1. **Deadlock.** When the frontier outgrows the buffer (`Workers*20`), every
   worker blocks sending children and no one is left to receive. A regression
   test (`TestCrawler_WideFanOutDoesNotDeadlock`) reproduced it: with 2 workers
   the old crawler froze after **6 fetches** of a 1,111-page site.
2. **Head-of-line blocking.** Rate limiting happened *inside* the worker, so a
   worker holding a URL for a throttled host slept while other hosts had work
   ready. With two workers and one slow host, throughput collapsed.

`internal/scheduler` replaces the channel with a Mercator-style frontier:

- one **unbounded FIFO queue per host**, so `Push` never blocks;
- a **min-heap of hosts keyed by `nextAllowed`**. `Next()` only hands out a URL
  whose host may be contacted *now*, then advances that host's clock by its
  interval (default, robots `Crawl-delay`, or `Retry-After`);
- **retries are rescheduled, not slept on**. A 5xx/429 puts the job back at the
  front of its host queue and pushes the host's clock out by a full-jitter
  exponential backoff, so the worker immediately moves on to other hosts;
- **exact termination**: the crawl is done when nothing is queued, in flight,
  or *held* by a producer (the seeding loop and async sitemap fetches take a
  hold), so no "wait group closes the channel" tricks are needed.

### Request pipeline

| Stage | What it does |
|---|---|
| `urlnorm` | Lower-cases scheme/host, strips default ports, userinfo, fragments and tracking params (`utm_*`, `gclid`, `fbclid`, …), resolves `.`/`..`, sorts query keys, removes trailing slashes. One canonical string per resource. |
| Spider-trap guard | Rejects URLs longer than 2 KB, with more than 10 query params, or with a path segment repeated ≥3 times (`/a/b/a/b/a`). |
| Dedup tier 0 | In-process set of 64-bit FNV-1a fingerprints (8 B/URL instead of the full string). |
| Dedup tiers 1+2 (optional) | Redis Bloom filter (`BF.ADD`, or a `SETBIT` fallback on vanilla Redis), then an exact PostgreSQL lookup **only on Bloom positives**, which recovers false positives instead of silently dropping URLs. |
| robots.txt | Fetched lazily by the first worker scheduled for a host, **once** even under concurrent demand (`singleflight`). Applies `Disallow`/`Allow` with longest-match precedence, `Crawl-delay`, and discovers `Sitemap:` entries. |
| SSRF guard (`netguard`) | Custom `DialContext` that resolves DNS itself and refuses private, loopback, link-local and cloud-metadata addresses, which also defeats DNS rebinding. |
| Fetch | Tuned `http.Transport` connection pool, redirect cap, `text/html` and `xhtml` only, `Content-Length` and streaming body caps, charset sniffing and transcoding to UTF-8. |
| Parse | WHATWG-compliant tokenizer (`x/net/html`): title, meta description, canonical, `<meta robots nofollow>`, links. |
| Persist | Results stream to `results.json` (flat memory) and, optionally, to PostgreSQL via an async `BatchWriter`; raw HTML is gzipped to a blob store. |

### Persistence

`BatchSaveResults` writes a batch in **4 round trips regardless of batch size**,
using `unnest()` over parallel arrays: upsert domains → upsert URLs
`RETURNING id` → upsert pages → attach outbound-link arrays (`array_agg`
`WITH ORDINALITY`). All statements are idempotent `ON CONFLICT … DO UPDATE`, so
re-delivering a batch after a crash is harmless. That is the at-least-once
contract the distributed frontier (`internal/frontier`, Redis Streams) builds on.

---

## Benchmarks

Measured on a 2-vCPU cloud container (Go 1.26, PostgreSQL 16 and Redis 7 on
localhost). Reproduce with the commands shown.

| What | Result | Command |
|---|---|---|
| End-to-end crawl, synthetic 1,111-page site, 32 workers, no delay | **~18,000 pages/s** | `go test -run x -bench FanOut ./internal/crawler/` |
| Same crawl, old channel-based design, 2 workers | **deadlocked after 6 pages** | `TestCrawler_WideFanOutDoesNotDeadlock` (on the pre-scheduler commit) |
| Persist 5,000 pages, batches of 50, old per-row inserts | 2,977 pages/s | — |
| Same, set-based `unnest` batch save | **10,075 pages/s (3.4×)** | `TestIntegration_BatchWriterLargeVolume` |

The persistence gap widens over a real network: per-row inserts cost about 150
round trips per 50-page batch, versus 4.

---

## Getting started

Requires **Go 1.26+** (the current `x/net`, `x/text` and `go-redis` releases need it).

```bash
git clone https://github.com/Mahimachandna28/Arachne.git
cd Arachne

# In-memory crawl; results streamed to results.json
go run ./cmd/crawler -seed=https://go.dev -depth=2 -maxpages=100

# View results in the dashboard (serves index.html + results.json)
go run ./cmd/web -port 8080
```

### With PostgreSQL and Redis

```bash
docker compose up -d            # postgres:16 on :5432, redis:7 on :6379

go run ./cmd/crawler -seed=https://go.dev -maxpages=500 \
  -db-url='postgres://arachne:arachne@localhost:5432/arachne?sslmode=disable' \
  -redis-addr=localhost:6379
```

The job, every URL (with depth, status and error) and every parsed page land in
PostgreSQL. A full-text GIN index over title and text is created by the schema.

### Flags

| Flag | Default | Description |
|---|---|---|
| `-seed` | *(seeds/seeds.txt)* | Comma-separated seed URLs |
| `-workers` | `5` | Concurrent fetchers |
| `-depth` | `2` | Max link depth from a seed |
| `-maxpages` | `20` | Global page budget (`0` = unlimited) |
| `-maxpagesperdomain` | `0` | Per-host page budget |
| `-ratelimit` | `500` | Default ms between requests to one host (robots `Crawl-delay` and `Retry-After` override it) |
| `-retries` | `2` | Retries for network errors, 5xx and 429 |
| `-timeout` | `10` | HTTP timeout, seconds |
| `-maxduration` | `0` | Stop after this long (e.g. `5m`) |
| `-samedomain` | `true` | Only follow links on the same host |
| `-output` | `results.json` | Streamed JSON array of results (`""` to disable) |
| `-payload-dir` | — | Archive gzipped raw HTML here |
| `-db-url` | `$ARACHNE_DATABASE_URL` | PostgreSQL DSN, enables persistence |
| `-redis-addr` | `$ARACHNE_REDIS_ADDR` | Redis address, enables the shared Bloom filter |
| `-bloom-capacity` / `-bloom-fp` | `10M` / `0.001` | Bloom filter sizing |
| `-allowprivate` | `false` | Disable SSRF guard (local testing only) |
| `-quiet` | `false` | No per-URL output |

`Ctrl-C` cancels gracefully: in-flight requests abort, buffered results are
flushed, and the job is marked `stopped`.

---

## Testing

```bash
go test -race ./...                       # unit + crawler tests (httptest servers, no network)

# PostgreSQL integration tests (skipped unless the env var is set)
docker compose up -d postgres
ARACHNE_TEST_DATABASE_URL='postgres://arachne:arachne@localhost:5432/arachne?sslmode=disable' \
  go test -race ./internal/storage/
```

Highlights:

- **Regression tests for the concurrency bugs**: wide fan-out deadlock,
  politeness not stalling other hosts, robots.txt fetched exactly once under
  16 concurrent workers.
- **Scheduler**: FIFO, politeness spacing, retry backoff, cancellation, and a
  20,000-job fan-out with 16 producer/consumer goroutines under `-race`.
- **Crawler behaviour**: robots compliance, canonical dedup, retries and
  `Retry-After`, content-type and size guards, charset decoding, SSRF, spider
  traps, per-host caps, context cancellation and timeouts, blob archiving.
- **Storage**: batch upsert correctness, in-batch duplicates, idempotent
  re-delivery, outbound-link arrays, 5,000-page throughput.

---

## Project layout

```
cmd/
  crawler/      CLI: flags, Postgres/Redis wiring, streamed JSON output
  web/          static dashboard server
internal/
  crawler/      admission, worker loop, fetch pipeline
  scheduler/    per-host frontier: back queues + ready heap
  urlnorm/      URL canonicalization and 64-bit fingerprints
  robots/       robots.txt parser (Allow/Disallow precedence, wildcards, Crawl-delay)
  sitemap/      sitemap and sitemap-index parser
  parser/       HTML metadata and link extraction
  netguard/     SSRF-safe dialer
  ratelimiter/  per-host interval registry
  dedup/        Bloom filters (memory, Redis) + two-tier deduplicator
  storage/      PostgreSQL pool, schema, set-based batch writer
  blobstore/    gzipped payload store
  frontier/     Redis Streams frontier (consumer groups, XAUTOCLAIM, DLQ) — for distributed mode
migrations/     SQL migrations (golang-migrate format)
```

---

## Design decisions & trade-offs

**Per-host queues versus one global queue.** A global FIFO preserves BFS order
but forces either sleeping workers or out-of-order skipping to stay polite.
Per-host queues give politeness for free and keep workers busy. The cost is
that the crawl is only *approximately* breadth-first across hosts.

**Retry by rescheduling, and backing off the whole host.** A 5xx or 429 says
the *server* is struggling, so the host's clock is pushed back, not just the
one URL's. The worker is released immediately.

**64-bit fingerprints versus strings.** About 10× less memory for the visited
set. Collision probability at 100M URLs is about 3×10⁻⁴, which is acceptable
for a crawler (worst case: one page skipped).

**Why the in-process set stays even with Redis/Postgres dedup.** The DB tier
only sees a URL after the batch writer flushes it. A URL rediscovered while its
first fetch is still pending would pass the DB check. Tier 0 covers that window
in a single process; across processes the plan is an atomic claim at admission
(see roadmap).

**At-least-once persistence.** Every write is an idempotent upsert, so the
system tolerates duplicate delivery. That is far easier to reason about than
exactly-once.

---

## Roadmap

- [x] **Phase 1 — correctness**: per-host scheduler (deadlock and head-of-line
      blocking fixed), URL normalization, robots via singleflight, bounded
      memory and streamed output, set-based batch persistence, depth recorded,
      Postgres/Redis wired into the CLI
- [ ] **Phase 2 — distributed**: coordinator plus stateless workers over Redis
      Streams (`internal/frontier`), cluster-wide per-host rate limiting (Redis
      Lua token bucket), atomic URL claim at admission
      (`INSERT … ON CONFLICT DO NOTHING`), `XAUTOCLAIM` crash recovery, DLQ
- [ ] **Phase 3 — observability**: Prometheus metrics (pages/s, latency
      histograms, frontier depth, Bloom FP rate), `log/slog`, health endpoints,
      pprof, Grafana dashboard
- [ ] **Phase 4 — operations**: full docker-compose stack with scalable
      workers, S3/MinIO blob store, REST API for jobs plus full-text search,
      GitHub Actions CI with `-race`, `golangci-lint` and integration tests
- [ ] **Phase 5 — features**: SimHash near-duplicate detection, conditional
      recrawl (ETag / `Last-Modified`), headless rendering for JS-heavy sites
