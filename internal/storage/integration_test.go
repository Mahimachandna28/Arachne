package storage

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Mahimachandna28/Arachne/internal/result"
)

// Integration tests run only when ARACHNE_TEST_DATABASE_URL points at a
// disposable PostgreSQL instance, e.g.
//
//	ARACHNE_TEST_DATABASE_URL=postgres://postgres@localhost:5432/postgres?sslmode=disable go test ./internal/storage/
func testDB(t *testing.T) *DB {
	t.Helper()
	dsn := os.Getenv("ARACHNE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("ARACHNE_TEST_DATABASE_URL not set; skipping PostgreSQL integration test")
	}
	ctx := context.Background()
	db, err := Connect(ctx, dsn, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := db.InitSchema(ctx); err != nil {
		t.Fatalf("init schema: %v", err)
	}
	t.Cleanup(db.Close)
	return db
}

func newJob(t *testing.T, db *DB) string {
	t.Helper()
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	id := fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
	if err := db.CreateJob(context.Background(), &CrawlJob{ID: id}); err != nil {
		t.Fatalf("create job: %v", err)
	}
	return id
}

func TestIntegration_BatchSaveResults(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	job := newJob(t, db)

	pages := []result.Page{
		{URL: "https://a.example/", Title: "Home", StatusCode: 200, Depth: 0, Links: []string{"/x", "/y"}},
		{URL: "https://a.example/x", Title: "X", StatusCode: 200, Depth: 1, PayloadPath: "payloads/x.gz"},
		{URL: "https://b.example/", StatusCode: 503, Depth: 1, Error: "HTTP 503"},
		{URL: "https://a.example/x", Title: "X v2", StatusCode: 200, Depth: 1}, // duplicate in batch: last wins
	}
	if err := db.BatchSaveResults(ctx, job, pages); err != nil {
		t.Fatalf("BatchSaveResults: %v", err)
	}

	var urlCount, pageCount, domainCount int
	_ = db.Pool().QueryRow(ctx, `SELECT count(*) FROM urls WHERE job_id=$1`, job).Scan(&urlCount)
	_ = db.Pool().QueryRow(ctx, `SELECT count(*) FROM pages p JOIN urls u ON u.id=p.url_id WHERE u.job_id=$1`, job).Scan(&pageCount)
	_ = db.Pool().QueryRow(ctx, `SELECT count(DISTINCT domain_id) FROM urls WHERE job_id=$1`, job).Scan(&domainCount)
	if urlCount != 3 || pageCount != 2 || domainCount != 2 {
		t.Fatalf("urls=%d pages=%d domains=%d, want 3/2/2", urlCount, pageCount, domainCount)
	}

	var depth int
	var title string
	_ = db.Pool().QueryRow(ctx, `
		SELECT u.depth, p.title FROM urls u JOIN pages p ON p.url_id=u.id
		WHERE u.job_id=$1 AND u.raw_url='https://a.example/x'`, job).Scan(&depth, &title)
	if depth != 1 || title != "X v2" {
		t.Fatalf("depth=%d title=%q, want 1 and 'X v2'", depth, title)
	}

	var links []string
	_ = db.Pool().QueryRow(ctx, `
		SELECT p.outbound_links FROM urls u JOIN pages p ON p.url_id=u.id
		WHERE u.job_id=$1 AND u.raw_url='https://a.example/'`, job).Scan(&links)
	if len(links) != 2 || links[0] != "/x" || links[1] != "/y" {
		t.Fatalf("outbound_links=%v", links)
	}

	var status string
	var code int
	_ = db.Pool().QueryRow(ctx, `SELECT status, status_code FROM urls WHERE job_id=$1 AND raw_url='https://b.example/'`, job).Scan(&status, &code)
	if status != URLStatusFailed || code != 503 {
		t.Fatalf("failed url stored as status=%q code=%d", status, code)
	}

	// Re-delivering the same batch must be idempotent.
	if err := db.BatchSaveResults(ctx, job, pages); err != nil {
		t.Fatalf("idempotent re-save: %v", err)
	}
	_ = db.Pool().QueryRow(ctx, `SELECT count(*) FROM urls WHERE job_id=$1`, job).Scan(&urlCount)
	if urlCount != 3 {
		t.Fatalf("re-save created duplicates: %d urls", urlCount)
	}

	visited, err := db.IsURLVisited(ctx, job, HashURL("https://a.example/x"))
	if err != nil || !visited {
		t.Fatalf("IsURLVisited = %v, %v", visited, err)
	}
}

func TestIntegration_BatchWriterLargeVolume(t *testing.T) {
	db := testDB(t)
	job := newJob(t, db)
	bw := NewBatchWriter(db, job, BatchWriterConfig{BatchSize: 200, FlushInterval: 50 * time.Millisecond})
	const n = 5000
	start := time.Now()
	for i := 0; i < n; i++ {
		bw.Submit(result.Page{
			URL:        fmt.Sprintf("https://host%d.example/p/%d", i%50, i),
			Title:      "t",
			StatusCode: 200,
			Depth:      i % 4,
			Links:      []string{"/a", "/b", "/c"},
		})
	}
	if err := bw.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	saved, _ := bw.Stats()
	if saved != n {
		t.Fatalf("saved %d, want %d", saved, n)
	}
	t.Logf("persisted %d pages in %v (%.0f pages/s)", n, time.Since(start), float64(n)/time.Since(start).Seconds())
}
