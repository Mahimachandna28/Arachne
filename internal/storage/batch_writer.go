package storage

import (
	"context"
	"sync"
	"time"

	"github.com/mahimachandna28/webcrawler/internal/result"
)

// BatchWriterConfig controls the flush thresholds of the streaming persistence pipeline.
type BatchWriterConfig struct {
	BatchSize     int           // Max items to buffer before flushing to PostgreSQL (default: 50)
	FlushInterval time.Duration // Max duration to wait before flushing pending items (default: 250ms)
}

// BatchWriter runs an asynchronous worker loop that buffers incoming crawled pages
// and writes them transactionally to PostgreSQL without blocking crawler workers.
type BatchWriter struct {
	db        *DB
	jobID     string
	config    BatchWriterConfig
	inputCh   chan result.Page
	flushDone chan struct{}
	closeOnce sync.Once

	statsMu    sync.Mutex
	totalSaved int
	totalError int
}

// NewBatchWriter initializes and starts a streaming BatchWriter.
func NewBatchWriter(db *DB, jobID string, cfg BatchWriterConfig) *BatchWriter {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 50
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 250 * time.Millisecond
	}

	bw := &BatchWriter{
		db:        db,
		jobID:     jobID,
		config:    cfg,
		inputCh:   make(chan result.Page, cfg.BatchSize*4),
		flushDone: make(chan struct{}),
	}

	go bw.loop()
	return bw
}

// Submit queues a crawled page for asynchronous batch persistence.
func (bw *BatchWriter) Submit(p result.Page) {
	bw.inputCh <- p
}

// loop buffers incoming results and flushes them by batch size or timer ticks.
func (bw *BatchWriter) loop() {
	defer close(bw.flushDone)

	ticker := time.NewTicker(bw.config.FlushInterval)
	defer ticker.Stop()

	buffer := make([]result.Page, 0, bw.config.BatchSize)

	flush := func() {
		if len(buffer) == 0 {
			return
		}
		toPersist := buffer
		buffer = make([]result.Page, 0, bw.config.BatchSize)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = bw.db.BatchSaveResults(ctx, bw.jobID, toPersist)
		cancel()

		bw.statsMu.Lock()
		for _, p := range toPersist {
			bw.totalSaved++
			if p.Error != "" {
				bw.totalError++
			}
		}
		bw.statsMu.Unlock()
	}

	for {
		select {
		case p, ok := <-bw.inputCh:
			if !ok {
				flush()
				return
			}
			buffer = append(buffer, p)
			if len(buffer) >= bw.config.BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// Close closes the input channel, flushes all remaining records, and waits for completion.
func (bw *BatchWriter) Close() error {
	bw.closeOnce.Do(func() {
		close(bw.inputCh)
	})
	<-bw.flushDone
	return nil
}

// Stats returns the total count of saved pages and errors.
func (bw *BatchWriter) Stats() (saved int, errors int) {
	bw.statsMu.Lock()
	defer bw.statsMu.Unlock()
	return bw.totalSaved, bw.totalError
}
