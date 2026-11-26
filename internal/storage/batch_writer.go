package storage

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Mahimachandna28/Arachne/internal/result"
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

	statsMu     sync.Mutex
	totalSaved  int   // pages durably written
	totalError  int   // saved pages that were crawl failures (HTTP/network error)
	totalFailed int   // pages lost because the DB write itself failed
	lastErr     error // most recent DB write error
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

		// One retry covers transient failures (connection reset, failover).
		var err error
		for attempt := 0; attempt < 2; attempt++ {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err = bw.db.BatchSaveResults(ctx, bw.jobID, toPersist)
			cancel()
			if err == nil {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}

		bw.statsMu.Lock()
		if err != nil {
			bw.totalFailed += len(toPersist)
			bw.lastErr = err
		} else {
			for _, p := range toPersist {
				bw.totalSaved++
				if p.Error != "" {
					bw.totalError++
				}
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
	bw.statsMu.Lock()
	defer bw.statsMu.Unlock()
	if bw.totalFailed > 0 {
		return fmt.Errorf("batch writer: %d pages not persisted, last error: %w", bw.totalFailed, bw.lastErr)
	}
	return nil
}

// Stats returns the total count of saved pages and errors.
func (bw *BatchWriter) Stats() (saved int, errors int) {
	bw.statsMu.Lock()
	defer bw.statsMu.Unlock()
	return bw.totalSaved, bw.totalError
}
