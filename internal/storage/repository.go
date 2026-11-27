package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	// ErrJobNotFound is returned when querying a job that does not exist.
	ErrJobNotFound = errors.New("crawl job not found")
)

// CreateJob persists a new crawl job record.
func (d *DB) CreateJob(ctx context.Context, job *CrawlJob) error {
	if job.ID == "" {
		return errors.New("job ID must not be empty")
	}
	if job.Status == "" {
		job.Status = JobStatusQueued
	}
	now := time.Now().UTC()
	job.CreatedAt = now
	job.UpdatedAt = now

	cfgJSON := job.Config
	if len(cfgJSON) == 0 {
		cfgJSON = json.RawMessage(`{}`)
	}
	statsJSON := job.Stats
	if len(statsJSON) == 0 {
		statsJSON = json.RawMessage(`{}`)
	}

	query := `
		INSERT INTO crawl_jobs (id, status, config, stats, started_at, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`
	_, err := d.pool.Exec(ctx, query, job.ID, job.Status, cfgJSON, statsJSON, job.StartedAt, job.CreatedAt, job.UpdatedAt)
	if err != nil {
		return fmt.Errorf("failed to create crawl job: %w", err)
	}
	return nil
}

// GetJob retrieves a crawl job by its ID.
func (d *DB) GetJob(ctx context.Context, id string) (*CrawlJob, error) {
	query := `
		SELECT id, status, config, stats, started_at, finished_at, created_at, updated_at
		FROM crawl_jobs
		WHERE id = $1
	`
	row := d.pool.QueryRow(ctx, query, id)

	var job CrawlJob
	err := row.Scan(&job.ID, &job.Status, &job.Config, &job.Stats, &job.StartedAt, &job.FinishedAt, &job.CreatedAt, &job.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrJobNotFound
		}
		return nil, fmt.Errorf("failed to get crawl job: %w", err)
	}
	return &job, nil
}

// UpdateJobStatus updates the state of a crawl job.
func (d *DB) UpdateJobStatus(ctx context.Context, id, status string) error {
	now := time.Now().UTC()
	query := `UPDATE crawl_jobs SET status = $1, updated_at = $2 WHERE id = $3`
	tag, err := d.pool.Exec(ctx, query, status, now, id)
	if err != nil {
		return fmt.Errorf("failed to update job status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrJobNotFound
	}
	return nil
}

// FinishJob marks a job as completed or failed and records final statistics and finish time.
func (d *DB) FinishJob(ctx context.Context, id, status string, stats map[string]interface{}) error {
	now := time.Now().UTC()
	statsBytes, err := json.Marshal(stats)
	if err != nil {
		statsBytes = []byte(`{}`)
	}

	query := `
		UPDATE crawl_jobs
		SET status = $1, stats = $2, finished_at = $3, updated_at = $3
		WHERE id = $4
	`
	tag, err := d.pool.Exec(ctx, query, status, statsBytes, now, id)
	if err != nil {
		return fmt.Errorf("failed to finish job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrJobNotFound
	}
	return nil
}

// GetOrCreateDomain retrieves or initializes a domain record.
func (d *DB) GetOrCreateDomain(ctx context.Context, domainName string) (int, error) {
	domainName = strings.ToLower(strings.TrimSpace(domainName))
	query := `
		INSERT INTO domains (domain) VALUES ($1)
		ON CONFLICT (domain) DO UPDATE SET domain = EXCLUDED.domain
		RETURNING id
	`
	var id int
	err := d.pool.QueryRow(ctx, query, domainName).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("failed to get or create domain: %w", err)
	}
	return id, nil
}

// HashURL computes a normalized SHA-256 hash of a URL.
func HashURL(rawURL string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(rawURL)))
	return hex.EncodeToString(sum[:])
}

// IsURLVisited checks whether a URL (by normalized hash) has already been stored for a crawl job.
func (d *DB) IsURLVisited(ctx context.Context, jobID, normalizedHash string) (bool, error) {
	query := `SELECT 1 FROM urls WHERE job_id = $1 AND normalized_hash = $2 LIMIT 1`
	var exists int
	err := d.pool.QueryRow(ctx, query, jobID, normalizedHash).Scan(&exists)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("failed to check url visited status: %w", err)
	}
	return true, nil
}

// IsVisited implements dedup.ExactVerifier so a *DB can serve as the exact
// (tier 2) check behind a Bloom filter.
func (d *DB) IsVisited(ctx context.Context, jobID, normalizedHash string) (bool, error) {
	return d.IsURLVisited(ctx, jobID, normalizedHash)
}
