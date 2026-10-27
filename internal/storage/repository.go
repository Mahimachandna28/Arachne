package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mahimachandna28/webcrawler/internal/result"
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

// BatchSaveResults persists a batch of crawled pages transactionally using pgx.Batch.
func (d *DB) BatchSaveResults(ctx context.Context, jobID string, pages []result.Page) error {
	if len(pages) == 0 {
		return nil
	}

	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, p := range pages {
		u, err := url.Parse(p.URL)
		if err != nil || u.Host == "" {
			continue
		}
		domain := strings.ToLower(u.Host)

		// 1. Resolve domain ID
		var domainID int
		err = tx.QueryRow(ctx, `
			INSERT INTO domains (domain) VALUES ($1)
			ON CONFLICT (domain) DO UPDATE SET domain = EXCLUDED.domain
			RETURNING id
		`, domain).Scan(&domainID)
		if err != nil {
			return fmt.Errorf("failed to resolve domain %q: %w", domain, err)
		}

		// 2. Insert into urls
		urlHash := HashURL(p.URL)
		status := URLStatusCompleted
		var errMessage *string
		if p.Error != "" {
			status = URLStatusFailed
			errMessage = &p.Error
		}

		var canonicalPtr *string
		if p.CanonicalURL != "" {
			canonicalPtr = &p.CanonicalURL
		}

		var urlID int64
		urlInsert := `
			INSERT INTO urls (job_id, domain_id, raw_url, normalized_hash, depth, status, status_code, error_message, canonical_url, crawled_at)
			VALUES ($1, $2, $3, $4, 0, $5, $6, $7, $8, NOW())
			ON CONFLICT (job_id, normalized_hash) DO UPDATE SET
				status = EXCLUDED.status,
				status_code = EXCLUDED.status_code,
				error_message = EXCLUDED.error_message,
				canonical_url = EXCLUDED.canonical_url,
				crawled_at = EXCLUDED.crawled_at
			RETURNING id
		`
		err = tx.QueryRow(ctx, urlInsert, jobID, domainID, p.URL, urlHash, status, p.StatusCode, errMessage, canonicalPtr).Scan(&urlID)
		if err != nil {
			return fmt.Errorf("failed to insert url record %q: %w", p.URL, err)
		}

		// 3. Insert page document if successful
		if p.Error == "" {
			var titlePtr *string
			if p.Title != "" {
				titlePtr = &p.Title
			}
			var descPtr *string
			if p.Description != "" {
				descPtr = &p.Description
			}

			var payloadPathPtr *string
			if p.PayloadPath != "" {
				payloadPathPtr = &p.PayloadPath
			}

			pageInsert := `
				INSERT INTO pages (url_id, title, meta_description, links_count, outbound_links, s3_payload_path)
				VALUES ($1, $2, $3, $4, $5, $6)
				ON CONFLICT (url_id) DO UPDATE SET
					title = EXCLUDED.title,
					meta_description = EXCLUDED.meta_description,
					links_count = EXCLUDED.links_count,
					outbound_links = EXCLUDED.outbound_links,
					s3_payload_path = EXCLUDED.s3_payload_path
			`
			_, err = tx.Exec(ctx, pageInsert, urlID, titlePtr, descPtr, len(p.Links), p.Links, payloadPathPtr)
			if err != nil {
				return fmt.Errorf("failed to insert page document for url %d: %w", urlID, err)
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit batch save transaction: %w", err)
	}
	return nil
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
