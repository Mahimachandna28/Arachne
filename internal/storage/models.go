// Package storage provides the PostgreSQL persistence layer and data models
// for crawl jobs, domain records, discovered URLs, and parsed page documents.
package storage

import (
	"encoding/json"
	"time"
)

// Job status constants
const (
	JobStatusQueued    = "queued"
	JobStatusRunning   = "running"
	JobStatusPaused    = "paused"
	JobStatusStopped   = "stopped"
	JobStatusCompleted = "completed"
	JobStatusFailed    = "failed"
)

// URL status constants
const (
	URLStatusDiscovered = "discovered"
	URLStatusProcessing = "processing"
	URLStatusCompleted  = "completed"
	URLStatusFailed     = "failed"
)

// CrawlJob represents the master record of a crawl execution.
type CrawlJob struct {
	ID         string          `json:"id"`
	Status     string          `json:"status"`
	Config     json.RawMessage `json:"config"`
	Stats      json.RawMessage `json:"stats"`
	StartedAt  *time.Time      `json:"started_at,omitempty"`
	FinishedAt *time.Time      `json:"finished_at,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

// Domain represents a target hostname, its politeness configuration, and robots.txt state.
type Domain struct {
	ID             int        `json:"id"`
	Domain         string     `json:"domain"`
	RobotsTxt      *string    `json:"robots_txt,omitempty"`
	CrawlDelayMs   int        `json:"crawl_delay_ms"`
	RobotsCachedAt *time.Time `json:"robots_cached_at,omitempty"`
	ErrorCount     int        `json:"error_count"`
	CreatedAt      time.Time  `json:"created_at"`
}

// URLRecord represents a single discovered, queued, or crawled URL.
type URLRecord struct {
	ID              int64      `json:"id"`
	JobID           string     `json:"job_id"`
	DomainID        int        `json:"domain_id"`
	RawURL          string     `json:"raw_url"`
	NormalizedHash  string     `json:"normalized_hash"` // SHA-256 hex string
	Depth           int        `json:"depth"`
	Status          string     `json:"status"`
	StatusCode      *int       `json:"status_code,omitempty"`
	Retries         int        `json:"retries"`
	ErrorMessage    *string    `json:"error_message,omitempty"`
	FetchDurationMs *int       `json:"fetch_duration_ms,omitempty"`
	CanonicalURL    *string    `json:"canonical_url,omitempty"`
	ContentHash     *string    `json:"content_hash,omitempty"`
	CrawledAt       *time.Time `json:"crawled_at,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
}

// PageDocument represents the extracted text, metadata, and link graph of a crawled HTML page.
type PageDocument struct {
	ID              int64           `json:"id"`
	URLID           int64           `json:"url_id"`
	Title           *string         `json:"title,omitempty"`
	MetaDescription *string         `json:"meta_description,omitempty"`
	TextContent     *string         `json:"text_content,omitempty"`
	Metadata        json.RawMessage `json:"metadata,omitempty"` // OpenGraph, Twitter, Schema, headers
	LinksCount      int             `json:"links_count"`
	OutboundLinks   []string        `json:"outbound_links,omitempty"`
	S3PayloadPath   *string         `json:"s3_payload_path,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
}
