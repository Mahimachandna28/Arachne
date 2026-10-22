package storage

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestStorage_ModelJSONSerialization(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	sampleTitle := "Arachne Crawler"
	statusCode := 200

	job := CrawlJob{
		ID:        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
		Status:    JobStatusRunning,
		Config:    json.RawMessage(`{"workers": 5}`),
		Stats:     json.RawMessage(`{"pages": 10}`),
		CreatedAt: now,
		UpdatedAt: now,
	}

	data, err := json.Marshal(job)
	if err != nil {
		t.Fatalf("Failed to marshal CrawlJob: %v", err)
	}

	var decoded CrawlJob
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal CrawlJob: %v", err)
	}

	if decoded.ID != job.ID || decoded.Status != JobStatusRunning {
		t.Errorf("Mismatch in decoded job: %+v", decoded)
	}

	urlRecord := URLRecord{
		ID:             1,
		JobID:          job.ID,
		DomainID:       10,
		RawURL:         "https://example.com/docs",
		NormalizedHash: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		Depth:          1,
		Status:         URLStatusCompleted,
		StatusCode:     &statusCode,
		Retries:        0,
		CreatedAt:      now,
	}

	urlData, err := json.Marshal(urlRecord)
	if err != nil {
		t.Fatalf("Failed to marshal URLRecord: %v", err)
	}

	var decodedURL URLRecord
	if err := json.Unmarshal(urlData, &decodedURL); err != nil {
		t.Fatalf("Failed to unmarshal URLRecord: %v", err)
	}

	if decodedURL.RawURL != urlRecord.RawURL || *decodedURL.StatusCode != 200 {
		t.Errorf("Mismatch in decoded URL: %+v", decodedURL)
	}

	page := PageDocument{
		ID:            100,
		URLID:         1,
		Title:         &sampleTitle,
		LinksCount:    2,
		OutboundLinks: []string{"https://example.com/about", "https://example.com/contact"},
		CreatedAt:     now,
	}

	pageData, err := json.Marshal(page)
	if err != nil {
		t.Fatalf("Failed to marshal PageDocument: %v", err)
	}

	var decodedPage PageDocument
	if err := json.Unmarshal(pageData, &decodedPage); err != nil {
		t.Fatalf("Failed to unmarshal PageDocument: %v", err)
	}

	if *decodedPage.Title != sampleTitle || len(decodedPage.OutboundLinks) != 2 {
		t.Errorf("Mismatch in decoded Page: %+v", decodedPage)
	}
}

func TestStorage_SchemaDDLValidation(t *testing.T) {
	requiredTables := []string{
		"crawl_jobs",
		"domains",
		"urls",
		"pages",
	}

	for _, table := range requiredTables {
		if !strings.Contains(InitSchemaSQL, "CREATE TABLE IF NOT EXISTS "+table) {
			t.Errorf("InitSchemaSQL missing CREATE TABLE definition for %s", table)
		}
	}

	requiredIndexes := []string{
		"idx_crawl_jobs_status",
		"idx_domains_domain",
		"idx_urls_job_status",
		"idx_urls_domain_id",
		"idx_urls_canonical",
		"idx_pages_text_gin",
	}

	for _, idx := range requiredIndexes {
		if !strings.Contains(InitSchemaSQL, idx) {
			t.Errorf("InitSchemaSQL missing index definition for %s", idx)
		}
	}

	requiredDrops := []string{
		"DROP TABLE IF EXISTS pages",
		"DROP TABLE IF EXISTS urls",
		"DROP TABLE IF EXISTS domains",
		"DROP TABLE IF EXISTS crawl_jobs",
	}

	for _, drop := range requiredDrops {
		if !strings.Contains(DropSchemaSQL, drop) {
			t.Errorf("DropSchemaSQL missing drop statement for %s", drop)
		}
	}
}

func TestStorage_DefaultPoolConfig(t *testing.T) {
	cfg := DefaultPoolConfig()
	if cfg.MaxConns != 50 {
		t.Errorf("Expected default MaxConns=50, got %d", cfg.MaxConns)
	}
	if cfg.MinConns != 5 {
		t.Errorf("Expected default MinConns=5, got %d", cfg.MinConns)
	}
	if cfg.MaxConnLifetime != time.Hour {
		t.Errorf("Expected default MaxConnLifetime=1h, got %v", cfg.MaxConnLifetime)
	}
	if cfg.MaxConnIdleTime != 30*time.Minute {
		t.Errorf("Expected default MaxConnIdleTime=30m, got %v", cfg.MaxConnIdleTime)
	}
}
