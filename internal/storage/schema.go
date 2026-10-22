package storage

// InitSchemaSQL contains the full DDL to create tables and indexes for Arachne on PostgreSQL 16.
const InitSchemaSQL = `
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS crawl_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    status VARCHAR(32) NOT NULL DEFAULT 'queued',
    config JSONB NOT NULL,
    stats JSONB NOT NULL DEFAULT '{}'::jsonb,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS domains (
    id SERIAL PRIMARY KEY,
    domain VARCHAR(255) UNIQUE NOT NULL,
    robots_txt TEXT,
    crawl_delay_ms INT DEFAULT 500,
    robots_cached_at TIMESTAMPTZ,
    error_count INT DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS urls (
    id BIGSERIAL PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES crawl_jobs(id) ON DELETE CASCADE,
    domain_id INT NOT NULL REFERENCES domains(id),
    raw_url TEXT NOT NULL,
    normalized_hash CHAR(64) NOT NULL,
    depth INT NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL DEFAULT 'discovered',
    status_code INT,
    retries INT DEFAULT 0,
    error_message TEXT,
    fetch_duration_ms INT,
    canonical_url TEXT,
    content_hash CHAR(64),
    crawled_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_job_url_hash UNIQUE (job_id, normalized_hash)
);

CREATE TABLE IF NOT EXISTS pages (
    id BIGSERIAL PRIMARY KEY,
    url_id BIGINT UNIQUE NOT NULL REFERENCES urls(id) ON DELETE CASCADE,
    title TEXT,
    meta_description TEXT,
    text_content TEXT,
    metadata JSONB DEFAULT '{}'::jsonb,
    links_count INT DEFAULT 0,
    outbound_links TEXT[],
    s3_payload_path TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_crawl_jobs_status ON crawl_jobs (status);
CREATE INDEX IF NOT EXISTS idx_domains_domain ON domains (domain);
CREATE INDEX IF NOT EXISTS idx_urls_job_status ON urls (job_id, status);
CREATE INDEX IF NOT EXISTS idx_urls_domain_id ON urls (domain_id);
CREATE INDEX IF NOT EXISTS idx_urls_canonical ON urls (canonical_url) WHERE canonical_url IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_pages_text_gin ON pages USING gin(to_tsvector('english', COALESCE(title, '') || ' ' || COALESCE(text_content, '')));
`

// DropSchemaSQL contains the rollback DDL.
const DropSchemaSQL = `
DROP INDEX IF EXISTS idx_pages_text_gin;
DROP INDEX IF EXISTS idx_urls_canonical;
DROP INDEX IF EXISTS idx_urls_domain_id;
DROP INDEX IF EXISTS idx_urls_job_status;
DROP INDEX IF EXISTS idx_domains_domain;
DROP INDEX IF EXISTS idx_crawl_jobs_status;

DROP TABLE IF EXISTS pages CASCADE;
DROP TABLE IF EXISTS urls CASCADE;
DROP TABLE IF EXISTS domains CASCADE;
DROP TABLE IF EXISTS crawl_jobs CASCADE;
`
