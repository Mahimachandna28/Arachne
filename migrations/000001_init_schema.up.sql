-- 000001_init_schema.up.sql
-- Production PostgreSQL 16 schema for Arachne Distributed Crawler

-- Enable pgcrypto extension for gen_random_uuid() if not already enabled
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- 1. Crawl Job Master Record
CREATE TABLE IF NOT EXISTS crawl_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    status VARCHAR(32) NOT NULL DEFAULT 'queued', -- queued, running, paused, stopped, completed, failed
    config JSONB NOT NULL,
    stats JSONB NOT NULL DEFAULT '{}'::jsonb,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. Target Domains (Politeness & Rules)
CREATE TABLE IF NOT EXISTS domains (
    id SERIAL PRIMARY KEY,
    domain VARCHAR(255) UNIQUE NOT NULL,
    robots_txt TEXT,
    crawl_delay_ms INT DEFAULT 500,
    robots_cached_at TIMESTAMPTZ,
    error_count INT DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 3. Discovered & Crawled URLs
CREATE TABLE IF NOT EXISTS urls (
    id BIGSERIAL PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES crawl_jobs(id) ON DELETE CASCADE,
    domain_id INT NOT NULL REFERENCES domains(id),
    raw_url TEXT NOT NULL,
    normalized_hash CHAR(64) NOT NULL, -- SHA-256 hex string of canonicalized URL
    depth INT NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL DEFAULT 'discovered', -- discovered, processing, completed, failed
    status_code INT,
    retries INT DEFAULT 0,
    error_message TEXT,
    fetch_duration_ms INT,
    canonical_url TEXT,
    content_hash CHAR(64), -- SHA-256 / SimHash of parsed page content
    crawled_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_job_url_hash UNIQUE (job_id, normalized_hash)
);

-- 4. Parsed Page Documents
CREATE TABLE IF NOT EXISTS pages (
    id BIGSERIAL PRIMARY KEY,
    url_id BIGINT UNIQUE NOT NULL REFERENCES urls(id) ON DELETE CASCADE,
    title TEXT,
    meta_description TEXT,
    text_content TEXT,
    metadata JSONB DEFAULT '{}'::jsonb, -- OpenGraph, Twitter, schema, custom headers
    links_count INT DEFAULT 0,
    outbound_links TEXT[],
    s3_payload_path TEXT, -- S3/MinIO key for raw WARC / compressed HTML payload
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- High-throughput operational indexes
CREATE INDEX IF NOT EXISTS idx_crawl_jobs_status ON crawl_jobs (status);
CREATE INDEX IF NOT EXISTS idx_domains_domain ON domains (domain);
CREATE INDEX IF NOT EXISTS idx_urls_job_status ON urls (job_id, status);
CREATE INDEX IF NOT EXISTS idx_urls_domain_id ON urls (domain_id);
CREATE INDEX IF NOT EXISTS idx_urls_canonical ON urls (canonical_url) WHERE canonical_url IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_pages_text_gin ON pages USING gin(to_tsvector('english', COALESCE(title, '') || ' ' || COALESCE(text_content, '')));
