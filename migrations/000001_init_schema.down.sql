-- 000001_init_schema.down.sql
-- Rollback migration for Arachne schema

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
