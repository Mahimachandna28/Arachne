package storage

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/Mahimachandna28/Arachne/internal/result"
)

// BatchSaveResults persists a batch of crawled pages in one transaction using
// four round trips regardless of batch size:
//
//  1. upsert every distinct domain             (1 statement, unnest of text[])
//  2. upsert every URL row, RETURNING its id    (1 statement, unnest of parallel arrays)
//  3. upsert page documents for successful URLs (1 statement, unnest)
//  4. attach outbound link arrays               (1 statement, unnest + array_agg)
//
// The previous implementation issued 2-3 statements per page. With a batch of
// 50 pages that is ~150 round trips versus 4 here.
//
// Upserts are idempotent (ON CONFLICT ... DO UPDATE), so re-delivering a batch
// after a crash or retry is safe — the at-least-once semantics the
// distributed frontier relies on.
func (d *DB) BatchSaveResults(ctx context.Context, jobID string, pages []result.Page) error {
	rows := prepareRows(pages)
	if len(rows) == 0 {
		return nil
	}

	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin batch save: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after Commit

	// 1. Domains
	domainSet := make(map[string]struct{}, len(rows))
	domains := make([]string, 0, len(rows))
	for _, r := range rows {
		if _, ok := domainSet[r.domain]; !ok {
			domainSet[r.domain] = struct{}{}
			domains = append(domains, r.domain)
		}
	}
	domainIDs := make(map[string]int32, len(domains))
	drows, err := tx.Query(ctx, `
		INSERT INTO domains (domain)
		SELECT unnest($1::text[])
		ON CONFLICT (domain) DO UPDATE SET domain = EXCLUDED.domain
		RETURNING id, domain`, domains)
	if err != nil {
		return fmt.Errorf("upsert domains: %w", err)
	}
	for drows.Next() {
		var id int32
		var name string
		if err := drows.Scan(&id, &name); err != nil {
			drows.Close()
			return fmt.Errorf("scan domain id: %w", err)
		}
		domainIDs[name] = id
	}
	drows.Close()
	if err := drows.Err(); err != nil {
		return fmt.Errorf("upsert domains: %w", err)
	}

	// 2. URLs
	n := len(rows)
	var (
		domainIDCol = make([]int32, n)
		rawURLs     = make([]string, n)
		hashes      = make([]string, n)
		depths      = make([]int32, n)
		statuses    = make([]string, n)
		codes       = make([]*int32, n)
		errMsgs     = make([]*string, n)
		canonicals  = make([]*string, n)
	)
	for i, r := range rows {
		domainIDCol[i] = domainIDs[r.domain]
		rawURLs[i] = r.page.URL
		hashes[i] = r.hash
		depths[i] = int32(r.page.Depth)
		statuses[i] = URLStatusCompleted
		if r.page.Error != "" {
			statuses[i] = URLStatusFailed
			errMsgs[i] = ptr(r.page.Error)
		}
		if r.page.StatusCode != 0 {
			c := int32(r.page.StatusCode)
			codes[i] = &c
		}
		canonicals[i] = nonEmpty(r.page.CanonicalURL)
	}

	urlIDs := make(map[string]int64, n)
	urows, err := tx.Query(ctx, `
		INSERT INTO urls (job_id, domain_id, raw_url, normalized_hash, depth, status, status_code, error_message, canonical_url, crawled_at)
		SELECT $1, t.domain_id, t.raw_url, t.hash, t.depth, t.status, t.code, t.err, t.canonical, NOW()
		FROM unnest($2::int[], $3::text[], $4::text[], $5::int[], $6::text[], $7::int[], $8::text[], $9::text[])
		     AS t(domain_id, raw_url, hash, depth, status, code, err, canonical)
		ON CONFLICT (job_id, normalized_hash) DO UPDATE SET
			depth         = LEAST(urls.depth, EXCLUDED.depth),
			status        = EXCLUDED.status,
			status_code   = EXCLUDED.status_code,
			error_message = EXCLUDED.error_message,
			canonical_url = EXCLUDED.canonical_url,
			crawled_at    = EXCLUDED.crawled_at
		RETURNING id, normalized_hash`,
		jobID, domainIDCol, rawURLs, hashes, depths, statuses, codes, errMsgs, canonicals)
	if err != nil {
		return fmt.Errorf("upsert urls: %w", err)
	}
	for urows.Next() {
		var id int64
		var h string
		if err := urows.Scan(&id, &h); err != nil {
			urows.Close()
			return fmt.Errorf("scan url id: %w", err)
		}
		urlIDs[h] = id
	}
	urows.Close()
	if err := urows.Err(); err != nil {
		return fmt.Errorf("upsert urls: %w", err)
	}

	// 3. Pages (successful fetches only)
	var (
		pURLIDs   []int64
		titles    []*string
		descs     []*string
		linkCount []int32
		payloads  []*string
	)
	type linkRow struct {
		urlID int64
		links []string
	}
	var linkRows []linkRow
	for _, r := range rows {
		if r.page.Error != "" {
			continue
		}
		id, ok := urlIDs[r.hash]
		if !ok {
			continue
		}
		pURLIDs = append(pURLIDs, id)
		titles = append(titles, nonEmpty(r.page.Title))
		descs = append(descs, nonEmpty(r.page.Description))
		linkCount = append(linkCount, int32(len(r.page.Links)))
		payloads = append(payloads, nonEmpty(r.page.PayloadPath))
		linkRows = append(linkRows, linkRow{id, r.page.Links})
	}
	if len(pURLIDs) > 0 {
		if _, err := tx.Exec(ctx, `
			INSERT INTO pages (url_id, title, meta_description, links_count, s3_payload_path)
			SELECT * FROM unnest($1::bigint[], $2::text[], $3::text[], $4::int[], $5::text[])
			ON CONFLICT (url_id) DO UPDATE SET
				title            = EXCLUDED.title,
				meta_description = EXCLUDED.meta_description,
				links_count      = EXCLUDED.links_count,
				s3_payload_path  = EXCLUDED.s3_payload_path`,
			pURLIDs, titles, descs, linkCount, payloads); err != nil {
			return fmt.Errorf("upsert pages: %w", err)
		}

		// outbound_links is a text[] per row. Postgres cannot unnest a
		// ragged 2-D array, so links go in a second set-based statement that
		// aggregates (url_id, link) pairs back into arrays.
		var lIDs []int64
		var lVals []string
		for _, lr := range linkRows {
			for _, l := range lr.links {
				lIDs = append(lIDs, lr.urlID)
				lVals = append(lVals, l)
			}
		}
		if len(lIDs) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE pages p SET outbound_links = agg.links
				FROM (
					SELECT t.url_id, array_agg(t.link ORDER BY t.ord) AS links
					FROM unnest($1::bigint[], $2::text[]) WITH ORDINALITY AS t(url_id, link, ord)
					GROUP BY t.url_id
				) agg
				WHERE p.url_id = agg.url_id`, lIDs, lVals); err != nil {
				return fmt.Errorf("update outbound links: %w", err)
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit batch save: %w", err)
	}
	return nil
}

type pageRow struct {
	page   result.Page
	domain string
	hash   string
}

// prepareRows drops unparseable URLs and collapses duplicates within the
// batch (last write wins). A multi-row INSERT ... ON CONFLICT DO UPDATE fails
// with "cannot affect row a second time" if the same key appears twice.
func prepareRows(pages []result.Page) []pageRow {
	idx := make(map[string]int, len(pages))
	rows := make([]pageRow, 0, len(pages))
	for _, p := range pages {
		u, err := url.Parse(p.URL)
		if err != nil || u.Host == "" {
			continue
		}
		h := HashURL(p.URL)
		r := pageRow{page: p, domain: strings.ToLower(u.Host), hash: h}
		if i, dup := idx[h]; dup {
			rows[i] = r
			continue
		}
		idx[h] = len(rows)
		rows = append(rows, r)
	}
	return rows
}

func ptr[T any](v T) *T { return &v }

func nonEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
