package sitemap

import (
	"strings"
	"testing"
)

func TestSitemap_ParseURLSet(t *testing.T) {
	xmlContent := `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
   <url>
      <loc>https://example.com/</loc>
      <lastmod>2026-01-01</lastmod>
   </url>
   <url>
      <loc>https://example.com/about</loc>
   </url>
   <url>
      <loc>https://example.com/contact</loc>
   </url>
</urlset>`

	res, err := Parse(strings.NewReader(xmlContent))
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if len(res.PageURLs) != 3 {
		t.Fatalf("Expected 3 page URLs, got %d", len(res.PageURLs))
	}
	expected := []string{"https://example.com/", "https://example.com/about", "https://example.com/contact"}
	for i, u := range res.PageURLs {
		if u != expected[i] {
			t.Errorf("PageURLs[%d] = %q, want %q", i, u, expected[i])
		}
	}
}

func TestSitemap_ParseSitemapIndex(t *testing.T) {
	xmlContent := `<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
   <sitemap>
      <loc>https://example.com/sitemap1.xml</loc>
   </sitemap>
   <sitemap>
      <loc>https://example.com/sitemap2.xml.gz</loc>
   </sitemap>
</sitemapindex>`

	res, err := Parse(strings.NewReader(xmlContent))
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if len(res.SitemapURLs) != 2 {
		t.Fatalf("Expected 2 child sitemaps, got %d", len(res.SitemapURLs))
	}
	if res.SitemapURLs[0] != "https://example.com/sitemap1.xml" {
		t.Errorf("Expected sitemap1.xml, got %q", res.SitemapURLs[0])
	}
	if res.SitemapURLs[1] != "https://example.com/sitemap2.xml.gz" {
		t.Errorf("Expected sitemap2.xml.gz, got %q", res.SitemapURLs[1])
	}
}
