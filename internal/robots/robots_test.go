package robots

import (
	"testing"
	"time"
)

func TestRobots_ParseAndIsAllowed(t *testing.T) {
	robotsTxt := `
User-agent: *
Disallow: /admin
Disallow: /private/
Allow: /private/public-doc
Disallow: /*.pdf$
Disallow: /temp/*/report
Sitemap: https://example.com/sitemap.xml
Sitemap: https://example.com/sitemap2.xml
Crawl-delay: 2.5
`

	policy := Parse(robotsTxt, "MyBot/1.0")

	if len(policy.Sitemaps) != 2 {
		t.Errorf("Expected 2 sitemaps, got %d: %v", len(policy.Sitemaps), policy.Sitemaps)
	}
	if policy.CrawlDelay != 2500*time.Millisecond {
		t.Errorf("Expected Crawl-delay 2.5s, got %v", policy.CrawlDelay)
	}

	tests := []struct {
		urlPath string
		allowed bool
	}{
		{"/", true},
		{"/index.html", true},
		{"/about", true},
		{"/admin", false},
		{"/admin/users", false},
		{"/private/secret", false},
		{"/private/public-doc", true},           // Allow overrides Disallow because it's longer
		{"/documents/annual.pdf", false},         // Matched /*.pdf$
		{"/documents/annual.pdf.txt", true},     // Not ending with .pdf
		{"/temp/2026/report", false},            // Matched /temp/*/report
		{"/temp/2026/report/summary", false},    // Prefix of /temp/*/report
	}

	for _, tt := range tests {
		got := policy.IsAllowed(tt.urlPath)
		if got != tt.allowed {
			t.Errorf("IsAllowed(%q) = %v, want %v", tt.urlPath, got, tt.allowed)
		}
	}
}

func TestRobots_WildcardAndSpecificAgent(t *testing.T) {
	robotsTxt := `
User-agent: SpecificBot
Disallow: /only-specific

User-agent: *
Disallow: /global-block
`

	specificPolicy := Parse(robotsTxt, "SpecificBot")
	if specificPolicy.IsAllowed("/only-specific") {
		t.Error("SpecificBot should be blocked from /only-specific")
	}

	otherPolicy := Parse(robotsTxt, "OtherBot")
	if otherPolicy.IsAllowed("/global-block") {
		t.Error("OtherBot should be blocked from /global-block")
	}
	if !otherPolicy.IsAllowed("/only-specific") {
		t.Error("OtherBot should be allowed to /only-specific")
	}
}

func TestRobots_EmptyRules(t *testing.T) {
	var nilPolicy *Policy
	if !nilPolicy.IsAllowed("/anything") {
		t.Error("Nil policy should allow everything")
	}

	emptyPolicy := Parse("", "Bot")
	if !emptyPolicy.IsAllowed("/anything") {
		t.Error("Empty robots.txt should allow everything")
	}
}
