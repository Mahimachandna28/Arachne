package robots

import (
	"testing"
	"time"
)

func TestRobots_ParseAndIsAllowed(t *testing.T) {
	content := `
# robots.txt example
User-agent: *
Disallow: /admin
Disallow: /private/
Allow: /private/public.html
Crawl-delay: 2.5
`

	policy := Parse(content, "ArachneCrawlerBot")
	if policy == nil {
		t.Fatal("Expected non-nil policy")
	}

	if policy.CrawlDelay != 2500*time.Millisecond {
		t.Errorf("Expected crawl-delay 2500ms, got %v", policy.CrawlDelay)
	}

	tests := []struct {
		urlPath string
		allowed bool
	}{
		{"/", true},
		{"/index.html", true},
		{"/admin", false},
		{"/admin/dashboard", false},
		{"/private/secret.pdf", false},
		{"/private/public.html", true}, // overridden by Allow
		{"/public", true},
	}

	for _, tc := range tests {
		got := policy.IsAllowed(tc.urlPath)
		if got != tc.allowed {
			t.Errorf("IsAllowed(%q) = %v; expected %v", tc.urlPath, got, tc.allowed)
		}
	}
}

func TestRobots_WildcardAndSpecificAgent(t *testing.T) {
	content := `
User-agent: Googlebot
Disallow: /no-google

User-agent: *
Disallow: /general-block
`

	policy := Parse(content, "ArachneCrawlerBot")
	if policy.IsAllowed("/general-block") {
		t.Errorf("Expected /general-block to be blocked for ArachneCrawlerBot")
	}
	if !policy.IsAllowed("/no-google") {
		t.Errorf("Expected /no-google to be allowed for ArachneCrawlerBot")
	}
}

func TestRobots_EmptyRules(t *testing.T) {
	policy := Parse("", "*")
	if !policy.IsAllowed("/anything") {
		t.Errorf("Expected empty robots.txt to allow all paths")
	}
}
