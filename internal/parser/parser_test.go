package parser

import (
	"reflect"
	"strings"
	"testing"
)

func TestExtractTitle(t *testing.T) {
	tests := []struct {
		name     string
		html     string
		expected string
	}{
		{
			name:     "normal title",
			html:     "<html><head><title>Hello World</title></head></html>",
			expected: "Hello World",
		},
		{
			name:     "title with whitespace and newlines",
			html:     "<title>  Trimmed \n  Title  </title>",
			expected: "Trimmed Title",
		},
		{
			name:     "no title tag",
			html:     "<html><body>No title here</body></html>",
			expected: "",
		},
		{
			name:     "case insensitive",
			html:     "<TITLE>Uppercase Title</TITLE>",
			expected: "Uppercase Title",
		},
		{
			name:     "title with html entities",
			html:     "<title>Go &amp; Concurrency &mdash; Rocks</title>",
			expected: "Go & Concurrency — Rocks",
		},
		{
			name:     "nested spans inside title",
			html:     "<title>Site <span>Name</span></title>",
			expected: "Site Name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractTitle(tt.html)
			if got != tt.expected {
				t.Errorf("ExtractTitle() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestExtractLinks(t *testing.T) {
	tests := []struct {
		name     string
		html     string
		expected []string
	}{
		{
			name:     "single link",
			html:     `<a href="https://example.com">Click</a>`,
			expected: []string{"https://example.com"},
		},
		{
			name:     "multiple links",
			html:     `<a href="/about">About</a> <a href="/contact">Contact</a>`,
			expected: []string{"/about", "/contact"},
		},
		{
			name:     "skip anchor links",
			html:     `<a href="#section">Jump</a> <a href="/page">Page</a>`,
			expected: []string{"/page"},
		},
		{
			name:     "skip javascript links",
			html:     `<a href="javascript:void(0)">JS</a> <a href="/real">Real</a>`,
			expected: []string{"/real"},
		},
		{
			name:     "skip mailto and tel and data",
			html:     `<a href="mailto:test@example.com">Mail</a><a href="tel:+123456789">Call</a><a href="data:text/html,test">Data</a><a href="/valid">Valid</a>`,
			expected: []string{"/valid"},
		},
		{
			name:     "no links",
			html:     `<p>No links here</p>`,
			expected: nil,
		},
		{
			name: "multi-line tag with attributes and formatting",
			html: `<a
				class="btn btn-primary"
				id="nav-link"
				href="/dashboard">
				Dashboard
			</a>`,
			expected: []string{"/dashboard"},
		},
		{
			name:     "attribute containing greater-than sign",
			html:     `<a href="/items?filter=price>100&sort=asc">Items</a>`,
			expected: []string{"/items?filter=price>100&sort=asc"},
		},
		{
			name: "ignores commented-out links and scripts",
			html: `
				<!-- <a href="/commented-out">Hidden</a> -->
				<script>
					var fake = '<a href="/in-script">Script Link</a>';
				</script>
				<a href="/visible">Visible</a>
			`,
			expected: []string{"/visible"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractLinks(tt.html)
			if len(got) != len(tt.expected) {
				t.Fatalf("ExtractLinks() returned %d links, want %d\ngot: %v\nwant: %v",
					len(got), len(tt.expected), got, tt.expected)
			}
			for i := range got {
				if got[i] != tt.expected[i] {
					t.Errorf("ExtractLinks()[%d] = %q, want %q", i, got[i], tt.expected[i])
				}
			}
		})
	}
}

func TestExtractMetadata(t *testing.T) {
	rawHTML := `
<!DOCTYPE html>
<html lang="en">
<head>
	<meta charset="UTF-8">
	<title>Arachne Web Crawler &amp; Engine</title>
	<link rel="canonical" href="https://example.com/canonical-url">
	<meta name="description" content="A scalable, production-grade web crawler in Go">
	<meta name="robots" content="noindex, nofollow">
	<meta property="og:title" content="Arachne Crawler">
	<meta property="og:image" content="https://example.com/og.png">
	<meta name="twitter:card" content="summary_large_image">
	<meta name="twitter:site" content="@arachne">
</head>
<body>
	<a href="/docs">Docs</a>
	<a href="/pricing">Pricing</a>
</body>
</html>
`

	meta := ExtractMetadata(rawHTML)

	if meta.Title != "Arachne Web Crawler & Engine" {
		t.Errorf("Expected Title 'Arachne Web Crawler & Engine', got %q", meta.Title)
	}
	if meta.Canonical != "https://example.com/canonical-url" {
		t.Errorf("Expected Canonical 'https://example.com/canonical-url', got %q", meta.Canonical)
	}
	if meta.Description != "A scalable, production-grade web crawler in Go" {
		t.Errorf("Expected Description 'A scalable, production-grade web crawler in Go', got %q", meta.Description)
	}
	if !meta.NoIndex {
		t.Errorf("Expected NoIndex=true, got false")
	}
	if !meta.NoFollow {
		t.Errorf("Expected NoFollow=true, got false")
	}
	if meta.OpenGraph["og:title"] != "Arachne Crawler" {
		t.Errorf("Expected og:title 'Arachne Crawler', got %q", meta.OpenGraph["og:title"])
	}
	if meta.OpenGraph["og:image"] != "https://example.com/og.png" {
		t.Errorf("Expected og:image 'https://example.com/og.png', got %q", meta.OpenGraph["og:image"])
	}
	if meta.Twitter["twitter:card"] != "summary_large_image" {
		t.Errorf("Expected twitter:card 'summary_large_image', got %q", meta.Twitter["twitter:card"])
	}
	if meta.Twitter["twitter:site"] != "@arachne" {
		t.Errorf("Expected twitter:site '@arachne', got %q", meta.Twitter["twitter:site"])
	}

	expectedLinks := []string{"/docs", "/pricing"}
	if !reflect.DeepEqual(meta.Links, expectedLinks) {
		t.Errorf("Expected Links %v, got %v", expectedLinks, meta.Links)
	}
}

func FuzzExtractTitle(f *testing.F) {
	seeds := []string{
		"<title>Hello World</title>",
		"<html><head><title>Test &amp; Check</title></head></html>",
		"",
		"<!DOCTYPE html><title>Malformed",
		"<title><script>alert(1)</script></title>",
		strings.Repeat("<a>", 1000),
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, data string) {
		// Must not panic under arbitrary byte stream
		_ = ExtractTitle(data)
	})
}

func FuzzExtractLinks(f *testing.F) {
	seeds := []string{
		`<a href="https://example.com">Link</a>`,
		`<a href="/path?a=>1">Inequality</a>`,
		`<a href="#anchor">Anchor</a>`,
		`<!-- <a href="/comment"> -->`,
		strings.Repeat("<a href='x'>", 500),
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, data string) {
		// Must not panic under arbitrary byte stream
		_ = ExtractLinks(data)
	})
}

func FuzzExtractMetadata(f *testing.F) {
	seeds := []string{
		`<meta name="description" content="test"><link rel="canonical" href="https://example.com">`,
		`<meta property="og:title" content="og test">`,
		`<meta name="robots" content="noindex, nofollow">`,
		"",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, data string) {
		// Must not panic under arbitrary byte stream
		_ = ExtractMetadata(data)
	})
}
