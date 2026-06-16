package parser

import (
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
			name:     "title with whitespace",
			html:     "<title>  Trimmed Title  </title>",
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
			name:     "no links",
			html:     `<p>No links here</p>`,
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractLinks(tt.html)
			if len(got) != len(tt.expected) {
				t.Errorf("ExtractLinks() returned %d links, want %d\ngot: %v\nwant: %v",
					len(got), len(tt.expected), got, tt.expected)
				return
			}
			for i := range got {
				if got[i] != tt.expected[i] {
					t.Errorf("ExtractLinks()[%d] = %q, want %q", i, got[i], tt.expected[i])
				}
			}
		})
	}
}
