// Package parser extracts titles and links from raw HTML content
// using only Go's standard library (no external dependencies).
package parser

import (
	"strings"
)

// ExtractTitle pulls the content of the <title> tag from raw HTML.
// Returns an empty string if no title tag is found.
func ExtractTitle(html string) string {
	lower := strings.ToLower(html)
	start := strings.Index(lower, "<title>")
	if start == -1 {
		return ""
	}
	start += len("<title>")
	end := strings.Index(lower[start:], "</title>")
	if end == -1 {
		return ""
	}
	return strings.TrimSpace(html[start : start+end])
}

// ExtractLinks scans raw HTML and returns all href values found in <a> tags.
// Only returns non-empty, non-anchor links.
func ExtractLinks(html string) []string {
	var links []string
	lower := strings.ToLower(html)
	search := lower

	for {
		// Find next <a tag
		aStart := strings.Index(search, "<a ")
		if aStart == -1 {
			break
		}

		// Find the end of this tag
		aEnd := strings.Index(search[aStart:], ">")
		if aEnd == -1 {
			break
		}

		tag := html[aStart : aStart+aEnd+1]
		tagLower := strings.ToLower(tag)

		// Extract href value
		hrefIdx := strings.Index(tagLower, "href=")
		if hrefIdx != -1 {
			rest := tag[hrefIdx+5:]
			var href string

			if len(rest) > 0 && (rest[0] == '"' || rest[0] == '\'') {
				// Quoted href
				quote := rest[0]
				endQuote := strings.IndexByte(rest[1:], quote)
				if endQuote != -1 {
					href = rest[1 : endQuote+1]
				}
			} else {
				// Unquoted href
				endSpace := strings.IndexAny(rest, " >")
				if endSpace != -1 {
					href = rest[:endSpace]
				}
			}

			href = strings.TrimSpace(href)
			// Skip empty, anchor-only, javascript: links
			if href != "" && !strings.HasPrefix(href, "#") && !strings.HasPrefix(strings.ToLower(href), "javascript:") {
				links = append(links, href)
			}
		}

		// Move past this tag
		search = search[aStart+aEnd+1:]
		html = html[aStart+aEnd+1:]
	}

	return links
}
