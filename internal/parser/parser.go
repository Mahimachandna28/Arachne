// Package parser extracts titles, links, and structured metadata from raw HTML content
// using WHATWG-compliant HTML5 tokenization and DOM parsing via golang.org/x/net/html.
package parser

import (
	"html"
	"io"
	"strings"

	nethtml "golang.org/x/net/html"
	"golang.org/x/net/html/atom"
)

// PageMetadata holds the extracted metadata and discovered links from a parsed HTML document.
type PageMetadata struct {
	Title       string            `json:"title,omitempty"`
	Canonical   string            `json:"canonical,omitempty"`
	Description string            `json:"description,omitempty"`
	Robots      string            `json:"robots,omitempty"`
	NoIndex     bool              `json:"noindex,omitempty"`
	NoFollow    bool              `json:"nofollow,omitempty"`
	OpenGraph   map[string]string `json:"opengraph,omitempty"`
	Twitter     map[string]string `json:"twitter,omitempty"`
	Links       []string          `json:"links,omitempty"`
}

// Parse reads an HTML document from r and extracts all metadata, canonical links,
// robots directives, social tags, and outbound hyperlinks.
func Parse(r io.Reader) (PageMetadata, error) {
	doc, err := nethtml.Parse(r)
	if err != nil {
		return PageMetadata{}, err
	}

	meta := PageMetadata{
		OpenGraph: make(map[string]string),
		Twitter:   make(map[string]string),
	}

	var walk func(*nethtml.Node)
	walk = func(n *nethtml.Node) {
		if n.Type == nethtml.ElementNode {
			switch n.DataAtom {
			case atom.Title:
				if meta.Title == "" {
					meta.Title = extractNodeText(n)
				}
			case atom.Link:
				rel, href := "", ""
				for _, a := range n.Attr {
					key := strings.ToLower(strings.TrimSpace(a.Key))
					if key == "rel" {
						rel = strings.ToLower(strings.TrimSpace(a.Val))
					} else if key == "href" {
						href = strings.TrimSpace(a.Val)
					}
				}
				if isCanonicalRel(rel) && href != "" && meta.Canonical == "" {
					meta.Canonical = href
				}
			case atom.Meta:
				name, property, content := "", "", ""
				for _, a := range n.Attr {
					key := strings.ToLower(strings.TrimSpace(a.Key))
					val := strings.TrimSpace(a.Val)
					switch key {
					case "name":
						name = strings.ToLower(val)
					case "property":
						property = strings.ToLower(val)
					case "content":
						content = val
					}
				}
				processMetaTag(&meta, name, property, content)
			case atom.A:
				for _, a := range n.Attr {
					if strings.ToLower(strings.TrimSpace(a.Key)) == "href" {
						href := strings.TrimSpace(a.Val)
						if isValidCrawlLink(href) {
							meta.Links = append(meta.Links, href)
						}
					}
				}
			}
		}

		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}

	walk(doc)
	return meta, nil
}

// ParseHTML parses an HTML string and returns its extracted metadata.
func ParseHTML(htmlStr string) PageMetadata {
	meta, _ := Parse(strings.NewReader(htmlStr))
	return meta
}

// ExtractTitle pulls and sanitizes the content of the <title> tag from raw HTML.
// Returns an empty string if no title tag is found.
func ExtractTitle(htmlStr string) string {
	return ParseHTML(htmlStr).Title
}

// ExtractLinks parses raw HTML and returns all valid, actionable href values found in <a> tags.
// Skips empty, anchor-only (#), and non-HTTP pseudo-schemes (javascript:, mailto:, tel:, data:).
func ExtractLinks(htmlStr string) []string {
	return ParseHTML(htmlStr).Links
}

// ExtractMetadata extracts the full PageMetadata from an HTML string.
func ExtractMetadata(htmlStr string) PageMetadata {
	return ParseHTML(htmlStr)
}

// extractNodeText recursively extracts, sanitizes, and cleans all text content inside an HTML node.
func extractNodeText(n *nethtml.Node) string {
	var sb strings.Builder
	var collect func(*nethtml.Node)
	collect = func(node *nethtml.Node) {
		if node.Type == nethtml.TextNode {
			sb.WriteString(node.Data)
		}
		for c := node.FirstChild; c != nil; c = c.NextSibling {
			collect(c)
		}
	}
	collect(n)

	raw := sb.String()
	// Strip any unexpected inner HTML tags (e.g. <span>, <b> inside RCDATA <title>)
	var cleaned strings.Builder
	inTag := false
	for _, r := range raw {
		if r == '<' {
			inTag = true
			continue
		}
		if r == '>' {
			inTag = false
			continue
		}
		if !inTag {
			cleaned.WriteRune(r)
		}
	}

	unescaped := html.UnescapeString(cleaned.String())
	return strings.Join(strings.Fields(unescaped), " ")
}

func isCanonicalRel(rel string) bool {
	fields := strings.Fields(rel)
	for _, f := range fields {
		if f == "canonical" {
			return true
		}
	}
	return false
}

func processMetaTag(meta *PageMetadata, name, property, content string) {
	if content == "" {
		return
	}

	key := name
	if key == "" {
		key = property
	}

	switch {
	case key == "description":
		if meta.Description == "" {
			meta.Description = content
		}
	case key == "robots":
		meta.Robots = content
		lower := strings.ToLower(content)
		if strings.Contains(lower, "noindex") {
			meta.NoIndex = true
		}
		if strings.Contains(lower, "nofollow") {
			meta.NoFollow = true
		}
	case strings.HasPrefix(key, "og:"):
		meta.OpenGraph[key] = content
	case strings.HasPrefix(key, "twitter:"):
		meta.Twitter[key] = content
	}
}

// isValidCrawlLink checks whether an extracted href is eligible for crawling.
func isValidCrawlLink(href string) bool {
	if href == "" {
		return false
	}
	if strings.HasPrefix(href, "#") {
		return false
	}

	lower := strings.ToLower(href)
	if strings.HasPrefix(lower, "javascript:") ||
		strings.HasPrefix(lower, "mailto:") ||
		strings.HasPrefix(lower, "tel:") ||
		strings.HasPrefix(lower, "data:") {
		return false
	}

	return true
}
