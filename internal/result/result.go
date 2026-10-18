// Package result defines the data structures used to represent
// the output of a crawled page.
package result

// Page holds the crawled data extracted from a single URL.
type Page struct {
	URL          string   `json:"url"`                     // The URL that was crawled
	Title        string   `json:"title,omitempty"`         // The <title> tag content of the page
	CanonicalURL string   `json:"canonical_url,omitempty"` // The author-declared canonical link if specified
	Description  string   `json:"description,omitempty"`   // The meta description if found
	Links        []string `json:"links,omitempty"`         // All hyperlinks found on the page
	StatusCode   int      `json:"status_code"`             // HTTP response status code
	Error        string   `json:"error,omitempty"`         // Non-empty if crawling failed
}
