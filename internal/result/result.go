// Package result defines the data structures used to represent
// the output of a crawled page.
package result

// Page holds the crawled data extracted from a single URL.
type Page struct {
	URL        string   // The URL that was crawled
	Title      string   // The <title> tag content of the page
	Links      []string // All hyperlinks found on the page
	StatusCode int      // HTTP response status code
	Error      string   // Non-empty if crawling failed
}
