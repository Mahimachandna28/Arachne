package crawler

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"golang.org/x/text/encoding/htmlindex"
	"golang.org/x/text/transform"
)

// 1.7 isSpiderTrap checks for repetitive directory patterns and query parameter bloat.
func isSpiderTrap(u *url.URL) bool {
	// Guard against excessively long URLs
	if len(u.String()) > 2048 {
		return true
	}

	// Guard against query parameter explosions (e.g. tracking parameters, dynamic loops)
	if len(u.Query()) > 10 {
		return true
	}

	// Guard against recursive directory traps (e.g., /dir/dir/dir/...)
	cleanPath := strings.Trim(u.Path, "/")
	if cleanPath != "" {
		segments := strings.Split(cleanPath, "/")
		seen := make(map[string]int)
		for _, s := range segments {
			if s != "" {
				seen[s]++
				if seen[s] >= 3 {
					return true
				}
			}
		}
	}

	return false
}

// parseRetryAfter parses the Retry-After header as either seconds or an RFC 1123 HTTP-date.
func parseRetryAfter(header string) time.Duration {
	header = strings.TrimSpace(header)
	if header == "" {
		return 0
	}

	if secs, err := strconv.Atoi(header); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}

	if t, err := http.ParseTime(header); err == nil {
		diff := time.Until(t)
		if diff > 0 {
			return diff
		}
	}

	return 0
}

// 1.5 decodeHTMLBody reads up to maxBytes and converts character encoding to UTF-8.
func decodeHTMLBody(r io.Reader, contentType string, maxBytes int64) (string, error) {
	bodyBytes, err := io.ReadAll(io.LimitReader(r, maxBytes))
	if err != nil {
		return "", err
	}

	charset := extractCharset(contentType)
	if charset == "" {
		charset = sniffCharsetFromHTML(bodyBytes)
	}

	if charset == "" || strings.EqualFold(charset, "utf-8") || strings.EqualFold(charset, "utf8") {
		return string(bodyBytes), nil
	}

	enc, err := htmlindex.Get(charset)
	if err != nil {
		// Fallback to raw string if charset is unrecognized
		return string(bodyBytes), nil
	}

	decoded, err := io.ReadAll(transform.NewReader(bytes.NewReader(bodyBytes), enc.NewDecoder()))
	if err != nil {
		return string(bodyBytes), nil
	}

	return string(decoded), nil
}

func extractCharset(contentType string) string {
	parts := strings.Split(contentType, ";")
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		lower := strings.ToLower(trimmed)
		if strings.HasPrefix(lower, "charset=") {
			val := strings.TrimPrefix(lower, "charset=")
			return strings.Trim(val, `"' `)
		}
	}
	return ""
}

func sniffCharsetFromHTML(body []byte) string {
	limit := 1024
	if len(body) < limit {
		limit = len(body)
	}
	preview := strings.ToLower(string(body[:limit]))
	if idx := strings.Index(preview, "charset="); idx != -1 {
		rest := preview[idx+8:]
		end := strings.IndexAny(rest, `"' >/`)
		if end != -1 {
			return strings.TrimSpace(rest[:end])
		}
	}
	return ""
}

// readLimited reads at most n bytes from r.
func readLimited(r io.Reader, n int64) (string, error) {
	b, err := io.ReadAll(io.LimitReader(r, n))
	return string(b), err
}
