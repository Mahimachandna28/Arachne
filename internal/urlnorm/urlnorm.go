// Package urlnorm canonicalizes URLs so that equivalent spellings of the same
// resource map to one string. Every component that hashes or compares URLs
// (visited set, Bloom filter, database key) must go through Normalize,
// otherwise "/about", "/about/" and "/About#team" would be crawled separately.
package urlnorm

import (
	"errors"
	"hash/fnv"
	"net"
	"net/url"
	"sort"
	"strings"
)

// ErrUnsupported is returned for URLs the crawler cannot fetch (mailto:, ftp:, ...).
var ErrUnsupported = errors.New("urlnorm: unsupported or invalid URL")

// trackingParams are query keys that never change page content and only
// fragment the URL space. Prefix matches end with '_'.
var trackingParams = []string{
	"utm_", "gclid", "fbclid", "msclkid", "dclid", "yclid", "mc_cid", "mc_eid", "_ga", "_hsenc", "_hsmi",
}

// Normalize returns the canonical form of rawURL:
//   - scheme and host lower-cased, default ports (:80, :443) removed
//   - fragment removed
//   - empty path becomes "/", trailing slash removed from non-root paths
//   - dot segments resolved ("/a/./b/../c" -> "/a/c")
//   - query keys sorted, tracking parameters (utm_*, gclid, ...) dropped
//
// Only http and https URLs are accepted.
func Normalize(rawURL string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return "", ErrUnsupported
	}
	return NormalizeURL(u)
}

// NormalizeURL is Normalize for an already-parsed URL. u is not modified.
func NormalizeURL(in *url.URL) (string, error) {
	u := *in
	u.Scheme = strings.ToLower(u.Scheme)
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", ErrUnsupported
	}
	if u.Host == "" {
		return "", ErrUnsupported
	}
	u.User = nil

	host := strings.ToLower(u.Hostname())
	host = strings.TrimSuffix(host, ".")
	port := u.Port()
	if (u.Scheme == "http" && port == "80") || (u.Scheme == "https" && port == "443") {
		port = ""
	}
	if port != "" {
		u.Host = net.JoinHostPort(host, port)
	} else if strings.Contains(host, ":") { // bare IPv6 literal
		u.Host = "[" + host + "]"
	} else {
		u.Host = host
	}

	u.Fragment = ""
	u.RawFragment = ""

	u.Path = cleanPath(u.Path)
	u.RawPath = ""

	if u.RawQuery != "" {
		q := u.Query()
		for k := range q {
			if isTracking(k) {
				delete(q, k)
			}
		}
		u.RawQuery = encodeSorted(q)
	}
	u.ForceQuery = false

	return u.String(), nil
}

// Hash64 returns a 64-bit FNV-1a fingerprint of an already-normalized URL.
// Storing 8-byte fingerprints instead of full strings cuts visited-set memory
// by roughly an order of magnitude; at 100M URLs the collision probability is
// still only about 3e-4.
func Hash64(normalized string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(normalized))
	return h.Sum64()
}

// Host returns the lower-cased host[:port] of a URL, or "" if it cannot be parsed.
func Host(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Host)
}

func isTracking(key string) bool {
	k := strings.ToLower(key)
	for _, p := range trackingParams {
		if strings.HasSuffix(p, "_") {
			if strings.HasPrefix(k, p) {
				return true
			}
		} else if k == p {
			return true
		}
	}
	return false
}

func cleanPath(p string) string {
	if p == "" {
		return "/"
	}
	segs := strings.Split(p, "/")
	out := make([]string, 0, len(segs))
	for i, s := range segs {
		switch s {
		case ".":
			continue
		case "..":
			if len(out) > 1 {
				out = out[:len(out)-1]
			}
			continue
		case "":
			// keep the leading empty segment (absolute path) only
			if i != 0 {
				continue
			}
		}
		out = append(out, s)
	}
	cleaned := strings.Join(out, "/")
	if cleaned == "" {
		return "/"
	}
	if !strings.HasPrefix(cleaned, "/") {
		cleaned = "/" + cleaned
	}
	return cleaned
}

func encodeSorted(v url.Values) string {
	if len(v) == 0 {
		return ""
	}
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		vals := append([]string(nil), v[k]...)
		sort.Strings(vals)
		for _, val := range vals {
			if b.Len() > 0 {
				b.WriteByte('&')
			}
			b.WriteString(url.QueryEscape(k))
			b.WriteByte('=')
			b.WriteString(url.QueryEscape(val))
		}
	}
	return b.String()
}
