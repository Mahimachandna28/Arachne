// Package robots implements a robots.txt parser and rule matcher supporting
// RFC 9309 directives, wildcards (*, $), and Sitemap definitions.
package robots

import (
	"bufio"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Policy represents the parsed robots.txt directives for a domain.
type Policy struct {
	Disallowed []string
	Allowed    []string
	CrawlDelay time.Duration
	Sitemaps   []string
}

// Parse parses the raw text of a robots.txt file and returns a Policy matching
// the given userAgent or the wildcard user-agent (*).
func Parse(content string, userAgent string) *Policy {
	policy := &Policy{}
	targetAgent := strings.ToLower(userAgent)

	scanner := bufio.NewScanner(strings.NewReader(content))
	var currentAgents []string

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		// Strip comments
		if idx := strings.Index(line, "#"); idx >= 0 {
			line = strings.TrimSpace(line[:idx])
		}
		if line == "" {
			currentAgents = nil
			continue
		}

		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		field := strings.ToLower(strings.TrimSpace(parts[0]))
		val := strings.TrimSpace(parts[1])

		switch field {
		case "user-agent":
			agent := strings.ToLower(val)
			currentAgents = append(currentAgents, agent)
		case "disallow":
			if agentMatches(currentAgents, targetAgent) && val != "" {
				policy.Disallowed = append(policy.Disallowed, val)
			}
		case "allow":
			if agentMatches(currentAgents, targetAgent) && val != "" {
				policy.Allowed = append(policy.Allowed, val)
			}
		case "crawl-delay":
			if agentMatches(currentAgents, targetAgent) {
				if secs, err := strconv.ParseFloat(val, 64); err == nil && secs > 0 {
					policy.CrawlDelay = time.Duration(secs * float64(time.Second))
				}
			}
		case "sitemap":
			if val != "" {
				policy.Sitemaps = append(policy.Sitemaps, val)
			}
		}
	}

	return policy
}

func agentMatches(agents []string, target string) bool {
	if len(agents) == 0 {
		return true
	}
	for _, a := range agents {
		if a == "*" || (target != "" && strings.Contains(target, a)) {
			return true
		}
	}
	return false
}

// IsAllowed reports whether rawURLOrPath is permitted to be crawled under this policy.
// Follows RFC 9309 specificity rules and supports wildcards (* and $).
func (p *Policy) IsAllowed(rawURLOrPath string) bool {
	if p == nil {
		return true
	}

	path := rawURLOrPath
	if u, err := url.Parse(rawURLOrPath); err == nil && u.Path != "" {
		path = u.Path
	}
	if path == "" {
		path = "/"
	}

	disallowed := false
	maxDisallowLen := 0
	for _, d := range p.Disallowed {
		if ok, length := matchPattern(d, path); ok && length > maxDisallowLen {
			disallowed = true
			maxDisallowLen = length
		}
	}

	if !disallowed {
		return true
	}

	// An explicit Allow directive that is as specific or more specific overrides Disallow
	for _, a := range p.Allowed {
		if ok, length := matchPattern(a, path); ok && length >= maxDisallowLen {
			return true
		}
	}

	return false
}

// matchPattern matches a robots.txt rule pattern against a URL path.
// Supports '*' (0 or more arbitrary characters) and '$' (end-of-path anchor).
func matchPattern(pattern, path string) (bool, int) {
	if pattern == "" {
		return false, 0
	}

	// Fast path: standard prefix match without wildcards or end-of-path anchor
	if !strings.Contains(pattern, "*") && !strings.HasSuffix(pattern, "$") {
		if strings.HasPrefix(path, pattern) {
			return true, len(pattern)
		}
		return false, 0
	}

	// Build regular expression for pattern with wildcards
	anchorEnd := strings.HasSuffix(pattern, "$")
	cleanPat := pattern
	if anchorEnd {
		cleanPat = cleanPat[:len(cleanPat)-1]
	}

	parts := strings.Split(cleanPat, "*")
	var regexParts []string
	for _, p := range parts {
		regexParts = append(regexParts, regexp.QuoteMeta(p))
	}

	regexStr := "^" + strings.Join(regexParts, ".*")
	if anchorEnd {
		regexStr += "$"
	}

	re, err := regexp.Compile(regexStr)
	if err != nil {
		return false, 0
	}

	if re.MatchString(path) {
		return true, len(pattern)
	}
	return false, 0
}
