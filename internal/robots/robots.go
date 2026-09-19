// Package robots implements a robots.txt parser and rule matcher.
package robots

import (
	"bufio"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Policy represents the parsed robots.txt directives for a domain.
type Policy struct {
	Disallowed []string
	Allowed    []string
	CrawlDelay time.Duration
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
// Allowed rules take precedence over Disallowed rules if the matched prefix is equal or longer.
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
		if strings.HasPrefix(path, d) && len(d) > maxDisallowLen {
			disallowed = true
			maxDisallowLen = len(d)
		}
	}

	if !disallowed {
		return true
	}

	// An explicit Allow directive that is as specific or more specific overrides Disallow
	for _, a := range p.Allowed {
		if strings.HasPrefix(path, a) && len(a) >= maxDisallowLen {
			return true
		}
	}

	return false
}
