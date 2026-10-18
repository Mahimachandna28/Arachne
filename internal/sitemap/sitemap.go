// Package sitemap provides XML Sitemap and Sitemap Index parsing capabilities.
package sitemap

import (
	"bytes"
	"encoding/xml"
	"io"
	"strings"
)

// URLSet represents a standard <urlset> sitemap containing document URLs.
type URLSet struct {
	XMLName xml.Name `xml:"urlset"`
	URLs    []struct {
		Loc string `xml:"loc"`
	} `xml:"url"`
}

// SitemapIndex represents a <sitemapindex> containing child sitemap files.
type SitemapIndex struct {
	XMLName  xml.Name `xml:"sitemapindex"`
	Sitemaps []struct {
		Loc string `xml:"loc"`
	} `xml:"sitemap"`
}

// Result holds the parsed URLs extracted from an XML sitemap or sitemap index.
type Result struct {
	PageURLs    []string // Leaf document URLs found in <urlset>
	SitemapURLs []string // Child sitemap URLs found in <sitemapindex>
}

// Parse parses raw XML bytes and extracts page URLs or child sitemap URLs.
func Parse(r io.Reader) (*Result, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	res := &Result{}

	// First try parsing as a Sitemap Index
	var idx SitemapIndex
	if err := xml.Unmarshal(data, &idx); err == nil && len(idx.Sitemaps) > 0 {
		for _, sm := range idx.Sitemaps {
			loc := strings.TrimSpace(sm.Loc)
			if loc != "" {
				res.SitemapURLs = append(res.SitemapURLs, loc)
			}
		}
		return res, nil
	}

	// Next try parsing as a standard URLSet
	var urlSet URLSet
	if err := xml.Unmarshal(data, &urlSet); err == nil && len(urlSet.URLs) > 0 {
		for _, u := range urlSet.URLs {
			loc := strings.TrimSpace(u.Loc)
			if loc != "" {
				res.PageURLs = append(res.PageURLs, loc)
			}
		}
		return res, nil
	}

	// Fallback: If root tag has custom namespace prefixes, try token decoding
	decoder := xml.NewDecoder(bytes.NewReader(data))
	var currentTag string
	for {
		t, err := decoder.Token()
		if err != nil {
			break
		}
		switch se := t.(type) {
		case xml.StartElement:
			currentTag = strings.ToLower(se.Name.Local)
		case xml.CharData:
			if currentTag == "loc" {
				val := strings.TrimSpace(string(se))
				if val != "" {
					if strings.HasSuffix(val, ".xml") || strings.HasSuffix(val, ".xml.gz") {
						res.SitemapURLs = append(res.SitemapURLs, val)
					} else {
						res.PageURLs = append(res.PageURLs, val)
					}
				}
			}
		case xml.EndElement:
			currentTag = ""
		}
	}

	return res, nil
}
