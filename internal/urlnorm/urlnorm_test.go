package urlnorm

import "testing"

func TestNormalize(t *testing.T) {
	cases := []struct{ in, want string }{
		{"http://example.com", "http://example.com/"},
		{"HTTP://Example.COM/About/", "http://example.com/About"},
		{"http://example.com:80/a", "http://example.com/a"},
		{"https://example.com:443/a", "https://example.com/a"},
		{"https://example.com:8443/a", "https://example.com:8443/a"},
		{"http://example.com/a/./b/../c", "http://example.com/a/c"},
		{"http://example.com//a///b/", "http://example.com/a/b"},
		{"http://example.com/page#frag", "http://example.com/page"},
		{"http://example.com/s?z=3&a=1&m=2", "http://example.com/s?a=1&m=2&z=3"},
		{"http://example.com/s?b=2&b=1", "http://example.com/s?b=1&b=2"},
		{"http://example.com/s?utm_source=x&id=7&gclid=abc&fbclid=q", "http://example.com/s?id=7"},
		{"http://example.com/s?utm_source=x", "http://example.com/s"},
		{"http://user:pw@example.com/", "http://example.com/"},
		{"http://example.com./", "http://example.com/"},
		{"http://[::1]:80/x", "http://[::1]/x"},
		{"http://127.0.0.1:8080/x", "http://127.0.0.1:8080/x"},
	}
	for _, c := range cases {
		got, err := Normalize(c.in)
		if err != nil {
			t.Errorf("Normalize(%q) error: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("Normalize(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestNormalizeRejects(t *testing.T) {
	for _, in := range []string{"mailto:a@b.c", "ftp://x.com/f", "javascript:void(0)", "/relative", "http://", "::bad"} {
		if got, err := Normalize(in); err == nil {
			t.Errorf("Normalize(%q) = %q, want error", in, got)
		}
	}
}

func TestNormalizeIdempotent(t *testing.T) {
	for _, in := range []string{"HTTP://A.com:80/x/../y/?b=1&a=2#f", "https://b.org/p?utm_x=1&q=%20z"} {
		once, _ := Normalize(in)
		twice, _ := Normalize(once)
		if once != twice {
			t.Errorf("not idempotent: %q -> %q -> %q", in, once, twice)
		}
	}
}

func TestHash64Stable(t *testing.T) {
	a, _ := Normalize("http://example.com/about/")
	b, _ := Normalize("http://EXAMPLE.com/about#x")
	if Hash64(a) != Hash64(b) {
		t.Fatal("equivalent URLs must share a fingerprint")
	}
	if Hash64(a) == Hash64("http://example.com/contact") {
		t.Fatal("distinct URLs should not collide")
	}
}

func BenchmarkNormalize(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = Normalize("https://Example.com:443/a/./b/../c/?utm_source=x&z=1&a=2#frag")
	}
}
