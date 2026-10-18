package netguard

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNetGuard_IsSafeIP(t *testing.T) {
	guard := New(Config{AllowPrivate: false})

	tests := []struct {
		ip   string
		safe bool
	}{
		// IPv4 Loopback
		{"127.0.0.1", false},
		{"127.0.0.5", false},

		// IPv4 Private (RFC 1918)
		{"10.0.0.1", false},
		{"10.255.255.254", false},
		{"172.16.0.1", false},
		{"172.31.255.255", false},
		{"192.168.1.1", false},
		{"192.168.0.254", false},

		// Cloud Metadata & Link-Local (RFC 3927)
		{"169.254.169.254", false},
		{"169.254.1.1", false},

		// Reserved / Current Network / Broadcast
		{"0.0.0.0", false},
		{"255.255.255.255", false},

		// IPv6 Loopback & Link-Local & ULA
		{"::1", false},
		{"fe80::1", false},
		{"fc00::1", false},
		{"fd12:3456:789a:1::1", false},

		// Public IPv4 & IPv6
		{"8.8.8.8", true},
		{"1.1.1.1", true},
		{"93.184.216.34", true},
		{"2606:4700:4700::1111", true},
	}

	for _, tt := range tests {
		parsed := net.ParseIP(tt.ip)
		if parsed == nil {
			t.Fatalf("Failed to parse test IP %q", tt.ip)
		}
		got := guard.IsSafeIP(parsed)
		if got != tt.safe {
			t.Errorf("IsSafeIP(%q) = %v, want %v", tt.ip, got, tt.safe)
		}
	}
}

func TestNetGuard_AllowPrivateFlag(t *testing.T) {
	guard := New(Config{AllowPrivate: true})

	if !guard.IsSafeIP(net.ParseIP("127.0.0.1")) {
		t.Error("Expected 127.0.0.1 to be safe when AllowPrivate=true")
	}
	if !guard.IsSafeIP(net.ParseIP("10.0.0.1")) {
		t.Error("Expected 10.0.0.1 to be safe when AllowPrivate=true")
	}
}

func TestNetGuard_DialContext_BlocksPrivateLiteral(t *testing.T) {
	guard := New(Config{AllowPrivate: false})
	ctx := context.Background()

	targets := []string{
		"127.0.0.1:80",
		"169.254.169.254:80",
		"10.0.0.1:443",
		"192.168.1.1:8080",
	}

	for _, target := range targets {
		conn, err := guard.DialContext(ctx, "tcp", target)
		if conn != nil {
			conn.Close()
			t.Errorf("Expected connection to %q to fail, but got connection", target)
		}
		if !errors.Is(err, ErrBlockedPrivateIP) {
			t.Errorf("DialContext(%q) error = %v, want ErrBlockedPrivateIP", target, err)
		}
	}
}

func TestNetGuard_DialContext_BlocksMetadataHostnames(t *testing.T) {
	guard := New(Config{AllowPrivate: false})
	ctx := context.Background()

	metadataHosts := []string{
		"metadata.google.internal:80",
		"metadata:80",
		"instance-data:80",
	}

	for _, host := range metadataHosts {
		conn, err := guard.DialContext(ctx, "tcp", host)
		if conn != nil {
			conn.Close()
			t.Errorf("Expected connection to %q to fail, but got connection", host)
		}
		if !errors.Is(err, ErrBlockedPrivateIP) {
			t.Errorf("DialContext(%q) error = %v, want ErrBlockedPrivateIP", host, err)
		}
	}
}

func TestNetGuard_DialContext_AllowsMockServerWhenAllowed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "OK")
	}))
	defer server.Close()

	guard := New(Config{
		AllowPrivate: true,
		DialTimeout:  2 * time.Second,
	})

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: guard.DialContext,
		},
	}

	resp, err := client.Get(server.URL)
	if err != nil {
		t.Fatalf("Failed to fetch mock server with AllowPrivate=true: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}
}
