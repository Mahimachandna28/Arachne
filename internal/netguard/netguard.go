// Package netguard provides network security controls, including SSRF
// (Server-Side Request Forgery) protection, private IP filtering, and DNS-pinning dialers.
package netguard

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"
)

var (
	// ErrBlockedPrivateIP is returned when a connection target resolves to a private, loopback, or cloud metadata IP.
	ErrBlockedPrivateIP = errors.New("connection blocked by SSRF policy: target resolves to private or reserved IP")

	// ErrNoSafeIPFound is returned when all resolved IP addresses for a host are prohibited.
	ErrNoSafeIPFound = errors.New("connection blocked by SSRF policy: no safe public IP addresses found")
)

// Config holds configuration options for the network security guard.
type Config struct {
	AllowPrivate bool          // When true, allows connections to private/loopback IP addresses (for local testing)
	DialTimeout  time.Duration // Network dial timeout (defaults to 10s)
	KeepAlive    time.Duration // TCP keep-alive interval (defaults to 30s)
}

// Guard inspects network dial targets and blocks connections to private/internal IPs.
type Guard struct {
	config      Config
	blockedNets []*net.IPNet
	dialer      *net.Dialer
	resolver    *net.Resolver
}

// New creates a new Guard with standard SSRF protection rules.
func New(cfg Config) *Guard {
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 10 * time.Second
	}
	if cfg.KeepAlive <= 0 {
		cfg.KeepAlive = 30 * time.Second
	}

	g := &Guard{
		config: cfg,
		dialer: &net.Dialer{
			Timeout:   cfg.DialTimeout,
			KeepAlive: cfg.KeepAlive,
		},
		resolver: net.DefaultResolver,
	}

	g.initBlockedRanges()
	return g
}

// initBlockedRanges initializes the list of reserved, private, and sensitive IP subnets.
func (g *Guard) initBlockedRanges() {
	cidrs := []string{
		// IPv4 Private & Reserved (RFC 1918, RFC 5735, RFC 3927)
		"0.0.0.0/8",          // Current network ("this" host)
		"10.0.0.0/8",          // Private network (RFC 1918)
		"100.64.0.0/10",       // Carrier-grade NAT (RFC 6598)
		"127.0.0.0/8",         // Loopback
		"169.254.0.0/16",      // Link-local & Cloud Metadata (169.254.169.254)
		"172.16.0.0/12",       // Private network (RFC 1918)
		"192.0.0.0/24",        // IETF Protocol Assignments
		"192.0.2.0/24",        // TEST-NET-1
		"192.88.99.0/24",      // 6to4 Relay Anycast
		"192.168.0.0/16",      // Private network (RFC 1918)
		"198.18.0.0/15",       // Network benchmark tests
		"198.51.100.0/24",     // TEST-NET-2
		"203.0.113.0/24",      // TEST-NET-3
		"224.0.0.0/4",         // Multicast
		"240.0.0.0/4",         // Reserved for future use
		"255.255.255.255/32",  // Broadcast

		// IPv6 Private & Reserved (RFC 4193, RFC 4291, RFC 5156)
		"::/128",              // Unspecified
		"::1/128",             // Loopback
		"100::/64",            // Discard-only
		"2001:db8::/32",       // Documentation
		"fc00::/7",            // Unique Local Address (ULA)
		"fe80::/10",           // Link-local Unicast
		"ff00::/8",            // Multicast
	}

	for _, cidr := range cidrs {
		_, ipNet, err := net.ParseCIDR(cidr)
		if err == nil {
			g.blockedNets = append(g.blockedNets, ipNet)
		}
	}
}

// IsSafeIP checks if an IP is a public, safe-to-crawl address.
func (g *Guard) IsSafeIP(ip net.IP) bool {
	if g.config.AllowPrivate {
		return true
	}

	// Unwrap IPv4-mapped IPv6 addresses (::ffff:192.168.1.1)
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}

	// Check standard library classifications
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsInterfaceLocalMulticast() ||
		ip.IsMulticast() || ip.IsUnspecified() {
		return false
	}

	// Check against explicitly blocked subnets (including cloud metadata 169.254.169.254)
	for _, blockedNet := range g.blockedNets {
		if blockedNet.Contains(ip) {
			return false
		}
	}

	return true
}

// DialContext performs DNS resolution, validates that the resolved IP is safe according
// to SSRF rules, and connects directly to the validated IP to prevent DNS rebinding attacks.
func (g *Guard) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("invalid address %q: %w", addr, err)
	}

	// Block well-known cloud metadata hostnames directly
	lowerHost := strings.ToLower(host)
	if !g.config.AllowPrivate {
		if lowerHost == "metadata.google.internal" ||
			lowerHost == "instance-data" ||
			lowerHost == "metadata" {
			return nil, fmt.Errorf("%w: blocked cloud metadata hostname %q", ErrBlockedPrivateIP, host)
		}
	}

	// If host is a literal IP address
	if literalIP := net.ParseIP(host); literalIP != nil {
		if !g.IsSafeIP(literalIP) {
			return nil, fmt.Errorf("%w: literal IP %s", ErrBlockedPrivateIP, literalIP)
		}
		return g.dialer.DialContext(ctx, network, addr)
	}

	// Resolve domain host
	ips, err := g.resolver.LookupIP(ctx, "ip", host)
	if err != nil {
		return nil, err
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("no IP addresses resolved for host %q", host)
	}

	// Find the first safe IP
	var safeIP net.IP
	for _, ip := range ips {
		if g.IsSafeIP(ip) {
			safeIP = ip
			break
		}
	}

	if safeIP == nil {
		return nil, fmt.Errorf("%w for host %q: resolved IPs %v", ErrNoSafeIPFound, host, ips)
	}

	// DNS Pinning: Dial the validated IP directly to prevent DNS rebinding
	pinnedAddr := net.JoinHostPort(safeIP.String(), port)
	return g.dialer.DialContext(ctx, network, pinnedAddr)
}
