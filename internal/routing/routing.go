// Package routing handles host → target domain resolution.
package routing

import (
	"fmt"
	"net/url"
	"strings"
)

// Config holds routing configuration.
type Config struct {
	BaseDomain string // e.g. "example.com"
	Prefix     string // e.g. "p2p-"
	Scheme     string // "https" (default) or "http"
}

// Resolver maps a gateway host (e.g. p2p-a.example.com) to the
// upstream target (e.g. https://a.example.com).
type Resolver struct {
	cfg Config
}

// NewResolver creates a new resolver.
func NewResolver(cfg Config) *Resolver {
	return &Resolver{cfg: cfg}
}

// Target returns the upstream URL for the given host.
// It strips the prefix and returns "https://<stripped>".
// If the host does not have the prefix or is the bare base domain,
// it returns an error.
func (r *Resolver) Target(host string) (string, error) {
	host = strings.ToLower(host)

	// Strip port if present.
	if i := strings.IndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}

	if r.cfg.Prefix == "" || r.cfg.BaseDomain == "" {
		return "", fmt.Errorf("routing not configured")
	}

	if !strings.HasSuffix(host, "."+r.cfg.BaseDomain) && host != r.cfg.BaseDomain {
		return "", fmt.Errorf("host %q does not match base domain %q", host, r.cfg.BaseDomain)
	}

	// Must begin with the prefix.
	if !strings.HasPrefix(host, r.cfg.Prefix) {
		return "", fmt.Errorf("host %q missing prefix %q", host, r.cfg.Prefix)
	}

	stripped := strings.TrimPrefix(host, r.cfg.Prefix)
	if stripped == "" || stripped == r.cfg.BaseDomain || strings.HasPrefix(stripped, ".") {
		return "", fmt.Errorf("host %q resolves to base domain or empty", host)
	}

	scheme := r.cfg.Scheme
	if scheme == "" {
		scheme = "https"
	}
	return scheme + "://" + stripped, nil
}

// TargetWithPort returns the upstream URL for the given host, with the
// supplied port appended (e.g. for development when the upstream runs
// on a non-default port).  Use this from tests and fallbacks.
func (r *Resolver) TargetWithPort(host string, port int) (string, error) {
	target, err := r.Target(host)
	if err != nil {
		return "", err
	}
	if port == 0 {
		return target, nil
	}
	// Insert port into URL.
	u, err := url.Parse(target)
	if err != nil {
		return "", err
	}
	u.Host = fmt.Sprintf("%s:%d", u.Hostname(), port)
	return u.String(), nil
}