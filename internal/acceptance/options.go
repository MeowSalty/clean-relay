// Package acceptance runs synthetic Responses API checks against a local relay.
// Reports contain fixed diagnostic messages, never credentials or response text.
package acceptance

import (
	"errors"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const maxTimeout = 10 * time.Minute

// Options supplies explicit probe destinations and credentials. Timeout applies
// separately to every check and must be positive and no greater than ten minutes.
// DirectBaseURL is optional; its probe is a synthetic direct conversation.
type Options struct {
	BaseURL       string
	DirectBaseURL string
	Model         string
	APIKey        string
	Timeout       time.Duration
}

// Check contains only a stable name, outcome and safe diagnostic information.
type Check struct {
	Name       string `json:"name"`
	Status     string `json:"status"`
	HTTPStatus int    `json:"http_status,omitempty"`
	DurationMS int64  `json:"duration_ms"`
	Detail     string `json:"detail"`
}

// Report deliberately separates automated probes from outstanding manual checks.
// AutomatedPassed does not claim real client compatibility or upstream cleanup.
type Report struct {
	SchemaVersion   int       `json:"schema_version"`
	StartedAt       time.Time `json:"started_at"`
	AutomatedPassed bool      `json:"automated_passed"`
	Checks          []Check   `json:"checks"`
	ManualChecks    []Check   `json:"manual_checks"`
}

// ValidateOptions performs no I/O and returns only fixed, safe error messages.
func ValidateOptions(options Options) error {
	if _, ok := parseBaseURL(options.BaseURL, true); !ok {
		return errors.New("acceptance: BaseURL must be a valid HTTP(S) loopback URL without userinfo, query, or fragment")
	}
	if options.DirectBaseURL != "" {
		if _, ok := parseBaseURL(options.DirectBaseURL, false); !ok {
			return errors.New("acceptance: DirectBaseURL must be a valid HTTP(S) URL without userinfo, query, or fragment")
		}
	}
	if strings.TrimSpace(options.Model) == "" || !utf8.ValidString(options.Model) {
		return errors.New("acceptance: Model must be a nonempty UTF-8 string")
	}
	if strings.TrimSpace(options.APIKey) == "" || !utf8.ValidString(options.APIKey) || strings.ContainsAny(options.APIKey, "\r\n") {
		return errors.New("acceptance: APIKey must be nonempty UTF-8 and contain no CR or LF")
	}
	if options.Timeout <= 0 || options.Timeout > maxTimeout {
		return errors.New("acceptance: Timeout must be positive and no greater than 10 minutes")
	}
	return nil
}

func parseBaseURL(value string, local bool) (*url.URL, bool) {
	u, err := url.Parse(value)
	if err != nil || !utf8.ValidString(value) || u.Host == "" || u.Opaque != "" || u.User != nil ||
		u.ForceQuery || u.RawQuery != "" || strings.Contains(value, "#") {
		return nil, false
	}
	u.Scheme = strings.ToLower(u.Scheme)
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, false
	}
	host, port, ok := splitAuthority(u.Host)
	if !ok {
		return nil, false
	}
	ip, ipErr := netip.ParseAddr(host)
	localhost := strings.EqualFold(strings.TrimSuffix(host, "."), "localhost")
	if local {
		if !localhost && (ipErr != nil || ip.Zone() != "" || !ip.Unmap().IsLoopback()) {
			return nil, false
		}
	} else if ipErr != nil && !validHostname(host) {
		return nil, false
	}
	if localhost {
		// Remove exactly one optional DNS root dot before dialing localhost.
		u.Host = "localhost" + port
	}
	return u, true
}

func splitAuthority(authority string) (host, port string, ok bool) {
	if strings.HasPrefix(authority, "[") {
		end := strings.IndexByte(authority, ']')
		if end < 0 {
			return "", "", false
		}
		host = authority[1:end]
		ip, err := netip.ParseAddr(host)
		if err != nil || !ip.Is6() {
			return "", "", false
		}
		port = authority[end+1:]
		if port == "" {
			return host, "", true
		}
		if !strings.HasPrefix(port, ":") || !validPort(port[1:]) {
			return "", "", false
		}
		return host, port, true
	}
	host, portText, hasPort := strings.Cut(authority, ":")
	if host == "" || strings.ContainsAny(host, "[]") {
		return "", "", false
	}
	if hasPort {
		if !validPort(portText) {
			return "", "", false
		}
		port = ":" + portText
	}
	return host, port, true
}

func validPort(value string) bool {
	if value == "" {
		return false
	}
	for _, c := range value {
		if c < '0' || c > '9' {
			return false
		}
	}
	port, err := strconv.ParseUint(value, 10, 16)
	return err == nil && port != 0
}

func validHostname(host string) bool {
	host = strings.TrimSuffix(host, ".")
	if host == "" || len(host) > 253 {
		return false
	}
	allNumeric := true
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, c := range label {
			switch {
			case c >= '0' && c <= '9':
			case c == '-' || unicode.IsLetter(c) || unicode.IsNumber(c) || unicode.IsMark(c):
				allNumeric = false
			default:
				return false
			}
		}
	}
	// Do not treat abbreviated or malformed numeric IP addresses as DNS names.
	return !allNumeric
}

func responsesURL(base *url.URL) *url.URL {
	u := *base
	// Do not clean decoded paths: %2F, %2e and their original hex case matter.
	u.RawPath = strings.TrimSuffix(base.EscapedPath(), "/") + "/responses"
	u.Path, _ = url.PathUnescape(u.RawPath) // EscapedPath plus a literal suffix is valid.
	return &u
}

func healthURL(base *url.URL) *url.URL {
	u := *base
	u.Path = "/_cleanrelay/healthz"
	u.RawPath = ""
	return &u
}
