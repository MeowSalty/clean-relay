// Package config loads and validates the startup configuration for CleanRelay.
package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/netip"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Config is validated by Parse or Load before it is used to start the relay.
type Config struct {
	Listen                string
	UpstreamOrigin        *url.URL
	DropInputFields       []string
	MaxBodyBytes          int64
	MaxInFlight           int
	ResponseHeaderTimeout time.Duration
	LogLevel              slog.Level
	AzureResourceRetry    AzureResourceRetryConfig
}

// ValidationError describes a rejected configuration without echoing supplied
// values. Load may wrap it with a filename; the CLI can display this inner error.
type ValidationError struct{ cause error }

func (e *ValidationError) Error() string { return e.cause.Error() }
func (e *ValidationError) Unwrap() error { return e.cause }

// Load reads a UTF-8 JSON configuration file and applies the same checks as Parse.
func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read configuration %q: %w", path, err)
	}
	cfg, err := Parse(data)
	if err != nil {
		return Config{}, fmt.Errorf("configuration %q: %w", path, err)
	}
	return cfg, nil
}

// Parse requires exactly one JSON object with case-sensitive configuration keys.
// Omitted optional fields receive defaults; explicitly supplied null is invalid.
func Parse(data []byte) (_ Config, err error) {
	defer func() {
		if err != nil {
			err = &ValidationError{cause: err}
		}
	}()
	if !utf8.Valid(data) {
		return Config{}, fmt.Errorf("configuration must contain valid UTF-8")
	}
	cfg := Config{
		Listen:                "127.0.0.1:17880",
		DropInputFields:       []string{"internal_chat_message_metadata_passthrough"},
		MaxBodyBytes:          32 << 20,
		MaxInFlight:           8,
		ResponseHeaderTimeout: 5 * time.Minute,
		LogLevel:              slog.LevelInfo,
		AzureResourceRetry: AzureResourceRetryConfig{
			CleanupMode: "portable_ids", InspectSSE: true,
			MaxInspectBytes: 65536, InspectTimeout: 2 * time.Second,
			HistoryScope: "request",
			SessionCache: defaultSessionCache(),
		},
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	token, err := dec.Token()
	if err != nil || token != json.Delim('{') {
		return Config{}, fmt.Errorf("configuration must be a JSON object")
	}
	seen := make(map[string]bool)
	for dec.More() {
		token, err := dec.Token()
		if err != nil {
			return Config{}, fmt.Errorf("configuration contains an invalid JSON object key")
		}
		key, ok := token.(string)
		if !ok {
			return Config{}, fmt.Errorf("configuration keys must be strings")
		}
		if seen[key] {
			return Config{}, fmt.Errorf("duplicate configuration key %q", key)
		}
		seen[key] = true
		switch key {
		case "listen", "upstream_origin", "drop_input_fields", "max_body_bytes", "max_in_flight", "response_header_timeout", "log_level", "azure_resource_retry":
		default:
			// An unknown key can itself contain a credential or body text.
			return Config{}, fmt.Errorf("unknown configuration key; field names are case-sensitive")
		}
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return Config{}, fmt.Errorf("%s: invalid JSON value", key)
		}
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return Config{}, fmt.Errorf("%s: null is not allowed", key)
		}
		if err := setField(&cfg, key, raw); err != nil {
			return Config{}, fmt.Errorf("%s: %w", key, err)
		}
	}
	if token, err := dec.Token(); err != nil || token != json.Delim('}') {
		return Config{}, fmt.Errorf("configuration contains an incomplete JSON object")
	}
	if _, err := dec.Token(); err != io.EOF {
		return Config{}, fmt.Errorf("configuration must contain exactly one JSON value; trailing data is not allowed")
	}
	if !seen["upstream_origin"] {
		return Config{}, fmt.Errorf("upstream_origin: required configuration key is missing")
	}
	listenIP, listenPort, err := parseListen(cfg.Listen)
	if err != nil {
		return Config{}, fmt.Errorf("listen: %w", err)
	}
	if pointsToListener(cfg.UpstreamOrigin, listenIP, listenPort) {
		return Config{}, fmt.Errorf("upstream_origin: would forward requests back to the configured listener")
	}
	return cfg, nil
}

func setField(cfg *Config, key string, raw json.RawMessage) error {
	switch key {
	case "azure_resource_retry":
		return setRetryConfig(&cfg.AzureResourceRetry, raw)
	case "listen":
		return decodeString(raw, &cfg.Listen)
	case "upstream_origin":
		var origin string
		if err := decodeString(raw, &origin); err != nil {
			return err
		}
		parsed, err := parseOrigin(origin)
		if err != nil {
			return err
		}
		cfg.UpstreamOrigin = parsed
	case "drop_input_fields":
		var elements []json.RawMessage
		if err := json.Unmarshal(raw, &elements); err != nil {
			return fmt.Errorf("must be an array of nonempty, unique strings")
		}
		fields := make([]string, len(elements))
		seen := make(map[string]bool, len(elements))
		for i, element := range elements {
			if bytes.Equal(bytes.TrimSpace(element), []byte("null")) {
				return fmt.Errorf("element %d: null is not allowed", i)
			}
			if err := decodeString(element, &fields[i]); err != nil {
				return fmt.Errorf("element %d: %w", i, err)
			}
			if fields[i] == "" {
				return fmt.Errorf("element %d: field name must not be empty", i)
			}
			if seen[fields[i]] {
				return fmt.Errorf("element %d: duplicate field name", i)
			}
			seen[fields[i]] = true
		}
		cfg.DropInputFields = fields
	case "max_body_bytes":
		if err := json.Unmarshal(raw, &cfg.MaxBodyBytes); err != nil || cfg.MaxBodyBytes <= 0 || cfg.MaxBodyBytes == math.MaxInt64 {
			return fmt.Errorf("must be a positive integer no greater than %d (leaving room for the limit check)", int64(math.MaxInt64-1))
		}
	case "max_in_flight":
		if err := json.Unmarshal(raw, &cfg.MaxInFlight); err != nil || cfg.MaxInFlight <= 0 {
			return fmt.Errorf("must be a positive integer that fits in an int")
		}
	case "response_header_timeout":
		var value string
		if err := decodeString(raw, &value); err != nil {
			return err
		}
		duration, err := time.ParseDuration(value)
		if err != nil || duration < 0 {
			return fmt.Errorf("must be a nonnegative Go duration string within the supported range, such as 5m or 0s")
		}
		cfg.ResponseHeaderTimeout = duration
	case "log_level":
		var value string
		if err := decodeString(raw, &value); err != nil {
			return err
		}
		switch value {
		case "debug":
			cfg.LogLevel = slog.LevelDebug
		case "info":
			cfg.LogLevel = slog.LevelInfo
		case "warn":
			cfg.LogLevel = slog.LevelWarn
		case "error":
			cfg.LogLevel = slog.LevelError
		default:
			return fmt.Errorf("must be one of debug, info, warn, error")
		}
	default:
		return fmt.Errorf("unknown configuration key")
	}
	return nil
}

func decodeString(raw json.RawMessage, value *string) error {
	if err := json.Unmarshal(raw, value); err != nil {
		return fmt.Errorf("must be a string")
	}
	return nil
}

func parseListen(value string) (netip.Addr, uint16, error) {
	host, portText, err := net.SplitHostPort(value)
	if err != nil {
		return netip.Addr{}, 0, fmt.Errorf("must be a loopback IP literal and a port, such as 127.0.0.1:17880 or [::1]:17880")
	}
	ip, err := netip.ParseAddr(host)
	if err != nil || ip.Zone() != "" || !ip.IsLoopback() {
		return netip.Addr{}, 0, fmt.Errorf("host must be a loopback IP literal")
	}
	port, err := parsePort(portText)
	if err != nil {
		return netip.Addr{}, 0, err
	}
	return ip.Unmap(), port, nil
}

func parsePort(value string) (uint16, error) {
	if value == "" {
		return 0, fmt.Errorf("port must be a decimal integer between 1 and 65535")
	}
	for _, c := range value {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("port must be a decimal integer between 1 and 65535")
		}
	}
	port, err := strconv.ParseUint(value, 10, 16)
	if err != nil || port == 0 {
		return 0, fmt.Errorf("port must be a decimal integer between 1 and 65535")
	}
	return uint16(port), nil
}

func parseOrigin(value string) (*url.URL, error) {
	// url.Parse errors include their input, which may contain credentials.
	// Report only validation context, never the original URL or parse error.
	origin, err := url.Parse(value)
	if err != nil {
		return nil, fmt.Errorf("must be a valid HTTP(S) origin URL")
	}
	if (origin.Scheme != "http" && origin.Scheme != "https") || origin.Host == "" || origin.Opaque != "" {
		return nil, fmt.Errorf("must use http or https with a nonempty host")
	}
	if origin.User != nil {
		return nil, fmt.Errorf("usernames and passwords are not allowed")
	}
	if origin.ForceQuery || origin.RawQuery != "" || strings.Contains(value, "#") {
		return nil, fmt.Errorf("query strings and fragments are not allowed")
	}
	if (origin.Path != "" && origin.Path != "/") || origin.RawPath != "" {
		return nil, fmt.Errorf("path must be empty or a single trailing slash")
	}
	host := origin.Host
	if strings.HasPrefix(host, "[") {
		end := strings.IndexByte(host, ']')
		if end < 0 {
			return nil, fmt.Errorf("invalid bracketed IP host")
		}
		ip, err := netip.ParseAddr(host[1:end])
		if err != nil || !ip.Is6() || ip.Zone() != "" {
			return nil, fmt.Errorf("bracketed host must be an IPv6 literal without a zone")
		}
		suffix := host[end+1:]
		if suffix != "" {
			if !strings.HasPrefix(suffix, ":") {
				return nil, fmt.Errorf("invalid text after IPv6 host")
			}
			if _, err := parsePort(suffix[1:]); err != nil {
				return nil, err
			}
		}
	} else {
		name, portText, hasPort := strings.Cut(host, ":")
		if hasPort {
			if _, err := parsePort(portText); err != nil {
				return nil, err
			}
		}
		if ip, err := netip.ParseAddr(name); err != nil || !ip.Is4() {
			if !validHostname(name) {
				return nil, fmt.Errorf("host must be a valid IP literal or DNS hostname")
			}
		}
	}
	origin.Path = ""
	origin.RawPath = ""
	return origin, nil
}

func validHostname(host string) bool {
	host = strings.TrimSuffix(host, ".")
	if len(host) == 0 || len(host) > 253 {
		return false
	}
	allNumeric := true
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, c := range label {
			if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '-' {
				allNumeric = false
			} else if c < '0' || c > '9' {
				return false
			}
		}
	}
	// Reject malformed or abbreviated numeric IP addresses as hostnames.
	return !allNumeric
}

func pointsToListener(origin *url.URL, listenIP netip.Addr, listenPort uint16) bool {
	port := uint16(80)
	if origin.Scheme == "https" {
		port = 443
	}
	if origin.Port() != "" {
		// parseOrigin already validated the port.
		parsed, err := parsePort(origin.Port())
		if err != nil {
			return false
		}
		port = parsed
	}
	if port != listenPort {
		return false
	}
	host := origin.Hostname()
	if ip, err := netip.ParseAddr(host); err == nil {
		return ip.Unmap() == listenIP
	}
	if strings.EqualFold(strings.TrimSuffix(host, "."), "localhost") {
		return listenIP == netip.MustParseAddr("127.0.0.1") || listenIP == netip.IPv6Loopback()
	}
	return false
}
