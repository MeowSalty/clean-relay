package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

// SessionCacheConfig bounds retained recovery state across all namespaces.
// Items includes both selectors and their order anchors, across generations.
type SessionCacheConfig struct {
	IdleTTL            time.Duration
	MaxSessions        int
	MaxItemsPerSession int
	MaxBytes           int64
}

func defaultSessionCache() SessionCacheConfig {
	return SessionCacheConfig{
		IdleTTL: 2 * time.Hour, MaxSessions: 256,
		MaxItemsPerSession: 4096, MaxBytes: 64 << 20,
	}
}

func validSessionHeader(name string) bool {
	if len(name) == 0 || len(name) > 128 {
		return false
	}
	for _, c := range name {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' ||
			c >= '0' && c <= '9' || strings.ContainsRune("!#$%&'*+-.^_`|~", c)) {
			return false
		}
	}
	switch strings.ToLower(name) {
	case "authorization", "proxy-authorization", "cookie", "set-cookie", "api-key", "x-api-key",
		"user-agent", "request-id", "x-request-id", "x-client-request-id", "traceparent", "tracestate",
		"idempotency-key", "x-idempotency-key", "last-event-id",
		"x-codex-installation-id", "x-codex-turn-metadata", "x-codex-window-id",
		"openai-organization", "openai-project":
		return false
	}
	return true
}

func setSessionCache(cfg *SessionCacheConfig, raw json.RawMessage) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	if token, err := dec.Token(); err != nil || token != json.Delim('{') {
		return fmt.Errorf("must be a JSON object")
	}
	seen := make(map[string]bool)
	for dec.More() {
		token, err := dec.Token()
		if err != nil {
			return fmt.Errorf("invalid JSON object key")
		}
		key, ok := token.(string)
		if !ok {
			return fmt.Errorf("keys must be strings")
		}
		switch key {
		case "idle_ttl", "max_sessions", "max_items_per_session", "max_bytes":
		default:
			return fmt.Errorf("unknown configuration key; field names are case-sensitive")
		}
		if seen[key] {
			return fmt.Errorf("duplicate configuration key %q", key)
		}
		seen[key] = true
		var value json.RawMessage
		if dec.Decode(&value) != nil {
			return fmt.Errorf("%s: invalid JSON value", key)
		}
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return fmt.Errorf("%s: null is not allowed", key)
		}
		switch key {
		case "idle_ttl":
			var text string
			if err := decodeString(value, &text); err != nil {
				return fmt.Errorf("%s: %w", key, err)
			}
			duration, err := time.ParseDuration(text)
			if err != nil || duration <= 0 {
				return fmt.Errorf("idle_ttl: must be a positive Go duration string")
			}
			cfg.IdleTTL = duration
		case "max_bytes":
			var limit int64
			if json.Unmarshal(value, &limit) != nil || limit <= 0 {
				return fmt.Errorf("max_bytes: must be a positive integer that fits in an int64")
			}
			cfg.MaxBytes = limit
		default:
			var limit int
			if json.Unmarshal(value, &limit) != nil || limit <= 0 {
				return fmt.Errorf("%s: must be a positive integer that fits in an int", key)
			}
			if key == "max_sessions" {
				cfg.MaxSessions = limit
			} else {
				cfg.MaxItemsPerSession = limit
			}
		}
	}
	if token, err := dec.Token(); err != nil || token != json.Delim('}') {
		return fmt.Errorf("incomplete JSON object")
	}
	if _, err := dec.Token(); err != io.EOF {
		return fmt.Errorf("must contain exactly one JSON object")
	}
	return nil
}
