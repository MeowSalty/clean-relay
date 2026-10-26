package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// AzureResourceRetryConfig controls bounded, opt-in history repair attempts.
// Its limits bound response inspection, not the lifetime of a response stream.
type AzureResourceRetryConfig struct {
	Enabled            bool
	CleanupMode        string
	InspectSSE         bool
	MaxInspectBytes    int64
	InspectTimeout     time.Duration
	HistoryScope       string
	SessionHeader      string
	SessionDiagnostics bool
	SessionCache       SessionCacheConfig
}

func setRetryConfig(cfg *AzureResourceRetryConfig, raw json.RawMessage) error {
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
		case "enabled", "cleanup_mode", "inspect_sse", "max_inspect_bytes", "inspect_timeout",
			"history_scope", "session_header", "session_diagnostics", "session_cache":
		default:
			return fmt.Errorf("unknown configuration key; field names are case-sensitive")
		}
		if seen[key] {
			return fmt.Errorf("duplicate configuration key %q", key)
		}
		seen[key] = true
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return fmt.Errorf("%s: invalid JSON value", key)
		}
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return fmt.Errorf("%s: null is not allowed", key)
		}
		if err := setRetryField(cfg, key, value); err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
	}
	if token, err := dec.Token(); err != nil || token != json.Delim('}') {
		return fmt.Errorf("incomplete JSON object")
	}
	if _, err := dec.Token(); err != io.EOF {
		return fmt.Errorf("must contain exactly one JSON object")
	}
	if cfg.HistoryScope == "session" && cfg.SessionHeader == "" {
		return fmt.Errorf("session_header: required when history_scope is session")
	}
	return nil
}

func setRetryField(cfg *AzureResourceRetryConfig, key string, raw json.RawMessage) error {
	switch key {
	case "enabled", "inspect_sse", "session_diagnostics":
		var value bool
		if err := json.Unmarshal(raw, &value); err != nil {
			return fmt.Errorf("must be a boolean")
		}
		if key == "enabled" {
			cfg.Enabled = value
		} else if key == "inspect_sse" {
			cfg.InspectSSE = value
		} else {
			cfg.SessionDiagnostics = value
		}
	case "history_scope":
		var value string
		if err := decodeString(raw, &value); err != nil {
			return err
		}
		if value != "request" && value != "session" {
			return fmt.Errorf("must be request or session")
		}
		cfg.HistoryScope = value
	case "session_header":
		var value string
		if err := decodeString(raw, &value); err != nil {
			return err
		}
		if !validSessionHeader(value) {
			return fmt.Errorf("must be an HTTP header name for a stable session identity, not a credential, request or installation header")
		}
		cfg.SessionHeader = value
	case "session_cache":
		return setSessionCache(&cfg.SessionCache, raw)
	case "cleanup_mode":
		var value string
		if err := decodeString(raw, &value); err != nil {
			return err
		}
		if value != "portable_ids" && value != "portable_ids_and_reasoning" && value != "portable_ids_then_reasoning" {
			return fmt.Errorf("must be portable_ids, portable_ids_and_reasoning or portable_ids_then_reasoning")
		}
		cfg.CleanupMode = value
	case "max_inspect_bytes":
		var value int64
		if err := json.Unmarshal(raw, &value); err != nil || value <= 0 || value > 1<<20 {
			return fmt.Errorf("must be an integer between 1 and 1048576")
		}
		cfg.MaxInspectBytes = value
	case "inspect_timeout":
		var value string
		if err := decodeString(raw, &value); err != nil {
			return err
		}
		duration, err := time.ParseDuration(value)
		if err != nil || duration <= 0 || duration > 10*time.Second {
			return fmt.Errorf("must be a positive Go duration string no greater than 10s")
		}
		cfg.InspectTimeout = duration
	}
	return nil
}
