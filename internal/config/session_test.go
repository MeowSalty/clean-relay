package config_test

import (
	"strings"
	"testing"
	"time"

	"cleanrelay/internal/config"
)

func expectedSessionCache() config.SessionCacheConfig {
	return config.SessionCacheConfig{IdleTTL: 2 * time.Hour, MaxSessions: 256, MaxItemsPerSession: 4096, MaxBytes: 64 << 20}
}

func TestSessionConfiguration(t *testing.T) {
	raw := withField("azure_resource_retry", `{"enabled":false,"history_scope":"session","session_header":"X-CleanRelay-Session","session_diagnostics":true,"session_cache":{"idle_ttl":"3m","max_sessions":2,"max_items_per_session":17,"max_bytes":8192}}`)
	cfg, err := config.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	retry := cfg.AzureResourceRetry
	if retry.HistoryScope != "session" || retry.SessionHeader != "X-CleanRelay-Session" || !retry.SessionDiagnostics ||
		retry.SessionCache != (config.SessionCacheConfig{IdleTTL: 3 * time.Minute, MaxSessions: 2, MaxItemsPerSession: 17, MaxBytes: 8192}) {
		t.Fatalf("session configuration = %+v", retry)
	}
	cfg, err = config.Parse(withField("azure_resource_retry", `{"session_diagnostics":true,"session_cache":{}}`))
	if err != nil || cfg.AzureResourceRetry.Enabled || cfg.AzureResourceRetry.SessionCache != expectedSessionCache() {
		t.Fatalf("diagnostic-only defaults = %+v, %v", cfg.AzureResourceRetry, err)
	}
}

func TestInvalidSessionConfiguration(t *testing.T) {
	for _, fields := range []string{
		`"history_scope":"session"`,
		`"history_scope":"Session"`,
		`"history_scope":null`,
		`"history_scope":false`,
		`"session_header":""`,
		`"session_header":"bad header"`,
		`"session_header":"Authorization"`,
		`"session_header":"x-api-key"`,
		`"session_header":"X-Codex-Installation-Id"`,
		`"session_header":"X-Request-Id"`,
		`"session_header":"User-Agent"`,
		`"session_header":"x-codex-turn-metadata"`,
		`"session_header":null`,
		`"session_diagnostics":"true"`,
		`"session_diagnostics":null`,
		`"session_cache":null`,
		`"session_cache":[]`,
		`"session_cache":{"idle_ttl":"0s"}`,
		`"session_cache":{"idle_ttl":"-1h"}`,
		`"session_cache":{"idle_ttl":5}`,
		`"session_cache":{"idle_ttl":null}`,
		`"session_cache":{"max_sessions":0}`,
		`"session_cache":{"max_items_per_session":-1}`,
		`"session_cache":{"max_bytes":0}`,
		`"session_cache":{"max_bytes":1.5}`,
		`"session_cache":{"max_bytes":999999999999999999999}`,
		`"session_cache":{"MaxBytes":3}`,
		`"session_cache":{"max_bytes":3,"max_bytes":4}`,
		`"history_scope":"request","history_scope":"session"`,
		`"session_header":"x-session","session_header":"y-session"`,
	} {
		t.Run(fields, func(t *testing.T) {
			if _, err := config.Parse(withField("azure_resource_retry", `{"enabled":false,`+fields+`}`)); err == nil {
				t.Fatal("accepted invalid session configuration")
			}
		})
	}
	_, err := config.Parse(withField("azure_resource_retry", `{"session_cache":{"synthetic-secret-key":1}}`))
	if err == nil || strings.Contains(err.Error(), "synthetic-secret-key") {
		t.Fatalf("unsafe diagnostic: %v", err)
	}
}
