package config_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"cleanrelay/internal/config"
)

func TestRetryConfiguration(t *testing.T) {
	defaults := config.AzureResourceRetryConfig{
		CleanupMode: "portable_ids", InspectSSE: true, MaxInspectBytes: 65536, InspectTimeout: 2 * time.Second,
	}
	for _, raw := range []string{`{}`, `{"enabled":false}`} {
		cfg, err := config.Parse(withField("azure_resource_retry", raw))
		if err != nil || !reflect.DeepEqual(cfg.AzureResourceRetry, defaults) {
			t.Fatalf("retry defaults = %+v, %v", cfg.AzureResourceRetry, err)
		}
	}
	cfg, err := config.Parse(withField("azure_resource_retry", `{"enabled":true,"cleanup_mode":"portable_ids_and_reasoning","inspect_sse":false,"max_inspect_bytes":1048576,"inspect_timeout":"10s"}`))
	want := config.AzureResourceRetryConfig{Enabled: true, CleanupMode: "portable_ids_and_reasoning", MaxInspectBytes: 1 << 20, InspectTimeout: 10 * time.Second}
	if err != nil || cfg.AzureResourceRetry != want {
		t.Fatalf("overrides = %+v, %v", cfg.AzureResourceRetry, err)
	}
	for _, enabled := range []string{"true", "false"} {
		cfg, err := config.Parse(withField("azure_resource_retry", `{"enabled":`+enabled+`,"cleanup_mode":"portable_ids_then_reasoning"}`))
		if err != nil || cfg.AzureResourceRetry.CleanupMode != "portable_ids_then_reasoning" || cfg.AzureResourceRetry.Enabled != (enabled == "true") {
			t.Fatalf("progressive mode = %+v, %v", cfg.AzureResourceRetry, err)
		}
	}
	for _, raw := range []string{`null`, `[]`, `true`, `1`, `"x"`, `{"Enabled":true}`, `{"secret-key":1}`,
		`{"enabled":true,"\u0065nabled":false}`, `{"enabled":false,"cleanup_mode":"bad"}`,
		`{"cleanup_mode":"portable_ids_then_reasoning","cleanup_mode":"portable_ids"}`,
		`{"cleanup_mode":"portable_ids_then_reasoning","\u0063leanup_mode":"portable_ids"}`} {
		t.Run(raw, func(t *testing.T) { requireError(t, withField("azure_resource_retry", raw), "azure_resource_retry") })
	}
	invalid := map[string][]string{
		"enabled":           {`null`, `"true"`, `0`, `[]`, `{}`},
		"inspect_sse":       {`null`, `"false"`, `1`, `[]`, `{}`},
		"cleanup_mode":      {`null`, `false`, `0`, `[]`, `{}`, `""`, `"Portable_ids"`, `"Portable_ids_then_reasoning"`, `"portable_ids_then_reasoning "`, `"all"`},
		"max_inspect_bytes": {`null`, `true`, `"1"`, `[]`, `{}`, `0`, `-1`, `1.5`, `1048577`, `999999999999999999999999999`},
		"inspect_timeout":   {`null`, `true`, `1`, `[]`, `{}`, `"0s"`, `"-1s"`, `"10.000000001s"`, `"bad"`},
	}
	for key, values := range invalid {
		for _, value := range values {
			t.Run(key+value, func(t *testing.T) {
				members := `"` + key + `":` + value
				if key != "enabled" {
					members = `"enabled":false,` + members
				}
				requireError(t, withField("azure_resource_retry", `{`+members+`}`), key)
			})
		}
	}
	_, err = config.Parse(withField("azure_resource_retry", `{"secret-query-and-key":1}`))
	if err == nil || strings.Contains(err.Error(), "secret-query-and-key") {
		t.Fatalf("unsafe diagnostic: %v", err)
	}
}
