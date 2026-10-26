package config_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"cleanrelay/internal/config"
)

const defaultOrigin = "https://upstream.example"

func jsonString(value string) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err) // Marshaling a string cannot fail.
	}
	return string(encoded)
}

func withField(key, rawValue string) []byte {
	if key == "upstream_origin" {
		return []byte(`{"upstream_origin":` + rawValue + `}`)
	}
	return []byte(`{"upstream_origin":` + jsonString(defaultOrigin) + `,` + jsonString(key) + `:` + rawValue + `}`)
}

func requireError(t *testing.T, data []byte, contains string) {
	t.Helper()
	cfg, err := config.Parse(data)
	if err == nil {
		t.Fatalf("Parse(%q) succeeded, want error containing %q", data, contains)
	}
	if !strings.Contains(err.Error(), contains) {
		t.Errorf("error = %q, want it to contain %q", err, contains)
	}
	if !reflect.DeepEqual(cfg, config.Config{}) {
		t.Errorf("failed Parse returned partial configuration: %+v", cfg)
	}
}

func TestParseDefaults(t *testing.T) {
	cfg, err := config.Parse([]byte(`{"upstream_origin":"https://upstream.example"}`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.UpstreamOrigin == nil || cfg.UpstreamOrigin.String() != defaultOrigin {
		t.Fatalf("UpstreamOrigin = %v", cfg.UpstreamOrigin)
	}
	want := config.Config{
		Listen:                "127.0.0.1:17880",
		UpstreamOrigin:        cfg.UpstreamOrigin,
		DropInputFields:       []string{"internal_chat_message_metadata_passthrough"},
		MaxBodyBytes:          33554432,
		MaxInFlight:           8,
		ResponseHeaderTimeout: 5 * time.Minute,
		LogLevel:              slog.LevelInfo,
		AzureResourceRetry: config.AzureResourceRetryConfig{
			CleanupMode: "portable_ids", InspectSSE: true, MaxInspectBytes: 65536, InspectTimeout: 2 * time.Second,
			HistoryScope: "request", SessionCache: expectedSessionCache(),
		},
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Errorf("config = %+v, want %+v", cfg, want)
	}
}

func TestParseOverrides(t *testing.T) {
	cfg, err := config.Parse([]byte(`{
		"log_level": "debug",
		"response_header_timeout": "1500ms",
		"max_in_flight": 19,
		"max_body_bytes": 9007199254740993,
		"drop_input_fields": ["literal.dotted.field", "中文", "Key", "key", " "],
		"upstream_origin": "http://127.0.0.1:3000/",
		"listen": "[::1]:17881"
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.UpstreamOrigin == nil || cfg.UpstreamOrigin.String() != "http://127.0.0.1:3000" || cfg.UpstreamOrigin.Path != "" || cfg.UpstreamOrigin.RawPath != "" {
		t.Fatalf("UpstreamOrigin was not normalized: %+v", cfg.UpstreamOrigin)
	}
	want := config.Config{
		Listen:                "[::1]:17881",
		UpstreamOrigin:        cfg.UpstreamOrigin,
		DropInputFields:       []string{"literal.dotted.field", "中文", "Key", "key", " "},
		MaxBodyBytes:          9007199254740993,
		MaxInFlight:           19,
		ResponseHeaderTimeout: 1500 * time.Millisecond,
		LogLevel:              slog.LevelDebug,
		AzureResourceRetry: config.AzureResourceRetryConfig{
			CleanupMode: "portable_ids", InspectSSE: true, MaxInspectBytes: 65536, InspectTimeout: 2 * time.Second,
			HistoryScope: "request", SessionCache: expectedSessionCache(),
		},
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Errorf("config = %+v, want %+v", cfg, want)
	}
}

func TestParseEmptyRules(t *testing.T) {
	cfg, err := config.Parse(withField("drop_input_fields", `[]`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DropInputFields == nil || len(cfg.DropInputFields) != 0 {
		t.Errorf("explicit [] must remain an empty list, got %v", cfg.DropInputFields)
	}
	if cfg.MaxBodyBytes != 33554432 || cfg.MaxInFlight != 8 {
		t.Errorf("empty rules changed independent limits: %+v", cfg)
	}
}

func TestParseResultsAreIndependent(t *testing.T) {
	data := []byte(`{"upstream_origin":"https://upstream.example/"}`)
	first, err := config.Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	first.DropInputFields[0] = "changed"
	first.UpstreamOrigin.Host = "changed.example"
	second, err := config.Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	if second.DropInputFields[0] != "internal_chat_message_metadata_passthrough" || second.UpstreamOrigin.String() != defaultOrigin {
		t.Errorf("Parse results share mutable defaults: %+v", second)
	}
}

func TestParseInvalidJSON(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{"empty", ""},
		{"whitespace", " \t\r\n"},
		{"null root", `null`},
		{"array root", `[]`},
		{"string root", `"config"`},
		{"number root", `42`},
		{"boolean root", `true`},
		{"missing close", `{"upstream_origin":"https://upstream.example"`},
		{"missing colon", `{"upstream_origin" "https://upstream.example"}`},
		{"missing comma", `{"upstream_origin":"https://upstream.example" "listen":"127.0.0.1:17880"}`},
		{"missing value", `{"upstream_origin":}`},
		{"nonstring key", `{42:"value"}`},
		{"single quotes", `{'upstream_origin':'https://upstream.example'}`},
		{"comment", `{"upstream_origin":"https://upstream.example" /*comment*/}`},
		{"trailing comma", `{"upstream_origin":"https://upstream.example",}`},
		{"bad escape", `{"upstream_origin":"https://upstream.example\x"}`},
		{"control byte", "{\"upstream_origin\":\"https://upstream.example\n\"}"},
		{"second object", `{"upstream_origin":"https://upstream.example"}{}`},
		{"second null", `{"upstream_origin":"https://upstream.example"} null`},
		{"second array", `{"upstream_origin":"https://upstream.example"} []`},
		{"second string", `{"upstream_origin":"https://upstream.example"} "extra"`},
		{"second number", `{"upstream_origin":"https://upstream.example"} 1`},
		{"trailing garbage", `{"upstream_origin":"https://upstream.example"} garbage`},
		{"BOM", "\xef\xbb\xbf{\"upstream_origin\":\"https://upstream.example\"}"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) { requireError(t, []byte(tt.data), "") })
	}
	t.Run("trailing whitespace allowed", func(t *testing.T) {
		if _, err := config.Parse([]byte(" \n {\"upstream_origin\":\"https://upstream.example\"} \t\r\n")); err != nil {
			t.Fatal(err)
		}
	})
}

func TestParseUTF8(t *testing.T) {
	for name, value := range map[string]string{
		"invalid byte in value":     `{"upstream_origin":"https://upstream.example","drop_input_fields":["` + string([]byte{0xff}) + `"]}`,
		"invalid byte in key":       `{"` + string([]byte{0xc0, 0xaf}) + `":"value","upstream_origin":"https://upstream.example"}`,
		"surrogate encoded in UTF8": `{"upstream_origin":"https://upstream.example","drop_input_fields":["` + string([]byte{0xed, 0xa0, 0x80}) + `"]}`,
		"truncated rune":            `{"upstream_origin":"https://upstream.example"}` + string([]byte{0xe4, 0xb8}),
	} {
		t.Run(name, func(t *testing.T) { requireError(t, []byte(value), "UTF-8") })
	}
	cfg, err := config.Parse(withField("drop_input_fields", `["\u4e2d\u6587","\ud83d\ude00","�"]`))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cfg.DropInputFields, []string{"中文", "😀", "�"}) {
		t.Errorf("Unicode fields = %q", cfg.DropInputFields)
	}
}

func TestParseStrictKeys(t *testing.T) {
	requireError(t, []byte(`{}`), "upstream_origin")
	for _, key := range []string{"Listen", "LISTEN", "UpstreamOrigin", "upstream_Origin", "MaxBodyBytes", "dropInputFields", "log_Level", "listen ", "unknown", ""} {
		t.Run("unknown "+key, func(t *testing.T) {
			requireError(t, withField(key, `"value"`), "unknown configuration key")
		})
	}
	values := map[string]string{
		"listen":                  `"127.0.0.1:17880"`,
		"upstream_origin":         jsonString(defaultOrigin),
		"drop_input_fields":       `[]`,
		"max_body_bytes":          `1`,
		"max_in_flight":           `1`,
		"response_header_timeout": `"0s"`,
		"log_level":               `"info"`,
	}
	for key, value := range values {
		t.Run("duplicate "+key, func(t *testing.T) {
			members := jsonString(key) + `:` + value + `,` + jsonString(key) + `:` + value
			if key != "upstream_origin" {
				members = `"upstream_origin":` + jsonString(defaultOrigin) + `,` + members
			}
			requireError(t, []byte(`{`+members+`}`), "duplicate configuration key")
		})
	}
	for name, data := range map[string]string{
		"escaped key":        `{"upstream_origin":"https://upstream.example","listen":"127.0.0.1:17880","\u006cisten":"127.0.0.1:17881"}`,
		"escaped underscore": `{"upstream_origin":"https://upstream.example","upstream\u005forigin":"https://different.example"}`,
		"both escaped":       `{"\u0075pstream_origin":"https://upstream.example","upstream_origi\u006e":"https://different.example"}`,
	} {
		t.Run(name, func(t *testing.T) { requireError(t, []byte(data), "duplicate configuration key") })
	}
	t.Run("escaped known key accepted", func(t *testing.T) {
		if _, err := config.Parse([]byte(`{"upstream\u005forigin":"https://upstream.example"}`)); err != nil {
			t.Fatal(err)
		}
	})
}

func TestParseNullAndTypes(t *testing.T) {
	fields := map[string][]string{
		"listen":                  {`true`, `1`, `[]`, `{}`},
		"upstream_origin":         {`false`, `1`, `[]`, `{}`},
		"drop_input_fields":       {`true`, `1`, `"field"`, `{}`},
		"max_body_bytes":          {`true`, `"1"`, `[]`, `{}`},
		"max_in_flight":           {`false`, `"1"`, `[]`, `{}`},
		"response_header_timeout": {`true`, `0`, `[]`, `{}`},
		"log_level":               {`false`, `1`, `[]`, `{}`},
	}
	for key, values := range fields {
		t.Run(key+" null", func(t *testing.T) { requireError(t, withField(key, "null"), "null") })
		for _, value := range values {
			t.Run(key+" "+value, func(t *testing.T) { requireError(t, withField(key, value), key) })
		}
	}
	for _, value := range []string{`[null]`, `["valid",null]`, `[1]`, `[true]`, `[{}]`, `[[]]`, `[""]`, `["a","a"]`, `["a","\u0061"]`, `["中文","\u4e2d\u6587"]`, `["😀","\ud83d\ude00"]`} {
		t.Run("fields "+value, func(t *testing.T) { requireError(t, withField("drop_input_fields", value), "drop_input_fields") })
	}
}

func TestParseListen(t *testing.T) {
	for _, listen := range []string{"127.0.0.1:1", "127.0.0.2:65535", "127.255.255.254:17880", "[::1]:17880", "[0:0:0:0:0:0:0:1]:17880", "[::ffff:127.0.0.1]:17880", "127.0.0.1:017880"} {
		t.Run("valid "+listen, func(t *testing.T) {
			cfg, err := config.Parse(withField("listen", jsonString(listen)))
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Listen != listen {
				t.Errorf("Listen = %q, want %q", cfg.Listen, listen)
			}
		})
	}
	for _, listen := range []string{"", ":17880", "localhost:17880", "LOCALHOST:17880", "0.0.0.0:17880", "192.168.1.1:17880", "[::]:17880", "[2001:db8::1]:17880", "[::ffff:192.168.1.1]:17880", "127.1:17880", "127.00.0.1:17880", "127.0.0.1", "::1:17880", "[::1]", "[::1%lo]:17880", "127.0.0.1:0", "127.0.0.1:65536", "127.0.0.1:99999999999999999999", "127.0.0.1:", "127.0.0.1:-1", "127.0.0.1:+1", "127.0.0.1:1.0", "127.0.0.1:http", "127.0.0.1: 80", "127.0.0.1:８０", " 127.0.0.1:17880", "127.0.0.1:17880 "} {
		t.Run("invalid "+listen, func(t *testing.T) { requireError(t, withField("listen", jsonString(listen)), "listen") })
	}
}

func TestParseOrigin(t *testing.T) {
	valid := []struct {
		value string
		want  string
	}{
		{"https://upstream.example", "https://upstream.example"},
		{"https://upstream.example/", "https://upstream.example"},
		{"HTTP://Example.COM/", "http://Example.COM"},
		{"http://127.0.0.1:3000", "http://127.0.0.1:3000"},
		{"http://localhost:3000/", "http://localhost:3000"},
		{"https://[::1]:3000/", "https://[::1]:3000"},
		{"http://[2001:db8::1]", "http://[2001:db8::1]"},
		{"https://[::ffff:192.0.2.1]:443", "https://[::ffff:192.0.2.1]:443"},
		{"http://example.invalid:1", "http://example.invalid:1"},
		{"https://example.invalid:65535", "https://example.invalid:65535"},
		{"http://example.invalid:080", "http://example.invalid:080"},
		{"http://LOCALHOST.:3000", "http://LOCALHOST.:3000"},
		{"https://my-host", "https://my-host"},
		{"https://xn--fiqs8s.example", "https://xn--fiqs8s.example"},
		{"https://" + strings.Repeat("a", 63) + ".example", "https://" + strings.Repeat("a", 63) + ".example"},
	}
	for _, tt := range valid {
		t.Run("valid "+tt.value, func(t *testing.T) {
			cfg, err := config.Parse(withField("upstream_origin", jsonString(tt.value)))
			if err != nil {
				t.Fatal(err)
			}
			if cfg.UpstreamOrigin.String() != tt.want || cfg.UpstreamOrigin.Path != "" || cfg.UpstreamOrigin.RawPath != "" {
				t.Errorf("origin = %+v, want %q with empty path", cfg.UpstreamOrigin, tt.want)
			}
		})
	}
	invalid := []string{
		"", "upstream.example", "//upstream.example", "http:upstream.example", "https:/upstream.example", "ftp://upstream.example", "ws://upstream.example", "https://", "http://:80",
		"http://user@upstream.example", "https://user:password@upstream.example", "https://@upstream.example",
		"https://upstream.example?x=1", "https://upstream.example?", "https://upstream.example/#fragment", "https://upstream.example#", "https://upstream.example/?#",
		"https://upstream.example/v1", "https://upstream.example//", "https://upstream.example/./", "https://upstream.example/%2f", "https://upstream.example/%2F", "https://upstream.example/%2e", "https://upstream.example/..", "https://upstream.example/;",
		"https://upstream.example:", "https://upstream.example:0", "https://upstream.example:65536", "https://upstream.example:99999999999999999999", "https://upstream.example:-1", "https://upstream.example:+80", "https://upstream.example:http", "https://upstream.example:1.5", "https://upstream.example:1:2",
		"http://[]", "http://[localhost]", "http://[127.0.0.1]", "http://[::1", "http://[::1]junk", "http://[::1]:", "http://[::1]:65536", "http://[::1%25lo]:3000", "http://::1", "http://2001:db8::1",
		"https://bad host", "https://bad_host.example", "https://-bad.example", "https://bad-.example", "https://bad..example", "https://.", "https://example..", "https://999.0.0.1", "https://127.1", "https://127.00.0.1", "https://2130706433",
		"https://example\\evil", "https://exa%6dple.example", "https://example%", "https://example.example\n", " https://example.example", "https://example.example ",
		"https://" + strings.Repeat("a", 64) + ".example", "https://" + strings.Repeat("abcd.", 51) + "example",
	}
	for i, value := range invalid {
		t.Run(fmt.Sprintf("invalid %d", i), func(t *testing.T) {
			requireError(t, withField("upstream_origin", jsonString(value)), "upstream_origin")
		})
	}
}

func TestParseSelfForwarding(t *testing.T) {
	tests := []struct {
		listen string
		origin string
		reject bool
	}{
		{"127.0.0.1:17880", "http://127.0.0.1:17880", true},
		{"127.0.0.1:17880", "https://127.0.0.1:17880/", true},
		{"127.0.0.1:17880", "http://localhost:17880", true},
		{"127.0.0.1:17880", "http://LOCALHOST.:17880", true},
		{"[::1]:17880", "http://localhost:17880", true},
		{"[::1]:17880", "http://[::1]:17880", true},
		{"[::1]:17880", "http://[0:0:0:0:0:0:0:1]:17880", true},
		{"127.0.0.1:17880", "http://[::ffff:127.0.0.1]:17880", true},
		{"[::ffff:127.0.0.1]:17880", "http://127.0.0.1:17880", true},
		{"127.0.0.2:17880", "http://127.0.0.2:17880", true},
		{"127.0.0.1:80", "http://127.0.0.1", true},
		{"127.0.0.1:443", "https://localhost", true},
		{"[::1]:443", "https://[::1]/", true},
		{"127.0.0.1:017880", "http://127.0.0.1:17880", true},
		{"127.0.0.1:17880", "http://127.0.0.1:017880", true},
		{"127.0.0.1:17880", "http://127.0.0.1:3000", false},
		{"127.0.0.1:17880", "http://127.0.0.2:17880", false},
		{"127.0.0.2:17880", "http://127.0.0.1:17880", false},
		{"127.0.0.2:17880", "http://localhost:17880", false},
		{"127.0.0.1:17880", "http://[::1]:17880", false},
		{"[::1]:17880", "http://127.0.0.1:17880", false},
		{"127.0.0.1:17880", "http://example.invalid:17880", false},
		{"127.0.0.1:80", "https://localhost", false},
		{"127.0.0.1:443", "http://localhost", false},
	}
	for _, tt := range tests {
		t.Run(tt.listen+" "+tt.origin, func(t *testing.T) {
			data := []byte(`{"listen":` + jsonString(tt.listen) + `,"upstream_origin":` + jsonString(tt.origin) + `}`)
			if tt.reject {
				requireError(t, data, "back to the configured listener")
				return
			}
			if _, err := config.Parse(data); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestParseIntegerLimits(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	for _, tt := range []struct {
		key   string
		value string
	}{
		{"max_body_bytes", "1"},
		{"max_body_bytes", strconv.FormatInt(math.MaxInt64-1, 10)},
		{"max_in_flight", "1"},
		{"max_in_flight", strconv.Itoa(maxInt)},
	} {
		t.Run("valid "+tt.key+" "+tt.value, func(t *testing.T) {
			cfg, err := config.Parse(withField(tt.key, tt.value))
			if err != nil {
				t.Fatal(err)
			}
			if tt.key == "max_body_bytes" && strconv.FormatInt(cfg.MaxBodyBytes, 10) != tt.value {
				t.Errorf("MaxBodyBytes lost integer precision: %d", cfg.MaxBodyBytes)
			}
			if tt.key == "max_in_flight" && strconv.Itoa(cfg.MaxInFlight) != tt.value {
				t.Errorf("MaxInFlight = %d, want %s", cfg.MaxInFlight, tt.value)
			}
		})
	}
	for _, key := range []string{"max_body_bytes", "max_in_flight"} {
		for _, value := range []string{"0", "-0", "-1", "1.0", "1.5", "1e3", "1E0", "9223372036854775808", "-9223372036854775809", "99999999999999999999999999999999999999999", "1e999", "01", "+1", "NaN", "Infinity"} {
			t.Run("invalid "+key+" "+value, func(t *testing.T) { requireError(t, withField(key, value), "") })
		}
	}
	requireError(t, withField("max_body_bytes", "9223372036854775807"), "limit check")
	requireError(t, withField("max_in_flight", strconv.FormatUint(uint64(maxInt)+1, 10)), "max_in_flight")
}

func TestParseDuration(t *testing.T) {
	for _, tt := range []struct {
		value string
		want  time.Duration
	}{
		{"0s", 0}, {"0", 0}, {"1ns", time.Nanosecond}, {"1.5s", 1500 * time.Millisecond}, {"1h2m3s", time.Hour + 2*time.Minute + 3*time.Second}, {"1µs", time.Microsecond}, {"9223372036854775807ns", time.Duration(math.MaxInt64)},
	} {
		t.Run("valid "+tt.value, func(t *testing.T) {
			cfg, err := config.Parse(withField("response_header_timeout", jsonString(tt.value)))
			if err != nil {
				t.Fatal(err)
			}
			if cfg.ResponseHeaderTimeout != tt.want {
				t.Errorf("timeout = %v, want %v", cfg.ResponseHeaderTimeout, tt.want)
			}
		})
	}
	for _, value := range []string{"", "-1ns", "-5m", "1", "5 m", " 5m", "5m ", "5M", "1d", "NaN", "Inf", "9223372036854775808ns", "99999999999999999999h", "-9223372036854775808ns"} {
		t.Run("invalid "+value, func(t *testing.T) {
			requireError(t, withField("response_header_timeout", jsonString(value)), "response_header_timeout")
		})
	}
}

func TestParseLogLevel(t *testing.T) {
	for value, want := range map[string]slog.Level{"debug": slog.LevelDebug, "info": slog.LevelInfo, "warn": slog.LevelWarn, "error": slog.LevelError} {
		t.Run(value, func(t *testing.T) {
			cfg, err := config.Parse(withField("log_level", jsonString(value)))
			if err != nil {
				t.Fatal(err)
			}
			if cfg.LogLevel != want {
				t.Errorf("LogLevel = %v, want %v", cfg.LogLevel, want)
			}
		})
	}
	for _, value := range []string{"", "INFO", "Info", "warning", "trace", " info", "info ", "DEBUG+1", "fatal"} {
		t.Run("invalid "+value, func(t *testing.T) { requireError(t, withField("log_level", jsonString(value)), "log_level") })
	}
}

func TestErrorsDoNotLeakOriginCredentials(t *testing.T) {
	for i, origin := range []string{
		"https://private-user:private-password@example.invalid",
		"https://private-user:private-password@example.invalid:bad-port",
		"https://private-user:private-password@[invalid",
		"https://private-user:private-password@example.invalid/%bad",
		"https://private-user:private-password@example.invalid?private-query#private-fragment",
		"ftp://private-user:private-password@example.invalid",
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			_, err := config.Parse(withField("upstream_origin", jsonString(origin)))
			if err == nil {
				t.Fatal("credential-bearing URL was accepted")
			}
			for _, secret := range []string{origin, "private-user", "private-password", "private-query", "private-fragment"} {
				if strings.Contains(err.Error(), secret) {
					t.Errorf("configuration error exposes secret %q: %v", secret, err)
				}
			}
		})
	}
}

func TestLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config with spaces.json")
	data := withField("drop_input_fields", `[]`)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	got, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	want, err := config.Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Load = %+v, Parse = %+v", got, want)
	}
	t.Run("relative path", func(t *testing.T) {
		t.Chdir(dir)
		if cfg, err := config.Load(filepath.Base(path)); err != nil || !reflect.DeepEqual(cfg, want) {
			t.Errorf("relative Load = %+v, %v", cfg, err)
		}
	})
	t.Run("missing file", func(t *testing.T) {
		missing := filepath.Join(dir, "missing.json")
		cfg, err := config.Load(missing)
		if !errors.Is(err, os.ErrNotExist) || !strings.Contains(err.Error(), "missing.json") {
			t.Fatalf("Load missing file = %+v, %v", cfg, err)
		}
		if !reflect.DeepEqual(cfg, config.Config{}) {
			t.Errorf("failed Load returned partial configuration: %+v", cfg)
		}
	})
	t.Run("directory", func(t *testing.T) {
		if _, err := config.Load(dir); err == nil {
			t.Fatal("loading a directory succeeded")
		}
	})
	for name, invalid := range map[string][]byte{
		"invalid.json": []byte(`{"upstream_origin":null}`),
		"utf8.json":    append(withField("drop_input_fields", `[]`), 0xff),
		"secret.json":  withField("upstream_origin", `"https://private-user:private-password@example.invalid:bad"`),
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(dir, name)
			if err := os.WriteFile(path, invalid, 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := config.Load(path)
			if err == nil || !strings.Contains(err.Error(), name) {
				t.Fatalf("Load = %+v, %v; want error with filename", cfg, err)
			}
			if strings.Contains(err.Error(), "private-password") || strings.Contains(err.Error(), "private-user") {
				t.Errorf("Load exposes URL credentials: %v", err)
			}
			if !reflect.DeepEqual(cfg, config.Config{}) {
				t.Errorf("failed Load returned partial configuration: %+v", cfg)
			}
		})
	}
}

func FuzzParse(f *testing.F) {
	for _, seed := range [][]byte{
		[]byte(`{"upstream_origin":"https://upstream.example"}`),
		[]byte(`{"upstream_origin":"https://upstream.example/","drop_input_fields":[]}`),
		[]byte(`{"upstream_origin":"https://upstream.example","listen":null}`),
		[]byte(`{"upstream_origin":"https://upstream.example","\u006cisten":"[::1]:17880"}`),
		[]byte(`{"upstream_origin":"https://private-user:private-password@[invalid"}`),
		[]byte(`{"upstream_origin":"http://localhost:17880"}`),
		[]byte(`[]`), {0xff},
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		cfg, err := config.Parse(data)
		if err != nil {
			if !reflect.DeepEqual(cfg, config.Config{}) {
				t.Fatal("error returned with partial configuration")
			}
			return
		}
		if cfg.UpstreamOrigin == nil || cfg.UpstreamOrigin.User != nil || cfg.UpstreamOrigin.Path != "" || cfg.UpstreamOrigin.RawQuery != "" || cfg.UpstreamOrigin.ForceQuery || cfg.UpstreamOrigin.Fragment != "" {
			t.Fatalf("accepted an unsafe origin: %+v", cfg.UpstreamOrigin)
		}
		if cfg.MaxBodyBytes <= 0 || cfg.MaxBodyBytes >= math.MaxInt64 || cfg.MaxInFlight <= 0 || cfg.ResponseHeaderTimeout < 0 {
			t.Fatalf("accepted invalid resource limits: %+v", cfg)
		}
	})
}
