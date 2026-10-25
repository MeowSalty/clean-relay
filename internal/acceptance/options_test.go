package acceptance

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestInvalidOptionsPerformNoNetworkActivity(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSON(w, `{"status":"ok"}`)
	}))
	defer server.Close()
	tests := []struct {
		name   string
		change func(*Options)
	}{
		{"empty base", func(o *Options) { o.BaseURL = "" }},
		{"remote base", func(o *Options) { o.BaseURL = "https://synthetic-host-secret.invalid/v1" }},
		{"relative base", func(o *Options) { o.BaseURL = "/synthetic-url-secret" }},
		{"userinfo", func(o *Options) { o.BaseURL = "http://synthetic-user-secret:synthetic-password-secret@127.0.0.1" }},
		{"query", func(o *Options) { o.BaseURL += "?synthetic-query-secret=1" }},
		{"empty query", func(o *Options) { o.BaseURL += "?" }},
		{"fragment", func(o *Options) { o.BaseURL += "#synthetic-fragment-secret" }},
		{"empty fragment", func(o *Options) { o.BaseURL += "#" }},
		{"localhost suffix", func(o *Options) { o.BaseURL = "http://localhost.synthetic-host-secret.invalid" }},
		{"localhost double dot", func(o *Options) { o.BaseURL = "http://localhost.." }},
		{"short IP", func(o *Options) { o.BaseURL = "http://127.1" }},
		{"integer IP", func(o *Options) { o.BaseURL = "http://2130706433" }},
		{"IPv4 root dot", func(o *Options) { o.BaseURL = "http://127.0.0.1." }},
		{"unspecified IP", func(o *Options) { o.BaseURL = "http://0.0.0.0" }},
		{"bare IPv6", func(o *Options) { o.BaseURL = "http://::1" }},
		{"bracketed non-IP", func(o *Options) { o.BaseURL = "http://[localhost]" }},
		{"zone loopback", func(o *Options) { o.BaseURL = "http://[::1%25synthetic-zone-secret]" }},
		{"zero port", func(o *Options) { o.BaseURL = "http://127.0.0.1:0" }},
		{"empty port", func(o *Options) { o.BaseURL = "http://127.0.0.1:" }},
		{"large port", func(o *Options) { o.BaseURL = "http://127.0.0.1:65536" }},
		{"invalid escape", func(o *Options) { o.BaseURL += "/synthetic-url-secret%xy" }},
		{"invalid direct scheme", func(o *Options) { o.DirectBaseURL = "ftp://synthetic-host-secret.invalid/v1" }},
		{"invalid direct userinfo", func(o *Options) { o.DirectBaseURL = "https://synthetic-user-secret@synthetic-host-secret.invalid" }},
		{"invalid direct host", func(o *Options) { o.DirectBaseURL = "https://synthetic_host_secret.invalid" }},
		{"invalid direct port", func(o *Options) { o.DirectBaseURL = "https://synthetic-host-secret.invalid:70000" }},
		{"invalid direct query", func(o *Options) { o.DirectBaseURL = server.URL + "?synthetic-query-secret=1" }},
		{"empty model", func(o *Options) { o.Model = " \t" }},
		{"empty key", func(o *Options) { o.APIKey = " \t" }},
		{"CR key", func(o *Options) { o.APIKey += "\rsynthetic-header-secret" }},
		{"LF key", func(o *Options) { o.APIKey += "\nsynthetic-header-secret" }},
		{"invalid UTF8 model", func(o *Options) { o.Model = string([]byte{0xff}) }},
		{"zero timeout", func(o *Options) { o.Timeout = 0 }},
		{"negative timeout", func(o *Options) { o.Timeout = -time.Second }},
		{"unbounded timeout", func(o *Options) { o.Timeout = 10*time.Minute + time.Nanosecond }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := testOptions(server.URL)
			options.DirectBaseURL = server.URL
			test.change(&options)
			report, err := Run(context.Background(), options)
			if err == nil || report.SchemaVersion != 0 || len(report.Checks) != 0 {
				t.Fatalf("invalid options must return an error and no report: %+v, %v", report, err)
			}
			for _, secret := range []string{"synthetic-", server.URL, options.APIKey, options.BaseURL, options.DirectBaseURL} {
				if secret != "" && strings.Contains(err.Error(), secret) {
					t.Error("validation error exposed a supplied value")
				}
			}
		})
	}
	if calls.Load() != 0 {
		t.Errorf("invalid options contacted local test servers %d times", calls.Load())
	}
	if _, err := Run(nil, testOptions(server.URL)); err == nil || calls.Load() != 0 {
		t.Error("nil context must fail safely before any networking")
	}
}

func TestValidOptionsAndOriginalEscapedPrefixes(t *testing.T) {
	for _, base := range []string{
		"http://127.0.0.1:17880/v1", "https://127.3.2.1:65535/api", "http://[::1]:80/v1",
		"http://[::ffff:127.0.0.1]/v1", "http://LOCALHOST.:17880/v1", "HTTP://localhost/v1",
	} {
		options := testOptions(base)
		options.DirectBaseURL = "https://synthetic-host.invalid/api%2Ftenant/v1"
		options.Timeout = 10 * time.Minute
		if err := ValidateOptions(options); err != nil {
			t.Errorf("valid synthetic options rejected: %s: %v", base, err)
		}
	}
	for _, direct := range []string{"http://192.0.2.1/v1", "https://[2001:db8::1]/v1", "https://synthetic-host.invalid.:443/", "https://例子.测试/v1"} {
		options := testOptions("http://127.0.0.1/v1")
		options.DirectBaseURL = direct
		if err := ValidateOptions(options); err != nil {
			t.Errorf("valid explicit direct host rejected: %v", err)
		}
	}
	tests := []struct{ base, want string }{
		{"http://127.0.0.1", "http://127.0.0.1/responses"},
		{"http://127.0.0.1/", "http://127.0.0.1/responses"},
		{"http://127.0.0.1/v1/", "http://127.0.0.1/v1/responses"},
		{"http://127.0.0.1/prefix//", "http://127.0.0.1/prefix//responses"},
		{"http://127.0.0.1/%76%31/a%2fb/%2E%2e", "http://127.0.0.1/%76%31/a%2fb/%2E%2e/responses"},
		{"http://127.0.0.1/a%2F", "http://127.0.0.1/a%2F/responses"},
		{"http://127.0.0.1/a%3Fq%23f", "http://127.0.0.1/a%3Fq%23f/responses"},
		{"http://LOCALHOST.:17880/%76%31", "http://localhost:17880/%76%31/responses"},
	}
	for _, test := range tests {
		base, ok := parseBaseURL(test.base, true)
		if !ok {
			t.Fatalf("valid URL rejected: %s", test.base)
		}
		if got := responsesURL(base).String(); got != test.want {
			t.Errorf("raw escaped prefix changed: got %s, want %s", got, test.want)
		}
		if healthURL(base).RequestURI() != "/_cleanrelay/healthz" {
			t.Error("health must use origin rather than Responses prefix")
		}
	}
}
