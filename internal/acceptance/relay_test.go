package acceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"cleanrelay/internal/config"
	"cleanrelay/internal/relay"
)

type capturedLogs struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (l *capturedLogs) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buffer.Write(p)
}

func (l *capturedLogs) text() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buffer.String()
}

func TestRunThroughLocalRelayDeletesProbeMetadataAndCancelsUpstream(t *testing.T) {
	prefix := "/synthetic-relay-path-secret%2fsegment/%76%31/"
	upstream := newScenario(t, prefix, false, nil)
	settings, err := json.Marshal(map[string]any{
		"listen":                  "127.0.0.1:1",
		"upstream_origin":         upstream.server.URL,
		"max_in_flight":           1,
		"response_header_timeout": "2s",
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Parse(settings)
	if err != nil {
		t.Fatal(err)
	}
	logs := &capturedLogs{}
	handler := relay.New(cfg, slog.New(slog.NewTextHandler(logs, nil)))
	server := httptest.NewServer(handler)
	defer func() {
		server.CloseClientConnections()
		server.Close()
		handler.CloseIdleConnections()
	}()
	report, err := Run(context.Background(), testOptions(server.URL+prefix))
	if err != nil {
		t.Fatal(err)
	}
	if !report.AutomatedPassed || report.Checks[7].Status != "skipped" {
		t.Fatalf("local relay synthetic acceptance failed: %+v", report)
	}
	waitCanceled(t, upstream.canceled)
	if upstream.posts.Load() != 6 || upstream.healths.Load() != 0 {
		t.Errorf("local health or cancellation recovery made extra upstream calls: business=%d health=%d", upstream.posts.Load(), upstream.healths.Load())
	}
	logText := logs.text()
	if strings.Count(logText, "removed_fields=1") != 6 || strings.Count(logText, "modified_items=1") != 6 || !strings.Contains(logText, "outcome=canceled") {
		t.Errorf("local synthetic deletion or cancellation evidence missing: %s", logText)
	}
	for _, secret := range []string{testOptions("").APIKey, testOptions("").Model, "synthetic-reasoning-secret", "synthetic-response-body-secret", "synthetic-call-secret"} {
		if strings.Contains(logText, secret) {
			t.Error("local relay logs exposed synthetic request or response secrets")
		}
	}
	// Local fake evidence cannot complete real client or upstream manual checks.
	assertSafeReport(t, report, server.URL, upstream.server.URL, prefix)
}
