package relay_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cleanrelay/internal/config"
	"cleanrelay/internal/relay"
)

const streamDeadline = 5 * time.Second

type streamLogs struct {
	mu     sync.Mutex
	buffer bytes.Buffer
	lines  chan string
}

func (l *streamLogs) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	n, err := l.buffer.Write(p)
	l.lines <- string(p)
	return n, err
}

func (l *streamLogs) text() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buffer.String()
}

type streamHarness struct {
	upstream *httptest.Server
	server   *httptest.Server
	handler  *relay.Handler
	client   *http.Client
	logs     *streamLogs
	calls    atomic.Int64
}

func streamNewHarness(t *testing.T, settings map[string]any, respond http.HandlerFunc) *streamHarness {
	t.Helper()
	h := &streamHarness{logs: &streamLogs{lines: make(chan string, 64)}}
	h.upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.calls.Add(1)
		respond(w, r)
	}))
	values := map[string]any{"listen": "127.0.0.1:1", "upstream_origin": h.upstream.URL, "response_header_timeout": "2s"}
	for key, value := range settings {
		values[key] = value
	}
	body, err := json.Marshal(values)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Parse(body)
	if err != nil {
		t.Fatal(err)
	}
	h.handler = relay.New(cfg, slog.New(slog.NewTextHandler(h.logs, &slog.HandlerOptions{Level: cfg.LogLevel})))
	// This wrapper only exposes Unwrap. ResponseController must reach the
	// real writer through both instrumentation layers to flush SSE.
	h.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.handler.ServeHTTP(&streamUnwrappingWriter{ResponseWriter: w}, r)
	}))
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DisableCompression = true
	transport.ResponseHeaderTimeout = streamDeadline
	h.client = &http.Client{Transport: transport}
	t.Cleanup(func() {
		h.server.CloseClientConnections()
		h.server.Close()
		h.handler.CloseIdleConnections()
		transport.CloseIdleConnections()
		h.upstream.CloseClientConnections()
		h.upstream.Close()
	})
	return h
}

type streamUnwrappingWriter struct{ http.ResponseWriter }

func (w *streamUnwrappingWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func streamGet(t *testing.T, h *streamHarness, path string) (*http.Response, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), streamDeadline)
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.server.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := h.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = response.Body.Close() })
	return response, cancel
}

func streamReceive[T any](t *testing.T, ch <-chan T, event string) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(streamDeadline):
		t.Fatalf("timed out waiting for %s", event)
		var zero T
		return zero
	}
}

func streamReadSegment(t *testing.T, response *http.Response, want string) {
	t.Helper()
	body := make([]byte, len(want))
	if _, err := io.ReadFull(response.Body, body); err != nil {
		t.Fatalf("read %d-byte segment before upstream completion: %v", len(want), err)
	}
	if string(body) != want {
		t.Fatalf("stream segment changed (%d bytes)", len(want))
	}
}

func streamReadAll(t *testing.T, response *http.Response) []byte {
	t.Helper()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	return body
}

func TestSSEStreamsSegmentsHeartbeatsAndLongEvents(t *testing.T) {
	segments := []string{
		": keepalive\n\n",
		"event: response.delta\r\ndata: " + strings.Repeat("long-event-中文", 20000) + "\r\n\r\n",
		"event: error\ndata: {\"type\":\"synthetic_upstream_error\"}\n\n",
		"data: [DONE]\n\n",
	}
	advance, finish := make(chan struct{}), make(chan struct{})
	finished := make(chan struct{})
	h := streamNewHarness(t, nil, func(w http.ResponseWriter, r *http.Request) {
		defer close(finished)
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		for i, segment := range segments {
			if i != 0 {
				select {
				case <-advance:
				case <-r.Context().Done():
					return
				}
			}
			if _, err := io.WriteString(w, segment); err != nil {
				return
			}
			w.(http.Flusher).Flush()
		}
		select {
		case <-finish:
		case <-r.Context().Done():
		}
	})
	response, _ := streamGet(t, h, "/custom/v1/responses")
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "text/event-stream; charset=utf-8" {
		t.Fatalf("unexpected SSE status or headers: %d %v", response.StatusCode, response.Header)
	}
	for i, segment := range segments {
		if i != 0 {
			select {
			case advance <- struct{}{}:
			case <-time.After(streamDeadline):
				t.Fatal("upstream did not accept permission to send the next segment")
			}
		}
		streamReadSegment(t, response, segment)
		select {
		case <-finished:
			t.Fatal("upstream ended before the client observed a segment")
		default:
		}
	}
	close(finish)
	if tail := streamReadAll(t, response); len(tail) != 0 {
		t.Fatalf("relay added %d bytes to the stream", len(tail))
	}
	line := streamReceive(t, h.logs.lines, "completed SSE log")
	if !strings.Contains(line, "status=200") || !strings.Contains(line, "outcome=completed") {
		t.Fatalf("SSE completion log = %q", line)
	}
}

func TestSSEFlushesHeadersAndHasNoTotalResponseTimeout(t *testing.T) {
	release := make(chan struct{})
	h := streamNewHarness(t, map[string]any{"response_header_timeout": "100ms"}, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		select {
		case <-release:
			_, _ = io.WriteString(w, "data: delayed\n\n")
		case <-r.Context().Done():
		}
	})
	response, _ := streamGet(t, h, "/v1/responses")
	// The header timeout has already ended once headers reach the client.
	// Hold the body beyond that timeout to detect an accidental total timer.
	timer := time.NewTimer(250 * time.Millisecond)
	defer timer.Stop()
	<-timer.C
	close(release)
	if body := streamReadAll(t, response); string(body) != "data: delayed\n\n" {
		t.Fatalf("delayed SSE = %q", body)
	}
}

func TestSSEClientCancellationReleasesUpstreamAndConcurrency(t *testing.T) {
	canceled := make(chan struct{})
	started := make(chan context.Context, 1)
	h := streamNewHarness(t, map[string]any{"max_in_flight": 1}, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/stream" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		started <- r.Context()
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(canceled)
	})
	response, cancel := streamGet(t, h, "/stream?token=synthetic-query-secret")
	upstreamCtx := streamReceive(t, started, "upstream stream start")
	streamReadSegment(t, response, "data: first\n\n")

	extra, _ := streamGet(t, h, "/extra")
	if extra.StatusCode != http.StatusServiceUnavailable || extra.Header.Get("Retry-After") != "1" {
		t.Fatalf("full relay returned %d %v", extra.StatusCode, extra.Header)
	}
	streamReadAll(t, extra)
	streamReceive(t, h.logs.lines, "concurrency rejection log")
	health, _ := streamGet(t, h, "/_cleanrelay/healthz")
	if health.StatusCode != http.StatusOK || strings.TrimSpace(string(streamReadAll(t, health))) != `{"status":"ok"}` {
		t.Fatal("health check must remain available at the concurrency limit")
	}
	ctx, healthCancel := context.WithTimeout(context.Background(), streamDeadline)
	defer healthCancel()
	healthReq, err := http.NewRequestWithContext(ctx, http.MethodPost, h.server.URL+"/_cleanrelay/healthz", nil)
	if err != nil {
		t.Fatal(err)
	}
	wrongMethod, err := h.client.Do(healthReq)
	if err != nil {
		t.Fatal(err)
	}
	if wrongMethod.StatusCode != http.StatusMethodNotAllowed || wrongMethod.Header.Get("Allow") != http.MethodGet {
		t.Fatalf("health method check returned %d %v", wrongMethod.StatusCode, wrongMethod.Header)
	}
	streamReadAll(t, wrongMethod)
	if h.calls.Load() != 1 {
		t.Fatalf("full relay or health check called upstream; calls=%d", h.calls.Load())
	}

	cancel()
	streamReceive(t, canceled, "actual upstream context cancellation")
	if !errors.Is(upstreamCtx.Err(), context.Canceled) {
		t.Fatalf("upstream context = %v", upstreamCtx.Err())
	}
	tail, readErr := io.ReadAll(response.Body)
	if readErr == nil || len(tail) != 0 {
		t.Fatalf("canceled stream appended %q; read error=%v", tail, readErr)
	}
	line := streamReceive(t, h.logs.lines, "canceled request log and slot release")
	if !strings.Contains(line, "outcome=canceled") || strings.Contains(line, "outcome=completed") || strings.Contains(line, "synthetic-query-secret") {
		t.Fatalf("cancellation log = %q", line)
	}
	after, _ := streamGet(t, h, "/after")
	streamReadAll(t, after)
	if after.StatusCode != http.StatusNoContent || h.calls.Load() != 2 {
		t.Fatalf("slot not released: status=%d calls=%d", after.StatusCode, h.calls.Load())
	}
}

func TestResponseHeaderTimeoutCancelsUpstreamAndReleasesSlot(t *testing.T) {
	canceled := make(chan struct{})
	h := streamNewHarness(t, map[string]any{"max_in_flight": 1, "response_header_timeout": "100ms"}, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			<-r.Context().Done()
			close(canceled)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	response, _ := streamGet(t, h, "/slow?token=synthetic-timeout-secret")
	body := streamReadAll(t, response)
	if response.StatusCode != http.StatusGatewayTimeout || !bytes.Contains(body, []byte(`"code":"upstream_timeout"`)) {
		t.Fatalf("header timeout response = %d %q", response.StatusCode, body)
	}
	streamReceive(t, canceled, "upstream cancellation after header timeout")
	line := streamReceive(t, h.logs.lines, "timeout log")
	if !strings.Contains(line, "outcome=upstream_timeout") || strings.Contains(line, "synthetic-timeout-secret") {
		t.Fatalf("timeout log = %q", line)
	}
	after, _ := streamGet(t, h, "/after")
	streamReadAll(t, after)
	if after.StatusCode != http.StatusNoContent || h.calls.Load() != 2 {
		t.Fatalf("timeout did not release the slot: status=%d calls=%d", after.StatusCode, h.calls.Load())
	}
}

func TestSSEUpstreamInterruptionDoesNotAppendErrorOrCompletion(t *testing.T) {
	abort := make(chan struct{})
	h := streamNewHarness(t, map[string]any{"max_in_flight": 1}, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/stream" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: partial\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-abort:
			panic(http.ErrAbortHandler)
		case <-r.Context().Done():
		}
	})
	response, _ := streamGet(t, h, "/stream?token=synthetic-interruption-secret")
	streamReadSegment(t, response, "data: partial\n\n")
	close(abort)
	tail, readErr := io.ReadAll(response.Body)
	if readErr == nil || len(tail) != 0 || response.StatusCode != http.StatusOK {
		t.Fatalf("interrupted stream = status %d, tail %q, error %v", response.StatusCode, tail, readErr)
	}
	line := streamReceive(t, h.logs.lines, "interrupted stream log")
	if !strings.Contains(line, "outcome=interrupted") || !strings.Contains(line, "status=200") || strings.Contains(line, "synthetic-interruption-secret") {
		t.Fatalf("interruption log = %q", line)
	}
	after, _ := streamGet(t, h, "/after")
	streamReadAll(t, after)
	if after.StatusCode != http.StatusNoContent || h.calls.Load() != 2 {
		t.Fatalf("interrupted stream did not release its slot: status=%d calls=%d", after.StatusCode, h.calls.Load())
	}
}

func TestRequestLogsCountsAndLocalIDsWithoutPayload(t *testing.T) {
	h := streamNewHarness(t, nil, func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			return
		}
		_, _ = io.WriteString(w, "synthetic-response-secret")
	})
	ids := make(map[string]bool)
	for range 2 {
		ctx, cancel := context.WithTimeout(context.Background(), streamDeadline)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.server.URL+"/v1/responses?token=synthetic-query-secret", bytes.NewReader(httpReadFixture(t, "metadata.before.json")))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer synthetic-auth-secret")
		req.Header.Set("X-Request-ID", "synthetic-client-id-secret")
		response, err := h.client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		streamReadAll(t, response)
		line := streamReceive(t, h.logs.lines, "request counts log")
		for _, field := range []string{"request_id=local-", "method=POST", "path=/v1/responses", "input_items=8", "removed_fields=7", "modified_items=7", "status=200", "duration_ms=", "outcome=completed"} {
			if !strings.Contains(line, field) {
				t.Errorf("log lacks %q: %s", field, line)
			}
		}
		for _, field := range strings.Fields(line) {
			if strings.HasPrefix(field, "request_id=") {
				if ids[field] {
					t.Fatalf("reused local request ID: %s", field)
				}
				ids[field] = true
			}
		}
	}
	logs := h.logs.text()
	for _, secret := range []string{"synthetic-response-secret", "synthetic-query-secret", "synthetic-auth-secret", "synthetic-client-id-secret", "internal_chat_message_metadata_passthrough", "fixture_large_integer", "9007199254740993"} {
		if strings.Contains(logs, secret) {
			t.Errorf("logs leaked %q: %s", secret, logs)
		}
	}
}

func TestUpstreamFailureLogsAreSanitizedAtEveryLevel(t *testing.T) {
	for _, level := range []string{"debug", "info", "warn", "error"} {
		t.Run(level, func(t *testing.T) {
			h := streamNewHarness(t, map[string]any{"log_level": level}, func(w http.ResponseWriter, r *http.Request) {
				panic(http.ErrAbortHandler)
			})
			ctx, cancel := context.WithTimeout(context.Background(), streamDeadline)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.server.URL+"/v1/responses?token=synthetic-query-secret", strings.NewReader(`{"input":[{"text":"synthetic-body-secret"}]}`))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer synthetic-auth-secret")
			response, err := h.client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body := streamReadAll(t, response)
			if response.StatusCode != http.StatusBadGateway || !bytes.Contains(body, []byte(`"code":"upstream_error"`)) || h.calls.Load() != 1 {
				t.Fatalf("upstream failure = %d %q, calls=%d", response.StatusCode, body, h.calls.Load())
			}
			line := streamReceive(t, h.logs.lines, "sanitized upstream failure log")
			if !strings.Contains(line, "outcome=upstream_error") {
				t.Fatalf("failure log = %q", line)
			}
			for _, secret := range []string{"synthetic-body-secret", "synthetic-query-secret", "synthetic-auth-secret"} {
				if strings.Contains(line, secret) || bytes.Contains(body, []byte(secret)) {
					t.Fatalf("upstream error leaked %q: log=%q body=%q", secret, line, body)
				}
			}
		})
	}
}

func TestUpstreamConnectionFailureDoesNotCallBusinessHandler(t *testing.T) {
	h := streamNewHarness(t, nil, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	h.upstream.Close()
	response, _ := streamGet(t, h, "/v1/models?token=synthetic-connection-secret")
	body := streamReadAll(t, response)
	if response.StatusCode != http.StatusBadGateway || h.calls.Load() != 0 || !bytes.Contains(body, []byte(`"code":"upstream_error"`)) {
		t.Fatalf("connection failure = %d %q, calls=%d", response.StatusCode, body, h.calls.Load())
	}
	line := streamReceive(t, h.logs.lines, "connection failure log")
	if !strings.Contains(line, "outcome=upstream_error") || strings.Contains(line, "synthetic-connection-secret") {
		t.Fatalf("connection failure log = %q", line)
	}
}

func TestUnsolicitedUpstreamUpgradeIsRejected(t *testing.T) {
	h := httpNewHarness(t, nil, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Connection", "Upgrade")
		w.Header().Set("Upgrade", "websocket")
		w.WriteHeader(http.StatusSwitchingProtocols)
	})
	req := httpNewRequest(t, h, http.MethodGet, "/v1/responses", nil)
	response, body := httpDo(t, h, req)
	if response.StatusCode != http.StatusBadGateway || !bytes.Contains(body, []byte(`"code":"upstream_error"`)) || h.calls.Load() != 1 {
		t.Fatalf("unsolicited upgrade = %d %q, calls=%d", response.StatusCode, body, h.calls.Load())
	}
}

func TestUntrustedTLSUpstreamDoesNotReceiveBusinessRequests(t *testing.T) {
	var calls atomic.Int64
	secure := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	secure.Config.ErrorLog = log.New(io.Discard, "", 0)
	t.Cleanup(secure.Close)
	h := httpNewHarness(t, map[string]any{"upstream_origin": secure.URL}, nil)
	response, body := httpDo(t, h, httpNewRequest(t, h, http.MethodGet, "/v1/models", nil))
	httpAssertLocalError(t, h, response, body, http.StatusBadGateway, "upstream_error")
	if calls.Load() != 0 {
		t.Fatalf("untrusted TLS upstream received %d business requests", calls.Load())
	}
}

func TestRepeatedContentTypeIsRejectedBeforeUpstream(t *testing.T) {
	h := httpNewHarness(t, nil, nil)
	req := httpNewRequest(t, h, http.MethodPost, "/v1/responses", []byte(`{"input":[]}`))
	req.Header["Content-Type"] = []string{"application/json", "application/json"}
	response, body := httpDo(t, h, req)
	httpAssertLocalError(t, h, response, body, http.StatusUnsupportedMediaType, "unsupported_media_type")
}

func TestTECapabilitiesAreNotForwarded(t *testing.T) {
	h := httpNewHarness(t, nil, nil)
	req := httpNewRequest(t, h, http.MethodGet, "/v1/models", nil)
	req.Header.Set("Te", "trailers")
	response, body := httpDo(t, h, req)
	httpAssertOK(t, response, body)
	if got := httpNextRequest(t, h); len(got.Header.Values("Te")) != 0 {
		t.Fatalf("hop-by-hop TE reached upstream: %v", got.Header.Values("Te"))
	}
}

func TestResponseTrailersArePreserved(t *testing.T) {
	h := httpNewHarness(t, nil, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Trailer", "X-Upstream-Final")
		_, _ = io.WriteString(w, "unchanged response")
		w.Header().Set("X-Upstream-Final", "synthetic-final-value")
	})
	response, body := httpDo(t, h, httpNewRequest(t, h, http.MethodGet, "/v1/models", nil))
	if string(body) != "unchanged response" || response.Trailer.Get("X-Upstream-Final") != "synthetic-final-value" {
		t.Fatalf("response body or trailers changed: %q %v", body, response.Trailer)
	}
}

func TestClientBodyReadTimeoutDoesNotReachUpstream(t *testing.T) {
	h := httpNewHarness(t, nil, nil)
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	req.Header.Set("Content-Type", "application/json")
	req.Body = io.NopCloser(streamTimeoutReader{})
	response, body := httpServeSynthetic(t, h, req)
	httpAssertLocalError(t, h, response, body, http.StatusRequestTimeout, "request_timeout")
}

type streamTimeoutReader struct{}

func (streamTimeoutReader) Read([]byte) (int, error) { return 0, streamTimeoutError{} }

type streamTimeoutError struct{}

func (streamTimeoutError) Error() string   { return "synthetic-read-secret" }
func (streamTimeoutError) Timeout() bool   { return true }
func (streamTimeoutError) Temporary() bool { return true }

func TestCanceledRequestDoesNotCallUpstreamOrWriteAnError(t *testing.T) {
	for _, cancelDuringRead := range []bool{false, true} {
		t.Run(map[bool]string{false: "before handler", true: "during body read"}[cancelDuringRead], func(t *testing.T) {
			h := streamNewHarness(t, nil, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			req := httptest.NewRequest(http.MethodPost, "/v1/responses?token=synthetic-query-secret", nil).WithContext(ctx)
			req.Header.Set("Content-Type", "application/json")
			started := make(chan struct{})
			if cancelDuringRead {
				req.Body = io.NopCloser(&streamCanceledReader{ctx: ctx, started: started})
			} else {
				cancel()
			}
			recorder := httptest.NewRecorder()
			done := make(chan struct{})
			go func() {
				h.handler.ServeHTTP(recorder, req)
				close(done)
			}()
			if cancelDuringRead {
				streamReceive(t, started, "request body read")
				cancel()
			}
			streamReceive(t, done, "canceled handler return")
			if recorder.Body.Len() != 0 || h.calls.Load() != 0 {
				t.Fatalf("canceled request wrote %q and made %d upstream calls", recorder.Body, h.calls.Load())
			}
			line := streamReceive(t, h.logs.lines, "canceled request log")
			if !strings.Contains(line, "outcome=canceled") || strings.Contains(line, "synthetic-query-secret") {
				t.Fatalf("canceled request log = %q", line)
			}
		})
	}
}

type streamCanceledReader struct {
	ctx     context.Context
	started chan struct{}
}

func (r *streamCanceledReader) Read([]byte) (int, error) {
	close(r.started)
	<-r.ctx.Done()
	return 0, r.ctx.Err()
}
