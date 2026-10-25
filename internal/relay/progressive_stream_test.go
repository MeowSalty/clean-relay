package relay_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/textproto"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestProgressiveSecondOutputEndsRepairOpportunity(t *testing.T) {
	for _, tc := range []struct{ event, stop string }{
		{"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"text\"}\n\n", "output_started"},
		{"event: response.output_item.added\ndata: {\"item\":{\"type\":\"function_call\"}}\n\n", "output_started"},
		{"data: {\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"reasoning\"}\n\n", "output_started"},
		{"data: {\"type\":\"response.compaction.delta\"}\n\n", "output_started"},
		{"event: extension\ndata: {}\n\n", "unrecognized_event"},
		{"data: [DONE]\n\n", "terminal_event"},
		{"event: response.completed\ndata: {}\n\n", "terminal_event"},
		{"event: response.incomplete\ndata: {}\n\n", "terminal_event"},
		{"event: error\ndata: {\"type\":\"response.failed\"}\n\n", "unrecognized_event"},
		{"event: error\ndata: {\"message\":\"ordinary\"}\n\n", "no_match"},
		{"event: response.failed\ndata: {\"response\":{}}\n\n", "no_match"},
	} {
		t.Run(tc.event, func(t *testing.T) {
			release := make(chan struct{})
			wire := tc.event + retrySSEError()
			var calls atomic.Int32
			h := streamNewHarness(t, progressiveOptions(map[string]any{"inspect_timeout": "10s"}), func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				if calls.Add(1) == 1 {
					progressiveMismatch(w, "http_json", 1)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, wire)
				w.(http.Flusher).Flush()
				select {
				case <-release:
				case <-r.Context().Done():
				}
			})
			response := retryDo(t, h, http.MethodPost, "/v1/responses", []byte(retryInput), nil)
			streamReadSegment(t, response, wire)
			close(release)
			if tail := streamReadAll(t, response); len(tail) != 0 || h.calls.Load() != 2 {
				t.Fatal("second output was replayed or failed to arrive before completion")
			}
			line := streamReceive(t, h.logs.lines, "second output log")
			if !strings.Contains(line, "retry_decision=retried") || !strings.Contains(line, "retry_stop_reason="+tc.stop) || !strings.Contains(line, "retry_business_failure=true") || strings.Contains(line, "retry_attempt_3") {
				t.Fatal(line)
			}
			outcome := "completed"
			if tc.stop == "no_match" {
				outcome = "upstream_business_error"
			}
			if !strings.Contains(line, "outcome="+outcome) {
				t.Fatal(line)
			}
		})
	}
}

func TestProgressiveBypassesHaveNoInspectionDelay(t *testing.T) {
	for _, tc := range []struct {
		name, body, path, method, encoding, stop string
		options                                  map[string]any
		headers                                  http.Header
		status, calls                            int
	}{
		{name: "no reasoning", body: `{"input":[{"id":"m","role":"user","content":"x"}]}`, calls: 2, stop: "stages_exhausted"},
		{name: "strong is final", body: `{"input":[{"role":"user","content":"x"},{"type":"reasoning"}]}`, calls: 2, stop: "stages_exhausted"},
		{name: "no candidate", body: `{"input":[{"type":"compaction","id":"p"}]}`, calls: 1},
		{name: "disabled", options: map[string]any{"enabled": false}, calls: 1},
		{name: "background", body: `{"background":true,"input":[{"type":"reasoning"},{"id":"m","role":"user","content":"x"}]}`, calls: 1},
		{name: "idempotency", headers: http.Header{"Idempotency-Key": {"key"}}, calls: 1},
		{name: "x idempotency", headers: http.Header{"X-Idempotency-Key": {""}}, calls: 1},
		{name: "resume", headers: http.Header{"Last-Event-Id": {"old"}}, calls: 1},
		{name: "other endpoint", path: "/v1/chat/completions", calls: 1},
		{name: "GET", method: http.MethodGet, calls: 1},
		{name: "second gzip", encoding: "gzip", calls: 2, stop: "unsupported_encoding"},
		{name: "second non-200", status: 400, calls: 2, stop: "response_not_inspected"},
		{name: "SSE disabled still repairs HTTP", options: map[string]any{"inspect_sse": false}, calls: 2, stop: "response_not_inspected"},
		{name: "compact", path: "/v1/responses/compact", calls: 2, stop: "response_not_inspected"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options := map[string]any{"inspect_timeout": "10s"}
			for key, value := range tc.options {
				options[key] = value
			}
			wire := []byte(retrySSEError())
			if tc.encoding == "gzip" {
				wire = retryGzip(t, wire)
			}
			release := make(chan struct{})
			var calls atomic.Int32
			h := streamNewHarness(t, progressiveOptions(options), func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				if int(calls.Add(1)) < tc.calls {
					progressiveMismatch(w, "http_json", 1)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				if tc.encoding != "" {
					w.Header().Set("Content-Encoding", tc.encoding)
				}
				status := tc.status
				if status == 0 {
					status = 200
				}
				w.WriteHeader(status)
				w.(http.Flusher).Flush()
				select {
				case <-release:
					_, _ = w.Write(wire)
				case <-r.Context().Done():
				}
			})
			method, path, body := tc.method, tc.path, tc.body
			if method == "" {
				method = http.MethodPost
			}
			if path == "" {
				path = "/v1/responses"
			}
			if body == "" {
				body = retryInput
			}
			started := time.Now()
			response := retryDo(t, h, method, path, []byte(body), tc.headers)
			if time.Since(started) > 2*time.Second {
				t.Fatal("bypass waited for an inspection window")
			}
			close(release)
			if got := streamReadAll(t, response); string(got) != string(wire) || h.calls.Load() != int64(tc.calls) {
				t.Fatalf("bypass changed response or attempts: %q / %d", got, h.calls.Load())
			}
			line := streamReceive(t, h.logs.lines, "bypass log")
			if tc.stop == "" {
				if strings.Contains(line, "retry_stop_reason") {
					t.Fatal("precondition skip logged as a candidate")
				}
			} else if !strings.Contains(line, "retry_stop_reason="+tc.stop) {
				t.Fatal(line)
			}
		})
	}
}

func TestProgressiveInformationalHeadersRemainGlobal(t *testing.T) {
	for _, stage := range []int{1, 2} {
		for _, source := range []string{"http_json", "sse"} {
			t.Run(strconv.Itoa(stage)+"/"+source, func(t *testing.T) {
				var calls, hints atomic.Int32
				h := streamNewHarness(t, progressiveOptions(nil), func(w http.ResponseWriter, r *http.Request) {
					_, _ = io.Copy(io.Discard, r.Body)
					attempt := int(calls.Add(1))
					if attempt < stage {
						progressiveMismatch(w, "http_json", attempt)
						return
					}
					w.Header().Set("Link", "</synthetic>; rel=preload")
					w.WriteHeader(http.StatusEarlyHints)
					w.Header().Del("Link")
					progressiveMismatch(w, source, attempt)
				})
				ctx, cancel := context.WithTimeout(context.Background(), streamDeadline)
				defer cancel()
				ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{Got1xxResponse: func(code int, header textproto.MIMEHeader) error {
					if code == 103 && header.Get("Link") != "" {
						hints.Add(1)
					}
					return nil
				}})
				response, err := h.client.Do(retryRequest(t, h, ctx, http.MethodPost, "/v1/responses", []byte(retryInput), nil))
				if err != nil {
					t.Fatal(err)
				}
				body := streamReadAll(t, response)
				if len(body) == 0 || hints.Load() != 1 || h.calls.Load() != int64(stage) {
					t.Fatalf("1xx boundary: calls=%d hints=%d body=%q", h.calls.Load(), hints.Load(), body)
				}
				line := streamReceive(t, h.logs.lines, "informational header log")
				if !strings.Contains(line, "retry_stop_reason=output_started") || stage == 2 && !strings.Contains(line, "retry_decision=retried") {
					t.Fatal(line)
				}
			})
		}
	}
}
