package relay_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/textproto"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConditionalSSEEarlyErrorDiscardsFirstPrefix(t *testing.T) {
	for _, kind := range []string{"error", "response.failed"} {
		t.Run(kind, func(t *testing.T) {
			var calls atomic.Int32
			firstCanceled, finishSecond := make(chan struct{}), make(chan struct{})
			secondPart := "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"second response\"}\n\n"
			secondEnd := "event: response.completed\ndata: {\"type\":\"response.completed\"}\n\n"
			h := streamNewHarness(t, retryOptions(map[string]any{"inspect_timeout": "1s"}), func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
				if calls.Add(1) == 1 {
					_, _ = io.WriteString(w, "\xef\xbb\xbf: heartbeat\r\nevent: response.created\r\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"first-response-id\",\"output\":[]}}\r\n\r\n")
					w.(http.Flusher).Flush()
					_, _ = io.WriteString(w, "event: response.in_progress\ndata: {\"type\":\"response.in_progress\",\"response\":{\"output\":[],\"error\":null}}\n\n")
					if kind == "error" {
						_, _ = io.WriteString(w, retrySSEError())
					} else {
						_, _ = io.WriteString(w, "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"message\":"+strconv.Quote(retryMessage)+"}}}\n\n")
					}
					w.(http.Flusher).Flush()
					<-r.Context().Done()
					close(firstCanceled)
					return
				}
				_, _ = io.WriteString(w, secondPart)
				w.(http.Flusher).Flush()
				select {
				case <-finishSecond:
					_, _ = io.WriteString(w, secondEnd)
				case <-r.Context().Done():
				}
			})
			response := retryDo(t, h, http.MethodPost, "/v1/responses", []byte(retryInput), nil)
			streamReadSegment(t, response, secondPart)
			streamReceive(t, firstCanceled, "discarded first stream cancellation")
			close(finishSecond)
			if tail := streamReadAll(t, response); string(tail) != secondEnd || h.calls.Load() != 2 {
				t.Fatalf("tail=%q calls=%d", tail, h.calls.Load())
			}
			line := streamReceive(t, h.logs.lines, "SSE retry log")
			if !strings.Contains(line, "retry_error_source=sse") || !strings.Contains(line, "first_upstream_status=200") || !strings.Contains(line, "retry_business_failure=true") || strings.Contains(line, "first-response-id") {
				t.Fatal(line)
			}
		})
	}
}

func TestConditionalSSEOutputPermanentlyClosesWindow(t *testing.T) {
	for _, first := range []string{
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"text\"}\n\n",
		"event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"function_call\"}}\n\n",
		"data: {\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"reasoning\"}\n\n",
		"data: {\"type\":\"response.compaction.delta\"}\n\n",
		"event: extension\ndata: {}\n\n", "data: [DONE]\n\n",
		"event: response.completed\ndata: {}\n\n", "event: response.incomplete\ndata: {}\n\n",
		"event: error\ndata: {\"type\":\"response.failed\"}\n\n",
		"event: error\ndata: {\"message\":\"ordinary error\"}\n\n",
	} {
		t.Run(first, func(t *testing.T) {
			finish := make(chan struct{})
			// Put output and the resource error in one Write. The parser must
			// decide in event order, not search the whole chunk for an error.
			wire := first + retrySSEError()
			h := streamNewHarness(t, retryOptions(map[string]any{"inspect_timeout": "10s"}), func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, wire)
				w.(http.Flusher).Flush()
				select {
				case <-finish:
				case <-r.Context().Done():
				}
			})
			response := retryDo(t, h, http.MethodPost, "/v1/responses", []byte(retryInput), nil)
			streamReadSegment(t, response, wire)
			close(finish)
			if tail := streamReadAll(t, response); len(tail) != 0 || h.calls.Load() != 1 {
				t.Fatal("output was replayed")
			}
		})
	}
}

func TestConditionalSSEBypassesDoNotWaitForFirstByte(t *testing.T) {
	cases := []struct {
		name                 string
		options              map[string]any
		body, path, encoding string
		headers              http.Header
		status               int
	}{
		{name: "SSE inspection disabled", options: map[string]any{"inspect_sse": false}},
		{name: "feature disabled", options: map[string]any{"enabled": false}},
		{name: "no candidate", body: `{"input":[{"type":"compaction","id":"p"}]}`},
		{name: "string input", body: `{"input":"hello"}`},
		{name: "gzip", encoding: "gzip"},
		{name: "non-200 SSE", status: 400},
		{name: "compact SSE", path: "/v1/responses/compact"},
		{name: "background", body: `{"background":true,"input":[{"id":"m","role":"user","content":"x"}]}`},
		{name: "resume", headers: http.Header{"Last-Event-Id": {"old"}}},
		{name: "idempotency", headers: http.Header{"Idempotency-Key": {"old"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			release := make(chan struct{})
			options := map[string]any{"inspect_timeout": "10s"}
			for key, value := range tc.options {
				options[key] = value
			}
			wire := []byte(retrySSEError())
			if tc.encoding == "gzip" {
				wire = retryGzip(t, wire)
			}
			h := streamNewHarness(t, retryOptions(options), func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
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
			body, path := tc.body, tc.path
			if body == "" {
				body = retryInput
			}
			if path == "" {
				path = "/v1/responses"
			}
			response := retryDo(t, h, http.MethodPost, path, []byte(body), tc.headers)
			// Headers must arrive while upstream is still waiting for release.
			close(release)
			if got := streamReadAll(t, response); !bytes.Equal(got, wire) || h.calls.Load() != 1 {
				t.Fatalf("bypass wire/calls changed: %d", h.calls.Load())
			}
		})
	}
}

func TestConditionalSSELimitsAndStalledRead(t *testing.T) {
	for _, tc := range []struct {
		name, prefix string
		limit        int
		timeout      string
	}{
		{"silent", "", 65536, "30ms"},
		{"heartbeat timeout", ": heartbeat\n\n", 65536, "30ms"},
		{"heartbeat limit", strings.Repeat(": heartbeat\n\n", 20), 64, "10s"},
		{"long event", "data: " + strings.Repeat("x", 100000), 65536, "10s"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			release := make(chan struct{})
			h := streamNewHarness(t, retryOptions(map[string]any{"max_inspect_bytes": tc.limit, "inspect_timeout": tc.timeout}), func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(200)
				_, _ = io.WriteString(w, tc.prefix)
				w.(http.Flusher).Flush()
				select {
				case <-release:
					_, _ = io.WriteString(w, retrySSEError())
				case <-r.Context().Done():
				}
			})
			start := time.Now()
			response := retryDo(t, h, http.MethodPost, "/v1/responses", []byte(retryInput), nil)
			if time.Since(start) > 2*time.Second {
				t.Fatal("bounded inspection stalled")
			}
			if tc.prefix != "" {
				streamReadSegment(t, response, tc.prefix)
			}
			close(release)
			if tail := streamReadAll(t, response); string(tail) != retrySSEError() || h.calls.Load() != 1 {
				t.Fatal("post-window error was retried or lost")
			}
		})
	}
}

func TestConditionalSSESecondErrorIsForwarded(t *testing.T) {
	h := streamNewHarness(t, retryOptions(nil), func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, retrySSEError())
	})
	response := retryDo(t, h, http.MethodPost, "/v1/responses", []byte(retryInput), nil)
	if got := streamReadAll(t, response); string(got) != retrySSEError() || h.calls.Load() != 2 {
		t.Fatalf("second error: %q calls=%d", got, h.calls.Load())
	}
}

func TestUpstreamInformationalHeadersPreventRetry(t *testing.T) {
	for _, sse := range []bool{false, true} {
		t.Run(strconv.FormatBool(sse), func(t *testing.T) {
			wire := retryHTTPError()
			if sse {
				wire = retrySSEError()
			}
			h := streamNewHarness(t, retryOptions(nil), func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Link", "</synthetic>; rel=preload")
				w.WriteHeader(http.StatusEarlyHints)
				w.Header().Del("Link")
				if sse {
					w.Header().Set("Content-Type", "text/event-stream")
				} else {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(400)
				}
				_, _ = io.WriteString(w, wire)
			})
			var hints atomic.Int32
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
			if got := streamReadAll(t, response); string(got) != wire || hints.Load() != 1 || h.calls.Load() != 1 {
				t.Fatalf("1xx boundary: hints=%d calls=%d body=%s", hints.Load(), h.calls.Load(), got)
			}
			if line := streamReceive(t, h.logs.lines, "1xx log"); !strings.Contains(line, "retry_decision=output_started") {
				t.Fatal(line)
			}
		})
	}
}

func TestLocalUploadContinueDoesNotPreventRetry(t *testing.T) {
	var calls atomic.Int32
	h := streamNewHarness(t, retryOptions(nil), func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if r.Header.Get("Expect") != "" {
			t.Error("Expect reached upstream")
		}
		w.Header().Set("Content-Type", "application/json")
		if calls.Add(1) == 1 {
			w.WriteHeader(400)
			_, _ = io.WriteString(w, retryHTTPError())
			return
		}
		_, _ = io.WriteString(w, `{"ok":true}`)
	})
	response := retryDo(t, h, http.MethodPost, "/v1/responses", []byte(retryInput), http.Header{"Expect": {"100-continue"}})
	streamReadAll(t, response)
	if response.StatusCode != 200 || h.calls.Load() != 2 {
		t.Fatal("local 100 Continue closed the window")
	}
}

func TestConditionalRetryCancellationReleasesAttemptAndSlot(t *testing.T) {
	for _, stage := range []string{"inspection", "second_headers", "second_stream", "second_inspection", "third_headers", "third_stream"} {
		t.Run(stage, func(t *testing.T) {
			started, canceled := make(chan context.Context, 1), make(chan struct{})
			var calls atomic.Int32
			settings := retryOptions(map[string]any{"inspect_timeout": "10s"})
			if stage == "second_inspection" || strings.HasPrefix(stage, "third_") {
				settings = progressiveOptions(map[string]any{"inspect_timeout": "10s"})
			}
			blockedAttempt := int32(2)
			if stage == "inspection" {
				blockedAttempt = 1
			} else if strings.HasPrefix(stage, "third_") {
				blockedAttempt = 3
			}
			settings["max_in_flight"] = 1
			h := streamNewHarness(t, settings, func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				if r.URL.Path == "/after" {
					w.WriteHeader(204)
					return
				}
				attempt := calls.Add(1)
				if attempt < blockedAttempt {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(400)
					_, _ = io.WriteString(w, retryHTTPError())
					return
				}
				if !strings.HasSuffix(stage, "headers") {
					w.Header().Set("Content-Type", "text/event-stream")
					w.WriteHeader(200)
					if strings.HasSuffix(stage, "stream") {
						_, _ = io.WriteString(w, "data: second\n\n")
					}
					w.(http.Flusher).Flush()
				}
				started <- r.Context()
				<-r.Context().Done()
				close(canceled)
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			type exchange struct {
				response *http.Response
				err      error
			}
			result := make(chan exchange, 1)
			go func() {
				response, err := h.client.Do(retryRequest(t, h, ctx, http.MethodPost, "/v1/responses", []byte(retryInput), nil))
				result <- exchange{response, err}
			}()
			upstream := streamReceive(t, started, "active attempt")
			extra := retryDo(t, h, http.MethodGet, "/extra", nil, nil)
			streamReadAll(t, extra)
			if extra.StatusCode != 503 {
				t.Fatal("attempt did not retain its concurrency slot")
			}
			streamReceive(t, h.logs.lines, "full slot log")
			health := retryDo(t, h, http.MethodGet, "/_cleanrelay/healthz", nil, nil)
			streamReadAll(t, health)
			if health.StatusCode != 200 {
				t.Fatal("health check blocked by the retry chain")
			}
			var active exchange
			if strings.HasSuffix(stage, "stream") {
				active = streamReceive(t, result, "second stream headers")
				if active.err != nil {
					t.Fatal(active.err)
				}
				streamReadSegment(t, active.response, "data: second\n\n")
			}
			cancel()
			streamReceive(t, canceled, "actual upstream cancellation")
			if upstream.Err() == nil {
				t.Fatal("upstream context remained live")
			}
			if strings.HasSuffix(stage, "stream") {
				tail, err := io.ReadAll(active.response.Body)
				_ = active.response.Body.Close()
				if err == nil || len(tail) != 0 {
					t.Fatalf("cancellation appended %q / %v", tail, err)
				}
			} else {
				active = streamReceive(t, result, "canceled client exchange")
				if active.err == nil {
					_ = active.response.Body.Close()
					t.Fatal("request unexpectedly completed")
				}
			}
			if line := streamReceive(t, h.logs.lines, "cancellation and released slot"); !strings.Contains(line, "outcome=canceled") || !strings.Contains(line, "retry_stop_reason=canceled") {
				t.Fatal(line)
			}
			after := retryDo(t, h, http.MethodGet, "/after", nil, nil)
			streamReadAll(t, after)
			if after.StatusCode != 204 {
				t.Fatal("slot not released")
			}
			if calls.Load() != blockedAttempt {
				t.Fatalf("unexpected attempts: %d", calls.Load())
			}
		})
	}
}

func TestConditionalRetryConcurrentRequestsAreIsolated(t *testing.T) {
	for _, mode := range []string{"portable_ids", "portable_ids_and_reasoning", "portable_ids_then_reasoning"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			var mu sync.Mutex
			counts := map[string]int{}
			h := streamNewHarness(t, retryOptions(map[string]any{"cleanup_mode": mode, "inspect_timeout": "1s"}), func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				id := r.Header.Get("X-Test-Case")
				mu.Lock()
				counts[id]++
				attempt := counts[id]
				mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				switch id {
				case "retry":
					if attempt == 1 || mode == "portable_ids_then_reasoning" && attempt == 2 {
						w.WriteHeader(400)
						_, _ = io.WriteString(w, retryHTTPError())
						return
					}
				case "ordinary":
					w.WriteHeader(400)
					_, _ = io.WriteString(w, `{"error":"ordinary"}`)
					return
				case "output":
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "data: {\"type\":\"response.output_item.added\"}\n\n"+retrySSEError())
					return
				}
				_, _ = io.WriteString(w, `{"ok":true}`)
			})
			var wg sync.WaitGroup
			for _, id := range []string{"retry", "ordinary", "output", "success"} {
				wg.Go(func() {
					response := retryDo(t, h, http.MethodPost, "/v1/responses", []byte(retryInput), http.Header{"X-Test-Case": {id}})
					streamReadAll(t, response)
					want := 200
					if id == "ordinary" {
						want = 400
					}
					if response.StatusCode != want {
						t.Errorf("%s status=%d", id, response.StatusCode)
					}
				})
			}
			wg.Wait()
			mu.Lock()
			defer mu.Unlock()
			want := "map[ordinary:1 output:1 retry:2 success:1]"
			if mode == "portable_ids_then_reasoning" {
				want = "map[ordinary:1 output:1 retry:3 success:1]"
			}
			if fmt.Sprint(counts) != want {
				t.Fatalf("counts=%v", counts)
			}
		})
	}
}
