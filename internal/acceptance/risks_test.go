package acceptance

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const localBusy = `{"error":{"type":"clean_relay_error","code":"too_many_requests","message":"synthetic-local-error-secret"}}`

func TestAfterCancelRetriesOnlyExplicitLocalConcurrency(t *testing.T) {
	t.Run("one local rejection recovers", func(t *testing.T) {
		s := newScenario(t, "", true, func(step int, w http.ResponseWriter, r *http.Request) bool {
			if step < 6 {
				return false
			}
			if step == 6 {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = io.WriteString(w, localBusy)
			} else {
				writeJSON(w, completedResponse("synthetic-after-cancel-secret"))
			}
			return true
		})
		report, err := Run(context.Background(), testOptions(s.server.URL))
		if err != nil {
			t.Fatal(err)
		}
		if !report.AutomatedPassed || report.Checks[6].Status != "passed" || report.Checks[7].Status != "skipped" || s.posts.Load() != 7 {
			t.Fatalf("bounded retry or optional direct semantics failed: %+v; posts=%d", report, s.posts.Load())
		}
		waitCanceled(t, s.canceled)
		assertSafeReport(t, report, "synthetic-local-error-secret", "synthetic-after-cancel-secret")
	})
	for _, test := range []struct {
		name string
		code int
		body string
	}{
		{"upstream 503", 503, `{"error":{"type":"upstream_error","code":"too_many_requests"}}`},
		{"different local error", 503, `{"error":{"type":"clean_relay_error","code":"upstream_timeout"}}`},
		{"upstream 429", 429, localBusy},
		{"duplicate discriminator", 503, `{"error":{"type":"upstream_error","type":"clean_relay_error","code":"too_many_requests"}}`},
		{"noncanonical discriminator", 503, `{"Error":{"type":"clean_relay_error","code":"too_many_requests"}}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := newScenario(t, "", true, func(step int, w http.ResponseWriter, r *http.Request) bool {
				if step < 6 {
					return false
				}
				w.WriteHeader(test.code)
				_, _ = io.WriteString(w, test.body)
				return true
			})
			report, err := Run(context.Background(), testOptions(s.server.URL))
			if err != nil {
				t.Fatal(err)
			}
			assertFailureAt(t, report, 6, test.code)
			if s.posts.Load() != 6 {
				t.Error("a nonlocal or ambiguous failure was retried")
			}
			waitCanceled(t, s.canceled)
		})
	}
	t.Run("two second ceiling", func(t *testing.T) {
		s := newScenario(t, "", true, func(step int, w http.ResponseWriter, r *http.Request) bool {
			if step < 6 {
				return false
			}
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, localBusy)
			return true
		})
		started := time.Now()
		report, err := Run(context.Background(), testOptions(s.server.URL))
		if err != nil {
			t.Fatal(err)
		}
		assertFailureAt(t, report, 6, http.StatusServiceUnavailable)
		if duration := time.Since(started); duration < 1900*time.Millisecond || duration > 2800*time.Millisecond || s.posts.Load() > 25 {
			t.Errorf("local concurrency retries escaped their short bounded window: elapsed=%s posts=%d", duration, s.posts.Load())
		}
		if report.Checks[6].Detail != errConcurrency.Error() {
			t.Error("retry exhaustion did not retain a fixed safe diagnostic")
		}
		waitCanceled(t, s.canceled)
	})
}

func TestStepTimeoutCancelsHeaderBodyAndSSEWaits(t *testing.T) {
	for _, phase := range []string{"headers", "body", "sse"} {
		t.Run(phase, func(t *testing.T) {
			canceled := make(chan struct{})
			var posts atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/_cleanrelay/healthz" {
					writeJSON(w, `{"status":"ok"}`)
					return
				}
				step := posts.Add(1)
				// Fully consume the request so HTTP/1's disconnect watcher can
				// observe client cancellation while this handler is waiting.
				_, _ = io.Copy(io.Discard, r.Body)
				if phase == "sse" && step == 1 {
					writeJSON(w, completedResponse("synthetic-before-timeout-secret"))
					return
				}
				if phase == "body" {
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(w, `{"status":"completed","output":[`)
					w.(http.Flusher).Flush()
				} else if phase == "sse" {
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, ": heartbeat\n\n")
					w.(http.Flusher).Flush()
				}
				select {
				case <-r.Context().Done():
					close(canceled)
				case <-time.After(testDeadline):
					t.Error("HTTP timeout did not cancel the fake upstream")
				}
			}))
			defer server.Close()
			options := testOptions(server.URL)
			options.Timeout = 150 * time.Millisecond
			report, err := Run(context.Background(), options)
			if err != nil {
				t.Fatal(err)
			}
			failed, status := 1, 0
			if phase == "body" {
				status = 200
			} else if phase == "sse" {
				failed, status = 2, 200
			}
			assertFailureAt(t, report, failed, status)
			if report.Checks[failed].Detail != errTimeout.Error() || int(posts.Load()) != failed {
				t.Error("timeout diagnostic or fail-fast behavior was incorrect")
			}
			waitCanceled(t, canceled)
		})
	}
}

func TestCallerCancellationCancelsActiveSSE(t *testing.T) {
	started, canceled := make(chan struct{}), make(chan struct{})
	s := newScenario(t, "", true, func(step int, w http.ResponseWriter, r *http.Request) bool {
		if step != 2 {
			return false
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, ": heartbeat\n\n")
		w.(http.Flusher).Flush()
		close(started)
		select {
		case <-r.Context().Done():
			close(canceled)
		case <-time.After(testDeadline):
			t.Error("caller cancellation did not reach the server")
		}
		return true
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type result struct {
		report Report
		err    error
	}
	resultCh := make(chan result, 1)
	go func() {
		report, err := Run(ctx, testOptions(s.server.URL))
		resultCh <- result{report, err}
	}()
	waitCanceled(t, started)
	cancel()
	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatal(result.err)
		}
		// Flush proves the server sent headers, not that the client received
		// them before this deliberately racing cancellation.
		status := result.report.Checks[2].HTTPStatus
		if status != 0 && status != 200 {
			t.Errorf("unexpected status during caller cancellation: %d", status)
		}
		assertFailureAt(t, result.report, 2, status)
		if result.report.Checks[2].Detail != errCanceled.Error() {
			t.Error("caller cancellation did not produce a safe check failure")
		}
	case <-time.After(testDeadline):
		t.Fatal("Run did not stop after caller cancellation")
	}
	waitCanceled(t, canceled)
}

func TestConversationRejectsIncompleteInvalidAndOversizeResponses(t *testing.T) {
	for _, body := range []string{
		`{"status":"incomplete","output":[{"type":"message","content":[{"type":"output_text","text":"synthetic-incomplete-secret"}]}]}`,
		`{"status":"completed","error":{"message":"synthetic-service-error-secret"},"output":[]}`,
		`{"status":"completed","incomplete_details":{"reason":"max_output_tokens"},"output":[]}`,
		`{"status":"completed","output":[]}`,
		`{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":" "}]}]}`,
		`{"status":"failed","status":"completed","output":[]}`,
		completedResponse("synthetic-text-secret") + `{}`,
		"synthetic-invalid-body-secret",
		strings.Repeat("s", maxResponseBytes+1),
	} {
		s := newScenario(t, "", true, func(step int, w http.ResponseWriter, r *http.Request) bool {
			writeJSON(w, body)
			return true
		})
		report, err := Run(context.Background(), testOptions(s.server.URL))
		if err != nil {
			t.Fatal(err)
		}
		assertFailureAt(t, report, 1, 200)
		if s.posts.Load() != 1 {
			t.Error("invalid or oversized conversation caused additional model probes")
		}
		assertSafeReport(t, report, "synthetic-incomplete-secret", "synthetic-service-error-secret", "synthetic-text-secret", "synthetic-invalid-body-secret")
	}
}

func TestToolArgumentsAreValidatedWithoutExecutionOrDisclosure(t *testing.T) {
	for _, call := range []string{
		`{"type":"function_call","name":"arbitrary_tool","call_id":"synthetic-call-secret","arguments":"{\"value\":\"cleanrelay-probe\"}"}`,
		`{"type":"function_call","name":"cleanrelay_echo","call_id":"synthetic-call-secret","arguments":"{\"value\":\"synthetic-argument-secret\"}"}`,
		`{"type":"function_call","name":"cleanrelay_echo","call_id":"synthetic-call-secret","arguments":"{\"value\":\"cleanrelay-probe\",\"extra\":\"synthetic-argument-secret\"}"}`,
		`{"type":"function_call","name":"cleanrelay_echo","call_id":"synthetic-call-secret","arguments":"{\"value\":\"synthetic-argument-secret\",\"value\":\"cleanrelay-probe\"}"}`,
		`{"type":"function_call","name":"cleanrelay_echo","call_id":"synthetic-call-secret","status":"incomplete","arguments":"{\"value\":\"cleanrelay-probe\"}"}`,
	} {
		s := newScenario(t, "", true, func(step int, w http.ResponseWriter, r *http.Request) bool {
			if step != 3 {
				return false
			}
			writeJSON(w, `{"status":"completed","output":[`+call+`]}`)
			return true
		})
		report, err := Run(context.Background(), testOptions(s.server.URL))
		if err != nil {
			t.Fatal(err)
		}
		assertFailureAt(t, report, 3, 200)
		if s.posts.Load() != 3 {
			t.Error("invalid tool calls must not produce a continuation")
		}
		assertSafeReport(t, report, "synthetic-argument-secret", "arbitrary_tool")
	}
}

func TestSSELimitsIncludeMultilineDataAndHeartbeatWireBytes(t *testing.T) {
	multiline := strings.Repeat("data: "+strings.Repeat("x", 1<<20)+"\n", 5) + "\n"
	reader := newSSEReader(strings.NewReader(multiline))
	if _, err := reader.next(); !errors.Is(err, errSSEEventLimit) {
		t.Errorf("aggregate multiline event escaped its size bound: %v", err)
	}
	frame := ":" + strings.Repeat("h", (1<<20)-3) + "\n\n"
	parts := make([]io.Reader, 33)
	for i := range parts {
		parts[i] = strings.NewReader(frame)
	}
	reader = newSSEReader(io.MultiReader(parts...))
	if _, err := reader.next(); !errors.Is(err, errSSEStreamLimit) {
		t.Errorf("heartbeat-only stream escaped its total bound: %v", err)
	}
	if reader.total > maxStreamBytes+1 {
		t.Error("SSE reader consumed beyond its bounded stream sentinel")
	}
}

func TestHealthFailurePreventsAllBusinessRequests(t *testing.T) {
	for _, test := range []struct {
		code int
		body string
	}{
		{200, `{"status":"starting"}`},
		{200, `{"Status":"ok"}`},
		{200, `{"status":"down","status":"ok"}`},
		{200, `{"status":"ok"} {}`},
		{http.StatusMovedPermanently, `{"status":"ok"}`},
	} {
		var businessCalls atomic.Int64
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/_cleanrelay/healthz" {
				businessCalls.Add(1)
				writeJSON(w, `{"status":"ok"}`)
				return
			}
			w.Header().Set("Location", "/synthetic-health-redirect-secret")
			w.WriteHeader(test.code)
			_, _ = io.WriteString(w, test.body)
		}))
		report, err := Run(context.Background(), testOptions(server.URL))
		server.Close()
		if err != nil {
			t.Fatal(err)
		}
		assertFailureAt(t, report, 0, test.code)
		if businessCalls.Load() != 0 {
			t.Error("failed health or a health redirect generated additional requests")
		}
	}
}

func TestNetworkFailureDoesNotReplayTheBufferedModelRequest(t *testing.T) {
	var modelCalls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_cleanrelay/healthz" {
			writeJSON(w, `{"status":"ok"}`)
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		modelCalls.Add(1)
		connection, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		// The server accepted the entire billable-shaped request, then lost
		// its response. Replaying this failure could charge twice.
		_ = connection.Close()
	}))
	defer server.Close()
	report, err := Run(context.Background(), testOptions(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	assertFailureAt(t, report, 1, 0)
	if modelCalls.Load() != 1 {
		t.Errorf("network failure replayed a buffered model request %d times", modelCalls.Load())
	}
}
