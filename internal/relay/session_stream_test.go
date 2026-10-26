package relay_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestSessionObservationDoesNotDelayStreamOutput(t *testing.T) {
	delta := "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"early output\"}\n\n"
	finish := make(chan struct{})
	var calls atomic.Int64
	h := streamNewHarness(t, sessionOptions("portable_ids_and_reasoning"), func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			progressiveMismatch(w, "http_json", 1)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, delta)
		w.(http.Flusher).Flush()
		select {
		case <-finish:
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, sessionCompletedSSE)
	})
	response := retryDo(t, h, http.MethodPost, "/v1/responses", []byte(progressiveInput), sessionHeaders())
	streamReadSegment(t, response, delta)
	select {
	case <-h.logs.lines:
		t.Fatal("request ended before the final completion event")
	default:
	}
	close(finish)
	if string(streamReadAll(t, response)) != sessionCompletedSSE {
		t.Fatal("completion observation changed stream bytes")
	}
	line := streamReceive(t, h.logs.lines, "observed stream completion")
	if !strings.Contains(line, "recovery_acceptance=completed") {
		t.Fatalf("final stream completion was not recognized: %s", line)
	}
}

type failedSessionWriter struct{ header http.Header }

func (w *failedSessionWriter) Header() http.Header       { return w.header }
func (w *failedSessionWriter) WriteHeader(int)           {}
func (w *failedSessionWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
func (w *failedSessionWriter) Flush()                    {}

func TestSessionClientWriteFailureCannotConfirmRecovery(t *testing.T) {
	var calls atomic.Int64
	seen := make(chan retryObserved, 5)
	h := streamNewHarness(t, sessionOptions("portable_ids_and_reasoning"), func(w http.ResponseWriter, r *http.Request) {
		call := calls.Add(1)
		seen <- retryObserve(t, r)
		if call != 2 {
			progressiveMismatch(w, "http_json", int(call))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, sessionCompletedJSON)
	})
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(progressiveInput))
	request.Header = sessionHeaders()
	request.Header.Set("Content-Type", "application/json")
	func() {
		defer func() {
			if caught := recover(); caught != nil && caught != http.ErrAbortHandler {
				panic(caught)
			}
		}()
		h.handler.ServeHTTP(&failedSessionWriter{header: make(http.Header)}, request)
	}()
	line := streamReceive(t, h.logs.lines, "failed client write")
	if !strings.Contains(line, "outcome=interrupted") || !strings.Contains(line, "recovery_acceptance=unknown") {
		t.Fatalf("failed forwarding certified a scope: %s", line)
	}
	<-seen
	<-seen
	status, _, line := sessionExchange(t, h, "/v1/responses", sessionNext(t), sessionHeaders())
	if status != 400 || calls.Load() != 4 || !strings.Contains(line, "scope_cache=scope_extended") {
		t.Fatalf("failed forwarding blocked a later repair: %s", line)
	}
	sessionAssertWindow(t, streamReceive(t, seen, "request after failed forwarding").body, false, true)
	sessionAssertWindow(t, streamReceive(t, seen, "repair after failed forwarding").body, false, false)
}

func TestSessionConcurrentFirstFaultMergesEachSnapshot(t *testing.T) {
	olderReady, longerReady := make(chan struct{}), make(chan struct{})
	releaseOlder, releaseLonger := make(chan struct{}), make(chan struct{})
	seen := make(chan retryObserved, 8)
	h := streamNewHarness(t, sessionOptions("portable_ids_and_reasoning"), func(w http.ResponseWriter, r *http.Request) {
		observed := retryObserve(t, r)
		seen <- observed
		if sessionHasID(t, observed.body, "m1") {
			ready, release := olderReady, releaseOlder
			if r.Header.Get("X-Scenario") == "longer" {
				ready, release = longerReady, releaseLonger
			}
			close(ready)
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
			progressiveMismatch(w, "http_json", 1)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, sessionCompletedJSON)
	})
	type result struct {
		name string
		err  error
	}
	done := make(chan result, 2)
	start := func(name string, body []byte) {
		headers := sessionHeaders()
		headers.Set("X-Scenario", name)
		ctx, cancel := context.WithTimeout(context.Background(), streamDeadline)
		t.Cleanup(cancel)
		request := retryRequest(t, h, ctx, http.MethodPost, "/v1/responses", body, headers)
		go func() {
			response, err := h.client.Do(request)
			if err == nil {
				_, err = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
				if err == nil && response.StatusCode != 200 {
					err = fmt.Errorf("unexpected status %d", response.StatusCode)
				}
			}
			done <- result{name: name, err: err}
		}()
	}
	start("older", []byte(progressiveInput))
	streamReceive(t, olderReady, "older initial request")
	start("longer", sessionNext(t))
	streamReceive(t, longerReady, "longer initial request")
	close(releaseOlder)
	first := streamReceive(t, done, "older recovery")
	firstLog := streamReceive(t, h.logs.lines, "older recovery state commit")
	if first.name != "older" || first.err != nil || !strings.Contains(firstLog, "recovery_acceptance=completed") {
		t.Fatalf("older recovery: %+v %s", first, firstLog)
	}
	close(releaseLonger)
	second := streamReceive(t, done, "concurrent longer recovery")
	line := streamReceive(t, h.logs.lines, "concurrent recovery log")
	if second.err != nil || h.calls.Load() != 4 || !strings.Contains(line, "scope_cache=scope_extended") {
		t.Fatalf("concurrent recovery: %+v calls=%d log=%s", second, h.calls.Load(), line)
	}
	checked := false
	for i := 0; i < 4; i++ {
		observed := streamReceive(t, seen, "concurrent upstream attempt")
		if observed.header.Get("X-Scenario") == "longer" && !sessionHasID(t, observed.body, "m1") {
			sessionAssertWindow(t, observed.body, false, false)
			checked = true
		}
	}
	if !checked {
		t.Fatal("longer request never repaired its own snapshot")
	}
}
