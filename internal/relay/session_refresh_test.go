package relay_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

// Compare complete documents without converting numbers to float64. The only
// permitted differences are the listed direct IDs and whole reasoning items.
func sessionAssertRepair(t *testing.T, original, actual []byte, ids, reasoning []string) {
	t.Helper()
	var wantRoot, gotRoot map[string]json.RawMessage
	if err := json.Unmarshal(original, &wantRoot); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(actual, &gotRoot); err != nil {
		t.Fatal(err)
	}
	var originalItems, gotItems []map[string]json.RawMessage
	if err := json.Unmarshal(wantRoot["input"], &originalItems); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(gotRoot["input"], &gotItems); err != nil {
		t.Fatal(err)
	}
	var wantItems []map[string]json.RawMessage
	for _, item := range originalItems {
		var id string
		_ = json.Unmarshal(item["id"], &id)
		if slices.Contains(reasoning, id) {
			if string(item["type"]) != `"reasoning"` {
				t.Fatal("test tried to drop an ordinary item")
			}
			continue
		}
		if slices.Contains(ids, id) {
			delete(item, "id")
		}
		wantItems = append(wantItems, item)
	}
	delete(wantRoot, "input")
	delete(gotRoot, "input")
	if !reflect.DeepEqual(wantRoot, gotRoot) || !reflect.DeepEqual(wantItems, gotItems) {
		t.Fatalf("repair changed protected content or selected the wrong items: %s", actual)
	}
}

func sessionAssertLog(t *testing.T, line string, fields ...string) {
	t.Helper()
	parts := strings.Fields(line)
	for _, field := range fields {
		if !slices.Contains(parts, field) {
			t.Errorf("missing %s: %s", field, line)
		}
	}
}

func TestSessionRefreshModesAndDeltas(t *testing.T) {
	for _, mode := range []string{"portable_ids", "portable_ids_and_reasoning", "portable_ids_then_reasoning"} {
		for _, source := range []string{"http_json", "sse"} {
			t.Run(mode+"/"+source, func(t *testing.T) {
				seen := make(chan retryObserved, 16)
				h := streamNewHarness(t, sessionOptions(mode), func(w http.ResponseWriter, r *http.Request) {
					seen <- retryObserve(t, r)
					if r.Header.Get("X-Scenario") == "success" {
						w.Header().Set("Content-Type", "application/json")
						_, _ = io.WriteString(w, sessionCompletedJSON)
						return
					}
					progressiveMismatch(w, source, 1)
				})
				calls := 2
				if mode == "portable_ids_then_reasoning" {
					calls = 3
				}
				_, _, line := sessionExchange(t, h, "/v1/responses", []byte(progressiveInput), sessionHeaders())
				sessionAssertLog(t, line, "upstream_attempts="+strconv.Itoa(calls), "recovery_acceptance=unknown",
					"scope_added_selectors=4", "scope_generation=1")
				for range calls {
					streamReceive(t, seen, "initial failed recovery")
				}
				body := sessionNext(t)
				oldIDs := []string{"m1", "f1", "o1"}
				var oldReasoning []string
				if mode != "portable_ids" {
					oldReasoning = []string{"r1"}
				}
				for _, next := range []struct {
					id, reasoning string
				}{
					{"b-message", "b-reasoning"},
					{"c-user", "c-reasoning"},
				} {
					_, _, line = sessionExchange(t, h, "/v1/responses", body, sessionHeaders())
					sessionAssertLog(t, line, "upstream_attempts="+strconv.Itoa(calls), "scope_cache=scope_extended",
						"scope_added_selectors=2", "scope_generation=1", "recovery_acceptance=unknown",
						"retry_attempt_2.new_removed_item_ids=1")
					sessionAssertRepair(t, body, streamReceive(t, seen, "precleaned first send").body, oldIDs, oldReasoning)
					oldIDs = append(oldIDs, next.id)
					if mode == "portable_ids_and_reasoning" {
						oldReasoning = append(oldReasoning, next.reasoning)
					}
					sessionAssertRepair(t, body, streamReceive(t, seen, "first repair").body, oldIDs, oldReasoning)
					if mode == "portable_ids_then_reasoning" {
						oldReasoning = append(oldReasoning, next.reasoning)
						sessionAssertRepair(t, body, streamReceive(t, seen, "strong repair").body, oldIDs, oldReasoning)
						sessionAssertLog(t, line, "retry_attempt_2.new_removed_reasoning_items=0",
							"retry_attempt_3.new_removed_item_ids=0", "retry_attempt_3.new_removed_reasoning_items=1")
					} else {
						newReasoning := 0
						if mode == "portable_ids_and_reasoning" {
							newReasoning = 1
						}
						sessionAssertLog(t, line, "retry_attempt_2.new_removed_reasoning_items="+strconv.Itoa(newReasoning))
					}
					// Appending a new user message is not an error and does not
					// authorize clearing its ID or the new reasoning.
					if next.id == "b-message" {
						body = sessionAppend(t, body,
							`{"id":"c-user","role":"user","status":"completed","content":"new user input"}`,
							`{"type":"reasoning","id":"c-reasoning","encrypted_content":"C-cipher"}`)
					} else {
						body = sessionAppend(t, body, `{"type":"reasoning","id":"d-reasoning","encrypted_content":"D-cipher"}`)
					}
					headers := sessionHeaders()
					headers.Set("X-Scenario", "success")
					_, _, successLog := sessionExchange(t, h, "/v1/responses", body, headers)
					sessionAssertLog(t, successLog, "upstream_attempts=1", "scope_added_selectors=0")
					sessionAssertRepair(t, body, streamReceive(t, seen, "successful appended history").body, oldIDs, oldReasoning)
				}
			})
		}
	}
}

func TestSessionRefreshAfterUncompletedStream(t *testing.T) {
	const delta = "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n"
	for _, ending := range []string{"cancel", "broken_stream", "done_only", "eof_only", "business_error"} {
		t.Run(ending, func(t *testing.T) {
			var calls atomic.Int64
			seen := make(chan retryObserved, 8)
			h := streamNewHarness(t, sessionOptions("portable_ids_and_reasoning"), func(w http.ResponseWriter, r *http.Request) {
				call := calls.Add(1)
				seen <- retryObserve(t, r)
				if call == 1 || call == 3 {
					progressiveMismatch(w, "http_json", int(call))
					return
				}
				if call != 2 {
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(w, sessionCompletedJSON)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, delta)
				w.(http.Flusher).Flush()
				switch ending {
				case "cancel":
					<-r.Context().Done()
				case "broken_stream":
					panic(http.ErrAbortHandler)
				case "done_only":
					_, _ = io.WriteString(w, "data: [DONE]\n\n")
				case "business_error":
					_, _ = io.WriteString(w, retrySSEError())
				}
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			request := retryRequest(t, h, ctx, http.MethodPost, "/v1/responses", []byte(progressiveInput), sessionHeaders())
			response, err := h.client.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			streamReadSegment(t, response, delta)
			if ending == "cancel" {
				cancel()
			}
			_, readErr := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if (ending == "cancel" || ending == "broken_stream") != (readErr != nil) {
				t.Fatalf("unexpected stream ending: %v", readErr)
			}
			line := streamReceive(t, h.logs.lines, "uncompleted repair")
			sessionAssertLog(t, line, "upstream_attempts=2", "scope_write_confirmed=true", "recovery_acceptance=unknown")
			progressiveCheckBody(t, streamReceive(t, seen, "original request").body, 1)
			progressiveCheckBody(t, streamReceive(t, seen, "written strong repair").body, 3)
			_, _, line = sessionExchange(t, h, "/v1/responses", sessionNext(t), sessionHeaders())
			sessionAssertLog(t, line, "upstream_attempts=2", "scope_cache=scope_extended", "scope_generation=1",
				"scope_added_selectors=2", "retry_attempt_2.new_removed_item_ids=1", "retry_attempt_2.new_removed_reasoning_items=1")
			sessionAssertWindow(t, streamReceive(t, seen, "first request after interruption").body, false, true)
			sessionAssertWindow(t, streamReceive(t, seen, "repair after interruption").body, false, false)
		})
	}
}

func TestSessionRefreshRequiresExactEarlyError(t *testing.T) {
	const textOutput = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"keep this output\"}\n\n"
	const toolOutput = "data: {\"type\":\"response.function_call_arguments.delta\",\"delta\":\"{}\"}\n\n"
	for _, tc := range []struct {
		name, media, wire string
		status            int
		earlyHints        bool
	}{
		{"ordinary 400", "application/json", `{"error":{"type":"invalid_request_error","message":"ordinary failure"}}`, 400, false},
		{"authentication", "application/json", retryHTTPError(), 401, false},
		{"rate limit", "application/json", retryHTTPError(), 429, false},
		{"ambiguous azure text", "application/json", `{"error":{"message":"created under a different azure openai resource"}}`, 400, false},
		{"duplicate error", "application/json", `{"error":{},` + strings.TrimPrefix(retryHTTPError(), "{"), 400, false},
		{"conflicting SSE", "text/event-stream", "event: error\ndata: {\"message\":\"other\",\"error\":{\"message\":" + strconv.Quote(retryMessage) + "}}\n\n", 200, false},
		{"SSE after text", "text/event-stream", textOutput + retrySSEError(), 200, false},
		{"SSE after tool output", "text/event-stream", toolOutput + retrySSEError(), 200, false},
		{"after response headers", "application/json", retryHTTPError(), 400, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int64
			seen := make(chan retryObserved, 8)
			h := streamNewHarness(t, sessionOptions("portable_ids_and_reasoning"), func(w http.ResponseWriter, r *http.Request) {
				call := calls.Add(1)
				seen <- retryObserve(t, r)
				if call == 1 {
					progressiveMismatch(w, "http_json", 1)
					return
				}
				if r.Header.Get("X-Scenario") == "probe" {
					if tc.earlyHints {
						w.WriteHeader(http.StatusEarlyHints)
					}
					w.Header().Set("Content-Type", tc.media)
					w.WriteHeader(tc.status)
					_, _ = io.WriteString(w, tc.wire)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, sessionCompletedJSON)
			})
			sessionExchange(t, h, "/v1/responses", []byte(progressiveInput), sessionHeaders())
			streamReceive(t, seen, "initial first send")
			streamReceive(t, seen, "initial repair")
			headers := sessionHeaders()
			headers.Set("X-Scenario", "probe")
			status, wire, line := sessionExchange(t, h, "/v1/responses", sessionNext(t), headers)
			if status != tc.status || string(wire) != tc.wire {
				t.Fatalf("unmatched response was changed: status=%d body=%s", status, wire)
			}
			sessionAssertLog(t, line, "upstream_attempts=1", "scope_cache=hit", "scope_added_selectors=0",
				"scope_pre_removed_item_ids=3", "scope_pre_removed_reasoning_items=1")
			sessionAssertWindow(t, streamReceive(t, seen, "unmatched error request").body, false, true)
			sessionExchange(t, h, "/v1/responses", sessionNext(t), sessionHeaders())
			sessionAssertWindow(t, streamReceive(t, seen, "request after unmatched error").body, false, true)
			if calls.Load() != 4 {
				t.Fatalf("unmatched response consumed a repair: calls=%d", calls.Load())
			}
		})
	}
}
