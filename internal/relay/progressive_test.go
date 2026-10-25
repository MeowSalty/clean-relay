package relay_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

const progressiveInput = `{"model":"m","store":true,"previous_response_id":"old","conversation":{"id":"conv"},"reasoning":{"effort":"high"},"n":9007199254740993123456789,"fraction":0.1234567890123456789,"input":[{"id":"m1","role":"user","content":[{"type":"input_text","text":"hello","id":"nested","n":1.234567890123456789e+99}]},{"type":"function_call","id":"f1","call_id":"pair","name":"fn","arguments":"{  \"n\": 123 }"},{"type":"function_call_output","id":"o1","call_id":"pair","output":"ok"},{"type":"reasoning","id":"r1","encrypted_content":"reasoning-opaque"},{"type":"compaction","id":"p1","encrypted_content":"compaction-opaque"},{"type":"item_reference","id":"ref"},{"type":"unknown","id":"keep","extension":{"x":1,"x":2}}]}`

func progressiveOptions(overrides map[string]any) map[string]any {
	options := map[string]any{"cleanup_mode": "portable_ids_then_reasoning", "inspect_timeout": "1s"}
	for key, value := range overrides {
		options[key] = value
	}
	return retryOptions(options)
}

func progressiveMismatch(w http.ResponseWriter, source string, attempt int) {
	w.Header().Set("X-Attempt-"+strconv.Itoa(attempt), "present")
	if source == "http_json" {
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(400)
		_, _ = io.WriteString(w, strings.ReplaceAll(retryHTTPError(), "synthetic-resource-id", "synthetic-resource-"+strconv.Itoa(attempt)))
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	// A fresh BOM and metadata prefix on each call exercise independent parsers.
	_, _ = io.WriteString(w, "\xef\xbb\xbf: heartbeat\r\nevent: response.created\r\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"synthetic-response-"+strconv.Itoa(attempt)+"\",\"output\":[]}}\r\n\r\n")
	if attempt == 1 {
		_, _ = io.WriteString(w, retrySSEError())
	} else {
		_, _ = io.WriteString(w, "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"message\":"+strconv.Quote(strings.ReplaceAll(retryMessage, "synthetic-resource-id", "synthetic-other-resource"))+"}}}\n\n")
	}
	w.(http.Flusher).Flush()
}

func progressiveCheckBody(t *testing.T, body []byte, attempt int) {
	t.Helper()
	var original, actual map[string]json.RawMessage
	if err := json.Unmarshal([]byte(progressiveInput), &original); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(body, &actual); err != nil {
		t.Fatal(err)
	}
	var items []map[string]json.RawMessage
	if err := json.Unmarshal(original["input"], &items); err != nil {
		t.Fatal(err)
	}
	if attempt > 1 {
		for i := 0; i < 3; i++ {
			delete(items[i], "id")
		}
	}
	if attempt == 3 {
		items = append(items[:3], items[4:]...)
	}
	var gotItems []map[string]json.RawMessage
	if err := json.Unmarshal(actual["input"], &gotItems); err != nil {
		t.Fatal(err)
	}
	delete(original, "input")
	delete(actual, "input")
	if !reflect.DeepEqual(items, gotItems) || !reflect.DeepEqual(original, actual) {
		t.Fatalf("attempt %d changed protected content: %s", attempt, body)
	}
}

func TestProgressiveBodiesAndMixedTriggers(t *testing.T) {
	for _, route := range []struct{ first, second, path string }{
		{"http_json", "http_json", "/responses/"},
		{"http_json", "sse", "/responses"},
		{"sse", "http_json", "/responses"},
		{"sse", "sse", "/responses"},
		{"http_json", "http_json", "/responses/compact/"},
	} {
		for _, compressed := range []bool{false, true} {
			for _, finalError := range []bool{false, true} {
				t.Run(route.first+"/"+route.second+route.path+"/gzip="+strconv.FormatBool(compressed)+"/final_error="+strconv.FormatBool(finalError), func(t *testing.T) {
					observed := make(chan retryObserved, 4)
					discarded := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
					var calls atomic.Int32
					finalBody, finalStatus := `{"reply":"third"}`, 201
					if finalError {
						finalBody, finalStatus = retryHTTPError(), 400
					}
					h := streamNewHarness(t, progressiveOptions(nil), func(w http.ResponseWriter, r *http.Request) {
						observed <- retryObserve(t, r)
						attempt := int(calls.Add(1))
						if attempt <= 2 {
							source := []string{route.first, route.second}[attempt-1]
							progressiveMismatch(w, source, attempt)
							if source == "sse" {
								<-r.Context().Done()
							}
							close(discarded[attempt-1])
							return
						}
						if attempt > 3 {
							t.Error("fourth upstream call")
						}
						w.Header().Set("Content-Type", "application/json")
						w.Header().Set("X-Final-Only", "third")
						w.Header().Set("Trailer", "X-Final-Trailer")
						w.WriteHeader(finalStatus)
						_, _ = io.WriteString(w, finalBody)
						w.Header().Set("X-Final-Trailer", "preserved")
					})
					wire := []byte(progressiveInput)
					headers := http.Header{"Authorization": {"Bearer synthetic-progressive-key"}, "X-Custom": {"kept"}, "Accept-Encoding": {"gzip"},
						"Content-Md5": {"old"}, "Digest": {"old"}, "Content-Digest": {"old"}, "Repr-Digest": {"old"}, "Expect": {"100-continue"}}
					if compressed {
						wire = retryGzip(t, wire)
						headers.Set("Content-Encoding", "gzip")
					}
					path := "/api/%61xon/v1" + route.path + "?x=1&x=2&q=%2F&key=synthetic-progressive-query"
					response := retryDo(t, h, http.MethodPost, path, wire, headers)
					if got := streamReadAll(t, response); string(got) != finalBody || response.StatusCode != finalStatus || h.calls.Load() != 3 {
						t.Fatalf("final response = %d %q, calls = %d", response.StatusCode, got, h.calls.Load())
					}
					if response.Header.Get("X-Attempt-1") != "" || response.Header.Get("X-Attempt-2") != "" || response.Header.Get("X-Final-Only") != "third" || response.Trailer.Get("X-Final-Trailer") != "preserved" {
						t.Fatal("responses or trailers mixed")
					}
					for i := 1; i <= 3; i++ {
						got := streamReceive(t, observed, "actual request")
						if got.length != int64(len(got.body)) || got.rawPath != "/api/%61xon/v1"+route.path || got.query != "x=1&x=2&q=%2F&key=synthetic-progressive-query" || got.header.Get("Authorization") != headers.Get("Authorization") || got.header.Get("X-Custom") != "kept" || got.header.Get("Accept-Encoding") != "gzip" {
							t.Fatalf("attempt %d changed request properties", i)
						}
						if i == 1 {
							if !bytes.Equal(got.body, wire) || got.header.Get("Digest") != "old" {
								t.Fatal("first request changed")
							}
							continue
						}
						progressiveCheckBody(t, got.body, i)
						for _, key := range []string{"Content-Encoding", "Transfer-Encoding", "Expect", "Content-MD5", "Digest", "Content-Digest", "Repr-Digest", "Idempotency-Key", "X-Idempotency-Key"} {
							if got.header.Get(key) != "" {
								t.Errorf("attempt %d retained %s", i, key)
							}
						}
					}
					for _, done := range discarded {
						streamReceive(t, done, "discarded response closure")
					}
					line := streamReceive(t, h.logs.lines, "final progressive log")
					firstStatus, secondStatus := "400", "400"
					if route.first == "sse" {
						firstStatus = "200"
					}
					if route.second == "sse" {
						secondStatus = "200"
					}
					for _, field := range []string{"upstream_attempts=3", "retry_cleanup_mode=portable_ids_then_reasoning", "retry_decision=retried", "retry_stop_reason=stages_exhausted",
						"first_upstream_status=" + firstStatus, "retry_error_source=" + route.first, "retry_removed_item_ids=3", "retry_removed_reasoning_items=1",
						"retry_attempt_2.cleanup_mode=portable_ids", "retry_attempt_2.trigger_status=" + firstStatus, "retry_attempt_2.error_source=" + route.first,
						"retry_attempt_2.removed_item_ids=3", "retry_attempt_2.removed_reasoning_items=0", "retry_attempt_3.cleanup_mode=portable_ids_and_reasoning",
						"retry_attempt_3.trigger_status=" + secondStatus, "retry_attempt_3.error_source=" + route.second, "retry_attempt_3.removed_item_ids=3", "retry_attempt_3.removed_reasoning_items=1",
						"retry_business_failure=true", "outcome=completed"} {
						if !strings.Contains(line, field) {
							t.Errorf("missing %s: %s", field, line)
						}
					}
					for _, secret := range []string{"synthetic-resource", "synthetic-other-resource", "synthetic-response", "synthetic-progressive-key", "synthetic-progressive-query", "reasoning-opaque", "compaction-opaque"} {
						if strings.Contains(line, secret) {
							t.Errorf("log leaked %s", secret)
						}
					}
					select {
					case extra := <-h.logs.lines:
						t.Fatalf("extra request log: %s", extra)
					default:
					}
				})
			}
		}
	}
}

func TestProgressiveCandidateCombinations(t *testing.T) {
	for _, tc := range []struct {
		name, body, mode, stop string
		calls                  int64
		drop                   []string
	}{
		{"IDs only", `{"input":[{"id":"m","role":"user","content":"x"}]}`, "portable_ids", "stages_exhausted", 2, nil},
		{"reasoning without ID", `{"input":[{"type":"reasoning"},{"role":"user","content":"x"}]}`, "portable_ids_and_reasoning", "stages_exhausted", 2, nil},
		{"empty strong copy", `{"input":[{"type":"reasoning"}]}`, "", "empty_input", 1, nil},
		{"none", `{"input":[{"role":"user","content":"x"}]}`, "", "", 1, nil},
		{"compaction", `{"input":[{"type":"compaction","id":"p"}]}`, "", "", 1, nil},
		{"references", `{"input":[{"type":"item_reference","id":"ref"}]}`, "", "", 1, nil},
		{"incomplete", `{"input":[{"id":"m"},{"id":"f","type":"function_call","call_id":"c"}]}`, "", "", 1, nil},
		{"legacy removed all IDs", `{"input":[{"id":"m","role":"user","content":"x"}]}`, "", "", 1, []string{"id"}},
		{"legacy IDs leave reasoning", `{"input":[{"id":"m","role":"user","content":"x"},{"type":"reasoning","id":"r"}]}`, "portable_ids_and_reasoning", "stages_exhausted", 2, []string{"id"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			settings := progressiveOptions(nil)
			if tc.drop != nil {
				settings["drop_input_fields"] = tc.drop
			}
			h := streamNewHarness(t, settings, func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				if tc.drop != nil && bytes.Contains(body, []byte(`"id"`)) {
					t.Error("legacy ID removal was undone")
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(400)
				_, _ = io.WriteString(w, retryHTTPError())
			})
			response := retryDo(t, h, http.MethodPost, "/v1/responses", []byte(tc.body), nil)
			if got := streamReadAll(t, response); string(got) != retryHTTPError() || response.StatusCode != 400 || h.calls.Load() != tc.calls {
				t.Fatalf("candidate result: calls=%d status=%d body=%s", h.calls.Load(), response.StatusCode, got)
			}
			line := streamReceive(t, h.logs.lines, "candidate log")
			if tc.stop == "" {
				if !strings.Contains(line, "retry_decision=no_candidate") || strings.Contains(line, "retry_stop_reason") {
					t.Fatal(line)
				}
			} else if !strings.Contains(line, "retry_stop_reason="+tc.stop) {
				t.Fatal(line)
			}
			if tc.calls == 2 {
				if !strings.Contains(line, "retry_attempt_2.cleanup_mode="+tc.mode) {
					t.Fatal(line)
				}
			} else if strings.Contains(line, "retry_attempt_2") {
				t.Fatal("generated copy logged as actually sent")
			}
			if strings.Contains(line, "retry_attempt_3") {
				t.Fatal("unused strong stage consumed a call")
			}
		})
	}
}

func TestProgressiveHTTPStopsAtCurrentResponse(t *testing.T) {
	for _, stage := range []int{1, 2} {
		for _, tc := range []struct {
			name, body, stop string
			status           int
			businessFailure  bool
		}{
			{"success", `{"ok":true}`, "response_not_inspected", 200, false},
			{"ordinary", `{"error":{"type":"invalid_request_error","message":"ordinary"}}`, "no_match", 400, true},
			{"authentication", retryHTTPError(), "response_not_inspected", 401, false},
			{"forbidden", retryHTTPError(), "response_not_inspected", 403, false},
			{"rate limit", retryHTTPError(), "response_not_inspected", 429, false},
			{"server", retryHTTPError(), "response_not_inspected", 500, false},
			{"encrypted content", strings.Replace(retryHTTPError(), `"type":`, `"code":"invalid_encrypted_content","type":`, 1), "no_match", 400, true},
			{"unknown code", strings.Replace(retryHTTPError(), `"type":`, `"code":"future","type":`, 1), "no_match", 400, true},
			{"duplicate envelope", strings.Replace(retryHTTPError(), `{"error":`, `{"error":{},"error":`, 1), "no_match", 400, false},
			{"incomplete", strings.TrimSuffix(retryHTTPError(), "}"), "no_match", 400, false},
			{"wrong location", `{"detail":` + retryHTTPError() + "}", "no_match", 400, false},
		} {
			t.Run(strconv.Itoa(stage)+"/"+tc.name, func(t *testing.T) {
				var calls atomic.Int32
				h := streamNewHarness(t, progressiveOptions(nil), func(w http.ResponseWriter, r *http.Request) {
					_, _ = io.Copy(io.Discard, r.Body)
					attempt := int(calls.Add(1))
					if attempt < stage {
						progressiveMismatch(w, "http_json", attempt)
						return
					}
					if attempt > stage {
						t.Error("advanced after a nonmatching response")
					}
					w.Header().Set("Content-Type", "application/json")
					w.Header().Set("X-Current", "preserved")
					w.Header().Set("Trailer", "X-Current-Trailer")
					w.WriteHeader(tc.status)
					_, _ = io.WriteString(w, tc.body)
					w.Header().Set("X-Current-Trailer", "preserved")
				})
				response := retryDo(t, h, http.MethodPost, "/v1/responses", []byte(retryInput), nil)
				if got := streamReadAll(t, response); response.StatusCode != tc.status || string(got) != tc.body || h.calls.Load() != int64(stage) || response.Header.Get("X-Current") != "preserved" || response.Trailer.Get("X-Current-Trailer") != "preserved" || response.Header.Get("X-Attempt-1") != "" {
					t.Fatalf("current response changed: %d %s, calls=%d", response.StatusCode, got, h.calls.Load())
				}
				line := streamReceive(t, h.logs.lines, "stop log")
				outcome := "completed"
				if tc.businessFailure {
					outcome = "upstream_business_error"
				}
				if !strings.Contains(line, "retry_stop_reason="+tc.stop) || !strings.Contains(line, "outcome="+outcome) || strings.Contains(line, "retry_attempt_3") {
					t.Fatal(line)
				}
				if stage == 2 && (!strings.Contains(line, "retry_decision=retried") || !strings.Contains(line, "retry_business_failure=true") || !strings.Contains(line, "retry_removed_reasoning_items=0")) {
					t.Fatal(line)
				}
			})
		}
	}
}
