package relay_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const retryMessage = "Item 'synthetic-resource-id' was created under a different Azure OpenAI resource. Please use the same resource."
const retryInput = `{"model":"m","store":true,"previous_response_id":"old","n":9007199254740993123456789,"input":[{"id":"m1","role":"user","content":"hello"},{"type":"function_call","id":"f1","call_id":"pair","name":"fn","arguments":"{  \"n\": 123 }"},{"type":"function_call_output","id":"o1","call_id":"pair","output":"ok"},{"type":"reasoning","id":"r1","encrypted_content":"reasoning-opaque"},{"type":"compaction","id":"p1","encrypted_content":"compaction-opaque"}]}`

func retryHTTPError() string {
	return `{"error":{"type":"invalid_request_error","message":` + strconv.Quote(retryMessage) + `}}`
}
func retrySSEError() string {
	return "event: error\ndata: {\"type\":\"error\",\"message\":" + strconv.Quote(retryMessage) + "}\n\n"
}

func retryOptions(overrides map[string]any) map[string]any {
	options := map[string]any{"enabled": true, "inspect_timeout": "100ms"}
	for key, value := range overrides {
		options[key] = value
	}
	return map[string]any{"azure_resource_retry": options}
}

func retryRequest(t *testing.T, h *streamHarness, ctx context.Context, method, path string, body []byte, headers http.Header) *http.Request {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, method, h.server.URL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header = headers.Clone()
	if request.Header == nil {
		request.Header = make(http.Header)
	}
	if request.Header.Get("Content-Type") == "" {
		request.Header.Set("Content-Type", "application/json")
	}
	return request
}

func retryDo(t *testing.T, h *streamHarness, method, path string, body []byte, headers http.Header) *http.Response {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), streamDeadline)
	t.Cleanup(cancel)
	response, err := h.client.Do(retryRequest(t, h, ctx, method, path, body, headers))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = response.Body.Close() })
	return response
}

func retryGzip(t *testing.T, body []byte) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	if _, err := writer.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

type retryObserved struct {
	body                 []byte
	header               http.Header
	path, rawPath, query string
	length               int64
}

func retryObserve(t *testing.T, r *http.Request) retryObserved {
	t.Helper()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		t.Error(err)
	}
	return retryObserved{body, r.Header.Clone(), r.URL.Path, r.URL.RawPath, r.URL.RawQuery, r.ContentLength}
}

func TestConditionalHTTPFirstBodyAndRepair(t *testing.T) {
	for _, mode := range []string{"portable_ids", "portable_ids_and_reasoning"} {
		for _, compressed := range []bool{false, true} {
			for _, endpoint := range []string{"/responses", "/responses/compact/"} {
				t.Run(mode+endpoint+strconv.FormatBool(compressed), func(t *testing.T) {
					observed := make(chan retryObserved, 2)
					var calls atomic.Int32
					h := streamNewHarness(t, retryOptions(map[string]any{"cleanup_mode": mode}), func(w http.ResponseWriter, r *http.Request) {
						observed <- retryObserve(t, r)
						if calls.Add(1) == 1 {
							w.Header().Set("Content-Type", "application/problem+json")
							w.Header().Set("X-First-Only", "first")
							w.WriteHeader(http.StatusBadRequest)
							_, _ = io.WriteString(w, retryHTTPError())
							return
						}
						w.Header().Set("Content-Type", "application/json")
						w.Header().Set("X-Second-Only", "second")
						w.WriteHeader(http.StatusCreated)
						_, _ = io.WriteString(w, `{"reply":"second"}`)
					})
					wire := []byte(retryInput)
					headers := http.Header{"Authorization": {"Bearer synthetic-retry-key"}, "Accept-Encoding": {"gzip"}, "Content-Md5": {"old-md5"}, "Digest": {"old-digest"}, "Content-Digest": {"old-content-digest"}, "Repr-Digest": {"old-repr-digest"}, "X-Custom": {"kept"}}
					if compressed {
						wire = retryGzip(t, wire)
						headers.Set("Content-Encoding", "gzip")
					}
					path := "/api/%61xon/v1" + endpoint + "?x=1&x=2&q=%2F&key=synthetic-query-secret"
					response := retryDo(t, h, http.MethodPost, path, wire, headers)
					if got := streamReadAll(t, response); response.StatusCode != http.StatusCreated || string(got) != `{"reply":"second"}` || response.Header.Get("X-First-Only") != "" || response.Header.Get("X-Second-Only") != "second" {
						t.Fatalf("mixed response: %d %s", response.StatusCode, got)
					}
					first := streamReceive(t, observed, "first request")
					second := streamReceive(t, observed, "second request")
					if !bytes.Equal(first.body, wire) {
						t.Fatal("first request bytes changed")
					}
					for _, got := range []retryObserved{first, second} {
						if got.length != int64(len(got.body)) || got.header.Get("Authorization") != headers.Get("Authorization") || got.header.Get("Accept-Encoding") != "gzip" || got.header.Get("X-Custom") != "kept" || got.rawPath != "/api/%61xon/v1"+endpoint || got.query != "x=1&x=2&q=%2F&key=synthetic-query-secret" {
							t.Fatalf("request properties changed: %+v", got)
						}
					}
					for _, key := range []string{"Content-Encoding", "Content-MD5", "Digest", "Content-Digest", "Repr-Digest"} {
						if second.header.Get(key) != "" {
							t.Errorf("second retained %s", key)
						}
					}
					if first.header.Get("Content-MD5") != "old-md5" || first.header.Get("Digest") != "old-digest" {
						t.Fatal("first digest removed")
					}
					var before, after map[string]json.RawMessage
					_ = json.Unmarshal([]byte(retryInput), &before)
					if err := json.Unmarshal(second.body, &after); err != nil {
						t.Fatal(err)
					}
					for key, value := range before {
						if key != "input" && !bytes.Equal(value, after[key]) {
							t.Errorf("root %s changed", key)
						}
					}
					var expected, actual []map[string]json.RawMessage
					_ = json.Unmarshal(before["input"], &expected)
					_ = json.Unmarshal(after["input"], &actual)
					for i := 0; i < 3; i++ {
						delete(expected[i], "id")
					}
					reasoning := "0"
					if mode == "portable_ids_and_reasoning" {
						expected = append(expected[:3], expected[4:]...)
						reasoning = "1"
					}
					if !reflect.DeepEqual(actual, expected) {
						t.Fatalf("wrong repair: %s", second.body)
					}
					line := streamReceive(t, h.logs.lines, "request log")
					for _, field := range []string{"upstream_attempts=2", "retry_decision=retried", "retry_removed_item_ids=3", "retry_removed_reasoning_items=" + reasoning, "first_upstream_status=400", "retry_rule=azure_resource_mismatch", "removed_fields=0"} {
						if !strings.Contains(line, field) {
							t.Errorf("missing %s: %s", field, line)
						}
					}
					for _, secret := range []string{"synthetic-resource-id", "synthetic-retry-key", "synthetic-query-secret", "reasoning-opaque", "compaction-opaque"} {
						if strings.Contains(h.logs.text(), secret) {
							t.Errorf("log contains %s", secret)
						}
					}
					if h.calls.Load() != 2 {
						t.Fatalf("attempts=%d", h.calls.Load())
					}
				})
			}
		}
	}
}

func TestConditionalHTTPBypass(t *testing.T) {
	cases := []struct {
		name, method, path, body string
		headers                  http.Header
		settings                 map[string]any
		status                   int
		response, contentType    string
	}{
		{name: "omitted config", settings: map[string]any{}},
		{name: "disabled", settings: retryOptions(map[string]any{"enabled": false})},
		{name: "GET", method: http.MethodGet},
		{name: "response retrieval", path: "/v1/responses/id"},
		{name: "case sensitive", path: "/v1/Responses"},
		{name: "extra slash", path: "/v1/responses//"},
		{name: "other endpoint", path: "/v1/chat/completions"},
		{name: "idempotency", headers: http.Header{"Idempotency-Key": {"value"}}},
		{name: "empty idempotency", headers: http.Header{"Idempotency-Key": {""}}},
		{name: "x idempotency", headers: http.Header{"X-Idempotency-Key": {"value"}}},
		{name: "stream resume", headers: http.Header{"Last-Event-Id": {"value"}}},
		{name: "background", body: `{"background":true,"input":[{"id":"m","role":"user","content":"x"}]}`},
		{name: "input string", body: `{"input":"x"}`},
		{name: "id only", body: `{"input":[{"id":"m"}]}`},
		{name: "opaque only", body: `{"input":[{"type":"compaction","id":"p"},{"type":"item_reference","id":"r"}]}`},
		{name: "default reasoning", body: `{"input":[{"type":"reasoning","id":"r"}]}`},
		{name: "empty repaired input", body: `{"input":[{"type":"reasoning","id":"r"}]}`, settings: retryOptions(map[string]any{"cleanup_mode": "portable_ids_and_reasoning"})},
		{name: "first success", status: 200, response: `{"ok":true}`},
		{name: "authentication", status: 401}, {name: "forbidden", status: 403}, {name: "quota", status: 429}, {name: "server", status: 500},
		{name: "ordinary 400", response: `{"error":{"type":"invalid_request_error","message":"Azure ordinary error"}}`},
		{name: "wrong code", response: `{"error":{"type":"invalid_request_error","code":"invalid_encrypted_content","message":` + strconv.Quote(retryMessage) + `}}`},
		{name: "quoted error", response: strconv.Quote(retryHTTPError())},
		{name: "unknown wrapper", response: `{"detail":` + retryHTTPError() + `}`},
		{name: "duplicate keys", response: `{"error":{},"error":{"type":"invalid_request_error","message":` + strconv.Quote(retryMessage) + `}}`},
		{name: "multiple root", response: retryHTTPError() + ` {}`},
		{name: "HTML", contentType: "text/html"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			settings := tc.settings
			if settings == nil {
				settings = retryOptions(nil)
			}
			status, responseBody, contentType := tc.status, tc.response, tc.contentType
			if status == 0 {
				status = http.StatusBadRequest
			}
			if responseBody == "" {
				responseBody = retryHTTPError()
			}
			if contentType == "" {
				contentType = "application/json"
			}
			h := streamNewHarness(t, settings, func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", contentType)
				w.Header().Set("X-Original", "preserved")
				w.WriteHeader(status)
				_, _ = io.WriteString(w, responseBody)
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
			response := retryDo(t, h, method, path, []byte(body), tc.headers)
			got := streamReadAll(t, response)
			if response.StatusCode != status || string(got) != responseBody || response.Header.Get("X-Original") != "preserved" || h.calls.Load() != 1 {
				t.Fatalf("bypass changed response: %d %s calls=%d", response.StatusCode, got, h.calls.Load())
			}
		})
	}
}

func TestConditionalHTTPLegacyRulesAndFreshRequests(t *testing.T) {
	t.Run("metadata and retry counts are independent", func(t *testing.T) {
		observed := make(chan retryObserved, 2)
		var calls atomic.Int32
		h := streamNewHarness(t, retryOptions(nil), func(w http.ResponseWriter, r *http.Request) {
			observed <- retryObserve(t, r)
			w.Header().Set("Content-Type", "application/json")
			if calls.Add(1) == 1 {
				w.WriteHeader(400)
				_, _ = io.WriteString(w, retryHTTPError())
				return
			}
			_, _ = io.WriteString(w, `{"ok":true}`)
		})
		wire := retryGzip(t, []byte(`{"input":[{"id":"m","role":"user","content":"x","internal_chat_message_metadata_passthrough":true}]}`))
		streamReadAll(t, retryDo(t, h, http.MethodPost, "/v1/responses", wire, http.Header{"Content-Encoding": {"gzip"}}))
		first, second := streamReceive(t, observed, "metadata-cleaned first request"), streamReceive(t, observed, "repaired second request")
		if first.header.Get("Content-Encoding") != "" || !bytes.Contains(first.body, []byte(`"id":"m"`)) || bytes.Contains(first.body, []byte("metadata")) || bytes.Contains(second.body, []byte(`"id"`)) || bytes.Contains(second.body, []byte("metadata")) {
			t.Fatalf("wrong cleanup ordering: %s / %s", first.body, second.body)
		}
		line := streamReceive(t, h.logs.lines, "independent counts")
		if !strings.Contains(line, "removed_fields=1") || !strings.Contains(line, "retry_removed_item_ids=1") || !strings.Contains(line, "upstream_attempts=2") {
			t.Fatal(line)
		}
	})
	t.Run("legacy rules keep their meaning", func(t *testing.T) {
		settings := retryOptions(nil)
		settings["drop_input_fields"] = []string{"id", "internal_chat_message_metadata_passthrough"}
		observed := make(chan retryObserved, 1)
		h := streamNewHarness(t, settings, func(w http.ResponseWriter, r *http.Request) {
			observed <- retryObserve(t, r)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(400)
			_, _ = io.WriteString(w, retryHTTPError())
		})
		streamReadAll(t, retryDo(t, h, http.MethodPost, "/v1/responses", []byte(`{"input":[{"id":"m","role":"user","content":"x","internal_chat_message_metadata_passthrough":true}]}`), nil))
		got := streamReceive(t, observed, "legacy cleaned request")
		if bytes.Contains(got.body, []byte(`"id"`)) || bytes.Contains(got.body, []byte("metadata")) || h.calls.Load() != 1 {
			t.Fatalf("legacy rules: %s", got.body)
		}
		line := streamReceive(t, h.logs.lines, "legacy log")
		if !strings.Contains(line, "removed_fields=2") || !strings.Contains(line, "retry_decision=no_candidate") {
			t.Fatal(line)
		}
	})
	t.Run("new request starts with original history", func(t *testing.T) {
		var calls atomic.Int32
		h := streamNewHarness(t, retryOptions(nil), func(w http.ResponseWriter, r *http.Request) {
			got := retryObserve(t, r)
			w.Header().Set("Content-Type", "application/json")
			if calls.Add(1)%2 == 1 {
				if string(got.body) != retryInput {
					t.Error("new request inherited repaired history")
				}
				w.WriteHeader(400)
				_, _ = io.WriteString(w, retryHTTPError())
				return
			}
			_, _ = io.WriteString(w, `{"ok":true}`)
		})
		for range 2 {
			streamReadAll(t, retryDo(t, h, http.MethodPost, "/v1/responses", []byte(retryInput), nil))
		}
		if h.calls.Load() != 4 {
			t.Fatalf("attempts=%d", h.calls.Load())
		}
	})
}

func TestConditionalHTTPResponseEncodingAndLimits(t *testing.T) {
	plain := []byte(strings.TrimSuffix(retryHTTPError(), "}") + `,"padding":"` + strings.Repeat("x", 2000) + `"}`)
	compressed := retryGzip(t, plain)
	corrupt := bytes.Clone(compressed)
	corrupt[len(corrupt)-1] ^= 0xff
	cases := []struct {
		name     string
		wire     []byte
		encoding string
		limit    int
		retry    bool
	}{
		{"plain exactly", plain, "", len(plain), true},
		{"plain plus one", plain, "identity", len(plain) - 1, false},
		{"gzip complete", compressed, "gzip", len(plain), true},
		{"gzip raw limit", compressed, "gzip", len(compressed) - 1, false},
		{"gzip decompressed limit", compressed, "gzip", len(plain) - 1, false},
		{"gzip checksum", corrupt, "gzip", len(plain), false},
		{"gzip trailing junk", append(bytes.Clone(compressed), []byte("junk")...), "gzip", len(plain), false},
		{"unsupported", plain, "br", len(plain), false},
		{"combined", compressed, "gzip, gzip", len(plain), false},
		{"incomplete JSON", plain[:len(plain)-1], "", len(plain), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			h := streamNewHarness(t, retryOptions(map[string]any{"max_inspect_bytes": tc.limit}), func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "application/json")
				if calls.Add(1) == 1 {
					if tc.encoding != "" {
						w.Header().Set("Content-Encoding", tc.encoding)
					}
					w.WriteHeader(400)
					_, _ = w.Write(tc.wire)
					return
				}
				_, _ = io.WriteString(w, `{"second":true}`)
			})
			response := retryDo(t, h, http.MethodPost, "/v1/responses", []byte(retryInput), nil)
			got := streamReadAll(t, response)
			if tc.retry {
				if response.StatusCode != 200 || string(got) != `{"second":true}` || h.calls.Load() != 2 {
					t.Fatalf("no retry: %d %s", response.StatusCode, got)
				}
			} else if response.StatusCode != 400 || !bytes.Equal(got, tc.wire) || response.Header.Get("Content-Encoding") != tc.encoding || h.calls.Load() != 1 {
				t.Fatalf("wire changed: %d length=%d calls=%d", response.StatusCode, len(got), h.calls.Load())
			}
		})
	}
}

func TestConditionalHTTPSecondFailure(t *testing.T) {
	for _, kind := range []string{"same_error", "connection", "headers_timeout"} {
		t.Run(kind, func(t *testing.T) {
			var calls atomic.Int32
			settings := retryOptions(nil)
			settings["response_header_timeout"] = "50ms"
			h := streamNewHarness(t, settings, func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				if calls.Add(1) == 1 || kind == "same_error" {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(400)
					_, _ = io.WriteString(w, retryHTTPError())
					return
				}
				if kind == "headers_timeout" {
					<-r.Context().Done()
					return
				}
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				_ = conn.Close()
			})
			response := retryDo(t, h, http.MethodPost, "/v1/responses", []byte(retryInput), nil)
			got := streamReadAll(t, response)
			want := map[string]int{"same_error": 400, "connection": 502, "headers_timeout": 504}[kind]
			if response.StatusCode != want || h.calls.Load() != 2 {
				t.Fatalf("status=%d attempts=%d body=%s", response.StatusCode, h.calls.Load(), got)
			}
			if kind == "same_error" && string(got) != retryHTTPError() {
				t.Fatal("second error changed")
			}
		})
	}
}

func TestConditionalHTTPTimeoutAndTrailers(t *testing.T) {
	release := make(chan struct{})
	first := retryHTTPError()[:20]
	h := streamNewHarness(t, retryOptions(map[string]any{"inspect_timeout": "30ms"}), func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Trailer", "X-Final")
		w.WriteHeader(400)
		_, _ = io.WriteString(w, first)
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, retryHTTPError()[len(first):])
		w.Header().Set("X-Final", "preserved")
		w.Header().Set(http.TrailerPrefix+"X-Late", "also-preserved")
	})
	started := time.Now()
	response := retryDo(t, h, http.MethodPost, "/v1/responses", []byte(retryInput), nil)
	if time.Since(started) > 2*time.Second {
		t.Fatal("header handoff blocked")
	}
	streamReadSegment(t, response, first)
	close(release)
	if tail := streamReadAll(t, response); string(tail) != retryHTTPError()[len(first):] || response.StatusCode != 400 || h.calls.Load() != 1 {
		t.Fatal("late error was retried or lost")
	}
	if response.Trailer.Get("X-Final") != "preserved" || response.Trailer.Get("X-Late") != "also-preserved" {
		t.Fatalf("trailers=%v", response.Trailer)
	}
}
