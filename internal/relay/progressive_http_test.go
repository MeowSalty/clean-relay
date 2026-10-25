package relay_test

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

func TestProgressiveLaterTransportFailures(t *testing.T) {
	for _, stage := range []int{2, 3} {
		for _, kind := range []string{"connection", "headers_timeout"} {
			t.Run(strconv.Itoa(stage)+"/"+kind, func(t *testing.T) {
				settings := progressiveOptions(nil)
				settings["response_header_timeout"] = "50ms"
				var calls atomic.Int32
				h := streamNewHarness(t, settings, func(w http.ResponseWriter, r *http.Request) {
					_, _ = io.Copy(io.Discard, r.Body)
					attempt := int(calls.Add(1))
					if attempt < stage {
						progressiveMismatch(w, "http_json", attempt)
						return
					}
					if kind == "headers_timeout" {
						<-r.Context().Done()
						return
					}
					connection, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					_ = connection.Close()
				})
				response := retryDo(t, h, http.MethodPost, "/v1/responses", []byte(retryInput), nil)
				body := streamReadAll(t, response)
				status := 502
				if kind == "headers_timeout" {
					status = 504
				}
				if response.StatusCode != status || h.calls.Load() != int64(stage) || !bytes.Contains(body, []byte("clean_relay_error")) || response.Header.Get("X-Attempt-1") != "" {
					t.Fatalf("transport failure fell back or retried: calls=%d status=%d body=%s", h.calls.Load(), response.StatusCode, body)
				}
				line := streamReceive(t, h.logs.lines, "transport failure log")
				if !strings.Contains(line, "retry_stop_reason=upstream_transport_error") || !strings.Contains(line, "retry_decision=retried") || !strings.Contains(line, "upstream_attempts="+strconv.Itoa(stage)) {
					t.Fatal(line)
				}
			})
		}
	}
}

func TestProgressiveSecondHTTPEncodingAndLimits(t *testing.T) {
	plain := []byte(strings.TrimSuffix(retryHTTPError(), "}") + `,"padding":"` + strings.Repeat("x", 2048) + `"}`)
	compressed := retryGzip(t, plain)
	corrupt := bytes.Clone(compressed)
	corrupt[len(corrupt)-1] ^= 0xff
	var rawLarge bytes.Buffer
	writer := gzip.NewWriter(&rawLarge)
	writer.Extra = bytes.Repeat([]byte{1}, 1000)
	if _, err := io.WriteString(writer, retryHTTPError()); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, encoding, stop string
		wire                 []byte
		limit                int
		retry                bool
	}{
		{"plain exact", "", "stages_exhausted", plain, len(plain), true},
		{"plain over", "identity", "inspection_limit", plain, len(plain) - 1, false},
		{"gzip decoded exact", "gzip", "stages_exhausted", compressed, len(plain), true},
		{"gzip decoded over", "gzip", "inspection_limit", compressed, len(plain) - 1, false},
		{"gzip raw exact", "gzip", "stages_exhausted", rawLarge.Bytes(), rawLarge.Len(), true},
		{"gzip raw over", "gzip", "inspection_limit", rawLarge.Bytes(), rawLarge.Len() - 1, false},
		{"gzip checksum", "gzip", "invalid_gzip", corrupt, len(plain), false},
		{"gzip trailing junk", "gzip", "invalid_gzip", append(bytes.Clone(compressed), []byte("junk")...), len(plain), false},
		{"unsupported", "br", "unsupported_encoding", plain, len(plain), false},
		{"combined", "gzip, gzip", "unsupported_encoding", compressed, len(plain), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			h := streamNewHarness(t, progressiveOptions(map[string]any{"max_inspect_bytes": tc.limit}), func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				switch calls.Add(1) {
				case 1:
					progressiveMismatch(w, "http_json", 1)
				case 2:
					w.Header().Set("Content-Type", "application/json")
					w.Header().Set("X-Current", "second")
					if tc.encoding != "" {
						w.Header().Set("Content-Encoding", tc.encoding)
					}
					w.WriteHeader(400)
					_, _ = w.Write(tc.wire)
				case 3:
					w.WriteHeader(201)
					_, _ = io.WriteString(w, "third")
				default:
					t.Error("fourth call")
				}
			})
			response := retryDo(t, h, http.MethodPost, "/v1/responses", []byte(retryInput), nil)
			got := streamReadAll(t, response)
			if tc.retry {
				if response.StatusCode != 201 || string(got) != "third" || h.calls.Load() != 3 {
					t.Fatalf("second error not matched: calls=%d status=%d", h.calls.Load(), response.StatusCode)
				}
			} else if !bytes.Equal(got, tc.wire) || response.StatusCode != 400 || h.calls.Load() != 2 || response.Header.Get("Content-Encoding") != tc.encoding || response.Header.Get("X-Current") != "second" {
				t.Fatal("second encoded response changed")
			}
			if line := streamReceive(t, h.logs.lines, "encoding log"); !strings.Contains(line, "retry_stop_reason="+tc.stop) {
				t.Fatal(line)
			}
		})
	}
}

func TestProgressiveFreshRequestsKeepLegacyCleanup(t *testing.T) {
	body := `{"input":[{"id":"m","role":"user","content":"x","internal_chat_message_metadata_passthrough":true},{"type":"reasoning","encrypted_content":"opaque"}]}`
	var calls atomic.Int32
	h := streamNewHarness(t, progressiveOptions(nil), func(w http.ResponseWriter, r *http.Request) {
		got := retryObserve(t, r)
		attempt := int((calls.Add(1)-1)%3 + 1)
		if bytes.Contains(got.body, []byte("metadata")) || got.header.Get("Content-Encoding") != "" {
			t.Error("legacy cleanup undone")
		}
		if bytes.Contains(got.body, []byte(`"id":"m"`)) != (attempt == 1) || bytes.Contains(got.body, []byte("encrypted_content")) != (attempt < 3) {
			t.Errorf("attempt %d inherited the wrong history: %s", attempt, got.body)
		}
		if attempt < 3 {
			progressiveMismatch(w, "http_json", attempt)
			return
		}
		_, _ = io.WriteString(w, "third")
	})
	for range 2 {
		response := retryDo(t, h, http.MethodPost, "/v1/responses", retryGzip(t, []byte(body)), http.Header{"Content-Encoding": {"gzip"}})
		if got := streamReadAll(t, response); string(got) != "third" {
			t.Fatalf("final body = %s", got)
		}
		line := streamReceive(t, h.logs.lines, "fresh request log")
		for _, field := range []string{"removed_fields=1", "retry_removed_item_ids=1", "retry_removed_reasoning_items=1", "upstream_attempts=3"} {
			if !strings.Contains(line, field) {
				t.Errorf("missing %s: %s", field, line)
			}
		}
	}
	if h.calls.Load() != 6 {
		t.Fatalf("fresh request attempts = %d", h.calls.Load())
	}
}

func TestProgressiveSecondSSEEOFIsFinal(t *testing.T) {
	const prefix = "event: response.created\ndata: {\"response\":{\"output\":[]}}\n\n"
	var calls atomic.Int32
	h := streamNewHarness(t, progressiveOptions(nil), func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if calls.Add(1) == 1 {
			progressiveMismatch(w, "http_json", 1)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, prefix)
	})
	response := retryDo(t, h, http.MethodPost, "/v1/responses", []byte(retryInput), nil)
	if got := streamReadAll(t, response); string(got) != prefix || h.calls.Load() != 2 {
		t.Fatal("EOF lost metadata or caused another call")
	}
	line := streamReceive(t, h.logs.lines, "EOF log")
	if !strings.Contains(line, "retry_stop_reason=inspection_eof") || !strings.Contains(line, "retry_business_failure=true") || !strings.Contains(line, "outcome=completed") {
		t.Fatal(line)
	}
}
