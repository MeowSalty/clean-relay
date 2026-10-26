package relay

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

const observedCompleted = "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"error\":null}}\n\n"

func TestCompletionObservation(t *testing.T) {
	for _, tc := range []struct {
		name, media, wire  string
		completed, failure bool
	}{
		{"json", "application/json", `{"status":"completed","output":[],"error":null}`, true, false},
		{"json transport success", "application/json", `{"ok":true}`, false, false},
		{"json missing result", "application/json", `{"status":"completed"}`, false, false},
		{"json unsupported result", "application/json", `{"status":"completed","output":{} }`, false, false},
		{"json different object", "application/json", `{"status":"completed","output":[],"object":"other"}`, false, false},
		{"json failed", "application/json", `{"status":"failed","error":{"message":"synthetic-failure"}}`, false, true},
		{"json incomplete", "application/json", `{"status":"completed","incomplete_details":{"reason":"limit"}}`, false, true},
		{"json duplicate", "application/json", `{"status":"failed","\u0073tatus":"completed"}`, false, false},
		{"json trailing", "application/json", `{"status":"completed"}{}`, false, false},
		{"json invalid UTF8", "application/json", "{\"status\":\"completed\",\"extra\":\"\xff\"}", false, false},
		{"sse", "text/event-stream", observedCompleted, true, false},
		{"sse BOM and CRLF", "text/event-stream", "\xef\xbb\xbf" + strings.ReplaceAll(observedCompleted, "\n", "\r\n"), true, false},
		{"sse data type only", "text/event-stream", "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n", true, false},
		{"sse event type only", "text/event-stream", "event: response.completed\ndata: {\"response\":{\"status\":\"completed\"}}\n\n", true, false},
		{"sse DONE", "text/event-stream", "data: [DONE]\n\n", false, false},
		{"sse DONE after completion", "text/event-stream", observedCompleted + "data: [DONE]\n\n", true, false},
		{"sse status missing", "text/event-stream", "event: response.completed\ndata: {}\n\n", false, false},
		{"sse status mismatch", "text/event-stream", strings.ReplaceAll(observedCompleted, "\"status\":\"completed\"", "\"status\":\"in_progress\""), false, false},
		{"sse event mismatch", "text/event-stream", strings.Replace(observedCompleted, "event: response.completed", "event: response.failed", 1), false, false},
		{"sse nested duplicate", "text/event-stream", "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"failed\",\"status\":\"completed\"}}\n\n", false, false},
		{"sse failure", "text/event-stream", "data: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"message\":\"synthetic\"}}}\n\n", false, true},
		{"sse trailing failure", "text/event-stream", observedCompleted + "data: {\"type\":\"error\",\"message\":\"synthetic\"}\n\n", false, true},
		{"sse missing delimiter", "text/event-stream", strings.TrimSuffix(observedCompleted, "\n"), false, false},
		{"sse trailing partial", "text/event-stream", observedCompleted + "data: {", false, false},
		{"sse two responses", "text/event-stream", observedCompleted + observedCompleted, false, false},
		{"unsupported media", "text/plain", observedCompleted, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, chunkSize := range []int{1, 7, 4096} {
				o := newCompletionObserver(&http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {tc.media}}})
				for start := 0; start < len(tc.wire); start += chunkSize {
					o.feed([]byte(tc.wire[start:min(start+chunkSize, len(tc.wire))]))
				}
				o.end(io.EOF)
				if o.completed() != tc.completed || o.businessFailure != tc.failure {
					t.Fatalf("chunk=%d completed=%v failure=%v reason=%s", chunkSize, o.completed(), o.businessFailure, o.reason)
				}
			}
		})
	}
}

func TestCompletionLimitsArePerEvent(t *testing.T) {
	o := newCompletionObserver(&http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}})
	delta := []byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"" + strings.Repeat("x", 1000) + "\"}\n\n")
	for i := 0; i < 2048; i++ {
		o.feed(delta)
		if len(o.data)+len(o.line) > 2*len(delta) || o.invalid {
			t.Fatal("observer accumulated the entire stream")
		}
	}
	o.feed([]byte(observedCompleted))
	o.end(io.EOF)
	if !o.completed() {
		t.Fatalf("long stream with bounded events was not observed: %s", o.reason)
	}
	for _, media := range []string{"application/json", "text/event-stream"} {
		for _, size := range []int{maxCompletionBytes, maxCompletionBytes + 1} {
			t.Run(media+"/"+string(rune(size-maxCompletionBytes+'0')), func(t *testing.T) {
				o := newCompletionObserver(&http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {media}}})
				prefix, suffix := `{"status":"completed","output":[],"extra":"`, `"}`
				if media == "text/event-stream" {
					prefix, suffix = "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"},\"extra\":\"", "\"}\n\n"
				}
				wire := prefix + strings.Repeat("x", size-len(prefix)-len(suffix)) + suffix
				for start := 0; start < len(wire); start += 4096 {
					o.feed([]byte(wire[start:min(start+4096, len(wire))]))
				}
				o.end(io.EOF)
				if o.completed() != (size == maxCompletionBytes) {
					t.Fatalf("size %d: completed=%v reason=%s", size, o.completed(), o.reason)
				}
			})
		}
	}
}

func TestCompletionRequiresUninterruptedForwarding(t *testing.T) {
	for _, end := range []error{nil, io.ErrUnexpectedEOF, errors.New("synthetic read failure")} {
		o := newCompletionObserver(&http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}})
		o.feed([]byte(observedCompleted))
		o.end(end)
		if o.completed() {
			t.Fatal("an unfinished or broken transport certified recovery")
		}
	}
	for _, media := range []string{"application/json", "text/event-stream"} {
		o := newCompletionObserver(&http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {media}, "Content-Encoding": {"gzip"}}})
		o.feed([]byte(observedCompleted))
		o.end(io.EOF)
		if o.completed() || o.reason != "unsupported_encoding" {
			t.Fatal("compressed response was treated as plaintext completion")
		}
	}
}
