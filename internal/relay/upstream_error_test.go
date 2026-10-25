package relay

import (
	"strconv"
	"strings"
	"testing"
)

const mismatchMessage = "Item 'synthetic-resource-id' was created under a different Azure OpenAI resource. Please use the same resource."

func mismatchJSON() string {
	return `{"error":{"type":"invalid_request_error","message":` + strconv.Quote(mismatchMessage) + `}}`
}

func TestHTTPResourceErrorMatching(t *testing.T) {
	message := strconv.Quote(mismatchMessage)
	cases := []struct {
		name, raw string
		match     bool
	}{
		{"exact", mismatchJSON(), true},
		{"code null", `{"error":{"type":"invalid_request_error","code":null,"message":` + message + `}}`, true},
		{"code empty", `{"error":{"type":"invalid_request_error","code":"","message":` + message + `}}`, true},
		{"code accepted", `{"error":{"type":"invalid_request_error","code":"invalid_request_error","message":` + message + `}}`, true},
		{"ascii case and whitespace", `{"error":{"type":"invalid_request_error","message":"CREATED\n UNDER\tA DIFFERENT AZURE OPENAI RESOURCE; USE   THE SAME RESOURCE"}}`, true},
		{"ordinary error", `{"error":{"type":"invalid_request_error","message":"Azure OpenAI rejected this request"}}`, false},
		{"one fragment", `{"error":{"type":"invalid_request_error","message":"created under a different Azure OpenAI resource"}}`, false},
		{"wrong code", `{"error":{"type":"invalid_request_error","code":"invalid_encrypted_content","message":` + message + `}}`, false},
		{"code object", `{"error":{"type":"invalid_request_error","code":{},"message":` + message + `}}`, false},
		{"code number", `{"error":{"type":"invalid_request_error","code":1,"message":` + message + `}}`, false},
		{"no type", `{"error":{"message":` + message + `}}`, false},
		{"wrong type", `{"error":{"type":"error","message":` + message + `}}`, false},
		{"null type", `{"error":{"type":null,"message":` + message + `}}`, false},
		{"message not string", `{"error":{"type":"invalid_request_error","message":{}}}`, false},
		{"duplicate error", `{"error":{},"error":{"type":"invalid_request_error","message":` + message + `}}`, false},
		{"duplicate escaped field", `{"error":{"type":"invalid_request_error","message":` + message + `,"\u006dessage":` + message + `}}`, false},
		{"duplicate unused key", `{"error":{"type":"invalid_request_error","message":` + message + `,"x":1,"x":1}}`, false},
		{"opaque duplicate subtree", `{"error":{"type":"invalid_request_error","message":` + message + `,"extra":{"x":1,"x":2}}}`, true},
		{"extra root", mismatchJSON() + ` {}`, false},
		{"quoted error", strconv.Quote(mismatchJSON()), false},
		{"ordinary body", `{"output":[{"text":` + message + `}]}`, false},
		{"unknown wrapper", `{"detail":` + mismatchJSON() + `}`, false},
		{"incomplete", `{"error":{"type":"invalid_request_error","message":` + message, false},
		{"invalid UTF8", strings.Replace(mismatchJSON(), "Item", "\xff", 1), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := matchHTTPError([]byte(tc.raw))
			if got != tc.match {
				t.Fatalf("match=%v, want %v", got, tc.match)
			}
		})
	}
}

func TestSSEResourceErrorEnvelopes(t *testing.T) {
	message := strconv.Quote(mismatchMessage)
	cases := []struct {
		name, event, data string
		match             bool
	}{
		{"root error", "error", `{"type":"error","message":` + message + `}`, true},
		{"inner error type absent", "error", `{"error":{"message":` + message + `}}`, true},
		{"inner error type", "error", `{"type":"error","error":{"type":"invalid_request_error","message":` + message + `}}`, true},
		{"event inferred", "", `{"type":"error","message":` + message + `}`, true},
		{"response failed", "response.failed", `{"type":"response.failed","response":{"error":{"message":` + message + `}}}`, true},
		{"consistent root", "error", `{"message":` + message + `,"error":{"message":` + message + `}}`, true},
		{"conflicting root", "error", `{"message":"different","error":{"message":` + message + `}}`, false},
		{"conflicting code", "error", `{"code":"invalid_request_error","error":{"code":"","message":` + message + `}}`, false},
		{"bad root code", "error", `{"code":123,"error":{"message":` + message + `}}`, false},
		{"bad inner type", "error", `{"error":{"type":"error","message":` + message + `}}`, false},
		{"null inner type", "error", `{"error":{"type":null,"message":` + message + `}}`, false},
		{"bad inner code", "error", `{"error":{"code":"invalid_encrypted_content","message":` + message + `}}`, false},
		{"conflicting event", "error", `{"type":"response.failed","response":{"error":{"message":` + message + `}}}`, false},
		{"null event type", "error", `{"type":null,"message":` + message + `}`, false},
		{"duplicate response", "response.failed", `{"response":{},"response":{"error":{"message":` + message + `}}}`, false},
		{"duplicate response error", "response.failed", `{"response":{"error":{},"error":{"message":` + message + `}}}`, false},
		{"wrong failure position", "response.failed", `{"error":{"message":` + message + `}}`, false},
		{"failure no details", "response.failed", `{"response":{"error":null}}`, false},
		{"failure conflicting root", "response.failed", `{"error":{"message":"other"},"response":{"error":{"message":` + message + `}}}`, false},
		{"extra root", "error", `{"message":` + message + `} null`, false},
		{"unknown event", "other", `{"message":` + message + `}`, false},
		{"no event", "", `{"message":` + message + `}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := classifySSE(tc.event, tc.event != "", []byte(tc.data))
			if result.matched != tc.match {
				t.Fatalf("result=%+v, want match %v", result, tc.match)
			}
		})
	}
}

func TestSSEPrefixSplitsAndOrder(t *testing.T) {
	errorFrame := "event: error\ndata: {\"type\":\"error\",\n" + "data: \"message\":" + strconv.Quote("中文😀 "+mismatchMessage) + "}\n\n"
	metadata := ": heartbeat\n\nevent: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"first\",\"output\":[],\"error\":null}}\n\n"
	for _, newline := range []string{"\n", "\r\n", "\r"} {
		wire := []byte("\xef\xbb\xbf" + strings.ReplaceAll(metadata+errorFrame, "\n", newline))
		for split := 1; split <= len(wire); split++ {
			parser := sseInspector{}
			var result inspectionResult
			for offset := 0; offset < len(wire); offset += split {
				result = parser.feed(wire[offset:min(offset+split, len(wire))])
				if result.finished() {
					break
				}
			}
			if !result.matched {
				t.Fatalf("newline=%q split=%d: %+v", newline, split, result)
			}
		}
	}
	for _, kind := range []string{"response.output_item.added", "response.output_text.delta", "response.function_call_arguments.delta", "response.reasoning_summary_text.delta", "response.compaction.delta"} {
		parser := sseInspector{}
		wire := "event: " + kind + "\ndata: {\"type\":" + strconv.Quote(kind) + "}\n\n" + errorFrame
		if result := parser.feed([]byte(wire)); result.matched || result.decision != "output_started" {
			t.Fatalf("output order: %+v", result)
		}
	}
	for _, frame := range []string{
		"data: [DONE]\n\n", "event: response.completed\ndata: {}\n\n", "event: other\ndata: {}\n\n",
		"event: error\ndata: {\"message\":\"ordinary error\"}\n\n", "event: response.created\ndata: {\"response\":{}}\n\n",
		"event: response.created\ndata: {\"response\":{\"output\":[{}]}}\n\n", "event: error\nevent: error\n",
		"id: opaque\n", "data: not-json\n\n",
	} {
		parser := sseInspector{}
		if result := parser.feed([]byte(frame + errorFrame)); result.matched || !result.finished() {
			t.Fatalf("must stop on %q: %+v", frame, result)
		}
	}
}

func FuzzSSEChunkBoundaries(f *testing.F) {
	f.Add([]byte("\xef\xbb\xbfevent: error\r\ndata: {\"message\":"+strconv.Quote(mismatchMessage)+"}\r\n\r\n"), uint8(3))
	f.Add([]byte(": heartbeat\r\rdata: [DONE]\r\r"), uint8(1))
	f.Fuzz(func(t *testing.T, wire []byte, size uint8) {
		if len(wire) > 1<<20 {
			t.Skip()
		}
		whole, split := sseInspector{}, sseInspector{}
		want := whole.feed(wire)
		var got inspectionResult
		step := int(size) + 1
		for offset := 0; offset < len(wire); offset += step {
			got = split.feed(wire[offset:min(len(wire), offset+step)])
			if got.finished() {
				break
			}
		}
		if got != want {
			t.Fatalf("classification depends on chunks: %+v / %+v", got, want)
		}
	})
}
