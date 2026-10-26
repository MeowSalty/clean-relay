package relay

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"strings"
)

const maxCompletionBytes = 1 << 20

// completionObserver consumes copies of bytes already being forwarded. It
// performs no reads, starts no goroutines and never controls retry eligibility.
// JSON is bounded as a document; SSE is bounded per event, not per stream.
type completionObserver struct {
	sse             bool
	status          int
	invalid         bool
	eof             bool
	success         bool
	terminal        bool
	businessFailure bool
	reason          string
	document        []byte
	start           [3]byte
	startLen        int
	started         bool
	skipLF          bool
	eventBytes      int
	line            []byte
	event           string
	eventSet        bool
	data            []byte
	dataSet         bool
}

func newCompletionObserver(response *http.Response) *completionObserver {
	o := &completionObserver{status: response.StatusCode, reason: "unknown"}
	encoding, supported := contentEncoding(response.Header)
	if !supported || encoding == "gzip" {
		o.invalidate("unsupported_encoding")
	} else if isSSEMediaType(response.Header) && response.StatusCode == http.StatusOK {
		o.sse = true
	} else if !isJSONMediaType(response.Header) {
		o.invalidate("unsupported_format")
	}
	return o
}

func (o *completionObserver) invalidate(reason string) {
	o.invalid, o.reason = true, reason
	o.document, o.line, o.data = nil, nil, nil
	o.event = ""
}

func (o *completionObserver) feed(chunk []byte) {
	if o.invalid {
		return
	}
	if !o.sse {
		if len(chunk) > maxCompletionBytes-len(o.document) {
			o.invalidate("completion_limit")
			return
		}
		o.document = append(o.document, chunk...)
		return
	}
	for _, c := range chunk {
		if !o.started {
			o.start[o.startLen] = c
			o.startLen++
			bom := [3]byte{0xef, 0xbb, 0xbf}
			if c == bom[o.startLen-1] {
				if o.startLen == len(bom) {
					o.started = true
				}
				continue
			}
			o.started = true
			for _, first := range o.start[:o.startLen] {
				o.character(first)
				if o.invalid {
					return
				}
			}
			continue
		}
		o.character(c)
		if o.invalid {
			return
		}
	}
}

func (o *completionObserver) character(c byte) {
	o.eventBytes++
	if o.eventBytes > maxCompletionBytes {
		o.invalidate("completion_limit")
		return
	}
	if o.skipLF {
		o.skipLF = false
		if c == '\n' {
			return
		}
	}
	if c != '\r' && c != '\n' {
		o.line = append(o.line, c)
		return
	}
	o.skipLF = c == '\r'
	if len(o.line) == 0 {
		if o.dataSet {
			o.frame()
		}
		o.event, o.eventSet, o.dataSet = "", false, false
		o.data = o.data[:0]
		o.eventBytes = 0
		return
	}
	if o.line[0] != ':' {
		field, value, _ := bytes.Cut(o.line, []byte(":"))
		value = bytes.TrimPrefix(value, []byte(" "))
		switch string(field) {
		case "event":
			if o.eventSet {
				o.invalidate("invalid_completion")
			} else {
				o.event, o.eventSet = string(value), true
			}
		case "data":
			if o.dataSet {
				o.data = append(o.data, '\n')
			}
			o.data = append(o.data, value...)
			o.dataSet = true
		case "id", "retry":
			// These SSE transport fields do not provide completion evidence.
		default:
			o.invalidate("unsupported_format")
		}
	}
	o.line = o.line[:0]
}

func nonNullJSON(value []byte) bool {
	return len(value) != 0 && !bytes.Equal(bytes.TrimSpace(value), []byte("null"))
}

func (o *completionObserver) frame() {
	if bytes.Equal(bytes.TrimSpace(o.data), []byte("[DONE]")) {
		o.terminal = true
		if !o.success {
			o.invalidate("completion_unconfirmed")
		}
		return
	}
	root, ok := strictObject(o.data)
	if !ok {
		o.invalidate("invalid_completion")
		return
	}
	kind := o.event
	if raw, exists := root["type"]; exists {
		value, valid := jsonString(raw)
		if !valid || value == "" || o.eventSet && o.event != "message" && o.event != value {
			o.invalidate("invalid_completion")
			return
		}
		kind = value
	}
	switch kind {
	case "error", "response.error", "response.failed", "response.incomplete", "response.canceled", "response.cancelled":
		o.businessFailure = true
	}
	if nonNullJSON(root["error"]) {
		o.businessFailure = true
	}
	var responseStatus string
	if raw, exists := root["response"]; exists {
		response, valid := strictObject(raw)
		if !valid {
			o.invalidate("invalid_completion")
			return
		}
		if status, exists := response["status"]; exists {
			var valid bool
			responseStatus, valid = jsonString(status)
			if !valid {
				o.invalidate("invalid_completion")
				return
			}
		}
		if nonNullJSON(response["error"]) || nonNullJSON(response["incomplete_details"]) {
			o.businessFailure = true
		}
		switch responseStatus {
		case "failed", "incomplete", "canceled", "cancelled":
			o.businessFailure = true
		}
	}
	if o.businessFailure {
		o.invalidate("business_failure")
		return
	}
	if o.terminal {
		o.invalidate("conflicting_terminal_event")
		return
	}
	if kind == "response.completed" {
		if responseStatus != "completed" {
			o.invalidate("completion_unconfirmed")
			return
		}
		o.success, o.terminal = true, true
	} else if !strings.HasPrefix(kind, "response.") {
		o.invalidate("unsupported_format")
	}
}

func (o *completionObserver) end(err error) {
	if err == nil {
		return
	}
	if !errors.Is(err, io.EOF) {
		o.invalidate("completion_read_error")
		return
	}
	o.eof = true
	if o.invalid {
		return
	}
	if o.sse {
		if len(o.line) != 0 || o.eventSet || o.dataSet || !o.started {
			o.invalidate("incomplete_event")
		}
	} else {
		root, ok := strictObject(o.document)
		if !ok {
			o.invalidate("invalid_completion")
			return
		}
		status, _ := jsonString(root["status"])
		if nonNullJSON(root["error"]) || nonNullJSON(root["incomplete_details"]) ||
			status == "failed" || status == "incomplete" || status == "canceled" || status == "cancelled" {
			o.businessFailure = true
			o.invalidate("business_failure")
			return
		}
		output := root["output"]
		o.success = o.status == http.StatusOK && status == "completed" && len(output) != 0 && output[0] == '['
		if object, exists := root["object"]; exists {
			kind, valid := jsonString(object)
			o.success = o.success && valid && kind == "response"
		}
		o.document = nil
	}
	if o.completed() {
		o.reason = "completed"
	} else if !o.invalid {
		o.reason = "completion_unconfirmed"
	}
}

func (o *completionObserver) completed() bool {
	return o.eof && o.success && !o.invalid && !o.businessFailure
}
