package relay

import (
	"bytes"
	"encoding/json"
	"strings"
)

type inspectionResult struct {
	matched         bool
	businessFailure bool
	decision        string
}

func (r inspectionResult) finished() bool { return r.matched || r.decision != "" }

// sseInspector only classifies a bounded prefix. The raw wire bytes are held
// separately and replayed unchanged. It never becomes the stream decoder.
type sseInspector struct {
	start    [3]byte
	startLen int
	started  bool
	skipLF   bool
	line     []byte
	event    string
	eventSet bool
	data     []byte
	dataSet  bool
}

func (s *sseInspector) feed(chunk []byte) inspectionResult {
	for _, c := range chunk {
		if !s.started {
			s.start[s.startLen] = c
			s.startLen++
			bom := [3]byte{0xef, 0xbb, 0xbf}
			if c == bom[s.startLen-1] {
				if s.startLen == len(bom) {
					s.started = true
				}
				continue
			}
			s.started = true
			for _, first := range s.start[:s.startLen] {
				if result := s.character(first); result.finished() {
					return result
				}
			}
			continue
		}
		if result := s.character(c); result.finished() {
			return result
		}
	}
	return inspectionResult{}
}

func (s *sseInspector) character(c byte) inspectionResult {
	if s.skipLF {
		s.skipLF = false
		if c == '\n' {
			return inspectionResult{}
		}
	}
	if c == '\r' || c == '\n' {
		s.skipLF = c == '\r'
		result := s.endLine()
		s.line = s.line[:0]
		return result
	}
	s.line = append(s.line, c)
	return inspectionResult{}
}

func (s *sseInspector) endLine() inspectionResult {
	if len(s.line) == 0 {
		result := inspectionResult{}
		if s.dataSet {
			result = classifySSE(s.event, s.eventSet, s.data)
		}
		s.event, s.eventSet, s.dataSet = "", false, false
		s.data = s.data[:0]
		return result
	}
	if s.line[0] == ':' {
		return inspectionResult{}
	}
	field, value, _ := bytes.Cut(s.line, []byte(":"))
	value = bytes.TrimPrefix(value, []byte(" "))
	switch string(field) {
	case "event":
		if s.eventSet {
			return inspectionResult{decision: "unrecognized_event"}
		}
		s.event, s.eventSet = string(value), true
	case "data":
		if s.dataSet {
			s.data = append(s.data, '\n')
		}
		s.data = append(s.data, value...)
		s.dataSet = true
	default:
		// ID/retry and extension fields are outside the recoverable prefix
		// grammar. They remain valid wire data and are immediately passed on.
		return inspectionResult{decision: "unrecognized_event"}
	}
	return inspectionResult{}
}

func classifySSE(event string, eventSet bool, data []byte) inspectionResult {
	if bytes.Equal(bytes.TrimSpace(data), []byte("[DONE]")) {
		return inspectionResult{decision: "terminal_event"}
	}
	root, ok := strictObject(data)
	if !ok {
		return inspectionResult{decision: "unrecognized_event"}
	}
	kind := event
	if raw, exists := root["type"]; exists {
		jsonType, valid := jsonString(raw)
		if !valid || eventSet && event != jsonType {
			return inspectionResult{decision: "unrecognized_event"}
		}
		kind = jsonType
	}
	switch kind {
	case "error", "response.failed":
		matched := matchSSEError(root, kind)
		return inspectionResult{matched: matched, businessFailure: true, decision: "no_match"}
	case "response.created", "response.in_progress":
		response, valid := strictObject(root["response"])
		if !valid {
			return inspectionResult{decision: "unrecognized_event"}
		}
		output := response["output"]
		if len(output) == 0 || output[0] != '[' {
			return inspectionResult{decision: "unrecognized_event"}
		}
		var items []json.RawMessage
		if json.Unmarshal(output, &items) != nil {
			return inspectionResult{decision: "unrecognized_event"}
		}
		if len(items) != 0 {
			return inspectionResult{decision: "output_started"}
		}
		if raw, exists := response["error"]; exists && !bytes.Equal(raw, []byte("null")) {
			return inspectionResult{decision: "unrecognized_event"}
		}
		return inspectionResult{}
	case "response.completed", "response.incomplete":
		return inspectionResult{decision: "terminal_event"}
	default:
		if strings.HasPrefix(kind, "response.") {
			return inspectionResult{decision: "output_started"}
		}
		return inspectionResult{decision: "unrecognized_event"}
	}
}
