package acceptance

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

var (
	errSSEEventLimit    = errors.New("An SSE event exceeded the 4 MiB size limit.")
	errSSEStreamLimit   = errors.New("The SSE stream exceeded the 32 MiB total size limit.")
	errSSEIncomplete    = errors.New("The SSE stream ended without a completed response.")
	errSSEData          = errors.New("The SSE stream contained an invalid or inconsistent JSON event.")
	errSSEFailure       = errors.New("The SSE stream reported an error, failed response, or incomplete response.")
	errSSEStatus        = errors.New("The SSE completion event did not report response status completed.")
	errSSENoText        = errors.New("The SSE response completed without a nonempty output text delta.")
	errCancelBeforeText = errors.New("The stream completed before a nonempty text delta could be canceled.")
)

type sseEvent struct {
	name string
	data []byte
}

type sseReader struct {
	reader    *bufio.Reader
	total     int64
	firstLine bool
}

func newSSEReader(reader io.Reader) *sseReader {
	return &sseReader{
		reader:    bufio.NewReaderSize(io.LimitReader(reader, maxStreamBytes+1), 32<<10),
		firstLine: true,
	}
}

// readLine uses bounded ReadSlice fragments instead of Scanner's 64 KiB limit.
// It counts all wire bytes, including comments and ignored fields.
func (r *sseReader) readLine(remaining int) ([]byte, int, error) {
	var line []byte
	for {
		fragment, err := r.reader.ReadSlice('\n')
		r.total += int64(len(fragment))
		if r.total > maxStreamBytes {
			return nil, 0, errSSEStreamLimit
		}
		if len(line)+len(fragment) > remaining {
			return nil, 0, errSSEEventLimit
		}
		line = append(line, fragment...)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err != nil {
			if errors.Is(err, io.EOF) && len(line) != 0 {
				return nil, 0, io.ErrUnexpectedEOF
			}
			return nil, 0, err
		}
		wireBytes := len(line)
		line = bytes.TrimSuffix(line, []byte{'\n'})
		line = bytes.TrimSuffix(line, []byte{'\r'})
		return line, wireBytes, nil
	}
}

func (r *sseReader) next() (sseEvent, error) {
	var event sseEvent
	wireBytes := 0
	hasData := false
	for {
		line, size, err := r.readLine(maxResponseBytes - wireBytes)
		if err != nil {
			if errors.Is(err, io.EOF) && wireBytes != 0 {
				err = io.ErrUnexpectedEOF
			}
			return sseEvent{}, err
		}
		wireBytes += size
		if r.firstLine {
			line = bytes.TrimPrefix(line, []byte{0xef, 0xbb, 0xbf})
			r.firstLine = false
		}
		if len(line) == 0 {
			if hasData || failureEvent(event.name) {
				event.data = bytes.TrimSuffix(event.data, []byte{'\n'})
				return event, nil
			}
			// Heartbeat-only blocks do not dispatch an application event.
			event, wireBytes, hasData = sseEvent{}, 0, false
			continue
		}
		if line[0] == ':' {
			continue
		}
		field, value, _ := bytes.Cut(line, []byte{':'})
		value = bytes.TrimPrefix(value, []byte{' '})
		switch string(field) {
		case "event":
			event.name = string(value)
		case "data":
			event.data = append(event.data, value...)
			event.data = append(event.data, '\n')
			hasData = true
		}
	}
}

func safeSSEReadError(err error) error {
	switch {
	case errors.Is(err, errSSEEventLimit):
		return errSSEEventLimit
	case errors.Is(err, errSSEStreamLimit):
		return errSSEStreamLimit
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return errSSEIncomplete
	default:
		return errResponseRead
	}
}

func failureEvent(kind string) bool {
	switch kind {
	case "error", "response.error", "response.failed", "response.incomplete":
		return true
	default:
		return false
	}
}

func parseSSEEvent(event sseEvent) (kind, delta string, completed bool, err error) {
	if failureEvent(event.name) {
		return "", "", false, errSSEFailure
	}
	if bytes.Equal(bytes.TrimSpace(event.data), []byte("[DONE]")) {
		return "", "", false, errSSEIncomplete
	}
	if len(event.data) == 0 && event.name == "" {
		return "", "", false, nil
	}
	object, err := decodeObject(event.data)
	if err != nil {
		return "", "", false, errSSEData
	}
	if rawType, exists := object["type"]; exists {
		if json.Unmarshal(rawType, &kind) != nil || kind == "" {
			return "", "", false, errSSEData
		}
	}
	if failureEvent(kind) || nonNull(object["error"]) {
		return "", "", false, errSSEFailure
	}
	if kind == "" {
		kind = event.name
	} else if event.name != "" && event.name != "message" && event.name != kind {
		return "", "", false, errSSEData
	}
	if nonNull(object["response"]) {
		response, err := decodeObject(object["response"])
		if err != nil {
			return "", "", false, errSSEData
		}
		var status string
		if rawStatus, exists := response["status"]; exists && json.Unmarshal(rawStatus, &status) != nil {
			return "", "", false, errSSEData
		}
		if status == "failed" || status == "incomplete" || status == "canceled" || status == "cancelled" ||
			nonNull(response["error"]) || nonNull(response["incomplete_details"]) {
			return "", "", false, errSSEFailure
		}
		completed = status == "completed"
	}
	if kind == "response.output_text.delta" && json.Unmarshal(object["delta"], &delta) != nil {
		return "", "", false, errSSEData
	}
	return kind, delta, completed, nil
}
