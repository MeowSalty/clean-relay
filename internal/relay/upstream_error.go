package relay

import (
	"bytes"
	"encoding/json"
	"strings"
	"unicode/utf8"
)

const resourceMismatchRule = "azure_resource_mismatch"

// strictObject validates just the object being used for a decision. Unknown
// subtrees remain opaque; quoted JSON and arbitrary nested errors are not read.
func strictObject(raw []byte) (map[string]json.RawMessage, bool) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || raw[0] != '{' || !utf8.Valid(raw) || !json.Valid(raw) {
		return nil, false
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	if _, err := dec.Token(); err != nil {
		return nil, false
	}
	object := make(map[string]json.RawMessage)
	for dec.More() {
		token, err := dec.Token()
		if err != nil {
			return nil, false
		}
		key, ok := token.(string)
		if !ok {
			return nil, false
		}
		if _, exists := object[key]; exists {
			return nil, false
		}
		var value json.RawMessage
		if dec.Decode(&value) != nil {
			return nil, false
		}
		object[key] = value
	}
	return object, true
}

func jsonString(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 || raw[0] != '"' {
		return "", false
	}
	var value string
	err := json.Unmarshal(raw, &value)
	return value, err == nil
}

type upstreamError struct {
	typ        string
	code       string
	message    string
	hasType    bool
	hasCode    bool
	hasMessage bool
}

func readUpstreamError(object map[string]json.RawMessage, eventRoot bool) (upstreamError, bool) {
	var result upstreamError
	if raw, exists := object["type"]; exists && !eventRoot {
		var ok bool
		result.typ, ok = jsonString(raw)
		if !ok {
			return upstreamError{}, false
		}
		result.hasType = true
	}
	if raw, exists := object["code"]; exists {
		result.hasCode = true
		if !bytes.Equal(raw, []byte("null")) {
			var ok bool
			result.code, ok = jsonString(raw)
			if !ok {
				return upstreamError{}, false
			}
		}
	}
	if raw, exists := object["message"]; exists {
		var ok bool
		result.message, ok = jsonString(raw)
		if !ok {
			return upstreamError{}, false
		}
		result.hasMessage = true
	}
	return result, true
}

func (e upstreamError) allowed(requireType bool) bool {
	return (!requireType || e.hasType) && (!e.hasType || e.typ == "invalid_request_error") &&
		(e.code == "" || e.code == "invalid_request_error")
}

// The brand word between "different" and "openai resource" is not required:
// intermediaries redact it (observed "created under a different *** OpenAI
// resource"), so only the stable sentence fragments may match.
func (e upstreamError) matches(requireType bool) bool {
	message := normalizeErrorMessage(e.message)
	return e.allowed(requireType) && e.hasMessage &&
		strings.Contains(message, "created under a different") &&
		strings.Contains(message, "openai resource") &&
		strings.Contains(message, "use the same resource")
}

func normalizeErrorMessage(message string) string {
	message = strings.Join(strings.Fields(message), " ")
	var normalized strings.Builder
	normalized.Grow(len(message))
	for i := 0; i < len(message); i++ {
		c := message[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		normalized.WriteByte(c)
	}
	return normalized.String()
}

func matchHTTPError(body []byte) (matched, businessFailure bool) {
	root, ok := strictObject(body)
	if !ok {
		return false, false
	}
	object, ok := strictObject(root["error"])
	if !ok {
		return false, false
	}
	detail, ok := readUpstreamError(object, false)
	return ok && detail.matches(true), ok && detail.hasMessage
}

// consistentErrors checks only fields actually supplied in both locations.
// Every supplied error must also independently satisfy the type/code contract.
func consistentErrors(a, b upstreamError) bool {
	return a.allowed(false) && b.allowed(false) &&
		(!a.hasType || !b.hasType || a.typ == b.typ) &&
		(!a.hasCode || !b.hasCode || a.code == b.code) &&
		(!a.hasMessage || !b.hasMessage || a.message == b.message)
}

func matchSSEError(root map[string]json.RawMessage, kind string) bool {
	outer, ok := readUpstreamError(root, true)
	if !ok || !outer.allowed(false) {
		return false
	}
	if kind == "error" {
		if raw, exists := root["error"]; exists {
			object, ok := strictObject(raw)
			if !ok {
				return false
			}
			inner, ok := readUpstreamError(object, false)
			return ok && consistentErrors(outer, inner) && (outer.matches(false) || inner.matches(false))
		}
		return outer.matches(false)
	}
	response, ok := strictObject(root["response"])
	if !ok {
		return false
	}
	object, ok := strictObject(response["error"])
	if !ok {
		return false
	}
	inner, ok := readUpstreamError(object, false)
	if !ok || !inner.matches(false) || !consistentErrors(outer, inner) {
		return false
	}
	if raw, exists := root["error"]; exists {
		otherObject, ok := strictObject(raw)
		if !ok {
			return false
		}
		other, ok := readUpstreamError(otherObject, false)
		if !ok || !consistentErrors(other, inner) {
			return false
		}
	}
	return true
}
