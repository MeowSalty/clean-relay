package acceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"unicode/utf8"
)

const (
	probeValue         = "cleanrelay-probe"
	conversationPrompt = "Reply briefly with cleanrelay-probe."
	streamPrompt       = "Reply with a short sentence containing cleanrelay-probe."
	toolPrompt         = "Call cleanrelay_echo exactly once with value cleanrelay-probe. After receiving the tool result, reply briefly with cleanrelay-probe."
	cancelPrompt       = "Write a long numbered list of at least 200 short sentences about counting. Start immediately with item 1."
)

var (
	errResponseStatus = errors.New("The response did not complete successfully.")
	errResponseText   = errors.New("The completed response contained no nonempty output text.")
	errToolCall       = errors.New("The response did not contain exactly one completed echo call with the fixed constant argument.")
)

type probeRequest struct {
	Model             string            `json:"model"`
	Input             []json.RawMessage `json:"input"`
	Stream            bool              `json:"stream"`
	Store             bool              `json:"store"`
	MaxOutputTokens   int               `json:"max_output_tokens"`
	Tools             []probeTool       `json:"tools,omitempty"`
	ToolChoice        json.RawMessage   `json:"tool_choice,omitempty"`
	ParallelToolCalls *bool             `json:"parallel_tool_calls,omitempty"`
}

type probeTool struct {
	Type        string          `json:"type"`
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
	Strict      bool            `json:"strict"`
}

func newProbe(model, prompt string, metadata bool) probeRequest {
	user := struct {
		Type    string `json:"type"`
		Role    string `json:"role"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		Metadata *struct {
			AcceptanceProbe bool `json:"acceptance_probe"`
		} `json:"internal_chat_message_metadata_passthrough,omitempty"`
	}{Type: "message", Role: "user"}
	user.Content = append(user.Content, struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}{Type: "input_text", Text: prompt})
	if metadata {
		user.Metadata = &struct {
			AcceptanceProbe bool `json:"acceptance_probe"`
		}{AcceptanceProbe: true}
	}
	// This contains only string and boolean fields, so marshaling cannot fail.
	input, _ := json.Marshal(user)
	return probeRequest{
		Model:           model,
		Input:           []json.RawMessage{input},
		MaxOutputTokens: 2048,
	}
}

func echoTools() []probeTool {
	return []probeTool{{
		Type:        "function",
		Name:        "cleanrelay_echo",
		Description: "A local acceptance probe returning a static value with no side effects.",
		// A singleton enum is the Responses strict-schema constant constraint.
		Parameters: json.RawMessage(`{"type":"object","properties":{"value":{"type":"string","enum":["cleanrelay-probe"]}},"required":["value"],"additionalProperties":false}`),
		Strict:     true,
	}}
}

func (e *engine) toolCall(ctx context.Context) (int, error) {
	request := newProbe(e.options.Model, toolPrompt, true)
	request.Tools = echoTools()
	request.ToolChoice = json.RawMessage(`{"type":"function","name":"cleanrelay_echo"}`)
	parallel := false
	request.ParallelToolCalls = &parallel
	body, status, err := e.post(ctx, e.base, request)
	if err != nil {
		return status, err
	}
	if status != http.StatusOK {
		return status, errHTTPStatus
	}
	output, err := completedOutput(body)
	if err != nil {
		return status, err
	}
	callID := ""
	for _, raw := range output {
		item, err := decodeObject(raw)
		if err != nil {
			return status, errResponseJSON
		}
		var kind string
		if json.Unmarshal(item["type"], &kind) != nil {
			return status, errResponseJSON
		}
		if kind != "function_call" {
			continue
		}
		var name, id, arguments, callStatus string
		if json.Unmarshal(item["name"], &name) != nil || json.Unmarshal(item["call_id"], &id) != nil ||
			json.Unmarshal(item["arguments"], &arguments) != nil {
			return status, errToolCall
		}
		if rawStatus, exists := item["status"]; exists && json.Unmarshal(rawStatus, &callStatus) != nil {
			return status, errToolCall
		}
		if callID != "" || name != "cleanrelay_echo" || strings.TrimSpace(id) == "" ||
			(callStatus != "" && callStatus != "completed") || !validEchoArguments(arguments) {
			return status, errToolCall
		}
		callID = id
	}
	if callID == "" {
		return status, errToolCall
	}
	result := struct {
		Type   string `json:"type"`
		CallID string `json:"call_id"`
		Output string `json:"output"`
	}{Type: "function_call_output", CallID: callID, Output: `{"value":"cleanrelay-probe"}`}
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return status, errRequest
	}
	// RawMessage preserves all output items, including unknown reasoning fields
	// and JSON number precision. Never substitute a previous_response_id.
	e.toolInput = make([]json.RawMessage, 0, len(output)+2)
	e.toolInput = append(e.toolInput, request.Input[0])
	e.toolInput = append(e.toolInput, output...)
	e.toolInput = append(e.toolInput, resultJSON)
	return status, nil
}

func (e *engine) toolResult(ctx context.Context) (int, error) {
	defer func() { e.toolInput = nil }()
	if len(e.toolInput) < 3 {
		return 0, errToolCall
	}
	request := newProbe(e.options.Model, toolPrompt, true)
	request.Input = e.toolInput
	request.Tools = echoTools()
	request.ToolChoice = json.RawMessage(`"none"`)
	parallel := false
	request.ParallelToolCalls = &parallel
	body, status, err := e.post(ctx, e.base, request)
	if err != nil {
		return status, err
	}
	if status != http.StatusOK {
		return status, errHTTPStatus
	}
	return status, completedText(body)
}

func validEchoArguments(arguments string) bool {
	object, err := decodeObject([]byte(arguments))
	if err != nil || len(object) != 1 {
		return false
	}
	var value string
	return json.Unmarshal(object["value"], &value) == nil && value == probeValue
}

func completedOutput(body []byte) ([]json.RawMessage, error) {
	object, err := decodeObject(body)
	if err != nil {
		return nil, errResponseJSON
	}
	var status string
	if json.Unmarshal(object["status"], &status) != nil {
		return nil, errResponseJSON
	}
	if status != "completed" || nonNull(object["error"]) || nonNull(object["incomplete_details"]) {
		return nil, errResponseStatus
	}
	var output []json.RawMessage
	if json.Unmarshal(object["output"], &output) != nil {
		return nil, errResponseJSON
	}
	return output, nil
}

func completedText(body []byte) error {
	output, err := completedOutput(body)
	if err != nil {
		return err
	}
	sawText := false
	for _, raw := range output {
		item, err := decodeObject(raw)
		if err != nil {
			return errResponseJSON
		}
		var kind string
		if json.Unmarshal(item["type"], &kind) != nil {
			return errResponseJSON
		}
		if kind != "message" {
			continue
		}
		var content []json.RawMessage
		if json.Unmarshal(item["content"], &content) != nil {
			return errResponseJSON
		}
		for _, rawPart := range content {
			part, err := decodeObject(rawPart)
			if err != nil {
				return errResponseJSON
			}
			var kind string
			if json.Unmarshal(part["type"], &kind) != nil {
				return errResponseJSON
			}
			if kind == "output_text" {
				var text string
				if json.Unmarshal(part["text"], &text) != nil {
					return errResponseJSON
				}
				sawText = sawText || strings.TrimSpace(text) != ""
			}
		}
	}
	if !sawText {
		return errResponseText
	}
	return nil
}

func localConcurrencyError(body []byte) bool {
	object, err := decodeObject(body)
	if err != nil {
		return false
	}
	local, err := decodeObject(object["error"])
	if err != nil {
		return false
	}
	var kind, code string
	return json.Unmarshal(local["type"], &kind) == nil && kind == "clean_relay_error" &&
		json.Unmarshal(local["code"], &code) == nil && code == "too_many_requests"
}

func validJSON(body []byte) bool {
	return utf8.Valid(body) && json.Valid(body)
}

func nonNull(raw json.RawMessage) bool {
	return len(raw) != 0 && !bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

// decodeObject rejects duplicate keys and keeps keys case-sensitive. Critical
// status, error and retry discriminator fields must not be silently overwritten.
// Values remain raw; unknown output subtrees and numbers are never decoded to any.
func decodeObject(body []byte) (map[string]json.RawMessage, error) {
	if !validJSON(body) {
		return nil, errResponseJSON
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return nil, errResponseJSON
	}
	object := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, errResponseJSON
		}
		key, ok := token.(string)
		if !ok {
			return nil, errResponseJSON
		}
		if _, exists := object[key]; exists {
			return nil, errResponseJSON
		}
		var raw json.RawMessage
		if decoder.Decode(&raw) != nil {
			return nil, errResponseJSON
		}
		object[key] = raw
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return nil, errResponseJSON
	}
	return object, nil
}
