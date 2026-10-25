package cleaner

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// HistoryMode selects a pure history transformation, not a retry strategy.
type HistoryMode string

const (
	// PortableIDs removes optional IDs only from replayable messages and tools.
	PortableIDs HistoryMode = "portable_ids"
	// PortableIDsAndReasoning additionally drops entire historical reasoning items.
	PortableIDsAndReasoning HistoryMode = "portable_ids_and_reasoning"
)

// HistoryAnalysis retains no decoded JSON or reference to the input body.
type HistoryAnalysis struct {
	Candidate  bool
	Background bool
	// Counts describe the strongest transformation, regardless of the mode
	// used for Candidate. Reasoning IDs are not counted as portable item IDs.
	PortableItemIDs int
	ReasoningItems  int
}

// HistoryResult describes a pure transformation. Body shares the input only
// when nothing changed; otherwise it is independently owned. InputItems is the
// number of remaining items, including opaque values and unsupported types.
type HistoryResult struct {
	Body                  []byte
	RemovedItemIDs        int
	RemovedReasoningItems int
	InputItems            int
}

// AnalyzeHistory checks eligibility without constructing a repaired request.
func AnalyzeHistory(body []byte, mode HistoryMode) (HistoryAnalysis, error) {
	analysis, _, err := visitHistory(body, mode, false)
	return analysis, err
}

// RepairHistory creates a repair copy without modifying body. Callers must not
// send a result with zero InputItems or without an actual removal.
func RepairHistory(body []byte, mode HistoryMode) (HistoryResult, error) {
	_, result, err := visitHistory(body, mode, true)
	return result, err
}

func visitHistory(body []byte, mode HistoryMode, repair bool) (HistoryAnalysis, HistoryResult, error) {
	if mode != PortableIDs && mode != PortableIDsAndReasoning {
		return HistoryAnalysis{}, HistoryResult{}, fmt.Errorf("unsupported history cleanup mode")
	}
	document, err := decodeDocument(body)
	if err != nil {
		return HistoryAnalysis{}, HistoryResult{}, err
	}
	result := HistoryResult{Body: body}
	if document[0] != '{' {
		return HistoryAnalysis{}, result, nil
	}
	root, err := decodeObject(document)
	if err != nil {
		return HistoryAnalysis{}, HistoryResult{}, err
	}
	analysis := HistoryAnalysis{Background: bytes.Equal(root["background"], []byte("true"))}
	input := root["input"]
	if len(input) == 0 || input[0] != '[' {
		return analysis, result, nil
	}
	var items []json.RawMessage
	if err := json.Unmarshal(input, &items); err != nil {
		return HistoryAnalysis{}, HistoryResult{}, fmt.Errorf("%w: input array", ErrInvalidJSON)
	}
	result.InputItems = len(items)
	var kept []json.RawMessage
	if repair {
		kept = make([]json.RawMessage, 0, len(items))
	}
	for _, item := range items {
		action := keepHistoryItem
		var object map[string]json.RawMessage
		if item[0] == '{' {
			object, err = decodeObject(item)
			if err != nil {
				return HistoryAnalysis{}, HistoryResult{}, err
			}
			action = historyAction(object)
		}
		switch action {
		case dropHistoryID:
			analysis.PortableItemIDs++
		case dropReasoningItem:
			analysis.ReasoningItems++
			if mode == PortableIDs {
				action = keepHistoryItem
			}
		}
		analysis.Candidate = analysis.Candidate || action != keepHistoryItem
		if !repair {
			continue
		}
		switch action {
		case dropReasoningItem:
			result.RemovedReasoningItems++
			continue
		case dropHistoryID:
			delete(object, "id")
			item, err = json.Marshal(object)
			if err != nil {
				return HistoryAnalysis{}, HistoryResult{}, err
			}
			result.RemovedItemIDs++
		}
		kept = append(kept, item)
	}
	if !repair || !analysis.Candidate {
		return analysis, result, nil
	}
	root["input"], err = json.Marshal(kept)
	if err == nil {
		result.Body, err = json.Marshal(root)
	}
	if err != nil {
		return HistoryAnalysis{}, HistoryResult{}, err
	}
	result.InputItems = len(kept)
	return analysis, result, nil
}

type historyItemAction uint8

const (
	keepHistoryItem historyItemAction = iota
	dropHistoryID
	dropReasoningItem
)

func historyAction(item map[string]json.RawMessage) historyItemAction {
	kind := "message"
	if raw, exists := item["type"]; exists {
		var ok bool
		kind, ok = historyString(raw)
		if !ok {
			return keepHistoryItem
		}
	}
	if kind == "reasoning" {
		return dropReasoningItem
	}
	if !nonemptyHistoryString(item["id"]) {
		return keepHistoryItem
	}
	if raw, exists := item["status"]; exists {
		status, ok := historyString(raw)
		if !ok || status != "completed" {
			return keepHistoryItem
		}
	}
	complete := false
	switch kind {
	case "message":
		role, _ := historyString(item["role"])
		complete = (role == "user" || role == "assistant" || role == "system" || role == "developer") && historyStringOrArray(item["content"])
	case "function_call", "custom_tool_call":
		field := "arguments"
		if kind == "custom_tool_call" {
			field = "input"
		}
		_, text := historyString(item[field])
		complete = nonemptyHistoryString(item["call_id"]) && nonemptyHistoryString(item["name"]) && text
	case "function_call_output", "custom_tool_call_output":
		complete = nonemptyHistoryString(item["call_id"]) && historyStringOrArray(item["output"])
	}
	if complete {
		return dropHistoryID
	}
	return keepHistoryItem
}

func historyString(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 || raw[0] != '"' {
		return "", false
	}
	var value string
	err := json.Unmarshal(raw, &value)
	return value, err == nil
}

func nonemptyHistoryString(raw json.RawMessage) bool {
	value, ok := historyString(raw)
	return ok && value != ""
}

func historyStringOrArray(raw json.RawMessage) bool {
	return len(raw) != 0 && (raw[0] == '[' || raw[0] == '"')
}
