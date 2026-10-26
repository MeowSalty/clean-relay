package cleaner

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"
)

// Fingerprint contains a caller-supplied keyed digest, never source data.
type Fingerprint [32]byte

// HistoryAction is the single permitted transformation for a selected item.
type HistoryAction uint8

const (
	// KeepHistory leaves the item unchanged.
	KeepHistory HistoryAction = iota
	// RemoveHistoryID removes a replayable item's direct id.
	RemoveHistoryID
	// RemoveHistoryReasoning removes an entire reasoning item.
	RemoveHistoryReasoning
)

type selectorKind uint8

const (
	stableSelector selectorKind = iota + 1
	anchoredSelector
	localSelector
)

// HistorySelector identifies an item without retaining IDs, text or ciphertext.
// Anonymous items use an unchanged interval between two unique stable anchors.
// A local selector is usable only by the original request's unique serial.
type HistorySelector struct {
	kind     selectorKind
	Action   HistoryAction
	Key      Fingerprint
	Left     Fingerprint
	Right    Fingerprint
	Interval Fingerprint
	Offset   int
	Length   int
	serial   uint64
}

// Units includes the two anchors required by an anonymous item.
func (s HistorySelector) Units() int {
	if s.kind == anchoredSelector {
		return 3
	}
	return 1
}

type historyEntry struct {
	content  Fingerprint
	identity Fingerprint
	stable   bool
	selector HistorySelector
}

// HistorySnapshot contains only fingerprints and counts. Serial must be unique
// for each incoming request; repeated repairs of that request reuse it.
type HistorySnapshot struct {
	Analysis HistoryAnalysis
	serial   uint64
	entries  []historyEntry
}

// SnapshotHistory analyzes a metadata-cleaned request. fingerprint must return
// a deterministic keyed digest within the repair namespace.
func SnapshotHistory(body []byte, serial uint64, fingerprint func([]byte) Fingerprint) (HistorySnapshot, error) {
	if fingerprint == nil || serial == 0 {
		return HistorySnapshot{}, fmt.Errorf("history snapshot requires a digest and a nonzero request serial")
	}
	root, items, err := historyDocument(body)
	if err != nil {
		return HistorySnapshot{}, err
	}
	snapshot := HistorySnapshot{
		serial: serial, entries: make([]historyEntry, len(items)),
		Analysis: HistoryAnalysis{Background: bytes.Equal(root["background"], []byte("true"))},
	}
	identities := make(map[Fingerprint]int)
	for i, raw := range items {
		entry := &snapshot.entries[i]
		var object map[string]json.RawMessage
		if raw[0] == '{' {
			object, err = decodeObject(raw)
			if err != nil {
				return HistorySnapshot{}, err
			}
		}
		action := historyAction(object)
		switch action {
		case dropHistoryID:
			snapshot.Analysis.PortableItemIDs++
			entry.selector.Action = RemoveHistoryID
		case dropReasoningItem:
			snapshot.Analysis.ReasoningItems++
			entry.selector.Action = RemoveHistoryReasoning
		}
		// An omitted type means message in the existing cleaning contract.
		if object != nil {
			if _, ok := object["type"]; !ok {
				object["type"] = json.RawMessage("\"message\"")
			}
			raw, err = json.Marshal(object)
			if err != nil {
				return HistorySnapshot{}, err
			}
		}
		canonical, err := canonicalHistoryValue(raw)
		if err != nil {
			return HistorySnapshot{}, err
		}
		entry.content = fingerprint(append([]byte("content\x00"), canonical...))
		if nonemptyHistoryString(object["id"]) {
			entry.stable, entry.identity = true, entry.content
		} else if action == dropReasoningItem && nonemptyHistoryString(object["encrypted_content"]) {
			cipher, _ := historyString(object["encrypted_content"])
			entry.stable = true
			entry.identity = fingerprint(append([]byte("reasoning-cipher\x00"), cipher...))
		}
		if entry.stable {
			identities[entry.identity]++
			entry.selector.kind, entry.selector.Key = stableSelector, entry.identity
		} else {
			entry.selector.kind, entry.selector.Key = localSelector, entry.content
			entry.selector.serial, entry.selector.Offset = serial, i
		}
	}
	snapshot.Analysis.Candidate = snapshot.Analysis.PortableItemIDs+snapshot.Analysis.ReasoningItems > 0
	// Each interval is hashed once. Neither an index nor an occurrence count
	// alone authorizes deletion after history has been edited or truncated.
	left := -1
	for right, entry := range snapshot.entries {
		if !entry.stable || identities[entry.identity] != 1 {
			continue
		}
		if left >= 0 {
			var interval []byte
			for _, between := range snapshot.entries[left+1 : right] {
				interval = append(interval, between.content[:]...)
			}
			digest := fingerprint(append([]byte("interval\x00"), interval...))
			for i := left + 1; i < right; i++ {
				selector := &snapshot.entries[i].selector
				if selector.Action != RemoveHistoryReasoning || snapshot.entries[i].stable {
					continue
				}
				selector.kind, selector.serial = anchoredSelector, 0
				selector.Left, selector.Right = snapshot.entries[left].identity, entry.identity
				selector.Interval, selector.Offset, selector.Length = digest, i-left-1, right-left-1
			}
		}
		left = right
	}
	return snapshot, nil
}

// Selectors returns independently owned candidates for freezing a fault scope.
func (s HistorySnapshot) Selectors() []HistorySelector {
	var selectors []HistorySelector
	seen := make(map[HistorySelector]bool)
	for _, entry := range s.entries {
		selector := entry.selector
		if selector.Action != KeepHistory && !seen[selector] {
			selectors = append(selectors, selector)
			seen[selector] = true
		}
	}
	return selectors
}

// Match returns actions indexed by the current input, which selectors matched,
// and whether anonymous identities could not be aligned. Missing stable items
// are normal after compaction and do not count as an alignment failure.
func (s HistorySnapshot) Match(selectors []HistorySelector) ([]HistoryAction, []bool, bool) {
	actions := make([]HistoryAction, len(s.entries))
	matched := make([]bool, len(selectors))
	if len(selectors) == 0 {
		return actions, matched, false
	}
	indices := make(map[HistorySelector][]int, len(selectors))
	for i, selector := range selectors {
		indices[selector] = append(indices[selector], i)
	}
	for pos, entry := range s.entries {
		for _, i := range indices[entry.selector] {
			actions[pos] = selectors[i].Action
			matched[i] = true
		}
	}
	unaligned := false
	for i, selector := range selectors {
		if !matched[i] && selector.kind != stableSelector {
			unaligned = true
		}
	}
	return actions, matched, unaligned
}

// RepairSelectedHistory applies only explicitly selected actions. All actions
// are rechecked against the existing item eligibility contract before use.
func RepairSelectedHistory(body []byte, actions []HistoryAction) (HistoryResult, error) {
	root, items, err := historyDocument(body)
	if err != nil {
		return HistoryResult{}, err
	}
	if len(items) != len(actions) {
		return HistoryResult{}, fmt.Errorf("history selection does not fit the request snapshot")
	}
	result := HistoryResult{Body: body, InputItems: len(items)}
	kept := make([]json.RawMessage, 0, len(items))
	for i, raw := range items {
		if actions[i] == KeepHistory {
			kept = append(kept, raw)
			continue
		}
		if raw[0] != '{' {
			return HistoryResult{}, fmt.Errorf("history selection targets an opaque item")
		}
		object, err := decodeObject(raw)
		if err != nil {
			return HistoryResult{}, err
		}
		actual := historyAction(object)
		switch {
		case actions[i] == RemoveHistoryReasoning && actual == dropReasoningItem:
			result.RemovedReasoningItems++
			continue
		case actions[i] == RemoveHistoryID && actual == dropHistoryID:
			delete(object, "id")
			raw, err = json.Marshal(object)
			if err != nil {
				return HistoryResult{}, err
			}
			result.RemovedItemIDs++
		default:
			return HistoryResult{}, fmt.Errorf("history selection is not an eligible transformation")
		}
		kept = append(kept, raw)
	}
	if result.RemovedItemIDs+result.RemovedReasoningItems == 0 {
		return result, nil
	}
	root["input"], err = json.Marshal(kept)
	if err == nil {
		result.Body, err = json.Marshal(root)
	}
	result.InputItems = len(kept)
	return result, err
}

func historyDocument(body []byte) (map[string]json.RawMessage, []json.RawMessage, error) {
	document, err := decodeDocument(body)
	if err != nil {
		return nil, nil, err
	}
	if document[0] != '{' {
		return nil, nil, nil
	}
	root, err := decodeObject(document)
	if err != nil {
		return nil, nil, err
	}
	input := root["input"]
	if len(input) == 0 || input[0] != '[' {
		return root, nil, nil
	}
	var items []json.RawMessage
	if err := json.Unmarshal(input, &items); err != nil {
		return nil, nil, fmt.Errorf("%w: input array", ErrInvalidJSON)
	}
	return root, items, nil
}

// canonicalHistoryValue sorts only objects whose decoded keys are unique.
// Duplicate-key objects retain their ordered pairs. Scalar lexemes (including
// numbers and strings containing unknown escapes) never pass through float64.
// This representation is used exclusively for hashing, never for forwarding.
func canonicalHistoryValue(raw json.RawMessage) ([]byte, error) {
	raw = bytes.TrimSpace(raw)
	if raw[0] == '[' {
		var items []json.RawMessage
		if err := json.Unmarshal(raw, &items); err != nil {
			return nil, err
		}
		result := []byte{'['}
		for _, item := range items {
			value, err := canonicalHistoryValue(item)
			if err != nil {
				return nil, err
			}
			result = appendCanonicalPart(result, value)
		}
		return append(result, ']'), nil
	}
	if raw[0] != '{' {
		return append([]byte(nil), raw...), nil
	}
	type field struct {
		name  string
		key   []byte
		value []byte
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	if _, err := dec.Token(); err != nil {
		return nil, err
	}
	var fields []field
	seen := make(map[string]bool)
	duplicate := false
	for dec.More() {
		before := dec.InputOffset()
		token, err := dec.Token()
		if err != nil {
			return nil, err
		}
		name := token.(string)
		key := bytes.TrimSpace(raw[before:dec.InputOffset()])
		key = bytes.TrimSpace(bytes.TrimPrefix(key, []byte(",")))
		duplicate = duplicate || seen[name]
		seen[name] = true
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return nil, err
		}
		normalized, err := canonicalHistoryValue(value)
		if err != nil {
			return nil, err
		}
		fields = append(fields, field{name: name, key: key, value: normalized})
	}
	if !duplicate {
		sort.Slice(fields, func(i, j int) bool { return fields[i].name < fields[j].name })
	}
	result := []byte{'{'}
	for _, field := range fields {
		result = appendCanonicalPart(result, field.key)
		result = appendCanonicalPart(result, field.value)
	}
	return append(result, '}'), nil
}

func appendCanonicalPart(dst, part []byte) []byte {
	dst = binary.BigEndian.AppendUint64(dst, uint64(len(part)))
	return append(dst, part...)
}
