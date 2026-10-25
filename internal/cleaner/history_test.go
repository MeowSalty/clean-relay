package cleaner_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"sync"
	"testing"

	"cleanrelay/internal/cleaner"
)

func TestHistoryItemEligibility(t *testing.T) {
	cases := []struct {
		item string
		id   bool
	}{
		{`{"id":"m","role":"user","content":""}`, true},
		{`{"type":"message","id":"m","role":"assistant","content":[],"status":"completed"}`, true},
		{`{"id":"m","role":"system","content":"x"}`, true},
		{`{"id":"m","role":"developer","content":[]}`, true},
		{`{"type":"function_call","id":"f","call_id":"c","name":"f","arguments":""}`, true},
		{`{"type":"function_call_output","id":"f","call_id":"c","output":[]}`, true},
		{`{"type":"custom_tool_call","id":"f","call_id":"c","name":"f","input":"raw"}`, true},
		{`{"type":"custom_tool_call_output","id":"f","call_id":"c","output":""}`, true},
		{`{"id":"only"}`, false},
		{`{"id":"","role":"user","content":"x"}`, false},
		{`{"id":1,"role":"user","content":"x"}`, false},
		{`{"id":null,"role":"user","content":"x"}`, false},
		{`{"id":"m","role":"tool","content":"x"}`, false},
		{`{"id":"m","role":"user","content":null}`, false},
		{`{"id":"m","role":"user","content":{}}`, false},
		{`{"id":"m","role":"user","content":"x","status":"in_progress"}`, false},
		{`{"id":"m","role":"user","content":"x","status":null}`, false},
		{`{"id":"m","role":"user","content":"x","type":null}`, false},
		{`{"id":"m","role":"user","content":"x","type":""}`, false},
		{`{"type":"function_call","id":"f","call_id":"","name":"f","arguments":"{}"}`, false},
		{`{"type":"function_call","id":"f","call_id":"c","name":"","arguments":"{}"}`, false},
		{`{"type":"function_call","id":"f","call_id":"c","name":"f","arguments":{}}`, false},
		{`{"type":"custom_tool_call","id":"f","call_id":"c","name":"f","input":null}`, false},
		{`{"type":"custom_tool_call_output","id":"f","call_id":"c","output":null}`, false},
		{`{"type":"compaction","id":"p","encrypted_content":"opaque"}`, false},
		{`{"type":"reasoning","id":"r","encrypted_content":"opaque"}`, false},
		{`{"type":"item_reference","id":"ref"}`, false},
		{`{"type":"future","id":"f","role":"user","content":"x"}`, false},
		{`null`, false}, {`"opaque"`, false}, {`42`, false},
	}
	for _, tc := range cases {
		t.Run(tc.item, func(t *testing.T) {
			body := []byte(`{"input":[` + tc.item + `]}`)
			original := bytes.Clone(body)
			analysis, err := cleaner.AnalyzeHistory(body, cleaner.PortableIDs)
			if err != nil || analysis.Candidate != tc.id {
				t.Fatalf("analysis = %+v, %v", analysis, err)
			}
			result, err := cleaner.RepairHistory(body, cleaner.PortableIDs)
			if err != nil || (result.RemovedItemIDs == 1) != tc.id || result.InputItems != 1 {
				t.Fatalf("repair = %+v, %v", result, err)
			}
			if analysis.PortableItemIDs != result.RemovedItemIDs {
				t.Fatalf("ID candidate count = %d, removals = %d", analysis.PortableItemIDs, result.RemovedItemIDs)
			}
			if !bytes.Equal(body, original) {
				t.Fatal("input mutated")
			}
			if !tc.id && !bytes.Equal(body, result.Body) {
				t.Fatal("noncandidate changed")
			}
			again, err := cleaner.RepairHistory(result.Body, cleaner.PortableIDs)
			if err != nil || !bytes.Equal(again.Body, result.Body) || again.RemovedItemIDs != 0 {
				t.Fatal("repair is not idempotent")
			}
		})
	}
}

func TestHistoryModesAndOpaqueContent(t *testing.T) {
	body := []byte(`{"previous_response_id":"old","conversation":{"id":"conv"},"context_management":[{"type":"compaction"}],"include":["reasoning.encrypted_content"],"store":true,"model":"m","stream":true,"reasoning":{"effort":"high"},"n":9007199254740993123456789,"input":[{"id":"m","role":"assistant","content":[{"type":"image","file_id":"nested","id":"nested-id","n":1.234567890123456789e+99}]},{"type":"function_call","id":"f","call_id":"pair","name":"fn","arguments":"{  \"n\": 9007199254740993123456789 }"},{"type":"function_call_output","id":"o","call_id":"pair","output":"raw"},{"type":"reasoning","id":"r","summary":[{"text":"x"}],"encrypted_content":"reasoning-opaque"},{"type":"compaction","id":"p","encrypted_content":"compaction-opaque"},{"type":"item_reference","id":"ref"},{"type":"unknown","id":"u","extension":{"x":1,"x":2}}]}`)
	var before map[string]json.RawMessage
	if err := json.Unmarshal(body, &before); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []cleaner.HistoryMode{cleaner.PortableIDs, cleaner.PortableIDsAndReasoning} {
		t.Run(string(mode), func(t *testing.T) {
			result, err := cleaner.RepairHistory(body, mode)
			wantReasoning := 0
			if mode == cleaner.PortableIDsAndReasoning {
				wantReasoning = 1
			}
			if err != nil || result.RemovedItemIDs != 3 || result.RemovedReasoningItems != wantReasoning || result.InputItems != 7-wantReasoning {
				t.Fatalf("result = %+v, %v", result, err)
			}
			analysis, err := cleaner.AnalyzeHistory(body, mode)
			if err != nil || analysis.PortableItemIDs != 3 || analysis.ReasoningItems != 1 {
				t.Fatalf("analysis = %+v, %v", analysis, err)
			}
			var after map[string]json.RawMessage
			if err := json.Unmarshal(result.Body, &after); err != nil {
				t.Fatal(err)
			}
			for key, value := range before {
				if key != "input" && !bytes.Equal(value, after[key]) {
					t.Errorf("root field %s changed", key)
				}
			}
			var original, repaired []map[string]json.RawMessage
			_ = json.Unmarshal(before["input"], &original)
			_ = json.Unmarshal(after["input"], &repaired)
			for i := 0; i < 3; i++ {
				delete(original[i], "id")
			}
			if wantReasoning == 1 {
				original = append(original[:3], original[4:]...)
			}
			if !reflect.DeepEqual(original, repaired) {
				t.Fatalf("protected content changed: %s", result.Body)
			}
		})
	}
	result, err := cleaner.RepairHistory([]byte(`{"input":[{"type":"reasoning","status":"in_progress"}]}`), cleaner.PortableIDsAndReasoning)
	if err != nil || result.InputItems != 0 || result.RemovedReasoningItems != 1 || string(result.Body) != `{"input":[]}` {
		t.Fatalf("empty result = %+v, %v", result, err)
	}
}

func TestHistoryValidationAndBackground(t *testing.T) {
	for _, raw := range []string{`[]`, `null`, `"text"`, `{}`, `{"input":"text"}`, `{"input":null}`, `{"input":[]}`} {
		analysis, err := cleaner.AnalyzeHistory([]byte(raw), cleaner.PortableIDs)
		if err != nil || analysis.Candidate {
			t.Fatalf("%s: %+v %v", raw, analysis, err)
		}
	}
	for _, raw := range []string{`{"input":[],"input":[]}`, `{"input":[{"id":"a","\u0069d":"b"}]}`, `{} {}`, `{"input":`, "\xff"} {
		if _, err := cleaner.AnalyzeHistory([]byte(raw), cleaner.PortableIDs); err == nil {
			t.Errorf("accepted %q", raw)
		}
		if _, err := cleaner.RepairHistory([]byte(raw), cleaner.PortableIDs); err == nil {
			t.Errorf("repaired %q", raw)
		}
	}
	analysis, err := cleaner.AnalyzeHistory([]byte(`{"background":true,"input":[{"id":"m","role":"user","content":"x"}]}`), cleaner.PortableIDs)
	if err != nil || !analysis.Background || !analysis.Candidate {
		t.Fatalf("background = %+v %v", analysis, err)
	}
	if _, err := cleaner.RepairHistory([]byte(`{}`), cleaner.HistoryMode("unknown")); err == nil {
		t.Fatal("accepted invalid mode")
	}
}

func TestHistoryConcurrent(t *testing.T) {
	body := []byte(`{"input":[{"id":"m","role":"user","content":"x"},{"type":"reasoning","id":"r"}]}`)
	original := bytes.Clone(body)
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			for _, mode := range []cleaner.HistoryMode{cleaner.PortableIDs, cleaner.PortableIDsAndReasoning} {
				if _, err := cleaner.RepairHistory(body, mode); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	if !bytes.Equal(body, original) {
		t.Fatal("shared input changed")
	}
}

func FuzzHistory(f *testing.F) {
	f.Add([]byte(`{"input":[{"id":"m","role":"user","content":"x"},{"type":"reasoning","id":"r"}]}`))
	f.Add([]byte(`{"input":[{"type":"compaction","id":"p"}]}`))
	f.Fuzz(func(t *testing.T, body []byte) {
		original := bytes.Clone(body)
		for _, mode := range []cleaner.HistoryMode{cleaner.PortableIDs, cleaner.PortableIDsAndReasoning} {
			analysis, analyzeErr := cleaner.AnalyzeHistory(body, mode)
			result, err := cleaner.RepairHistory(body, mode)
			if (analyzeErr == nil) != (err == nil) {
				t.Fatal("analysis/repair validation diverged")
			}
			if err != nil {
				continue
			}
			if !json.Valid(result.Body) || analysis.Candidate != (result.RemovedItemIDs+result.RemovedReasoningItems > 0) {
				t.Fatal("invalid repair")
			}
			if analysis.PortableItemIDs != result.RemovedItemIDs || mode == cleaner.PortableIDsAndReasoning && analysis.ReasoningItems != result.RemovedReasoningItems {
				t.Fatal("candidate counts differ from removals")
			}
			again, err := cleaner.RepairHistory(result.Body, mode)
			if err != nil || !bytes.Equal(result.Body, again.Body) {
				t.Fatal("non-idempotent repair")
			}
			if mode == cleaner.PortableIDs {
				fromOriginal, err := cleaner.RepairHistory(body, cleaner.PortableIDsAndReasoning)
				fromIDs, idsErr := cleaner.RepairHistory(result.Body, cleaner.PortableIDsAndReasoning)
				var originalJSON, idsJSON bytes.Buffer
				if err != nil || idsErr != nil || json.Compact(&originalJSON, fromOriginal.Body) != nil || json.Compact(&idsJSON, fromIDs.Body) != nil || !bytes.Equal(originalJSON.Bytes(), idsJSON.Bytes()) {
					t.Fatal("strong repair differs by baseline")
				}
				if fromOriginal.RemovedItemIDs != result.RemovedItemIDs || fromIDs.RemovedItemIDs != 0 || fromOriginal.RemovedReasoningItems != fromIDs.RemovedReasoningItems {
					t.Fatal("removal counts are not relative to each baseline")
				}
			}
		}
		if !bytes.Equal(body, original) {
			t.Fatal("input changed")
		}
	})
}
