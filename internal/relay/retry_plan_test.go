package relay

import (
	"reflect"
	"testing"

	"cleanrelay/internal/cleaner"
)

func TestHistoryPlans(t *testing.T) {
	ids := cleaner.PortableIDs
	strong := cleaner.PortableIDsAndReasoning
	for _, tc := range []struct {
		name, body string
		p, r       int
		plans      [][]cleaner.HistoryMode
	}{
		{"both", `{"input":[{"id":"m","role":"user","content":"x"},{"type":"reasoning","id":"r"}]}`, 1, 1,
			[][]cleaner.HistoryMode{{ids}, {strong}, {ids, strong}}},
		{"IDs only", `{"input":[{"id":"m","role":"user","content":"x"}]}`, 1, 0,
			[][]cleaner.HistoryMode{{ids}, {strong}, {ids}}},
		{"reasoning without ID", `{"input":[{"role":"user","content":"x"},{"type":"reasoning","status":"in_progress"}]}`, 0, 1,
			[][]cleaner.HistoryMode{nil, {strong}, {strong}}},
		{"no candidate", `{"input":[{"id":"incomplete"},{"type":"compaction","id":"c"},{"type":"item_reference","id":"r"},{"type":"Reasoning"}]}`, 0, 0,
			[][]cleaner.HistoryMode{nil, nil, nil}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			analysis, err := cleaner.AnalyzeHistory([]byte(tc.body), cleaner.PortableIDsAndReasoning)
			if err != nil || analysis.PortableItemIDs != tc.p || analysis.ReasoningItems != tc.r {
				t.Fatalf("analysis = %+v, %v", analysis, err)
			}
			for i, mode := range []string{"portable_ids", "portable_ids_and_reasoning", "portable_ids_then_reasoning"} {
				if plan := historyPlan(mode, analysis); !reflect.DeepEqual(plan, tc.plans[i]) {
					t.Errorf("%s plan = %v, want %v", mode, plan, tc.plans[i])
				}
			}
		})
	}
	if _, err := cleaner.RepairHistory([]byte(`{}`), cleaner.HistoryMode("portable_ids_then_reasoning")); err == nil {
		t.Fatal("scheduling strategy accepted as a pure transformation")
	}
}
