package cleaner_test

import (
	"bytes"
	"crypto/sha256"
	"strings"
	"testing"

	"cleanrelay/internal/cleaner"
)

func scopeBody(items ...string) []byte {
	return []byte(`{"n":9007199254740993123456789,"previous_response_id":"protected","input":[` + strings.Join(items, ",") + `]}`)
}

func scopeDigest(value []byte) cleaner.Fingerprint { return sha256.Sum256(value) }

func scopeSnapshot(t *testing.T, body []byte, serial uint64) cleaner.HistorySnapshot {
	t.Helper()
	snapshot, err := cleaner.SnapshotHistory(body, serial, scopeDigest)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestScopeProtectsNewAndEditedItems(t *testing.T) {
	oldMessage := `{"id":"old-message","role":"assistant","content":[{"text":"old","n":9007199254740993123456789}]}`
	oldReasoning := `{"type":"reasoning","id":"old-reasoning","encrypted_content":"old-cipher"}`
	old := scopeSnapshot(t, scopeBody(oldMessage, oldReasoning), 1)
	newMessage := `{"id":"new-message","role":"assistant","content":"new"}`
	newReasoning := `{"type":"reasoning","id":"new-reasoning","encrypted_content":"new-cipher"}`
	body := scopeBody(
		`{"content":[{"n":9007199254740993123456789,"text":"old"}],"role":"assistant","id":"old-message"}`,
		oldReasoning, newMessage, newReasoning)
	before := bytes.Clone(body)
	actions, matched, unaligned := scopeSnapshot(t, body, 2).Match(old.Selectors())
	result, err := cleaner.RepairSelectedHistory(body, actions)
	if err != nil || unaligned || len(matched) != 2 || !matched[0] || !matched[1] ||
		result.RemovedItemIDs != 1 || result.RemovedReasoningItems != 1 || result.InputItems != 3 {
		t.Fatalf("scope repair: %+v, matches=%v unaligned=%v error=%v", result, matched, unaligned, err)
	}
	for _, text := range []string{"new-message", "new-reasoning", "new-cipher", "9007199254740993123456789", "protected"} {
		if !bytes.Contains(result.Body, []byte(text)) {
			t.Fatalf("lost protected value %s", text)
		}
	}
	if bytes.Contains(result.Body, []byte("old-message")) || bytes.Contains(result.Body, []byte("old-cipher")) || !bytes.Equal(body, before) {
		t.Fatal("old selectors did not apply independently of the source body")
	}
	edited := scopeBody(strings.ReplaceAll(oldMessage, "3123456789", "3123456790"),
		strings.ReplaceAll(oldReasoning, "old-cipher", "edited-cipher"))
	actions, _, _ = scopeSnapshot(t, edited, 3).Match(old.Selectors())
	result, err = cleaner.RepairSelectedHistory(edited, actions)
	if err != nil || !bytes.Equal(result.Body, edited) || result.RemovedItemIDs+result.RemovedReasoningItems != 0 {
		t.Fatal("an edited item matched an old ID")
	}
}

func TestScopeCanonicalizationPreservesDuplicateOrder(t *testing.T) {
	original := scopeBody(`{"id":"m","role":"user","content":"x","extra":{"a":1,"b":[2,3],"duplicate":{"x":1,"x":2}}}`)
	old := scopeSnapshot(t, original, 1)
	for _, tc := range []struct {
		item  string
		match bool
	}{
		{`{"extra":{"duplicate":{"x":1,"x":2},"b":[2,3],"a":1},"content":"x","id":"m","role":"user"}`, true},
		{`{"id":"m","role":"user","content":"x","extra":{"a":1,"b":[2,3],"duplicate":{"x":2,"x":1}}}`, false},
		{`{"id":"m","role":"user","content":"x","extra":{"a":1,"b":[3,2],"duplicate":{"x":1,"x":2}}}`, false},
		{`{"id":"m","role":"user","content":"x","extra":{"a":1.0,"b":[2,3],"duplicate":{"x":1,"x":2}}}`, false},
	} {
		body := scopeBody(tc.item)
		actions, _, _ := scopeSnapshot(t, body, 2).Match(old.Selectors())
		result, err := cleaner.RepairSelectedHistory(body, actions)
		if err != nil || (result.RemovedItemIDs == 1) != tc.match {
			t.Fatalf("canonical matching = %+v, %v", result, err)
		}
	}
}

func TestScopeCipherIdentityAndAnonymousAnchors(t *testing.T) {
	left := `{"id":"left","role":"user","content":"left"}`
	right := `{"id":"right","role":"assistant","content":"right"}`
	anonymous := `{"type":"reasoning","summary":[]}`
	old := scopeSnapshot(t, scopeBody(left, anonymous, anonymous, right), 1)
	var reasoning []cleaner.HistorySelector
	for _, selector := range old.Selectors() {
		if selector.Action == cleaner.RemoveHistoryReasoning {
			reasoning = append(reasoning, selector)
			if selector.Units() != 3 {
				t.Fatal("anonymous anchors were not budgeted")
			}
		}
	}
	for _, tc := range []struct {
		name      string
		body      []byte
		removed   int
		unaligned bool
	}{
		{"append after right anchor", scopeBody(left, anonymous, anonymous, right, anonymous), 2, false},
		{"insert duplicate in interval", scopeBody(left, anonymous, anonymous, anonymous, right), 0, true},
		{"missing left anchor", scopeBody(anonymous, anonymous, right), 0, true},
		{"nonunique anchor", scopeBody(left, anonymous, anonymous, right, left), 0, true},
		{"changed right anchor", scopeBody(left, anonymous, anonymous, strings.ReplaceAll(right, "assistant", "user")), 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actions, _, unaligned := scopeSnapshot(t, tc.body, 2).Match(reasoning)
			result, err := cleaner.RepairSelectedHistory(tc.body, actions)
			if err != nil || result.RemovedReasoningItems != tc.removed || unaligned != tc.unaligned {
				t.Fatalf("anchor repair = %+v, unaligned=%v, %v", result, unaligned, err)
			}
		})
	}
	cipher := scopeSnapshot(t, scopeBody(`{"type":"reasoning","encrypted_content":"old","summary":[]}`), 3)
	body := scopeBody(left, `{"type":"reasoning","encrypted_content":"old","summary":[{"text":"edited summary"}]}`,
		`{"type":"reasoning","encrypted_content":"new","summary":[]}`)
	actions, _, _ := scopeSnapshot(t, body, 4).Match(cipher.Selectors())
	result, err := cleaner.RepairSelectedHistory(body, actions)
	if err != nil || result.RemovedReasoningItems != 1 || !bytes.Contains(result.Body, []byte(`"encrypted_content":"new"`)) {
		t.Fatalf("cipher identity = %+v, %v", result, err)
	}
}

func TestScopeUnanchoredItemsAreRequestLocal(t *testing.T) {
	body := scopeBody(`{"role":"user","content":"x"}`, `{"type":"reasoning"}`)
	old := scopeSnapshot(t, body, 1)
	actions, _, unaligned := old.Match(old.Selectors())
	result, err := cleaner.RepairSelectedHistory(body, actions)
	if err != nil || unaligned || result.RemovedReasoningItems != 1 {
		t.Fatalf("original snapshot was not repairable: %+v, %v", result, err)
	}
	actions, _, unaligned = scopeSnapshot(t, body, 2).Match(old.Selectors())
	result, err = cleaner.RepairSelectedHistory(body, actions)
	if err != nil || !unaligned || !bytes.Equal(result.Body, body) {
		t.Fatal("anonymous content was matched across requests without anchors")
	}
}

func FuzzScopedHistory(f *testing.F) {
	for _, seed := range []string{
		string(scopeBody(`{"id":"m","role":"user","content":"x"}`, `{"type":"reasoning"}`)),
		string(scopeBody(`{"id":"m","role":"user","content":"x","extra":{"x":1,"x":2}}`)),
		`{"input":[null,42,"opaque",{"type":"compaction","encrypted_content":"protected"}]}`,
		`{"input":[{"id":"a","id":"b"}]}`,
		`[]`,
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		before := bytes.Clone(body)
		snapshot, err := cleaner.SnapshotHistory(body, 1, scopeDigest)
		if err != nil {
			return
		}
		actions, _, _ := snapshot.Match(snapshot.Selectors())
		if _, err := cleaner.RepairSelectedHistory(body, actions); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(body, before) {
			t.Fatal("scope transformation mutated its input")
		}
	})
}
