package relay

import (
	"crypto/sha256"
	"testing"
	"time"

	"cleanrelay/internal/cleaner"
	"cleanrelay/internal/config"
)

func cacheKey(conversation, namespace byte) recoveryKey {
	return recoveryKey{conversation: cleaner.Fingerprint{conversation}, namespace: cleaner.Fingerprint{namespace}}
}

func cacheSelectors(t *testing.T, body string, serial uint64) []cleaner.HistorySelector {
	t.Helper()
	snapshot, err := cleaner.SnapshotHistory([]byte(body), serial, func(value []byte) cleaner.Fingerprint { return sha256.Sum256(value) })
	if err != nil {
		t.Fatal(err)
	}
	return snapshot.Selectors()
}

const cacheOld = `{"input":[{"id":"old","role":"user","content":"old"},{"type":"reasoning","id":"old-r"}]}`
const cacheNew = `{"input":[{"id":"old","role":"user","content":"old"},{"type":"reasoning","id":"old-r"},{"id":"new","role":"assistant","content":"new"}]}`

func testRecoveryStore() *recoveryStore {
	return newRecoveryStore(config.SessionCacheConfig{IdleTTL: time.Hour, MaxSessions: 3, MaxItemsPerSession: 30, MaxBytes: 1 << 20})
}

func TestRecoveryCacheVersionedLifecycle(t *testing.T) {
	c := testRecoveryStore()
	key := cacheKey(1, 1)
	absent := c.load(key)
	first, reason := c.freeze(key, absent, cacheSelectors(t, cacheOld, 1))
	if reason != "scope_created" || !first.cached {
		t.Fatalf("freeze: %s", reason)
	}
	concurrent, reason := c.freeze(key, absent, cacheSelectors(t, cacheNew, 2))
	if reason != "scope_reused" || len(concurrent.scope.items) != 2 {
		t.Fatal("a later concurrent request enlarged the initial boundary")
	}
	sentIDs, exact := c.sent(key, first, []bool{true, false})
	if !exact || first.scope.items[0].applied || first.scope.revision == sentIDs.scope.revision {
		t.Fatal("sent update mutated a published snapshot")
	}
	sentStrong, exact := c.sent(key, sentIDs, []bool{true, true})
	if !exact || c.confirm(key, sentIDs) {
		t.Fatal("an older weak response confirmed a stronger version")
	}
	stale, exact := c.sent(key, first, []bool{true, false})
	if exact || !stale.scope.items[1].applied {
		t.Fatal("a stale writer overwrote a stronger action")
	}
	if !c.confirm(key, sentStrong) {
		t.Fatal("the exact written version was not confirmed")
	}
	accepted := c.load(key)
	// The original request did not have completion evidence at its dispatch.
	reused, _ := c.freeze(key, first, cacheSelectors(t, cacheNew, 3))
	if reused.scope.generation != first.scope.generation {
		t.Fatal("a stale request borrowed a later completion")
	}
	next, reason := c.freeze(key, accepted, cacheSelectors(t, cacheNew, 4))
	if reason != "scope_extended" || next.scope.generation == first.scope.generation ||
		len(next.scope.items) != 3 || !next.scope.items[0].applied || !next.scope.items[1].applied || next.scope.items[2].applied {
		t.Fatalf("next generation lost prior actions: %s", reason)
	}
	if c.confirm(key, sentStrong) {
		t.Fatal("old generation overwrote a newer fault")
	}
}

func TestRecoveryFailedWithoutNewItemsRevokesExpansionEvidence(t *testing.T) {
	c := testRecoveryStore()
	key := cacheKey(1, 1)
	first, _ := c.freeze(key, c.load(key), cacheSelectors(t, cacheOld, 1))
	sent, _ := c.sent(key, first, []bool{true, true})
	c.confirm(key, sent)
	before := c.load(key)
	failed, _ := c.freeze(key, before, cacheSelectors(t, cacheOld, 2))
	if failed.scope.accepted || failed.scope.generation != before.scope.generation {
		t.Fatal("failure with no new candidate retained expansion evidence")
	}
	next, _ := c.freeze(key, failed, cacheSelectors(t, cacheNew, 3))
	if len(next.scope.items) != 2 {
		t.Fatal("persistent error absorbed later history")
	}
}

func TestRecoveryCacheExpiryLRUAndNoResurrection(t *testing.T) {
	c := testRecoveryStore()
	c.config.MaxSessions = 2
	now := time.Unix(100, 0)
	c.now = func() time.Time { return now }
	key1, key2, key3 := cacheKey(1, 1), cacheKey(2, 1), cacheKey(3, 1)
	selectors := cacheSelectors(t, cacheOld, 1)
	first, _ := c.freeze(key1, c.load(key1), selectors)
	staleAbsence := c.load(key3)
	now = now.Add(time.Minute)
	second, _ := c.freeze(key2, c.load(key2), selectors)
	now = now.Add(time.Minute)
	c.touch(key1, first)
	third, _ := c.freeze(key3, c.load(key3), selectors)
	if c.load(key1).scope == nil || c.load(key2).scope != nil || c.bytes > c.config.MaxBytes {
		t.Fatal("LRU did not retain the actively matched scope")
	}
	if _, exact := c.sent(key2, second, []bool{true, true}); exact {
		t.Fatal("evicted scope was resurrected by an in-flight writer")
	}
	now = now.Add(2 * time.Hour)
	if c.load(key1).scope != nil || c.load(key3).scope != nil || c.bytes != 0 {
		t.Fatal("expired state was retained")
	}
	if got, reason := c.freeze(key3, staleAbsence, selectors); got.scope != nil || reason != "scope_changed" {
		t.Fatal("an old absent-cache request recreated an evicted scope")
	}
	if c.confirm(key3, third) {
		t.Fatal("old completion recreated expired state")
	}
	replacement, _ := c.freeze(key3, c.load(key3), selectors)
	if replacement.scope.generation == third.scope.generation {
		t.Fatal("generation token was reused after eviction")
	}
}

func TestRecoveryCacheBudgetsCoverWholeConversation(t *testing.T) {
	for _, budget := range []string{"items", "bytes"} {
		t.Run(budget, func(t *testing.T) {
			c := testRecoveryStore()
			if budget == "items" {
				c.config.MaxItemsPerSession = 3
			} else {
				c.config.MaxBytes = recoveryConversationBytes + recoveryNamespaceBytes + 2*recoveryItemBytes
			}
			key := cacheKey(1, 1)
			c.freeze(key, c.load(key), cacheSelectors(t, cacheOld, 1))
			other := cacheKey(1, 2)
			view, reason := c.freeze(other, c.load(other), cacheSelectors(t, cacheOld, 2))
			if reason != "scope_cache_budget" || view.cached || c.load(key).scope != nil || c.load(other).scope != nil || c.bytes != 0 {
				t.Fatalf("oversized conversation was partially retained: %s", reason)
			}
			if _, exact := c.sent(other, view, []bool{true, true}); exact {
				t.Fatal("uncached repair became persistent from its write callback")
			}
		})
	}
	c := testRecoveryStore()
	c.config.MaxBytes = 2 * (recoveryConversationBytes + recoveryNamespaceBytes + 2*recoveryItemBytes)
	for i := byte(1); i <= 3; i++ {
		key := cacheKey(i, 1)
		c.freeze(key, c.load(key), cacheSelectors(t, cacheOld, uint64(i)))
	}
	if c.load(cacheKey(1, 1)).scope != nil || len(c.conversations) != 2 || c.bytes > c.config.MaxBytes {
		t.Fatal("global byte budget did not evict whole least-recently-used conversations")
	}
}
