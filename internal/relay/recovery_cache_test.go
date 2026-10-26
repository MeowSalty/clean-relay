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
	first, reason, added := c.freeze(key, absent, cacheSelectors(t, cacheOld, 1))
	if reason != "scope_created" || !first.cached || added != 2 {
		t.Fatalf("freeze: %s", reason)
	}
	concurrent, reason, added := c.freeze(key, absent, cacheSelectors(t, cacheNew, 2))
	if reason != "scope_extended" || added != 1 || len(concurrent.scope.items) != 3 ||
		concurrent.scope.generation != first.scope.generation || concurrent.scope.revision <= first.scope.revision {
		t.Fatal("a concurrent request did not extend the same cache lifetime")
	}
	sentIDs, exact := c.sent(key, first, []bool{true, false})
	if exact || first.scope.items[0].applied || concurrent.scope.items[0].applied ||
		!sentIDs.scope.items[0].applied || sentIDs.scope.items[2].applied {
		t.Fatal("stale send lost its action, changed a published snapshot or certified later items")
	}
	sentStrong, exact := c.sent(key, sentIDs, []bool{true, true, false})
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
	// Revision changes, including completion, never gate candidate merging.
	reused, reason, added := c.freeze(key, first, cacheSelectors(t, cacheNew, 3))
	if reason != "scope_reused" || added != 0 || reused.scope.generation != first.scope.generation || reused.scope.accepted {
		t.Fatal("a repeated error changed the lifetime or retained completion")
	}
	next, reason, added := c.freeze(key, accepted, cacheSelectors(t, `{"input":[{"id":"last","role":"user","content":"last"}]}`, 4))
	if reason != "scope_extended" || added != 1 || next.scope.generation != first.scope.generation ||
		len(next.scope.items) != 4 || !next.scope.items[0].applied || !next.scope.items[1].applied || next.scope.items[2].applied || next.scope.items[3].applied {
		t.Fatalf("short history lost prior actions or did not add its own candidate: %s", reason)
	}
	if c.confirm(key, sentStrong) {
		t.Fatal("old completion confirmed a newer revision")
	}
}

func TestRecoveryFailureDoesNotGateLaterExpansion(t *testing.T) {
	c := testRecoveryStore()
	key := cacheKey(1, 1)
	first, _, _ := c.freeze(key, c.load(key), cacheSelectors(t, cacheOld, 1))
	sent, _ := c.sent(key, first, []bool{true, true})
	c.confirm(key, sent)
	before := c.load(key)
	failed, _, _ := c.freeze(key, before, cacheSelectors(t, cacheOld, 2))
	if failed.scope.accepted || failed.scope.generation != before.scope.generation {
		t.Fatal("failure with no new candidate retained completion or changed lifetime")
	}
	next, _, _ := c.freeze(key, failed, cacheSelectors(t, cacheNew, 3))
	if len(next.scope.items) != 3 || !next.scope.items[0].applied || !next.scope.items[1].applied || next.scope.items[2].applied {
		t.Fatal("persistent error failed to merge later history without losing prior actions")
	}
}

func TestRecoveryCacheExpiryLRUAndNoResurrection(t *testing.T) {
	c := testRecoveryStore()
	c.config.MaxSessions = 2
	now := time.Unix(100, 0)
	c.now = func() time.Time { return now }
	key1, key2, key3 := cacheKey(1, 1), cacheKey(2, 1), cacheKey(3, 1)
	selectors := cacheSelectors(t, cacheOld, 1)
	first, _, _ := c.freeze(key1, c.load(key1), selectors)
	staleAbsence := c.load(key3)
	now = now.Add(time.Minute)
	second, _, _ := c.freeze(key2, c.load(key2), selectors)
	now = now.Add(time.Minute)
	c.touch(key1, first)
	third, _, _ := c.freeze(key3, c.load(key3), selectors)
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
	if got, reason, added := c.freeze(key3, staleAbsence, selectors); got.scope != nil || reason != "scope_changed" || added != 0 {
		t.Fatal("an old absent-cache request recreated an evicted scope")
	}
	if c.confirm(key3, third) {
		t.Fatal("old completion recreated expired state")
	}
	replacement, _, _ := c.freeze(key3, c.load(key3), selectors)
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
			view, reason, added := c.freeze(other, c.load(other), cacheSelectors(t, cacheOld, 2))
			if reason != "scope_cache_budget" || added != 0 || view.cached || c.load(key).scope != nil || c.load(other).scope != nil || c.bytes != 0 {
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

func TestRecoveryCacheOldSnapshotsCannotJoinReplacement(t *testing.T) {
	for _, eviction := range []string{"expiry", "lru", "budget"} {
		t.Run(eviction, func(t *testing.T) {
			c := testRecoveryStore()
			now := time.Unix(100, 0)
			c.now = func() time.Time { return now }
			key := cacheKey(1, 1)
			absent := c.load(key)
			first, _, _ := c.freeze(key, absent, cacheSelectors(t, cacheOld, 1))
			switch eviction {
			case "expiry":
				now = now.Add(2 * time.Hour)
			case "lru":
				c.config.MaxSessions = 1
				other := cacheKey(2, 1)
				c.freeze(other, c.load(other), cacheSelectors(t, cacheOld, 2))
			case "budget":
				c.config.MaxItemsPerSession = 2
				local, reason, added := c.freeze(key, first, cacheSelectors(t, cacheNew, 3))
				if local.cached || reason != "scope_cache_budget" || added != 0 {
					t.Fatal("oversized extension was cached")
				}
			}
			if c.load(key).scope != nil {
				t.Fatal("test did not evict the original record")
			}
			for _, old := range []recoverySnapshot{absent, first} {
				if got, reason, added := c.freeze(key, old, cacheSelectors(t, cacheNew, 4)); got.scope != nil || reason != "scope_changed" || added != 0 {
					t.Fatal("an old snapshot recreated an evicted record")
				}
			}
			replacement, _, _ := c.freeze(key, c.load(key), cacheSelectors(t, cacheOld, 5))
			for _, old := range []recoverySnapshot{absent, first} {
				if got, reason, added := c.freeze(key, old, cacheSelectors(t, cacheNew, 6)); got.scope != nil || reason != "scope_changed" || added != 0 {
					t.Fatal("an old hit or miss joined the replacement record")
				}
			}
			if written, exact := c.sent(key, first, []bool{true, true}); written.scope != nil || exact || c.confirm(key, first) {
				t.Fatal("an evicted writer or completion affected the replacement")
			}
			if c.load(key).scope != replacement.scope || replacement.scope.generation == first.scope.generation {
				t.Fatal("old work changed the replacement lifetime or revision")
			}
		})
	}
}

func TestRecoveryCachePendingAnchorsCountAgainstExtensionBudget(t *testing.T) {
	const expanded = `{"input":[{"id":"old","role":"user","content":"old"},{"type":"reasoning","summary":[]},{"id":"right","role":"assistant","content":"right"},{"type":"reasoning","id":"old-r"}]}`
	for _, budget := range []string{"anchors", "bytes"} {
		t.Run(budget, func(t *testing.T) {
			c := testRecoveryStore()
			key := cacheKey(1, 1)
			first, _, _ := c.freeze(key, c.load(key), cacheSelectors(t, cacheOld, 1))
			written, _ := c.sent(key, first, []bool{true, true})
			if budget == "anchors" {
				c.config.MaxItemsPerSession = 5 // Two old items plus one three-unit anonymous selector already fill it.
			} else {
				c.config.MaxBytes = recoveryConversationBytes + recoveryNamespaceBytes + 3*recoveryItemBytes
			}
			local, reason, added := c.freeze(key, written, cacheSelectors(t, expanded, 2))
			units, _ := scopeSize(local.scope)
			if reason != "scope_cache_budget" || added != 0 || local.cached || units != 6 ||
				c.load(key).scope != nil || c.bytes != 0 || !local.scope.items[0].applied || !local.scope.items[1].applied {
				t.Fatalf("pending candidates or anchors escaped accounting: reason=%s units=%d bytes=%d", reason, units, c.bytes)
			}
			if view, exact := c.sent(key, local, []bool{true, true, true, true}); view.scope != nil || exact {
				t.Fatal("local repair recreated its evicted group")
			}
		})
	}
}

func TestRecoveryCacheConcurrentMissPreservesLongerHistory(t *testing.T) {
	c := testRecoveryStore()
	key := cacheKey(1, 1)
	shortMiss, longMiss := c.load(key), c.load(key)
	selectors := cacheSelectors(t, cacheNew, 1)
	// Duplicate candidates count once, including duplicates within one merge.
	long, _, added := c.freeze(key, longMiss, append(selectors, selectors...))
	if added != 3 {
		t.Fatalf("duplicate candidates counted %d times", added)
	}
	short, reason, added := c.freeze(key, shortMiss, cacheSelectors(t, cacheOld, 2))
	if reason != "scope_reused" || added != 0 || short.scope.generation != long.scope.generation || len(short.scope.items) != 3 {
		t.Fatal("short concurrent history truncated the longer range")
	}
	for i := range long.scope.items {
		if long.scope.items[i].selector != short.scope.items[i].selector {
			t.Fatal("reusing the range reordered its selector prefix")
		}
	}
}
