package relay

import (
	"container/list"
	"sync"
	"time"

	"cleanrelay/internal/cleaner"
	"cleanrelay/internal/config"
)

type recoveryKey struct {
	conversation cleaner.Fingerprint
	namespace    cleaner.Fingerprint
}

type recoveryItem struct {
	selector cleaner.HistorySelector
	applied  bool
}

// Published scopes and their slices are immutable. Readers never hold the
// store lock while hashing, transforming, inspecting or sending a request.
type recoveryScope struct {
	generation uint64
	revision   uint64
	accepted   bool
	items      []recoveryItem
}

type recoverySnapshot struct {
	scope  *recoveryScope
	epoch  uint64
	cached bool
}

type recoveryConversation struct {
	key        cleaner.Fingerprint
	namespaces map[cleaner.Fingerprint]*recoveryScope
	lastSeen   time.Time
	units      int
	bytes      int64
	element    *list.Element
}

// Accounting deliberately exceeds the fixed Go struct sizes and includes map,
// list and slice overhead. Live request snapshots are separately bounded by
// max_body_bytes and max_in_flight; the cache never keeps a request body.
const (
	recoveryConversationBytes int64 = 512
	recoveryNamespaceBytes    int64 = 512
	recoveryItemBytes         int64 = 256
)

type recoveryStore struct {
	mu            sync.Mutex
	config        config.SessionCacheConfig
	now           func() time.Time
	conversations map[cleaner.Fingerprint]*recoveryConversation
	lru           list.List
	bytes         int64
	generation    uint64
	// Invalidation epoch also protects requests which observed an absent scope:
	// after eviction they may not recreate an older, already discarded fault.
	epoch uint64
}

func newRecoveryStore(cfg config.SessionCacheConfig) *recoveryStore {
	return &recoveryStore{
		config: cfg, now: time.Now, epoch: 1,
		conversations: make(map[cleaner.Fingerprint]*recoveryConversation),
	}
}

func (c *recoveryStore) load(key recoveryKey) recoverySnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.expire()
	return c.snapshot(key)
}

func (c *recoveryStore) snapshot(key recoveryKey) recoverySnapshot {
	view := recoverySnapshot{epoch: c.epoch}
	if conversation := c.conversations[key.conversation]; conversation != nil {
		view.scope = conversation.namespaces[key.namespace]
		view.cached = view.scope != nil
	}
	return view
}

func sameRecoveryVersion(a, b *recoveryScope) bool {
	return a != nil && b != nil && a.generation == b.generation && a.revision == b.revision
}

func (c *recoveryStore) touch(key recoveryKey, view recoverySnapshot) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.expire()
	current := c.snapshot(key)
	if view.scope != nil && current.scope != nil && view.scope.generation == current.scope.generation {
		conversation := c.conversations[key.conversation]
		conversation.lastSeen = c.now()
		c.lru.MoveToBack(conversation.element)
	}
}

// freeze uses evidence available when the incoming request was prepared, not
// a completion which raced ahead while that request was in flight.
func (c *recoveryStore) freeze(key recoveryKey, before recoverySnapshot, selectors []cleaner.HistorySelector) (recoverySnapshot, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.expire()
	current := c.snapshot(key)
	if before.scope != nil && (current.scope == nil || current.scope.generation != before.scope.generation) {
		return recoverySnapshot{}, "scope_changed"
	}
	if before.scope == nil && current.scope == nil && before.epoch != c.epoch {
		return recoverySnapshot{}, "scope_changed"
	}
	if current.scope != nil && !(sameRecoveryVersion(before.scope, current.scope) && before.scope.accepted) {
		return current, "scope_reused"
	}
	seen := make(map[cleaner.HistorySelector]bool)
	var prior []recoveryItem
	if current.scope != nil {
		prior = current.scope.items
		for _, item := range prior {
			seen[item.selector] = true
		}
	}
	var added []recoveryItem
	for _, selector := range selectors {
		if !seen[selector] {
			added = append(added, recoveryItem{selector: selector})
			seen[selector] = true
		}
	}
	if len(added) == 0 {
		if current.scope == nil {
			return current, "no_candidate"
		}
		// A fresh failure without new candidates does not create a generation,
		// nor leave an old completion available to justify future expansion.
		next := *current.scope
		next.accepted = false
		next.revision++
		c.conversations[key.conversation].namespaces[key.namespace] = &next
		current.scope = &next
		return current, "scope_reused"
	}
	c.generation++
	scope := &recoveryScope{
		generation: c.generation, revision: 1,
		items: make([]recoveryItem, len(prior)+len(added)),
	}
	copy(scope.items, prior)
	copy(scope.items[len(prior):], added)
	view := recoverySnapshot{scope: scope, epoch: c.epoch}
	if !c.install(key, scope) {
		view.epoch = c.epoch
		return view, "scope_cache_budget"
	}
	view.cached, view.epoch = true, c.epoch
	if len(prior) == 0 {
		return view, "scope_created"
	}
	return view, "scope_extended"
}

func scopeSize(scope *recoveryScope) (int, int64) {
	units := 0
	for _, item := range scope.items {
		units += item.selector.Units()
	}
	return units, recoveryNamespaceBytes + int64(len(scope.items))*recoveryItemBytes
}

func (c *recoveryStore) install(key recoveryKey, scope *recoveryScope) bool {
	conversation := c.conversations[key.conversation]
	units, size := scopeSize(scope)
	totalUnits, totalBytes := units, size+recoveryConversationBytes
	if conversation != nil {
		totalUnits += conversation.units
		totalBytes += conversation.bytes - recoveryConversationBytes
		if old := conversation.namespaces[key.namespace]; old != nil {
			oldUnits, oldSize := scopeSize(old)
			totalUnits -= oldUnits
			totalBytes -= oldSize
		}
	}
	if totalUnits > c.config.MaxItemsPerSession || totalBytes > c.config.MaxBytes {
		if conversation != nil {
			c.remove(conversation)
		}
		return false
	}
	if conversation == nil {
		conversation = &recoveryConversation{
			key: key.conversation, namespaces: make(map[cleaner.Fingerprint]*recoveryScope),
		}
		conversation.element = c.lru.PushBack(conversation)
		c.conversations[key.conversation] = conversation
	}
	c.bytes += totalBytes - conversation.bytes
	conversation.units, conversation.bytes = totalUnits, totalBytes
	conversation.namespaces[key.namespace] = scope
	conversation.lastSeen = c.now()
	c.lru.MoveToBack(conversation.element)
	for len(c.conversations) > c.config.MaxSessions || c.bytes > c.config.MaxBytes {
		c.remove(c.lru.Front().Value.(*recoveryConversation))
	}
	return c.conversations[key.conversation] != nil
}

// sent publishes only actions whose request was actually written. Concurrent
// same-generation updates merge monotonically. A stale writer can add actions
// but cannot certify that its response covers the newer version.
func (c *recoveryStore) sent(key recoveryKey, before recoverySnapshot, desired []bool) (recoverySnapshot, bool) {
	if before.scope == nil || len(desired) != len(before.scope.items) {
		return recoverySnapshot{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.expire()
	current := c.snapshot(key)
	if !before.cached || current.scope == nil || current.scope.generation != before.scope.generation {
		return recoverySnapshot{}, false
	}
	exact := sameRecoveryVersion(before.scope, current.scope)
	changed := false
	for i, apply := range desired {
		changed = changed || apply && !current.scope.items[i].applied
	}
	if changed {
		next := *current.scope
		next.items = append([]recoveryItem(nil), current.scope.items...)
		for i, apply := range desired {
			next.items[i].applied = next.items[i].applied || apply
		}
		next.revision++
		next.accepted = false
		c.conversations[key.conversation].namespaces[key.namespace] = &next
		current.scope = &next
	}
	return current, exact
}

func (c *recoveryStore) confirm(key recoveryKey, sent recoverySnapshot) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.expire()
	current := c.snapshot(key)
	if !sent.cached || !sameRecoveryVersion(sent.scope, current.scope) {
		return false
	}
	if !current.scope.accepted {
		next := *current.scope
		next.accepted = true
		next.revision++
		c.conversations[key.conversation].namespaces[key.namespace] = &next
	}
	return true
}

func (c *recoveryStore) expire() {
	now := c.now()
	for element := c.lru.Front(); element != nil; element = c.lru.Front() {
		conversation := element.Value.(*recoveryConversation)
		if now.Sub(conversation.lastSeen) < c.config.IdleTTL {
			break
		}
		c.remove(conversation)
	}
}

func (c *recoveryStore) remove(conversation *recoveryConversation) {
	delete(c.conversations, conversation.key)
	c.lru.Remove(conversation.element)
	c.bytes -= conversation.bytes
	c.epoch++
}
