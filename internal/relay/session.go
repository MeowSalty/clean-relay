package relay

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"log/slog"
	"net/http"
	"net/http/httptrace"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"cleanrelay/internal/cleaner"
	"cleanrelay/internal/config"
)

type sessionManager struct {
	secret   [32]byte
	upstream string
	store    *recoveryStore
	serial   atomic.Uint64
}

var credentialHeaders = [...]string{"Authorization", "Api-Key", "X-Api-Key", "OpenAI-Organization", "OpenAI-Project"}

func newSessionManager(cfg config.Config) *sessionManager {
	m := &sessionManager{upstream: cfg.UpstreamOrigin.String(), store: newRecoveryStore(cfg.AzureResourceRetry.SessionCache)}
	// Go's crypto/rand.Read fills the slice or terminates the process.
	_, _ = rand.Read(m.secret[:])
	return m
}

func (m *sessionManager) digest(domain string, parts ...[]byte) cleaner.Fingerprint {
	hash := hmac.New(sha256.New, m.secret[:])
	for _, part := range append([][]byte{[]byte(domain)}, parts...) {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(part)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write(part)
	}
	var digest cleaner.Fingerprint
	copy(digest[:], hash.Sum(nil))
	return digest
}

func (m *sessionManager) credentialIdentity(header http.Header) cleaner.Fingerprint {
	var parts [][]byte
	for _, name := range credentialHeaders {
		parts = append(parts, []byte(name))
		values := headerValues(header, name)
		parts = append(parts, []byte(strconv.Itoa(len(values))))
		for _, value := range values {
			parts = append(parts, []byte(value))
		}
	}
	return m.digest("credentials", parts...)
}

func sessionHeaderValue(header http.Header, name string) (string, bool) {
	values := headerValues(header, name)
	if len(values) != 1 || len(values[0]) == 0 || len(values[0]) > 1024 {
		return "", false
	}
	for _, c := range values[0] {
		if c < 33 || c > 126 || c == ',' {
			return "", false
		}
	}
	return values[0], true
}

func (m *sessionManager) key(r *http.Request, header string, body []byte) (recoveryKey, bool) {
	session, ok := sessionHeaderValue(r.Header, header)
	if !ok {
		return recoveryKey{}, false
	}
	for _, name := range credentialHeaders {
		if len(headerValues(r.Header, name)) > 1 {
			return recoveryKey{}, false
		}
	}
	credentials := m.credentialIdentity(r.Header)
	conversation := m.digest("conversation", credentials[:], []byte(strings.ToLower(header)), []byte(session))
	root, ok := strictObject(body)
	if !ok {
		return recoveryKey{}, false
	}
	model := []byte("absent")
	if raw, exists := root["model"]; exists {
		value, valid := jsonString(raw)
		if !valid || value == "" {
			return recoveryKey{}, false
		}
		model = append([]byte("model\x00"), value...)
	}
	path := strings.TrimSuffix(r.URL.EscapedPath(), "/")
	path = strings.TrimSuffix(path, "/compact")
	// Query parameters may distinguish API versions or carry credentials.
	// Their keyed digest participates in isolation, never in diagnostics.
	namespace := m.digest("namespace", conversation[:], []byte(m.upstream), []byte(path), []byte(r.URL.RawQuery), model)
	return recoveryKey{conversation: conversation, namespace: namespace}, true
}

func (h *Handler) addSessionDiagnostics(r *http.Request, state *requestState) {
	if h.sessions == nil || !h.retryConfig.SessionDiagnostics {
		return
	}
	names := []string{"session_id", "conversation_id", "x-cleanrelay-session-id"}
	configured := strings.ToLower(h.retryConfig.SessionHeader)
	found := configured == ""
	for _, name := range names {
		found = found || name == configured
	}
	if !found {
		names = append(names, configured)
	}
	credentials := h.sessions.credentialIdentity(r.Header)
	for i, name := range names {
		value, valid := sessionHeaderValue(r.Header, name)
		fields := []any{"header", name, "present", len(headerValues(r.Header, name)) > 0, "valid", valid}
		if valid {
			digest := h.sessions.digest("diagnostic", credentials[:], []byte(name), []byte(value))
			fields = append(fields, "hmac", hex.EncodeToString(digest[:8]))
		}
		state.sessionDiagnostics = append(state.sessionDiagnostics, slog.Group("session_identity_"+strconv.Itoa(i+1), fields...))
	}
}

type sessionLog struct {
	cache            string
	alignment        string
	acceptance       string
	generation       uint64
	revision         uint64
	preIDs           int
	preReasoning     int
	addedSelectors   int
	appliedSelectors int
	writeConfirmed   bool
}

// sendProof is owned by one upstream attempt. Transport may invoke the trace
// from its writer goroutine; callbacks never mutate the request's log or plan.
type sendProof struct {
	mu      sync.Mutex
	view    recoverySnapshot
	written bool
	exact   bool
}

func (p *sendProof) value() (recoverySnapshot, bool) {
	if p == nil {
		return recoverySnapshot{}, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.view, p.written && p.exact
}

func (p *sendProof) wasWritten() bool {
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.written
}

type sessionRequest struct {
	manager  *sessionManager
	key      recoveryKey
	initial  recoverySnapshot
	view     recoverySnapshot
	snapshot cleaner.HistorySnapshot
	desired  []bool
	proof    *sendProof
	frozen   bool
}

func scopeSelectors(view recoverySnapshot) []cleaner.HistorySelector {
	if view.scope == nil {
		return nil
	}
	selectors := make([]cleaner.HistorySelector, len(view.scope.items))
	for i, item := range view.scope.items {
		selectors[i] = item.selector
	}
	return selectors
}

func allowsHistoryAction(mode cleaner.HistoryMode, action cleaner.HistoryAction) bool {
	return mode == cleaner.PortableIDsAndReasoning || mode == cleaner.PortableIDs && action == cleaner.RemoveHistoryID
}

func scopeActions(snapshot cleaner.HistorySnapshot, view recoverySnapshot, mode cleaner.HistoryMode) ([]cleaner.HistoryAction, []bool, bool, bool) {
	selectors := scopeSelectors(view)
	_, matched, unaligned := snapshot.Match(selectors)
	desired := make([]bool, len(selectors))
	var active []cleaner.HistorySelector
	anyMatch := false
	for i, selector := range selectors {
		item := view.scope.items[i]
		if matched[i] {
			anyMatch = true
			if item.applied || allowsHistoryAction(mode, selector.Action) {
				active = append(active, selector)
				desired[i] = true
			}
		}
	}
	actions, _, _ := snapshot.Match(active)
	return actions, desired, anyMatch, unaligned
}

func (s *sessionRequest) analysis() cleaner.HistoryAnalysis {
	actions, _, _ := s.snapshot.Match(scopeSelectors(s.view))
	var analysis cleaner.HistoryAnalysis
	// Count input items, not deduplicated selectors: one stable selector can
	// match multiple identical items in the same request.
	for _, action := range actions {
		switch action {
		case cleaner.RemoveHistoryID:
			analysis.PortableItemIDs++
		case cleaner.RemoveHistoryReasoning:
			analysis.ReasoningItems++
		}
	}
	analysis.Candidate = analysis.PortableItemIDs+analysis.ReasoningItems > 0
	return analysis
}

func (h *Handler) prepareSession(r *http.Request, state *requestState, plain []byte, sse bool) []byte {
	key, ok := h.sessions.key(r, h.retryConfig.SessionHeader, plain)
	if !ok {
		state.retry.decision = "session_identity_unavailable"
		return plain
	}
	snapshot, err := cleaner.SnapshotHistory(plain, h.sessions.serial.Add(1), func(value []byte) cleaner.Fingerprint {
		return h.sessions.digest("history-item", key.namespace[:], value)
	})
	if err != nil {
		state.retry.decision = "transform_failed"
		return plain
	}
	view := h.sessions.store.load(key)
	session := &sessionRequest{manager: h.sessions, key: key, initial: view, view: view, snapshot: snapshot}
	state.session = session
	state.sessionLog.acceptance = "unknown"
	state.sessionLog.cache = "miss"
	if view.scope != nil {
		state.sessionLog.cache = "hit"
		state.sessionLog.generation, state.sessionLog.revision = view.scope.generation, view.scope.revision
	}
	actions, desired, matched, unaligned := scopeActions(snapshot, view, "")
	session.desired = desired
	if matched {
		h.sessions.store.touch(key, view)
	}
	if unaligned {
		state.sessionLog.alignment = "scope_alignment_unavailable"
	}
	prepared, err := cleaner.RepairSelectedHistory(plain, actions)
	if err != nil || prepared.InputItems == 0 && prepared.RemovedItemIDs+prepared.RemovedReasoningItems > 0 {
		state.retry.decision = "empty_input"
		if err != nil {
			state.retry.decision = "transform_failed"
		}
		// A rejected preclean must not certify a scope it did not apply.
		state.session = nil
		return plain
	}
	state.sessionLog.preIDs, state.sessionLog.preReasoning = prepared.RemovedItemIDs, prepared.RemovedReasoningItems
	analysis := snapshot.Analysis
	analysis.PortableItemIDs -= prepared.RemovedItemIDs
	analysis.ReasoningItems -= prepared.RemovedReasoningItems
	analysis.Candidate = analysis.PortableItemIDs+analysis.ReasoningItems > 0
	plan := historyPlan(h.retryConfig.CleanupMode, analysis)
	if len(plan) != 0 {
		state.candidate = &retryRequest{body: plain, plan: plan, sse: sse}
		state.retry.decision = "response_not_inspected"
	} else {
		state.retry.decision = "no_candidate"
	}
	return prepared.Body
}

func (s *sessionRequest) freeze(state *requestState, mode string) []cleaner.HistoryMode {
	if s.frozen {
		return nil
	}
	s.frozen = true
	view, reason, added := s.manager.store.freeze(s.key, s.initial, s.snapshot.Selectors())
	state.sessionLog.cache = reason
	state.sessionLog.addedSelectors = added
	s.view = view
	if view.scope == nil {
		state.retry.stop(reason)
		return nil
	}
	state.sessionLog.generation, state.sessionLog.revision = view.scope.generation, view.scope.revision
	_, _, _, unaligned := scopeActions(s.snapshot, view, "")
	if unaligned {
		state.sessionLog.alignment = "scope_alignment_unavailable"
	}
	// A concurrent request may have published actions after our first send.
	// Include them here; the transport compares against this request's last
	// actually sent removal counts before consuming another upstream call.
	return historyPlan(mode, s.analysis())
}

func (s *sessionRequest) repair(body []byte, mode cleaner.HistoryMode) (cleaner.HistoryResult, bool, error) {
	if s.view.cached {
		current := s.manager.store.load(s.key)
		if current.scope == nil || current.scope.generation != s.view.scope.generation {
			return cleaner.HistoryResult{}, false, nil
		}
		s.view = current
	}
	actions, desired, _, _ := scopeActions(s.snapshot, s.view, mode)
	s.desired = desired
	result, err := cleaner.RepairSelectedHistory(body, actions)
	return result, true, err
}

func (s *sessionRequest) outgoing(request *http.Request) *http.Request {
	if s.view.scope == nil {
		return request
	}
	view, manager, key := s.view, s.manager, s.key
	desired := append([]bool(nil), s.desired...)
	proof := &sendProof{}
	s.proof = proof
	trace := &httptrace.ClientTrace{WroteRequest: func(info httptrace.WroteRequestInfo) {
		if info.Err != nil {
			return
		}
		proof.mu.Lock()
		defer proof.mu.Unlock()
		if proof.written {
			return
		}
		proof.written = true
		proof.view, proof.exact = manager.store.sent(key, view, desired)
	}}
	return request.Clone(httptrace.WithClientTrace(request.Context(), trace))
}

func (h *Handler) finishSession(r *http.Request, state *requestState) {
	if state.session == nil {
		return
	}
	view, exact := state.session.proof.value()
	state.sessionLog.writeConfirmed = state.session.proof.wasWritten()
	logged := state.session.view
	if view.scope != nil {
		logged = view
	}
	if logged.scope != nil {
		state.sessionLog.generation, state.sessionLog.revision = logged.scope.generation, logged.scope.revision
		for _, item := range logged.scope.items {
			if item.applied {
				state.sessionLog.appliedSelectors++
			}
		}
	}
	if state.completion == nil {
		return
	}
	observer := state.completion
	if observer.businessFailure {
		state.retry.businessFailure = true
		if state.outcomeValue() == "completed" {
			state.setOutcome("upstream_business_error")
		}
	}
	if exact && r.Context().Err() == nil && state.outcomeValue() == "completed" && observer.completed() &&
		state.session.manager.store.confirm(state.session.key, view) {
		state.sessionLog.acceptance = "completed"
	}
}
