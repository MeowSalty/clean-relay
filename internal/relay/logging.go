package relay

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type requestStateKey struct{}

type requestState struct {
	id                 string
	started            time.Time
	inputItems         int
	modifiedItems      int
	removedFields      int
	mu                 sync.Mutex // Flush can run concurrently with the upstream read.
	outcome            string
	outputStarted      atomic.Bool
	upstreamAttempts   int
	candidate          *retryRequest
	retry              retryLog
	session            *sessionRequest
	sessionLog         sessionLog
	sessionDiagnostics []any
	completion         *completionObserver
}

func (s *requestState) setOutcome(outcome string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.outcome = outcome
}

func (s *requestState) outcomeValue() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.outcome
}

func (h *Handler) logRequest(r *http.Request, state *requestState, status int) {
	outcome := state.outcomeValue()
	level := slog.LevelInfo
	switch outcome {
	case "rejected", "upstream_business_error":
		level = slog.LevelWarn
	case "interrupted", "upstream_error", "upstream_timeout":
		level = slog.LevelError
	}
	attrs := []any{
		"request_id", state.id, "method", r.Method, "path", r.URL.Path,
		"input_items", state.inputItems, "removed_fields", state.removedFields,
		"modified_items", state.modifiedItems, "status", status,
		"duration_ms", time.Since(state.started).Milliseconds(), "outcome", outcome,
		"upstream_attempts", state.upstreamAttempts,
	}
	if state.retry.mode != "" {
		attrs = append(attrs, "retry_cleanup_mode", state.retry.mode, "retry_decision", state.retry.decision)
	}
	if state.retry.rule != "" {
		attrs = append(attrs, "retry_rule", state.retry.rule, "retry_error_source", state.retry.source,
			"retry_removed_item_ids", state.retry.removedIDs, "retry_removed_reasoning_items", state.retry.removedReasoning)
	}
	if state.retry.firstStatus != 0 {
		attrs = append(attrs, "first_upstream_status", state.retry.firstStatus)
	}
	if state.retry.stopReason != "" {
		attrs = append(attrs, "retry_stop_reason", state.retry.stopReason)
	}
	for i, name := range []string{"retry_attempt_2", "retry_attempt_3"} {
		attempt := state.retry.attempts[i]
		if attempt.mode != "" {
			attrs = append(attrs, slog.Group(name,
				"cleanup_mode", string(attempt.mode), "trigger_status", attempt.triggerStatus,
				"error_source", attempt.source, "removed_item_ids", attempt.removedIDs,
				"removed_reasoning_items", attempt.removedReasoning))
		}
	}
	if state.retry.businessFailure {
		attrs = append(attrs, "retry_business_failure", true)
	}
	if h.retryConfig.HistoryScope == "session" {
		attrs = append(attrs, "history_scope", "session")
	}
	if state.sessionLog.cache != "" {
		attrs = append(attrs, "scope_cache", state.sessionLog.cache,
			"scope_generation", state.sessionLog.generation, "scope_revision", state.sessionLog.revision,
			"scope_pre_removed_item_ids", state.sessionLog.preIDs,
			"scope_pre_removed_reasoning_items", state.sessionLog.preReasoning,
			"scope_applied_selectors", state.sessionLog.appliedSelectors,
			"scope_write_confirmed", state.sessionLog.writeConfirmed,
			"recovery_acceptance", state.sessionLog.acceptance)
	}
	if state.sessionLog.alignment != "" {
		attrs = append(attrs, "scope_alignment", state.sessionLog.alignment)
	}
	if state.completion != nil {
		attrs = append(attrs, "recovery_observation", state.completion.reason)
	}
	attrs = append(attrs, state.sessionDiagnostics...)
	h.logger.Log(context.Background(), level, "request", attrs...)
}

// responseWriter retains streaming capabilities and ResponseController access
// while observing the final status and write failures. Informational responses
// do not commit the final status.
type responseWriter struct {
	http.ResponseWriter
	state  *requestState
	status int
}

func (w *responseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *responseWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.state.outputStarted.Store(true)
	if status >= 200 {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *responseWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(body)
	if err != nil || n != len(body) {
		w.state.setOutcome("interrupted")
	}
	return n, err
}

func (w *responseWriter) Flush() { _ = w.FlushError() }

func (w *responseWriter) FlushError() error {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	err := http.NewResponseController(w.ResponseWriter).Flush()
	if err != nil {
		w.state.setOutcome("interrupted")
	}
	return err
}

// responseBody observes errors that happen after response headers, when a local
// error envelope can no longer be sent. ReverseProxy still owns the copy and
// abort behavior. Session completion observation never controls forwarding.
type responseBody struct {
	io.ReadCloser
	state *requestState
}

func (b *responseBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if b.state.completion != nil {
		b.state.completion.feed(p[:n])
		b.state.completion.end(err)
	}
	if err != nil && !errors.Is(err, io.EOF) {
		b.state.setOutcome("interrupted")
	}
	return n, err
}

func writeLocalError(w http.ResponseWriter, status int, code, message, requestID string) {
	envelope := struct {
		Error struct {
			Type      string `json:"type"`
			Code      string `json:"code"`
			Message   string `json:"message"`
			RequestID string `json:"request_id"`
		} `json:"error"`
	}{}
	envelope.Error.Type = "clean_relay_error"
	envelope.Error.Code = code
	envelope.Error.Message = message
	envelope.Error.RequestID = requestID
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	// Strings here are fixed diagnostics and the local ID. A write failure
	// means the client disconnected; the observed writer records that result.
	_ = json.NewEncoder(w).Encode(envelope)
}
