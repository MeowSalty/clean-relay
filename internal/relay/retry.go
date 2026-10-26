package relay

import (
	"bytes"
	"context"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
	"sync"
	"time"

	"cleanrelay/internal/cleaner"
	"cleanrelay/internal/config"
)

type retryRequest struct {
	body []byte
	plan []cleaner.HistoryMode
	sse  bool
}

type retryLog struct {
	mode             string
	decision         string
	stopReason       string
	rule             string
	source           string
	removedIDs       int
	removedReasoning int
	firstStatus      int
	businessFailure  bool
	attempts         [2]retryAttemptLog
}

type retryAttemptLog struct {
	mode                cleaner.HistoryMode
	triggerStatus       int
	source              string
	removedIDs          int
	removedReasoning    int
	newRemovedIDs       int
	newRemovedReasoning int
}

func (l *retryLog) stop(reason string) {
	l.stopReason = reason
	if l.decision != "retried" {
		l.decision = reason
	}
}

// historyPlan maps scheduling strategies to at most two distinct pure
// transformations. It holds no generated bodies and consumes no upstream calls.
func historyPlan(mode string, analysis cleaner.HistoryAnalysis) []cleaner.HistoryMode {
	var plan []cleaner.HistoryMode
	switch mode {
	case "portable_ids":
		if analysis.PortableItemIDs > 0 {
			plan = append(plan, cleaner.PortableIDs)
		}
	case "portable_ids_and_reasoning":
		if analysis.PortableItemIDs > 0 || analysis.ReasoningItems > 0 {
			plan = append(plan, cleaner.PortableIDsAndReasoning)
		}
	case "portable_ids_then_reasoning":
		if analysis.PortableItemIDs > 0 {
			plan = append(plan, cleaner.PortableIDs)
		}
		if analysis.ReasoningItems > 0 {
			plan = append(plan, cleaner.PortableIDsAndReasoning)
		}
	}
	return plan
}

func retryEndpoint(r *http.Request) (eligible, sse bool) {
	if r.Method != http.MethodPost {
		return false, false
	}
	path := strings.TrimSuffix(r.URL.Path, "/")
	if strings.HasSuffix(path, "/responses") {
		return true, true
	}
	return strings.HasSuffix(path, "/responses/compact"), false
}

func (h *Handler) prepareRetry(r *http.Request, state *requestState, plain []byte) []byte {
	if !h.retryConfig.Enabled {
		return plain
	}
	eligible, sse := retryEndpoint(r)
	if !eligible {
		state.retry.decision = "out_of_scope"
		return plain
	}
	for key := range r.Header {
		if strings.EqualFold(key, "Idempotency-Key") || strings.EqualFold(key, "X-Idempotency-Key") {
			state.retry.decision = "idempotency_key"
			return plain
		}
		if strings.EqualFold(key, "Last-Event-ID") {
			state.retry.decision = "stream_resume"
			return plain
		}
	}
	analysis, err := cleaner.AnalyzeHistory(plain, cleaner.PortableIDsAndReasoning)
	if err != nil {
		state.retry.decision = "transform_failed"
		return plain
	}
	if analysis.Background {
		state.retry.decision = "background_request"
		return plain
	}
	if h.retryConfig.HistoryScope == "session" {
		return h.prepareSession(r, state, plain, sse)
	}
	plan := historyPlan(h.retryConfig.CleanupMode, analysis)
	if len(plan) == 0 {
		state.retry.decision = "no_candidate"
		return plain
	}
	state.candidate = &retryRequest{body: plain, plan: plan, sse: sse}
	state.retry.decision = "response_not_inspected"
	return plain
}

type retryTransport struct {
	base   http.RoundTripper
	config config.AzureResourceRetryConfig
}

func (t *retryTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	state, _ := request.Context().Value(requestStateKey{}).(*requestState)
	if state == nil {
		return t.base.RoundTrip(request)
	}
	candidate := state.candidate
	defer func() {
		state.candidate = nil
		if state.session != nil {
			state.session.snapshot = cleaner.HistorySnapshot{}
		}
	}()
	if candidate == nil {
		if state.session != nil {
			request = state.session.outgoing(request)
		}
		state.upstreamAttempts++
		return t.base.RoundTrip(request)
	}
	if err := request.Context().Err(); err != nil {
		state.retry.stop("canceled")
		return nil, err
	}
	maxAttempts := 2
	if t.config.CleanupMode == "portable_ids_then_reasoning" {
		maxAttempts = 3
	}
	plan := candidate.plan
	sentIDs, sentReasoning := 0, 0
	initial := request
	if state.session != nil {
		sentIDs, sentReasoning = state.sessionLog.preIDs, state.sessionLog.preReasoning
		initial = state.session.outgoing(request)
	}
	state.upstreamAttempts++
	response, err := t.attempt(initial)
	started := time.Now()
	for {
		if err != nil {
			reason := "upstream_transport_error"
			if request.Context().Err() != nil || errors.Is(err, context.Canceled) {
				reason = "canceled"
			}
			state.retry.stop(reason)
			return response, err
		}
		if request.Context().Err() != nil {
			state.retry.stop("canceled")
			_ = response.Body.Close()
			return nil, request.Context().Err()
		}
		if len(plan) == 0 || state.upstreamAttempts >= maxAttempts {
			state.retry.stop("stages_exhausted")
			return response, nil
		}
		if state.outputStarted.Load() {
			state.retry.stop("output_started")
			return response, nil
		}
		var result inspectionResult
		var source string
		response, result, source = t.inspect(request.Context(), response, candidate.sse, started)
		state.retry.businessFailure = state.retry.businessFailure || result.businessFailure
		if result.businessFailure {
			state.setOutcome("upstream_business_error")
		}
		if request.Context().Err() != nil {
			state.retry.stop("canceled")
			_ = response.Body.Close()
			return nil, request.Context().Err()
		}
		if !result.matched {
			state.retry.stop(result.decision)
			return response, nil
		}
		if state.outputStarted.Load() {
			state.retry.stop("output_started")
			return response, nil
		}
		if state.retry.rule == "" {
			state.retry.rule, state.retry.source = resourceMismatchRule, source
		}
		if state.session != nil && !state.session.frozen {
			plan = state.session.freeze(state, t.config.CleanupMode)
			if state.session.view.scope == nil {
				return response, nil
			}
		}

		var next *http.Request
		var record retryAttemptLog
		for len(plan) > 0 {
			mode := plan[0]
			plan = plan[1:]
			// Every copy comes from B0 and each item has one eligible action.
			// Modes only strengthen; session selectors and applied actions only
			// grow within a lifetime. These invariants make per-action counts
			// sufficient to compare against this request's last sent copy.
			var repaired cleaner.HistoryResult
			var repairErr error
			if state.session != nil {
				var current bool
				repaired, current, repairErr = state.session.repair(candidate.body, mode)
				if !current {
					state.retry.stop("scope_changed")
					return response, nil
				}
			} else {
				repaired, repairErr = cleaner.RepairHistory(candidate.body, mode)
			}
			if repairErr != nil {
				state.retry.stop("transform_failed")
				return response, nil
			}
			state.retry.removedIDs = repaired.RemovedItemIDs
			state.retry.removedReasoning = repaired.RemovedReasoningItems
			if repaired.InputItems == 0 && repaired.RemovedItemIDs+repaired.RemovedReasoningItems > 0 {
				state.retry.stop("empty_input")
				return response, nil
			}
			if repaired.RemovedItemIDs < sentIDs || repaired.RemovedReasoningItems < sentReasoning ||
				repaired.RemovedItemIDs == sentIDs && repaired.RemovedReasoningItems == sentReasoning {
				continue
			}
			next = repairedRequest(request, repaired.Body)
			record = retryAttemptLog{mode: mode, triggerStatus: response.StatusCode, source: source,
				removedIDs: repaired.RemovedItemIDs, removedReasoning: repaired.RemovedReasoningItems,
				newRemovedIDs: repaired.RemovedItemIDs - sentIDs, newRemovedReasoning: repaired.RemovedReasoningItems - sentReasoning}
			break
		}
		if request.Context().Err() != nil {
			state.retry.stop("canceled")
			_ = response.Body.Close()
			return nil, request.Context().Err()
		}
		if state.outputStarted.Load() {
			state.retry.stop("output_started")
			return response, nil
		}
		if next == nil {
			state.retry.stop("no_change")
			return response, nil
		}
		// Close also joins the current inspection pump. No two upstream
		// calls or inspection readers are active at the same time.
		_ = response.Body.Close()
		if request.Context().Err() != nil {
			state.retry.stop("canceled")
			_ = next.Body.Close()
			return nil, request.Context().Err()
		}
		if state.outputStarted.Load() {
			state.retry.stop("output_started")
			_ = next.Body.Close()
			return response, nil
		}
		if state.upstreamAttempts == 1 {
			state.retry.firstStatus = response.StatusCode
		}
		state.retry.decision = "retried"
		state.retry.attempts[state.upstreamAttempts-1] = record
		sentIDs, sentReasoning = record.removedIDs, record.removedReasoning
		state.upstreamAttempts++
		// Historical business failures remain in retry.businessFailure;
		// only the currently returned response determines the outcome.
		state.setOutcome("completed")
		response = nil
		if len(plan) == 0 {
			candidate.body = nil
		}
		if state.session != nil {
			next = state.session.outgoing(next)
		}
		response, err = t.attempt(next)
		started = time.Now()
	}
}

// inspect starts a fresh parser, buffer and deadline for each eligible response.
func (t *retryTransport) inspect(ctx context.Context, response *http.Response, allowSSE bool, started time.Time) (*http.Response, inspectionResult, string) {
	encoding, supported := contentEncoding(response.Header)
	jsonError := response.StatusCode == http.StatusBadRequest && isJSONMediaType(response.Header)
	sse := allowSSE && t.config.InspectSSE && response.StatusCode == http.StatusOK && isSSEMediaType(response.Header)
	if !jsonError && !sse {
		return response, inspectionResult{decision: "response_not_inspected"}, ""
	}
	if !supported || sse && encoding == "gzip" {
		return response, inspectionResult{decision: "unsupported_encoding"}, ""
	}
	source := "http_json"
	if sse {
		source = "sse"
	}
	forward, result := inspectResponse(ctx, response, t.config, sse, encoding, started)
	return forward, result, source
}

func repairedRequest(original *http.Request, body []byte) *http.Request {
	next := original.Clone(original.Context())
	next.Body = io.NopCloser(bytes.NewReader(body))
	next.ContentLength = int64(len(body))
	next.GetBody = nil
	next.TransferEncoding = nil
	for _, key := range []string{"Content-Length", "Content-Encoding", "Transfer-Encoding", "Expect", "Content-MD5", "Digest", "Content-Digest", "Repr-Digest"} {
		deleteHeader(next.Header, key)
	}
	return next
}

// attempt derives from the original request context, never from an already
// canceled first attempt. The response owns cancellation until it is closed.
func (t *retryTransport) attempt(request *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithCancel(request.Context())
	out := request.Clone(ctx)
	out.GetBody = nil
	response, err := t.base.RoundTrip(out)
	if err != nil {
		cancel()
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		return nil, err
	}
	response.Body = &attemptBody{ReadCloser: response.Body, cancel: cancel}
	return response, nil
}

type attemptBody struct {
	io.ReadCloser
	cancel context.CancelFunc
	once   sync.Once
	err    error
}

func (b *attemptBody) Close() error {
	b.once.Do(func() {
		b.cancel()
		b.err = b.ReadCloser.Close()
	})
	return b.err
}

func isSSEMediaType(header http.Header) bool {
	values := headerValues(header, "Content-Type")
	if len(values) != 1 {
		return false
	}
	mediaType, _, err := mime.ParseMediaType(values[0])
	return err == nil && mediaType == "text/event-stream"
}
