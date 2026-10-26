package relay

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptrace"
	"strings"
	"testing"

	"cleanrelay/internal/cleaner"
	"cleanrelay/internal/config"
)

func sessionFixture(t *testing.T, ctx context.Context) (*Handler, *http.Request, *requestState, config.AzureResourceRetryConfig) {
	t.Helper()
	cfg, err := config.Parse([]byte(`{"upstream_origin":"http://127.0.0.1:1","azure_resource_retry":{"enabled":true,"history_scope":"session","session_header":"X-Session","cleanup_mode":"portable_ids_and_reasoning"}}`))
	if err != nil {
		t.Fatal(err)
	}
	h := New(cfg, nil)
	t.Cleanup(h.CloseIdleConnections)
	state := &requestState{outcome: "completed", retry: retryLog{mode: cfg.AzureResourceRetry.CleanupMode}}
	request, err := http.NewRequestWithContext(context.WithValue(ctx, requestStateKey{}, state), http.MethodPost, "http://127.0.0.1:1/v1/responses", strings.NewReader(cacheOld))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer synthetic-credential")
	request.Header.Set("X-Session", "synthetic-session")
	h.prepareRetry(request, state, []byte(cacheOld))
	if state.session == nil {
		t.Fatal("session fixture was not prepared")
	}
	return h, request, state, cfg.AzureResourceRetry
}

func TestSessionKeyIsolation(t *testing.T) {
	h, request, _, _ := sessionFixture(t, context.Background())
	body := []byte(`{"model":"m","input":[]}`)
	first, ok := h.sessions.key(request, "X-Session", body)
	if !ok {
		t.Fatal("valid session identity was not accepted")
	}
	for _, tc := range []struct {
		name                            string
		change                          func(*http.Request) []byte
		sameConversation, sameNamespace bool
	}{
		{"same chat next request", func(r *http.Request) []byte { return []byte(`{"model":"m","input":[{"role":"user","content":"new"}]}`) }, true, true},
		{"compact family", func(r *http.Request) []byte { r.URL.Path = "/v1/responses/compact"; return body }, true, true},
		{"trailing slash", func(r *http.Request) []byte { r.URL.Path += "/"; return body }, true, true},
		{"different path prefix", func(r *http.Request) []byte { r.URL.Path = "/other/responses"; return body }, true, false},
		{"different query", func(r *http.Request) []byte { r.URL.RawQuery = "api-version=other"; return body }, true, false},
		{"different model", func(r *http.Request) []byte { return []byte(`{"model":"other","input":[]}`) }, true, false},
		{"missing model", func(r *http.Request) []byte { return []byte(`{"input":[]}`) }, true, false},
		{"different credential", func(r *http.Request) []byte { r.Header.Set("Authorization", "Bearer other"); return body }, false, false},
		{"different API key", func(r *http.Request) []byte { r.Header.Set("Api-Key", "other"); return body }, false, false},
		{"different organization", func(r *http.Request) []byte { r.Header.Set("OpenAI-Organization", "other"); return body }, false, false},
		{"different chat", func(r *http.Request) []byte { r.Header.Set("X-Session", "other"); return body }, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			next := request.Clone(request.Context())
			key, ok := h.sessions.key(next, "X-Session", tc.change(next))
			if !ok || (key.conversation == first.conversation) != tc.sameConversation || (key.namespace == first.namespace) != tc.sameNamespace {
				t.Fatal("incorrect conversation or namespace isolation")
			}
		})
	}
	h2, request2, _, _ := sessionFixture(t, context.Background())
	otherProcess, _ := h2.sessions.key(request2, "X-Session", body)
	if otherProcess == first {
		t.Fatal("a new process retained an old identity digest")
	}
	h.sessions.upstream = "http://another.example"
	otherUpstream, _ := h.sessions.key(request, "X-Session", body)
	if otherUpstream.namespace == first.namespace || otherUpstream.conversation != first.conversation {
		t.Fatal("upstream was not part of the repair namespace")
	}
	request.Header.Add("Authorization", "Bearer conflicting")
	if _, ok := h.sessions.key(request, "X-Session", body); ok {
		t.Fatal("ambiguous credentials established a session identity")
	}
}

func TestSessionActionsRequireSuccessfulWriteCallback(t *testing.T) {
	h, request, state, cfg := sessionFixture(t, context.Background())
	s := state.session
	s.freeze(state, cfg.CleanupMode)
	repair, current, err := s.repair([]byte(cacheOld), cleaner.PortableIDsAndReasoning)
	if err != nil || !current {
		t.Fatalf("repair preparation: %v", err)
	}
	assertUnapplied := func() {
		t.Helper()
		for _, item := range h.sessions.store.load(s.key).scope.items {
			if item.applied {
				t.Fatal("an unsent transformation was marked applied")
			}
		}
	}
	assertUnapplied()
	traced := s.outgoing(repairedRequest(request, repair.Body))
	trace := httptrace.ContextClientTrace(traced.Context())
	trace.WroteRequest(httptrace.WroteRequestInfo{Err: context.Canceled})
	assertUnapplied()
	trace.WroteRequest(httptrace.WroteRequestInfo{})
	written := h.sessions.store.load(s.key)
	for _, item := range written.scope.items {
		if !item.applied {
			t.Fatal("a written repair was not retained")
		}
	}
	if _, exact := s.proof.value(); !exact {
		t.Fatal("written version did not have a completion proof")
	}
	trace.WroteRequest(httptrace.WroteRequestInfo{})
	if h.sessions.store.load(s.key).scope.revision != written.scope.revision {
		t.Fatal("a duplicate write callback mutated the state twice")
	}
}

func TestSessionCancellationBeforeRepairSendLeavesActionsUnapplied(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h, request, state, cfg := sessionFixture(t, ctx)
	calls := 0
	transport := retryTransport{config: cfg, base: roundTripFunc(func(out *http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: 400, Header: http.Header{"Content-Type": {"application/json"}}, Request: out,
			Body: &closeHookBody{Reader: strings.NewReader(mismatchJSON()), hook: cancel}}, nil
	})}
	response, err := transport.RoundTrip(request)
	if calls != 1 || response != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: calls=%d error=%v", calls, err)
	}
	view := h.sessions.store.load(state.session.key)
	if view.scope == nil {
		t.Fatal("confirmed fault boundary was not frozen")
	}
	for _, item := range view.scope.items {
		if item.applied {
			t.Fatal("cancellation before the retry write committed an action")
		}
	}
}

func TestSessionEmptyRepairDoesNotCommitActions(t *testing.T) {
	h, request, state, cfg := sessionFixture(t, context.Background())
	body := []byte(`{"input":[{"type":"reasoning","id":"only"}]}`)
	state.candidate, state.session = nil, nil
	request.Body = io.NopCloser(strings.NewReader(string(body)))
	h.prepareRetry(request, state, body)
	calls := 0
	transport := retryTransport{config: cfg, base: roundTripFunc(func(out *http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: 400, Header: http.Header{"Content-Type": {"application/json"}}, Request: out,
			Body: io.NopCloser(strings.NewReader(mismatchJSON()))}, nil
	})}
	response, err := transport.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if calls != 1 || state.retry.stopReason != "empty_input" {
		t.Fatalf("empty repair sent: calls=%d decision=%s", calls, state.retry.stopReason)
	}
	for _, item := range h.sessions.store.load(state.session.key).scope.items {
		if item.applied {
			t.Fatal("empty repair was marked applied")
		}
	}
}
