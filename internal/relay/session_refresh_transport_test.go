package relay

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptrace"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cleanrelay/internal/cleaner"
)

const refreshCompleted = `{"status":"completed","output":[],"error":null}`

func preparedSessionRequest(t *testing.T, h *Handler, ctx context.Context, body string) (*http.Request, *requestState) {
	t.Helper()
	state := &requestState{outcome: "completed", retry: retryLog{mode: h.retryConfig.CleanupMode}}
	request, err := http.NewRequestWithContext(context.WithValue(ctx, requestStateKey{}, state), http.MethodPost,
		"http://127.0.0.1:1/v1/responses", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer synthetic-credential")
	request.Header.Set("X-Session", "synthetic-session")
	prepared := h.prepareRetry(request, state, []byte(body))
	return repairedRequest(request, prepared), state
}

func assertSessionWire(t *testing.T, got []byte, want string) {
	t.Helper()
	decode := func(body string) any {
		decoder := json.NewDecoder(strings.NewReader(body))
		decoder.UseNumber()
		var value any
		if err := decoder.Decode(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	if !reflect.DeepEqual(decode(string(got)), decode(want)) {
		t.Fatalf("unexpected upstream body: %s; want %s", got, want)
	}
}

func sessionWritten(request *http.Request) {
	if trace := httptrace.ContextClientTrace(request.Context()); trace != nil && trace.WroteRequest != nil {
		trace.WroteRequest(httptrace.WroteRequestInfo{})
	}
}

func sessionResponse(request *http.Request, status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}},
		Request: request, Body: io.NopCloser(strings.NewReader(body))}
}

func sessionReceive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for session test barrier")
		var zero T
		return zero
	}
}

func seedSessionActions(t *testing.T, h *Handler, body string) recoveryKey {
	t.Helper()
	request, state := preparedSessionRequest(t, h, context.Background(), body)
	state.session.freeze(state, h.retryConfig.CleanupMode)
	repaired, current, err := state.session.repair([]byte(body), cleaner.PortableIDsAndReasoning)
	if err != nil || !current {
		t.Fatalf("seed repair: current=%v error=%v", current, err)
	}
	sessionWritten(state.session.outgoing(repairedRequest(request, repaired.Body)))
	return state.session.key
}

func TestSessionConcurrentRefreshMergesWrittenActions(t *testing.T) {
	for _, hit := range []bool{false, true} {
		for _, firstFault := range []int{0, 1} {
			for _, firstWrite := range []int{0, 1} {
				name := "miss"
				if hit {
					name = "hit"
				}
				t.Run(name+"/fault="+string(rune('P'+firstFault))+"/write="+string(rune('P'+firstWrite)), func(t *testing.T) {
					h, _, _, cfg := sessionFixture(t, context.Background())
					if hit {
						seedSessionActions(t, h, cacheOld)
					}
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					var requests [2]*http.Request
					var states [2]*requestState
					var calls [2]atomic.Int32
					var initial, repair [2]chan []byte
					var releaseFault, releaseWrite [2]chan struct{}
					bodies := [2]string{
						`{"input":[{"id":"old","role":"user","content":"old"},{"type":"reasoning","id":"old-r"},{"id":"p","role":"assistant","content":"P"},{"type":"reasoning","id":"p-r"}]}`,
						`{"input":[{"id":"old","role":"user","content":"old"},{"type":"reasoning","id":"old-r"},{"id":"q","role":"assistant","content":"Q"},{"type":"reasoning","id":"q-r"}]}`,
					}
					wantRepair := [2]string{
						`{"input":[{"role":"user","content":"old"},{"role":"assistant","content":"P"}]}`,
						`{"input":[{"role":"user","content":"old"},{"role":"assistant","content":"Q"}]}`,
					}
					for i := range requests {
						requests[i], states[i] = preparedSessionRequest(t, h, ctx, bodies[i])
						requests[i].Header.Set("X-Scenario", string(rune('P'+i)))
						initial[i], repair[i] = make(chan []byte, 1), make(chan []byte, 1)
						releaseFault[i], releaseWrite[i] = make(chan struct{}), make(chan struct{})
					}
					transport := retryTransport{config: cfg, base: roundTripFunc(func(out *http.Request) (*http.Response, error) {
						i := int(out.Header.Get("X-Scenario")[0] - 'P')
						wire, err := io.ReadAll(out.Body)
						_ = out.Body.Close()
						if err != nil {
							return nil, err
						}
						if calls[i].Add(1) == 1 {
							sessionWritten(out)
							initial[i] <- wire
							select {
							case <-releaseFault[i]:
								return sessionResponse(out, 400, mismatchJSON()), nil
							case <-out.Context().Done():
								return nil, out.Context().Err()
							}
						}
						repair[i] <- wire
						select {
						case <-releaseWrite[i]:
							sessionWritten(out)
							return sessionResponse(out, 200, refreshCompleted), nil
						case <-out.Context().Done():
							return nil, out.Context().Err()
						}
					})}
					type result struct {
						index int
						err   error
					}
					done := make(chan result, 2)
					for i := range requests {
						go func() {
							response, err := transport.RoundTrip(requests[i])
							if err == nil {
								states[i].completion = newCompletionObserver(response)
								wire, readErr := io.ReadAll(response.Body)
								_ = response.Body.Close()
								states[i].completion.feed(wire)
								if readErr == nil {
									states[i].completion.end(io.EOF)
								} else {
									states[i].completion.end(readErr)
								}
								h.finishSession(requests[i], states[i])
								err = readErr
							}
							done <- result{i, err}
						}()
					}
					for i := range requests {
						want := bodies[i]
						if hit {
							want = strings.Replace(want, `"id":"old",`, "", 1)
							want = strings.Replace(want, `,{"type":"reasoning","id":"old-r"}`, "", 1)
						}
						assertSessionWire(t, sessionReceive(t, initial[i]), want)
					}
					for _, i := range []int{firstFault, 1 - firstFault} {
						close(releaseFault[i])
						assertSessionWire(t, sessionReceive(t, repair[i]), wantRepair[i])
					}
					key := states[0].session.key
					pending := h.sessions.store.load(key)
					applied := 0
					for _, item := range pending.scope.items {
						if item.applied {
							applied++
						}
					}
					wantApplied := 0
					if hit {
						wantApplied = 2
					}
					if len(pending.scope.items) != 6 || applied != wantApplied {
						t.Fatalf("pending candidates were truncated or marked written: items=%d applied=%d", len(pending.scope.items), applied)
					}
					for _, i := range []int{firstWrite, 1 - firstWrite} {
						close(releaseWrite[i])
						got := sessionReceive(t, done)
						if got.index != i || got.err != nil || calls[i].Load() != 2 {
							t.Fatalf("concurrent request: %+v calls=%d", got, calls[i].Load())
						}
					}
					written := h.sessions.store.load(key)
					for i, item := range written.scope.items {
						if !item.applied || item.selector != pending.scope.items[i].selector || pending.scope.items[i].applied != (hit && i < 2) {
							t.Fatal("concurrent writes lost actions, reordered selectors or mutated an old snapshot")
						}
					}
					if written.scope.generation != pending.scope.generation || states[firstFault].sessionLog.acceptance != "unknown" ||
						states[firstFault].completion.reason != "completed" {
						t.Fatal("an old revision confirmed the larger scope or lost completion observation")
					}
					// A later request replays both branches' written actions without
					// another repair, even if the upstream still returns this error.
					both := strings.TrimSuffix(bodies[0], "]}") + `,{"id":"q","role":"assistant","content":"Q"},{"type":"reasoning","id":"q-r"}]}`
					request, state := preparedSessionRequest(t, h, ctx, both)
					probeCalls := 0
					probe := retryTransport{config: cfg, base: roundTripFunc(func(out *http.Request) (*http.Response, error) {
						probeCalls++
						wire, err := io.ReadAll(out.Body)
						if err != nil {
							return nil, err
						}
						assertSessionWire(t, wire, `{"input":[{"role":"user","content":"old"},{"role":"assistant","content":"P"},{"role":"assistant","content":"Q"}]}`)
						sessionWritten(out)
						return sessionResponse(out, 400, mismatchJSON()), nil
					})}
					response, err := probe.RoundTrip(request)
					if err != nil {
						t.Fatal(err)
					}
					_ = response.Body.Close()
					if probeCalls != 1 || state.retry.decision != "no_candidate" {
						t.Fatal("a fully precleaned union consumed another repair")
					}
				})
			}
		}
	}
}

func TestSessionRefreshAfterUnwrittenRepair(t *testing.T) {
	for _, failure := range []string{"cancel_before_send", "connection_failure", "failed_write_callback"} {
		t.Run(failure, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			h, request, state, cfg := sessionFixture(t, ctx)
			calls := 0
			transport := retryTransport{config: cfg, base: roundTripFunc(func(out *http.Request) (*http.Response, error) {
				calls++
				if calls == 1 {
					response := sessionResponse(out, 400, mismatchJSON())
					if failure == "cancel_before_send" {
						response.Body = &closeHookBody{Reader: strings.NewReader(mismatchJSON()), hook: cancel}
					}
					return response, nil
				}
				wire, err := io.ReadAll(out.Body)
				if err != nil {
					return nil, err
				}
				assertSessionWire(t, wire, `{"input":[{"role":"user","content":"old"}]}`)
				if failure == "failed_write_callback" {
					httptrace.ContextClientTrace(out.Context()).WroteRequest(httptrace.WroteRequestInfo{Err: io.ErrClosedPipe})
				}
				return nil, io.ErrClosedPipe
			})}
			response, err := transport.RoundTrip(request)
			if err == nil || response != nil || (failure == "cancel_before_send" && (calls != 1 || !errors.Is(err, context.Canceled))) {
				t.Fatalf("failed repair: calls=%d error=%v", calls, err)
			}
			for _, item := range h.sessions.store.load(state.session.key).scope.items {
				if item.applied {
					t.Fatal("an unwritten repair was remembered as applied")
				}
			}
			next, nextState := preparedSessionRequest(t, h, context.Background(), cacheNew)
			calls = 0
			recovery := retryTransport{config: cfg, base: roundTripFunc(func(out *http.Request) (*http.Response, error) {
				calls++
				wire, err := io.ReadAll(out.Body)
				if err != nil {
					return nil, err
				}
				sessionWritten(out)
				if calls == 1 {
					assertSessionWire(t, wire, cacheNew)
					return sessionResponse(out, 400, mismatchJSON()), nil
				}
				assertSessionWire(t, wire, `{"input":[{"role":"user","content":"old"},{"role":"assistant","content":"new"}]}`)
				return sessionResponse(out, 200, refreshCompleted), nil
			})}
			response, err = recovery.RoundTrip(next)
			if err != nil {
				t.Fatal(err)
			}
			_ = response.Body.Close()
			if calls != 2 || nextState.sessionLog.addedSelectors != 1 || nextState.sessionLog.preIDs != 0 || nextState.sessionLog.preReasoning != 0 {
				t.Fatalf("unwritten candidates blocked the next recovery: %+v", nextState.sessionLog)
			}
			for _, item := range h.sessions.store.load(nextState.session.key).scope.items {
				if !item.applied {
					t.Fatal("the later written recovery was not retained")
				}
			}
		})
	}
}

func TestSessionRefreshDeltaUsesLastSentCopy(t *testing.T) {
	for _, tc := range []struct {
		name          string
		peerWritten   bool
		resourceError bool
	}{
		{"pending candidates and success", false, false},
		{"pending candidates and error", false, true},
		{"peer writes after prepare", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, _, _, cfg := sessionFixture(t, context.Background())
			seedSessionActions(t, h, cacheOld)
			peer, peerState := preparedSessionRequest(t, h, context.Background(), cacheNew)
			peerState.session.freeze(peerState, cfg.CleanupMode)
			repaired, current, err := peerState.session.repair([]byte(cacheNew), cleaner.PortableIDsAndReasoning)
			if err != nil || !current {
				t.Fatalf("peer repair: %v", err)
			}
			peer = peerState.session.outgoing(repairedRequest(peer, repaired.Body))
			request, state := preparedSessionRequest(t, h, context.Background(), cacheNew)
			if tc.peerWritten {
				sessionWritten(peer)
			}
			calls := 0
			transport := retryTransport{config: cfg, base: roundTripFunc(func(out *http.Request) (*http.Response, error) {
				calls++
				wire, err := io.ReadAll(out.Body)
				if err != nil {
					return nil, err
				}
				sessionWritten(out)
				if calls == 1 {
					assertSessionWire(t, wire, `{"input":[{"role":"user","content":"old"},{"id":"new","role":"assistant","content":"new"}]}`)
					if tc.resourceError {
						return sessionResponse(out, 400, mismatchJSON()), nil
					}
				} else {
					assertSessionWire(t, wire, `{"input":[{"role":"user","content":"old"},{"role":"assistant","content":"new"}]}`)
				}
				return sessionResponse(out, 200, refreshCompleted), nil
			})}
			response, err := transport.RoundTrip(request)
			if err != nil {
				t.Fatal(err)
			}
			_ = response.Body.Close()
			if state.sessionLog.addedSelectors != 0 {
				t.Fatal("reused selectors were counted as additions")
			}
			applied := h.sessions.store.load(state.session.key).scope.items[2].applied
			if tc.resourceError {
				if calls != 2 || !applied || state.retry.attempts[0].newRemovedIDs != 1 || state.retry.attempts[0].newRemovedReasoning != 0 {
					t.Fatalf("a peer's candidates hid this request's delta: calls=%d attempt=%+v", calls, state.retry.attempts[0])
				}
			} else if calls != 1 || applied || state.session.frozen {
				t.Fatal("a successful first send applied a peer's unwritten candidate")
			}
		})
	}
}

func TestSessionWriteCallbacksKeepAttemptActions(t *testing.T) {
	for _, weakFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "weak first", false: "weak last"}[weakFirst], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			h, request, state, cfg := sessionFixture(t, ctx)
			s := state.session
			s.freeze(state, cfg.CleanupMode)
			weakResult, _, err := s.repair([]byte(cacheOld), cleaner.PortableIDs)
			if err != nil {
				t.Fatal(err)
			}
			weak := s.outgoing(repairedRequest(request, weakResult.Body))
			weakProof := s.proof
			assertSessionWire(t, weakResult.Body, `{"input":[{"role":"user","content":"old"},{"type":"reasoning","id":"old-r"}]}`)
			h.sessions.store.freeze(s.key, s.initial, cacheSelectors(t, cacheNew, 10))
			strongResult, _, err := s.repair([]byte(cacheOld), cleaner.PortableIDsAndReasoning)
			if err != nil {
				t.Fatal(err)
			}
			strong := s.outgoing(repairedRequest(request, strongResult.Body))
			assertSessionWire(t, strongResult.Body, `{"input":[{"role":"user","content":"old"}]}`)
			// Cancellation cannot erase a write that actually succeeded.
			cancel()
			if weakFirst {
				sessionWritten(weak)
				view := h.sessions.store.load(s.key)
				if !view.scope.items[0].applied || view.scope.items[1].applied || view.scope.items[2].applied {
					t.Fatal("weak callback used a later attempt's actions")
				}
			}
			sessionWritten(strong)
			sessionWritten(weak)
			view := h.sessions.store.load(s.key)
			if !view.scope.items[0].applied || !view.scope.items[1].applied || view.scope.items[2].applied {
				t.Fatal("callbacks lost an upgrade or applied another request's item")
			}
			if _, exact := weakProof.value(); exact || !weakProof.wasWritten() {
				t.Fatal("stale write lost transport evidence or certified the new revision")
			}
			sessionWritten(weak)
			sessionWritten(strong)
			if h.sessions.store.load(s.key).scope.revision != view.scope.revision {
				t.Fatal("duplicate successful callbacks advanced the revision")
			}
		})
	}
}

func TestSessionRemainingCandidatesCountInputItems(t *testing.T) {
	const old = `{"input":[{"id":"m","role":"user","content":"same"},{"id":"m","role":"user","content":"same"},{"type":"reasoning","id":"r"},{"type":"reasoning","id":"r"}]}`
	const clean = `{"input":[{"role":"user","content":"same"},{"role":"user","content":"same"}]}`
	h, _, _, cfg := sessionFixture(t, context.Background())
	seedSessionActions(t, h, old)
	h.retryConfig.CleanupMode, cfg.CleanupMode = "portable_ids_then_reasoning", "portable_ids_then_reasoning"
	_, empty := preparedSessionRequest(t, h, context.Background(), old)
	if empty.candidate != nil || empty.retry.decision != "no_candidate" || empty.sessionLog.preIDs != 2 || empty.sessionLog.preReasoning != 2 {
		t.Fatalf("deduplicated selectors were mistaken for input counts: %+v", empty.sessionLog)
	}
	body := strings.TrimSuffix(old, "]}") + `,{"type":"reasoning","id":"new-r"}]}`
	request, state := preparedSessionRequest(t, h, context.Background(), body)
	calls := 0
	transport := retryTransport{config: cfg, base: roundTripFunc(func(out *http.Request) (*http.Response, error) {
		calls++
		wire, err := io.ReadAll(out.Body)
		if err != nil {
			return nil, err
		}
		sessionWritten(out)
		if calls == 1 {
			assertSessionWire(t, wire, strings.TrimSuffix(clean, "]}")+`,{"type":"reasoning","id":"new-r"}]}`)
			return sessionResponse(out, 400, mismatchJSON()), nil
		}
		assertSessionWire(t, wire, clean)
		return sessionResponse(out, 400, mismatchJSON()), nil
	})}
	response, err := transport.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	attempt := state.retry.attempts[0]
	if calls != 2 || attempt.mode != cleaner.PortableIDsAndReasoning || attempt.removedIDs != 2 || attempt.removedReasoning != 3 ||
		attempt.newRemovedIDs != 0 || attempt.newRemovedReasoning != 1 || state.sessionLog.addedSelectors != 1 {
		t.Fatalf("reasoning-only delta repeated an ID stage or miscounted duplicates: calls=%d attempt=%+v", calls, attempt)
	}
}

func TestSessionAnonymousItemsNeedEachRequestsError(t *testing.T) {
	const body = `{"input":[{"role":"user","content":"keep"},{"type":"reasoning","summary":[]},{"type":"reasoning","summary":[]}]}`
	h, _, _, cfg := sessionFixture(t, context.Background())
	seedSessionActions(t, h, body)
	for range 2 {
		request, state := preparedSessionRequest(t, h, context.Background(), body)
		calls := 0
		transport := retryTransport{config: cfg, base: roundTripFunc(func(out *http.Request) (*http.Response, error) {
			calls++
			wire, err := io.ReadAll(out.Body)
			if err != nil {
				return nil, err
			}
			sessionWritten(out)
			if calls == 1 {
				assertSessionWire(t, wire, body)
				return sessionResponse(out, 400, mismatchJSON()), nil
			}
			assertSessionWire(t, wire, `{"input":[{"role":"user","content":"keep"}]}`)
			return sessionResponse(out, 400, mismatchJSON()), nil
		})}
		response, err := transport.RoundTrip(request)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if calls != 2 || state.sessionLog.preReasoning != 0 || state.sessionLog.addedSelectors != 2 ||
			state.retry.attempts[0].newRemovedReasoning != 2 {
			t.Fatalf("anonymous items became a session-wide deletion rule or lost local repair: %+v", state.sessionLog)
		}
	}
}

func TestSessionCacheRevisionDoesNotCreateRetryDelta(t *testing.T) {
	h, _, _, cfg := sessionFixture(t, context.Background())
	key := seedSessionActions(t, h, cacheOld)
	request, state := preparedSessionRequest(t, h, context.Background(), cacheOld)
	// Exercise a stale, already scheduled plan with no new actions. Neither
	// a cache revision nor JSON reformatting can justify another call.
	state.candidate = &retryRequest{body: []byte(cacheOld), plan: []cleaner.HistoryMode{cleaner.PortableIDsAndReasoning}}
	h.sessions.store.freeze(key, h.sessions.store.load(key), cacheSelectors(t, cacheOld, 20))
	calls := 0
	transport := retryTransport{config: cfg, base: roundTripFunc(func(out *http.Request) (*http.Response, error) {
		calls++
		sessionWritten(out)
		return sessionResponse(out, 400, mismatchJSON()), nil
	})}
	response, err := transport.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil || string(wire) != mismatchJSON() || calls != 1 || state.retry.stopReason != "no_change" || state.retry.attempts[0].mode != "" {
		t.Fatalf("cache-only change consumed a retry: calls=%d reason=%s", calls, state.retry.stopReason)
	}
}

func TestSessionPrecleanEmptyFallsBackWithoutProof(t *testing.T) {
	h, _, _, cfg := sessionFixture(t, context.Background())
	key := seedSessionActions(t, h, cacheOld)
	before := h.sessions.store.load(key)
	const body = `{"input":[{"type":"reasoning","id":"old-r"}]}`
	request, state := preparedSessionRequest(t, h, context.Background(), body)
	if state.session != nil || state.candidate != nil || state.retry.decision != "empty_input" {
		t.Fatal("empty preclean left session repair enabled")
	}
	calls := 0
	transport := retryTransport{config: cfg, base: roundTripFunc(func(out *http.Request) (*http.Response, error) {
		calls++
		wire, err := io.ReadAll(out.Body)
		if err != nil {
			return nil, err
		}
		assertSessionWire(t, wire, body)
		sessionWritten(out)
		return sessionResponse(out, 400, mismatchJSON()), nil
	})}
	response, err := transport.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if calls != 1 || h.sessions.store.load(key).scope != before.scope || state.sessionLog.writeConfirmed {
		t.Fatal("rejected preclean published a proof or retried")
	}
}

func TestSessionEvictionStopsOldRequestRepair(t *testing.T) {
	for _, initialHit := range []bool{false, true} {
		t.Run(map[bool]string{false: "initial miss", true: "initial hit"}[initialHit], func(t *testing.T) {
			h, _, _, cfg := sessionFixture(t, context.Background())
			now := time.Unix(100, 0)
			h.sessions.store.now = func() time.Time { return now }
			if initialHit {
				seedSessionActions(t, h, cacheOld)
			}
			request, state := preparedSessionRequest(t, h, context.Background(), cacheNew)
			if !initialHit {
				seedSessionActions(t, h, cacheOld)
			}
			now = now.Add(3 * time.Hour)
			seedSessionActions(t, h, cacheOld)
			key := state.session.key
			replacement := h.sessions.store.load(key)
			calls := 0
			transport := retryTransport{config: cfg, base: roundTripFunc(func(out *http.Request) (*http.Response, error) {
				calls++
				sessionWritten(out)
				return sessionResponse(out, 400, mismatchJSON()), nil
			})}
			response, err := transport.RoundTrip(request)
			if err != nil {
				t.Fatal(err)
			}
			wire, err := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if err != nil || string(wire) != mismatchJSON() || calls != 1 || state.retry.stopReason != "scope_changed" ||
				state.sessionLog.addedSelectors != 0 || h.sessions.store.load(key).scope != replacement.scope {
				t.Fatalf("stale request affected a replacement: calls=%d stop=%s", calls, state.retry.stopReason)
			}
		})
	}
}

func TestSessionOversizedRefreshRepairsLocally(t *testing.T) {
	h, _, _, cfg := sessionFixture(t, context.Background())
	h.sessions.store.config.MaxItemsPerSession = 2
	key := seedSessionActions(t, h, cacheOld)
	request, state := preparedSessionRequest(t, h, context.Background(), cacheNew)
	calls := 0
	transport := retryTransport{config: cfg, base: roundTripFunc(func(out *http.Request) (*http.Response, error) {
		calls++
		wire, err := io.ReadAll(out.Body)
		if err != nil {
			return nil, err
		}
		sessionWritten(out)
		if calls == 1 {
			assertSessionWire(t, wire, `{"input":[{"role":"user","content":"old"},{"id":"new","role":"assistant","content":"new"}]}`)
			return sessionResponse(out, 400, mismatchJSON()), nil
		}
		assertSessionWire(t, wire, `{"input":[{"role":"user","content":"old"},{"role":"assistant","content":"new"}]}`)
		return sessionResponse(out, 200, refreshCompleted), nil
	})}
	response, err := transport.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if calls != 2 || state.sessionLog.cache != "scope_cache_budget" || state.sessionLog.addedSelectors != 0 ||
		!state.session.proof.wasWritten() || h.sessions.store.load(key).scope != nil || h.sessions.store.bytes != 0 {
		t.Fatalf("oversized local repair was blocked or cached: calls=%d log=%+v", calls, state.sessionLog)
	}
	next, nextState := preparedSessionRequest(t, h, context.Background(), cacheNew)
	wire, err := io.ReadAll(next.Body)
	if err != nil {
		t.Fatal(err)
	}
	assertSessionWire(t, wire, cacheNew)
	if nextState.sessionLog.preIDs != 0 || nextState.sessionLog.preReasoning != 0 {
		t.Fatal("a local repair became persistent memory")
	}
}

type sessionOutputReader struct {
	io.Reader
	state *requestState
}

func (r *sessionOutputReader) Read(p []byte) (int, error) {
	r.state.outputStarted.Store(true)
	return r.Reader.Read(p)
}

func TestSessionOutputClosesFreezeAndSendWindows(t *testing.T) {
	for _, when := range []string{"inspection", "close"} {
		t.Run(when, func(t *testing.T) {
			h, _, _, cfg := sessionFixture(t, context.Background())
			key := seedSessionActions(t, h, cacheOld)
			request, state := preparedSessionRequest(t, h, context.Background(), cacheNew)
			calls := 0
			transport := retryTransport{config: cfg, base: roundTripFunc(func(out *http.Request) (*http.Response, error) {
				calls++
				sessionWritten(out)
				response := sessionResponse(out, 400, mismatchJSON())
				if when == "inspection" {
					response.Body = io.NopCloser(&sessionOutputReader{Reader: strings.NewReader(mismatchJSON()), state: state})
				} else {
					response.Body = &closeHookBody{Reader: strings.NewReader(mismatchJSON()), hook: func() { state.outputStarted.Store(true) }}
				}
				return response, nil
			})}
			response, err := transport.RoundTrip(request)
			if err != nil {
				t.Fatal(err)
			}
			wire, err := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if err != nil || string(wire) != mismatchJSON() || calls != 1 || state.retry.stopReason != "output_started" || state.retry.attempts[0].mode != "" {
				t.Fatalf("output allowed another call or damaged the current error: calls=%d error=%v stop=%s", calls, err, state.retry.stopReason)
			}
			view := h.sessions.store.load(key)
			if when == "inspection" {
				if len(view.scope.items) != 2 || state.session.frozen || state.sessionLog.addedSelectors != 0 {
					t.Fatal("output during inspection absorbed new candidates")
				}
			} else if len(view.scope.items) != 3 || view.scope.items[2].applied {
				t.Fatal("output while closing committed an unsent action")
			}
		})
	}
}
