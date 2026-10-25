package relay

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"cleanrelay/internal/cleaner"
	"cleanrelay/internal/config"
)

const progressiveSnapshot = `{"input":[{"id":"m","role":"user","content":"x"},{"type":"reasoning"}]}`

func progressiveTransportFixture(t *testing.T, ctx context.Context) (*requestState, *http.Request, config.AzureResourceRetryConfig) {
	t.Helper()
	state := &requestState{outcome: "completed", retry: retryLog{mode: "portable_ids_then_reasoning"},
		candidate: &retryRequest{body: []byte(progressiveSnapshot), plan: []cleaner.HistoryMode{cleaner.PortableIDs, cleaner.PortableIDsAndReasoning}, sse: true}}
	request, err := http.NewRequestWithContext(context.WithValue(ctx, requestStateKey{}, state), http.MethodPost, "https://upstream.invalid/v1/responses", strings.NewReader(progressiveSnapshot))
	if err != nil {
		t.Fatal(err)
	}
	cfg := inspectionConfig(4096, time.Second)
	cfg.CleanupMode = "portable_ids_then_reasoning"
	return state, request, cfg
}

func progressiveReceive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case got := <-ch:
		return got
	case <-time.After(3 * time.Second):
		t.Fatal("progressive transport event timed out")
		var zero T
		return zero
	}
}

func TestProgressiveSecondCloseCompletesBeforeThird(t *testing.T) {
	for _, cancelOnClose := range []bool{false, true} {
		t.Run(map[bool]string{false: "independent contexts", true: "canceled while closing"}[cancelOnClose], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			state, request, cfg := progressiveTransportFixture(t, ctx)
			closing, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			third := make(chan struct{}, 1)
			var contexts []context.Context
			var readers []*scriptedBody
			calls := 0
			transport := retryTransport{config: cfg, base: roundTripFunc(func(out *http.Request) (*http.Response, error) {
				calls++
				contexts = append(contexts, out.Context())
				if out.GetBody != nil || out.Context().Err() != nil || ctx.Err() != nil {
					t.Error("replay body or canceled context used by a new call")
				}
				if calls > 1 && contexts[calls-2].Err() == nil {
					t.Error("previous attempt context remains live")
				}
				if calls == 3 {
					third <- struct{}{}
					return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("third")), Request: out}, nil
				}
				reader := newScriptedBody()
				readers = append(readers, reader)
				reader.steps <- scriptedRead{data: "event: error\ndata: {\"message\":\"" + mismatchMessage + "\"}\n\n"}
				attempt := calls
				upstream := &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Request: out}
				upstream.Body = &closeHookBody{Reader: reader, hook: func() {
					if out.Context().Err() == nil {
						t.Error("response close did not cancel its own attempt")
					}
					if attempt == 2 {
						close(closing)
						<-release
						if cancelOnClose {
							cancel()
						}
					}
					_ = reader.Close()
				}}
				// The pump's second Read stays blocked until Close releases it.
				return upstream, nil
			})}
			type result struct {
				response *http.Response
				err      error
			}
			done := make(chan result, 1)
			go func() {
				response, err := transport.RoundTrip(request)
				done <- result{response, err}
			}()
			progressiveReceive(t, closing)
			select {
			case <-third:
				t.Fatal("third call preceded completion of second Close")
			default:
			}
			once.Do(func() { close(release) })
			got := progressiveReceive(t, done)
			if cancelOnClose {
				if !errors.Is(got.err, context.Canceled) || got.response != nil || calls != 2 || state.upstreamAttempts != 2 || state.retry.stopReason != "canceled" {
					t.Fatalf("canceled result: calls=%d err=%v state=%+v", calls, got.err, state.retry)
				}
				if state.retry.removedReasoning != 1 || state.retry.attempts[1].mode != "" || state.retry.attempts[0].removedReasoning != 0 {
					t.Fatal("generated but unsent strong copy recorded as sent")
				}
				return
			}
			if got.err != nil || calls != 3 || state.upstreamAttempts != 3 {
				t.Fatalf("result: calls=%d err=%v", calls, got.err)
			}
			_ = got.response.Body.Close()
			for _, attempt := range contexts {
				if attempt.Err() == nil {
					t.Error("attempt context leaked")
				}
			}
			for _, reader := range readers {
				if reader.active.Load() != 0 || reader.maximum.Load() != 1 {
					t.Error("inspection read leaked or overlapped")
				}
			}
			if ctx.Err() != nil {
				t.Fatal("response close canceled the original context")
			}
		})
	}
}

func TestProgressiveTransformationGuardsPreserveSecond(t *testing.T) {
	for _, reason := range []string{"transform_failed", "no_change", "empty_input"} {
		t.Run(reason, func(t *testing.T) {
			state, request, cfg := progressiveTransportFixture(t, context.Background())
			candidate := state.candidate
			baseline := bytes.Clone(candidate.body)
			wire := " \n" + mismatchJSON() + "\n "
			trailers := http.Header{"X-Current": {"preserved"}, "X-Late": {"also-preserved"}}
			calls := 0
			transport := retryTransport{config: cfg, base: roundTripFunc(func(out *http.Request) (*http.Response, error) {
				calls++
				response := &http.Response{StatusCode: 400, Header: http.Header{"Content-Type": {"application/json"}}, Request: out, Body: io.NopCloser(strings.NewReader(mismatchJSON()))}
				if calls == 1 {
					return response, nil
				}
				if calls > 2 {
					t.Error("guard permitted a third call")
				}
				if !bytes.Equal(candidate.body, baseline) {
					t.Error("B1 construction mutated B0")
				}
				// Inject faults after B1 has been sent, without adding a
				// production-only test hook to the pure transformation.
				switch reason {
				case "transform_failed":
					candidate.plan[1] = cleaner.HistoryMode("invalid")
				case "no_change":
					candidate.plan[1] = cleaner.PortableIDs
				case "empty_input":
					candidate.body = []byte(`{"input":[{"type":"reasoning"}]}`)
				}
				body := newScriptedBody()
				response.Header.Set("X-Current", "second")
				response.Trailer = http.Header{"X-Current": nil}
				response.Body, body.response = body, response
				body.steps <- scriptedRead{data: wire, err: io.EOF, trailer: trailers}
				return response, nil
			})}
			response, err := transport.RoundTrip(request)
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if err != nil || string(got) != wire || response.StatusCode != 400 || response.Header.Get("X-Current") != "second" || !reflect.DeepEqual(response.Trailer, trailers) || calls != 2 || state.retry.stopReason != reason || state.retry.decision != "retried" {
				t.Fatalf("guard lost second response: calls=%d body=%q err=%v log=%+v", calls, got, err, state.retry)
			}
			if state.retry.attempts[1].mode != "" || state.retry.attempts[0].mode != cleaner.PortableIDs || !state.retry.businessFailure || state.outcomeValue() != "upstream_business_error" {
				t.Fatal("guard lost the current business failure or invented a send")
			}
			if reason == "transform_failed" && (state.retry.removedIDs != 1 || state.retry.removedReasoning != 0) {
				t.Fatal("failed transformation overwrote prior successful counts")
			}
		})
	}
}

func TestRetryHardLimitsAndNoOpStages(t *testing.T) {
	for _, mode := range []string{"portable_ids", "portable_ids_and_reasoning", "portable_ids_then_reasoning"} {
		t.Run(mode, func(t *testing.T) {
			state, request, cfg := progressiveTransportFixture(t, context.Background())
			cfg.CleanupMode = mode
			// An accidentally overlong/duplicate plan must not bypass either cap.
			state.candidate.plan = append(state.candidate.plan[:1], cleaner.PortableIDs, cleaner.PortableIDsAndReasoning, cleaner.PortableIDsAndReasoning)
			calls := 0
			transport := retryTransport{config: cfg, base: roundTripFunc(func(out *http.Request) (*http.Response, error) {
				calls++
				return &http.Response{StatusCode: 400, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(mismatchJSON())), Request: out}, nil
			})}
			response, err := transport.RoundTrip(request)
			if err != nil {
				t.Fatal(err)
			}
			_ = response.Body.Close()
			want := 2
			if mode == "portable_ids_then_reasoning" {
				want = 3
			}
			if calls != want || state.upstreamAttempts != want || state.retry.stopReason != "stages_exhausted" {
				t.Fatalf("mode=%s calls=%d state=%+v", mode, calls, state.retry)
			}
		})
	}
}
