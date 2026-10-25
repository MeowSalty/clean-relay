package relay

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"cleanrelay/internal/cleaner"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type closeHookBody struct {
	io.Reader
	once sync.Once
	hook func()
}

func (b *closeHookBody) Close() error { b.once.Do(b.hook); return nil }

func TestRetryWaitsForCloseAndOwnsIndependentContexts(t *testing.T) {
	for _, cancelOnClose := range []bool{false, true} {
		t.Run(map[bool]string{false: "independent contexts", true: "canceled while discarding"}[cancelOnClose], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			state := &requestState{candidate: &retryRequest{body: []byte(`{"input":[{"id":"m","role":"user","content":"x"}]}`), plan: []cleaner.HistoryMode{cleaner.PortableIDs}}}
			request, err := http.NewRequestWithContext(context.WithValue(ctx, requestStateKey{}, state), http.MethodPost, "https://upstream.invalid/v1/responses", strings.NewReader("original"))
			if err != nil {
				t.Fatal(err)
			}
			closing, releaseClose, secondCalled := make(chan struct{}), make(chan struct{}), make(chan struct{}, 1)
			var firstContext context.Context
			calls := 0
			transport := retryTransport{config: inspectionConfig(4096, time.Second), base: roundTripFunc(func(out *http.Request) (*http.Response, error) {
				calls++
				if out.GetBody != nil {
					t.Error("transport received GetBody")
				}
				if calls == 1 {
					firstContext = out.Context()
					return &http.Response{StatusCode: 400, Header: http.Header{"Content-Type": {"application/json"}}, Request: out,
						Body: &closeHookBody{Reader: strings.NewReader(mismatchJSON()), hook: func() {
							if firstContext.Err() == nil {
								t.Error("Close did not cancel the first attempt")
							}
							close(closing)
							<-releaseClose
							if cancelOnClose {
								cancel()
							}
						}}}, nil
				}
				secondCalled <- struct{}{}
				if out.Context().Err() != nil || ctx.Err() != nil {
					t.Error("first cancellation affected second attempt")
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Request: out, Body: io.NopCloser(strings.NewReader("second"))}, nil
			})}
			type result struct {
				response *http.Response
				err      error
			}
			done := make(chan result, 1)
			go func() { response, err := transport.RoundTrip(request); done <- result{response, err} }()
			select {
			case <-closing:
			case <-time.After(2 * time.Second):
				t.Fatal("first close not reached")
			}
			select {
			case <-secondCalled:
				t.Fatal("second started before first Close completed")
			default:
			}
			close(releaseClose)
			var got result
			select {
			case got = <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("RoundTrip did not finish")
			}
			if cancelOnClose {
				if !errors.Is(got.err, context.Canceled) || calls != 1 || got.response != nil {
					t.Fatalf("canceled retry: calls=%d err=%v", calls, got.err)
				}
				return
			}
			if got.err != nil || calls != 2 || state.upstreamAttempts != 2 {
				t.Fatalf("retry: calls=%d err=%v", calls, got.err)
			}
			secondContext := got.response.Request.Context()
			if _, err := io.ReadAll(got.response.Body); err != nil {
				t.Fatal(err)
			}
			_ = got.response.Body.Close()
			if secondContext.Err() == nil || ctx.Err() != nil {
				t.Fatal("response did not own only its attempt context")
			}
		})
	}
}

func TestRetryTransformationGuardsReturnOriginal(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		mode       cleaner.HistoryMode
		decision   string
	}{
		{"no change", `{"input":[{"role":"user","content":"x"}]}`, cleaner.PortableIDs, "no_change"},
		{"empty input", `{"input":[{"type":"reasoning","id":"r"}]}`, cleaner.PortableIDsAndReasoning, "empty_input"},
		{"invalid snapshot", `invalid`, cleaner.PortableIDs, "transform_failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := &requestState{candidate: &retryRequest{body: []byte(tc.body), plan: []cleaner.HistoryMode{tc.mode}}}
			ctx := context.WithValue(context.Background(), requestStateKey{}, state)
			request, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://upstream.invalid/v1/responses", nil)
			calls := 0
			transport := retryTransport{config: inspectionConfig(4096, time.Second), base: roundTripFunc(func(out *http.Request) (*http.Response, error) {
				calls++
				return &http.Response{StatusCode: 400, Header: http.Header{"Content-Type": {"application/json"}}, Request: out, Body: io.NopCloser(strings.NewReader(mismatchJSON()))}, nil
			})}
			response, err := transport.RoundTrip(request)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if err != nil || string(body) != mismatchJSON() || calls != 1 || state.retry.decision != tc.decision || state.candidate != nil {
				t.Fatalf("guard: calls=%d decision=%s body=%s err=%v", calls, state.retry.decision, body, err)
			}
		})
	}
}

func TestHTTPReadFailureDoesNotTriggerRetry(t *testing.T) {
	body := newScriptedBody()
	body.steps <- scriptedRead{data: mismatchJSON(), err: io.ErrUnexpectedEOF}
	response := &http.Response{Body: body}
	forward, result := inspectResponse(context.Background(), response, inspectionConfig(4096, time.Second), false, "", time.Now())
	defer forward.Body.Close()
	got, err := io.ReadAll(forward.Body)
	if result.matched || string(got) != mismatchJSON() || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("read failure masked: %+v %q %v", result, got, err)
	}
}

func TestFirstTransportFailureIsNeverRetried(t *testing.T) {
	for _, failure := range []error{io.ErrUnexpectedEOF, context.DeadlineExceeded, errors.New("synthetic connection error")} {
		state := &requestState{candidate: &retryRequest{body: []byte(`{"input":[{"id":"m","role":"user","content":"x"}]}`), plan: []cleaner.HistoryMode{cleaner.PortableIDs}}}
		request, _ := http.NewRequestWithContext(context.WithValue(context.Background(), requestStateKey{}, state), http.MethodPost, "https://upstream.invalid/v1/responses", nil)
		calls := 0
		transport := retryTransport{config: inspectionConfig(4096, time.Second), base: roundTripFunc(func(out *http.Request) (*http.Response, error) {
			calls++
			return nil, failure
		})}
		response, err := transport.RoundTrip(request)
		if response != nil || !errors.Is(err, failure) || calls != 1 || state.upstreamAttempts != 1 {
			t.Fatalf("transport failure retried: calls=%d error=%v", calls, err)
		}
	}
}

func TestSSEInspectionExactByteBoundary(t *testing.T) {
	wire := "event: error\ndata: {\"message\":" + `"` + mismatchMessage + `"` + "}\n\n"
	for _, limit := range []int{len(wire), len(wire) - 1} {
		upstream := &http.Response{Body: io.NopCloser(strings.NewReader(wire))}
		response, result := inspectResponse(context.Background(), upstream, inspectionConfig(int64(limit), time.Second), true, "", time.Now())
		if result.matched != (limit == len(wire)) {
			t.Errorf("limit=%d result=%+v", limit, result)
		}
		if len(response.Body.(*inspectedBody).prefix) > limit {
			t.Error("SSE prefix exceeded byte limit")
		}
		got, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil || string(got) != wire {
			t.Fatal("boundary lost wire data")
		}
	}
}
