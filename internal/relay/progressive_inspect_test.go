package relay

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestProgressiveSecondInspectionHandoff(t *testing.T) {
	for _, readFailure := range []bool{false, true} {
		t.Run(strconv.FormatBool(readFailure), func(t *testing.T) {
			state, request, cfg := progressiveTransportFixture(t, context.Background())
			cfg.InspectTimeout = 30 * time.Millisecond
			reader := newScriptedBody()
			prefix, late := ": heartbeat\n\n", "event: error\ndata: {\"message\":\""+mismatchMessage+"\"}\n\n"
			if readFailure {
				prefix, late = mismatchJSON(), ""
				reader.steps <- scriptedRead{data: prefix, err: io.ErrUnexpectedEOF}
			} else {
				reader.steps <- scriptedRead{data: prefix}
			}
			var active context.Context
			calls := 0
			transport := retryTransport{config: cfg, base: roundTripFunc(func(out *http.Request) (*http.Response, error) {
				calls++
				response := &http.Response{StatusCode: 400, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(mismatchJSON())), Request: out}
				if calls == 1 {
					return response, nil
				}
				active = out.Context()
				response.Body, reader.response = reader, response
				if !readFailure {
					response.StatusCode = 200
					response.Header.Set("Content-Type", "text/event-stream")
					response.Trailer = http.Header{"X-Tail": nil}
				}
				return response, nil
			})}
			started := time.Now()
			response, err := transport.RoundTrip(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if !readFailure {
				if time.Since(started) > 2*time.Second || active.Err() != nil {
					t.Fatal("inspection timeout canceled or blocked the healthy stream")
				}
				select {
				case <-reader.closed:
					t.Fatal("timeout closed the reader")
				default:
				}
				reader.steps <- scriptedRead{data: late, err: io.EOF, trailer: http.Header{"X-Tail": {"preserved"}}}
			}
			body, readErr := io.ReadAll(response.Body)
			if string(body) != prefix+late || readFailure && !errors.Is(readErr, io.ErrUnexpectedEOF) || !readFailure && readErr != nil {
				t.Fatalf("replay=%q, err=%v", body, readErr)
			}
			_ = response.Body.Close()
			reason := "inspection_timeout"
			if readFailure {
				reason = "inspection_read_error"
			} else if response.Trailer.Get("X-Tail") != "preserved" {
				t.Fatal("late trailer lost")
			}
			if calls != 2 || state.retry.stopReason != reason || state.retry.decision != "retried" || !state.retry.businessFailure || reader.maximum.Load() != 1 || reader.active.Load() != 0 {
				t.Fatalf("handoff: calls=%d retry=%+v", calls, state.retry)
			}
			select {
			case <-response.Body.(*inspectedBody).done:
			default:
				t.Fatal("second pump outlived Close")
			}
		})
	}
}

type progressiveChunkReader struct {
	io.Reader
	size int
}

func (r progressiveChunkReader) Read(p []byte) (int, error) {
	return r.Reader.Read(p[:min(len(p), r.size)])
}

func TestProgressiveSecondSSEBoundaries(t *testing.T) {
	for _, newline := range []string{"\n", "\r", "\r\n"} {
		for _, chunk := range []int{1, 7, inspectBlockBytes} {
			for _, complete := range []bool{false, true} {
				t.Run(strconv.Quote(newline)+"/"+strconv.Itoa(chunk)+"/"+strconv.FormatBool(complete), func(t *testing.T) {
					wire := "\xef\xbb\xbf: comment\n\nevent: response.in_progress\ndata: {\"type\":\"response.in_progress\",\"response\":{\"output\":[]}}\n\nevent: error\ndata: {\"type\":\"error\",\ndata: \"message\":\"" + mismatchMessage + "\"}\n\n"
					wire = strings.ReplaceAll(wire, "\n", newline)
					state, request, cfg := progressiveTransportFixture(t, context.Background())
					cfg.MaxInspectBytes = int64(len(wire))
					if newline == "\r\n" {
						// CR dispatches the frame; the final LF is optional.
						cfg.MaxInspectBytes--
					}
					if !complete {
						cfg.MaxInspectBytes--
					}
					calls := 0
					transport := retryTransport{config: cfg, base: roundTripFunc(func(out *http.Request) (*http.Response, error) {
						calls++
						response := &http.Response{StatusCode: 400, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(mismatchJSON())), Request: out}
						if calls == 2 {
							response.StatusCode = 200
							response.Header.Set("Content-Type", "text/event-stream")
							response.Body = io.NopCloser(progressiveChunkReader{Reader: strings.NewReader(wire), size: chunk})
						} else if calls == 3 {
							response.StatusCode = 201
							response.Body = io.NopCloser(strings.NewReader("third"))
						} else if calls > 3 {
							t.Error("fourth call")
						}
						return response, nil
					})}
					response, err := transport.RoundTrip(request)
					if err != nil {
						t.Fatal(err)
					}
					got, err := io.ReadAll(response.Body)
					_ = response.Body.Close()
					want, wantCalls, stop := wire, 2, "inspection_limit"
					if complete {
						want, wantCalls, stop = "third", 3, "stages_exhausted"
					}
					if err != nil || string(got) != want || calls != wantCalls || state.retry.stopReason != stop {
						t.Fatalf("boundary: calls=%d stop=%s body=%q err=%v", calls, state.retry.stopReason, got, err)
					}
				})
			}
		}
	}
}

func TestProgressiveInspectionWindowsShareOnlyClientDeadline(t *testing.T) {
	for _, clientDeadline := range []bool{false, true} {
		t.Run(strconv.FormatBool(clientDeadline), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			if clientDeadline {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 500*time.Millisecond)
			}
			defer cancel()
			state, request, cfg := progressiveTransportFixture(t, ctx)
			cfg.InspectTimeout = 450 * time.Millisecond
			var producers []chan struct{}
			calls := 0
			transport := retryTransport{config: cfg, base: roundTripFunc(func(out *http.Request) (*http.Response, error) {
				calls++
				deadline, hasDeadline := out.Context().Deadline()
				want, wantDeadline := ctx.Deadline()
				if hasDeadline != wantDeadline || !deadline.Equal(want) {
					t.Error("attempt changed the client deadline")
				}
				response := &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("third")), Request: out}
				if calls > 2 {
					return response, nil
				}
				reader := newScriptedBody()
				response.Body = reader
				response.StatusCode = 400
				response.Header.Set("Content-Type", "application/json")
				done := make(chan struct{})
				producers = append(producers, done)
				go func() {
					defer close(done)
					timer := time.NewTimer(300 * time.Millisecond)
					defer timer.Stop()
					select {
					case <-out.Context().Done():
						return
					case <-timer.C:
					}
					select {
					case <-out.Context().Done():
					case reader.steps <- scriptedRead{data: mismatchJSON(), err: io.EOF}:
					}
				}()
				return response, nil
			})}
			response, err := transport.RoundTrip(request)
			if response != nil {
				_ = response.Body.Close()
			}
			for _, done := range producers {
				progressiveReceive(t, done)
			}
			if clientDeadline {
				if !errors.Is(err, context.DeadlineExceeded) || response != nil || calls != 2 || state.retry.stopReason != "canceled" {
					t.Fatalf("deadline: calls=%d err=%v log=%+v", calls, err, state.retry)
				}
			} else if err != nil || calls != 3 || response.StatusCode != 200 {
				t.Fatalf("second window reused first deadline: calls=%d err=%v", calls, err)
			}
		})
	}
}
