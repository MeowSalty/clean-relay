package relay

import (
	"context"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cleanrelay/internal/config"
)

type scriptedRead struct {
	data    string
	err     error
	trailer http.Header
}

type scriptedBody struct {
	steps    chan scriptedRead
	reads    chan struct{}
	closed   chan struct{}
	once     sync.Once
	active   atomic.Int32
	maximum  atomic.Int32
	response *http.Response
}

func newScriptedBody() *scriptedBody {
	return &scriptedBody{steps: make(chan scriptedRead, 8), reads: make(chan struct{}, 8), closed: make(chan struct{})}
}

func (b *scriptedBody) Read(p []byte) (int, error) {
	active := b.active.Add(1)
	defer b.active.Add(-1)
	for previous := b.maximum.Load(); active > previous && !b.maximum.CompareAndSwap(previous, active); previous = b.maximum.Load() {
	}
	b.reads <- struct{}{}
	select {
	case step := <-b.steps:
		if len(step.data) > len(p) {
			panic("test block exceeds read buffer")
		}
		if step.trailer != nil {
			b.response.Trailer = step.trailer.Clone()
		}
		return copy(p, step.data), step.err
	case <-b.closed:
		return 0, io.ErrClosedPipe
	}
}

func (b *scriptedBody) Close() error { b.once.Do(func() { close(b.closed) }); return nil }

func inspectionConfig(limit int64, timeout time.Duration) config.AzureResourceRetryConfig {
	return config.AzureResourceRetryConfig{Enabled: true, CleanupMode: "portable_ids", InspectSSE: true, MaxInspectBytes: limit, InspectTimeout: timeout}
}

func TestInspectionTimeoutHandsOffBlockedReaderAndError(t *testing.T) {
	body := newScriptedBody()
	body.steps <- scriptedRead{data: "original-prefix-"}
	response := &http.Response{Body: body, Trailer: http.Header{"X-Announced": nil}}
	body.response = response
	start := time.Now()
	forward, result := inspectResponse(context.Background(), response, inspectionConfig(1000, 30*time.Millisecond), false, "", start)
	defer forward.Body.Close()
	if result.decision != "inspection_timeout" || time.Since(start) > 2*time.Second {
		t.Fatalf("timeout: %+v", result)
	}
	select {
	case <-body.closed:
		t.Fatal("inspection timeout closed the stream")
	default:
	}
	tailError := errors.New("synthetic read failure")
	body.steps <- scriptedRead{data: "tail", err: tailError}
	got, err := io.ReadAll(forward.Body)
	if string(got) != "original-prefix-tail" || !errors.Is(err, tailError) {
		t.Fatalf("replay=%q, %v", got, err)
	}
	if body.maximum.Load() != 1 {
		t.Fatal("concurrent reads from original body")
	}
	if err := forward.Body.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-forward.Body.(*inspectedBody).done:
	default:
		t.Fatal("pump leaked")
	}
}

func TestInspectionPublishesTrailersOnlyAtReplayEOF(t *testing.T) {
	body := newScriptedBody()
	final := http.Header{"X-Announced": {"one"}, "X-Late": {"two"}}
	body.steps <- scriptedRead{data: `{"error":{"message":"ordinary"}}`, err: io.EOF, trailer: final}
	response := &http.Response{Body: body, Trailer: http.Header{"X-Announced": nil}}
	body.response = response
	forward, result := inspectResponse(context.Background(), response, inspectionConfig(1000, time.Second), false, "", time.Now())
	defer forward.Body.Close()
	if result.matched || !reflect.DeepEqual(forward.Trailer, http.Header{"X-Announced": nil}) {
		t.Fatalf("early trailers: %+v", forward.Trailer)
	}
	if _, err := io.ReadAll(forward.Body); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(forward.Trailer, final) {
		t.Fatalf("lost trailers: %+v", forward.Trailer)
	}
}

func TestInspectionLimitPreservesPrefetchedBytes(t *testing.T) {
	wire := strings.Repeat("a", inspectBlockBytes*3+11)
	response := &http.Response{Body: io.NopCloser(strings.NewReader(wire))}
	forward, result := inspectResponse(context.Background(), response, inspectionConfig(17, time.Second), false, "", time.Now())
	defer forward.Body.Close()
	body := forward.Body.(*inspectedBody)
	if result.decision != "inspection_limit" || len(body.prefix) != 18 || cap(body.free) != 2 || cap(body.ready) != 1 {
		t.Fatalf("unbounded inspection: %+v", result)
	}
	got, err := io.ReadAll(forward.Body)
	if err != nil || string(got) != wire {
		t.Fatalf("lost prefetched data: %d, %v", len(got), err)
	}
}

func TestInspectionCloseWakesBlockedPump(t *testing.T) {
	body := newScriptedBody()
	response := &http.Response{Body: body}
	forward, pump := newInspectedBody(context.Background(), response, 64)
	select {
	case <-body.reads:
	case <-time.After(time.Second):
		t.Fatal("read did not start")
	}
	done := make(chan struct{})
	go func() { _ = forward.Body.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close did not wake the reader")
	}
	select {
	case <-pump.done:
	default:
		t.Fatal("pump still running")
	}
}
