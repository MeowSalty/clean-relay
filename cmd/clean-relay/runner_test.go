package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"cleanrelay/internal/config"
	"cleanrelay/internal/relay"
)

func TestInterruptDrainsActiveStream(t *testing.T) {
	stream := newUpstreamStream(t)
	running := startRelay(t, stream.server.URL, time.Minute)
	response := running.get(t, "/v1/responses?token=secret-query")
	upstreamCtx := receive(t, stream.started, "upstream request")
	body := readBody(response)

	running.signals <- os.Interrupt
	waitEvent(t, running.listener.closed, "listener close")
	assertNotFinished(t, running.done)
	if err := upstreamCtx.Err(); err != nil {
		t.Fatalf("first interrupt canceled the active upstream: %v", err)
	}
	connection, err := net.DialTimeout("tcp", running.listener.Addr().String(), time.Second)
	if err == nil {
		_ = connection.Close()
		t.Fatal("new connection accepted after shutdown started")
	}

	stream.finish()
	result := receive(t, body, "complete response")
	if result.err != nil || result.body != "data: first\n\ndata: last\n\n" {
		t.Fatalf("drained response = %q, error = %v", result.body, result.err)
	}
	if err := receive(t, running.done, "graceful shutdown"); err != nil {
		t.Fatalf("graceful shutdown: %v", err)
	}
	waitEvent(t, running.handler.idleClosed, "upstream idle connection cleanup")
}

func TestGraceDeadlineCancelsUpstreamAndClosesStream(t *testing.T) {
	stream := newUpstreamStream(t)
	running := startRelay(t, stream.server.URL, 100*time.Millisecond)
	response := running.get(t, "/v1/responses")
	upstreamCtx := receive(t, stream.started, "upstream request")
	body := readBody(response)

	running.signals <- os.Interrupt
	waitEvent(t, running.listener.closed, "listener close")
	waitEvent(t, stream.canceled, "upstream cancellation at grace deadline")
	if !errors.Is(upstreamCtx.Err(), context.Canceled) {
		t.Fatalf("upstream context error = %v, want canceled", upstreamCtx.Err())
	}
	if err := receive(t, running.done, "forced shutdown at grace deadline"); err != nil {
		t.Fatalf("signal shutdown must be successful: %v", err)
	}
	result := receive(t, body, "interrupted response")
	if result.err == nil || result.body != "data: first\n\n" {
		t.Fatalf("interrupted response = %q, error = %v", result.body, result.err)
	}
	waitEvent(t, running.handler.idleClosed, "upstream idle connection cleanup")
}

func TestSecondInterruptCancelsUpstreamImmediately(t *testing.T) {
	stream := newUpstreamStream(t)
	// The event waits below expire well before this grace period. A second
	// interrupt must wake shutdown directly instead of waiting for its timer.
	running := startRelay(t, stream.server.URL, time.Minute)
	response := running.get(t, "/v1/responses")
	upstreamCtx := receive(t, stream.started, "upstream request")
	body := readBody(response)

	running.signals <- os.Interrupt
	waitEvent(t, running.listener.closed, "listener close")
	assertNotFinished(t, running.done)
	if err := upstreamCtx.Err(); err != nil {
		t.Fatalf("first interrupt canceled the active upstream: %v", err)
	}
	running.signals <- os.Interrupt
	if err := receive(t, running.done, "second interrupt shutdown"); err != nil {
		t.Fatalf("signal shutdown must be successful: %v", err)
	}
	waitEvent(t, stream.canceled, "upstream cancellation after second interrupt")
	if !errors.Is(upstreamCtx.Err(), context.Canceled) {
		t.Fatalf("upstream context error = %v, want canceled", upstreamCtx.Err())
	}
	result := receive(t, body, "interrupted response")
	if result.err == nil || result.body != "data: first\n\n" {
		t.Fatalf("interrupted response = %q, error = %v", result.body, result.err)
	}
	waitEvent(t, running.handler.idleClosed, "upstream idle connection cleanup")
}

func TestForcedShutdownCancelsRootWithoutWaitingForHandler(t *testing.T) {
	listener := observeListener(listenForTest(t))
	cfg := loadTestConfig(t, listener.Addr().String(), "http://upstream.invalid")
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseHandler)
	handlerExited := make(chan struct{})
	handler := trackHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerExited)
		entered <- r.Context()
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		// Intentionally ignore cancellation until released by the test.
		<-release
	}), func() {})
	signals := make(chan os.Signal, 2)
	done := make(chan error, 1)
	go func() {
		done <- runServer(cfg, handler, discardLogger(), serverOptions{
			listener: listener, signals: signals, shutdownTimeout: time.Minute,
		})
	}()
	t.Cleanup(func() { stopTestServer(t, signals, done) })
	response := getForTest(t, "http://"+listener.Addr().String()+"/stream")
	requestCtx := receive(t, entered, "handler context")
	if _, deadline := requestCtx.Deadline(); deadline {
		t.Fatal("service imposed a total request deadline")
	}
	signals <- os.Interrupt
	waitEvent(t, listener.closed, "listener close")
	select {
	case <-requestCtx.Done():
		t.Fatal("service root was canceled on the first interrupt")
	default:
	}
	signals <- os.Interrupt
	if err := receive(t, done, "forced shutdown with a blocked handler"); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, requestCtx.Done(), "request root cancellation")
	select {
	case <-handlerExited:
		t.Fatal("handler should still be blocked after the runner exits")
	default:
	}
	waitEvent(t, handler.idleClosed, "idle connection cleanup")
	_ = response.Body.Close()
	releaseHandler()
	waitEvent(t, handlerExited, "handler release")
}

func TestUnexpectedServeFailureCleansUp(t *testing.T) {
	rawError := errors.New("http://user:secret-payload@example.test/?token=secret-payload")
	listener := &failedListener{Listener: listenForTest(t), failure: rawError}
	cfg := loadTestConfig(t, listener.Addr().String(), "http://upstream.invalid")
	handler := trackHandler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}), func() {})
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, nil))
	err := runServer(cfg, handler, logger, serverOptions{listener: listener})
	if !errors.Is(err, rawError) {
		t.Fatalf("error = %v, want wrapped Serve failure", err)
	}
	waitEvent(t, handler.idleClosed, "idle connection cleanup after failure")
	if strings.Contains(output.String(), "secret-payload") {
		t.Fatalf("raw server error leaked: %q", output.String())
	}
}

func TestServerPanicLogSuppressesRequestAndPanicValues(t *testing.T) {
	listener := observeListener(listenForTest(t))
	cfg := loadTestConfig(t, listener.Addr().String(), "http://upstream.invalid")
	handler := trackHandler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("http://user:secret-payload@example.test/?token=secret-payload body=secret-body")
	}), func() {})
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, nil))
	signals := make(chan os.Signal, 2)
	done := make(chan error, 1)
	go func() { done <- runServer(cfg, handler, logger, serverOptions{listener: listener, signals: signals}) }()
	t.Cleanup(func() { stopTestServer(t, signals, done) })
	client := testClient(t)
	response, err := client.Get("http://" + listener.Addr().String() + "/?token=secret-query")
	if response != nil {
		_ = response.Body.Close()
	}
	if err == nil {
		t.Fatal("panic should close the HTTP connection")
	}
	signals <- os.Interrupt
	if err := receive(t, done, "shutdown after handler panic"); err != nil {
		t.Fatal(err)
	}
	logs := output.String()
	if !strings.Contains(logs, "HTTP server connection error") {
		t.Fatalf("sanitized server diagnostic missing: %q", logs)
	}
	for _, secret := range []string{"secret-payload", "secret-body", "secret-query"} {
		if strings.Contains(logs, secret) {
			t.Fatalf("server diagnostic leaked %q: %q", secret, logs)
		}
	}
}

func TestStartupLogLevelAndValidatedOrigin(t *testing.T) {
	for _, level := range []slog.Level{slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError} {
		t.Run(level.String(), func(t *testing.T) {
			listener := listenForTest(t)
			cfg := loadTestConfig(t, listener.Addr().String(), "http://upstream.invalid/")
			cfg.LogLevel = level
			var output bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{Level: cfg.LogLevel}))
			signals := make(chan os.Signal, 2)
			signals <- os.Interrupt
			handler := trackHandler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}), func() {})
			if err := runServer(cfg, handler, logger, serverOptions{listener: listener, signals: signals}); err != nil {
				t.Fatal(err)
			}
			if level <= slog.LevelInfo {
				if !strings.Contains(output.String(), "listen="+cfg.Listen) || !strings.Contains(output.String(), "upstream_origin=http://upstream.invalid") {
					t.Fatalf("startup log = %q", output.String())
				}
			} else if output.Len() != 0 {
				t.Fatalf("INFO logs exceeded the configured level: %q", output.String())
			}
		})
	}
}

type upstreamStream struct {
	server   *httptest.Server
	started  chan context.Context
	canceled chan struct{}
	finish   func()
}

func newUpstreamStream(t *testing.T) *upstreamStream {
	t.Helper()
	stream := &upstreamStream{started: make(chan context.Context, 1), canceled: make(chan struct{})}
	release := make(chan struct{})
	var once sync.Once
	stream.finish = func() { once.Do(func() { close(release) }) }
	stream.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stream.started <- r.Context()
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-release:
			_, _ = io.WriteString(w, "data: last\n\n")
		case <-r.Context().Done():
			close(stream.canceled)
		}
	}))
	t.Cleanup(func() {
		stream.finish()
		stream.server.Close()
	})
	return stream
}

type runningRelay struct {
	listener *observedListener
	handler  *trackedHandler
	signals  chan os.Signal
	done     chan error
	client   *http.Client
}

func startRelay(t *testing.T, origin string, grace time.Duration) *runningRelay {
	t.Helper()
	listener := observeListener(listenForTest(t))
	cfg := loadTestConfig(t, listener.Addr().String(), origin)
	logger := discardLogger()
	proxy := relay.New(cfg, logger)
	running := &runningRelay{
		listener: listener,
		handler:  trackHandler(proxy, proxy.CloseIdleConnections),
		signals:  make(chan os.Signal, 2),
		done:     make(chan error, 1),
		client:   testClient(t),
	}
	go func() {
		running.done <- runServer(cfg, running.handler, logger, serverOptions{
			listener: listener, signals: running.signals, shutdownTimeout: grace,
		})
	}()
	t.Cleanup(func() { stopTestServer(t, running.signals, running.done) })
	return running
}

func (r *runningRelay) get(t *testing.T, path string) *http.Response {
	t.Helper()
	response, err := r.client.Get("http://" + r.listener.Addr().String() + path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = response.Body.Close() })
	return response
}

type trackedHandler struct {
	http.Handler
	closeIdle  func()
	idleClosed chan struct{}
	once       sync.Once
}

func trackHandler(handler http.Handler, closeIdle func()) *trackedHandler {
	return &trackedHandler{Handler: handler, closeIdle: closeIdle, idleClosed: make(chan struct{})}
}

func (h *trackedHandler) CloseIdleConnections() {
	h.once.Do(func() {
		h.closeIdle()
		close(h.idleClosed)
	})
}

type observedListener struct {
	net.Listener
	closed chan struct{}
	once   sync.Once
}

func observeListener(listener net.Listener) *observedListener {
	return &observedListener{Listener: listener, closed: make(chan struct{})}
}

func (l *observedListener) Close() error {
	err := l.Listener.Close()
	l.once.Do(func() { close(l.closed) })
	return err
}

type failedListener struct {
	net.Listener
	failure error
}

func (l *failedListener) Accept() (net.Conn, error) { return nil, l.failure }

func loadTestConfig(t *testing.T, listen, origin string) config.Config {
	t.Helper()
	cfg, err := config.Load(writeConfig(t, listen, origin))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func testClient(t *testing.T) *http.Client {
	t.Helper()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.ResponseHeaderTimeout = 5 * time.Second
	t.Cleanup(transport.CloseIdleConnections)
	return &http.Client{Transport: transport}
}

func getForTest(t *testing.T, target string) *http.Response {
	t.Helper()
	response, err := testClient(t).Get(target)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = response.Body.Close() })
	return response
}

type bodyResult struct {
	body string
	err  error
}

func readBody(response *http.Response) <-chan bodyResult {
	done := make(chan bodyResult, 1)
	go func() {
		body, err := io.ReadAll(response.Body)
		done <- bodyResult{body: string(body), err: err}
	}()
	return done
}

func receive[T any](t *testing.T, ch <-chan T, event string) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", event)
		var zero T
		return zero
	}
}

func waitEvent(t *testing.T, ch <-chan struct{}, event string) {
	t.Helper()
	receive(t, ch, event)
}

func assertNotFinished(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("runner exited while an active stream was held: %v", err)
	default:
	}
}

func stopTestServer(t *testing.T, signals chan<- os.Signal, done <-chan error) {
	t.Helper()
	// Sending two buffered interrupts also stops a server whose test failed
	// before the first interrupt. Do not sleep to guess the current phase.
	for range 2 {
		select {
		case signals <- os.Interrupt:
		default:
		}
	}
	// A test may already have consumed the result. Closing the underlying
	// listener is registered separately; there is no blocking cleanup wait.
	select {
	case <-done:
	default:
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
