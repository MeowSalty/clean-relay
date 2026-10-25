//go:build windows

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// This runs only in a helper process. Its console is detached before attaching
// to the relay's private hidden console, so Ctrl+C cannot affect the test runner
// or any application in the user's console.
func TestWindowsConsoleSignalHelper(t *testing.T) {
	value := os.Getenv("CLEANRELAY_TEST_CONSOLE_PID")
	if value == "" {
		return
	}
	pid, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid console helper PID")
		os.Exit(1)
	}
	kernel := syscall.NewLazyDLL("kernel32.dll")
	free := kernel.NewProc("FreeConsole")
	// A pipe-backed helper may start without a console; detachment is best effort.
	_, _, _ = free.Call()
	if ok, _, err := kernel.NewProc("AttachConsole").Call(uintptr(pid)); ok == 0 {
		fmt.Fprintf(os.Stderr, "AttachConsole failed: %v\n", err)
		os.Exit(1)
	}
	if ok, _, err := kernel.NewProc("SetConsoleCtrlHandler").Call(0, 1); ok == 0 {
		fmt.Fprintf(os.Stderr, "SetConsoleCtrlHandler failed: %v\n", err)
		os.Exit(1)
	}
	// CTRL_C_EVENT, process group 0: broadcast inside that private console.
	if ok, _, err := kernel.NewProc("GenerateConsoleCtrlEvent").Call(0, 0); ok == 0 {
		fmt.Fprintf(os.Stderr, "GenerateConsoleCtrlEvent failed: %v\n", err)
		os.Exit(1)
	}
	_, _, _ = free.Call()
	os.Exit(0)
}

func windowsSendInterrupt(t *testing.T, pid int) {
	t.Helper()
	testBinary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	helper := exec.CommandContext(ctx, testBinary, "-test.run=^TestWindowsConsoleSignalHelper$")
	helper.Env = append(os.Environ(), "CLEANRELAY_TEST_CONSOLE_PID="+strconv.Itoa(pid))
	helper.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	if output, err := helper.CombinedOutput(); err != nil {
		t.Fatalf("deliver real Windows Ctrl+C: %v\n%s", err, output)
	}
}

type windowsObservedRequest struct {
	body          []byte
	authorization string
	host          string
	query         string
	contentLength int64
}

func TestWindowsExecutableHTTPAndConsoleSignals(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "clean-relay.exe")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	build := exec.CommandContext(ctx, "go", "build", "-trimpath", "-ldflags", "-X main.version=windows-smoke", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build Windows executable: %v\n%s", err, output)
	}
	for _, shutdown := range []string{"drain", "grace_deadline", "second_interrupt"} {
		t.Run(shutdown, func(t *testing.T) { windowsExerciseExecutable(t, binary, shutdown) })
	}
	for _, stage := range []string{"inspection", "second_attempt", "second_inspection", "third_attempt"} {
		for _, shutdown := range []string{"drain", "grace_deadline", "second_interrupt"} {
			t.Run(stage+"/"+shutdown, func(t *testing.T) { windowsExerciseRetry(t, binary, stage, shutdown) })
		}
	}
}

func windowsExerciseExecutable(t *testing.T, binary, shutdown string) {
	t.Helper()
	before, err := os.ReadFile(filepath.Join("..", "..", "testdata", "requests", "metadata.before.json"))
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(filepath.Join("..", "..", "testdata", "requests", "metadata.after.json"))
	if err != nil {
		t.Fatal(err)
	}
	observed := make(chan windowsObservedRequest, 1)
	started := make(chan context.Context, 1)
	canceled := make(chan struct{})
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/gateway/v1/responses" {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read executable's upstream request: %v", err)
				return
			}
			observed <- windowsObservedRequest{body, r.Header.Get("Authorization"), r.Host, r.URL.RawQuery, r.ContentLength}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(w, `{"reply":"ok","n":9007199254740993}`)
			return
		}
		started <- r.Context()
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-release:
			_, _ = io.WriteString(w, "data: last\n\n")
		case <-r.Context().Done():
			close(canceled)
		}
	}))
	t.Cleanup(func() {
		upstream.CloseClientConnections()
		upstream.Close()
	})
	reservation := listenForTest(t)
	address := reservation.Addr().String()
	if err := reservation.Close(); err != nil {
		t.Fatal(err)
	}
	path := writeConfig(t, address, upstream.URL)
	processCtx, cancelProcess := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancelProcess()
	cmd := exec.CommandContext(processCtx, binary, "-config", path)
	cmd.Dir = t.TempDir()
	// CREATE_NEW_CONSOLE with a hidden window keeps the broadcast signal
	// isolated. Pipes still capture the normal executable stdout and stderr.
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: 0x00000010, HideWindow: true}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			_ = cmd.Process.Kill()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("Windows executable did not exit after cleanup")
			}
		}
	})
	client := testClient(t)
	windowsWaitHealth(t, client, "http://"+address)
	request, err := http.NewRequestWithContext(processCtx, http.MethodPost, "http://"+address+"/api/gateway/v1/responses?x=1&x=2", bytes.NewReader(before))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer synthetic-windows-secret")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil || response.StatusCode != http.StatusCreated || string(body) != `{"reply":"ok","n":9007199254740993}` {
		t.Fatalf("executable HTTP response = %d %q, error=%v", response.StatusCode, body, err)
	}
	got := receive(t, observed, "executable upstream request")
	if !reflect.DeepEqual(windowsDecodeJSON(t, got.body), windowsDecodeJSON(t, want)) || got.authorization != "Bearer synthetic-windows-secret" || got.host != strings.TrimPrefix(upstream.URL, "http://") || got.query != "x=1&x=2" || got.contentLength != int64(len(got.body)) {
		t.Fatalf("executable upstream request changed body, authentication, target, or length: %+v", got)
	}
	streamReq, err := http.NewRequestWithContext(processCtx, http.MethodGet, "http://"+address+"/stream?token=synthetic-windows-query-secret", nil)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := client.Do(streamReq)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stream.Body.Close() })
	first := make([]byte, len("data: first\n\n"))
	if _, err := io.ReadFull(stream.Body, first); err != nil || string(first) != "data: first\n\n" {
		t.Fatalf("executable SSE did not arrive before completion: %q %v", first, err)
	}
	upstreamCtx := receive(t, started, "executable's active upstream context")
	remaining := readBody(stream)
	firstInterrupt := time.Now()
	windowsSendInterrupt(t, cmd.Process.Pid)
	windowsWaitListenerClosed(t, address)
	if err := upstreamCtx.Err(); err != nil {
		t.Fatalf("first Ctrl+C canceled the active stream before its grace period: %v", err)
	}

	switch shutdown {
	case "drain":
		close(release)
		result := receive(t, remaining, "executable drained SSE")
		if result.err != nil || result.body != "data: last\n\n" {
			t.Fatalf("executable drained SSE tail = %q, error=%v", result.body, result.err)
		}
	case "second_interrupt":
		windowsSendInterrupt(t, cmd.Process.Pid)
		waitEvent(t, canceled, "real second Ctrl+C upstream cancellation")
	case "grace_deadline":
		select {
		case <-canceled:
		case <-time.After(15 * time.Second):
			t.Fatal("production grace period did not cancel the upstream")
		}
		if time.Since(firstInterrupt) < 9*time.Second {
			t.Fatal("production grace period ended before its configured ten seconds")
		}
	}
	if shutdown != "drain" {
		result := receive(t, remaining, "executable interrupted SSE")
		if result.err == nil || result.body != "" {
			t.Fatalf("executable interrupted SSE appended %q, error=%v", result.body, result.err)
		}
	}
	processErr := receive(t, done, "Windows executable exit")
	stopped = true
	if processErr != nil {
		t.Fatalf("signal shutdown exit: %v\n%s", processErr, stderr.String())
	}
	logs := stderr.String()
	for _, field := range []string{"input_items=8", "removed_fields=7", "modified_items=7", "shutdown started"} {
		if !strings.Contains(logs, field) {
			t.Fatalf("executable log lacks %q: %s", field, logs)
		}
	}
	for _, secret := range []string{"synthetic-windows-secret", "synthetic-windows-query-secret", "internal_chat_message_metadata_passthrough"} {
		if strings.Contains(logs, secret) || strings.Contains(stdout.String(), secret) {
			t.Fatalf("executable logs leaked %q", secret)
		}
	}
	if stdout.Len() != 0 {
		t.Fatalf("running executable wrote unexpected stdout: %q", stdout.String())
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("executable did not release its listening port: %v", err)
	}
	_ = listener.Close()
}

func windowsWaitHealth(t *testing.T, client *http.Client, origin string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, origin+"/_cleanrelay/healthz", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.Do(req)
		if err == nil {
			body, readErr := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if readErr == nil && response.StatusCode == http.StatusOK && strings.TrimSpace(string(body)) == `{"status":"ok"}` {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("executable health check did not become available")
		case <-ticker.C:
		}
	}
}

func windowsWaitListenerClosed(t *testing.T, address string) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		connection, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if err != nil {
			return
		}
		_ = connection.Close()
		select {
		case <-deadline.C:
			t.Fatal("Ctrl+C did not stop new connections")
		case <-ticker.C:
		}
	}
}

func windowsDecodeJSON(t *testing.T, body []byte) any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		t.Fatal(err)
	}
	return value
}
