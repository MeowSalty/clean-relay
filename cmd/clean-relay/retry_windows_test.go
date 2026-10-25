//go:build windows

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func windowsExerciseRetry(t *testing.T, binary, stage, shutdown string) {
	t.Helper()
	const message = "Item 'synthetic-windows-resource' was created under a different Azure OpenAI resource. Please use the same resource."
	const recovered = "data: recovered\n\n"
	activeAttempt, lastAttempt := int32(1), int32(2)
	inspection := stage == "inspection" || stage == "second_inspection"
	mode := "portable_ids"
	switch stage {
	case "second_attempt":
		activeAttempt = 2
	case "second_inspection":
		activeAttempt, lastAttempt, mode = 2, 3, "portable_ids_then_reasoning"
	case "third_attempt":
		activeAttempt, lastAttempt, mode = 3, 3, "portable_ids_then_reasoning"
	}
	var calls atomic.Int32
	started := make(chan context.Context, 1)
	canceled, release := make(chan struct{}), make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		attempt := calls.Add(1)
		if attempt > lastAttempt {
			t.Errorf("upstream attempts %d exceed %s cap %d", attempt, mode, lastAttempt)
			w.WriteHeader(500)
			return
		}
		if attempt < activeAttempt {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(400)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"type": "invalid_request_error", "message": message}})
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		if attempt > activeAttempt {
			_, _ = io.WriteString(w, recovered)
			return
		}
		started <- r.Context()
		select {
		case <-release:
			if inspection {
				_, _ = io.WriteString(w, "event: error\ndata: {\"type\":\"error\",\"message\":\""+message+"\"}\n\n")
				w.(http.Flusher).Flush()
				<-r.Context().Done()
				close(canceled)
				return
			}
			_, _ = io.WriteString(w, recovered)
		case <-r.Context().Done():
			close(canceled)
		}
	}))
	t.Cleanup(func() { upstream.CloseClientConnections(); upstream.Close() })
	reservation := listenForTest(t)
	address := reservation.Addr().String()
	if err := reservation.Close(); err != nil {
		t.Fatal(err)
	}
	configuration, err := json.Marshal(map[string]any{
		"listen": address, "upstream_origin": upstream.URL,
		"azure_resource_retry": map[string]any{"enabled": true, "cleanup_mode": mode, "inspect_timeout": "10s"},
	})
	if err != nil {
		t.Fatal(err)
	}
	path := writeConfigFile(t, string(configuration))
	processCtx, cancelProcess := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancelProcess()
	cmd := exec.CommandContext(processCtx, binary, "-config", path)
	cmd.Dir = t.TempDir()
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
				t.Error("retry executable did not exit")
			}
		}
	})
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DisableCompression = true
	transport.ResponseHeaderTimeout = 20 * time.Second
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport}
	windowsWaitHealth(t, client, "http://"+address)
	requestBody := `{"input":[{"id":"m","role":"user","content":"synthetic-windows-body"}]}`
	if mode == "portable_ids_then_reasoning" {
		requestBody = `{"input":[{"id":"m","role":"user","content":"synthetic-windows-body"},{"type":"reasoning","encrypted_content":"synthetic-windows-reasoning"}]}`
	}
	request, err := http.NewRequestWithContext(processCtx, http.MethodPost, "http://"+address+"/v1/responses?token=synthetic-windows-query", strings.NewReader(requestBody))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer synthetic-windows-key")
	result := make(chan bodyResult, 1)
	go func() {
		response, err := client.Do(request)
		if err != nil {
			result <- bodyResult{err: err}
			return
		}
		body, readErr := io.ReadAll(response.Body)
		_ = response.Body.Close()
		result <- bodyResult{body: string(body), err: readErr}
	}()
	upstreamCtx := receive(t, started, "retry lifecycle stage")
	firstInterrupt := time.Now()
	windowsSendInterrupt(t, cmd.Process.Pid)
	windowsWaitListenerClosed(t, address)
	if err := upstreamCtx.Err(); err != nil {
		t.Fatalf("first interrupt canceled %s: %v", stage, err)
	}
	switch shutdown {
	case "drain":
		close(release)
	case "second_interrupt":
		windowsSendInterrupt(t, cmd.Process.Pid)
		waitEvent(t, canceled, "retry second-interrupt cancellation")
	case "grace_deadline":
		select {
		case <-canceled:
		case <-time.After(15 * time.Second):
			t.Fatal("retry attempt outlived the shutdown grace period")
		}
		if time.Since(firstInterrupt) < 9*time.Second {
			t.Fatal("retry shutdown ended the grace period early")
		}
	}
	got := receive(t, result, "retry client completion")
	if shutdown == "drain" {
		if got.err != nil || got.body != recovered || calls.Load() != lastAttempt {
			t.Fatalf("retry drain: body=%q err=%v attempts=%d", got.body, got.err, calls.Load())
		}
	} else {
		if got.err == nil || got.body != "" || calls.Load() != activeAttempt {
			t.Fatalf("retry forced exit: body=%q err=%v attempts=%d", got.body, got.err, calls.Load())
		}
	}
	processErr := receive(t, done, "retry Windows executable exit")
	stopped = true
	if processErr != nil {
		t.Fatalf("retry signal exit: %v\n%s", processErr, stderr.String())
	}
	logs := stderr.String()
	if !strings.Contains(logs, "shutdown started") {
		t.Fatalf("missing shutdown log: %s", logs)
	}
	if shutdown == "drain" && (!strings.Contains(logs, "upstream_attempts="+strconv.Itoa(int(lastAttempt))) || !strings.Contains(logs, "retry_decision=retried")) {
		t.Fatalf("missing retry counts: %s", logs)
	}
	for _, secret := range []string{"synthetic-windows-query", "synthetic-windows-key", "synthetic-windows-body", "synthetic-windows-resource", "synthetic-windows-reasoning"} {
		if strings.Contains(logs, secret) || strings.Contains(stdout.String(), secret) {
			t.Fatalf("retry log leaked %q", secret)
		}
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("retry executable did not release the port: %v", err)
	}
	_ = listener.Close()
}
