package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestArguments(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.json")
	tests := []struct {
		name string
		args []string
		code int
		text string
	}{
		{"version", []string{"-version", "-config", missing}, exitSuccess, "clean-relay " + version},
		{"version_over_check", []string{"-version", "-check-config", "-config", missing}, exitSuccess, "clean-relay " + version},
		{"help", []string{"-help", "-config", missing}, exitSuccess, "Usage: clean-relay"},
		{"short_help", []string{"-h"}, exitSuccess, "Usage: clean-relay"},
		{"unknown", []string{"-unknown"}, exitUsage, "invalid arguments"},
		{"unknown_after_version", []string{"-version", "-unknown"}, exitUsage, "invalid arguments"},
		{"unknown_after_help", []string{"-help", "-unknown"}, exitUsage, "invalid arguments"},
		{"positional", []string{"extra"}, exitUsage, "positional arguments"},
		{"positional_after_version", []string{"-version", "extra"}, exitUsage, "positional arguments"},
		{"flags_after_positional", []string{"extra", "-version"}, exitUsage, "positional arguments"},
		{"after_terminator", []string{"-version", "--", "extra"}, exitUsage, "positional arguments"},
		{"missing_path", []string{"-config"}, exitUsage, "invalid arguments"},
		{"invalid_bool", []string{"-check-config=secret-payload"}, exitUsage, "invalid arguments"},
		{"missing_config", []string{"-config", missing}, exitUsage, "cannot read configuration"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := run(tt.args, &stdout, &stderr); code != tt.code {
				t.Fatalf("exit code = %d, want %d; stderr = %q", code, tt.code, stderr.String())
			}
			output := stderr.String()
			if tt.code == exitSuccess {
				output = stdout.String()
				if stderr.Len() != 0 {
					t.Fatalf("unexpected stderr: %q", stderr.String())
				}
			} else if stdout.Len() != 0 {
				t.Fatalf("unexpected stdout: %q", stdout.String())
			}
			if !strings.Contains(output, tt.text) {
				t.Fatalf("output = %q, want %q", output, tt.text)
			}
			if strings.Contains(output, "secret-payload") {
				t.Fatalf("argument value leaked: %q", output)
			}
		})
	}
}

func TestHelpDescribesAllModes(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"-help"}, &stdout, &stderr); code != exitSuccess {
		t.Fatalf("exit code = %d", code)
	}
	for _, text := range []string{"-config", "-check-config", "-version", "-help", "clean-relay.json", "current directory", "offline"} {
		if !strings.Contains(stdout.String(), text) {
			t.Errorf("help is missing %q", text)
		}
	}
}

func TestConfigurationErrors(t *testing.T) {
	for _, body := range []string{
		`{`,
		`{}`,
		`{"upstream_origin":"https://user:secret-payload@example.test/path?token=secret-payload"}`,
		`{"upstream_origin":"http://example.test","secret-payload":{"body":"secret-payload"}}`,
		`{"upstream_origin":"http://example.test","listen":"0.0.0.0:17880"}`,
		`{"upstream_origin":"http://example.test","max_in_flight":null}`,
	} {
		path := writeConfigFile(t, body)
		var stdout, stderr bytes.Buffer
		if code := run([]string{"-config", path, "-check-config"}, &stdout, &stderr); code != exitUsage {
			t.Fatalf("configuration %q: exit code = %d, want 2", body, code)
		}
		if stdout.Len() != 0 || !strings.Contains(stderr.String(), "invalid configuration") {
			t.Fatalf("stdout = %q, stderr = %q", stdout.String(), stderr.String())
		}
		if strings.Contains(stderr.String(), "secret-payload") {
			t.Fatalf("configuration value leaked: %q", stderr.String())
		}
	}
}

func TestCheckConfigDefaultPathIsOffline(t *testing.T) {
	var upstreamCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()
	occupied := listenForTest(t)
	path := writeConfig(t, occupied.Addr().String(), upstream.URL)
	t.Chdir(filepath.Dir(path))
	var stdout, stderr bytes.Buffer
	if code := run([]string{"-check-config"}, &stdout, &stderr); code != exitSuccess {
		t.Fatalf("exit code = %d; stderr = %q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "valid") || stderr.Len() != 0 {
		t.Fatalf("stdout = %q, stderr = %q", stdout.String(), stderr.String())
	}
	if upstreamCalls.Load() != 0 {
		t.Fatalf("offline check contacted the upstream %d times", upstreamCalls.Load())
	}
}

func TestConfigurationDiagnosticsIdentifyTheConstraint(t *testing.T) {
	tests := []struct{ body, diagnostic string }{
		{`{}`, "upstream_origin: required"},
		{`{"upstream_origin":"http://example.test","listen":"0.0.0.0:17880"}`, "listen: host must be a loopback"},
		{`{"upstream_origin":"http://example.test","max_in_flight":null}`, "max_in_flight: null is not allowed"},
		{`{"upstream_origin":"http://example.test","response_header_timeout":"secret-payload"}`, "response_header_timeout: must be a nonnegative"},
		{`{"upstream_origin":"http://user:secret-payload@example.test"}`, "upstream_origin: usernames and passwords are not allowed"},
		{`{"upstream_origin":"http://example.test","secret-payload":null}`, "unknown configuration key"},
	}
	for _, tt := range tests {
		var stdout, stderr bytes.Buffer
		if code := run([]string{"-config", writeConfigFile(t, tt.body), "-check-config"}, &stdout, &stderr); code != exitUsage {
			t.Fatalf("exit code = %d", code)
		}
		if !strings.Contains(stderr.String(), tt.diagnostic) || strings.Contains(stderr.String(), "secret-payload") {
			t.Fatalf("diagnostic = %q; want %q without supplied values", stderr.String(), tt.diagnostic)
		}
	}
}

func TestOccupiedPortReturnsRuntimeError(t *testing.T) {
	occupied := listenForTest(t)
	path := writeConfig(t, occupied.Addr().String(), "http://upstream.invalid")
	var stdout, stderr bytes.Buffer
	if code := run([]string{"-config", path}, &stdout, &stderr); code != exitRuntime {
		t.Fatalf("exit code = %d, want 1; stderr = %q", code, stderr.String())
	}
	if stdout.Len() != 0 || !strings.Contains(stderr.String(), "level=ERROR") {
		t.Fatalf("stdout = %q, stderr = %q", stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "cannot listen") || !strings.Contains(stderr.String(), occupied.Addr().String()) {
		t.Fatalf("port conflict diagnostic = %q", stderr.String())
	}
}

// Exercise os.Exit and ldflags with the ordinary executable interface. Signal
// delivery to a Windows console is covered separately by the console smoke test.
func TestCLIExecutable(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "clean-relay.exe")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	build := exec.CommandContext(ctx, "go", "build", "-ldflags", "-X main.version=cli-test", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v\n%s", err, output)
	}
	occupied := listenForTest(t)
	validConfig := writeConfig(t, occupied.Addr().String(), "http://upstream.invalid")
	invalidConfig := writeConfigFile(t, `{"upstream_origin":"http://user:secret-payload@example.test?token=secret-payload"}`)
	missingConfig := filepath.Join(t.TempDir(), "missing.json")
	tests := []struct {
		name string
		args []string
		code int
		text string
	}{
		{"version", []string{"-version", "-config", missingConfig}, 0, "clean-relay cli-test"},
		{"help", []string{"-help", "-config", missingConfig}, 0, "Usage: clean-relay"},
		{"unknown", []string{"-unknown"}, 2, "invalid arguments"},
		{"position", []string{"-version", "extra"}, 2, "positional arguments"},
		{"missing_config", []string{"-config", missingConfig}, 2, "cannot read configuration"},
		{"invalid_config", []string{"-config", invalidConfig}, 2, "invalid configuration"},
		{"offline_check", []string{"-check-config", "-config", validConfig}, 0, "Configuration is valid"},
		{"occupied_port", []string{"-config", validConfig}, 1, "level=ERROR"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, binary, tt.args...)
			cmd.Dir = t.TempDir()
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			err := cmd.Run()
			code := 0
			if err != nil {
				var exitErr *exec.ExitError
				if !errors.As(err, &exitErr) {
					t.Fatalf("run CLI: %v", err)
				}
				code = exitErr.ExitCode()
			}
			if code != tt.code {
				t.Fatalf("exit code = %d, want %d; stdout = %q, stderr = %q", code, tt.code, stdout.String(), stderr.String())
			}
			output := stdout.String() + stderr.String()
			if !strings.Contains(output, tt.text) || strings.Contains(output, "secret-payload") {
				t.Fatalf("unexpected output: %q", output)
			}
		})
	}
}

func writeConfig(t *testing.T, listen, upstream string) string {
	t.Helper()
	body, err := json.Marshal(map[string]any{"listen": listen, "upstream_origin": upstream})
	if err != nil {
		t.Fatal(err)
	}
	return writeConfigFile(t, string(body))
}

func writeConfigFile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "clean-relay.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func listenForTest(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}
