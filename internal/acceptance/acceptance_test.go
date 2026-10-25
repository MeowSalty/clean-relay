package acceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testDeadline = 3 * time.Second

var testOutput = []json.RawMessage{
	json.RawMessage(`{"type":"reasoning","id":"synthetic-reasoning-id","summary":[],"encrypted_content":"synthetic-reasoning-secret","opaque":{"integer":9007199254740993,"decimal":0.123456789012345678901,"exponent":1e+400}}`),
	json.RawMessage(`{"type":"function_call","id":"synthetic-item-id","call_id":"synthetic-call-secret","status":"completed","name":"cleanrelay_echo","arguments":"{\"value\":\"cleanrelay-probe\"}"}`),
	json.RawMessage(`{"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"synthetic-tool-text-secret"}]}`),
}

type scenario struct {
	t          *testing.T
	server     *httptest.Server
	posts      atomic.Int64
	healths    atomic.Int64
	canceled   chan struct{}
	uri        string
	metadata   bool
	override   func(int, http.ResponseWriter, *http.Request) bool
	mu         sync.Mutex
	toolUser   json.RawMessage
	toolSchema json.RawMessage
}

func newScenario(t *testing.T, prefix string, metadata bool, override func(int, http.ResponseWriter, *http.Request) bool) *scenario {
	t.Helper()
	s := &scenario{t: t, canceled: make(chan struct{}), uri: strings.TrimSuffix(prefix, "/") + "/responses", metadata: metadata, override: override}
	s.server = httptest.NewServer(http.HandlerFunc(s.serve))
	t.Cleanup(func() {
		s.server.CloseClientConnections()
		s.server.Close()
	})
	return s
}

func testOptions(base string) Options {
	return Options{BaseURL: base, Model: "synthetic-model-secret", APIKey: "synthetic-key  unchanged-secret", Timeout: testDeadline}
}

func (s *scenario) serve(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/_cleanrelay/healthz" {
		s.healths.Add(1)
		if r.Method != http.MethodGet || r.RequestURI != "/_cleanrelay/healthz" {
			s.t.Errorf("health must use GET on the origin's health path")
		}
		writeJSON(w, `{"status":"ok"}`)
		return
	}
	step := int(s.posts.Add(1))
	if r.Method != http.MethodPost || r.RequestURI != s.uri {
		s.t.Errorf("probe %d used wrong method or escaped path: %s %s", step, r.Method, r.RequestURI)
	}
	if r.Header.Get("Authorization") != "Bearer "+testOptions("").APIKey {
		s.t.Errorf("probe %d did not preserve the supplied authorization", step)
	}
	if r.Header.Get("Content-Type") != "application/json" || r.Header.Get("Accept-Encoding") != "" {
		s.t.Errorf("probe %d used unexpected content or compression headers", step)
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		s.t.Errorf("read synthetic probe %d: %v", step, err)
		return
	}
	request := testObject(s.t, body)
	assertProbe(s.t, request, s.metadata)
	var stream bool
	if json.Unmarshal(request["stream"], &stream) != nil || stream != (step == 2 || step == 5) {
		s.t.Errorf("probe %d stream setting is incorrect", step)
	}
	if s.override != nil && s.override(step, w, r) {
		return
	}
	switch step {
	case 1, 6:
		writeJSON(w, completedResponse("synthetic-response-body-secret"))
	case 2:
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		// This single event is well above Scanner's default 64 KiB token size.
		delta := strings.Repeat("synthetic-stream-secret-", 4000)
		_, _ = io.WriteString(w, ": heartbeat\r\n\r\nevent: response.output_text.delta\r\ndata: {\"type\":\"response.output_text.delta\",\r\ndata: \"delta\":\""+delta+"\"}\r\n\r\n"+completionEvent())
	case 3:
		var input []json.RawMessage
		if json.Unmarshal(request["input"], &input) != nil || len(input) != 1 {
			s.t.Error("tool call must contain one original synthetic user")
			return
		}
		assertEchoTool(s.t, request, true)
		s.mu.Lock()
		s.toolUser = bytes.Clone(input[0])
		s.toolSchema = bytes.Clone(request["tools"])
		s.mu.Unlock()
		output, err := json.Marshal(testOutput)
		if err != nil {
			s.t.Error(err)
			return
		}
		writeJSON(w, `{"id":"synthetic-response-id-secret","status":"completed","error":null,"output":`+string(output)+`}`)
	case 4:
		s.assertToolContinuation(request)
		writeJSON(w, completedResponse("synthetic-tool-result-secret"))
	case 5:
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, ": waiting\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"\"}\n\n"+deltaEvent("synthetic-cancel-secret"))
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
			close(s.canceled)
		case <-time.After(testDeadline):
			s.t.Error("synthetic upstream did not observe actual stream cancellation")
		}
	default:
		s.t.Errorf("unexpected extra paid-style probe %d", step)
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func assertProbe(t *testing.T, request map[string]json.RawMessage, metadata bool) {
	t.Helper()
	var model string
	if json.Unmarshal(request["model"], &model) != nil || model != testOptions("").Model {
		t.Error("synthetic model was not preserved")
	}
	if string(request["store"]) != "false" || string(request["max_output_tokens"]) != "2048" {
		t.Error("every business probe must explicitly disable storage and bound output tokens")
	}
	if _, exists := request["previous_response_id"]; exists {
		t.Error("previous_response_id must never be sent")
	}
	var input []json.RawMessage
	if json.Unmarshal(request["input"], &input) != nil || len(input) == 0 {
		t.Fatal("synthetic input must be a nonempty array of direct objects")
	}
	user := testObject(t, input[0])
	if string(user["type"]) != `"message"` || string(user["role"]) != `"user"` {
		t.Error("first input must be the synthetic user message")
	}
	marker, exists := user["internal_chat_message_metadata_passthrough"]
	if exists != metadata || (exists && string(marker) != `{"acceptance_probe":true}`) {
		t.Error("direct synthetic user metadata marker is incorrect")
	}
}

func assertEchoTool(t *testing.T, request map[string]json.RawMessage, forced bool) {
	t.Helper()
	if string(request["parallel_tool_calls"]) != "false" {
		t.Error("tool probes must disable parallel tool calls")
	}
	var tools []json.RawMessage
	if json.Unmarshal(request["tools"], &tools) != nil || len(tools) != 1 {
		t.Fatal("tool probe must retain exactly one local echo tool")
	}
	tool := testObject(t, tools[0])
	if string(tool["type"]) != `"function"` || string(tool["name"]) != `"cleanrelay_echo"` || string(tool["strict"]) != "true" {
		t.Error("echo tool must have Responses syntax and strict schema")
	}
	parameters := testObject(t, tool["parameters"])
	properties := testObject(t, parameters["properties"])
	value := testObject(t, properties["value"])
	if string(parameters["additionalProperties"]) != "false" || string(parameters["required"]) != `["value"]` || string(value["enum"]) != `["cleanrelay-probe"]` {
		t.Error("echo schema must constrain exactly one constant value")
	}
	if forced {
		choice := testObject(t, request["tool_choice"])
		if string(choice["type"]) != `"function"` || string(choice["name"]) != `"cleanrelay_echo"` {
			t.Error("initial tool choice must force the echo function")
		}
	} else if string(request["tool_choice"]) != `"none"` {
		t.Error("tool result continuation must prohibit further tool calls")
	}
}

func (s *scenario) assertToolContinuation(request map[string]json.RawMessage) {
	s.t.Helper()
	assertEchoTool(s.t, request, false)
	var input []json.RawMessage
	if json.Unmarshal(request["input"], &input) != nil || len(input) != len(testOutput)+2 {
		s.t.Error("tool continuation must contain original user, all raw output and tool result")
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !bytes.Equal(compactJSON(s.t, input[0]), compactJSON(s.t, s.toolUser)) ||
		!bytes.Equal(compactJSON(s.t, request["tools"]), compactJSON(s.t, s.toolSchema)) {
		s.t.Error("tool continuation changed original user or tool schema")
	}
	for i, want := range testOutput {
		if !bytes.Equal(compactJSON(s.t, input[i+1]), compactJSON(s.t, want)) {
			s.t.Errorf("raw output %d changed, including reasoning or numeric precision", i)
		}
	}
	result := testObject(s.t, input[len(input)-1])
	if string(result["type"]) != `"function_call_output"` || string(result["call_id"]) != `"synthetic-call-secret"` {
		s.t.Error("tool result must use the returned call ID")
	}
	var output string
	if json.Unmarshal(result["output"], &output) != nil || output != `{"value":"cleanrelay-probe"}` {
		s.t.Error("tool result must be a static result without executing returned arguments")
	}
}

func testObject(t *testing.T, body []byte) map[string]json.RawMessage {
	t.Helper()
	object, err := decodeObject(body)
	if err != nil {
		t.Fatalf("invalid synthetic JSON: %v", err)
	}
	return object
}

func compactJSON(t *testing.T, raw []byte) []byte {
	t.Helper()
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err != nil {
		t.Fatal(err)
	}
	return compact.Bytes()
}

func writeJSON(w http.ResponseWriter, body string) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, body)
}

func completedResponse(text string) string {
	encoded, _ := json.Marshal(text)
	return `{"status":"completed","error":null,"output":[{"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":` + string(encoded) + `}]}]}`
}

func deltaEvent(delta string) string {
	encoded, _ := json.Marshal(delta)
	return "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":" + string(encoded) + "}\n\n"
}

func completionEvent() string {
	return "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"error\":null}}\n\n"
}

func assertManualPending(t *testing.T, report Report) {
	t.Helper()
	want := []string{"client_conversation", "client_tool", "client_cancel", "cleaning_counts", "upstream_cancellation", "client_rollback"}
	if len(report.ManualChecks) != len(want) {
		t.Fatalf("manual checks = %d, want %d", len(report.ManualChecks), len(want))
	}
	for i, check := range report.ManualChecks {
		if check.Name != want[i] || check.Status != "pending" || check.HTTPStatus != 0 || check.DurationMS != 0 {
			t.Errorf("manual check %d must remain pending: %+v", i, check)
		}
	}
}

func assertSafeReport(t *testing.T, report Report, forbidden ...string) {
	t.Helper()
	body, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range append([]string{testOptions("").APIKey, testOptions("").Model, "synthetic-response-body-secret", "synthetic-reasoning-secret", "synthetic-call-secret", "synthetic-tool-text-secret", "synthetic-tool-result-secret", "synthetic-stream-secret", "synthetic-cancel-secret"}, forbidden...) {
		if secret != "" && bytes.Contains(body, []byte(secret)) {
			t.Errorf("report exposed a synthetic secret")
		}
	}
	assertManualPending(t, report)
}

func assertFailureAt(t *testing.T, report Report, failed int, status int) {
	t.Helper()
	if report.AutomatedPassed || len(report.Checks) != 8 {
		t.Fatalf("failed report must have all checks and AutomatedPassed=false: %+v", report)
	}
	for i, check := range report.Checks {
		want := "passed"
		if i == failed {
			want = "failed"
		} else if i > failed {
			want = "skipped"
		}
		if check.Status != want {
			t.Errorf("check %d status %s, want %s", i, check.Status, want)
		}
	}
	if report.Checks[failed].HTTPStatus != status {
		t.Errorf("failed HTTP status = %d, want %d", report.Checks[failed].HTTPStatus, status)
	}
	assertSafeReport(t, report)
}

func waitCanceled(t *testing.T, canceled <-chan struct{}) {
	t.Helper()
	select {
	case <-canceled:
	case <-time.After(testDeadline):
		t.Fatal("fake upstream did not observe cancellation")
	}
}

func TestRunCompleteFakeUpstreamAndExplicitDirect(t *testing.T) {
	prefix := "/synthetic-path-secret%2fsegment//%76%31/%2e%2E/"
	s := newScenario(t, prefix, true, nil)
	var directCalls atomic.Int64
	direct := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		directCalls.Add(1)
		if r.Method != http.MethodPost || r.RequestURI != "/direct%2Fsecret/%76%31/responses" || r.Header.Get("Authorization") != "Bearer "+testOptions("").APIKey {
			t.Error("explicit direct probe did not preserve its path or authorization")
		}
		body, _ := io.ReadAll(r.Body)
		request := testObject(t, body)
		assertProbe(t, request, false)
		if string(request["stream"]) != "false" || len(request["tools"]) != 0 {
			t.Error("direct probe must be a plain nonstreaming synthetic conversation")
		}
		writeJSON(w, completedResponse("synthetic-direct-secret"))
	}))
	defer direct.Close()
	options := testOptions(s.server.URL + prefix)
	options.DirectBaseURL = direct.URL + "/direct%2Fsecret/%76%31/"
	report, err := Run(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	if !report.AutomatedPassed || report.SchemaVersion != 1 || report.StartedAt.IsZero() || report.StartedAt.Location() != time.UTC || len(report.Checks) != 8 {
		t.Fatalf("complete synthetic chain did not pass: %+v", report)
	}
	wantNames := []string{"health", "conversation", "sse", "tool_call", "tool_result", "cancel", "after_cancel", "direct_conversation"}
	for i, check := range report.Checks {
		if check.Name != wantNames[i] || check.Status != "passed" || check.HTTPStatus != 200 || check.DurationMS < 0 {
			t.Errorf("unexpected complete check: %+v", check)
		}
	}
	waitCanceled(t, s.canceled)
	if s.posts.Load() != 6 || s.healths.Load() != 1 || directCalls.Load() != 1 {
		t.Errorf("unexpected request counts: relay=%d health=%d direct=%d", s.posts.Load(), s.healths.Load(), directCalls.Load())
	}
	assertSafeReport(t, report, s.server.URL, prefix, direct.URL, "synthetic-direct-secret")
}

func TestRunAuthenticationAndRedirectFailuresStop(t *testing.T) {
	for _, code := range []int{http.StatusUnauthorized, http.StatusTemporaryRedirect, http.StatusFound} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			var followed atomic.Int64
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				followed.Add(1)
				writeJSON(w, completedResponse("synthetic-redirect-secret"))
			}))
			defer target.Close()
			s := newScenario(t, "/v1", true, func(step int, w http.ResponseWriter, r *http.Request) bool {
				w.Header().Set("Location", target.URL+"/synthetic-location-secret")
				w.WriteHeader(code)
				_, _ = io.WriteString(w, `{"error":{"message":"synthetic-error-body-secret"}}`)
				return true
			})
			options := testOptions(s.server.URL + "/v1")
			options.DirectBaseURL = target.URL + "/v1"
			report, err := Run(context.Background(), options)
			if err != nil {
				t.Fatal(err)
			}
			assertFailureAt(t, report, 1, code)
			if followed.Load() != 0 || s.posts.Load() != 1 {
				t.Error("failed authorization or redirect generated extra model requests")
			}
			assertSafeReport(t, report, target.URL, "synthetic-location-secret", "synthetic-error-body-secret")
		})
	}
}

func TestRunSSEFailuresStop(t *testing.T) {
	tests := []struct{ name, stream string }{
		{"error event", "event: error\ndata: {\"message\":\"synthetic-sse-error-secret\"}\n\n"},
		{"error data type", "data: {\"type\":\"error\",\"message\":\"synthetic-sse-error-secret\"}\n\n"},
		{"failed", "event: response.failed\ndata: {\"type\":\"response.failed\"}\n\n"},
		{"incomplete", "data: {\"type\":\"response.incomplete\"}\n\n"},
		{"no completion", deltaEvent("synthetic-partial-secret")},
		{"done without completion", deltaEvent("partial") + "data: [DONE]\n\n"},
		{"no text delta", completionEvent()},
		{"empty text delta", deltaEvent("") + completionEvent()},
		{"wrong completion status", deltaEvent("partial") + "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"in_progress\"}}\n\n"},
		{"missing response status", deltaEvent("partial") + "data: {\"type\":\"response.completed\",\"response\":{}}\n\n"},
		{"completion contains error", deltaEvent("partial") + "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"error\":{\"message\":\"synthetic-sse-error-secret\"}}}\n\n"},
		{"error before completion", deltaEvent("partial") + "data: {\"type\":\"error\"}\n\n" + completionEvent()},
		{"mismatched type", "event: response.output_text.delta\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"},
		{"malformed JSON", deltaEvent("partial") + "data: synthetic-invalid-secret\n\n"},
		{"missing blank delimiter", deltaEvent("partial") + strings.TrimSuffix(completionEvent(), "\n")},
		{"oversize event", deltaEvent(strings.Repeat("s", maxResponseBytes+1)) + completionEvent()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := newScenario(t, "", true, func(step int, w http.ResponseWriter, r *http.Request) bool {
				if step != 2 {
					return false
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, test.stream)
				return true
			})
			report, err := Run(context.Background(), testOptions(s.server.URL))
			if err != nil {
				t.Fatal(err)
			}
			assertFailureAt(t, report, 2, http.StatusOK)
			if s.posts.Load() != 2 {
				t.Error("an invalid SSE stream caused later model probes")
			}
			assertSafeReport(t, report, "synthetic-sse-error-secret", "synthetic-partial-secret", "synthetic-invalid-secret")
		})
	}
}

func TestCancelRequiresNonemptyTextBeforeTerminalEvent(t *testing.T) {
	for _, stream := range []string{completionEvent(), deltaEvent("") + completionEvent(), "data: {\"type\":\"response.failed\"}\n\n", ": heartbeat\n\n"} {
		s := newScenario(t, "", true, func(step int, w http.ResponseWriter, r *http.Request) bool {
			if step != 5 {
				return false
			}
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, stream)
			return true
		})
		report, err := Run(context.Background(), testOptions(s.server.URL))
		if err != nil {
			t.Fatal(err)
		}
		assertFailureAt(t, report, 5, http.StatusOK)
		if s.posts.Load() != 5 {
			t.Error("failed cancellation must skip after_cancel")
		}
	}
}

func TestUntrustedTLSDoesNotReceiveRequests(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSON(w, `{"status":"ok"}`)
	}))
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	defer server.Close()
	report, err := Run(context.Background(), testOptions(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	assertFailureAt(t, report, 0, 0)
	if calls.Load() != 0 {
		t.Error("TLS verification was bypassed")
	}
	assertSafeReport(t, report, server.URL)
}
