package relay_test

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

const sessionHeader = "X-CleanRelay-Session"
const sessionCompletedJSON = `{"status":"completed","error":null,"output":[]}`
const sessionCompletedSSE = "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"error\":null}}\n\n"

func sessionOptions(mode string) map[string]any {
	return retryOptions(map[string]any{
		"history_scope": "session", "session_header": sessionHeader,
		"cleanup_mode": mode, "inspect_timeout": "1s",
	})
}

func sessionHeaders() http.Header {
	headers := make(http.Header)
	headers.Set(sessionHeader, "synthetic-chat-a")
	headers.Set("Authorization", "Bearer synthetic-session-secret")
	return headers
}

func sessionAppend(t *testing.T, body []byte, items ...string) []byte {
	t.Helper()
	var root map[string]json.RawMessage
	if err := json.Unmarshal(body, &root); err != nil {
		t.Fatal(err)
	}
	var input []json.RawMessage
	if err := json.Unmarshal(root["input"], &input); err != nil {
		t.Fatal(err)
	}
	for _, item := range items {
		input = append(input, json.RawMessage(item))
	}
	var err error
	root["input"], err = json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	result, err := json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func sessionNext(t *testing.T) []byte {
	return sessionAppend(t, []byte(progressiveInput),
		`{"type":"message","id":"b-message","role":"assistant","content":"new B output"}`,
		`{"type":"reasoning","id":"b-reasoning","encrypted_content":"B-cipher"}`,
		`{"role":"user","content":"continue with B"}`)
}

func sessionExchange(t *testing.T, h *streamHarness, path string, body []byte, headers http.Header) (int, []byte, string) {
	t.Helper()
	response := retryDo(t, h, http.MethodPost, path, body, headers)
	wire := streamReadAll(t, response)
	line := streamReceive(t, h.logs.lines, "session request completion")
	return response.StatusCode, wire, line
}

func sessionPlain(t *testing.T, observed retryObserved) []byte {
	t.Helper()
	if observed.header.Get("Content-Encoding") != "gzip" {
		return observed.body
	}
	reader, err := gzip.NewReader(bytes.NewReader(observed.body))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	body, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func sessionHasID(t *testing.T, body []byte, id string) bool {
	t.Helper()
	var root struct {
		Input []map[string]json.RawMessage
	}
	if err := json.Unmarshal(body, &root); err != nil {
		t.Fatal(err)
	}
	for _, item := range root.Input {
		var actual string
		_ = json.Unmarshal(item["id"], &actual)
		if actual == id {
			return true
		}
	}
	return false
}

func sessionAssertWindow(t *testing.T, body []byte, oldReasoning bool, keepB bool) {
	t.Helper()
	for _, id := range []string{"m1", "f1", "o1"} {
		if sessionHasID(t, body, id) {
			t.Fatalf("old portable ID %s was not removed", id)
		}
	}
	for _, id := range []string{"p1", "ref", "keep"} {
		if !sessionHasID(t, body, id) {
			t.Fatalf("protected ID %s was removed", id)
		}
	}
	if sessionHasID(t, body, "r1") != oldReasoning ||
		sessionHasID(t, body, "b-message") != keepB || sessionHasID(t, body, "b-reasoning") != keepB {
		t.Fatalf("wrong old/new boundary: %s", body)
	}
	for _, protected := range []string{"9007199254740993123456789", `"call_id":"pair"`, "0.1234567890123456789", "compaction-opaque"} {
		if !bytes.Contains(body, []byte(protected)) {
			t.Fatalf("lost protected history: %s", protected)
		}
	}
}

func TestSessionRepairRemembersOnlyOldItems(t *testing.T) {
	for _, mode := range []string{"portable_ids", "portable_ids_and_reasoning", "portable_ids_then_reasoning"} {
		for _, source := range []string{"http_json", "sse"} {
			for _, compressed := range []bool{false, true} {
				name := mode + "/" + source
				if compressed {
					name += "/gzip"
				}
				t.Run(name, func(t *testing.T) {
					failures := int64(1)
					if mode == "portable_ids_then_reasoning" {
						failures = 2
					}
					var sequence atomic.Int64
					seen := make(chan retryObserved, 8)
					h := streamNewHarness(t, sessionOptions(mode), func(w http.ResponseWriter, r *http.Request) {
						call := sequence.Add(1)
						seen <- retryObserve(t, r)
						if call <= failures {
							progressiveMismatch(w, source, int(call))
							return
						}
						if source == "sse" {
							w.Header().Set("Content-Type", "text/event-stream")
							_, _ = io.WriteString(w, sessionCompletedSSE)
						} else {
							w.Header().Set("Content-Type", "application/json")
							_, _ = io.WriteString(w, sessionCompletedJSON)
						}
					})
					headers := sessionHeaders()
					headers.Set("Content-Digest", "synthetic-body-digest")
					firstBody, nextBody := []byte(progressiveInput), sessionNext(t)
					if compressed {
						headers.Set("Content-Encoding", "gzip")
						firstBody, nextBody = retryGzip(t, firstBody), retryGzip(t, nextBody)
					}
					status, wire, line := sessionExchange(t, h, "/v1/responses", firstBody, headers)
					wantWire := sessionCompletedJSON
					if source == "sse" {
						wantWire = sessionCompletedSSE
					}
					if status != 200 || string(wire) != wantWire || sequence.Load() != failures+1 ||
						!strings.Contains(line, "recovery_acceptance=completed") {
						t.Fatalf("initial recovery: calls=%d status=%d log=%s", sequence.Load(), status, line)
					}
					for attempt := int64(1); attempt <= failures+1; attempt++ {
						observed := streamReceive(t, seen, "initial upstream request")
						stage := int(attempt)
						if mode == "portable_ids_and_reasoning" && attempt == 2 {
							stage = 3
						}
						progressiveCheckBody(t, sessionPlain(t, observed), stage)
					}
					status, _, line = sessionExchange(t, h, "/v1/responses", nextBody, headers)
					if status != 200 || sequence.Load() != failures+2 || !strings.Contains(line, "upstream_attempts=1") {
						t.Fatalf("cache did not avoid another error: %s", line)
					}
					observed := streamReceive(t, seen, "precleaned next request")
					sessionAssertWindow(t, observed.body, mode == "portable_ids", true)
					if observed.header.Get("Content-Encoding") != "" || observed.header.Get("Content-Digest") != "" ||
						observed.length != int64(len(observed.body)) {
						t.Fatal("preclean retained stale wire metadata")
					}
					if strings.Contains(h.logs.text(), "synthetic-session-secret") || strings.Contains(h.logs.text(), "B-cipher") {
						t.Fatal("session logs leaked a credential or history")
					}
				})
			}
		}
	}
}

func TestSessionPersistentErrorsExtendScopeWithoutCompletion(t *testing.T) {
	var sequence atomic.Int64
	seen := make(chan retryObserved, 8)
	h := streamNewHarness(t, sessionOptions("portable_ids_and_reasoning"), func(w http.ResponseWriter, r *http.Request) {
		call := sequence.Add(1)
		seen <- retryObserve(t, r)
		if call == 2 {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: [DONE]\n\n")
			return
		}
		progressiveMismatch(w, "http_json", int(call))
	})
	_, _, line := sessionExchange(t, h, "/v1/responses", []byte(progressiveInput), sessionHeaders())
	if !strings.Contains(line, "recovery_acceptance=unknown") {
		t.Fatalf("EOF was treated as recovery: %s", line)
	}
	<-seen
	<-seen
	status, _, line := sessionExchange(t, h, "/v1/responses", sessionNext(t), sessionHeaders())
	if status != 400 || !strings.Contains(line, "upstream_attempts=2") || !strings.Contains(line, "scope_cache=scope_extended") {
		t.Fatalf("persistent fault did not repair new history: %s", line)
	}
	sessionAssertWindow(t, streamReceive(t, seen, "persistent fault first send").body, false, true)
	sessionAssertWindow(t, streamReceive(t, seen, "persistent fault repair").body, false, false)
	status, _, line = sessionExchange(t, h, "/v1/responses", sessionNext(t), sessionHeaders())
	if status != 400 || !strings.Contains(line, "upstream_attempts=1") || !strings.Contains(line, "retry_decision=no_candidate") {
		t.Fatalf("fully precleaned history was resent: %s", line)
	}
	sessionAssertWindow(t, streamReceive(t, seen, "fully precleaned request").body, false, false)
}

func TestSessionCompletedRecoveryExtendsSameGeneration(t *testing.T) {
	var sequence atomic.Int64
	seen := make(chan retryObserved, 10)
	h := streamNewHarness(t, sessionOptions("portable_ids_and_reasoning"), func(w http.ResponseWriter, r *http.Request) {
		call := sequence.Add(1)
		seen <- retryObserve(t, r)
		if call == 1 || call == 3 {
			progressiveMismatch(w, "http_json", int(call))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, sessionCompletedJSON)
	})
	sessionExchange(t, h, "/v1/responses", []byte(progressiveInput), sessionHeaders())
	<-seen
	<-seen
	_, _, line := sessionExchange(t, h, "/v1/responses", sessionNext(t), sessionHeaders())
	if !strings.Contains(line, "scope_cache=scope_extended") || !strings.Contains(line, "recovery_acceptance=completed") ||
		!strings.Contains(line, "scope_generation=1 ") {
		t.Fatalf("extended scope changed lifetime or was not confirmed: %s", line)
	}
	sessionAssertWindow(t, streamReceive(t, seen, "B first send").body, false, true)
	sessionAssertWindow(t, streamReceive(t, seen, "B repair").body, false, false)
	third := sessionAppend(t, sessionNext(t), `{"type":"reasoning","id":"c-reasoning","encrypted_content":"C-cipher"}`)
	sessionExchange(t, h, "/v1/responses", third, sessionHeaders())
	observed := streamReceive(t, seen, "C first send")
	sessionAssertWindow(t, observed.body, false, false)
	if !sessionHasID(t, observed.body, "c-reasoning") || sequence.Load() != 5 {
		t.Fatal("extended scope removed later C history or repeated the retry")
	}
}

func TestSessionIdentityUnavailableDoesNotUseRequestRepair(t *testing.T) {
	for _, values := range [][]string{nil, {""}, {"one", "two"}, {"one,two"}, {"bad value"}, {strings.Repeat("x", 1025)}} {
		t.Run(strings.Join(values, ","), func(t *testing.T) {
			h := streamNewHarness(t, sessionOptions("portable_ids_and_reasoning"), func(w http.ResponseWriter, r *http.Request) {
				observed := retryObserve(t, r)
				progressiveCheckBody(t, observed.body, 1)
				progressiveMismatch(w, "http_json", 1)
			})
			headers := sessionHeaders()
			headers.Del(sessionHeader)
			headers[sessionHeader] = values
			status, _, line := sessionExchange(t, h, "/v1/responses", []byte(progressiveInput), headers)
			if status != 400 || h.calls.Load() != 1 || !strings.Contains(line, "session_identity_unavailable") {
				t.Fatalf("invalid identity fell back to request-wide repair: %s", line)
			}
		})
	}
}

func TestSessionPrecleanHonorsRetryExclusions(t *testing.T) {
	var sequence atomic.Int64
	seen := make(chan retryObserved, 16)
	h := streamNewHarness(t, sessionOptions("portable_ids_and_reasoning"), func(w http.ResponseWriter, r *http.Request) {
		call := sequence.Add(1)
		seen <- retryObserve(t, r)
		if call == 1 {
			progressiveMismatch(w, "http_json", 1)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, sessionCompletedJSON)
	})
	sessionExchange(t, h, "/v1/responses", []byte(progressiveInput), sessionHeaders())
	<-seen
	<-seen
	for _, tc := range []struct {
		name, header, path string
		background         bool
	}{
		{name: "idempotency_key", header: "Idempotency-Key"},
		{name: "idempotency_key", header: "X-Idempotency-Key"},
		{name: "stream_resume", header: "Last-Event-ID"},
		{name: "background_request", background: true},
		{name: "out_of_scope", path: "/v1/embeddings"},
	} {
		t.Run(tc.name+"/"+tc.header, func(t *testing.T) {
			headers, body, path := sessionHeaders(), sessionNext(t), "/v1/responses"
			if tc.header != "" {
				headers.Set(tc.header, "synthetic-token")
			}
			if tc.path != "" {
				path = tc.path
			}
			if tc.background {
				body = append([]byte(`{"background":true,`), body[1:]...)
			}
			_, _, line := sessionExchange(t, h, path, body, headers)
			observed := streamReceive(t, seen, "excluded request")
			if !bytes.Equal(observed.body, body) || !strings.Contains(line, "retry_decision="+tc.name) ||
				!strings.Contains(line, "upstream_attempts=1") {
				t.Fatalf("exclusion did not protect first-send history: %s", line)
			}
		})
	}
}

func TestSessionWindowsAndNamespaces(t *testing.T) {
	var sequence atomic.Int64
	seen := make(chan retryObserved, 16)
	h := streamNewHarness(t, sessionOptions("portable_ids_and_reasoning"), func(w http.ResponseWriter, r *http.Request) {
		seen <- retryObserve(t, r)
		if sequence.Add(1) == 1 {
			progressiveMismatch(w, "http_json", 1)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, sessionCompletedJSON)
	})
	first := []byte(strings.Replace(progressiveInput, `"id":"m1"`, `"id":"m1","internal_chat_message_metadata_passthrough":"first"`, 1))
	sessionExchange(t, h, "/v1/responses", first, sessionHeaders())
	<-seen
	<-seen
	next := []byte(strings.Replace(string(sessionNext(t)), `"id":"m1"`, `"id":"m1","internal_chat_message_metadata_passthrough":"changed"`, 1))
	_, _, line := sessionExchange(t, h, "/v1/responses", next, sessionHeaders())
	sessionAssertWindow(t, streamReceive(t, seen, "metadata-cleaned matching").body, false, true)
	if !strings.Contains(line, "removed_fields=1") || !strings.Contains(line, "scope_pre_removed_item_ids=3") {
		t.Fatalf("metadata and scope counts were mixed: %s", line)
	}
	compact := []byte(`{"model":"m","input":[{"role":"user","content":"compacted summary"},{"type":"reasoning","id":"post-compact","encrypted_content":"fresh"}]}`)
	sessionExchange(t, h, "/v1/responses", compact, sessionHeaders())
	if observed := streamReceive(t, seen, "compacted window"); !bytes.Equal(observed.body, compact) {
		t.Fatal("a compacted fresh window was changed")
	}
	sessionExchange(t, h, "/v1/responses/compact", []byte(progressiveInput), sessionHeaders())
	progressiveCheckBody(t, streamReceive(t, seen, "rollback and compact family").body, 3)
	for _, dimension := range []string{"chat", "credential", "model", "path"} {
		t.Run(dimension, func(t *testing.T) {
			headers, body, path := sessionHeaders(), []byte(progressiveInput), "/v1/responses"
			switch dimension {
			case "chat":
				headers.Set(sessionHeader, "different-chat")
			case "credential":
				headers.Set("Authorization", "Bearer different-credential")
			case "model":
				body = []byte(strings.Replace(progressiveInput, `"model":"m"`, `"model":"other"`, 1))
			case "path":
				path = "/another/responses"
			}
			sessionExchange(t, h, path, body, headers)
			if observed := streamReceive(t, seen, "isolated namespace"); !bytes.Equal(observed.body, body) {
				t.Fatal("recovery actions leaked to another namespace")
			}
		})
	}
}

func TestSessionDiagnosticsAreExplicitAndRedacted(t *testing.T) {
	h := streamNewHarness(t, retryOptions(map[string]any{
		"enabled": false, "session_diagnostics": true, "session_header": sessionHeader,
	}), func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, sessionCompletedJSON)
	})
	digests := make([]string, 0, 4)
	for i := 0; i < 4; i++ {
		headers := sessionHeaders()
		if i == 2 {
			headers.Set(sessionHeader, "synthetic-other-chat")
		}
		if i == 3 {
			headers.Set("Authorization", "Bearer synthetic-other-secret")
		}
		_, _, line := sessionExchange(t, h, "/v1/responses", []byte(progressiveInput), headers)
		for _, part := range strings.Fields(line) {
			if strings.HasPrefix(part, "session_identity_4.hmac=") {
				digests = append(digests, strings.TrimPrefix(part, "session_identity_4.hmac="))
			}
		}
		for _, forbidden := range []string{"synthetic-chat-a", "synthetic-other-chat", "synthetic-session-secret", "synthetic-other-secret", "reasoning-opaque"} {
			if strings.Contains(line, forbidden) {
				t.Fatal("diagnostics exposed a raw identity or history value")
			}
		}
	}
	if len(digests) != 4 || len(digests[0]) != 16 || digests[0] != digests[1] || digests[0] == digests[2] || digests[0] == digests[3] {
		t.Fatalf("diagnostics did not provide stable isolated short HMACs: %v", digests)
	}
}
