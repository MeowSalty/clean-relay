package relay_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cleanrelay/internal/config"
	"cleanrelay/internal/relay"
)

const (
	httpMetadataField = "internal_chat_message_metadata_passthrough"
	httpTestTimeout   = 5 * time.Second
)

// HTTP helpers deliberately have their own prefix: stream_test.go also uses
// package relay_test and owns streaming, cancellation, and lifecycle coverage.
type httpObservedRequest struct {
	Method           string
	Host             string
	Path             string
	RawPath          string
	RawQuery         string
	ForceQuery       bool
	RequestURI       string
	Header           http.Header
	Body             []byte
	ContentLength    int64
	TransferEncoding []string
	ReadErr          error
}

type httpHarness struct {
	upstream *httptest.Server
	server   *httptest.Server
	handler  *relay.Handler
	client   *http.Client
	calls    atomic.Int64
	requests chan httpObservedRequest
}

func httpNewHarness(t *testing.T, overrides map[string]any, respond http.HandlerFunc) *httpHarness {
	t.Helper()
	h := &httpHarness{requests: make(chan httpObservedRequest, 32)}
	h.upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.calls.Add(1)
		body, err := io.ReadAll(r.Body)
		observed := httpObservedRequest{
			Method: r.Method, Host: r.Host, Path: r.URL.Path, RawPath: r.URL.RawPath,
			RawQuery: r.URL.RawQuery, ForceQuery: r.URL.ForceQuery, RequestURI: r.RequestURI,
			Header: r.Header.Clone(), Body: body, ContentLength: r.ContentLength,
			TransferEncoding: append([]string(nil), r.TransferEncoding...), ReadErr: err,
		}
		select {
		case h.requests <- observed:
		default:
			http.Error(w, "unexpected extra upstream requests", http.StatusInternalServerError)
			return
		}
		if err != nil {
			http.Error(w, "cannot read upstream request", http.StatusBadRequest)
			return
		}
		if respond != nil {
			respond(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Upstream-Result", "received")
		if _, err := io.WriteString(w, `{"upstream":"ok"}`); err != nil {
			t.Errorf("write fake upstream response: %v", err)
		}
	}))
	t.Cleanup(h.upstream.Close)

	settings := map[string]any{
		"listen": "127.0.0.1:1", "upstream_origin": h.upstream.URL,
		"response_header_timeout": "2s",
	}
	for key, value := range overrides {
		settings[key] = value
	}
	data, err := json.Marshal(settings)
	if err != nil {
		t.Fatalf("encode test configuration: %v", err)
	}
	cfg, err := config.Parse(data)
	if err != nil {
		t.Fatalf("parse test configuration: %v", err)
	}
	h.handler = relay.New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(h.handler.CloseIdleConnections)
	h.server = httptest.NewServer(h.handler)
	t.Cleanup(h.server.Close)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DisableCompression = true
	transport.ExpectContinueTimeout = 100 * time.Millisecond
	h.client = &http.Client{
		Transport: transport, Timeout: httpTestTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	t.Cleanup(transport.CloseIdleConnections)
	return h
}

func httpNewRequest(t *testing.T, h *httpHarness, method, target string, body []byte) *http.Request {
	t.Helper()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, h.server.URL+target, reader)
	if err != nil {
		t.Fatalf("build client request: %v", err)
	}
	return req
}

// Hiding the concrete reader makes the client send an unknown-length body,
// including an actually empty chunked body rather than http.NoBody.
type httpUnknownLengthReader struct{ io.Reader }

func httpSetChunkedBody(req *http.Request, body []byte) {
	req.Body = io.NopCloser(httpUnknownLengthReader{bytes.NewReader(body)})
	req.ContentLength = -1
	req.TransferEncoding = []string{"chunked"}
	req.GetBody = nil
}

func httpDo(t *testing.T, h *httpHarness, req *http.Request) (*http.Response, []byte) {
	t.Helper()
	res, err := h.client.Do(req)
	if err != nil {
		t.Fatalf("client request: %v", err)
	}
	body, readErr := io.ReadAll(res.Body)
	closeErr := res.Body.Close()
	if readErr != nil {
		t.Fatalf("read client response: %v", readErr)
	}
	if closeErr != nil {
		t.Fatalf("close client response: %v", closeErr)
	}
	return res, body
}

func httpNextRequest(t *testing.T, h *httpHarness) httpObservedRequest {
	t.Helper()
	select {
	case got := <-h.requests:
		if got.ReadErr != nil {
			t.Fatalf("fake upstream body read: %v", got.ReadErr)
		}
		if calls := h.calls.Load(); calls != 1 {
			t.Errorf("upstream calls = %d, want 1", calls)
		}
		return got
	case <-time.After(httpTestTimeout):
		t.Fatal("fake upstream received no request within the test deadline")
		return httpObservedRequest{}
	}
}

func httpAssertOK(t *testing.T, res *http.Response, body []byte) {
	t.Helper()
	if res.StatusCode != http.StatusOK || string(body) != `{"upstream":"ok"}` {
		t.Fatalf("response = %d %q, want fake upstream success", res.StatusCode, body)
	}
}

func httpAssertNoUpstream(t *testing.T, h *httpHarness) {
	t.Helper()
	if calls := h.calls.Load(); calls != 0 {
		t.Errorf("locally rejected request made %d upstream calls, want 0", calls)
	}
}

func httpAssertLocalError(t *testing.T, h *httpHarness, res *http.Response, body []byte, status int, code string) {
	t.Helper()
	httpAssertNoUpstream(t, h)
	if res.StatusCode != status {
		t.Errorf("local status = %d, want %d; body = %q", res.StatusCode, status, body)
	}
	mediaType, _, err := mime.ParseMediaType(res.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		t.Errorf("local Content-Type = %q, want application/json", res.Header.Get("Content-Type"))
	}
	var envelope struct {
		Error struct {
			Type      string `json:"type"`
			Code      string `json:"code"`
			Message   string `json:"message"`
			RequestID string `json:"request_id"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Errorf("local error is not the JSON envelope: %v; body = %q", err, body)
		return
	}
	if envelope.Error.Type != "clean_relay_error" || envelope.Error.Code != code {
		t.Errorf("local error type/code = %q/%q, want clean_relay_error/%s", envelope.Error.Type, envelope.Error.Code, code)
	}
	if envelope.Error.Message == "" || envelope.Error.RequestID == "" {
		t.Errorf("local error lacks message or request_id: %s", body)
	}
}

func httpDecodeJSON(t *testing.T, body []byte) any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		t.Fatalf("decode complete JSON: %v; body = %q", err, body)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		t.Fatalf("content after JSON value: %v; trailing = %q", err, trailing)
	}
	return value
}

func httpAssertJSONEqual(t *testing.T, got, want []byte) {
	t.Helper()
	if !reflect.DeepEqual(httpDecodeJSON(t, got), httpDecodeJSON(t, want)) {
		t.Errorf("JSON differs:\ngot:  %s\nwant: %s", got, want)
	}
}

func httpRawObject(t *testing.T, body []byte) map[string]json.RawMessage {
	t.Helper()
	var object map[string]json.RawMessage
	if err := json.Unmarshal(body, &object); err != nil || object == nil {
		t.Fatalf("decode JSON object: %v; body = %q", err, body)
	}
	return object
}

func httpRawArray(t *testing.T, body []byte) []json.RawMessage {
	t.Helper()
	var items []json.RawMessage
	if err := json.Unmarshal(body, &items); err != nil {
		t.Fatalf("decode input array: %v; body = %q", err, body)
	}
	return items
}

func httpReadFixture(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "..", "testdata", "requests", name))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return body
}

func httpAssertBufferedBody(t *testing.T, got httpObservedRequest) {
	t.Helper()
	if got.ContentLength != int64(len(got.Body)) {
		t.Errorf("upstream ContentLength = %d, body has %d bytes", got.ContentLength, len(got.Body))
	}
	if len(got.Body) != 0 && got.Header.Get("Content-Length") != strconv.Itoa(len(got.Body)) {
		t.Errorf("upstream Content-Length header = %q, want %d", got.Header.Get("Content-Length"), len(got.Body))
	}
	if len(got.TransferEncoding) != 0 || len(got.Header.Values("Transfer-Encoding")) != 0 {
		t.Errorf("buffered upstream request still uses Transfer-Encoding: %v / %v", got.TransferEncoding, got.Header.Values("Transfer-Encoding"))
	}
	if len(got.Header.Values("Expect")) != 0 {
		t.Errorf("consumed Expect reached upstream: %v", got.Header.Values("Expect"))
	}
}

func TestHTTPBaselineJSONAuthorizationAndHost(t *testing.T) {
	h := httpNewHarness(t, nil, nil)
	before := httpReadFixture(t, "metadata.before.json")
	want := httpReadFixture(t, "metadata.after.json")
	req := httpNewRequest(t, h, http.MethodPost, "/api/gateway/v1/responses", before)
	req.Host = "untrusted-client.example:4567"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer synthetic-client-key")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Client-Business", "keep-this")
	res, body := httpDo(t, h, req)
	httpAssertOK(t, res, body)
	got := httpNextRequest(t, h)
	httpAssertJSONEqual(t, got.Body, want)
	httpAssertBufferedBody(t, got)
	upstreamURL, err := url.Parse(h.upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	if got.Method != http.MethodPost || got.Host != upstreamURL.Host || got.Path != "/api/gateway/v1/responses" {
		t.Errorf("upstream target = %s %s%s", got.Method, got.Host, got.Path)
	}
	for key, value := range map[string]string{
		"Authorization": "Bearer synthetic-client-key", "Accept": "application/json",
		"X-Client-Business": "keep-this", "Content-Type": "application/json",
	} {
		if got.Header.Get(key) != value {
			t.Errorf("upstream %s = %q, want %q", key, got.Header.Get(key), value)
		}
	}
	beforeItems := httpRawArray(t, httpRawObject(t, before)["input"])
	afterRoot := httpRawObject(t, got.Body)
	afterItems := httpRawArray(t, afterRoot["input"])
	if len(beforeItems) != 8 || len(afterItems) != 8 {
		t.Fatalf("baseline item counts = %d -> %d, want 8 -> 8", len(beforeItems), len(afterItems))
	}
	modified, removed := 0, 0
	for i := range beforeItems {
		beforeItem := httpRawObject(t, beforeItems[i])
		afterItem := httpRawObject(t, afterItems[i])
		if _, exists := beforeItem[httpMetadataField]; exists {
			removed++
		}
		if _, exists := afterItem[httpMetadataField]; exists {
			t.Errorf("input[%d] still has the direct metadata field", i)
		}
		if !reflect.DeepEqual(httpDecodeJSON(t, beforeItems[i]), httpDecodeJSON(t, afterItems[i])) {
			modified++
		}
	}
	if modified != 7 || removed != 7 {
		t.Errorf("HTTP baseline items/modified/removed = 8/%d/%d, want 8/7/7", modified, removed)
	}
	for key, value := range map[string]string{
		"fixture_large_integer": "9007199254740993", "fixture_decimal": "0.123456789012345678901",
	} {
		if string(afterRoot[key]) != value {
			t.Errorf("%s = %s, want exact numeric token %s", key, afterRoot[key], value)
		}
	}
}

func TestHTTPPreservesPathAndRawQuery(t *testing.T) {
	tests := []struct{ name, target string }{
		{"v1", "/v1/responses"},
		{"multilevel prefix", "/api/gateway/team/v1/responses?x=1&x=2"},
		{"empty repeated parameters", "/api/v1/models?x=1&x=&x=2&&=blank&flag&empty=&"},
		{"escaped slash and percent", "/api/a%2fb/%252F/%3F/%23/%20/v1/responses?token=%2f%2F%25&space=+&space=%20"},
		{"escaped unreserved and Unicode", "/%76%31/%e4%b8%ad/%7eitem?name=%E4%B8%AD&key=%7e&key=~"},
		{"path is not normalized", "/api//gateway/.././v1/responses?x=1"},
		{"query semicolon", "/api/v1/responses?a=1;b=2&x=1&x=2"},
		{"invalid query escape", "/api/v1/responses?bad=%zz&partial=%&x=1&&x=2"},
		{"force empty query", "/api/gateway/v1/models?"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := httpNewHarness(t, nil, nil)
			req := httpNewRequest(t, h, http.MethodGet, tt.target, nil)
			wantURL := *req.URL
			res, body := httpDo(t, h, req)
			httpAssertOK(t, res, body)
			got := httpNextRequest(t, h)
			if got.Path != wantURL.Path || got.RawPath != wantURL.RawPath || got.RawQuery != wantURL.RawQuery || got.ForceQuery != wantURL.ForceQuery {
				t.Errorf("URL changed: got Path=%q RawPath=%q RawQuery=%q ForceQuery=%t; want Path=%q RawPath=%q RawQuery=%q ForceQuery=%t",
					got.Path, got.RawPath, got.RawQuery, got.ForceQuery, wantURL.Path, wantURL.RawPath, wantURL.RawQuery, wantURL.ForceQuery)
			}
			if got.RequestURI != tt.target {
				t.Errorf("upstream RequestURI = %q, want exact target %q", got.RequestURI, tt.target)
			}
		})
	}
}

func TestHTTPUnmodifiedJSONKeepsOriginalBytes(t *testing.T) {
	tests := []struct{ name, body string }{
		{"no target", " \r\n {\"input\" : [{\"content\":\"\\u4e2d / <tag>\",\"n\":9007199254740993}],\"decimal\":0.123456789012345678901} \t\n"},
		{"missing input", ` {"internal_chat_message_metadata_passthrough":null,"n":1E+999} `},
		{"null input", ` {"input":null} `},
		{"string input", ` {"input":"internal_chat_message_metadata_passthrough"} `},
		{"empty input", ` {"input": [ ]} `},
		{"nonarray input", ` {"input":{"internal_chat_message_metadata_passthrough":1}} `},
		{"scalar root", ` 9007199254740993 `},
		{"boolean root", " \n true \t"},
		{"string root", ` "internal_chat_message_metadata_passthrough" `},
		{"null root", " \n null \r\n"},
		{"array root", ` [ {"internal_chat_message_metadata_passthrough":1},"text",null ] `},
		{"only nested target", ` {"input":[{"nested":{"internal_chat_message_metadata_passthrough":1},"arguments":"{\"internal_chat_message_metadata_passthrough\":1}"}]} `},
		{"nested duplicate keys stay opaque", ` {"input":[{"content":{"x":1,"x":2}}],"extension":{"k":1,"k":2}} `},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := httpNewHarness(t, nil, nil)
			req := httpNewRequest(t, h, http.MethodPost, "/custom/request", []byte(tt.body))
			req.Header.Set("Content-Type", "application/json")
			res, body := httpDo(t, h, req)
			httpAssertOK(t, res, body)
			got := httpNextRequest(t, h)
			if !bytes.Equal(got.Body, []byte(tt.body)) {
				t.Errorf("unmodified JSON bytes changed:\ngot:  %q\nwant: %q", got.Body, tt.body)
			}
			httpAssertBufferedBody(t, got)
		})
	}
}

func TestHTTPNoBodyAndActuallyEmptyBodiesAreForwarded(t *testing.T) {
	tests := []struct {
		name, method, contentType, encoding string
		chunked                             bool
	}{
		{name: "GET without body", method: http.MethodGet},
		{name: "POST without body", method: http.MethodPost},
		{name: "zero known length JSON", method: http.MethodPost, contentType: "application/json"},
		{name: "zero unknown length without type", method: http.MethodPost, chunked: true},
		{name: "zero unknown length JSON", method: http.MethodPatch, contentType: "application/json", chunked: true},
		{name: "empty body bypasses type and encoding", method: http.MethodPost, contentType: "text/plain", encoding: "br", chunked: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := httpNewHarness(t, nil, nil)
			req := httpNewRequest(t, h, tt.method, "/api/models?x=1&x=2", nil)
			if tt.chunked {
				httpSetChunkedBody(req, []byte{})
			}
			if tt.contentType != "" {
				req.Header.Set("Content-Type", tt.contentType)
			}
			if tt.encoding != "" {
				req.Header.Set("Content-Encoding", tt.encoding)
			}
			res, body := httpDo(t, h, req)
			httpAssertOK(t, res, body)
			got := httpNextRequest(t, h)
			if got.Method != tt.method || len(got.Body) != 0 || got.RawQuery != "x=1&x=2" {
				t.Errorf("empty request changed: method=%s body=%q query=%q", got.Method, got.Body, got.RawQuery)
			}
			httpAssertBufferedBody(t, got)
		})
	}
}

func httpDigestHeaders() http.Header {
	return http.Header{
		"Content-Md5":    {"synthetic-md5-value"},
		"Digest":         {"sha-256=synthetic-digest-value"},
		"Content-Digest": {"sha-256=:synthetic-content-digest:"},
		"Repr-Digest":    {"sha-256=:synthetic-representation-digest:"},
	}
}

func TestHTTPBufferedBodyUpdatesFramingAndDigests(t *testing.T) {
	for _, chunked := range []bool{false, true} {
		for _, modified := range []bool{false, true} {
			t.Run("chunked="+strconv.FormatBool(chunked)+"/modified="+strconv.FormatBool(modified), func(t *testing.T) {
				h := httpNewHarness(t, nil, nil)
				original := []byte(` {"input":[{"keep":9007199254740993}],"decimal":0.123456789012345678901} `)
				want := original
				if modified {
					original = []byte(` {"input":[{"internal_chat_message_metadata_passthrough":null,"keep":9007199254740993}],"decimal":0.123456789012345678901} `)
				}
				req := httpNewRequest(t, h, http.MethodPost, "/custom/prefix/responses", original)
				if chunked {
					httpSetChunkedBody(req, original)
				}
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Expect", "100-continue")
				for key, values := range httpDigestHeaders() {
					req.Header[key] = append([]string(nil), values...)
				}
				res, body := httpDo(t, h, req)
				httpAssertOK(t, res, body)
				got := httpNextRequest(t, h)
				httpAssertJSONEqual(t, got.Body, want)
				if !modified && !bytes.Equal(got.Body, original) {
					t.Error("buffering changed the bytes of an unmodified request")
				}
				if modified && len(got.Body) >= len(original) {
					t.Errorf("removing metadata did not shorten this fixture: %d -> %d", len(original), len(got.Body))
				}
				httpAssertBufferedBody(t, got)
				for key, values := range httpDigestHeaders() {
					if modified {
						if len(got.Header.Values(key)) != 0 {
							t.Errorf("modified body kept stale %s: %v", key, got.Header.Values(key))
						}
					} else if !reflect.DeepEqual(got.Header.Values(key), values) {
						t.Errorf("unchanged body lost %s: %v, want %v", key, got.Header.Values(key), values)
					}
				}
				if got.Header.Get("Idempotency-Key") != "" || got.Header.Get("X-Idempotency-Key") != "" {
					t.Error("relay added an idempotency header")
				}
			})
		}
	}
}

func TestHTTPRemovesForwardingAndHopByHopRequestHeaders(t *testing.T) {
	h := httpNewHarness(t, nil, nil)
	req := httpNewRequest(t, h, http.MethodPost, "/v1/responses", []byte(`{"input":[]}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer synthetic")
	req.Header.Set("X-End-To-End", "preserved")
	for _, key := range []string{
		"Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto", "X-Forwarded-Port",
		"X-Forwarded-Server", "X-Forwarded-Prefix", "X-Forwarded-Client-Cert", "X-Forwarded-Custom",
	} {
		req.Header.Add(key, "spoofed-first")
		req.Header.Add(key, "spoofed-second")
	}
	req.Header["x-fOrWaRdEd-OdD"] = []string{"spoofed-case"}
	for key, value := range map[string]string{
		"Connection": "keep-alive, X-Hop-Secret, x-hop-other", "X-Hop-Secret": "remove-me",
		"X-Hop-Other": "remove-me-too", "Proxy-Connection": "keep-alive", "Keep-Alive": "timeout=5",
		"Proxy-Authenticate": `Basic realm="synthetic"`, "Proxy-Authorization": "Basic synthetic",
		"Te": "deflate",
	} {
		req.Header.Set(key, value)
	}
	res, body := httpDo(t, h, req)
	httpAssertOK(t, res, body)
	got := httpNextRequest(t, h)
	for key := range got.Header {
		lower := strings.ToLower(key)
		if lower == "forwarded" || strings.HasPrefix(lower, "x-forwarded-") {
			t.Errorf("forwarding header reached upstream: %s=%v", key, got.Header[key])
		}
	}
	for _, key := range []string{
		"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
		"Te", "Transfer-Encoding", "X-Hop-Secret", "X-Hop-Other",
	} {
		if len(got.Header.Values(key)) != 0 {
			t.Errorf("hop-by-hop header reached upstream: %s=%v", key, got.Header.Values(key))
		}
	}
	if got.Header.Get("Authorization") != "Bearer synthetic" || got.Header.Get("X-End-To-End") != "preserved" {
		t.Error("removing forwarding headers changed end-to-end headers")
	}
}

func httpGzipWithComment(t *testing.T, body []byte, comment string) []byte {
	t.Helper()
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	writer.Name = "wire-request.json"
	writer.Comment = comment
	writer.ModTime = time.Unix(1700000000, 0)
	if _, err := writer.Write(body); err != nil {
		t.Fatalf("gzip fixture write: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("gzip fixture close: %v", err)
	}
	return compressed.Bytes()
}

func httpGzip(t *testing.T, body []byte) []byte {
	t.Helper()
	return httpGzipWithComment(t, body, "preserve exact gzip bytes")
}

func TestHTTPGzipRequestsPreserveOrRemoveEncodingAndDigests(t *testing.T) {
	for _, chunked := range []bool{false, true} {
		for _, modified := range []bool{false, true} {
			t.Run("chunked="+strconv.FormatBool(chunked)+"/modified="+strconv.FormatBool(modified), func(t *testing.T) {
				h := httpNewHarness(t, nil, nil)
				plain := []byte(` {"input":[{"keep":9007199254740993}],"decimal":0.123456789012345678901} `)
				want := plain
				if modified {
					plain = []byte(` {"input":[{"internal_chat_message_metadata_passthrough":{"discard":true},"keep":9007199254740993}],"decimal":0.123456789012345678901} `)
				}
				compressed := httpGzip(t, plain)
				req := httpNewRequest(t, h, http.MethodPost, "/api/responses", compressed)
				if chunked {
					httpSetChunkedBody(req, compressed)
				}
				req.Header.Set("Content-Type", "application/json; charset=utf-8")
				req.Header.Set("Content-Encoding", "gzip")
				req.Header.Set("Expect", "100-continue")
				for key, values := range httpDigestHeaders() {
					req.Header[key] = append([]string(nil), values...)
				}
				res, body := httpDo(t, h, req)
				httpAssertOK(t, res, body)
				got := httpNextRequest(t, h)
				httpAssertBufferedBody(t, got)
				if modified {
					httpAssertJSONEqual(t, got.Body, want)
					if len(got.Header.Values("Content-Encoding")) != 0 {
						t.Errorf("cleaned gzip still has Content-Encoding: %v", got.Header.Values("Content-Encoding"))
					}
				} else {
					if !bytes.Equal(got.Body, compressed) {
						t.Error("unchanged gzip request did not preserve the original compressed bytes")
					}
					if got.Header.Get("Content-Encoding") != "gzip" {
						t.Errorf("unchanged gzip encoding = %q, want gzip", got.Header.Get("Content-Encoding"))
					}
				}
				for key, values := range httpDigestHeaders() {
					if modified {
						if len(got.Header.Values(key)) != 0 {
							t.Errorf("cleaned gzip retained %s", key)
						}
					} else if !reflect.DeepEqual(got.Header.Values(key), values) {
						t.Errorf("unchanged gzip lost %s: %v, want %v", key, got.Header.Values(key), values)
					}
				}
			})
		}
	}
}

func TestHTTPRejectsInvalidGzipBeforeUpstream(t *testing.T) {
	valid := httpGzip(t, []byte(`{"input":[]}`))
	badChecksum := bytes.Clone(valid)
	badChecksum[len(badChecksum)-8] ^= 0x80
	badSize := bytes.Clone(valid)
	badSize[len(badSize)-1] ^= 0x01
	tests := []struct {
		name string
		body []byte
		code string
	}{
		{"plain JSON labeled gzip", []byte(`{"input":[]}`), "invalid_gzip"},
		{"truncated header", valid[:5], "invalid_gzip"},
		{"missing trailer", valid[:len(valid)-8], "invalid_gzip"},
		{"truncated trailer", valid[:len(valid)-1], "invalid_gzip"},
		{"bad checksum", badChecksum, "invalid_gzip"},
		{"bad uncompressed size", badSize, "invalid_gzip"},
		{"trailing garbage", append(bytes.Clone(valid), []byte("not-a-gzip-member")...), "invalid_gzip"},
		{"single trailing garbage byte", append(bytes.Clone(valid), 0), "invalid_gzip"},
		{"trailing truncated member", append(bytes.Clone(valid), valid[:5]...), "invalid_gzip"},
		{"gzip of empty JSON", httpGzip(t, nil), "invalid_json"},
		{"gzip of invalid JSON", httpGzip(t, []byte(`{"input":`)), "invalid_json"},
		{"gzip of duplicate JSON", httpGzip(t, []byte(`{"input":[{"keep":1,"keep":2}]}`)), "ambiguous_json"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := httpNewHarness(t, nil, nil)
			defer httpAssertNoUpstream(t, h)
			req := httpNewRequest(t, h, http.MethodPost, "/v1/responses", tt.body)
			httpSetChunkedBody(req, tt.body)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Content-Encoding", "gzip")
			res, body := httpDo(t, h, req)
			httpAssertLocalError(t, h, res, body, http.StatusBadRequest, tt.code)
		})
	}
}

func TestHTTPAcceptsJSONMediaTypesAndIdentity(t *testing.T) {
	tests := []struct{ name, method, mediaType, encoding string }{
		{"JSON", http.MethodPost, "application/json", ""},
		{"charset parameter", http.MethodPatch, "application/json; charset=utf-8", "identity"},
		{"case insensitive type", http.MethodDelete, "APPLICATION/JSON; Charset=UTF-8", "IDENTITY"},
		{"vendor JSON and parameters", http.MethodPost, `application/vnd.example.responses+json; charset="utf-8"; profile="https://example.test/profile"`, ""},
		{"problem JSON", http.MethodGet, "application/problem+json", "identity"},
		{"custom JSON subtype", http.MethodPut, "application/x.example+json; version=1", ""},
		{"gzip vendor JSON", http.MethodPost, "application/vnd.example+json; charset=utf-8", "GZIP"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := httpNewHarness(t, nil, nil)
			original := []byte(`{"input":[{"internal_chat_message_metadata_passthrough":null,"keep":true}]}`)
			wire := original
			if strings.EqualFold(tt.encoding, "gzip") {
				wire = httpGzip(t, original)
			}
			req := httpNewRequest(t, h, tt.method, "/arbitrary/nested/api/action", wire)
			req.Header.Set("Content-Type", tt.mediaType)
			if tt.encoding != "" {
				req.Header.Set("Content-Encoding", tt.encoding)
			}
			res, body := httpDo(t, h, req)
			httpAssertOK(t, res, body)
			got := httpNextRequest(t, h)
			httpAssertJSONEqual(t, got.Body, []byte(`{"input":[{"keep":true}]}`))
			if got.Method != tt.method || got.Header.Get("Content-Type") != tt.mediaType {
				t.Errorf("method or media type changed: %s / %q", got.Method, got.Header.Get("Content-Type"))
			}
		})
	}
}

func TestHTTPRejectsUnsupportedMediaTypeBeforeUpstream(t *testing.T) {
	for _, mediaType := range []string{
		"", "text/plain", "text/json", "text/vnd.example+json", "application/octet-stream",
		"application/x-www-form-urlencoded", "multipart/form-data; boundary=synthetic",
		"application/jsonp", "application/json; broken", "application/json; charset=utf-8; charset=ascii",
		"application/json, application/json", "not-a-media-type",
	} {
		name := mediaType
		if name == "" {
			name = "missing Content-Type"
		}
		t.Run(name, func(t *testing.T) {
			h := httpNewHarness(t, nil, nil)
			defer httpAssertNoUpstream(t, h)
			req := httpNewRequest(t, h, http.MethodPost, "/v1/responses", []byte(`{"input":[]}`))
			req.Header.Set("Content-Type", mediaType)
			res, body := httpDo(t, h, req)
			httpAssertLocalError(t, h, res, body, http.StatusUnsupportedMediaType, "unsupported_media_type")
		})
	}
}

func TestHTTPRejectsUnknownAndCombinedContentEncodings(t *testing.T) {
	tests := []struct {
		name   string
		values []string
	}{
		{"brotli", []string{"br"}},
		{"deflate", []string{"deflate"}},
		{"gzip then brotli", []string{"gzip, br"}},
		{"brotli then gzip", []string{"br, gzip"}},
		{"double gzip", []string{"gzip, gzip"}},
		{"identity plus gzip", []string{"identity, gzip"}},
		{"gzip plus identity", []string{"gzip, identity"}},
		{"encoding parameters", []string{"gzip; q=1"}},
		{"repeated gzip header fields", []string{"gzip", "gzip"}},
		{"repeated mixed header fields", []string{"identity", "br"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := httpNewHarness(t, nil, nil)
			defer httpAssertNoUpstream(t, h)
			compressed := httpGzip(t, []byte(`{"input":[]}`))
			req := httpNewRequest(t, h, http.MethodPost, "/v1/responses", compressed)
			req.Header.Set("Content-Type", "application/json")
			for _, value := range tt.values {
				req.Header.Add("Content-Encoding", value)
			}
			res, body := httpDo(t, h, req)
			httpAssertLocalError(t, h, res, body, http.StatusUnsupportedMediaType, "unsupported_content_encoding")
		})
	}
}

func TestHTTPRejectsInvalidAndAmbiguousJSONBeforeUpstream(t *testing.T) {
	tests := []struct{ name, body, code string }{
		{"whitespace is nonempty", " \r\n\t ", "invalid_json"},
		{"truncated", `{"input":`, "invalid_json"},
		{"invalid later item after removal", `{"input":[{"internal_chat_message_metadata_passthrough":1},{"keep":}]}`, "invalid_json"},
		{"invalid nested value without input", `{"extension":{"n":[1,]}}`, "invalid_json"},
		{"invalid root array", `[1,]`, "invalid_json"},
		{"second JSON value", `{"input":[]} {}`, "invalid_json"},
		{"trailing garbage", `{"input":[]} garbage`, "invalid_json"},
		{"invalid UTF-8", "{\"input\":[{\"text\":\"\xff\"}]}", "invalid_json"},
		{"BOM", "\xef\xbb\xbf{}", "invalid_json"},
		{"duplicate input", `{"input":[],"input":[]}`, "ambiguous_json"},
		{"escaped duplicate root key", `{"input":[],"\u0069nput":[]}`, "ambiguous_json"},
		{"unrelated duplicate root key", `{"model":"a","model":"b"}`, "ambiguous_json"},
		{"duplicate target key", `{"input":[{"internal_chat_message_metadata_passthrough":1,"internal_chat_message_metadata_passthrough":2}]}`, "ambiguous_json"},
		{"escaped duplicate item key", `{"input":[{"keep":1,"\u006beep":2}]}`, "ambiguous_json"},
		{"duplicate later item after removal", `{"input":[{"internal_chat_message_metadata_passthrough":1},{"keep":1,"keep":2}]}`, "ambiguous_json"},
	}
	for _, chunked := range []bool{false, true} {
		for _, tt := range tests {
			t.Run("chunked="+strconv.FormatBool(chunked)+"/"+tt.name, func(t *testing.T) {
				h := httpNewHarness(t, nil, nil)
				defer httpAssertNoUpstream(t, h)
				req := httpNewRequest(t, h, http.MethodPost, "/custom/v1/action", []byte(tt.body))
				if chunked {
					httpSetChunkedBody(req, []byte(tt.body))
				}
				req.Header.Set("Content-Type", "application/json")
				res, body := httpDo(t, h, req)
				httpAssertLocalError(t, h, res, body, http.StatusBadRequest, tt.code)
			})
		}
	}
}

func httpJSONOfSize(t *testing.T, size int) []byte {
	t.Helper()
	if size < 2 {
		t.Fatalf("JSON string fixture needs at least 2 bytes, got %d", size)
	}
	return []byte(`"` + strings.Repeat("x", size-2) + `"`)
}

func httpGzipOfWireSize(t *testing.T, size int) []byte {
	t.Helper()
	plain := []byte(`{}`)
	base := httpGzipWithComment(t, plain, "")
	padding := size - len(base) - 1 // A nonempty gzip comment adds its terminating NUL.
	if padding < 1 {
		t.Fatalf("gzip wire fixture needs more than %d bytes, got %d", len(base)+1, size)
	}
	body := httpGzipWithComment(t, plain, strings.Repeat("p", padding))
	if len(body) != size {
		t.Fatalf("gzip fixture wire length = %d, want %d", len(body), size)
	}
	return body
}

func TestHTTPRawAndDecompressedBodySizeBoundaries(t *testing.T) {
	const limit = 512
	for _, kind := range []string{"raw JSON", "raw gzip", "decompressed gzip"} {
		for _, chunked := range []bool{false, true} {
			for _, size := range []int{limit, limit + 1} {
				t.Run(kind+"/chunked="+strconv.FormatBool(chunked)+"/size="+strconv.Itoa(size), func(t *testing.T) {
					h := httpNewHarness(t, map[string]any{"max_body_bytes": limit}, nil)
					wire := httpJSONOfSize(t, size)
					encoding := ""
					switch kind {
					case "raw gzip":
						wire = httpGzipOfWireSize(t, size)
						encoding = "gzip"
					case "decompressed gzip":
						wire = httpGzip(t, wire)
						encoding = "gzip"
						if len(wire) >= limit {
							t.Fatalf("decompression fixture wire length %d must be below limit %d", len(wire), limit)
						}
					}
					req := httpNewRequest(t, h, http.MethodPost, "/v1/responses", wire)
					if chunked {
						httpSetChunkedBody(req, wire)
					}
					req.Header.Set("Content-Type", "application/json")
					if encoding != "" {
						req.Header.Set("Content-Encoding", encoding)
					}
					if size > limit {
						defer httpAssertNoUpstream(t, h)
						res, body := httpDo(t, h, req)
						httpAssertLocalError(t, h, res, body, http.StatusRequestEntityTooLarge, "body_too_large")
						return
					}
					res, body := httpDo(t, h, req)
					httpAssertOK(t, res, body)
					got := httpNextRequest(t, h)
					if !bytes.Equal(got.Body, wire) {
						t.Error("body exactly at its configured limit was changed")
					}
					httpAssertBufferedBody(t, got)
				})
			}
		}
	}
}

func TestHTTPEmptyDropRulesStillValidateJSONAndLimits(t *testing.T) {
	t.Run("valid body remains byte identical", func(t *testing.T) {
		h := httpNewHarness(t, map[string]any{"drop_input_fields": []string{}}, nil)
		original := []byte(` {"input":[{"internal_chat_message_metadata_passthrough":null,"n":9007199254740993}]} `)
		req := httpNewRequest(t, h, http.MethodPost, "/v1/responses", original)
		req.Header.Set("Content-Type", "application/json")
		res, body := httpDo(t, h, req)
		httpAssertOK(t, res, body)
		if got := httpNextRequest(t, h); !bytes.Equal(got.Body, original) {
			t.Error("explicit empty drop rules still removed metadata or changed bytes")
		}
	})
	const limit = 128
	tests := []struct {
		name, encoding, mediaType, code string
		body                            []byte
		status                          int
	}{
		{name: "invalid JSON", body: []byte(`{"input":`), status: http.StatusBadRequest, code: "invalid_json"},
		{name: "duplicate root", body: []byte(`{"x":1,"x":2}`), status: http.StatusBadRequest, code: "ambiguous_json"},
		{name: "duplicate input object", body: []byte(`{"input":[{"x":1,"x":2}]}`), status: http.StatusBadRequest, code: "ambiguous_json"},
		{name: "invalid gzip", body: []byte("bad-gzip"), encoding: "gzip", status: http.StatusBadRequest, code: "invalid_gzip"},
		{name: "unsupported type", body: []byte(`{}`), mediaType: "text/plain", status: http.StatusUnsupportedMediaType, code: "unsupported_media_type"},
		{name: "unsupported encoding", body: []byte(`{}`), encoding: "br", status: http.StatusUnsupportedMediaType, code: "unsupported_content_encoding"},
		{name: "raw limit still applies", body: httpJSONOfSize(t, limit+1), status: http.StatusRequestEntityTooLarge, code: "body_too_large"},
		{name: "decompressed limit still applies", body: httpGzip(t, httpJSONOfSize(t, limit+1)), encoding: "gzip", status: http.StatusRequestEntityTooLarge, code: "body_too_large"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := httpNewHarness(t, map[string]any{"drop_input_fields": []string{}, "max_body_bytes": limit}, nil)
			defer httpAssertNoUpstream(t, h)
			req := httpNewRequest(t, h, http.MethodPost, "/v1/responses", tt.body)
			httpSetChunkedBody(req, tt.body)
			mediaType := tt.mediaType
			if mediaType == "" {
				mediaType = "application/json"
			}
			req.Header.Set("Content-Type", mediaType)
			if tt.encoding != "" {
				req.Header.Set("Content-Encoding", tt.encoding)
			}
			res, body := httpDo(t, h, req)
			httpAssertLocalError(t, h, res, body, tt.status, tt.code)
		})
	}
}

func TestHTTPRejectsCONNECTAndUpgradeBeforeUpstream(t *testing.T) {
	tests := []struct {
		name, method        string
		connection, upgrade string
	}{
		{name: "CONNECT", method: http.MethodConnect},
		{name: "WebSocket", method: http.MethodGet, connection: "Upgrade", upgrade: "websocket"},
		{name: "upgrade protocol is case insensitive", method: http.MethodGet, connection: "keep-alive, uPgRaDe", upgrade: "WebSocket"},
		{name: "bare Upgrade header", method: http.MethodGet, upgrade: "websocket"},
		{name: "bare Connection upgrade token", method: http.MethodGet, connection: "upgrade"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := httpNewHarness(t, nil, nil)
			defer httpAssertNoUpstream(t, h)
			req := httpNewRequest(t, h, tt.method, "/v1/responses", nil)
			if tt.connection != "" {
				req.Header.Set("Connection", tt.connection)
			}
			if tt.upgrade != "" {
				req.Header.Set("Upgrade", tt.upgrade)
			}
			res, body := httpDo(t, h, req)
			httpAssertLocalError(t, h, res, body, http.StatusNotImplemented, "unsupported_protocol")
		})
	}
}

// Synthetic server requests cover Trailer declarations that net/http normally
// moves out of Header. The exported Handler is still exercised as a black box.
func httpServeSynthetic(t *testing.T, h *httpHarness, req *http.Request) (*http.Response, []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(req.Context(), httpTestTimeout)
	defer cancel()
	recorder := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		h.handler.ServeHTTP(recorder, req.WithContext(ctx))
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("Handler did not complete the synthetic request within the test deadline")
	}
	res := recorder.Result()
	body, readErr := io.ReadAll(res.Body)
	closeErr := res.Body.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("read/close synthetic response: %v / %v", readErr, closeErr)
	}
	return res, body
}

func TestHTTPRejectsDeclaredRequestTrailersBeforeUpstream(t *testing.T) {
	t.Run("wire chunked trailer declaration", func(t *testing.T) {
		h := httpNewHarness(t, nil, nil)
		defer httpAssertNoUpstream(t, h)
		original := []byte(`{"input":[]}`)
		req := httpNewRequest(t, h, http.MethodPost, "/v1/responses", original)
		httpSetChunkedBody(req, original)
		req.Header.Set("Content-Type", "application/json")
		req.Trailer = http.Header{"X-Request-Final": {"synthetic-trailer"}}
		res, body := httpDo(t, h, req)
		httpAssertLocalError(t, h, res, body, http.StatusNotImplemented, "unsupported_trailers")
	})
	tests := []struct {
		name         string
		trailer      http.Header
		declarations []string
		emptyBody    bool
	}{
		{name: "nonempty Trailer map with nil values", trailer: http.Header{"X-Request-Final": nil}},
		{name: "nonempty Trailer map without body", trailer: http.Header{"X-Request-Final": nil}, emptyBody: true},
		{name: "raw declaration header", declarations: []string{"X-Request-Final"}},
		{name: "raw declaration header without body", declarations: []string{"X-Request-Final"}, emptyBody: true},
		{name: "repeated raw declarations", declarations: []string{"", "X-Request-Final"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := httpNewHarness(t, nil, nil)
			defer httpAssertNoUpstream(t, h)
			var reader io.Reader
			if !tt.emptyBody {
				reader = strings.NewReader(`{"input":[]}`)
			}
			req := httptest.NewRequest(http.MethodPost, "http://client.example/v1/responses", reader)
			req.Header.Set("Content-Type", "application/json")
			req.Trailer = tt.trailer
			if tt.declarations != nil {
				req.Header["Trailer"] = tt.declarations
			}
			res, body := httpServeSynthetic(t, h, req)
			httpAssertLocalError(t, h, res, body, http.StatusNotImplemented, "unsupported_trailers")
		})
	}
}

func TestHTTPUpstreamStatusesBodiesAndResponseHeadersArePreserved(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusFound} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			wantBody := []byte(`{"upstream_error":` + strconv.Itoa(status) + `,"detail":"synthetic"}` + "\n")
			var h *httpHarness
			h = httpNewHarness(t, nil, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/problem+json; charset=utf-8")
				w.Header().Set("Content-Length", strconv.Itoa(len(wantBody)))
				w.Header().Set("X-Upstream-Request-ID", "synthetic-upstream-id")
				w.Header().Set("Cache-Control", "no-store")
				w.Header().Add("Set-Cookie", "first=one; HttpOnly")
				w.Header().Add("Set-Cookie", "second=two; SameSite=Strict")
				w.Header().Set("Connection", "keep-alive, X-Upstream-Hop")
				w.Header().Set("Keep-Alive", "timeout=5")
				w.Header().Set("X-Upstream-Hop", "remove-this")
				switch status {
				case http.StatusUnauthorized:
					w.Header().Set("WWW-Authenticate", `Bearer realm="synthetic-upstream"`)
				case http.StatusTooManyRequests:
					w.Header().Set("Retry-After", "17")
				case http.StatusFound:
					w.Header().Set("Location", h.upstream.URL+"/must-not-follow?x=1&x=2")
				}
				w.WriteHeader(status)
				if _, err := w.Write(wantBody); err != nil {
					t.Errorf("write upstream error response: %v", err)
				}
			})
			req := httpNewRequest(t, h, http.MethodPost, "/v1/responses", []byte(`{"input":[{"internal_chat_message_metadata_passthrough":null}]}`))
			req.Header.Set("Content-Type", "application/json")
			res, body := httpDo(t, h, req)
			if res.StatusCode != status || !bytes.Equal(body, wantBody) {
				t.Errorf("upstream response changed: status=%d body=%q, want %d %q", res.StatusCode, body, status, wantBody)
			}
			got := httpNextRequest(t, h)
			if got.Path != "/v1/responses" {
				t.Errorf("proxy followed upstream redirect to %q", got.Path)
			}
			for key, value := range map[string]string{
				"Content-Type":   "application/problem+json; charset=utf-8",
				"Content-Length": strconv.Itoa(len(wantBody)), "X-Upstream-Request-ID": "synthetic-upstream-id",
				"Cache-Control": "no-store",
			} {
				if res.Header.Get(key) != value {
					t.Errorf("response %s = %q, want %q", key, res.Header.Get(key), value)
				}
			}
			if !reflect.DeepEqual(res.Header.Values("Set-Cookie"), []string{"first=one; HttpOnly", "second=two; SameSite=Strict"}) {
				t.Errorf("Set-Cookie response values changed: %v", res.Header.Values("Set-Cookie"))
			}
			for _, key := range []string{"Connection", "Keep-Alive", "X-Upstream-Hop"} {
				if len(res.Header.Values(key)) != 0 {
					t.Errorf("hop-by-hop response header reached client: %s=%v", key, res.Header.Values(key))
				}
			}
			switch status {
			case http.StatusUnauthorized:
				if res.Header.Get("WWW-Authenticate") != `Bearer realm="synthetic-upstream"` {
					t.Error("WWW-Authenticate was not preserved")
				}
			case http.StatusTooManyRequests:
				if res.Header.Get("Retry-After") != "17" {
					t.Error("Retry-After was not preserved")
				}
			case http.StatusFound:
				if res.Header.Get("Location") != h.upstream.URL+"/must-not-follow?x=1&x=2" {
					t.Error("Location was not preserved")
				}
			}
		})
	}
}

func TestHTTPGzipResponseBytesAndAcceptEncodingArePreserved(t *testing.T) {
	compressed := httpGzip(t, []byte(`{"upstream":"compressed response","n":9007199254740993}`))
	for _, acceptEncoding := range []string{"", "gzip, br"} {
		t.Run("Accept-Encoding="+acceptEncoding, func(t *testing.T) {
			h := httpNewHarness(t, nil, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Content-Encoding", "gzip")
				w.Header().Set("Content-Length", strconv.Itoa(len(compressed)))
				for key, values := range httpDigestHeaders() {
					w.Header()[key] = append([]string(nil), values...)
				}
				if _, err := w.Write(compressed); err != nil {
					t.Errorf("write upstream gzip response: %v", err)
				}
			})
			req := httpNewRequest(t, h, http.MethodGet, "/v1/models", nil)
			if acceptEncoding != "" {
				req.Header.Set("Accept-Encoding", acceptEncoding)
			}
			res, body := httpDo(t, h, req)
			if res.StatusCode != http.StatusOK || !bytes.Equal(body, compressed) {
				t.Errorf("gzip upstream response changed: status=%d bytes=%d, want original %d bytes", res.StatusCode, len(body), len(compressed))
			}
			if res.Header.Get("Content-Encoding") != "gzip" || res.ContentLength != int64(len(compressed)) || res.Uncompressed {
				t.Errorf("gzip response encoding/length changed: encoding=%q length=%d uncompressed=%t", res.Header.Get("Content-Encoding"), res.ContentLength, res.Uncompressed)
			}
			for key, values := range httpDigestHeaders() {
				if !reflect.DeepEqual(res.Header.Values(key), values) {
					t.Errorf("gzip response digest %s changed: %v, want %v", key, res.Header.Values(key), values)
				}
			}
			got := httpNextRequest(t, h)
			if got.Header.Get("Accept-Encoding") != acceptEncoding {
				t.Errorf("relay Transport changed Accept-Encoding: got %q, want %q", got.Header.Get("Accept-Encoding"), acceptEncoding)
			}
			if acceptEncoding == "" && len(got.Header.Values("Accept-Encoding")) != 0 {
				t.Error("relay Transport automatically added Accept-Encoding")
			}
		})
	}
}
