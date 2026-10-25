package acceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"time"
)

const (
	maxResponseBytes = 4 << 20
	maxStreamBytes   = 32 << 20
)

var (
	errRequest       = errors.New("The synthetic HTTP request could not be completed.")
	errTimeout       = errors.New("The check exceeded its time limit.")
	errCanceled      = errors.New("The check was canceled.")
	errHTTPStatus    = errors.New("The endpoint returned an unexpected HTTP status.")
	errResponseRead  = errors.New("The response body could not be read completely.")
	errResponseLimit = errors.New("The response exceeded the 4 MiB size limit.")
	errResponseJSON  = errors.New("The endpoint did not return a valid JSON response.")
	errHealth        = errors.New("The health response did not report status ok.")
	errStreamType    = errors.New("The endpoint did not return a text/event-stream response.")
	errConcurrency   = errors.New("Local relay concurrency remained unavailable after the bounded retry window.")
)

type engine struct {
	options   Options
	client    *http.Client
	base      *url.URL
	direct    *url.URL
	toolInput []json.RawMessage
}

// Run executes ordered, synthetic probes. Invalid options return an error before
// any network activity. HTTP, response and acceptance failures are safe Checks
// with a nil error; every later configured probe is skipped after a failure.
// Only explicitly identified local concurrency rejections in after_cancel may
// be retried, for at most two seconds. That check proves fresh service availability
// after local cancellation; upstream cancellation and slot release stay manual.
func Run(ctx context.Context, options Options) (Report, error) {
	if err := ValidateOptions(options); err != nil {
		return Report{}, err
	}
	if ctx == nil {
		return Report{}, errors.New("acceptance: context is required")
	}
	base, _ := parseBaseURL(options.BaseURL, true)
	var direct *url.URL
	if options.DirectBaseURL != "" {
		direct, _ = parseBaseURL(options.DirectBaseURL, false)
	}
	transport := &http.Transport{
		Proxy:                  nil,
		DialContext:            (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:      true,
		MaxIdleConns:           4,
		MaxIdleConnsPerHost:    2,
		IdleConnTimeout:        30 * time.Second,
		TLSHandshakeTimeout:    10 * time.Second,
		ResponseHeaderTimeout:  options.Timeout,
		MaxResponseHeaderBytes: 64 << 10,
		DisableCompression:     true,
	}
	defer transport.CloseIdleConnections()
	e := &engine{
		options: options,
		base:    base,
		direct:  direct,
		client: &http.Client{
			Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
	report := Report{
		SchemaVersion:   1,
		StartedAt:       time.Now().UTC(),
		AutomatedPassed: true,
		Checks:          make([]Check, 0, 8),
		ManualChecks:    manualChecks(),
	}
	steps := []struct {
		name    string
		detail  string
		enabled bool
		run     func(context.Context) (int, error)
	}{
		{"health", "The local process returned a healthy JSON response; upstream reachability is checked separately.", true, e.health},
		{"conversation", "A synthetic conversation returned completed status and nonempty output text.", true, e.conversation},
		{"sse", "The synthetic SSE response contained nonempty text deltas and a completed response.", true, e.sse},
		{"tool_call", "The forced synthetic echo call matched its fixed name and constant argument.", true, e.toolCall},
		{"tool_result", "The full raw output and original user input were continued with a static tool result and completed text.", true, e.toolResult},
		{"cancel", "The local stream was canceled and closed immediately after its first nonempty text delta; upstream effects remain manual.", true, e.cancelStream},
		{"after_cancel", "A fresh synthetic conversation completed after local cancellation; upstream cancellation and slot release remain manual.", true, e.afterCancel},
		{"direct_conversation", "An explicitly configured direct synthetic conversation completed; actual client rollback remains manual.", direct != nil, e.directConversation},
	}
	failed := false
	for _, step := range steps {
		check := Check{Name: step.name, Status: "skipped"}
		if !step.enabled {
			check.Detail = "No direct endpoint was configured."
		} else if failed {
			check.Detail = "Skipped because an earlier automated check failed."
		} else {
			started := time.Now()
			stepCtx, cancel := context.WithTimeout(ctx, options.Timeout)
			status, err := step.run(stepCtx)
			cancel()
			check.HTTPStatus = status
			check.DurationMS = time.Since(started).Milliseconds()
			check.Status, check.Detail = "passed", step.detail
			if err != nil {
				// All probe helpers return fixed internal errors, never service errors.
				check.Status, check.Detail = "failed", err.Error()
				failed, report.AutomatedPassed = true, false
			}
		}
		report.Checks = append(report.Checks, check)
	}
	return report, nil
}

func manualChecks() []Check {
	return []Check{
		{Name: "client_conversation", Status: "pending", Detail: "Verify a conversation in the actual client using the intended relay configuration."},
		{Name: "client_tool", Status: "pending", Detail: "Verify tool invocation and result continuation in the actual client."},
		{Name: "client_cancel", Status: "pending", Detail: "Verify stream cancellation in the actual client."},
		{Name: "cleaning_counts", Status: "pending", Detail: "Review real relay logs for direct input metadata deletion counts without logging bodies or credentials."},
		{Name: "upstream_cancellation", Status: "pending", Detail: "Confirm actual upstream cancellation and release of its concurrency slot using upstream evidence."},
		{Name: "client_rollback", Status: "pending", Detail: "Restore and verify the actual client's prior configuration; a direct synthetic probe is insufficient."},
	}
}

func (e *engine) send(ctx context.Context, target *url.URL, method string, body []byte, stream bool) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, target.String(), bytes.NewReader(body))
	if err != nil {
		return nil, errRequest
	}
	// Buffered model requests must not become replayable on connection failure.
	req.GetBody = nil
	if method == http.MethodPost {
		req.Header.Set("Authorization", "Bearer "+e.options.APIKey)
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	if stream {
		req.Header.Set("Accept", "text/event-stream")
	}
	response, err := e.client.Do(req)
	if err != nil {
		return nil, safeRequestError(ctx, err)
	}
	return response, nil
}

func safeRequestError(ctx context.Context, err error) error {
	var networkError net.Error
	switch {
	case errors.Is(ctx.Err(), context.DeadlineExceeded), errors.Is(err, context.DeadlineExceeded), errors.As(err, &networkError) && networkError.Timeout():
		return errTimeout
	case errors.Is(ctx.Err(), context.Canceled), errors.Is(err, context.Canceled):
		return errCanceled
	default:
		return errRequest
	}
}

func readResponse(ctx context.Context, response *http.Response) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if len(body) > maxResponseBytes {
		return nil, errResponseLimit
	}
	if err != nil {
		if ctx.Err() != nil {
			return nil, safeRequestError(ctx, err)
		}
		return nil, errResponseRead
	}
	if ctx.Err() != nil {
		return nil, safeRequestError(ctx, ctx.Err())
	}
	return body, nil
}

func (e *engine) health(ctx context.Context) (int, error) {
	response, err := e.send(ctx, healthURL(e.base), http.MethodGet, nil, false)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return response.StatusCode, errHTTPStatus
	}
	body, err := readResponse(ctx, response)
	if err != nil {
		return response.StatusCode, err
	}
	health, err := decodeObject(body)
	if err != nil {
		return response.StatusCode, errResponseJSON
	}
	var healthStatus string
	if json.Unmarshal(health["status"], &healthStatus) != nil {
		return response.StatusCode, errResponseJSON
	}
	if healthStatus != "ok" {
		return response.StatusCode, errHealth
	}
	return response.StatusCode, nil
}

func (e *engine) post(ctx context.Context, base *url.URL, request probeRequest) ([]byte, int, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return nil, 0, errRequest
	}
	response, err := e.send(ctx, responsesURL(base), http.MethodPost, body, request.Stream)
	if err != nil {
		return nil, 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusServiceUnavailable {
		return nil, response.StatusCode, errHTTPStatus
	}
	data, err := readResponse(ctx, response)
	return data, response.StatusCode, err
}

func (e *engine) conversation(ctx context.Context) (int, error) {
	return e.textConversation(ctx, e.base, false, false)
}

func (e *engine) directConversation(ctx context.Context) (int, error) {
	return e.textConversation(ctx, e.direct, true, false)
}

func (e *engine) afterCancel(ctx context.Context) (int, error) {
	return e.textConversation(ctx, e.base, false, true)
}

func (e *engine) textConversation(ctx context.Context, base *url.URL, direct, retryLocal bool) (int, error) {
	request := newProbe(e.options.Model, conversationPrompt, !direct)
	retryDeadline := time.Now().Add(2 * time.Second)
	for {
		data, status, err := e.post(ctx, base, request)
		if err != nil {
			return status, err
		}
		if status == http.StatusOK {
			return status, completedText(data)
		}
		if !retryLocal || status != http.StatusServiceUnavailable || !localConcurrencyError(data) {
			return status, errHTTPStatus
		}
		remaining := time.Until(retryDeadline)
		if remaining <= 0 {
			return status, errConcurrency
		}
		timer := time.NewTimer(min(100*time.Millisecond, remaining))
		select {
		case <-ctx.Done():
			timer.Stop()
			return status, safeRequestError(ctx, ctx.Err())
		case <-timer.C:
		}
		if !time.Now().Before(retryDeadline) {
			return status, errConcurrency
		}
	}
}

func (e *engine) sse(ctx context.Context) (int, error) {
	request := newProbe(e.options.Model, streamPrompt, true)
	request.Stream = true
	return e.stream(ctx, request, false)
}

func (e *engine) cancelStream(ctx context.Context) (int, error) {
	request := newProbe(e.options.Model, cancelPrompt, true)
	request.Stream = true
	return e.stream(ctx, request, true)
}

func (e *engine) stream(ctx context.Context, request probeRequest, cancelOnText bool) (int, error) {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	body, err := json.Marshal(request)
	if err != nil {
		return 0, errRequest
	}
	response, err := e.send(streamCtx, responsesURL(e.base), http.MethodPost, body, true)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	status := response.StatusCode
	if status != http.StatusOK {
		return status, errHTTPStatus
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "text/event-stream" {
		return status, errStreamType
	}
	reader := newSSEReader(response.Body)
	sawText := false
	for {
		event, err := reader.next()
		if err != nil {
			if ctx.Err() != nil {
				return status, safeRequestError(ctx, err)
			}
			return status, safeSSEReadError(err)
		}
		if ctx.Err() != nil {
			return status, safeRequestError(ctx, ctx.Err())
		}
		kind, delta, completed, err := parseSSEEvent(event)
		if err != nil {
			return status, err
		}
		if kind == "response.output_text.delta" && delta != "" {
			sawText = true
			if cancelOnText {
				cancel()
				_ = response.Body.Close() // Intentional cancellation, not a completion read.
				return status, nil
			}
		}
		if kind == "response.completed" {
			if cancelOnText {
				return status, errCancelBeforeText
			}
			if !completed {
				return status, errSSEStatus
			}
			if !sawText {
				return status, errSSENoText
			}
			return status, nil
		}
	}
}
