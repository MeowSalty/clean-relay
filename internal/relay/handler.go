// Package relay validates and cleans requests before forwarding HTTP responses.
package relay

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"net/http/httputil"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"cleanrelay/internal/cleaner"
	"cleanrelay/internal/config"
)

const healthPath = "/_cleanrelay/healthz"

var errBodyTooLarge = errors.New("request body exceeds the configured limit")

// Handler shares an immutable cleaner and an upstream connection pool. Config
// must have passed config.Parse or config.Load before New is called.
type Handler struct {
	cleaner      *cleaner.Cleaner
	maxBodyBytes int64
	inFlight     chan struct{}
	proxy        *httputil.ReverseProxy
	transport    *http.Transport
	logger       *slog.Logger
	idPrefix     string
	nextID       atomic.Uint64
	retryConfig  config.AzureResourceRetryConfig
}

// New copies configuration used by requests, so subsequent changes to cfg do
// not change the running relay. A nil logger uses slog's default logger.
func New(cfg config.Config, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	h := &Handler{
		cleaner:      cleaner.New(cfg.DropInputFields),
		maxBodyBytes: cfg.MaxBodyBytes,
		inFlight:     make(chan struct{}, cfg.MaxInFlight),
		logger:       logger,
		idPrefix:     "local-" + rand.Text() + "-",
		retryConfig:  cfg.AzureResourceRetry,
	}
	h.transport, h.proxy = h.newProxy(cfg)
	return h
}

// CloseIdleConnections releases idle upstream connections during shutdown.
// Active requests are canceled through their incoming request contexts.
func (h *Handler) CloseIdleConnections() {
	h.transport.CloseIdleConnections()
}

func (h *Handler) requestID() string {
	return h.idPrefix + strconv.FormatUint(h.nextID.Add(1), 10)
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == healthPath {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			writeLocalError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Health checks require GET.", h.requestID())
			return
		}
		_, _ = io.WriteString(w, "{\"status\":\"ok\"}\n")
		return
	}

	state := &requestState{id: h.requestID(), started: time.Now(), outcome: "completed"}
	state.retry = retryLog{mode: h.retryConfig.CleanupMode, decision: "disabled"}
	if h.retryConfig.Enabled {
		state.retry.decision = "no_candidate"
	}
	observed := &responseWriter{ResponseWriter: w, state: state}
	defer func() {
		caught := recover()
		if caught != nil {
			state.setOutcome("interrupted")
		}
		if r.Context().Err() != nil {
			state.setOutcome("canceled")
			if state.candidate != nil || state.retry.stopReason != "" {
				state.retry.stop("canceled")
			}
		}
		h.logRequest(r, state, observed.status)
		if caught != nil {
			// ReverseProxy uses ErrAbortHandler to terminate a partial response.
			// Returning normally here would make an interrupted stream look complete.
			panic(caught)
		}
	}()

	if r.Context().Err() != nil {
		return
	}
	if r.Method == http.MethodConnect || len(headerValues(r.Header, "Upgrade")) != 0 || headerHasToken(r.Header, "Connection", "upgrade") {
		h.reject(observed, r, state, http.StatusNotImplemented, "unsupported_protocol", "CONNECT and protocol upgrades are not supported.")
		return
	}
	if hasRequestTrailers(r) {
		h.reject(observed, r, state, http.StatusNotImplemented, "unsupported_trailers", "Request trailers are not supported.")
		return
	}
	select {
	case h.inFlight <- struct{}{}:
		defer func() { <-h.inFlight }()
	default:
		observed.Header().Set("Retry-After", "1")
		h.reject(observed, r, state, http.StatusServiceUnavailable, "too_many_requests", "The relay is at its configured concurrency limit.")
		return
	}

	wire, err := readBounded(r.Body, h.maxBodyBytes)
	if err != nil {
		switch {
		case errors.Is(err, errBodyTooLarge):
			h.reject(observed, r, state, http.StatusRequestEntityTooLarge, "body_too_large", "Request body exceeds the configured limit.")
		case isTimeout(err):
			h.reject(observed, r, state, http.StatusRequestTimeout, "request_timeout", "Reading the request body timed out.")
		default:
			h.reject(observed, r, state, http.StatusBadRequest, "invalid_body", "Request body could not be read.")
		}
		return
	}
	// A chunked request may declare trailers only at the end of its body.
	if hasRequestTrailers(r) {
		h.reject(observed, r, state, http.StatusNotImplemented, "unsupported_trailers", "Request trailers are not supported.")
		return
	}
	forward := r.Clone(context.WithValue(r.Context(), requestStateKey{}, state))
	if len(wire) != 0 {
		if !isJSONMediaType(r.Header) {
			h.reject(observed, r, state, http.StatusUnsupportedMediaType, "unsupported_media_type", "A nonempty request body requires an application/json or application/*+json Content-Type.")
			return
		}
		encoding, ok := contentEncoding(r.Header)
		if !ok {
			h.reject(observed, r, state, http.StatusUnsupportedMediaType, "unsupported_content_encoding", "Only identity or a single gzip content encoding is supported.")
			return
		}
		plain := wire
		if encoding == "gzip" {
			plain, err = decompress(wire, h.maxBodyBytes)
			if err != nil {
				if errors.Is(err, errBodyTooLarge) {
					h.reject(observed, r, state, http.StatusRequestEntityTooLarge, "body_too_large", "Decompressed request body exceeds the configured limit.")
				} else {
					h.reject(observed, r, state, http.StatusBadRequest, "invalid_gzip", "Request body is not a complete, valid gzip stream.")
				}
				return
			}
		}
		result, cleanErr := h.cleaner.Clean(plain)
		if cleanErr != nil {
			if errors.Is(cleanErr, cleaner.ErrAmbiguousJSON) {
				h.reject(observed, r, state, http.StatusBadRequest, "ambiguous_json", "The root object or a direct input object contains duplicate keys.")
			} else {
				h.reject(observed, r, state, http.StatusBadRequest, "invalid_json", "Request body must contain exactly one valid UTF-8 JSON value.")
			}
			return
		}
		state.inputItems, state.modifiedItems, state.removedFields = result.InputItems, result.ModifiedItems, result.RemovedFields
		h.prepareRetry(r, state, result.Body)
		if result.RemovedFields != 0 {
			wire = result.Body
			for _, key := range []string{"Content-MD5", "Digest", "Content-Digest", "Repr-Digest"} {
				deleteHeader(forward.Header, key)
			}
			if encoding == "gzip" {
				deleteHeader(forward.Header, "Content-Encoding")
			}
		}
	}
	forward.Body = http.NoBody
	if len(wire) != 0 {
		forward.Body = io.NopCloser(bytes.NewReader(wire))
	}
	forward.ContentLength = int64(len(wire))
	forward.TransferEncoding = nil
	forward.GetBody = nil
	for _, key := range []string{"Content-Length", "Transfer-Encoding", "Expect"} {
		deleteHeader(forward.Header, key)
	}
	if r.Context().Err() != nil {
		return
	}
	h.proxy.ServeHTTP(observed, forward)
}

func (h *Handler) reject(w http.ResponseWriter, r *http.Request, state *requestState, status int, code, message string) {
	state.setOutcome("rejected")
	if r.Context().Err() == nil {
		writeLocalError(w, status, code, message, state.id)
	}
}

func readBounded(reader io.Reader, limit int64) ([]byte, error) {
	if reader == nil {
		return nil, nil
	}
	body, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if int64(len(body)) > limit {
		return nil, errBodyTooLarge
	}
	return body, err
}

func decompress(wire []byte, limit int64) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(wire))
	if err != nil {
		return nil, err
	}
	// Multistream remains enabled so every member and any trailing junk are
	// checked. Reaching EOF within the limit also checks the gzip checksum.
	body, readErr := readBounded(reader, limit)
	closeErr := reader.Close()
	return body, errors.Join(readErr, closeErr)
}

func isJSONMediaType(header http.Header) bool {
	values := headerValues(header, "Content-Type")
	if len(values) != 1 {
		return false
	}
	mediaType, _, err := mime.ParseMediaType(values[0])
	return err == nil && (mediaType == "application/json" ||
		strings.HasPrefix(mediaType, "application/") && strings.HasSuffix(mediaType, "+json") && len(mediaType) > len("application/+json"))
}

func contentEncoding(header http.Header) (string, bool) {
	values := headerValues(header, "Content-Encoding")
	if len(values) == 0 {
		return "", true
	}
	if len(values) != 1 {
		return "", false
	}
	encoding := strings.ToLower(strings.TrimSpace(values[0]))
	return encoding, encoding == "" || encoding == "identity" || encoding == "gzip"
}

func hasRequestTrailers(r *http.Request) bool {
	return len(r.Trailer) != 0 || len(headerValues(r.Header, "Trailer")) != 0
}

func headerValues(header http.Header, name string) []string {
	var values []string
	for key, entries := range header {
		if strings.EqualFold(key, name) {
			values = append(values, entries...)
		}
	}
	return values
}

func headerHasToken(header http.Header, name, token string) bool {
	for _, value := range headerValues(header, name) {
		for _, part := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(part), token) {
				return true
			}
		}
	}
	return false
}

func deleteHeader(header http.Header, name string) {
	for key := range header {
		if strings.EqualFold(key, name) {
			delete(header, key)
		}
	}
}

func isTimeout(err error) bool {
	var networkError net.Error
	return errors.Is(err, context.DeadlineExceeded) || errors.As(err, &networkError) && networkError.Timeout()
}
