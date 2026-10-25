package relay

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"strings"
	"time"

	"cleanrelay/internal/config"
)

var errUpstreamUpgrade = errors.New("upstream protocol upgrades are not supported")

func (h *Handler) newProxy(cfg config.Config) (*http.Transport, *httputil.ReverseProxy) {
	origin := *cfg.UpstreamOrigin
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DisableCompression = true
	transport.DialContext = (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	transport.TLSHandshakeTimeout = 10 * time.Second
	transport.ResponseHeaderTimeout = cfg.ResponseHeaderTimeout
	transport.MaxIdleConnsPerHost = cfg.MaxInFlight
	proxy := &httputil.ReverseProxy{
		Transport:     &retryTransport{base: transport, config: cfg.AzureResourceRetry},
		FlushInterval: -1,
		Rewrite: func(request *httputil.ProxyRequest) {
			request.SetURL(&origin)
			request.Out.URL.Path = request.In.URL.Path
			request.Out.URL.RawPath = request.In.URL.RawPath
			request.Out.URL.RawQuery = request.In.URL.RawQuery
			request.Out.URL.ForceQuery = request.In.URL.ForceQuery
			request.Out.URL.Opaque = ""
			request.Out.Host = origin.Host
			for key := range request.Out.Header {
				lower := strings.ToLower(key)
				if lower == "forwarded" || strings.HasPrefix(lower, "x-forwarded-") {
					delete(request.Out.Header, key)
				}
			}
			// ReverseProxy may reintroduce TE: trailers; this relay does not
			// advertise hop-by-hop capabilities to the fixed upstream.
			deleteHeader(request.Out.Header, "Te")
		},
		ModifyResponse: func(response *http.Response) error {
			if response.StatusCode == http.StatusSwitchingProtocols {
				return errUpstreamUpgrade
			}
			if state, ok := response.Request.Context().Value(requestStateKey{}).(*requestState); ok {
				response.Body = &responseBody{ReadCloser: response.Body, state: state}
			}
			return nil
		},
		ErrorHandler: h.proxyError,
		// Raw ReverseProxy diagnostics may contain URLs or other request
		// values. The handler logs classified outcomes after each request.
		ErrorLog: log.New(io.Discard, "", 0),
	}
	return transport, proxy
}

func (h *Handler) proxyError(w http.ResponseWriter, r *http.Request, err error) {
	state := r.Context().Value(requestStateKey{}).(*requestState)
	if r.Context().Err() != nil || errors.Is(err, context.Canceled) {
		state.setOutcome("canceled")
		return
	}
	status, code, message := http.StatusBadGateway, "upstream_error", "The relay could not complete the upstream request."
	if isTimeout(err) {
		status, code, message = http.StatusGatewayTimeout, "upstream_timeout", "The upstream connection or response headers timed out."
	}
	state.setOutcome(code)
	writeLocalError(w, status, code, message, state.id)
}
