package relay

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"

	"cleanrelay/internal/config"
)

const inspectBlockBytes = 4096

var errInspectionTimeout = errors.New("response inspection timed out")

type inspectBlock struct {
	storage []byte
	data    []byte
	err     error
}

// inspectedBody has exactly two reusable blocks. Only pump reads the original
// Body. The inspector and then ReverseProxy are successive consumers; timeout
// changes the consumer, never the upstream context or reader ownership.
type inspectedBody struct {
	ctx       context.Context
	upstream  *http.Response
	response  *http.Response
	free      chan []byte
	ready     chan inspectBlock
	stopped   chan struct{}
	done      chan struct{}
	closeOnce sync.Once
	closeErr  error

	prefix    []byte
	prefixPos int
	current   inspectBlock
	terminal  error
	published bool
}

func newInspectedBody(ctx context.Context, upstream *http.Response, prefixCapacity int) (*http.Response, *inspectedBody) {
	response := new(http.Response)
	*response = *upstream
	// Transport fills upstream.Trailer while reading EOF. ReverseProxy reads
	// response.Trailer before copying the body, so they cannot share this map.
	response.Trailer = upstream.Trailer.Clone()
	body := &inspectedBody{
		ctx: ctx, upstream: upstream, response: response,
		free: make(chan []byte, 2), ready: make(chan inspectBlock, 1),
		stopped: make(chan struct{}), done: make(chan struct{}),
		prefix: make([]byte, 0, prefixCapacity),
	}
	for range 2 {
		body.free <- make([]byte, inspectBlockBytes)
	}
	response.Body = body
	go body.pump()
	return response, body
}

func (b *inspectedBody) pump() {
	defer close(b.done)
	defer close(b.ready)
	emptyReads := 0
	for {
		var storage []byte
		select {
		case <-b.stopped:
			return
		case storage = <-b.free:
		}
		n, err := b.upstream.Body.Read(storage)
		if n == 0 && err == nil {
			emptyReads++
			if emptyReads < 100 {
				b.free <- storage
				continue
			}
			err = io.ErrNoProgress
		} else {
			emptyReads = 0
		}
		block := inspectBlock{storage: storage, data: storage[:n], err: err}
		select {
		case <-b.stopped:
			return
		case b.ready <- block:
		}
		if err != nil {
			return
		}
	}
}

// readWire advances only the raw stream. Trailer publication and prefix replay
// are deliberately delayed until the public Read is used by ReverseProxy.
func (b *inspectedBody) readWire(p []byte, timeout <-chan time.Time) (int, error) {
	if b.terminal != nil {
		return 0, b.terminal
	}
	if b.current.storage == nil {
		select {
		case <-b.ctx.Done():
			return 0, b.ctx.Err()
		case <-b.stopped:
			return 0, io.ErrClosedPipe
		case <-timeout:
			return 0, errInspectionTimeout
		case block, ok := <-b.ready:
			if !ok {
				return 0, io.ErrClosedPipe
			}
			b.current = block
		}
	}
	n := copy(p, b.current.data)
	b.current.data = b.current.data[n:]
	if len(b.current.data) != 0 {
		return n, nil
	}
	err := b.current.err
	b.terminal = err
	b.free <- b.current.storage
	b.current = inspectBlock{}
	return n, err
}

func (b *inspectedBody) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if b.ctx.Err() != nil {
		return 0, b.ctx.Err()
	}
	if b.prefixPos < len(b.prefix) {
		n := copy(p, b.prefix[b.prefixPos:])
		b.prefixPos += n
		return n, nil
	}
	b.prefix = nil
	n, err := b.readWire(p, nil)
	if b.terminal != nil && !b.published {
		<-b.done
		b.response.Trailer = b.upstream.Trailer.Clone()
		b.published = true
	}
	return n, err
}

func (b *inspectedBody) Close() error {
	b.closeOnce.Do(func() {
		close(b.stopped)
		// The underlying attempt body cancels its context before closing the
		// socket, which wakes any blocked Read. No unbounded drain is needed.
		b.closeErr = b.upstream.Body.Close()
		<-b.done
	})
	return b.closeErr
}

func inspectResponse(ctx context.Context, response *http.Response, cfg config.AzureResourceRetryConfig, sse bool, encoding string, started time.Time) (*http.Response, inspectionResult) {
	deadline := started.Add(cfg.InspectTimeout)
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	prefixCapacity := int(cfg.MaxInspectBytes) + 1
	if sse {
		// A complete error ending exactly at the limit can still match. An
		// unfinished frame at the limit cannot match without exceeding it.
		prefixCapacity--
	}
	forward, body := newInspectedBody(ctx, response, prefixCapacity)
	parser := sseInspector{}
	for {
		if ctx.Err() != nil {
			return forward, inspectionResult{decision: "canceled"}
		}
		if !time.Now().Before(deadline) {
			return forward, inspectionResult{decision: "inspection_timeout"}
		}
		before := len(body.prefix)
		end := min(cap(body.prefix), before+inspectBlockBytes)
		n, err := body.readWire(body.prefix[before:end], timer.C)
		body.prefix = body.prefix[:before+n]
		if ctx.Err() != nil {
			return forward, inspectionResult{decision: "canceled"}
		}
		if errors.Is(err, errInspectionTimeout) || !time.Now().Before(deadline) {
			return forward, inspectionResult{decision: "inspection_timeout"}
		}
		if sse {
			// Parse only bytes within the limit, even if the next network block
			// includes both an in-window error and additional wire data.
			validEnd := min(len(body.prefix), int(cfg.MaxInspectBytes))
			if result := parser.feed(body.prefix[before:validEnd]); result.finished() {
				if !time.Now().Before(deadline) {
					return forward, inspectionResult{decision: "inspection_timeout"}
				}
				return forward, result
			}
		}
		if int64(len(body.prefix)) > cfg.MaxInspectBytes || sse && int64(len(body.prefix)) == cfg.MaxInspectBytes {
			return forward, inspectionResult{decision: "inspection_limit"}
		}
		if err == nil {
			continue
		}
		if !errors.Is(err, io.EOF) {
			return forward, inspectionResult{decision: "inspection_read_error"}
		}
		if sse {
			return forward, inspectionResult{decision: "inspection_eof"}
		}
		plain := body.prefix
		if encoding == "gzip" {
			var decodeErr error
			plain, decodeErr = decompress(body.prefix, cfg.MaxInspectBytes)
			if errors.Is(decodeErr, errBodyTooLarge) {
				return forward, inspectionResult{decision: "inspection_limit"}
			}
			if decodeErr != nil {
				return forward, inspectionResult{decision: "invalid_gzip"}
			}
		}
		matched, failed := matchHTTPError(plain)
		if !time.Now().Before(deadline) {
			return forward, inspectionResult{decision: "inspection_timeout"}
		}
		return forward, inspectionResult{matched: matched, businessFailure: failed, decision: "no_match"}
	}
}
