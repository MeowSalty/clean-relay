package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"cleanrelay/internal/config"
)

const shutdownGracePeriod = 10 * time.Second

type lifecycleHandler interface {
	http.Handler
	CloseIdleConnections()
}

// runServer owns the listener, including one supplied by a test. A zero
// shutdownTimeout uses the production grace period; a nil signal channel
// disables signal handling, so only a server failure can end the run.
type serverOptions struct {
	listener        net.Listener
	signals         <-chan os.Signal
	shutdownTimeout time.Duration
}

func runServer(cfg config.Config, handler lifecycleHandler, logger *slog.Logger, options serverOptions) error {
	defer handler.CloseIdleConnections()
	listener := options.listener
	if listener == nil {
		var err error
		listener, err = net.Listen("tcp", cfg.Listen)
		if err != nil {
			return fmt.Errorf("listen: %w", err)
		}
	}
	root, cancelRoot := context.WithCancel(context.Background())
	server := newHTTPServer(root, cfg.Listen, handler, logger)
	defer func() {
		cancelRoot()
		// Best-effort cleanup also covers an unexpected Serve failure.
		_ = server.Close()
	}()

	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	// Both values came from the validated configuration, never an inbound URL.
	logger.Info("listening", "listen", cfg.Listen, "upstream_origin", cfg.UpstreamOrigin.String())

	signals := options.signals
	for {
		select {
		case err := <-serveDone:
			return serveError(err)
		case interrupt, ok := <-signals:
			if !ok {
				signals = nil
				continue
			}
			if interrupt != os.Interrupt {
				continue
			}
		}
		break
	}

	grace := options.shutdownTimeout
	if grace == 0 {
		grace = shutdownGracePeriod
	}
	// Shutdown gets its own deadline. The service root remains live while
	// active responses drain, including streams without a total timeout.
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), grace)
	defer cancelShutdown()
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- server.Shutdown(shutdownCtx) }()
	logger.Info("shutdown started", "grace_period", grace)

	for {
		select {
		case err := <-shutdownDone:
			if err == nil {
				logger.Info("shutdown completed")
				return serveError(<-serveDone)
			}
			cancelRoot()
			closeErr := server.Close()
			if errors.Is(err, context.DeadlineExceeded) {
				logger.Info("shutdown forced", "reason", "grace_period_expired")
				return errors.Join(serveError(<-serveDone), closeError(closeErr))
			}
			return errors.Join(fmt.Errorf("shutdown: %w", err), serveError(<-serveDone), closeError(closeErr))
		case interrupt, ok := <-signals:
			if !ok {
				signals = nil
				continue
			}
			if interrupt != os.Interrupt {
				continue
			}
			logger.Info("shutdown forced", "reason", "second_interrupt")
			cancelRoot()
			closeErr := server.Close()
			cancelShutdown()
			// Cancellation wakes Shutdown; neither Close nor this wait waits for
			// a handler to finish after its connection has been closed.
			<-shutdownDone
			return errors.Join(serveError(<-serveDone), closeError(closeErr))
		}
	}
}

func newHTTPServer(ctx context.Context, addr string, handler http.Handler, logger *slog.Logger) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      0,
		IdleTimeout:       60 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return ctx },
		ErrorLog:          log.New(safeServerLogWriter{logger: logger}, "", 0),
	}
}

func serveError(err error) error {
	if err == nil || errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return fmt.Errorf("serve: %w", err)
}

func closeError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("close: %w", err)
}

// net/http's diagnostics can contain request values, panic values and URLs.
// Preserve the event at the configured log level without copying its payload.
type safeServerLogWriter struct {
	logger *slog.Logger
}

func (w safeServerLogWriter) Write(p []byte) (int, error) {
	w.logger.Error("HTTP server connection error")
	return len(p), nil
}
