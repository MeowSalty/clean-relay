package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"

	"cleanrelay/internal/config"
	"cleanrelay/internal/relay"
)

// Set with -ldflags "-X main.version=<version>" when building a release.
var version = "dev"

const (
	exitSuccess = 0
	exitRuntime = 1
	exitUsage   = 2
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("clean-relay", flag.ContinueOnError)
	// Parser errors can include user-supplied values. Use fixed diagnostics below.
	flags.SetOutput(io.Discard)
	var configPath string
	var checkConfig, showVersion, showHelp bool
	flags.StringVar(&configPath, "config", "clean-relay.json", "configuration path")
	flags.BoolVar(&checkConfig, "check-config", false, "validate configuration offline")
	flags.BoolVar(&showVersion, "version", false, "print version")
	flags.BoolVar(&showHelp, "help", false, "print usage")
	flags.BoolVar(&showHelp, "h", false, "print usage")
	if err := flags.Parse(args); err != nil {
		fmt.Fprintln(stderr, "clean-relay: invalid arguments; use -help for usage")
		return exitUsage
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "clean-relay: positional arguments are not supported; use -help for usage")
		return exitUsage
	}
	if showHelp {
		printUsage(stdout)
		return exitSuccess
	}
	if showVersion {
		fmt.Fprintf(stdout, "clean-relay %s\n", version)
		return exitSuccess
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		var pathErr *os.PathError
		var validationErr *config.ValidationError
		if errors.As(err, &pathErr) {
			fmt.Fprintln(stderr, "clean-relay: cannot read configuration file; check -config and file access permissions")
		} else if errors.As(err, &validationErr) {
			fmt.Fprintf(stderr, "clean-relay: invalid configuration: %s\n", validationErr)
		} else {
			fmt.Fprintln(stderr, "clean-relay: invalid configuration; check the configuration fields")
		}
		return exitUsage
	}
	if checkConfig {
		fmt.Fprintln(stdout, "Configuration is valid.")
		return exitSuccess
	}

	logger := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: cfg.LogLevel}))
	interrupts := make(chan os.Signal, 2)
	signal.Notify(interrupts, os.Interrupt)
	defer signal.Stop(interrupts)
	if err := runServer(cfg, relay.New(cfg, logger), logger, serverOptions{signals: interrupts}); err != nil {
		// Never print the wrapped error: network errors can contain full URLs.
		var listenErr *net.OpError
		if errors.As(err, &listenErr) && listenErr.Op == "listen" {
			logger.Error("cannot listen; check whether the address is already in use", "listen", cfg.Listen)
		} else {
			logger.Error("server failed to run")
		}
		return exitRuntime
	}
	return exitSuccess
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, `Usage: clean-relay [-config path] [-check-config]

  -config path   Configuration file (default: clean-relay.json in the current directory).
  -check-config  Validate configuration offline and exit without listening.
  -version       Print the build version and exit without loading configuration.
  -help, -h      Print this help and exit.`)
}
