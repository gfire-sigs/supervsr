package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"
)

type options struct {
	dataDir        string
	replicas       uint
	commandTimeout time.Duration
	directIO       bool
	prettyLogs     bool
	logLevel       string
}

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	cfg := parseFlags()
	logger, err := newLogger(cfg)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cluster, err := openCluster(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if closeErr := cluster.Close(closeCtx); closeErr != nil {
			event := logger.Error()
			event.Err(closeErr)
			event.Msg("cluster shutdown failed")
		}
	}()

	if err := cluster.RegisterClient(ctx, max(cfg.commandTimeout, 30*time.Second)); err != nil {
		return err
	}
	event := logger.Info()
	event.Uint("replicas", cfg.replicas)
	event.Str("data_dir", cfg.dataDir)
	event.Msg("toykv ready")
	return serveCommands(ctx, cluster, cfg.commandTimeout)
}

func parseFlags() options {
	var cfg options
	flag.StringVar(&cfg.dataDir, "data", "toykv-data", "directory containing cluster data")
	flag.UintVar(&cfg.replicas, "replicas", 3, "active replica count used when initializing a cluster")
	flag.DurationVar(&cfg.commandTimeout, "timeout", 10*time.Second, "per-command response timeout")
	flag.BoolVar(&cfg.directIO, "direct-io", false, "enable direct I/O for replica files")
	flag.BoolVar(&cfg.prettyLogs, "pretty-logs", false, "render human-readable logs")
	flag.StringVar(&cfg.logLevel, "log-level", "info", "zerolog level")
	flag.Parse()
	return cfg
}

func newLogger(cfg options) (zerolog.Logger, error) {
	level, err := zerolog.ParseLevel(cfg.logLevel)
	if err != nil {
		return zerolog.Logger{}, fmt.Errorf("parse log level: %w", err)
	}
	var logger zerolog.Logger
	if cfg.prettyLogs {
		logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339Nano})
	} else {
		logger = zerolog.New(os.Stderr)
	}
	logger = logger.Level(level)
	context := logger.With()
	context = context.Timestamp()
	context = context.Str("service", "toykv")
	return context.Logger(), nil
}

func serveCommands(ctx context.Context, cluster *localCluster, timeout time.Duration) error {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 4096), maxRequestBytes+512)
	for {
		if isTerminal(os.Stdin) {
			_, _ = fmt.Fprint(os.Stdout, "toykv> ")
		}
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return fmt.Errorf("read command: %w", err)
			}
			return nil
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if line == "quit" || line == "exit" {
			return nil
		}
		result, err := executeLine(ctx, cluster, timeout, line)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			_, _ = fmt.Fprintf(os.Stdout, "ERR %v\n", err)
			continue
		}
		_, _ = fmt.Fprintln(os.Stdout, result)
	}
}

func executeLine(ctx context.Context, cluster *localCluster, timeout time.Duration, line string) (string, error) {
	command, rest, _ := strings.Cut(line, " ")
	switch strings.ToLower(command) {
	case "put":
		key, value, found := strings.Cut(strings.TrimSpace(rest), " ")
		if !found || key == "" {
			return "", errors.New("usage: put KEY VALUE")
		}
		return cluster.Put(ctx, timeout, key, value)
	case "get":
		key := strings.TrimSpace(rest)
		if key == "" || strings.ContainsRune(key, ' ') {
			return "", errors.New("usage: get KEY")
		}
		return cluster.Get(ctx, timeout, key)
	case "delete", "del":
		key := strings.TrimSpace(rest)
		if key == "" || strings.ContainsRune(key, ' ') {
			return "", errors.New("usage: delete KEY")
		}
		return cluster.Delete(ctx, timeout, key)
	default:
		return "", errors.New("commands: put, get, delete, quit")
	}
}

func isTerminal(file *os.File) bool {
	info, err := file.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice != 0
}
