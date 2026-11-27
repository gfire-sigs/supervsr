package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"z.gfire.dev/supervsr/replication/protocol"
)

var (
	errUsagePut            = errors.New("usage: put KEY VALUE")
	errUsageGet            = errors.New("usage: get KEY")
	errUsageDelete         = errors.New("usage: delete KEY")
	errUnknownCommand      = errors.New("commands: put, get, delete, quit")
	errInvalidChecksumMode = errors.New("checksum must be aegis or wyhash")
)

type options struct {
	dataDir        string
	replicas       uint
	commandTimeout time.Duration
	directIO       bool
	prettyLogs     bool
	logLevel       string
	checksumMode   string
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
			logger.Error("cluster shutdown failed", "error", closeErr)
		}
	}()

	if err := cluster.RegisterClient(ctx, max(cfg.commandTimeout, 30*time.Second)); err != nil {
		return err
	}
	logger.Info("toykv ready", "replicas", cfg.replicas, "data_dir", cfg.dataDir)
	return serveCommands(ctx, cluster, cfg.commandTimeout)
}

func parseFlags() options {
	var cfg options
	flag.StringVar(&cfg.dataDir, "data", "toykv-data", "directory containing cluster data")
	flag.UintVar(&cfg.replicas, "replicas", 3, "active replica count used when initializing a cluster")
	flag.DurationVar(&cfg.commandTimeout, "timeout", 10*time.Second, "per-command response timeout")
	flag.BoolVar(&cfg.directIO, "direct-io", false, "enable direct I/O for replica files")
	flag.BoolVar(&cfg.prettyLogs, "pretty-logs", false, "render human-readable logs")
	flag.StringVar(&cfg.logLevel, "log-level", "info", "log level: debug, info, warn, error")
	flag.StringVar(&cfg.checksumMode, "checksum", "aegis", "checksum algorithm: aegis or wyhash")
	flag.Parse()
	return cfg
}

func parseChecksumMode(mode string) (protocol.ChecksumAlgorithm, string, error) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", "aegis", "aegis128l":
		return protocol.ChecksumAEGIS128L, "aegis", nil
	case "wyhash", "wyhash128":
		return protocol.ChecksumWyhash128, "wyhash", nil
	default:
		return 0, "", errInvalidChecksumMode
	}
}

func checksumModeName(algorithm protocol.ChecksumAlgorithm) string {
	if algorithm == protocol.ChecksumWyhash128 {
		return "wyhash"
	}
	return "aegis"
}

func newLogger(cfg options) (*slog.Logger, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(strings.TrimSpace(cfg.logLevel))); err != nil {
		return nil, fmt.Errorf("parse log level: %w", err)
	}
	handlerOpts := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	if cfg.prettyLogs {
		handler = slog.NewTextHandler(os.Stderr, handlerOpts)
	} else {
		handler = slog.NewJSONHandler(os.Stderr, handlerOpts)
	}
	return slog.New(handler).With("service", "toykv"), nil
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
			return "", errUsagePut
		}
		return cluster.Put(ctx, timeout, key, value)
	case "get":
		key := strings.TrimSpace(rest)
		if key == "" || strings.ContainsRune(key, ' ') {
			return "", errUsageGet
		}
		return cluster.Get(ctx, timeout, key)
	case "delete", "del":
		key := strings.TrimSpace(rest)
		if key == "" || strings.ContainsRune(key, ' ') {
			return "", errUsageDelete
		}
		return cluster.Delete(ctx, timeout, key)
	default:
		return "", errUnknownCommand
	}
}

func isTerminal(file *os.File) bool {
	info, err := file.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice != 0
}
