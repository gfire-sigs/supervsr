package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

const processHelperEnvironment = "TOYKV_PROCESS_HELPER"

func TestToyKVProcessCrashRecoveryAndCompatibility(t *testing.T) {
	if os.Getenv(processHelperEnvironment) == "1" {
		runToyKVProcessHelper(t)
		return
	}
	directory := t.TempDir()
	first := startToyKVProcess(t, directory, 3)
	first.command(t, "put durable value", "OK")
	first.kill(t)

	second := startToyKVProcess(t, directory, 3)
	second.command(t, "get durable", "value")
	second.stop(t)

	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(executable, "-test.run=^TestToyKVProcessCrashRecoveryAndCompatibility$", "-test.count=1")
	command.Env = toyKVProcessEnvironment(directory, 2)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Run(); err == nil {
		t.Fatal("incompatible replica count reopened durable cluster")
	}
	if !strings.Contains(output.String(), errClusterMetadata.Error()) {
		t.Fatalf("incompatible restart output = %q", output.String())
	}
}

func runToyKVProcessHelper(t testing.TB) {
	t.Helper()
	replicas, err := strconv.ParseUint(os.Getenv("TOYKV_REPLICAS"), 10, 8)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	cluster, err := openCluster(ctx, options{
		dataDir: os.Getenv("TOYKV_DATA_DIR"), replicas: uint(replicas), commandTimeout: 15 * time.Second,
	}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := cluster.Close(closeCtx); err != nil {
			t.Error(err)
		}
	}()
	if err := cluster.RegisterClient(ctx, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintln(os.Stdout, "READY"); err != nil {
		t.Fatal(err)
	}
	if err := serveCommands(ctx, cluster, 15*time.Second); err != nil {
		t.Fatal(err)
	}
}

type toyKVProcess struct {
	commandProcess *exec.Cmd
	input          io.WriteCloser
	output         *bufio.Reader
	stderr         bytes.Buffer
}

func startToyKVProcess(t testing.TB, directory string, replicas uint8) *toyKVProcess {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(executable, "-test.run=^TestToyKVProcessCrashRecoveryAndCompatibility$", "-test.count=1")
	command.Env = toyKVProcessEnvironment(directory, replicas)
	input, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	process := &toyKVProcess{commandProcess: command, input: input, output: bufio.NewReader(output)}
	command.Stderr = &process.stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	if line := process.readLine(t); line != "READY" {
		process.kill(t)
		t.Fatalf("process readiness = %q stderr=%q", line, process.stderr.String())
	}
	return process
}

func toyKVProcessEnvironment(directory string, replicas uint8) []string {
	return append(os.Environ(),
		processHelperEnvironment+"=1",
		"TOYKV_DATA_DIR="+directory,
		"TOYKV_REPLICAS="+strconv.FormatUint(uint64(replicas), 10),
	)
}

func (process *toyKVProcess) command(t testing.TB, command, expected string) {
	t.Helper()
	if _, err := io.WriteString(process.input, command+"\n"); err != nil {
		t.Fatal(err)
	}
	if line := process.readLine(t); line != expected {
		t.Fatalf("command %q result = %q stderr=%q", command, line, process.stderr.String())
	}
}

func (process *toyKVProcess) readLine(t testing.TB) string {
	t.Helper()
	type result struct {
		line string
		err  error
	}
	ready := make(chan result, 1)
	go func() {
		line, err := process.output.ReadString('\n')
		ready <- result{line: strings.TrimSpace(line), err: err}
	}()
	select {
	case output := <-ready:
		if output.err != nil {
			t.Fatalf("read process output: %v stderr=%q", output.err, process.stderr.String())
		}
		return output.line
	case <-time.After(45 * time.Second):
		process.kill(t)
		t.Fatalf("process output timed out stderr=%q", process.stderr.String())
		return ""
	}
}

func (process *toyKVProcess) kill(t testing.TB) {
	t.Helper()
	if err := process.commandProcess.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := process.commandProcess.Wait(); err == nil {
		t.Fatal("killed process exited successfully")
	}
}

func (process *toyKVProcess) stop(t testing.TB) {
	t.Helper()
	if _, err := io.WriteString(process.input, "quit\n"); err != nil {
		t.Fatal(err)
	}
	if err := process.input.Close(); err != nil {
		t.Fatal(err)
	}
	if err := process.commandProcess.Wait(); err != nil {
		t.Fatalf("process stop: %v stderr=%q", err, process.stderr.String())
	}
}
