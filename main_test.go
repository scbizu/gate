package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	protocol "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2aclient/agentcard"
	"github.com/anra-studio/gate/internal/testutil/acppeer"
)

func gateTestBinary(t *testing.T) string {
	t.Helper()
	binary := os.Getenv("GATE_TEST_BINARY")
	if binary == "" {
		binary = filepath.Join("bin", "gate")
	}
	binary, err := filepath.Abs(binary)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(binary)
	if err != nil {
		t.Fatalf("prebuilt Gate binary unavailable at %s: %v; run mise run build", binary, err)
	}
	if !info.Mode().IsRegular() {
		t.Fatalf("Gate binary is not a regular file: %s", binary)
	}
	return binary
}

func TestCLIValidation(t *testing.T) {
	binary := gateTestBinary(t)
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"no agent command", nil},
		{"non-HTTP public URL", []string{"-public-url", "ftp://example.com", "--", "unused"}},
		{"public URL with path", []string{"-public-url", "https://example.com/path", "--", "unused"}},
		{"public URL credentials", []string{"-public-url", "https://user:password@example.com", "--", "unused"}},
		{"missing directory", []string{"-cwd", filepath.Join(t.TempDir(), "missing"), "--", "unused"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := exec.CommandContext(t.Context(), binary, tc.args...).Run()
			var exit *exec.ExitError
			if !errors.As(err, &exit) {
				t.Fatalf("expected unsuccessful exit, got %v", err)
			}
		})
	}
}

func TestCLIHelp(t *testing.T) {
	output, err := exec.CommandContext(t.Context(), gateTestBinary(t), "-h").CombinedOutput()
	if err != nil {
		t.Fatalf("help: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "Usage: gate") {
		t.Fatalf("help did not show usage: %s", output)
	}
}

func TestCLIInterruptWithPendingTurn(t *testing.T) {
	origin, stop := startGateCommand(t, gateTestBinary(t))
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	card, err := agentcard.DefaultResolver.Resolve(ctx, origin)
	if err != nil {
		t.Fatal(err)
	}
	client, err := a2aclient.NewFromCard(ctx, card)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Destroy()
	req := &protocol.SendMessageRequest{Message: protocol.NewMessage(protocol.MessageRoleUser, protocol.NewTextPart("wait"))}
	for event, err := range client.SendStreamingMessage(ctx, req) {
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := event.(*protocol.TaskArtifactUpdateEvent); ok {
			if err := stop(); err != nil {
				t.Fatal(err)
			}
			return
		}
	}
	t.Fatal("agent never started its pending turn")
}

// TestACPAgentProcess is the deterministic child agent for command tests.
func TestACPAgentProcess(t *testing.T) {
	if os.Getenv(acppeer.Environment) != "1" {
		return
	}
	acppeer.Run()
}

// startGateCommand launches the production CLI and a deterministic ACP subprocess.
func startGateCommand(t *testing.T, binary string) (string, func() error) {
	t.Helper()
	helper, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "-listen", "127.0.0.1:0", "-cwd", t.TempDir(), "--", helper, "-test.run=^TestACPAgentProcess$")
	cmd.Env = append(os.Environ(), acppeer.Environment+"=1")
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	var stopOnce sync.Once
	var stopErr error
	stop := func() error {
		stopOnce.Do(func() {
			_ = cmd.Process.Signal(os.Interrupt)
			select {
			case err := <-done:
				if err != nil {
					stopErr = fmt.Errorf("gate shutdown: %w", err)
				}
			case <-time.After(15 * time.Second):
				_ = cmd.Process.Kill()
				<-done
				stopErr = fmt.Errorf("gate did not shut down within 15 seconds")
			}
		})
		return stopErr
	}
	t.Cleanup(func() {
		if err := stop(); err != nil {
			t.Error(err)
		}
	})
	ready := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			_, origin, ok := strings.Cut(scanner.Text(), "Gate listening at ")
			if ok {
				ready <- origin
			}
		}
	}()
	var origin string
	select {
	case origin = <-ready:
	case <-time.After(15 * time.Second):
		t.Fatal("gate did not listen")
	}
	return origin, stop
}
