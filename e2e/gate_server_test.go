package e2e_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	gate "github.com/anra-studio/gate/internal/a2a"
	acpv1 "github.com/anra-studio/gate/internal/acp/v1"
	"github.com/anra-studio/gate/internal/testutil/acppeer"
)

// startGateServer invokes the same server entry point as main, bypassing only
// CLI parsing and signal handling. The ACP agent remains a real stdio subprocess.
func startGateServer(t *testing.T, parent context.Context) (string, func() error) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cwd := mustWorkingDirectory(t)
	ctx, cancel := context.WithCancel(parent)
	ready := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- gate.ServeProxy(ctx, gate.ProxyConfig{
			ListenAddress: "127.0.0.1:0",
			Process: acpv1.ProcessConfig{
				Command: executable, Args: []string{"-test.run=^TestACPAgentProcess$"},
				Dir: cwd, Env: []string{acppeer.Environment + "=1"},
			},
			OnListening: func(origin string) { ready <- origin },
		})
	}()
	var once sync.Once
	var stopErr error
	stop := func() error {
		once.Do(func() {
			cancel()
			select {
			case stopErr = <-done:
			case <-time.After(15 * time.Second):
				stopErr = fmt.Errorf("Gate server did not stop within 15 seconds")
			}
		})
		return stopErr
	}
	t.Cleanup(func() {
		if err := stop(); err != nil {
			t.Error(err)
		}
	})
	select {
	case origin := <-ready:
		return origin, stop
	case err := <-done:
		once.Do(func() { cancel(); stopErr = err })
		t.Fatalf("start Gate server: %v", err)
	case <-ctx.Done():
		t.Fatalf("Gate server did not start: %v", ctx.Err())
	}
	return "", stop
}
