package v1_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/anra-studio/gate/internal/testutil/acppeer"

	"github.com/anra-studio/gate/internal/acp"
	acpv1 "github.com/anra-studio/gate/internal/acp/v1"
)

func TestACPProcessPromptAndPermission(t *testing.T) {
	client := startACPAgentProcess(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	initialized, err := client.Initialize(ctx, acp.InitializeRequest{
		ClientInfo: acp.ClientInfo{Name: "gate-e2e", Version: "test"},
	})
	if err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	if initialized.ProtocolVersion != acp.ProtocolVersion || initialized.AgentInfo.Name != "gate-e2e-agent" {
		t.Fatalf("Initialize() = %#v", initialized)
	}

	session, err := client.NewSession(ctx, acp.NewSessionRequest{CWD: mustWorkingDirectory(t)})
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}
	if session.ID != "e2e-session" {
		t.Fatalf("session ID = %q, want e2e-session", session.ID)
	}

	handler := &acpProcessHandler{
		permission: func(_ context.Context, request acp.PermissionRequest) (acp.PermissionOutcome, error) {
			if request.SessionID != session.ID || request.ToolCall.ID != "call-e2e" {
				return acp.PermissionOutcome{}, fmt.Errorf("unexpected permission request: %#v", request)
			}
			return acp.SelectPermission("allow-once"), nil
		},
	}
	reason, err := client.Prompt(ctx, acp.PromptRequest{
		SessionID: session.ID,
		Content:   []acp.Content{acp.Text("investigate the failure")},
	}, handler)
	if err != nil {
		t.Fatalf("Prompt() error = %v", err)
	}
	if reason != acp.StopEndTurn {
		t.Fatalf("Prompt() stop reason = %q, want %q", reason, acp.StopEndTurn)
	}

	updates := handler.snapshot()
	wantKinds := []acp.UpdateKind{
		acp.UpdateAgentMessage,
		acp.UpdateAgentThought,
		acp.UpdateToolCall,
		acp.UpdateToolCallDiff,
		acp.UpdateAgentMessage,
	}
	if len(updates) != len(wantKinds) {
		t.Fatalf("update count = %d, want %d: %#v", len(updates), len(wantKinds), updates)
	}
	for index, want := range wantKinds {
		if updates[index].Kind != want {
			t.Fatalf("update[%d].Kind = %q, want %q", index, updates[index].Kind, want)
		}
	}
	if updates[1].MessageID != "00000000-0000-4000-8000-000000000002" {
		t.Fatalf("thought message ID = %q", updates[1].MessageID)
	}
	if updates[2].ToolCall.ID != "call-e2e" || updates[2].ToolCall.Title != "Run tests" {
		t.Fatalf("tool call = %#v", updates[2].ToolCall)
	}
	if updates[4].Content.Text != "Research complete." {
		t.Fatalf("final message = %#v", updates[4])
	}
}

func TestACPProcessCancelPendingPermission(t *testing.T) {
	client := startACPAgentProcess(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := client.Initialize(ctx, acp.InitializeRequest{}); err != nil {
		t.Fatal(err)
	}
	session, err := client.NewSession(ctx, acp.NewSessionRequest{CWD: mustWorkingDirectory(t)})
	if err != nil {
		t.Fatal(err)
	}

	permissionStarted := make(chan struct{})
	handler := &acpProcessHandler{permission: func(ctx context.Context, _ acp.PermissionRequest) (acp.PermissionOutcome, error) {
		close(permissionStarted)
		<-ctx.Done()
		return acp.PermissionOutcome{}, ctx.Err()
	}}
	promptResult := make(chan struct {
		reason acp.StopReason
		err    error
	}, 1)
	go func() {
		reason, err := client.Prompt(ctx, acp.PromptRequest{
			SessionID: session.ID,
			Content:   []acp.Content{acp.Text("wait for permission")},
		}, handler)
		promptResult <- struct {
			reason acp.StopReason
			err    error
		}{reason: reason, err: err}
	}()

	select {
	case <-permissionStarted:
	case <-ctx.Done():
		t.Fatal("permission request was not received")
	}
	if err := client.Cancel(ctx, session.ID); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	select {
	case result := <-promptResult:
		if result.err != nil {
			t.Fatalf("Prompt() error = %v", result.err)
		}
		if result.reason != acp.StopCancelled {
			t.Fatalf("Prompt() stop reason = %q, want %q", result.reason, acp.StopCancelled)
		}
	case <-ctx.Done():
		t.Fatal("prompt did not finish after cancellation")
	}
}

func startACPAgentProcess(t *testing.T, extraEnv ...string) *acpv1.Client {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	client, err := acpv1.StartProcess(t.Context(), acpv1.ProcessConfig{
		Command: executable,
		Args:    []string{"-test.run=^TestACPAgentProcess$"},
		Env:     append([]string{acppeer.Environment + "=1"}, extraEnv...),
	})
	if err != nil {
		t.Fatalf("StartProcess() error = %v", err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return client
}

func mustWorkingDirectory(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

type acpProcessHandler struct {
	mu         sync.Mutex
	updates    []acp.Update
	permission func(context.Context, acp.PermissionRequest) (acp.PermissionOutcome, error)
}

func (h *acpProcessHandler) OnUpdate(_ context.Context, update acp.Update) {
	h.mu.Lock()
	h.updates = append(h.updates, update)
	h.mu.Unlock()
}

func (h *acpProcessHandler) RequestPermission(ctx context.Context, request acp.PermissionRequest) (acp.PermissionOutcome, error) {
	return h.permission(ctx, request)
}

func (h *acpProcessHandler) snapshot() []acp.Update {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]acp.Update(nil), h.updates...)
}

// TestACPAgentProcess is the child entry point for the shared SDK peer fixture.
func TestACPAgentProcess(t *testing.T) {
	if os.Getenv(acppeer.Environment) != "1" {
		return
	}
	acppeer.Run()
}
