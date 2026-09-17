package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	acpsdk "github.com/coder/acp-go-sdk"

	"github.com/anra-studio/gate/internal/acp"
	acpv1 "github.com/anra-studio/gate/internal/acp/v1"
)

const acpAgentHelperEnv = "GATE_E2E_ACP_AGENT"

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

	handler := &e2eACPHandler{
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
	handler := &e2eACPHandler{permission: func(ctx context.Context, _ acp.PermissionRequest) (acp.PermissionOutcome, error) {
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

func startACPAgentProcess(t *testing.T) *acpv1.Client {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	client, err := acpv1.StartProcess(t.Context(), acpv1.ProcessConfig{
		Command: executable,
		Args:    []string{"-test.run=^TestACPAgentProcess$"},
		Env:     []string{acpAgentHelperEnv + "=1"},
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

type e2eACPHandler struct {
	mu         sync.Mutex
	updates    []acp.Update
	permission func(context.Context, acp.PermissionRequest) (acp.PermissionOutcome, error)
}

func (h *e2eACPHandler) OnUpdate(_ context.Context, update acp.Update) {
	h.mu.Lock()
	h.updates = append(h.updates, update)
	h.mu.Unlock()
}

func (h *e2eACPHandler) RequestPermission(ctx context.Context, request acp.PermissionRequest) (acp.PermissionOutcome, error) {
	return h.permission(ctx, request)
}

func (h *e2eACPHandler) snapshot() []acp.Update {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]acp.Update(nil), h.updates...)
}

// TestACPAgentProcess runs only in the child test process started above. Both
// ends deliberately use the open-source ACP SDK so the test exercises the real
// stdio framing, reverse RPC and cancellation behavior.
func TestACPAgentProcess(t *testing.T) {
	if os.Getenv(acpAgentHelperEnv) != "1" {
		return
	}
	agent := &e2eACPAgent{}
	connection := acpsdk.NewAgentSideConnection(agent, os.Stdout, os.Stdin)
	agent.connection = connection
	<-connection.Done()
}

type e2eACPAgent struct {
	connection *acpsdk.AgentSideConnection
}

func (*e2eACPAgent) Initialize(_ context.Context, request acpsdk.InitializeRequest) (acpsdk.InitializeResponse, error) {
	if request.ProtocolVersion != acpsdk.ProtocolVersionNumber {
		return acpsdk.InitializeResponse{}, fmt.Errorf("unsupported protocol version %d", request.ProtocolVersion)
	}
	return acpsdk.InitializeResponse{
		ProtocolVersion: acpsdk.ProtocolVersionNumber,
		AgentInfo: &acpsdk.Implementation{
			Name: "gate-e2e-agent", Version: "test",
		},
		AgentCapabilities: acpsdk.AgentCapabilities{
			PromptCapabilities: acpsdk.PromptCapabilities{},
		},
	}, nil
}

func (*e2eACPAgent) NewSession(_ context.Context, request acpsdk.NewSessionRequest) (acpsdk.NewSessionResponse, error) {
	if request.Cwd == "" {
		return acpsdk.NewSessionResponse{}, errors.New("missing cwd")
	}
	return acpsdk.NewSessionResponse{SessionId: "e2e-session"}, nil
}

func (a *e2eACPAgent) Prompt(ctx context.Context, request acpsdk.PromptRequest) (acpsdk.PromptResponse, error) {
	message := acpsdk.UpdateAgentMessageText("Researching.")
	message.AgentMessageChunk.MessageId = acpsdk.Ptr("00000000-0000-4000-8000-000000000001")
	if err := a.sendUpdate(ctx, request.SessionId, message); err != nil {
		return acpsdk.PromptResponse{}, err
	}
	thought := acpsdk.UpdateAgentThoughtText("Inspect the failing package first.")
	thought.AgentThoughtChunk.MessageId = acpsdk.Ptr("00000000-0000-4000-8000-000000000002")
	if err := a.sendUpdate(ctx, request.SessionId, thought); err != nil {
		return acpsdk.PromptResponse{}, err
	}
	tool := acpsdk.StartToolCall(
		"call-e2e",
		"Run tests",
		acpsdk.WithStartKind(acpsdk.ToolKindExecute),
		acpsdk.WithStartStatus(acpsdk.ToolCallStatusPending),
		acpsdk.WithStartLocations([]acpsdk.ToolCallLocation{{Path: "/workspace/project"}}),
		acpsdk.WithStartRawInput(map[string]any{"credential": "must-not-cross-adapter"}),
	)
	if err := a.sendUpdate(ctx, request.SessionId, tool); err != nil {
		return acpsdk.PromptResponse{}, err
	}

	permission, err := a.connection.RequestPermission(ctx, acpsdk.RequestPermissionRequest{
		SessionId: request.SessionId,
		ToolCall: acpsdk.ToolCallUpdate{
			ToolCallId: "call-e2e",
			Title:      acpsdk.Ptr("Run tests"),
			Kind:       acpsdk.Ptr(acpsdk.ToolKindExecute),
			Status:     acpsdk.Ptr(acpsdk.ToolCallStatusPending),
			RawInput:   map[string]any{"credential": "must-not-cross-adapter"},
		},
		Options: []acpsdk.PermissionOption{
			{OptionId: "allow-once", Name: "Allow once", Kind: acpsdk.PermissionOptionKindAllowOnce},
			{OptionId: "reject", Name: "Reject", Kind: acpsdk.PermissionOptionKindRejectOnce},
		},
	})
	if ctx.Err() != nil || (err == nil && permission.Outcome.Cancelled != nil) {
		return acpsdk.PromptResponse{StopReason: acpsdk.StopReasonCancelled}, nil
	}
	if err != nil {
		return acpsdk.PromptResponse{}, err
	}
	if permission.Outcome.Selected == nil || permission.Outcome.Selected.OptionId != "allow-once" {
		return acpsdk.PromptResponse{StopReason: acpsdk.StopReasonRefusal}, nil
	}
	if err := a.sendUpdate(ctx, request.SessionId, acpsdk.UpdateToolCall(
		"call-e2e",
		acpsdk.WithUpdateStatus(acpsdk.ToolCallStatusCompleted),
		acpsdk.WithUpdateRawOutput(map[string]any{"secret": "must-not-cross-adapter"}),
	)); err != nil {
		return acpsdk.PromptResponse{}, err
	}
	if err := a.sendUpdate(ctx, request.SessionId, acpsdk.UpdateAgentMessageText("Research complete.")); err != nil {
		return acpsdk.PromptResponse{}, err
	}
	return acpsdk.PromptResponse{StopReason: acpsdk.StopReasonEndTurn}, nil
}

func (a *e2eACPAgent) sendUpdate(ctx context.Context, sessionID acpsdk.SessionId, update acpsdk.SessionUpdate) error {
	return a.connection.SessionUpdate(ctx, acpsdk.SessionNotification{SessionId: sessionID, Update: update})
}

func (*e2eACPAgent) Authenticate(context.Context, acpsdk.AuthenticateRequest) (acpsdk.AuthenticateResponse, error) {
	return acpsdk.AuthenticateResponse{}, nil
}
func (*e2eACPAgent) Logout(context.Context, acpsdk.LogoutRequest) (acpsdk.LogoutResponse, error) {
	return acpsdk.LogoutResponse{}, nil
}
func (*e2eACPAgent) Cancel(context.Context, acpsdk.CancelNotification) error { return nil }
func (*e2eACPAgent) CloseSession(context.Context, acpsdk.CloseSessionRequest) (acpsdk.CloseSessionResponse, error) {
	return acpsdk.CloseSessionResponse{}, nil
}
func (*e2eACPAgent) ListSessions(context.Context, acpsdk.ListSessionsRequest) (acpsdk.ListSessionsResponse, error) {
	return acpsdk.ListSessionsResponse{}, nil
}
func (*e2eACPAgent) ResumeSession(context.Context, acpsdk.ResumeSessionRequest) (acpsdk.ResumeSessionResponse, error) {
	return acpsdk.ResumeSessionResponse{}, nil
}
func (*e2eACPAgent) SetSessionConfigOption(context.Context, acpsdk.SetSessionConfigOptionRequest) (acpsdk.SetSessionConfigOptionResponse, error) {
	return acpsdk.SetSessionConfigOptionResponse{}, nil
}
func (*e2eACPAgent) SetSessionMode(context.Context, acpsdk.SetSessionModeRequest) (acpsdk.SetSessionModeResponse, error) {
	return acpsdk.SetSessionModeResponse{}, nil
}

var _ acpsdk.Agent = (*e2eACPAgent)(nil)
