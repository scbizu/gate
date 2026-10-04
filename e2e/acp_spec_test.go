package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anra-studio/gate/internal/acp"
	acpv1 "github.com/anra-studio/gate/internal/acp/v1"
	sdk "github.com/coder/acp-go-sdk"
)

// specPrompt provides deterministic ACP peer behavior, without a model provider.
func (a *e2eACPAgent) specPrompt(ctx context.Context, req sdk.PromptRequest) (bool, sdk.PromptResponse, error) {
	a.mu.Lock()
	session, exists := a.sessions[req.SessionId]
	a.mu.Unlock()
	if !exists {
		return true, sdk.PromptResponse{}, sdk.NewInvalidParams(nil)
	}
	text := ""
	if len(req.Prompt) > 0 && req.Prompt[0].Text != nil {
		text = req.Prompt[0].Text.Text
	}
	end := sdk.PromptResponse{StopReason: sdk.StopReasonEndTurn}
	switch {
	case strings.HasPrefix(text, "stop:"):
		return true, sdk.PromptResponse{StopReason: sdk.StopReason(strings.TrimPrefix(text, "stop:"))}, nil
	case text == "rpc-error":
		return true, sdk.PromptResponse{}, sdk.NewInvalidParams(map[string]any{"private": "must-not-cross-adapter"})
	case text == "wait":
		if err := a.sendUpdate(ctx, req.SessionId, sdk.UpdateAgentMessageText("waiting")); err != nil {
			return true, end, err
		}
		<-ctx.Done()
		return true, sdk.PromptResponse{StopReason: sdk.StopReasonCancelled}, nil
	case text == "session":
		return true, end, a.sendUpdate(ctx, req.SessionId, sdk.UpdateAgentMessageText(string(req.SessionId)))
	case text == "mcp":
		if len(session.McpServers) != 3 || len(session.AdditionalDirectories) != 1 {
			return true, end, errors.New("missing session configuration")
		}
		stdio, http, sse := session.McpServers[0].Stdio, session.McpServers[1].Http, session.McpServers[2].Sse
		if stdio == nil || stdio.Command != "/bin/echo" || len(stdio.Args) != 1 || len(stdio.Env) != 1 || stdio.Env[0].Value != "test-token" || http == nil || len(http.Headers) != 1 || http.Headers[0].Value != "test-header" || sse == nil || sse.Url != "https://example.com/sse" {
			return true, end, errors.New("incorrect MCP conversion")
		}
		return true, end, nil
	case text == "reverse-rpc":
		_, readErr := a.connection.ReadTextFile(ctx, sdk.ReadTextFileRequest{SessionId: req.SessionId, Path: "/workspace/file"})
		_, writeErr := a.connection.WriteTextFile(ctx, sdk.WriteTextFileRequest{SessionId: req.SessionId, Path: "/workspace/file", Content: "unsafe"})
		_, createErr := a.connection.CreateTerminal(ctx, sdk.CreateTerminalRequest{SessionId: req.SessionId, Command: "echo"})
		_, outputErr := a.connection.TerminalOutput(ctx, sdk.TerminalOutputRequest{SessionId: req.SessionId, TerminalId: "test"})
		_, killErr := a.connection.KillTerminal(ctx, sdk.KillTerminalRequest{SessionId: req.SessionId, TerminalId: "test"})
		_, releaseErr := a.connection.ReleaseTerminal(ctx, sdk.ReleaseTerminalRequest{SessionId: req.SessionId, TerminalId: "test"})
		_, waitErr := a.connection.WaitForTerminalExit(ctx, sdk.WaitForTerminalExitRequest{SessionId: req.SessionId, TerminalId: "test"})
		for _, err := range []error{readErr, writeErr, createErr, outputErr, killErr, releaseErr, waitErr} {
			var rpc *sdk.RequestError
			if !errors.As(err, &rpc) || rpc.Code != -32601 {
				return true, end, fmt.Errorf("unsupported reverse RPC: %v", err)
			}
		}
		return true, end, nil
	case text == "observations":
		updates := []sdk.SessionUpdate{
			sdk.UpdateUserMessageText("history is not agent output"),
			sdk.UpdatePlan(sdk.PlanEntry{Content: "inspect", Priority: sdk.PlanEntryPriorityMedium, Status: sdk.PlanEntryStatusPending}),
			sdk.UpdateAgentMessageText("research "),
			sdk.UpdateAgentThoughtText("Inspect."),
			sdk.StartToolCall("spec-tool", "Edit file", sdk.WithStartKind(sdk.ToolKindEdit), sdk.WithStartStatus(sdk.ToolCallStatusInProgress), sdk.WithStartRawInput(map[string]any{"secret": "must-not-cross-adapter"}), sdk.WithStartContent([]sdk.ToolCallContent{sdk.ToolDiffContent("/workspace/file", "new", "old"), sdk.ToolTerminalRef("terminal-1"), sdk.ToolContent(sdk.TextBlock("output"))})),
			sdk.UpdateToolCall("spec-tool", sdk.WithUpdateStatus(sdk.ToolCallStatusCompleted), sdk.WithUpdateRawOutput(map[string]any{"secret": "must-not-cross-adapter"})),
			sdk.UpdateAgentMessageText("complete"),
		}
		for _, update := range updates {
			if err := a.sendUpdate(ctx, req.SessionId, update); err != nil {
				return true, end, err
			}
		}
		return true, end, nil
	case text == "echo":
		for _, block := range req.Prompt[1:] {
			if err := a.sendUpdate(ctx, req.SessionId, sdk.UpdateAgentMessage(block)); err != nil {
				return true, end, err
			}
		}
		return true, end, nil
	default:
		return false, end, nil
	}
}

func initializedACP(t *testing.T) (*acpv1.Client, string) {
	t.Helper()
	client := startACPAgentProcess(t)
	result, err := client.Initialize(t.Context(), acp.InitializeRequest{ClientInfo: acp.ClientInfo{Name: "gate-spec", Title: "Spec client", Version: "test"}})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Capabilities.PromptImage || !result.Capabilities.PromptAudio || !result.Capabilities.MCPHTTP || !result.Capabilities.MCPSSE {
		t.Fatalf("capabilities: %#v", result.Capabilities)
	}
	session, err := client.NewSession(t.Context(), acp.NewSessionRequest{CWD: mustWorkingDirectory(t)})
	if err != nil {
		t.Fatal(err)
	}
	return client, session.ID
}

func rejectingHandler() *e2eACPHandler {
	return &e2eACPHandler{permission: func(context.Context, acp.PermissionRequest) (acp.PermissionOutcome, error) {
		return acp.CancelPermission(), nil
	}}
}

func TestACPProcessStopReasons(t *testing.T) {
	client, id := initializedACP(t)
	for _, reason := range []acp.StopReason{acp.StopEndTurn, acp.StopMaxTokens, acp.StopMaxTurnRequests, acp.StopRefusal, acp.StopCancelled} {
		t.Run(string(reason), func(t *testing.T) {
			got, err := client.Prompt(t.Context(), acp.PromptRequest{SessionID: id, Content: []acp.Content{acp.Text("stop:" + string(reason))}}, rejectingHandler())
			if err != nil || got != reason {
				t.Fatalf("stop = %q, %v", got, err)
			}
		})
	}
}

func TestACPProcessContentRoundTrip(t *testing.T) {
	client, id := initializedACP(t)
	blocks := []acp.Content{acp.Text("中文\nframing\"" + strings.Repeat("large prompt\n", 10000)), {Type: "image", Data: "aW1hZ2U=", MIMEType: "image/png", URI: "https://example.com/image"}, {Type: "audio", Data: "YXVkaW8=", MIMEType: "audio/wav"}, {Type: "resource_link", URI: "file:///workspace/readme"}}
	handler := rejectingHandler()
	reason, err := client.Prompt(t.Context(), acp.PromptRequest{SessionID: id, Content: append([]acp.Content{acp.Text("echo")}, blocks...)}, handler)
	if err != nil || reason != acp.StopEndTurn {
		t.Fatalf("Prompt: %s, %v", reason, err)
	}
	updates := handler.snapshot()
	if len(updates) != len(blocks) {
		t.Fatalf("updates = %#v", updates)
	}
	for i, update := range updates {
		if update.Kind != acp.UpdateAgentMessage || !reflect.DeepEqual(update.Content, blocks[i]) {
			t.Fatalf("block %d = %#v, want %#v", i, update, blocks[i])
		}
	}
}

func TestACPProcessMCPTransports(t *testing.T) {
	client := startACPAgentProcess(t)
	if _, err := client.Initialize(t.Context(), acp.InitializeRequest{}); err != nil {
		t.Fatal(err)
	}
	session, err := client.NewSession(t.Context(), acp.NewSessionRequest{CWD: mustWorkingDirectory(t), AdditionalDirectories: []string{"/workspace/extra"}, MCPServers: []acp.MCPServer{
		{Name: "stdio", Command: "/bin/echo", Args: []string{"hello"}, Env: []acp.EnvVariable{{Name: "TOKEN", Value: "test-token"}}},
		{Type: "http", Name: "http", URL: "https://example.com/mcp", Headers: []acp.Header{{Name: "Authorization", Value: "test-header"}}},
		{Type: "sse", Name: "sse", URL: "https://example.com/sse"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Prompt(t.Context(), acp.PromptRequest{SessionID: session.ID, Content: []acp.Content{acp.Text("mcp")}}, rejectingHandler()); err != nil {
		t.Fatal(err)
	}
}

func TestACPProcessProtocolErrors(t *testing.T) {
	t.Run("version", func(t *testing.T) {
		client := startACPAgentProcess(t, "GATE_E2E_VERSION=2")
		_, err := client.Initialize(t.Context(), acp.InitializeRequest{})
		var protocol *acp.ProtocolError
		if !errors.As(err, &protocol) {
			t.Fatalf("version error = %v", err)
		}
		if _, err := client.NewSession(t.Context(), acp.NewSessionRequest{CWD: mustWorkingDirectory(t)}); !errors.Is(err, acp.ErrClosed) {
			t.Fatalf("client remains open: %v", err)
		}
	})
	client, id := initializedACP(t)
	t.Run("rpc", func(t *testing.T) {
		_, err := client.Prompt(t.Context(), acp.PromptRequest{SessionID: id, Content: []acp.Content{acp.Text("rpc-error")}}, rejectingHandler())
		var rpc *acp.RPCError
		if !errors.As(err, &rpc) || rpc.Code != -32602 {
			t.Fatalf("RPC error = %v", err)
		}
	})
	t.Run("unsupported-client-capabilities", func(t *testing.T) {
		if _, err := client.Prompt(t.Context(), acp.PromptRequest{SessionID: id, Content: []acp.Content{acp.Text("reverse-rpc")}}, rejectingHandler()); err != nil {
			t.Fatal(err)
		}
	})
	for _, tc := range []struct {
		name    string
		req     acp.PromptRequest
		handler acp.Handler
	}{
		{"missing-session", acp.PromptRequest{Content: []acp.Content{acp.Text("x")}}, rejectingHandler()},
		{"missing-content", acp.PromptRequest{SessionID: id}, rejectingHandler()},
		{"missing-handler", acp.PromptRequest{SessionID: id, Content: []acp.Content{acp.Text("x")}}, nil},
		{"image-without-data", acp.PromptRequest{SessionID: id, Content: []acp.Content{{Type: "image", MIMEType: "image/png"}}}, rejectingHandler()},
		{"audio-without-mime", acp.PromptRequest{SessionID: id, Content: []acp.Content{{Type: "audio", Data: "YQ=="}}}, rejectingHandler()},
		{"resource-without-uri", acp.PromptRequest{SessionID: id, Content: []acp.Content{{Type: "resource_link"}}}, rejectingHandler()},
		{"unsupported-content", acp.PromptRequest{SessionID: id, Content: []acp.Content{{Type: "unknown"}}}, rejectingHandler()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.Prompt(t.Context(), tc.req, tc.handler)
			var protocol *acp.ProtocolError
			if !errors.As(err, &protocol) {
				t.Fatalf("error = %v", err)
			}
		})
	}
	for _, tc := range []struct {
		name string
		req  acp.NewSessionRequest
	}{
		{"relative-cwd", acp.NewSessionRequest{CWD: "relative"}},
		{"relative-additional-directory", acp.NewSessionRequest{CWD: "/workspace", AdditionalDirectories: []string{"relative"}}},
		{"mcp-missing-name", acp.NewSessionRequest{CWD: "/workspace", MCPServers: []acp.MCPServer{{Command: "/bin/echo"}}}},
		{"mcp-relative-command", acp.NewSessionRequest{CWD: "/workspace", MCPServers: []acp.MCPServer{{Name: "test", Command: "echo"}}}},
		{"mcp-relative-url", acp.NewSessionRequest{CWD: "/workspace", MCPServers: []acp.MCPServer{{Name: "test", Type: "http", URL: "relative"}}}},
		{"mcp-unknown-transport", acp.NewSessionRequest{CWD: "/workspace", MCPServers: []acp.MCPServer{{Name: "test", Type: "unknown"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.NewSession(t.Context(), tc.req)
			var protocol *acp.ProtocolError
			if !errors.As(err, &protocol) {
				t.Fatalf("error = %v", err)
			}
		})
	}
	if err := client.Cancel(t.Context(), ""); err == nil {
		t.Fatal("accepted empty cancel session")
	}
}

func TestACPProcessPermissionOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name       string
		outcome    acp.PermissionOutcome
		handlerErr error
		want       acp.StopReason
		rpcError   bool
	}{
		{name: "allow", outcome: acp.SelectPermission("allow-once"), want: acp.StopEndTurn},
		{name: "reject", outcome: acp.SelectPermission("reject"), want: acp.StopRefusal},
		{name: "cancel", outcome: acp.CancelPermission(), want: acp.StopCancelled},
		{name: "handler-error", handlerErr: errors.New("decision failed"), want: acp.StopCancelled},
		{name: "unknown-option", outcome: acp.SelectPermission("invented"), rpcError: true},
		{name: "cancel-with-option", outcome: acp.PermissionOutcome{Cancelled: true, OptionID: "allow-once"}, rpcError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, id := initializedACP(t)
			h := &e2eACPHandler{permission: func(_ context.Context, r acp.PermissionRequest) (acp.PermissionOutcome, error) {
				if len(r.Options) != 2 || r.Options[0].Kind != "allow_once" || r.ToolCall.Title != "Run tests" {
					return acp.PermissionOutcome{}, errors.New("bad permission conversion")
				}
				return tc.outcome, tc.handlerErr
			}}
			reason, err := client.Prompt(t.Context(), acp.PromptRequest{SessionID: id, Content: []acp.Content{acp.Text("permission")}}, h)
			if tc.rpcError {
				var rpc *acp.RPCError
				if !errors.As(err, &rpc) || rpc.Code != -32602 {
					t.Fatalf("error = %v", err)
				}
			} else if err != nil || reason != tc.want {
				t.Fatalf("result = %q, %v", reason, err)
			}
		})
	}
}

func TestACPProcessConcurrentPromptAndClose(t *testing.T) {
	client, id := initializedACP(t)
	started := make(chan struct{})
	h := &signalUpdateHandler{e2eACPHandler: rejectingHandler(), started: started}
	result := make(chan error, 1)
	go func() {
		_, err := client.Prompt(t.Context(), acp.PromptRequest{SessionID: id, Content: []acp.Content{acp.Text("wait")}}, h)
		result <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("prompt did not start")
	}
	if _, err := client.Prompt(t.Context(), acp.PromptRequest{SessionID: id, Content: []acp.Content{acp.Text("session")}}, rejectingHandler()); !errors.Is(err, acp.ErrPromptInProgress) {
		t.Fatalf("concurrent error = %v", err)
	}
	// A different ACP session can run while the first is blocked.
	session, err := client.NewSession(t.Context(), acp.NewSessionRequest{CWD: mustWorkingDirectory(t)})
	if err != nil {
		t.Fatal(err)
	}
	if session.ID == id {
		t.Fatal("session IDs are not unique")
	}
	if _, err := client.Prompt(t.Context(), acp.PromptRequest{SessionID: session.ID, Content: []acp.Content{acp.Text("session")}}, rejectingHandler()); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("pending prompt succeeded after close")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("close did not unblock prompt")
	}
	if _, err := client.Initialize(t.Context(), acp.InitializeRequest{}); !errors.Is(err, acp.ErrClosed) {
		t.Fatalf("Initialize after close: %v", err)
	}
	if err := client.Cancel(t.Context(), id); !errors.Is(err, acp.ErrClosed) {
		t.Fatalf("Cancel after close: %v", err)
	}
}

type signalUpdateHandler struct {
	*e2eACPHandler
	started chan struct{}
	once    sync.Once
}

func (h *signalUpdateHandler) OnUpdate(ctx context.Context, u acp.Update) {
	h.e2eACPHandler.OnUpdate(ctx, u)
	h.once.Do(func() { close(h.started) })
}
