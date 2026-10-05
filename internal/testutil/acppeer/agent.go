// Package acppeer provides a deterministic ACP subprocess fixture for tests.
// Production code must not depend on this package.
package acppeer

import (
	"context"
	"errors"
	"fmt"
	sdk "github.com/coder/acp-go-sdk"
	"os"
	"strings"
	"sync"
)

// Environment selects the fixture in a child test process.
const Environment = "GATE_E2E_ACP_AGENT"

// Run serves ACP on stdin/stdout until the parent closes the connection.
// Call it only from the child-process entry point, after checking Environment.
func Run() {
	a := &agent{}
	connection := sdk.NewAgentSideConnection(a, os.Stdout, os.Stdin)
	a.connection = connection
	<-connection.Done()
}

type agent struct {
	connection *sdk.AgentSideConnection
	mu         sync.Mutex
	sessions   map[sdk.SessionId]sdk.NewSessionRequest
}

func (*agent) Initialize(_ context.Context, request sdk.InitializeRequest) (sdk.InitializeResponse, error) {
	if request.ProtocolVersion != sdk.ProtocolVersionNumber {
		return sdk.InitializeResponse{}, fmt.Errorf("unsupported protocol version %d", request.ProtocolVersion)
	}
	if request.ClientCapabilities.Fs.ReadTextFile || request.ClientCapabilities.Fs.WriteTextFile || request.ClientCapabilities.Terminal {
		return sdk.InitializeResponse{}, errors.New("client must not advertise unsupported capabilities")
	}
	version := sdk.ProtocolVersion(sdk.ProtocolVersionNumber)
	if os.Getenv("GATE_E2E_VERSION") == "2" {
		version = 2
	}
	return sdk.InitializeResponse{
		ProtocolVersion: version,
		AgentInfo: &sdk.Implementation{
			Name: "gate-e2e-agent", Version: "test",
		},
		AgentCapabilities: sdk.AgentCapabilities{
			PromptCapabilities: sdk.PromptCapabilities{Image: true, Audio: true},
			McpCapabilities:    sdk.McpCapabilities{Http: true, Sse: true},
		},
	}, nil
}

func (a *agent) NewSession(_ context.Context, request sdk.NewSessionRequest) (sdk.NewSessionResponse, error) {
	if request.Cwd == "" {
		return sdk.NewSessionResponse{}, errors.New("missing cwd")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.sessions == nil {
		a.sessions = make(map[sdk.SessionId]sdk.NewSessionRequest)
	}
	id := sdk.SessionId("e2e-session")
	if len(a.sessions) != 0 {
		id = sdk.SessionId(fmt.Sprintf("e2e-session-%d", len(a.sessions)+1))
	}
	a.sessions[id] = request
	return sdk.NewSessionResponse{SessionId: id}, nil
}

func (a *agent) Prompt(ctx context.Context, request sdk.PromptRequest) (sdk.PromptResponse, error) {
	if handled, response, err := a.specPrompt(ctx, request); handled {
		return response, err
	}
	message := sdk.UpdateAgentMessageText("Researching.")
	message.AgentMessageChunk.MessageId = sdk.Ptr("00000000-0000-4000-8000-000000000001")
	if err := a.sendUpdate(ctx, request.SessionId, message); err != nil {
		return sdk.PromptResponse{}, err
	}
	thought := sdk.UpdateAgentThoughtText("Inspect the failing package first.")
	thought.AgentThoughtChunk.MessageId = sdk.Ptr("00000000-0000-4000-8000-000000000002")
	if err := a.sendUpdate(ctx, request.SessionId, thought); err != nil {
		return sdk.PromptResponse{}, err
	}
	tool := sdk.StartToolCall(
		"call-e2e",
		"Run tests",
		sdk.WithStartKind(sdk.ToolKindExecute),
		sdk.WithStartStatus(sdk.ToolCallStatusPending),
		sdk.WithStartLocations([]sdk.ToolCallLocation{{Path: "/workspace/project"}}),
		sdk.WithStartRawInput(map[string]any{"credential": "must-not-cross-adapter"}),
	)
	if err := a.sendUpdate(ctx, request.SessionId, tool); err != nil {
		return sdk.PromptResponse{}, err
	}

	permission, err := a.connection.RequestPermission(ctx, sdk.RequestPermissionRequest{
		SessionId: request.SessionId,
		ToolCall: sdk.ToolCallUpdate{
			ToolCallId: "call-e2e",
			Title:      sdk.Ptr("Run tests"),
			Kind:       sdk.Ptr(sdk.ToolKindExecute),
			Status:     sdk.Ptr(sdk.ToolCallStatusPending),
			RawInput:   map[string]any{"credential": "must-not-cross-adapter"},
		},
		Options: []sdk.PermissionOption{
			{OptionId: "allow-once", Name: "Allow once", Kind: sdk.PermissionOptionKindAllowOnce},
			{OptionId: "reject", Name: "Reject", Kind: sdk.PermissionOptionKindRejectOnce},
		},
	})
	if ctx.Err() != nil || (err == nil && permission.Outcome.Cancelled != nil) {
		return sdk.PromptResponse{StopReason: sdk.StopReasonCancelled}, nil
	}
	if err != nil {
		return sdk.PromptResponse{}, err
	}
	if permission.Outcome.Selected == nil || permission.Outcome.Selected.OptionId != "allow-once" {
		return sdk.PromptResponse{StopReason: sdk.StopReasonRefusal}, nil
	}
	if err := a.sendUpdate(ctx, request.SessionId, sdk.UpdateToolCall(
		"call-e2e",
		sdk.WithUpdateStatus(sdk.ToolCallStatusCompleted),
		sdk.WithUpdateRawOutput(map[string]any{"secret": "must-not-cross-adapter"}),
	)); err != nil {
		return sdk.PromptResponse{}, err
	}
	if err := a.sendUpdate(ctx, request.SessionId, sdk.UpdateAgentMessageText("Research complete.")); err != nil {
		return sdk.PromptResponse{}, err
	}
	return sdk.PromptResponse{StopReason: sdk.StopReasonEndTurn}, nil
}

func (a *agent) sendUpdate(ctx context.Context, sessionID sdk.SessionId, update sdk.SessionUpdate) error {
	return a.connection.SessionUpdate(ctx, sdk.SessionNotification{SessionId: sessionID, Update: update})
}

func (*agent) Authenticate(context.Context, sdk.AuthenticateRequest) (sdk.AuthenticateResponse, error) {
	return sdk.AuthenticateResponse{}, nil
}
func (*agent) Logout(context.Context, sdk.LogoutRequest) (sdk.LogoutResponse, error) {
	return sdk.LogoutResponse{}, nil
}
func (*agent) Cancel(context.Context, sdk.CancelNotification) error { return nil }
func (*agent) CloseSession(context.Context, sdk.CloseSessionRequest) (sdk.CloseSessionResponse, error) {
	return sdk.CloseSessionResponse{}, nil
}
func (*agent) ListSessions(context.Context, sdk.ListSessionsRequest) (sdk.ListSessionsResponse, error) {
	return sdk.ListSessionsResponse{}, nil
}
func (*agent) ResumeSession(context.Context, sdk.ResumeSessionRequest) (sdk.ResumeSessionResponse, error) {
	return sdk.ResumeSessionResponse{}, nil
}
func (*agent) SetSessionConfigOption(context.Context, sdk.SetSessionConfigOptionRequest) (sdk.SetSessionConfigOptionResponse, error) {
	return sdk.SetSessionConfigOptionResponse{}, nil
}
func (*agent) SetSessionMode(context.Context, sdk.SetSessionModeRequest) (sdk.SetSessionModeResponse, error) {
	return sdk.SetSessionModeResponse{}, nil
}

var _ sdk.Agent = (*agent)(nil)

// specPrompt provides deterministic ACP peer behavior, without a model provider.
func (a *agent) specPrompt(ctx context.Context, req sdk.PromptRequest) (bool, sdk.PromptResponse, error) {
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
