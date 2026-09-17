package v1_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/anra-studio/gate/internal/acp"
	acpv1 "github.com/anra-studio/gate/internal/acp/v1"
)

type duplexCloser struct {
	once    sync.Once
	closers []io.Closer
}

func (c *duplexCloser) Close() error {
	c.once.Do(func() {
		for _, closer := range c.closers {
			_ = closer.Close()
		}
	})
	return nil
}

type peer struct {
	decode *json.Decoder
	encode *json.Encoder
}

type message struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	Result  json.RawMessage `json:"result"`
	Error   json.RawMessage `json:"error"`
}

func newPair(t *testing.T) (*acpv1.Client, *peer) {
	t.Helper()
	clientIn, agentOut := io.Pipe()
	agentIn, clientOut := io.Pipe()
	closer := &duplexCloser{closers: []io.Closer{clientIn, agentOut, agentIn, clientOut}}
	client := acpv1.New(clientIn, clientOut, closer)
	t.Cleanup(func() { _ = client.Close() })
	return client, &peer{decode: json.NewDecoder(agentIn), encode: json.NewEncoder(agentOut)}
}

func (p *peer) receive() (message, error) {
	var got message
	return got, p.decode.Decode(&got)
}

func (p *peer) send(value any) error { return p.encode.Encode(value) }

func response(id json.RawMessage, result any) any {
	return struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  any             `json:"result"`
	}{"2.0", id, result}
}

type recordingHandler struct {
	mu         sync.Mutex
	updates    []acp.Update
	permission func(context.Context, acp.PermissionRequest) (acp.PermissionOutcome, error)
}

func (h *recordingHandler) OnUpdate(_ context.Context, update acp.Update) {
	h.mu.Lock()
	h.updates = append(h.updates, update)
	h.mu.Unlock()
}

func (h *recordingHandler) RequestPermission(ctx context.Context, request acp.PermissionRequest) (acp.PermissionOutcome, error) {
	return h.permission(ctx, request)
}

func TestClientLifecycleAndBidirectionalEvents(t *testing.T) {
	client, agent := newPair(t)
	agentErr := make(chan error, 1)
	go func() {
		request, err := agent.receive()
		if err != nil {
			agentErr <- err
			return
		}
		if request.Method != "initialize" {
			agentErr <- fmt.Errorf("first method = %q", request.Method)
			return
		}
		var initParams struct {
			ProtocolVersion int `json:"protocolVersion"`
		}
		if err := json.Unmarshal(request.Params, &initParams); err != nil || initParams.ProtocolVersion != 1 {
			agentErr <- fmt.Errorf("bad initialize params: %s", request.Params)
			return
		}
		if err := agent.send(response(request.ID, map[string]any{
			"protocolVersion": 1,
			"agentInfo":       map[string]any{"name": "fake", "version": "1.2.3"},
			"agentCapabilities": map[string]any{
				"promptCapabilities": map[string]any{"image": true},
				"mcpCapabilities":    map[string]any{"http": true},
			},
		})); err != nil {
			agentErr <- err
			return
		}

		request, err = agent.receive()
		if err != nil || request.Method != "session/new" {
			agentErr <- fmt.Errorf("session/new: method=%q err=%v", request.Method, err)
			return
		}
		var sessionParams struct {
			MCPServers []struct {
				Env []struct {
					Name  string `json:"name"`
					Value string `json:"value"`
				} `json:"env"`
			} `json:"mcpServers"`
		}
		if err := json.Unmarshal(request.Params, &sessionParams); err != nil ||
			len(sessionParams.MCPServers) != 1 || len(sessionParams.MCPServers[0].Env) != 1 ||
			sessionParams.MCPServers[0].Env[0].Name != "TOKEN" {
			agentErr <- fmt.Errorf("bad session params: %s", request.Params)
			return
		}
		if err := agent.send(response(request.ID, map[string]any{"sessionId": "session-1"})); err != nil {
			agentErr <- err
			return
		}

		request, err = agent.receive()
		if err != nil || request.Method != "session/prompt" {
			agentErr <- fmt.Errorf("session/prompt: method=%q err=%v", request.Method, err)
			return
		}
		promptID := request.ID
		messages := []any{
			map[string]any{"jsonrpc": "2.0", "method": "session/update", "params": map[string]any{
				"sessionId": "session-1", "update": map[string]any{
					"sessionUpdate": "agent_message_chunk", "messageId": "message-1",
					"content": map[string]any{"type": "text", "text": "answer"},
				},
			}},
			map[string]any{"jsonrpc": "2.0", "method": "session/update", "params": map[string]any{
				"sessionId": "session-1", "update": map[string]any{
					"sessionUpdate": "agent_thought_chunk", "messageId": "thought-1",
					"content": map[string]any{"type": "text", "text": "inspect first"},
				},
			}},
			map[string]any{"jsonrpc": "2.0", "method": "session/update", "params": map[string]any{
				"sessionId": "session-1", "update": map[string]any{
					"sessionUpdate": "tool_call", "toolCallId": "call-1", "title": "Run tests",
					"kind": "execute", "status": "in_progress", "rawInput": map[string]any{"token": "secret"},
					"locations": []any{map[string]any{"path": "/workspace/main.go", "line": 7}},
				},
			}},
		}
		for _, event := range messages {
			if err := agent.send(event); err != nil {
				agentErr <- err
				return
			}
		}
		if err := agent.send(map[string]any{
			"jsonrpc": "2.0", "id": "permission-rpc", "method": "session/request_permission",
			"params": map[string]any{
				"sessionId": "session-1",
				"toolCall":  map[string]any{"toolCallId": "call-1", "title": "Run tests", "kind": "execute"},
				"options": []any{
					map[string]any{"optionId": "allow-once", "name": "Allow once", "kind": "allow_once"},
					map[string]any{"optionId": "reject", "name": "Reject", "kind": "reject_once"},
				},
			},
		}); err != nil {
			agentErr <- err
			return
		}
		permissionResponse, err := agent.receive()
		if err != nil {
			agentErr <- err
			return
		}
		var selected struct {
			Outcome struct {
				Outcome  string `json:"outcome"`
				OptionID string `json:"optionId"`
			} `json:"outcome"`
		}
		if err := json.Unmarshal(permissionResponse.Result, &selected); err != nil || selected.Outcome.OptionID != "allow-once" {
			agentErr <- fmt.Errorf("bad permission response: %s", permissionResponse.Result)
			return
		}
		if err := agent.send(response(promptID, map[string]any{"stopReason": "end_turn"})); err != nil {
			agentErr <- err
			return
		}
		agentErr <- nil
	}()

	initialized, err := client.Initialize(t.Context(), acp.InitializeRequest{
		ClientInfo: acp.ClientInfo{Name: "gate", Version: "dev"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if initialized.ProtocolVersion != 1 || initialized.AgentInfo.Name != "fake" || !initialized.Capabilities.PromptImage {
		t.Fatalf("unexpected initialize result: %#v", initialized)
	}
	session, err := client.NewSession(t.Context(), acp.NewSessionRequest{
		CWD: "/workspace",
		MCPServers: []acp.MCPServer{{
			Name: "tools", Command: "/bin/tools", Env: []acp.EnvVariable{{Name: "TOKEN", Value: "secret"}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	handler := &recordingHandler{permission: func(_ context.Context, request acp.PermissionRequest) (acp.PermissionOutcome, error) {
		if request.SessionID != session.ID || request.ToolCall.ID != "call-1" || len(request.Options) != 2 {
			return acp.PermissionOutcome{}, fmt.Errorf("unexpected permission: %#v", request)
		}
		return acp.SelectPermission("allow-once"), nil
	}}
	reason, err := client.Prompt(t.Context(), acp.PromptRequest{
		SessionID: session.ID, Content: []acp.Content{acp.Text("research this")},
	}, handler)
	if err != nil || reason != acp.StopEndTurn {
		t.Fatalf("Prompt() = %q, %v", reason, err)
	}
	if err := <-agentErr; err != nil {
		t.Fatal(err)
	}
	handler.mu.Lock()
	defer handler.mu.Unlock()
	if len(handler.updates) != 3 {
		t.Fatalf("update count = %d, want 3", len(handler.updates))
	}
	if handler.updates[0].Content.Text != "answer" || handler.updates[1].MessageID != "thought-1" {
		t.Fatalf("unexpected content updates: %#v", handler.updates)
	}
	tool := handler.updates[2].ToolCall
	if tool.ID != "call-1" || len(tool.Locations) != 1 || tool.Locations[0].Line == nil || *tool.Locations[0].Line != 7 {
		t.Fatalf("unexpected tool update: %#v", tool)
	}
}

func TestInitializeRejectsNonV1(t *testing.T) {
	client, agent := newPair(t)
	go func() {
		request, _ := agent.receive()
		_ = agent.send(response(request.ID, map[string]any{"protocolVersion": 2}))
	}()
	_, err := client.Initialize(t.Context(), acp.InitializeRequest{})
	var protocolErr *acp.ProtocolError
	if !errors.As(err, &protocolErr) {
		t.Fatalf("Initialize() error = %v, want ProtocolError", err)
	}
	if _, err := client.NewSession(t.Context(), acp.NewSessionRequest{CWD: "/workspace"}); !errors.Is(err, acp.ErrClosed) {
		t.Fatalf("call after version mismatch = %v, want ErrClosed", err)
	}
}

func TestCancelResolvesPendingPermission(t *testing.T) {
	client, agent := newPair(t)
	requestStarted := make(chan struct{})
	agentErr := make(chan error, 1)
	go func() {
		prompt, err := agent.receive()
		if err != nil {
			agentErr <- err
			return
		}
		if err := agent.send(map[string]any{
			"jsonrpc": "2.0", "id": 77, "method": "session/request_permission",
			"params": map[string]any{
				"sessionId": "session-1", "toolCall": map[string]any{"toolCallId": "call-1"},
				"options": []any{map[string]any{"optionId": "allow", "name": "Allow", "kind": "allow_once"}},
			},
		}); err != nil {
			agentErr <- err
			return
		}
		seenCancel, seenPermission := false, false
		for !seenCancel || !seenPermission {
			message, err := agent.receive()
			if err != nil {
				agentErr <- err
				return
			}
			if message.Method == "session/cancel" {
				seenCancel = true
			}
			if string(message.ID) == "77" {
				var result struct {
					Outcome struct {
						Outcome string `json:"outcome"`
					} `json:"outcome"`
				}
				if err := json.Unmarshal(message.Result, &result); err != nil || result.Outcome.Outcome != "cancelled" {
					agentErr <- fmt.Errorf("permission was not cancelled: %s", message.Result)
					return
				}
				seenPermission = true
			}
		}
		if err := agent.send(response(prompt.ID, map[string]any{"stopReason": "cancelled"})); err != nil {
			agentErr <- err
			return
		}
		agentErr <- nil
	}()

	handler := &recordingHandler{permission: func(ctx context.Context, _ acp.PermissionRequest) (acp.PermissionOutcome, error) {
		close(requestStarted)
		<-ctx.Done()
		return acp.PermissionOutcome{}, ctx.Err()
	}}
	promptResult := make(chan error, 1)
	go func() {
		reason, err := client.Prompt(context.Background(), acp.PromptRequest{
			SessionID: "session-1", Content: []acp.Content{acp.Text("run")},
		}, handler)
		if err == nil && reason != acp.StopCancelled {
			err = fmt.Errorf("stop reason = %q", reason)
		}
		promptResult <- err
	}()

	select {
	case <-requestStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("permission handler did not start")
	}
	if err := client.Cancel(t.Context(), "session-1"); err != nil {
		t.Fatal(err)
	}
	if err := <-promptResult; err != nil {
		t.Fatal(err)
	}
	if err := <-agentErr; err != nil {
		t.Fatal(err)
	}
}

func TestNewSessionRejectsRelativePaths(t *testing.T) {
	client, _ := newPair(t)
	_, err := client.NewSession(t.Context(), acp.NewSessionRequest{CWD: "relative"})
	var protocolErr *acp.ProtocolError
	if !errors.As(err, &protocolErr) {
		t.Fatalf("NewSession() error = %v, want ProtocolError", err)
	}
}

func TestMissingToolCallIDIsProtocolError(t *testing.T) {
	client, agent := newPair(t)
	go func() {
		prompt, _ := agent.receive()
		_ = agent.send(map[string]any{
			"jsonrpc": "2.0", "method": "session/update", "params": map[string]any{
				"sessionId": "session-1", "update": map[string]any{
					"sessionUpdate": "tool_call", "title": "missing identity",
				},
			},
		})
		_, _ = agent.receive() // session/cancel emitted by the SDK after adapter validation fails.
		_ = agent.send(response(prompt.ID, map[string]any{"stopReason": "cancelled"}))
	}()
	handler := &recordingHandler{permission: func(context.Context, acp.PermissionRequest) (acp.PermissionOutcome, error) {
		return acp.CancelPermission(), nil
	}}
	_, err := client.Prompt(t.Context(), acp.PromptRequest{
		SessionID: "session-1", Content: []acp.Content{acp.Text("run")},
	}, handler)
	var protocolErr *acp.ProtocolError
	if !errors.As(err, &protocolErr) {
		t.Fatalf("Prompt() error = %v, want ProtocolError", err)
	}
}

func TestStartProcess(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	client, err := acpv1.StartProcess(t.Context(), acpv1.ProcessConfig{
		Command: executable,
		Args:    []string{"-test.run=^TestACPHelperProcess$"},
		Env:     []string{"GATE_ACP_HELPER=1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.Initialize(t.Context(), acp.InitializeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if result.ProtocolVersion != acp.ProtocolVersion || result.AgentInfo.Name != "helper" {
		t.Fatalf("unexpected process initialize result: %#v", result)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestACPHelperProcess(t *testing.T) {
	if os.Getenv("GATE_ACP_HELPER") != "1" {
		return
	}
	decoder := json.NewDecoder(os.Stdin)
	encoder := json.NewEncoder(os.Stdout)
	for {
		var request message
		if err := decoder.Decode(&request); err != nil {
			return
		}
		if request.Method == "initialize" {
			_ = encoder.Encode(response(request.ID, map[string]any{
				"protocolVersion": 1,
				"agentInfo":       map[string]any{"name": "helper", "version": "test"},
			}))
		}
	}
}
