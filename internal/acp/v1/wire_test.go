package v1_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/anra-studio/gate/internal/acp"
	acpv1 "github.com/anra-studio/gate/internal/acp/v1"
)

// A deliberately non-SDK peer can emit malformed wire data that an SDK agent
// would normally prevent. stdout is exclusively NDJSON in the child process.
func TestACPRawAgentProcess(t *testing.T) {
	if os.Getenv("GATE_E2E_RAW") != "1" {
		return
	}
	decoder := json.NewDecoder(os.Stdin)
	for {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if err := decoder.Decode(&req); err != nil {
			return
		}
		var result any
		switch req.Method {
		case "initialize":
			result = map[string]any{"protocolVersion": 1, "agentCapabilities": map[string]any{}, "agentInfo": map[string]any{"name": "raw-peer", "version": "test"}, "_meta": map[string]any{"future": "ignored"}}
		case "session/new":
			id := "raw-session"
			if os.Getenv("GATE_E2E_EMPTY_SESSION") == "1" {
				id = ""
			}
			result = map[string]any{"sessionId": id}
		case "session/cancel":
			continue
		case "session/prompt":
			var params struct {
				SessionID string `json:"sessionId"`
				Prompt    []struct {
					Text string `json:"text"`
				} `json:"prompt"`
			}
			if err := json.Unmarshal(req.Params, &params); err != nil {
				os.Exit(2)
			}
			text := params.Prompt[0].Text
			switch text {
			case "unknown-stop":
				result = map[string]any{"stopReason": "future_stop"}
			case "disconnect":
				os.Exit(0)
			case "missing-tool-id", "missing-update-id":
				kind := "tool_call"
				if text == "missing-update-id" {
					kind = "tool_call_update"
				}
				writeRaw(map[string]any{"jsonrpc": "2.0", "method": "session/update", "params": map[string]any{"sessionId": params.SessionID, "update": map[string]any{"sessionUpdate": kind, "title": "invalid tool"}}})
				// Give the adapter time to validate this notification before completing.
				var cancel struct {
					Method string `json:"method"`
				}
				for {
					if err := decoder.Decode(&cancel); err != nil {
						return
					}
					if cancel.Method == "session/cancel" {
						break
					}
					if cancel.Method != "$/cancel_request" {
						os.Exit(3)
					}
				}
				result = map[string]any{"stopReason": "cancelled"}
			case "batched":
				// Multiple notifications and the final response share one write. Updates
				// for another session must not contaminate this prompt's observations.
				var batch strings.Builder
				for i := 0; i < 10; i++ {
					sessionID := params.SessionID
					if i == 0 {
						sessionID = "unrelated-session"
					}
					data, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": "session/update", "params": map[string]any{"sessionId": sessionID, "update": map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": fmt.Sprint(i)}}}})
					batch.Write(data)
					batch.WriteByte('\n')
				}
				data, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": map[string]any{"stopReason": "end_turn"}})
				batch.Write(data)
				batch.WriteByte('\n')
				_, _ = os.Stdout.WriteString(batch.String())
				continue
			default:
				result = map[string]any{"stopReason": "end_turn"}
			}
		default:
			os.Exit(4)
		}
		writeRaw(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result})
	}
}
func writeRaw(value any) {
	data, err := json.Marshal(value)
	if err != nil {
		os.Exit(5)
	}
	data = append(data, '\n')
	// Deliberately split records across writes, including JSON tokens.
	middle := len(data) / 2
	_, _ = os.Stdout.Write(data[:middle])
	_, _ = os.Stdout.Write(data[middle:])
}

func rawACP(t *testing.T, env ...string) *acpv1.Client {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	client, err := acpv1.StartProcess(ctx, acpv1.ProcessConfig{Command: executable, Args: []string{"-test.run=^TestACPRawAgentProcess$"}, Env: append([]string{"GATE_E2E_RAW=1"}, env...)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if _, err := client.Initialize(ctx, acp.InitializeRequest{}); err != nil {
		t.Fatal(err)
	}
	return client
}
func TestACPWireProtocol(t *testing.T) {
	t.Run("empty-session-id", func(t *testing.T) {
		client := rawACP(t, "GATE_E2E_EMPTY_SESSION=1")
		_, err := client.NewSession(t.Context(), acp.NewSessionRequest{CWD: mustWorkingDirectory(t)})
		var protocol *acp.ProtocolError
		if !errors.As(err, &protocol) {
			t.Fatalf("error = %v", err)
		}
	})
	for _, name := range []string{"unknown-stop", "missing-tool-id", "missing-update-id", "disconnect", "batched"} {
		t.Run(name, func(t *testing.T) {
			client := rawACP(t)
			session, err := client.NewSession(t.Context(), acp.NewSessionRequest{CWD: mustWorkingDirectory(t)})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			h := rejectingHandler()
			reason, err := client.Prompt(ctx, acp.PromptRequest{SessionID: session.ID, Content: []acp.Content{acp.Text(name)}}, h)
			switch name {
			case "batched":
				if err != nil || reason != acp.StopEndTurn {
					t.Fatalf("result = %s, %v", reason, err)
				}
				updates := h.snapshot()
				if len(updates) != 9 {
					t.Fatalf("update count = %d", len(updates))
				}
				for i, update := range updates {
					if update.Content.Text != fmt.Sprint(i+1) {
						t.Fatalf("out of order update %d: %#v", i, update)
					}
				}
			case "disconnect":
				if err == nil || errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("disconnect error = %v", err)
				}
			default:
				var protocol *acp.ProtocolError
				if !errors.As(err, &protocol) {
					t.Fatalf("error = %v, want ProtocolError", err)
				}
			}
		})
	}
}

func TestACPProcessToolContent(t *testing.T) {
	client, id := initializedACP(t)
	h := rejectingHandler()
	if _, err := client.Prompt(t.Context(), acp.PromptRequest{SessionID: id, Content: []acp.Content{acp.Text("observations")}}, h); err != nil {
		t.Fatal(err)
	}
	updates := h.snapshot()
	if len(updates) != 5 {
		t.Fatalf("updates = %#v", updates)
	}
	content := updates[2].ToolCall.Content
	if len(content) != 3 || content[0].Type != "diff" || content[0].Path != "/workspace/file" || content[0].OldText != "old" || content[0].NewText != "new" || content[1].Type != "terminal" || content[1].Terminal != "terminal-1" || content[2].Content.Text != "output" {
		t.Fatalf("tool content = %#v", content)
	}
	if updates[3].ToolCall.ID != "spec-tool" || updates[3].ToolCall.Status != "completed" {
		t.Fatalf("tool update = %#v", updates[3])
	}
}
