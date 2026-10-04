package e2e_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	protocol "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2aclient/agentcard"
	"github.com/a2aproject/a2a-go/v2/a2aext"
	pb "github.com/a2aproject/a2a-go/v2/a2apb/v1"
	"github.com/a2aproject/a2a-go/v2/a2apb/v1/pbconv"
	a2apbconnect "github.com/anra-studio/gate/gen/a2a/a2apbconnect"
	gate "github.com/anra-studio/gate/internal/a2a"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestGateCommand builds and launches the actual main.go, then traverses HTTP,
// Connect v2, ACPExecutor, the v1 adapter and a separate ACP agent process.
func TestGateCommand(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "gate")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "..")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build gate: %v\n%s", err, output)
	}

	t.Run("cli-validation", func(t *testing.T) {
		for _, args := range [][]string{
			{}, {"-public-url", "ftp://example.com", "--", "unused"},
			{"-public-url", "https://example.com/path", "--", "unused"},
			{"-public-url", "https://user:password@example.com", "--", "unused"},
			{"-cwd", filepath.Join(t.TempDir(), "missing"), "--", "unused"},
		} {
			if err := exec.CommandContext(t.Context(), binary, args...).Run(); err == nil {
				t.Fatalf("accepted arguments: %v", args)
			}
		}
		if err := exec.CommandContext(t.Context(), binary, "-h").Run(); err != nil {
			t.Fatalf("help: %v", err)
		}
	})
	helper, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "-listen", "127.0.0.1:0", "-cwd", mustWorkingDirectory(t), "--", helper, "-test.run=^TestACPAgentProcess$")
	cmd.Env = append(os.Environ(), acpAgentHelperEnv+"=1")
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = cmd.Process.Signal(os.Interrupt)
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("gate shutdown: %v", err)
			}
		case <-time.After(15 * time.Second):
			_ = cmd.Process.Kill()
			<-done
			t.Error("gate did not shut down")
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
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	card, err := agentcard.DefaultResolver.Resolve(ctx, origin)
	if err != nil {
		t.Fatal(err)
	}
	if card.Name != "Gate" || !card.Capabilities.Streaming || len(card.SupportedInterfaces) != 2 {
		t.Fatalf("card = %#v", card)
	}
	for _, iface := range card.SupportedInterfaces {
		if iface.URL != origin {
			t.Fatalf("advertised URL = %q, want %q", iface.URL, origin)
		}
	}
	if len(card.Capabilities.Extensions) != 2 {
		t.Fatalf("extensions = %#v", card.Capabilities.Extensions)
	}
	client, err := a2aclient.NewFromCard(ctx, card)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Destroy() })

	t.Run("rest-stop-reasons", func(t *testing.T) {
		for _, tc := range []struct {
			reason string
			state  protocol.TaskState
		}{
			{"end_turn", protocol.TaskStateCompleted}, {"max_tokens", protocol.TaskStateFailed}, {"max_turn_requests", protocol.TaskStateFailed}, {"refusal", protocol.TaskStateRejected}, {"cancelled", protocol.TaskStateCanceled},
		} {
			t.Run(tc.reason, func(t *testing.T) {
				task := sendTask(t, ctx, client, "stop:"+tc.reason, "")
				if task.Status.State != tc.state {
					t.Fatalf("state = %s, want %s", task.Status.State, tc.state)
				}
			})
		}
	})
	t.Run("session-reuse", func(t *testing.T) {
		first := sendTask(t, ctx, client, "session", "")
		next := sendTask(t, ctx, client, "session", first.ContextID)
		if first.ID == next.ID || first.ContextID != next.ContextID || artifactText(first.Artifacts) != artifactText(next.Artifacts) {
			t.Fatalf("session not reused: %#v / %#v", first, next)
		}
		saved, err := client.GetTask(ctx, &protocol.GetTaskRequest{ID: next.ID})
		if err != nil {
			t.Fatal(err)
		}
		if saved.Status.State != protocol.TaskStateCompleted || artifactText(saved.Artifacts) != artifactText(next.Artifacts) {
			t.Fatalf("stored task = %#v", saved)
		}
	})
	t.Run("permission-fails-closed", func(t *testing.T) {
		task := sendTask(t, ctx, client, "permission", "")
		if task.Status.State != protocol.TaskStateCanceled || artifactText(task.Artifacts) != "Researching." {
			t.Fatalf("permission result = %#v", task)
		}
	})
	t.Run("rpc-error-is-sanitized", func(t *testing.T) {
		task := sendTask(t, ctx, client, "rpc-error", "")
		if task.Status.State != protocol.TaskStateFailed {
			t.Fatalf("state = %s", task.Status.State)
		}
		data, _ := json.Marshal(task)
		if strings.Contains(string(data), "must-not-cross-adapter") {
			t.Fatal("RPC details leaked")
		}
	})
	t.Run("stream-extension-negotiation", func(t *testing.T) {
		for _, uris := range [][]string{nil, {gate.ThoughtExtensionURI}, {gate.ToolCallExtensionURI}, {gate.ThoughtExtensionURI, gate.ToolCallExtensionURI}} {
			t.Run(strings.Join(uris, ","), func(t *testing.T) {
				activated, err := a2aclient.NewFromCard(ctx, card, a2aclient.WithCallInterceptors(a2aext.NewActivator(uris...)))
				if err != nil {
					t.Fatal(err)
				}
				defer activated.Destroy()
				var extensionCount, artifactCount int
				var lastSequence uint64
				var terminal protocol.TaskState
				for event, err := range activated.SendStreamingMessage(ctx, messageRequest("observations", "")) {
					if err != nil {
						t.Fatal(err)
					}
					wire, _ := json.Marshal(event)
					if strings.Contains(string(wire), "must-not-cross-adapter") {
						t.Fatal("raw tool data leaked")
					}
					switch e := event.(type) {
					case *protocol.TaskArtifactUpdateEvent:
						artifactCount++
					case *protocol.TaskStatusUpdateEvent:
						terminal = e.Status.State
						if e.Status.Message != nil {
							extensionCount++
							if e.Status.State != protocol.TaskStateWorking || len(e.Status.Message.Extensions) != 1 {
								t.Fatalf("extension event = %#v", e)
							}
							uri := e.Status.Message.Extensions[0]
							enabled := false
							for _, want := range uris {
								enabled = enabled || uri == want
							}
							if !enabled {
								t.Fatalf("unrequested extension: %s", uri)
							}
							part, ok := e.Status.Message.Parts[0].Content.(protocol.Data)
							if !ok {
								t.Fatalf("extension content = %T", e.Status.Message.Parts[0].Content)
							}
							data, ok := part.Value.(map[string]any)
							if !ok {
								t.Fatalf("extension data = %T", part.Value)
							}
							sequence, err := strconv.ParseUint(fmt.Sprint(data["sequence"]), 10, 64)
							if err != nil || sequence <= lastSequence {
								t.Fatalf("sequence = %#v", data["sequence"])
							}
							lastSequence = sequence
							if uri == gate.ThoughtExtensionURI && data["thoughtId"] == "" {
								t.Fatal("empty thought ID")
							}
							if uri == gate.ToolCallExtensionURI && data["toolCallId"] != "spec-tool" {
								t.Fatalf("tool ID = %v", data["toolCallId"])
							}
						}
					}
				}
				want := 0
				for _, uri := range uris {
					if uri == gate.ThoughtExtensionURI {
						want++
					} else {
						want += 2
					}
				}
				if extensionCount != want || artifactCount != 2 || terminal != protocol.TaskStateCompleted {
					t.Fatalf("extensions=%d artifacts=%d terminal=%s", extensionCount, artifactCount, terminal)
				}
			})
		}
	})
	t.Run("cancel-active-task", func(t *testing.T) {
		var id protocol.TaskID
		var contextID string
		for event, err := range client.SendStreamingMessage(ctx, messageRequest("wait", "")) {
			if err != nil {
				t.Fatal(err)
			}
			switch e := event.(type) {
			case *protocol.Task:
				id = e.ID
				contextID = e.ContextID
			case *protocol.TaskArtifactUpdateEvent:
				canceled, err := client.CancelTask(ctx, &protocol.CancelTaskRequest{ID: id})
				if err != nil {
					t.Fatal(err)
				}
				if canceled.Status.State != protocol.TaskStateCanceled {
					t.Fatalf("cancel state = %s", canceled.Status.State)
				}
				goto finished
			}
		}
		t.Fatal("waiting artifact not received")
	finished:
		if next := sendTask(t, ctx, client, "session", contextID); next.Status.State != protocol.TaskStateCompleted {
			t.Fatalf("session did not survive cancel: %#v", next)
		}
	})

	t.Run("stream-disconnect-preserves-turn", func(t *testing.T) {
		var id protocol.TaskID
		for event, err := range client.SendStreamingMessage(ctx, messageRequest("wait", "")) {
			if err != nil {
				t.Fatal(err)
			}
			if task, ok := event.(*protocol.Task); ok {
				id = task.ID
			}
			if _, ok := event.(*protocol.TaskArtifactUpdateEvent); ok {
				break
			}
		}
		if id == "" {
			t.Fatal("missing task ID")
		}
		saved, err := client.GetTask(ctx, &protocol.GetTaskRequest{ID: id})
		if err != nil {
			t.Fatal(err)
		}
		if saved.Status.State != protocol.TaskStateWorking {
			t.Fatalf("task stopped on disconnect: %s", saved.Status.State)
		}
		task, err := client.CancelTask(ctx, &protocol.CancelTaskRequest{ID: id})
		if err != nil {
			t.Fatal(err)
		}
		if task.Status.State != protocol.TaskStateCanceled {
			t.Fatalf("cancel = %s", task.Status.State)
		}
	})
	t.Run("connect-and-grpc-web", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			options []connecthttp.Option
		}{{name: "connect"}, {name: "grpc-web", options: []connecthttp.Option{connecthttp.WithGRPCWeb()}}} {
			t.Run(tc.name, func(t *testing.T) {
				req, err := pbconv.ToProtoSendMessageRequest(messageRequest("observations", ""))
				if err != nil {
					t.Fatal(err)
				}
				rpc := connect.NewClient(connecthttp.NewTransport(http.DefaultClient, origin, tc.options...))
				result, err := a2apbconnect.NewA2AServiceClient(rpc).SendMessage(ctx, req)
				if err != nil {
					t.Fatal(err)
				}
				converted, err := pbconv.FromProtoSendMessageResponse(result)
				if err != nil {
					t.Fatal(err)
				}
				task := converted.(*protocol.Task)
				if task.Status.State != protocol.TaskStateCompleted || artifactText(task.Artifacts) != "research complete" {
					t.Fatalf("task = %#v", task)
				}
			})
		}
	})
	t.Run("grpc-h2c", func(t *testing.T) {
		conn, err := grpc.NewClient(strings.TrimPrefix(origin, "http://"), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		req, err := pbconv.ToProtoSendMessageRequest(messageRequest("observations", ""))
		if err != nil {
			t.Fatal(err)
		}
		result, err := pb.NewA2AServiceClient(conn).SendMessage(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		converted, err := pbconv.FromProtoSendMessageResponse(result)
		if err != nil {
			t.Fatal(err)
		}
		if task := converted.(*protocol.Task); task.Status.State != protocol.TaskStateCompleted {
			t.Fatalf("state = %s", task.Status.State)
		}
	})
	t.Run("missing-task", func(t *testing.T) {
		_, err := client.GetTask(ctx, &protocol.GetTaskRequest{ID: "missing"})
		if !errors.Is(err, protocol.ErrTaskNotFound) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("shutdown-with-pending-turn", func(t *testing.T) {
		// Leave an ACP prompt running. The command's cleanup sends an
		// interrupt and asserts that ServeProxy shuts down successfully.
		for event, err := range client.SendStreamingMessage(ctx, messageRequest("wait", "")) {
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := event.(*protocol.TaskArtifactUpdateEvent); ok {
				return
			}
		}
		t.Fatal("waiting turn did not start")
	})
}

func messageRequest(text, contextID string) *protocol.SendMessageRequest {
	message := protocol.NewMessage(protocol.MessageRoleUser, protocol.NewTextPart(text))
	message.ContextID = contextID
	return &protocol.SendMessageRequest{Message: message}
}
func sendTask(t *testing.T, ctx context.Context, client *a2aclient.Client, text, contextID string) *protocol.Task {
	t.Helper()
	result, err := client.SendMessage(ctx, messageRequest(text, contextID))
	if err != nil {
		t.Fatal(err)
	}
	task, ok := result.(*protocol.Task)
	if !ok {
		t.Fatalf("result = %T", result)
	}
	return task
}
