package e2e_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"net/http"
	"slices"
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
	"github.com/cucumber/godog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Behavior scenarios start the production server in-process and access it over
// real transports. No Gate binary is built or launched for this suite.
func TestGateBehavior(t *testing.T) {
	suite := godog.TestSuite{
		Name: "Gate behavior",
		Options: &godog.Options{
			Format: "pretty", Paths: []string{"features/gate.feature"}, Strict: true,
			TestingT: t, DefaultContext: t.Context(), Concurrency: 1,
		},
		ScenarioInitializer: func(sc *godog.ScenarioContext) {
			b := newBehaviorScenario(sc)
			sc.Step(`^Gate is running with a local ACP agent$`, func() error {
				b.origin, b.stop = startGateServer(t, b.ctx)
				return b.discover()
			})
			sc.Step(`^a client discovers Gate$`, func() error { return b.discover() })
			sc.Step(`^the card advertises Gate, streaming, and both observation extensions$`, func() error {
				if b.card.Name != "Gate" || !b.card.Capabilities.Streaming || len(b.card.SupportedInterfaces) != 2 {
					return fmt.Errorf("unexpected card: %#v", b.card)
				}
				var uris []string
				for _, ext := range b.card.Capabilities.Extensions {
					uris = append(uris, ext.URI)
				}
				if len(uris) != 2 || !slices.Contains(uris, gate.ThoughtExtensionURI) || !slices.Contains(uris, gate.ToolCallExtensionURI) {
					return fmt.Errorf("advertised extensions: %v", uris)
				}
				return nil
			})
			sc.Step(`^every advertised interface points to the running service$`, func() error {
				for _, iface := range b.card.SupportedInterfaces {
					if iface.URL != b.origin {
						return fmt.Errorf("advertised %q, running at %q", iface.URL, b.origin)
					}
				}
				return nil
			})
			sc.Step(`^the agent stops with "([^"]*)"$`, func(reason string) error { return b.send("stop:"+reason, "") })
			sc.Step(`^the task state is "([^"]*)"$`, func(state string) error { return b.taskState(state) })
			sc.Step(`^a client sends two messages in the same conversation$`, func() error {
				if err := b.send("session", ""); err != nil {
					return err
				}
				b.first = b.task
				return b.send("session", b.first.ContextID)
			})
			sc.Step(`^the messages have distinct task IDs and the same ACP session$`, func() error {
				if b.first.ID == "" || b.task.ID == "" || b.first.ID == b.task.ID || b.first.ContextID == "" || b.first.ContextID != b.task.ContextID || artifactText(b.first.Artifacts) == "" || artifactText(b.first.Artifacts) != artifactText(b.task.Artifacts) {
					return fmt.Errorf("conversation not retained: %#v / %#v", b.first, b.task)
				}
				return nil
			})
			sc.Step(`^retrieving the latest task preserves its completed result$`, func() error {
				saved, err := b.client.GetTask(b.ctx, &protocol.GetTaskRequest{ID: b.task.ID})
				if err != nil {
					return err
				}
				if saved.ID != b.task.ID || saved.ContextID != b.task.ContextID || saved.Status.State != protocol.TaskStateCompleted || artifactText(saved.Artifacts) != artifactText(b.task.Artifacts) {
					return fmt.Errorf("stored task changed: %#v", saved)
				}
				return nil
			})
			sc.Step(`^the agent requests permission to run a tool$`, func() error { return b.send("permission", "") })
			sc.Step(`^the result contains only "([^"]*)"$`, func(text string) error {
				if got := artifactText(b.task.Artifacts); got != text {
					return fmt.Errorf("result = %q, want %q", got, text)
				}
				return nil
			})
			sc.Step(`^the agent returns an RPC error containing private details$`, func() error { return b.send("rpc-error", "") })
			sc.Step(`^private agent details are absent from the task$`, func() error { return noPrivateDetails(b.task) })
			sc.Step(`^the client requests "([^"]*)" observations$`, func(observations string) error {
				switch observations {
				case "none":
					b.uris = nil
				case "thoughts":
					b.uris = []string{gate.ThoughtExtensionURI}
				case "tools":
					b.uris = []string{gate.ToolCallExtensionURI}
				case "both":
					b.uris = []string{gate.ThoughtExtensionURI, gate.ToolCallExtensionURI}
				default:
					return fmt.Errorf("unknown observations %q", observations)
				}
				_ = b.client.Destroy()
				var err error
				b.client, err = a2aclient.NewFromCard(b.ctx, b.card, a2aclient.WithCallInterceptors(a2aext.NewActivator(b.uris...)))
				return err
			})
			sc.Step(`^the agent streams text, thoughts, and tool activity$`, func() error {
				for event, err := range b.client.SendStreamingMessage(b.ctx, messageRequest("observations", "")) {
					if err != nil {
						return err
					}
					b.events = append(b.events, event)
				}
				return nil
			})
			sc.Step(`^the client receives (\d+) thoughts and (\d+) tool observations$`, func(thoughts, tools int) error { return b.observationCounts(thoughts, tools) })
			sc.Step(`^observations have increasing sequence numbers and usable IDs$`, func() error { return b.observationIDs() })
			sc.Step(`^private agent details are absent from the stream$`, func() error { return noPrivateDetails(b.events) })

			sc.Step(`^the stream starts with a submitted task followed by working status$`, func() error {
				if len(b.events) < 2 {
					return errors.New("stream did not report task submission and working status")
				}
				task, ok := b.events[0].(*protocol.Task)
				if !ok || task.Status.State != protocol.TaskStateSubmitted || task.ID == "" || task.ContextID == "" {
					return fmt.Errorf("first event = %#v, want submitted task with identifiers", b.events[0])
				}
				working, ok := b.events[1].(*protocol.TaskStatusUpdateEvent)
				if !ok || working.Status.State != protocol.TaskStateWorking {
					return fmt.Errorf("second event = %#v, want working status", b.events[1])
				}
				return nil
			})
			sc.Step(`^result chunks append to the same artifact$`, func() error {
				var artifactID protocol.ArtifactID
				var chunks int
				for _, event := range b.events {
					chunk, ok := event.(*protocol.TaskArtifactUpdateEvent)
					if !ok {
						continue
					}
					if chunks == 0 {
						artifactID = chunk.Artifact.ID
						if artifactID == "" || chunk.Append {
							return fmt.Errorf("first chunk must create a named artifact: %#v", chunk)
						}
					} else if chunk.Artifact.ID != artifactID || !chunk.Append {
						return fmt.Errorf("chunk must append to artifact %q: %#v", artifactID, chunk)
					}
					chunks++
				}
				if chunks < 2 {
					return fmt.Errorf("received %d chunks, want incremental output", chunks)
				}
				return nil
			})
			sc.Step(`^the completed result is "([^"]*)" with no history or plans$`, func(text string) error { return b.streamResult(text) })
			sc.Step(`^the agent is working on a task$`, func() error { return b.startWorking() })
			sc.Step(`^the client cancels the task$`, func() error {
				var err error
				b.task, err = b.client.CancelTask(b.ctx, &protocol.CancelTaskRequest{ID: b.task.ID})
				return err
			})
			sc.Step(`^the same ACP session can complete another message$`, func() error {
				contextID := b.first.ContextID
				if err := b.send("session", contextID); err != nil {
					return err
				}
				if b.task.ContextID != contextID || artifactText(b.task.Artifacts) != artifactText(b.first.Artifacts) {
					return fmt.Errorf("ACP session changed after cancellation")
				}
				return b.taskState("completed")
			})
			sc.Step(`^the client disconnects the stream$`, func() { b.streamStop() })
			sc.Step(`^the saved task is still working$`, func() error {
				task, err := b.client.GetTask(b.ctx, &protocol.GetTaskRequest{ID: b.task.ID})
				if err != nil {
					return err
				}
				b.task = task
				return b.taskState("working")
			})
			sc.Step(`^a client sends a message over "([^"]*)"$`, func(transport string) error { return b.sendTransport(transport) })
			sc.Step(`^a client retrieves an unknown task$`, func() error {
				_, b.requestErr = b.client.GetTask(b.ctx, &protocol.GetTaskRequest{ID: "missing"})
				return nil
			})
			sc.Step(`^the service reports task not found$`, func() error {
				if !errors.Is(b.requestErr, protocol.ErrTaskNotFound) {
					return fmt.Errorf("missing-task error = %v", b.requestErr)
				}
				return nil
			})

		},
	}
	if suite.Run() != 0 {
		t.Fatal("Gate behavior scenarios failed")
	}
}

type gateBehavior struct {
	origin      string
	ctx         context.Context
	cancel      context.CancelFunc
	stop        func() error
	card        *protocol.AgentCard
	client      *a2aclient.Client
	task, first *protocol.Task
	uris        []string
	events      []protocol.Event
	requestErr  error
	streamStop  func()
}

func (b *gateBehavior) discover() error {
	card, err := agentcard.DefaultResolver.Resolve(b.ctx, b.origin)
	if err != nil {
		return err
	}
	b.card = card
	if b.client != nil {
		_ = b.client.Destroy()
	}
	b.client, err = a2aclient.NewFromCard(b.ctx, card)
	return err
}

func (b *gateBehavior) send(text, contextID string) error {
	result, err := b.client.SendMessage(b.ctx, messageRequest(text, contextID))
	if err != nil {
		return err
	}
	task, ok := result.(*protocol.Task)
	if !ok {
		return fmt.Errorf("result = %T, want task", result)
	}
	b.task = task
	return nil
}

func (b *gateBehavior) taskState(state string) error {
	if b.task == nil {
		return errors.New("no task received")
	}
	want, ok := map[string]protocol.TaskState{
		"completed": protocol.TaskStateCompleted, "failed": protocol.TaskStateFailed,
		"rejected": protocol.TaskStateRejected, "canceled": protocol.TaskStateCanceled,
		"working": protocol.TaskStateWorking,
	}[state]
	if !ok {
		return fmt.Errorf("unknown task state %q", state)
	}
	if b.task.Status.State != want {
		return fmt.Errorf("task state = %s, want %s", b.task.Status.State, state)
	}
	return nil
}

func noPrivateDetails(value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if strings.Contains(string(data), "must-not-cross-adapter") {
		return errors.New("private agent details leaked")
	}
	return nil
}

func observation(event protocol.Event) (string, map[string]any, error) {
	status, ok := event.(*protocol.TaskStatusUpdateEvent)
	if !ok || status.Status.Message == nil {
		return "", nil, nil
	}
	message := status.Status.Message
	if status.Status.State != protocol.TaskStateWorking || len(message.Extensions) != 1 || len(message.Parts) != 1 {
		return "", nil, fmt.Errorf("invalid observation: %#v", status)
	}
	part, ok := message.Parts[0].Content.(protocol.Data)
	if !ok {
		return "", nil, fmt.Errorf("observation content = %T", message.Parts[0].Content)
	}
	data, ok := part.Value.(map[string]any)
	if !ok {
		return "", nil, fmt.Errorf("observation data = %T", part.Value)
	}
	return message.Extensions[0], data, nil
}

func (b *gateBehavior) observationCounts(wantThoughts, wantTools int) error {
	var thoughts, tools int
	for _, event := range b.events {
		uri, _, err := observation(event)
		if err != nil {
			return err
		}
		if uri == "" {
			continue
		}
		if !slices.Contains(b.uris, uri) {
			return fmt.Errorf("unrequested observation: %s", uri)
		}
		switch uri {
		case gate.ThoughtExtensionURI:
			thoughts++
		case gate.ToolCallExtensionURI:
			tools++
		default:
			return fmt.Errorf("unknown observation: %s", uri)
		}
	}
	if thoughts != wantThoughts || tools != wantTools {
		return fmt.Errorf("thoughts=%d tools=%d, want %d/%d", thoughts, tools, wantThoughts, wantTools)
	}
	return nil
}

func (b *gateBehavior) observationIDs() error {
	var last uint64
	for _, event := range b.events {
		uri, data, err := observation(event)
		if err != nil {
			return err
		}
		if uri == "" {
			continue
		}
		sequence, err := strconv.ParseUint(fmt.Sprint(data["sequence"]), 10, 64)
		if err != nil || sequence <= last {
			return fmt.Errorf("sequence = %v after %d", data["sequence"], last)
		}
		last = sequence
		if uri == gate.ThoughtExtensionURI {
			id, ok := data["thoughtId"].(string)
			if !ok || id == "" {
				return errors.New("missing thought ID")
			}
		} else if data["toolCallId"] != "spec-tool" {
			return fmt.Errorf("tool ID = %v", data["toolCallId"])
		}
	}
	return nil
}

func (b *gateBehavior) streamResult(want string) error {
	var text string
	var chunks int
	var terminal protocol.TaskState
	for _, event := range b.events {
		switch e := event.(type) {
		case *protocol.TaskArtifactUpdateEvent:
			text += artifactText([]*protocol.Artifact{e.Artifact})
			chunks++
		case *protocol.TaskStatusUpdateEvent:
			terminal = e.Status.State
		}
	}
	if text != want || chunks != 2 || terminal != protocol.TaskStateCompleted {
		return fmt.Errorf("stream result = %q, chunks=%d, state=%s", text, chunks, terminal)
	}
	return nil
}

func (b *gateBehavior) startWorking() error {
	// Remember the ACP session identity before cancellation, so reuse is tested
	// against the actual session rather than just a successful follow-up request.
	if err := b.send("session", ""); err != nil {
		return err
	}
	b.first = b.task
	next, stop := iter.Pull2(b.client.SendStreamingMessage(b.ctx, messageRequest("wait", b.first.ContextID)))
	b.streamStop = stop
	for {
		event, err, ok := next()
		if !ok {
			break
		}
		if err != nil {
			return err
		}
		if task, ok := event.(*protocol.Task); ok {
			b.task = task
		}
		if _, ok := event.(*protocol.TaskArtifactUpdateEvent); ok {
			if b.task == nil || b.task.ID == "" || b.task.ID == b.first.ID {
				return errors.New("missing working task ID")
			}
			return nil
		}
	}

	return errors.New("agent never started working")
}

func (b *gateBehavior) sendTransport(transport string) error {
	if transport == "HTTP+JSON" {
		return b.send("observations", "")
	}
	req, err := pbconv.ToProtoSendMessageRequest(messageRequest("observations", ""))
	if err != nil {
		return err
	}
	var result *pb.SendMessageResponse
	switch transport {
	case "Connect", "gRPC-Web":
		var options []connecthttp.Option
		if transport == "gRPC-Web" {
			options = []connecthttp.Option{connecthttp.WithGRPCWeb()}
		}
		rpc := connect.NewClient(connecthttp.NewTransport(http.DefaultClient, b.origin, options...))
		result, err = a2apbconnect.NewA2AServiceClient(rpc).SendMessage(b.ctx, req)
	case "gRPC":
		conn, dialErr := grpc.NewClient(strings.TrimPrefix(b.origin, "http://"), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if dialErr != nil {
			return dialErr
		}
		defer conn.Close()
		result, err = pb.NewA2AServiceClient(conn).SendMessage(b.ctx, req)
	default:
		return fmt.Errorf("unknown transport %q", transport)
	}
	if err != nil {
		return err
	}
	converted, err := pbconv.FromProtoSendMessageResponse(result)
	if err != nil {
		return err
	}
	task, ok := converted.(*protocol.Task)
	if !ok {
		return fmt.Errorf("result = %T, want task", converted)
	}
	b.task = task
	return nil
}

// newBehaviorScenario isolates state and cleans up transports and servers.
func newBehaviorScenario(sc *godog.ScenarioContext) *gateBehavior {
	b := &gateBehavior{}
	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		*b = gateBehavior{ctx: ctx, cancel: cancel}
		return ctx, nil
	})
	sc.After(func(ctx context.Context, _ *godog.Scenario, _ error) (context.Context, error) {
		if b.streamStop != nil {
			b.streamStop()
		}
		if b.client != nil {
			_ = b.client.Destroy()
		}
		if b.stop != nil {
			if err := b.stop(); err != nil {
				b.cancel()
				return ctx, err
			}
		}
		b.cancel()
		return ctx, nil
	})
	return b
}
