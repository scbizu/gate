package e2e_test

import (
	"context"
	"errors"
	"iter"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2aclient/agentcard"
	"github.com/a2aproject/a2a-go/v2/a2aext"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

const thoughtExtension = "https://anra.dev/a2a/extensions/acp-thought/v1"

type observedRequest struct {
	messageID        string
	role             a2a.MessageRole
	text             string
	extensions       []string
	protocolVersion  string
	requestedHeaders []string
}

type testExecutor struct {
	requests chan observedRequest
}

func (e *testExecutor) Execute(_ context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		var version, extensions []string
		if execCtx.ServiceParams != nil {
			version, _ = execCtx.ServiceParams.Get(a2a.SvcParamVersion)
			extensions, _ = execCtx.ServiceParams.Get(a2a.SvcParamExtensions)
		}
		e.requests <- observedRequest{
			messageID:        execCtx.Message.ID,
			role:             execCtx.Message.Role,
			text:             execCtx.Message.Parts[0].Text(),
			extensions:       slices.Clone(execCtx.Message.Extensions),
			protocolVersion:  first(version),
			requestedHeaders: slices.Clone(extensions),
		}

		if !yield(a2a.NewSubmittedTask(execCtx, execCtx.Message), nil) {
			return
		}
		if !yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateWorking, nil), nil) {
			return
		}
		artifact := a2a.NewArtifactEvent(execCtx, a2a.NewTextPart("research "))
		if !yield(artifact, nil) {
			return
		}
		lastChunk := a2a.NewArtifactUpdateEvent(execCtx, artifact.Artifact.ID, a2a.NewTextPart("complete"))
		lastChunk.LastChunk = true
		if !yield(lastChunk, nil) {
			return
		}
		yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateCompleted, nil), nil)
	}
}

func (e *testExecutor) Cancel(_ context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateCanceled, nil), nil)
	}
}

func TestA2AClientSendMessage(t *testing.T) {
	executor := &testExecutor{requests: make(chan observedRequest, 1)}
	client := newA2AClient(t, executor)

	result, err := client.SendMessage(t.Context(), newSendMessageRequest())
	if err != nil {
		t.Fatalf("SendMessage() error = %v", err)
	}

	assertObservedRequest(t, <-executor.requests)
	task, ok := result.(*a2a.Task)
	if !ok {
		t.Fatalf("SendMessage() result type = %T, want *a2a.Task", result)
	}
	if task.ID == "" || task.ContextID == "" {
		t.Fatalf("task identifiers must be populated: id=%q contextID=%q", task.ID, task.ContextID)
	}
	if task.Status.State != a2a.TaskStateCompleted {
		t.Errorf("task state = %s, want %s", task.Status.State, a2a.TaskStateCompleted)
	}
	if got := artifactText(task.Artifacts); got != "research complete" {
		t.Errorf("artifact text = %q, want %q", got, "research complete")
	}
}

func TestA2AClientSendStreamingMessage(t *testing.T) {
	executor := &testExecutor{requests: make(chan observedRequest, 1)}
	client := newA2AClient(t, executor)

	var events []a2a.Event
	for event, err := range client.SendStreamingMessage(t.Context(), newSendMessageRequest()) {
		if err != nil {
			t.Fatalf("SendStreamingMessage() error = %v", err)
		}
		events = append(events, event)
	}

	assertObservedRequest(t, <-executor.requests)
	if got := len(events); got != 5 {
		t.Fatalf("stream event count = %d, want 5", got)
	}
	if task, ok := events[0].(*a2a.Task); !ok || task.Status.State != a2a.TaskStateSubmitted {
		t.Fatalf("first event = %#v, want submitted task", events[0])
	}
	if status, ok := events[1].(*a2a.TaskStatusUpdateEvent); !ok || status.Status.State != a2a.TaskStateWorking {
		t.Fatalf("second event = %#v, want working status", events[1])
	}
	firstChunk, ok := events[2].(*a2a.TaskArtifactUpdateEvent)
	if !ok || firstChunk.Artifact.Parts[0].Text() != "research " || firstChunk.Append {
		t.Fatalf("third event = %#v, want initial artifact chunk", events[2])
	}
	lastChunk, ok := events[3].(*a2a.TaskArtifactUpdateEvent)
	if !ok || lastChunk.Artifact.Parts[0].Text() != "complete" || !lastChunk.Append || !lastChunk.LastChunk {
		t.Fatalf("fourth event = %#v, want final appended artifact chunk", events[3])
	}
	if status, ok := events[4].(*a2a.TaskStatusUpdateEvent); !ok || status.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("final event = %#v, want completed status", events[4])
	}
}

func TestA2AClientPropagatesProtocolError(t *testing.T) {
	executor := &testExecutor{requests: make(chan observedRequest, 1)}
	client := newA2AClient(t, executor)

	_, err := client.GetTask(t.Context(), &a2a.GetTaskRequest{ID: "missing"})
	if !errors.Is(err, a2a.ErrTaskNotFound) {
		t.Fatalf("GetTask() error = %v, want %v", err, a2a.ErrTaskNotFound)
	}
}

func newA2AClient(t *testing.T, executor a2asrv.AgentExecutor) *a2aclient.Client {
	t.Helper()

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	card := &a2a.AgentCard{
		Name:        "Research Engineering Agent",
		Description: "Investigates technologies and solves difficult engineering problems.",
		Version:     "1.0.0",
		Capabilities: a2a.AgentCapabilities{
			Streaming:  true,
			Extensions: []a2a.AgentExtension{{URI: thoughtExtension}},
		},
		SupportedInterfaces: []*a2a.AgentInterface{
			a2a.NewAgentInterface(server.URL+"/invoke", a2a.TransportProtocolJSONRPC),
		},
		DefaultInputModes:  []string{"text/plain"},
		DefaultOutputModes: []string{"text/plain"},
		Skills: []a2a.AgentSkill{{
			ID:          "technical-research",
			Name:        "Technical research",
			Description: "Research and engineering",
			Tags:        []string{"research", "engineering"},
		}},
	}
	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(card))
	mux.Handle("/invoke", a2asrv.NewJSONRPCHandler(a2asrv.NewHandler(executor)))
	t.Cleanup(server.Close)

	discovered, err := agentcard.DefaultResolver.Resolve(t.Context(), server.URL)
	if err != nil {
		t.Fatalf("resolve Agent Card: %v", err)
	}
	client, err := a2aclient.NewFromCard(
		t.Context(),
		discovered,
		a2aclient.WithConfig(a2aclient.Config{AcceptedOutputModes: []string{"text/plain"}}),
		a2aclient.WithCallInterceptors(a2aext.NewActivator(thoughtExtension)),
	)
	if err != nil {
		t.Fatalf("create A2A client: %v", err)
	}
	t.Cleanup(func() {
		if err := client.Destroy(); err != nil {
			t.Errorf("destroy A2A client: %v", err)
		}
	})
	return client
}

func newSendMessageRequest() *a2a.SendMessageRequest {
	message := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("Investigate the failure"))
	message.ID = "message-01"
	message.Extensions = []string{thoughtExtension}
	return &a2a.SendMessageRequest{Message: message}
}

func assertObservedRequest(t *testing.T, got observedRequest) {
	t.Helper()
	if got.messageID != "message-01" {
		t.Errorf("message ID = %q, want %q", got.messageID, "message-01")
	}
	if got.role != a2a.MessageRoleUser {
		t.Errorf("message role = %s, want %s", got.role, a2a.MessageRoleUser)
	}
	if got.text != "Investigate the failure" {
		t.Errorf("message text = %q, want %q", got.text, "Investigate the failure")
	}
	if !slices.Equal(got.extensions, []string{thoughtExtension}) {
		t.Errorf("message extensions = %v, want %v", got.extensions, []string{thoughtExtension})
	}
	if got.protocolVersion != string(a2a.Version) {
		t.Errorf("A2A-Version = %q, want %q", got.protocolVersion, a2a.Version)
	}
	if !slices.Equal(got.requestedHeaders, []string{thoughtExtension}) {
		t.Errorf("A2A-Extensions = %v, want %v", got.requestedHeaders, []string{thoughtExtension})
	}
}

func artifactText(artifacts []*a2a.Artifact) string {
	var result string
	for _, artifact := range artifacts {
		for _, part := range artifact.Parts {
			result += part.Text()
		}
	}
	return result
}

func first(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}
