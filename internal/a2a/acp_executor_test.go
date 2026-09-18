package a2a

import (
	"context"
	"iter"
	"runtime"
	"sync"
	"testing"
	"time"

	protocol "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/anra-studio/gate/internal/acp"
)

type fakeACPClient struct {
	mu sync.Mutex

	initializeCalls int
	newSessionCalls int
	promptCalls     int
	cancelCalls     int
	closeCalls      int
	updates         []acp.Update
	stopReason      acp.StopReason
	promptStarted   chan struct{}
	cancelPrompt    chan struct{}
	blockPrompt     bool
}

func (c *fakeACPClient) Initialize(context.Context, acp.InitializeRequest) (acp.InitializeResult, error) {
	c.mu.Lock()
	c.initializeCalls++
	c.mu.Unlock()
	return acp.InitializeResult{ProtocolVersion: acp.ProtocolVersion}, nil
}

func (c *fakeACPClient) NewSession(context.Context, acp.NewSessionRequest) (acp.Session, error) {
	c.mu.Lock()
	c.newSessionCalls++
	c.mu.Unlock()
	return acp.Session{ID: "acp-session-1"}, nil
}

func (c *fakeACPClient) Prompt(ctx context.Context, _ acp.PromptRequest, handler acp.Handler) (acp.StopReason, error) {
	c.mu.Lock()
	c.promptCalls++
	updates := append([]acp.Update(nil), c.updates...)
	block := c.blockPrompt
	started := c.promptStarted
	reason := c.stopReason
	c.mu.Unlock()
	if started != nil {
		select {
		case <-started:
		default:
			close(started)
		}
	}
	if block {
		if c.cancelPrompt == nil {
			<-ctx.Done()
			return "", context.Cause(ctx)
		}
		select {
		case <-ctx.Done():
			return "", context.Cause(ctx)
		case <-c.cancelPrompt:
			return acp.StopCancelled, nil
		}
	}
	for _, update := range updates {
		handler.OnUpdate(ctx, update)
	}
	return reason, nil
}

func (c *fakeACPClient) Cancel(context.Context, string) error {
	c.mu.Lock()
	c.cancelCalls++
	if c.cancelPrompt != nil {
		select {
		case <-c.cancelPrompt:
		default:
			close(c.cancelPrompt)
		}
	}
	c.mu.Unlock()
	return nil
}

func (c *fakeACPClient) Close() error {
	c.mu.Lock()
	c.closeCalls++
	c.mu.Unlock()
	return nil
}

func TestACPExecutorStreamsUpdatesAndReusesSession(t *testing.T) {
	client := &fakeACPClient{
		stopReason: acp.StopEndTurn,
		updates: []acp.Update{
			{Kind: acp.UpdateAgentThought, MessageID: "thought-1", Content: acp.Text("thinking")},
			{Kind: acp.UpdateToolCall, ToolCall: acp.ToolCall{ID: "tool-1", Title: "Test", Status: "in_progress"}},
			{Kind: acp.UpdateAgentMessage, Content: acp.Text("answer")},
		},
	}
	executor := newTestACPExecutor(t, client)
	params := a2asrv.NewServiceParams(map[string][]string{
		protocol.SvcParamExtensions: {ThoughtExtensionURI, ToolCallExtensionURI},
	})

	first := collectExecutorEvents(t, executor.Execute(t.Context(), &a2asrv.ExecutorContext{
		Message: newUserMessage("first"), TaskID: "task-1", ContextID: "context-1", ServiceParams: params,
	}))
	second := collectExecutorEvents(t, executor.Execute(t.Context(), &a2asrv.ExecutorContext{
		Message: newUserMessage("second"), TaskID: "task-2", ContextID: "context-1", ServiceParams: params,
	}))

	if got := len(first); got != 6 {
		t.Fatalf("first event count = %d, want 6", got)
	}
	if got := len(second); got != 6 {
		t.Fatalf("second event count = %d, want 6", got)
	}
	assertTaskState(t, first[0], protocol.TaskStateSubmitted)
	assertTaskState(t, first[1], protocol.TaskStateWorking)
	assertExtensionEvent(t, first[2], ThoughtExtensionURI, "3")
	assertExtensionEvent(t, first[3], ToolCallExtensionURI, "4")
	artifact, ok := first[4].(*protocol.TaskArtifactUpdateEvent)
	if !ok || artifact.Artifact.Parts[0].Text() != "answer" {
		t.Fatalf("artifact event = %#v, want answer text", first[4])
	}
	assertTaskState(t, first[5], protocol.TaskStateCompleted)

	client.mu.Lock()
	initializeCalls := client.initializeCalls
	newSessionCalls := client.newSessionCalls
	promptCalls := client.promptCalls
	client.mu.Unlock()
	if initializeCalls != 1 || newSessionCalls != 1 || promptCalls != 2 {
		t.Fatalf("ACP calls initialize=%d session=%d prompt=%d, want 1,1,2", initializeCalls, newSessionCalls, promptCalls)
	}
}

func TestACPExecutorConsumesSequenceForHiddenExtensions(t *testing.T) {
	client := &fakeACPClient{
		stopReason: acp.StopEndTurn,
		updates: []acp.Update{
			{Kind: acp.UpdateToolCall, ToolCall: acp.ToolCall{ID: "hidden", Status: "pending"}},
			{Kind: acp.UpdateAgentThought, MessageID: "visible", Content: acp.Text("thinking")},
		},
	}
	executor := newTestACPExecutor(t, client)
	params := a2asrv.NewServiceParams(map[string][]string{
		protocol.SvcParamExtensions: {ThoughtExtensionURI},
	})
	events := collectExecutorEvents(t, executor.Execute(t.Context(), &a2asrv.ExecutorContext{
		Message: newUserMessage("prompt"), TaskID: "task-1", ContextID: "context-1", ServiceParams: params,
	}))
	if len(events) != 4 {
		t.Fatalf("event count = %d, want 4", len(events))
	}
	assertExtensionEvent(t, events[2], ThoughtExtensionURI, "4")
}

func TestACPExecutorCancelForwardsToACP(t *testing.T) {
	client := &fakeACPClient{
		blockPrompt: true, promptStarted: make(chan struct{}), cancelPrompt: make(chan struct{}),
	}
	executor := newTestACPExecutor(t, client)
	execCtx := &a2asrv.ExecutorContext{
		Message: newUserMessage("wait"), TaskID: "task-1", ContextID: "context-1",
	}
	done := make(chan struct{})
	go func() {
		for range executor.Execute(context.Background(), execCtx) {
		}
		close(done)
	}()
	<-client.promptStarted

	cancelEvents := collectExecutorEvents(t, executor.Cancel(t.Context(), &a2asrv.ExecutorContext{
		TaskID: "task-1", ContextID: "context-1",
	}))
	if len(cancelEvents) != 1 {
		t.Fatalf("cancel event count = %d, want 1", len(cancelEvents))
	}
	assertTaskState(t, cancelEvents[0], protocol.TaskStateCanceled)
	<-done

	client.mu.Lock()
	cancelCalls := client.cancelCalls
	client.mu.Unlock()
	if cancelCalls != 1 {
		t.Fatalf("ACP cancel calls = %d, want 1", cancelCalls)
	}
}

func TestACPExecutorCancelQueuedTurnDoesNotCancelActivePrompt(t *testing.T) {
	client := &fakeACPClient{blockPrompt: true, promptStarted: make(chan struct{})}
	executor := newTestACPExecutor(t, client)
	firstDone := make(chan struct{})
	go func() {
		for range executor.Execute(context.Background(), &a2asrv.ExecutorContext{
			Message: newUserMessage("first"), TaskID: "task-1", ContextID: "context-1",
		}) {
		}
		close(firstDone)
	}()
	<-client.promptStarted

	secondDone := make(chan struct{})
	go func() {
		for range executor.Execute(context.Background(), &a2asrv.ExecutorContext{
			Message: newUserMessage("second"), TaskID: "task-2", ContextID: "context-1",
		}) {
		}
		close(secondDone)
	}()
	deadline := time.After(5 * time.Second)
	for {
		executor.mu.Lock()
		queued := executor.turns["task-2"] != nil
		executor.mu.Unlock()
		if queued {
			break
		}
		select {
		case <-deadline:
			t.Fatal("queued turn was not registered")
		default:
			runtime.Gosched()
		}
	}

	cancelEvents := collectExecutorEvents(t, executor.Cancel(t.Context(), &a2asrv.ExecutorContext{
		TaskID: "task-2", ContextID: "context-1",
	}))
	assertTaskState(t, cancelEvents[0], protocol.TaskStateCanceled)
	client.mu.Lock()
	cancelCalls := client.cancelCalls
	client.mu.Unlock()
	if cancelCalls != 0 {
		t.Fatalf("ACP cancel calls = %d, want 0 for queued turn", cancelCalls)
	}

	if err := executor.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	<-firstDone
	<-secondDone
}

func TestACPExecutorPermissionFailsClosed(t *testing.T) {
	handler := newACPEventHandler(t.Context(), &a2asrv.ExecutorContext{})
	outcome, err := handler.RequestPermission(t.Context(), acp.PermissionRequest{})
	if err != nil {
		t.Fatalf("RequestPermission() error = %v", err)
	}
	if !outcome.Cancelled || outcome.OptionID != "" {
		t.Fatalf("permission outcome = %#v, want cancelled", outcome)
	}
}

func newTestACPExecutor(t *testing.T, client acp.Client) *ACPExecutor {
	t.Helper()
	executor, err := NewACPExecutor(ACPExecutorConfig{
		NewClient:         func(context.Context) (acp.Client, error) { return client, nil },
		InitializeRequest: acp.InitializeRequest{ClientInfo: acp.ClientInfo{Name: "gate", Version: "test"}},
		SessionRequest:    acp.NewSessionRequest{CWD: "/tmp"},
	})
	if err != nil {
		t.Fatalf("NewACPExecutor: %v", err)
	}
	t.Cleanup(func() {
		if err := executor.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return executor
}

func collectExecutorEvents(t *testing.T, sequence iter.Seq2[protocol.Event, error]) []protocol.Event {
	t.Helper()
	var events []protocol.Event
	for event, err := range sequence {
		if err != nil {
			t.Fatalf("executor error: %v", err)
		}
		events = append(events, event)
	}
	return events
}

func newUserMessage(text string) *protocol.Message {
	message := protocol.NewMessage(protocol.MessageRoleUser, protocol.NewTextPart(text))
	message.ID = protocol.NewMessageID()
	return message
}

func assertTaskState(t *testing.T, event protocol.Event, want protocol.TaskState) {
	t.Helper()
	switch typed := event.(type) {
	case *protocol.Task:
		if typed.Status.State != want {
			t.Fatalf("task state = %s, want %s", typed.Status.State, want)
		}
	case *protocol.TaskStatusUpdateEvent:
		if typed.Status.State != want {
			t.Fatalf("status state = %s, want %s", typed.Status.State, want)
		}
	default:
		t.Fatalf("event = %T, want task state %s", event, want)
	}
}

func assertExtensionEvent(t *testing.T, event protocol.Event, uri string, sequence string) {
	t.Helper()
	status, ok := event.(*protocol.TaskStatusUpdateEvent)
	if !ok || status.Status.Message == nil {
		t.Fatalf("extension event = %#v, want status message", event)
	}
	if len(status.Status.Message.Extensions) != 1 || status.Status.Message.Extensions[0] != uri {
		t.Fatalf("extensions = %v, want %q", status.Status.Message.Extensions, uri)
	}
	data, ok := status.Status.Message.Parts[0].Data().(map[string]any)
	if !ok {
		t.Fatalf("extension data = %T, want map", status.Status.Message.Parts[0].Data())
	}
	if got := data["sequence"]; got != sequence {
		t.Fatalf("extension sequence = %#v, want %#v", got, sequence)
	}
}

var _ acp.Client = (*fakeACPClient)(nil)
