package a2a

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"iter"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	protocol "github.com/a2aproject/a2a-go/v2/a2a"
	a2apb "github.com/a2aproject/a2a-go/v2/a2apb/v1"
	"github.com/a2aproject/a2a-go/v2/a2apb/v1/pbconv"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"google.golang.org/protobuf/encoding/protojson"
)

const testExtension = "https://anra.dev/a2a/extensions/acp-thought/v1"

type testExecutor struct {
	requests chan *a2asrv.ExecutorContext
}

func (e *testExecutor) Execute(_ context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[protocol.Event, error] {
	return func(yield func(protocol.Event, error) bool) {
		e.requests <- execCtx
		if !yield(protocol.NewSubmittedTask(execCtx, execCtx.Message), nil) {
			return
		}
		if !yield(protocol.NewStatusUpdateEvent(execCtx, protocol.TaskStateWorking, nil), nil) {
			return
		}
		artifact := protocol.NewArtifactEvent(execCtx, protocol.NewTextPart("hello "))
		if !yield(artifact, nil) {
			return
		}
		last := protocol.NewArtifactUpdateEvent(execCtx, artifact.Artifact.ID, protocol.NewTextPart("world"))
		last.LastChunk = true
		if !yield(last, nil) {
			return
		}
		yield(protocol.NewStatusUpdateEvent(execCtx, protocol.TaskStateCompleted, nil), nil)
	}
}

func (*testExecutor) Cancel(_ context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[protocol.Event, error] {
	return func(yield func(protocol.Event, error) bool) {
		yield(protocol.NewStatusUpdateEvent(execCtx, protocol.TaskStateCanceled, nil), nil)
	}
}

func TestServerConnectAndAgentCard(t *testing.T) {
	executor := &testExecutor{requests: make(chan *a2asrv.ExecutorContext, 1)}
	server := newTestServer(t, executor)

	response, err := server.Client().Get(server.URL + a2asrv.WellKnownAgentCardPath)
	if err != nil {
		t.Fatalf("get agent card: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("agent card status = %d, want %d", response.StatusCode, http.StatusOK)
	}
	var card protocol.AgentCard
	if err := json.NewDecoder(response.Body).Decode(&card); err != nil {
		t.Fatalf("decode agent card: %v", err)
	}
	if card.Name != "Test Agent" {
		t.Fatalf("agent card name = %q, want %q", card.Name, "Test Agent")
	}

	message := protocol.NewMessage(protocol.MessageRoleUser, protocol.NewTextPart("hello"))
	message.ID = "message-1"
	pbRequest, err := pbconv.ToProtoSendMessageRequest(&protocol.SendMessageRequest{Message: message})
	if err != nil {
		t.Fatalf("convert send request: %v", err)
	}
	client := connect.NewClient[a2apb.SendMessageRequest, a2apb.SendMessageResponse](
		server.Client(), server.URL+a2apb.A2AService_SendMessage_FullMethodName,
	)
	request := connect.NewRequest(pbRequest)
	request.Header().Set(protocol.SvcParamVersion, string(protocol.Version))
	request.Header().Set(protocol.SvcParamExtensions, testExtension)
	result, err := client.CallUnary(t.Context(), request)
	if err != nil {
		t.Fatalf("Connect SendMessage: %v", err)
	}
	converted, err := pbconv.FromProtoSendMessageResponse(result.Msg)
	if err != nil {
		t.Fatalf("convert send response: %v", err)
	}
	task, ok := converted.(*protocol.Task)
	if !ok {
		t.Fatalf("SendMessage result = %T, want *a2a.Task", converted)
	}
	if task.Status.State != protocol.TaskStateCompleted {
		t.Fatalf("task state = %s, want %s", task.Status.State, protocol.TaskStateCompleted)
	}
	if got := task.Artifacts[0].Parts[0].Text() + task.Artifacts[0].Parts[1].Text(); got != "hello world" {
		t.Fatalf("artifact text = %q, want %q", got, "hello world")
	}

	observed := <-executor.requests
	versions, _ := observed.ServiceParams.Get(protocol.SvcParamVersion)
	if len(versions) != 1 || versions[0] != string(protocol.Version) {
		t.Fatalf("observed A2A-Version = %v, want %q", versions, protocol.Version)
	}
}

func TestServerConnectStreamingAndErrors(t *testing.T) {
	executor := &testExecutor{requests: make(chan *a2asrv.ExecutorContext, 1)}
	server := newTestServer(t, executor)
	message := protocol.NewMessage(protocol.MessageRoleUser, protocol.NewTextPart("hello"))
	message.ID = "message-stream"
	pbRequest, err := pbconv.ToProtoSendMessageRequest(&protocol.SendMessageRequest{Message: message})
	if err != nil {
		t.Fatalf("convert send request: %v", err)
	}

	streamClient := connect.NewClient[a2apb.SendMessageRequest, a2apb.StreamResponse](
		server.Client(), server.URL+a2apb.A2AService_SendStreamingMessage_FullMethodName,
	)
	stream, err := streamClient.CallServerStream(t.Context(), connect.NewRequest(pbRequest))
	if err != nil {
		t.Fatalf("Connect SendStreamingMessage: %v", err)
	}
	var eventCount int
	for stream.Receive() {
		if _, err := pbconv.FromProtoStreamResponse(stream.Msg()); err != nil {
			t.Fatalf("convert stream response: %v", err)
		}
		eventCount++
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("receive stream: %v", err)
	}
	if eventCount != 5 {
		t.Fatalf("stream event count = %d, want 5", eventCount)
	}
	<-executor.requests

	getClient := connect.NewClient[a2apb.GetTaskRequest, a2apb.Task](
		server.Client(), server.URL+a2apb.A2AService_GetTask_FullMethodName,
	)
	_, err = getClient.CallUnary(t.Context(), connect.NewRequest(&a2apb.GetTaskRequest{Id: "missing"}))
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("GetTask error = %v, code = %s, want %s", err, connect.CodeOf(err), connect.CodeNotFound)
	}
}

func TestServerVanguardHTTPJSON(t *testing.T) {
	executor := &testExecutor{requests: make(chan *a2asrv.ExecutorContext, 1)}
	server := newTestServer(t, executor)
	message := protocol.NewMessage(protocol.MessageRoleUser, protocol.NewTextPart("hello"))
	message.ID = "message-rest"
	pbRequest, err := pbconv.ToProtoSendMessageRequest(&protocol.SendMessageRequest{Message: message})
	if err != nil {
		t.Fatalf("convert send request: %v", err)
	}
	body, err := protojson.Marshal(pbRequest)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+"/message:send", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatalf("HTTP+JSON SendMessage: %v", err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("HTTP+JSON status = %d, body = %s", response.StatusCode, responseBody)
	}
	var pbResponse a2apb.SendMessageResponse
	if err := protojson.Unmarshal(responseBody, &pbResponse); err != nil {
		t.Fatalf("decode response %s: %v", responseBody, err)
	}
	converted, err := pbconv.FromProtoSendMessageResponse(&pbResponse)
	if err != nil {
		t.Fatalf("convert response: %v", err)
	}
	if task, ok := converted.(*protocol.Task); !ok || task.Status.State != protocol.TaskStateCompleted {
		t.Fatalf("HTTP+JSON result = %#v, want completed task", converted)
	}
	<-executor.requests
}

func TestNewServerValidation(t *testing.T) {
	executor := &testExecutor{requests: make(chan *a2asrv.ExecutorContext, 1)}
	if _, err := NewServer(nil, executor); err == nil {
		t.Fatal("NewServer(nil card) error = nil")
	}
	card := testCard()
	if _, err := NewServer(card, nil); err == nil {
		t.Fatal("NewServer(nil executor) error = nil")
	}
	card.Name = ""
	if _, err := NewServer(card, executor); err == nil {
		t.Fatal("NewServer(invalid card) error = nil")
	}
}

func newTestServer(t *testing.T, executor a2asrv.AgentExecutor) *httptest.Server {
	t.Helper()
	handler, err := NewServer(testCard(), executor)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

func testCard() *protocol.AgentCard {
	return &protocol.AgentCard{
		Name:        "Test Agent",
		Description: "Test A2A agent",
		Version:     "1.0.0",
		Capabilities: protocol.AgentCapabilities{
			Streaming:  true,
			Extensions: []protocol.AgentExtension{{URI: testExtension}},
		},
		SupportedInterfaces: []*protocol.AgentInterface{
			protocol.NewAgentInterface("https://example.com", protocol.TransportProtocolHTTPJSON),
		},
		DefaultInputModes:  []string{"text/plain"},
		DefaultOutputModes: []string{"text/plain"},
		Skills: []protocol.AgentSkill{{
			ID: "test", Name: "Test", Description: "Test", Tags: []string{"test"},
		}},
	}
}
