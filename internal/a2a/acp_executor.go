package a2a

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"path/filepath"
	"strings"
	"sync"
	"time"

	protocol "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	extensionsv1 "github.com/anra-studio/gate/gen/anra/gate/a2a/extensions/v1"
	"github.com/anra-studio/gate/internal/acp"
	acpv1 "github.com/anra-studio/gate/internal/acp/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	ToolCallExtensionURI   = "https://anra.dev/a2a/extensions/acp-tool-call/v1"
	ThoughtExtensionURI    = "https://anra.dev/a2a/extensions/acp-thought/v1"
	PermissionExtensionURI = "https://anra.dev/a2a/extensions/acp-permission/v1"
)

// ACPClientFactory creates one long-lived ACP connection. The context belongs
// to the executor, not to an individual HTTP request.
type ACPClientFactory func(context.Context) (acp.Client, error)

// NewProcessACPClientFactory creates ACP clients by starting a v1 agent
// process whose stdin/stdout carry ACP's NDJSON transport.
func NewProcessACPClientFactory(config acpv1.ProcessConfig) ACPClientFactory {
	return func(ctx context.Context) (acp.Client, error) {
		return acpv1.StartProcess(ctx, config)
	}
}

// ACPExecutorConfig configures the ACP-backed A2A executor.
type ACPExecutorConfig struct {
	NewClient         ACPClientFactory
	InitializeRequest acp.InitializeRequest
	SessionRequest    acp.NewSessionRequest
	// CancelTimeout bounds how long Cancel waits for session/prompt to return
	// the cancelled stop reason. It defaults to five seconds.
	CancelTimeout time.Duration
}

// ACPExecutor maps A2A contexts to long-lived ACP sessions and A2A tasks to
// ACP prompt turns. Prompts sharing a context are serialized.
type ACPExecutor struct {
	config ACPExecutorConfig
	ctx    context.Context
	cancel context.CancelFunc

	mu       sync.Mutex
	sessions map[string]*sessionSlot
	turns    map[protocol.TaskID]*activeTurn
	closed   bool
}

type sessionSlot struct {
	ready chan struct{}
	actor *sessionActor
	err   error
}

type sessionActor struct {
	client    acp.Client
	sessionID string
	promptMu  sync.Mutex
}

type activeTurn struct {
	actor  *sessionActor
	cancel context.CancelCauseFunc
	done   chan struct{}

	mu              sync.Mutex
	started         bool
	cancelRequested bool
	result          promptResult
}

type promptResult struct {
	reason acp.StopReason
	err    error
}

var (
	_ a2asrv.AgentExecutor       = (*ACPExecutor)(nil)
	_ interface{ Close() error } = (*ACPExecutor)(nil)
)

// NewACPExecutor creates an executor. ACP processes and sessions are started
// lazily when the first message for an A2A context arrives.
func NewACPExecutor(config ACPExecutorConfig) (*ACPExecutor, error) {
	if config.NewClient == nil {
		return nil, errors.New("a2a: ACP client factory is required")
	}
	if !filepath.IsAbs(config.SessionRequest.CWD) {
		return nil, errors.New("a2a: ACP session cwd must be absolute")
	}
	if config.CancelTimeout <= 0 {
		config.CancelTimeout = 5 * time.Second
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &ACPExecutor{
		config: config, ctx: ctx, cancel: cancel,
		sessions: make(map[string]*sessionSlot),
		turns:    make(map[protocol.TaskID]*activeTurn),
	}, nil
}

// Close terminates every ACP client owned by the executor.
func (e *ACPExecutor) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.cancel()
	actors := make([]*sessionActor, 0, len(e.sessions))
	for _, slot := range e.sessions {
		if slot.actor != nil {
			actors = append(actors, slot.actor)
		}
	}
	e.mu.Unlock()

	var result error
	for _, actor := range actors {
		result = errors.Join(result, actor.client.Close())
	}
	return result
}

// Execute implements a2asrv.AgentExecutor.
func (e *ACPExecutor) Execute(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[protocol.Event, error] {
	return func(yield func(protocol.Event, error) bool) {
		content, err := messageToACP(execCtx.Message)
		if err != nil {
			yield(nil, err)
			return
		}
		if execCtx.StoredTask == nil {
			if !yield(protocol.NewSubmittedTask(execCtx, execCtx.Message), nil) {
				return
			}
		}

		actor, err := e.session(execCtx.ContextID)
		if err != nil {
			yield(failedEvent(execCtx, "Unable to start the ACP session."), nil)
			return
		}
		if !yield(protocol.NewStatusUpdateEvent(execCtx, protocol.TaskStateWorking, nil), nil) {
			return
		}

		turnCtx, cancel := context.WithCancelCause(e.ctx)
		turn := &activeTurn{actor: actor, cancel: cancel, done: make(chan struct{})}
		if err := e.registerTurn(execCtx.TaskID, turn); err != nil {
			cancel(err)
			yield(failedEvent(execCtx, "The task is already running."), nil)
			return
		}
		defer func() {
			e.unregisterTurn(execCtx.TaskID, turn)
			cancel(nil)
		}()

		handler := newACPEventHandler(turnCtx, execCtx)
		go func() {
			actor.promptMu.Lock()
			defer actor.promptMu.Unlock()
			if !turn.markStarted(turnCtx) {
				turn.finish(promptResult{err: context.Cause(turnCtx)})
				return
			}
			reason, promptErr := actor.client.Prompt(turnCtx, acp.PromptRequest{
				SessionID: actor.sessionID,
				Content:   content,
			}, handler)
			turn.finish(promptResult{reason: reason, err: promptErr})
		}()

		for {
			select {
			case event := <-handler.events:
				if event != nil && !yield(event, nil) {
					return
				}
			case <-turn.done:
				result := turn.promptResult()
				for {
					select {
					case event := <-handler.events:
						if event != nil && !yield(event, nil) {
							return
						}
					default:
						if turn.wasCancelRequested() {
							return
						}
						if result.err != nil {
							yield(failedEvent(execCtx, "The ACP prompt failed."), nil)
							return
						}
						yield(protocol.NewStatusUpdateEvent(execCtx, taskState(result.reason), nil), nil)
						return
					}
				}
			case <-ctx.Done():
				// The SDK normally keeps this execution context alive after an HTTP
				// stream disconnect. If the execution itself is stopped, terminate
				// the ACP turn so it cannot leak.
				cancel(context.Cause(ctx))
				return
			}
		}
	}
}

// Cancel forwards A2A cancellation to the active ACP session.
func (e *ACPExecutor) Cancel(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[protocol.Event, error] {
	return func(yield func(protocol.Event, error) bool) {
		e.mu.Lock()
		turn := e.turns[execCtx.TaskID]
		e.mu.Unlock()
		if turn == nil {
			yield(nil, protocol.ErrTaskNotFound)
			return
		}
		turn.mu.Lock()
		turn.cancelRequested = true
		if !turn.started {
			turn.cancel(context.Canceled)
			turn.mu.Unlock()
			yield(protocol.NewStatusUpdateEvent(execCtx, protocol.TaskStateCanceled, nil), nil)
			return
		}
		turn.mu.Unlock()
		if err := turn.actor.client.Cancel(ctx, turn.actor.sessionID); err != nil {
			yield(nil, fmt.Errorf("cancel ACP prompt: %w", err))
			return
		}
		timer := time.NewTimer(e.config.CancelTimeout)
		defer timer.Stop()
		select {
		case <-turn.done:
			result := turn.promptResult()
			if result.err != nil {
				yield(nil, fmt.Errorf("wait for ACP cancellation: %w", result.err))
				return
			}
			if result.reason != acp.StopCancelled {
				yield(nil, protocol.ErrTaskNotCancelable)
				return
			}
		case <-timer.C:
			turn.cancel(context.DeadlineExceeded)
		case <-ctx.Done():
			turn.cancel(context.Cause(ctx))
			yield(nil, ctx.Err())
			return
		}
		yield(protocol.NewStatusUpdateEvent(execCtx, protocol.TaskStateCanceled, nil), nil)
	}
}

func (e *ACPExecutor) session(contextID string) (*sessionActor, error) {
	if contextID == "" {
		return nil, protocol.ErrInvalidParams
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil, acp.ErrClosed
	}
	if existing := e.sessions[contextID]; existing != nil {
		e.mu.Unlock()
		<-existing.ready
		return existing.actor, existing.err
	}
	slot := &sessionSlot{ready: make(chan struct{})}
	e.sessions[contextID] = slot
	e.mu.Unlock()

	client, err := e.config.NewClient(e.ctx)
	if err == nil && client == nil {
		err = errors.New("a2a: ACP client factory returned nil")
	}
	if err == nil {
		var initialized acp.InitializeResult
		initialized, err = client.Initialize(e.ctx, e.config.InitializeRequest)
		if err == nil && initialized.ProtocolVersion != acp.ProtocolVersion {
			err = &acp.ProtocolError{Message: fmt.Sprintf(
				"agent selected protocol version %d; only version %d is supported",
				initialized.ProtocolVersion, acp.ProtocolVersion,
			)}
		}
	}
	var session acp.Session
	if err == nil {
		session, err = client.NewSession(e.ctx, e.config.SessionRequest)
		if err == nil && session.ID == "" {
			err = &acp.ProtocolError{Message: "session/new returned an empty sessionId"}
		}
	}
	if err != nil && client != nil {
		_ = client.Close()
	}

	e.mu.Lock()
	if e.closed && err == nil {
		err = acp.ErrClosed
		_ = client.Close()
	}
	if err == nil {
		slot.actor = &sessionActor{client: client, sessionID: session.ID}
	} else {
		slot.err = err
		delete(e.sessions, contextID)
	}
	close(slot.ready)
	e.mu.Unlock()
	return slot.actor, slot.err
}

func (t *activeTurn) markStarted(ctx context.Context) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if ctx.Err() != nil {
		return false
	}
	t.started = true
	return true
}

func (t *activeTurn) finish(result promptResult) {
	t.mu.Lock()
	t.result = result
	close(t.done)
	t.mu.Unlock()
}

func (t *activeTurn) promptResult() promptResult {
	<-t.done
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.result
}

func (t *activeTurn) wasCancelRequested() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.cancelRequested
}

func (e *ACPExecutor) registerTurn(taskID protocol.TaskID, turn *activeTurn) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return acp.ErrClosed
	}
	if _, exists := e.turns[taskID]; exists {
		return acp.ErrPromptInProgress
	}
	e.turns[taskID] = turn
	return nil
}

func (e *ACPExecutor) unregisterTurn(taskID protocol.TaskID, turn *activeTurn) {
	e.mu.Lock()
	if e.turns[taskID] == turn {
		delete(e.turns, taskID)
	}
	e.mu.Unlock()
}

type acpEventHandler struct {
	ctx    context.Context
	info   *a2asrv.ExecutorContext
	events chan protocol.Event

	mu            sync.Mutex
	sequence      uint64
	artifactID    protocol.ArtifactID
	lastThoughtID string
	wantThought   bool
	wantToolCall  bool
}

func newACPEventHandler(ctx context.Context, info *a2asrv.ExecutorContext) *acpEventHandler {
	return &acpEventHandler{
		ctx: ctx, info: info, events: make(chan protocol.Event, 64),
		sequence:     2,
		wantThought:  extensionEnabled(info, ThoughtExtensionURI),
		wantToolCall: extensionEnabled(info, ToolCallExtensionURI),
	}
}

func (h *acpEventHandler) OnUpdate(_ context.Context, update acp.Update) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sequence++
	var event protocol.Event
	switch update.Kind {
	case acp.UpdateAgentMessage:
		h.lastThoughtID = ""
		part := acpContentToPart(update.Content)
		if part == nil {
			return
		}
		if h.artifactID == "" {
			artifact := protocol.NewArtifactEvent(h.info, part)
			h.artifactID = artifact.Artifact.ID
			event = artifact
		} else {
			event = protocol.NewArtifactUpdateEvent(h.info, h.artifactID, part)
		}
	case acp.UpdateAgentThought:
		if update.MessageID != "" {
			h.lastThoughtID = update.MessageID
		} else if h.lastThoughtID == "" {
			h.lastThoughtID = protocol.NewMessageID()
		}
		if !h.wantThought || update.Content.Type != "text" {
			return
		}
		data, err := protoData(&extensionsv1.ThoughtChunk{
			Type: ThoughtExtensionURI, ThoughtId: h.lastThoughtID,
			Sequence: h.sequence, Delta: update.Content.Text,
		})
		if err != nil {
			return
		}
		event = extensionEvent(h.info, ThoughtExtensionURI, data)
	case acp.UpdateToolCall, acp.UpdateToolCallDiff:
		h.lastThoughtID = ""
		if !h.wantToolCall {
			return
		}
		data, err := toolCallData(update.ToolCall, h.sequence)
		if err != nil {
			return
		}
		event = extensionEvent(h.info, ToolCallExtensionURI, data)
	}
	if event != nil {
		select {
		case h.events <- event:
		case <-h.ctx.Done():
		}
	}
}

// Permission handling is intentionally fail-closed in this first executor.
// A durable permission actor will replace this behavior; no request is ever
// implicitly approved.
func (*acpEventHandler) RequestPermission(context.Context, acp.PermissionRequest) (acp.PermissionOutcome, error) {
	return acp.CancelPermission(), nil
}

func messageToACP(message *protocol.Message) ([]acp.Content, error) {
	if message == nil || message.Role != protocol.MessageRoleUser || len(message.Parts) == 0 {
		return nil, protocol.ErrInvalidParams
	}
	result := make([]acp.Content, 0, len(message.Parts))
	for _, part := range message.Parts {
		if part == nil {
			return nil, protocol.ErrInvalidParams
		}
		switch value := part.Content.(type) {
		case protocol.Text:
			result = append(result, acp.Text(string(value)))
		case protocol.URL:
			if value == "" {
				return nil, protocol.ErrInvalidParams
			}
			result = append(result, acp.Content{Type: "resource_link", URI: string(value), MIMEType: part.MediaType})
		case protocol.Raw:
			kind := "resource"
			if strings.HasPrefix(part.MediaType, "image/") {
				kind = "image"
			} else if strings.HasPrefix(part.MediaType, "audio/") {
				kind = "audio"
			}
			if kind == "resource" {
				return nil, protocol.ErrUnsupportedContentType
			}
			result = append(result, acp.Content{
				Type: kind, Data: base64.StdEncoding.EncodeToString([]byte(value)), MIMEType: part.MediaType,
			})
		default:
			return nil, protocol.ErrUnsupportedContentType
		}
	}
	return result, nil
}

func acpContentToPart(content acp.Content) *protocol.Part {
	switch content.Type {
	case "text":
		return protocol.NewTextPart(content.Text)
	case "image", "audio":
		data, err := base64.StdEncoding.DecodeString(content.Data)
		if err != nil {
			return nil
		}
		part := protocol.NewRawPart(data)
		part.MediaType = content.MIMEType
		return part
	case "resource_link":
		return protocol.NewFileURLPart(protocol.URL(content.URI), content.MIMEType)
	default:
		return nil
	}
}

func extensionEnabled(info *a2asrv.ExecutorContext, uri string) bool {
	if info.ServiceParams == nil {
		return false
	}
	values, _ := info.ServiceParams.Get(protocol.SvcParamExtensions)
	for _, value := range values {
		for _, candidate := range strings.Split(value, ",") {
			if strings.TrimSpace(candidate) == uri {
				return true
			}
		}
	}
	return false
}

func extensionEvent(info *a2asrv.ExecutorContext, uri string, data map[string]any) protocol.Event {
	message := protocol.NewMessageForTask(protocol.MessageRoleAgent, info, protocol.NewDataPart(data))
	message.Extensions = []string{uri}
	return protocol.NewStatusUpdateEvent(info, protocol.TaskStateWorking, message)
}

func protoData(message proto.Message) (map[string]any, error) {
	encoded, err := protojson.Marshal(message)
	if err != nil {
		return nil, err
	}
	var result map[string]any
	if err := json.Unmarshal(encoded, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func toolCallData(tool acp.ToolCall, sequence uint64) (map[string]any, error) {
	content := make([]*structpb.Value, 0, len(tool.Content))
	for _, item := range tool.Content {
		value, err := structpb.NewValue(map[string]any{
			"type": item.Type, "content": item.Content.Text, "path": item.Path,
			"oldText": item.OldText, "newText": item.NewText, "terminal": item.Terminal,
		})
		if err != nil {
			return nil, err
		}
		content = append(content, value)
	}
	locations := make([]*extensionsv1.ToolCallLocation, 0, len(tool.Locations))
	for _, location := range tool.Locations {
		locations = append(locations, &extensionsv1.ToolCallLocation{Uri: location.Path, StartLine: location.Line})
	}
	return protoData(&extensionsv1.ToolCallUpdate{
		Type: ToolCallExtensionURI, ToolCallId: tool.ID, Sequence: sequence,
		Status: toolCallStatus(tool.Status), Title: tool.Title, Kind: tool.Kind,
		Content: content, Locations: locations,
	})
}

func toolCallStatus(status string) extensionsv1.ToolCallStatus {
	switch status {
	case "pending":
		return extensionsv1.ToolCallStatus_TOOL_CALL_STATUS_PENDING
	case "in_progress":
		return extensionsv1.ToolCallStatus_TOOL_CALL_STATUS_IN_PROGRESS
	case "completed":
		return extensionsv1.ToolCallStatus_TOOL_CALL_STATUS_COMPLETED
	case "failed":
		return extensionsv1.ToolCallStatus_TOOL_CALL_STATUS_FAILED
	default:
		return extensionsv1.ToolCallStatus_TOOL_CALL_STATUS_UNSPECIFIED
	}
}

func taskState(reason acp.StopReason) protocol.TaskState {
	switch reason {
	case acp.StopEndTurn:
		return protocol.TaskStateCompleted
	case acp.StopCancelled:
		return protocol.TaskStateCanceled
	case acp.StopRefusal:
		return protocol.TaskStateRejected
	default:
		return protocol.TaskStateFailed
	}
}

func failedEvent(info protocol.TaskInfoProvider, text string) protocol.Event {
	message := protocol.NewMessageForTask(protocol.MessageRoleAgent, info, protocol.NewTextPart(text))
	return protocol.NewStatusUpdateEvent(info, protocol.TaskStateFailed, message)
}
