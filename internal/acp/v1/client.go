// Package v1 adapts the open-source coder/acp-go-sdk to Gate's stable,
// protocol-independent ACP boundary.
package v1

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"path/filepath"
	"sync"

	acpsdk "github.com/coder/acp-go-sdk"

	"github.com/anra-studio/gate/internal/acp"
)

type Client struct {
	conn   *acpsdk.ClientSideConnection
	closer io.Closer

	mu      sync.Mutex
	prompts map[string]*promptState
	closed  bool
	once    sync.Once
}

type promptState struct {
	handler      acp.Handler
	cancelPrompt context.CancelCauseFunc

	mu                  sync.Mutex
	protocolError       error
	nextPermissionID    uint64
	permissionCancelled bool
	permissions         map[uint64]context.CancelCauseFunc
}

func (s *promptState) fail(err error) {
	s.mu.Lock()
	if s.protocolError == nil {
		s.protocolError = err
		s.cancelPrompt(err)
	}
	s.mu.Unlock()
}

func (s *promptState) failure() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.protocolError
}

func (s *promptState) beginPermission(parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(parent)

	s.mu.Lock()
	if s.permissionCancelled {
		s.mu.Unlock()
		cancel(context.Canceled)
		return ctx, func() {}
	}
	s.nextPermissionID++
	id := s.nextPermissionID
	s.permissions[id] = cancel
	s.mu.Unlock()

	return ctx, func() {
		s.mu.Lock()
		delete(s.permissions, id)
		s.mu.Unlock()
		cancel(nil)
	}
}

func (s *promptState) cancelPermissions() {
	s.mu.Lock()
	s.permissionCancelled = true
	cancels := make([]context.CancelCauseFunc, 0, len(s.permissions))
	for id, cancel := range s.permissions {
		cancels = append(cancels, cancel)
		delete(s.permissions, id)
	}
	s.mu.Unlock()
	for _, cancel := range cancels {
		cancel(context.Canceled)
	}
}

// New constructs a v1 adapter. ACP framing, request correlation, validation,
// cancellation and notification ordering are owned by acp-go-sdk.
func New(input io.Reader, output io.Writer, closer io.Closer) *Client {
	client := &Client{closer: closer, prompts: make(map[string]*promptState)}
	client.conn = acpsdk.NewClientSideConnection(&sdkCallbacks{owner: client}, output, input)
	return client
}

func NewReadWriteCloser(stream io.ReadWriteCloser) *Client {
	return New(stream, stream, stream)
}

func (c *Client) Initialize(ctx context.Context, req acp.InitializeRequest) (acp.InitializeResult, error) {
	if err := c.ensureOpen(); err != nil {
		return acp.InitializeResult{}, err
	}
	params := acpsdk.InitializeRequest{
		ProtocolVersion: acpsdk.ProtocolVersionNumber,
		ClientCapabilities: acpsdk.ClientCapabilities{
			Fs: acpsdk.FileSystemCapabilities{},
		},
	}
	if req.ClientInfo.Name != "" || req.ClientInfo.Version != "" {
		params.ClientInfo = &acpsdk.Implementation{Name: req.ClientInfo.Name, Version: req.ClientInfo.Version}
		if req.ClientInfo.Title != "" {
			params.ClientInfo.Title = acpsdk.Ptr(req.ClientInfo.Title)
		}
	}
	result, err := c.conn.Initialize(ctx, params)
	if err != nil {
		return acp.InitializeResult{}, convertRPCError(err)
	}
	if int(result.ProtocolVersion) != acp.ProtocolVersion {
		_ = c.Close()
		return acp.InitializeResult{}, &acp.ProtocolError{Message: fmt.Sprintf(
			"agent selected protocol version %d; only version %d is supported",
			result.ProtocolVersion, acp.ProtocolVersion,
		)}
	}
	converted := acp.InitializeResult{
		ProtocolVersion: int(result.ProtocolVersion),
		Capabilities: acp.AgentCapabilities{
			LoadSession:     result.AgentCapabilities.LoadSession,
			PromptImage:     result.AgentCapabilities.PromptCapabilities.Image,
			PromptAudio:     result.AgentCapabilities.PromptCapabilities.Audio,
			EmbeddedContext: result.AgentCapabilities.PromptCapabilities.EmbeddedContext,
			MCPHTTP:         result.AgentCapabilities.McpCapabilities.Http,
			MCPSSE:          result.AgentCapabilities.McpCapabilities.Sse,
		},
	}
	if result.AgentInfo != nil {
		converted.AgentInfo.Name = result.AgentInfo.Name
		converted.AgentInfo.Version = result.AgentInfo.Version
		if result.AgentInfo.Title != nil {
			converted.AgentInfo.Title = *result.AgentInfo.Title
		}
	}
	return converted, nil
}

func (c *Client) NewSession(ctx context.Context, req acp.NewSessionRequest) (acp.Session, error) {
	if err := c.ensureOpen(); err != nil {
		return acp.Session{}, err
	}
	if !filepath.IsAbs(req.CWD) {
		return acp.Session{}, &acp.ProtocolError{Message: "session cwd must be an absolute path"}
	}
	for _, dir := range req.AdditionalDirectories {
		if !filepath.IsAbs(dir) {
			return acp.Session{}, &acp.ProtocolError{Message: "each additional session directory must be an absolute path"}
		}
	}
	servers := make([]acpsdk.McpServer, 0, len(req.MCPServers))
	for _, server := range req.MCPServers {
		converted, err := encodeMCPServer(server)
		if err != nil {
			return acp.Session{}, err
		}
		servers = append(servers, converted)
	}
	result, err := c.conn.NewSession(ctx, acpsdk.NewSessionRequest{
		Cwd: req.CWD, AdditionalDirectories: req.AdditionalDirectories, McpServers: servers,
	})
	if err != nil {
		return acp.Session{}, convertRPCError(err)
	}
	if result.SessionId == "" {
		return acp.Session{}, &acp.ProtocolError{Message: "session/new returned an empty sessionId"}
	}
	return acp.Session{ID: string(result.SessionId)}, nil
}

func (c *Client) Prompt(ctx context.Context, req acp.PromptRequest, handler acp.Handler) (acp.StopReason, error) {
	if req.SessionID == "" {
		return "", &acp.ProtocolError{Message: "prompt sessionId is required"}
	}
	if handler == nil {
		return "", &acp.ProtocolError{Message: "prompt handler is required"}
	}
	if len(req.Content) == 0 {
		return "", &acp.ProtocolError{Message: "prompt content is required"}
	}
	content := make([]acpsdk.ContentBlock, 0, len(req.Content))
	for _, block := range req.Content {
		converted, err := encodeContent(block)
		if err != nil {
			return "", err
		}
		content = append(content, converted)
	}

	promptCtx, cancelPrompt := context.WithCancelCause(ctx)
	state := &promptState{
		handler: handler, cancelPrompt: cancelPrompt,
		permissions: make(map[uint64]context.CancelCauseFunc),
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		cancelPrompt(acp.ErrClosed)
		return "", acp.ErrClosed
	}
	if _, exists := c.prompts[req.SessionID]; exists {
		c.mu.Unlock()
		cancelPrompt(acp.ErrPromptInProgress)
		return "", acp.ErrPromptInProgress
	}
	c.prompts[req.SessionID] = state
	c.mu.Unlock()
	defer func() {
		state.cancelPermissions()
		cancelPrompt(nil)
		c.mu.Lock()
		if c.prompts[req.SessionID] == state {
			delete(c.prompts, req.SessionID)
		}
		c.mu.Unlock()
	}()

	result, err := c.conn.Prompt(promptCtx, acpsdk.PromptRequest{
		SessionId: acpsdk.SessionId(req.SessionID), Prompt: content,
	})
	if failure := state.failure(); failure != nil {
		return "", failure
	}
	if err != nil {
		return "", convertRPCError(err)
	}
	reason := acp.StopReason(result.StopReason)
	switch reason {
	case acp.StopEndTurn, acp.StopMaxTokens, acp.StopMaxTurnRequests, acp.StopRefusal, acp.StopCancelled:
		return reason, nil
	default:
		return "", &acp.ProtocolError{Message: fmt.Sprintf("unknown prompt stopReason %q", result.StopReason)}
	}
}

func (c *Client) Cancel(ctx context.Context, sessionID string) error {
	if sessionID == "" {
		return &acp.ProtocolError{Message: "cancel sessionId is required"}
	}
	if err := c.ensureOpen(); err != nil {
		return err
	}
	c.mu.Lock()
	state := c.prompts[sessionID]
	c.mu.Unlock()
	if state != nil {
		state.cancelPermissions()
	}
	return convertRPCError(c.conn.Cancel(ctx, acpsdk.CancelNotification{SessionId: acpsdk.SessionId(sessionID)}))
}

func (c *Client) Close() error {
	var result error
	c.once.Do(func() {
		c.mu.Lock()
		c.closed = true
		states := make([]*promptState, 0, len(c.prompts))
		for _, state := range c.prompts {
			states = append(states, state)
		}
		c.mu.Unlock()
		for _, state := range states {
			state.cancelPermissions()
			state.cancelPrompt(acp.ErrClosed)
		}
		if c.closer != nil {
			result = c.closer.Close()
		}
	})
	return result
}

func (c *Client) ensureOpen() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return acp.ErrClosed
	}
	select {
	case <-c.conn.Done():
		return acp.ErrClosed
	default:
		return nil
	}
}

func (c *Client) prompt(sessionID string) *promptState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.prompts[sessionID]
}

type sdkCallbacks struct{ owner *Client }

func (s *sdkCallbacks) SessionUpdate(ctx context.Context, notification acpsdk.SessionNotification) error {
	state := s.owner.prompt(string(notification.SessionId))
	if state == nil {
		return nil
	}
	update, ok, err := decodeUpdate(notification.Update)
	if err != nil {
		state.fail(err)
		return acpsdk.NewInvalidParams(map[string]any{"error": err.Error()})
	}
	if ok {
		state.handler.OnUpdate(ctx, update)
	}
	return nil
}

func (s *sdkCallbacks) RequestPermission(ctx context.Context, request acpsdk.RequestPermissionRequest) (acpsdk.RequestPermissionResponse, error) {
	state := s.owner.prompt(string(request.SessionId))
	if state == nil {
		return acpsdk.RequestPermissionResponse{}, acpsdk.NewInvalidParams(map[string]any{"error": "permission request has no active prompt"})
	}
	converted, err := decodePermissionRequest(request)
	if err != nil {
		return acpsdk.RequestPermissionResponse{}, acpsdk.NewInvalidParams(map[string]any{"error": err.Error()})
	}
	permissionCtx, finishPermission := state.beginPermission(ctx)
	defer finishPermission()
	type result struct {
		outcome acp.PermissionOutcome
		err     error
	}
	decided := make(chan result, 1)
	go func() {
		outcome, err := state.handler.RequestPermission(permissionCtx, converted)
		decided <- result{outcome: outcome, err: err}
	}()
	var outcome acp.PermissionOutcome
	select {
	case <-permissionCtx.Done():
		outcome = acp.CancelPermission()
	case got := <-decided:
		if got.err != nil {
			outcome = acp.CancelPermission()
		} else {
			outcome = got.outcome
		}
	}
	if err := validatePermissionOutcome(converted, outcome); err != nil {
		return acpsdk.RequestPermissionResponse{}, acpsdk.NewInvalidParams(map[string]any{"error": err.Error()})
	}
	return acpsdk.RequestPermissionResponse{Outcome: encodePermissionOutcome(outcome)}, nil
}

// Gate does not advertise filesystem or terminal capabilities in initialize;
// fail closed if an agent calls them anyway.
func (*sdkCallbacks) ReadTextFile(context.Context, acpsdk.ReadTextFileRequest) (acpsdk.ReadTextFileResponse, error) {
	return acpsdk.ReadTextFileResponse{}, acpsdk.NewMethodNotFound(acpsdk.ClientMethodFsReadTextFile)
}
func (*sdkCallbacks) WriteTextFile(context.Context, acpsdk.WriteTextFileRequest) (acpsdk.WriteTextFileResponse, error) {
	return acpsdk.WriteTextFileResponse{}, acpsdk.NewMethodNotFound(acpsdk.ClientMethodFsWriteTextFile)
}
func (*sdkCallbacks) CreateTerminal(context.Context, acpsdk.CreateTerminalRequest) (acpsdk.CreateTerminalResponse, error) {
	return acpsdk.CreateTerminalResponse{}, acpsdk.NewMethodNotFound(acpsdk.ClientMethodTerminalCreate)
}
func (*sdkCallbacks) KillTerminal(context.Context, acpsdk.KillTerminalRequest) (acpsdk.KillTerminalResponse, error) {
	return acpsdk.KillTerminalResponse{}, acpsdk.NewMethodNotFound(acpsdk.ClientMethodTerminalKill)
}
func (*sdkCallbacks) TerminalOutput(context.Context, acpsdk.TerminalOutputRequest) (acpsdk.TerminalOutputResponse, error) {
	return acpsdk.TerminalOutputResponse{}, acpsdk.NewMethodNotFound(acpsdk.ClientMethodTerminalOutput)
}
func (*sdkCallbacks) ReleaseTerminal(context.Context, acpsdk.ReleaseTerminalRequest) (acpsdk.ReleaseTerminalResponse, error) {
	return acpsdk.ReleaseTerminalResponse{}, acpsdk.NewMethodNotFound(acpsdk.ClientMethodTerminalRelease)
}
func (*sdkCallbacks) WaitForTerminalExit(context.Context, acpsdk.WaitForTerminalExitRequest) (acpsdk.WaitForTerminalExitResponse, error) {
	return acpsdk.WaitForTerminalExitResponse{}, acpsdk.NewMethodNotFound(acpsdk.ClientMethodTerminalWaitForExit)
}

func encodeContent(content acp.Content) (acpsdk.ContentBlock, error) {
	switch content.Type {
	case "text":
		return acpsdk.TextBlock(content.Text), nil
	case "image":
		if content.Data == "" || content.MIMEType == "" {
			return acpsdk.ContentBlock{}, &acp.ProtocolError{Message: "image content requires data and mimeType"}
		}
		block := acpsdk.ImageBlock(content.Data, content.MIMEType)
		if content.URI != "" {
			block.Image.Uri = acpsdk.Ptr(content.URI)
		}
		return block, nil
	case "audio":
		if content.Data == "" || content.MIMEType == "" {
			return acpsdk.ContentBlock{}, &acp.ProtocolError{Message: "audio content requires data and mimeType"}
		}
		return acpsdk.AudioBlock(content.Data, content.MIMEType), nil
	case "resource_link":
		if content.URI == "" {
			return acpsdk.ContentBlock{}, &acp.ProtocolError{Message: "resource_link content requires uri"}
		}
		return acpsdk.ResourceLinkBlock(content.URI, content.URI), nil
	default:
		return acpsdk.ContentBlock{}, &acp.ProtocolError{Message: fmt.Sprintf("unsupported content type %q", content.Type)}
	}
}

func decodeContent(content acpsdk.ContentBlock) (acp.Content, bool) {
	switch {
	case content.Text != nil:
		return acp.Text(content.Text.Text), true
	case content.Image != nil:
		result := acp.Content{Type: "image", Data: content.Image.Data, MIMEType: content.Image.MimeType}
		if content.Image.Uri != nil {
			result.URI = *content.Image.Uri
		}
		return result, true
	case content.Audio != nil:
		return acp.Content{Type: "audio", Data: content.Audio.Data, MIMEType: content.Audio.MimeType}, true
	case content.ResourceLink != nil:
		return acp.Content{Type: "resource_link", URI: content.ResourceLink.Uri, MIMEType: pointerValue(content.ResourceLink.MimeType)}, true
	default:
		return acp.Content{}, false
	}
}

func decodeUpdate(update acpsdk.SessionUpdate) (acp.Update, bool, error) {
	switch {
	case update.AgentMessageChunk != nil:
		return decodeContentUpdate(acp.UpdateAgentMessage, update.AgentMessageChunk.MessageId, update.AgentMessageChunk.Content)
	case update.AgentThoughtChunk != nil:
		return decodeContentUpdate(acp.UpdateAgentThought, update.AgentThoughtChunk.MessageId, update.AgentThoughtChunk.Content)
	case update.ToolCall != nil:
		tool := decodeToolCallStart(*update.ToolCall)
		if tool.ID == "" {
			return acp.Update{}, false, &acp.ProtocolError{Message: "tool call update is missing toolCallId"}
		}
		return acp.Update{Kind: acp.UpdateToolCall, ToolCall: tool}, true, nil
	case update.ToolCallUpdate != nil:
		tool := decodeToolCallUpdate(*update.ToolCallUpdate)
		if tool.ID == "" {
			return acp.Update{}, false, &acp.ProtocolError{Message: "tool call update is missing toolCallId"}
		}
		return acp.Update{Kind: acp.UpdateToolCallDiff, ToolCall: tool}, true, nil
	default:
		return acp.Update{}, false, nil
	}
}

func decodeContentUpdate(kind acp.UpdateKind, messageID *string, content acpsdk.ContentBlock) (acp.Update, bool, error) {
	converted, ok := decodeContent(content)
	if !ok {
		return acp.Update{}, false, &acp.ProtocolError{Message: "unsupported content in session update"}
	}
	return acp.Update{Kind: kind, MessageID: pointerValue(messageID), Content: converted}, true, nil
}

func decodeToolCallStart(tool acpsdk.SessionUpdateToolCall) acp.ToolCall {
	return decodeToolCall(string(tool.ToolCallId), tool.Title, string(tool.Kind), string(tool.Status), tool.Content, tool.Locations)
}

func decodeToolCallUpdate(tool acpsdk.SessionToolCallUpdate) acp.ToolCall {
	return decodeToolCall(string(tool.ToolCallId), pointerValue(tool.Title), stringPointerValue(tool.Kind), stringPointerValue(tool.Status), tool.Content, tool.Locations)
}

func decodeToolCall(id, title, kind, status string, content []acpsdk.ToolCallContent, locations []acpsdk.ToolCallLocation) acp.ToolCall {
	result := acp.ToolCall{ID: id, Title: title, Kind: kind, Status: status}
	for _, item := range content {
		converted := acp.ToolCallContent{}
		switch {
		case item.Content != nil:
			converted.Type = "content"
			converted.Content, _ = decodeContent(item.Content.Content)
		case item.Diff != nil:
			converted.Type = "diff"
			converted.Path = item.Diff.Path
			converted.OldText = pointerValue(item.Diff.OldText)
			converted.NewText = item.Diff.NewText
		case item.Terminal != nil:
			converted.Type = "terminal"
			converted.Terminal = item.Terminal.TerminalId
		}
		result.Content = append(result.Content, converted)
	}
	for _, location := range locations {
		converted := acp.ToolCallLocation{Path: location.Path}
		if location.Line != nil && *location.Line >= 0 && uint64(*location.Line) <= math.MaxUint32 {
			line := uint32(*location.Line)
			converted.Line = &line
		}
		result.Locations = append(result.Locations, converted)
	}
	return result
}

func decodePermissionRequest(request acpsdk.RequestPermissionRequest) (acp.PermissionRequest, error) {
	if request.SessionId == "" || request.ToolCall.ToolCallId == "" || len(request.Options) == 0 {
		return acp.PermissionRequest{}, &acp.ProtocolError{Message: "incomplete permission request"}
	}
	result := acp.PermissionRequest{
		SessionID: string(request.SessionId), ToolCall: decodeToolCallUpdateFromPermission(request.ToolCall),
		Options: make([]acp.PermissionOption, 0, len(request.Options)),
	}
	seen := make(map[string]struct{}, len(request.Options))
	for _, option := range request.Options {
		id := string(option.OptionId)
		kind := string(option.Kind)
		if id == "" || option.Name == "" || !validPermissionKind(kind) {
			return acp.PermissionRequest{}, &acp.ProtocolError{Message: "invalid permission option"}
		}
		if _, exists := seen[id]; exists {
			return acp.PermissionRequest{}, &acp.ProtocolError{Message: "duplicate permission optionId"}
		}
		seen[id] = struct{}{}
		result.Options = append(result.Options, acp.PermissionOption{ID: id, Name: option.Name, Kind: kind})
	}
	return result, nil
}

func decodeToolCallUpdateFromPermission(tool acpsdk.ToolCallUpdate) acp.ToolCall {
	return decodeToolCall(string(tool.ToolCallId), pointerValue(tool.Title), stringPointerValue(tool.Kind), stringPointerValue(tool.Status), tool.Content, tool.Locations)
}

func validatePermissionOutcome(request acp.PermissionRequest, outcome acp.PermissionOutcome) error {
	if outcome.Cancelled {
		if outcome.OptionID != "" {
			return &acp.ProtocolError{Message: "cancelled permission outcome must not include optionId"}
		}
		return nil
	}
	for _, option := range request.Options {
		if option.ID == outcome.OptionID {
			return nil
		}
	}
	return &acp.ProtocolError{Message: "permission outcome selected an unknown optionId"}
}

func encodePermissionOutcome(outcome acp.PermissionOutcome) acpsdk.RequestPermissionOutcome {
	if outcome.Cancelled {
		return acpsdk.RequestPermissionOutcome{Cancelled: &acpsdk.RequestPermissionOutcomeCancelled{Outcome: "cancelled"}}
	}
	return acpsdk.RequestPermissionOutcome{Selected: &acpsdk.RequestPermissionOutcomeSelected{
		Outcome: "selected", OptionId: acpsdk.PermissionOptionId(outcome.OptionID),
	}}
}

func encodeMCPServer(server acp.MCPServer) (acpsdk.McpServer, error) {
	if server.Name == "" {
		return acpsdk.McpServer{}, &acp.ProtocolError{Message: "MCP server name is required"}
	}
	switch server.Type {
	case "", "stdio":
		if !filepath.IsAbs(server.Command) {
			return acpsdk.McpServer{}, &acp.ProtocolError{Message: "stdio MCP command must be an absolute path"}
		}
		result := &acpsdk.McpServerStdio{Name: server.Name, Command: server.Command, Args: server.Args}
		for _, variable := range server.Env {
			result.Env = append(result.Env, acpsdk.EnvVariable{Name: variable.Name, Value: variable.Value})
		}
		return acpsdk.McpServer{Stdio: result}, nil
	case "http", "sse":
		parsed, err := url.ParseRequestURI(server.URL)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" {
			return acpsdk.McpServer{}, &acp.ProtocolError{Message: "HTTP/SSE MCP server URL must be absolute"}
		}
		headers := make([]acpsdk.HttpHeader, 0, len(server.Headers))
		for _, header := range server.Headers {
			headers = append(headers, acpsdk.HttpHeader{Name: header.Name, Value: header.Value})
		}
		if server.Type == "http" {
			return acpsdk.McpServer{Http: &acpsdk.McpServerHttpInline{Name: server.Name, Url: server.URL, Headers: headers, Type: "http"}}, nil
		}
		return acpsdk.McpServer{Sse: &acpsdk.McpServerSseInline{Name: server.Name, Url: server.URL, Headers: headers, Type: "sse"}}, nil
	default:
		return acpsdk.McpServer{}, &acp.ProtocolError{Message: fmt.Sprintf("unsupported MCP transport %q", server.Type)}
	}
}

func validPermissionKind(kind string) bool {
	switch kind {
	case "allow_once", "allow_always", "reject_once", "reject_always":
		return true
	default:
		return false
	}
}

func convertRPCError(err error) error {
	if err == nil {
		return nil
	}
	var requestErr *acpsdk.RequestError
	if errors.As(err, &requestErr) {
		return &acp.RPCError{Code: requestErr.Code, Message: requestErr.Message}
	}
	return err
}

func pointerValue[T any](value *T) T {
	if value == nil {
		var zero T
		return zero
	}
	return *value
}

func stringPointerValue[T ~string](value *T) string {
	if value == nil {
		return ""
	}
	return string(*value)
}

var (
	_ acp.Client    = (*Client)(nil)
	_ acpsdk.Client = (*sdkCallbacks)(nil)
)
