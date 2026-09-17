// Package acp defines the protocol-independent boundary between Gate's
// runtime and an Agent Client Protocol implementation.
package acp

import (
	"context"
	"errors"
	"fmt"
)

const ProtocolVersion = 1

var (
	ErrClosed           = errors.New("acp: client is closed")
	ErrPromptInProgress = errors.New("acp: a prompt is already in progress for the session")
)

// Client owns one connection to an ACP agent. Implementations must remain
// alive independently of an individual Prompt call.
type Client interface {
	Initialize(context.Context, InitializeRequest) (InitializeResult, error)
	NewSession(context.Context, NewSessionRequest) (Session, error)
	Prompt(context.Context, PromptRequest, Handler) (StopReason, error)
	Cancel(context.Context, string) error
	Close() error
}

type InitializeRequest struct {
	ClientInfo ClientInfo
}

type InitializeResult struct {
	ProtocolVersion int
	AgentInfo       AgentInfo
	Capabilities    AgentCapabilities
}

type ClientInfo struct {
	Name    string
	Title   string
	Version string
}

type AgentInfo struct {
	Name    string
	Title   string
	Version string
}

type AgentCapabilities struct {
	LoadSession     bool
	PromptImage     bool
	PromptAudio     bool
	EmbeddedContext bool
	MCPHTTP         bool
	MCPSSE          bool
}

type NewSessionRequest struct {
	CWD                   string
	AdditionalDirectories []string
	MCPServers            []MCPServer
}

type Session struct {
	ID string
}

type MCPServer struct {
	Type    string
	Name    string
	Command string
	Args    []string
	Env     []EnvVariable
	URL     string
	Headers []Header
}

type EnvVariable struct {
	Name  string
	Value string
}

type Header struct {
	Name  string
	Value string
}

type PromptRequest struct {
	SessionID string
	Content   []Content
}

type Content struct {
	Type     string
	Text     string
	Data     string
	MIMEType string
	URI      string
}

func Text(text string) Content { return Content{Type: "text", Text: text} }

type StopReason string

const (
	StopEndTurn         StopReason = "end_turn"
	StopMaxTokens       StopReason = "max_tokens"
	StopMaxTurnRequests StopReason = "max_turn_requests"
	StopRefusal         StopReason = "refusal"
	StopCancelled       StopReason = "cancelled"
)

type UpdateKind string

const (
	UpdateAgentMessage UpdateKind = "agent_message_chunk"
	UpdateAgentThought UpdateKind = "agent_thought_chunk"
	UpdateToolCall     UpdateKind = "tool_call"
	UpdateToolCallDiff UpdateKind = "tool_call_update"
)

// Update is the sanitized, SDK-independent representation consumed by the
// runtime. Sequence numbers belong to the A2A task event log and are therefore
// deliberately not assigned by the ACP adapter.
type Update struct {
	Kind      UpdateKind
	MessageID string
	Content   Content
	ToolCall  ToolCall
}

type ToolCall struct {
	ID        string
	Title     string
	Kind      string
	Status    string
	Content   []ToolCallContent
	Locations []ToolCallLocation
}

type ToolCallContent struct {
	Type     string
	Content  Content
	Path     string
	OldText  string
	NewText  string
	Terminal string
}

type ToolCallLocation struct {
	Path string
	Line *uint32
}

// Handler receives observations for one prompt turn. Implementations must be
// safe to call from the ACP connection's background goroutines.
type Handler interface {
	OnUpdate(context.Context, Update)
	RequestPermission(context.Context, PermissionRequest) (PermissionOutcome, error)
}

type PermissionRequest struct {
	SessionID string
	ToolCall  ToolCall
	Options   []PermissionOption
}

type PermissionOption struct {
	ID   string
	Name string
	Kind string
}

type PermissionOutcome struct {
	Cancelled bool
	OptionID  string
}

func SelectPermission(optionID string) PermissionOutcome {
	return PermissionOutcome{OptionID: optionID}
}

func CancelPermission() PermissionOutcome {
	return PermissionOutcome{Cancelled: true}
}

// RPCError is an error returned by the ACP peer.
type RPCError struct {
	Code    int
	Message string
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("acp: rpc error %d: %s", e.Code, e.Message)
}

type ProtocolError struct {
	Message string
}

func (e *ProtocolError) Error() string { return "acp: protocol error: " + e.Message }
