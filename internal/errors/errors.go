// Package errors classifies Gate failures while preserving their original causes.
package errors

import (
	"context"
	"errors"

	"connectrpc.com/connect/v2"
	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/anra-studio/gate/internal/acp"
)

// Error wraps an A2A or ACP failure with its public message and RPC status.
// Use errors.Is and errors.AsType to inspect the original failure.
type Error struct {
	cause   error
	kind    error
	code    connect.Code
	message string
}

func (e *Error) Error() string { return e.message }

// Unwrap exposes both the original failure and its A2A classification.
// ACP wire codes describe the upstream request, so they are not reused as
// statuses for the caller's A2A request.
func (e *Error) Unwrap() []error { return []error{e.cause, e.kind} }

func (e *Error) Code() connect.Code { return e.code }

var classifications = []struct {
	kind error
	code connect.Code
}{
	{a2a.ErrParseError, connect.CodeInvalidArgument},
	{a2a.ErrInvalidRequest, connect.CodeInvalidArgument},
	{a2a.ErrInvalidParams, connect.CodeInvalidArgument},
	{a2a.ErrUnsupportedContentType, connect.CodeInvalidArgument},
	{a2a.ErrTaskNotFound, connect.CodeNotFound},
	{a2a.ErrTaskNotCancelable, connect.CodeFailedPrecondition},
	{a2a.ErrUnsupportedOperation, connect.CodeFailedPrecondition},
	{a2a.ErrPushNotificationNotSupported, connect.CodeFailedPrecondition},
	{a2a.ErrExtendedCardNotConfigured, connect.CodeFailedPrecondition},
	{a2a.ErrExtensionSupportRequired, connect.CodeFailedPrecondition},
	{a2a.ErrVersionNotSupported, connect.CodeFailedPrecondition},
	{a2a.ErrUnauthenticated, connect.CodeUnauthenticated},
	{a2a.ErrUnauthorized, connect.CodePermissionDenied},
	{a2a.ErrMethodNotFound, connect.CodeUnimplemented},
	{a2a.ErrInvalidAgentResponse, connect.CodeInternal},
	{a2a.ErrInternalError, connect.CodeInternal},
	{a2a.ErrServerError, connect.CodeInternal},
	{context.Canceled, connect.CodeCanceled},
	{context.DeadlineExceeded, connect.CodeDeadlineExceeded},
}

// Wrap classifies a failure without discarding its error chain. It is safe to
// call repeatedly; wrapping with fmt.Errorf still preserves that outer context.
func Wrap(err error) *Error {
	if err == nil {
		return nil
	}
	if existing, ok := err.(*Error); ok && existing != nil {
		return existing
	}
	if existing, ok := errors.AsType[*Error](err); ok && existing != nil {
		return &Error{cause: err, kind: existing.kind, code: existing.code, message: existing.message}
	}
	for _, classification := range classifications {
		if errors.Is(err, classification.kind) {
			return &Error{cause: err, kind: classification.kind, code: classification.code, message: err.Error()}
		}
	}
	message := a2a.ErrInternalError.Error()
	if rpcErr, ok := errors.AsType[*acp.RPCError](err); ok && rpcErr != nil {
		message = "ACP agent request failed."
	} else if protocolErr, ok := errors.AsType[*acp.ProtocolError](err); ok && protocolErr != nil {
		message = "ACP agent protocol error."
	}
	return &Error{cause: err, kind: a2a.ErrInternalError, code: connect.CodeInternal, message: message}
}
