package errors_test

import (
	"errors"
	"fmt"
	"testing"

	"connectrpc.com/connect/v2"
	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/anra-studio/gate/internal/acp"
	gateerrors "github.com/anra-studio/gate/internal/errors"
)

func TestWrapPreservesACPFailureAndAddsA2AClassification(t *testing.T) {
	rpcErr := &acp.RPCError{Code: -32602, Message: "private upstream data"}
	protocolErr := &acp.ProtocolError{Message: "private wire data"}
	for _, cause := range []error{rpcErr, protocolErr, acp.ErrClosed} {
		err := fmt.Errorf("cancel prompt: %w", cause)
		failure := gateerrors.Wrap(err)
		if !errors.Is(failure, cause) || !errors.Is(failure, a2a.ErrInternalError) {
			t.Fatalf("lost cause or A2A classification: %v", failure)
		}
		if failure.Code() != connect.CodeInternal {
			t.Fatalf("upstream error code = %v, want Internal", failure.Code())
		}
		converted := gateerrors.ToConnect(failure)
		if !errors.Is(converted, cause) || connect.CodeOf(converted) != connect.CodeInternal {
			t.Fatalf("RPC conversion lost cause or code: %v", converted)
		}
	}
	wrapped := gateerrors.Wrap(rpcErr)
	if got, ok := errors.AsType[*acp.RPCError](wrapped); !ok || got != rpcErr {
		t.Fatal("ACP RPC error cannot be asserted through wrapper")
	}
	if got, ok := errors.AsType[*acp.ProtocolError](gateerrors.Wrap(protocolErr)); !ok || got != protocolErr {
		t.Fatal("ACP protocol error cannot be asserted through wrapper")
	}
	if wrapped.Error() != "ACP agent request failed." {
		t.Fatalf("unsafe public message: %q", wrapped.Error())
	}
}

func TestWrapPreservesA2AErrorDetails(t *testing.T) {
	cause := a2a.NewError(a2a.ErrTaskNotFound, "missing task").WithDetails(map[string]any{"taskId": "task-1"})
	failure := gateerrors.Wrap(fmt.Errorf("lookup: %w", cause))
	if !errors.Is(failure, a2a.ErrTaskNotFound) || failure.Code() != connect.CodeNotFound {
		t.Fatal("lost A2A classification")
	}
	if got, ok := errors.AsType[*a2a.Error](failure); !ok || got != cause {
		t.Fatal("original A2A error and details were lost")
	}
	if gateerrors.Wrap(failure) != failure {
		t.Fatal("repeated wrapping replaced the error")
	}
	outer := fmt.Errorf("outer: %w", failure)
	if !errors.Is(gateerrors.Wrap(outer), outer) {
		t.Fatal("repeated wrapping lost outer context")
	}
	if gateerrors.Wrap(nil) != nil {
		t.Fatal("Wrap(nil) must return nil")
	}
}
