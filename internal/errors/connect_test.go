package errors_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"connectrpc.com/connect/v2"
	protocol "github.com/a2aproject/a2a-go/v2/a2a"
	gateerrors "github.com/anra-studio/gate/internal/errors"
)

func TestToConnectPreservesTransportErrors(t *testing.T) {
	rpcErr := connect.NewError(connect.CodeResourceExhausted, "retry later").WithCause(protocol.ErrTaskNotFound)
	for _, err := range []error{
		nil,
		rpcErr,
		fmt.Errorf("wrapped: %w", rpcErr),
		fmt.Errorf("canceled: %w", context.Canceled),
		fmt.Errorf("deadline: %w", context.DeadlineExceeded),
	} {
		if got := gateerrors.ToConnect(err); got != err {
			t.Errorf("error %v was replaced with %v", err, got)
		}
	}
}

func TestToConnectMapsWrappedDomainError(t *testing.T) {
	err := fmt.Errorf("lookup: %w", protocol.ErrTaskNotFound)
	got := gateerrors.ToConnect(err)
	if connect.CodeOf(got) != connect.CodeNotFound {
		t.Fatalf("code = %v, want NotFound", connect.CodeOf(got))
	}
	if !errors.Is(got, err) {
		t.Fatal("original cause was lost")
	}
}
