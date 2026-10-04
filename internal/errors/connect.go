package errors

import (
	"context"
	"errors"

	"connectrpc.com/connect/v2"
)

// ToConnect converts domain failures at the RPC boundary. Existing Connect
// errors keep their details and remote marker; context errors are handled by
// Connect's transport.
func ToConnect(err error) error {
	if err == nil {
		return nil
	}
	if rpcErr, ok := errors.AsType[*connect.Error](err); ok && rpcErr != nil {
		return err
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	failure := Wrap(err)
	return connect.NewError(failure.Code(), failure.Error()).WithCause(failure)
}

func InvalidRequest(err error) error {
	return connect.Errorf(connect.CodeInvalidArgument, "failed to convert request: %v", err).WithCause(err)
}

func InvalidResponse(err error) error {
	return connect.NewError(connect.CodeInternal, "failed to convert response").WithCause(err)
}
