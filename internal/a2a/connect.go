package a2a

import (
	"context"
	"errors"
	"strings"

	"connectrpc.com/connect/v2"
	protocol "github.com/a2aproject/a2a-go/v2/a2a"
	a2apb "github.com/a2aproject/a2a-go/v2/a2apb/v1"
	"github.com/a2aproject/a2a-go/v2/a2apb/v1/pbconv"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	a2apbconnect "github.com/anra-studio/gate/gen/a2a/a2apbconnect"
	"google.golang.org/protobuf/types/known/emptypb"
)

type connectAdapter struct {
	handler a2asrv.RequestHandler
}

var _ a2apbconnect.A2AServiceHandler = (*connectAdapter)(nil)

func (a *connectAdapter) SendMessage(ctx context.Context, request *a2apb.SendMessageRequest) (*a2apb.SendMessageResponse, error) {
	req, err := pbconv.FromProtoSendMessageRequest(request)
	if err != nil {
		return nil, invalidArgument(err)
	}
	ctx, call := connectCallContext(ctx)
	result, err := a.handler.SendMessage(ctx, req)
	if err != nil {
		return nil, connectError(err)
	}
	msg, err := pbconv.ToProtoSendMessageResponse(result)
	if err != nil {
		return nil, internalError(err)
	}
	return responseWithExtensions(ctx, msg, call), nil
}

func (a *connectAdapter) SendStreamingMessage(
	ctx context.Context,
	request *a2apb.SendMessageRequest,
	stream a2apbconnect.A2AServiceSendStreamingMessageServerStream,
) error {
	req, err := pbconv.FromProtoSendMessageRequest(request)
	if err != nil {
		return invalidArgument(err)
	}
	ctx, call := connectCallContext(ctx)
	for event, eventErr := range a.handler.SendStreamingMessage(ctx, req) {
		if eventErr != nil {
			return connectError(eventErr)
		}
		msg, convertErr := pbconv.ToProtoStreamResponse(event)
		if convertErr != nil {
			return internalError(convertErr)
		}
		if sendErr := stream.Send(msg); sendErr != nil {
			return connect.NewError(connect.CodeAborted, sendErr.Error()).WithCause(sendErr)
		}
	}
	setActiveExtensions(ctx, call)
	return nil
}

func (a *connectAdapter) GetTask(ctx context.Context, request *a2apb.GetTaskRequest) (*a2apb.Task, error) {
	req, err := pbconv.FromProtoGetTaskRequest(request)
	if err != nil {
		return nil, invalidArgument(err)
	}
	ctx, call := connectCallContext(ctx)
	task, err := a.handler.GetTask(ctx, req)
	if err != nil {
		return nil, connectError(err)
	}
	msg, err := pbconv.ToProtoTask(task)
	if err != nil {
		return nil, internalError(err)
	}
	return responseWithExtensions(ctx, msg, call), nil
}

func (a *connectAdapter) ListTasks(ctx context.Context, request *a2apb.ListTasksRequest) (*a2apb.ListTasksResponse, error) {
	req, err := pbconv.FromProtoListTasksRequest(request)
	if err != nil {
		return nil, invalidArgument(err)
	}
	ctx, call := connectCallContext(ctx)
	result, err := a.handler.ListTasks(ctx, req)
	if err != nil {
		return nil, connectError(err)
	}
	msg, err := pbconv.ToProtoListTasksResponse(result)
	if err != nil {
		return nil, internalError(err)
	}
	return responseWithExtensions(ctx, msg, call), nil
}

func (a *connectAdapter) CancelTask(ctx context.Context, request *a2apb.CancelTaskRequest) (*a2apb.Task, error) {
	req, err := pbconv.FromProtoCancelTaskRequest(request)
	if err != nil {
		return nil, invalidArgument(err)
	}
	ctx, call := connectCallContext(ctx)
	task, err := a.handler.CancelTask(ctx, req)
	if err != nil {
		return nil, connectError(err)
	}
	msg, err := pbconv.ToProtoTask(task)
	if err != nil {
		return nil, internalError(err)
	}
	return responseWithExtensions(ctx, msg, call), nil
}

func (a *connectAdapter) SubscribeToTask(ctx context.Context, request *a2apb.SubscribeToTaskRequest, stream a2apbconnect.A2AServiceSubscribeToTaskServerStream) error {
	req, err := pbconv.FromProtoSubscribeToTaskRequest(request)
	if err != nil {
		return invalidArgument(err)
	}
	ctx, call := connectCallContext(ctx)
	for event, eventErr := range a.handler.SubscribeToTask(ctx, req) {
		if eventErr != nil {
			return connectError(eventErr)
		}
		msg, convertErr := pbconv.ToProtoStreamResponse(event)
		if convertErr != nil {
			return internalError(convertErr)
		}
		if sendErr := stream.Send(msg); sendErr != nil {
			return connect.NewError(connect.CodeAborted, sendErr.Error()).WithCause(sendErr)
		}
	}
	setActiveExtensions(ctx, call)
	return nil
}

func (a *connectAdapter) CreateTaskPushNotificationConfig(ctx context.Context, request *a2apb.TaskPushNotificationConfig) (*a2apb.TaskPushNotificationConfig, error) {
	req, err := pbconv.FromProtoTaskPushConfig(request)
	if err != nil {
		return nil, invalidArgument(err)
	}
	ctx, call := connectCallContext(ctx)
	result, err := a.handler.CreateTaskPushConfig(ctx, req)
	if err != nil {
		return nil, connectError(err)
	}
	msg, err := pbconv.ToProtoTaskPushConfig(result)
	if err != nil {
		return nil, internalError(err)
	}
	return responseWithExtensions(ctx, msg, call), nil
}

func (a *connectAdapter) GetTaskPushNotificationConfig(ctx context.Context, request *a2apb.GetTaskPushNotificationConfigRequest) (*a2apb.TaskPushNotificationConfig, error) {
	req, err := pbconv.FromProtoGetTaskPushConfigRequest(request)
	if err != nil {
		return nil, invalidArgument(err)
	}
	ctx, call := connectCallContext(ctx)
	result, err := a.handler.GetTaskPushConfig(ctx, req)
	if err != nil {
		return nil, connectError(err)
	}
	msg, err := pbconv.ToProtoTaskPushConfig(result)
	if err != nil {
		return nil, internalError(err)
	}
	return responseWithExtensions(ctx, msg, call), nil
}

func (a *connectAdapter) ListTaskPushNotificationConfigs(ctx context.Context, request *a2apb.ListTaskPushNotificationConfigsRequest) (*a2apb.ListTaskPushNotificationConfigsResponse, error) {
	req, err := pbconv.FromProtoListTaskPushConfigRequest(request)
	if err != nil {
		return nil, invalidArgument(err)
	}
	ctx, call := connectCallContext(ctx)
	result, err := a.handler.ListTaskPushConfigs(ctx, req)
	if err != nil {
		return nil, connectError(err)
	}
	msg, err := pbconv.ToProtoListTaskPushConfigResponse(result)
	if err != nil {
		return nil, internalError(err)
	}
	return responseWithExtensions(ctx, msg, call), nil
}

func (a *connectAdapter) GetExtendedAgentCard(ctx context.Context, request *a2apb.GetExtendedAgentCardRequest) (*a2apb.AgentCard, error) {
	req, err := pbconv.FromProtoGetExtendedAgentCardRequest(request)
	if err != nil {
		return nil, invalidArgument(err)
	}
	ctx, call := connectCallContext(ctx)
	result, err := a.handler.GetExtendedAgentCard(ctx, req)
	if err != nil {
		return nil, connectError(err)
	}
	msg, err := pbconv.ToProtoAgentCard(result)
	if err != nil {
		return nil, internalError(err)
	}
	return responseWithExtensions(ctx, msg, call), nil
}

func (a *connectAdapter) DeleteTaskPushNotificationConfig(ctx context.Context, request *a2apb.DeleteTaskPushNotificationConfigRequest) (*emptypb.Empty, error) {
	req, err := pbconv.FromProtoDeleteTaskPushConfigRequest(request)
	if err != nil {
		return nil, invalidArgument(err)
	}
	ctx, call := connectCallContext(ctx)
	if err := a.handler.DeleteTaskPushConfig(ctx, req); err != nil {
		return nil, connectError(err)
	}
	return responseWithExtensions(ctx, &emptypb.Empty{}, call), nil
}

func connectCallContext(ctx context.Context) (context.Context, *a2asrv.CallContext) {
	headers := make(map[string][]string)
	if info, ok := connect.CallInfoForServerContext(ctx); ok {
		for name, values := range info.RequestHeader().All() {
			headers[name] = values
		}
	}
	return a2asrv.NewCallContext(ctx, a2asrv.NewServiceParams(headers))
}

func responseWithExtensions[T any](ctx context.Context, msg *T, call *a2asrv.CallContext) *T {
	setActiveExtensions(ctx, call)
	return msg
}

func setActiveExtensions(ctx context.Context, call *a2asrv.CallContext) {
	if info, ok := connect.CallInfoForServerContext(ctx); ok {
		for _, uri := range call.Extensions().ActivatedURIs() {
			info.ResponseTrailer().Add(protocol.SvcParamExtensions, uri)
		}
	}
}

func invalidArgument(err error) error {
	return connect.Errorf(connect.CodeInvalidArgument, "failed to convert request: %v", err).WithCause(err)
}

func internalError(err error) error {
	return connect.Errorf(connect.CodeInternal, "failed to convert response: %v", err).WithCause(err)
}

func connectError(err error) error {
	code := connect.CodeInternal
	switch {
	case errors.Is(err, protocol.ErrParseError),
		errors.Is(err, protocol.ErrInvalidRequest),
		errors.Is(err, protocol.ErrInvalidParams),
		errors.Is(err, protocol.ErrUnsupportedContentType):
		code = connect.CodeInvalidArgument
	case errors.Is(err, protocol.ErrTaskNotFound):
		code = connect.CodeNotFound
	case errors.Is(err, protocol.ErrTaskNotCancelable):
		code = connect.CodeFailedPrecondition
	case errors.Is(err, protocol.ErrUnsupportedOperation),
		errors.Is(err, protocol.ErrPushNotificationNotSupported),
		errors.Is(err, protocol.ErrExtendedCardNotConfigured),
		errors.Is(err, protocol.ErrExtensionSupportRequired),
		errors.Is(err, protocol.ErrVersionNotSupported):
		code = connect.CodeFailedPrecondition
	case errors.Is(err, protocol.ErrUnauthenticated):
		code = connect.CodeUnauthenticated
	case errors.Is(err, protocol.ErrUnauthorized):
		code = connect.CodePermissionDenied
	case errors.Is(err, protocol.ErrMethodNotFound):
		code = connect.CodeUnimplemented
	case errors.Is(err, context.Canceled):
		code = connect.CodeCanceled
	case errors.Is(err, context.DeadlineExceeded):
		code = connect.CodeDeadlineExceeded
	}
	message := strings.TrimSpace(err.Error())
	if message == "" {
		message = code.String()
	}
	return connect.NewError(code, message).WithCause(err)
}
