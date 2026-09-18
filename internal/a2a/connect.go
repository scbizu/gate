package a2a

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"connectrpc.com/connect"
	protocol "github.com/a2aproject/a2a-go/v2/a2a"
	a2apb "github.com/a2aproject/a2a-go/v2/a2apb/v1"
	"github.com/a2aproject/a2a-go/v2/a2apb/v1/pbconv"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/emptypb"
)

type connectAdapter struct {
	handler a2asrv.RequestHandler
}

func newConnectHandler(handler a2asrv.RequestHandler, options ...connect.HandlerOption) (http.Handler, error) {
	service := a2apb.File_a2av1_proto.Services().ByName("A2AService")
	if service == nil {
		return nil, errors.New("a2a: protobuf service descriptor is unavailable")
	}
	method := func(name string) connect.HandlerOption {
		return connect.WithSchema(service.Methods().ByName(protoreflectName(name)))
	}
	opts := func(name string) []connect.HandlerOption {
		return append([]connect.HandlerOption{method(name)}, options...)
	}

	adapter := &connectAdapter{handler: handler}
	routes := map[string]http.Handler{
		a2apb.A2AService_SendMessage_FullMethodName: connect.NewUnaryHandler(
			a2apb.A2AService_SendMessage_FullMethodName, adapter.sendMessage,
			opts("SendMessage")...,
		),
		a2apb.A2AService_SendStreamingMessage_FullMethodName: connect.NewServerStreamHandler(
			a2apb.A2AService_SendStreamingMessage_FullMethodName, adapter.sendStreamingMessage,
			opts("SendStreamingMessage")...,
		),
		a2apb.A2AService_GetTask_FullMethodName: connect.NewUnaryHandler(
			a2apb.A2AService_GetTask_FullMethodName, adapter.getTask,
			opts("GetTask")...,
		),
		a2apb.A2AService_ListTasks_FullMethodName: connect.NewUnaryHandler(
			a2apb.A2AService_ListTasks_FullMethodName, adapter.listTasks,
			opts("ListTasks")...,
		),
		a2apb.A2AService_CancelTask_FullMethodName: connect.NewUnaryHandler(
			a2apb.A2AService_CancelTask_FullMethodName, adapter.cancelTask,
			opts("CancelTask")...,
		),
		a2apb.A2AService_SubscribeToTask_FullMethodName: connect.NewServerStreamHandler(
			a2apb.A2AService_SubscribeToTask_FullMethodName, adapter.subscribeToTask,
			opts("SubscribeToTask")...,
		),
		a2apb.A2AService_CreateTaskPushNotificationConfig_FullMethodName: connect.NewUnaryHandler(
			a2apb.A2AService_CreateTaskPushNotificationConfig_FullMethodName, adapter.createTaskPushConfig,
			opts("CreateTaskPushNotificationConfig")...,
		),
		a2apb.A2AService_GetTaskPushNotificationConfig_FullMethodName: connect.NewUnaryHandler(
			a2apb.A2AService_GetTaskPushNotificationConfig_FullMethodName, adapter.getTaskPushConfig,
			opts("GetTaskPushNotificationConfig")...,
		),
		a2apb.A2AService_ListTaskPushNotificationConfigs_FullMethodName: connect.NewUnaryHandler(
			a2apb.A2AService_ListTaskPushNotificationConfigs_FullMethodName, adapter.listTaskPushConfigs,
			opts("ListTaskPushNotificationConfigs")...,
		),
		a2apb.A2AService_GetExtendedAgentCard_FullMethodName: connect.NewUnaryHandler(
			a2apb.A2AService_GetExtendedAgentCard_FullMethodName, adapter.getExtendedAgentCard,
			opts("GetExtendedAgentCard")...,
		),
		a2apb.A2AService_DeleteTaskPushNotificationConfig_FullMethodName: connect.NewUnaryHandler(
			a2apb.A2AService_DeleteTaskPushNotificationConfig_FullMethodName, adapter.deleteTaskPushConfig,
			opts("DeleteTaskPushNotificationConfig")...,
		),
	}

	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		handler, ok := routes[request.URL.Path]
		if !ok {
			http.NotFound(response, request)
			return
		}
		handler.ServeHTTP(response, request)
	}), nil
}

func protoreflectName(name string) protoreflect.Name { return protoreflect.Name(name) }

func (a *connectAdapter) sendMessage(ctx context.Context, request *connect.Request[a2apb.SendMessageRequest]) (*connect.Response[a2apb.SendMessageResponse], error) {
	req, err := pbconv.FromProtoSendMessageRequest(request.Msg)
	if err != nil {
		return nil, invalidArgument(err)
	}
	ctx, call := connectCallContext(ctx, request.Header())
	result, err := a.handler.SendMessage(ctx, req)
	if err != nil {
		return nil, connectError(err)
	}
	msg, err := pbconv.ToProtoSendMessageResponse(result)
	if err != nil {
		return nil, internalError(err)
	}
	return responseWithExtensions(msg, call), nil
}

func (a *connectAdapter) sendStreamingMessage(ctx context.Context, request *connect.Request[a2apb.SendMessageRequest], stream *connect.ServerStream[a2apb.StreamResponse]) error {
	req, err := pbconv.FromProtoSendMessageRequest(request.Msg)
	if err != nil {
		return invalidArgument(err)
	}
	ctx, call := connectCallContext(ctx, request.Header())
	for event, eventErr := range a.handler.SendStreamingMessage(ctx, req) {
		if eventErr != nil {
			return connectError(eventErr)
		}
		msg, convertErr := pbconv.ToProtoStreamResponse(event)
		if convertErr != nil {
			return internalError(convertErr)
		}
		if sendErr := stream.Send(msg); sendErr != nil {
			return connect.NewError(connect.CodeAborted, sendErr)
		}
	}
	setActiveExtensions(stream.ResponseTrailer(), call)
	return nil
}

func (a *connectAdapter) getTask(ctx context.Context, request *connect.Request[a2apb.GetTaskRequest]) (*connect.Response[a2apb.Task], error) {
	req, err := pbconv.FromProtoGetTaskRequest(request.Msg)
	if err != nil {
		return nil, invalidArgument(err)
	}
	ctx, call := connectCallContext(ctx, request.Header())
	task, err := a.handler.GetTask(ctx, req)
	if err != nil {
		return nil, connectError(err)
	}
	msg, err := pbconv.ToProtoTask(task)
	if err != nil {
		return nil, internalError(err)
	}
	return responseWithExtensions(msg, call), nil
}

func (a *connectAdapter) listTasks(ctx context.Context, request *connect.Request[a2apb.ListTasksRequest]) (*connect.Response[a2apb.ListTasksResponse], error) {
	req, err := pbconv.FromProtoListTasksRequest(request.Msg)
	if err != nil {
		return nil, invalidArgument(err)
	}
	ctx, call := connectCallContext(ctx, request.Header())
	result, err := a.handler.ListTasks(ctx, req)
	if err != nil {
		return nil, connectError(err)
	}
	msg, err := pbconv.ToProtoListTasksResponse(result)
	if err != nil {
		return nil, internalError(err)
	}
	return responseWithExtensions(msg, call), nil
}

func (a *connectAdapter) cancelTask(ctx context.Context, request *connect.Request[a2apb.CancelTaskRequest]) (*connect.Response[a2apb.Task], error) {
	req, err := pbconv.FromProtoCancelTaskRequest(request.Msg)
	if err != nil {
		return nil, invalidArgument(err)
	}
	ctx, call := connectCallContext(ctx, request.Header())
	task, err := a.handler.CancelTask(ctx, req)
	if err != nil {
		return nil, connectError(err)
	}
	msg, err := pbconv.ToProtoTask(task)
	if err != nil {
		return nil, internalError(err)
	}
	return responseWithExtensions(msg, call), nil
}

func (a *connectAdapter) subscribeToTask(ctx context.Context, request *connect.Request[a2apb.SubscribeToTaskRequest], stream *connect.ServerStream[a2apb.StreamResponse]) error {
	req, err := pbconv.FromProtoSubscribeToTaskRequest(request.Msg)
	if err != nil {
		return invalidArgument(err)
	}
	ctx, call := connectCallContext(ctx, request.Header())
	for event, eventErr := range a.handler.SubscribeToTask(ctx, req) {
		if eventErr != nil {
			return connectError(eventErr)
		}
		msg, convertErr := pbconv.ToProtoStreamResponse(event)
		if convertErr != nil {
			return internalError(convertErr)
		}
		if sendErr := stream.Send(msg); sendErr != nil {
			return connect.NewError(connect.CodeAborted, sendErr)
		}
	}
	setActiveExtensions(stream.ResponseTrailer(), call)
	return nil
}

func (a *connectAdapter) createTaskPushConfig(ctx context.Context, request *connect.Request[a2apb.TaskPushNotificationConfig]) (*connect.Response[a2apb.TaskPushNotificationConfig], error) {
	req, err := pbconv.FromProtoTaskPushConfig(request.Msg)
	if err != nil {
		return nil, invalidArgument(err)
	}
	ctx, call := connectCallContext(ctx, request.Header())
	result, err := a.handler.CreateTaskPushConfig(ctx, req)
	if err != nil {
		return nil, connectError(err)
	}
	msg, err := pbconv.ToProtoTaskPushConfig(result)
	if err != nil {
		return nil, internalError(err)
	}
	return responseWithExtensions(msg, call), nil
}

func (a *connectAdapter) getTaskPushConfig(ctx context.Context, request *connect.Request[a2apb.GetTaskPushNotificationConfigRequest]) (*connect.Response[a2apb.TaskPushNotificationConfig], error) {
	req, err := pbconv.FromProtoGetTaskPushConfigRequest(request.Msg)
	if err != nil {
		return nil, invalidArgument(err)
	}
	ctx, call := connectCallContext(ctx, request.Header())
	result, err := a.handler.GetTaskPushConfig(ctx, req)
	if err != nil {
		return nil, connectError(err)
	}
	msg, err := pbconv.ToProtoTaskPushConfig(result)
	if err != nil {
		return nil, internalError(err)
	}
	return responseWithExtensions(msg, call), nil
}

func (a *connectAdapter) listTaskPushConfigs(ctx context.Context, request *connect.Request[a2apb.ListTaskPushNotificationConfigsRequest]) (*connect.Response[a2apb.ListTaskPushNotificationConfigsResponse], error) {
	req, err := pbconv.FromProtoListTaskPushConfigRequest(request.Msg)
	if err != nil {
		return nil, invalidArgument(err)
	}
	ctx, call := connectCallContext(ctx, request.Header())
	result, err := a.handler.ListTaskPushConfigs(ctx, req)
	if err != nil {
		return nil, connectError(err)
	}
	msg, err := pbconv.ToProtoListTaskPushConfigResponse(result)
	if err != nil {
		return nil, internalError(err)
	}
	return responseWithExtensions(msg, call), nil
}

func (a *connectAdapter) getExtendedAgentCard(ctx context.Context, request *connect.Request[a2apb.GetExtendedAgentCardRequest]) (*connect.Response[a2apb.AgentCard], error) {
	req, err := pbconv.FromProtoGetExtendedAgentCardRequest(request.Msg)
	if err != nil {
		return nil, invalidArgument(err)
	}
	ctx, call := connectCallContext(ctx, request.Header())
	result, err := a.handler.GetExtendedAgentCard(ctx, req)
	if err != nil {
		return nil, connectError(err)
	}
	msg, err := pbconv.ToProtoAgentCard(result)
	if err != nil {
		return nil, internalError(err)
	}
	return responseWithExtensions(msg, call), nil
}

func (a *connectAdapter) deleteTaskPushConfig(ctx context.Context, request *connect.Request[a2apb.DeleteTaskPushNotificationConfigRequest]) (*connect.Response[emptypb.Empty], error) {
	req, err := pbconv.FromProtoDeleteTaskPushConfigRequest(request.Msg)
	if err != nil {
		return nil, invalidArgument(err)
	}
	ctx, call := connectCallContext(ctx, request.Header())
	if err := a.handler.DeleteTaskPushConfig(ctx, req); err != nil {
		return nil, connectError(err)
	}
	return responseWithExtensions(&emptypb.Empty{}, call), nil
}

func connectCallContext(ctx context.Context, header http.Header) (context.Context, *a2asrv.CallContext) {
	params := a2asrv.NewServiceParams(map[string][]string(header))
	return a2asrv.NewCallContext(ctx, params)
}

func responseWithExtensions[T any](msg *T, call *a2asrv.CallContext) *connect.Response[T] {
	response := connect.NewResponse(msg)
	setActiveExtensions(response.Trailer(), call)
	return response
}

func setActiveExtensions(header http.Header, call *a2asrv.CallContext) {
	for _, uri := range call.Extensions().ActivatedURIs() {
		header.Add(protocol.SvcParamExtensions, uri)
	}
}

func invalidArgument(err error) error {
	return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("failed to convert request: %w", err))
}

func internalError(err error) error {
	return connect.NewError(connect.CodeInternal, fmt.Errorf("failed to convert response: %w", err))
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
	return connect.NewError(code, errors.New(message))
}
