// Package a2a exposes Gate's A2A implementation over Connect RPC and
// HTTP+JSON. Connect v2 serves protobuf RPCs; the official A2A REST handler
// provides A2A's SSE framing and structured HTTP errors.
package a2a

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	protocol "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	a2apbconnect "github.com/anra-studio/gate/gen/a2a/a2apbconnect"
)

const ServiceName = a2apbconnect.A2AServiceName

// Server serves an A2A agent card and the canonical A2A protobuf service.
// The RPC endpoint supports Connect, gRPC, and gRPC-Web. The REST endpoint
// supports HTTP+JSON and server-sent events.
type Server struct {
	handler http.Handler
}

var _ http.Handler = (*Server)(nil)

// Option configures an A2A server.
type Option func(*serverOptions)

type serverOptions struct {
	requestHandlerOptions []a2asrv.RequestHandlerOption
	connectHTTPOptions    []connecthttp.Option
	connectInterceptors   []connect.ServerInterceptor
}

// WithRequestHandlerOptions forwards options to the official A2A request
// handler. It can be used to provide a durable task store or queue manager.
func WithRequestHandlerOptions(options ...a2asrv.RequestHandlerOption) Option {
	return func(config *serverOptions) {
		config.requestHandlerOptions = append(config.requestHandlerOptions, options...)
	}
}

// WithConnectHTTPOptions forwards transport options to connecthttp.Mount.
func WithConnectHTTPOptions(options ...connecthttp.Option) Option {
	return func(config *serverOptions) { config.connectHTTPOptions = append(config.connectHTTPOptions, options...) }
}

// WithConnectInterceptors applies interceptors to every Connect RPC method.
func WithConnectInterceptors(interceptors ...connect.ServerInterceptor) Option {
	return func(config *serverOptions) {
		config.connectInterceptors = append(config.connectInterceptors, interceptors...)
	}
}

// NewServer constructs a protocol-complete A2A HTTP handler. The card must
// describe the public URL(s) at which this handler will be served.
func NewServer(card *protocol.AgentCard, executor a2asrv.AgentExecutor, options ...Option) (*Server, error) {
	if card == nil {
		return nil, errors.New("a2a: agent card is required")
	}
	if executor == nil {
		return nil, errors.New("a2a: agent executor is required")
	}
	if err := validateCard(card); err != nil {
		return nil, err
	}

	config := serverOptions{}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}

	requestOptions := append([]a2asrv.RequestHandlerOption{
		a2asrv.WithCapabilityChecks(&card.Capabilities),
	}, config.requestHandlerOptions...)
	requestHandler := a2asrv.NewHandler(executor, requestOptions...)
	rpcServer := connect.NewServer(config.connectInterceptors...)
	a2apbconnect.RegisterA2AServiceHandler(rpcServer, &connectAdapter{handler: requestHandler})
	mux := http.NewServeMux()
	connecthttp.Mount(mux, rpcServer, config.connectHTTPOptions...)
	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(card))
	mux.Handle("/", a2asrv.NewRESTHandler(requestHandler))
	return &Server{handler: mux}, nil
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	s.handler.ServeHTTP(response, request)
}

func validateCard(card *protocol.AgentCard) error {
	if strings.TrimSpace(card.Name) == "" {
		return errors.New("a2a: agent card name is required")
	}
	if strings.TrimSpace(card.Version) == "" {
		return errors.New("a2a: agent card version is required")
	}
	if len(card.SupportedInterfaces) == 0 {
		return errors.New("a2a: agent card must advertise at least one interface")
	}
	for index, iface := range card.SupportedInterfaces {
		if iface == nil || strings.TrimSpace(iface.URL) == "" {
			return fmt.Errorf("a2a: agent card interface %d has no URL", index)
		}
	}
	return nil
}
