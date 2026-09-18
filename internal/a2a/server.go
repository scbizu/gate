// Package a2a exposes Gate's A2A implementation over Connect RPC and
// HTTP+JSON. Vanguard provides protocol routing and REST transcoding from the
// canonical A2A protobuf service descriptor.
package a2a

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"connectrpc.com/connect"
	"connectrpc.com/vanguard"
	protocol "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

const ServiceName = "lf.a2a.v1.A2AService"

// Server serves an A2A agent card and the canonical A2A protobuf service.
// The RPC endpoint supports Connect, gRPC, and gRPC-Web. Vanguard additionally
// exposes the HTTP+JSON routes declared by the A2A protobuf schema.
type Server struct {
	handler http.Handler
}

var _ http.Handler = (*Server)(nil)

// Option configures an A2A server.
type Option func(*serverOptions)

type serverOptions struct {
	requestHandlerOptions []a2asrv.RequestHandlerOption
	connectHandlerOptions []connect.HandlerOption
}

// WithRequestHandlerOptions forwards options to the official A2A request
// handler. It can be used to provide a durable task store or queue manager.
func WithRequestHandlerOptions(options ...a2asrv.RequestHandlerOption) Option {
	return func(config *serverOptions) {
		config.requestHandlerOptions = append(config.requestHandlerOptions, options...)
	}
}

// WithConnectHandlerOptions forwards options to every Connect RPC handler.
func WithConnectHandlerOptions(options ...connect.HandlerOption) Option {
	return func(config *serverOptions) {
		config.connectHandlerOptions = append(config.connectHandlerOptions, options...)
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
	rpcHandler, err := newConnectHandler(requestHandler, config.connectHandlerOptions...)
	if err != nil {
		return nil, err
	}

	fallback := http.NewServeMux()
	fallback.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(card))

	transcoder, err := vanguard.NewTranscoder(
		[]*vanguard.Service{vanguard.NewService(ServiceName, rpcHandler)},
		vanguard.WithUnknownHandler(fallback),
	)
	if err != nil {
		return nil, fmt.Errorf("a2a: create vanguard transcoder: %w", err)
	}
	return &Server{handler: transcoder}, nil
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
