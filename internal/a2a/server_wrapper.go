package a2a

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	protocol "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/anra-studio/gate/internal/acp"
	acpv1 "github.com/anra-studio/gate/internal/acp/v1"
)

// ProxyConfig configures the ACP-backed A2A service and its HTTP listener.
type ProxyConfig struct {
	// ListenAddress defaults to 127.0.0.1:8080. A port of zero selects a free port.
	ListenAddress string
	// PublicURL overrides the origin advertised in the agent card.
	PublicURL string
	// Process specifies the ACP agent command, arguments, directory and environment.
	// Its directory is also used as the ACP session working directory.
	Process acpv1.ProcessConfig
	// OnListening is called with the advertised origin after binding the listener.
	OnListening func(string)
}

// ServeProxy owns the listener, HTTP server and ACP runtime until ctx is
// cancelled or serving fails. Cancellation closes ACP processes before draining
// HTTP requests, so pending turns cannot hold shutdown open.
func ServeProxy(ctx context.Context, config ProxyConfig) error {
	if config.Process.Command == "" {
		return errors.New("a2a: ACP process command is required")
	}
	if config.ListenAddress == "" {
		config.ListenAddress = "127.0.0.1:8080"
	}
	dir, err := filepath.Abs(config.Process.Dir)
	if err != nil {
		return fmt.Errorf("resolve cwd: %w", err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("agent cwd: %w", err)
	}
	if !info.IsDir() {
		return errors.New("agent cwd must be a directory")
	}
	if config.PublicURL != "" {
		parsed, err := url.Parse(config.PublicURL)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
			return errors.New("public-url must be an HTTP(S) origin without credentials, path, query or fragment")
		}
	}
	listener, err := net.Listen("tcp", config.ListenAddress)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer listener.Close()
	origin := strings.TrimRight(config.PublicURL, "/")
	if origin == "" {
		host, port, err := net.SplitHostPort(listener.Addr().String())
		if err != nil {
			return err
		}
		if host == "" || host == "0.0.0.0" || host == "::" {
			host = "127.0.0.1"
		}
		origin = "http://" + net.JoinHostPort(host, port)
	}
	config.Process.Dir = dir
	executor, err := NewACPExecutor(ACPExecutorConfig{
		NewClient:         NewProcessACPClientFactory(config.Process),
		InitializeRequest: acp.InitializeRequest{ClientInfo: acp.ClientInfo{Name: "gate", Version: "1.0.0"}},
		SessionRequest:    acp.NewSessionRequest{CWD: dir},
	})
	if err != nil {
		return err
	}
	defer executor.Close()
	card := &protocol.AgentCard{
		Name:        "Gate",
		Description: "An A2A proxy for an ACP agent.",
		Version:     "1.0.0",
		Capabilities: protocol.AgentCapabilities{Streaming: true, Extensions: []protocol.AgentExtension{
			{URI: ThoughtExtensionURI}, {URI: ToolCallExtensionURI},
		}},
		SupportedInterfaces: []*protocol.AgentInterface{
			protocol.NewAgentInterface(origin, protocol.TransportProtocolHTTPJSON),
			protocol.NewAgentInterface(origin, protocol.TransportProtocolGRPC),
		},
		DefaultInputModes: []string{"text/plain"}, DefaultOutputModes: []string{"text/plain"},
		Skills: []protocol.AgentSkill{{ID: "acp-proxy", Name: "ACP agent proxy", Description: "Forward prompts to the configured ACP agent and relay its responses over A2A.", Tags: []string{"acp", "a2a", "proxy"}}},
	}
	handler, err := NewServer(card, executor)
	if err != nil {
		return err
	}
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	server := &http.Server{
		Handler:           handler,
		Protocols:         protocols,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	finished := make(chan error, 1)
	go func() { finished <- server.Serve(listener) }()
	if config.OnListening != nil {
		config.OnListening(origin)
	}
	select {
	case err := <-finished:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		// Close the runtime first so pending ACP turns cannot hold shutdown open.
		closeErr := executor.Close()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			_ = server.Close()
			return errors.Join(closeErr, err)
		}
		return closeErr
	}
}
