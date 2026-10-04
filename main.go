package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/anra-studio/gate/internal/a2a"
	acpv1 "github.com/anra-studio/gate/internal/acp/v1"
	"github.com/peterbourgon/ff/v3"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("gate", flag.ContinueOnError)
	listen := flags.String("listen", "127.0.0.1:8080", "HTTP listen address")
	publicURL := flags.String("public-url", "", "Public origin advertised in the agent card (defaults to listen address)")
	cwd := flags.String("cwd", ".", "ACP agent working directory")
	flags.Usage = func() {
		fmt.Fprintln(flags.Output(), "Usage: gate [flags] -- <acp-agent> [agent arguments...]")
		flags.PrintDefaults()
	}
	if err := ff.Parse(flags, args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() == 0 {
		return errors.New("an ACP agent command is required: gate [flags] -- <acp-agent> [arguments...]")
	}
	return a2a.ServeProxy(ctx, a2a.ProxyConfig{
		ListenAddress: *listen,
		PublicURL:     *publicURL,
		Process: acpv1.ProcessConfig{
			Command: flags.Arg(0), Args: flags.Args()[1:], Dir: *cwd, Stderr: os.Stderr,
		},
		OnListening: func(origin string) { log.Printf("Gate listening at %s", origin) },
	})
}
