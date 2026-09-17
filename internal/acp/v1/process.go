package v1

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
)

type ProcessConfig struct {
	Command string
	Args    []string
	Dir     string
	// Env augments the inherited environment. Secrets should be supplied by
	// the process supervisor and must never be copied into A2A events.
	Env    []string
	Stderr io.Writer
}

// StartProcess starts an ACP agent whose stdin/stdout carry the stable v1
// NDJSON transport. The process is owned by the returned Client and is stopped
// when that client is closed or ctx is cancelled.
func StartProcess(ctx context.Context, config ProcessConfig) (*Client, error) {
	if config.Command == "" {
		return nil, errors.New("acp: process command is required")
	}
	if config.Dir != "" && !filepath.IsAbs(config.Dir) {
		return nil, errors.New("acp: process directory must be an absolute path")
	}

	command := exec.CommandContext(ctx, config.Command, config.Args...)
	command.Dir = config.Dir
	if len(config.Env) != 0 {
		command.Env = append(os.Environ(), config.Env...)
	}
	command.Stderr = config.Stderr
	stdin, err := command.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("acp: open agent stdin: %w", err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, fmt.Errorf("acp: open agent stdout: %w", err)
	}
	if err := command.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, fmt.Errorf("acp: start agent: %w", err)
	}
	transport := &processTransport{command: command, stdin: stdin, stdout: stdout}
	return New(stdout, stdin, transport), nil
}

type processTransport struct {
	command *exec.Cmd
	stdin   io.WriteCloser
	stdout  io.ReadCloser
	once    sync.Once
	err     error
}

func (p *processTransport) Close() error {
	p.once.Do(func() {
		_ = p.stdin.Close()
		_ = p.stdout.Close()
		if p.command.Process != nil {
			if err := p.command.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				p.err = err
			}
		}
		if err := p.command.Wait(); err != nil {
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) && p.err == nil {
				p.err = err
			}
		}
	})
	return p.err
}
