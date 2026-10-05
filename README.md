# Gate

Gate exposes an ACP v1 agent over A2A.

## Run

Go 1.27.1 and development tools are pinned in `mise.toml`.

```sh
mise install
mise run build
./bin/gate -listen 127.0.0.1:8080 -cwd /absolute/project -- /path/to/acp-agent [agent arguments...]
```

The agent must speak ACP over stdin/stdout. It inherits Gate's environment;
set provider credentials there. Agent stderr goes to Gate's stderr.

Agent card: `http://127.0.0.1:8080/.well-known/agent-card.json`.
HTTP+JSON (SSE), Connect, gRPC, and gRPC-Web share one listener; cleartext gRPC
uses h2c. Set `-public-url` to the public origin behind a reverse proxy.
Use `-listen 127.0.0.1:0` to select and advertise an available port.

## Docker

```sh
docker build -t gate .
```

The image contains Gate and CA certificates. Install the agent and its runtime
in a derived image, or mount a Linux binary matching the container architecture:

```sh
docker run --rm --init -p 8080:8080 \
  --mount type=bind,src="$(pwd)",dst=/workspace \
  --mount type=bind,src=/absolute/path/to/acp-agent,dst=/usr/local/bin/acp-agent,readonly \
  gate -cwd /workspace -public-url http://localhost:8080 -- /usr/local/bin/acp-agent
```

The container runs as UID/GID `10001:10001`. Grant it read access to mounted
projects and write access if the agent edits files. Pass credentials with `--env`
or `--env-file`. It listens on `0.0.0.0:8080`; set `-public-url` to the client-facing
origin.

## Behavior

Gate starts one ACP process/session on demand per A2A context and reuses it
across turns. Prompts within a context run serially. Interrupt or SIGTERM closes
the agent processes and HTTP connections.

Clients opt in to thought and tool-call extensions; raw tool input/output is
excluded. Permission requests receive `cancelled`; interactive approval and the
permission extension are unsupported. ACP authentication, session loading,
session modes/configuration, and client filesystem/terminal capabilities are
also unsupported.

## Development

```sh
mise run test            # Build, validate schemas, and run all tests
mise run e2e             # Build and run BDD scenarios
mise run cli-test        # Build and test CLI arguments and shutdown
mise run proto-generate  # Regenerate protobuf and Connect bindings
```

See [e2e coverage](e2e/README.md), [ACP tests](internal/acp/v1/README.md),
and [protobuf contract](proto/README.md).
