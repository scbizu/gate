# Gate

Gate is a protocol proxy that exposes an ACP v1 agent process over A2A.
The configured agent provides the actual capabilities; Gate forwards requests,
responses, and supported lifecycle operations between the two protocols.

Requires Go 1.26 or newer. RPCs use Connect v2 (`v2.0.0-rc.1`), registered on
`connect.NewServer` and mounted with `connecthttp.Mount`.

```sh
mise run build
./bin/gate -listen 127.0.0.1:8080 -cwd /absolute/project -- /path/to/acp-agent [agent arguments...]
```

The agent must speak ACP over stdin/stdout. Its environment is inherited from
Gate; supply provider credentials through the environment required by that
agent. Agent stderr goes to Gate's stderr.

### Docker

```sh
docker build -t gate .
```

The image contains Gate and CA certificates. Install your ACP agent and its
runtime in a derived image, or mount a compatible Linux executable. For example,
with a standalone agent binary built for the container's architecture:

```sh
docker run --rm --init -p 8080:8080 \
  --mount type=bind,src="$(pwd)",dst=/workspace \
  --mount type=bind,src=/absolute/path/to/acp-agent,dst=/usr/local/bin/acp-agent,readonly \
  gate -cwd /workspace -public-url http://localhost:8080 -- /usr/local/bin/acp-agent
```

The container runs as UID/GID `10001:10001`; mounted projects must be readable
and, if the agent edits files, writable by that user. Pass the agent's credentials
at runtime with `--env` or `--env-file`. The default listener is `0.0.0.0:8080`;
set `-public-url` to the origin clients use, including when behind a reverse proxy.
Gate flags and the required `-- <acp-agent> [arguments...]` follow the image name.

Discover the agent at `http://127.0.0.1:8080/.well-known/agent-card.json`.
HTTP+JSON (including SSE), Connect, gRPC, and gRPC-Web share the same listener.
Cleartext gRPC uses HTTP/2 via h2c. Use `-public-url https://gate.example.com`
when a reverse proxy supplies the public origin. `-listen 127.0.0.1:0` chooses
an available port and advertises the resulting address.

Gate starts one ACP process/session lazily per A2A context, reuses that session
across turns, and serializes prompts within the context. Interrupt or SIGTERM
closes ACP processes and shuts down HTTP connections.

The agent card advertises optional thought and tool-call extensions. Clients
must activate each extension they want. Raw tool input/output is excluded.
Permission requests currently receive `cancelled`; interactive permission
approval/resumption is not implemented and the permission extension is not
advertised. ACP authentication, session loading, session modes/configuration,
and client filesystem/terminal capabilities are also not exposed by the adapter.

Run `mise run test` for all checks or `mise run e2e` to build Gate and run BDD
scenarios. CLI checks use ordinary Go tests in `main_test.go`; run them with
`mise run cli-test`. See [e2e coverage](e2e/README.md)
for executable behavior definitions, and [ACP package tests](internal/acp/v1/README.md)
for protocol boundary coverage.

Development tools and protobuf generation are pinned in `mise.toml`.
Run `mise install`, then `mise run proto-generate` to regenerate bindings or
`mise run test` to validate schemas and run tests. See [protobuf contract](proto/README.md).
