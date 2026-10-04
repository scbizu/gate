# Gate

Gate is a protocol proxy that exposes an ACP v1 agent process over A2A.
The configured agent provides the actual capabilities; Gate forwards requests,
responses, and supported lifecycle operations between the two protocols.

Requires Go 1.26 or newer. RPCs use Connect v2 (`v2.0.0-rc.1`), registered on
`connect.NewServer` and mounted with `connecthttp.Mount`.

```sh
go build -o bin/gate .
./bin/gate -listen 127.0.0.1:8080 -cwd /absolute/project -- /path/to/acp-agent [agent arguments...]
```

The agent must speak ACP over stdin/stdout. Its environment is inherited from
Gate; supply provider credentials through the environment required by that
agent. Agent stderr goes to Gate's stderr.

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

Run `go test ./...` or `go test -race ./...`. See [e2e coverage](e2e/README.md)
for the exercised protocol cases and current boundaries.

Development tools and protobuf generation are pinned in `mise.toml`.
Run `mise install`, then `mise run proto-generate` to regenerate bindings or
`mise run test` to validate schemas and run tests. See [protobuf contract](proto/README.md).
