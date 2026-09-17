# Gate A2A protobuf contract

The normative A2A Protocol v1.0.0 schema and Go client types are not copied or
generated into this repository. Buf validates the schema directly from the
immutable upstream commit configured as `A2A_GIT_INPUT` in the Makefile, while
Go code uses the official `github.com/a2aproject/a2a-go/v2` SDK.

`anra/gate/a2a/extensions/v1/extensions.proto` is an independent local Buf
module defining gate-owned ACP observability and permission payloads. It does
not define another RPC service or import the upstream schema. The official A2A
SDK remains the source of core protocol types and transport implementations.

## Official Go SDK surface

Generate the gate-owned extension messages with:

```sh
make proto-generate
```

This generates only local Gate extensions via `buf.gen.yaml`. Network access is
required for the remote plugin.

The official SDK exposes the canonical A2A operations, including `SendMessage`,
`SendStreamingMessage`, `GetTask`, `ListTasks`, `CancelTask`, and
`SubscribeToTask` across its supported transports.

The public Agent Card must advertise a supported interface with protocol
version `1.0` and streaming capability. Use the official SDK's JSON-RPC, REST,
or gRPC binding for wire interoperability.

`examples/research-engineering-agent-card.json` is a schema-tested discovery
template for the research/engineering agent. Replace its endpoint and identity
fields before publishing it at `/.well-known/agent-card.json`.

## Extension negotiation and placement

Declare these optional extensions in `AgentCard.capabilities.extensions`:

- `https://anra.dev/a2a/extensions/acp-tool-call/v1`
- `https://anra.dev/a2a/extensions/acp-thought/v1`
- `https://anra.dev/a2a/extensions/acp-permission/v1`

Clients opt in with the `A2A-Extensions` request header. Encode extension
messages as ProtoJSON in `Part.data`, and add the matching URI to the enclosing
`Message.extensions` list.

- Tool-call and thought updates are placed in
  `TaskStatusUpdateEvent.status.message` while the task remains `WORKING`.
- Permission requests are placed in the same field and transition the task to
  `INPUT_REQUIRED`.
- Permission responses are new user messages on the same `task_id` and
  `context_id`; they are not new tasks.
- Final answer chunks are `TaskArtifactUpdateEvent` values and must not be mixed
  with thought content.

## Invariants enforced by the server

- The server creates task IDs. One ACP prompt turn maps to one A2A task; turns
  in the same ACP session share a context ID.
- Every emitted event consumes one task-global sequence number, including
  events hidden because an optional extension was not negotiated.
- Permission validation binds task, context, session, permission ID, option ID,
  snapshot digest, expiry, and a compare-and-swap version. First valid response
  wins; repeats are idempotent; incomplete or mismatched input fails closed.
- `CancelTask` resolves all pending permission waiters as cancelled before
  forwarding ACP cancellation.
- Raw tool input/output, credentials, environment variables, and other secrets
  are outside this public schema and must be redacted before constructing a
  payload.

## Evolution policy

Update `A2A_GIT_INPUT` only after reviewing an official release, and pin an
immutable commit rather than a branch or mutable tag. Regenerate and run
`buf breaking` before merge. Additive optional fields may stay in extension
`v1`; incompatible wire or semantic changes require a new package and
extension URI.
