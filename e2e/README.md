# End-to-end protocol tests

```sh
go test ./e2e -v -timeout 90s
go test -race ./... -timeout 120s
```

Tests use subprocess ACP peers and the pinned `coder/acp-go-sdk` version from
`go.mod`. They do not need network access to a model provider or an API key.
`config.toml` contains local credentials, is ignored by Git, and is not read or
printed by these deterministic tests.

`TestGateCommand` builds and launches the actual root `main.go` on an ephemeral
loopback port. Requests traverse the production server, ACP executor, v1 adapter,
and ACP subprocess. The tests require permission to bind local ports and start
child processes.

| Area | Cases |
| --- | --- |
| Initialization | v1 negotiation, agent identity, image/audio and MCP capabilities, rejection of v2 and closure afterward, unsupported filesystem/terminal capabilities omitted |
| Sessions | unique IDs, same A2A context reuses the ACP session with a new task ID, absolute cwd/additional directory validation, empty session ID rejected |
| MCP | stdio arguments/environment, HTTP headers, SSE URL; missing names, relative commands/URLs and unknown transports rejected |
| Prompt content | Unicode, escaped/newline text, messages above 64 KiB, image data/MIME/URI, audio data/MIME, resource links; malformed and unsupported blocks rejected |
| Observations | ordered message/thought/tool start/tool update, tool diff/content/terminal references, unrelated-session updates ignored, user history and plans excluded from agent output |
| Stop reasons | end_turn, max_tokens, max_turn_requests, refusal, cancelled; corresponding A2A terminal states; unknown reason rejected |
| Permissions | allow, reject, cancelled, handler error, unknown option, conflicting cancelled/selected outcome; pending permission cancellation; production executor defaults to cancellation |
| Concurrency/lifecycle | same-session overlapping prompt rejected, distinct sessions run concurrently, close unblocks pending prompt, idempotent close, calls after close rejected, cancel preserves the session |
| Wire/error boundaries | split NDJSON writes, batched ordered notifications/response, missing tool IDs, peer exit, ACP JSON-RPC errors, raw tool/RPC details excluded from A2A output |
| Reverse RPC | unadvertised read/write file and all five terminal methods return method-not-found |
| A2A transports | agent-card discovery, HTTP+JSON unary and SSE, Connect, gRPC-Web, gRPC/h2c, GetTask, missing-task error, CancelTask |
| A2A extensions | thought only, tool only, both, neither; IDs retained/generated, monotonically increasing sequence, final artifacts unaffected |
| CLI | missing command, invalid public origins, missing cwd, help, ephemeral port advertised correctly, graceful signal exit |
| Stream lifetime | disconnect leaves the ACP turn working and cancellable afterward |

The suite follows the supported subset of the [ACP v1 protocol](https://agentclientprotocol.com/protocol/v1/overview).
It is not a claim of full ACP conformance. Gate does not yet expose authentication,
load/resume, mode/configuration controls, embedded resource content, or interactive
A2A permission resumption. Plans and other session metadata are deliberately not
forwarded. Filesystem/terminal capabilities are deliberately disabled; their
rejection is tested rather than successful execution. Live-provider behavior
would be a separate opt-in integration test.
