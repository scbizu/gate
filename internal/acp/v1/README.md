# ACP v1 protocol boundary tests

These Go tests belong to the ACP adapter package. Product behavior definitions
live separately in [`e2e/features`](../../../e2e/features).

```sh
go test ./internal/acp/v1 -v -timeout 90s
go test -race ./... -timeout 120s
```

- `client_test.go`: in-memory wire peers and client lifecycle checks.
- `process_test.go`: SDK subprocess lifecycle, permission handling, and helpers.
- `spec_test.go`: protocol validation, content, MCP, stop reasons, and concurrency.
- `wire_test.go`: malformed/raw subprocess records and tool content conversion.

The SDK subprocess fixture is shared with the BDD suite through
`internal/testutil/acppeer`. Child-process entry points are test fixtures; they
are selected explicitly by the process launcher. These tests need no model
provider, API key, or external service.

| Area | Cases |
| --- | --- |
| Initialization | v1 negotiation, agent identity, image/audio and MCP capabilities, rejection of v2 and closure afterward, unsupported filesystem/terminal capabilities omitted |
| Sessions | unique IDs, absolute cwd/additional directory validation, empty session ID rejected |
| MCP | stdio arguments/environment, HTTP headers, SSE URL; missing names, relative commands/URLs and unknown transports rejected |
| Prompt content | Unicode, escaped/newline text, messages above 64 KiB, image data/MIME/URI, audio data/MIME, resource links; malformed and unsupported blocks rejected |
| Observations | ordered message/thought/tool start/tool update, tool diff/content/terminal references, unrelated-session updates ignored, user history and plans excluded from agent updates |
| Stop reasons | end_turn, max_tokens, max_turn_requests, refusal, cancelled; unknown reason rejected |
| Permissions | allow, reject, cancelled, handler error, unknown option, conflicting cancelled/selected outcome; pending permission cancellation |
| Concurrency/lifecycle | same-session overlapping prompt rejected, distinct sessions run concurrently, close unblocks pending prompt, idempotent close, calls after close rejected, cancel preserves the session |
| Wire/error boundaries | split NDJSON writes, batched ordered notifications/response, missing tool IDs, peer exit, ACP JSON-RPC errors |
| Reverse RPC | unadvertised read/write file and all five terminal methods return method-not-found |
