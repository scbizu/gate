# End-to-end behavior tests

Review the executable behavior definitions first:

- [`features/gate.feature`](features/gate.feature): discovery, stop outcomes,
  conversation reuse, permissions, privacy, observation negotiation, cancellation,
  disconnection, transport equivalence, and missing tasks.

These Gherkin files run through [Godog](https://github.com/cucumber/godog), pinned
in `go.mod`. `Scenario Outline` tables make the outcome rules visible without
reading Go. Undefined or pending steps fail the suite (`Strict: true`). Each Gate
scenario starts its own production Gate server in-process and ACP subprocess,
with a fresh client and a 30-second deadline. Mise builds `bin/gate` before the
tests. CLI argument/help/signal checks are ordinary Go tests in `main_test.go`
and use the prebuilt binary; they are not BDD scenarios.

```sh
# Build Gate, then run all BDD scenarios.
mise run e2e
# Server behavior only (does not need a Gate binary).
go test ./e2e -run '^TestGateBehavior$' -v -count=1 -timeout 90s
# Build once, then run ordinary CLI/signal tests.
mise run cli-test
# One named server scenario.
go test ./e2e -run '^TestGateBehavior$/Cancel_a_task_and_continue_the_conversation$' -v -count=1
# Build, validate schemas, and run all repository tests.
mise run test
# Full repository with race detection, using the prebuilt binary.
mise run build
go test -race ./... -count=1 -timeout 120s
```

`make e2e`, `make cli-test`, and `make build` are aliases for the mise tasks.
Direct CLI test runs use `bin/gate` relative to the root package, or an override
supplied through `GATE_TEST_BINARY`. A missing binary fails with instructions to
run `mise run build`. Use `-count=1` for direct CLI runs against the current binary.

Tests use subprocess ACP peers and the pinned `coder/acp-go-sdk` version from
`go.mod`. They do not need network access to a model provider or an API key.
`config.toml` contains local credentials, is ignored by Git, and is not read or
printed by these deterministic tests.

`TestGateBehavior` calls `internal/a2a.ServeProxy`, the same server entry point
used by `main.go`, on an ephemeral loopback port. Requests traverse the real
HTTP/Connect/gRPC transports, production server, ACP executor, v1 adapter, and
ACP subprocess. It bypasses CLI parsing and OS-signal handling; those are covered
by ordinary tests in `main_test.go`. The tests require permission
to bind local ports and start child processes.

| Behavior | Scenarios |
| --- | --- |
| Discovery | usable origin, streaming, advertised observation extensions |
| Tasks | stop outcomes, new task for each turn, conversation reuse, stored results, missing-task error |
| Permissions/privacy | tool permission cancelled, private RPC/tool details excluded |
| Streaming | requested observations only, sequence and IDs, submitted/working status, appended output, history/plans excluded |
| Cancellation/lifecycle | cancel preserves the ACP session, disconnect preserves work |
| Transports | equivalent results over HTTP+JSON, Connect, gRPC-Web, and gRPC/h2c |

When adding or changing end-to-end behavior, start by editing a `.feature` file:

1. Describe the initial situation in **Given**, the client/agent action in **When**,
   and the observable outcome in **Then**. Use concrete wording and example
   tables for alternative outcomes.
2. Assert task states, returned content, session continuity, negotiated events,
   or process exits. Keep SDK types, fixture commands, and implementation details
   in the Go step definitions in `behavior_test.go`.
3. Make every action step perform the described action and every outcome step
   check it. Avoid a single opaque step that calls an entire old test.
4. Keep scenario state isolated; clean up clients, streams, and processes even
   when an assertion fails. Use observed events rather than sleeps to coordinate.

`e2e/` contains executable features, their runner/steps, and process fixtures.
`TestACPAgentProcess` is a child-process entry point for the deterministic peer,
not a protocol assertion test. The peer implementation is shared through
`internal/testutil/acppeer`.

ACP protocol boundary tests live in [`internal/acp/v1`](../internal/acp/v1/README.md).
A2A request-metadata checks live beside the server in `internal/a2a/server_test.go`.

The suite follows the supported subset of the [ACP v1 protocol](https://agentclientprotocol.com/protocol/v1/overview).
It is not a claim of full ACP conformance. Gate does not yet expose authentication,
load/resume, mode/configuration controls, embedded resource content, or interactive
A2A permission resumption. Plans and other session metadata are deliberately not
forwarded. Filesystem/terminal capabilities are deliberately disabled; their
rejection is covered by the ACP package tests. Live-provider behavior
would be a separate opt-in integration test.
