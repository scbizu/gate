# ACP Agent 通过 A2A 接入

## 背景

`tape-go` 及其上层运行时通过 A2A 调用远程 Agent。ACP 描述的是完整 Agent Runtime 与客户端之间的双向协议，不适合作为普通模型 Provider 直接塞进 `tape-go`。因此由 `gate` 提供独立的 ACP → A2A 接入能力，对外暴露 A2A Agent，对内作为 ACP Client 管理 ACP Agent 进程和会话。

## 目标

- 由 `gate` 将 ACP Agent 包装成标准 A2A Agent。
- 将 ACP 的会话与前台工作生命周期映射为 A2A task、message、状态和流式事件。
- 托管 ACP Agent 的进程、连接和会话，使其生命周期独立于单个 A2A HTTP/SSE 请求。
- 转发 ACP 的流式消息和 tool call，并支持取消与 permission request 的双向交互。
- 首期仅支持稳定的 ACP v1 协议。
- permission 审批必须可审计、幂等并默认拒绝不完整或失效的请求。

## 协议映射

| ACP | A2A |
| --- | --- |
| `session/prompt` 开始处理 | `WORKING` |
| `agent_thought_chunk` | 通过 A2A 扩展 `DataPart` 流式转发，并与最终回答明确区分 |
| `tool_call` / `tool_call_update` | 通过 A2A 扩展 `DataPart` 流式转发；tool call 状态不改变 task 状态 |
| `session/request_permission` | `INPUT_REQUIRED` |
| permission response | 同一 `taskId`、`contextId` 上的新 `SendMessage` |
| `session/prompt` 返回 `end_turn` | `COMPLETED` |
| `session/prompt` 返回 `cancelled` | `CANCELED` |
| `session/prompt` 返回 `refusal` | `REJECTED` |

普通 permission request 需要用户明确选择，应映射为 `INPUT_REQUIRED`。只有 OAuth、凭证授权等可在协议外完成的认证流程才考虑 `AUTH_REQUIRED`。

## Permission 扩展

A2A 当前没有标准 permission option 数据结构。`gate` 应定义并在 Agent Card 中声明一个扩展 URI，通过 `DataPart` 传递结构化请求和回答。

请求示例：

```json
{
  "type": "https://anra.dev/a2a/extensions/acp-permission/v1/request",
  "permissionId": "perm_01",
  "title": "Run the test suite?",
  "description": "The agent wants to execute tests.",
  "options": [
    { "optionId": "allow_once", "name": "Allow once", "kind": "allow_once" },
    { "optionId": "reject_once", "name": "Reject", "kind": "reject_once" }
  ],
  "subject": {
    "type": "tool_call",
    "toolCallId": "call_01"
  }
}
```

回答示例：

```json
{
  "type": "https://anra.dev/a2a/extensions/acp-permission/v1/response",
  "permissionId": "perm_01",
  "optionId": "allow_once"
}
```

扩展是 `gate` 的稳定边界，不应直接暴露某个 ACP SDK 的生成类型。ACP v1 adapter 从 `session/request_permission` 的 `toolCall` 和 `options` 构造内部 permission 模型；缺少展示标题时使用 `gate` 定义的通用标题，并保留原始 tool call 的不可变快照。

## Tool Call 和 Thought 扩展

`gate` 应分别定义 tool call 和 thought 扩展，不把 ACP 的观测事件直接编码进 A2A 核心类型：

- `https://anra.dev/a2a/extensions/acp-tool-call/v1`
- `https://anra.dev/a2a/extensions/acp-thought/v1`

两个扩展都应在 Agent Card 中声明。它们用于增强执行过程的可观测性，不影响客户端提交普通 prompt 或接收最终结果，因此默认 `required: false`。客户端未启用对应扩展时，`gate` 可以省略相关事件，但仍须正确维护 task 状态并返回最终 Artifact。

扩展事件通过 `TaskStatusUpdateEvent.status.message` 中的 `DataPart` 发送。发送 tool call 或 thought 更新时，A2A task 保持当前状态，通常为 `WORKING`；tool call 自身的 `completed` 或 `failed` 不代表整个 task 进入终态。

Tool call 示例：

```json
{
  "type": "https://anra.dev/a2a/extensions/acp-tool-call/v1",
  "toolCallId": "call_01",
  "sequence": 12,
  "status": "in_progress",
  "title": "Run tests",
  "kind": "execute",
  "content": [],
  "locations": []
}
```

Thought 示例：

```json
{
  "type": "https://anra.dev/a2a/extensions/acp-thought/v1",
  "thoughtId": "thought_01",
  "sequence": 13,
  "delta": "I should inspect the failing package first."
}
```

扩展设计必须遵循以下规则：

- `toolCallId` 必须保留 ACP 提供的 ID；缺失或无法关联时按协议错误处理，不能由 `gate` 猜测或替换。
- `thoughtId` 优先使用 ACP message ID；缺失时由 `gate` 为连续 thought chunk 生成稳定 ID。
- `sequence` 使用当前 A2A task 事件日志的单调递增序号，使 tool call、thought、文本和状态事件可以恢复全局顺序。
- adapter 先将 ACP v1 wire 类型转换为 `gate` 内部模型，再由扩展 codec 编码，扩展 schema 不直接依赖 ACP SDK 的生成类型。
- `rawInput`、`rawOutput`、环境变量、凭证和其他敏感字段默认不对外发送；需要开放时必须经过显式配置和脱敏策略。
- 扩展采用 URI 版本控制；破坏兼容性的字段变更必须发布新的 URI，新增可选字段可以保持当前版本。
- permission 扩展通过 `toolCallId` 引用 tool call，但不能依赖客户端已经接收过 tool call 扩展事件；permission request 必须自包含完成审批所需的信息。

## Runtime 模型

`gate` 必须将 Agent Runtime 与单个 HTTP/SSE 请求解耦。ACP session 与 A2A context、ACP prompt turn 与 A2A task 的关系固定为：

```mermaid
sequenceDiagram
    autonumber
    participant C as A2A Client
    participant G as gate / A2A Server
    participant S as SessionActor
    participant T as TaskActor
    participant A as ACP v1 Agent

    C->>G: Get Agent Card
    G-->>C: A2A capabilities + gate extensions

    C->>G: SendStreamingMessage(prompt, contextId?)
    G->>S: Get or create session by contextId

    alt New ACP session
        S->>A: Start process and JSON-RPC connection
        S->>A: initialize(protocolVersion: 1)
        A-->>S: capabilities + protocolVersion: 1
        S->>A: session/new(cwd, mcpServers)
        A-->>S: sessionId
        S->>S: Bind sessionId to one contextId
    else Existing ACP session
        S->>S: Reuse sessionId and the same contextId
    end

    S->>T: Create taskId for this prompt turn
    T-->>C: Task(WORKING, taskId, contextId)
    T->>A: session/prompt(sessionId, prompt)

    loop Each ACP session/update
        alt Agent message
            A-->>T: agent_message_chunk
            T-->>C: TaskArtifactUpdateEvent
        else Thought
            A-->>T: agent_thought_chunk
            T-->>C: TaskStatusUpdateEvent + thought DataPart
        else Tool call
            A-->>T: tool_call / tool_call_update
            T-->>C: TaskStatusUpdateEvent + tool-call DataPart
        end
    end

    opt ACP requests permission
        A->>T: session/request_permission (JSON-RPC request pending)
        T->>T: Snapshot request and create permissionId
        T-->>C: Task(INPUT_REQUIRED) + permission DataPart
        Note over C,T: The current stream may close; process, session, turn and waiter remain alive
        C->>G: SendMessage(response, same taskId/contextId)
        G->>T: Validate task, session, permission, option and CAS version
        T-->>A: Permission outcome with original optionId
        T-->>C: TaskStatusUpdateEvent(WORKING)
    end

    alt Prompt finishes
        A-->>T: session/prompt response(stopReason)
        T->>T: Map stopReason to terminal TaskState
        T-->>C: COMPLETED / FAILED / REJECTED
    else Client cancels task
        C->>G: CancelTask(taskId)
        G->>T: Cancel current turn
        T-->>A: Resolve pending permissions as cancelled
        T->>A: session/cancel(sessionId)
        A-->>T: session/prompt response(cancelled)
        T-->>C: CANCELED
    end

    opt Next turn in the same conversation
        C->>G: SendStreamingMessage(prompt, same contextId)
        G->>S: Reuse the bound ACP session
        S->>T: Create a new taskId for the new turn
        Note over C,A: Same contextId and sessionId; different taskId
    end
```

- 一个 ACP `sessionId` 对应一个 A2A `contextId`。
- 同一 ACP session 中产生的所有 A2A task 必须使用相同的 `contextId`。
- 每次 ACP `session/prompt` turn 对应一个新的 A2A `taskId`。
- 一个 A2A task 只表示一个 ACP turn；task 进入终态后不可用于开始下一个 turn。
- permission response 是当前 turn 的继续，必须发送到原 `taskId`，不能创建新 task。
- 同一 session 内的 prompt turn 默认串行执行；前一个 task 未进入终态前，不启动下一个 turn。

每个 ACP session 由常驻 `SessionActor` 管理：

- ACP `sessionId`、连接和子进程
- 唯一对应的 A2A `contextId`
- 当前 turn 和等待执行的 turn
- session 生命周期、关闭和异常恢复

每个 ACP turn 由 `TaskActor` 管理：

- A2A `taskId` 及父 `contextId`
- 当前任务状态
- 待处理 permission 集合
- 有序事件日志及 stream subscribers
- 取消、超时、版本和幂等控制

当 `gate` 发布 `INPUT_REQUIRED` 后，当前 A2A stream 可以结束，但 ACP 进程、session、prompt turn 和反向 JSON-RPC request 必须保持存活。客户端随后在同一 task 上发送 permission response，TaskActor 校验后唤醒等待中的 ACP request，再发布 `WORKING`。

新 stream 应先订阅事件日志，再提交 permission response，避免错过恢复后立即产生的状态或输出。

## 协议版本范围

首期 `gate` 在 `initialize` 时只接受 ACP v1。Agent 未协商到 `protocolVersion: 1` 时必须终止初始化并返回明确错误，不能尝试按其他版本继续通信。

ACP v2 不属于首期实现范围。后续接入 v2 时，应通过独立 adapter 和 feature flag 引入，不改变本扩展已经发布的 wire schema。

## 正确性和安全要求

- 为每个请求生成 `gate` 自己的不可猜测 `permissionId`。
- 保存 title、options、subject 和关联 tool call 的不可变快照及摘要。
- 回答必须匹配 task、session、permission 和原始 option。
- 使用 compare-and-swap；第一个有效回答获胜，重复回答幂等返回当前状态。
- 请求过期、上下文缺失、option 未知或 subject 不一致时 fail closed。
- 同时存在多个 permission 时，payload 支持数组；所有阻塞请求解决前保持 `INPUT_REQUIRED`。
- `CancelTask` 应以 `cancelled` 回答所有 pending permission，发送 ACP `session/cancel`，并等待 `session/prompt` 返回 `cancelled` 或超时退出。
- `gate` 重启后不能重建原 JSON-RPC waiter。除非 Agent resume 后重新发起 permission，否则将 task 标记为失败并说明原因。

## 建议模块

```text
gate/
  internal/a2a/             # A2A server and extension codec
  internal/acp/             # ACP client abstraction
  internal/acp/v1/          # v1 adapter
  internal/runtime/         # TaskActor, session/process lifecycle
  internal/permission/      # pending requests, validation, audit
  internal/eventlog/        # ordered replay and stream subscription
```

## 验收标准

- A2A 客户端可以发现并调用一个由 ACP Agent 提供能力的 Agent Card。
- 文本和 tool call 更新能够按顺序流式转发。
- thought 内容能够流式转发给 A2A 客户端，并可与最终回答区分。
- Agent Card 分别声明 tool call、thought 和 permission 扩展；客户端可独立协商启用。
- tool call 和 thought 扩展事件包含稳定 ID 与单调递增 `sequence`，并与其他 task 事件保持顺序。
- 客户端未启用可选的 tool call 或 thought 扩展时，task 仍能正常执行并返回最终结果。
- 同一 ACP session 的多个 turn 分别产生不同 `taskId`，但保持相同 `contextId`。
- permission request 会使 task 进入 `INPUT_REQUIRED`，原 stream 可以关闭且 ACP runtime 不被销毁。
- 客户端可在同一 task 上回答 permission，ACP Agent 随后继续执行。
- 重复、过期、错误 task 或错误 option 的回答不会被执行。
- 取消会终止 pending permission 和 ACP 前台工作。
- ACP v1 有完整的状态映射、permission 恢复、取消和并发测试。

## 参考资料

- [ACP v1 schema](https://github.com/agentclientprotocol/agent-client-protocol/blob/main/schema/v1/schema.json)
- [A2A: Life of a Task](https://github.com/a2aproject/A2A/blob/main/docs/topics/life-of-a-task.md)
- [A2A: Streaming and Async](https://github.com/a2aproject/A2A/blob/main/docs/topics/streaming-and-async.md)
