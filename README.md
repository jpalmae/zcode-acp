# zcode-acp

An **ACP (Agent Client Protocol) adapter** that lets the [ZCode](https://z.ai) CLI
participate as a first-class agent in [Block's Buzz](https://github.com/block/buzz),
with the same streaming, tool-call, and permission semantics Codex has via
[`zed-industries/codex-acp`](https://github.com/zed-industries/codex-acp).

```text
buzz-acp ──stdio(ACP / JSON-RPC)──► zcode-acp ──spawn──► node zcode.cjs app-server
  (host)                              (this shim)         (one per ACP session)
     ◄── session/update (text/tool/usage) ──             ◄── NDJSON events ──
     ── session/requestPermission ──►                    ──► interaction/requestPermission ──►
```

`zcode-acp` implements the **Agent** role of ACP v1. Buzz's `buzz-acp` harness spawns it
as a subprocess, sends `initialize` / `session/new` / `session/prompt` over stdio, and
receives streamed `session/update` notifications as ZCode works.

## How it talks to ZCode

ZCode is a Node bundle (`zcode.cjs`) that exposes a stdio **app-server** speaking the
"ZCode Protocol" (NDJSON — one JSON object per line, **no `"jsonrpc":"2.0"` field**).
The adapter spawns one `app-server` per ACP session and drives it:

- `session/create` + `session/subscribe` to open a session and opt into streaming.
- `session/send` with the prompt; all output arrives as `session/event` notifications
  (`part.delta` for token streaming, `tool.updated` for the tool-call lifecycle,
  `permission.requested` + `interaction/requestPermission` for approvals,
  `turn.completed` / `turn.failed` to end the turn).
- `session/stop` / `session/close` for cancel and teardown.

This was reverse-engineered from the running binary (see the in-repo research notes);
the event schemas are typed in [`src/zcode/protocol.rs`](src/zcode/protocol.rs).

## Feature parity with codex-acp

| Capability | Status | Notes |
| --- | --- | --- |
| `initialize` with capabilities + auth methods | ✅ | Advertises `promptCapabilities` (text/image, embedded context), `loadSession`, `sessionCapabilities` (list/resume/close), `auth.logout`, and two `AuthMethod`s. |
| `authenticate` / `logout` | ✅ | `zai-oauth` runs the full Z.AI init/poll OAuth flow and persists credentials in `~/.zcode/v2/credentials.json` (AES-256-GCM, same format as the ZCode CLI). `zai-api-key` verifies a pre-configured provider. |
| `session/new` | ✅ | Spawns a dedicated `app-server`, forwards `mcpServers` and `cwd`. |
| `session/load` / `session/resume` | ✅ | Resumes a ZCode session id (`sess_...`) via `session/resume`. |
| `session/list` | ✅ (stub) | Returns an empty list (could enumerate ZCode's persisted sessions). |
| `session/prompt` | ✅ | Streams text deltas, reasoning deltas, tool calls, tool updates, usage, and ends with a `StopReason`. |
| `session/setMode` | ✅ (deferred) | Logs the requested mode; applied on the next prompt. |
| `session/cancel` | ✅ | Issues `session/stop` on the live session. |
| `session/close` | ✅ | `session/close` + subprocess shutdown. |
| Tool calls → ACP `ToolCall` / `ToolCallUpdate` | ✅ | `tool.updated` events (`scheduled`/`started`/`result`/`error`) translated; `ToolKind` inferred from the tool name; file paths surfaced as `ToolCallLocation`s. |
| Token + reasoning streaming | ✅ | `part.delta` with `field: text` → `AgentMessageChunk`; `field: reasoning` → `AgentThoughtChunk`. |
| Usage reporting | ✅ | `turn.completed.usage` → `UsageUpdate`. |
| Permissions (bidirectional) | ✅ | `interaction/requestPermission` from ZCode is re-asked via ACP `session/request_permission`; the host's decision is mapped back to a ZCode `qg` response (`allow`/`deny`/...) and forwarded with the correct wire id. |
| Auth-aware provider config | ✅ | `~/.zcode/cli/config.json` (or `--data-base-dir`) is honored; OAuth writes the same credential store `zcode login` uses. |

## Build

Requires Rust 1.85+ (edition 2024) and `node` on `PATH`.

```bash
cargo build --release
# binary: target/release/zcode-acp
```

## Configure ZCode

The `app-server` requires a model provider configured at `~/.zcode/cli/config.json`
before it will run a turn. Either:

- run `zcode login` once (the adapter's `authenticate` method with `zai-oauth` does
  the same thing at runtime), or
- create the config file by hand with an enabled provider + API key.

Verify headless mode works on its own:

```bash
node "/Applications/ZCode.app/Contents/Resources/glm/zcode.cjs" -p "say PONG"
```

## Run

```bash
zcode-acp \
  --zcode-path "/Applications/ZCode.app/Contents/Resources/glm/zcode.cjs" \
  --node node
```

(The `--zcode-path` is auto-detected inside `/Applications/ZCode.app` on macOS if omitted.)

| Flag / env | Meaning | Default |
| --- | --- | --- |
| `--zcode-path` / `ZCODE_ACP_ZCODE_PATH` | Path to `zcode.cjs`. | Auto-detected in `/Applications/ZCode.app`. |
| `--node` / `ZCODE_ACP_NODE` | Executable to run `zcode.cjs`. | `node` |
| `--zcode-args` / `ZCODE_ACP_ZCODE_ARGS` | Extra args appended to every spawn. | (none) |
| `--fallback-cwd` / `ZCODE_ACP_FALLBACK_CWD` | `cwd` when a session does not carry one. | process cwd |
| `--prompt-timeout-secs` / `ZCODE_ACP_PROMPT_TIMEOUT_SECS` | Hard per-prompt timeout. | `900` |
| `--no-browser` / `ZCODE_ACP_NO_BROWSER` | Disable the OAuth browser flow. | `false` |
| `--data-base-dir` / `ZCODE_DATA_BASE_DIR` | Override `~/.zcode` for credentials/config. | `$HOME` |

Logs go to stderr under `RUST_LOG` (default `info`); stdout is the protocol channel.

## Register ZCode in Buzz

Two changes, mirroring how Codex is registered.

### 1. Add `zcode` to `buzz-acp`'s agent normaliser

In `crates/buzz-acp/src/config.rs`, function `default_agent_args`, add `zcode` /
`zcode-acp` to the branch that needs no default args:

```rust
"codex" | "codex-acp" | "claude-agent-acp" | /* ... */ | "zcode" | "zcode-acp" => Some(Vec::new()),
```

Rebuild `buzz-acp`. No other Buzz change is needed — the rest of the integration is the
ACP contract, which `zcode-acp` speaks.

### 2. Install the persona pack

```bash
mkdir -p ~/.buzz/packs/zcode/.plugin
cp pack/zcode.persona.md      ~/.buzz/packs/zcode/
cp pack/.plugin/plugin.json   ~/.buzz/packs/zcode/.plugin/
```

### 3. Launch

```bash
buzz-acp --agent-command zcode-acp --secret-key <nostr-hex-key> ...
```

## Test locally without Buzz

A fake `app-server` (Node) and a minimal ACP client (Python) live under `tests/`:

```bash
cargo build
python3 tests/acp_client.py
```

Expected:

```
[ok] initialize -> agent=zcode-acp, 2 auth method(s)
[ok] session/new -> sessionId=..., mode=yolo
[ok] session/prompt -> stop=end_turn, text='PONG'
[ok] streamed 1 ToolCall(s) + 1 update(s)
[ok] session/close
```

This validates the full ACP round-trip with streaming text, a tool call (schedule +
result), and turn completion — independently of whether your ZCode installation is
configured for headless use.

Unit tests cover the prompt-text flattening, ANSI stripping, tool-call translation,
and the AES-256-GCM credential round-trip.

```bash
cargo test --lib
```

## Repository layout

```
src/
├── main.rs             # binary entry + CLI flags
├── lib.rs              # run(): wire ByteStreams(stdout, stdin) to the agent
├── agent.rs            # ZcodeAgent: all ACP handlers + event → ACP translation
├── acp_translator.rs   # ZCode events → ACP SessionUpdate / ToolCall / permissions
└── zcode/
    ├── mod.rs          # ZcodeSession + DriverState + dispatcher/command-loop tasks
    ├── transport.rs    # spawn app-server, NDJSON framing
    ├── protocol.rs     # typed ZCode Protocol messages + event schemas
    └── auth.rs         # Z.AI OAuth init/poll + AES-256-GCM credential store
tests/
├── fake-app-server.cjs # canned app-server for the E2E test
└── acp_client.py       # minimal ACP client driving the binary
pack/                   # Buzz persona pack
```

## Known limitations

- **Tool-call progress** (`stdout_tail` / `stderr_tail` on `kind: progress`) is
  dropped to avoid flooding; only `scheduled` + `result`/`error` are surfaced.
- **`session/list` returns an empty list** rather than enumerating ZCode's
  persisted sessions per workspace.
- **`session/setMode` is logged and deferred** to the next prompt rather than
  mutating the live session mid-turn.
- **`model.streaming`** raw provider chunks are not separately surfaced (the
  `part.delta` stream already carries the user-visible text/reasoning).
- **Other server→client interactions** (`interaction/requestUserInput`,
  `interaction/browserList`, `interaction/requestProviderRuntimeHeaders`) are
  answered with a safe default rather than bridged to the host.

## License

Apache-2.0, matching both Buzz and the ACP SDK.
