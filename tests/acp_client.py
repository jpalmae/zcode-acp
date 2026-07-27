#!/usr/bin/env python3
"""End-to-end test for `zcode-acp` against a fake ZCode `app-server`.

Drives the compiled `zcode-acp` binary as an ACP Agent over stdio (this side is
the ACP Client / host). The binary in turn spawns the fake app-server script,
which streams a canned reply (a text delta, a tool call schedule+result, and a
turn.completed event). We assert the ACP side surfaces all of that correctly.

Validates:
  - initialize advertises agentInfo.name == "zcode-acp" with auth_methods
  - session/new returns a sessionId and a modes payload
  - session/prompt streams:
      * an AgentMessageChunk ("PONG")
      * a ToolCall (Bash, scheduled)
      * a ToolCallUpdate (Completed)
      * then resolves with stopReason == "end_turn"

This is a test harness; not production code.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
ZCODE_ACP_BIN = REPO_ROOT / "target" / "debug" / "zcode-acp"
FAKE_SERVER = REPO_ROOT / "tests" / "fake-app-server.cjs"


def send(proc: subprocess.Popen, obj: dict) -> None:
    line = json.dumps(obj) + "\n"
    assert proc.stdin is not None
    proc.stdin.write(line.encode())
    proc.stdin.flush()


def read_message(proc: subprocess.Popen) -> dict:
    assert proc.stdout is not None
    raw = proc.stdout.readline()
    if not raw:
        raise EOFError("agent closed stdout")
    return json.loads(raw)


def expect_result(proc: subprocess.Popen, req_id: int) -> dict:
    """Skip notifications; return the result for req_id."""
    while True:
        msg = read_message(proc)
        if msg.get("id") == req_id:
            if "error" in msg:
                raise RuntimeError(f"RPC error for id={req_id}: {msg['error']}")
            return msg["result"]


def main() -> int:
    if not ZCODE_ACP_BIN.exists():
        print(f"ERROR: build first (cargo build): {ZCODE_ACP_BIN}", file=sys.stderr)
        return 2

    # The shim treats `--node` as the executable and `--zcode-path` as the
    # script. We point them at the fake app-server (a node script). The fake
    # sees argv[1] === "app-server" and dispatches on that.
    proc = subprocess.Popen(
        [
            str(ZCODE_ACP_BIN),
            "--node", "node",
            "--zcode-path", str(FAKE_SERVER),
            "--fallback-cwd", "/tmp",
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=sys.stderr,  # surface shim logs inline for debugging
    )

    try:
        # 1. initialize
        send(proc, {
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": 1, "clientCapabilities": {}},
        })
        init = expect_result(proc, 1)
        name = init.get("agentInfo", {}).get("name")
        assert name == "zcode-acp", f"agentInfo.name={name!r}"
        auth_methods = init.get("authMethods", [])
        assert len(auth_methods) >= 1, f"expected auth methods, got {auth_methods}"
        print(f"[ok] initialize -> agent={name}, {len(auth_methods)} auth method(s)")

        # 2. session/new
        send(proc, {
            "jsonrpc": "2.0", "id": 2, "method": "session/new",
            "params": {"cwd": "/tmp", "mcpServers": []},
        })
        new_session = expect_result(proc, 2)
        session_id = new_session["sessionId"]
        modes = new_session.get("modes", {})
        assert modes.get("currentModeId"), f"expected modes payload, got {modes}"
        print(f"[ok] session/new -> sessionId={session_id}, mode={modes['currentModeId']}")

        # 3. session/prompt — collect notifications while waiting for the result.
        print(f"[debug] sending session/prompt with sessionId={session_id}", file=sys.stderr)
        send(proc, {
            "jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": "say PONG"}],
            },
        })
        print("[debug] sent; waiting for notifications/result", file=sys.stderr)

        chunks: list[str] = []
        tool_calls: list[dict] = []
        tool_updates: list[dict] = []
        stop = None
        while stop is None:
            msg = read_message(proc)
            if msg.get("id") == 3:
                if "error" in msg:
                    raise RuntimeError(f"session/prompt error: {msg['error']}")
                stop = msg["result"]["stopReason"]
                continue
            method = msg.get("method", "")
            if method == "session/update":
                update = msg["params"]["update"]
                kind = update.get("sessionUpdate")
                if kind == "agent_message_chunk":
                    chunks.append(update.get("content", {}).get("text", ""))
                elif kind == "tool_call":
                    tool_calls.append(update)
                elif kind == "tool_call_update":
                    tool_updates.append(update)

        assert chunks, "expected at least one AgentMessageChunk"
        assert "PONG" in "".join(chunks), f"PONG missing: {chunks!r}"
        assert tool_calls, "expected at least one ToolCall"
        assert tool_updates, "expected at least one ToolCallUpdate"
        assert stop == "end_turn", f"unexpected stopReason={stop!r}"
        print(f"[ok] session/prompt -> stop={stop}, text={''.join(chunks)!r}")
        print(f"[ok] streamed {len(tool_calls)} ToolCall(s) + {len(tool_updates)} update(s)")

        # 4. session/close (clean teardown)
        send(proc, {
            "jsonrpc": "2.0", "id": 4, "method": "session/close",
            "params": {"sessionId": session_id},
        })
        _ = expect_result(proc, 4)
        print("[ok] session/close")

        return 0
    finally:
        try:
            if proc.poll() is None:
                proc.terminate()
                proc.wait(timeout=3)
        except Exception:
            proc.kill()


if __name__ == "__main__":
    raise SystemExit(main())
