#!/usr/bin/env node
// Fake ZCode `app-server` for end-to-end testing of zcode-acp.
//
// Speaks the ZCode Protocol over stdio (NDJSON, no "jsonrpc" field). Driven
// by tests/acp_client.py through the real zcode-acp binary. Implements:
//   - session/create  -> returns a sessionId
//   - session/subscribe -> ack
//   - session/send     -> ack, then streams:
//                          * session/event: part.delta (text) "PONG"
//                          * session/event: tool.updated (scheduled+result)
//                          * session/event: turn.completed
//   - session/stop     -> ack
//   - session/close    -> ack
//
// Reading is line-based; one JSON object per line. We never emit "jsonrpc".

const readline = require("readline");

const rl = readline.createInterface({ input: process.stdin, terminal: false });

let sessionIdCounter = 0;
let activeSession = null;
let seqCounter = 0;

function send(obj) {
  process.stdout.write(JSON.stringify(obj) + "\n");
}

function respond(id, result) {
  send({ id, result });
}

function event(sessionId, type, payload) {
  // Server-pushed notification (no id).
  send({
    method: "session/event",
    params: {
      eventId: `evt_${++seqCounter}`,
      sessionId,
      seq: seqCounter,
      timestamp: Date.now(),
      type,
      payload,
    },
  });
}

function handle(line) {
  let msg;
  try {
    msg = JSON.parse(line);
  } catch (e) {
    send({ id: "invalid-message", error: { code: -32700, message: "Parse error" } });
    return;
  }
  const { id, method, params } = msg;
  switch (method) {
    case "session/create": {
      activeSession = `sess_fake_${++sessionIdCounter}`;
      respond(id, { sessionId: activeSession });
      break;
    }
    case "session/subscribe": {
      respond(id, { sessionId: activeSession, eventSeq: seqCounter, events: [] });
      break;
    }
    case "session/send": {
      respond(id, { sessionId: activeSession, accepted: true, stateRevision: 1 });
      // Stream a reply asynchronously, mirroring the real app-server's event
      // sequence: model.streaming text deltas (kind = text_delta), then a
      // tool.updated schedule/result pair, then turn.completed with the real
      // usage payload shape (inputTokens/outputTokens/totalTokens).
      setTimeout(() => {
        const sid = activeSession;
        // 1. model text delta (the real app-server streams token-by-token)
        event(sid, "model.streaming", {
          assistantMessageId: "msg_1",
          delta: "P",
          done: false,
          kind: "text_delta",
        });
        event(sid, "model.streaming", {
          assistantMessageId: "msg_1",
          delta: "ONG",
          done: false,
          kind: "text_delta",
        });
        event(sid, "model.streaming", {
          assistantMessageId: "msg_1",
          delta: null,
          done: true,
          kind: "text_delta",
        });
        // 2. tool scheduled
        event(sid, "tool.updated", {
          toolCallId: "call_1",
          toolName: "Bash",
          kind: "scheduled",
          input: { command: "echo hi" },
        });
        // 3. tool result
        event(sid, "tool.updated", {
          toolCallId: "call_1",
          toolName: "Bash",
          kind: "result",
          result: { output: "hi\n" },
        });
        // 4. turn completed (real usage payload shape)
        event(sid, "turn.completed", {
          response: "PONG",
          tokenCount: 11813,
          usage: {
            source: "provider",
            modelRequestCount: 1,
            inputTokens: 11810,
            outputTokens: 3,
            totalTokens: 11813,
            cacheReadTokens: 9088,
            cacheWriteTokens: 0,
            reasoningTokens: 0,
          },
          toolCallCount: 1,
          resultType: "success",
          duration: 3437,
        });
      }, 50);
      break;
    }
    case "session/stop":
      respond(id, { stopped: true });
      break;
    case "session/close":
      respond(id, { closed: true });
      activeSession = null;
      break;
    default:
      send({ id, error: { code: -32601, message: `Method not found: ${method}` } });
  }
}

rl.on("line", handle);
rl.on("close", () => process.exit(0));
