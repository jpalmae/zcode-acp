//! Types for the "ZCode Protocol" spoken over stdio by `zcode.cjs app-server`.
//!
//! Key facts (verified empirically against the running server — see the
//! research notes in the repo):
//!
//! - Framing is **NDJSON**: one JSON object per `\n`-terminated line.
//! - The envelope is `.strict()` and **rejects `"jsonrpc":"2.0"`**.
//! - There is **no `initialize` handshake**; you call `session/*` directly.
//! - `session/send` takes `content: string` and acks immediately; streaming
//!   arrives as `session/event` notifications after `session/subscribe`.
//! - Server→client requests (e.g. `interaction/requestPermission`) use wire
//!   ids like `"server-<N>"` and may be re-sent under new ids until answered.
//!
//! Only the parts of the protocol the adapter actually uses are typed
//! strongly; everything else stays as [`serde_json::Value`].

use serde::{Deserialize, Serialize};
use serde_json::Value;

// ---------------------------------------------------------------------------
// Wire envelope
// ---------------------------------------------------------------------------

/// A message sent from the adapter to the app-server: either a request
/// (with `id`) or a notification (without).
#[derive(Debug, Clone, Serialize)]
pub struct OutgoingMessage<'a> {
    /// Omitted for notifications.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<RequestId>,
    pub method: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

/// A message received from the app-server. Discriminated by which fields are
/// present: `result` → success response, `error` → error response, neither
/// with a `method` → notification, neither without a `method` is invalid.
#[derive(Debug, Clone, Deserialize)]
pub struct IncomingMessage {
    #[serde(default)]
    pub id: Option<RequestId>,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(default)]
    pub params: Option<Value>,
    #[serde(default)]
    pub result: Option<Value>,
    #[serde(default)]
    pub error: Option<RpcError>,
}

/// JSON-RPC id. The app-server uses both numbers and strings (`"server-N"`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(untagged)]
pub enum RequestId {
    Num(i64),
    Str(String),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    #[serde(default)]
    pub data: Option<Value>,
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "rpc error {}: {}", self.code, self.message)
    }
}

/// Classification of an [`IncomingMessage`].
#[derive(Debug, Clone)]
pub enum Incoming {
    /// Response to a request we sent.
    Response {
        id: RequestId,
        result: ResponseResult,
    },
    /// Notification pushed by the server (no `id`).
    Notification {
        method: String,
        params: Option<Value>,
    },
    /// A request the server directs at us (e.g.
    /// `interaction/requestPermission`). We must reply with the same `id`.
    ServerRequest {
        id: RequestId,
        method: String,
        params: Option<Value>,
    },
}

#[derive(Debug, Clone)]
pub enum ResponseResult {
    Ok(Value),
    Err(RpcError),
}

impl IncomingMessage {
    /// Classify this raw message into a structured [`Incoming`].
    ///
    /// Rules (matching the app-server's strict union):
    /// - has `id` + (`result` xor `error`) → Response
    /// - has `id` + `method` → ServerRequest
    /// - no `id` + `method` → Notification
    /// - otherwise → returns `None` (malformed; caller logs and ignores)
    pub fn classify(self) -> Option<Incoming> {
        match (self.id, self.method, self.result, self.error) {
            (Some(id), _, Some(result), None) => Some(Incoming::Response {
                id,
                result: ResponseResult::Ok(result),
            }),
            (Some(id), _, None, Some(err)) => Some(Incoming::Response {
                id,
                result: ResponseResult::Err(err),
            }),
            (Some(id), Some(method), _, _) => Some(Incoming::ServerRequest {
                id,
                method,
                params: self.params,
            }),
            (None, Some(method), _, _) => Some(Incoming::Notification {
                method,
                params: self.params,
            }),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Method names (the `Rr` registry, abridged to what we use)
// ---------------------------------------------------------------------------

pub mod methods {
    pub const SESSION_CREATE: &str = "session/create";
    pub const SESSION_RESUME: &str = "session/resume";
    pub const SESSION_LIST: &str = "session/list";
    pub const SESSION_SUBSCRIBE: &str = "session/subscribe";
    pub const SESSION_SEND: &str = "session/send";
    pub const SESSION_STOP: &str = "session/stop";
    pub const SESSION_CLOSE: &str = "session/close";
    pub const SESSION_MESSAGES: &str = "session/messages";
    pub const SESSION_SET_MODE: &str = "session/setMode";
    pub const SESSION_USAGE: &str = "session/usage";
    pub const WORKSPACE_READ_STATE: &str = "workspace/readState";
    pub const SESSION_EVENT: &str = "session/event";
}

// ---------------------------------------------------------------------------
// session/create request
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct CreateSessionParams {
    pub workspace: WorkspaceRef,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<ModelRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mcp_servers: Option<Vec<McpServerEntry>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkspaceRef {
    pub workspace_key: String,
    pub workspace_path: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ModelRef {
    pub provider_id: String,
    pub model_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
}

/// One entry of `session/create`'s `mcpServers`. The exact field set varies;
/// we pass through a typed common subset and preserve the rest via the raw
/// JSON builder in [`crate::zcode::session`].
#[derive(Debug, Clone, Serialize)]
pub struct McpServerEntry {
    #[serde(rename = "serverLabel")]
    pub server_label: String,
    #[serde(flatten)]
    pub rest: Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateSessionResult {
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub state_revision: Option<i64>,
    #[serde(flatten)]
    pub extra: Value,
}

// ---------------------------------------------------------------------------
// session/subscribe
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscribeParams {
    pub session_id: String,
    pub delivery_kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after_seq: Option<u64>,
    pub include_snapshot: bool,
}

// ---------------------------------------------------------------------------
// session/send
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SendParams {
    pub session_id: String,
    pub content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SendResult {
    #[serde(default)]
    pub accepted: Option<bool>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub state_revision: Option<i64>,
}

// ---------------------------------------------------------------------------
// session/list
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct ListSessionsResult {
    #[serde(default)]
    pub sessions: Vec<SessionSummary>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SessionSummary {
    pub session_id: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub mode: Option<String>,
    #[serde(default)]
    pub workspace: Option<Value>,
    #[serde(default)]
    pub created_at: Option<u64>,
    #[serde(default)]
    pub updated_at: Option<u64>,
}

// ---------------------------------------------------------------------------
// Events (session/event params)
// ---------------------------------------------------------------------------

/// Common envelope fields for every event.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventEnvelope {
    pub event_id: String,
    pub session_id: String,
    pub seq: u64,
    #[serde(default)]
    pub turn_id: Option<String>,
    pub timestamp: u64,
    /// The discriminator: `session.created`, `turn.started`, `part.delta`,
    /// `tool.updated`, `permission.requested`, `turn.completed`, ...
    #[serde(rename = "type")]
    pub type_: String,
    #[serde(default)]
    pub payload: Option<Value>,
}

// ----- tool.updated payload, discriminated by `kind` -----

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolUpdatedPayload {
    pub tool_call_id: String,
    #[serde(default)]
    pub tool_name: Option<String>,
    #[serde(default)]
    pub parent_tool_call_id: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    pub kind: ToolKind,
    /// Present on `scheduled`.
    #[serde(default)]
    pub input: Option<Value>,
    /// Present on `result`.
    #[serde(default)]
    pub result: Option<Value>,
    /// Present on `error`.
    #[serde(default)]
    pub error: Option<Value>,
    /// Present on `started` / `result` / `error`.
    #[serde(default)]
    pub started_at: Option<u64>,
    #[serde(default)]
    pub completed_at: Option<u64>,
    /// Present on `progress` (live Bash output tails).
    #[serde(default)]
    pub stdout_tail: Option<String>,
    #[serde(default)]
    pub stderr_tail: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ToolKind {
    Scheduled,
    Started,
    Progress,
    Result,
    Error,
    #[serde(other)]
    Other,
}

// ----- part.delta payload -----

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PartDeltaPayload {
    pub message_id: String,
    pub part_id: String,
    #[serde(default)]
    pub field: Option<String>, // "text" | "reasoning" | "input" | "output"
    pub delta: String,
}

// ----- permission.requested payload (event form; the request form omits
//       suggestedPermissionUpdates but is otherwise the same shape) -----

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PermissionPayload {
    #[serde(default)]
    pub request_id: Option<String>,
    pub tool_call_id: String,
    #[serde(default)]
    pub tool_name: Option<String>,
    pub risk_level: String, // "low" | "medium" | "high" | "critical"
    pub reason: String,
    #[serde(default)]
    pub input: Option<Value>,
    #[serde(default)]
    pub options: Vec<PermissionOption>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PermissionOption {
    pub option_id: String,
    pub kind: String,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    /// A full pre-built `qg` response for this option.
    pub response: Value,
}

// ----- turn.started / turn.completed / turn.failed payloads -----

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnStartedPayload {
    pub turn_number: u64,
    #[serde(default)]
    pub input: Option<String>,
    #[serde(default)]
    pub input_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnCompletedPayload {
    #[serde(default)]
    pub response: Option<String>,
    #[serde(default)]
    pub token_count: Option<u64>,
    #[serde(default)]
    pub usage: Option<Value>,
    #[serde(default)]
    pub tool_call_count: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnFailedPayload {
    #[serde(default)]
    pub error: Option<Value>,
    #[serde(default)]
    pub turn_phase: Option<String>,
}

// ---------------------------------------------------------------------------
// interaction/requestPermission request params + response (`qg`)
// ---------------------------------------------------------------------------

/// What we send back to the server for `interaction/requestPermission`.
#[derive(Debug, Clone, Serialize)]
pub struct PermissionResponse {
    pub decision: PermissionDecision,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modified_input: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permission_updates: Option<Vec<PermissionRuleUpdate>>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PermissionDecision {
    Allow,
    Deny,
    Escalate,
    Modify,
}

#[derive(Debug, Clone, Serialize)]
pub struct PermissionRuleUpdate {
    #[serde(rename = "type")]
    pub type_: &'static str, // always "addRules"
    pub behavior: PermissionRuleBehavior,
    pub rules: Vec<PermissionRule>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PermissionRuleBehavior {
    Allow,
    Deny,
    Ask,
}

#[derive(Debug, Clone, Serialize)]
pub struct PermissionRule {
    pub tool_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rule_content: Option<String>,
}

// ---------------------------------------------------------------------------
// Error code constants (subset that we react to)
// ---------------------------------------------------------------------------

pub mod codes {
    /// A prompt is already running for this session.
    pub const PROMPT_ALREADY_RUNNING: i64 = -32010;
    /// The model bound to the session is unavailable.
    pub const MODEL_UNAVAILABLE: i64 = -32031;
    /// Method not found.
    pub const METHOD_NOT_FOUND: i64 = -32601;
    /// Invalid params.
    pub const INVALID_PARAMS: i64 = -32602;
    /// Internal error.
    pub const INTERNAL: i64 = -32603;
}

/// Internal stop-reason used by the session driver. Mapped to the ACP
/// `StopReason` by [`crate::acp_translator`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    EndTurn,
    Cancelled,
    MaxTurns,
}
