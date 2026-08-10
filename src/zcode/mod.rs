//! The ZCode Protocol client: spawns and drives one `app-server` per ACP
//! session.
//!
//! A [`ZcodeSession`] owns:
//! - a dedicated `node zcode.cjs app-server` subprocess (via
//!   [`transport::AppServerTransport`]),
//! - a background task that reads NDJSON lines, dispatches responses to
//!   pending requests, and forwards notifications/events/interactions through
//!   channels,
//! - the bookkeeping to correlate a `session/send` with the eventual
//!   `turn.completed` / `turn.failed` events.
//!
//! Public surface used by the ACP layer:
//! - [`ZcodeSession::start`] — spawn + `session/create` (+ optional subscribe).
//! - [`ZcodeSession::resume`] — spawn + `session/resume`.
//! - [`ZcodeSession::send`] — `session/send`, returning a [`TurnStream`] of
//!   events until the turn ends.
//! - [`ZcodeSession::stop`] — `session/stop`.
//! - [`ZcodeSession::close`] — `session/close` + subprocess shutdown.
//! - [`ZcodeSession::answer_permission`] — reply to an
//!   `interaction/requestPermission`.

pub mod auth;
pub mod protocol;
pub mod transport;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::{Value, json};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;

use protocol::{
    CreateSessionParams, Incoming, IncomingMessage, PermissionResponse, RequestId, RpcError,
    SendParams, SubscribeParams, methods,
};
use transport::{AppServerTransport, IdSource};

/// Configuration shared by all sessions (mirrors [`crate::RunConfig`]).
#[derive(Debug, Clone)]
pub struct SessionConfig {
    pub zcode_path: PathBuf,
    pub node: String,
    pub extra_args: Vec<String>,
    pub fallback_cwd: PathBuf,
}

/// A live ZCode session backed by one `app-server` subprocess.
pub struct ZcodeSession {
    /// The ZCode `sessionId` returned by `session/create` / `session/resume`.
    pub zcode_session_id: String,
    /// Sender into which the reader task pushes every event/notification and
    /// every server→client interaction request. The ACP layer consumes this.
    pub events: mpsc::UnboundedReceiver<SessionMessage>,
    /// Commands the ACP layer sends back (send/stop/close/answer).
    pub cmd_tx: mpsc::UnboundedSender<SessionCommand>,
    /// Handle on the driver task so we can abort it on close.
    driver: JoinHandle<()>,
}

/// What flows out of a session to the ACP layer.
pub enum SessionMessage {
    /// A `session/event` notification (the streaming channel). Must be
    /// forwarded to the ACP host (translated) before consuming later messages.
    Event(protocol::EventEnvelope),
    /// A server→client request (`interaction/requestPermission`, etc.).
    /// `reply_tx` must be used to answer with a JSON `result`.
    ServerRequest {
        id: RequestId,
        method: String,
        params: Value,
        reply_tx: oneshot::Sender<Value>,
    },
    /// The turn finished. Emitted on the same channel as `Event`s, **after**
    /// all preceding events for the turn — so the consumer resolves the
    /// attached `completion` only once every tool/text update has been
    /// forwarded to the ACP host. This is what prevents the `prompt` handler
    /// from returning before the streamed updates land.
    TurnEnded {
        outcome: std::result::Result<TurnOutcome, TurnError>,
        completion: Option<oneshot::Sender<std::result::Result<TurnOutcome, TurnError>>>,
    },
    /// The transport died (subprocess exited, EOF, or fatal error).
    Closed(String),
}

/// Commands the ACP layer sends into the session driver.
#[derive(Debug)]
pub enum SessionCommand {
    /// Send a prompt and subscribe to events. The receiver is fulfilled when
    /// the turn ends (`turn.completed` / `turn.failed`).
    Send {
        text: String,
        reply: oneshot::Sender<Result<TurnOutcome, TurnError>>,
    },
    /// `session/stop` the active turn.
    Stop,
    /// Answer a pending server→client request.
    AnswerPermission {
        id: RequestId,
        response: PermissionResponse,
    },
    /// `session/close` and shut the subprocess down.
    Close,
}

/// Outcome of a completed turn.
#[derive(Debug, Clone)]
pub struct TurnOutcome {
    pub stop_reason: protocol::StopReason,
    pub final_text: Option<String>,
}

/// Why a turn ended unsuccessfully.
#[derive(Debug, Clone, thiserror::Error)]
pub enum TurnError {
    #[error("turn failed: {0}")]
    Failed(String),
    #[error("transport closed: {0}")]
    Transport(String),
    #[error("rpc error {code}: {message}")]
    Rpc { code: i64, message: String },
}

impl ZcodeSession {
    /// Spawn an `app-server`, call `session/create`, and start the driver.
    pub async fn start(
        cfg: SessionConfig,
        cwd: &Path,
        mcp_servers: Vec<Value>,
        mode: Option<String>,
        model: Option<protocol::ModelRef>,
    ) -> Result<Self, SessionError> {
        // Hand-build params to preserve unknown MCP fields verbatim.
        let workspace = protocol::WorkspaceRef {
            workspace_key: cwd.to_string_lossy().into_owned(),
            workspace_path: cwd.to_string_lossy().into_owned(),
        };
        let create = CreateSessionParams {
            workspace,
            mode,
            model,
            mcp_servers: if mcp_servers.is_empty() {
                None
            } else {
                Some(
                    mcp_servers
                        .iter()
                        .map(|s| protocol::McpServerEntry {
                            server_label: s
                                .get("name")
                                .and_then(|v| v.as_str())
                                .unwrap_or("unnamed")
                                .to_string(),
                            rest: s.clone(),
                        })
                        .collect(),
                )
            },
        };
        let params =
            serde_json::to_value(&create).map_err(|e| SessionError::Encode(e.to_string()))?;
        build_session(cfg, cwd, methods::SESSION_CREATE, params).await
    }

    /// Spawn an `app-server` and call `session/resume` (no streaming until
    /// `send` is called, which subscribes).
    pub async fn resume(
        cfg: SessionConfig,
        cwd: &Path,
        zcode_session_id: String,
    ) -> Result<Self, SessionError> {
        let params = json!({ "sessionId": zcode_session_id });
        build_session(cfg, cwd, methods::SESSION_RESUME, params).await
    }

    /// Forward a [`SessionCommand`] into the driver.
    pub fn command(&self, cmd: SessionCommand) {
        let _ = self.cmd_tx.send(cmd);
    }

    /// Clone the command sender, so callers can hold a cheap handle after the
    /// session itself has been moved into the pump task.
    pub fn command_sender(&self) -> mpsc::UnboundedSender<SessionCommand> {
        self.cmd_tx.clone()
    }

    /// Abort the driver task. Safe to call after close.
    pub fn abort(&self) {
        self.driver.abort();
    }
}

// ---------------------------------------------------------------------------
// Driver: shared protocol state plus two cooperating tasks — a *dispatcher*
// (reads incoming messages and resolves pending requests / events) and a
// *command loop* (handles Send/Stop/Close from the ACP layer). Splitting them
// is essential: a command handler that awaits a response (subscribe/send)
// must NOT own the read loop, or it deadlocks waiting for the very response
// the dispatcher would have delivered.
// ---------------------------------------------------------------------------

/// Shared, lock-protected driver state.
struct DriverState {
    write_tx: mpsc::UnboundedSender<Value>,
    id_src: Arc<IdSource>,
    zcode_session_id: String,
    pending: HashMap<RequestId, oneshot::Sender<Result<Value, RpcError>>>,
    active_turn: Option<ActiveTurn>,
    events_tx: mpsc::UnboundedSender<SessionMessage>,
    subscribed: bool,
}

struct ActiveTurn {
    /// Resolved on turn.completed/failed.
    completion: Option<oneshot::Sender<Result<TurnOutcome, TurnError>>>,
    #[allow(dead_code)]
    accumulated_text: String,
    #[allow(dead_code)]
    announced_tools: HashMap<String, ()>,
}

impl DriverState {
    /// Send a request and return a receiver for its response. Does NOT block:
    /// the dispatcher task will resolve the receiver when the response lands.
    fn request(
        &mut self,
        method: &str,
        params: Value,
    ) -> Result<oneshot::Receiver<Result<Value, RpcError>>, String> {
        let id = self.id_src.next();
        let msg = serde_json::to_value(&protocol::OutgoingMessage {
            id: Some(id.clone()),
            method,
            params: Some(params),
        })
        .map_err(|e| e.to_string())?;
        self.write_tx.send(msg).map_err(|e| e.to_string())?;
        let (tx, rx) = oneshot::channel();
        self.pending.insert(id, tx);
        Ok(rx)
    }

    fn dispatch(&mut self, raw: IncomingMessage) {
        match raw.classify() {
            Some(Incoming::Response { id, result }) => {
                if let Some(tx) = self.pending.remove(&id) {
                    let _ = tx.send(match result {
                        protocol::ResponseResult::Ok(v) => Ok(v),
                        protocol::ResponseResult::Err(e) => Err(e),
                    });
                }
            }
            Some(Incoming::Notification { method, params }) => {
                if method == methods::SESSION_EVENT {
                    if let Some(p) = params
                        && let Ok(ev) = serde_json::from_value::<protocol::EventEnvelope>(p.clone())
                    {
                        self.handle_event(ev.clone());
                        let _ = self.events_tx.send(SessionMessage::Event(ev));
                    }
                } else {
                    tracing::debug!(target: "zcode::driver", %method, "ignoring notification");
                }
            }
            Some(Incoming::ServerRequest { id, method, params }) => {
                // The ACP layer answers via `reply_tx`; a side task writes the
                // reply back through `write_tx` echoing the wire id.
                let (reply_tx, reply_rx) = oneshot::channel();
                let write_tx = self.write_tx.clone();
                let wait_id = id.clone();
                tokio::spawn(async move {
                    if let Ok(value) = reply_rx.await {
                        let val = json!({ "id": wait_id, "result": value });
                        let _ = write_tx.send(val);
                    }
                });
                let _ = self.events_tx.send(SessionMessage::ServerRequest {
                    id,
                    method,
                    params: params.unwrap_or_default(),
                    reply_tx,
                });
            }
            None => {
                tracing::debug!(target: "zcode::driver", "unclassifiable message");
            }
        }
    }

    fn handle_event(&mut self, ev: protocol::EventEnvelope) {
        match ev.type_.as_str() {
            // Legacy/alternate path: some surfaces emit `part.delta`.
            "part.delta" => {
                if let Some(p) = &ev.payload
                    && let Ok(d) = serde_json::from_value::<protocol::PartDeltaPayload>(p.clone())
                    && d.field.as_deref().unwrap_or("text") == "text"
                    && let Some(t) = self.active_turn.as_mut()
                {
                    t.accumulated_text.push_str(&d.delta);
                }
            }
            // Primary streaming path of the real `app-server`: token deltas
            // arrive as `model.streaming` events with `kind = text_delta` or
            // `reasoning_delta`. We accumulate the text deltas so they are
            // available as `final_text` on turn completion.
            "model.streaming" => {
                if let Some(p) = &ev.payload
                    && let Ok(d) =
                        serde_json::from_value::<protocol::ModelStreamingPayload>(p.clone())
                    && d.is_text()
                    && let Some(text) = d.delta.as_deref()
                    && let Some(t) = self.active_turn.as_mut()
                {
                    t.accumulated_text.push_str(text);
                }
            }
            "turn.completed" => {
                let text = ev.payload.as_ref().and_then(|p| {
                    serde_json::from_value::<protocol::TurnCompletedPayload>(p.clone())
                        .ok()
                        .and_then(|c| c.response)
                });
                self.end_turn(Ok(TurnOutcome {
                    stop_reason: protocol::StopReason::EndTurn,
                    final_text: text,
                }));
            }
            "turn.failed" => {
                let msg = ev
                    .payload
                    .as_ref()
                    .and_then(|p| {
                        serde_json::from_value::<protocol::TurnFailedPayload>(p.clone()).ok()
                    })
                    .and_then(|f| {
                        f.error.as_ref().and_then(|e| {
                            e.get("message")
                                .and_then(|m| m.as_str())
                                .map(str::to_string)
                        })
                    })
                    .unwrap_or_else(|| "turn failed".into());
                self.end_turn(Err(TurnError::Failed(msg)));
            }
            _ => {}
        }
    }

    /// Hand the turn outcome to the pump via the event channel, so the
    /// `completion` is resolved only after every preceding event has been
    /// forwarded to the ACP host. This preserves ordering for callers waiting
    /// on `session/prompt`'s reply.
    fn end_turn(&mut self, outcome: std::result::Result<TurnOutcome, TurnError>) {
        if let Some(mut t) = self.active_turn.take() {
            let completion = t.completion.take();
            let _ = self.events_tx.send(SessionMessage::TurnEnded {
                outcome,
                completion,
            });
        }
    }

    /// Used by the EOF path (no chance to order against in-flight events).
    fn fail_active_turn(&mut self, err: TurnError) {
        if let Some(mut t) = self.active_turn.take()
            && let Some(tx) = t.completion.take()
        {
            let _ = tx.send(Err(err));
        }
    }
}

/// Run the dispatcher: read framed messages, dispatch each into shared state.
async fn run_dispatcher(
    state: Arc<Mutex<DriverState>>,
    mut incoming_rx: mpsc::UnboundedReceiver<IncomingMessage>,
) {
    while let Some(raw) = incoming_rx.recv().await {
        let mut s = state.lock().await;
        s.dispatch(raw);
    }
    // EOF: notify and fail any active turn.
    let mut s = state.lock().await;
    let _ = s
        .events_tx
        .send(SessionMessage::Closed("app-server EOF".into()));
    s.fail_active_turn(TurnError::Transport("app-server EOF".into()));
}

/// Run the command loop: handle Send/Stop/Answer/Close from the ACP layer.
async fn run_command_loop(
    state: Arc<Mutex<DriverState>>,
    mut cmd_rx: mpsc::UnboundedReceiver<SessionCommand>,
) {
    while let Some(cmd) = cmd_rx.recv().await {
        match cmd {
            SessionCommand::Send { text, reply } => {
                handle_send(state.clone(), text, reply).await;
            }
            SessionCommand::Stop => {
                handle_request_void(state.clone(), methods::SESSION_STOP).await;
            }
            SessionCommand::AnswerPermission { id, response } => {
                let val = json!({
                    "id": id,
                    "result": serde_json::to_value(&response)
                        .unwrap_or_else(|_| json!({"decision":"deny"}))
                });
                let s = state.lock().await;
                let _ = s.write_tx.send(val);
            }
            SessionCommand::Close => {
                handle_request_void(state.clone(), methods::SESSION_CLOSE).await;
                let s = state.lock().await;
                let _ = s.write_tx.send(Value::Null); // shutdown sentinel
                break;
            }
        }
    }
}

async fn handle_send(
    state: Arc<Mutex<DriverState>>,
    text: String,
    reply: oneshot::Sender<Result<TurnOutcome, TurnError>>,
) {
    // Subscribe once, then send. We release the lock between sending the
    // request and awaiting its response so the dispatcher can run.
    let sub_rx = {
        let mut s = state.lock().await;
        if !s.subscribed {
            let sub_params = serde_json::to_value(&SubscribeParams {
                session_id: s.zcode_session_id.clone(),
                delivery_kind: "desktop-continuous",
                after_seq: None,
                include_snapshot: false,
            })
            .unwrap();
            s.subscribed = true;
            Some(s.request(methods::SESSION_SUBSCRIBE, sub_params))
        } else {
            None
        }
    };
    if let Some(Err(e)) = sub_rx {
        let _ = reply.send(Err(TurnError::Transport(e)));
        return;
    }
    if let Some(Ok(rx)) = sub_rx {
        let _ = rx.await; // ignore subscribe result
    }

    let send_outcome = {
        let mut s = state.lock().await;
        let send_params = serde_json::to_value(&SendParams {
            session_id: s.zcode_session_id.clone(),
            content: text,
            input_id: None,
            query_id: None,
        })
        .unwrap();
        s.request(methods::SESSION_SEND, send_params)
    };
    match send_outcome {
        Ok(rx) => match rx.await {
            Ok(Ok(_)) => {
                let mut s = state.lock().await;
                s.active_turn = Some(ActiveTurn {
                    completion: Some(reply),
                    accumulated_text: String::new(),
                    announced_tools: HashMap::new(),
                });
            }
            Ok(Err(rpc)) => {
                let _ = reply.send(Err(TurnError::Rpc {
                    code: rpc.code,
                    message: rpc.message,
                }));
            }
            Err(_) => {
                let _ = reply.send(Err(TurnError::Transport(
                    "session/send channel closed".into(),
                )));
            }
        },
        Err(e) => {
            let _ = reply.send(Err(TurnError::Transport(e)));
        }
    }
}

/// Fire a one-shot request whose response we discard (stop/close).
async fn handle_request_void(state: Arc<Mutex<DriverState>>, method: &'static str) {
    let rx = {
        let mut s = state.lock().await;
        let params = json!({ "sessionId": s.zcode_session_id });
        s.request(method, params)
    };
    if let Ok(rx) = rx {
        let _ = rx.await;
    }
}

/// Spawn the reader + writer tasks for a transport and return their channel
/// endpoints. The reader pushes [`IncomingMessage`]s; the writer consumes JSON
/// values (one per NDJSON line, with `Value::Null` as the shutdown sentinel).
fn spawn_io(
    transport: AppServerTransport,
) -> (
    mpsc::UnboundedReceiver<IncomingMessage>,
    mpsc::UnboundedSender<Value>,
) {
    // Split the transport so each task owns its half without sharing.
    let AppServerTransport {
        child,
        stdin,
        stdout,
    } = transport;

    let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();
    let (write_tx, mut write_rx) = mpsc::unbounded_channel::<Value>();

    // Reader task: read framed messages from stdout until EOF.
    tokio::spawn(async move {
        let mut reader = stdout;
        loop {
            match read_line_incoming(&mut reader).await {
                Ok(Some(raw)) => {
                    if incoming_tx.send(raw).is_err() {
                        break; // driver gone
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    tracing::warn!(target: "zcode::reader", "recv error: {e}");
                }
            }
        }
    });

    // Writer task: drain `write_tx` and write each value as a line. A
    // `Value::Null` flushes and shuts the subprocess down.
    tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;
        let mut writer = stdin;
        let mut child = child;
        while let Some(v) = write_rx.recv().await {
            if v.is_null() {
                let _ = writer.shutdown().await;
                let _ = child.start_kill();
                let _ = tokio::time::timeout(std::time::Duration::from_secs(2), child.wait()).await;
                break;
            }
            let line = match serde_json::to_string(&v) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(target: "zcode::writer", "encode error: {e}");
                    continue;
                }
            };
            if let Err(e) = write_line(&mut writer, &line).await {
                tracing::warn!(target: "zcode::writer", "send error: {e}");
                break;
            }
        }
    });

    (incoming_rx, write_tx)
}

/// Read one framed message from a buffered stdout. Re-implements the
/// `AppServerTransport::recv` framing without owning the transport.
async fn read_line_incoming(
    reader: &mut tokio::io::BufReader<transport::ChildStdout>,
) -> std::io::Result<Option<IncomingMessage>> {
    use tokio::io::AsyncBufReadExt;
    loop {
        let mut line = String::new();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            return Ok(None);
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<IncomingMessage>(trimmed) {
            Ok(m) => return Ok(Some(m)),
            Err(e) => {
                tracing::warn!(target: "zcode::reader", line = %trimmed, "non-JSON line: {e}");
                continue;
            }
        }
    }
}

async fn write_line(writer: &mut transport::ChildStdin, line: &str) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    writer.write_all(line.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    Ok(())
}

// Silence the unused-import warning for `AsyncWriteExt` when only `shutdown`
// (in spawn_io's writer task) consumes it. The `use` inside `write_line` keeps
// the trait in scope for the whole module via name resolution at call sites.

// (The legacy `wait_response` helper has been folded into `build_session`,
// which now drives the initial request through the same IO channels the
// driver uses, avoiding a separate pre-driver read loop.)

/// Spawn the app-server, run `method` (create/resume), then start the driver
/// with a paired command channel. The single entry point used by
/// [`ZcodeSession::start`] and [`ZcodeSession::resume`].
pub async fn build_session(
    cfg: SessionConfig,
    cwd: &Path,
    initial_method: &str,
    initial_params: Value,
) -> Result<ZcodeSession, SessionError> {
    let transport = transport::spawn(&cfg.node, &cfg.zcode_path, cwd, &cfg.extra_args)
        .map_err(SessionError::Spawn)?;
    let id_src = Arc::new(IdSource::default());
    let (mut incoming_rx, write_tx) = spawn_io(transport);

    // Send the initial request and await its response through the same
    // channels the driver will later use.
    let id = id_src.next();
    let msg = serde_json::to_value(&protocol::OutgoingMessage {
        id: Some(id.clone()),
        method: initial_method,
        params: Some(initial_params),
    })
    .map_err(|e| SessionError::Encode(e.to_string()))?;
    write_tx
        .send(msg)
        .map_err(|e| SessionError::Send(e.to_string()))?;

    let result_value = loop {
        match incoming_rx.recv().await {
            Some(raw) => match raw.classify() {
                Some(Incoming::Response { id: rid, result }) if rid == id => match result {
                    protocol::ResponseResult::Ok(v) => break v,
                    protocol::ResponseResult::Err(e) => return Err(SessionError::CreateFailed(e)),
                },
                _ => continue,
            },
            None => return Err(SessionError::TransportClosed),
        }
    };
    // ZCode's `session/create` response nests the id at `result.session.sessionId`
    // (3.5.x); older surfaces put it at `result.sessionId`. Accept both.
    let zcode_session_id = result_value
        .get("sessionId")
        .or_else(|| result_value.get("session").and_then(|s| s.get("sessionId")))
        .and_then(|x| x.as_str())
        .map(str::to_string)
        .ok_or_else(|| SessionError::Protocol("no sessionId in response".into()))?;

    let (events_tx, events_rx) = mpsc::unbounded_channel();
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

    let state = Arc::new(Mutex::new(DriverState {
        write_tx: write_tx.clone(),
        id_src,
        zcode_session_id: zcode_session_id.clone(),
        pending: HashMap::new(),
        active_turn: None,
        events_tx,
        subscribed: false,
    }));

    // Two cooperating tasks share `state`.
    let disp_state = state.clone();
    let cmd_state = state.clone();
    let driver_handle = tokio::spawn(async move {
        let (disp, cmd) = tokio::join!(
            run_dispatcher(disp_state, incoming_rx),
            run_command_loop(cmd_state, cmd_rx),
        );
        let _ = (disp, cmd);
    });

    Ok(ZcodeSession {
        zcode_session_id,
        events: events_rx,
        cmd_tx,
        driver: driver_handle,
    })
}

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("failed to spawn app-server: {0}")]
    Spawn(std::io::Error),
    #[error("failed to encode params: {0}")]
    Encode(String),
    #[error("failed to send to app-server: {0}")]
    Send(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("initial session request failed: {0}")]
    CreateFailed(RpcError),
    #[error("transport closed before response")]
    TransportClosed,
}
