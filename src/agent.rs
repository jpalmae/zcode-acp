//! The ACP Agent implementation.
//!
//! [`ZcodeAgent`] owns the connection to the ACP host and a table of active
//! sessions. Each ACP session is backed by a dedicated
//! [`crate::zcode::ZcodeSession`] (one `app-server` subprocess per session).
//! Events from ZCode are translated to ACP `SessionUpdate`s by
//! [`crate::acp_translator`]; permissions are bridged bidirectionally.

use std::collections::HashMap;
use std::sync::Arc;

use agent_client_protocol as acp;
use acp::schema::{
    AgentAuthCapabilities, AgentCapabilities, AuthMethod, AuthMethodAgent, CancelNotification,
    CloseSessionRequest, CloseSessionResponse, ContentBlock, ContentChunk, InitializeRequest,
    InitializeResponse, Implementation, ListSessionsResponse, LoadSessionRequest,
    LoadSessionResponse, LogoutCapabilities, LogoutRequest, LogoutResponse, NewSessionRequest,
    NewSessionResponse, PermissionOption, PermissionOptionId, PermissionOptionKind, PromptCapabilities,
    PromptRequest, PromptResponse, ProtocolVersion, RequestPermissionRequest,
    RequestPermissionResponse, SessionCloseCapabilities, SessionId, SessionListCapabilities,
    SessionMode, SessionModeId, SessionModeState, SessionResumeCapabilities,
    SessionUpdate, SetSessionModeRequest, SetSessionModeResponse, StopReason,
    ToolCallId, ToolCallStatus, ToolCallUpdate, ToolCallUpdateFields, UsageUpdate,
};
use acp::{Client, ConnectionTo, Error, Result};
use tokio::sync::Mutex;

use crate::acp_translator;
use crate::zcode::{
    auth, protocol as zp, SessionCommand, SessionConfig, SessionMessage, TurnError, ZcodeSession,
};
use crate::RunConfig;

/// Auth-method identifiers we advertise (mirrors codex-acp's multi-method set,
/// adapted to ZCode's Z.AI OAuth + API-key options).
mod auth_method {
    pub const ZAI_OAUTH: &str = "zai-oauth";
    pub const ZAI_API_KEY: &str = "zai-api-key";
}

/// Agent state shared across all sessions.
pub struct ZcodeAgent {
    cfg: RunConfig,
    sessions: Mutex<HashMap<SessionId, Arc<SessionEntry>>>,
}

/// One ACP session = one ZCode `app-server` subprocess.
struct SessionEntry {
    /// Sender into the driver; used for stop/close/send commands. The driver
    /// itself (and the transport) are owned by the pump task below.
    cmd: tokio::sync::mpsc::UnboundedSender<SessionCommand>,
    /// Pump task forwarding ZCode events → ACP `session/update`.
    pump: tokio::task::JoinHandle<()>,
}

impl ZcodeAgent {
    pub fn new(cfg: RunConfig) -> Self {
        Self {
            cfg,
            sessions: Mutex::new(HashMap::new()),
        }
    }

    fn session_cfg(&self) -> SessionConfig {
        SessionConfig {
            zcode_path: self.cfg.zcode_path.clone(),
            node: self.cfg.node.clone(),
            extra_args: self.cfg.extra_args.clone(),
            fallback_cwd: self.cfg.fallback_cwd.clone(),
        }
    }

    /// Drive the ACP connection. The terminal call is
    /// `Builder::connect_to(transport).await`.
    pub async fn serve(
        self: Arc<Self>,
        transport: impl acp::ConnectTo<acp::Agent> + 'static,
    ) -> Result<()> {
        let agent = self;

        acp::Agent
            .builder()
            .name("zcode-acp")
            // initialize
            .on_receive_request(
                {
                    let agent = agent.clone();
                    async move |request: InitializeRequest, responder, _cx| {
                        responder.respond_with_result(agent.initialize(request).await)
                    }
                },
                acp::on_receive_request!(),
            )
            // session/new
            .on_receive_request(
                {
                    let agent = agent.clone();
                    async move |request: NewSessionRequest, responder, cx: ConnectionTo<Client>| {
                        let agent = agent.clone();
                        let inner_cx = cx.clone();
                        cx.spawn(async move {
                            responder
                                .respond_with_result(agent.new_session(request, inner_cx).await)
                        })?;
                        Ok(())
                    }
                },
                acp::on_receive_request!(),
            )
            // session/load
            .on_receive_request(
                {
                    let agent = agent.clone();
                    async move |request: LoadSessionRequest, responder, cx: ConnectionTo<Client>| {
                        let agent = agent.clone();
                        let inner_cx = cx.clone();
                        cx.spawn(async move {
                            responder
                                .respond_with_result(agent.load_session(request, inner_cx).await)
                        })?;
                        Ok(())
                    }
                },
                acp::on_receive_request!(),
            )
            // session/list
            .on_receive_request(
                {
                    let agent = agent.clone();
                    async move |_request: acp::schema::ListSessionsRequest, responder, _cx| {
                        let agent = agent.clone();
                        responder.respond_with_result(agent.list_sessions().await)
                    }
                },
                acp::on_receive_request!(),
            )
            // session/prompt
            .on_receive_request(
                {
                    let agent = agent.clone();
                    async move |request: PromptRequest, responder, cx: ConnectionTo<Client>| {
                        let agent = agent.clone();
                        let inner_cx = cx.clone();
                        cx.spawn(async move {
                            responder.respond_with_result(agent.prompt(request, inner_cx).await)
                        })?;
                        Ok(())
                    }
                },
                acp::on_receive_request!(),
            )
            // session/setMode
            .on_receive_request(
                {
                    let agent = agent.clone();
                    async move |request: SetSessionModeRequest, responder, cx: ConnectionTo<Client>| {
                        let agent = agent.clone();
                        let inner_cx = cx.clone();
                        cx.spawn(async move {
                            responder
                                .respond_with_result(agent.set_mode(request, inner_cx).await)
                        })?;
                        Ok(())
                    }
                },
                acp::on_receive_request!(),
            )
            // session/close
            .on_receive_request(
                {
                    let agent = agent.clone();
                    async move |request: CloseSessionRequest, responder, cx: ConnectionTo<Client>| {
                        let agent = agent.clone();
                        cx.spawn(async move {
                            let _ = agent.close_session(request).await;
                            responder.respond_with_result(Ok(CloseSessionResponse::default()))
                        })?;
                        Ok(())
                    }
                },
                acp::on_receive_request!(),
            )
            // authenticate
            .on_receive_request(
                {
                    let agent = agent.clone();
                    async move |request: acp::schema::AuthenticateRequest, responder, _cx| {
                        let agent = agent.clone();
                        responder.respond_with_result(agent.authenticate(request).await)
                    }
                },
                acp::on_receive_request!(),
            )
            // logout
            .on_receive_request(
                {
                    let agent = agent.clone();
                    async move |request: LogoutRequest, responder, _cx| {
                        let agent = agent.clone();
                        responder.respond_with_result(agent.logout(request).await)
                    }
                },
                acp::on_receive_request!(),
            )
            // session/cancel
            .on_receive_notification(
                {
                    let agent = agent.clone();
                    async move |notification: CancelNotification, cx: ConnectionTo<Client>| {
                        let agent = agent.clone();
                        cx.spawn(async move {
                            if let Err(e) = agent.cancel(notification).await {
                                tracing::warn!("session/cancel handler error: {e:?}");
                            }
                            Ok(())
                        })?;
                        Ok(())
                    }
                },
                acp::on_receive_notification!(),
            )
            .connect_to(transport)
            .await
    }

    // -----------------------------------------------------------------------
    // Handlers
    // -----------------------------------------------------------------------

    async fn initialize(&self, _req: InitializeRequest) -> Result<InitializeResponse> {
        let caps = AgentCapabilities::new()
            .prompt_capabilities(PromptCapabilities::new().embedded_context(true).image(true))
            .load_session(true)
            .auth(AgentAuthCapabilities::new().logout(LogoutCapabilities::new()))
            .session_capabilities(
                acp::schema::SessionCapabilities::new()
                    .list(SessionListCapabilities::new())
                    .resume(SessionResumeCapabilities::new())
                    .close(SessionCloseCapabilities::new()),
            );

        let auth_methods = vec![
            AuthMethod::Agent(
                AuthMethodAgent::new(auth_method::ZAI_OAUTH, "Z.AI (browser flow)")
                    .description("Sign in with Z.AI via the OAuth init/poll flow."),
            ),
            AuthMethod::Agent(
                AuthMethodAgent::new(auth_method::ZAI_API_KEY, "API key")
                    .description("Use a provider configured in ~/.zcode/cli/config.json."),
            ),
        ];

        Ok(InitializeResponse::new(ProtocolVersion::V1)
            .agent_capabilities(caps)
            .agent_info(
                Implementation::new("zcode-acp", env!("CARGO_PKG_VERSION")).title("ZCode"),
            )
            .auth_methods(auth_methods))
    }

    async fn new_session(
        self: Arc<Self>,
        req: NewSessionRequest,
        cx: ConnectionTo<Client>,
    ) -> Result<NewSessionResponse> {
        let cwd = if req.cwd.as_os_str().is_empty() {
            self.cfg.fallback_cwd.clone()
        } else {
            req.cwd.clone()
        };

        let mcp_servers: Vec<serde_json::Value> = req
            .mcp_servers
            .iter()
            .map(|s| serde_json::to_value(s).unwrap_or(serde_json::Value::Null))
            .collect();

        tracing::info!(?cwd, mcp_count = mcp_servers.len(), "session/new");

        let zcode = ZcodeSession::start(self.session_cfg(), &cwd, mcp_servers, None, None)
            .await
            .map_err(map_session_error)?;

        let session_id = SessionId::new(uuid::Uuid::new_v4().to_string());
        let entry = self.spawn_pump(session_id.clone(), zcode, cx);
        self.sessions.lock().await.insert(session_id.clone(), entry);

        Ok(NewSessionResponse::new(session_id).modes(self.session_modes()))
    }

    async fn load_session(
        self: Arc<Self>,
        req: LoadSessionRequest,
        cx: ConnectionTo<Client>,
    ) -> Result<LoadSessionResponse> {
        // The ACP `session_id` is opaque to the host; we treat it as a ZCode
        // session id (`sess_...`) to resume.
        let zcode_sid = req.session_id.to_string();
        let cwd = if req.cwd.as_os_str().is_empty() {
            self.cfg.fallback_cwd.clone()
        } else {
            req.cwd.clone()
        };
        tracing::info!(%zcode_sid, "session/load");

        let zcode = ZcodeSession::resume(self.session_cfg(), &cwd, zcode_sid)
            .await
            .map_err(map_session_error)?;

        let session_id = SessionId::new(uuid::Uuid::new_v4().to_string());
        let entry = self.spawn_pump(session_id.clone(), zcode, cx);
        self.sessions.lock().await.insert(session_id, entry);

        Ok(LoadSessionResponse::new().modes(self.session_modes()))
    }

    async fn list_sessions(self: Arc<Self>) -> Result<ListSessionsResponse> {
        // We do not currently enumerate ZCode's persisted sessions; advertise
        // an empty list. (Could be wired to session/list on each workspace.)
        Ok(ListSessionsResponse::new(vec![]))
    }

    async fn set_mode(
        self: Arc<Self>,
        req: SetSessionModeRequest,
        _cx: ConnectionTo<Client>,
    ) -> Result<SetSessionModeResponse> {
        let mode_id = req.mode_id.to_string();
        tracing::info!(%mode_id, "session/setMode (deferred to next prompt)");
        // ZCode mode is forwarded on the next session/send via the driver; we
        // do not currently mutate the live session's mode mid-flight.
        Ok(SetSessionModeResponse::new())
    }

    async fn close_session(self: Arc<Self>, req: CloseSessionRequest) -> Result<()> {
        let entry = self.sessions.lock().await.remove(&req.session_id);
        if let Some(entry) = entry {
            let _ = entry.cmd.send(SessionCommand::Close);
            entry.pump.abort();
        }
        Ok(())
    }

    async fn cancel(self: Arc<Self>, notif: CancelNotification) -> Result<()> {
        let entry = self.sessions.lock().await.get(&notif.session_id).cloned();
        if let Some(entry) = entry {
            let _ = entry.cmd.send(SessionCommand::Stop);
        }
        Ok(())
    }

    async fn authenticate(
        self: Arc<Self>,
        req: acp::schema::AuthenticateRequest,
    ) -> Result<acp::schema::AuthenticateResponse> {
        let method = req.method_id.to_string();
        match method.as_str() {
            auth_method::ZAI_OAUTH => self.authenticate_oauth().await,
            auth_method::ZAI_API_KEY => self.authenticate_api_key().await,
            other => Err(Error::invalid_params()
                .data(format!("unknown auth method `{other}`"))),
        }
    }

    async fn authenticate_oauth(self: Arc<Self>) -> Result<acp::schema::AuthenticateResponse> {
        if self.cfg.options.no_browser {
            return Err(Error::invalid_params()
                .data("OAuth is disabled (--no-browser); use the api-key method"));
        }
        let origin = auth::resolve_origin(&env_map());
        let http = reqwest::Client::builder()
            .build()
            .map_err(|e| Error::internal_error().data(format!("http client: {e}")))?;

        let store = auth::CredentialStore::open(self.cfg.options.data_base_dir.as_deref());
        let outcome = auth::login(&http, &origin, |url| {
            tracing::info!(%url, "OAuth: open this URL to authorize");
        })
        .await
        .map_err(map_auth_error)?;

        store
            .save_login(&outcome)
            .map_err(|e| Error::internal_error().data(format!("save credentials: {e}")))?;

        Ok(acp::schema::AuthenticateResponse::new())
    }

    async fn authenticate_api_key(self: Arc<Self>) -> Result<acp::schema::AuthenticateResponse> {
        // We do not accept secrets inline; the user must have pre-populated
        // ~/.zcode/cli/config.json. We only verify a provider file is present.
        let cfg_path = self
            .cfg
            .options
            .data_base_dir
            .clone()
            .unwrap_or_else(|| {
                std::env::var_os("HOME")
                    .map(std::path::PathBuf::from)
                    .unwrap_or_else(|| std::path::PathBuf::from("."))
            })
            .join(".zcode/cli/config.json");
        if !cfg_path.exists() {
            return Err(Error::invalid_params().data(format!(
                "no provider configured at {cfg_path:?}; run `zcode login` or create it"
            )));
        }
        Ok(acp::schema::AuthenticateResponse::new())
    }

    async fn logout(self: Arc<Self>, _req: LogoutRequest) -> Result<LogoutResponse> {
        let store = auth::CredentialStore::open(self.cfg.options.data_base_dir.as_deref());
        store
            .clear()
            .map_err(|e| Error::internal_error().data(format!("clear credentials: {e}")))?;
        Ok(LogoutResponse::new())
    }

    async fn prompt(
        self: Arc<Self>,
        req: PromptRequest,
        cx: ConnectionTo<Client>,
    ) -> Result<PromptResponse> {
        let session_id = req.session_id.clone();

        let entry = {
            let sessions = self.sessions.lock().await;
            sessions
                .get(&session_id)
                .cloned()
                .ok_or_else(|| Error::resource_not_found(None))?
        };

        // Flatten the prompt's content blocks into a single string. Non-text
        // blocks are dropped (image support would require ZCode attachments).
        let prompt_text = collect_prompt_text(&req.prompt);
        if prompt_text.trim().is_empty() {
            return Ok(PromptResponse::new(StopReason::EndTurn));
        }

        tracing::info!(?session_id, prompt_len = prompt_text.len(), "session/prompt");

        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = entry
            .cmd
            .send(SessionCommand::Send { text: prompt_text, reply: tx });

        let timeout = std::time::Duration::from_secs(self.cfg.options.prompt_timeout_secs);
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(Ok(outcome))) => Ok(PromptResponse::new(map_stop_reason(outcome.stop_reason))),
            Ok(Ok(Err(e))) => {
                tracing::warn!(?session_id, "turn failed: {e}");
                let stop = match e {
                    TurnError::Transport(_) => StopReason::Cancelled,
                    _ => StopReason::EndTurn,
                };
                let _ = cx.send_notification(acp::schema::SessionNotification::new(
                    session_id.clone(),
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(
                        format!("⚠️ {e}\n").into(),
                    )),
                ));
                Ok(PromptResponse::new(stop))
            }
            Ok(Err(_)) => Err(Error::internal_error().data("prompt channel closed")),
            Err(_) => {
                let _ = entry.cmd.send(SessionCommand::Stop);
                Ok(PromptResponse::new(StopReason::Cancelled))
            }
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /// Spawn the pump task that consumes [`SessionMessage`]s from a ZCode
    /// session and forwards ACP `session/update` notifications to the host.
    fn spawn_pump(
        self: &Arc<Self>,
        session_id: SessionId,
        mut zcode: ZcodeSession,
        cx: ConnectionTo<Client>,
    ) -> Arc<SessionEntry> {
        // Clone the command sender before moving the session into the pump.
        let cmd = zcode.command_sender();
        let sid = session_id.clone();
        let pump = tokio::spawn(async move {
            let mut announced_tools: HashMap<String, ()> = HashMap::new();
            while let Some(msg) = zcode.events.recv().await {
                match msg {
                    SessionMessage::Event(ev) => {
                        forward_event(&sid, &cx, &ev, &mut announced_tools);
                    }
                    SessionMessage::TurnEnded { outcome, completion } => {
                        // All preceding events have already been forwarded
                        // (the channel is FIFO), so it is now safe to resolve
                        // the prompt's completion.
                        if let Some(tx) = completion {
                            let _ = tx.send(outcome);
                        }
                    }
                    SessionMessage::ServerRequest {
                        id,
                        method,
                        params,
                        reply_tx,
                    } => {
                        handle_server_request(&sid, &cx, id, &method, &params, reply_tx);
                    }
                    SessionMessage::Closed(reason) => {
                        tracing::warn!(?sid, "zcode session closed: {reason}");
                        break;
                    }
                }
            }
        });
        Arc::new(SessionEntry { cmd, pump })
    }

    /// Build the `modes` payload advertising ZCode's plan/build/edit/yolo modes.
    fn session_modes(&self) -> SessionModeState {
        let modes: Vec<SessionMode> = [
            ("plan", "Plan"),
            ("build", "Build"),
            ("edit", "Edit"),
            ("yolo", "Yolo"),
        ]
        .into_iter()
        .map(|(id, name)| SessionMode::new(id, name))
        .collect();
        SessionModeState::new(SessionModeId::new("yolo"), modes)
    }
}

/// Translate a ZCode event into one or more ACP `session/update` notifications.
fn forward_event(
    session_id: &SessionId,
    cx: &ConnectionTo<Client>,
    ev: &zp::EventEnvelope,
    announced_tools: &mut HashMap<String, ()>,
) {
    match ev.type_.as_str() {
        "part.delta" => {
            if let Some(p) = &ev.payload
                && let Ok(d) = serde_json::from_value::<zp::PartDeltaPayload>(p.clone())
                    && let Some(update) = acp_translator::part_delta_to_update(&d) {
                        let _ = cx.send_notification(acp::schema::SessionNotification::new(
                            session_id.clone(),
                            update,
                        ));
                    }
        }
        "tool.updated" => {
            if let Some(p) = &ev.payload
                && let Ok(t) = serde_json::from_value::<zp::ToolUpdatedPayload>(p.clone()) {
                    match acp_translator::tool_updated_to_calls(&t, announced_tools) {
                        acp_translator::ToolEventTranslation::New(call) => {
                            let _ = cx.send_notification(acp::schema::SessionNotification::new(
                                session_id.clone(),
                                SessionUpdate::ToolCall(call),
                            ));
                        }
                        acp_translator::ToolEventTranslation::Update(upd) => {
                            let _ = cx.send_notification(acp::schema::SessionNotification::new(
                                session_id.clone(),
                                SessionUpdate::ToolCallUpdate(upd),
                            ));
                        }
                        acp_translator::ToolEventTranslation::None => {}
                    }
                }
        }
        "turn.completed" => {
            if let Some(p) = &ev.payload
                && let Ok(c) = serde_json::from_value::<zp::TurnCompletedPayload>(p.clone())
                    && let Some(u) = c.usage.as_ref()
                        && let (Some(used), Some(window)) = (
                            u.get("total").and_then(|v| v.as_u64()),
                            u.get("context_window").and_then(|v| v.as_u64()),
                        ) {
                            let _ = cx.send_notification(acp::schema::SessionNotification::new(
                                session_id.clone(),
                                SessionUpdate::UsageUpdate(UsageUpdate::new(used, window)),
                            ));
                        }
        }
        _ => {}
    }
}

/// Handle a server→client request (e.g. `interaction/requestPermission`) by
/// re-asking the ACP host via `session/request_permission` and forwarding the
/// decision back through `reply_tx`.
///
/// This is synchronous because the actual ACP request is dispatched via
/// `SentRequest::on_receiving_result` (a callback registration), not awaited
/// inline — awaiting inside the pump would deadlock the event loop.
fn handle_server_request(
    session_id: &SessionId,
    cx: &ConnectionTo<Client>,
    id: zp::RequestId,
    method: &str,
    params: &serde_json::Value,
    reply_tx: tokio::sync::oneshot::Sender<serde_json::Value>,
) {
    match method {
        "interaction/requestPermission" => {
            let req = match serde_json::from_value::<zp::PermissionPayload>(params.clone()) {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!("bad permission params: {e}");
                    let _ = reply_tx.send(serde_json::json!({"decision":"deny","reason":"bad params"}));
                    return;
                }
            };
            let acp_req = acp_translator::permission_to_acp(&req);

            let tool_call_id: ToolCallId = acp_req.tool_call_id.clone().into();
            let tool_call_update = ToolCallUpdate::new(
                tool_call_id,
                ToolCallUpdateFields::new()
                    .title(acp_req.title.clone())
                    .kind(acp_req.kind)
                    .status(ToolCallStatus::Pending),
            );

            // Build ACP permission options from ZCode's. ZCode pre-builds a
            // `qg` response per option; we map each into an ACP option whose
            // id matches, so the host's selection can be looked up later.
            let options: Vec<PermissionOption> = acp_req
                .options
                .iter()
                .map(|o| {
                    let kind = match o.id.as_str() {
                        s if s.contains("always") => PermissionOptionKind::AllowAlways,
                        s if s.starts_with("allow") => PermissionOptionKind::AllowOnce,
                        s if s.starts_with("deny") => PermissionOptionKind::RejectOnce,
                        _ => PermissionOptionKind::AllowOnce,
                    };
                    PermissionOption::new(
                        PermissionOptionId::new(o.id.clone()),
                        o.label.clone(),
                        kind,
                    )
                })
                .collect();

            let request =
                RequestPermissionRequest::new(session_id.clone(), tool_call_update, options);

            // Capture the original ZCode options so the response handler can
            // look up the pre-built `qg` for the chosen option id, then move
            // `reply_tx` into the callback so it owns the answer delivery.
            let zcode_options = req.options.clone();
            let result = cx.send_request(request).on_receiving_result(
                move |resp: std::result::Result<RequestPermissionResponse, Error>| async move {
                    let qg = match resp {
                        Ok(r) => match r.outcome {
                            acp::schema::RequestPermissionOutcome::Selected(sel) => {
                                let opt_id = sel.option_id.to_string();
                                zcode_options
                                    .iter()
                                    .find(|o| o.option_id == opt_id)
                                    .map(|o| o.response.clone())
                                    .unwrap_or_else(|| {
                                        serde_json::json!({"decision":"deny","reason":"option not found"})
                                    })
                            }
                            acp::schema::RequestPermissionOutcome::Cancelled => {
                                serde_json::json!({"decision":"deny","reason":"cancelled"})
                            }
                            _ => serde_json::json!({"decision":"deny","reason":"unknown outcome"}),
                        },
                        Err(e) => {
                            tracing::warn!("host denied permission request: {e:?}");
                            serde_json::json!({"decision":"deny","reason":"host unavailable"})
                        }
                    };
                    let _ = reply_tx.send(qg);
                    Ok::<(), Error>(())
                },
            );

            if let Err(e) = result {
                tracing::warn!("failed to send request_permission: {e:?}");
            }
            let _ = id;
        }
        other => {
            tracing::debug!(method = other, "unsupported server→client interaction; denying");
            let _ = reply_tx.send(serde_json::json!({"decision":"deny","reason":"unsupported interaction"}));
        }
    }
}

/// Concatenate the text of every `ContentBlock::Text` in a prompt.
fn collect_prompt_text(blocks: &[ContentBlock]) -> String {
    let mut out = String::new();
    for block in blocks {
        if let ContentBlock::Text(t) = block {
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str(&t.text);
        }
    }
    out
}

fn env_map() -> std::collections::HashMap<String, String> {
    std::env::vars().collect()
}

fn map_session_error(e: crate::zcode::SessionError) -> Error {
    use crate::zcode::SessionError as E;
    match e {
        E::CreateFailed(rpc) => {
            if rpc.code == zp::codes::MODEL_UNAVAILABLE {
                Error::internal_error()
                    .data(format!("ZCode model unavailable: {}", rpc.message))
            } else {
                Error::internal_error().data(rpc.to_string())
            }
        }
        other => Error::internal_error().data(other.to_string()),
    }
}

fn map_auth_error(e: auth::AuthError) -> Error {
    Error::internal_error()
        .data(format!("{}: {e}", e.reason_code()))
}

fn map_stop_reason(r: zp::StopReason) -> StopReason {
    match r {
        zp::StopReason::EndTurn => StopReason::EndTurn,
        zp::StopReason::Cancelled => StopReason::Cancelled,
        zp::StopReason::MaxTurns => StopReason::MaxTurnRequests,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use acp::schema::TextContent;

    #[test]
    fn collect_prompt_text_concatenates_text_blocks() {
        let blocks = vec![
            ContentBlock::Text(TextContent::new("hello")),
            ContentBlock::Text(TextContent::new("world")),
        ];
        assert_eq!(collect_prompt_text(&blocks), "hello\nworld");
    }
}
