//! Translation from ZCode Protocol events into ACP `SessionUpdate` values.
//!
//! The driver ([`crate::zcode::Driver`]) consumes `session/event` notifications
//! and emits ACP updates through the agent. This module owns the per-event
//! mapping so the agent code stays readable.

use agent_client_protocol as acp;
use acp::schema::{
    Content, ContentBlock, ContentChunk, SessionUpdate, ToolCall, ToolCallContent,
    ToolCallLocation, ToolCallStatus, ToolCallUpdate, ToolCallUpdateFields, ToolKind,
};
use serde_json::Value;

use crate::zcode::protocol::{
    EventEnvelope, PartDeltaPayload, PermissionPayload, ToolKind as ZToolKind,
    ToolUpdatedPayload,
};

/// Build the [`SessionUpdate`] for a streamed text/reasoning delta.
pub fn part_delta_to_update(delta: &PartDeltaPayload) -> Option<SessionUpdate> {
    match delta.field.as_deref().unwrap_or("text") {
        "text" | "input" | "output" => Some(SessionUpdate::AgentMessageChunk(ContentChunk::new(
            delta.delta.clone().into(),
        ))),
        "reasoning" => Some(SessionUpdate::AgentThoughtChunk(ContentChunk::new(
            delta.delta.clone().into(),
        ))),
        _ => None,
    }
}

/// Map a ZCode `tool.updated` event into either a new ACP `ToolCall` (when the
/// tool is first seen, kind = `scheduled`) or a `ToolCallUpdate` (subsequent
/// kinds). Returns `None` for kinds we don't surface (e.g. `progress` deltas
/// are dropped to avoid flooding; clients can still see the final result).
pub fn tool_updated_to_calls(
    payload: &ToolUpdatedPayload,
    already_announced: &mut std::collections::HashMap<String, ()>,
) -> ToolEventTranslation {
    let id = payload.tool_call_id.clone();
    let title = payload
        .tool_name
        .clone()
        .unwrap_or_else(|| "tool".to_string());
    let kind = tool_kind_for(payload.tool_name.as_deref().unwrap_or(""));

    match payload.kind {
        ZToolKind::Scheduled => {
            already_announced.insert(id.clone(), ());
            let input = payload
                .input
                .as_ref()
                .map(|v| v.to_string())
                .unwrap_or_default();
            let locations = extract_locations(&payload.input);
            ToolEventTranslation::New(ToolCall::new(id.clone(), title.clone())
                .raw_input(input)
                .kind(kind)
                .locations(locations))
        }
        ZToolKind::Started => {
            // First-seen fallback if we missed `scheduled`.
            if already_announced.insert(id.clone(), ()).is_none() {
                let input = payload
                    .input
                    .as_ref()
                    .map(|v| v.to_string())
                    .unwrap_or_default();
                ToolEventTranslation::New(ToolCall::new(id.clone(), title)
                    .raw_input(input)
                    .kind(tool_kind_for(payload.tool_name.as_deref().unwrap_or(""))))
            } else {
                ToolEventTranslation::None
            }
        }
        ZToolKind::Result => {
            let content = tool_text_content(payload.result.as_ref().and_then(|r| r.get("output")));
            ToolEventTranslation::Update(ToolCallUpdate::new(
                id.clone(),
                ToolCallUpdateFields::new()
                    .status(ToolCallStatus::Completed)
                    .content(content),
            ))
        }
        ZToolKind::Error => {
            let content =
                tool_text_content(payload.error.as_ref().and_then(|e| e.get("message")));
            ToolEventTranslation::Update(ToolCallUpdate::new(
                id.clone(),
                ToolCallUpdateFields::new()
                    .status(ToolCallStatus::Failed)
                    .content(content),
            ))
        }
        ZToolKind::Progress | ZToolKind::Other => ToolEventTranslation::None,
    }
}

pub enum ToolEventTranslation {
    New(ToolCall),
    Update(ToolCallUpdate),
    None,
}

/// Heuristic mapping from a ZCode tool name to an ACP [`ToolKind`].
fn tool_kind_for(name: &str) -> ToolKind {
    let lower = name.to_ascii_lowercase();
    if lower.starts_with("read") || lower == "glob" || lower == "grep" {
        ToolKind::Read
    } else if lower.starts_with("edit") || lower.starts_with("write") || lower == "apply_patch" {
        ToolKind::Edit
    } else if lower.starts_with("bash") || lower == "exec" || lower == "shell" {
        ToolKind::Execute
    } else if lower.starts_with("fetch") || lower.contains("web") {
        ToolKind::Fetch
    } else {
        ToolKind::Other
    }
}

/// Pull file paths out of a tool input for ACP `ToolCallLocation`s.
fn extract_locations(input: &Option<Value>) -> Vec<ToolCallLocation> {
    let Some(v) = input else {
        return vec![];
    };
    let mut out = vec![];
    if let Some(path) = v.get("path").and_then(|p| p.as_str()) {
        out.push(ToolCallLocation::new(path.to_string()));
    }
    if let Some(file) = v.get("file").and_then(|p| p.as_str()) {
        out.push(ToolCallLocation::new(file.to_string()));
    }
    if let Some(arr) = v.get("files").and_then(|p| p.as_array()) {
        for f in arr {
            if let Some(s) = f.as_str() {
                out.push(ToolCallLocation::new(s.to_string()));
            }
        }
    }
    out
}

/// Build a single text-content item list from a JSON value (string or any
/// JSON-serialized form). Returns an empty vec for missing/empty values.
fn tool_text_content(v: Option<&Value>) -> Vec<ToolCallContent> {
    match v {
        Some(Value::String(s)) if !s.is_empty() => {
            let block: ContentBlock = s.clone().into();
            vec![ToolCallContent::Content(Content::new(block))]
        }
        Some(other) => {
            let block: ContentBlock = other.to_string().into();
            vec![ToolCallContent::Content(Content::new(block))]
        }
        None => vec![],
    }
}

/// Convert a ZCode `permission.requested` payload into the data needed to ask
/// the ACP host via `session/requestPermission`.
pub struct AcpPermissionRequest {
    pub tool_call_id: String,
    pub title: String,
    pub kind: ToolKind,
    pub raw_input: String,
    pub options: Vec<AcpPermissionOption>,
}

pub struct AcpPermissionOption {
    pub id: String,
    pub label: String,
    /// The pre-built `qg` response to send back if this option is chosen.
    pub response: Value,
}

pub fn permission_to_acp(payload: &PermissionPayload) -> AcpPermissionRequest {
    let tool_name = payload.tool_name.clone().unwrap_or_default();
    AcpPermissionRequest {
        tool_call_id: payload.tool_call_id.clone(),
        title: tool_name.clone(),
        kind: tool_kind_for(&tool_name),
        raw_input: payload
            .input
            .as_ref()
            .map(|v| v.to_string())
            .unwrap_or_default(),
        options: payload
            .options
            .iter()
            .map(|o| AcpPermissionOption {
                id: o.option_id.clone(),
                label: o.name.clone(),
                response: o.response.clone(),
            })
            .collect(),
    }
}

/// Map a ZCode plan (if/when surfaced) to an ACP `SessionUpdate::Plan`.
/// ZCode surfaces planning via tool calls rather than a dedicated plan event,
/// so this is currently a no-op kept for future use.
pub fn maybe_plan(_ev: &EventEnvelope) -> Option<acp::schema::SessionUpdate> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_delta_maps_to_agent_message_chunk() {
        let delta = PartDeltaPayload {
            message_id: "m".into(),
            part_id: "p".into(),
            field: Some("text".into()),
            delta: "hi".into(),
        };
        assert!(matches!(
            part_delta_to_update(&delta),
            Some(SessionUpdate::AgentMessageChunk(_))
        ));
    }

    #[test]
    fn reasoning_delta_maps_to_thought_chunk() {
        let delta = PartDeltaPayload {
            message_id: "m".into(),
            part_id: "p".into(),
            field: Some("reasoning".into()),
            delta: "thinking".into(),
        };
        assert!(matches!(
            part_delta_to_update(&delta),
            Some(SessionUpdate::AgentThoughtChunk(_))
        ));
    }

    #[test]
    fn scheduled_then_result_emit_call_then_update() {
        let mut seen = std::collections::HashMap::new();
        let scheduled = ToolUpdatedPayload {
            tool_call_id: "c1".into(),
            tool_name: Some("Bash".into()),
            parent_tool_call_id: None,
            description: None,
            kind: ZToolKind::Scheduled,
            input: Some(serde_json::json!({ "command": "ls" })),
            result: None,
            error: None,
            started_at: None,
            completed_at: None,
            stdout_tail: None,
            stderr_tail: None,
        };
        assert!(matches!(
            tool_updated_to_calls(&scheduled, &mut seen),
            ToolEventTranslation::New(_)
        ));
        let result = ToolUpdatedPayload {
            kind: ZToolKind::Result,
            result: Some(serde_json::json!({ "output": "file.txt" })),
            ..scheduled.clone()
        };
        match tool_updated_to_calls(&result, &mut seen) {
            ToolEventTranslation::Update(u) => {
                assert!(matches!(
                    u.fields.status,
                    Some(ToolCallStatus::Completed)
                ));
            }
            other => panic!("expected update, got {other:?}", other = match other {
                ToolEventTranslation::New(_) => "New",
                ToolEventTranslation::Update(_) => "Update",
                ToolEventTranslation::None => "None",
            }),
        }
    }
}
