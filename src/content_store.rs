//! Content-addressed request body storage.
//!
//! An inference request body is split into addressable **items** — the
//! instructions/system block, each tool definition, and each message or input
//! item — before it is persisted. Every item is canonicalized, secret-redacted,
//! and hashed; the body that remains after each item is replaced by a hash
//! reference is the **skeleton**. A skeleton plus its items reproduces the full
//! body, while items shared across requests (the system prompt, the tool list,
//! every earlier turn of a conversation) are stored exactly once.
//!
//! This module is pure: no IO, no queue, no store handle. The persistence layer
//! turns a [`SplitBody`] into blob/item rows; the history API turns rows back
//! into a body with [`assemble`].

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::fmt;

/// Client protocol labels, shared with `flow_persistence::client_protocol_for_path`.
pub const PROTOCOL_RESPONSES: &str = "responses";
pub const PROTOCOL_CHAT_COMPLETIONS: &str = "chat_completions";
pub const PROTOCOL_ANTHROPIC_MESSAGES: &str = "anthropic_messages";

/// The object key a skeleton uses to reference an item by hash. Deliberately
/// not `$ref`, which appears in JSON-Schema tool definitions; and assembly
/// only ever inspects the layout positions, never arbitrary nested objects.
pub const REF_KEY: &str = "$llmconduit_blob";

/// Media label stored with every blob. Items are always canonical JSON text.
pub const MEDIA_JSON: &str = "json";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ItemSection {
    Instructions,
    Tool,
    Message,
}

impl ItemSection {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Instructions => "instructions",
            Self::Tool => "tool",
            Self::Message => "message",
        }
    }

    pub fn parse(label: &str) -> Option<Self> {
        match label {
            "instructions" => Some(Self::Instructions),
            "tool" => Some(Self::Tool),
            "message" => Some(Self::Message),
            _ => None,
        }
    }
}

/// One addressable unit of a request body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitItem {
    /// Position within the body, across all sections, in body order.
    pub ordinal: i64,
    pub section: ItemSection,
    /// `role`/`type` for messages, the function name for tools, absent for instructions.
    pub kind: Option<String>,
    /// Lowercase hex SHA-256 of `canonical`.
    pub hash: String,
    /// Canonical JSON (sorted keys, no whitespace) of the redacted item.
    pub canonical: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitBody {
    /// The body with every item replaced by `{"$llmconduit_blob": "<hash>"}`.
    pub skeleton: String,
    pub items: Vec<SplitItem>,
}

impl SplitBody {
    /// Total canonical bytes across items (duplicates counted once per position).
    pub fn item_bytes(&self) -> usize {
        self.items.iter().map(|item| item.canonical.len()).sum()
    }

    /// Distinct item hashes, in first-occurrence order.
    pub fn unique_hashes(&self) -> Vec<&str> {
        let mut seen = std::collections::HashSet::new();
        self.items
            .iter()
            .filter(|item| seen.insert(item.hash.as_str()))
            .map(|item| item.hash.as_str())
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SplitError {
    InvalidJson,
    NotAnObject,
    UnknownProtocol(String),
}

impl fmt::Display for SplitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidJson => formatter.write_str("request body is not valid JSON"),
            Self::NotAnObject => formatter.write_str("request body is not a JSON object"),
            Self::UnknownProtocol(protocol) => {
                write!(formatter, "unknown client protocol '{protocol}'")
            }
        }
    }
}

impl std::error::Error for SplitError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssembleError {
    InvalidSkeleton,
    UnknownProtocol(String),
    MissingBlob(String),
    InvalidBlob(String),
}

impl fmt::Display for AssembleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSkeleton => formatter.write_str("stored skeleton is not a JSON object"),
            Self::UnknownProtocol(protocol) => {
                write!(formatter, "unknown client protocol '{protocol}'")
            }
            Self::MissingBlob(hash) => write!(formatter, "content blob {hash} is missing"),
            Self::InvalidBlob(hash) => write!(formatter, "content blob {hash} is not JSON"),
        }
    }
}

impl std::error::Error for AssembleError {}

/// Where a protocol keeps its items.
struct Layout {
    instructions: Option<&'static str>,
    tools: Option<&'static str>,
    messages: &'static str,
}

fn layout(protocol: &str) -> Option<Layout> {
    match protocol {
        PROTOCOL_RESPONSES => Some(Layout {
            instructions: Some("instructions"),
            tools: Some("tools"),
            messages: "input",
        }),
        PROTOCOL_CHAT_COMPLETIONS => Some(Layout {
            instructions: None,
            tools: Some("tools"),
            messages: "messages",
        }),
        PROTOCOL_ANTHROPIC_MESSAGES => Some(Layout {
            instructions: Some("system"),
            tools: Some("tools"),
            messages: "messages",
        }),
        _ => None,
    }
}

/// Parse and split a raw body. Secrets are always redacted; image/data URIs
/// are redacted only when `keep_media` is false.
pub fn split_body(protocol: &str, raw: &[u8], keep_media: bool) -> Result<SplitBody, SplitError> {
    let value = serde_json::from_slice::<Value>(raw).map_err(|_| SplitError::InvalidJson)?;
    split_value(protocol, value, keep_media)
}

/// Split an already-parsed body. See [`split_body`].
pub fn split_value(
    protocol: &str,
    mut value: Value,
    keep_media: bool,
) -> Result<SplitBody, SplitError> {
    let layout =
        layout(protocol).ok_or_else(|| SplitError::UnknownProtocol(protocol.to_string()))?;
    if !value.is_object() {
        return Err(SplitError::NotAnObject);
    }
    crate::redaction::redact_payload_secrets_in_value(&mut value);
    if !keep_media {
        crate::redaction::redact_image_uris_in_value(&mut value);
    }
    let object = value.as_object_mut().expect("checked object");
    let mut items = Vec::new();

    if let Some(key) = layout.instructions
        && let Some(slot) = object.get_mut(key)
        && !slot.is_null()
    {
        let item = std::mem::take(slot);
        *slot = push_item(&mut items, ItemSection::Instructions, None, item);
    }
    if let Some(key) = layout.tools
        && let Some(Value::Array(tools)) = object.get_mut(key)
    {
        for slot in tools.iter_mut() {
            let item = std::mem::take(slot);
            let kind = tool_kind(&item);
            *slot = push_item(&mut items, ItemSection::Tool, kind, item);
        }
    }
    match object.get_mut(layout.messages) {
        Some(Value::Array(messages)) => {
            for slot in messages.iter_mut() {
                let item = std::mem::take(slot);
                let kind = message_kind(&item);
                *slot = push_item(&mut items, ItemSection::Message, kind, item);
            }
        }
        // The Responses API accepts a bare string as `input`; store it as one item.
        Some(slot @ Value::String(_)) => {
            let item = std::mem::take(slot);
            *slot = push_item(&mut items, ItemSection::Message, None, item);
        }
        _ => {}
    }

    let skeleton = serde_json::to_string(&value).map_err(|_| SplitError::InvalidJson)?;
    Ok(SplitBody { skeleton, items })
}

fn push_item(
    items: &mut Vec<SplitItem>,
    section: ItemSection,
    kind: Option<String>,
    item: Value,
) -> Value {
    // `serde_json::Value` objects are `BTreeMap`-backed (no `preserve_order`
    // feature), so `to_string` already yields sorted keys without whitespace.
    let canonical = serde_json::to_string(&item).unwrap_or_else(|_| "null".to_string());
    let hash = hash_canonical(&canonical);
    let marker = reference(&hash);
    items.push(SplitItem {
        ordinal: i64::try_from(items.len()).unwrap_or(i64::MAX),
        section,
        kind,
        hash,
        canonical,
    });
    marker
}

/// Lowercase hex SHA-256 of canonical item bytes.
pub fn hash_canonical(canonical: &str) -> String {
    let digest = Sha256::digest(canonical.as_bytes());
    let mut hex = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

fn reference(hash: &str) -> Value {
    let mut map = Map::new();
    map.insert(REF_KEY.to_string(), Value::String(hash.to_string()));
    Value::Object(map)
}

fn reference_hash(value: &Value) -> Option<&str> {
    let object = value.as_object()?;
    if object.len() != 1 {
        return None;
    }
    object.get(REF_KEY)?.as_str()
}

fn tool_kind(tool: &Value) -> Option<String> {
    let object = tool.as_object()?;
    object
        .get("function")
        .and_then(|function| function.get("name"))
        .or_else(|| object.get("name"))
        .or_else(|| object.get("type"))
        .and_then(Value::as_str)
        .map(|kind| bounded_kind(kind).to_string())
}

fn message_kind(message: &Value) -> Option<String> {
    let object = message.as_object()?;
    object
        .get("role")
        .or_else(|| object.get("type"))
        .and_then(Value::as_str)
        .map(|kind| bounded_kind(kind).to_string())
}

/// `kind` is a label column, not content: keep it short.
fn bounded_kind(kind: &str) -> &str {
    const KIND_CAP: usize = 128;
    if kind.len() <= KIND_CAP {
        return kind;
    }
    let mut end = KIND_CAP;
    while !kind.is_char_boundary(end) {
        end -= 1;
    }
    &kind[..end]
}

/// Rebuild the full body from a skeleton and a blob resolver. Only the layout
/// positions are inspected, so a genuine item containing the reference key is
/// never mistaken for a reference.
pub fn assemble(
    protocol: &str,
    skeleton: &str,
    mut resolve: impl FnMut(&str) -> Option<String>,
) -> Result<String, AssembleError> {
    let layout =
        layout(protocol).ok_or_else(|| AssembleError::UnknownProtocol(protocol.to_string()))?;
    let mut value =
        serde_json::from_str::<Value>(skeleton).map_err(|_| AssembleError::InvalidSkeleton)?;
    let object = value
        .as_object_mut()
        .ok_or(AssembleError::InvalidSkeleton)?;

    if let Some(key) = layout.instructions
        && let Some(slot) = object.get_mut(key)
    {
        restore(slot, &mut resolve)?;
    }
    if let Some(key) = layout.tools
        && let Some(Value::Array(tools)) = object.get_mut(key)
    {
        for slot in tools.iter_mut() {
            restore(slot, &mut resolve)?;
        }
    }
    match object.get_mut(layout.messages) {
        Some(Value::Array(messages)) => {
            for slot in messages.iter_mut() {
                restore(slot, &mut resolve)?;
            }
        }
        Some(slot) => restore(slot, &mut resolve)?,
        None => {}
    }
    serde_json::to_string(&value).map_err(|_| AssembleError::InvalidSkeleton)
}

fn restore(
    slot: &mut Value,
    resolve: &mut impl FnMut(&str) -> Option<String>,
) -> Result<(), AssembleError> {
    let Some(hash) = reference_hash(slot).map(str::to_string) else {
        return Ok(());
    };
    let canonical = resolve(&hash).ok_or_else(|| AssembleError::MissingBlob(hash.clone()))?;
    *slot = serde_json::from_str(&canonical).map_err(|_| AssembleError::InvalidBlob(hash))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    fn lookup(split: &SplitBody) -> HashMap<String, String> {
        split
            .items
            .iter()
            .map(|item| (item.hash.clone(), item.canonical.clone()))
            .collect()
    }

    fn round_trip(protocol: &str, body: Value) -> SplitBody {
        let raw = serde_json::to_vec(&body).unwrap();
        let split = split_body(protocol, &raw, true).expect("split");
        let blobs = lookup(&split);
        let assembled =
            assemble(protocol, &split.skeleton, |hash| blobs.get(hash).cloned()).expect("assemble");
        let assembled: Value = serde_json::from_str(&assembled).unwrap();
        assert_eq!(assembled, body, "round trip must reproduce the body");
        split
    }

    #[test]
    fn responses_body_splits_into_instructions_tools_and_input_items() {
        let split = round_trip(
            PROTOCOL_RESPONSES,
            json!({
                "model": "m",
                "instructions": "be brief",
                "tools": [
                    {"type": "function", "name": "read_file", "parameters": {"$ref": "#/x"}},
                    {"type": "web_search"}
                ],
                "input": [
                    {"type": "message", "role": "user", "content": "hi"},
                    {"type": "function_call", "call_id": "c1", "name": "read_file", "arguments": "{}"},
                    {"type": "function_call_output", "call_id": "c1", "output": "ok"}
                ],
                "stream": true
            }),
        );
        let sections: Vec<_> = split.items.iter().map(|item| item.section).collect();
        assert_eq!(
            sections,
            [
                ItemSection::Instructions,
                ItemSection::Tool,
                ItemSection::Tool,
                ItemSection::Message,
                ItemSection::Message,
                ItemSection::Message
            ]
        );
        let kinds: Vec<_> = split
            .items
            .iter()
            .map(|item| item.kind.as_deref())
            .collect();
        assert_eq!(
            kinds,
            [
                None,
                Some("read_file"),
                Some("web_search"),
                Some("user"),
                Some("function_call"),
                Some("function_call_output")
            ]
        );
        let ordinals: Vec<_> = split.items.iter().map(|item| item.ordinal).collect();
        assert_eq!(ordinals, [0, 1, 2, 3, 4, 5]);
        // Scalars outside the layout stay in the skeleton verbatim.
        let skeleton: Value = serde_json::from_str(&split.skeleton).unwrap();
        assert_eq!(skeleton["model"], "m");
        assert_eq!(skeleton["stream"], true);
        assert!(skeleton["instructions"][REF_KEY].is_string());
    }

    #[test]
    fn chat_body_splits_tools_and_messages_with_function_names() {
        let split = round_trip(
            PROTOCOL_CHAT_COMPLETIONS,
            json!({
                "model": "m",
                "messages": [
                    {"role": "system", "content": "sys"},
                    {"role": "user", "content": [{"type": "text", "text": "hi"}]},
                    {"role": "assistant", "tool_calls": [{"id": "t", "type": "function", "function": {"name": "f", "arguments": "{}"}}]},
                    {"role": "tool", "tool_call_id": "t", "content": "done"}
                ],
                "tools": [{"type": "function", "function": {"name": "f", "parameters": {}}}]
            }),
        );
        let kinds: Vec<_> = split
            .items
            .iter()
            .map(|item| item.kind.as_deref())
            .collect();
        assert_eq!(
            kinds,
            [
                Some("f"),
                Some("system"),
                Some("user"),
                Some("assistant"),
                Some("tool")
            ]
        );
    }

    #[test]
    fn anthropic_body_splits_system_blocks_tools_and_messages() {
        let split = round_trip(
            PROTOCOL_ANTHROPIC_MESSAGES,
            json!({
                "model": "m",
                "system": [{"type": "text", "text": "sys", "cache_control": {"type": "ephemeral"}}],
                "tools": [{"name": "bash", "input_schema": {"type": "object"}}],
                "messages": [
                    {"role": "user", "content": "hi"},
                    {"role": "assistant", "content": [{"type": "text", "text": "yo"}]}
                ],
                "metadata": {"user_id": "user_x_session_y"}
            }),
        );
        assert_eq!(split.items[0].section, ItemSection::Instructions);
        assert_eq!(split.items[1].kind.as_deref(), Some("bash"));
        let skeleton: Value = serde_json::from_str(&split.skeleton).unwrap();
        assert_eq!(skeleton["metadata"]["user_id"], "user_x_session_y");
    }

    #[test]
    fn responses_string_input_is_one_message_item() {
        let split = round_trip(
            PROTOCOL_RESPONSES,
            json!({"model": "m", "input": "just text"}),
        );
        assert_eq!(split.items.len(), 1);
        assert_eq!(split.items[0].section, ItemSection::Message);
        assert_eq!(split.items[0].canonical, "\"just text\"");
    }

    #[test]
    fn identical_items_share_one_hash() {
        let split = round_trip(
            PROTOCOL_CHAT_COMPLETIONS,
            json!({
                "model": "m",
                "messages": [
                    {"role": "user", "content": "same"},
                    {"role": "assistant", "content": "other"},
                    {"content": "same", "role": "user"}
                ]
            }),
        );
        assert_eq!(split.items.len(), 3);
        assert_eq!(split.items[0].hash, split.items[2].hash);
        assert_eq!(split.unique_hashes().len(), 2);
        // Both key orders of the same object must canonicalize identically.
        assert_eq!(split.items[0].canonical, split.items[2].canonical);
    }

    #[test]
    fn secrets_are_redacted_inside_items_and_skeleton() {
        let raw = serde_json::to_vec(&json!({
            "model": "m",
            "api_key": "sk-top",
            "messages": [{"role": "user", "content": "x", "authorization": "Bearer leak"}]
        }))
        .unwrap();
        let split = split_body(PROTOCOL_CHAT_COMPLETIONS, &raw, true).unwrap();
        assert!(!split.skeleton.contains("sk-top"));
        assert!(split.skeleton.contains("[redacted]"));
        assert!(!split.items[0].canonical.contains("leak"));
        assert!(split.items[0].canonical.contains("[redacted]"));
    }

    #[test]
    fn media_is_kept_by_default_and_redacted_on_request() {
        let body = json!({
            "model": "m",
            "messages": [{"role": "user", "content": [{"type": "image_url", "image_url": {"url": "data:image/png;base64,AAAA"}}]}]
        });
        let raw = serde_json::to_vec(&body).unwrap();
        let kept = split_body(PROTOCOL_CHAT_COMPLETIONS, &raw, true).unwrap();
        assert!(
            kept.items[0]
                .canonical
                .contains("data:image/png;base64,AAAA")
        );
        let stripped = split_body(PROTOCOL_CHAT_COMPLETIONS, &raw, false).unwrap();
        assert!(!stripped.items[0].canonical.contains("base64,AAAA"));
        assert_ne!(kept.items[0].hash, stripped.items[0].hash);
    }

    #[test]
    fn hash_is_sha256_of_canonical_bytes() {
        assert_eq!(
            hash_canonical("\"abc\""),
            // sha256 of the five bytes "abc" in quotes
            {
                let digest = Sha256::digest(b"\"abc\"");
                digest
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>()
            }
        );
        assert_eq!(hash_canonical("x").len(), 64);
    }

    #[test]
    fn invalid_bodies_are_rejected() {
        assert_eq!(
            split_body(PROTOCOL_CHAT_COMPLETIONS, b"not json", true),
            Err(SplitError::InvalidJson)
        );
        assert_eq!(
            split_body(PROTOCOL_CHAT_COMPLETIONS, b"[1,2]", true),
            Err(SplitError::NotAnObject)
        );
        assert_eq!(
            split_body("grpc", b"{}", true),
            Err(SplitError::UnknownProtocol("grpc".to_string()))
        );
    }

    #[test]
    fn missing_or_corrupt_blobs_fail_assembly_explicitly() {
        let raw = serde_json::to_vec(
            &json!({"model": "m", "messages": [{"role": "user", "content": "hi"}]}),
        )
        .unwrap();
        let split = split_body(PROTOCOL_CHAT_COMPLETIONS, &raw, true).unwrap();
        let hash = split.items[0].hash.clone();
        assert_eq!(
            assemble(PROTOCOL_CHAT_COMPLETIONS, &split.skeleton, |_| None),
            Err(AssembleError::MissingBlob(hash.clone()))
        );
        assert_eq!(
            assemble(PROTOCOL_CHAT_COMPLETIONS, &split.skeleton, |_| Some(
                "{".to_string()
            )),
            Err(AssembleError::InvalidBlob(hash))
        );
        assert_eq!(
            assemble(PROTOCOL_CHAT_COMPLETIONS, "[]", |_| None),
            Err(AssembleError::InvalidSkeleton)
        );
    }

    #[test]
    fn a_real_item_carrying_the_reference_key_survives() {
        // Only layout positions are restored; nested occurrences are content.
        let body = json!({
            "model": "m",
            "messages": [{"role": "user", "content": {"$llmconduit_blob": "not-a-ref"}}]
        });
        round_trip(PROTOCOL_CHAT_COMPLETIONS, body);
    }

    #[test]
    fn bodies_without_layout_keys_produce_no_items() {
        let split = round_trip(
            PROTOCOL_CHAT_COMPLETIONS,
            json!({"model": "m", "messages": 7}),
        );
        // A `messages` that is neither an array nor a string is left in place.
        assert!(split.items.is_empty());
    }
}
