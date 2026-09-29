//! Event wire normalization.
//!
//! Transports (the future Matrix adapter or the test fake) hand over plain JSON
//! objects as returned by `/sync` and `/messages`. This module validates the
//! envelope, extracts relation metadata, decides edit validity and tells the
//! store which events are messages, edits, redactions or opaque unknowns.
//!
//! We deliberately do not implement a Matrix state renderer. Events of unknown
//! or non-message types are stored verbatim and never projected.

use serde_json::{Map, Value};

/// Where an event was fetched from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    Sync,
    History,
}

impl Source {
    pub fn as_str(self) -> &'static str {
        match self {
            Source::Sync => "sync",
            Source::History => "history",
        }
    }
}

/// One `/sync` response, normalized for the engine.
#[derive(Debug, Clone)]
pub struct SyncBatch {
    /// The opaque `next_batch` token. Persisted only after the batch commits.
    pub next_batch: String,
    pub rooms: Vec<SyncRoomUpdate>,
}

/// Room section of a `/sync` response.
#[derive(Debug, Clone, Default)]
pub struct SyncRoomUpdate {
    pub room_id: String,
    /// Timeline events, oldest first (as returned by the server).
    pub timeline: Vec<Value>,
    /// State events accompanying the timeline.
    pub state: Vec<Value>,
    /// Opaque token pointing at the start of this timeline; used to seed
    /// history backfill and as the upper bound of a limited-sync gap.
    pub prev_batch: Option<String>,
    /// True when the server omitted events between `prev_batch` and the
    /// previously known position.
    pub limited: bool,
}

/// One `/messages` page.
#[derive(Debug, Clone)]
pub struct HistoryPage {
    pub start: String,
    /// Next token when paging further in the same direction; `None` means the
    /// end of accessible history was reached.
    pub end: Option<String>,
    pub chunk: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EventError {
    #[error("event is not a JSON object")]
    NotAnObject,
    #[error("event is missing a string `type`")]
    MissingType,
    #[error("event is missing a string `event_id`")]
    MissingEventId,
}

pub const MESSAGE: &str = "m.room.message";
pub const ENCRYPTED: &str = "m.room.encrypted";
pub const REDACTION: &str = "m.room.redaction";
pub const ROOM_CREATE: &str = "m.room.create";
pub const ROOM_ENCRYPTION: &str = "m.room.encryption";
pub const ROOM_TOMBSTONE: &str = "m.room.tombstone";

/// Message-like event types that take part in the text projection.
pub fn is_message_type(event_type: &str) -> bool {
    matches!(event_type, MESSAGE | ENCRYPTED)
}

/// A normalized archive event.
#[derive(Debug, Clone)]
pub struct NormalizedEvent {
    pub room_id: String,
    pub event_id: String,
    pub event_type: String,
    pub sender: Option<String>,
    pub state_key: Option<String>,
    pub origin_server_ts: Option<i64>,
    pub received_at: i64,
    pub source: Source,
    /// Raw event JSON. Redacted representations are pruned here so no body can
    /// survive in the archive.
    pub raw_json: String,
    /// Effective text for plain messages, `m.new_content.body` for valid edits.
    pub body_text: Option<String>,
    pub relation_type: Option<String>,
    pub relates_to_event_id: Option<String>,
    /// Set only for valid `m.replace` edits; the id of the replaced event.
    pub edit_target: Option<String>,
    /// True for any `m.replace` attempt, including invalid ones, so invalid
    /// edits are never projected as standalone messages.
    pub edit_attempt: bool,
    pub thread_root_id: Option<String>,
    /// The payload itself says the event is already redacted (for example
    /// `unsigned.redacted_because` or an empty content object for a message).
    pub redacted: bool,
    pub encrypted: bool,
    /// Target of `m.room.redaction`, resolved with room-version rules.
    pub redaction_target: Option<String>,
}

/// Parse and normalize one wire event.
pub fn normalize(
    room_id: &str,
    value: &Value,
    room_version: Option<&str>,
    source: Source,
    received_at: i64,
) -> Result<NormalizedEvent, EventError> {
    let obj = value.as_object().ok_or(EventError::NotAnObject)?;
    let event_type = obj
        .get("type")
        .and_then(Value::as_str)
        .ok_or(EventError::MissingType)?
        .to_owned();
    let event_id = obj
        .get("event_id")
        .and_then(Value::as_str)
        .ok_or(EventError::MissingEventId)?
        .to_owned();

    let sender = obj.get("sender").and_then(Value::as_str).map(str::to_owned);
    let state_key = obj
        .get("state_key")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let origin_server_ts = obj.get("origin_server_ts").and_then(Value::as_i64);

    let content = obj.get("content").and_then(Value::as_object);
    let already_redacted = obj
        .get("unsigned")
        .and_then(Value::as_object)
        .is_some_and(|unsigned| unsigned.contains_key("redacted_because"))
        || (is_message_type(&event_type) && content.is_none_or(|c| c.is_empty()));

    let mut body_text = None;
    let mut relation_type = None;
    let mut relates_to_event_id = None;
    let mut edit_target = None;
    let mut edit_attempt = false;
    let mut thread_root_id = None;

    if event_type == MESSAGE {
        if let Some(content) = content {
            body_text = content
                .get("body")
                .and_then(Value::as_str)
                .map(str::to_owned);

            if let Some(relates_to) = content.get("m.relates_to").and_then(Value::as_object) {
                let rel_type = relates_to
                    .get("rel_type")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                let rel_event_id = relates_to
                    .get("event_id")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                relation_type = rel_type.clone();
                relates_to_event_id = rel_event_id.clone();

                match rel_type.as_deref() {
                    Some("m.replace") => {
                        edit_attempt = true;
                        let new_content = content.get("m.new_content").and_then(Value::as_object);
                        let new_body = new_content
                            .and_then(|c| c.get("body"))
                            .and_then(Value::as_str);
                        // Valid edits carry both the target and a new text body.
                        if let (Some(target), Some(new_body)) = (rel_event_id, new_body) {
                            edit_target = Some(target);
                            body_text = Some(new_body.to_owned());
                        }
                    }
                    Some("m.thread") => {
                        thread_root_id = rel_event_id;
                        if let Some(reply) =
                            relates_to.get("m.in_reply_to").and_then(Value::as_object)
                        {
                            relation_type = Some("m.thread".to_owned());
                            relates_to_event_id = reply
                                .get("event_id")
                                .and_then(Value::as_str)
                                .map(str::to_owned);
                        }
                    }
                    _ => {
                        if let Some(reply) =
                            relates_to.get("m.in_reply_to").and_then(Value::as_object)
                        {
                            relation_type = Some("m.in_reply_to".to_owned());
                            relates_to_event_id = reply
                                .get("event_id")
                                .and_then(Value::as_str)
                                .map(str::to_owned);
                        }
                    }
                }
            }
        }
    }

    if already_redacted {
        body_text = None;
        edit_target = None;
    }

    let redaction_target = if event_type == REDACTION {
        resolve_redaction_target(obj, room_version)
    } else {
        None
    };

    let raw_json = if already_redacted {
        prune_redacted(value, &event_type, room_version)
    } else {
        serde_json::to_string(value).unwrap_or_else(|_| "{}".to_owned())
    };

    Ok(NormalizedEvent {
        room_id: room_id.to_owned(),
        event_id,
        event_type,
        sender,
        state_key,
        origin_server_ts,
        received_at,
        source,
        raw_json,
        body_text,
        relation_type,
        relates_to_event_id,
        edit_target,
        edit_attempt,
        thread_root_id,
        redacted: already_redacted,
        encrypted: false,
        redaction_target,
    })
}

/// Resolve the target of an `m.room.redaction` event using room-version rules.
///
/// Room version 11 moved `redacts` from the top level into `content`. For
/// unknown room versions we prefer `content.redacts` when present and fall back
/// to the top-level field.
fn resolve_redaction_target(
    obj: &Map<String, Value>,
    room_version: Option<&str>,
) -> Option<String> {
    let top_level = obj
        .get("redacts")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let in_content = obj
        .get("content")
        .and_then(Value::as_object)
        .and_then(|c| c.get("redacts"))
        .and_then(Value::as_str)
        .map(str::to_owned);

    match version_major(room_version) {
        Some(major) if major >= 11 => in_content.or(top_level),
        Some(_) => top_level.or(in_content),
        None => in_content.or(top_level),
    }
}

/// Parse the numeric major part of a room version id, if it is a plain number.
fn version_major(room_version: Option<&str>) -> Option<u64> {
    let version = room_version?;
    let digits: String = version.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok()
}

/// The room-version dependent parts of the redaction algorithm we apply.
struct RedactionFlags {
    keep_room_aliases_aliases: bool,
    keep_room_join_rules_allow: bool,
    keep_room_member_join_authorised_via_users_server: bool,
    keep_room_member_third_party_invite_signed: bool,
    keep_room_create_content: bool,
    keep_room_redaction_redacts: bool,
    keep_room_power_levels_invite: bool,
}

fn redaction_flags(room_version: Option<&str>) -> RedactionFlags {
    let major = version_major(room_version).unwrap_or(1);
    RedactionFlags {
        keep_room_aliases_aliases: major < 6,
        keep_room_join_rules_allow: major >= 8,
        keep_room_member_join_authorised_via_users_server: major >= 9,
        keep_room_member_third_party_invite_signed: major >= 11,
        keep_room_create_content: major >= 11,
        keep_room_redaction_redacts: major >= 11,
        keep_room_power_levels_invite: major >= 11,
    }
}

fn keep(content: &Map<String, Value>, keys: &[&str]) -> Value {
    let mut out = Map::new();
    for key in keys {
        if let Some(value) = content.get(*key) {
            out.insert((*key).to_owned(), value.clone());
        }
    }
    Value::Object(out)
}

/// Prune a redacted event's content down to the fields the spec preserves for
/// its type and room version, then serialize the event. This is what removes
/// bodies from the raw JSON we keep.
pub fn prune_redacted(value: &Value, event_type: &str, room_version: Option<&str>) -> String {
    let Some(obj) = value.as_object() else {
        return value.to_string();
    };
    let flags = redaction_flags(room_version);
    let content = obj.get("content").and_then(Value::as_object);

    let redacted_content = match (event_type, content) {
        (_, None) => Value::Object(Map::new()),
        ("m.room.member", Some(content)) => {
            let mut keys = vec!["membership"];
            if flags.keep_room_member_join_authorised_via_users_server {
                keys.push("join_authorised_via_users_server");
            }
            let mut pruned = keep(content, &keys);
            if flags.keep_room_member_third_party_invite_signed {
                if let Some(signed) = content
                    .get("third_party_invite")
                    .and_then(Value::as_object)
                    .and_then(|t| t.get("signed"))
                {
                    let mut third_party = Map::new();
                    third_party.insert("signed".to_owned(), signed.clone());
                    if let Value::Object(ref mut pruned) = pruned {
                        pruned.insert("third_party_invite".to_owned(), Value::Object(third_party));
                    }
                }
            }
            pruned
        }
        ("m.room.create", Some(content)) if flags.keep_room_create_content => {
            Value::Object(content.clone())
        }
        ("m.room.create", Some(content)) => keep(content, &["creator"]),
        ("m.room.join_rules", Some(content)) => {
            let mut keys = vec!["join_rule"];
            if flags.keep_room_join_rules_allow {
                keys.push("allow");
            }
            keep(content, &keys)
        }
        ("m.room.power_levels", Some(content)) => {
            let mut keys = vec![
                "ban",
                "events",
                "events_default",
                "kick",
                "redact",
                "state_default",
                "users",
                "users_default",
            ];
            if flags.keep_room_power_levels_invite {
                keys.push("invite");
            }
            keep(content, &keys)
        }
        ("m.room.history_visibility", Some(content)) => keep(content, &["history_visibility"]),
        ("m.room.aliases", Some(content)) if flags.keep_room_aliases_aliases => {
            keep(content, &["aliases"])
        }
        ("m.room.redaction", Some(content)) if flags.keep_room_redaction_redacts => {
            keep(content, &["redacts"])
        }
        _ => Value::Object(Map::new()),
    };

    let mut pruned = obj.clone();
    pruned.insert("content".to_owned(), redacted_content);
    if version_major(room_version).is_some_and(|major| major >= 11) {
        pruned.remove("origin");
        pruned.remove("membership");
        pruned.remove("prev_state");
    }
    Value::Object(pruned).to_string()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn valid_edit_normalizes_to_target_and_body() {
        let value = json!({
            "type": "m.room.message",
            "event_id": "$edit",
            "sender": "@a:example.org",
            "origin_server_ts": 20,
            "content": {
                "msgtype": "m.text",
                "body": "* fixed",
                "m.new_content": { "msgtype": "m.text", "body": "fixed" },
                "m.relates_to": { "rel_type": "m.replace", "event_id": "$orig" }
            }
        });
        let ev = normalize("!r:example.org", &value, Some("11"), Source::Sync, 1).unwrap();
        assert_eq!(ev.edit_target.as_deref(), Some("$orig"));
        assert_eq!(ev.body_text.as_deref(), Some("fixed"));
        assert!(ev.edit_attempt);
    }

    #[test]
    fn invalid_edit_is_an_attempt_without_target() {
        let value = json!({
            "type": "m.room.message",
            "event_id": "$edit",
            "content": {
                "msgtype": "m.text",
                "body": "* fixed",
                "m.relates_to": { "rel_type": "m.replace", "event_id": "$orig" }
            }
        });
        let ev = normalize("!r:example.org", &value, None, Source::Sync, 1).unwrap();
        assert_eq!(ev.edit_target, None);
        assert!(ev.edit_attempt);
    }

    #[test]
    fn redaction_target_follows_room_version() {
        let v11 = json!({
            "type": "m.room.redaction",
            "event_id": "$red",
            "content": { "redacts": "$target" }
        });
        assert_eq!(
            normalize("!r:example.org", &v11, Some("11"), Source::Sync, 1)
                .unwrap()
                .redaction_target
                .as_deref(),
            Some("$target")
        );

        let v10 = json!({
            "type": "m.room.redaction",
            "event_id": "$red",
            "redacts": "$target",
            "content": {}
        });
        assert_eq!(
            normalize("!r:example.org", &v10, Some("10"), Source::Sync, 1)
                .unwrap()
                .redaction_target
                .as_deref(),
            Some("$target")
        );
    }

    #[test]
    fn pruning_removes_message_body_and_keeps_member_metadata() {
        let message = json!({
            "type": "m.room.message",
            "event_id": "$m",
            "content": { "msgtype": "m.text", "body": "secret" }
        });
        let pruned = prune_redacted(&message, "m.room.message", Some("11"));
        assert!(!pruned.contains("secret"));

        let member = json!({
            "type": "m.room.member",
            "event_id": "$mem",
            "content": { "membership": "join", "displayname": "A" }
        });
        let pruned: Value =
            serde_json::from_str(&prune_redacted(&member, "m.room.member", Some("9"))).unwrap();
        assert_eq!(pruned["content"]["membership"], "join");
        assert!(pruned["content"].get("displayname").is_none());
    }

    #[test]
    fn already_redacted_message_keeps_no_body() {
        let value = json!({
            "type": "m.room.message",
            "event_id": "$m",
            "unsigned": { "redacted_because": { "type": "m.room.redaction" } },
            "content": {}
        });
        let ev = normalize("!r:example.org", &value, None, Source::History, 1).unwrap();
        assert!(ev.redacted);
        assert!(ev.body_text.is_none());
    }
}
