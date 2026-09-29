//! Event wire normalization.
//!
//! Transports (the future Matrix adapter or the test fake) hand over plain JSON
//! objects as returned by `/sync` and `/messages`. This module validates the
//! envelope, extracts relation metadata, decides edit validity and tells the
//! store which events are messages, edits, redactions or opaque unknowns.
//!
//! We deliberately do not implement a Matrix state renderer. Events of unknown
//! or non-message types are stored verbatim and never projected.

use ruma_common::{
    canonical_json::{redact, CanonicalJsonObject, CanonicalJsonValue, RedactedBecause},
    RoomVersionId,
};
use serde_json::{Map, Value};

/// Where an event was fetched from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    Sync,
    History,
    /// A validated replacement recovered from an enclosing event's
    /// `unsigned.m.relations.m.replace` bundle. Derived transport metadata, not
    /// a separately fetched event.
    Bundle,
}

impl Source {
    pub fn as_str(self) -> &'static str {
        match self {
            Source::Sync => "sync",
            Source::History => "history",
            Source::Bundle => "bundle",
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
    /// Validated replacements recovered from this event's
    /// `unsigned.m.relations.m.replace` bundle, to be stored as their own
    /// events through the normal path. Bundles of bundles are never expanded.
    pub bundled_replacements: Vec<NormalizedEvent>,
}

/// Parse and normalize one wire event.
pub fn normalize(
    room_id: &str,
    value: &Value,
    room_version: Option<&str>,
    source: Source,
    received_at: i64,
) -> Result<NormalizedEvent, EventError> {
    normalize_inner(room_id, value, room_version, source, received_at, true)
}

/// Normalize one event. `allow_bundles` is false when normalizing a bundled
/// replacement itself, so nested bundles are never expanded recursively.
fn normalize_inner(
    room_id: &str,
    value: &Value,
    room_version: Option<&str>,
    source: Source,
    received_at: i64,
    allow_bundles: bool,
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
                        // A valid replacement must carry a target and a whole
                        // message in `m.new_content`, at least `msgtype` and
                        // `body`, not merely an arbitrary `body`.
                        if let Some(new_content) =
                            content.get("m.new_content").and_then(Value::as_object)
                        {
                            let new_body = new_content.get("body").and_then(Value::as_str);
                            let new_msgtype = new_content.get("msgtype").and_then(Value::as_str);
                            if let (Some(target), Some(new_body), Some(_msgtype)) =
                                (rel_event_id, new_body, new_msgtype)
                            {
                                edit_target = Some(target);
                                body_text = Some(new_body.to_owned());
                            }
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
        canonical_raw(value)
    };

    // A replacement cached under `unsigned.m.relations.m.replace` is ingested
    // as its own event when it self-identifies consistently. Only a direct
    // bundle of a non-edit message is considered; bundles are never expanded
    // recursively.
    let bundled_replacements = if allow_bundles && !edit_attempt {
        extract_bundled_replacements(
            room_id,
            &event_id,
            &event_type,
            sender.as_deref(),
            obj,
            room_version,
            received_at,
        )
    } else {
        Vec::new()
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
        bundled_replacements,
    })
}

/// Serialize an unredacted event for the archive after dropping the opaque
/// server-generated `unsigned` block. Raw export is the archived event
/// representation, not a byte-for-byte transport dump: relation caches
/// (`unsigned.m.relations`, `unsigned.prev_content`) are either ingested as
/// their own events or discarded, never duplicated inside the parent.
fn canonical_raw(value: &Value) -> String {
    match value.as_object() {
        Some(object) => {
            let mut archived = object.clone();
            archived.remove("unsigned");
            Value::Object(archived).to_string()
        }
        None => "{}".to_owned(),
    }
}

/// Validate and recover a replacement bundled under
/// `unsigned.m.relations.m.replace` of a plain message.
///
/// A bundle is accepted only when it identifies itself consistently: the
/// enclosing event is a message with a non-missing sender, the bundle shares
/// that sender and (if present) the room, and normalizing it with the same
/// rules yields a valid `m.replace` whose target is the enclosing event. Any
/// other bundle is dropped and changes nothing.
#[allow(clippy::too_many_arguments)]
fn extract_bundled_replacements(
    room_id: &str,
    event_id: &str,
    event_type: &str,
    sender: Option<&str>,
    obj: &Map<String, Value>,
    room_version: Option<&str>,
    received_at: i64,
) -> Vec<NormalizedEvent> {
    if event_type != MESSAGE || sender.is_none() {
        return Vec::new();
    }
    let Some(bundle) = obj
        .get("unsigned")
        .and_then(Value::as_object)
        .and_then(|unsigned| unsigned.get("m.relations"))
        .and_then(Value::as_object)
        .and_then(|relations| relations.get("m.replace"))
    else {
        return Vec::new();
    };
    let Some(bundle_object) = bundle.as_object() else {
        return Vec::new();
    };
    // A `room_id` that is present must match exactly. A non-string value is
    // invalid, not equivalent to an absent field.
    if let Some(room_field) = bundle_object.get("room_id") {
        if room_field.as_str() != Some(room_id) {
            return Vec::new();
        }
    }
    if bundle_object.get("sender").and_then(Value::as_str) != sender {
        return Vec::new();
    }
    let Ok(bundled) = normalize_inner(
        room_id,
        bundle,
        room_version,
        Source::Bundle,
        received_at,
        false,
    ) else {
        return Vec::new();
    };
    // The bundle must be a distinct event replacing the enclosing one. A
    // derived cache may never claim the enclosing event's identity, or it
    // would later be applied over the original it belongs to.
    if bundled.event_id == event_id || bundled.edit_target.as_deref() != Some(event_id) {
        return Vec::new();
    }
    vec![bundled]
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

/// Prune a redacted event down to the envelope and content the Matrix
/// redaction algorithm preserves for its type and room version, then serialize
/// the event. This is what removes bodies from the raw JSON we keep.
///
/// Unknown or malformed room versions and values that cannot be represented as
/// canonical JSON fail closed to a provenance-only envelope with empty
/// content: the original payload is never returned.
pub fn prune_redacted(value: &Value, event_type: &str, room_version: Option<&str>) -> String {
    match redact_with_rules(value, room_version) {
        Some(redacted) => Value::from(redacted).to_string(),
        None => strict_redacted_envelope(value, event_type).to_string(),
    }
}

/// Apply ruma-common's maintained redaction algorithm with the rules of the
/// room version. Returns `None` for unknown or unparseable versions, for
/// non-object events, for values canonical JSON cannot represent (for example
/// floats) or for a payload ruma rejects, so the caller can fail closed.
fn redact_with_rules(value: &Value, room_version: Option<&str>) -> Option<CanonicalJsonValue> {
    let version: RoomVersionId = room_version?.parse().ok()?;
    let rules = version.rules()?.redaction;
    let object = canonical_object(value)?;
    let redacted_because = value
        .get("unsigned")
        .and_then(Value::as_object)
        .and_then(|unsigned| unsigned.get("redacted_because"))
        .and_then(|because| canonical_object(&sanitize_redacted_because(because)))
        .map(RedactedBecause::from_json);
    let redacted = redact(object, &rules, redacted_because).ok()?;
    Some(CanonicalJsonValue::Object(redacted))
}

/// Convert a JSON value to a canonical JSON object, or `None` when it is not an
/// object or cannot be represented in canonical JSON.
fn canonical_object(value: &Value) -> Option<CanonicalJsonObject> {
    match CanonicalJsonValue::try_from(value.clone()).ok()? {
        CanonicalJsonValue::Object(object) => Some(object),
        _ => None,
    }
}

/// Keep only an object marker and identifier provenance from a
/// `unsigned.redacted_because` value. Arbitrary payload (for example a
/// redaction `reason` or nested relations) is never carried forward.
fn sanitize_redacted_because(because: &Value) -> Value {
    let Some(object) = because.as_object() else {
        return Value::Object(Map::new());
    };
    let mut sanitized = Map::new();
    for key in ["type", "event_id", "sender"] {
        if let Some(field) = object.get(key).and_then(Value::as_str) {
            sanitized.insert(key.to_owned(), Value::String(field.to_owned()));
        }
    }
    if let Some(ts) = object.get("origin_server_ts").and_then(Value::as_i64) {
        sanitized.insert("origin_server_ts".to_owned(), Value::Number(ts.into()));
    }
    Value::Object(sanitized)
}

/// A provenance-only redacted envelope used when the room version is unknown or
/// the event cannot be canonicalized: event identity and sender survive, content
/// is emptied and every other field is dropped.
fn strict_redacted_envelope(value: &Value, event_type: &str) -> Value {
    let Some(object) = value.as_object() else {
        return Value::Object(Map::new());
    };
    let mut envelope = Map::new();
    envelope.insert("type".to_owned(), Value::String(event_type.to_owned()));
    for key in ["event_id", "room_id", "sender", "state_key"] {
        if let Some(field) = object.get(key).and_then(Value::as_str) {
            envelope.insert(key.to_owned(), Value::String(field.to_owned()));
        }
    }
    if let Some(ts) = object.get("origin_server_ts").and_then(Value::as_i64) {
        envelope.insert("origin_server_ts".to_owned(), Value::Number(ts.into()));
    }
    envelope.insert("content".to_owned(), Value::Object(Map::new()));
    if let Some(because) = object
        .get("unsigned")
        .and_then(Value::as_object)
        .and_then(|unsigned| unsigned.get("redacted_because"))
    {
        let mut unsigned = Map::new();
        unsigned.insert(
            "redacted_because".to_owned(),
            sanitize_redacted_because(because),
        );
        envelope.insert("unsigned".to_owned(), Value::Object(unsigned));
    }
    Value::Object(envelope)
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

    #[test]
    fn replacement_without_msgtype_is_not_valid() {
        let value = json!({
            "type": "m.room.message",
            "event_id": "$edit",
            "sender": "@a:example.org",
            "origin_server_ts": 20,
            "content": {
                "msgtype": "m.text",
                "body": "* no msgtype",
                "m.new_content": { "body": "no msgtype" },
                "m.relates_to": { "rel_type": "m.replace", "event_id": "$orig" }
            }
        });
        let ev = normalize("!r:example.org", &value, Some("11"), Source::Sync, 1).unwrap();
        assert!(ev.edit_attempt);
        assert_eq!(
            ev.edit_target, None,
            "no msgtype means no valid replacement"
        );
    }

    #[test]
    fn pruning_drops_unsigned_relations_prev_content_and_extensions() {
        let value = json!({
            "type": "m.room.message",
            "event_id": "$edit",
            "room_id": "!r:example.org",
            "sender": "@a:example.org",
            "origin_server_ts": 7,
            "content": {
                "msgtype": "m.text",
                "body": "* hidden edit",
                "m.new_content": { "msgtype": "m.text", "body": "hidden edit" },
                "m.relates_to": { "rel_type": "m.replace", "event_id": "$orig" }
            },
            "unsigned": {
                "m.relations": {
                    "m.replace": { "event_id": "$edit", "content": { "body": "hidden aggregate" } }
                },
                "prev_content": { "body": "hidden previous" }
            },
            "arbitrary_extension": { "note": "hidden extension" }
        });
        let pruned = prune_redacted(&value, "m.room.message", Some("11"));
        assert!(!pruned.contains("hidden"), "removed text leaked: {pruned}");
        let parsed: Value = serde_json::from_str(&pruned).unwrap();
        assert!(parsed.get("unsigned").is_none());
        assert!(parsed.get("arbitrary_extension").is_none());
        assert_eq!(parsed["event_id"], "$edit");
        assert_eq!(parsed["sender"], "@a:example.org");
        assert_eq!(parsed["content"], json!({}));
    }

    #[test]
    fn pruning_sanitizes_redacted_because_payload() {
        let value = json!({
            "type": "m.room.message",
            "event_id": "$m",
            "room_id": "!r:example.org",
            "content": {},
            "unsigned": {
                "redacted_because": {
                    "type": "m.room.redaction",
                    "event_id": "$red",
                    "sender": "@mod:example.org",
                    "content": { "reason": "secret reason" }
                }
            }
        });
        let pruned = prune_redacted(&value, "m.room.message", Some("11"));
        assert!(!pruned.contains("secret reason"), "reason leaked: {pruned}");
        let parsed: Value = serde_json::from_str(&pruned).unwrap();
        assert_eq!(
            parsed["unsigned"]["redacted_because"]["type"],
            "m.room.redaction"
        );
        assert_eq!(parsed["unsigned"]["redacted_because"]["event_id"], "$red");
        assert!(parsed["unsigned"]["redacted_because"]
            .get("content")
            .is_none());
        assert_eq!(parsed["event_id"], "$m");
    }

    #[test]
    fn pruning_fails_closed_for_unknown_room_version() {
        let value = json!({
            "type": "m.room.message",
            "event_id": "$m",
            "room_id": "!r:example.org",
            "sender": "@a:example.org",
            "origin_server_ts": 5,
            "content": { "msgtype": "m.text", "body": "secret body" },
            "unsigned": {
                "m.relations": { "m.replace": { "content": { "body": "secret aggregate" } } }
            }
        });
        let pruned = prune_redacted(&value, "m.room.message", Some("org.example.custom"));
        assert!(!pruned.contains("secret"), "removed text leaked: {pruned}");
        let parsed: Value = serde_json::from_str(&pruned).unwrap();
        assert_eq!(parsed["type"], "m.room.message");
        assert_eq!(parsed["event_id"], "$m");
        assert_eq!(parsed["sender"], "@a:example.org");
        assert_eq!(parsed["origin_server_ts"], 5);
        assert_eq!(parsed["content"], json!({}));
        assert!(parsed.get("unsigned").is_none());
    }

    #[test]
    fn bundled_replacement_is_validated_and_unsigned_is_stripped() {
        let value = json!({
            "type": "m.room.message",
            "event_id": "$orig",
            "room_id": "!r:example.org",
            "sender": "@a:example.org",
            "origin_server_ts": 5,
            "content": { "msgtype": "m.text", "body": "original body" },
            "unsigned": {
                "m.relations": {
                    "m.replace": {
                        "type": "m.room.message",
                        "event_id": "$bundle",
                        "sender": "@a:example.org",
                        "room_id": "!r:example.org",
                        "origin_server_ts": 6,
                        "content": {
                            "msgtype": "m.text",
                            "body": "* bundled body",
                            "m.new_content": { "msgtype": "m.text", "body": "bundled body" },
                            "m.relates_to": { "rel_type": "m.replace", "event_id": "$orig" }
                        },
                        "unsigned": {
                            "m.relations": {
                                "m.replace": {
                                    "type": "m.room.message",
                                    "event_id": "$nested",
                                    "sender": "@a:example.org",
                                    "content": {
                                        "msgtype": "m.text",
                                        "body": "* nested body",
                                        "m.new_content": { "msgtype": "m.text", "body": "nested body" },
                                        "m.relates_to": { "rel_type": "m.replace", "event_id": "$bundle" }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });
        let ev = normalize("!r:example.org", &value, Some("11"), Source::Sync, 1).unwrap();
        assert_eq!(ev.bundled_replacements.len(), 1);
        let bundled = &ev.bundled_replacements[0];
        assert_eq!(bundled.event_id, "$bundle");
        assert_eq!(bundled.source, Source::Bundle);
        assert_eq!(bundled.edit_target.as_deref(), Some("$orig"));
        assert_eq!(bundled.body_text.as_deref(), Some("bundled body"));
        assert!(bundled.bundled_replacements.is_empty());
        assert!(!ev.raw_json.contains("bundled body"));
        assert!(!ev.raw_json.contains("nested body"));
        assert!(!ev.raw_json.contains("m.relations"));
        assert!(!bundled.raw_json.contains("nested body"));
        assert!(!bundled.raw_json.contains("m.relations"));
    }

    #[test]
    fn invalid_bundle_is_not_exposed_as_a_replacement() {
        let value = json!({
            "type": "m.room.message",
            "event_id": "$orig",
            "room_id": "!r:example.org",
            "sender": "@a:example.org",
            "content": { "msgtype": "m.text", "body": "original body" },
            "unsigned": {
                "m.relations": {
                    "m.replace": {
                        "type": "m.room.message",
                        "event_id": "$bundle",
                        "sender": "@b:example.org",
                        "content": {
                            "msgtype": "m.text",
                            "body": "* bundled body",
                            "m.new_content": { "msgtype": "m.text", "body": "bundled body" },
                            "m.relates_to": { "rel_type": "m.replace", "event_id": "$orig" }
                        }
                    }
                }
            }
        });
        let ev = normalize("!r:example.org", &value, Some("11"), Source::Sync, 1).unwrap();
        assert!(ev.bundled_replacements.is_empty());
        assert!(!ev.raw_json.contains("bundled body"));
        assert!(!ev.raw_json.contains("m.relations"));
    }

    #[test]
    fn bundle_identity_collision_and_non_string_room_are_rejected() {
        let parent = |bundle: Value| -> Value {
            json!({
                "type": "m.room.message",
                "event_id": "$orig",
                "room_id": "!r:example.org",
                "sender": "@a:example.org",
                "content": { "msgtype": "m.text", "body": "original body" },
                "unsigned": { "m.relations": { "m.replace": bundle } }
            })
        };
        let base = |id: &str| -> Value {
            json!({
                "type": "m.room.message",
                "event_id": id,
                "sender": "@a:example.org",
                "origin_server_ts": 6,
                "content": {
                    "msgtype": "m.text",
                    "body": "* bundled body",
                    "m.new_content": { "msgtype": "m.text", "body": "bundled body" },
                    "m.relates_to": { "rel_type": "m.replace", "event_id": "$orig" }
                }
            })
        };

        // A bundle may not claim the enclosing event's identity.
        let ev = normalize(
            "!r:example.org",
            &parent(base("$orig")),
            Some("11"),
            Source::Sync,
            1,
        )
        .unwrap();
        assert!(ev.bundled_replacements.is_empty());
        assert!(!ev.raw_json.contains("bundled body"));

        // A present non-string `room_id` is invalid, not absent.
        let mut typed_room = base("$bundle");
        typed_room["room_id"] = json!(42);
        let ev = normalize(
            "!r:example.org",
            &parent(typed_room),
            Some("11"),
            Source::Sync,
            1,
        )
        .unwrap();
        assert!(ev.bundled_replacements.is_empty());
        assert!(!ev.raw_json.contains("bundled body"));

        // A matching string `room_id` is still accepted.
        let mut matching_room = base("$bundle");
        matching_room["room_id"] = json!("!r:example.org");
        let ev = normalize(
            "!r:example.org",
            &parent(matching_room),
            Some("11"),
            Source::Sync,
            1,
        )
        .unwrap();
        assert_eq!(ev.bundled_replacements.len(), 1);
    }
}
