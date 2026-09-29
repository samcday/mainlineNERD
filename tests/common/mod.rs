//! Shared fixtures and a scripted fake transport for integration tests.
#![allow(dead_code)]

use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::Mutex;

use async_trait::async_trait;
use mainlinenerd_ingest::engine::{HistoryRequest, SyncRequest, Transport, TransportError};
use mainlinenerd_ingest::event::{HistoryPage, SyncBatch, SyncRoomUpdate};
use mainlinenerd_ingest::store::{ArchiveIdentity, Store};
use rusqlite::Connection;
use serde_json::{json, Value};

pub const ROOM: &str = "!room:hs.example.org";
pub const ROOM2: &str = "!room2:hs.example.org";
pub const ALICE: &str = "@alice:hs.example.org";
pub const BOB: &str = "@bob:hs.example.org";

pub fn identity() -> ArchiveIdentity {
    ArchiveIdentity {
        homeserver: "https://hs.example.org".to_owned(),
        user_id: "@ingest:hs.example.org".to_owned(),
        device_id: "MLN".to_owned(),
    }
}

pub fn open_store(path: &Path) -> Store {
    Store::open(path, &identity()).expect("open store")
}

pub fn db(path: &Path) -> Connection {
    Connection::open(path).expect("open sqlite directly")
}

pub fn scalar_i64(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |row| row.get(0)).expect("scalar")
}

pub fn scalar_string(conn: &Connection, sql: &str) -> Option<String> {
    conn.query_row(sql, [], |row| row.get(0)).expect("scalar")
}

// ---------------------------------------------------------------------------
// Event fixtures
// ---------------------------------------------------------------------------

pub fn message(id: &str, ts: i64, body: &str) -> Value {
    message_from(id, ts, body, ALICE)
}

pub fn message_from(id: &str, ts: i64, body: &str, sender: &str) -> Value {
    json!({
        "type": "m.room.message",
        "event_id": id,
        "sender": sender,
        "origin_server_ts": ts,
        "content": { "msgtype": "m.text", "body": body }
    })
}

pub fn edit(id: &str, ts: i64, target: &str, new_body: &str) -> Value {
    json!({
        "type": "m.room.message",
        "event_id": id,
        "sender": ALICE,
        "origin_server_ts": ts,
        "content": {
            "msgtype": "m.text",
            "body": format!("* {new_body}"),
            "m.new_content": { "msgtype": "m.text", "body": new_body },
            "m.relates_to": { "rel_type": "m.replace", "event_id": target }
        }
    })
}

pub fn edit_from(id: &str, ts: i64, target: &str, new_body: &str, sender: &str) -> Value {
    let mut value = edit(id, ts, target, new_body);
    value["sender"] = json!(sender);
    value
}

pub fn invalid_edit(id: &str, ts: i64, target: &str) -> Value {
    json!({
        "type": "m.room.message",
        "event_id": id,
        "sender": ALICE,
        "origin_server_ts": ts,
        "content": {
            "msgtype": "m.text",
            "body": "* broken",
            "m.relates_to": { "rel_type": "m.replace", "event_id": target }
        }
    })
}

pub fn redaction(id: &str, ts: i64, target: &str, v11: bool) -> Value {
    if v11 {
        json!({
            "type": "m.room.redaction",
            "event_id": id,
            "sender": ALICE,
            "origin_server_ts": ts,
            "content": { "redacts": target }
        })
    } else {
        json!({
            "type": "m.room.redaction",
            "event_id": id,
            "sender": ALICE,
            "origin_server_ts": ts,
            "redacts": target,
            "content": {}
        })
    }
}

/// A plain message carrying a valid replacement under
/// `unsigned.m.relations.m.replace`. The bundle is transport metadata, not a
/// separately fetched event.
pub fn message_with_bundled_edit(
    id: &str,
    ts: i64,
    body: &str,
    edit_id: &str,
    edit_ts: i64,
    new_body: &str,
) -> Value {
    json!({
        "type": "m.room.message",
        "event_id": id,
        "sender": ALICE,
        "origin_server_ts": ts,
        "content": { "msgtype": "m.text", "body": body },
        "unsigned": {
            "m.relations": {
                "m.replace": {
                    "type": "m.room.message",
                    "event_id": edit_id,
                    "sender": ALICE,
                    "origin_server_ts": edit_ts,
                    "content": {
                        "msgtype": "m.text",
                        "body": format!("* {new_body}"),
                        "m.new_content": { "msgtype": "m.text", "body": new_body },
                        "m.relates_to": { "rel_type": "m.replace", "event_id": id }
                    }
                }
            }
        }
    })
}

pub fn already_redacted_message(id: &str, ts: i64) -> Value {
    json!({
        "type": "m.room.message",
        "event_id": id,
        "sender": ALICE,
        "origin_server_ts": ts,
        "unsigned": { "redacted_because": { "type": "m.room.redaction", "event_id": "$red" } },
        "content": {}
    })
}

pub fn create_room(version: &str) -> Value {
    json!({
        "type": "m.room.create",
        "event_id": "$create",
        "sender": ALICE,
        "state_key": "",
        "origin_server_ts": 1,
        "content": { "creator": ALICE, "room_version": version }
    })
}

pub fn encryption_event() -> Value {
    json!({
        "type": "m.room.encryption",
        "event_id": "$enc",
        "sender": ALICE,
        "state_key": "",
        "origin_server_ts": 2,
        "content": { "algorithm": "m.megolm.v1.aes-sha2" }
    })
}

pub fn tombstone(successor: &str) -> Value {
    json!({
        "type": "m.room.tombstone",
        "event_id": "$tomb",
        "sender": ALICE,
        "state_key": "",
        "origin_server_ts": 3,
        "content": { "body": "upgraded", "replacement_room": successor }
    })
}

pub fn member(id: &str, ts: i64) -> Value {
    json!({
        "type": "m.room.member",
        "event_id": id,
        "sender": ALICE,
        "state_key": ALICE,
        "origin_server_ts": ts,
        "content": { "membership": "join", "displayname": "Alice" }
    })
}

// ---------------------------------------------------------------------------
// Transport fixtures
// ---------------------------------------------------------------------------

pub fn room_update(room_id: &str) -> SyncRoomUpdate {
    SyncRoomUpdate {
        room_id: room_id.to_owned(),
        ..Default::default()
    }
}

pub fn sync_batch(next_batch: &str, rooms: Vec<SyncRoomUpdate>) -> SyncBatch {
    SyncBatch {
        next_batch: next_batch.to_owned(),
        rooms,
    }
}

pub fn history_page(start: &str, end: Option<&str>, chunk: Vec<Value>) -> HistoryPage {
    HistoryPage {
        start: start.to_owned(),
        end: end.map(str::to_owned),
        chunk,
    }
}

/// Scripted transport: `/sync` responses come from one queue, `/messages`
/// responses from per-`from`-token queues. Every request is recorded.
pub struct FakeTransport {
    sync: Mutex<VecDeque<Result<SyncBatch, TransportError>>>,
    history: Mutex<HashMap<String, VecDeque<Result<HistoryPage, TransportError>>>>,
    pub sync_requests: Mutex<Vec<SyncRequest>>,
    pub history_requests: Mutex<Vec<HistoryRequest>>,
}

impl FakeTransport {
    pub fn new() -> Self {
        Self {
            sync: Mutex::new(VecDeque::new()),
            history: Mutex::new(HashMap::new()),
            sync_requests: Mutex::new(Vec::new()),
            history_requests: Mutex::new(Vec::new()),
        }
    }

    pub fn push_sync(&self, batch: SyncBatch) {
        self.sync.lock().unwrap().push_back(Ok(batch));
    }

    pub fn push_sync_error(&self, error: TransportError) {
        self.sync.lock().unwrap().push_back(Err(error));
    }

    pub fn push_history(&self, from: &str, page: HistoryPage) {
        self.history
            .lock()
            .unwrap()
            .entry(from.to_owned())
            .or_default()
            .push_back(Ok(page));
    }

    pub fn push_history_error(&self, from: &str, error: TransportError) {
        self.history
            .lock()
            .unwrap()
            .entry(from.to_owned())
            .or_default()
            .push_back(Err(error));
    }

    pub fn history_call_count(&self) -> usize {
        self.history_requests.lock().unwrap().len()
    }
}

#[async_trait]
impl Transport for FakeTransport {
    async fn sync(&self, request: SyncRequest) -> Result<SyncBatch, TransportError> {
        self.sync_requests.lock().unwrap().push(request);
        self.sync.lock().unwrap().pop_front().unwrap_or_else(|| {
            Err(TransportError::Transient(
                "no scripted /sync response".to_owned(),
            ))
        })
    }

    async fn history(&self, request: HistoryRequest) -> Result<HistoryPage, TransportError> {
        self.history_requests.lock().unwrap().push(request.clone());
        let mut map = self.history.lock().unwrap();
        let queue = map.get_mut(&request.from).ok_or_else(|| {
            TransportError::Transient(format!(
                "no scripted /messages response for {}",
                request.from
            ))
        })?;
        queue.pop_front().unwrap_or_else(|| {
            Err(TransportError::Transient(format!(
                "scripted /messages queue exhausted for {}",
                request.from
            )))
        })
    }
}
