//! Repro for review finding F63: a deeply nested (>128 levels) event is
//! silently dropped by the real MatrixTransport while the batch commits and the
//! sync token / history cursor advance.

mod common;

use common::*;
use mainlinenerd_ingest::config::{Config, RoomConfig, RoomSelector, Token};
use mainlinenerd_ingest::engine::{HistoryRequest, SyncRequest, Transport};
use mainlinenerd_ingest::matrix::MatrixTransport;
use mainlinenerd_ingest::store::{ArchiveIdentity, HistoryWork, Store};
use serde_json::{json, Value};

fn config(server: &MockServer, dir: &std::path::Path) -> Config {
    Config {
        homeserver: server.base_url.clone(),
        user_id: "@ingest:hs.example.org".to_owned(),
        device_id: "MLN".to_owned(),
        token_env: "MLN_UNUSED".to_owned(),
        data_dir: dir.to_path_buf(),
        database: dir.join("archive.sqlite3"),
        history_interval_ms: 1_000,
        history_limit: 50,
        sync_timeout_ms: 2_000,
        rooms: vec![RoomConfig {
            selector: RoomSelector::Id(ROOM.to_owned()),
            join: false,
            via: Vec::new(),
        }],
    }
}

fn ident(cfg: &Config) -> ArchiveIdentity {
    ArchiveIdentity {
        homeserver: cfg.homeserver.clone(),
        user_id: cfg.user_id.clone(),
        device_id: cfg.device_id.clone(),
    }
}

fn msg(id: &str, body: &str) -> Value {
    json!({
        "type": "m.room.message",
        "event_id": id,
        "sender": "@alice:hs.example.org",
        "origin_server_ts": 100,
        "content": { "msgtype": "m.text", "body": body }
    })
}

/// A normal, valid m.room.message whose content carries `depth` nested arrays
/// in an extra key. The whole event is ~a few hundred bytes.
fn deep_msg(id: &str, depth: usize) -> Value {
    let mut nested = json!(0);
    for _ in 0..depth {
        nested = Value::Array(vec![nested]);
    }
    let mut event = msg(id, "deep but valid");
    event["content"]["x"] = nested;
    event
}

fn create_event() -> Value {
    json!({
        "type": "m.room.create",
        "event_id": "$create",
        "sender": "@alice:hs.example.org",
        "state_key": "",
        "origin_server_ts": 1,
        "content": { "creator": "@alice:hs.example.org", "room_version": "11" }
    })
}

#[tokio::test]
async fn f63_deeply_nested_sync_event_is_not_silently_dropped() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(&server, dir.path());
    let transport = MatrixTransport::connect(&cfg, &Token::new("fake-token"))
        .await
        .expect("connect adapter");
    transport.allow_room(ROOM.parse().unwrap());

    let deep = deep_msg("$deep", 130);
    eprintln!("F63 deep event bytes: {}", deep.to_string().len());
    let mut join = serde_json::Map::new();
    join.insert(
        ROOM.to_owned(),
        json!({
            "timeline": { "events": [msg("$ok", "fine"), deep], "prev_batch": "p1", "limited": false },
            "state": { "events": [create_event()] }
        }),
    );
    server.state.push_sync(MockResponse::json(
        json!({ "next_batch": "s1", "rooms": { "join": Value::Object(join) } }),
    ));

    let result = transport
        .sync(SyncRequest {
            since: None,
            timeout_ms: 1_000,
        })
        .await;
    let batch = match result {
        Ok(batch) => batch,
        // Surfacing the problem as an error would also be acceptable behaviour.
        Err(error) => {
            eprintln!("F63 sync surfaced an error (acceptable): {error:?}");
            return;
        }
    };
    let timeline_ids: Vec<String> = batch.rooms[0]
        .timeline
        .iter()
        .map(|e| e["event_id"].as_str().unwrap_or("?").to_owned())
        .collect();
    eprintln!("F63 sync timeline delivered by MatrixTransport: {timeline_ids:?}");

    let mut store = Store::open(&cfg.database, &ident(&cfg)).unwrap();
    store.apply_sync_batch(&batch, 10).unwrap();
    let conn = db(&cfg.database);
    let ok = scalar_i64(&conn, "SELECT COUNT(*) FROM events WHERE event_id = '$ok'");
    let deep_n = scalar_i64(&conn, "SELECT COUNT(*) FROM events WHERE event_id = '$deep'");
    let token = scalar_string(&conn, "SELECT since_token FROM sync_progress WHERE id = 1");
    let last_error = scalar_string(&conn, "SELECT last_error FROM sync_progress WHERE id = 1");
    eprintln!(
        "F63 sync store: $ok={ok} $deep={deep_n} since_token={token:?} last_error={last_error:?}"
    );
    assert_eq!(
        deep_n, 1,
        "valid, homeserver-served event $deep was silently dropped while the sync \
         token advanced to {token:?} (last_error={last_error:?})"
    );
}

#[tokio::test]
async fn f63_deeply_nested_history_event_is_not_silently_dropped() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(&server, dir.path());
    let transport = MatrixTransport::connect(&cfg, &Token::new("fake-token"))
        .await
        .expect("connect adapter");
    transport.allow_room(ROOM.parse().unwrap());

    let mut store = Store::open(&cfg.database, &ident(&cfg)).unwrap();
    let mut room = common::room_update(ROOM);
    room.timeline = vec![msg("$live", "live")];
    room.state = vec![create_event()];
    room.prev_batch = Some("p1".to_owned());
    store
        .apply_sync_batch(&sync_batch("s1", vec![room]), 10)
        .unwrap();

    server.state.push_messages(
        "p1",
        MockResponse::json(json!({
            "start": "p1",
            "end": "p2",
            "chunk": [msg("$hist_ok", "fine"), deep_msg("$hist_deep", 130)]
        })),
    );
    let page = match transport
        .history(HistoryRequest {
            room_id: ROOM.to_owned(),
            from: "p1".to_owned(),
            to: None,
            limit: 50,
        })
        .await
    {
        Ok(page) => page,
        Err(error) => {
            eprintln!("F63 history surfaced an error (acceptable): {error:?}");
            return;
        }
    };
    eprintln!("F63 history chunk len delivered: {}", page.chunk.len());
    let applied = store
        .apply_history_page(ROOM, HistoryWork::Base, "p1", &page, 20)
        .unwrap();
    eprintln!("F63 history applied: {applied:?}");
    let conn = db(&cfg.database);
    let deep_n = scalar_i64(
        &conn,
        "SELECT COUNT(*) FROM events WHERE event_id = '$hist_deep'",
    );
    let ok = scalar_i64(
        &conn,
        "SELECT COUNT(*) FROM events WHERE event_id = '$hist_ok'",
    );
    eprintln!("F63 history store: $hist_ok={ok} $hist_deep={deep_n}");
    assert_eq!(
        deep_n, 1,
        "valid /messages event $hist_deep was silently dropped while the page applied"
    );
}

/// Mechanism check for the lone-surrogate vector: the exact expression used by
/// matrix.rs:472 on a ruma Raw captured from wire text with a lone surrogate.
#[test]
fn f63_lone_surrogate_raw_to_value_fails() {
    use ruma_common::serde::Raw;
    let wire = r#"{"type":"m.room.message","event_id":"$s","sender":"@a:hs","origin_server_ts":1,"content":{"msgtype":"m.text","body":"\ud83d"}}"#;
    let raw: Raw<Value> = serde_json::from_str(wire).expect("Raw capture accepts lone surrogate");
    let converted = serde_json::to_value(&raw);
    eprintln!("F63 lone surrogate to_value: {converted:?}");
    assert!(
        converted.is_ok(),
        "raw_event_value would map this event to Null and drop it"
    );
}
