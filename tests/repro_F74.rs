//! F74 reproducer: a room admitted after the archive already has a since token
//! never learns its (old) m.room.encryption / m.room.tombstone state, so it is
//! not flagged and base backfill walks its history.
//!
//! Spec: docs/architecture.md "encrypted rooms and upgrade successors are
//! flagged for operator action instead of being expanded"; docs/operations.md
//! "an encrypted room, or a room with a known successor is flagged and skipped;
//! no work is scheduled".

mod common;

use common::*;
use mainlinenerd_ingest::config::{Config, RoomConfig, RoomSelector, Token};
use mainlinenerd_ingest::matrix::MatrixTransport;
use mainlinenerd_ingest::runtime::{self, RunSettings, StartupPolicy};
use mainlinenerd_ingest::store::{ArchiveIdentity, Store};
use serde_json::{json, Value};

fn tiny_policy() -> StartupPolicy {
    StartupPolicy {
        max_attempts: 4,
        transient_min: std::time::Duration::from_millis(1),
        transient_max: std::time::Duration::from_millis(2),
        rate_limit_fallback: std::time::Duration::from_millis(1),
    }
}

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

fn identity_of(cfg: &Config) -> ArchiveIdentity {
    ArchiveIdentity {
        homeserver: cfg.homeserver.clone(),
        user_id: cfg.user_id.clone(),
        device_id: cfg.device_id.clone(),
    }
}

fn ciphertext(id: &str, ts: i64) -> Value {
    json!({
        "type": "m.room.encrypted",
        "event_id": id,
        "sender": "@alice:hs.example.org",
        "origin_server_ts": ts,
        "content": {
            "algorithm": "m.megolm.v1.aes-sha2",
            "sender_key": "SENDERKEY",
            "device_id": "ALICEDEV",
            "session_id": "SESSIONID",
            "ciphertext": "AwgAEnACgAkLmt6qF84IK++J7UDH2Za1YVchHyprqTqsg"
        }
    })
}

struct Outcome {
    encrypted: i64,
    successor: Option<String>,
    hist_archived: i64,
    messages_requests: usize,
    state_requests: Vec<String>,
}

/// The bot is already joined to ROOM, whose `state_type` state was set long
/// ago. The archive already has a committed since token when the operator adds
/// ROOM to config. The homeserver truthfully serves the old state on the state
/// endpoints; incremental /sync does not resend it.
async fn admit_after_first_sync(state_type: &str, content: Value, hist: Value) -> Outcome {
    let hist_id = hist["event_id"].as_str().unwrap().to_owned();
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(&server, dir.path());
    server
        .state
        .set_create_state(ROOM, MockResponse::json(json!({ "room_version": "11" })));
    server.state.set_room_state(ROOM, state_type, content);

    // Existing archive: a committed global token from before ROOM was configured.
    let mut store = Store::open(&cfg.database, &identity_of(&cfg)).unwrap();
    store.apply_sync_batch(&sync_batch("s5", vec![]), 10).unwrap();

    let transport = MatrixTransport::connect(&cfg, &Token::new("fake-token"))
        .await
        .unwrap();
    let report = runtime::initialize_with_policy(&transport, &cfg, &mut store, &tiny_policy())
        .await
        .unwrap();
    eprintln!(
        "F74 initialize: rooms={:?} unready={:?} versions_learned={:?}",
        report.rooms, report.unready, report.versions_learned
    );

    // Incremental sync: only new timeline, no old state (spec behaviour for an
    // already-joined room).
    let mut join = serde_json::Map::new();
    join.insert(
        ROOM.to_owned(),
        json!({
            "timeline": { "events": [ciphertext("$live", 500)], "limited": false },
            "state": { "events": [] }
        }),
    );
    server.state.push_sync(MockResponse::json(
        json!({ "next_batch": "s6", "rooms": { "join": Value::Object(join) } }),
    ));
    // Old history behind the seeded cursor.
    server.state.push_messages(
        "s5",
        MockResponse::json(json!({ "start": "s5", "end": "s4", "chunk": [hist] })),
    );

    let engine = mainlinenerd_ingest::engine::Engine::new(
        transport,
        store,
        mainlinenerd_ingest::engine::EngineConfig {
            sync_timeout_ms: 1_000,
            history_limit: 10,
            max_pages_per_room: 4,
        },
    );
    let (tx, rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(runtime::run(engine, RunSettings::run(1), async move {
        let _ = rx.await;
    }));
    let probe = format!("SELECT COUNT(*) FROM events WHERE event_id = '{hist_id}'");
    for _ in 0..150 {
        if scalar_i64(&db(&cfg.database), &probe) == 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let _ = tx.send(());
    let summary = handle.await.unwrap();
    eprintln!("F74 run result ok={}", summary.is_ok());

    let conn = db(&cfg.database);
    let requests = server.state.recorded();
    Outcome {
        encrypted: scalar_i64(
            &conn,
            &format!("SELECT encrypted FROM rooms WHERE room_id = '{ROOM}'"),
        ),
        successor: scalar_string(
            &conn,
            &format!("SELECT successor_room_id FROM rooms WHERE room_id = '{ROOM}'"),
        ),
        hist_archived: scalar_i64(&conn, &probe),
        messages_requests: requests
            .iter()
            .filter(|r| r.path.ends_with("/messages"))
            .count(),
        state_requests: requests
            .iter()
            .filter(|r| r.path.contains("/state"))
            .map(|r| r.path.clone())
            .collect(),
    }
}

#[tokio::test]
async fn f74_encrypted_room_admitted_after_first_sync_is_flagged_and_not_backfilled() {
    let hist = ciphertext("$cipher-hist", 50);
    let out = admit_after_first_sync(
        "m.room.encryption",
        json!({ "algorithm": "m.megolm.v1.aes-sha2" }),
        hist,
    )
    .await;
    eprintln!(
        "F74 encrypted: rooms.encrypted={} hist_archived={} messages_requests={} state_requests={:?}",
        out.encrypted, out.hist_archived, out.messages_requests, out.state_requests
    );
    assert_eq!(out.encrypted, 1, "encrypted room was never flagged");
    assert_eq!(
        out.messages_requests, 0,
        "base backfill paged an encrypted room's history"
    );
    assert_eq!(out.hist_archived, 0, "historical ciphertext envelope archived");
}

#[tokio::test]
async fn f74_tombstoned_room_admitted_after_first_sync_is_flagged_and_not_backfilled() {
    let hist = json!({
        "type": "m.room.message",
        "event_id": "$old-hist",
        "sender": "@alice:hs.example.org",
        "origin_server_ts": 50,
        "content": { "msgtype": "m.text", "body": "old" }
    });
    let out = admit_after_first_sync(
        "m.room.tombstone",
        json!({ "body": "upgraded", "replacement_room": "!new:hs.example.org" }),
        hist,
    )
    .await;
    eprintln!(
        "F74 tombstone: successor={:?} hist_archived={} messages_requests={} state_requests={:?}",
        out.successor, out.hist_archived, out.messages_requests, out.state_requests
    );
    assert_eq!(
        out.successor.as_deref(),
        Some("!new:hs.example.org"),
        "tombstoned room's successor was never learned"
    );
    assert_eq!(
        out.messages_requests, 0,
        "base backfill paged an upgraded room's history"
    );
}
