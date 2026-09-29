//! F43 reproducer: a room whose startup readiness check hit only transient
//! 5xx failures is never re-checked during the run, so once the homeserver
//! recovers the room still stays out of /sync and history until a restart.

mod common;

use std::time::Duration;

use common::*;
use mainlinenerd_ingest::config::{Config, RoomConfig, RoomSelector, Token};
use mainlinenerd_ingest::engine::{Engine, EngineConfig, Transport};
use mainlinenerd_ingest::matrix::MatrixTransport;
use mainlinenerd_ingest::runtime::{self, RunSettings, StartupPolicy};
use mainlinenerd_ingest::store::{ArchiveIdentity, Store};
use serde_json::{json, Value};

fn room_id(value: &str) -> RoomConfig {
    RoomConfig {
        selector: RoomSelector::Id(value.to_owned()),
        join: false,
        via: Vec::new(),
    }
}

fn config(server: &MockServer, dir: &std::path::Path, rooms: Vec<RoomConfig>) -> Config {
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
        rooms,
    }
}

fn identity(cfg: &Config) -> ArchiveIdentity {
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

fn joined(events: Vec<Value>) -> Value {
    json!({ "timeline": { "events": events, "limited": false }, "state": { "events": [] } })
}

fn count(cfg: &Config, event_id: &str) -> i64 {
    scalar_i64(
        &db(&cfg.database),
        &format!("SELECT COUNT(*) FROM events WHERE event_id = '{event_id}'"),
    )
}

#[tokio::test]
async fn f43_transiently_unready_room_recovers_during_the_run() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(&server, dir.path(), vec![room_id(ROOM), room_id(ROOM2)]);

    // Default startup policy shape (3 attempts), shrunk delays. ROOM's create
    // fetch sees a 502 blip for exactly those 3 attempts; afterwards the mock
    // answers every create fetch with a valid v11 (the server has recovered).
    let policy = StartupPolicy {
        max_attempts: 3,
        transient_min: Duration::from_millis(1),
        transient_max: Duration::from_millis(2),
        rate_limit_fallback: Duration::from_millis(1),
    };
    for _ in 0..3 {
        server.state.push_create_state(
            ROOM,
            MockResponse::status(502, json!({ "errcode": "M_UNKNOWN", "error": "bad gateway" })),
        );
    }

    let transport = MatrixTransport::connect(&cfg, &Token::new("fake-token"))
        .await
        .expect("connect adapter");
    let mut store = Store::open(&cfg.database, &identity(&cfg)).unwrap();
    let report = runtime::initialize_with_policy(&transport, &cfg, &mut store, &policy)
        .await
        .unwrap();
    // Precondition: the blip made ROOM unready, ROOM2 is healthy.
    assert!(
        report.unready.iter().any(|(room, _)| room == ROOM),
        "precondition: ROOM unready after 3x 502: {report:?}"
    );
    assert!(report.rooms.iter().any(|room| room == ROOM2), "{report:?}");
    let requests_after_init = server.state.recorded().len();

    // Healthy server from here on. The first live sync carries only ROOM2.
    let mut first = serde_json::Map::new();
    first.insert(ROOM2.to_owned(), joined(vec![msg("$healthy-1", "ok")]));
    server.state.push_sync(MockResponse::json(json!({
        "next_batch": "s1", "rooms": { "join": Value::Object(first) }
    })));

    let observer = transport.clone();
    let engine = Engine::new(
        transport,
        store,
        EngineConfig {
            sync_timeout_ms: 1_000,
            history_limit: 10,
            max_pages_per_room: 4,
        },
    );
    let (tx, rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(runtime::run(engine, RunSettings::run(1), async move {
        let _ = rx.await;
    }));

    // Give the runtime several seconds of a fully healthy homeserver.
    tokio::time::sleep(Duration::from_secs(4)).await;

    // A later live batch carries a message in ROOM (server ignores the filter,
    // as the deferred-room adapter test also models).
    let mut later = serde_json::Map::new();
    later.insert(ROOM.to_owned(), joined(vec![msg("$late-room1", "after recovery")]));
    later.insert(ROOM2.to_owned(), joined(vec![msg("$healthy-2", "ok")]));
    server.state.push_sync(MockResponse::json(json!({
        "next_batch": "s2", "rooms": { "join": Value::Object(later) }
    })));
    for _ in 0..300 {
        if count(&cfg, "$healthy-2") == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let _ = tx.send(());
    let summary = handle.await.unwrap().unwrap();

    let post_init = server.state.recorded()[requests_after_init..].to_vec();
    let create_refetches = post_init
        .iter()
        .filter(|r| r.path.contains(&ROOM.replace('!', "%21")) || r.path.contains(ROOM))
        .filter(|r| r.path.contains("m.room.create"))
        .count();
    let syncs = post_init.iter().filter(|r| r.path.ends_with("/sync")).count();
    let last_sync_filter = post_init
        .iter()
        .rev()
        .find(|r| r.path.ends_with("/sync"))
        .map(|r| r.query.clone())
        .unwrap_or_default();
    let healthy2 = count(&cfg, "$healthy-2");
    let late = count(&cfg, "$late-room1");
    let allowed = observer.is_allowed_room(ROOM);
    eprintln!(
        "F43 diag: summary={summary:?} syncs_after_init={syncs} \
         room1_create_refetches_after_init={create_refetches} \
         room1_allowed={allowed} healthy2_archived={healthy2} room1_late_archived={late} \
         last_sync_query_mentions_room1={}",
        last_sync_filter.contains("21room%3A") || last_sync_filter.contains("!room:")
    );

    assert_eq!(healthy2, 1, "harness sanity: the second live batch was applied");
    // Correct behaviour: a room that was unready only because of a transient
    // startup blip is re-checked once the homeserver is healthy, admitted, and
    // its later live data is archived without an operator restart.
    assert!(
        allowed && late == 1,
        "F43: ROOM stayed excluded for the whole run after a transient startup \
         failure (allowed={allowed}, late_archived={late}, create re-fetches={create_refetches})"
    );
}
