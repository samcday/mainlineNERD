//! F51 reproducer: a `join = false`, never-joined but world-readable room is
//! reported "ready", backfilled once to `complete`, and then silently never
//! receives anything again, while `status` gives no hint that it is not
//! followed.
//!
//! The loopback mock's default `/sync` response lists no rooms, which is what a
//! real homeserver returns for a room the user is not a member of (Client-Server
//! `/sync` has only join/invite/leave/knock sections; no peeking).

mod common;

use common::*;
use mainlinenerd_ingest::config::{Config, RoomConfig, RoomSelector, Token};
use mainlinenerd_ingest::engine::{Engine, EngineConfig};
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

fn cfg_for(server: &MockServer, dir: &std::path::Path) -> Config {
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
            // The config.example.toml default: no explicit join.
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

fn count(cfg: &Config, id: &str) -> i64 {
    scalar_i64(
        &db(&cfg.database),
        &format!("SELECT COUNT(*) FROM events WHERE event_id = '{id}'"),
    )
}

async fn run_for(transport: MatrixTransport, store: Store, cfg: &Config, until: impl Fn() -> bool, max_ms: u64) {
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
    let _ = cfg;
    for _ in 0..(max_ms / 10) {
        if until() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let _ = tx.send(());
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn f51_unjoined_world_readable_room_is_not_silently_reported_as_followed() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = cfg_for(&server, dir.path());
    // World-readable: create state is served to a non-member.
    server
        .state
        .set_create_state(ROOM, MockResponse::json(json!({ "room_version": "11" })));

    let mut store = Store::open(&cfg.database, &ident(&cfg)).unwrap();
    store.apply_sync_batch(&sync_batch("s5", vec![]), 10).unwrap();

    // ---- First run: startup + one-shot backfill.
    let transport = MatrixTransport::connect(&cfg, &Token::new("fake-token"))
        .await
        .unwrap();
    let report = runtime::initialize_with_policy(&transport, &cfg, &mut store, &tiny_policy())
        .await
        .unwrap();
    eprintln!(
        "F51 run1 report: ready={:?} joined={:?} unready={:?} own_membership={:?}",
        report.rooms,
        report.joined,
        report.unready,
        store.own_membership(ROOM).unwrap()
    );
    // Existing history is readable (world_readable) and ends: no `end`.
    server.state.push_messages(
        "s5",
        MockResponse::json(json!({ "start": "s5", "chunk": [msg("$hist", "history")] })),
    );
    {
        let c = cfg.clone();
        run_for(transport, store, &cfg, move || count(&c, "$hist") == 1, 2_000).await;
    }
    let store = Store::open(&cfg.database, &ident(&cfg)).unwrap();
    eprintln!(
        "F51 run1 after: $hist={} complete={} own_membership={:?}",
        count(&cfg, "$hist"),
        store.room_history_complete(ROOM).unwrap(),
        store.own_membership(ROOM).unwrap()
    );
    drop(store);

    // ---- Someone posts in the room after the first run. A real homeserver
    // never lists a non-member room in /sync (mock default: empty rooms), so
    // the only way to see it would be another /messages call. Script it for
    // every token the runtime could plausibly use.
    for token in ["s5", "empty"] {
        server.state.push_messages(
            token,
            MockResponse::json(json!({ "start": token, "chunk": [msg("$later", "posted later")] })),
        );
    }

    // ---- Restart (the documented recovery path), run for a while.
    let mut store = Store::open(&cfg.database, &ident(&cfg)).unwrap();
    let transport = MatrixTransport::connect(&cfg, &Token::new("fake-token"))
        .await
        .unwrap();
    let report2 = runtime::initialize_with_policy(&transport, &cfg, &mut store, &tiny_policy())
        .await
        .unwrap();
    eprintln!(
        "F51 run2 report: ready={:?} joined={:?} unready={:?}",
        report2.rooms, report2.joined, report2.unready
    );
    {
        let c = cfg.clone();
        run_for(transport, store, &cfg, move || count(&c, "$later") == 1, 1_500).await;
    }

    let store = Store::open(&cfg.database, &ident(&cfg)).unwrap();
    let status = store.render_status().unwrap();
    let room_line = status
        .lines()
        .find(|line| line.starts_with(&format!("room {ROOM} ")))
        .unwrap_or("<no room line>")
        .to_owned();
    let later = count(&cfg, "$later");
    let messages_calls = server
        .state
        .recorded()
        .iter()
        .filter(|r| r.path.contains("/messages"))
        .count();
    let syncs = server
        .state
        .recorded()
        .iter()
        .filter(|r| r.path.ends_with("/sync"))
        .count();
    eprintln!("F51 status line: {room_line}");
    eprintln!(
        "F51 after restart: $later={later} /messages calls total={messages_calls} /sync calls={syncs} own_membership={:?} complete={}",
        store.own_membership(ROOM).unwrap(),
        store.room_history_complete(ROOM).unwrap()
    );

    // Correct behaviour: a room reported "ready" is either actually followed
    // (later events arrive), or status honestly says it is not joined /
    // history-only. Neither holds.
    let flagged = room_line.contains("membership")
        || room_line.contains("not-joined")
        || room_line.contains("history-only")
        || room_line.contains("not-ready");
    assert!(
        later == 1 || flagged,
        "F51: never-joined room was reported ready {:?}, marked history complete, \
         and after restart the later event was not archived ($later={later}) while \
         status gives no not-joined indication: {room_line}",
        report2.rooms
    );
}
