//! Repro for review finding F15: a room that drops out of sync scope for a run
//! gets a permanent, unrecorded hole because every gap lower bound is the single
//! global `since` token.
//!
//! Drives the real `runtime::initialize_with_policy` + real `MatrixTransport`
//! against the loopback mock homeserver, across three "runs" (restarts):
//!   1. `#eng` (-> ROOM) and ROOM2 are ready; ROOM goes live and its base
//!      history completes. Global token s1.
//!   2. `#eng` fails to resolve (404) for the whole run: ROOM is unready, not
//!      allowed, not in the sync filter. Two batches for ROOM2 commit s2, s3.
//!      Anything said in ROOM during this run is after s1 and before s3.
//!   3. `#eng` resolves again. The first incremental sync from s3 carries one
//!      new ROOM event, not limited (as a real server would return it: only
//!      events after `since`).
//!
//! Correct behaviour (docs/architecture.md: "never claims coverage"; a gap is
//! "bounded" by the last committed token for that room): after run 3 ROOM must
//! either have a repair job covering (s1, s3] or not report complete coverage.
//! The explicit recovery control (`retry_stalled_configured`) is also checked.

mod common;

use common::*;
use mainlinenerd_ingest::config::{Config, RoomConfig, RoomSelector, Token};
use mainlinenerd_ingest::engine::{SyncRequest, Transport};
use mainlinenerd_ingest::matrix::MatrixTransport;
use mainlinenerd_ingest::runtime::{self, StartupPolicy};
use mainlinenerd_ingest::store::{ArchiveIdentity, HistoryStatus, HistoryWork, Store};
use serde_json::{json, Value};

const ALIAS: &str = "#eng:hs.example.org";

fn tiny_policy() -> StartupPolicy {
    StartupPolicy {
        max_attempts: 3,
        transient_min: std::time::Duration::from_millis(1),
        transient_max: std::time::Duration::from_millis(2),
        rate_limit_fallback: std::time::Duration::from_millis(1),
    }
}

fn room_id(value: &str) -> RoomConfig {
    RoomConfig {
        selector: RoomSelector::Id(value.to_owned()),
        join: false,
        via: Vec::new(),
    }
}

fn alias(value: &str) -> RoomConfig {
    RoomConfig {
        selector: RoomSelector::Alias(value.to_owned()),
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

async fn adapter(cfg: &Config) -> MatrixTransport {
    MatrixTransport::connect(cfg, &Token::new("fake-token"))
        .await
        .expect("connect adapter")
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

fn create_event(id: &str, version: &str) -> Value {
    json!({
        "type": "m.room.create",
        "event_id": id,
        "sender": "@alice:hs.example.org",
        "state_key": "",
        "origin_server_ts": 1,
        "content": { "creator": "@alice:hs.example.org", "room_version": version }
    })
}

fn joined(events: Vec<Value>, prev_batch: &str, limited: bool) -> Value {
    json!({
        "timeline": { "events": events, "prev_batch": prev_batch, "limited": limited },
        "state": { "events": [] }
    })
}

fn sync_body(next_batch: &str, join: serde_json::Map<String, Value>) -> Value {
    json!({ "next_batch": next_batch, "rooms": { "join": Value::Object(join) } })
}

async fn sync_and_apply(transport: &MatrixTransport, store: &mut Store, at: i64) {
    let since = store.since_token().unwrap();
    let batch = transport
        .sync(SyncRequest {
            since,
            timeout_ms: 1_000,
        })
        .await
        .unwrap();
    store.apply_sync_batch(&batch, at).unwrap();
}

#[tokio::test]
async fn f15_room_out_of_scope_for_a_run_leaves_a_recorded_gap() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(&server, dir.path(), vec![alias(ALIAS), room_id(ROOM2)]);
    server.state.set_alias(ALIAS, ROOM);
    server
        .state
        .set_create_state(ROOM, MockResponse::json(create_event("$createA", "11")));
    server
        .state
        .set_create_state(ROOM2, MockResponse::json(create_event("$createB", "11")));
    let mut store = Store::open(&cfg.database, &identity(&cfg)).unwrap();

    // ---- Run 1: both rooms ready; ROOM goes live and its base completes. ----
    let t1 = adapter(&cfg).await;
    let report = runtime::initialize_with_policy(&t1, &cfg, &mut store, &tiny_policy())
        .await
        .unwrap();
    assert!(report.rooms.iter().any(|r| r == ROOM), "{report:?}");
    assert!(report.rooms.iter().any(|r| r == ROOM2), "{report:?}");
    let mut join = serde_json::Map::new();
    join.insert(ROOM.to_owned(), joined(vec![msg("$a1", "a1")], "pA0", false));
    join.insert(ROOM2.to_owned(), joined(vec![msg("$b1", "b1")], "pB0", false));
    server
        .state
        .push_sync(MockResponse::json(sync_body("s1", join)));
    sync_and_apply(&t1, &mut store, 10).await;
    assert_eq!(store.since_token().unwrap().as_deref(), Some("s1"));
    let done = store
        .apply_history_page(ROOM, HistoryWork::Base, "pA0", &history_page("pA0", None, vec![]), 15)
        .unwrap();
    assert_eq!(done.status, Some(HistoryStatus::Completed));
    assert!(store.room_history_complete(ROOM).unwrap());

    // ---- Run 2 (restart): the alias 404s for the whole run. ----
    server.state.aliases.lock().unwrap().remove(ALIAS);
    let t2 = adapter(&cfg).await;
    let report = runtime::initialize_with_policy(&t2, &cfg, &mut store, &tiny_policy())
        .await
        .unwrap();
    assert!(
        report.unready.iter().any(|(r, _)| r == ALIAS),
        "alias must be unready this run: {report:?}"
    );
    assert!(
        !t2.is_allowed_room(ROOM),
        "the pinned room is out of scope for this run"
    );
    assert!(!store.room_configured(ROOM).unwrap());
    // ROOM is not in the server-side filter, so a real server returns only
    // ROOM2; meanwhile people are talking in ROOM ($a-missed-*), which the
    // archive never sees.
    for (token, id) in [("s2", "$b2"), ("s3", "$b3")] {
        let mut join = serde_json::Map::new();
        join.insert(ROOM2.to_owned(), joined(vec![msg(id, id)], "pBx", false));
        server
            .state
            .push_sync(MockResponse::json(sync_body(token, join)));
        sync_and_apply(&t2, &mut store, 20).await;
    }
    assert_eq!(store.since_token().unwrap().as_deref(), Some("s3"));

    // ---- Run 3 (restart): the alias resolves again. ----
    server.state.set_alias(ALIAS, ROOM);
    let t3 = adapter(&cfg).await;
    let report = runtime::initialize_with_policy(&t3, &cfg, &mut store, &tiny_policy())
        .await
        .unwrap();
    assert!(report.rooms.iter().any(|r| r == ROOM), "{report:?}");
    assert!(t3.is_allowed_room(ROOM));
    // Incremental sync from s3: only ROOM events after s3; not limited.
    let mut join = serde_json::Map::new();
    join.insert(ROOM.to_owned(), joined(vec![msg("$a-after", "after")], "pA3", false));
    server
        .state
        .push_sync(MockResponse::json(sync_body("s4", join)));
    sync_and_apply(&t3, &mut store, 30).await;

    // The explicit operator recovery control.
    let retried = store.retry_stalled_configured(40).unwrap();

    let status = store.status().unwrap();
    let room = status
        .rooms
        .iter()
        .find(|r| r.room_id == ROOM)
        .expect("ROOM in status")
        .clone();
    let gaps_for_room: Vec<_> = store
        .open_gap_positions()
        .unwrap()
        .into_iter()
        .filter(|g| g.room_id == ROOM)
        .collect();
    let conn = db(&cfg.database);
    let gap_rows = scalar_i64(
        &conn,
        "SELECT COUNT(*) FROM gap_jobs WHERE room_id = '!room:hs.example.org'",
    );
    eprintln!(
        "F15 DIAG: configured={} history_complete={} history_token_set={} open_gaps={} \
         unresolved_gaps={} gap_jobs_rows={} open_gap_positions={:?} \
         rooms_needing_history={} retry_stalled={:?} since={:?}",
        room.configured,
        room.history_complete,
        room.history_token_set,
        room.open_gaps,
        room.unresolved_gaps,
        gap_rows,
        gaps_for_room,
        store.rooms_needing_history().unwrap().len(),
        retried,
        store.since_token().unwrap(),
    );

    // Correct behaviour: the (s1, s3] interval in which ROOM was out of the
    // sync filter is either queued for repair or at least recorded as an
    // unresolved hole; the archive must not claim complete coverage.
    assert!(
        room.open_gaps + room.unresolved_gaps > 0 || !room.history_complete,
        "ROOM was out of sync scope while the global token advanced s1 -> s3, \
         yet status claims history_complete={} with open_gaps={} unresolved_gaps={} \
         (gap_jobs rows for ROOM: {}); nothing will ever fetch the missed interval",
        room.history_complete,
        room.open_gaps,
        room.unresolved_gaps,
        gap_rows,
    );
}
