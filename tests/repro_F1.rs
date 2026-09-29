//! F1 reproduction: one malformed timeline event in one room of a /sync batch
//! must not wedge live ingestion for every other allowlisted room across
//! restarts. Correct behaviour asserted: after the bad batch (and at most one
//! restart that re-fetches the same `since`), the healthy room's live event is
//! archived and the run is still alive.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::*;
use mainlinenerd_ingest::engine::{
    Engine, EngineConfig, HistoryRequest, SyncRequest, Transport, TransportError,
};
use mainlinenerd_ingest::event::{HistoryPage, SyncBatch};
use mainlinenerd_ingest::runtime::{self, RunSettings};
use serde_json::json;
use tokio::sync::oneshot;

#[derive(Clone)]
struct RecordingSince {
    inner: ControllableTransport,
    since: Arc<Mutex<Vec<Option<String>>>>,
}

#[async_trait::async_trait]
impl Transport for RecordingSince {
    async fn sync(&self, request: SyncRequest) -> Result<SyncBatch, TransportError> {
        self.since.lock().unwrap().push(request.since.clone());
        self.inner.sync(request).await
    }
    async fn history(&self, request: HistoryRequest) -> Result<HistoryPage, TransportError> {
        self.inner.history(request).await
    }
}

fn engine_config() -> EngineConfig {
    EngineConfig {
        sync_timeout_ms: 30_000,
        history_limit: 50,
        max_pages_per_room: 64,
    }
}

/// The batch a homeserver keeps returning for `since = s2`: a healthy live
/// message in ROOM and, in ROOM2, a timeline event with a string `type` but no
/// `event_id` (select_events in matrix.rs keeps it: it only drops non-string
/// `type`).
fn poisoned_batch() -> SyncBatch {
    let mut healthy = room_update(ROOM);
    healthy.timeline = vec![message("$healthy", 500, "healthy live message")];
    let mut bad = room_update(ROOM2);
    bad.timeline = vec![json!({
        "type": "m.room.message",
        "sender": ALICE,
        "origin_server_ts": 501,
        "content": { "msgtype": "m.text", "body": "no event id" }
    })];
    sync_batch("s3", vec![healthy, bad])
}

/// One daemon lifetime: open the archive, feed the server's response for the
/// current `since`, then let /sync park. Returns (run result, since tokens
/// requested).
async fn one_lifetime(
    path: &std::path::Path,
) -> (
    Result<runtime::RunSummary, runtime::RuntimeError>,
    Vec<Option<String>>,
) {
    let store = open_store(path);
    let inner = ControllableTransport::new();
    inner.hold_sync(true);
    inner.hold_history(true);
    // A conformant replay: the server answers the same `since` with the same
    // batch.
    inner.push_sync(poisoned_batch());
    let transport = RecordingSince {
        inner: inner.clone(),
        since: Arc::new(Mutex::new(Vec::new())),
    };
    let engine = Engine::new(transport.clone(), store, engine_config());
    let (tx, rx) = oneshot::channel::<()>();
    let handle = tokio::spawn(async move {
        runtime::run(engine, RunSettings::run(1_000), async move {
            let _ = rx.await;
        })
        .await
    });
    // A live daemon issues a second /sync (parked) after the first batch; a
    // dead one never does. Paused time makes the timeout instant.
    let _ = tokio::time::timeout(Duration::from_secs(120), inner.wait_for_syncs(2)).await;
    let _ = tx.send(());
    let result = handle.await.unwrap();
    let since = transport.since.lock().unwrap().clone();
    (result, since)
}

/// Reachability, not a correctness assertion: the REAL MatrixTransport (matrix-sdk
/// Client::send against the loopback mock) delivers a timeline event with no
/// `event_id` to the store, and the store rejects the whole batch. This test
/// PASSING means the trigger is reachable through the production adapter.
#[tokio::test]
async fn f1_reachability_real_adapter_passes_event_without_event_id() {
    use mainlinenerd_ingest::config::{Config, RoomConfig, RoomSelector, Token};
    use mainlinenerd_ingest::matrix::MatrixTransport;
    use mainlinenerd_ingest::store::{ArchiveIdentity, Store, StoreError};
    use serde_json::Value;

    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let room = |id: &str| RoomConfig {
        selector: RoomSelector::Id(id.to_owned()),
        join: false,
        via: Vec::new(),
    };
    let cfg = Config {
        homeserver: server.base_url.clone(),
        user_id: "@ingest:hs.example.org".to_owned(),
        device_id: "MLN".to_owned(),
        token_env: "MLN_UNUSED".to_owned(),
        data_dir: dir.path().to_path_buf(),
        database: dir.path().join("archive.sqlite3"),
        history_interval_ms: 1_000,
        history_limit: 50,
        sync_timeout_ms: 2_000,
        rooms: vec![room(ROOM), room(ROOM2)],
    };
    let transport = MatrixTransport::connect(&cfg, &Token::new("fake-token"))
        .await
        .expect("connect adapter");
    transport.allow_room(ROOM.parse().unwrap());
    transport.allow_room(ROOM2.parse().unwrap());

    let mut join = serde_json::Map::new();
    join.insert(
        ROOM.to_owned(),
        json!({ "timeline": { "events": [message("$healthy", 100, "ok")], "limited": false },
                "state": { "events": [] } }),
    );
    join.insert(
        ROOM2.to_owned(),
        json!({ "timeline": { "events": [{
                    "type": "m.room.message",
                    "sender": ALICE,
                    "origin_server_ts": 101,
                    "content": { "msgtype": "m.text", "body": "no event id" }
                }], "limited": false },
                "state": { "events": [] } }),
    );
    server.state.push_sync(MockResponse::json(
        json!({ "next_batch": "s1", "rooms": { "join": Value::Object(join) } }),
    ));

    let batch = transport
        .sync(SyncRequest {
            since: None,
            timeout_ms: 1_000,
        })
        .await
        .expect("the SDK accepts a timeline event without event_id");
    let bad = batch
        .rooms
        .iter()
        .find(|r| r.room_id == ROOM2)
        .expect("ROOM2 delivered");
    eprintln!("F1 ADAPTER: ROOM2 timeline delivered = {:?}", bad.timeline);
    assert_eq!(bad.timeline.len(), 1, "select_events kept the event");
    assert!(bad.timeline[0].get("event_id").is_none());

    let mut store = Store::open(
        &cfg.database,
        &ArchiveIdentity {
            homeserver: cfg.homeserver.clone(),
            user_id: cfg.user_id.clone(),
            device_id: cfg.device_id.clone(),
        },
    )
    .unwrap();
    let err = store.apply_sync_batch(&batch, 10).unwrap_err();
    eprintln!("F1 ADAPTER: apply_sync_batch error = {err:?}");
    assert!(matches!(err, StoreError::MalformedEvent { .. }), "{err:?}");
    assert_eq!(
        scalar_i64(
            &db(&cfg.database),
            "SELECT COUNT(*) FROM events WHERE event_id = '$healthy'"
        ),
        0,
        "the healthy room's event was rolled back with the bad one"
    );
}

#[tokio::test(start_paused = true)]
async fn f1_one_malformed_sync_event_must_not_wedge_live_ingest_for_all_rooms() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    {
        let mut store = open_store(&path);
        // Two healthy, ready rooms; the committed global token is s2.
        for (room, token, live, next) in [(ROOM, "p1", "$a", "s1"), (ROOM2, "q1", "$b", "s2")] {
            store.register_configured_room(room, None, 10).unwrap();
            let mut update = room_update(room);
            update.timeline = vec![message(live, 100, "live")];
            update.state = vec![create_room("11")];
            update.prev_batch = Some(token.to_owned());
            store
                .apply_sync_batch(&sync_batch(next, vec![update]), 10)
                .unwrap();
        }
    }

    // Lifetime 1 and a supervisor restart (lifetime 2).
    let (first, first_since) = one_lifetime(&path).await;
    let (second, second_since) = one_lifetime(&path).await;

    let conn = db(&path);
    let healthy = scalar_i64(
        &conn,
        "SELECT COUNT(*) FROM events WHERE event_id = '$healthy'",
    );
    let since_token = scalar_string(&conn, "SELECT since_token FROM sync_progress WHERE id = 1");
    drop(conn);

    let observed = format!(
        "\nlifetime 1: result={first:?} since_requested={first_since:?}\
         \nlifetime 2: result={second:?} since_requested={second_since:?}\
         \nhealthy room event archived rows={healthy} committed since_token={since_token:?}\n"
    );
    eprintln!("F1 OBSERVED:{observed}");

    assert!(
        first.is_ok() || second.is_ok(),
        "a single malformed event in ROOM2 killed the daemon on every restart:{observed}"
    );
    assert_eq!(
        healthy, 1,
        "the healthy room's live event was never archived:{observed}"
    );
}
