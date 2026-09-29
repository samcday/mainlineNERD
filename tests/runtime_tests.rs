//! Concurrent scheduler tests: a held long-poll must not block backfill, an
//! active backfill must not block a new live batch, pacing and backoff are
//! enforced, failures stay isolated, and shutdown leaves a committed
//! checkpoint. Tokio paused time keeps these deterministic and instant.

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common::*;
use mainlinenerd_ingest::engine::{Engine, EngineConfig, EngineError, TransportError};
use mainlinenerd_ingest::runtime::{self, RunSettings};
use mainlinenerd_ingest::store::{HistoryStatus, Store};
use tokio::sync::oneshot;

fn engine_config() -> EngineConfig {
    EngineConfig {
        sync_timeout_ms: 30_000,
        history_limit: 50,
        max_pages_per_room: 64,
    }
}

fn spawn_run(
    transport: &ControllableTransport,
    store: Store,
    settings: RunSettings,
) -> (
    oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<runtime::RunSummary, runtime::RuntimeError>>,
) {
    let engine = Engine::new(transport.clone(), store, engine_config());
    let (tx, rx) = oneshot::channel();
    let handle = tokio::spawn(async move {
        runtime::run(engine, settings, async move {
            let _ = rx.await;
        })
        .await
    });
    (tx, handle)
}

async fn wait_until(label: &str, mut check: impl FnMut() -> bool) {
    for _ in 0..400 {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {label}");
}

/// Seed one ready room's base history cursor directly, as an initial sync
/// would: configured, version known from create state, and a base cursor.
fn seed_room(store: &mut Store, room: &str, token: &str, live_id: &str, next_batch: &str) {
    store.register_configured_room(room, None, 10).unwrap();
    let mut update = room_update(room);
    update.timeline = vec![message(live_id, 100, "live")];
    update.state = vec![create_room("11")];
    update.prev_batch = Some(token.to_owned());
    store
        .apply_sync_batch(&sync_batch(next_batch, vec![update]), 10)
        .expect("seed sync batch");
}

#[tokio::test(start_paused = true)]
async fn held_sync_does_not_block_backfill() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);
    seed_room(&mut store, ROOM, "p1", "$live", "s0");

    let transport = ControllableTransport::new();
    transport.hold_sync(true);
    transport.push_history(
        "p1",
        history_page("p1", None, vec![message("$old", 50, "old")]),
    );

    let (tx, handle) = spawn_run(&transport, store, RunSettings::run(1_000));
    transport.wait_for_history(1).await;
    transport.wait_for_syncs(1).await;

    // The backfill committed while the long poll is still held open.
    let conn = db(&path);
    assert_eq!(
        scalar_i64(&conn, "SELECT COUNT(*) FROM events WHERE event_id = '$old'"),
        1,
        "backfill must progress while /sync is held"
    );
    assert!(transport.sync_call_count() >= 1);
    drop(conn);

    let _ = tx.send(());
    let summary = handle.await.unwrap().unwrap();
    assert!(summary.history_pages >= 1);
}

#[tokio::test(start_paused = true)]
async fn active_backfill_does_not_block_a_new_live_batch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);
    seed_room(&mut store, ROOM, "p1", "$a", "s1");

    let transport = ControllableTransport::new();
    transport.hold_history(true);
    transport.push_history(
        "p1",
        history_page("p1", None, vec![message("$old", 50, "old")]),
    );

    let (tx, handle) = spawn_run(&transport, store, RunSettings::run(1_000));
    transport.wait_for_history(1).await;

    // A new live batch arrives while the backfill request is held.
    let mut live = room_update(ROOM);
    live.timeline = vec![message("$live2", 300, "live2")];
    transport.push_sync(sync_batch("s2", vec![live]));

    wait_until("the new live batch to commit", || {
        let conn = db(&path);
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM events WHERE event_id = '$live2'",
        ) == 1
    })
    .await;
    assert_eq!(transport.history_call_count(), 1, "backfill is still held");

    let _ = tx.send(());
    let summary = handle.await.unwrap().unwrap();
    assert_eq!(summary.sync_batches, 1, "the new live batch was applied");
    assert_eq!(
        scalar_string(
            &db(&path),
            "SELECT since_token FROM sync_progress WHERE id = 1"
        ),
        Some("s2".to_owned()),
        "the live checkpoint advanced while backfill was held"
    );
}

#[tokio::test(start_paused = true)]
async fn history_is_paced_and_fair_across_two_rooms() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);
    seed_room(&mut store, ROOM, "p1", "$a", "s1");
    seed_room(&mut store, ROOM2, "q1", "$b", "s2");

    let transport = ControllableTransport::new();
    transport.hold_sync(true);
    transport.push_history(
        "p1",
        history_page("p1", Some("p2"), vec![message("$a-old", 40, "a")]),
    );
    transport.push_history(
        "p2",
        history_page("p2", None, vec![message("$a-older", 30, "a")]),
    );
    transport.push_history(
        "q1",
        history_page("q1", Some("q2"), vec![message("$b-old", 20, "b")]),
    );
    transport.push_history(
        "q2",
        history_page("q2", None, vec![message("$b-older", 10, "b")]),
    );

    let (tx, handle) = spawn_run(&transport, store, RunSettings::run(1_000));
    transport.wait_for_history(4).await;

    let requests = transport.history_requests();
    assert_eq!(requests.len(), 4);
    // Round robin: consecutive pages come from different rooms.
    for pair in requests.windows(2) {
        assert_ne!(
            pair[0].room_id, pair[1].room_id,
            "work must be fair across rooms: {requests:?}"
        );
    }

    // One request per interval, globally.
    let times = transport.history_request_times();
    for pair in times.windows(2) {
        assert!(
            pair[1] - pair[0] >= Duration::from_millis(1_000),
            "history requests must be paced: {times:?}"
        );
    }

    let _ = tx.send(());
    handle.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn live_batches_do_not_shorten_the_history_interval() {
    use mainlinenerd_ingest::engine::{HistoryRequest, SyncRequest, Transport};
    use mainlinenerd_ingest::event::{HistoryPage, SyncBatch};

    #[derive(Clone)]
    struct FrequentSync(ControllableTransport);

    #[async_trait::async_trait]
    impl Transport for FrequentSync {
        async fn sync(&self, request: SyncRequest) -> Result<SyncBatch, TransportError> {
            tokio::time::sleep(Duration::from_millis(100)).await;
            self.0.sync(request).await
        }

        async fn history(&self, request: HistoryRequest) -> Result<HistoryPage, TransportError> {
            self.0.history(request).await
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);
    seed_room(&mut store, ROOM, "p0", "$seed", "s0");
    let transport = ControllableTransport::new();
    transport.hold_sync(true);

    for index in 0..4 {
        let from = format!("p{index}");
        let end = (index < 3).then(|| format!("p{}", index + 1));
        transport.push_history(&from, history_page(&from, end.as_deref(), vec![]));
    }
    for index in 1..=40 {
        let mut update = room_update(ROOM);
        update.timeline = vec![message(&format!("$live-{index}"), index, "live")];
        transport.push_sync(sync_batch(&format!("s{index}"), vec![update]));
    }

    let engine = Engine::new(FrequentSync(transport.clone()), store, engine_config());
    let (tx, rx) = oneshot::channel();
    let handle = tokio::spawn(runtime::run(engine, RunSettings::run(1_000), async move {
        let _ = rx.await;
    }));
    tokio::time::timeout(Duration::from_secs(10), transport.wait_for_history(4))
        .await
        .expect("four history requests should finish within the bounded test");
    let times = transport.history_request_times();
    let _ = tx.send(());
    handle.await.unwrap().unwrap();

    assert_eq!(times.len(), 4);
    assert!(
        transport.sync_call_count() > 1,
        "live batches must overlap backfill"
    );
    for pair in times.windows(2) {
        assert!(
            pair[1] - pair[0] >= Duration::from_secs(1),
            "a live batch shortened the history pacing deadline: {times:?}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn rate_limit_hint_and_transient_backoff_are_honored() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);
    seed_room(&mut store, ROOM, "p1", "$a", "s1");

    let transport = ControllableTransport::new();
    transport.hold_sync(true);
    transport.push_history_error(
        "p1",
        TransportError::RateLimited {
            retry_after_ms: Some(2_000),
        },
    );
    transport.push_history(
        "p1",
        history_page("p1", None, vec![message("$old", 50, "old")]),
    );

    let (tx, handle) = spawn_run(&transport, store, RunSettings::run(1_000));
    transport.wait_for_history(2).await;
    let times = transport.history_request_times();
    assert!(
        times[1] - times[0] >= Duration::from_millis(2_000),
        "the Retry-After hint must be honored: {times:?}"
    );
    let _ = tx.send(());
    handle.await.unwrap().unwrap();

    // A rate limit without a hint uses the conservative fallback.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);
    seed_room(&mut store, ROOM, "p1", "$a", "s1");
    let transport = ControllableTransport::new();
    transport.hold_sync(true);
    transport.push_history_error(
        "p1",
        TransportError::RateLimited {
            retry_after_ms: None,
        },
    );
    transport.push_history(
        "p1",
        history_page("p1", None, vec![message("$old", 50, "old")]),
    );
    let settings = RunSettings {
        rate_limit_fallback: Duration::from_secs(30),
        ..RunSettings::run(1_000)
    };
    let (tx, handle) = spawn_run(&transport, store, settings);
    transport.wait_for_history(2).await;
    let times = transport.history_request_times();
    assert!(
        times[1] - times[0] >= Duration::from_secs(30),
        "no hint must fall back conservatively: {times:?}"
    );
    let _ = tx.send(());
    handle.await.unwrap().unwrap();

    // Transient errors back off at least the minimum delay.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);
    seed_room(&mut store, ROOM, "p1", "$a", "s1");
    let transport = ControllableTransport::new();
    transport.hold_sync(true);
    transport.push_history_error("p1", TransportError::Transient("reset".to_owned()));
    transport.push_history(
        "p1",
        history_page("p1", None, vec![message("$old", 50, "old")]),
    );
    let (tx, handle) = spawn_run(&transport, store, RunSettings::run(1_000));
    transport.wait_for_history(2).await;
    let times = transport.history_request_times();
    assert!(
        times[1] - times[0] >= Duration::from_millis(500),
        "transient errors must back off: {times:?}"
    );
    let _ = tx.send(());
    handle.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn authentication_stops_the_run_globally() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);
    seed_room(&mut store, ROOM, "p1", "$a", "s1");

    let transport = ControllableTransport::new();
    transport.hold_sync(true);
    transport.push_history_error("p1", TransportError::Authentication("401".to_owned()));

    let (_tx, handle) = spawn_run(&transport, store, RunSettings::run(1_000));
    let result = handle.await.unwrap();
    assert!(
        matches!(
            result,
            Err(runtime::RuntimeError::Engine(EngineError::Transport(
                TransportError::Authentication(_)
            )))
        ),
        "authentication must stop the run: {result:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn room_local_failure_does_not_starve_another_room() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);
    seed_room(&mut store, ROOM, "p1", "$a", "s1");
    seed_room(&mut store, ROOM2, "q1", "$b", "s2");

    let transport = ControllableTransport::new();
    transport.hold_sync(true);
    transport.push_history_error("p1", TransportError::RoomUnavailable("403".to_owned()));
    transport.push_history(
        "q1",
        history_page("q1", None, vec![message("$b-old", 20, "b")]),
    );

    let (tx, handle) = spawn_run(&transport, store, RunSettings::run(1_000));
    transport.wait_for_history(2).await;
    // The healthy room completed despite the other room's 403.
    wait_until("the healthy room to complete", || {
        let conn = db(&path);
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM events WHERE event_id = '$b-old'",
        ) == 1
    })
    .await;

    let _ = tx.send(());
    let summary = handle.await.unwrap().unwrap();
    assert_eq!(summary.unavailable, 1);
    assert!(summary.rooms_completed >= 1);

    let conn = db(&path);
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT stalled FROM room_history WHERE room_id = '!room:hs.example.org'"
        ),
        1,
        "the failing room is stalled"
    );
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT complete FROM room_history WHERE room_id = '!room2:hs.example.org'"
        ),
        1,
        "the healthy room is complete"
    );
}

#[tokio::test(start_paused = true)]
async fn shutdown_leaves_a_committed_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let store = open_store(&path);
    store.register_configured_room(ROOM, None, 10).unwrap();

    let transport = ControllableTransport::new();
    let mut room = room_update(ROOM);
    room.timeline = vec![message("$live", 100, "live")];
    room.state = vec![create_room("11")];
    room.prev_batch = Some("p1".to_owned());
    transport.push_sync(sync_batch("s1", vec![room]));
    transport.push_history(
        "p1",
        history_page("p1", None, vec![message("$old", 50, "old")]),
    );

    let (tx, handle) = spawn_run(&transport, store, RunSettings::run(1_000));
    transport.wait_for_history(1).await;

    let _ = tx.send(());
    let summary = handle.await.unwrap().unwrap();
    assert!(summary.sync_batches >= 1);

    let conn = db(&path);
    assert_eq!(
        scalar_string(&conn, "SELECT since_token FROM sync_progress WHERE id = 1"),
        Some("s1".to_owned()),
        "the durable sync checkpoint is committed"
    );
    // The archive is intact and readable after cancellation.
    drop(conn);
    let reopened = Store::open_read_only(&path).unwrap();
    assert_eq!(reopened.status().unwrap().rooms.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn continuing_base_discovers_new_gap_and_room() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);
    seed_room(&mut store, ROOM, "p0", "$a", "s0");
    // ROOM2 becomes ready only from the sync batch that arrives mid-run.
    store.register_configured_room(ROOM2, None, 10).unwrap();

    let transport = ControllableTransport::new();
    for index in 0..40 {
        transport.push_history(
            &format!("p{index}"),
            history_page(
                &format!("p{index}"),
                Some(&format!("p{}", index + 1)),
                vec![message(&format!("$h{index}"), 10, "h")],
            ),
        );
    }
    transport.push_sync(sync_batch("s1", vec![]));
    let mut room2 = room_update(ROOM2);
    room2.timeline = vec![message("$r2", 50, "r2")];
    room2.state = vec![create_room("11")];
    room2.prev_batch = Some("q1".to_owned());
    room2.limited = true;
    room2.own_membership = Some("join".to_owned());
    transport.push_sync(sync_batch("s2", vec![room2]));

    let (tx, handle) = spawn_run(&transport, store, RunSettings::run(1_000));
    transport.wait_for_history(5).await;
    let requests = transport.history_requests();
    assert!(
        requests.iter().take(4).any(|r| r.room_id == ROOM2),
        "a newly ready room/gap must be discovered promptly, not after the base drains: {:?}",
        requests
            .iter()
            .map(|r| (r.room_id.clone(), r.from.clone()))
            .collect::<Vec<_>>()
    );

    let _ = tx.send(());
    handle.await.unwrap().unwrap();
}

/// Insert `count` open gap jobs for `room` with initial cursor tokens `t0..`.
fn insert_gaps(path: &std::path::Path, room: &str, prefix: &str, count: i64) {
    let conn = db(path);
    for index in 0..count {
        conn.execute(
            "INSERT INTO gap_jobs (room_id, created_at, reason, boundary_token, upper_token, cursor_token, status)
             VALUES (?1, ?2, 'limited_sync', 'b', ?3, ?3, 'open')",
            rusqlite::params![room, index, format!("{prefix}{index}")],
        )
        .unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn bounded_scheduler_services_a_large_gap_backlog() {
    const GAPS: i64 = 150;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);
    seed_room(&mut store, ROOM, "p0", "$a", "s0");
    // Finish the base so only the gap backlog remains.
    store
        .apply_history_page(
            ROOM,
            mainlinenerd_ingest::store::HistoryWork::Base,
            "p0",
            &history_page("p0", None, vec![]),
            20,
        )
        .unwrap();
    insert_gaps(&path, ROOM, "g", GAPS);

    let transport = ControllableTransport::new();
    transport.hold_sync(true);
    for index in 0..GAPS {
        transport.push_history(
            &format!("g{index}"),
            history_page(&format!("g{index}"), None, vec![]),
        );
    }

    let (tx, handle) = spawn_run(&transport, store, RunSettings::run(1_000));
    transport.wait_for_history(GAPS as usize).await;
    let reached = transport
        .history_requests()
        .iter()
        .filter_map(|request| request.from.strip_prefix('g')?.parse::<i64>().ok())
        .max()
        .unwrap_or(-1);
    assert_eq!(
        reached,
        GAPS - 1,
        "every durable gap must be reachable without preloading the backlog"
    );

    let _ = tx.send(());
    handle.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn continuing_gaps_rotate_fairly_and_rooms_share_the_scheduler() {
    const GAPS: i64 = 40;
    const TURNS: usize = 900;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);
    seed_room(&mut store, ROOM, "p0", "$a", "s0");
    seed_room(&mut store, ROOM2, "q0", "$b", "s1");
    insert_gaps(&path, ROOM, "c", GAPS);

    let transport = ControllableTransport::new();
    transport.hold_sync(true);
    // Base and every gap keep advancing (end Some, never the boundary), so no
    // job ever completes and an old ready-window design would loop forever.
    for index in 0..GAPS {
        transport.push_history(
            &format!("p{index}"),
            history_page(
                &format!("p{index}"),
                Some(&format!("p{}", index + 1)),
                vec![],
            ),
        );
        transport.push_history(
            &format!("c{index}"),
            history_page(
                &format!("c{index}"),
                Some(&format!("c{}", index + 1)),
                vec![],
            ),
        );
        transport.push_history(
            &format!("q{index}"),
            history_page(
                &format!("q{index}"),
                Some(&format!("q{}", index + 1)),
                vec![],
            ),
        );
    }

    let (tx, handle) = spawn_run(&transport, store, RunSettings::run(1_000));
    // Drive in bounded steps until the intermediate gap is observed.
    let mut saw_intermediate = false;
    for _ in 0..TURNS {
        if transport
            .history_requests()
            .iter()
            .any(|request| request.from == "c16")
        {
            saw_intermediate = true;
            break;
        }
        tokio::time::advance(Duration::from_millis(1_000)).await;
        tokio::task::yield_now().await;
    }
    assert!(
        saw_intermediate,
        "a continuing backlog must rotate to an intermediate gap: {:?}",
        transport
            .history_requests()
            .iter()
            .map(|r| (r.room_id.clone(), r.from.clone()))
            .collect::<Vec<_>>()
    );
    let requests = transport.history_requests();
    assert!(
        requests.iter().any(|r| r.room_id == ROOM2),
        "the second room must share the scheduler"
    );
    assert!(
        requests
            .iter()
            .any(|r| r.room_id == ROOM && r.from.starts_with('p')),
        "the continuing base must still progress"
    );
    // No competitor completed before the intermediate gap was reached.
    let closed = scalar_i64(
        &db(&path),
        "SELECT COUNT(*) FROM gap_jobs WHERE status != 'open'",
    );
    assert_eq!(closed, 0, "no continuing gap may complete first");

    let _ = tx.send(());
    handle.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn base_only_room_pages_back_to_back_without_idling() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);
    seed_room(&mut store, ROOM, "p1", "$a", "s0");

    let transport = ControllableTransport::new();
    transport.hold_sync(true);
    for index in 1..60 {
        transport.push_history(
            &format!("p{index}"),
            history_page(
                &format!("p{index}"),
                Some(&format!("p{}", index + 1)),
                vec![message(&format!("$h{index}"), 10, "h")],
            ),
        );
    }

    let (tx, handle) = spawn_run(&transport, store, RunSettings::run(1_000));
    transport.wait_for_history(3).await;
    let times = transport.history_request_times();
    assert!(
        times[1] - times[0] <= Duration::from_millis(2_000),
        "a base-only room must not idle-poll between pages: {times:?}"
    );
    assert!(
        times[1] - times[0] >= Duration::from_millis(1_000),
        "pacing is still enforced: {times:?}"
    );

    // A gap appearing later is still served fairly alongside the base.
    insert_gaps(&path, ROOM, "c", 1);
    transport.push_history("c0", history_page("c0", None, vec![]));
    let mut saw_gap = false;
    for _ in 0..20 {
        if transport
            .history_requests()
            .iter()
            .any(|request| request.from == "c0")
        {
            saw_gap = true;
            break;
        }
        tokio::time::advance(Duration::from_millis(1_000)).await;
        tokio::task::yield_now().await;
    }
    assert!(saw_gap, "a later gap must still be served");
    let gap_index = transport
        .history_requests()
        .iter()
        .position(|r| r.from == "c0")
        .unwrap();
    // Let the room rotate a few more turns, then confirm the base continues.
    for _ in 0..4 {
        tokio::time::advance(Duration::from_millis(1_000)).await;
        tokio::task::yield_now().await;
    }
    let requests = transport.history_requests();
    assert!(
        requests[gap_index + 1..]
            .iter()
            .any(|r| r.from.starts_with('p')),
        "the base must keep progressing after the gap appears"
    );

    let _ = tx.send(());
    handle.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn new_gap_is_served_promptly_amid_a_continuing_backlog() {
    const OLD_GAPS: i64 = 100;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);
    seed_room(&mut store, ROOM, "p0", "$a", "s0");
    // Finish the base so only the gap backlog remains.
    store
        .apply_history_page(
            ROOM,
            mainlinenerd_ingest::store::HistoryWork::Base,
            "p0",
            &history_page("p0", None, vec![]),
            20,
        )
        .unwrap();
    insert_gaps(&path, ROOM, "c", OLD_GAPS);

    let transport = ControllableTransport::new();
    transport.hold_sync(true);
    for index in 0..OLD_GAPS {
        transport.push_history(
            &format!("c{index}"),
            history_page(
                &format!("c{index}"),
                Some(&format!("c{}", index + 1)),
                vec![],
            ),
        );
    }
    transport.push_history("n1", history_page("n1", None, vec![]));

    let (tx, handle) = spawn_run(&transport, store, RunSettings::run(1_000));
    transport.wait_for_history(4).await;
    // Inject a new limited-sync gap mid-run (highest id, so it is the newest).
    {
        let conn = db(&path);
        conn.execute(
            "INSERT INTO gap_jobs (room_id, created_at, reason, boundary_token, upper_token, cursor_token, status)
             VALUES (?1, 999, 'limited_sync', 's0', 'n1', 'n1', 'open')",
            rusqlite::params![ROOM],
        )
        .unwrap();
    }
    let before = transport.history_call_count();

    let mut served_at = None;
    for _ in 0..20 {
        if let Some(index) = transport
            .history_requests()
            .iter()
            .position(|request| request.from == "n1")
        {
            served_at = Some(index);
            break;
        }
        tokio::time::advance(Duration::from_millis(1_000)).await;
        tokio::task::yield_now().await;
    }
    let served_at = served_at.expect("the new gap must be served");
    assert!(
        served_at - before <= 6,
        "a fresh gap must not queue behind the whole backlog: served at {served_at} from {before}"
    );
    // Older ids still advance after the fresh opportunity.
    for _ in 0..4 {
        tokio::time::advance(Duration::from_millis(1_000)).await;
        tokio::task::yield_now().await;
    }
    let requests = transport.history_requests();
    assert!(
        requests[served_at + 1..]
            .iter()
            .any(|r| r.from.starts_with('c')),
        "ascending rotation must continue after the fresh gap"
    );

    let _ = tx.send(());
    handle.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn cooldown_is_never_shortened_by_the_other_plane() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);
    seed_room(&mut store, ROOM, "p0", "$a", "s0");

    let transport = ControllableTransport::new();
    transport.push_sync_error(TransportError::RateLimited {
        retry_after_ms: Some(30_000),
    });
    transport.push_history_error(
        "p0",
        TransportError::RateLimited {
            retry_after_ms: Some(1_000),
        },
    );

    let (tx, handle) = spawn_run(&transport, store, RunSettings::run(1_000));
    transport.wait_for_syncs(2).await;
    let times = transport.sync_request_times();
    assert!(
        times[1] - times[0] >= Duration::from_secs(30),
        "a shorter history cooldown must not shorten the sync cooldown: {times:?}"
    );

    let _ = tx.send(());
    handle.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn empty_sync_responses_are_paced_and_committed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let store = open_store(&path);

    let transport = ControllableTransport::new();
    for index in 0..4 {
        transport.push_sync(sync_batch(&format!("s{index}"), vec![]));
    }
    transport.hold_sync(true);

    let (tx, handle) = spawn_run(&transport, store, RunSettings::follow());
    // Drive virtual time explicitly and in bounded steps rather than relying on
    // implicit auto-advance, so the pacing is proven, not just observed.
    for _ in 0..20 {
        if transport.sync_call_count() >= 4 {
            break;
        }
        tokio::time::advance(Duration::from_millis(1_000)).await;
        tokio::task::yield_now().await;
    }
    if handle.is_finished() {
        let outcome = handle.await;
        panic!("runtime ended before the floor was reached: {outcome:?}");
    }
    assert!(
        transport.sync_call_count() >= 4,
        "empty sync responses must keep polling at the floor: {} calls",
        transport.sync_call_count()
    );
    let times = transport.sync_request_times();
    let times = times[..4].to_vec();
    for pair in times.windows(2) {
        assert!(
            pair[1] - pair[0] >= Duration::from_millis(1_000),
            "empty sync responses must be paced even with fresh tokens: {times:?}"
        );
    }
    assert_eq!(
        scalar_string(
            &db(&path),
            "SELECT since_token FROM sync_progress WHERE id = 1"
        ),
        Some("s3".to_owned()),
        "the empty batches still committed their checkpoint"
    );

    let _ = tx.send(());
    handle.await.unwrap().unwrap();
}

/// A fresh archive has no committed token, so `initialize` cannot seed an idle
/// configured room. The first successful sync must seed it in the same run.
#[tokio::test(start_paused = true)]
async fn first_committed_token_seeds_a_ready_idle_room_in_the_same_run() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let store = open_store(&path);
    store.register_configured_room(ROOM, None, 10).unwrap();
    store.set_room_version_control(ROOM, "11", 10).unwrap();
    assert_eq!(store.since_token().unwrap(), None);
    assert_eq!(store.room_history_token(ROOM).unwrap(), None);

    let transport = ControllableTransport::new();
    transport.hold_sync(true);
    transport.push_sync(sync_batch("s1", vec![]));
    transport.push_history(
        "s1",
        history_page("s1", None, vec![message("$hist", 50, "hist")]),
    );

    let (tx, handle) = spawn_run(&transport, store, RunSettings::run(1_000));
    tokio::time::timeout(Duration::from_secs(30), transport.wait_for_history(1))
        .await
        .expect("the first committed token must seed a request in the same run");
    let requests = transport.history_requests();
    assert_eq!(requests[0].room_id, ROOM);
    assert_eq!(
        requests[0].from, "s1",
        "the first committed token must seed the idle room in the same run"
    );

    wait_until("the seeded page to commit", || {
        scalar_i64(
            &db(&path),
            "SELECT COUNT(*) FROM events WHERE event_id = '$hist'",
        ) == 1
    })
    .await;
    let _ = tx.send(());
    handle.await.unwrap().unwrap();

    let conn = db(&path);
    assert_eq!(
        scalar_string(&conn, "SELECT since_token FROM sync_progress WHERE id = 1"),
        Some("s1".to_owned())
    );
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT complete FROM room_history WHERE room_id = '!room:hs.example.org'"
        ),
        1,
        "the seeded page completed the base backfill"
    );
}

/// A server `Retry-After` longer than one hour is preserved in full: no request
/// may happen at 3600s, and none before the full 7200s hint.
#[tokio::test(start_paused = true)]
async fn long_rate_limit_hint_is_honored_in_full() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);
    seed_room(&mut store, ROOM, "p0", "$a", "s0");

    let transport = ControllableTransport::new();
    transport.hold_sync(true);
    transport.push_history_error(
        "p0",
        TransportError::RateLimited {
            retry_after_ms: Some(7_200_000),
        },
    );
    transport.push_history("p0", history_page("p0", None, vec![]));

    let (tx, handle) = spawn_run(&transport, store, RunSettings::run(1_000));
    tokio::time::timeout(Duration::from_secs(30), transport.wait_for_history(1))
        .await
        .expect("the rate-limited work item must be attempted");
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    let first = transport.history_request_times()[0];

    // One hour later no second request may have happened: the hint is 7200s,
    // not a clamped 3600s.
    tokio::time::advance(Duration::from_secs(3_600)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        transport.history_call_count(),
        1,
        "a 7200s hint must not be shortened to 3600s"
    );

    // The second request only happens once the full hint has elapsed; the
    // bound is larger than the full hint so paused time can reach the timer.
    tokio::time::timeout(Duration::from_secs(7_500), transport.wait_for_history(2))
        .await
        .expect("the full hint must eventually release the retry");
    let times = transport.history_request_times();
    assert!(
        times[1] - first >= Duration::from_secs(7_200),
        "the full server hint must be honored: {times:?}"
    );
    let _ = tx.send(());
    handle.await.unwrap().unwrap();
}

/// Cancellation must still work while a long rate-limit pause is pending.
#[tokio::test(start_paused = true)]
async fn cancellation_during_a_long_rate_limit_pause_is_prompt() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);
    seed_room(&mut store, ROOM, "p0", "$a", "s0");

    let transport = ControllableTransport::new();
    transport.hold_sync(true);
    transport.push_history_error(
        "p0",
        TransportError::RateLimited {
            retry_after_ms: Some(7_200_000),
        },
    );
    transport.push_history("p0", history_page("p0", None, vec![]));

    let (tx, handle) = spawn_run(&transport, store, RunSettings::run(1_000));
    tokio::time::timeout(Duration::from_secs(30), transport.wait_for_history(1))
        .await
        .expect("the rate-limited work item must be attempted");
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(3_600)).await;
    tokio::task::yield_now().await;
    assert_eq!(transport.history_call_count(), 1);

    let _ = tx.send(());
    let result = tokio::time::timeout(Duration::from_secs(60), handle)
        .await
        .expect("cancellation must not wait out the rate-limit pause")
        .unwrap();
    assert!(result.is_ok());
    assert_eq!(
        transport.history_call_count(),
        1,
        "cancellation must not fire another request"
    );
}

/// A live history pause whose deadline cannot be represented must stop the run
/// with a controlled error: the pause is neither capped nor skipped, no
/// follow-on request is issued and no cursor or ledger row is touched
/// speculatively.
#[tokio::test(start_paused = true)]
async fn unrepresentable_live_history_pause_stops_without_follow_on_requests() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);
    seed_room(&mut store, ROOM, "p0", "$a", "s0");

    let transport = ControllableTransport::new();
    transport.hold_sync(true);
    transport.push_history_error(
        "p0",
        TransportError::RateLimited {
            retry_after_ms: None,
        },
    );
    transport.push_history("p0", history_page("p0", None, vec![]));
    let settings = RunSettings {
        rate_limit_fallback: Duration::MAX,
        ..RunSettings::run(1_000)
    };

    let (_tx, handle) = spawn_run(&transport, store, settings);
    let result = tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("an unrepresentable pause must not be waited out")
        .unwrap();
    assert!(
        matches!(
            result,
            Err(runtime::RuntimeError::DeadlineUnrepresentable { delay })
                if delay == Duration::MAX
        ),
        "unexpected run result: {result:?}"
    );
    assert_eq!(
        transport.history_call_count(),
        1,
        "no follow-on history request may be issued"
    );
    // The fair select may handle the history error before polling the initial
    // sync future. Either zero or one initial sync is valid; a second is not.
    assert!(
        transport.sync_call_count() <= 1,
        "no follow-on sync request may be issued"
    );

    let conn = db(&path);
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT token FROM room_history WHERE room_id = '!room:hs.example.org'"
        ),
        Some("p0".to_owned()),
        "the rate-limited result must not advance the cursor"
    );
    assert_eq!(
        scalar_i64(&conn, "SELECT COUNT(*) FROM history_visited"),
        0,
        "no speculative work item or ledger mutation"
    );
}

/// The same rule applies to a sync-plane rate limit: an unrepresentable pause
/// stops the run before another sync is attempted.
#[tokio::test(start_paused = true)]
async fn unrepresentable_live_sync_pause_stops_without_another_request() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let store = open_store(&path);

    let transport = ControllableTransport::new();
    transport.push_sync_error(TransportError::RateLimited {
        retry_after_ms: None,
    });
    let settings = RunSettings {
        rate_limit_fallback: Duration::MAX,
        ..RunSettings::run(1_000)
    };

    let (_tx, handle) = spawn_run(&transport, store, settings);
    let result = tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("an unrepresentable pause must not be waited out")
        .unwrap();
    assert!(
        matches!(
            result,
            Err(runtime::RuntimeError::DeadlineUnrepresentable { delay })
                if delay == Duration::MAX
        ),
        "unexpected run result: {result:?}"
    );
    assert_eq!(
        transport.sync_call_count(),
        1,
        "the run must stop before another sync request"
    );
    assert_eq!(
        transport.history_call_count(),
        0,
        "no history request is issued after the global stop"
    );
}

#[tokio::test(start_paused = true)]
async fn held_pages_cannot_revive_disabled_rooms() {
    let cases: [(&str, Vec<serde_json::Value>, bool, Option<&str>); 3] = [
        ("encrypted", vec![encryption_event()], true, None),
        (
            "upgraded",
            vec![tombstone("!next:hs.example.org")],
            false,
            None,
        ),
        ("left", vec![], false, Some("leave")),
    ];
    for (label, state, expect_encrypted, membership) in cases {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("archive.db");
        let mut store = open_store(&path);
        seed_room(&mut store, ROOM, "p1", "$live", "s0");

        let transport = ControllableTransport::new();
        transport.hold_history(true);
        transport.push_history(
            "p1",
            history_page("p1", None, vec![message("$old", 50, "old")]),
        );
        let (tx, handle) = spawn_run(&transport, store, RunSettings::run(1_000));
        transport.wait_for_history(1).await;

        let mut control = room_update(ROOM);
        control.state = state;
        control.own_membership = membership.map(str::to_owned);
        transport.push_sync(sync_batch("s1", vec![control]));

        let check_path = path.clone();
        wait_until("the control event to apply", move || {
            let conn = db(&check_path);
            let encrypted =
                scalar_i64(&conn, "SELECT encrypted FROM rooms WHERE room_id = '!room:hs.example.org'")
                    == 1;
            let upgraded = scalar_i64(
                &conn,
                "SELECT successor_room_id IS NOT NULL FROM rooms WHERE room_id = '!room:hs.example.org'",
            ) == 1;
            let left = scalar_string(
                &conn,
                "SELECT own_membership FROM rooms WHERE room_id = '!room:hs.example.org'",
            )
            .as_deref()
                == Some("leave");
            (expect_encrypted && encrypted) || upgraded || left
        })
        .await;

        transport.hold_history(false);
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = tx.send(());
        handle.await.unwrap().unwrap();

        let conn = db(&path);
        assert_eq!(
            scalar_i64(&conn, "SELECT COUNT(*) FROM events WHERE event_id = '$old'"),
            0,
            "{label}: a held page must not commit after the room is disabled"
        );
        assert_eq!(
            scalar_string(
                &conn,
                "SELECT token FROM room_history WHERE room_id = '!room:hs.example.org'"
            ),
            Some("p1".to_owned()),
            "{label}: cursor unchanged"
        );
        assert_eq!(
            scalar_i64(&conn, "SELECT COUNT(*) FROM history_visited"),
            0,
            "{label}: ledger unchanged"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn late_401_after_room_deactivation_still_halts_globally() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);
    seed_room(&mut store, ROOM, "p0", "$a", "s0");
    seed_room(&mut store, ROOM2, "q0", "$b", "s1");

    let transport = ControllableTransport::new();
    transport.hold_history(true);
    let (_tx, handle) = spawn_run(&transport, store, RunSettings::run(1_000));
    transport.wait_for_history(1).await;
    let first = transport.history_requests()[0].clone();
    let (disabled, cursor_token) = if first.room_id == ROOM {
        (ROOM, "p0")
    } else {
        assert_eq!(first.room_id, ROOM2);
        (ROOM2, "q0")
    };

    // The room leaves while its response is still in flight.
    let mut control = room_update(disabled);
    control.own_membership = Some("leave".to_owned());
    transport.push_sync(sync_batch("s2", vec![control]));
    let check_path = path.clone();
    let disabled_check = disabled.to_owned();
    wait_until("the leave to apply", move || {
        scalar_string(
            &db(&check_path),
            &format!("SELECT own_membership FROM rooms WHERE room_id = '{disabled_check}'"),
        )
        .as_deref()
            == Some("leave")
    })
    .await;

    transport.push_history_error(
        &first.from,
        TransportError::Authentication("401".to_owned()),
    );
    transport.hold_history(false);
    let result = handle.await.unwrap();
    assert!(
        matches!(
            result,
            Err(runtime::RuntimeError::Engine(EngineError::Transport(
                TransportError::Authentication(_)
            )))
        ),
        "a late 401 from a deactivated room must still halt the run: {result:?}"
    );
    let conn = db(&path);
    assert_eq!(
        scalar_string(
            &conn,
            &format!("SELECT token FROM room_history WHERE room_id = '{disabled}'")
        ),
        Some(cursor_token.to_owned()),
        "the deactivated room's cursor is untouched"
    );
}

#[tokio::test(start_paused = true)]
async fn late_429_after_room_deactivation_still_cools_other_rooms() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);
    seed_room(&mut store, ROOM, "p0", "$a", "s0");
    seed_room(&mut store, ROOM2, "q0", "$b", "s1");

    let transport = ControllableTransport::new();
    transport.hold_history(true);
    let (tx, handle) = spawn_run(&transport, store, RunSettings::run(1_000));
    transport.wait_for_history(1).await;
    let first = transport.history_requests()[0].clone();
    let (disabled, active) = if first.room_id == ROOM {
        (ROOM, ROOM2)
    } else {
        assert_eq!(first.room_id, ROOM2);
        (ROOM2, ROOM)
    };

    let mut control = room_update(disabled);
    control.own_membership = Some("leave".to_owned());
    transport.push_sync(sync_batch("s2", vec![control]));
    let check_path = path.clone();
    let disabled_check = disabled.to_owned();
    wait_until("the leave to apply", move || {
        scalar_string(
            &db(&check_path),
            &format!("SELECT own_membership FROM rooms WHERE room_id = '{disabled_check}'"),
        )
        .as_deref()
            == Some("leave")
    })
    .await;

    transport.push_history_error(
        &first.from,
        TransportError::RateLimited {
            retry_after_ms: Some(2_000),
        },
    );
    transport.hold_history(false);
    transport.wait_for_history(2).await;
    let requests = transport.history_requests();
    assert_eq!(requests[1].room_id, active, "the eligible room continues");
    let times = transport.history_request_times();
    assert!(
        times[1] - times[0] >= Duration::from_millis(2_000),
        "a late 429 must cool every other room: {times:?}"
    );

    let _ = tx.send(());
    handle.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn cancellation_replays_committed_positions_exactly_once() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);
    seed_room(&mut store, ROOM, "p0", "$a", "s0");

    let transport = ControllableTransport::new();
    // Both planes are held with nothing scripted: the requests are genuinely
    // uncommitted when the run is cancelled.
    transport.hold_sync(true);
    transport.hold_history(true);
    let (tx, handle) = spawn_run(&transport, store, RunSettings::run(1_000));
    transport.wait_for_syncs(1).await;
    transport.wait_for_history(1).await;
    let _ = tx.send(());
    handle.await.unwrap().unwrap();

    {
        let conn = db(&path);
        assert_eq!(
            scalar_i64(
                &conn,
                "SELECT COUNT(*) FROM events WHERE event_id IN ('$live','$old')"
            ),
            0,
            "an uncommitted response must not be stored"
        );
        assert_eq!(
            scalar_string(&conn, "SELECT since_token FROM sync_progress WHERE id = 1"),
            Some("s0".to_owned())
        );
        assert_eq!(
            scalar_string(
                &conn,
                "SELECT token FROM room_history WHERE room_id = '!room:hs.example.org'"
            ),
            Some("p0".to_owned())
        );
    }

    // Reopen and replay the same positions; each response applies exactly once.
    let store = Store::open(&path, &identity()).unwrap();
    let transport = ControllableTransport::new();
    let mut room = room_update(ROOM);
    room.timeline = vec![message("$live", 100, "live")];
    transport.push_sync(sync_batch("s1", vec![room]));
    transport.push_history(
        "p0",
        history_page("p0", None, vec![message("$old", 50, "old")]),
    );
    let (tx, handle) = spawn_run(&transport, store, RunSettings::run(1_000));
    wait_until("both replayed events to commit", || {
        let conn = db(&path);
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM events WHERE event_id = '$live'",
        ) == 1
            && scalar_i64(&conn, "SELECT COUNT(*) FROM events WHERE event_id = '$old'") == 1
    })
    .await;
    let _ = tx.send(());
    handle.await.unwrap().unwrap();

    let conn = db(&path);
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM events WHERE event_id = '$live'"
        ),
        1
    );
    assert_eq!(
        scalar_i64(&conn, "SELECT COUNT(*) FROM events WHERE event_id = '$old'"),
        1
    );
    assert_eq!(
        scalar_string(&conn, "SELECT since_token FROM sync_progress WHERE id = 1"),
        Some("s1".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn retry_stalled_resumes_a_403d_bounded_gap_at_its_saved_from_to() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);
    seed_room(&mut store, ROOM, "p1", "$a", "s0");
    // Open a bounded gap (boundary s0, upper g1, cursor g1).
    let mut limited = room_update(ROOM);
    limited.timeline = vec![message("$live2", 200, "live2")];
    limited.prev_batch = Some("g1".to_owned());
    limited.limited = true;
    store
        .apply_sync_batch(&sync_batch("s1", vec![limited]), 20)
        .unwrap();
    let gap_id = store.open_gap_positions().unwrap()[0].gap_id;
    // Finish the base so the gap is the only eligible work.
    store
        .apply_history_page(
            ROOM,
            mainlinenerd_ingest::store::HistoryWork::Base,
            "p1",
            &history_page("p1", None, vec![]),
            25,
        )
        .unwrap();
    // A cursor-less unresolved gap and a repaired gap must stay untouched.
    let (no_token_id, repaired_id);
    {
        let conn = db(&path);
        conn.execute(
            "INSERT INTO gap_jobs (room_id, created_at, reason, boundary_token, upper_token, cursor_token, status, close_reason)
             VALUES (?1, 1, 'limited_sync', 'b2', 'no-token-upper', NULL, 'unresolved', 'no repair token available')",
            rusqlite::params![ROOM],
        )
        .unwrap();
        no_token_id = scalar_i64(&conn, "SELECT MAX(gap_id) FROM gap_jobs");
        conn.execute(
            "INSERT INTO gap_jobs (room_id, created_at, reason, boundary_token, upper_token, cursor_token, status)
             VALUES (?1, 1, 'limited_sync', 'b3', 'done-upper', 'done-cursor', 'open')",
            rusqlite::params![ROOM],
        )
        .unwrap();
        repaired_id = scalar_i64(&conn, "SELECT MAX(gap_id) FROM gap_jobs");
    }
    let repaired = store
        .apply_history_page(
            ROOM,
            mainlinenerd_ingest::store::HistoryWork::Gap(repaired_id),
            "done-cursor",
            &history_page("done-cursor", Some("b3"), vec![]),
            30,
        )
        .unwrap();
    assert_eq!(repaired.status, Some(HistoryStatus::Completed));

    // A 403 stalls the gap but keeps its saved cursor and boundary.
    let transport = ControllableTransport::new();
    transport.hold_sync(true);
    transport.push_history_error("g1", TransportError::RoomUnavailable("403".to_owned()));
    let (tx, handle) = spawn_run(&transport, store, RunSettings::run(1_000));
    transport.wait_for_history(1).await;
    let _ = tx.send(());
    handle.await.unwrap().unwrap();
    {
        let conn = db(&path);
        assert_eq!(
            scalar_string(
                &conn,
                &format!("SELECT status FROM gap_jobs WHERE gap_id = {gap_id}")
            ),
            Some("unresolved".to_owned())
        );
        assert_eq!(
            scalar_string(
                &conn,
                &format!("SELECT cursor_token FROM gap_jobs WHERE gap_id = {gap_id}")
            ),
            Some("g1".to_owned()),
            "the 403'd gap keeps its saved cursor"
        );
        assert_eq!(
            scalar_string(
                &conn,
                &format!("SELECT boundary_token FROM gap_jobs WHERE gap_id = {gap_id}")
            ),
            Some("s0".to_owned())
        );
    }

    // Without the explicit control it stays stalled and unscheduled.
    let transport = ControllableTransport::new();
    transport.hold_sync(true);
    transport.push_history(
        "g1",
        history_page("g1", Some("s0"), vec![message("$gap-unused", 50, "unused")]),
    );
    let store = open_store(&path);
    let (tx, handle) = spawn_run(&transport, store, RunSettings::run(1_000));
    for _ in 0..5 {
        tokio::time::advance(Duration::from_millis(1_000)).await;
        tokio::task::yield_now().await;
    }
    assert_eq!(
        transport.history_call_count(),
        0,
        "an unresolved gap is not scheduled without the explicit control"
    );
    let _ = tx.send(());
    handle.await.unwrap().unwrap();

    // Explicit recovery resumes the same saved from/to, with no join.
    let mut retry_store = open_store(&path);
    let retried = retry_store.retry_stalled_configured(now_ms()).unwrap();
    assert_eq!(retried.base, 0);
    assert_eq!(retried.gaps, 1);

    let transport = ControllableTransport::new();
    transport.hold_sync(true);
    transport.push_history(
        "g1",
        history_page(
            "g1",
            Some("s0"),
            vec![message("$gap-resumed", 50, "resumed")],
        ),
    );
    let (tx, handle) = spawn_run(&transport, open_store(&path), RunSettings::run(1_000));
    transport.wait_for_history(1).await;
    let requests = transport.history_requests();
    assert_eq!(requests[0].from, "g1");
    assert_eq!(requests[0].to.as_deref(), Some("s0"));
    let _ = tx.send(());
    handle.await.unwrap().unwrap();
    let conn = db(&path);
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM events WHERE event_id = '$gap-resumed'"
        ),
        1
    );
    assert_eq!(
        scalar_string(
            &conn,
            &format!("SELECT status FROM gap_jobs WHERE gap_id = {gap_id}")
        ),
        Some("repaired".to_owned())
    );
    assert_eq!(
        scalar_string(
            &conn,
            &format!("SELECT status FROM gap_jobs WHERE gap_id = {no_token_id}")
        ),
        Some("unresolved".to_owned()),
        "a cursor-less unresolved gap stays terminal"
    );
    assert_eq!(
        scalar_string(
            &conn,
            &format!("SELECT status FROM gap_jobs WHERE gap_id = {repaired_id}")
        ),
        Some("repaired".to_owned()),
        "a repaired gap stays repaired"
    );
}

fn now_ms() -> i64 {
    mainlinenerd_ingest::store::now_unix_ms()
}

#[tokio::test(start_paused = true)]
async fn eager_sync_prefix_does_not_starve_history_or_other_tasks() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);
    seed_room(&mut store, ROOM, "p1", "$a", "s0");

    let transport = ControllableTransport::new();
    // A finite eager prefix of immediately-ready, non-empty sync batches.
    for index in 0..300 {
        let mut room = room_update(ROOM);
        room.timeline = vec![message(&format!("$s{index}"), index, "s")];
        transport.push_sync(sync_batch(&format!("s{index}"), vec![room]));
    }
    transport.push_history("p1", history_page("p1", None, vec![]));

    let controller_ran = Arc::new(AtomicBool::new(false));
    {
        let flag = controller_ran.clone();
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            flag.store(true, Ordering::SeqCst);
        });
    }

    let (tx, handle) = spawn_run(&transport, store, RunSettings::run(1_000));
    transport.wait_for_history(1).await;
    assert!(
        transport.sync_call_count() <= 100,
        "history must be polled before the eager sync prefix is exhausted: {} syncs",
        transport.sync_call_count()
    );

    // A separate controller task must also get scheduled.
    for _ in 0..100 {
        if controller_ran.load(Ordering::SeqCst) {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        controller_ran.load(Ordering::SeqCst),
        "other tasks must run"
    );

    let _ = tx.send(());
    let summary = handle.await.unwrap().unwrap();
    assert!(summary.history_pages >= 1);
}
