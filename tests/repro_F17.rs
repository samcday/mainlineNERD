//! Repro for review finding F17: the history plane's transient backoff and
//! attempt counter are shared with /sync.

mod common;

use std::time::Duration;

use common::*;
use mainlinenerd_ingest::engine::{
    Engine, EngineConfig, HistoryRequest, SyncRequest, Transport, TransportError,
};
use mainlinenerd_ingest::event::{HistoryPage, SyncBatch};
use mainlinenerd_ingest::runtime::{self, RunSettings};
use mainlinenerd_ingest::store::Store;
use tokio::sync::oneshot;

fn engine_config() -> EngineConfig {
    EngineConfig {
        sync_timeout_ms: 30_000,
        history_limit: 50,
        max_pages_per_room: 64,
    }
}

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

/// /sync takes 100ms of (virtual) server time per response.
#[derive(Clone)]
struct SlowSync(ControllableTransport);

#[async_trait::async_trait]
impl Transport for SlowSync {
    async fn sync(&self, request: SyncRequest) -> Result<SyncBatch, TransportError> {
        tokio::time::sleep(Duration::from_millis(100)).await;
        self.0.sync(request).await
    }

    async fn history(&self, request: HistoryRequest) -> Result<HistoryPage, TransportError> {
        self.0.history(request).await
    }
}

/// (a) architecture.md: "an active backfill never blocks a live batch" and
/// "A transient failure defers only that work item". One room's /messages
/// keeps failing transiently while /sync is perfectly healthy and delivering
/// useful batches (which are not paced). The live plane must keep its ~100ms
/// cadence; it must not wait out the history item's backoff.
#[tokio::test(start_paused = true)]
async fn f17_history_transient_failure_does_not_delay_live_sync() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);
    seed_room(&mut store, ROOM, "p1", "$seed", "s0");

    let transport = ControllableTransport::new();
    // No scripted /messages response for "p1": every history request is a
    // TransportError::Transient (like a room whose /messages always 5xxs).
    for index in 1..=400 {
        let mut update = room_update(ROOM);
        update.timeline = vec![message(&format!("$live-{index}"), 1_000 + index, "live")];
        transport.push_sync(sync_batch(&format!("s{index}"), vec![update]));
    }

    let engine = Engine::new(SlowSync(transport.clone()), store, engine_config());
    let (tx, rx) = oneshot::channel();
    let handle = tokio::spawn(runtime::run(engine, RunSettings::run(1_000), async move {
        let _ = rx.await;
    }));
    tokio::time::sleep(Duration::from_secs(10)).await;
    let _ = tx.send(());
    let summary = handle.await.unwrap().unwrap();

    let sync_times = transport.sync_request_times();
    let history_times = transport.history_request_times();
    let base = sync_times[0];
    let rel = |v: &[tokio::time::Instant]| -> Vec<u128> {
        v.iter().map(|t| (*t - base).as_millis()).collect()
    };
    eprintln!("summary: {summary:?}");
    eprintln!("sync requests in 10s: {}", sync_times.len());
    eprintln!("sync request times (ms): {:?}", rel(&sync_times));
    eprintln!("history request times (ms): {:?}", rel(&history_times));

    assert!(
        history_times.len() >= 2,
        "the failing history item must actually be retried"
    );
    let max_gap = sync_times
        .windows(2)
        .map(|w| w[1] - w[0])
        .max()
        .unwrap();
    eprintln!("max gap between sync requests: {max_gap:?}");
    assert!(
        max_gap < Duration::from_millis(500),
        "a transient /messages failure delayed the next live /sync by {max_gap:?}"
    );
    assert!(
        sync_times.len() >= 50,
        "healthy /sync at 100ms/response should issue ~100 requests in 10s, got {}",
        sync_times.len()
    );
}

/// Control for (a): identical setup with the history plane off. Proves the
/// 100ms live cadence is what the runtime does absent history failures.
#[tokio::test(start_paused = true)]
async fn f17_control_live_sync_cadence_without_history() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);
    seed_room(&mut store, ROOM, "p1", "$seed", "s0");
    let transport = ControllableTransport::new();
    for index in 1..=400 {
        let mut update = room_update(ROOM);
        update.timeline = vec![message(&format!("$live-{index}"), 1_000 + index, "live")];
        transport.push_sync(sync_batch(&format!("s{index}"), vec![update]));
    }
    let engine = Engine::new(SlowSync(transport.clone()), store, engine_config());
    let (tx, rx) = oneshot::channel();
    let handle = tokio::spawn(runtime::run(engine, RunSettings::follow(), async move {
        let _ = rx.await;
    }));
    tokio::time::sleep(Duration::from_secs(10)).await;
    let _ = tx.send(());
    handle.await.unwrap().unwrap();
    let sync_times = transport.sync_request_times();
    let max_gap = sync_times.windows(2).map(|w| w[1] - w[0]).max().unwrap();
    eprintln!("control(a): sync requests in 10s: {}, max gap {max_gap:?}", sync_times.len());
    assert!(max_gap < Duration::from_millis(500));
    assert!(sync_times.len() >= 50);
}

/// Control for (b): identical failing /sync with the history plane off.
/// Proves exponential escalation works when nothing resets the counter.
#[tokio::test(start_paused = true)]
async fn f17_control_sync_backoff_escalates_without_history() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);
    seed_room(&mut store, ROOM, "p0", "$seed", "s0");
    let transport = ControllableTransport::new();
    let engine = Engine::new(transport.clone(), store, engine_config());
    let (tx, rx) = oneshot::channel();
    let handle = tokio::spawn(runtime::run(engine, RunSettings::follow(), async move {
        let _ = rx.await;
    }));
    tokio::time::sleep(Duration::from_secs(30)).await;
    let _ = tx.send(());
    handle.await.unwrap().unwrap();
    let sync_times = transport.sync_request_times();
    let base = sync_times[0];
    let rel: Vec<u128> = sync_times.iter().map(|t| (*t - base).as_millis()).collect();
    eprintln!("control(b): failing sync requests in 30s: {} at {rel:?}", sync_times.len());
    assert!(sync_times.len() <= 8);
}

/// (b) "Transient failures back off with bounded exponential delay": a /sync
/// that fails transiently every time must escalate its delay, even while the
/// history plane is succeeding. Escalating 1s,2s,4s,8s,16s (+<=25% jitter)
/// allows at most ~6 attempts in 30s.
#[tokio::test(start_paused = true)]
async fn f17_sync_transient_backoff_escalates_while_history_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);
    seed_room(&mut store, ROOM, "p0", "$seed", "s0");

    let transport = ControllableTransport::new();
    // /sync: nothing scripted and not held => every call is Transient.
    // History: a long healthy chain p0 -> p1 -> ... -> p60.
    for index in 0..60 {
        let from = format!("p{index}");
        let end = format!("p{}", index + 1);
        transport.push_history(&from, history_page(&from, Some(&end), vec![]));
    }

    let engine = Engine::new(transport.clone(), store, engine_config());
    let (tx, rx) = oneshot::channel();
    let handle = tokio::spawn(runtime::run(engine, RunSettings::run(1_000), async move {
        let _ = rx.await;
    }));
    tokio::time::sleep(Duration::from_secs(30)).await;
    let _ = tx.send(());
    let summary = handle.await.unwrap().unwrap();

    let sync_times = transport.sync_request_times();
    let history_times = transport.history_request_times();
    let base = sync_times[0];
    let rel = |v: &[tokio::time::Instant]| -> Vec<u128> {
        v.iter().map(|t| (*t - base).as_millis()).collect()
    };
    eprintln!("summary: {summary:?}");
    eprintln!("failing sync requests in 30s: {}", sync_times.len());
    eprintln!("sync request times (ms): {:?}", rel(&sync_times));
    eprintln!("history request times (ms): {:?}", rel(&history_times));

    assert!(summary.history_pages >= 5, "history must be succeeding");
    assert!(
        sync_times.len() <= 8,
        "a persistently failing /sync must back off exponentially; got {} attempts in 30s",
        sync_times.len()
    );
}
