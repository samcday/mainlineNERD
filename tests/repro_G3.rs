//! Repro for review finding G3: a room upgrade (tombstone) permanently freezes
//! the predecessor's unfinished base backfill and its open gaps, including the
//! gap opened by the very limited sync that carried the tombstone.
//!
//! The assertions encode the behaviour an archiver should have: the
//! predecessor's history is static and still readable over /messages, so its
//! base cursor and bounded gaps must keep being served, and at minimum an
//! explicit --retry-stalled must be able to resume it.

mod common;

use std::path::Path;

use common::*;
use mainlinenerd_ingest::engine::{Engine, EngineConfig};

fn engine(path: &Path, max_pages_per_room: usize) -> Engine<FakeTransport> {
    Engine::new(
        FakeTransport::new(),
        open_store(path),
        EngineConfig {
            sync_timeout_ms: 30_000,
            history_limit: 50,
            max_pages_per_room,
        },
    )
}

#[tokio::test]
async fn g3_tombstone_does_not_freeze_predecessor_history() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut engine = engine(&path, 1);
    engine
        .store()
        .register_configured_room(ROOM, None, 1)
        .unwrap();
    engine
        .store()
        .set_room_version_control(ROOM, "11", 1)
        .unwrap();

    // s1: initial sync seeds the base cursor at p1.
    let mut initial = room_update(ROOM);
    initial.timeline = vec![message("$live", 100, "live")];
    initial.prev_batch = Some("p1".to_owned());
    engine.transport().push_sync(sync_batch("s1", vec![initial]));
    engine.poll_sync_once(10).await.unwrap();

    // Base backfill makes one page of progress (p1 -> p1b): "page 300 of 2000".
    engine.transport().push_history(
        "p1",
        history_page("p1", Some("p1b"), vec![message("$old1", 50, "old1")]),
    );
    let first = engine.run_room_history_once(ROOM, 15).await.unwrap();
    assert_eq!(first.pages_fetched, 1);
    assert_eq!(
        engine.store().room_history_token(ROOM).unwrap().as_deref(),
        Some("p1b")
    );

    // s2: a limited sync opens an older bounded gap A.
    let mut limited = room_update(ROOM);
    limited.timeline = vec![message("$live2", 300, "live2")];
    limited.prev_batch = Some("p2".to_owned());
    limited.limited = true;
    engine.transport().push_sync(sync_batch("s2", vec![limited]));
    engine.poll_sync_once(20).await.unwrap();
    let gap_a = engine.store().open_gap_positions().unwrap()[0].gap_id;

    // s3: the room is upgraded; the limited sync carrying the tombstone opens
    // gap G for the final pre-upgrade messages.
    let mut upgrade = room_update(ROOM);
    upgrade.state = vec![tombstone("!next:hs.example.org")];
    upgrade.timeline = vec![message("$live3", 500, "live3")];
    upgrade.prev_batch = Some("p3".to_owned());
    upgrade.limited = true;
    engine.transport().push_sync(sync_batch("s3", vec![upgrade]));
    engine.poll_sync_once(30).await.unwrap();

    // Sanity: the trigger happened exactly as the finding describes.
    let report = engine.store().status().unwrap();
    assert_eq!(
        report.rooms[0].successor_room_id.as_deref(),
        Some("!next:hs.example.org"),
        "tombstone must be recorded"
    );
    let gaps = engine.store().open_gap_positions().unwrap();
    assert_eq!(gaps.len(), 2, "gap A and the tombstone-sync gap G are open");
    let gap_g = gaps
        .iter()
        .find(|g| g.token == "p3")
        .expect("gap G for the tombstone sync")
        .gap_id;

    let mut failures: Vec<String> = Vec::new();

    // 1. Request builders for the predecessor's own static history.
    let base_req = engine.base_history_request(ROOM).unwrap();
    eprintln!("G3: base_history_request after tombstone = {base_req:?}");
    if base_req.is_none() {
        failures.push("base_history_request(ROOM) is None after tombstone (cursor p1b frozen)".into());
    }
    for (label, gap) in [("A", gap_a), ("G", gap_g)] {
        let req = engine.gap_history_request(gap).unwrap();
        eprintln!("G3: gap_history_request({label}) after tombstone = {req:?}");
        if req.is_none() {
            failures.push(format!("gap_history_request(gap {label}) is None after tombstone"));
        }
    }

    // 2. The sequential base path actually fetches the next page.
    engine.transport().push_history(
        "p1b",
        history_page("p1b", Some("p1c"), vec![message("$old2", 40, "old2")]),
    );
    let next = engine.run_room_history_once(ROOM, 40).await.unwrap();
    eprintln!(
        "G3: run_room_history_once after tombstone = {next:?}, transport history calls = {}",
        engine.transport().history_call_count()
    );
    if next.pages_fetched == 0 {
        failures.push("run_room_history_once fetched 0 pages for the predecessor".into());
    }

    // 3. Restart: a fresh engine on the same archive.
    drop(engine);
    let mut engine = engine_reopen(&path);
    let after_restart = engine.base_history_request(ROOM).unwrap();
    eprintln!("G3: base_history_request after restart = {after_restart:?}");
    if after_restart.is_none() {
        failures.push("restart does not resume predecessor base backfill".into());
    }

    // 4. Explicit operator recovery: --retry-stalled after the base stalled.
    engine
        .store()
        .mark_history_stalled(ROOM, "transient 500", 50)
        .unwrap();
    let retried = engine.store_mut().retry_stalled_configured(60).unwrap();
    eprintln!("G3: retry_stalled_configured = {retried:?}");
    if retried.base == 0 {
        failures.push(format!(
            "--retry-stalled resumed nothing for the tombstoned predecessor: {retried:?}"
        ));
    }

    assert!(
        failures.is_empty(),
        "G3 reproduced; predecessor history frozen by tombstone:\n  - {}",
        failures.join("\n  - ")
    );
}

fn engine_reopen(path: &Path) -> Engine<FakeTransport> {
    engine(path, 1)
}
