//! Repro for review finding G2: a base-backfill /messages page with no `end`
//! that did not reach m.room.create (Synapse's shape when its local DB is
//! exhausted and foreground federation backfill failed) permanently completes
//! the base, and no recovery path (restart, re-seed, --retry-stalled) reopens it.

mod common;

use common::*;
use mainlinenerd_ingest::store::{HistoryStatus, HistoryWork};

#[test]
fn g2_endless_page_without_room_create_is_not_permanently_complete() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    // Bot joined a federated room; it only holds recent events locally.
    store.register_configured_room(ROOM, None, 1).unwrap();
    let mut update = room_update(ROOM);
    update.state = vec![create_room("11")];
    update.timeline = vec![message("$live", 1000, "live")];
    update.prev_batch = Some("p1".to_owned());
    store
        .apply_sync_batch(&sync_batch("s1", vec![update]), 10)
        .unwrap();

    // Page 1: local DB still has a couple of older events.
    let first = store
        .apply_history_page(
            ROOM,
            HistoryWork::Base,
            "p1",
            &history_page(
                "p1",
                Some("p2"),
                vec![message("$old2", 900, "old2"), message("$old1", 800, "old1")],
            ),
            20,
        )
        .unwrap();
    assert_eq!(first.status, Some(HistoryStatus::Advanced));

    // Page 2: local frontier reached, federation backfill failed/backoff ->
    // Synapse returns {chunk: [], start} with no `end`. No m.room.create seen.
    let frontier = store
        .apply_history_page(
            ROOM,
            HistoryWork::Base,
            "p2",
            &history_page("p2", None, vec![]),
            30,
        )
        .unwrap();
    let status_after_frontier = frontier.status;
    let complete_after_frontier = store.room_history_complete(ROOM).unwrap();
    let token_after_frontier = store.room_history_token(ROOM).unwrap();

    // Recovery path 1: operator runs `run --retry-stalled`.
    let retried = store.retry_stalled_configured(40).unwrap();

    // Recovery path 2: restart (reopen the archive), re-seed configured rooms.
    drop(store);
    let store = open_store(&path);
    let reseeded = store.seed_configured_room_history(ROOM, 50).unwrap();

    // Recovery path 3: a later sync with a fresh prev_batch.
    let mut store = store;
    let mut later = room_update(ROOM);
    later.timeline = vec![message("$live2", 2000, "live2")];
    later.prev_batch = Some("p9".to_owned());
    store
        .apply_sync_batch(&sync_batch("s2", vec![later]), 60)
        .unwrap();

    let needing = store.rooms_needing_history().unwrap();
    let scheduled = needing.iter().any(|p| p.room_id == ROOM);
    let complete_final = store.room_history_complete(ROOM).unwrap();
    let report = store.status().unwrap();
    let reported_complete = report
        .rooms
        .iter()
        .find(|r| r.room_id == ROOM)
        .map(|r| r.history_complete);

    eprintln!(
        "G2 OBSERVED: frontier_status={status_after_frontier:?} \
         complete_after_frontier={complete_after_frontier} \
         token_after_frontier={token_after_frontier:?} \
         retry_stalled={retried:?} reseeded={reseeded} \
         scheduled_after_restart_and_sync={scheduled} complete_final={complete_final} \
         status_report_history_complete={reported_complete:?}"
    );

    // Correct behaviour (per finding G2): without reaching m.room.create, an
    // end-less page must not be a permanent, silent completion. At least one
    // recovery path must put the base back on the schedule, and status must
    // not claim the history is complete.
    assert!(
        scheduled || retried.base > 0,
        "G2: base history truncated at the local frontier is never rescheduled \
         (complete={complete_final}, retry_stalled={retried:?}, reseeded={reseeded})"
    );
    assert_ne!(
        reported_complete,
        Some(true),
        "G2: status reports history complete although m.room.create was never reached"
    );
}
