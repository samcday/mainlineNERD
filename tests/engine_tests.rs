//! Integration tests for the engine: pagination progress, stall detection,
//! live/history overlap, gap repair and transport error classification.

mod common;

use std::path::Path;

use common::*;
use mainlinenerd_ingest::engine::{
    Engine, EngineConfig, EngineError, SyncPollOutcome, TransportError,
};

fn engine(transport: FakeTransport, path: &Path) -> Engine<FakeTransport> {
    Engine::new(
        transport,
        open_store(path),
        EngineConfig {
            sync_timeout_ms: 30_000,
            history_limit: 50,
            max_pages_per_room: 16,
        },
    )
}

async fn seed_live_room(engine: &mut Engine<FakeTransport>) {
    let mut room = room_update(ROOM);
    room.timeline = vec![message("$live", 100, "live")];
    room.prev_batch = Some("p1".to_owned());
    engine.transport().push_sync(sync_batch("s1", vec![room]));
    let outcome = engine.poll_sync_once(10).await.unwrap();
    assert!(matches!(outcome, SyncPollOutcome::Applied(_)));
}

#[tokio::test]
async fn empty_intermediate_page_with_end_token_progresses() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut engine = engine(FakeTransport::new(), &path);
    seed_live_room(&mut engine).await;
    assert_eq!(
        engine.store().room_history_token(ROOM).unwrap().as_deref(),
        Some("p1")
    );

    // An empty page with a fresh end token is not completion: keep going.
    engine
        .transport()
        .push_history("p1", history_page("p1", Some("p2"), vec![]));
    engine.transport().push_history(
        "p2",
        history_page("p2", None, vec![message("$old", 10, "old")]),
    );

    let outcome = engine.run_room_history_once(ROOM, 20).await.unwrap();
    assert_eq!(outcome.pages_fetched, 2);
    assert!(outcome.completed);
    assert!(!outcome.stalled);
    assert_eq!(engine.transport().history_call_count(), 2);
    assert!(engine.store().room_history_complete(ROOM).unwrap());
    assert_eq!(engine.store().room_history_token(ROOM).unwrap(), None);
}

#[tokio::test]
async fn repeated_token_stalls_without_looping() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut engine = engine(FakeTransport::new(), &path);
    seed_live_room(&mut engine).await;

    engine
        .transport()
        .push_history("p1", history_page("p1", Some("p1"), vec![]));

    let outcome = engine.run_room_history_once(ROOM, 20).await.unwrap();
    assert!(outcome.stalled);
    assert_eq!(outcome.pages_fetched, 1);
    assert_eq!(
        engine.transport().history_call_count(),
        1,
        "a repeated token must fail after one request, not loop"
    );
    assert!(engine.store().room_history_stalled(ROOM).unwrap());
    assert_eq!(
        engine.store().room_history_token(ROOM).unwrap().as_deref(),
        Some("p1"),
        "a stalled room must not advance its token"
    );
}

#[tokio::test]
async fn token_cycle_is_detected_by_the_engine() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut engine = engine(FakeTransport::new(), &path);
    seed_live_room(&mut engine).await;

    // p1 -> p2 -> p1 is a cycle across pages, not a repeated end token.
    engine
        .transport()
        .push_history("p1", history_page("p1", Some("p2"), vec![]));
    engine
        .transport()
        .push_history("p2", history_page("p2", Some("p1"), vec![]));

    let outcome = engine.run_room_history_once(ROOM, 20).await.unwrap();
    assert!(outcome.stalled);
    assert_eq!(outcome.pages_fetched, 2);
    assert_eq!(engine.transport().history_call_count(), 2);
    assert!(engine.store().room_history_stalled(ROOM).unwrap());
}

#[tokio::test]
async fn limited_sync_gap_is_repaired_by_engine_pagination() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut engine = engine(FakeTransport::new(), &path);
    seed_live_room(&mut engine).await;

    let mut limited = room_update(ROOM);
    limited.timeline = vec![message("$live2", 300, "live2")];
    limited.prev_batch = Some("p2".to_owned());
    limited.limited = true;
    engine
        .transport()
        .push_sync(sync_batch("s2", vec![limited]));
    engine.poll_sync_once(20).await.unwrap();

    let room = &engine.store().status().unwrap().rooms[0];
    assert_eq!(room.open_gaps, 1, "limited sync must persist a repair job");
    assert_eq!(
        engine.store().room_history_token(ROOM).unwrap().as_deref(),
        Some("p1"),
        "the limited sync must not rewind base backfill"
    );

    // Base backfill and the bounded gap are separate work items.
    engine.transport().push_history(
        "p1",
        history_page("p1", None, vec![message("$old", 50, "old")]),
    );
    engine.transport().push_history(
        "p2",
        history_page("p2", Some("s1"), vec![message("$gap1", 200, "gap1")]),
    );

    let outcome = engine.run_history_once(30).await.unwrap();
    assert_eq!(outcome.rooms_completed, 1);
    assert_eq!(outcome.gaps_repaired, 1);
    assert_eq!(outcome.rooms_stalled, 0);

    let report = engine.store().status().unwrap();
    let room = &report.rooms[0];
    assert!(room.history_complete);
    assert_eq!(room.open_gaps, 0);
    assert_eq!(room.unresolved_gaps, 0);
    assert_eq!(room.messages, 4);

    // The bounded repair request carried the saved lower sync boundary; the
    // archival backfill request has no lower bound.
    let requests = engine.transport().history_requests.lock().unwrap();
    let base = requests.iter().find(|r| r.from == "p1").unwrap();
    assert_eq!(base.room_id, ROOM);
    assert_eq!(base.to, None);
    let gap = requests.iter().find(|r| r.from == "p2").unwrap();
    assert_eq!(gap.room_id, ROOM);
    assert_eq!(gap.to.as_deref(), Some("s1"));
}

#[tokio::test]
async fn gap_token_cycle_marks_only_that_gap_unresolved() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut engine = engine(FakeTransport::new(), &path);
    seed_live_room(&mut engine).await;

    let mut limited = room_update(ROOM);
    limited.timeline = vec![message("$live2", 300, "live2")];
    limited.prev_batch = Some("p2".to_owned());
    limited.limited = true;
    engine
        .transport()
        .push_sync(sync_batch("s2", vec![limited]));
    engine.poll_sync_once(20).await.unwrap();
    let gap_id = engine.store().open_gap_positions().unwrap()[0].gap_id;

    engine
        .transport()
        .push_history("p2", history_page("p2", Some("p3"), vec![]));
    engine
        .transport()
        .push_history("p3", history_page("p3", Some("p2"), vec![]));

    let outcome = engine.run_gap_repair_once(gap_id, 30).await.unwrap();
    assert!(outcome.unresolved);
    assert_eq!(outcome.pages_fetched, 2);
    assert_eq!(engine.transport().history_call_count(), 2);

    let report = engine.store().status().unwrap();
    let room = &report.rooms[0];
    assert_eq!(room.open_gaps, 0);
    assert_eq!(room.unresolved_gaps, 1);
    assert!(!room.history_stalled, "the base backfill is not stalled");
    assert_eq!(
        engine.store().room_history_token(ROOM).unwrap().as_deref(),
        Some("p1")
    );
}

#[tokio::test]
async fn live_and_history_overlap_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut engine = engine(FakeTransport::new(), &path);
    seed_live_room(&mut engine).await;

    engine.transport().push_history(
        "p1",
        history_page(
            "p1",
            None,
            vec![message("$live", 100, "live"), message("$old", 50, "old")],
        ),
    );
    let outcome = engine.run_room_history_once(ROOM, 20).await.unwrap();
    assert!(outcome.completed);
    assert_eq!(
        outcome.events_stored, 1,
        "the overlapping live event is a duplicate"
    );

    let report = engine.store().status().unwrap();
    assert_eq!(report.rooms[0].events, 2);
    assert_eq!(report.rooms[0].messages, 2);
}

#[tokio::test]
async fn sync_transport_errors_are_classified_and_recorded() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut engine = engine(FakeTransport::new(), &path);

    engine
        .transport()
        .push_sync_error(TransportError::RateLimited {
            retry_after_ms: Some(1234),
        });
    let outcome = engine.poll_sync_once(10).await.unwrap();
    match outcome {
        SyncPollOutcome::RateLimited { retry_after_ms } => {
            assert_eq!(retry_after_ms, Some(1234));
        }
        other => panic!("expected rate limit, got {other:?}"),
    }
    let health = engine.store().live_health().unwrap();
    assert_eq!(health.consecutive_failures, 1);
    assert_eq!(health.last_error.as_deref(), Some("rate limited"));

    engine
        .transport()
        .push_sync_error(TransportError::Authentication(
            "401 unauthorized".to_owned(),
        ));
    let error = engine.poll_sync_once(20).await.unwrap_err();
    assert!(matches!(
        error,
        EngineError::Transport(TransportError::Authentication(_))
    ));
    assert_eq!(
        engine.store().live_health().unwrap().consecutive_failures,
        2
    );
}

/// Two rooms with distinct `updated_at` values so the run order is
/// deterministic: ROOM first, ROOM2 second.
async fn seed_two_rooms_ordered(engine: &mut Engine<FakeTransport>) {
    let mut first = room_update(ROOM);
    first.timeline = vec![message("$a", 100, "a")];
    first.prev_batch = Some("pa".to_owned());
    engine.transport().push_sync(sync_batch("s1", vec![first]));
    engine.poll_sync_once(10).await.unwrap();

    let mut second = room_update(ROOM2);
    second.timeline = vec![message("$b", 150, "b")];
    second.prev_batch = Some("pb".to_owned());
    engine.transport().push_sync(sync_batch("s2", vec![second]));
    engine.poll_sync_once(20).await.unwrap();
}

#[tokio::test]
async fn authentication_history_error_stops_the_run() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut engine = engine(FakeTransport::new(), &path);
    seed_live_room(&mut engine).await;

    engine.transport().push_history_error(
        "p1",
        TransportError::Authentication("401 unauthorized".to_owned()),
    );
    let error = engine.run_history_once(20).await.unwrap_err();
    assert!(matches!(
        error,
        EngineError::Transport(TransportError::Authentication(_))
    ));
    assert_eq!(engine.transport().history_call_count(), 1);
}

#[tokio::test]
async fn room_local_history_failure_does_not_starve_later_rooms() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut engine = engine(FakeTransport::new(), &path);
    seed_two_rooms_ordered(&mut engine).await;

    engine.transport().push_history_error(
        "pa",
        TransportError::RoomUnavailable("403 forbidden".to_owned()),
    );
    engine.transport().push_history(
        "pb",
        history_page("pb", None, vec![message("$old", 10, "old")]),
    );

    let outcome = engine.run_history_once(30).await.unwrap();
    assert_eq!(outcome.items_failed, 1);
    assert_eq!(outcome.items_deferred, 0);
    assert_eq!(outcome.rooms_completed, 1);
    assert_eq!(outcome.retry_after_ms, None);
    assert_eq!(engine.transport().history_call_count(), 2);

    let report = engine.store().status().unwrap();
    let failed = report.rooms.iter().find(|r| r.room_id == ROOM).unwrap();
    assert!(failed.history_stalled, "the failing room is flagged");
    assert_eq!(
        failed.history_error.as_deref(),
        Some("room unavailable: 403 forbidden")
    );
    let later = report.rooms.iter().find(|r| r.room_id == ROOM2).unwrap();
    assert!(later.history_complete, "the later room still progressed");
}

#[tokio::test]
async fn transient_history_failure_defers_room_and_run_continues() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut engine = engine(FakeTransport::new(), &path);
    seed_two_rooms_ordered(&mut engine).await;

    engine.transport().push_history_error(
        "pa",
        TransportError::Transient("connection reset".to_owned()),
    );
    engine.transport().push_history(
        "pb",
        history_page("pb", None, vec![message("$old", 10, "old")]),
    );

    let outcome = engine.run_history_once(30).await.unwrap();
    assert_eq!(outcome.items_deferred, 1);
    assert_eq!(outcome.items_failed, 0);
    assert_eq!(outcome.rooms_completed, 1);

    let report = engine.store().status().unwrap();
    let deferred = report.rooms.iter().find(|r| r.room_id == ROOM).unwrap();
    assert!(
        !deferred.history_stalled,
        "transient failures are not terminal"
    );
    assert_eq!(
        deferred.history_error.as_deref(),
        Some("transient transport error: connection reset")
    );
    assert_eq!(
        engine.store().room_history_token(ROOM).unwrap().as_deref(),
        Some("pa")
    );
    assert_eq!(
        engine.store().rooms_needing_history().unwrap().len(),
        1,
        "the deferred work item stays queued"
    );

    // A later run retries the deferred room and completes it.
    engine.transport().push_history(
        "pa",
        history_page("pa", None, vec![message("$a-old", 5, "a")]),
    );
    let outcome = engine.run_history_once(40).await.unwrap();
    assert_eq!(outcome.rooms_completed, 1);
    assert!(engine.store().room_history_complete(ROOM).unwrap());
}

#[tokio::test]
async fn history_rate_limit_stops_run_with_retry_hint() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut engine = engine(FakeTransport::new(), &path);
    seed_two_rooms_ordered(&mut engine).await;

    engine.transport().push_history_error(
        "pa",
        TransportError::RateLimited {
            retry_after_ms: Some(500),
        },
    );

    let outcome = engine.run_history_once(30).await.unwrap();
    assert_eq!(outcome.retry_after_ms, Some(500));
    assert_eq!(outcome.items_deferred, 1);
    assert_eq!(outcome.pages_fetched, 0);
    assert_eq!(
        engine.transport().history_call_count(),
        1,
        "a rate limit must stop the run instead of hammering other rooms"
    );
    assert_eq!(engine.store().rooms_needing_history().unwrap().len(), 2);
    assert!(!engine.store().room_history_stalled(ROOM).unwrap());
    assert!(!engine.store().room_history_stalled(ROOM2).unwrap());
}
