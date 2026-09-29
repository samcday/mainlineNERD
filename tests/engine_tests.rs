//! Integration tests for the engine: pagination progress, stall detection,
//! live/history overlap, gap repair and transport error classification.

mod common;

use std::path::Path;
use std::time::Duration;

use common::*;
use mainlinenerd_ingest::engine::{
    Engine, EngineConfig, EngineError, SyncPollOutcome, TransportError,
};

fn engine(transport: FakeTransport, path: &Path) -> Engine<FakeTransport> {
    engine_with_budget(transport, path, 16)
}

fn engine_with_budget(
    transport: FakeTransport,
    path: &Path,
    max_pages_per_room: usize,
) -> Engine<FakeTransport> {
    Engine::new(
        transport,
        open_store(path),
        EngineConfig {
            sync_timeout_ms: 30_000,
            history_limit: 50,
            max_pages_per_room,
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
    match error {
        EngineError::HistoryAborted { source, partial } => {
            assert!(matches!(source, TransportError::Authentication(_)));
            assert_eq!(partial.pages_fetched, 0);
            assert_eq!(partial.events_stored, 0);
        }
        other => panic!("expected an aborted history run, got {other:?}"),
    }
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

#[tokio::test]
async fn rate_limit_without_hint_is_still_an_explicit_halt() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut engine = engine(FakeTransport::new(), &path);
    seed_two_rooms_ordered(&mut engine).await;

    engine.transport().push_history_error(
        "pa",
        TransportError::RateLimited {
            retry_after_ms: None,
        },
    );

    let outcome = engine.run_history_once(30).await.unwrap();
    assert!(
        outcome.rate_limited,
        "a rate limit without a hint must still be classified"
    );
    assert_eq!(outcome.retry_after_ms, None);
    assert_eq!(outcome.items_deferred, 1);
    assert_eq!(
        engine.transport().history_call_count(),
        1,
        "no later room may be requested after a rate-limited halt"
    );
    assert_eq!(engine.store().rooms_needing_history().unwrap().len(), 2);
}

#[tokio::test]
async fn persisted_ledger_detects_a_cycle_across_reopen_with_page_budget_one() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");

    {
        let mut engine = engine_with_budget(FakeTransport::new(), &path, 1);
        seed_live_room(&mut engine).await;
        engine
            .transport()
            .push_history("p1", history_page("p1", Some("p2"), vec![]));
        let outcome = engine.run_room_history_once(ROOM, 20).await.unwrap();
        assert_eq!(outcome.pages_fetched, 1);
        assert!(!outcome.stalled);
        assert_eq!(
            engine.store().room_history_token(ROOM).unwrap().as_deref(),
            Some("p2")
        );
    }

    // Reopen: the visited-token ledger survives, so p2 -> p1 is recognized as
    // a cycle even though each call only fetched one page.
    {
        let mut engine = engine_with_budget(FakeTransport::new(), &path, 1);
        engine
            .transport()
            .push_history("p2", history_page("p2", Some("p1"), vec![]));
        let outcome = engine.run_room_history_once(ROOM, 30).await.unwrap();
        assert!(outcome.stalled, "the persisted cycle must stall the base");
        assert_eq!(outcome.pages_fetched, 1);
        assert_eq!(engine.transport().history_call_count(), 1);
        assert!(engine.store().room_history_stalled(ROOM).unwrap());
        assert_eq!(
            engine.store().room_history_token(ROOM).unwrap().as_deref(),
            Some("p2"),
            "a stalled cycle must not advance the cursor"
        );
    }
}

#[tokio::test]
async fn longer_cycle_is_detected_by_the_persisted_ledger() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut engine = engine(FakeTransport::new(), &path);
    seed_live_room(&mut engine).await;

    // p1 -> p2 -> p3 -> p2: p2 was visited by the first page, so the third
    // response is a cycle, not a fresh advance.
    engine
        .transport()
        .push_history("p1", history_page("p1", Some("p2"), vec![]));
    engine
        .transport()
        .push_history("p2", history_page("p2", Some("p3"), vec![]));
    engine
        .transport()
        .push_history("p3", history_page("p3", Some("p2"), vec![]));

    let outcome = engine.run_room_history_once(ROOM, 20).await.unwrap();
    assert!(outcome.stalled);
    assert_eq!(outcome.pages_fetched, 3);
    assert_eq!(engine.transport().history_call_count(), 3);
    assert!(engine.store().room_history_stalled(ROOM).unwrap());
}

#[tokio::test]
async fn transient_gap_failure_is_attributed_to_the_gap_not_the_base() {
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
    assert_eq!(engine.store().open_gap_positions().unwrap().len(), 1);

    // The base completes, then the gap request fails transiently.
    engine.transport().push_history(
        "p1",
        history_page("p1", None, vec![message("$old", 50, "old")]),
    );
    engine.transport().push_history_error(
        "p2",
        TransportError::Transient("connection reset".to_owned()),
    );

    let outcome = engine.run_history_once(30).await.unwrap();
    assert_eq!(outcome.rooms_completed, 1);
    assert_eq!(outcome.items_deferred, 1);
    assert_eq!(outcome.pages_fetched, 1);
    assert!(!outcome.rate_limited);

    let report = engine.store().status().unwrap();
    let room = &report.rooms[0];
    assert!(
        !room.history_stalled,
        "a transient gap failure must not stall the base backfill"
    );
    assert_eq!(
        room.history_error, None,
        "a transient gap failure must not be written to the base"
    );
    assert_eq!(room.open_gaps, 1);
    assert_eq!(
        room.open_gap_error.as_deref(),
        Some("transient transport error: connection reset"),
        "status attributes the failure to the gap work item"
    );
    assert_eq!(
        engine.store().open_gap_positions().unwrap().len(),
        1,
        "the transient gap stays queued"
    );
}

#[tokio::test]
async fn partial_progress_survives_a_transient_failure_after_a_committed_page() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut engine = engine(FakeTransport::new(), &path);
    seed_live_room(&mut engine).await;

    engine.transport().push_history(
        "p1",
        history_page("p1", Some("p2"), vec![message("$h", 50, "h")]),
    );
    engine
        .transport()
        .push_history_error("p2", TransportError::Transient("blip".to_owned()));

    let outcome = engine.run_history_once(20).await.unwrap();
    assert_eq!(outcome.pages_fetched, 1);
    assert_eq!(outcome.events_stored, 1);
    assert_eq!(outcome.items_deferred, 1);
    assert!(!outcome.rate_limited);
    assert_eq!(
        engine.store().room_history_token(ROOM).unwrap().as_deref(),
        Some("p2")
    );
}

#[tokio::test]
async fn partial_progress_survives_an_unavailable_failure_after_a_committed_page() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut engine = engine(FakeTransport::new(), &path);
    seed_live_room(&mut engine).await;

    engine.transport().push_history(
        "p1",
        history_page("p1", Some("p2"), vec![message("$h", 50, "h")]),
    );
    engine.transport().push_history_error(
        "p2",
        TransportError::RoomUnavailable("403 forbidden".to_owned()),
    );

    let outcome = engine.run_history_once(20).await.unwrap();
    assert_eq!(outcome.pages_fetched, 1);
    assert_eq!(outcome.events_stored, 1);
    assert_eq!(outcome.items_failed, 1);
    assert!(engine.store().room_history_stalled(ROOM).unwrap());
}

#[tokio::test]
async fn partial_progress_survives_a_rate_limit_after_a_committed_page() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut engine = engine(FakeTransport::new(), &path);
    seed_live_room(&mut engine).await;

    engine.transport().push_history(
        "p1",
        history_page("p1", Some("p2"), vec![message("$h", 50, "h")]),
    );
    engine.transport().push_history_error(
        "p2",
        TransportError::RateLimited {
            retry_after_ms: None,
        },
    );

    let outcome = engine.run_history_once(20).await.unwrap();
    assert_eq!(outcome.pages_fetched, 1);
    assert_eq!(outcome.events_stored, 1);
    assert!(outcome.rate_limited);
    assert_eq!(outcome.retry_after_ms, None);
    assert_eq!(outcome.items_deferred, 1);
}

#[tokio::test]
async fn partial_progress_is_reported_on_authentication_abort() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut engine = engine(FakeTransport::new(), &path);
    seed_live_room(&mut engine).await;

    engine.transport().push_history(
        "p1",
        history_page("p1", Some("p2"), vec![message("$h", 50, "h")]),
    );
    engine.transport().push_history_error(
        "p2",
        TransportError::Authentication("401 unauthorized".to_owned()),
    );

    let error = engine.run_history_once(20).await.unwrap_err();
    match error {
        EngineError::HistoryAborted { source, partial } => {
            assert!(matches!(source, TransportError::Authentication(_)));
            assert_eq!(partial.pages_fetched, 1);
            assert_eq!(partial.events_stored, 1);
            assert_eq!(
                engine.store().room_history_token(ROOM).unwrap().as_deref(),
                Some("p2"),
                "the committed page is durable even when the run aborts"
            );
        }
        other => panic!("expected an aborted history run, got {other:?}"),
    }
}

#[tokio::test]
async fn stalled_base_makes_no_requests_even_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");

    {
        let mut engine = engine(FakeTransport::new(), &path);
        seed_live_room(&mut engine).await;
        engine
            .transport()
            .push_history("p1", history_page("p1", Some("p1"), vec![]));
        let outcome = engine.run_room_history_once(ROOM, 20).await.unwrap();
        assert!(outcome.stalled);
        assert_eq!(outcome.pages_fetched, 1);
        assert_eq!(engine.transport().history_call_count(), 1);

        // A second call on the same engine must not refetch the stalled cursor.
        let outcome = engine.run_room_history_once(ROOM, 25).await.unwrap();
        assert!(outcome.stalled);
        assert_eq!(outcome.pages_fetched, 0);
        assert_eq!(
            engine.transport().history_call_count(),
            1,
            "a stalled base must not be refetched"
        );
    }

    // Reopen: the persisted stall is checked before any network work, so the
    // old cursor is never refetched and nothing is counted as committed.
    {
        let mut engine = engine(FakeTransport::new(), &path);
        assert!(engine.store().room_history_stalled(ROOM).unwrap());
        let outcome = engine.run_room_history_once(ROOM, 30).await.unwrap();
        assert!(outcome.stalled);
        assert_eq!(outcome.pages_fetched, 0);
        assert_eq!(outcome.events_stored, 0);
        assert_eq!(
            engine.transport().history_call_count(),
            0,
            "a persisted stalled base must make zero requests"
        );
        assert_eq!(
            engine.store().room_history_token(ROOM).unwrap().as_deref(),
            Some("p1")
        );
    }
}

/// Every policy block (leave, ban, encrypted, upgraded) must stop both the
/// base and the gap public sequential paths before any transport request is
/// built, leave all durable state untouched, and never starve a healthy room.
#[tokio::test]
async fn disabled_rooms_make_no_sequential_history_requests() {
    let cases: [(&str, Vec<serde_json::Value>, Option<&str>); 4] = [
        ("encrypted", vec![encryption_event()], None),
        ("upgraded", vec![tombstone("!next:hs.example.org")], None),
        ("left", vec![], Some("leave")),
        ("banned", vec![], Some("ban")),
    ];
    for (label, state, membership) in cases {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("archive.db");
        let mut engine = engine(FakeTransport::new(), &path);

        // ROOM has an active base cursor and one open bounded gap.
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

        // ROOM2 is healthy and has its own base work.
        let mut healthy = room_update(ROOM2);
        healthy.prev_batch = Some("q1".to_owned());
        engine
            .transport()
            .push_sync(sync_batch("s3", vec![healthy]));
        engine.poll_sync_once(30).await.unwrap();

        // Disable ROOM with the current policy signal.
        let mut control = room_update(ROOM);
        control.state = state;
        control.own_membership = membership.map(str::to_owned);
        engine
            .transport()
            .push_sync(sync_batch("s4", vec![control]));
        engine.poll_sync_once(40).await.unwrap();

        // The request builders themselves honor the policy.
        assert!(
            engine.base_history_request(ROOM).unwrap().is_none(),
            "{label}: base request builder"
        );
        assert!(
            engine.gap_history_request(gap_id).unwrap().is_none(),
            "{label}: gap request builder"
        );

        // Both sequential entry points return promptly with zero requests.
        let base = tokio::time::timeout(
            Duration::from_secs(2),
            engine.run_room_history_once(ROOM, 50),
        )
        .await
        .expect("a disabled base must return promptly")
        .unwrap();
        assert_eq!(base.pages_fetched, 0, "{label}: no page may be counted");
        assert!(!base.completed && !base.stalled, "{label}");
        let gap = tokio::time::timeout(
            Duration::from_secs(2),
            engine.run_gap_repair_once(gap_id, 51),
        )
        .await
        .expect("a disabled gap must return promptly")
        .unwrap();
        assert_eq!(gap.pages_fetched, 0, "{label}: no page may be counted");
        assert!(!gap.repaired && !gap.unresolved, "{label}");
        assert_eq!(
            engine.transport().history_call_count(),
            0,
            "{label}: a disabled room must make zero transport requests"
        );

        // Neither the base cursor/progress nor the gap job moved.
        let report = engine.store().status().unwrap();
        let disabled = report.rooms.iter().find(|r| r.room_id == ROOM).unwrap();
        assert!(disabled.history_token_set, "{label}");
        assert_eq!(
            engine.store().room_history_token(ROOM).unwrap().as_deref(),
            Some("p1"),
            "{label}: the base cursor must not move"
        );
        assert_eq!(disabled.history_pages, 0, "{label}");
        assert!(!disabled.history_complete, "{label}");
        assert!(!disabled.history_stalled, "{label}");
        let gap_position = engine.store().open_gap_position(gap_id).unwrap().unwrap();
        assert_eq!(gap_position.token, "p2", "{label}");
        assert_eq!(gap_position.to_token.as_deref(), Some("s1"), "{label}");

        // In the same run, the healthy room still progresses.
        engine.transport().push_history(
            "q1",
            history_page("q1", None, vec![message("$r2old", 10, "r2")]),
        );
        let run = engine.run_history_once(60).await.unwrap();
        assert_eq!(
            run.rooms_completed, 1,
            "{label}: the healthy room completes"
        );
        assert_eq!(run.pages_fetched, 1, "{label}");
        assert_eq!(run.rooms_stalled, 0, "{label}");
        assert_eq!(run.gaps_repaired, 0, "{label}");
        assert_eq!(
            engine.transport().history_call_count(),
            1,
            "{label}: only the healthy room may be requested"
        );
        assert!(
            engine.store().room_history_complete(ROOM2).unwrap(),
            "{label}: the healthy room's base completed"
        );
    }
}
