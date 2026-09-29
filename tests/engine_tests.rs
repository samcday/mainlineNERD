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

    engine.transport().push_history(
        "p2",
        history_page("p2", Some("p3"), vec![message("$gap1", 200, "gap1")]),
    );
    engine.transport().push_history(
        "p3",
        history_page("p3", Some("p1"), vec![message("$gap0", 50, "gap0")]),
    );
    engine
        .transport()
        .push_history("p1", history_page("p1", None, vec![]));

    let outcome = engine.run_history_once(30).await.unwrap();
    assert_eq!(outcome.rooms_completed, 1);
    assert_eq!(outcome.rooms_stalled, 0);

    let report = engine.store().status().unwrap();
    let room = &report.rooms[0];
    assert!(room.history_complete);
    assert_eq!(room.open_gaps, 0);
    assert_eq!(room.unresolved_gaps, 0);
    assert_eq!(room.messages, 4);
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
        .push_sync_error(TransportError::Permanent("401 unauthorized".to_owned()));
    let error = engine.poll_sync_once(20).await.unwrap_err();
    assert!(matches!(
        error,
        EngineError::Transport(TransportError::Permanent(_))
    ));
    assert_eq!(
        engine.store().live_health().unwrap().consecutive_failures,
        2
    );
}

#[tokio::test]
async fn permanent_history_error_is_surfaced() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut engine = engine(FakeTransport::new(), &path);
    seed_live_room(&mut engine).await;

    engine
        .transport()
        .push_history_error("p1", TransportError::Permanent("403 forbidden".to_owned()));
    let error = engine.run_room_history_once(ROOM, 20).await.unwrap_err();
    assert!(matches!(
        error,
        EngineError::Transport(TransportError::Permanent(_))
    ));
    assert_eq!(engine.transport().history_call_count(), 1);
}
