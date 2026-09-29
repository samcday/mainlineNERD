//! Integration tests for the durable store: atomicity, resume, projection,
//! redaction suppression and gap bookkeeping.

mod common;

use common::*;
use mainlinenerd_ingest::store::{HistoryStatus, StoreError};
use serde_json::json;

#[test]
fn malformed_event_rolls_back_batch_and_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut room = room_update(ROOM);
    room.timeline = vec![
        message("$ok", 100, "one"),
        json!({"type": "m.room.message"}),
    ];
    let error = store
        .apply_sync_batch(&sync_batch("s1", vec![room]), 10)
        .unwrap_err();
    assert!(matches!(error, StoreError::MalformedEvent { .. }));
    assert_eq!(store.since_token().unwrap(), None);
    drop(store);

    let conn = db(&path);
    assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM events"), 0);
    assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM rooms"), 0);
    assert_eq!(
        scalar_string(&conn, "SELECT since_token FROM sync_progress WHERE id = 1"),
        None
    );
}

#[test]
fn reopen_resumes_from_persisted_token() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");

    {
        let mut store = open_store(&path);
        let mut room = room_update(ROOM);
        room.timeline = vec![message("$1", 100, "one")];
        store
            .apply_sync_batch(&sync_batch("s1", vec![room]), 10)
            .unwrap();
    }

    let mut store = open_store(&path);
    assert_eq!(store.since_token().unwrap().as_deref(), Some("s1"));
    let mut room = room_update(ROOM);
    room.timeline = vec![message("$2", 200, "two")];
    store
        .apply_sync_batch(&sync_batch("s2", vec![room]), 20)
        .unwrap();
    assert_eq!(store.since_token().unwrap().as_deref(), Some("s2"));

    drop(store);
    let conn = db(&path);
    assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM events"), 2);
    assert_eq!(
        scalar_i64(&conn, "SELECT COUNT(*) FROM current_messages"),
        2
    );
}

#[test]
fn duplicate_overlap_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut first = room_update(ROOM);
    first.timeline = vec![message("$1", 100, "one")];
    store
        .apply_sync_batch(&sync_batch("s1", vec![first]), 10)
        .unwrap();

    let mut overlap = room_update(ROOM);
    overlap.timeline = vec![message("$1", 100, "one"), message("$2", 200, "two")];
    let outcome = store
        .apply_sync_batch(&sync_batch("s2", vec![overlap]), 20)
        .unwrap();
    assert_eq!(outcome.events_duplicate, 1);

    let conn = db(&path);
    assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM events"), 2);
    assert_eq!(
        scalar_i64(&conn, "SELECT COUNT(*) FROM current_messages"),
        2
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body FROM current_messages WHERE event_id = '$1'"
        ),
        Some("one".to_owned())
    );
}

#[test]
fn archive_binding_rejects_another_account() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    drop(open_store(&path));

    // Same account with trivially different homeserver spelling is fine.
    let mut same = identity();
    same.homeserver = "https://HS.example.org/".to_owned();
    drop(mainlinenerd_ingest::store::Store::open(&path, &same).unwrap());

    let mut other = identity();
    other.user_id = "@someone-else:hs.example.org".to_owned();
    let error = match mainlinenerd_ingest::store::Store::open(&path, &other) {
        Ok(_) => panic!("expected identity mismatch"),
        Err(error) => error,
    };
    assert!(matches!(error, StoreError::IdentityMismatch(_)));
}

#[test]
fn edits_apply_latest_valid_same_sender_only() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut room = room_update(ROOM);
    room.timeline = vec![
        message("$orig", 100, "original"),
        edit("$edit", 200, "$orig", "edited"),
        invalid_edit("$bad", 300, "$orig"),
        edit_from("$foreign", 400, "$orig", "bob edit", BOB),
        edit("$older", 50, "$orig", "older edit"),
        edit("$newest", 500, "$orig", "final"),
    ];
    store
        .apply_sync_batch(&sync_batch("s1", vec![room]), 10)
        .unwrap();

    let conn = db(&path);
    assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM events"), 6);
    assert_eq!(
        scalar_i64(&conn, "SELECT COUNT(*) FROM current_messages"),
        1,
        "edits must never become messages of their own"
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body FROM current_messages WHERE event_id = '$orig'"
        ),
        Some("final".to_owned())
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT latest_edit_event_id FROM current_messages WHERE event_id = '$orig'"
        ),
        Some("$newest".to_owned())
    );
}

#[test]
fn edit_before_original_still_applies() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut edits_first = room_update(ROOM);
    edits_first.timeline = vec![edit("$edit", 200, "$orig", "edited")];
    store
        .apply_sync_batch(&sync_batch("s1", vec![edits_first]), 10)
        .unwrap();
    {
        let conn = db(&path);
        assert_eq!(
            scalar_i64(&conn, "SELECT COUNT(*) FROM current_messages"),
            0
        );
    }

    let mut original = room_update(ROOM);
    original.timeline = vec![message("$orig", 100, "original")];
    store
        .apply_sync_batch(&sync_batch("s2", vec![original]), 20)
        .unwrap();

    let conn = db(&path);
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body FROM current_messages WHERE event_id = '$orig'"
        ),
        Some("edited".to_owned())
    );
}

#[test]
fn redaction_before_original_is_never_resurrected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    // Redaction arrives first: the target has not been fetched yet.
    let mut first = room_update(ROOM);
    first.timeline = vec![create_room("11"), redaction("$red", 150, "$orig", true)];
    store
        .apply_sync_batch(&sync_batch("s1", vec![first]), 10)
        .unwrap();
    {
        let conn = db(&path);
        assert_eq!(
            scalar_i64(&conn, "SELECT COUNT(*) FROM current_messages"),
            0
        );
        assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM redactions"), 1);
    }

    // Target arrives later with a body; it must be stored redacted.
    let mut second = room_update(ROOM);
    second.timeline = vec![message("$orig", 100, "secret text")];
    store
        .apply_sync_batch(&sync_batch("s2", vec![second]), 20)
        .unwrap();
    {
        let conn = db(&path);
        assert_eq!(
            scalar_i64(
                &conn,
                "SELECT redacted FROM current_messages WHERE event_id = '$orig'"
            ),
            1
        );
        assert_eq!(
            scalar_string(
                &conn,
                "SELECT body FROM current_messages WHERE event_id = '$orig'"
            ),
            None
        );
        let raw = scalar_string(
            &conn,
            "SELECT raw_json FROM events WHERE event_id = '$orig'",
        )
        .expect("raw event stored");
        assert!(
            !raw.contains("secret text"),
            "body leaked into raw_json: {raw}"
        );
        assert_eq!(
            scalar_string(
                &conn,
                "SELECT body_text FROM events WHERE event_id = '$orig'"
            ),
            None
        );
    }

    // Older replay of the original must not resurrect the body.
    let mut replay = room_update(ROOM);
    replay.timeline = vec![message("$orig", 100, "secret text")];
    store
        .apply_sync_batch(&sync_batch("s3", vec![replay]), 30)
        .unwrap();
    {
        let conn = db(&path);
        assert_eq!(
            scalar_string(
                &conn,
                "SELECT body FROM current_messages WHERE event_id = '$orig'"
            ),
            None
        );
        assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM redactions"), 1);
    }
}

#[test]
fn redaction_of_edit_reverts_to_original_and_pre_v11_form_works() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut room = room_update(ROOM);
    room.timeline = vec![
        create_room("10"),
        message("$a", 100, "original a"),
        edit("$a-edit", 200, "$a", "edited a"),
        message("$b", 110, "original b"),
        edit("$b-edit", 210, "$b", "edited b"),
        redaction("$b-red", 300, "$b-edit", false),
    ];
    store
        .apply_sync_batch(&sync_batch("s1", vec![room]), 10)
        .unwrap();

    let conn = db(&path);
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body FROM current_messages WHERE event_id = '$a'"
        ),
        Some("edited a".to_owned())
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body FROM current_messages WHERE event_id = '$b'"
        ),
        Some("original b".to_owned()),
        "redacting an edit must fall back to the original body"
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT latest_edit_event_id FROM current_messages WHERE event_id = '$b'"
        ),
        None
    );
}

#[test]
fn already_redacted_representation_is_not_projected_as_text() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut room = room_update(ROOM);
    room.timeline = vec![already_redacted_message("$m", 100)];
    store
        .apply_sync_batch(&sync_batch("s1", vec![room]), 10)
        .unwrap();

    let conn = db(&path);
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT redacted FROM current_messages WHERE event_id = '$m'"
        ),
        1
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body FROM current_messages WHERE event_id = '$m'"
        ),
        None
    );
}

#[test]
fn unknown_events_are_stored_but_not_projected_and_room_flags_surface() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut room = room_update(ROOM);
    room.state = vec![
        create_room("11"),
        encryption_event(),
        tombstone("!next:hs.example.org"),
    ];
    room.timeline = vec![
        message("$m", 100, "hello"),
        member("$member", 90),
        json!({
            "type": "m.reaction",
            "event_id": "$reaction",
            "sender": BOB,
            "origin_server_ts": 110,
            "content": { "m.relates_to": { "rel_type": "m.annotation", "event_id": "$m", "key": "x" } }
        }),
    ];
    store
        .apply_sync_batch(&sync_batch("s1", vec![room]), 10)
        .unwrap();

    let conn = db(&path);
    assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM events"), 6);
    assert_eq!(
        scalar_i64(&conn, "SELECT COUNT(*) FROM current_messages"),
        1,
        "only m.room.message should be projected"
    );

    let report = store.status().unwrap();
    let status = &report.rooms[0];
    assert!(status.encrypted);
    assert!(status.needs_operator_action);
    assert_eq!(
        status.successor_room_id.as_deref(),
        Some("!next:hs.example.org")
    );
    assert_eq!(status.room_version.as_deref(), Some("11"));
}

#[test]
fn limited_sync_creates_bounded_gap_job_and_repair_closes_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    // Initial sync seeds the history cursor with prev_batch.
    let mut initial = room_update(ROOM);
    initial.timeline = vec![message("$live", 100, "live")];
    initial.prev_batch = Some("p1".to_owned());
    store
        .apply_sync_batch(&sync_batch("s1", vec![initial]), 10)
        .unwrap();
    assert_eq!(
        store.room_history_token(ROOM).unwrap().as_deref(),
        Some("p1")
    );

    // A limited sync arrives: events between the old position and the new
    // timeline were skipped, so a gap job must exist before the token moves.
    let mut limited = room_update(ROOM);
    limited.timeline = vec![message("$live2", 300, "live2")];
    limited.prev_batch = Some("p2".to_owned());
    limited.limited = true;
    store
        .apply_sync_batch(&sync_batch("s2", vec![limited]), 20)
        .unwrap();
    assert_eq!(store.since_token().unwrap().as_deref(), Some("s2"));
    assert_eq!(
        store.room_history_token(ROOM).unwrap().as_deref(),
        Some("p2")
    );
    {
        let conn = db(&path);
        assert_eq!(
            scalar_string(
                &conn,
                "SELECT boundary_token FROM gap_jobs WHERE status = 'open'"
            ),
            Some("p1".to_owned())
        );
        assert_eq!(
            scalar_string(
                &conn,
                "SELECT upper_token FROM gap_jobs WHERE status = 'open'"
            ),
            Some("p2".to_owned())
        );
    }

    // Repair walks backward from p2; the gap closes at its recorded boundary.
    let outcome = store
        .apply_history_page(
            ROOM,
            &history_page("p2", Some("p3"), vec![message("$gap1", 200, "gap1")]),
            30,
        )
        .unwrap();
    assert_eq!(outcome.status, Some(HistoryStatus::Advanced));
    assert_eq!(
        scalar_i64(
            &db(&path),
            "SELECT COUNT(*) FROM gap_jobs WHERE status = 'open'"
        ),
        1
    );

    let outcome = store
        .apply_history_page(
            ROOM,
            &history_page("p3", Some("p1"), vec![message("$gap0", 50, "gap0")]),
            40,
        )
        .unwrap();
    assert_eq!(outcome.status, Some(HistoryStatus::Advanced));

    let conn = db(&path);
    assert_eq!(
        scalar_i64(&conn, "SELECT COUNT(*) FROM gap_jobs WHERE status = 'open'"),
        0
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT close_reason FROM gap_jobs WHERE room_id = '!room:hs.example.org'"
        ),
        Some("token".to_owned())
    );
    assert_eq!(
        scalar_i64(&conn, "SELECT COUNT(*) FROM current_messages"),
        4,
        "live, live2 and both gap events must be present"
    );
}

#[test]
fn stalled_history_token_is_persisted_and_reported() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut initial = room_update(ROOM);
    initial.timeline = vec![message("$live", 100, "live")];
    initial.prev_batch = Some("p1".to_owned());
    store
        .apply_sync_batch(&sync_batch("s1", vec![initial]), 10)
        .unwrap();

    let mut limited = room_update(ROOM);
    limited.timeline = vec![message("$live2", 300, "live2")];
    limited.prev_batch = Some("p2".to_owned());
    limited.limited = true;
    store
        .apply_sync_batch(&sync_batch("s2", vec![limited]), 20)
        .unwrap();

    // Empty page returning the same token makes no progress: stall, do not loop.
    let outcome = store
        .apply_history_page(ROOM, &history_page("p2", Some("p2"), vec![]), 30)
        .unwrap();
    assert_eq!(outcome.status, Some(HistoryStatus::Stalled));
    assert_eq!(
        store.room_history_token(ROOM).unwrap().as_deref(),
        Some("p2")
    );

    let report = store.status().unwrap();
    let room = &report.rooms[0];
    assert!(room.history_stalled);
    assert_eq!(room.unresolved_gaps, 1);
    assert_eq!(room.open_gaps, 0);
}

#[test]
fn reaching_history_start_completes_and_closes_gaps() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut initial = room_update(ROOM);
    initial.timeline = vec![message("$live", 100, "live")];
    initial.prev_batch = Some("p1".to_owned());
    store
        .apply_sync_batch(&sync_batch("s1", vec![initial]), 10)
        .unwrap();

    let mut limited = room_update(ROOM);
    limited.timeline = vec![message("$live2", 300, "live2")];
    limited.prev_batch = Some("p2".to_owned());
    limited.limited = true;
    store
        .apply_sync_batch(&sync_batch("s2", vec![limited]), 20)
        .unwrap();

    let outcome = store
        .apply_history_page(
            ROOM,
            &history_page("p2", None, vec![message("$old", 10, "old")]),
            30,
        )
        .unwrap();
    assert_eq!(outcome.status, Some(HistoryStatus::Completed));
    assert!(store.room_history_complete(ROOM).unwrap());

    let conn = db(&path);
    assert_eq!(
        scalar_i64(&conn, "SELECT COUNT(*) FROM gap_jobs WHERE status = 'open'"),
        0
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT close_reason FROM gap_jobs WHERE room_id = '!room:hs.example.org'"
        ),
        Some("history_start".to_owned())
    );
}

#[test]
fn malformed_history_event_rolls_back_page_and_token() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut initial = room_update(ROOM);
    initial.timeline = vec![message("$live", 100, "live")];
    initial.prev_batch = Some("p1".to_owned());
    store
        .apply_sync_batch(&sync_batch("s1", vec![initial]), 10)
        .unwrap();

    let error = store
        .apply_history_page(
            ROOM,
            &history_page(
                "p1",
                Some("p2"),
                vec![message("$ok", 10, "ok"), json!({"type": "m.room.message"})],
            ),
            20,
        )
        .unwrap_err();
    assert!(matches!(error, StoreError::MalformedEvent { .. }));
    assert_eq!(
        store.room_history_token(ROOM).unwrap().as_deref(),
        Some("p1")
    );
    {
        let conn = db(&path);
        assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM events"), 1);
    }
}

#[test]
fn exports_jsonl_messages_and_events() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut room = room_update(ROOM);
    room.timeline = vec![
        create_room("11"),
        message("$a", 100, "one"),
        edit("$edit", 150, "$a", "uno"),
        message("$b", 200, "secret"),
        redaction("$b-red", 250, "$b", true),
    ];
    store
        .apply_sync_batch(&sync_batch("s1", vec![room]), 10)
        .unwrap();

    let mut messages = Vec::new();
    let count = store.export_messages(None, &mut messages).unwrap();
    assert_eq!(count, 2);
    let text = String::from_utf8(messages).unwrap();
    let records: Vec<serde_json::Value> = text
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let a = records.iter().find(|r| r["event_id"] == "$a").unwrap();
    assert_eq!(a["body"], "uno");
    assert_eq!(a["latest_edit_event_id"], "$edit");
    assert_eq!(a["redacted"], false);
    let b = records.iter().find(|r| r["event_id"] == "$b").unwrap();
    assert_eq!(b["redacted"], true);
    assert!(b["body"].is_null());

    let mut events = Vec::new();
    let count = store.export_events(None, &mut events).unwrap();
    assert_eq!(count, 5);
    let text = String::from_utf8(events).unwrap();
    assert!(!text.contains("secret"), "redacted body leaked into export");
    assert!(text.lines().all(|line| {
        let value: serde_json::Value = serde_json::from_str(line).unwrap();
        value["raw"].is_object()
    }));
}

#[test]
fn read_only_status_and_export_work_on_a_closed_archive() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    {
        let mut store = open_store(&path);
        let mut room = room_update(ROOM);
        room.timeline = vec![message("$m", 100, "hello")];
        room.prev_batch = Some("p1".to_owned());
        store
            .apply_sync_batch(&sync_batch("s1", vec![room]), 10)
            .unwrap();
    }

    let store = mainlinenerd_ingest::store::Store::open_read_only(&path).unwrap();
    let report = store.status().unwrap();
    assert_eq!(report.rooms.len(), 1);
    assert_eq!(report.rooms[0].messages, 1);
    let mut out = Vec::new();
    assert_eq!(store.export_messages(None, &mut out).unwrap(), 1);
}

#[test]
fn status_never_exposes_tokens_or_bodies() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut initial = room_update(ROOM);
    initial.timeline = vec![message("$live", 100, "top secret")];
    initial.prev_batch = Some("opaque-history-token".to_owned());
    store
        .apply_sync_batch(&sync_batch("opaque-sync-token", vec![initial]), 10)
        .unwrap();

    let text = store.render_status().unwrap();
    assert!(!text.contains("opaque-sync-token"));
    assert!(!text.contains("opaque-history-token"));
    assert!(!text.contains("top secret"));

    let json = serde_json::to_string(&store.status().unwrap()).unwrap();
    assert!(!json.contains("opaque-sync-token"));
    assert!(!json.contains("opaque-history-token"));
    assert!(!json.contains("top secret"));
}
