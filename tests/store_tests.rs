//! Integration tests for the durable store: atomicity, resume, projection,
//! redaction suppression and gap bookkeeping.

mod common;

use common::*;
use mainlinenerd_ingest::store::{HistoryStatus, HistoryWork, StoreError};
use serde_json::json;

#[test]
fn failed_schema_bootstrap_rolls_back_entirely() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    {
        // A pre-existing table makes the last CREATE TABLE in the bootstrap
        // fail after earlier statements have already run.
        let conn = db(&path);
        conn.execute_batch("CREATE TABLE current_messages (x INTEGER);")
            .unwrap();
    }

    let error = match mainlinenerd_ingest::store::Store::open(&path, &identity()) {
        Ok(_) => panic!("expected the schema bootstrap to fail"),
        Err(error) => error,
    };
    assert!(matches!(error, StoreError::Sqlite(_)));

    let conn = db(&path);
    assert_eq!(
        scalar_i64(&conn, "PRAGMA user_version"),
        0,
        "a failed bootstrap must not record a schema version"
    );
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'archive_meta'"
        ),
        0,
        "tables created before the failure must roll back"
    );
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'sync_progress'"
        ),
        0
    );
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'gap_jobs'"
        ),
        0
    );
}

#[test]
fn malformed_event_rolls_back_batch_checkpoint_and_new_gaps() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    // Establish a previously committed sync token.
    let mut seed = room_update(ROOM2);
    seed.timeline = vec![message("$seed", 50, "seed")];
    seed.prev_batch = Some("p0".to_owned());
    store
        .apply_sync_batch(&sync_batch("s0", vec![seed]), 5)
        .unwrap();
    assert_eq!(store.since_token().unwrap().as_deref(), Some("s0"));

    // ROOM's limited sync would persist a new bounded gap job; the malformed
    // event in the second room then fails the batch, so events, the new gap
    // and the global next_batch must all roll back together.
    let mut limited = room_update(ROOM);
    limited.timeline = vec![message("$ok", 100, "one")];
    limited.prev_batch = Some("p1".to_owned());
    limited.limited = true;
    let mut bad = room_update("!bad:hs.example.org");
    bad.timeline = vec![json!({"type": "m.room.message"})];
    let error = store
        .apply_sync_batch(&sync_batch("s1", vec![limited, bad]), 10)
        .unwrap_err();
    assert!(matches!(error, StoreError::MalformedEvent { .. }));
    assert_eq!(store.since_token().unwrap().as_deref(), Some("s0"));
    assert!(store.open_gap_positions().unwrap().is_empty());
    drop(store);

    let conn = db(&path);
    assert_eq!(
        scalar_i64(&conn, "SELECT COUNT(*) FROM events"),
        1,
        "only the pre-existing seed event survives"
    );
    assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM rooms"), 1);
    assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM gap_jobs"), 0);
    assert_eq!(
        scalar_string(&conn, "SELECT since_token FROM sync_progress WHERE id = 1"),
        Some("s0".to_owned())
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
fn initial_limited_sync_is_backfill_not_a_gap() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    // The first sync has no previous committed token: its prev_batch seeds the
    // base backfill and no live gap exists yet.
    let mut initial = room_update(ROOM);
    initial.timeline = vec![message("$live", 100, "live")];
    initial.prev_batch = Some("p1".to_owned());
    initial.limited = true;
    store
        .apply_sync_batch(&sync_batch("s1", vec![initial]), 10)
        .unwrap();
    assert_eq!(
        store.room_history_token(ROOM).unwrap().as_deref(),
        Some("p1")
    );
    assert!(!store.room_history_complete(ROOM).unwrap());
    assert!(store.open_gap_positions().unwrap().is_empty());
    assert_eq!(store.status().unwrap().rooms[0].open_gaps, 0);

    // The next limited sync does have a previous token, so it opens a bounded
    // gap covering [s1, p2].
    let mut limited = room_update(ROOM);
    limited.timeline = vec![message("$live2", 300, "live2")];
    limited.prev_batch = Some("p2".to_owned());
    limited.limited = true;
    store
        .apply_sync_batch(&sync_batch("s2", vec![limited]), 20)
        .unwrap();
    let gaps = store.open_gap_positions().unwrap();
    assert_eq!(gaps.len(), 1);
    assert_eq!(gaps[0].token, "p2");
    assert_eq!(gaps[0].to_token.as_deref(), Some("s1"));
    assert_eq!(
        store.room_history_token(ROOM).unwrap().as_deref(),
        Some("p1")
    );
}

#[test]
fn limited_sync_gap_repairs_without_moving_base_history() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    // Initial sync seeds the base backfill cursor with prev_batch.
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

    // A later limited sync creates an independent bounded repair job and must
    // not rewind the in-progress backfill position.
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
        Some("p1"),
        "a limited live batch must not reset archival backfill"
    );

    let gaps = store.open_gap_positions().unwrap();
    assert_eq!(gaps.len(), 1);
    let gap_id = gaps[0].gap_id;
    assert_eq!(gaps[0].room_id, ROOM);
    assert_eq!(gaps[0].token, "p2");
    assert_eq!(gaps[0].to_token.as_deref(), Some("s1"));
    {
        let conn = db(&path);
        assert_eq!(
            conn.query_row(
                "SELECT boundary_token FROM gap_jobs WHERE gap_id = ?1",
                [gap_id],
                |row| row.get::<_, Option<String>>(0),
            )
            .unwrap(),
            Some("s1".to_owned())
        );
        assert_eq!(
            conn.query_row(
                "SELECT cursor_token FROM gap_jobs WHERE gap_id = ?1",
                [gap_id],
                |row| row.get::<_, Option<String>>(0),
            )
            .unwrap(),
            Some("p2".to_owned())
        );
    }

    // Repair only the gap: p2 -> p3, then p3 -> its boundary s1.
    let outcome = store
        .apply_history_page(
            ROOM,
            HistoryWork::Gap(gap_id),
            "p2",
            &history_page("p2", Some("p3"), vec![message("$gap1", 200, "gap1")]),
            30,
        )
        .unwrap();
    assert_eq!(outcome.status, Some(HistoryStatus::Advanced));
    assert_eq!(
        store.room_history_token(ROOM).unwrap().as_deref(),
        Some("p1"),
        "gap repair must not move the base cursor"
    );
    assert_eq!(store.open_gap_positions().unwrap()[0].token, "p3");

    let outcome = store
        .apply_history_page(
            ROOM,
            HistoryWork::Gap(gap_id),
            "p3",
            &history_page("p3", Some("s1"), vec![message("$gap0", 50, "gap0")]),
            40,
        )
        .unwrap();
    assert_eq!(outcome.status, Some(HistoryStatus::Completed));
    assert!(store.open_gap_positions().unwrap().is_empty());
    assert!(!store.room_history_complete(ROOM).unwrap());

    let conn = db(&path);
    assert_eq!(
        conn.query_row(
            "SELECT close_reason FROM gap_jobs WHERE gap_id = ?1",
            [gap_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .unwrap(),
        Some("token".to_owned())
    );
    assert_eq!(
        scalar_i64(&conn, "SELECT COUNT(*) FROM current_messages"),
        4,
        "live, live2 and both gap events must be present"
    );

    // Base backfill continues from its own cursor, unaffected by the repair.
    let outcome = store
        .apply_history_page(
            ROOM,
            HistoryWork::Base,
            "p1",
            &history_page("p1", None, vec![message("$old", 10, "old")]),
            50,
        )
        .unwrap();
    assert_eq!(outcome.status, Some(HistoryStatus::Completed));
    assert!(store.room_history_complete(ROOM).unwrap());
}

#[test]
fn limited_sync_without_prev_batch_stays_unresolved() {
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
    limited.prev_batch = None;
    limited.limited = true;
    store
        .apply_sync_batch(&sync_batch("s2", vec![limited]), 20)
        .unwrap();

    assert!(store.open_gap_positions().unwrap().is_empty());
    assert_eq!(
        store.room_history_token(ROOM).unwrap().as_deref(),
        Some("p1"),
        "an unresolvable gap must not touch the base cursor"
    );
    let report = store.status().unwrap();
    assert_eq!(report.rooms[0].open_gaps, 0);
    assert_eq!(report.rooms[0].unresolved_gaps, 1);
    let conn = db(&path);
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT status FROM gap_jobs WHERE reason = 'limited_sync_no_prev_batch'"
        ),
        Some("unresolved".to_owned())
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT close_reason FROM gap_jobs WHERE reason = 'limited_sync_no_prev_batch'"
        ),
        Some("no repair token available".to_owned())
    );
}

#[test]
fn completed_base_history_can_still_have_open_gap_repair() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut initial = room_update(ROOM);
    initial.timeline = vec![message("$live", 100, "live")];
    initial.prev_batch = Some("p1".to_owned());
    store
        .apply_sync_batch(&sync_batch("s1", vec![initial]), 10)
        .unwrap();
    let outcome = store
        .apply_history_page(
            ROOM,
            HistoryWork::Base,
            "p1",
            &history_page("p1", None, vec![]),
            20,
        )
        .unwrap();
    assert_eq!(outcome.status, Some(HistoryStatus::Completed));

    let mut limited = room_update(ROOM);
    limited.timeline = vec![message("$live2", 300, "live2")];
    limited.prev_batch = Some("p2".to_owned());
    limited.limited = true;
    store
        .apply_sync_batch(&sync_batch("s2", vec![limited]), 30)
        .unwrap();

    assert!(store.room_history_complete(ROOM).unwrap());
    assert_eq!(store.rooms_needing_history().unwrap().len(), 0);
    let gaps = store.open_gap_positions().unwrap();
    assert_eq!(gaps.len(), 1, "completed history does not close open gaps");
    assert_eq!(gaps[0].token, "p2");
    assert_eq!(gaps[0].to_token.as_deref(), Some("s1"));
}

#[test]
fn two_limited_syncs_create_independently_resumable_jobs() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");

    {
        let mut store = open_store(&path);
        let mut initial = room_update(ROOM);
        initial.timeline = vec![message("$live", 100, "live")];
        initial.prev_batch = Some("p0".to_owned());
        store
            .apply_sync_batch(&sync_batch("s1", vec![initial]), 10)
            .unwrap();
        assert_eq!(
            store.room_history_token(ROOM).unwrap().as_deref(),
            Some("p0")
        );

        for (next_batch, prev_batch, at) in [("s2", "p1", 20), ("s3", "p2", 30)] {
            let mut limited = room_update(ROOM);
            limited.timeline = vec![message(&format!("$live-{prev_batch}"), 300, "live")];
            limited.prev_batch = Some(prev_batch.to_owned());
            limited.limited = true;
            store
                .apply_sync_batch(&sync_batch(next_batch, vec![limited]), at)
                .unwrap();
        }

        let gaps = store.open_gap_positions().unwrap();
        assert_eq!(gaps.len(), 2);
        assert_eq!(gaps[0].to_token.as_deref(), Some("s1"));
        assert_eq!(gaps[1].to_token.as_deref(), Some("s2"));
        assert_eq!(gaps[0].token, "p1");
        assert_eq!(gaps[1].token, "p2");

        // Advance each job independently; an empty middle page still moves the
        // job's own cursor.
        let outcome = store
            .apply_history_page(
                ROOM,
                HistoryWork::Gap(gaps[0].gap_id),
                "p1",
                &history_page("p1", Some("q1"), vec![]),
                40,
            )
            .unwrap();
        assert_eq!(outcome.status, Some(HistoryStatus::Advanced));
        let outcome = store
            .apply_history_page(
                ROOM,
                HistoryWork::Gap(gaps[1].gap_id),
                "p2",
                &history_page("p2", Some("q2"), vec![]),
                50,
            )
            .unwrap();
        assert_eq!(outcome.status, Some(HistoryStatus::Advanced));
    }

    // Reopen: both cursors resume exactly where they stopped.
    let mut store = open_store(&path);
    assert_eq!(
        store.room_history_token(ROOM).unwrap().as_deref(),
        Some("p0"),
        "reopening resumes base backfill from its persisted cursor"
    );
    let gaps = store.open_gap_positions().unwrap();
    assert_eq!(gaps.len(), 2);
    assert_eq!(gaps[0].token, "q1");
    assert_eq!(gaps[1].token, "q2");

    // Exhausting one job must not close the other.
    let outcome = store
        .apply_history_page(
            ROOM,
            HistoryWork::Gap(gaps[0].gap_id),
            "q1",
            &history_page("q1", None, vec![message("$a", 400, "a")]),
            60,
        )
        .unwrap();
    assert_eq!(outcome.status, Some(HistoryStatus::Completed));
    assert_eq!(store.open_gap_positions().unwrap().len(), 1);
    assert_eq!(store.open_gap_positions().unwrap()[0].token, "q2");

    let outcome = store
        .apply_history_page(
            ROOM,
            HistoryWork::Gap(gaps[1].gap_id),
            "q2",
            &history_page("q2", Some("s2"), vec![message("$b", 410, "b")]),
            70,
        )
        .unwrap();
    assert_eq!(outcome.status, Some(HistoryStatus::Completed));
    assert!(store.open_gap_positions().unwrap().is_empty());
}

#[test]
fn stale_history_response_cannot_overwrite_newer_cursor() {
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
    let gap_id = store.open_gap_positions().unwrap()[0].gap_id;

    // The base cursor advances p1 -> p2.
    let outcome = store
        .apply_history_page(
            ROOM,
            HistoryWork::Base,
            "p1",
            &history_page("p1", Some("p2b"), vec![message("$new", 200, "new")]),
            30,
        )
        .unwrap();
    assert_eq!(outcome.status, Some(HistoryStatus::Advanced));

    // A late page for the old p1 must not move the base cursor or store events.
    let events_before = {
        let conn = db(&path);
        scalar_i64(&conn, "SELECT COUNT(*) FROM events")
    };
    let outcome = store
        .apply_history_page(
            ROOM,
            HistoryWork::Base,
            "p1",
            &history_page("p1", Some("stale"), vec![message("$stale", 210, "stale")]),
            40,
        )
        .unwrap();
    assert_eq!(outcome.status, Some(HistoryStatus::Stale));
    assert_eq!(
        store.room_history_token(ROOM).unwrap().as_deref(),
        Some("p2b")
    );
    {
        let conn = db(&path);
        assert_eq!(
            scalar_i64(&conn, "SELECT COUNT(*) FROM events"),
            events_before,
            "a stale base page must not store events"
        );
    }

    // The gap cursor advances p2 -> p3; a late page for p2 is stale too.
    let outcome = store
        .apply_history_page(
            ROOM,
            HistoryWork::Gap(gap_id),
            "p2",
            &history_page("p2", Some("p3"), vec![message("$gap", 220, "gap")]),
            50,
        )
        .unwrap();
    assert_eq!(outcome.status, Some(HistoryStatus::Advanced));
    let events_before_stale_gap = {
        let conn = db(&path);
        scalar_i64(&conn, "SELECT COUNT(*) FROM events")
    };
    let outcome = store
        .apply_history_page(
            ROOM,
            HistoryWork::Gap(gap_id),
            "p2",
            &history_page("p2", Some("stale"), vec![message("$stale2", 230, "stale2")]),
            60,
        )
        .unwrap();
    assert_eq!(outcome.status, Some(HistoryStatus::Stale));
    assert_eq!(store.open_gap_positions().unwrap()[0].token, "p3");

    let conn = db(&path);
    assert_eq!(
        scalar_i64(&conn, "SELECT COUNT(*) FROM events"),
        events_before_stale_gap,
        "a stale gap page must not store events"
    );
}

#[test]
fn gap_history_start_closes_only_the_selected_gap() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut initial = room_update(ROOM);
    initial.timeline = vec![message("$live", 100, "live")];
    initial.prev_batch = Some("p0".to_owned());
    store
        .apply_sync_batch(&sync_batch("s1", vec![initial]), 10)
        .unwrap();
    for (next_batch, prev_batch, at) in [("s2", "p1", 20), ("s3", "p2", 30)] {
        let mut limited = room_update(ROOM);
        limited.timeline = vec![message(&format!("$live-{prev_batch}"), 300, "live")];
        limited.prev_batch = Some(prev_batch.to_owned());
        limited.limited = true;
        store
            .apply_sync_batch(&sync_batch(next_batch, vec![limited]), at)
            .unwrap();
    }
    let gaps = store.open_gap_positions().unwrap();
    assert_eq!(gaps.len(), 2);

    // Walking gap 0 back to the start of history closes only that job: the
    // other gap and the base backfill are untouched.
    let outcome = store
        .apply_history_page(
            ROOM,
            HistoryWork::Gap(gaps[0].gap_id),
            "p1",
            &history_page("p1", None, vec![message("$start", 1, "start")]),
            40,
        )
        .unwrap();
    assert_eq!(outcome.status, Some(HistoryStatus::Completed));

    let remaining = store.open_gap_positions().unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].gap_id, gaps[1].gap_id);
    assert_eq!(remaining[0].token, "p2");
    assert!(!store.room_history_complete(ROOM).unwrap());
    assert_eq!(
        store.room_history_token(ROOM).unwrap().as_deref(),
        Some("p0")
    );
    let conn = db(&path);
    assert_eq!(
        conn.query_row(
            "SELECT close_reason FROM gap_jobs WHERE gap_id = ?1",
            [gaps[0].gap_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .unwrap(),
        Some("history_start".to_owned())
    );
}

#[test]
fn base_history_completion_does_not_close_open_gaps() {
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
            HistoryWork::Base,
            "p1",
            &history_page("p1", None, vec![message("$old", 10, "old")]),
            30,
        )
        .unwrap();
    assert_eq!(outcome.status, Some(HistoryStatus::Completed));
    assert!(store.room_history_complete(ROOM).unwrap());
    let gaps = store.open_gap_positions().unwrap();
    assert_eq!(gaps.len(), 1, "base completion must not close gap jobs");
    assert_eq!(gaps[0].token, "p2");
}

#[test]
fn malformed_gap_page_rolls_back_cursor_and_events() {
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
    let gap_id = store.open_gap_positions().unwrap()[0].gap_id;

    let error = store
        .apply_history_page(
            ROOM,
            HistoryWork::Gap(gap_id),
            "p2",
            &history_page(
                "p2",
                Some("p3"),
                vec![message("$ok", 200, "ok"), json!({"type": "m.room.message"})],
            ),
            30,
        )
        .unwrap_err();
    assert!(matches!(error, StoreError::MalformedEvent { .. }));
    assert_eq!(store.open_gap_positions().unwrap()[0].token, "p2");
    let conn = db(&path);
    assert_eq!(
        scalar_i64(&conn, "SELECT COUNT(*) FROM events"),
        2,
        "only the two live events survive the rolled-back gap page"
    );
}

#[test]
fn base_stalled_history_token_is_persisted_and_reported() {
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

    // Empty page returning the same token makes no progress: stall the base
    // backfill, do not loop, and leave the independent gap untouched.
    let outcome = store
        .apply_history_page(
            ROOM,
            HistoryWork::Base,
            "p1",
            &history_page("p1", Some("p1"), vec![]),
            30,
        )
        .unwrap();
    assert_eq!(outcome.status, Some(HistoryStatus::Stalled));
    assert_eq!(
        store.room_history_token(ROOM).unwrap().as_deref(),
        Some("p1")
    );

    let report = store.status().unwrap();
    let room = &report.rooms[0];
    assert!(room.history_stalled);
    assert_eq!(
        room.history_error.as_deref(),
        Some("history pagination returned a repeated token")
    );
    assert_eq!(room.open_gaps, 1, "the gap job is independent");
    assert_eq!(room.unresolved_gaps, 0);
}

#[test]
fn repeated_gap_token_unresolves_only_that_gap() {
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
    let gap_id = store.open_gap_positions().unwrap()[0].gap_id;

    // A repeated token on a gap job closes only that job: the base backfill is
    // not stalled and its cursor does not move.
    let outcome = store
        .apply_history_page(
            ROOM,
            HistoryWork::Gap(gap_id),
            "p2",
            &history_page("p2", Some("p2"), vec![]),
            30,
        )
        .unwrap();
    assert_eq!(outcome.status, Some(HistoryStatus::Stalled));
    assert!(!store.room_history_stalled(ROOM).unwrap());
    assert_eq!(
        store.room_history_token(ROOM).unwrap().as_deref(),
        Some("p1")
    );

    let report = store.status().unwrap();
    assert_eq!(report.rooms[0].open_gaps, 0);
    assert_eq!(report.rooms[0].unresolved_gaps, 1);
    let conn = db(&path);
    assert_eq!(
        conn.query_row(
            "SELECT close_reason FROM gap_jobs WHERE gap_id = ?1",
            [gap_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .unwrap(),
        Some("history pagination returned a repeated token".to_owned())
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
            HistoryWork::Base,
            "p1",
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

#[test]
fn clock_skewed_replacement_is_not_discarded() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    // Every replacement predates the original's timestamp; the latest
    // replacement by its own (timestamp, event id) still wins.
    let mut room = room_update(ROOM);
    room.timeline = vec![
        create_room("11"),
        message("$orig", 500, "original"),
        edit("$older", 50, "$orig", "older edit"),
        edit("$skewed", 100, "$orig", "skewed edit"),
    ];
    store
        .apply_sync_batch(&sync_batch("s1", vec![room]), 10)
        .unwrap();

    let conn = db(&path);
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body FROM current_messages WHERE event_id = '$orig'"
        ),
        Some("skewed edit".to_owned())
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT latest_edit_event_id FROM current_messages WHERE event_id = '$orig'"
        ),
        Some("$skewed".to_owned())
    );
}

#[test]
fn replacement_tie_breaks_by_event_id() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut room = room_update(ROOM);
    room.timeline = vec![
        create_room("11"),
        message("$orig", 500, "original"),
        edit("$tie-a", 100, "$orig", "from a"),
        edit("$tie-b", 100, "$orig", "from b"),
    ];
    store
        .apply_sync_batch(&sync_batch("s1", vec![room]), 10)
        .unwrap();

    let conn = db(&path);
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body FROM current_messages WHERE event_id = '$orig'"
        ),
        Some("from b".to_owned())
    );
}

#[test]
fn replacements_do_not_match_missing_senders() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut room = room_update(ROOM);
    room.timeline = vec![
        json!({
            "type": "m.room.message",
            "event_id": "$anon",
            "origin_server_ts": 100,
            "content": { "msgtype": "m.text", "body": "anon original" }
        }),
        json!({
            "type": "m.room.message",
            "event_id": "$anon-edit",
            "origin_server_ts": 200,
            "content": {
                "msgtype": "m.text",
                "body": "* anon edited",
                "m.new_content": { "msgtype": "m.text", "body": "anon edited" },
                "m.relates_to": { "rel_type": "m.replace", "event_id": "$anon" }
            }
        }),
    ];
    store
        .apply_sync_batch(&sync_batch("s1", vec![room]), 10)
        .unwrap();

    let conn = db(&path);
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body FROM current_messages WHERE event_id = '$anon'"
        ),
        Some("anon original".to_owned())
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT latest_edit_event_id FROM current_messages WHERE event_id = '$anon'"
        ),
        None
    );
}

#[test]
fn replacement_without_msgtype_is_never_projected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut room = room_update(ROOM);
    room.timeline = vec![
        message("$orig", 100, "original"),
        json!({
            "type": "m.room.message",
            "event_id": "$bad",
            "sender": ALICE,
            "origin_server_ts": 200,
            "content": {
                "msgtype": "m.text",
                "body": "* no msgtype",
                "m.new_content": { "body": "no msgtype" },
                "m.relates_to": { "rel_type": "m.replace", "event_id": "$orig" }
            }
        }),
    ];
    store
        .apply_sync_batch(&sync_batch("s1", vec![room]), 10)
        .unwrap();

    let conn = db(&path);
    assert_eq!(
        scalar_i64(&conn, "SELECT COUNT(*) FROM current_messages"),
        1,
        "an invalid replacement must not become a message of its own"
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body FROM current_messages WHERE event_id = '$orig'"
        ),
        Some("original".to_owned())
    );
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT edit_attempt FROM events WHERE event_id = '$bad'"
        ),
        1
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT edit_target FROM events WHERE event_id = '$bad'"
        ),
        None
    );
}

#[test]
fn redacting_original_suppresses_related_edit_bodies() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut room = room_update(ROOM);
    room.timeline = vec![
        create_room("11"),
        message("$orig", 100, "original body"),
        edit("$edit", 150, "$orig", "edited secret"),
        redaction("$red", 200, "$orig", true),
    ];
    store
        .apply_sync_batch(&sync_batch("s1", vec![room]), 10)
        .unwrap();

    {
        let conn = db(&path);
        assert_eq!(
            scalar_string(
                &conn,
                "SELECT body_text FROM events WHERE event_id = '$edit'"
            ),
            None
        );
        let raw = scalar_string(
            &conn,
            "SELECT raw_json FROM events WHERE event_id = '$edit'",
        )
        .expect("edit raw stored");
        assert!(!raw.contains("edited secret"), "edit body leaked: {raw}");
        assert_eq!(
            scalar_string(
                &conn,
                "SELECT edit_target FROM events WHERE event_id = '$edit'"
            )
            .as_deref(),
            Some("$orig"),
            "relation provenance must survive so replay is suppressed again"
        );
    }

    let mut events = Vec::new();
    store.export_events(None, &mut events).unwrap();
    let events = String::from_utf8(events).unwrap();
    assert!(
        !events.contains("edited secret"),
        "edit body leaked into export"
    );

    let mut messages = Vec::new();
    store.export_messages(None, &mut messages).unwrap();
    let messages = String::from_utf8(messages).unwrap();
    assert!(!messages.contains("edited secret"));
}

#[test]
fn edit_arriving_after_original_redaction_is_suppressed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    // The redaction is already known before the edit is backfilled.
    let mut first = room_update(ROOM);
    first.timeline = vec![
        create_room("11"),
        message("$orig", 100, "original body"),
        redaction("$red", 120, "$orig", true),
    ];
    store
        .apply_sync_batch(&sync_batch("s1", vec![first]), 10)
        .unwrap();

    let mut backfill = room_update(ROOM);
    backfill.timeline = vec![edit("$edit", 150, "$orig", "late secret")];
    store
        .apply_sync_batch(&sync_batch("s2", vec![backfill]), 20)
        .unwrap();
    {
        let conn = db(&path);
        assert_eq!(
            scalar_string(
                &conn,
                "SELECT body_text FROM events WHERE event_id = '$edit'"
            ),
            None
        );
        let raw = scalar_string(
            &conn,
            "SELECT raw_json FROM events WHERE event_id = '$edit'",
        )
        .expect("edit raw stored");
        assert!(!raw.contains("late secret"), "late edit body leaked: {raw}");
    }

    // Replaying the edit must not restore the body either.
    let mut replay = room_update(ROOM);
    replay.timeline = vec![edit("$edit", 150, "$orig", "late secret")];
    store
        .apply_sync_batch(&sync_batch("s3", vec![replay]), 30)
        .unwrap();
    {
        let conn = db(&path);
        assert_eq!(
            scalar_string(
                &conn,
                "SELECT body_text FROM events WHERE event_id = '$edit'"
            ),
            None
        );
        assert_eq!(
            scalar_string(
                &conn,
                "SELECT raw_json FROM events WHERE event_id = '$edit'"
            )
            .map(|raw| raw.contains("late secret")),
            Some(false)
        );
    }
}

#[test]
fn already_redacted_original_suppresses_stored_edits() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    // The edit arrives first, then the original is returned already redacted.
    let mut first = room_update(ROOM);
    first.timeline = vec![edit("$edit", 150, "$orig", "early secret")];
    store
        .apply_sync_batch(&sync_batch("s1", vec![first]), 10)
        .unwrap();
    assert_eq!(
        scalar_string(
            &db(&path),
            "SELECT body_text FROM events WHERE event_id = '$edit'"
        ),
        Some("early secret".to_owned())
    );

    let mut second = room_update(ROOM);
    second.timeline = vec![already_redacted_message("$orig", 100)];
    store
        .apply_sync_batch(&sync_batch("s2", vec![second]), 20)
        .unwrap();

    let conn = db(&path);
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body_text FROM events WHERE event_id = '$edit'"
        ),
        None
    );
    let raw = scalar_string(
        &conn,
        "SELECT raw_json FROM events WHERE event_id = '$edit'",
    )
    .expect("edit raw stored");
    assert!(
        !raw.contains("early secret"),
        "early edit body leaked: {raw}"
    );
}

#[test]
fn replacement_suppression_is_room_scoped() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut redacted_room = room_update(ROOM);
    redacted_room.timeline = vec![
        create_room("11"),
        message("$orig", 100, "one"),
        redaction("$red", 120, "$orig", true),
    ];
    // The other room reuses the event id as an edit target; it must survive.
    let mut other_room = room_update(ROOM2);
    other_room.timeline = vec![
        create_room("11"),
        edit("$edit2", 150, "$orig", "other room secret"),
    ];
    store
        .apply_sync_batch(&sync_batch("s1", vec![redacted_room, other_room]), 10)
        .unwrap();

    let conn = db(&path);
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body_text FROM events WHERE room_id = '!room2:hs.example.org' AND event_id = '$edit2'"
        ),
        Some("other room secret".to_owned())
    );
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM events WHERE room_id = '!room:hs.example.org' AND event_id = '$edit2'"
        ),
        0,
        "the other room's edit must not appear in the redacted room"
    );
}

#[test]
fn forged_non_state_policy_events_do_not_change_room_flags() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut room = room_update(ROOM);
    room.timeline = vec![
        json!({
            "type": "m.room.create", "event_id": "$fake-create", "sender": ALICE,
            "origin_server_ts": 1,
            "content": { "creator": ALICE, "room_version": "11" }
        }),
        json!({
            "type": "m.room.encryption", "event_id": "$fake-enc", "sender": ALICE,
            "origin_server_ts": 2,
            "content": { "algorithm": "m.megolm.v1.aes-sha2" }
        }),
        json!({
            "type": "m.room.tombstone", "event_id": "$fake-tomb", "sender": ALICE,
            "origin_server_ts": 3,
            "content": { "body": "upgraded", "replacement_room": "!next:hs.example.org" }
        }),
        json!({
            "type": "m.room.encrypted", "event_id": "$cipher", "sender": ALICE,
            "origin_server_ts": 4,
            "content": { "algorithm": "m.megolm.v1.aes-sha2", "ciphertext": "opaque" }
        }),
    ];
    store
        .apply_sync_batch(&sync_batch("s1", vec![room]), 10)
        .unwrap();

    let report = store.status().unwrap();
    let status = &report.rooms[0];
    assert!(
        !status.encrypted,
        "a non-state encryption event must not flag E2EE"
    );
    assert_eq!(status.successor_room_id, None);
    assert_eq!(status.room_version, None);
    assert!(!status.needs_operator_action);

    let conn = db(&path);
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT encrypted FROM events WHERE event_id = '$cipher'"
        ),
        1,
        "the encrypted event itself stays opaque"
    );
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT encrypted FROM current_messages WHERE event_id = '$cipher'"
        ),
        1
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body FROM current_messages WHERE event_id = '$cipher'"
        ),
        None
    );
}

#[test]
fn genuine_state_policy_events_still_set_room_flags() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut room = room_update(ROOM);
    room.state = vec![
        create_room("11"),
        encryption_event(),
        tombstone("!next:hs.example.org"),
    ];
    room.timeline = vec![message("$m", 100, "hello")];
    store
        .apply_sync_batch(&sync_batch("s1", vec![room]), 10)
        .unwrap();

    let report = store.status().unwrap();
    let status = &report.rooms[0];
    assert!(status.encrypted);
    assert!(status.needs_operator_action);
    assert_eq!(
        status.successor_room_id.as_deref(),
        Some("!next:hs.example.org")
    );
    assert_eq!(status.room_version.as_deref(), Some("11"));
    let conn = db(&path);
    assert_eq!(
        scalar_i64(&conn, "SELECT encrypted FROM events WHERE event_id = '$m'"),
        0,
        "a plain message in an encrypted room is not itself a ciphertext event"
    );
}

#[test]
fn isolated_encrypted_timeline_event_never_enables_room_e2ee() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut room = room_update(ROOM);
    room.timeline = vec![
        create_room("11"),
        json!({
            "type": "m.room.encrypted", "event_id": "$cipher", "sender": ALICE,
            "origin_server_ts": 4,
            "content": { "algorithm": "m.megolm.v1.aes-sha2", "ciphertext": "opaque" }
        }),
    ];
    store
        .apply_sync_batch(&sync_batch("s1", vec![room]), 10)
        .unwrap();

    let report = store.status().unwrap();
    assert!(!report.rooms[0].encrypted);
    let conn = db(&path);
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT encrypted FROM current_messages WHERE event_id = '$cipher'"
        ),
        1
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body FROM current_messages WHERE event_id = '$cipher'"
        ),
        None
    );
}

#[test]
fn bundled_replacement_is_projected_and_redaction_removes_every_copy() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut room = room_update(ROOM);
    room.timeline = vec![
        create_room("11"),
        message_with_bundled_edit(
            "$orig",
            100,
            "original body",
            "$bundled-edit",
            150,
            "bundled secret",
        ),
    ];
    store
        .apply_sync_batch(&sync_batch("s1", vec![room]), 10)
        .unwrap();

    {
        let conn = db(&path);
        assert_eq!(
            scalar_i64(
                &conn,
                "SELECT COUNT(*) FROM events WHERE event_id = '$bundled-edit'"
            ),
            1,
            "a valid bundled replacement becomes its own event"
        );
        assert_eq!(
            scalar_string(
                &conn,
                "SELECT source FROM events WHERE event_id = '$bundled-edit'"
            )
            .as_deref(),
            Some("bundle")
        );
        assert_eq!(
            scalar_string(
                &conn,
                "SELECT edit_target FROM events WHERE event_id = '$bundled-edit'"
            )
            .as_deref(),
            Some("$orig")
        );
        assert_eq!(
            scalar_string(
                &conn,
                "SELECT body FROM current_messages WHERE event_id = '$orig'"
            ),
            Some("bundled secret".to_owned()),
            "the bundled replacement is the effective message"
        );
        let raw = scalar_string(
            &conn,
            "SELECT raw_json FROM events WHERE event_id = '$orig'",
        )
        .expect("original raw stored");
        assert!(
            !raw.contains("bundled secret"),
            "bundle cached inside the parent raw: {raw}"
        );
        assert!(
            !raw.contains("m.relations"),
            "unsigned relation cache survived: {raw}"
        );
    }

    // Redact the replacement, not the original.
    let mut later = room_update(ROOM);
    later.timeline = vec![redaction("$red", 200, "$bundled-edit", true)];
    store
        .apply_sync_batch(&sync_batch("s2", vec![later]), 20)
        .unwrap();

    {
        let conn = db(&path);
        assert_eq!(
            scalar_string(
                &conn,
                "SELECT body FROM current_messages WHERE event_id = '$orig'"
            ),
            Some("original body".to_owned()),
            "redacting the bundled replacement falls back to the original"
        );
        assert_eq!(
            scalar_i64(
                &conn,
                "SELECT redacted FROM events WHERE event_id = '$bundled-edit'"
            ),
            1
        );
        assert_eq!(
            scalar_string(
                &conn,
                "SELECT body_text FROM events WHERE event_id = '$bundled-edit'"
            ),
            None
        );
    }

    let mut events = Vec::new();
    store.export_events(None, &mut events).unwrap();
    let text = String::from_utf8(events).unwrap();
    assert!(
        !text.contains("bundled secret"),
        "removed text leaked into raw export"
    );
    assert!(
        text.contains("original body"),
        "the original's own body must remain"
    );
}

#[test]
fn invalid_bundled_replacements_never_become_events_or_touch_originals() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let parent = |id: &str, ts: i64, body: &str, bundle: serde_json::Value| -> serde_json::Value {
        json!({
            "type": "m.room.message",
            "event_id": id,
            "sender": ALICE,
            "origin_server_ts": ts,
            "content": { "msgtype": "m.text", "body": body },
            "unsigned": { "m.relations": { "m.replace": bundle } }
        })
    };
    let edit_content = |new_body: &str, target: &str| {
        json!({
            "msgtype": "m.text",
            "body": format!("* {new_body}"),
            "m.new_content": { "msgtype": "m.text", "body": new_body },
            "m.relates_to": { "rel_type": "m.replace", "event_id": target }
        })
    };

    let mut room = room_update(ROOM);
    room.timeline = vec![
        create_room("11"),
        parent(
            "$orig-sender",
            100,
            "sender original",
            json!({
                "type": "m.room.message", "event_id": "$bad-sender", "sender": BOB,
                "origin_server_ts": 150,
                "content": edit_content("bad sender secret", "$orig-sender")
            }),
        ),
        parent(
            "$orig-target",
            200,
            "target original",
            json!({
                "type": "m.room.message", "event_id": "$bad-target", "sender": ALICE,
                "origin_server_ts": 250,
                "content": edit_content("bad target secret", "$elsewhere")
            }),
        ),
        parent(
            "$orig-noid",
            300,
            "id original",
            json!({
                "type": "m.room.message", "sender": ALICE,
                "origin_server_ts": 350,
                "content": edit_content("missing id secret", "$orig-noid")
            }),
        ),
        parent(
            "$orig-room",
            400,
            "room original",
            json!({
                "type": "m.room.message", "event_id": "$bad-room", "sender": ALICE,
                "room_id": "!other:hs.example.org",
                "origin_server_ts": 450,
                "content": edit_content("wrong room secret", "$orig-room")
            }),
        ),
        parent(
            "$orig-msgtype",
            500,
            "msgtype original",
            json!({
                "type": "m.room.message", "event_id": "$bad-msgtype", "sender": ALICE,
                "origin_server_ts": 550,
                "content": {
                    "msgtype": "m.text",
                    "body": "* no msgtype",
                    "m.new_content": { "body": "missing msgtype secret" },
                    "m.relates_to": { "rel_type": "m.replace", "event_id": "$orig-msgtype" }
                }
            }),
        ),
        parent(
            "$orig-room-type",
            450,
            "room type original",
            json!({
                "type": "m.room.message", "event_id": "$bad-room-type", "sender": ALICE,
                "room_id": 42,
                "origin_server_ts": 475,
                "content": edit_content("non-string room secret", "$orig-room-type")
            }),
        ),
        parent(
            "$orig-nested",
            600,
            "nested original",
            json!({
                "type": "m.room.message", "event_id": "$outer-edit", "sender": ALICE,
                "origin_server_ts": 650,
                "content": edit_content("outer secret", "$orig-nested"),
                "unsigned": {
                    "m.relations": {
                        "m.replace": {
                            "type": "m.room.message", "event_id": "$nested-edit", "sender": ALICE,
                            "origin_server_ts": 660,
                            "content": edit_content("nested secret", "$outer-edit")
                        }
                    }
                }
            }),
        ),
    ];
    store
        .apply_sync_batch(&sync_batch("s1", vec![room]), 10)
        .unwrap();

    let conn = db(&path);
    for bad in [
        "$bad-sender",
        "$bad-target",
        "$bad-noid",
        "$bad-room",
        "$bad-room-type",
        "$bad-msgtype",
        "$nested-edit",
    ] {
        assert_eq!(
            scalar_i64(
                &conn,
                &format!("SELECT COUNT(*) FROM events WHERE event_id = '{bad}'")
            ),
            0,
            "invalid bundle {bad} must not become an event"
        );
    }
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM events WHERE event_id = '$outer-edit'"
        ),
        1,
        "a valid bundle is still ingested"
    );
    for (orig, body) in [
        ("$orig-sender", "sender original"),
        ("$orig-target", "target original"),
        ("$orig-noid", "id original"),
        ("$orig-room", "room original"),
        ("$orig-room-type", "room type original"),
        ("$orig-msgtype", "msgtype original"),
    ] {
        assert_eq!(
            scalar_string(
                &conn,
                &format!("SELECT body FROM current_messages WHERE event_id = '{orig}'")
            ),
            Some(body.to_owned()),
            "{orig} must keep its own body"
        );
    }
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body FROM current_messages WHERE event_id = '$orig-nested'"
        ),
        Some("outer secret".to_owned())
    );
    for (id, secret) in [
        ("$orig-sender", "bad sender secret"),
        ("$orig-target", "bad target secret"),
        ("$orig-noid", "missing id secret"),
        ("$orig-room", "wrong room secret"),
        ("$orig-room-type", "non-string room secret"),
        ("$orig-msgtype", "missing msgtype secret"),
        ("$orig-nested", "nested secret"),
    ] {
        let raw = scalar_string(
            &conn,
            &format!("SELECT raw_json FROM events WHERE event_id = '{id}'"),
        )
        .expect("original raw stored");
        assert!(
            !raw.contains(secret),
            "{id} raw leaked a dropped bundle body: {raw}"
        );
    }
}

#[test]
fn redacted_original_suppresses_its_bundled_replacement() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    // The original is redacted before it is ever archived with its bundle.
    let mut first = room_update(ROOM);
    first.timeline = vec![create_room("11"), redaction("$red", 120, "$orig", true)];
    store
        .apply_sync_batch(&sync_batch("s1", vec![first]), 10)
        .unwrap();

    let mut second = room_update(ROOM);
    second.timeline = vec![message_with_bundled_edit(
        "$orig",
        100,
        "original body",
        "$bundled-edit",
        150,
        "bundled secret",
    )];
    store
        .apply_sync_batch(&sync_batch("s2", vec![second]), 20)
        .unwrap();

    let conn = db(&path);
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM events WHERE event_id = '$orig' AND redacted = 1"
        ),
        1
    );
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM events WHERE event_id = '$bundled-edit' AND redacted = 1"
        ),
        1,
        "a bundled replacement of a redacted original is stored suppressed"
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body_text FROM events WHERE event_id = '$bundled-edit'"
        ),
        None
    );
    let raw = scalar_string(
        &conn,
        "SELECT raw_json FROM events WHERE event_id = '$bundled-edit'",
    )
    .expect("bundled row stored");
    assert!(
        !raw.contains("bundled secret"),
        "suppressed raw leaked: {raw}"
    );

    let mut events = Vec::new();
    store.export_events(None, &mut events).unwrap();
    let text = String::from_utf8(events).unwrap();
    assert!(!text.contains("bundled secret"));
}

#[test]
fn redacting_original_after_bundle_suppresses_the_bundle() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut first = room_update(ROOM);
    first.timeline = vec![
        create_room("11"),
        message_with_bundled_edit(
            "$orig",
            100,
            "original body",
            "$bundled-edit",
            150,
            "bundled secret",
        ),
    ];
    store
        .apply_sync_batch(&sync_batch("s1", vec![first]), 10)
        .unwrap();
    assert_eq!(
        scalar_string(
            &db(&path),
            "SELECT body FROM current_messages WHERE event_id = '$orig'"
        ),
        Some("bundled secret".to_owned())
    );

    // Redacting the original must cascade to the replacement recovered from
    // the bundle, exactly like a standalone edit.
    let mut later = room_update(ROOM);
    later.timeline = vec![redaction("$red", 200, "$orig", true)];
    store
        .apply_sync_batch(&sync_batch("s2", vec![later]), 20)
        .unwrap();

    let conn = db(&path);
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT redacted FROM events WHERE event_id = '$bundled-edit'"
        ),
        1
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body_text FROM events WHERE event_id = '$bundled-edit'"
        ),
        None
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body FROM current_messages WHERE event_id = '$orig'"
        ),
        None
    );
    let mut events = Vec::new();
    store.export_events(None, &mut events).unwrap();
    let text = String::from_utf8(events).unwrap();
    assert!(!text.contains("bundled secret"));
}

#[test]
fn redaction_before_bundled_edit_arrival_suppresses_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    // The redaction of the bundled edit arrives before the original that
    // carries the bundle; only the `redactions` row exists at first.
    let mut first = room_update(ROOM);
    first.timeline = vec![
        create_room("11"),
        redaction("$red", 160, "$bundled-edit", true),
    ];
    store
        .apply_sync_batch(&sync_batch("s1", vec![first]), 10)
        .unwrap();
    assert_eq!(scalar_i64(&db(&path), "SELECT COUNT(*) FROM redactions"), 1);

    let mut second = room_update(ROOM);
    second.timeline = vec![message_with_bundled_edit(
        "$orig",
        100,
        "original body",
        "$bundled-edit",
        150,
        "bundled secret",
    )];
    store
        .apply_sync_batch(&sync_batch("s2", vec![second]), 20)
        .unwrap();

    let conn = db(&path);
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM events WHERE event_id = '$bundled-edit' AND redacted = 1"
        ),
        1,
        "a pending redaction suppresses the late bundled edit"
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body FROM current_messages WHERE event_id = '$orig'"
        ),
        Some("original body".to_owned())
    );
    let mut events = Vec::new();
    store.export_events(None, &mut events).unwrap();
    let text = String::from_utf8(events).unwrap();
    assert!(!text.contains("bundled secret"));
}

#[test]
fn stale_bundle_replay_cannot_restore_a_redacted_bundled_edit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut first = room_update(ROOM);
    first.timeline = vec![
        create_room("11"),
        message_with_bundled_edit(
            "$orig",
            100,
            "original body",
            "$bundled-edit",
            150,
            "bundled secret",
        ),
    ];
    store
        .apply_sync_batch(&sync_batch("s1", vec![first]), 10)
        .unwrap();

    let mut redact = room_update(ROOM);
    redact.timeline = vec![redaction("$red", 200, "$bundled-edit", true)];
    store
        .apply_sync_batch(&sync_batch("s2", vec![redact]), 20)
        .unwrap();

    // Replaying the original with the stale bundle must not restore anything.
    let mut replay = room_update(ROOM);
    replay.timeline = vec![message_with_bundled_edit(
        "$orig",
        100,
        "original body",
        "$bundled-edit",
        150,
        "bundled secret",
    )];
    store
        .apply_sync_batch(&sync_batch("s3", vec![replay]), 30)
        .unwrap();

    let conn = db(&path);
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT redacted FROM events WHERE event_id = '$bundled-edit'"
        ),
        1
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body_text FROM events WHERE event_id = '$bundled-edit'"
        ),
        None
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body FROM current_messages WHERE event_id = '$orig'"
        ),
        Some("original body".to_owned())
    );
    let mut events = Vec::new();
    store.export_events(None, &mut events).unwrap();
    let text = String::from_utf8(events).unwrap();
    assert!(
        !text.contains("bundled secret"),
        "stale bundle replay leaked removed text"
    );
}

#[test]
fn bundle_reusing_the_enclosing_event_id_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut room = room_update(ROOM);
    room.timeline = vec![
        create_room("11"),
        json!({
            "type": "m.room.message",
            "event_id": "$self",
            "sender": ALICE,
            "origin_server_ts": 100,
            "content": { "msgtype": "m.text", "body": "original body" },
            "unsigned": {
                "m.relations": {
                    "m.replace": {
                        "type": "m.room.message",
                        "event_id": "$self",
                        "sender": ALICE,
                        "origin_server_ts": 150,
                        "content": {
                            "msgtype": "m.text",
                            "body": "* self secret",
                            "m.new_content": { "msgtype": "m.text", "body": "self secret" },
                            "m.relates_to": { "rel_type": "m.replace", "event_id": "$self" }
                        }
                    }
                }
            }
        }),
    ];
    store
        .apply_sync_batch(&sync_batch("s1", vec![room]), 10)
        .unwrap();

    let conn = db(&path);
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM events WHERE event_id = '$self'"
        ),
        1,
        "the bundle must not create a second row under the original id"
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body_text FROM events WHERE event_id = '$self'"
        ),
        Some("original body".to_owned()),
        "the original row must not be rewritten by its own bundled cache"
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT edit_target FROM events WHERE event_id = '$self'"
        ),
        None
    );
    let raw = scalar_string(
        &conn,
        "SELECT raw_json FROM events WHERE event_id = '$self'",
    )
    .expect("original raw stored");
    assert!(raw.contains("original body"));
    assert!(!raw.contains("self secret"), "self bundle leaked: {raw}");
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body FROM current_messages WHERE event_id = '$self'"
        ),
        Some("original body".to_owned())
    );
}

#[test]
fn bundle_colliding_with_a_fetched_event_does_not_rewrite_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    // A standalone message with the id the bundle wants to reuse.
    let mut first = room_update(ROOM);
    first.timeline = vec![
        create_room("11"),
        message("$shared", 90, "canonical shared"),
    ];
    store
        .apply_sync_batch(&sync_batch("s1", vec![first]), 10)
        .unwrap();

    let mut second = room_update(ROOM);
    second.timeline = vec![json!({
        "type": "m.room.message",
        "event_id": "$orig",
        "sender": ALICE,
        "origin_server_ts": 100,
        "content": { "msgtype": "m.text", "body": "orig body" },
        "unsigned": {
            "m.relations": {
                "m.replace": {
                    "type": "m.room.message",
                    "event_id": "$shared",
                    "sender": ALICE,
                    "origin_server_ts": 150,
                    "content": {
                        "msgtype": "m.text",
                        "body": "* cache secret",
                        "m.new_content": { "msgtype": "m.text", "body": "cache secret" },
                        "m.relates_to": { "rel_type": "m.replace", "event_id": "$orig" }
                    }
                }
            }
        }
    })];
    store
        .apply_sync_batch(&sync_batch("s2", vec![second]), 20)
        .unwrap();

    let conn = db(&path);
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM events WHERE event_id = '$shared'"
        ),
        1
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT source FROM events WHERE event_id = '$shared'"
        ),
        Some("sync".to_owned())
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body_text FROM events WHERE event_id = '$shared'"
        ),
        Some("canonical shared".to_owned())
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT edit_target FROM events WHERE event_id = '$shared'"
        ),
        None,
        "the fetched row must not gain the bundle's relation"
    );
    let raw = scalar_string(
        &conn,
        "SELECT raw_json FROM events WHERE event_id = '$shared'",
    )
    .expect("fetched raw stored");
    assert!(raw.contains("canonical shared"));
    assert!(
        !raw.contains("cache secret"),
        "colliding bundle leaked: {raw}"
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body FROM current_messages WHERE event_id = '$orig'"
        ),
        Some("orig body".to_owned()),
        "the skipped bundle must not project onto its claimed target"
    );
}

#[test]
fn conflicting_replayed_bundles_keep_the_first_representation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let bundle = |target: &str, body: &str| {
        json!({
            "type": "m.room.message",
            "event_id": "$bundle",
            "sender": ALICE,
            "origin_server_ts": 150,
            "content": {
                "msgtype": "m.text",
                "body": format!("* {body}"),
                "m.new_content": { "msgtype": "m.text", "body": body },
                "m.relates_to": { "rel_type": "m.replace", "event_id": target }
            }
        })
    };
    let parent = |id: &str, body: &str, bundle: serde_json::Value| {
        json!({
            "type": "m.room.message",
            "event_id": id,
            "sender": ALICE,
            "origin_server_ts": 100,
            "content": { "msgtype": "m.text", "body": body },
            "unsigned": { "m.relations": { "m.replace": bundle } }
        })
    };

    let mut room = room_update(ROOM);
    room.timeline = vec![
        create_room("11"),
        parent("$orig-a", "a body", bundle("$orig-a", "first secret")),
        parent("$orig-b", "b body", bundle("$orig-b", "second secret")),
    ];
    store
        .apply_sync_batch(&sync_batch("s1", vec![room]), 10)
        .unwrap();

    let conn = db(&path);
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT edit_target FROM events WHERE event_id = '$bundle'"
        ),
        Some("$orig-a".to_owned()),
        "the first bundle keeps its relation"
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body_text FROM events WHERE event_id = '$bundle'"
        ),
        Some("first secret".to_owned())
    );
    let raw = scalar_string(
        &conn,
        "SELECT raw_json FROM events WHERE event_id = '$bundle'",
    )
    .expect("bundle raw stored");
    assert!(
        !raw.contains("second secret"),
        "conflicting replay won: {raw}"
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body FROM current_messages WHERE event_id = '$orig-a'"
        ),
        Some("first secret".to_owned())
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body FROM current_messages WHERE event_id = '$orig-b'"
        ),
        Some("b body".to_owned())
    );
}

#[test]
fn fetched_standalone_replaces_a_bundle_only_representation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut first = room_update(ROOM);
    first.timeline = vec![
        create_room("11"),
        message_with_bundled_edit("$orig", 100, "original body", "$edit", 150, "bundle secret"),
    ];
    store
        .apply_sync_batch(&sync_batch("s1", vec![first]), 10)
        .unwrap();
    {
        let conn = db(&path);
        assert_eq!(
            scalar_string(&conn, "SELECT source FROM events WHERE event_id = '$edit'"),
            Some("bundle".to_owned())
        );
        assert_eq!(
            scalar_string(
                &conn,
                "SELECT body FROM current_messages WHERE event_id = '$orig'"
            ),
            Some("bundle secret".to_owned())
        );
    }

    // The real standalone edit arrives later with different canonical content.
    let mut second = room_update(ROOM);
    second.timeline = vec![edit("$edit", 150, "$orig", "fetched version")];
    store
        .apply_sync_batch(&sync_batch("s2", vec![second]), 20)
        .unwrap();

    let conn = db(&path);
    assert_eq!(
        scalar_string(&conn, "SELECT source FROM events WHERE event_id = '$edit'"),
        Some("sync".to_owned()),
        "the fetched representation takes precedence"
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body_text FROM events WHERE event_id = '$edit'"
        ),
        Some("fetched version".to_owned())
    );
    let raw = scalar_string(
        &conn,
        "SELECT raw_json FROM events WHERE event_id = '$edit'",
    )
    .expect("fetched raw stored");
    assert!(raw.contains("fetched version"));
    assert!(!raw.contains("bundle secret"));
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body FROM current_messages WHERE event_id = '$orig'"
        ),
        Some("fetched version".to_owned())
    );
    let mut events = Vec::new();
    store.export_events(None, &mut events).unwrap();
    let text = String::from_utf8(events).unwrap();
    assert!(!text.contains("bundle secret"));
}

#[test]
fn fetched_non_edit_replaces_a_bundle_only_edit_and_clears_stale_relation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut first = room_update(ROOM);
    first.timeline = vec![
        create_room("11"),
        message_with_bundled_edit("$orig", 100, "original body", "$edit", 150, "bundle secret"),
    ];
    store
        .apply_sync_batch(&sync_batch("s1", vec![first]), 10)
        .unwrap();
    assert_eq!(
        scalar_string(
            &db(&path),
            "SELECT body FROM current_messages WHERE event_id = '$orig'"
        ),
        Some("bundle secret".to_owned())
    );

    // The authoritative fetched payload for the same id is *not* an edit.
    let mut second = room_update(ROOM);
    second.timeline = vec![message("$edit", 150, "plain fetched")];
    store
        .apply_sync_batch(&sync_batch("s2", vec![second]), 20)
        .unwrap();

    let conn = db(&path);
    assert_eq!(
        scalar_string(&conn, "SELECT source FROM events WHERE event_id = '$edit'"),
        Some("sync".to_owned())
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT edit_target FROM events WHERE event_id = '$edit'"
        ),
        None,
        "the stale derived relation must not survive the promotion"
    );
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT edit_attempt FROM events WHERE event_id = '$edit'"
        ),
        0
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body_text FROM events WHERE event_id = '$edit'"
        ),
        Some("plain fetched".to_owned())
    );
    let raw = scalar_string(
        &conn,
        "SELECT raw_json FROM events WHERE event_id = '$edit'",
    )
    .expect("fetched raw stored");
    assert!(!raw.contains("bundle secret"));
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body FROM current_messages WHERE event_id = '$orig'"
        ),
        Some("original body".to_owned()),
        "the stale derived edit must no longer affect the old original"
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT latest_edit_event_id FROM current_messages WHERE event_id = '$orig'"
        ),
        None
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body FROM current_messages WHERE event_id = '$edit'"
        ),
        Some("plain fetched".to_owned()),
        "the fetched standalone message stands on its own"
    );
    let mut events = Vec::new();
    store.export_events(None, &mut events).unwrap();
    let text = String::from_utf8(events).unwrap();
    assert!(!text.contains("bundle secret"));
}

#[test]
fn redacted_bundle_row_stays_suppressed_when_the_fetched_event_arrives() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut first = room_update(ROOM);
    first.timeline = vec![
        create_room("11"),
        message_with_bundled_edit("$orig", 100, "original body", "$edit", 150, "bundle secret"),
    ];
    store
        .apply_sync_batch(&sync_batch("s1", vec![first]), 10)
        .unwrap();

    let mut redact = room_update(ROOM);
    redact.timeline = vec![redaction("$red", 200, "$edit", true)];
    store
        .apply_sync_batch(&sync_batch("s2", vec![redact]), 20)
        .unwrap();

    // A later standalone fetch of the redacted edit must keep it suppressed.
    let mut fetched = room_update(ROOM);
    fetched.timeline = vec![edit("$edit", 150, "$orig", "fetched secret")];
    store
        .apply_sync_batch(&sync_batch("s3", vec![fetched]), 30)
        .unwrap();

    let conn = db(&path);
    assert_eq!(
        scalar_string(&conn, "SELECT source FROM events WHERE event_id = '$edit'"),
        Some("sync".to_owned())
    );
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT redacted FROM events WHERE event_id = '$edit'"
        ),
        1,
        "prior redaction must survive the authoritative upsert"
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body_text FROM events WHERE event_id = '$edit'"
        ),
        None
    );
    let raw = scalar_string(
        &conn,
        "SELECT raw_json FROM events WHERE event_id = '$edit'",
    )
    .expect("fetched raw stored");
    assert!(
        !raw.contains("fetched secret"),
        "suppressed fetch leaked: {raw}"
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body FROM current_messages WHERE event_id = '$orig'"
        ),
        Some("original body".to_owned())
    );
    let mut events = Vec::new();
    store.export_events(None, &mut events).unwrap();
    let text = String::from_utf8(events).unwrap();
    assert!(!text.contains("fetched secret"));
    assert!(!text.contains("bundle secret"));
}

// ---------------------------------------------------------------------------
// Schema compatibility
// ---------------------------------------------------------------------------

#[test]
fn old_schema_without_cursor_token_is_rejected_and_left_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    {
        // The baseline v1 shape: no `gap_jobs.cursor_token` and no durable
        // visited-token ledger.
        let conn = db(&path);
        conn.execute_batch(
            "CREATE TABLE archive_meta (
                 id INTEGER PRIMARY KEY CHECK (id = 1),
                 homeserver TEXT NOT NULL,
                 user_id TEXT NOT NULL,
                 device_id TEXT NOT NULL,
                 bound_at INTEGER NOT NULL
             );
             INSERT INTO archive_meta VALUES
                 (1, 'https://hs.example.org', '@ingest:hs.example.org', 'MLN', 7);
             CREATE TABLE gap_jobs (
                 gap_id INTEGER PRIMARY KEY AUTOINCREMENT,
                 room_id TEXT NOT NULL,
                 created_at INTEGER NOT NULL,
                 reason TEXT NOT NULL,
                 boundary_token TEXT,
                 upper_token TEXT,
                 status TEXT NOT NULL DEFAULT 'open'
             );
             INSERT INTO gap_jobs (room_id, created_at, reason)
                 VALUES ('!room:hs.example.org', 1, 'legacy');
             PRAGMA user_version = 1;",
        )
        .unwrap();
        assert_eq!(
            scalar_i64(
                &conn,
                "SELECT COUNT(*) FROM pragma_table_info('gap_jobs') WHERE name = 'cursor_token'"
            ),
            0,
            "the baseline v1 shape really lacks gap_jobs.cursor_token"
        );
    }

    let writable = mainlinenerd_ingest::store::Store::open(&path, &identity())
        .err()
        .expect("open must be rejected");
    let advice = writable.to_string();
    assert!(
        advice.contains("new database path") && advice.contains("export"),
        "the error must preserve the archive and suggest a new path or an older build: {advice}"
    );
    assert!(
        !advice.to_lowercase().contains("delete"),
        "the error must not advise deletion: {advice}"
    );
    assert!(matches!(
        writable,
        StoreError::SchemaTooOld {
            found: 1,
            supported: 3
        }
    ));
    let read_only = mainlinenerd_ingest::store::Store::open_read_only(&path)
        .err()
        .expect("read-only open must be rejected");
    assert!(matches!(
        read_only,
        StoreError::SchemaTooOld {
            found: 1,
            supported: 3
        }
    ));

    let conn = db(&path);
    assert_eq!(
        scalar_i64(&conn, "PRAGMA user_version"),
        1,
        "rejection must not rewrite the schema version"
    );
    assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM gap_jobs"), 1);
    assert_eq!(
        scalar_string(&conn, "SELECT reason FROM gap_jobs"),
        Some("legacy".to_owned())
    );
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'history_visited'"
        ),
        0,
        "no part of the new schema may be created on rejection"
    );
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM pragma_table_info('gap_jobs') WHERE name = 'cursor_token'"
        ),
        0
    );
}

#[test]
fn newer_schema_version_is_rejected_in_both_open_paths() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    {
        let conn = db(&path);
        conn.execute_batch("CREATE TABLE sentinel (x INTEGER); PRAGMA user_version = 4;")
            .unwrap();
    }

    let writable = mainlinenerd_ingest::store::Store::open(&path, &identity())
        .err()
        .expect("open must be rejected");
    assert!(matches!(
        writable,
        StoreError::SchemaTooNew {
            found: 4,
            supported: 3
        }
    ));
    let read_only = mainlinenerd_ingest::store::Store::open_read_only(&path)
        .err()
        .expect("read-only open must be rejected");
    assert!(matches!(
        read_only,
        StoreError::SchemaTooNew {
            found: 4,
            supported: 3
        }
    ));

    let conn = db(&path);
    assert_eq!(scalar_i64(&conn, "PRAGMA user_version"), 4);
}

#[test]
fn fresh_archive_records_schema_version_three_and_the_ledger() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let store = open_store(&path);
    assert_eq!(store.status().unwrap().schema_version, 3);
    drop(store);

    let conn = db(&path);
    assert_eq!(scalar_i64(&conn, "PRAGMA user_version"), 3);
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'history_visited'"
        ),
        1
    );
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'room_pins'"
        ),
        1
    );
}

// ---------------------------------------------------------------------------
// Room-version-dependent redaction target
// ---------------------------------------------------------------------------

#[test]
fn pre_v11_redaction_uses_only_the_top_level_field() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut room = room_update(ROOM);
    room.timeline = vec![
        create_room("10"),
        message("$a", 100, "a body"),
        message("$b", 110, "b body"),
        // content.redacts is not a target before v11 and must delete nothing.
        json!({
            "type": "m.room.redaction", "event_id": "$wrong-field", "sender": ALICE,
            "origin_server_ts": 200,
            "content": { "redacts": "$a" }
        }),
        json!({
            "type": "m.room.redaction", "event_id": "$right-field", "sender": ALICE,
            "origin_server_ts": 210,
            "redacts": "$b",
            "content": {}
        }),
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
        Some("a body".to_owned()),
        "the pre-v11 wrong field must not delete the other target"
    );
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM redactions WHERE redaction_event_id = '$wrong-field'"
        ),
        0,
        "a wrong-field-only redaction records no deletion"
    );
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT redacted FROM current_messages WHERE event_id = '$b'"
        ),
        1,
        "the pre-v11 top-level field deletes"
    );
}

#[test]
fn v11_redaction_uses_content_and_conflicts_leave_the_other_target() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut room = room_update(ROOM);
    room.timeline = vec![
        create_room("11"),
        message("$c", 100, "c body"),
        message("$d", 110, "d body"),
        // Conflicting fields: v11's authoritative `content.redacts` wins and
        // the top-level field must not delete its target.
        json!({
            "type": "m.room.redaction", "event_id": "$conflict", "sender": ALICE,
            "origin_server_ts": 200,
            "redacts": "$d",
            "content": { "redacts": "$c" }
        }),
    ];
    store
        .apply_sync_batch(&sync_batch("s1", vec![room]), 10)
        .unwrap();

    let conn = db(&path);
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT redacted FROM current_messages WHERE event_id = '$c'"
        ),
        1,
        "v11 content.redacts is authoritative"
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body FROM current_messages WHERE event_id = '$d'"
        ),
        Some("d body".to_owned()),
        "the conflicting top-level field must not delete the other target"
    );
}

#[test]
fn wrong_field_only_v11_redaction_deletes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut room = room_update(ROOM);
    room.timeline = vec![
        create_room("11"),
        message("$m", 100, "secret"),
        json!({
            "type": "m.room.redaction", "event_id": "$red", "sender": ALICE,
            "origin_server_ts": 200,
            "redacts": "$m",
            "content": {}
        }),
    ];
    store
        .apply_sync_batch(&sync_batch("s1", vec![room]), 10)
        .unwrap();

    let conn = db(&path);
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body FROM current_messages WHERE event_id = '$m'"
        ),
        Some("secret".to_owned())
    );
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM redactions WHERE redaction_event_id = '$red'"
        ),
        0,
        "a wrong-field-only redaction records no deletion"
    );
}

#[test]
fn versionless_create_defaults_to_v1_for_redaction_rules() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut room = room_update(ROOM);
    room.timeline = vec![
        json!({
            "type": "m.room.create", "event_id": "$create", "sender": ALICE,
            "state_key": "", "origin_server_ts": 1,
            "content": { "creator": ALICE }
        }),
        message("$m", 100, "secret"),
        json!({
            "type": "m.room.redaction", "event_id": "$red", "sender": ALICE,
            "origin_server_ts": 200,
            "redacts": "$m",
            "content": {}
        }),
    ];
    store
        .apply_sync_batch(&sync_batch("s1", vec![room]), 10)
        .unwrap();

    let conn = db(&path);
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT redacted FROM current_messages WHERE event_id = '$m'"
        ),
        1,
        "an absent room_version behaves as v1, where top-level redacts applies"
    );
    assert_eq!(
        store.status().unwrap().rooms[0].room_version.as_deref(),
        Some("1")
    );
}

#[test]
fn versionless_create_ignores_content_redacts() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut room = room_update(ROOM);
    room.timeline = vec![
        json!({
            "type": "m.room.create", "event_id": "$create", "sender": ALICE,
            "state_key": "", "origin_server_ts": 1,
            "content": { "creator": ALICE }
        }),
        message("$m", 100, "secret"),
        json!({
            "type": "m.room.redaction", "event_id": "$red", "sender": ALICE,
            "origin_server_ts": 200,
            "content": { "redacts": "$m" }
        }),
    ];
    store
        .apply_sync_batch(&sync_batch("s1", vec![room]), 10)
        .unwrap();

    let conn = db(&path);
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body FROM current_messages WHERE event_id = '$m'"
        ),
        Some("secret".to_owned()),
        "v1 rules ignore content.redacts"
    );
}

#[test]
fn present_non_string_create_version_is_malformed_and_rolls_back() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    // A committed checkpoint proves the failing batches cannot move it.
    let mut seed = room_update(ROOM2);
    seed.timeline = vec![message("$seed", 50, "seed")];
    seed.prev_batch = Some("p0".to_owned());
    store
        .apply_sync_batch(&sync_batch("s0", vec![seed]), 5)
        .unwrap();

    // Every present non-string value is invalid; none may be coerced into a
    // recognized version such as "1" or "11".
    for version in [
        json!(1),
        json!(11),
        json!(null),
        json!(true),
        json!({ "payload": "PRIVATE_FIELD_MARKER" }),
        json!([]),
    ] {
        let mut room = room_update(ROOM);
        room.timeline = vec![
            json!({
                "type": "m.room.create", "event_id": "$create", "sender": ALICE,
                "state_key": "", "origin_server_ts": 1,
                "content": { "creator": ALICE, "room_version": version }
            }),
            message("$m", 100, "secret"),
            json!({
                "type": "m.room.redaction", "event_id": "$red", "sender": ALICE,
                "origin_server_ts": 200,
                "content": { "redacts": "$m" }
            }),
        ];
        let error = store
            .apply_sync_batch(&sync_batch("s1", vec![room]), 10)
            .unwrap_err();
        assert!(
            matches!(error, StoreError::MalformedRoomVersion { .. }),
            "version {version} must be rejected: {error}"
        );
        assert!(!error.to_string().contains("PRIVATE_FIELD_MARKER"));
        assert!(!format!("{error:?}").contains("PRIVATE_FIELD_MARKER"));
        assert_eq!(
            store.since_token().unwrap().as_deref(),
            Some("s0"),
            "version {version} must not advance the checkpoint"
        );
        assert_eq!(
            store.room_history_token(ROOM).unwrap(),
            None,
            "version {version} must not seed history"
        );
    }

    let conn = db(&path);
    assert_eq!(
        scalar_i64(&conn, "SELECT COUNT(*) FROM events"),
        1,
        "only the committed seed event survives"
    );
    assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM rooms"), 1);
    assert_eq!(
        scalar_i64(&conn, "SELECT COUNT(*) FROM redactions"),
        0,
        "no rejected redaction may delete a target"
    );
    assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM room_history"), 1);
}

#[test]
fn redaction_without_a_known_room_version_rolls_back_the_batch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut seed = room_update(ROOM2);
    seed.timeline = vec![message("$seed", 50, "seed")];
    seed.prev_batch = Some("p0".to_owned());
    store
        .apply_sync_batch(&sync_batch("s0", vec![seed]), 5)
        .unwrap();
    assert_eq!(store.since_token().unwrap().as_deref(), Some("s0"));

    let mut room = room_update(ROOM);
    room.timeline = vec![
        message("$m", 100, "secret"),
        json!({
            "type": "m.room.redaction", "event_id": "$red", "sender": ALICE,
            "origin_server_ts": 200,
            "content": { "redacts": "$m" }
        }),
    ];
    let error = store
        .apply_sync_batch(&sync_batch("s1", vec![room]), 20)
        .unwrap_err();
    assert!(matches!(error, StoreError::MalformedEvent { .. }));

    assert_eq!(
        store.since_token().unwrap().as_deref(),
        Some("s0"),
        "the checkpoint must not advance"
    );
    let conn = db(&path);
    assert_eq!(
        scalar_i64(&conn, "SELECT COUNT(*) FROM events"),
        1,
        "the whole batch rolls back"
    );
    assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM rooms"), 1);
    assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM gap_jobs"), 0);
}

// ---------------------------------------------------------------------------
// Durable visited-token ledger
// ---------------------------------------------------------------------------

#[test]
fn malformed_or_stale_pages_do_not_poison_the_visited_ledger() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut room = room_update(ROOM);
    room.timeline = vec![message("$live", 100, "live")];
    room.prev_batch = Some("p1".to_owned());
    store
        .apply_sync_batch(&sync_batch("s1", vec![room]), 10)
        .unwrap();

    let error = store
        .apply_history_page(
            ROOM,
            HistoryWork::Base,
            "p1",
            &history_page(
                "p1",
                Some("p2"),
                vec![message("$ok", 50, "ok"), json!({"type": "m.room.message"})],
            ),
            20,
        )
        .unwrap_err();
    assert!(matches!(error, StoreError::MalformedEvent { .. }));
    {
        let conn = db(&path);
        assert_eq!(
            scalar_i64(&conn, "SELECT COUNT(*) FROM history_visited"),
            0,
            "a rolled-back page must not record visited tokens"
        );
        assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM events"), 1);
    }

    let outcome = store
        .apply_history_page(
            ROOM,
            HistoryWork::Base,
            "wrong",
            &history_page("wrong", Some("q"), vec![]),
            30,
        )
        .unwrap();
    assert_eq!(outcome.status, Some(HistoryStatus::Stale));
    {
        let conn = db(&path);
        assert_eq!(
            scalar_i64(&conn, "SELECT COUNT(*) FROM history_visited"),
            0,
            "a stale page must not record visited tokens"
        );
    }

    let outcome = store
        .apply_history_page(
            ROOM,
            HistoryWork::Base,
            "p1",
            &history_page("p1", Some("p2"), vec![]),
            40,
        )
        .unwrap();
    assert_eq!(outcome.status, Some(HistoryStatus::Advanced));
    {
        let conn = db(&path);
        assert_eq!(
            scalar_i64(&conn, "SELECT COUNT(*) FROM history_visited"),
            2,
            "a committed page records its requested and returned tokens"
        );
    }

    // The recorded p1 now makes p2 -> p1 a cycle within the same call.
    let outcome = store
        .apply_history_page(
            ROOM,
            HistoryWork::Base,
            "p2",
            &history_page("p2", Some("p1"), vec![]),
            50,
        )
        .unwrap();
    assert_eq!(outcome.status, Some(HistoryStatus::Stalled));
}

#[test]
fn visited_ledger_is_independent_per_work_item() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut initial = room_update(ROOM);
    initial.timeline = vec![message("$live", 100, "live")];
    initial.prev_batch = Some("p0".to_owned());
    store
        .apply_sync_batch(&sync_batch("s1", vec![initial]), 10)
        .unwrap();
    for (next_batch, prev_batch, at) in [("s2", "p1", 20), ("s3", "p2", 30)] {
        let mut limited = room_update(ROOM);
        limited.timeline = vec![message(&format!("$live-{prev_batch}"), 300, "live")];
        limited.prev_batch = Some(prev_batch.to_owned());
        limited.limited = true;
        store
            .apply_sync_batch(&sync_batch(next_batch, vec![limited]), at)
            .unwrap();
    }
    let gaps = store.open_gap_positions().unwrap();
    assert_eq!(gaps.len(), 2);
    let gap_one = gaps[0].gap_id;
    let gap_two = gaps[1].gap_id;

    // Gap one cycles through a token it has already visited.
    let outcome = store
        .apply_history_page(
            ROOM,
            HistoryWork::Gap(gap_one),
            "p1",
            &history_page("p1", Some("x"), vec![]),
            40,
        )
        .unwrap();
    assert_eq!(outcome.status, Some(HistoryStatus::Advanced));
    let outcome = store
        .apply_history_page(
            ROOM,
            HistoryWork::Gap(gap_one),
            "x",
            &history_page("x", Some("p1"), vec![]),
            41,
        )
        .unwrap();
    assert_eq!(outcome.status, Some(HistoryStatus::Stalled));

    // Gap two walks through the same token strings; gap one's ledger must not
    // block it, so it reaches p1.
    let outcome = store
        .apply_history_page(
            ROOM,
            HistoryWork::Gap(gap_two),
            "p2",
            &history_page("p2", Some("x"), vec![]),
            50,
        )
        .unwrap();
    assert_eq!(outcome.status, Some(HistoryStatus::Advanced));
    let outcome = store
        .apply_history_page(
            ROOM,
            HistoryWork::Gap(gap_two),
            "x",
            &history_page("x", Some("p1"), vec![]),
            51,
        )
        .unwrap();
    assert_eq!(outcome.status, Some(HistoryStatus::Advanced));
    assert_eq!(
        store.open_gap_position(gap_two).unwrap().unwrap().token,
        "p1"
    );

    // The base backfill is untouched.
    assert_eq!(
        store.room_history_token(ROOM).unwrap().as_deref(),
        Some("p0")
    );
    assert!(!store.room_history_stalled(ROOM).unwrap());
    let report = store.status().unwrap();
    assert_eq!(report.rooms[0].open_gaps, 1);
    assert_eq!(report.rooms[0].unresolved_gaps, 1);
}

#[test]
fn ineligible_work_items_refuse_late_in_flight_pages() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut room = room_update(ROOM);
    room.timeline = vec![message("$live", 100, "live")];
    room.prev_batch = Some("p1".to_owned());
    store
        .apply_sync_batch(&sync_batch("s1", vec![room]), 10)
        .unwrap();

    // Stall the base; its token stays p1, so a late p1 page still matches the
    // token and must be refused on eligibility alone.
    let outcome = store
        .apply_history_page(
            ROOM,
            HistoryWork::Base,
            "p1",
            &history_page("p1", Some("p1"), vec![]),
            20,
        )
        .unwrap();
    assert_eq!(outcome.status, Some(HistoryStatus::Stalled));
    let events_before = {
        let conn = db(&path);
        scalar_i64(&conn, "SELECT COUNT(*) FROM events")
    };
    let outcome = store
        .apply_history_page(
            ROOM,
            HistoryWork::Base,
            "p1",
            &history_page("p1", Some("p2"), vec![message("$late", 5, "late")]),
            30,
        )
        .unwrap();
    assert_eq!(
        outcome.status,
        Some(HistoryStatus::Stale),
        "a stalled base must refuse its old in-flight page"
    );
    assert_eq!(
        store.room_history_token(ROOM).unwrap().as_deref(),
        Some("p1")
    );
    {
        let conn = db(&path);
        assert_eq!(
            scalar_i64(&conn, "SELECT COUNT(*) FROM events"),
            events_before
        );
    }

    // A completed base refuses a late page too.
    let mut done = room_update(ROOM2);
    done.timeline = vec![message("$live", 100, "live")];
    done.prev_batch = Some("p9".to_owned());
    store
        .apply_sync_batch(&sync_batch("s2", vec![done]), 40)
        .unwrap();
    let outcome = store
        .apply_history_page(
            ROOM2,
            HistoryWork::Base,
            "p9",
            &history_page("p9", None, vec![]),
            50,
        )
        .unwrap();
    assert_eq!(outcome.status, Some(HistoryStatus::Completed));
    let outcome = store
        .apply_history_page(
            ROOM2,
            HistoryWork::Base,
            "p9",
            &history_page("p9", Some("p10"), vec![message("$late2", 5, "late")]),
            60,
        )
        .unwrap();
    assert_eq!(outcome.status, Some(HistoryStatus::Stale));

    // A closed (unresolved) gap refuses a late page as well.
    let mut limited = room_update(ROOM);
    limited.timeline = vec![message("$live2", 300, "live2")];
    limited.prev_batch = Some("p2".to_owned());
    limited.limited = true;
    store
        .apply_sync_batch(&sync_batch("s3", vec![limited]), 70)
        .unwrap();
    let gap_id = store.open_gap_positions().unwrap()[0].gap_id;
    let outcome = store
        .apply_history_page(
            ROOM,
            HistoryWork::Gap(gap_id),
            "p2",
            &history_page("p2", Some("p2"), vec![]),
            80,
        )
        .unwrap();
    assert_eq!(outcome.status, Some(HistoryStatus::Stalled));
    let outcome = store
        .apply_history_page(
            ROOM,
            HistoryWork::Gap(gap_id),
            "p2",
            &history_page("p2", Some("p3"), vec![message("$late3", 5, "late")]),
            90,
        )
        .unwrap();
    assert_eq!(outcome.status, Some(HistoryStatus::Stale));
}

// ---------------------------------------------------------------------------
// Limited-sync span and unseeded base rows
// ---------------------------------------------------------------------------

#[test]
fn limited_sync_with_no_span_neither_opens_a_gap_nor_rewinds_base() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut first = room_update(ROOM);
    first.timeline = vec![message("$live", 100, "live")];
    first.prev_batch = Some("p0".to_owned());
    store
        .apply_sync_batch(&sync_batch("s1", vec![first]), 10)
        .unwrap();

    // prev_batch equals the last committed token: no missing span, no gap.
    let mut equal = room_update(ROOM);
    equal.timeline = vec![message("$live2", 300, "live2")];
    equal.prev_batch = Some("s1".to_owned());
    equal.limited = true;
    store
        .apply_sync_batch(&sync_batch("s2", vec![equal]), 20)
        .unwrap();
    assert!(
        store.open_gap_positions().unwrap().is_empty(),
        "an interval with no span must not open a gap job"
    );
    assert_eq!(
        store.room_history_token(ROOM).unwrap().as_deref(),
        Some("p0"),
        "the existing base backfill must not be rewound"
    );

    // A new room whose first appearance is an equal-span limited sync still
    // gets its base history seeded.
    let mut new_room = room_update(ROOM3);
    new_room.timeline = vec![message("$new", 400, "new")];
    new_room.prev_batch = Some("s2".to_owned());
    new_room.limited = true;
    store
        .apply_sync_batch(&sync_batch("s3", vec![new_room]), 30)
        .unwrap();
    assert!(store.open_gap_positions().unwrap().is_empty());
    assert_eq!(
        store.room_history_token(ROOM3).unwrap().as_deref(),
        Some("s2")
    );
    assert!(!store.room_history_complete(ROOM3).unwrap());
}

#[test]
fn a_never_started_base_row_adopts_a_later_prev_batch_and_never_rewinds() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    // A sync with no prev_batch leaves the base honestly unseeded.
    let mut no_token = room_update(ROOM);
    no_token.timeline = vec![message("$live", 100, "live")];
    no_token.prev_batch = None;
    store
        .apply_sync_batch(&sync_batch("s1", vec![no_token]), 10)
        .unwrap();
    assert_eq!(store.room_history_token(ROOM).unwrap(), None);
    assert!(!store.room_history_complete(ROOM).unwrap());
    let report = store.status().unwrap();
    assert!(
        !report.rooms[0].history_token_set,
        "a missing token is not completion"
    );
    assert!(!report.rooms[0].history_complete);
    assert!(store.render_status().unwrap().contains("no-token"));

    // A later explicit prev_batch seeds only that never-started row.
    let mut later = room_update(ROOM);
    later.timeline = vec![message("$live2", 200, "live2")];
    later.prev_batch = Some("p1".to_owned());
    store
        .apply_sync_batch(&sync_batch("s2", vec![later]), 20)
        .unwrap();
    assert_eq!(
        store.room_history_token(ROOM).unwrap().as_deref(),
        Some("p1")
    );

    // Progress from there; a still-later prev_batch must not rewind it.
    let outcome = store
        .apply_history_page(
            ROOM,
            HistoryWork::Base,
            "p1",
            &history_page("p1", Some("p2"), vec![message("$old", 50, "old")]),
            30,
        )
        .unwrap();
    assert_eq!(outcome.status, Some(HistoryStatus::Advanced));
    let mut again = room_update(ROOM);
    again.timeline = vec![message("$live3", 300, "live3")];
    again.prev_batch = Some("q".to_owned());
    store
        .apply_sync_batch(&sync_batch("s3", vec![again]), 40)
        .unwrap();
    assert_eq!(
        store.room_history_token(ROOM).unwrap().as_deref(),
        Some("p2"),
        "in-progress backfill is never rewound"
    );

    // A completed base never adopts a later token either.
    let outcome = store
        .apply_history_page(
            ROOM,
            HistoryWork::Base,
            "p2",
            &history_page("p2", None, vec![]),
            50,
        )
        .unwrap();
    assert_eq!(outcome.status, Some(HistoryStatus::Completed));
    let mut finished = room_update(ROOM);
    finished.timeline = vec![message("$live4", 400, "live4")];
    finished.prev_batch = Some("r".to_owned());
    store
        .apply_sync_batch(&sync_batch("s4", vec![finished]), 60)
        .unwrap();
    assert!(store.room_history_complete(ROOM).unwrap());
    assert_eq!(store.room_history_token(ROOM).unwrap(), None);
}

#[test]
fn history_commit_rechecks_mutable_room_policy() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");

    let controls: Vec<(&str, Vec<serde_json::Value>, Option<&str>)> = vec![
        ("encrypted", vec![encryption_event()], None),
        ("upgraded", vec![tombstone("!next:hs.example.org")], None),
        ("left", vec![], Some("leave")),
    ];
    for (label, state, membership) in controls {
        let _ = std::fs::remove_file(&path);
        let mut store = open_store(&path);
        let mut room = room_update(ROOM);
        room.state = vec![create_room("11")];
        room.state.extend(state);
        room.timeline = vec![message("$live", 100, "live")];
        room.prev_batch = Some("p1".to_owned());
        room.own_membership = membership.map(str::to_owned);
        store
            .apply_sync_batch(&sync_batch("s1", vec![room]), 10)
            .unwrap();

        let outcome = store
            .apply_history_page(
                ROOM,
                HistoryWork::Base,
                "p1",
                &history_page("p1", None, vec![message("$old", 50, "old")]),
                20,
            )
            .unwrap();
        assert_eq!(outcome.status, Some(HistoryStatus::Stale), "{label}");
        assert_eq!(outcome.events_seen, 0, "{label}");
        let conn = db(&path);
        assert_eq!(
            scalar_i64(&conn, "SELECT COUNT(*) FROM events WHERE event_id = '$old'"),
            0,
            "{label}: no speculative event"
        );
        assert_eq!(
            store.room_history_token(ROOM).unwrap().as_deref(),
            Some("p1"),
            "{label}: cursor unchanged"
        );
        assert_eq!(
            scalar_i64(&conn, "SELECT COUNT(*) FROM history_visited"),
            0,
            "{label}: ledger unchanged"
        );
    }
}

#[test]
fn seed_configured_room_history_only_fills_never_started_base() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);
    store.register_configured_room(ROOM, None, 10).unwrap();
    // A first sync with no prev_batch leaves the room honestly unseeded; the
    // committed global token becomes a valid starting point.
    let mut room = room_update(ROOM);
    room.timeline = vec![message("$live", 100, "live")];
    store
        .apply_sync_batch(&sync_batch("s5", vec![room]), 10)
        .unwrap();
    assert_eq!(store.room_history_token(ROOM).unwrap(), None);

    assert!(store.seed_configured_room_history(ROOM, 20).unwrap());
    assert_eq!(
        store.room_history_token(ROOM).unwrap().as_deref(),
        Some("s5")
    );
    // A second seed is a no-op and never rewinds.
    assert!(!store.seed_configured_room_history(ROOM, 30).unwrap());
    assert_eq!(
        store.room_history_token(ROOM).unwrap().as_deref(),
        Some("s5")
    );

    // A non-configured room is never seeded.
    let mut other = room_update(ROOM2);
    other.timeline = vec![message("$other", 100, "other")];
    store
        .apply_sync_batch(&sync_batch("s6", vec![other]), 10)
        .unwrap();
    assert!(!store.seed_configured_room_history(ROOM2, 30).unwrap());
    assert_eq!(store.room_history_token(ROOM2).unwrap(), None);
}

#[test]
fn set_configured_rooms_clears_removed_entries() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let store = open_store(&path);
    store.register_configured_room(ROOM, None, 10).unwrap();
    store.register_configured_room(ROOM2, None, 10).unwrap();
    store.set_configured_rooms(&[ROOM.to_owned()], 20).unwrap();
    assert!(store.room_configured(ROOM).unwrap());
    assert!(!store.room_configured(ROOM2).unwrap());
    let report = store.status().unwrap();
    let removed = report.rooms.iter().find(|r| r.room_id == ROOM2).unwrap();
    assert!(!removed.configured, "removed config must not look current");
}

#[test]
fn discovery_pages_are_bounded_and_ordered() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);
    for (index, room) in [ROOM, ROOM2, ROOM3].iter().enumerate() {
        store
            .register_configured_room(room, None, index as i64)
            .unwrap();
        let mut update = room_update(room);
        update.timeline = vec![message(&format!("$live{index}"), 100, "live")];
        update.prev_batch = Some(format!("p{index}"));
        store
            .apply_sync_batch(
                &sync_batch(&format!("s{index}"), vec![update]),
                (index + 1) as i64,
            )
            .unwrap();
    }
    let conn = db(&path);
    for index in 0..20 {
        conn.execute(
            "INSERT INTO gap_jobs (room_id, created_at, reason, boundary_token, upper_token, cursor_token, status)
             VALUES (?1, ?2, 'limited_sync', 'b', ?3, ?3, 'open')",
            rusqlite::params![ROOM, index, format!("g{index}")],
        )
        .unwrap();
    }
    drop(conn);

    assert_eq!(store.bases_needing_history_page(None, 2).unwrap().len(), 2);
    let newest = store.bases_needing_history_newest(2).unwrap();
    assert_eq!(newest.len(), 2);
    assert_eq!(newest[0].room_id, ROOM3, "newest activity first");
    let first_gaps = store.open_gap_positions_page(None, 5).unwrap();
    assert_eq!(first_gaps.len(), 5);
    let after = first_gaps.last().unwrap().gap_id;
    let next_gaps = store.open_gap_positions_page(Some(after), 5).unwrap();
    assert_eq!(next_gaps.len(), 5);
    assert!(next_gaps[0].gap_id > after);
    let newest_gaps = store.open_gap_positions_newest(3).unwrap();
    assert!(newest_gaps[0].gap_id > newest_gaps[1].gap_id);
}

#[test]
fn retry_stalled_only_re_enables_eligible_configured_rooms() {
    const ROOM4: &str = "!room4:hs.example.org";
    const ROOM5: &str = "!room5:hs.example.org";
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let seed = |store: &mut mainlinenerd_ingest::store::Store,
                room: &str,
                state: Vec<serde_json::Value>,
                token: &str,
                batch: &str| {
        store.register_configured_room(room, None, 1).unwrap();
        let mut update = room_update(room);
        update.state = state;
        update.timeline = vec![message(&format!("$live-{room}"), 100, "live")];
        update.prev_batch = Some(token.to_owned());
        store
            .apply_sync_batch(&sync_batch(batch, vec![update]), 10)
            .unwrap();
    };

    // Eligible A: stalled base with a saved cursor.
    seed(&mut store, ROOM, vec![create_room("11")], "p1", "s1");
    store.mark_history_stalled(ROOM, "403", 20).unwrap();
    // Encrypted B.
    seed(
        &mut store,
        ROOM2,
        vec![create_room("11"), encryption_event()],
        "q1",
        "s2",
    );
    store.mark_history_stalled(ROOM2, "403", 20).unwrap();
    // Versionless (not ready) C.
    seed(&mut store, ROOM3, vec![], "r1", "s3");
    store.mark_history_stalled(ROOM3, "403", 20).unwrap();
    // Departed D.
    store.register_configured_room(ROOM4, None, 1).unwrap();
    let mut departed = room_update(ROOM4);
    departed.state = vec![create_room("11")];
    departed.timeline = vec![message("$live-departed", 100, "live")];
    departed.prev_batch = Some("t1".to_owned());
    departed.own_membership = Some("leave".to_owned());
    store
        .apply_sync_batch(&sync_batch("s4", vec![departed]), 10)
        .unwrap();
    store.mark_history_stalled(ROOM4, "403", 20).unwrap();
    // Completed E must never be reopened.
    seed(&mut store, ROOM5, vec![create_room("11")], "u1", "s5");
    let completed = store
        .apply_history_page(
            ROOM5,
            HistoryWork::Base,
            "u1",
            &history_page("u1", None, vec![]),
            30,
        )
        .unwrap();
    assert_eq!(completed.status, Some(HistoryStatus::Completed));

    // An open gap with a stale error note, a bounded gap that fails 403
    // (unresolved but with saved cursor+boundary), a cursor-less unresolved
    // gap, and a repaired gap.
    let open_gap_id;
    let failed_gap_id;
    let bounded_gap_id;
    {
        let conn = db(&path);
        conn.execute(
            "INSERT INTO gap_jobs (room_id, created_at, reason, boundary_token, upper_token, cursor_token, status)
             VALUES (?1, 1, 'limited_sync', 'b', 'open-upper', 'open-cursor', 'open')",
            rusqlite::params![ROOM],
        )
        .unwrap();
        open_gap_id = scalar_i64(&conn, "SELECT MAX(gap_id) FROM gap_jobs");
        conn.execute(
            "INSERT INTO gap_jobs (room_id, created_at, reason, boundary_token, upper_token, cursor_token, status, close_reason)
             VALUES (?1, 1, 'limited_sync', 'b', 'failed-upper', 'failed-cursor', 'unresolved', 'room unavailable: 403')",
            rusqlite::params![ROOM],
        )
        .unwrap();
        failed_gap_id = scalar_i64(&conn, "SELECT MAX(gap_id) FROM gap_jobs");
        conn.execute(
            "INSERT INTO gap_jobs (room_id, created_at, reason, boundary_token, upper_token, cursor_token, status, close_reason)
             VALUES (?1, 1, 'limited_sync', 'b', 'no-token-upper', NULL, 'unresolved', 'no repair token available')",
            rusqlite::params![ROOM],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO gap_jobs (room_id, created_at, reason, boundary_token, upper_token, cursor_token, status)
             VALUES (?1, 1, 'limited_sync', 'b', 'done-upper', 'done-cursor', 'open')",
            rusqlite::params![ROOM],
        )
        .unwrap();
        bounded_gap_id = scalar_i64(&conn, "SELECT MAX(gap_id) FROM gap_jobs");
        conn.execute(
            "INSERT INTO history_visited (room_id, work_kind, work_id, token, visited_at)
             VALUES (?1, 'gap', ?2, 'failed-cursor', 5)",
            rusqlite::params![ROOM, failed_gap_id],
        )
        .unwrap();
    }
    store.record_gap_error(open_gap_id, "transient").unwrap();
    // Repair the last gap so it must never be reopened.
    let repaired = store
        .apply_history_page(
            ROOM,
            HistoryWork::Gap(bounded_gap_id),
            "done-cursor",
            &history_page("done-cursor", Some("b"), vec![]),
            35,
        )
        .unwrap();
    assert_eq!(repaired.status, Some(HistoryStatus::Completed));

    let retried = store.retry_stalled_configured(40).unwrap();
    assert_eq!(
        retried,
        mainlinenerd_ingest::store::RetryStalledOutcome { base: 1, gaps: 1 },
        "one base and one bounded gap are resumed; notes are not counted"
    );
    assert_eq!(retried.total(), 2);
    assert!(!store.room_history_stalled(ROOM).unwrap());
    assert_eq!(
        store.room_history_token(ROOM).unwrap().as_deref(),
        Some("p1")
    );
    for room in [ROOM2, ROOM3, ROOM4] {
        assert!(
            store.room_history_stalled(room).unwrap(),
            "{room} keeps its stall"
        );
    }
    assert!(store.room_history_complete(ROOM5).unwrap());
    assert!(!store.room_history_stalled(ROOM5).unwrap());

    // The 403'd bounded gap is resumable at exactly its saved from/to, with its
    // ledger intact.
    let reopened = store
        .open_gap_position(failed_gap_id)
        .unwrap()
        .expect("the bounded gap is open again");
    assert_eq!(reopened.token, "failed-cursor");
    assert_eq!(reopened.to_token.as_deref(), Some("b"));
    let conn = db(&path);
    assert_eq!(
        scalar_string(
            &conn,
            &format!("SELECT upper_token FROM gap_jobs WHERE gap_id = {failed_gap_id}")
        ),
        Some("failed-upper".to_owned()),
        "the upper boundary is preserved"
    );
    assert_eq!(
        scalar_string(
            &conn,
            &format!("SELECT close_reason FROM gap_jobs WHERE gap_id = {failed_gap_id}")
        ),
        None
    );
    assert_eq!(
        scalar_i64(
            &conn,
            &format!(
                "SELECT COUNT(*) FROM history_visited WHERE work_kind = 'gap' AND work_id = {failed_gap_id}"
            )
        ),
        1,
        "the visited-token ledger is preserved"
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT close_reason FROM gap_jobs WHERE cursor_token = 'open-cursor'"
        ),
        None,
        "an open resumable gap loses only a stale note"
    );
    assert_eq!(
        scalar_string(
            &conn,
            &format!("SELECT status FROM gap_jobs WHERE gap_id = {bounded_gap_id}")
        ),
        Some("repaired".to_owned()),
        "a repaired gap is never reopened"
    );
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT status FROM gap_jobs WHERE cursor_token IS NULL AND upper_token = 'no-token-upper'"
        ),
        Some("unresolved".to_owned()),
        "a cursor-less unresolved gap is never reopened"
    );
}
