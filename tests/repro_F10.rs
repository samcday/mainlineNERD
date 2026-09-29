//! Repro for review finding F10: project_event only skips rows with an
//! `edit_target`, so an invalid m.replace (edit_attempt=1, edit_target=NULL)
//! is projected into current_messages whenever something else refreshes it.
//! architecture.md: "Invalid edits are stored, never projected and never
//! become messages".

mod common;

use common::*;
use serde_json::json;

fn invalid_replace(id: &str, ts: i64, target: &str) -> serde_json::Value {
    // Same shape as replacement_without_msgtype_is_never_projected: an
    // m.replace whose m.new_content lacks msgtype.
    json!({
        "type": "m.room.message",
        "event_id": id,
        "sender": ALICE,
        "origin_server_ts": ts,
        "content": {
            "msgtype": "m.text",
            "body": "* no msgtype",
            "m.new_content": { "body": "no msgtype" },
            "m.relates_to": { "rel_type": "m.replace", "event_id": target }
        }
    })
}

/// Scenario (a): O, invalid edit I, redaction R of I.
#[test]
fn f10_redacting_an_invalid_edit_does_not_project_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut room = room_update(ROOM);
    room.timeline = vec![
        create_room("11"),
        message("$orig", 100, "original"),
        invalid_replace("$bad", 200, "$orig"),
        redaction("$red", 300, "$bad", true),
    ];
    store
        .apply_sync_batch(&sync_batch("s1", vec![room]), 10)
        .unwrap();

    let conn = db(&path);
    assert_eq!(
        scalar_i64(&conn, "SELECT edit_attempt FROM events WHERE event_id = '$bad'"),
        1,
        "precondition: $bad is stored as an invalid edit attempt"
    );
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM current_messages WHERE event_id = '$bad'"
        ),
        0,
        "an invalid edit must never become a current_messages row, even after it is redacted"
    );
    assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM current_messages"), 1);
}

/// Scenario (b): O, invalid edit I, then a valid same-sender edit E2 that
/// targets I.
#[test]
fn f10_valid_edit_targeting_an_invalid_edit_does_not_project_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    let mut room = room_update(ROOM);
    room.timeline = vec![
        create_room("11"),
        message("$orig", 100, "original"),
        invalid_replace("$bad", 200, "$orig"),
        edit("$e2", 300, "$bad", "edit of an edit"),
    ];
    store
        .apply_sync_batch(&sync_batch("s1", vec![room]), 10)
        .unwrap();

    let conn = db(&path);
    let phantom_body = scalar_string(
        &conn,
        "SELECT (SELECT body FROM current_messages WHERE event_id = '$bad')",
    );
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM current_messages WHERE event_id = '$bad'"
        ),
        0,
        "an invalid edit must never become a current_messages row (phantom body: {phantom_body:?})"
    );
    assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM current_messages"), 1);
}
