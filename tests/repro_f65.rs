//! Repro for review finding F65: a later sync `m.room.create` overwrites the
//! startup-validated room version with an unvalidated string, after which
//! every redaction in that room fails and the batch (and all replays) roll back.

mod common;

use common::*;
use serde_json::json;

#[test]
fn f65_sync_create_cannot_replace_validated_room_version() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");

    {
        let mut store = open_store(&path);
        // Startup: the runtime validated "11" (validate_room_version) and
        // recorded it as control metadata.
        store.set_room_version_control(ROOM, "11", 5).unwrap();

        // A normal batch archives a message.
        let mut room = room_update(ROOM);
        room.timeline = vec![message("$m", 100, "secret")];
        store
            .apply_sync_batch(&sync_batch("s0", vec![room]), 10)
            .unwrap();

        // A buggy/malicious server sends a create state event with an
        // unknown version in a later sync state block.
        let mut room = room_update(ROOM);
        room.state = vec![json!({
            "type": "m.room.create", "event_id": "$create_bogus", "sender": ALICE,
            "state_key": "", "origin_server_ts": 1,
            "content": { "creator": ALICE, "room_version": "99" }
        })];
        let bogus = store.apply_sync_batch(&sync_batch("s1", vec![room]), 20);
        eprintln!("F65 bogus-create batch result: {:?}", bogus.as_ref().map(|_| ()));
        eprintln!(
            "F65 room_version after bogus create: {:?}",
            store.room_version_of(ROOM).unwrap()
        );
        eprintln!("F65 since after bogus create: {:?}", store.since_token().unwrap());
    }

    // Restart (recovery path): reopen and apply a normal v11 redaction.
    let mut store = open_store(&path);
    let mut room = room_update(ROOM);
    room.timeline = vec![redaction("$red", 200, "$m", true)];
    let batch = sync_batch("s2", vec![room]);
    let first = store.apply_sync_batch(&batch, 30);
    eprintln!("F65 redaction batch result: {:?}", first.as_ref().map(|_| ()));
    // Replay (what a restarted run does with the same since token).
    let replay = store.apply_sync_batch(&batch, 40);
    eprintln!("F65 redaction replay result: {:?}", replay.as_ref().map(|_| ()));
    eprintln!("F65 since after redaction attempts: {:?}", store.since_token().unwrap());

    assert_eq!(
        store.room_version_of(ROOM).unwrap().as_deref(),
        Some("11"),
        "a sync-derived unvalidated create must not replace the validated version"
    );
    assert!(first.is_ok(), "v11 redaction must apply: {first:?}");
    let conn = db(&path);
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT redacted FROM current_messages WHERE event_id = '$m'"
        ),
        1
    );
}
