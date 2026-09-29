//! CLI integration tests for the real `mln-ingest` binary.
//!
//! These build synthetic temporary archives and exercise export path safety:
//! an export may never truncate an existing file, symlink or hardlink path, and
//! a newly created export is owner-only. Nothing here touches a fixed path or a
//! real archive.

use std::path::Path;
use std::process::{Command, Output};

use mainlinenerd_ingest::event::{SyncBatch, SyncRoomUpdate};
use mainlinenerd_ingest::store::{ArchiveIdentity, Store};
use serde_json::{json, Value};

const BIN: &str = env!("CARGO_BIN_EXE_mln-ingest");
const ROOM: &str = "!room:hs.example.org";
const ALICE: &str = "@alice:hs.example.org";

fn identity() -> ArchiveIdentity {
    ArchiveIdentity {
        homeserver: "https://hs.example.org".to_owned(),
        user_id: "@ingest:hs.example.org".to_owned(),
        device_id: "MLN".to_owned(),
    }
}

fn message(id: &str, ts: i64, body: &str) -> Value {
    json!({
        "type": "m.room.message",
        "event_id": id,
        "sender": ALICE,
        "origin_server_ts": ts,
        "content": { "msgtype": "m.text", "body": body }
    })
}

fn redaction(id: &str, ts: i64, target: &str) -> Value {
    json!({
        "type": "m.room.redaction",
        "event_id": id,
        "sender": ALICE,
        "origin_server_ts": ts,
        "content": { "redacts": target }
    })
}

/// Build a small bound archive with one room and four events.
fn build_archive(path: &Path) {
    let mut store = Store::open(path, &identity()).expect("open archive");
    let mut room = SyncRoomUpdate {
        room_id: ROOM.to_owned(),
        ..Default::default()
    };
    room.timeline = vec![
        json!({
            "type": "m.room.create",
            "event_id": "$create",
            "sender": ALICE,
            "state_key": "",
            "origin_server_ts": 1,
            "content": { "creator": ALICE, "room_version": "11" }
        }),
        message("$a", 100, "one"),
        message("$b", 200, "secret"),
        redaction("$b-red", 250, "$b"),
    ];
    let batch = SyncBatch {
        next_batch: "s1".to_owned(),
        rooms: vec![room],
    };
    store.apply_sync_batch(&batch, 10).expect("apply batch");
}

fn run_export(db: &Path, kind: &str, out: Option<&Path>) -> Output {
    let mut command = Command::new(BIN);
    command
        .arg("--db")
        .arg(db)
        .arg("export")
        .arg("--kind")
        .arg(kind);
    if let Some(out) = out {
        command.arg("--out").arg(out);
    }
    command.output().expect("run mln-ingest")
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn cli_stdout_export_matches_the_read_only_export() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("archive.sqlite3");
    build_archive(&db);

    let store = Store::open_read_only(&db).unwrap();
    let mut expected_events = Vec::new();
    store.export_events(None, &mut expected_events).unwrap();
    let mut expected_messages = Vec::new();
    store.export_messages(None, &mut expected_messages).unwrap();
    drop(store);

    let output = run_export(&db, "events", None);
    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert_eq!(output.stdout, expected_events);
    assert!(
        stderr(&output).contains("exported 4 records"),
        "{}",
        stderr(&output)
    );

    let output = run_export(&db, "messages", None);
    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert_eq!(output.stdout, expected_messages);
}

#[test]
fn export_refuses_an_existing_file_without_truncating_it() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("archive.sqlite3");
    build_archive(&db);

    let existing = dir.path().join("existing.jsonl");
    std::fs::write(&existing, "PRECIOUS").unwrap();

    let output = run_export(&db, "events", Some(&existing));
    assert!(!output.status.success(), "an existing file must be refused");
    assert_eq!(std::fs::read_to_string(&existing).unwrap(), "PRECIOUS");
}

#[test]
fn export_refuses_the_archive_path_itself() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("archive.sqlite3");
    build_archive(&db);

    let output = run_export(&db, "events", Some(&db));
    assert!(
        !output.status.success(),
        "the archive must never be truncated"
    );

    // The archive is still a valid, readable SQLite archive.
    let store = Store::open_read_only(&db).unwrap();
    assert_eq!(store.status().unwrap().rooms.len(), 1);
}

#[cfg(unix)]
#[test]
fn export_refuses_a_symlink_without_following_it() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("archive.sqlite3");
    build_archive(&db);

    let target = dir.path().join("target.jsonl");
    std::fs::write(&target, "TARGET").unwrap();
    let link = dir.path().join("link.jsonl");
    symlink(&target, &link).unwrap();

    let output = run_export(&db, "events", Some(&link));
    assert!(!output.status.success(), "a symlink must be refused");
    assert_eq!(std::fs::read_to_string(&target).unwrap(), "TARGET");
    assert!(std::fs::symlink_metadata(&link)
        .unwrap()
        .file_type()
        .is_symlink());
}

#[cfg(unix)]
#[test]
fn export_refuses_a_hardlink_path_without_truncating_the_original() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("archive.sqlite3");
    build_archive(&db);

    let original = dir.path().join("original.jsonl");
    std::fs::write(&original, "HARD").unwrap();
    let hard = dir.path().join("hard.jsonl");
    std::fs::hard_link(&original, &hard).unwrap();

    let output = run_export(&db, "events", Some(&hard));
    assert!(!output.status.success(), "a hardlink path must be refused");
    assert_eq!(std::fs::read_to_string(&original).unwrap(), "HARD");
    assert_eq!(std::fs::read_to_string(&hard).unwrap(), "HARD");
}

#[test]
fn export_creates_an_owner_only_file_with_correct_output() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("archive.sqlite3");
    build_archive(&db);

    let store = Store::open_read_only(&db).unwrap();
    let mut expected = Vec::new();
    store.export_messages(None, &mut expected).unwrap();
    drop(store);

    let out = dir.path().join("export.jsonl");
    let output = run_export(&db, "messages", Some(&out));
    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert_eq!(std::fs::read(&out).unwrap(), expected);

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&out).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "a new export must be owner-only");
    }
}
