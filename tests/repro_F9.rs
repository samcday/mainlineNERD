//! Repro for review finding F9: a legacy rich-reply fallback keeps the text of
//! a redacted parent in the reply's body_text, raw_json, current_messages.body
//! and both JSONL exports.

mod common;

use common::*;
use serde_json::json;

#[test]
fn f9_reply_fallback_does_not_resurrect_redacted_parent_text() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.db");
    let mut store = open_store(&path);

    // Bob replies with a spec (pre-v1.13) reply fallback quoting Alice.
    let reply = json!({
        "type": "m.room.message",
        "event_id": "$b",
        "sender": BOB,
        "origin_server_ts": 150,
        "content": {
            "msgtype": "m.text",
            "body": format!("> <{ALICE}> my token is hunter2\n\nplease rotate that"),
            "format": "org.matrix.custom.html",
            "formatted_body": format!(
                "<mx-reply><blockquote><a href=\"https://matrix.to/#/{ROOM}/$a\">In reply to</a> \
                 <a href=\"https://matrix.to/#/{ALICE}\">{ALICE}</a><br>my token is hunter2\
                 </blockquote></mx-reply>please rotate that"
            ),
            "m.relates_to": { "m.in_reply_to": { "event_id": "$a" } }
        }
    });

    let mut room = room_update(ROOM);
    room.timeline = vec![
        create_room("11"),
        message("$a", 100, "my token is hunter2"),
        reply,
        redaction("$red", 200, "$a", true),
    ];
    store
        .apply_sync_batch(&sync_batch("s1", vec![room]), 10)
        .unwrap();

    let mut leaks: Vec<String> = Vec::new();
    {
        let conn = db(&path);
        // Sanity: the parent itself is pruned and the reply is stored.
        assert_eq!(
            scalar_string(&conn, "SELECT body_text FROM events WHERE event_id = '$a'"),
            None,
            "parent body must be pruned"
        );
        assert_eq!(
            scalar_string(
                &conn,
                "SELECT relates_to_event_id FROM events WHERE event_id = '$b'"
            )
            .as_deref(),
            Some("$a"),
            "reply relation must be recorded"
        );

        for (what, sql) in [
            (
                "events.body_text",
                "SELECT body_text FROM events WHERE event_id = '$b'",
            ),
            (
                "events.raw_json",
                "SELECT raw_json FROM events WHERE event_id = '$b'",
            ),
            (
                "current_messages.body",
                "SELECT body FROM current_messages WHERE event_id = '$b'",
            ),
        ] {
            let v = scalar_string(&conn, sql);
            println!("{what} = {v:?}");
            if v.as_deref().is_some_and(|s| s.contains("hunter2")) {
                leaks.push(what.to_owned());
            }
        }
    }

    let mut messages = Vec::new();
    store.export_messages(None, &mut messages).unwrap();
    let messages = String::from_utf8(messages).unwrap();
    println!("export messages:\n{messages}");
    if messages.contains("hunter2") {
        leaks.push("export --kind messages".to_owned());
    }

    let mut events = Vec::new();
    store.export_events(None, &mut events).unwrap();
    let events = String::from_utf8(events).unwrap();
    println!("export events:\n{events}");
    if events.contains("hunter2") {
        leaks.push("export --kind events".to_owned());
    }

    assert!(
        leaks.is_empty(),
        "redacted parent text 'hunter2' survives via reply fallback in: {leaks:?}"
    );
}
