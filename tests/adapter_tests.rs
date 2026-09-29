//! Adapter tests against a loopback mock homeserver exercising the real
//! matrix-sdk typed request/response boundary.
//!
//! Only synthetic JSON and loopback HTTP are used. The tests assert observable
//! database state and the exact requests the adapter made (in particular that
//! it never publishes messages, receipts, presence, typing or key requests).

mod common;

use common::*;
use mainlinenerd_ingest::config::{Config, RoomConfig, RoomSelector, Token};
use mainlinenerd_ingest::engine::{HistoryRequest, SyncRequest, Transport, TransportError};
use mainlinenerd_ingest::matrix::MatrixTransport;
use mainlinenerd_ingest::runtime::{self, RunSettings, RuntimeError, StartupPolicy};
use mainlinenerd_ingest::store::{ArchiveIdentity, HistoryWork, Store, StoreError};
use serde_json::{json, Value};

fn tiny_policy() -> StartupPolicy {
    StartupPolicy {
        max_attempts: 4,
        transient_min: std::time::Duration::from_millis(1),
        transient_max: std::time::Duration::from_millis(2),
        rate_limit_fallback: std::time::Duration::from_millis(1),
    }
}

fn room_id(value: &str) -> RoomConfig {
    RoomConfig {
        selector: RoomSelector::Id(value.to_owned()),
        join: false,
        via: Vec::new(),
    }
}

fn alias(value: &str) -> RoomConfig {
    RoomConfig {
        selector: RoomSelector::Alias(value.to_owned()),
        join: false,
        via: Vec::new(),
    }
}

fn config(server: &MockServer, dir: &std::path::Path, rooms: Vec<RoomConfig>) -> Config {
    Config {
        homeserver: server.base_url.clone(),
        user_id: "@ingest:hs.example.org".to_owned(),
        device_id: "MLN".to_owned(),
        token_env: "MLN_UNUSED".to_owned(),
        data_dir: dir.to_path_buf(),
        database: dir.join("archive.sqlite3"),
        history_interval_ms: 1_000,
        history_limit: 50,
        sync_timeout_ms: 2_000,
        rooms,
    }
}

fn identity(cfg: &Config) -> ArchiveIdentity {
    ArchiveIdentity {
        homeserver: cfg.homeserver.clone(),
        user_id: cfg.user_id.clone(),
        device_id: cfg.device_id.clone(),
    }
}

async fn adapter(_server: &MockServer, cfg: &Config) -> MatrixTransport {
    MatrixTransport::connect(cfg, &Token::new("fake-token"))
        .await
        .expect("connect adapter")
}

fn message(id: &str, body: &str) -> Value {
    json!({
        "type": "m.room.message",
        "event_id": id,
        "sender": "@alice:hs.example.org",
        "origin_server_ts": 100,
        "content": { "msgtype": "m.text", "body": body }
    })
}

fn member(id: &str, user: &str, membership: &str) -> Value {
    json!({
        "type": "m.room.member",
        "event_id": id,
        "sender": user,
        "state_key": user,
        "origin_server_ts": 90,
        "content": { "membership": membership }
    })
}

fn create_event(id: &str, version: &str) -> Value {
    json!({
        "type": "m.room.create",
        "event_id": id,
        "sender": "@alice:hs.example.org",
        "state_key": "",
        "origin_server_ts": 1,
        "content": { "creator": "@alice:hs.example.org", "room_version": version }
    })
}

fn sync_body(next_batch: &str, join: Value) -> Value {
    json!({ "next_batch": next_batch, "rooms": { "join": join } })
}

#[tokio::test]
async fn whoami_mismatch_is_refused_before_ingestion() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(&server, dir.path(), vec![room_id(ROOM)]);
    cfg.user_id = "@someone-else:hs.example.org".to_owned();
    let transport = adapter(&server, &cfg).await;
    let mut store = Store::open(&cfg.database, &identity(&cfg)).unwrap();

    let error = runtime::initialize(&transport, &cfg, &mut store)
        .await
        .unwrap_err();
    assert!(
        matches!(error, RuntimeError::IdentityMismatch { .. }),
        "{error:?}"
    );

    let conn = db(&cfg.database);
    assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM rooms"), 0);
    assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM events"), 0);
}

#[tokio::test]
async fn sync_is_typed_filtered_and_never_marks_presence_online() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(&server, dir.path(), vec![room_id(ROOM), room_id(ROOM2)]);
    let transport = adapter(&server, &cfg).await;
    transport.allow_room(ROOM.parse().unwrap());
    // ROOM2 is configured but deliberately not in the local allowlist yet, to
    // prove the allowlist boundary is enforced on top of the server filter.
    transport.allow_room(ROOM2.parse().unwrap());

    let mut join = serde_json::Map::new();
    join.insert(
        ROOM.to_owned(),
        json!({
            "timeline": {
                "events": [message("$a", "hello"), member("$m1", "@ingest:hs.example.org", "join")],
                "prev_batch": "p1",
                "limited": false
            },
            "state": { "events": [create_event("$create", "11")] },
            "ephemeral": { "events": [ { "type": "m.receipt", "content": {} } ] }
        }),
    );
    join.insert(
        ROOM2.to_owned(),
        json!({
            "timeline": { "events": [message("$other", "should be ignored")], "limited": false },
            "state": { "events": [] }
        }),
    );
    server
        .state
        .push_sync(MockResponse::json(sync_body("s1", Value::Object(join))));

    let batch = transport
        .sync(SyncRequest {
            since: None,
            timeout_ms: 1_000,
        })
        .await
        .unwrap();
    assert_eq!(batch.rooms.len(), 2, "both allowlisted rooms are delivered");

    let mut store = Store::open(&cfg.database, &identity(&cfg)).unwrap();
    store.apply_sync_batch(&batch, 10).unwrap();

    assert_eq!(
        scalar_i64(
            &db(&cfg.database),
            "SELECT COUNT(*) FROM events WHERE event_id = '$a'"
        ),
        1
    );
    assert_eq!(
        scalar_i64(
            &db(&cfg.database),
            "SELECT COUNT(*) FROM events WHERE event_type = 'm.room.member'"
        ),
        0,
        "rosters are never archived"
    );
    assert_eq!(
        scalar_i64(
            &db(&cfg.database),
            "SELECT COUNT(*) FROM events WHERE event_id = '$other'"
        ),
        1
    );
    // Own membership remains a control signal.
    assert_eq!(
        scalar_string(
            &db(&cfg.database),
            "SELECT own_membership FROM rooms WHERE room_id = '!room:hs.example.org'"
        ),
        Some("join".to_owned())
    );

    let requests = server.state.recorded();
    let sync_request = requests
        .iter()
        .find(|request| request.path.ends_with("/sync"))
        .expect("a /sync request");
    assert!(
        sync_request.query.contains("set_presence=offline"),
        "polling must not mark the bot online: {}",
        sync_request.query
    );
    assert_no_write_requests(&requests);
}

#[tokio::test]
async fn unconfigured_room_is_not_persisted_by_the_runtime_allowlist() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(&server, dir.path(), vec![room_id(ROOM)]);
    let transport = adapter(&server, &cfg).await;
    transport.allow_room(ROOM.parse().unwrap());
    // Note: ROOM2 is absent from the local allowlist.

    let mut join = serde_json::Map::new();
    join.insert(
        ROOM.to_owned(),
        json!({ "timeline": { "events": [message("$a", "hello")], "limited": false }, "state": {"events": []} }),
    );
    join.insert(
        ROOM2.to_owned(),
        json!({ "timeline": { "events": [message("$other", "ignored")], "limited": false }, "state": {"events": []} }),
    );
    server
        .state
        .push_sync(MockResponse::json(sync_body("s1", Value::Object(join))));

    let batch = transport
        .sync(SyncRequest {
            since: None,
            timeout_ms: 1_000,
        })
        .await
        .unwrap();
    assert_eq!(batch.rooms.len(), 1);
    assert_eq!(batch.rooms[0].room_id, ROOM);

    let mut store = Store::open(&cfg.database, &identity(&cfg)).unwrap();
    store.apply_sync_batch(&batch, 10).unwrap();
    let conn = db(&cfg.database);
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM rooms WHERE room_id = '!room2:hs.example.org'"
        ),
        0
    );
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM events WHERE room_id = '!room2:hs.example.org'"
        ),
        0
    );
}

#[tokio::test]
async fn alias_resolution_is_pinned_and_drift_is_refused() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(&server, dir.path(), vec![alias("#eng:hs.example.org")]);
    server.state.set_alias("#eng:hs.example.org", ROOM);
    let transport = adapter(&server, &cfg).await;
    let mut store = Store::open(&cfg.database, &identity(&cfg)).unwrap();

    let report = runtime::initialize(&transport, &cfg, &mut store)
        .await
        .unwrap();
    assert_eq!(report.rooms, vec![ROOM.to_owned()]);
    assert_eq!(
        store.alias_pin("#eng:hs.example.org").unwrap(),
        Some(ROOM.to_owned())
    );

    // The alias now resolves elsewhere: refuse rather than widen the allowlist.
    server
        .state
        .set_alias("#eng:hs.example.org", "!evil:hs.example.org");
    let transport2 = adapter(&server, &cfg).await;
    let error = runtime::initialize(&transport2, &cfg, &mut store)
        .await
        .unwrap_err();
    assert!(
        matches!(error, RuntimeError::Store(StoreError::AliasDrift { .. })),
        "drift must be refused: {error:?}"
    );
    assert_eq!(
        store.alias_pin("#eng:hs.example.org").unwrap(),
        Some(ROOM.to_owned()),
        "the pin is unchanged"
    );
}

#[tokio::test]
async fn messages_are_typed_and_apply_through_the_store() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(&server, dir.path(), vec![room_id(ROOM)]);
    let transport = adapter(&server, &cfg).await;
    transport.allow_room(ROOM.parse().unwrap());

    let mut store = Store::open(&cfg.database, &identity(&cfg)).unwrap();
    // Seed a room and its base history cursor directly. The create state
    // establishes room version 11, which redaction target resolution needs.
    let mut room = common::room_update(ROOM);
    room.timeline = vec![message("$live", "live")];
    room.state = vec![create_event("$create", "11")];
    room.prev_batch = Some("p1".to_owned());
    store
        .apply_sync_batch(&sync_batch("s1", vec![room]), 10)
        .unwrap();

    let thread_message = json!({
        "type": "m.room.message",
        "event_id": "$thread",
        "sender": "@alice:hs.example.org",
        "origin_server_ts": 50,
        "content": {
            "msgtype": "m.text",
            "body": "in thread",
            "m.relates_to": { "rel_type": "m.thread", "event_id": "$root" }
        }
    });
    let edit = json!({
        "type": "m.room.message",
        "event_id": "$edit",
        "sender": "@alice:hs.example.org",
        "origin_server_ts": 60,
        "content": {
            "msgtype": "m.text",
            "body": "* fixed",
            "m.new_content": { "msgtype": "m.text", "body": "fixed" },
            "m.relates_to": { "rel_type": "m.replace", "event_id": "$original" }
        }
    });
    let original = message("$original", "original body");
    let redaction = json!({
        "type": "m.room.redaction",
        "event_id": "$red",
        "sender": "@alice:hs.example.org",
        "origin_server_ts": 70,
        "content": { "redacts": "$original" }
    });
    server.state.push_messages(
        "p1",
        MockResponse::json(json!({
            "start": "p1",
            "end": "p2",
            "chunk": [original, thread_message, edit, redaction]
        })),
    );

    let page = transport
        .history(HistoryRequest {
            room_id: ROOM.to_owned(),
            from: "p1".to_owned(),
            to: None,
            limit: 50,
        })
        .await
        .unwrap();
    assert_eq!(page.chunk.len(), 4);
    assert_eq!(page.end.as_deref(), Some("p2"));

    let applied = store
        .apply_history_page(ROOM, HistoryWork::Base, "p1", &page, 20)
        .unwrap();
    assert_eq!(applied.events_seen, 4);
    let conn = db(&cfg.database);
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM events WHERE event_id = '$thread'"
        ),
        1
    );
    // The redaction pruned the original body in the archive.
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body_text FROM events WHERE event_id = '$original'"
        ),
        None
    );
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM events WHERE event_type = 'm.room.member'"
        ),
        0
    );
}

#[tokio::test]
async fn create_state_bootstrap_handles_event_and_content_only() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(&server, dir.path(), vec![room_id(ROOM), room_id(ROOM2)]);
    server
        .state
        .set_create_state(ROOM, MockResponse::json(create_event("$create1", "10")));
    server
        .state
        .set_create_state(ROOM2, MockResponse::json(json!({ "room_version": "11" })));
    let transport = adapter(&server, &cfg).await;
    let mut store = Store::open(&cfg.database, &identity(&cfg)).unwrap();

    let report = runtime::initialize(&transport, &cfg, &mut store)
        .await
        .unwrap();
    assert_eq!(report.rooms.len(), 2);
    assert_eq!(store.room_version_of(ROOM).unwrap().as_deref(), Some("10"));
    assert_eq!(store.room_version_of(ROOM2).unwrap().as_deref(), Some("11"));

    let conn = db(&cfg.database);
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM events WHERE event_id = '$create1'"
        ),
        1,
        "a real create state event is archived"
    );
    // The content-only response must not invent an event id.
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM events WHERE room_id = '!room2:hs.example.org'"
        ),
        0
    );
}

#[tokio::test]
async fn encryption_upgrade_and_own_leave_flag_and_stop_work() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(&server, dir.path(), vec![room_id(ROOM), room_id(ROOM2)]);
    let transport = adapter(&server, &cfg).await;
    transport.allow_room(ROOM.parse().unwrap());
    transport.allow_room(ROOM2.parse().unwrap());

    let mut join = serde_json::Map::new();
    join.insert(
        ROOM.to_owned(),
        json!({
            "timeline": { "events": [], "limited": false },
            "state": { "events": [
                { "type": "m.room.encryption", "event_id": "$enc", "sender": "@alice:hs.example.org",
                  "state_key": "", "origin_server_ts": 5, "content": { "algorithm": "m.megolm.v1.aes-sha2" } },
                { "type": "m.room.tombstone", "event_id": "$tomb", "sender": "@alice:hs.example.org",
                  "state_key": "", "origin_server_ts": 6,
                  "content": { "body": "upgraded", "replacement_room": "!new:hs.example.org" } }
            ] }
        }),
    );
    join.insert(
        ROOM2.to_owned(),
        json!({
            "timeline": { "events": [member("$leave", "@ingest:hs.example.org", "leave")], "limited": false },
            "state": { "events": [] }
        }),
    );
    server
        .state
        .push_sync(MockResponse::json(sync_body("s1", Value::Object(join))));

    let batch = transport
        .sync(SyncRequest {
            since: None,
            timeout_ms: 1_000,
        })
        .await
        .unwrap();
    let mut store = Store::open(&cfg.database, &identity(&cfg)).unwrap();
    store.apply_sync_batch(&batch, 10).unwrap();

    let report = store.status().unwrap();
    let room = report.rooms.iter().find(|r| r.room_id == ROOM).unwrap();
    assert!(room.encrypted);
    assert!(room.successor_room_id.is_some());
    assert!(!store.room_history_allowed(ROOM).unwrap());
    let left = report.rooms.iter().find(|r| r.room_id == ROOM2).unwrap();
    assert_eq!(left.own_membership.as_deref(), Some("leave"));
    assert!(left.inactive);
    assert!(!store.room_history_allowed(ROOM2).unwrap());
}

#[tokio::test]
async fn rate_limit_and_status_classification() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(&server, dir.path(), vec![room_id(ROOM)]);
    let transport = adapter(&server, &cfg).await;
    transport.allow_room(ROOM.parse().unwrap());
    let request = || SyncRequest {
        since: None,
        timeout_ms: 1_000,
    };

    // 429 with a body hint.
    server.state.push_sync(MockResponse::matrix_error(
        429,
        "M_LIMIT_EXCEEDED",
        "slow down",
    ));
    match transport.sync(request()).await {
        Err(TransportError::RateLimited { retry_after_ms }) => assert_eq!(retry_after_ms, None),
        other => panic!("expected rate limit, got {other:?}"),
    }

    // 429 header delay takes precedence over the body hint.
    server.state.push_sync(
        MockResponse::matrix_error(429, "M_LIMIT_EXCEEDED", "slow down").header("Retry-After", "2"),
    );
    match transport.sync(request()).await {
        Err(TransportError::RateLimited { retry_after_ms }) => {
            assert_eq!(retry_after_ms, Some(2_000))
        }
        other => panic!("expected rate limit, got {other:?}"),
    }

    // 5xx is transient.
    server
        .state
        .push_sync(MockResponse::matrix_error(500, "M_UNKNOWN", "boom"));
    assert!(matches!(
        transport.sync(request()).await,
        Err(TransportError::Transient(_))
    ));

    // 401 is an authentication failure.
    server.state.push_sync(MockResponse::matrix_error(
        401,
        "M_UNKNOWN_TOKEN",
        "bad token",
    ));
    assert!(matches!(
        transport.sync(request()).await,
        Err(TransportError::Authentication(_))
    ));

    // A room-local 403 on /messages does not stop other rooms.
    server.state.push_messages(
        "p1",
        MockResponse::matrix_error(403, "M_FORBIDDEN", "no access"),
    );
    match transport
        .history(HistoryRequest {
            room_id: ROOM.to_owned(),
            from: "p1".to_owned(),
            to: None,
            limit: 10,
        })
        .await
    {
        Err(TransportError::RoomUnavailable(_)) => {}
        other => panic!("expected room unavailable, got {other:?}"),
    }
}

#[tokio::test]
async fn explicit_join_only_for_configured_opt_in_and_never_after_leave() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let mut join_room = room_id(ROOM);
    join_room.join = true;
    let cfg = config(&server, dir.path(), vec![join_room]);
    server
        .state
        .join_result
        .lock()
        .unwrap()
        .replace(ROOM.to_owned());
    let transport = adapter(&server, &cfg).await;
    let mut store = Store::open(&cfg.database, &identity(&cfg)).unwrap();

    let report = runtime::initialize(&transport, &cfg, &mut store)
        .await
        .unwrap();
    assert_eq!(report.joined, vec![ROOM.to_owned()]);
    let joins = server
        .state
        .recorded()
        .into_iter()
        .filter(|r| r.method == "POST" && r.path.contains("/join/"))
        .count();
    assert_eq!(joins, 1);

    // Once a departure is observed, an explicit join is not retried.
    let mut room = common::room_update(ROOM);
    room.own_membership = Some("leave".to_owned());
    store
        .apply_sync_batch(&sync_batch("s1", vec![room]), 10)
        .unwrap();
    let transport2 = adapter(&server, &cfg).await;
    let report = runtime::initialize(&transport2, &cfg, &mut store)
        .await
        .unwrap();
    assert!(
        report.joined.is_empty(),
        "a leave must not be auto-reversed"
    );
}

fn assert_no_write_requests(requests: &[MockRequest]) {
    for request in requests {
        for forbidden in [
            "/send/",
            "/receipt/",
            "/presence/",
            "/keys/",
            "/invite",
            "/typing",
            "/profile",
        ] {
            assert!(
                !request.path.contains(forbidden),
                "adapter must not call {}: {}",
                forbidden,
                request.path_with_query()
            );
        }
        assert!(
            request.method != "PUT" && request.method != "DELETE",
            "adapter must not mutate server state: {} {}",
            request.method,
            request.path_with_query()
        );
        assert!(
            request.method != "POST" || request.path.contains("/join/"),
            "adapter must only POST joins, not {}",
            request.path_with_query()
        );
    }
}

fn power_levels() -> Value {
    json!({
        "type": "m.room.power_levels",
        "event_id": "$power",
        "sender": "@alice:hs.example.org",
        "state_key": "",
        "origin_server_ts": 80,
        "content": { "users": { "@alice:hs.example.org": 100 }, "users_default": 0 }
    })
}

#[tokio::test]
async fn bootstrap_metadata_is_validated_and_recovers() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(&server, dir.path(), vec![room_id(ROOM), room_id(ROOM2)]);
    server
        .state
        .set_create_state(ROOM2, MockResponse::json(json!({ "room_version": "11" })));
    let transport = adapter(&server, &cfg).await;
    let mut store = Store::open(&cfg.database, &identity(&cfg)).unwrap();

    let bad_shapes = [
        json!([]),
        json!({ "room_version": 11 }),
        json!({ "room_version": null }),
        json!({ "room_version": true }),
        json!({ "room_version": { "v": 11 } }),
        json!({ "room_version": "999" }),
        json!({ "type": "m.room.message", "event_id": "$c", "state_key": "", "content": {} }),
        json!({ "type": "m.room.create", "event_id": "$c", "state_key": "x", "content": {} }),
        json!({ "room_id": "!elsewhere:hs.example.org", "type": "m.room.create",
                "event_id": "$c", "state_key": "", "content": { "room_version": "11" } }),
        // A present room_id of any non-string type is a wrong type, not absence.
        json!({ "room_id": null, "type": "m.room.create", "event_id": "$c",
                "state_key": "", "content": { "room_version": "11" } }),
        json!({ "room_id": 7, "type": "m.room.create", "event_id": "$c",
                "state_key": "", "content": { "room_version": "11" } }),
        json!({ "room_id": { "x": 1 }, "type": "m.room.create", "event_id": "$c",
                "state_key": "", "content": { "room_version": "11" } }),
        json!({ "room_id": "!SECRET_MARKER:hs.example.org", "type": "m.room.create",
                "event_id": "$c", "state_key": "", "content": { "room_version": "11" } }),
    ];
    for bad in &bad_shapes {
        server
            .state
            .set_create_state(ROOM, MockResponse::json(bad.clone()));
        let report = runtime::initialize_with_policy(&transport, &cfg, &mut store, &tiny_policy())
            .await
            .unwrap();
        assert!(
            store.room_version_of(ROOM).unwrap().is_none(),
            "no guessed version for {bad}"
        );
        let note = store.room_metadata_error(ROOM).unwrap();
        assert!(note.is_some(), "the bad room is reported unready for {bad}");
        assert!(
            !note.unwrap().contains("SECRET_MARKER"),
            "a malformed value must not be echoed"
        );
        assert!(report.unready.iter().any(|(room, _)| room == ROOM));
        assert_eq!(store.room_version_of(ROOM2).unwrap().as_deref(), Some("11"));
        assert!(report.rooms.iter().any(|room| room == ROOM2));
    }

    // Correcting the response lets a restart recover without a poisoned value.
    server
        .state
        .set_create_state(ROOM, MockResponse::json(create_event("$create", "10")));
    let transport = adapter(&server, &cfg).await;
    let report = runtime::initialize_with_policy(&transport, &cfg, &mut store, &tiny_policy())
        .await
        .unwrap();
    assert_eq!(store.room_version_of(ROOM).unwrap().as_deref(), Some("10"));
    assert!(store.room_metadata_error(ROOM).unwrap().is_none());
    assert!(report.rooms.iter().any(|room| room == ROOM));
}

#[tokio::test]
async fn bootstrap_versions_resolve_pre_v11_and_v11_redaction_targets() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(&server, dir.path(), vec![room_id(ROOM), room_id(ROOM2)]);
    server
        .state
        .set_create_state(ROOM, MockResponse::json(create_event("$create", "10")));
    server
        .state
        .set_create_state(ROOM2, MockResponse::json(json!({ "room_version": "11" })));
    let transport = adapter(&server, &cfg).await;
    let mut store = Store::open(&cfg.database, &identity(&cfg)).unwrap();
    runtime::initialize_with_policy(&transport, &cfg, &mut store, &tiny_policy())
        .await
        .unwrap();
    transport.allow_room(ROOM.parse().unwrap());
    transport.allow_room(ROOM2.parse().unwrap());

    // Seed base cursors, then feed a redaction through the typed /messages path.
    for room in [ROOM, ROOM2] {
        let mut update = room_update(room);
        update.timeline = vec![message("$live", "live")];
        update.prev_batch = Some("p1".to_owned());
        store
            .apply_sync_batch(&sync_batch("s1", vec![update]), 10)
            .unwrap();
    }
    let pre_v11 = json!({
        "type": "m.room.redaction", "event_id": "$red", "sender": "@alice:hs.example.org",
        "origin_server_ts": 90, "redacts": "$target", "content": {}
    });
    let v11 = json!({
        "type": "m.room.redaction", "event_id": "$red", "sender": "@alice:hs.example.org",
        "origin_server_ts": 90, "content": { "redacts": "$target" }
    });
    server.state.push_messages(
        "p1",
        MockResponse::json(
            json!({ "start": "p1", "end": null, "chunk": [message("$target", "old"), pre_v11] }),
        ),
    );
    let page = transport
        .history(HistoryRequest {
            room_id: ROOM.to_owned(),
            from: "p1".to_owned(),
            to: None,
            limit: 10,
        })
        .await
        .unwrap();
    let applied = store
        .apply_history_page(ROOM, HistoryWork::Base, "p1", &page, 20)
        .unwrap();
    assert!(applied.events_seen >= 1, "pre-v11 redaction resolved");

    server.state.push_messages(
        "p1",
        MockResponse::json(
            json!({ "start": "p1", "end": null, "chunk": [message("$target", "old"), v11] }),
        ),
    );
    let page = transport
        .history(HistoryRequest {
            room_id: ROOM2.to_owned(),
            from: "p1".to_owned(),
            to: None,
            limit: 10,
        })
        .await
        .unwrap();
    let applied = store
        .apply_history_page(ROOM2, HistoryWork::Base, "p1", &page, 20)
        .unwrap();
    assert!(applied.events_seen >= 1, "v11+ redaction resolved");
}

#[tokio::test]
async fn startup_isolation_and_bounded_metadata_retries() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let mut join_room = room_id(ROOM);
    join_room.join = true;
    let cfg = config(&server, dir.path(), vec![join_room, room_id(ROOM2)]);
    // The first room's join is refused; the second's metadata is briefly 5xx.
    server
        .state
        .join_response
        .lock()
        .unwrap()
        .replace(MockResponse::matrix_error(403, "M_FORBIDDEN", "no"));
    server
        .state
        .push_create_state(ROOM2, MockResponse::matrix_error(500, "M_UNKNOWN", "boom"));
    server.state.push_create_state(
        ROOM2,
        MockResponse::matrix_error(429, "M_LIMIT_EXCEEDED", "slow"),
    );
    server
        .state
        .push_create_state(ROOM2, MockResponse::json(json!({ "room_version": "11" })));

    let transport = adapter(&server, &cfg).await;
    let mut store = Store::open(&cfg.database, &identity(&cfg)).unwrap();
    let report = runtime::initialize_with_policy(&transport, &cfg, &mut store, &tiny_policy())
        .await
        .expect("a room-local startup failure must not abort the others");

    assert_eq!(report.joined.len(), 0, "the refused join is not reported");
    assert!(report.rooms.iter().any(|room| room == ROOM2));
    assert_eq!(store.room_version_of(ROOM2).unwrap().as_deref(), Some("11"));
    assert!(report.unready.iter().any(|(room, _)| room == ROOM));
    let conn = db(&cfg.database);
    let note = scalar_string(
        &conn,
        "SELECT metadata_error FROM rooms WHERE room_id = '!room:hs.example.org'",
    );
    assert!(note.is_some(), "status must explain the unready room");
}

#[tokio::test]
async fn device_binding_must_be_reported_and_match() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(&server, dir.path(), vec![room_id(ROOM)]);

    // Matching device (the default mock) is accepted.
    let transport = adapter(&server, &cfg).await;
    let mut store = Store::open(&cfg.database, &identity(&cfg)).unwrap();
    runtime::initialize_with_policy(&transport, &cfg, &mut store, &tiny_policy())
        .await
        .unwrap();

    *server.state.whoami.lock().unwrap() =
        Some(json!({ "user_id": "@ingest:hs.example.org", "device_id": "OTHER" }));
    let transport = adapter(&server, &cfg).await;
    let error = runtime::initialize_with_policy(&transport, &cfg, &mut store, &tiny_policy())
        .await
        .unwrap_err();
    assert!(
        matches!(error, RuntimeError::IdentityMismatch { .. }),
        "{error:?}"
    );

    *server.state.whoami.lock().unwrap() = Some(json!({ "user_id": "@ingest:hs.example.org" }));
    let transport = adapter(&server, &cfg).await;
    let error = runtime::initialize_with_policy(&transport, &cfg, &mut store, &tiny_policy())
        .await
        .unwrap_err();
    assert!(
        matches!(error, RuntimeError::DeviceUnverified(_)),
        "{error:?}"
    );
}

#[tokio::test]
async fn alias_pins_are_case_sensitive_and_join_uses_retained_servers() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(
        &server,
        dir.path(),
        vec![alias("#Room:hs.example.org"), alias("#room:hs.example.org")],
    );
    server.state.set_alias("#Room:hs.example.org", ROOM);
    server.state.set_alias("#room:hs.example.org", ROOM2);
    let transport = adapter(&server, &cfg).await;
    let mut store = Store::open(&cfg.database, &identity(&cfg)).unwrap();
    let report = runtime::initialize_with_policy(&transport, &cfg, &mut store, &tiny_policy())
        .await
        .unwrap();
    assert_eq!(report.rooms.len(), 2);
    assert_eq!(
        store.alias_pin("#Room:hs.example.org").unwrap(),
        Some(ROOM.to_owned())
    );
    assert_eq!(
        store.alias_pin("#room:hs.example.org").unwrap(),
        Some(ROOM2.to_owned())
    );

    // Drift on one exact alias is still refused.
    server
        .state
        .set_alias("#Room:hs.example.org", "!other:hs.example.org");
    let transport = adapter(&server, &cfg).await;
    let error = runtime::initialize_with_policy(&transport, &cfg, &mut store, &tiny_policy())
        .await
        .unwrap_err();
    assert!(
        matches!(error, RuntimeError::Store(StoreError::AliasDrift { .. })),
        "{error:?}"
    );

    // A join retains the alias' server hints.
    server.state.hold_sync(false);
    server
        .state
        .join_requires_via
        .store(true, std::sync::atomic::Ordering::SeqCst);
    server
        .state
        .set_alias_with_servers("#eng:hs.example.org", ROOM, &["hs2.example.org"]);
    let mut eng = alias("#eng:hs.example.org");
    eng.join = true;
    let cfg = config(&server, dir.path(), vec![eng]);
    let transport = adapter(&server, &cfg).await;
    let mut store = Store::open(&dir.path().join("second.db"), &identity(&cfg)).unwrap();
    let report = runtime::initialize_with_policy(&transport, &cfg, &mut store, &tiny_policy())
        .await
        .unwrap();
    assert_eq!(report.joined, vec![ROOM.to_owned()]);
    let joins = server
        .state
        .recorded()
        .into_iter()
        .filter(|request| request.method == "POST" && request.path.contains("/join/"))
        .collect::<Vec<_>>();
    assert!(
        joins
            .last()
            .is_some_and(|request| request.query.contains("via=hs2.example.org")),
        "the join must carry the retained via hint: {joins:?}"
    );
}

#[tokio::test]
async fn local_selection_drops_rosters_and_power_levels() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(&server, dir.path(), vec![room_id(ROOM)]);
    let transport = adapter(&server, &cfg).await;
    transport.allow_room(ROOM.parse().unwrap());

    let mut join = serde_json::Map::new();
    join.insert(
        ROOM.to_owned(),
        json!({
            "timeline": { "events": [
                message("$live", "hello"),
                member("$self", "@ingest:hs.example.org", "join"),
                member("$other", "@bob:hs.example.org", "join"),
                power_levels()
            ], "prev_batch": "p1", "limited": false },
            "state": { "events": [create_event("$create", "11")] }
        }),
    );
    server
        .state
        .push_sync(MockResponse::json(sync_body("s1", Value::Object(join))));
    let batch = transport
        .sync(SyncRequest {
            since: None,
            timeout_ms: 1_000,
        })
        .await
        .unwrap();
    let mut store = Store::open(&cfg.database, &identity(&cfg)).unwrap();
    store.apply_sync_batch(&batch, 10).unwrap();
    assert_eq!(store.own_membership(ROOM).unwrap().as_deref(), Some("join"));

    // History returns the same forbidden types despite the filter.
    let edit = json!({
        "type": "m.room.message", "event_id": "$edit", "sender": "@alice:hs.example.org",
        "origin_server_ts": 60,
        "content": { "msgtype": "m.text", "body": "* fixed",
            "m.new_content": { "msgtype": "m.text", "body": "fixed" },
            "m.relates_to": { "rel_type": "m.replace", "event_id": "$oldmsg" } }
    });
    server.state.push_messages(
        "p1",
        MockResponse::json(json!({ "start": "p1", "end": null, "chunk": [
            member("$old", "@bob:hs.example.org", "leave"),
            power_levels(),
            message("$oldmsg", "original"),
            edit
        ] })),
    );
    let page = transport
        .history(HistoryRequest {
            room_id: ROOM.to_owned(),
            from: "p1".to_owned(),
            to: None,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(
        page.chunk.len(),
        2,
        "only message-like events survive selection"
    );
    store
        .apply_history_page(ROOM, HistoryWork::Base, "p1", &page, 20)
        .unwrap();

    let conn = db(&cfg.database);
    for excluded in ["m.room.member", "m.room.power_levels"] {
        assert_eq!(
            scalar_i64(
                &conn,
                &format!("SELECT COUNT(*) FROM events WHERE event_type = '{excluded}'")
            ),
            0,
            "{excluded} must not be archived"
        );
    }
    let mut raw = Vec::new();
    store.export_events(None, &mut raw).unwrap();
    let raw = String::from_utf8(raw).unwrap();
    assert!(
        !raw.contains("users_default"),
        "power-level map leaked into export"
    );
    // Backfilled membership never rewrites current membership.
    assert_eq!(store.own_membership(ROOM).unwrap().as_deref(), Some("join"));
    // A real message and its edit still work.
    assert_eq!(
        scalar_string(
            &conn,
            "SELECT body FROM current_messages WHERE event_id = '$oldmsg'"
        )
        .as_deref(),
        Some("fixed")
    );
}

#[tokio::test]
async fn diagnostics_never_leak_tokens_urls_or_server_text() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(&server, dir.path(), vec![room_id(ROOM)]);
    let transport = adapter(&server, &cfg).await;
    transport.allow_room(ROOM.parse().unwrap());
    server.state.push_messages(
        "p1",
        MockResponse::matrix_error(403, "M_FORBIDDEN", "SECRET_SERVER_TEXT"),
    );
    let error = transport
        .history(HistoryRequest {
            room_id: ROOM.to_owned(),
            from: "p1".to_owned(),
            to: None,
            limit: 10,
        })
        .await
        .unwrap_err()
        .to_string();
    assert!(
        !error.contains("SECRET_SERVER_TEXT"),
        "server text leaked: {error}"
    );

    // A network failure after the server is gone must not stringify the URL
    // (which carries the `since` pagination token).
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(&server, dir.path(), vec![room_id(ROOM)]);
    let transport = adapter(&server, &cfg).await;
    server.stop();
    let error = transport
        .sync(SyncRequest {
            since: Some("SECRET_SINCE_TOKEN".to_owned()),
            timeout_ms: 100,
        })
        .await
        .unwrap_err()
        .to_string();
    assert!(
        !error.contains("SECRET_SINCE_TOKEN"),
        "pagination token leaked: {error}"
    );
    assert!(!error.contains("http://"), "request URL leaked: {error}");
}

#[tokio::test]
async fn idle_configured_room_is_seeded_from_the_committed_token() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(&server, dir.path(), vec![room_id(ROOM)]);
    server
        .state
        .set_create_state(ROOM, MockResponse::json(json!({ "room_version": "11" })));

    // An existing archive already has a committed global token.
    let mut store = Store::open(&cfg.database, &identity(&cfg)).unwrap();
    store
        .apply_sync_batch(&sync_batch("s5", vec![]), 10)
        .unwrap();

    let transport = adapter(&server, &cfg).await;
    let report = runtime::initialize_with_policy(&transport, &cfg, &mut store, &tiny_policy())
        .await
        .unwrap();
    assert!(report.rooms.iter().any(|room| room == ROOM));
    assert_eq!(
        store.room_history_token(ROOM).unwrap().as_deref(),
        Some("s5")
    );

    // The idle room is backfilled from that cursor even with no room event.
    server.state.push_messages(
        "s5",
        MockResponse::json(json!({ "start": "s5", "end": "s4", "chunk": [
            message("$hist", "history")
        ] })),
    );
    let engine = mainlinenerd_ingest::engine::Engine::new(
        transport,
        store,
        mainlinenerd_ingest::engine::EngineConfig {
            sync_timeout_ms: 1_000,
            history_limit: 10,
            max_pages_per_room: 4,
        },
    );
    let (tx, rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(runtime::run(engine, RunSettings::run(1), async move {
        let _ = rx.await;
    }));
    for _ in 0..200 {
        if scalar_i64(
            &db(&cfg.database),
            "SELECT COUNT(*) FROM events WHERE event_id = '$hist'",
        ) == 1
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let _ = tx.send(());
    handle.await.unwrap().unwrap();
    assert_eq!(
        scalar_i64(
            &db(&cfg.database),
            "SELECT COUNT(*) FROM events WHERE event_id = '$hist'"
        ),
        1,
        "the idle configured room's history was collected"
    );
}

fn room_with_via(id: &str, join: bool, via: &[&str]) -> RoomConfig {
    RoomConfig {
        selector: RoomSelector::Id(id.to_owned()),
        join,
        via: via.iter().map(|server| (*server).to_owned()).collect(),
    }
}

#[tokio::test]
async fn explicit_join_precedes_metadata_and_uses_via() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(
        &server,
        dir.path(),
        vec![room_with_via(ROOM, true, &["hs2.example.org"])],
    );
    // A cold public-join room that refuses state until the bot joins, and a
    // homeserver that needs the via hint to route the join.
    server
        .state
        .metadata_requires_join
        .store(true, std::sync::atomic::Ordering::SeqCst);
    server
        .state
        .join_requires_via
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let transport = adapter(&server, &cfg).await;
    let mut store = Store::open(&cfg.database, &identity(&cfg)).unwrap();

    let report = runtime::initialize_with_policy(&transport, &cfg, &mut store, &tiny_policy())
        .await
        .unwrap();
    assert_eq!(report.joined, vec![ROOM.to_owned()]);
    assert!(
        report.rooms.iter().any(|room| room == ROOM),
        "metadata after the join makes the room ready: {:?}",
        report.unready
    );
    assert_eq!(store.room_version_of(ROOM).unwrap().as_deref(), Some("11"));

    let requests = server.state.recorded();
    let join_index = requests
        .iter()
        .position(|request| request.method == "POST" && request.path.contains("/join/"))
        .expect("the explicit join was attempted");
    let state_index = requests
        .iter()
        .position(|request| request.path.contains("/state/m.room.create"))
        .expect("metadata was fetched");
    assert!(
        join_index < state_index,
        "the join must precede metadata: {:?}",
        requests
            .iter()
            .map(|r| (r.method.clone(), r.path.clone()))
            .collect::<Vec<_>>()
    );
    assert!(
        requests[join_index].query.contains("via=hs2.example.org"),
        "the join must carry the explicit via hint: {}",
        requests[join_index].query
    );
    assert_no_write_requests(&requests);
}

#[tokio::test]
async fn deferred_room_is_not_admitted_but_healthy_scope_advances() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(&server, dir.path(), vec![room_id(ROOM), room_id(ROOM2)]);
    // A's metadata never validates; B is healthy.
    server
        .state
        .set_create_state(ROOM, MockResponse::json(json!({ "room_version": 7 })));
    server
        .state
        .set_create_state(ROOM2, MockResponse::json(json!({ "room_version": "11" })));
    let transport = adapter(&server, &cfg).await;
    let mut store = Store::open(&cfg.database, &identity(&cfg)).unwrap();
    let report = runtime::initialize_with_policy(&transport, &cfg, &mut store, &tiny_policy())
        .await
        .unwrap();
    assert!(report.unready.iter().any(|(room, _)| room == ROOM));
    assert!(report.rooms.iter().any(|room| room == ROOM2));
    assert!(
        !transport.is_allowed_room(ROOM),
        "an unready candidate is not in the data-admission set"
    );

    // The server sends both rooms despite its filter: only the admitted one is
    // stored, while the global checkpoint still advances.
    let mut join = serde_json::Map::new();
    join.insert(
        ROOM.to_owned(),
        json!({ "timeline": { "events": [message("$deferred-live", "no")], "limited": false },
                "state": { "events": [] } }),
    );
    join.insert(
        ROOM2.to_owned(),
        json!({ "timeline": { "events": [message("$healthy", "yes")], "limited": false },
                "state": { "events": [] } }),
    );
    server
        .state
        .push_sync(MockResponse::json(sync_body("s1", Value::Object(join))));
    let batch = transport
        .sync(SyncRequest {
            since: None,
            timeout_ms: 1_000,
        })
        .await
        .unwrap();
    assert_eq!(batch.rooms.len(), 1);
    assert_eq!(batch.rooms[0].room_id, ROOM2);
    store.apply_sync_batch(&batch, 10).unwrap();
    let conn = db(&cfg.database);
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM events WHERE event_id = '$deferred-live'"
        ),
        0,
        "an unready room's live data is never archived"
    );
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM events WHERE event_id = '$healthy'"
        ),
        1
    );
    assert_eq!(
        scalar_string(&conn, "SELECT since_token FROM sync_progress WHERE id = 1"),
        Some("s1".to_owned()),
        "the healthy scope's checkpoint advances"
    );
    assert!(store.room_metadata_error(ROOM).unwrap().is_some());

    // A corrected restart admits A, seeds its cursor from the committed token
    // and backfills the previously deferred accessible history.
    server
        .state
        .set_create_state(ROOM, MockResponse::json(create_event("$createA", "10")));
    let transport = adapter(&server, &cfg).await;
    let report = runtime::initialize_with_policy(&transport, &cfg, &mut store, &tiny_policy())
        .await
        .unwrap();
    assert!(report.rooms.iter().any(|room| room == ROOM));
    assert_eq!(store.room_version_of(ROOM).unwrap().as_deref(), Some("10"));
    assert_eq!(
        store.room_history_token(ROOM).unwrap().as_deref(),
        Some("s1")
    );
    assert!(transport.is_allowed_room(ROOM));

    server.state.push_messages(
        "s1",
        MockResponse::json(json!({ "start": "s1", "end": null, "chunk": [
            message("$deferred-hist", "deferred")
        ] })),
    );
    let page = transport
        .history(HistoryRequest {
            room_id: ROOM.to_owned(),
            from: "s1".to_owned(),
            to: None,
            limit: 10,
        })
        .await
        .unwrap();
    store
        .apply_history_page(ROOM, HistoryWork::Base, "s1", &page, 20)
        .unwrap();
    assert_eq!(
        scalar_i64(
            &db(&cfg.database),
            "SELECT COUNT(*) FROM events WHERE event_id = '$deferred-hist'"
        ),
        1
    );
}

#[tokio::test]
async fn final_rate_limit_pause_is_honored_before_next_room() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(&server, dir.path(), vec![room_id(ROOM), room_id(ROOM2)]);
    // A exhausts its bounded retries, every attempt carrying a 300ms hint.
    for _ in 0..3 {
        server.state.push_create_state(
            ROOM,
            MockResponse::status(
                429,
                json!({ "errcode": "M_LIMIT_EXCEEDED", "error": "slow", "retry_after_ms": 300 }),
            ),
        );
    }
    server
        .state
        .set_create_state(ROOM2, MockResponse::json(json!({ "room_version": "11" })));
    let transport = adapter(&server, &cfg).await;
    let mut store = Store::open(&cfg.database, &identity(&cfg)).unwrap();
    let policy = StartupPolicy {
        max_attempts: 2,
        ..tiny_policy()
    };

    let started = std::time::Instant::now();
    let report = runtime::initialize_with_policy(&transport, &cfg, &mut store, &policy)
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert!(report.unready.iter().any(|(room, _)| room == ROOM));
    assert!(report.rooms.iter().any(|room| room == ROOM2));
    assert!(
        elapsed >= std::time::Duration::from_millis(250),
        "the final exhausted 429 hint must still pause startup: {elapsed:?}"
    );

    let requests = server.state.recorded();
    let room_a: Vec<_> = requests
        .iter()
        .filter(|request| request.path.contains("/rooms/!room:hs.example.org/state"))
        .collect();
    let room_b = requests
        .iter()
        .find(|request| request.path.contains("/rooms/!room2:hs.example.org/state"))
        .expect("the healthy room's metadata was fetched");
    assert_eq!(room_a.len(), 2, "retries stay bounded to the policy");
    let last_a = room_a.last().unwrap();
    assert!(
        room_b.at.duration_since(last_a.at) >= std::time::Duration::from_millis(250),
        "the next room must wait for the final hint"
    );
}

#[tokio::test]
async fn final_join_429_defers_same_room_metadata() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(
        &server,
        dir.path(),
        vec![room_with_via(ROOM, true, &["hs2.example.org"])],
    );
    // Every join attempt is rate limited; metadata would succeed if fetched.
    server
        .state
        .join_response
        .lock()
        .unwrap()
        .replace(MockResponse::status(
            429,
            json!({ "errcode": "M_LIMIT_EXCEEDED", "error": "slow", "retry_after_ms": 300 }),
        ));
    server
        .state
        .set_create_state(ROOM, MockResponse::json(json!({ "room_version": "11" })));
    let transport = adapter(&server, &cfg).await;
    let mut store = Store::open(&cfg.database, &identity(&cfg)).unwrap();
    let policy = StartupPolicy {
        max_attempts: 2,
        ..tiny_policy()
    };

    let started = std::time::Instant::now();
    let report = runtime::initialize_with_policy(&transport, &cfg, &mut store, &policy)
        .await
        .unwrap();
    assert!(
        started.elapsed() >= std::time::Duration::from_millis(250),
        "the final join hint must be honored"
    );
    assert!(report.joined.is_empty());
    assert!(report.unready.iter().any(|(room, _)| room == ROOM));
    assert!(store.room_version_of(ROOM).unwrap().is_none());

    let requests = server.state.recorded();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method == "POST" && request.path.contains("/join/"))
            .count(),
        2,
        "join retries stay bounded"
    );
    assert!(
        !requests
            .iter()
            .any(|request| request.path.contains("/state/m.room.create")),
        "a failed join must defer the room instead of fetching same-room metadata"
    );
}
