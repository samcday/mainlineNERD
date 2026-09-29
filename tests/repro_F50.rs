//! Repro F50: a transient alias-resolution failure ignores the known pin and
//! un-configures the room; an all-alias config un-configures every room before
//! returning EmptyRooms.
//!
//! Spec: docs/operations.md "Aliases ... are resolved once and pinned to their
//! canonical room id"; docs/architecture.md "Startup is isolated per room".

mod common;

use common::*;
use mainlinenerd_ingest::config::{Config, RoomConfig, RoomSelector, Token};
use mainlinenerd_ingest::matrix::MatrixTransport;
use mainlinenerd_ingest::runtime::{self, StartupPolicy};
use mainlinenerd_ingest::store::{ArchiveIdentity, Store};

fn tiny_policy() -> StartupPolicy {
    StartupPolicy {
        max_attempts: 4,
        transient_min: std::time::Duration::from_millis(1),
        transient_max: std::time::Duration::from_millis(2),
        rate_limit_fallback: std::time::Duration::from_millis(1),
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

async fn adapter(cfg: &Config) -> MatrixTransport {
    MatrixTransport::connect(cfg, &Token::new("fake-token"))
        .await
        .expect("connect adapter")
}

const A: &str = "#a:remote.example.org";
const B: &str = "#b:remote.example.org";

fn federation_down() -> MockResponse {
    MockResponse::matrix_error(502, "M_UNKNOWN", "remote.example.org unreachable")
}

/// All rooms configured by alias; the alias server is down at the next start.
#[tokio::test]
async fn f50_all_alias_outage_keeps_pinned_rooms_configured() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(&server, dir.path(), vec![alias(A), alias(B)]);
    server.state.set_alias(A, ROOM);
    server.state.set_alias(B, ROOM2);
    let mut store = Store::open(&cfg.database, &identity(&cfg)).unwrap();

    let transport = adapter(&cfg).await;
    let report = runtime::initialize_with_policy(&transport, &cfg, &mut store, &tiny_policy())
        .await
        .unwrap();
    assert_eq!(report.rooms.len(), 2, "{report:?}");
    assert_eq!(store.alias_pin(A).unwrap(), Some(ROOM.to_owned()));
    assert_eq!(store.alias_pin(B).unwrap(), Some(ROOM2.to_owned()));
    assert!(store.room_configured(ROOM).unwrap());
    assert!(store.room_configured(ROOM2).unwrap());

    // Restart while the alias' server is unreachable (Synapse answers 502).
    server.state.alias_failures.lock().unwrap().insert(A.to_owned(), federation_down());
    server.state.alias_failures.lock().unwrap().insert(B.to_owned(), federation_down());
    let transport = adapter(&cfg).await;
    let result =
        runtime::initialize_with_policy(&transport, &cfg, &mut store, &tiny_policy()).await;
    println!("F50 all-alias restart result: {result:?}");
    println!(
        "F50 after outage: configured(ROOM)={} configured(ROOM2)={} configured_room_ids={:?} pins=({:?},{:?})",
        store.room_configured(ROOM).unwrap(),
        store.room_configured(ROOM2).unwrap(),
        store.configured_room_ids().unwrap(),
        store.alias_pin(A).unwrap(),
        store.alias_pin(B).unwrap(),
    );
    println!("F50 status:\n{}", store.render_status().unwrap());

    // Correct behaviour: the pins are known, so the rooms stay configured.
    assert_eq!(
        store.configured_room_ids().unwrap(),
        vec![ROOM.to_owned(), ROOM2.to_owned()],
        "a transient alias outage must not un-configure pinned rooms"
    );
}

/// Only one alias fails: its pinned room must still be admitted for the run.
#[tokio::test]
async fn f50_partial_alias_outage_keeps_pinned_room_in_scope() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(&server, dir.path(), vec![alias(A), alias(B)]);
    server.state.set_alias(A, ROOM);
    server.state.set_alias(B, ROOM2);
    let mut store = Store::open(&cfg.database, &identity(&cfg)).unwrap();

    let transport = adapter(&cfg).await;
    runtime::initialize_with_policy(&transport, &cfg, &mut store, &tiny_policy())
        .await
        .unwrap();

    server.state.alias_failures.lock().unwrap().insert(A.to_owned(), federation_down());
    let transport = adapter(&cfg).await;
    let report = runtime::initialize_with_policy(&transport, &cfg, &mut store, &tiny_policy())
        .await
        .unwrap();
    println!("F50 partial restart report: {report:?}");
    println!(
        "F50 partial: configured(ROOM)={} configured(ROOM2)={} pin(A)={:?}",
        store.room_configured(ROOM).unwrap(),
        store.room_configured(ROOM2).unwrap(),
        store.alias_pin(A).unwrap(),
    );

    assert!(
        store.room_configured(ROOM).unwrap(),
        "pinned room behind a transiently failing alias must stay configured"
    );
    assert!(
        report.rooms.contains(&ROOM.to_owned()),
        "pinned room must be admitted this run: {report:?}"
    );
}
