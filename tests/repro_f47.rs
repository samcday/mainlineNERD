//! F47 reproducer: alias drift on one room aborts startup for every room.
//!
//! docs/architecture.md: "Startup is isolated per room: an alias, join or
//! metadata failure marks only that room unready (with a bounded status
//! reason) and the others proceed". Alias drift (e.g. after a room upgrade
//! moved the alias to the successor) must be refused for that alias only.

mod common;

use common::*;
use mainlinenerd_ingest::config::{Config, RoomConfig, RoomSelector, Token};
use mainlinenerd_ingest::matrix::MatrixTransport;
use mainlinenerd_ingest::runtime;
use mainlinenerd_ingest::store::{ArchiveIdentity, Store};

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

async fn adapter(cfg: &Config) -> MatrixTransport {
    MatrixTransport::connect(cfg, &Token::new("fake-token"))
        .await
        .expect("connect adapter")
}

#[tokio::test]
async fn repro_f47_alias_drift_is_isolated_to_that_room() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(
        &server,
        dir.path(),
        vec![alias("#eng:hs.example.org"), room_id(ROOM2)],
    );
    server.state.set_alias("#eng:hs.example.org", ROOM);
    let transport = adapter(&cfg).await;
    let mut store = Store::open(&cfg.database, &identity(&cfg)).unwrap();

    let first = runtime::initialize(&transport, &cfg, &mut store)
        .await
        .unwrap();
    eprintln!("F47 first start report: {first:?}");
    assert_eq!(first.rooms.len(), 2, "both rooms ready on first start");
    assert_eq!(
        store.alias_pin("#eng:hs.example.org").unwrap(),
        Some(ROOM.to_owned())
    );

    // The room is upgraded and the server moves the alias to the successor.
    server.state.set_alias("#eng:hs.example.org", ROOM3);
    let transport2 = adapter(&cfg).await;
    let second = match runtime::initialize(&transport2, &cfg, &mut store).await {
        Ok(report) => report,
        Err(error) => panic!(
            "F47: alias drift on #eng aborted startup for EVERY room \
             (ROOM2 is unrelated and should have proceeded): {error:?}"
        ),
    };
    eprintln!("F47 second start report: {second:?}");
    assert!(
        second.rooms.contains(&ROOM2.to_owned()),
        "unrelated room must proceed: {second:?}"
    );
    assert!(
        !second.rooms.contains(&ROOM3.to_owned()),
        "drift must not widen the allowlist"
    );
    assert!(
        second
            .unready
            .iter()
            .any(|(sel, _)| sel == "#eng:hs.example.org"),
        "the drifted alias is reported unready: {second:?}"
    );
}
