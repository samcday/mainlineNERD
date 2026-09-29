//! F41 reproducer: Retry-After conversion wraps / maps a past date to 0.

mod common;

use std::sync::atomic::Ordering;
use std::time::Duration;

use common::*;
use mainlinenerd_ingest::config::{Config, RoomConfig, RoomSelector, Token};
use mainlinenerd_ingest::engine::{Engine, EngineConfig, SyncRequest, Transport, TransportError};
use mainlinenerd_ingest::matrix::MatrixTransport;
use mainlinenerd_ingest::runtime::{self, RunSettings};
use mainlinenerd_ingest::store::{ArchiveIdentity, Store};

fn config(server: &MockServer, dir: &std::path::Path) -> Config {
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
        rooms: vec![RoomConfig {
            selector: RoomSelector::Id(ROOM.to_owned()),
            join: false,
            via: Vec::new(),
        }],
    }
}

fn identity_for(cfg: &Config) -> ArchiveIdentity {
    ArchiveIdentity {
        homeserver: cfg.homeserver.clone(),
        user_id: cfg.user_id.clone(),
        device_id: cfg.device_id.clone(),
    }
}

/// Spec (architecture.md): a delay is never capped, shortened or skipped.
/// `Retry-After: 18446744073709552` (seconds) must not become a ~384 ms hint.
#[tokio::test]
async fn f41_huge_retry_after_header_is_not_wrapped() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(&server, dir.path());
    let transport = MatrixTransport::connect(&cfg, &Token::new("fake-token"))
        .await
        .expect("connect adapter");

    server.state.push_sync(
        MockResponse::matrix_error(429, "M_LIMIT_EXCEEDED", "slow down")
            .header("Retry-After", "18446744073709552"),
    );
    let result = transport
        .sync(SyncRequest {
            since: None,
            timeout_ms: 1_000,
        })
        .await;
    eprintln!("F41 huge header -> {result:?}");
    match result {
        Err(TransportError::RateLimited {
            retry_after_ms: Some(ms),
        }) => assert!(
            ms >= 1_000_000_000_000_000_000,
            "a ~584-million-year Retry-After was shortened to {ms} ms"
        ),
        other => panic!("expected a preserved rate-limit hint, got {other:?}"),
    }
}

/// Consequence of a past HTTP-date Retry-After mapping to `Some(0)`: /sync is
/// re-issued at round-trip speed with no floor. A server asking for less
/// traffic must not receive more than a couple of /sync requests per second.
#[tokio::test]
async fn f41_past_http_date_retry_after_does_not_hot_loop_sync() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(&server, dir.path());
    let transport = MatrixTransport::connect(&cfg, &Token::new("fake-token"))
        .await
        .expect("connect adapter");
    for _ in 0..500 {
        server.state.push_sync(
            MockResponse::matrix_error(429, "M_LIMIT_EXCEEDED", "slow down")
                .header("Retry-After", "Wed, 21 Oct 2015 07:28:00 GMT"),
        );
    }
    let store = Store::open(&cfg.database, &identity_for(&cfg)).unwrap();
    let engine = Engine::new(
        transport,
        store,
        EngineConfig {
            sync_timeout_ms: 1_000,
            history_limit: 10,
            max_pages_per_room: 4,
        },
    );
    let before = server.state.sync_seen.load(Ordering::SeqCst);
    let (tx, rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(runtime::run(engine, RunSettings::follow(), async move {
        let _ = rx.await;
    }));
    tokio::time::sleep(Duration::from_secs(1)).await;
    let _ = tx.send(());
    let summary = handle.await.unwrap();
    let syncs = server.state.sync_seen.load(Ordering::SeqCst) - before;
    eprintln!("F41 past-date: {syncs} /sync requests in 1s; summary={summary:?}");
    assert!(
        syncs <= 3,
        "a 429 with a past Retry-After date caused {syncs} /sync requests in one second"
    );
}
