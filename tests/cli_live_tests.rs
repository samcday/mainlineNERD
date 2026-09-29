//! Real-binary CLI test for `follow` vs `run` against the loopback mock.
//!
//! A temporary config and archive and a fake token env var are used. The child
//! is left running until the parent has observed the expected committed state
//! in SQLite, then a controlled authentication failure stops it. The archive is
//! inspected directly through a read-only connection and no production-only
//! flag exists for tests.

mod common;

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use common::*;
use rusqlite::{Connection, OpenFlags};
use serde_json::json;

const BIN: &str = env!("CARGO_BIN_EXE_mln-ingest");
const TOKEN_ENV: &str = "MLN_CLI_TEST_TOKEN";
const TOKEN_VALUE: &str = "cli-secret-token-value";

fn write_config(dir: &Path, server: &MockServer) -> PathBuf {
    let path = dir.join("mln.toml");
    // tempfile's directory mode depends on the umask here, so use an explicit
    // owner-only data directory as the runtime requires.
    let data = dir.join("data");
    std::fs::create_dir(&data).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&data, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let text = format!(
        r#"
homeserver = "{}"
user_id = "@ingest:hs.example.org"
device_id = "MLN"
token_env = "{TOKEN_ENV}"
data_dir = "{}"
database = "archive.sqlite3"
history_interval_ms = 1
sync_timeout_ms = 1000

[[rooms]]
id = "!room:hs.example.org"
"#,
        server.base_url,
        data.display()
    );
    std::fs::write(&path, text).unwrap();
    path
}

fn room_batch() -> serde_json::Value {
    json!({
        "next_batch": "s1",
        "rooms": { "join": {
            "!room:hs.example.org": {
                "timeline": { "events": [{
                    "type": "m.room.message", "event_id": "$live",
                    "sender": "@alice:hs.example.org", "origin_server_ts": 100,
                    "content": { "msgtype": "m.text", "body": "live" }
                }], "prev_batch": "p1", "limited": false },
                "state": { "events": [{
                    "type": "m.room.create", "event_id": "$create",
                    "sender": "@alice:hs.example.org", "state_key": "",
                    "origin_server_ts": 1, "content": { "room_version": "11" }
                }] }
            }
        }}
    })
}

/// Owns the child and guarantees it is killed and reaped on drop, including on
/// panic or task cancellation.
struct ChildGuard {
    child: Option<Child>,
}

impl ChildGuard {
    fn spawn(config: &Path, mode: &str) -> Self {
        let child = Command::new(BIN)
            .arg("--config")
            .arg(config)
            .arg(mode)
            .env(TOKEN_ENV, TOKEN_VALUE)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn mln-ingest");
        Self { child: Some(child) }
    }

    fn exited(&mut self) -> bool {
        self.child
            .as_mut()
            .expect("child present")
            .try_wait()
            .unwrap()
            .is_some()
    }

    fn take_output(&mut self) -> Output {
        let child = self.child.take().expect("child present");
        child.wait_with_output().expect("collect child output")
    }

    fn kill(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn probe_i64(archive: &Path, sql: &str) -> Option<i64> {
    let conn = Connection::open_with_flags(
        archive,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .ok()?;
    conn.query_row(sql, [], |row| row.get(0)).ok()
}

fn probe_string(archive: &Path, sql: &str) -> Option<String> {
    let conn = Connection::open_with_flags(
        archive,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .ok()?;
    conn.query_row(sql, [], |row| row.get(0)).ok().flatten()
}

fn event_count(archive: &Path, event_id: &str) -> Option<i64> {
    probe_i64(
        archive,
        &format!("SELECT COUNT(*) FROM events WHERE event_id = '{event_id}'"),
    )
}

/// Wait until `check` holds, the child exits, or the deadline passes.
async fn wait_for(
    archive: &Path,
    guard: &mut ChildGuard,
    mut check: impl FnMut(&Path) -> bool,
) -> bool {
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        if check(archive) {
            return true;
        }
        if guard.exited() {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

/// Stop the child with a controlled 401, waiting for exit while the guard still
/// owns it, then collect its output.
async fn stop_with_auth_failure(server: &MockServer, guard: &mut ChildGuard) -> Output {
    server.state.push_sync(MockResponse::matrix_error(
        401,
        "M_UNKNOWN_TOKEN",
        "token rejected",
    ));
    server.state.hold_sync(false);
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline && !guard.exited() {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(guard.exited(), "the controlled 401 must stop the child");
    guard.take_output()
}

/// Release the parked child with a 401 and collect its diagnostics, used only
/// to explain a failed precondition. Bounded and safe on a stuck child.
async fn stop_and_collect(server: &MockServer, guard: &mut ChildGuard) -> String {
    server.state.push_sync(MockResponse::matrix_error(
        401,
        "M_UNKNOWN_TOKEN",
        "token rejected",
    ));
    server.state.hold_sync(false);
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline && !guard.exited() {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    if !guard.exited() {
        guard.kill();
    }
    let output = guard.take_output();
    format!(
        "status={:?} stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    )
}

fn assert_clean(output: &Output, requests: &[MockRequest], expect_history: bool) {
    assert!(
        !output.status.success(),
        "the controlled 401 must stop the run"
    );
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(
        !stderr.contains(TOKEN_VALUE),
        "the access token must never reach stderr: {stderr}"
    );
    assert!(
        requests
            .iter()
            .any(|request| request.path.ends_with("/sync")),
        "live sync must run"
    );
    assert_eq!(
        requests
            .iter()
            .any(|request| request.path.contains("/messages")),
        expect_history,
        "history pagination must match the mode"
    );
    for request in requests {
        for forbidden in [
            "/send/",
            "/receipt/",
            "/presence/",
            "/keys/",
            "/typing",
            "/profile",
        ] {
            assert!(
                !request.path.contains(forbidden),
                "the live client must not call {forbidden}"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follow_stops_after_live_checkpoint_and_writes_no_history() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let config = write_config(dir.path(), &server);
    let archive = dir.path().join("data").join("archive.sqlite3");
    server.state.push_sync(MockResponse::json(room_batch()));
    server.state.hold_sync(true);

    let mut guard = ChildGuard::spawn(&config, "follow");
    let committed = wait_for(&archive, &mut guard, |archive| {
        event_count(archive, "$live") == Some(1)
            && probe_string(
                archive,
                "SELECT since_token FROM sync_progress WHERE id = 1",
            )
            .as_deref()
                == Some("s1")
    })
    .await;
    if !committed {
        panic!(
            "the live batch must commit before the run is stopped: {}",
            stop_and_collect(&server, &mut guard).await
        );
    }
    assert_eq!(event_count(&archive, "$hist"), Some(0));

    let output = stop_with_auth_failure(&server, &mut guard).await;
    assert_clean(&output, &server.state.recorded(), false);
    // The speculative 401 request committed nothing.
    assert_eq!(
        probe_string(
            &archive,
            "SELECT since_token FROM sync_progress WHERE id = 1"
        )
        .as_deref(),
        Some("s1")
    );
    assert_eq!(event_count(&archive, "$hist"), Some(0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_backfills_history_before_the_stop() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let config = write_config(dir.path(), &server);
    let archive = dir.path().join("data").join("archive.sqlite3");
    server.state.push_sync(MockResponse::json(room_batch()));
    server.state.hold_sync(true);
    server.state.push_messages(
        "p1",
        MockResponse::json(json!({ "start": "p1", "end": null, "chunk": [{
            "type": "m.room.message", "event_id": "$hist",
            "sender": "@alice:hs.example.org", "origin_server_ts": 50,
            "content": { "msgtype": "m.text", "body": "history" }
        }] })),
    );

    let mut guard = ChildGuard::spawn(&config, "run");
    let committed = wait_for(&archive, &mut guard, |archive| {
        event_count(archive, "$live") == Some(1) && event_count(archive, "$hist") == Some(1)
    })
    .await;
    if !committed {
        panic!(
            "run must backfill history before the run is stopped: {}",
            stop_and_collect(&server, &mut guard).await
        );
    }
    assert_eq!(
        probe_i64(
            &archive,
            "SELECT complete FROM room_history WHERE room_id = '!room:hs.example.org'"
        ),
        Some(1),
        "the base backfill completed"
    );
    assert_eq!(
        probe_string(
            &archive,
            "SELECT since_token FROM sync_progress WHERE id = 1"
        )
        .as_deref(),
        Some("s1"),
        "the sync checkpoint is the scripted token"
    );

    let output = stop_with_auth_failure(&server, &mut guard).await;
    assert_clean(&output, &server.state.recorded(), true);
}
