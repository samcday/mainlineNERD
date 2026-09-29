//! F48 reproducer: a first run with a mistyped device_id binds the archive to
//! the unverified identity before /whoami rejects it, so the corrected config is
//! then refused forever even though the archive holds no cursors.

mod common;

use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

use common::*;
use rusqlite::{Connection, OpenFlags};

const BIN: &str = env!("CARGO_BIN_EXE_mln-ingest");
const TOKEN_ENV: &str = "MLN_F48_TEST_TOKEN";
const TOKEN_VALUE: &str = "f48-secret-token-value";

fn write_config(dir: &Path, server: &MockServer, device_id: &str) -> PathBuf {
    let path = dir.join("mln.toml");
    let data = dir.join("data");
    if !data.exists() {
        std::fs::create_dir(&data).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&data, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
    }
    let text = format!(
        r#"
homeserver = "{}"
user_id = "@ingest:hs.example.org"
device_id = "{device_id}"
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

async fn run_to_exit(config: &Path, mode: &str) -> Output {
    let mut child = Command::new(BIN)
        .arg("--config")
        .arg(config)
        .arg(mode)
        .env(TOKEN_ENV, TOKEN_VALUE)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn mln-ingest");
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if child.try_wait().unwrap().is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    if child.try_wait().unwrap().is_none() {
        let _ = child.kill();
    }
    child.wait_with_output().unwrap()
}

fn count(archive: &Path, sql: &str) -> Option<i64> {
    let conn = Connection::open_with_flags(archive, OpenFlags::SQLITE_OPEN_READ_ONLY).ok()?;
    conn.query_row(sql, [], |row| row.get(0)).ok()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f48_first_run_device_typo_must_not_brick_the_archive_for_the_corrected_config() {
    let server = MockServer::start().await;
    // The token really belongs to device "MLN" (mock whoami default).
    let dir = tempfile::tempdir().unwrap();
    let archive = dir.path().join("data").join("archive.sqlite3");

    // Run 1: operator typo in device_id.
    let config = write_config(dir.path(), &server, "MLN-INGEST");
    let first = run_to_exit(&config, "follow").await;
    let first_err = String::from_utf8_lossy(&first.stderr).into_owned();
    eprintln!("RUN1 status={:?}\nRUN1 stderr:\n{first_err}", first.status);
    assert!(!first.status.success(), "whoami must refuse the typo'd device");
    let syncs_after_first = server
        .state
        .recorded()
        .iter()
        .filter(|r| r.path.ends_with("/sync"))
        .count();
    assert_eq!(syncs_after_first, 0, "run 1 must stop before any sync");
    eprintln!(
        "after run1: archive exists={} archive_meta rows={:?} device_id={:?} sync_progress rows={:?} events={:?}",
        archive.exists(),
        count(&archive, "SELECT COUNT(*) FROM archive_meta"),
        Connection::open_with_flags(&archive, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .ok()
            .and_then(|c| c
                .query_row("SELECT device_id FROM archive_meta", [], |r| r
                    .get::<_, String>(0))
                .ok()),
        count(&archive, "SELECT COUNT(*) FROM sync_progress"),
        count(&archive, "SELECT COUNT(*) FROM events"),
    );

    // Run 2: operator fixes the config. A 401 on the first sync stops it once
    // it has got past identity validation and the archive binding.
    let config = write_config(dir.path(), &server, "MLN");
    server.state.push_sync(MockResponse::matrix_error(
        401,
        "M_UNKNOWN_TOKEN",
        "token rejected",
    ));
    let second = run_to_exit(&config, "follow").await;
    let second_err = String::from_utf8_lossy(&second.stderr).into_owned();
    eprintln!("RUN2 status={:?}\nRUN2 stderr:\n{second_err}", second.status);

    assert!(
        !second_err.contains("refusing to reuse its cursors"),
        "the corrected config must not be refused by a binding written from the \
         unverified first-run identity: {second_err}"
    );
    let syncs_after_second = server
        .state
        .recorded()
        .iter()
        .filter(|r| r.path.ends_with("/sync"))
        .count();
    assert!(
        syncs_after_second > 0,
        "the corrected run must reach live sync: {second_err}"
    );
}
