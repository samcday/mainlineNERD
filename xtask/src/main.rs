//! Real-Synapse startup smoke test. No shell script or mocked Matrix server.
use std::{
    env, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    thread::sleep,
    time::{Duration, Instant},
};

use anyhow::{ensure, Context, Result};
use reqwest::blocking::Client;
use rusqlite::{Connection, OpenFlags};
use serde_json::{json, Value};
use tempfile::TempDir;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use uuid::Uuid;

const SYNAPSE_IMAGE: &str = "ghcr.io/element-hq/synapse:v1.162.0";
const USER: &str = "@smoke:smoke.localhost";
const DEVICE: &str = "MAINLINENERDSMOKE";

fn main() -> Result<()> {
    ensure!(
        env::args().skip(1).collect::<Vec<_>>() == ["smoke"],
        "usage: cargo xtask smoke"
    );
    smoke()
}

/// Owns just this invocation's container and private directory.
struct Fixture {
    runtime: String,
    name: String,
    data: Option<TempDir>,
    success: bool,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = Command::new(&self.runtime)
            .args(["rm", "-f", &self.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        if !self.success {
            if let Some(data) = self.data.take() {
                eprintln!("kept run state in {}", data.keep().display());
            }
        }
    }
}

impl Fixture {
    fn sync_lines(&self) -> Result<Vec<String>> {
        let logs = output(Command::new(&self.runtime).args(["logs", &self.name]))?;
        Ok(String::from_utf8_lossy(&logs.stdout)
            .lines()
            .chain(String::from_utf8_lossy(&logs.stderr).lines())
            .filter(|line| line.contains("/_matrix/client/v3/sync?"))
            .map(str::to_owned)
            .collect())
    }
}

/// Capture command output without putting arguments (which may include a
/// throwaway password) into diagnostics.
fn output(command: &mut Command) -> Result<Output> {
    let result = command.output().context("starting fixture command")?;
    ensure!(
        result.status.success(),
        "fixture command failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    Ok(result)
}

fn stdout(command: &mut Command) -> Result<String> {
    Ok(String::from_utf8(output(command)?.stdout)?
        .trim()
        .to_owned())
}

fn binary(bin: &Path, hs: &str, token: &str, user: &str, device: &str, db: &Path) -> Command {
    let mut command = Command::new(bin);
    command
        .env("MATRIX_ACCESS_TOKEN", token)
        .args([
            "--homeserver",
            hs,
            "--user",
            user,
            "--device",
            device,
            "--db",
        ])
        .arg(db);
    command
}

fn smoke() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let target = root.join("target");
    let cargo = env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    ensure!(
        Command::new(cargo)
            .current_dir(root)
            .args([
                "build",
                "--locked",
                "--package",
                "mainlinenerd",
                "--target-dir"
            ])
            .arg(&target)
            .status()?
            .success(),
        "building the startup binary failed"
    );
    let bin = env::var_os("BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| target.join("debug/mainlinenerd"));
    let runtime = env::var("CONTAINER_RUNTIME").unwrap_or_else(|_| {
        if Command::new("docker").arg("--version").output().is_ok() {
            "docker".into()
        } else {
            "podman".into()
        }
    });
    let data = tempfile::Builder::new()
        .prefix("smoke-")
        .tempdir_in(&target)?;
    let path = data.path().to_owned();
    let mut fixture = Fixture {
        runtime,
        name: format!(
            "mainlinenerd-{}",
            path.file_name().unwrap().to_string_lossy()
        ),
        data: Some(data),
        success: false,
    };
    let image = env::var("SYNAPSE_IMAGE").unwrap_or_else(|_| SYNAPSE_IMAGE.into());
    let ready_timeout = env::var("SMOKE_READY_TIMEOUT")
        .unwrap_or_else(|_| "90".into())
        .parse::<u64>()
        .context("SMOKE_READY_TIMEOUT must be seconds")?;
    let synapse = path.join("synapse");
    fs::create_dir(&synapse)?;
    let volume = format!("{}:/data:Z", synapse.display());
    let owner = format!(
        "{}:{}",
        stdout(Command::new("id").arg("-u"))?,
        stdout(Command::new("id").arg("-g"))?
    );
    let mut user_args = vec!["--user".to_owned(), owner];
    if Path::new(&fixture.runtime)
        .file_name()
        .is_some_and(|name| name == "podman")
    {
        user_args.push("--userns=keep-id".into());
    }
    println!("Generating fresh Synapse config ({image})");
    output(
        Command::new(&fixture.runtime)
            .args(["run", "--rm", "-e", "SYNAPSE_SERVER_NAME=smoke.localhost"])
            .args(["-e", "SYNAPSE_REPORT_STATS=no", "-v", &volume])
            .args(&user_args)
            .args([&image, "generate"]),
    )?;
    let port = env::var("SMOKE_PORT").unwrap_or_default();
    output(
        Command::new(&fixture.runtime)
            .args(["run", "-d", "--name", &fixture.name, "-p"])
            .arg(format!("127.0.0.1:{port}:8008"))
            .args(["-v", &volume])
            .args(&user_args)
            .arg(&image),
    )?;
    let address: SocketAddr =
        stdout(Command::new(&fixture.runtime).args(["port", &fixture.name, "8008/tcp"]))?
            .parse()?;
    let hs = format!("http://{address}");
    println!("Starting {} on {hs}", fixture.name);
    let http = Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(5))
        .build()?;
    let start = Instant::now();
    loop {
        if http
            .get(format!("{hs}/health"))
            .send()
            .is_ok_and(|r| r.status().is_success())
        {
            break;
        }
        ensure!(
            start.elapsed() < Duration::from_secs(ready_timeout),
            "Synapse did not become healthy within {ready_timeout}s"
        );
        sleep(Duration::from_secs(1));
    }
    let password = Uuid::new_v4().simple().to_string();
    output(
        Command::new(&fixture.runtime)
            .args(["exec", &fixture.name, "register_new_matrix_user"])
            .args([
                "-c",
                "/data/homeserver.yaml",
                "-u",
                "smoke",
                "-p",
                &password,
            ])
            .args(["--no-admin", "http://localhost:8008"]),
    )?;
    let login: Value = http
        .post(format!("{hs}/_matrix/client/v3/login"))
        .json(&json!({
            "type": "m.login.password",
            "identifier": {"type": "m.id.user", "user": "smoke"},
            "password": password,
            "device_id": DEVICE
        }))
        .send()?
        .error_for_status()?
        .json()?;
    let token = login["access_token"]
        .as_str()
        .context("login returned no token")?;
    ensure!(
        login["device_id"] == DEVICE,
        "login returned a different device"
    );
    let before = fixture.sync_lines()?.len();
    let db = path.join("startup.db");
    let mut happy = binary(&bin, &hs, token, USER, DEVICE, &db);
    for key in [
        "ALL_PROXY",
        "all_proxy",
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
    ] {
        happy.env(key, "http://127.0.0.1:1");
    }
    happy.env("NO_PROXY", "").env("no_proxy", "");
    let receipt = stdout(&mut happy)?;
    println!("{receipt}");
    ensure!(
        receipt.contains(&format!(
            "startup-check ok homeserver={hs}/ user={USER} device={DEVICE} "
        )),
        "missing successful receipt"
    );
    let connection = Connection::open_with_flags(&db, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let (server, user, timestamp): (String, String, String) = connection.query_row(
        "SELECT homeserver, user_id, last_successful_startup FROM startup_state WHERE id = 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    ensure!(
        server == format!("{hs}/") && user == USER,
        "wrong startup identity"
    );
    ensure!(
        OffsetDateTime::parse(&timestamp, &Rfc3339)?
            .offset()
            .is_utc(),
        "startup time is not UTC"
    );
    let tables: i64 = connection.query_row(
        "SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name != 'startup_state'",
        [],
        |row| row.get(0),
    )?;
    ensure!(tables == 0, "startup created unexpected tables");
    drop(connection);

    let start = Instant::now();
    let syncs = loop {
        let lines = fixture.sync_lines()?;
        if lines.len() > before || start.elapsed() >= Duration::from_secs(15) {
            break lines;
        }
        sleep(Duration::from_secs(1));
    };
    ensure!(
        syncs.len() == before + 1,
        "expected exactly one new /sync, saw {}",
        syncs.len().saturating_sub(before)
    );
    let sync = &syncs[before];
    ensure!(
        sync.contains(" 200 \"GET /_matrix/client/v3/sync?"),
        "initial sync did not return 200"
    );
    ensure!(
        sync.contains("set_presence=offline") && !sync.contains("since="),
        "sync was not offline and initial"
    );
    println!("Verified SQLite receipt and one successful offline initial sync in Synapse's log");

    for (name, token, user, device, reason) in [
        (
            "bad-token",
            "deliberately-invalid-token",
            USER,
            DEVICE,
            "verifying the session",
        ),
        (
            "wrong-user",
            token,
            "@nobody:smoke.localhost",
            DEVICE,
            "identity mismatch",
        ),
        (
            "wrong-device",
            token,
            USER,
            "WRONGDEVICE",
            "device mismatch",
        ),
    ] {
        let db = path.join(format!("{name}.db"));
        let result = binary(&bin, &hs, token, user, device, &db).output()?;
        ensure!(!result.status.success(), "{name} was accepted");
        ensure!(!db.exists(), "{name} created a database");
        ensure!(
            String::from_utf8_lossy(&result.stderr).contains(reason),
            "{name} failed for an unexpected reason"
        );
        println!("Rejected {name} without writing a receipt");
    }
    ensure!(
        fixture.sync_lines()?.len() == before + 1,
        "a rejected run made a sync request"
    );
    fixture.success = true;
    println!("ok: {image} passed the startup-check smoke test");
    Ok(())
}
