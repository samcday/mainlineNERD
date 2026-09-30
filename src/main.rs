//! mainlineNERD: one-shot homeserver startup check.
//!
//! Deliberately not a collector yet. One run: restore the supplied session,
//! verify it with `/whoami`, perform a single initial `/sync`, then record only
//! that startup succeeded. E2EE is compiled out (see Cargo.toml) and presence is
//! Offline: this binary does not decrypt or archive messages, or advertise
//! online presence.
//!
//! "One-shot" is not "time-bounded": the sync below only caps the server-side
//! long-poll, while matrix-sdk's default retry policy still governs total
//! runtime. Wall-clock limits belong to the smoke fixture and the CI job.

use std::{path::Path, path::PathBuf, time::Duration};

use anyhow::{bail, ensure, Context, Result};
use clap::Parser;
use matrix_sdk::{
    authentication::matrix::MatrixSession,
    config::{SyncSettings, SyncToken},
    reqwest::{self, tls::Version, Url},
    ruma::{presence::PresenceState, OwnedDeviceId, UserId},
    Client, SessionMeta, SessionTokens,
};
use rusqlite::{params, Connection};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use url::Host;

/// Access tokens are read from the environment only: never from argv (visible in
/// the process table) and never written to logs or disk.
const TOKEN_ENV: &str = "MATRIX_ACCESS_TOKEN";

/// Caps the server-side long-poll of the one `/sync` request. It does not bound
/// the process: matrix-sdk's default retries may outlast it.
const SYNC_TIMEOUT: Duration = Duration::from_secs(10);

/// Total per-request HTTP timeout, matching matrix-sdk's own default settings.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Parser)]
#[command(
    name = "mainlinenerd",
    version,
    about = "One-shot Matrix homeserver startup check (not a collector yet)"
)]
struct Args {
    /// Homeserver base URL, e.g. http://127.0.0.1:8008
    #[arg(long)]
    homeserver: String,

    /// Full user ID to verify, e.g. @smoke:smoke.localhost
    #[arg(long)]
    user: String,

    /// Device ID the access token was issued for
    #[arg(long)]
    device: String,

    /// SQLite file for startup bookkeeping; written only after a verified sync
    #[arg(long)]
    db: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let token = std::env::var(TOKEN_ENV).with_context(|| {
        format!("{TOKEN_ENV} must be set; tokens are never passed as arguments")
    })?;
    let user_id = UserId::parse(args.user.as_str())
        .with_context(|| format!("not a valid user ID: {}", args.user))?;
    let device_id = OwnedDeviceId::from(args.device.as_str());
    let homeserver = validate_homeserver(&args.homeserver)?;

    // The SDK's own client, keeping its retry/backoff request loop rather than
    // any custom retry framework. The single HTTP client is built here with
    // `no_proxy` so ambient HTTP_PROXY/ALL_PROXY cannot divert homeserver
    // traffic (and the access token); its timeout and TLS floor match the SDK's
    // defaults, and E2EE stays compiled out.
    let http = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .user_agent(concat!("mainlinenerd/", env!("CARGO_PKG_VERSION")))
        .min_tls_version(Version::TLS_1_2)
        .timeout(REQUEST_TIMEOUT)
        .build()
        .context("building the HTTP client")?;
    let client = Client::builder()
        .homeserver_url(homeserver.as_str())
        .http_client(http)
        .build()
        .await
        .context("building the Matrix client")?;

    let session = MatrixSession {
        meta: SessionMeta {
            user_id: user_id.clone(),
            device_id: device_id.clone(),
        },
        tokens: SessionTokens {
            access_token: token,
            refresh_token: None,
        },
    };
    client
        .restore_session(session)
        .await
        .context("restoring the supplied session")?;

    // 1. The homeserver must confirm the token, user and device before we trust
    //    the session or write anything down.
    let whoami = client
        .whoami()
        .await
        .context("verifying the session with /whoami")?;
    ensure!(
        whoami.user_id == user_id,
        "identity mismatch: /whoami reports {} but {} was supplied",
        whoami.user_id,
        user_id
    );
    ensure!(
        whoami.device_id.as_ref() == Some(&device_id),
        "device mismatch: /whoami reports {:?} but {} was supplied",
        whoami.device_id,
        device_id
    );

    // 2. Exactly one initial sync, then exit: no sync loop, no scheduler.
    //    `NoToken` makes it an initial sync rather than a resume; Offline
    //    presence keeps the check from advertising the user as available.
    client
        .sync_once(
            SyncSettings::new()
                .token(SyncToken::NoToken)
                .timeout(SYNC_TIMEOUT)
                .set_presence(PresenceState::Offline),
        )
        .await
        .context("initial sync failed")?;

    // 3. Only now may startup leave a record behind.
    record_startup(&args.db, homeserver.as_str(), &user_id)
        .with_context(|| format!("writing startup bookkeeping to {}", args.db.display()))?;

    println!(
        "startup-check ok homeserver={} user={} device={} db={}",
        homeserver.as_str(),
        user_id,
        device_id,
        args.db.display()
    );
    Ok(())
}

/// Records the verified homeserver/user and when startup last succeeded.
///
/// No sync cursor or events are persisted: a `since` token is meaningless until
/// there is an ingestion pipeline to resume (out of scope for this change).
fn record_startup(db: &Path, homeserver: &str, user_id: &UserId) -> Result<()> {
    if let Some(dir) = db.parent().filter(|dir| !dir.as_os_str().is_empty()) {
        std::fs::create_dir_all(dir)?;
    }
    let connection = Connection::open(db)?;
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS startup_state (
             id INTEGER PRIMARY KEY CHECK (id = 1),
             homeserver TEXT NOT NULL,
             user_id TEXT NOT NULL,
             last_successful_startup TEXT NOT NULL
         );",
    )?;
    connection.execute(
        "INSERT INTO startup_state (id, homeserver, user_id, last_successful_startup)
         VALUES (1, ?1, ?2, ?3)
         ON CONFLICT (id) DO UPDATE SET
             homeserver = excluded.homeserver,
             user_id = excluded.user_id,
             last_successful_startup = excluded.last_successful_startup",
        params![
            homeserver,
            user_id.as_str(),
            OffsetDateTime::now_utc().format(&Rfc3339)?
        ],
    )?;
    Ok(())
}

/// Parses the operator-supplied homeserver URL and enforces the transport
/// policy: HTTPS for any host, plain HTTP only for loopback (`localhost` or a
/// loopback IPv4/IPv6 address), never a hostless URL and never credentials
/// embedded in the URL.
fn validate_homeserver(raw: &str) -> Result<Url> {
    let url = Url::parse(raw).context("homeserver must be an absolute http(s) URL")?;
    ensure!(
        url.username().is_empty() && url.password().is_none(),
        "homeserver URL must not embed credentials"
    );
    let loopback = match url.host() {
        // Hostless http(s) URLs do not parse; keep this arm conservative.
        None => false,
        Some(Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
        Some(Host::Ipv4(ip)) => ip.is_loopback(),
        Some(Host::Ipv6(ip)) => ip.is_loopback(),
    };
    match url.scheme() {
        "https" => Ok(url),
        "http" if loopback => Ok(url),
        "http" => bail!(
            "plain http is only allowed for loopback homeservers; the access token would be sent in cleartext"
        ),
        scheme => bail!("homeserver URL must use https or loopback http, not {scheme:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::validate_homeserver;

    /// Transport policy table: HTTPS anywhere, HTTP only on loopback, hostless
    /// URLs and embedded credentials rejected. No network access involved.
    #[test]
    fn homeserver_urls_are_https_or_loopback_http() {
        for (url, ok) in [
            ("https://matrix.example.org", true),
            ("https://127.0.0.1:8448", true),
            ("http://localhost:8008", true),
            ("http://127.0.0.1:8008", true),
            ("http://127.99.1.2:8008", true),
            ("http://[::1]:8008", true),
            ("http://matrix.example.org", false),
            ("http://[2001:db8::1]:8008", false),
            ("https://", false),
            ("http://user:pass@localhost:8008", false),
            ("ftp://localhost:8008", false),
        ] {
            assert_eq!(validate_homeserver(url).is_ok(), ok, "verdict for {url}");
        }
    }
}
