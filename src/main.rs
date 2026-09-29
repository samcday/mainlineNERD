//! `mln-ingest`: live Matrix ingestion plus offline status and export.
//!
//! `follow` runs live sync only; `run` also steadily backfills accessible
//! history for the configured room allowlist. `status` and `export` operate
//! purely on the durable archive and never touch the network.

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};
use mainlinenerd_ingest::config::Config;
use mainlinenerd_ingest::engine::{Engine, EngineConfig};
use mainlinenerd_ingest::matrix::MatrixTransport;
use mainlinenerd_ingest::runtime::{self, RunSettings};
use mainlinenerd_ingest::store::{ArchiveIdentity, Store};

#[derive(Parser)]
#[command(
    name = "mln-ingest",
    about = "mainlineNERD passive Matrix ingestion (durable store; live sync and history)"
)]
struct Cli {
    /// Path to the SQLite archive, for status/export
    #[arg(long, env = "MLN_DB", global = true)]
    db: Option<PathBuf>,

    /// Path to the TOML config, for follow/run
    #[arg(long, env = "MLN_CONFIG", global = true)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Show archive status. Tokens and message bodies are never printed.
    Status {
        /// Emit the machine-readable JSON report
        #[arg(long)]
        json: bool,
    },
    /// Export stored data as JSONL
    Export {
        #[arg(long, value_enum, default_value_t = ExportKind::Messages)]
        kind: ExportKind,
        /// Restrict the export to one room id
        #[arg(long)]
        room: Option<String>,
        /// Write to a file instead of stdout
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Follow live sync for the configured rooms only
    Follow,
    /// Follow live sync plus paced history backfill for the configured rooms
    Run {
        /// Explicit one-shot recovery: resume stalled base and bounded gap work
        /// for currently configured, still-eligible rooms at saved cursors.
        /// It never joins, clears membership/policy flags or rewrites cursors.
        #[arg(long)]
        retry_stalled: bool,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ExportKind {
    /// Current text projection, one row per message
    Messages,
    /// Raw stored events
    Events,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match execute(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error:#}");
            ExitCode::FAILURE
        }
    }
}

async fn execute(cli: Cli) -> anyhow::Result<()> {
    match cli.command {
        Command::Status { json } => {
            let db = cli
                .db
                .clone()
                .unwrap_or_else(|| PathBuf::from("mainlinenerd.sqlite3"));
            let store = Store::open_read_only(&db)
                .with_context(|| format!("opening archive {}", db.display()))?;
            if json {
                println!("{}", serde_json::to_string_pretty(&store.status()?)?);
            } else {
                print!("{}", store.render_status()?);
            }
        }
        Command::Export { kind, room, out } => {
            let db = cli
                .db
                .clone()
                .unwrap_or_else(|| PathBuf::from("mainlinenerd.sqlite3"));
            let store = Store::open_read_only(&db)
                .with_context(|| format!("opening archive {}", db.display()))?;
            let mut writer: Box<dyn Write> = match &out {
                Some(path) => Box::new(BufWriter::new(create_export_file(path)?)),
                None => Box::new(BufWriter::new(io::stdout().lock())),
            };
            let count = match kind {
                ExportKind::Messages => store.export_messages(room.as_deref(), &mut writer)?,
                ExportKind::Events => store.export_events(room.as_deref(), &mut writer)?,
            };
            writer.flush()?;
            eprintln!("exported {count} records");
        }
        Command::Follow => live(cli.config, false, false).await?,
        Command::Run { retry_stalled } => live(cli.config, true, retry_stalled).await?,
    }
    Ok(())
}

async fn live(
    config_path: Option<PathBuf>,
    history: bool,
    retry_stalled: bool,
) -> anyhow::Result<()> {
    let config_path = config_path.context(
        "follow/run require --config (or MLN_CONFIG); see config.example.toml in the repository",
    )?;
    let config = Config::load(&config_path)
        .with_context(|| format!("loading config {}", config_path.display()))?;
    config
        .ensure_storage()
        .context("checking private storage")?;
    let token = config.obtain_token()?;

    let adapter = MatrixTransport::connect(&config, &token)
        .await
        .context("connecting to the homeserver")?;

    let identity = ArchiveIdentity {
        homeserver: config.homeserver.clone(),
        user_id: config.user_id.clone(),
        device_id: config.device_id.clone(),
    };
    let mut store = Store::open(&config.database, &identity)
        .with_context(|| format!("opening archive {}", config.database.display()))?;

    let report = runtime::initialize(&adapter, &config, &mut store)
        .await
        .context("validating identity and the configured rooms")?;
    eprintln!(
        "rooms: {} ready, {} joined, {} versions learned, {} unready",
        report.rooms.len(),
        report.joined.len(),
        report.versions_learned.len(),
        report.unready.len()
    );
    for (room, reason) in &report.unready {
        eprintln!("unready room {room}: {reason}");
    }

    if retry_stalled {
        // Explicit one-shot recovery after access was restored outside this
        // tool. Only currently configured, still-eligible rooms are re-enabled,
        // at their saved cursors; membership and policy flags are untouched.
        let retried = store
            .retry_stalled_configured(mainlinenerd_ingest::store::now_unix_ms())
            .context("re-enabling stalled history work")?;
        eprintln!(
            "retry-stalled: {} work item(s) resumed ({} base, {} bounded gap(s))",
            retried.total(),
            retried.base,
            retried.gaps
        );
    }

    let engine_config = EngineConfig {
        sync_timeout_ms: config.sync_timeout_ms,
        history_limit: config.history_limit,
        max_pages_per_room: 64,
    };
    let engine = Engine::new(adapter, store, engine_config);
    let settings = if history {
        RunSettings::run(config.history_interval_ms)
    } else {
        RunSettings::follow()
    };

    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    let summary = runtime::run(engine, settings, shutdown)
        .await
        .context("live ingestion stopped")?;
    eprintln!(
        "stopped: sync_batches={} sync_events={} history_pages={} history_events={} \
         rooms_completed={} gaps_repaired={} deferred={} unavailable={} rate_limits={}",
        summary.sync_batches,
        summary.sync_events,
        summary.history_pages,
        summary.history_events,
        summary.rooms_completed,
        summary.gaps_repaired,
        summary.deferred,
        summary.unavailable,
        summary.rate_limits
    );
    Ok(())
}

/// Create the export destination without ever truncating an existing path.
///
/// `create_new` refuses files, symlinks (including dangling ones) and hardlink
/// paths, so the archive and any unrelated file are safe from a stray `--out`.
/// On Unix the new file is owner-only (`0600`), forced past any umask.
fn create_export_file(path: &Path) -> anyhow::Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }

    let file = options.open(path).map_err(|error| {
        anyhow::anyhow!(
            "refusing to write export to {}: {error} \
             (the destination may already exist, be a symlink, or be the archive itself)",
            path.display()
        )
    })?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // Apply the mode exactly, even when the process umask masked it.
        let mut permissions = file.metadata()?.permissions();
        permissions.set_mode(0o600);
        file.set_permissions(permissions)?;
    }

    Ok(file)
}
