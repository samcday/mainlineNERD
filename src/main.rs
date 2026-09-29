//! `mln-ingest`: status, export and an honest stub for live ingestion.
//!
//! The live Matrix transport adapter is not part of this checkpoint; `status`
//! and `export` operate on the durable archive only.

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};
use mainlinenerd_ingest::store::Store;

#[derive(Parser)]
#[command(
    name = "mln-ingest",
    about = "mainlineNERD passive Matrix ingestion (durable store; live adapter pending)"
)]
struct Cli {
    /// Path to the SQLite archive
    #[arg(
        long,
        env = "MLN_DB",
        default_value = "mainlinenerd.sqlite3",
        global = true
    )]
    db: PathBuf,

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
    /// Live sync follower (not implemented in this checkpoint)
    Follow,
    /// Live sync plus concurrent history backfill (not implemented in this checkpoint)
    Run,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ExportKind {
    /// Current text projection, one row per message
    Messages,
    /// Raw stored events
    Events,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match execute(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error:#}");
            ExitCode::FAILURE
        }
    }
}

fn execute(cli: Cli) -> anyhow::Result<()> {
    match cli.command {
        Command::Status { json } => {
            let store = Store::open_read_only(&cli.db)
                .with_context(|| format!("opening archive {}", cli.db.display()))?;
            if json {
                println!("{}", serde_json::to_string_pretty(&store.status()?)?);
            } else {
                print!("{}", store.render_status()?);
            }
        }
        Command::Export { kind, room, out } => {
            let store = Store::open_read_only(&cli.db)
                .with_context(|| format!("opening archive {}", cli.db.display()))?;
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
        Command::Follow | Command::Run => {
            anyhow::bail!(
                "the live Matrix transport adapter is not implemented in this checkpoint. \
                 `status` and `export` work against an existing archive; ingestion will be \
                 enabled by the matrix-sdk adapter task (see docs/architecture.md)."
            );
        }
    }
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
