//! Durable SQLite archive store.
//!
//! All mutations go through one synchronous `Connection` owned by the caller.
//! Network waits never happen while a transaction is open. Every batch of
//! events and the cursor that describes it commit in the same transaction, so
//! a crash or error can only lose work that was never acknowledged.

use std::fmt::Write as _;
use std::io::Write;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{params, Connection, OpenFlags, OptionalExtension, TransactionBehavior};
use serde::Serialize;
use serde_json::{json, Value};

use crate::event::{self, HistoryPage, NormalizedEvent, Source, SyncBatch};

pub const SCHEMA_VERSION: i64 = 2;

/// Stall reason for a page whose `end` equals the token it was requested from.
const REPEATED_TOKEN_REASON: &str = "history pagination returned a repeated token";
/// Stall reason for a page whose `end` was already visited by that work item.
const CYCLED_TOKEN_REASON: &str = "history pagination cycled to a previously seen token";

/// The homeserver/account this archive belongs to. Cursors are only reusable
/// for the same binding; there are no credentials in the store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArchiveIdentity {
    pub homeserver: String,
    pub user_id: String,
    pub device_id: String,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("{}", .0)]
    IdentityMismatch(Box<IdentityMismatch>),
    #[error("archive has no account binding")]
    Unbound,
    #[error("archive schema version {found} is newer than supported version {supported}")]
    SchemaTooNew { found: i64, supported: i64 },
    #[error(
        "archive schema version {found} predates this build (supported version {supported}); \
         refusing to modify it: keep this archive and point this build at a new database path, \
         or use a build that supports version {found} to export its contents"
    )]
    SchemaTooOld { found: i64, supported: i64 },
    #[error(
        "room {room_id} has an m.room.create with a present non-string room_version; \
         refusing to guess a version and rolling the batch back"
    )]
    MalformedRoomVersion { room_id: String },
    #[error("event {event_id} in {room_id} is malformed: {reason}")]
    MalformedEvent {
        room_id: String,
        event_id: String,
        reason: String,
    },
    #[error("room {0} is not known to this archive")]
    UnknownRoom(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Details of an account-binding mismatch. Boxed inside [`StoreError`] to keep
/// the error small.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IdentityMismatch {
    pub existing: ArchiveIdentity,
    pub requested: ArchiveIdentity,
}

impl std::fmt::Display for IdentityMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "archive is bound to {}/{}/{}; refusing to reuse its cursors for {}/{}/{}",
            self.existing.homeserver,
            self.existing.user_id,
            self.existing.device_id,
            self.requested.homeserver,
            self.requested.user_id,
            self.requested.device_id
        )
    }
}

const SCHEMA_SQL: &str = r#"
CREATE TABLE archive_meta (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  homeserver TEXT NOT NULL,
  user_id TEXT NOT NULL,
  device_id TEXT NOT NULL,
  bound_at INTEGER NOT NULL
);

CREATE TABLE sync_progress (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  since_token TEXT,
  last_success_at INTEGER,
  consecutive_failures INTEGER NOT NULL DEFAULT 0,
  last_error TEXT
);
INSERT OR IGNORE INTO sync_progress (id, since_token) VALUES (1, NULL);

CREATE TABLE rooms (
  room_id TEXT PRIMARY KEY,
  room_version TEXT,
  predecessor_room_id TEXT,
  successor_room_id TEXT,
  encrypted INTEGER NOT NULL DEFAULT 0,
  needs_operator_action INTEGER NOT NULL DEFAULT 0,
  action_note TEXT,
  updated_at INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TABLE room_history (
  room_id TEXT PRIMARY KEY REFERENCES rooms(room_id),
  token TEXT,
  complete INTEGER NOT NULL DEFAULT 0,
  stalled INTEGER NOT NULL DEFAULT 0,
  pages INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  updated_at INTEGER
) WITHOUT ROWID;

CREATE TABLE gap_jobs (
  gap_id INTEGER PRIMARY KEY AUTOINCREMENT,
  room_id TEXT NOT NULL REFERENCES rooms(room_id),
  created_at INTEGER NOT NULL,
  reason TEXT NOT NULL,
  boundary_token TEXT,
  upper_token TEXT,
  cursor_token TEXT,
  status TEXT NOT NULL DEFAULT 'open',
  closed_at INTEGER,
  close_reason TEXT
);
CREATE UNIQUE INDEX gap_jobs_dedupe ON gap_jobs(room_id, upper_token);
CREATE INDEX gap_jobs_by_status ON gap_jobs(room_id, status);

CREATE TABLE history_visited (
  room_id TEXT NOT NULL,
  work_kind TEXT NOT NULL CHECK (work_kind IN ('base', 'gap')),
  work_id INTEGER NOT NULL DEFAULT 0,
  token TEXT NOT NULL,
  visited_at INTEGER NOT NULL,
  PRIMARY KEY (room_id, work_kind, work_id, token)
) WITHOUT ROWID;

CREATE TABLE events (
  room_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  sender TEXT,
  state_key TEXT,
  origin_server_ts INTEGER,
  received_at INTEGER NOT NULL,
  last_seen_at INTEGER NOT NULL,
  source TEXT NOT NULL,
  raw_json TEXT NOT NULL,
  body_text TEXT,
  relation_type TEXT,
  relates_to_event_id TEXT,
  edit_target TEXT,
  edit_attempt INTEGER NOT NULL DEFAULT 0,
  thread_root_id TEXT,
  redacted INTEGER NOT NULL DEFAULT 0,
  redacted_by TEXT,
  redacted_at INTEGER,
  encrypted INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (room_id, event_id)
) WITHOUT ROWID;
CREATE INDEX events_edit_target ON events(room_id, edit_target);
CREATE INDEX events_ordering ON events(room_id, origin_server_ts);

CREATE TABLE redactions (
  room_id TEXT NOT NULL,
  target_event_id TEXT NOT NULL,
  redaction_event_id TEXT NOT NULL,
  origin_server_ts INTEGER,
  received_at INTEGER NOT NULL,
  PRIMARY KEY (room_id, target_event_id, redaction_event_id)
) WITHOUT ROWID;
CREATE INDEX redactions_by_target ON redactions(room_id, target_event_id);

CREATE TABLE current_messages (
  room_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  sender TEXT,
  origin_server_ts INTEGER,
  body TEXT,
  latest_edit_event_id TEXT,
  redacted INTEGER NOT NULL DEFAULT 0,
  redaction_ts INTEGER,
  relation_type TEXT,
  relates_to_event_id TEXT,
  thread_root_id TEXT,
  encrypted INTEGER NOT NULL DEFAULT 0,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (room_id, event_id)
) WITHOUT ROWID;
"#;

/// Outcome of one committed `/sync` batch.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct SyncApplyOutcome {
    pub rooms: u64,
    pub events_seen: u64,
    pub events_duplicate: u64,
    pub gaps_opened: u64,
}

/// Outcome of one committed `/messages` page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum HistoryStatus {
    Advanced,
    Completed,
    Stalled,
    /// The response no longer matches the durable cursor of the work item it
    /// was requested for; nothing was applied.
    Stale,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct HistoryApplyOutcome {
    pub events_seen: u64,
    pub events_duplicate: u64,
    pub status: Option<HistoryStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LiveHealth {
    pub since_token_set: bool,
    pub last_success_at: Option<i64>,
    pub consecutive_failures: i64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RoomStatus {
    pub room_id: String,
    pub room_version: Option<String>,
    pub encrypted: bool,
    pub needs_operator_action: bool,
    pub action_note: Option<String>,
    pub successor_room_id: Option<String>,
    pub events: i64,
    pub messages: i64,
    pub history_token_set: bool,
    pub history_complete: bool,
    pub history_stalled: bool,
    pub history_pages: i64,
    pub history_error: Option<String>,
    pub open_gaps: i64,
    pub unresolved_gaps: i64,
    /// Last non-terminal error recorded against an open gap repair job, if any.
    /// Base backfill errors are kept separately in `history_error`.
    pub open_gap_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StatusReport {
    pub binding: ArchiveIdentity,
    pub bound_at: i64,
    pub schema_version: i64,
    pub live: LiveHealth,
    pub rooms: Vec<RoomStatus>,
}

/// A room with unfinished (or stalled) base archival backfill.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryPosition {
    pub room_id: String,
    pub token: String,
}

/// Which durable cursor a `/messages` response belongs to. Base backfill and
/// each bounded gap repair job have independent cursors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum HistoryWork {
    Base,
    Gap(i64),
}

/// One bounded gap repair job with its own durable cursor and lower bound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GapPosition {
    pub gap_id: i64,
    pub room_id: String,
    /// The cursor the next page must be requested from.
    pub token: String,
    /// The lower bound of the bounded repair (the previous committed sync
    /// token). Bounded repair stops here; it never walks past it.
    pub to_token: Option<String>,
}

/// The durable archive.
pub struct Store {
    conn: Connection,
}

impl Store {
    /// Open or create an archive and bind it to `identity` on first use.
    pub fn open(path: &Path, identity: &ArchiveIdentity) -> Result<Self, StoreError> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let conn = Self::configure(Connection::open(path)?)?;
        let mut store = Self { conn };
        // Validate (and, for a fresh file, create) the schema before enabling
        // durable-write mode, so an unsupported archive is rejected without
        // even switching its journal mode.
        store.ensure_schema(false)?;
        // FULL keeps the documented guarantee that a committed batch/page
        // survives a crash; WAL with NORMAL may lose acknowledged commits on
        // power loss.
        store
            .conn
            .execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")?;
        store.ensure_binding(identity)?;
        Ok(store)
    }

    /// Open an existing archive read-only (status/export).
    pub fn open_read_only(path: &Path) -> Result<Self, StoreError> {
        let conn = Self::configure(Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?)?;
        let mut store = Self { conn };
        store.ensure_schema(true)?;
        store.require_binding()?;
        Ok(store)
    }

    fn configure(conn: Connection) -> Result<Connection, StoreError> {
        conn.busy_timeout(Duration::from_secs(5))?;
        conn.execute_batch("PRAGMA foreign_keys=ON;")?;
        Ok(conn)
    }

    /// Create the schema and its version in one transaction, so a late DDL
    /// failure cannot strand a half-initialized archive.
    ///
    /// An existing nonzero `user_version` that is not [`SCHEMA_VERSION`] is
    /// rejected before any DDL or DML runs, in both writable and read-only
    /// opens. Older archives are never reset or migrated in place: the schema
    /// is read from `PRAGMA user_version` alone, so even an old layout missing
    /// a later column is recognized and left untouched.
    fn ensure_schema(&mut self, read_only: bool) -> Result<(), StoreError> {
        let found: i64 = self
            .conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if found > SCHEMA_VERSION {
            return Err(StoreError::SchemaTooNew {
                found,
                supported: SCHEMA_VERSION,
            });
        }
        if found != 0 && found < SCHEMA_VERSION {
            return Err(StoreError::SchemaTooOld {
                found,
                supported: SCHEMA_VERSION,
            });
        }
        if found == 0 {
            if read_only {
                return Err(StoreError::Unbound);
            }
            let tx = self
                .conn
                .transaction_with_behavior(TransactionBehavior::Immediate)?;
            tx.execute_batch(SCHEMA_SQL)?;
            tx.execute_batch(&format!("PRAGMA user_version = {SCHEMA_VERSION};"))?;
            tx.commit()?;
        }
        Ok(())
    }

    fn ensure_binding(&self, identity: &ArchiveIdentity) -> Result<(), StoreError> {
        let existing: Option<(String, String, String)> = self
            .conn
            .query_row(
                "SELECT homeserver, user_id, device_id FROM archive_meta WHERE id = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        match existing {
            None => {
                self.conn.execute(
                    "INSERT INTO archive_meta (id, homeserver, user_id, device_id, bound_at) VALUES (1, ?1, ?2, ?3, ?4)",
                    params![
                        normalize_homeserver(&identity.homeserver),
                        identity.user_id,
                        identity.device_id,
                        now_unix_ms(),
                    ],
                )?;
                Ok(())
            }
            Some((homeserver, user_id, device_id)) => {
                if homeserver == normalize_homeserver(&identity.homeserver)
                    && user_id == identity.user_id
                    && device_id == identity.device_id
                {
                    Ok(())
                } else {
                    Err(StoreError::IdentityMismatch(Box::new(IdentityMismatch {
                        existing: ArchiveIdentity {
                            homeserver,
                            user_id,
                            device_id,
                        },
                        requested: ArchiveIdentity {
                            homeserver: normalize_homeserver(&identity.homeserver),
                            user_id: identity.user_id.clone(),
                            device_id: identity.device_id.clone(),
                        },
                    })))
                }
            }
        }
    }

    fn require_binding(&self) -> Result<(), StoreError> {
        let bound: Option<i64> = self
            .conn
            .query_row("SELECT id FROM archive_meta WHERE id = 1", [], |row| {
                row.get(0)
            })
            .optional()?;
        bound.ok_or(StoreError::Unbound).map(|_| ())
    }

    /// The persisted `since` token, if any.
    pub fn since_token(&self) -> Result<Option<String>, StoreError> {
        Ok(self
            .conn
            .query_row(
                "SELECT since_token FROM sync_progress WHERE id = 1",
                [],
                |row| row.get(0),
            )
            .optional()?
            .flatten())
    }

    pub fn live_health(&self) -> Result<LiveHealth, StoreError> {
        let (since_token, last_success_at, failures, last_error): (
            Option<String>,
            Option<i64>,
            i64,
            Option<String>,
        ) = self.conn.query_row(
            "SELECT since_token, last_success_at, consecutive_failures, last_error FROM sync_progress WHERE id = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        Ok(LiveHealth {
            since_token_set: since_token.is_some(),
            last_success_at,
            consecutive_failures: failures,
            last_error,
        })
    }

    pub fn record_sync_failure(&self, error: &str) -> Result<(), StoreError> {
        self.conn.execute(
            "UPDATE sync_progress SET consecutive_failures = consecutive_failures + 1, last_error = ?1 WHERE id = 1",
            params![error],
        )?;
        Ok(())
    }

    /// Commit one `/sync` batch: all room events, bounded gap repair jobs and
    /// the global `next_batch` token in a single transaction. Each limited
    /// sync gap is bounded by the previously committed global token and gets
    /// its own durable cursor; the room's base backfill cursor is never
    /// rewound or reset by a live batch.
    pub fn apply_sync_batch(
        &mut self,
        batch: &SyncBatch,
        received_at: i64,
    ) -> Result<SyncApplyOutcome, StoreError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let previous_since: Option<String> = tx.query_row(
            "SELECT since_token FROM sync_progress WHERE id = 1",
            [],
            |row| row.get(0),
        )?;
        let mut outcome = SyncApplyOutcome::default();
        for room in &batch.rooms {
            apply_sync_room(
                &tx,
                room,
                previous_since.as_deref(),
                received_at,
                &mut outcome,
            )?;
        }
        tx.execute(
            "UPDATE sync_progress SET since_token = ?1, last_success_at = ?2, consecutive_failures = 0, last_error = NULL WHERE id = 1",
            params![batch.next_batch, received_at],
        )?;
        tx.commit()?;
        Ok(outcome)
    }

    /// Commit one `/messages` page and update exactly the durable cursor the
    /// page was requested for: the room's base backfill cursor or one bounded
    /// gap repair job. `expected_from` must still match that work item's own
    /// cursor and the work item must still be eligible (a base that is not
    /// complete or stalled; a gap that is still open); otherwise the response
    /// is [`HistoryStatus::Stale`] and changes nothing, so a late in-flight
    /// page can never overwrite newer progress. Exhaustion, stalls and
    /// completion of one work item never close another.
    ///
    /// Successful page tokens are persisted per work item in the same
    /// transaction that applies the page. A returned `end` already visited by
    /// this work item is a cycle: it stalls (base) or unresolves (gap) the work
    /// item instead of advancing, even across calls and process restarts. A
    /// malformed page rolls the ledger back with the events and cursor, and a
    /// stale page never touches it.
    pub fn apply_history_page(
        &mut self,
        room_id: &str,
        work: HistoryWork,
        expected_from: &str,
        page: &HistoryPage,
        received_at: i64,
    ) -> Result<HistoryApplyOutcome, StoreError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if !room_exists(&tx, room_id)? {
            return Err(StoreError::UnknownRoom(room_id.to_owned()));
        }

        // Validate the work item before storing anything, so a stale response
        // cannot touch events, cursors or visited tokens.
        let target = match work {
            HistoryWork::Base => match base_state(&tx, room_id)? {
                Some(state)
                    if !state.complete
                        && !state.stalled
                        && state.token.as_deref() == Some(expected_from) =>
                {
                    AppliedWork::Base
                }
                _ => return Ok(stale_outcome()),
            },
            HistoryWork::Gap(gap_id) => match gap_row(&tx, gap_id)? {
                Some(gap)
                    if gap.room_id == room_id
                        && gap.status == "open"
                        && gap.cursor_token.as_deref() == Some(expected_from) =>
                {
                    AppliedWork::Gap {
                        gap_id,
                        boundary: gap.boundary_token,
                    }
                }
                _ => return Ok(stale_outcome()),
            },
        };
        let (work_kind, work_id) = ledger_key(work);

        let room_version = room_version(&tx, room_id)?;
        let mut outcome = HistoryApplyOutcome::default();
        for value in &page.chunk {
            let event = normalize_event(
                room_id,
                value,
                room_version.as_deref(),
                Source::History,
                received_at,
            )?;
            outcome.events_seen += 1;
            if store_event(&tx, &event, received_at)? {
                outcome.events_duplicate += 1;
            }
        }

        let repeated = page.end.as_deref() == Some(expected_from);
        let cycled = match page.end.as_deref() {
            Some(end) if !repeated => token_visited(&tx, room_id, work_kind, work_id, end)?,
            _ => false,
        };
        match (&target, &page.end, repeated, cycled) {
            (AppliedWork::Base, _, true, _) => {
                mark_base_stalled(&tx, room_id, REPEATED_TOKEN_REASON, received_at)?;
                outcome.status = Some(HistoryStatus::Stalled);
            }
            (AppliedWork::Base, _, _, true) => {
                mark_base_stalled(&tx, room_id, CYCLED_TOKEN_REASON, received_at)?;
                outcome.status = Some(HistoryStatus::Stalled);
            }
            (AppliedWork::Base, None, _, _) => {
                tx.execute(
                    "UPDATE room_history SET token = NULL, complete = 1, stalled = 0, pages = pages + 1, last_error = NULL, updated_at = ?2 WHERE room_id = ?1",
                    params![room_id, received_at],
                )?;
                clear_visited(&tx, room_id, work_kind, work_id)?;
                outcome.status = Some(HistoryStatus::Completed);
            }
            (AppliedWork::Base, Some(end), _, _) => {
                record_visited(&tx, room_id, work_kind, work_id, expected_from, received_at)?;
                record_visited(&tx, room_id, work_kind, work_id, end, received_at)?;
                tx.execute(
                    "UPDATE room_history SET token = ?2, complete = 0, stalled = 0, pages = pages + 1, last_error = NULL, updated_at = ?3 WHERE room_id = ?1",
                    params![room_id, end, received_at],
                )?;
                outcome.status = Some(HistoryStatus::Advanced);
            }
            (AppliedWork::Gap { gap_id, .. }, _, true, _) => {
                close_gap(
                    &tx,
                    *gap_id,
                    "unresolved",
                    REPEATED_TOKEN_REASON,
                    received_at,
                )?;
                outcome.status = Some(HistoryStatus::Stalled);
            }
            (AppliedWork::Gap { gap_id, .. }, _, _, true) => {
                close_gap(&tx, *gap_id, "unresolved", CYCLED_TOKEN_REASON, received_at)?;
                outcome.status = Some(HistoryStatus::Stalled);
            }
            (AppliedWork::Gap { gap_id, .. }, None, _, _) => {
                close_gap(&tx, *gap_id, "repaired", "history_start", received_at)?;
                clear_visited(&tx, room_id, work_kind, work_id)?;
                outcome.status = Some(HistoryStatus::Completed);
            }
            (AppliedWork::Gap { gap_id, boundary }, Some(end), _, _)
                if boundary.as_deref() == Some(end.as_str()) =>
            {
                close_gap(&tx, *gap_id, "repaired", "token", received_at)?;
                clear_visited(&tx, room_id, work_kind, work_id)?;
                outcome.status = Some(HistoryStatus::Completed);
            }
            (AppliedWork::Gap { gap_id, .. }, Some(end), _, _) => {
                record_visited(&tx, room_id, work_kind, work_id, expected_from, received_at)?;
                record_visited(&tx, room_id, work_kind, work_id, end, received_at)?;
                tx.execute(
                    "UPDATE gap_jobs SET cursor_token = ?2, close_reason = NULL WHERE gap_id = ?1",
                    params![gap_id, end],
                )?;
                outcome.status = Some(HistoryStatus::Advanced);
            }
        }
        tx.commit()?;
        Ok(outcome)
    }

    /// Mark a room's base backfill stalled without advancing its token. Gap
    /// repair jobs are independent work items and are not touched.
    pub fn mark_history_stalled(
        &self,
        room_id: &str,
        reason: &str,
        at: i64,
    ) -> Result<(), StoreError> {
        mark_base_stalled(&self.conn, room_id, reason, at)
    }

    /// Persist a non-terminal base backfill error without stalling the room,
    /// so the work item is retried on a later run.
    pub fn record_history_error(
        &self,
        room_id: &str,
        error: &str,
        at: i64,
    ) -> Result<(), StoreError> {
        self.conn.execute(
            "UPDATE room_history SET last_error = ?2, updated_at = ?3 WHERE room_id = ?1",
            params![room_id, error, at],
        )?;
        Ok(())
    }

    /// Record a non-terminal error against one open gap repair job, leaving it
    /// open and queued for a later run. The room's base backfill row is never
    /// touched, so `status` attributes the failure to the actual work item.
    pub fn record_gap_error(&self, gap_id: i64, error: &str) -> Result<(), StoreError> {
        self.conn.execute(
            "UPDATE gap_jobs SET close_reason = ?2 WHERE gap_id = ?1 AND status = 'open'",
            params![gap_id, error],
        )?;
        Ok(())
    }

    /// Close one gap repair job as unresolved (for example after a repeated
    /// token). Other jobs and the base cursor are untouched.
    pub fn mark_gap_unresolved(
        &self,
        gap_id: i64,
        reason: &str,
        at: i64,
    ) -> Result<(), StoreError> {
        close_gap(&self.conn, gap_id, "unresolved", reason, at)
    }

    /// Rooms with a usable history token that are not complete or stalled,
    /// oldest activity first.
    pub fn rooms_needing_history(&self) -> Result<Vec<HistoryPosition>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT room_id, token FROM room_history
             WHERE complete = 0 AND stalled = 0 AND token IS NOT NULL
             ORDER BY COALESCE(updated_at, 0) ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(HistoryPosition {
                room_id: row.get(0)?,
                token: row.get(1)?,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    /// Open bounded gap repair jobs, oldest first, each with its own durable
    /// cursor. These are serviced separately from [`Self::rooms_needing_history`].
    pub fn open_gap_positions(&self) -> Result<Vec<GapPosition>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT gap_id, room_id, cursor_token, boundary_token FROM gap_jobs
             WHERE status = 'open' AND cursor_token IS NOT NULL
             ORDER BY gap_id ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(GapPosition {
                gap_id: row.get(0)?,
                room_id: row.get(1)?,
                token: row.get(2)?,
                to_token: row.get(3)?,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    /// One open gap repair job, if it is still open and has a cursor.
    pub fn open_gap_position(&self, gap_id: i64) -> Result<Option<GapPosition>, StoreError> {
        Ok(self
            .conn
            .query_row(
                "SELECT gap_id, room_id, cursor_token, boundary_token FROM gap_jobs
                 WHERE gap_id = ?1 AND status = 'open' AND cursor_token IS NOT NULL",
                params![gap_id],
                |row| {
                    Ok(GapPosition {
                        gap_id: row.get(0)?,
                        room_id: row.get(1)?,
                        token: row.get(2)?,
                        to_token: row.get(3)?,
                    })
                },
            )
            .optional()?)
    }

    pub fn room_history_token(&self, room_id: &str) -> Result<Option<String>, StoreError> {
        history_token(&self.conn, room_id)
    }

    pub fn room_history_complete(&self, room_id: &str) -> Result<bool, StoreError> {
        Ok(self
            .conn
            .query_row(
                "SELECT complete FROM room_history WHERE room_id = ?1",
                params![room_id],
                |row| row.get(0),
            )
            .optional()?
            .unwrap_or(false))
    }

    pub fn room_history_stalled(&self, room_id: &str) -> Result<bool, StoreError> {
        Ok(self
            .conn
            .query_row(
                "SELECT stalled FROM room_history WHERE room_id = ?1",
                params![room_id],
                |row| row.get(0),
            )
            .optional()?
            .unwrap_or(false))
    }

    pub fn status(&self) -> Result<StatusReport, StoreError> {
        let (homeserver, user_id, device_id, bound_at): (String, String, String, i64) =
            self.conn.query_row(
                "SELECT homeserver, user_id, device_id, bound_at FROM archive_meta WHERE id = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )?;
        let live = self.live_health()?;
        let mut stmt = self.conn.prepare(
            "SELECT r.room_id, r.room_version, r.encrypted, r.needs_operator_action, r.action_note,
                    r.successor_room_id,
                    (SELECT COUNT(*) FROM events e WHERE e.room_id = r.room_id),
                    (SELECT COUNT(*) FROM current_messages m WHERE m.room_id = r.room_id),
                    h.token IS NOT NULL, h.complete, h.stalled, h.pages, h.last_error,
                    (SELECT COUNT(*) FROM gap_jobs g WHERE g.room_id = r.room_id AND g.status = 'open'),
                    (SELECT COUNT(*) FROM gap_jobs g WHERE g.room_id = r.room_id AND g.status = 'unresolved'),
                    (SELECT g.close_reason FROM gap_jobs g
                       WHERE g.room_id = r.room_id AND g.status = 'open' AND g.close_reason IS NOT NULL
                       ORDER BY g.gap_id DESC LIMIT 1)
             FROM rooms r LEFT JOIN room_history h ON h.room_id = r.room_id
             ORDER BY r.room_id",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(RoomStatus {
                room_id: row.get(0)?,
                room_version: row.get(1)?,
                encrypted: row.get(2)?,
                needs_operator_action: row.get(3)?,
                action_note: row.get(4)?,
                successor_room_id: row.get(5)?,
                events: row.get(6)?,
                messages: row.get(7)?,
                history_token_set: row.get(8)?,
                history_complete: row.get::<_, Option<bool>>(9)?.unwrap_or(false),
                history_stalled: row.get::<_, Option<bool>>(10)?.unwrap_or(false),
                history_pages: row.get::<_, Option<i64>>(11)?.unwrap_or(0),
                history_error: row.get(12)?,
                open_gaps: row.get(13)?,
                unresolved_gaps: row.get(14)?,
                open_gap_error: row.get(15)?,
            })
        })?;
        let mut rooms = Vec::new();
        for row in rows {
            rooms.push(row?);
        }
        Ok(StatusReport {
            binding: ArchiveIdentity {
                homeserver,
                user_id,
                device_id,
            },
            bound_at,
            schema_version: SCHEMA_VERSION,
            live,
            rooms,
        })
    }

    /// Export the current text projection as JSONL. Redacted rows keep
    /// metadata with a `null` body and `"redacted": true`.
    pub fn export_messages<W: Write>(
        &self,
        room: Option<&str>,
        out: &mut W,
    ) -> Result<u64, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT room_id, event_id, sender, origin_server_ts, body, latest_edit_event_id,
                    redacted, relation_type, relates_to_event_id, thread_root_id, encrypted
             FROM current_messages
             WHERE (?1 IS NULL OR room_id = ?1)
             ORDER BY room_id ASC, COALESCE(origin_server_ts, 0) ASC, event_id ASC",
        )?;
        let mut rows = stmt.query(params![room])?;
        let mut count = 0u64;
        while let Some(row) = rows.next()? {
            let value = json!({
                "kind": "message",
                "room_id": row.get::<_, String>(0)?,
                "event_id": row.get::<_, String>(1)?,
                "sender": row.get::<_, Option<String>>(2)?,
                "origin_server_ts": row.get::<_, Option<i64>>(3)?,
                "body": row.get::<_, Option<String>>(4)?,
                "latest_edit_event_id": row.get::<_, Option<String>>(5)?,
                "redacted": row.get::<_, bool>(6)?,
                "relation_type": row.get::<_, Option<String>>(7)?,
                "relates_to_event_id": row.get::<_, Option<String>>(8)?,
                "thread_root_id": row.get::<_, Option<String>>(9)?,
                "encrypted": row.get::<_, bool>(10)?,
            });
            writeln!(out, "{}", serde_json::to_string(&value)?)?;
            count += 1;
        }
        Ok(count)
    }

    /// Export raw stored events as JSONL. Bodies of redacted events were
    /// pruned before they were written, so this export cannot resurrect them.
    pub fn export_events<W: Write>(
        &self,
        room: Option<&str>,
        out: &mut W,
    ) -> Result<u64, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT room_id, event_id, event_type, sender, origin_server_ts, received_at,
                    source, redacted, raw_json
             FROM events
             WHERE (?1 IS NULL OR room_id = ?1)
             ORDER BY room_id ASC, COALESCE(origin_server_ts, 0) ASC, event_id ASC",
        )?;
        let mut rows = stmt.query(params![room])?;
        let mut count = 0u64;
        while let Some(row) = rows.next()? {
            let raw_json: String = row.get(8)?;
            let raw: Value = serde_json::from_str(&raw_json).unwrap_or(Value::Null);
            let value = json!({
                "kind": "event",
                "room_id": row.get::<_, String>(0)?,
                "event_id": row.get::<_, String>(1)?,
                "event_type": row.get::<_, String>(2)?,
                "sender": row.get::<_, Option<String>>(3)?,
                "origin_server_ts": row.get::<_, Option<i64>>(4)?,
                "received_at": row.get::<_, i64>(5)?,
                "source": row.get::<_, String>(6)?,
                "redacted": row.get::<_, bool>(7)?,
                "raw": raw,
            });
            writeln!(out, "{}", serde_json::to_string(&value)?)?;
            count += 1;
        }
        Ok(count)
    }

    /// Render a short human-readable status report. Tokens and bodies are
    /// deliberately never printed.
    pub fn render_status(&self) -> Result<String, StoreError> {
        let report = self.status()?;
        let mut out = String::new();
        let _ = writeln!(
            out,
            "archive: {} as {}/{} (bound {})",
            report.binding.homeserver,
            report.binding.user_id,
            report.binding.device_id,
            format_ms(report.bound_at)
        );
        let _ = writeln!(
            out,
            "live sync: checkpoint={} last_success={} failures={}{}",
            if report.live.since_token_set {
                "set"
            } else {
                "unset"
            },
            report
                .live
                .last_success_at
                .map(format_ms)
                .unwrap_or_else(|| "never".to_owned()),
            report.live.consecutive_failures,
            report
                .live
                .last_error
                .as_deref()
                .map(|e| format!(" last_error={e}"))
                .unwrap_or_default()
        );
        if report.rooms.is_empty() {
            let _ = writeln!(out, "rooms: none");
        }
        for room in &report.rooms {
            let mut flags = Vec::new();
            if room.encrypted {
                flags.push("encrypted".to_owned());
            }
            if room.needs_operator_action {
                flags.push(format!(
                    "operator-action({})",
                    room.action_note.as_deref().unwrap_or("unspecified")
                ));
            }
            if room.successor_room_id.is_some() {
                flags.push("upgraded".to_owned());
            }
            let _ = writeln!(
                out,
                "room {} v{} events={} messages={} history={} pages={} gaps(open={},unresolved={}{}){}",
                room.room_id,
                room.room_version.as_deref().unwrap_or("?"),
                room.events,
                room.messages,
                if room.history_complete {
                    "complete".to_owned()
                } else if room.history_stalled {
                    format!("stalled({})", room.history_error.as_deref().unwrap_or("?"))
                } else if room.history_token_set {
                    "pending".to_owned()
                } else {
                    "no-token".to_owned()
                },
                room.history_pages,
                room.open_gaps,
                room.unresolved_gaps,
                room.open_gap_error
                    .as_deref()
                    .map(|error| format!(",error={error}"))
                    .unwrap_or_default(),
                if flags.is_empty() {
                    String::new()
                } else {
                    format!(" [{}]", flags.join(", "))
                }
            );
        }
        Ok(out)
    }
}

pub fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn format_ms(ms: i64) -> String {
    time::OffsetDateTime::from_unix_timestamp_nanos(ms as i128 * 1_000_000)
        .map(|dt| {
            dt.format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_else(|_| ms.to_string())
        })
        .unwrap_or_else(|_| ms.to_string())
}

/// Normalize a homeserver base URL for binding comparison: scheme and
/// authority are case-insensitive, a trailing slash is ignored, paths are kept.
fn normalize_homeserver(input: &str) -> String {
    let trimmed = input.trim().trim_end_matches('/');
    match trimmed.find("://") {
        Some(scheme_end) => {
            let authority_end = trimmed[scheme_end + 3..]
                .find('/')
                .map(|i| scheme_end + 3 + i)
                .unwrap_or(trimmed.len());
            let (prefix, path) = trimmed.split_at(authority_end);
            format!("{}{}", prefix.to_ascii_lowercase(), path)
        }
        None => trimmed.to_ascii_lowercase(),
    }
}

// ---------------------------------------------------------------------------
// Internal row helpers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct EventRow {
    event_type: String,
    sender: Option<String>,
    origin_server_ts: Option<i64>,
    body_text: Option<String>,
    edit_target: Option<String>,
    redacted: bool,
    redacted_at: Option<i64>,
    relation_type: Option<String>,
    relates_to_event_id: Option<String>,
    thread_root_id: Option<String>,
    source: String,
}

fn normalize_event(
    room_id: &str,
    value: &Value,
    room_version: Option<&str>,
    source: Source,
    received_at: i64,
) -> Result<NormalizedEvent, StoreError> {
    event::normalize(room_id, value, room_version, source, received_at).map_err(|error| {
        StoreError::MalformedEvent {
            room_id: room_id.to_owned(),
            event_id: value
                .get("event_id")
                .and_then(Value::as_str)
                .unwrap_or("<missing>")
                .to_owned(),
            reason: error.to_string(),
        }
    })
}

fn room_exists(conn: &Connection, room_id: &str) -> Result<bool, StoreError> {
    Ok(conn
        .query_row(
            "SELECT 1 FROM rooms WHERE room_id = ?1",
            params![room_id],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .is_some())
}

fn room_version(conn: &Connection, room_id: &str) -> Result<Option<String>, StoreError> {
    Ok(conn
        .query_row(
            "SELECT room_version FROM rooms WHERE room_id = ?1",
            params![room_id],
            |row| row.get(0),
        )
        .optional()?
        .flatten())
}

fn history_token(conn: &Connection, room_id: &str) -> Result<Option<String>, StoreError> {
    Ok(conn
        .query_row(
            "SELECT token FROM room_history WHERE room_id = ?1",
            params![room_id],
            |row| row.get(0),
        )
        .optional()?
        .flatten())
}

fn get_event_row(
    conn: &Connection,
    room_id: &str,
    event_id: &str,
) -> Result<Option<EventRow>, StoreError> {
    Ok(conn
        .query_row(
            "SELECT event_type, sender, origin_server_ts, body_text, edit_target, redacted,
                    redacted_at, relation_type, relates_to_event_id, thread_root_id, source
             FROM events WHERE room_id = ?1 AND event_id = ?2",
            params![room_id, event_id],
            |row| {
                Ok(EventRow {
                    event_type: row.get(0)?,
                    sender: row.get(1)?,
                    origin_server_ts: row.get(2)?,
                    body_text: row.get(3)?,
                    edit_target: row.get(4)?,
                    redacted: row.get(5)?,
                    redacted_at: row.get(6)?,
                    relation_type: row.get(7)?,
                    relates_to_event_id: row.get(8)?,
                    thread_root_id: row.get(9)?,
                    source: row.get(10)?,
                })
            },
        )
        .optional()?)
}

fn apply_sync_room(
    conn: &Connection,
    room: &event::SyncRoomUpdate,
    previous_since: Option<&str>,
    received_at: i64,
    outcome: &mut SyncApplyOutcome,
) -> Result<(), StoreError> {
    let mut room_version = room_version(conn, &room.room_id)?;
    let mut predecessor_room_id = None;
    let mut successor_room_id = None;
    let mut encrypted = false;

    let state_and_timeline = room.state.iter().chain(room.timeline.iter());
    for value in state_and_timeline.clone() {
        let Some(obj) = value.as_object() else {
            continue;
        };
        let Some(event_type) = obj.get("type").and_then(Value::as_str) else {
            continue;
        };
        // Room policy is derived only from state events: the type alone is not
        // enough, the expected empty `state_key` must be present. A plain
        // timeline event bearing one of these types is not a state change, and
        // an isolated `m.room.encrypted` payload never enables room-wide E2EE.
        let is_state = obj.get("state_key").and_then(Value::as_str) == Some("");
        if !is_state {
            continue;
        }
        match event_type {
            event::ROOM_CREATE => {
                if let Some(content) = obj.get("content").and_then(Value::as_object) {
                    match content.get("room_version") {
                        // Per the spec an `m.room.create` without a room
                        // version is room version 1. A present but non-string
                        // value is malformed, never coerced into a string that
                        // might accidentally name a recognized version.
                        None => {
                            room_version = room_version.or_else(|| Some("1".to_owned()));
                        }
                        Some(version) => match version.as_str() {
                            Some(version) => room_version = Some(version.to_owned()),
                            None => {
                                return Err(StoreError::MalformedRoomVersion {
                                    room_id: room.room_id.clone(),
                                });
                            }
                        },
                    }
                    predecessor_room_id = content
                        .get("predecessor")
                        .and_then(Value::as_object)
                        .and_then(|p| p.get("room_id"))
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                        .or(predecessor_room_id);
                }
            }
            event::ROOM_ENCRYPTION => encrypted = true,
            event::ROOM_TOMBSTONE => {
                successor_room_id = obj
                    .get("content")
                    .and_then(Value::as_object)
                    .and_then(|c| c.get("replacement_room"))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .or(successor_room_id);
            }
            _ => {}
        }
    }

    let action_note = if successor_room_id.is_some() {
        Some("room upgraded; successor needs operator decision")
    } else if encrypted {
        Some("encrypted room; no key import attempted")
    } else {
        None
    };
    let needs_operator_action = action_note.is_some();

    conn.execute(
        "INSERT INTO rooms (room_id, room_version, predecessor_room_id, successor_room_id, encrypted,
                            needs_operator_action, action_note, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
         ON CONFLICT(room_id) DO UPDATE SET
           room_version = COALESCE(excluded.room_version, rooms.room_version),
           predecessor_room_id = COALESCE(excluded.predecessor_room_id, rooms.predecessor_room_id),
           successor_room_id = COALESCE(excluded.successor_room_id, rooms.successor_room_id),
           encrypted = MAX(rooms.encrypted, excluded.encrypted),
           needs_operator_action = MAX(rooms.needs_operator_action, excluded.needs_operator_action),
           action_note = COALESCE(excluded.action_note, rooms.action_note),
           updated_at = excluded.updated_at",
        params![
            room.room_id,
            room_version,
            predecessor_room_id,
            successor_room_id,
            encrypted,
            needs_operator_action,
            action_note,
            received_at
        ],
    )?;
    outcome.rooms += 1;

    for value in room.state.iter().chain(room.timeline.iter()) {
        let event = normalize_event(
            &room.room_id,
            value,
            room_version.as_deref(),
            Source::Sync,
            received_at,
        )?;
        outcome.events_seen += 1;
        if store_event(conn, &event, received_at)? {
            outcome.events_duplicate += 1;
        }
    }

    if room.limited {
        match previous_since {
            // The first sync has no previous committed token: it is the start
            // of archival backfill, not a missing-live interval.
            None => {
                seed_room_history(conn, &room.room_id, room.prev_batch.as_deref(), received_at)?;
            }
            Some(boundary) => match room.prev_batch.as_deref() {
                Some(upper) if upper != boundary => {
                    let inserted = conn.execute(
                        "INSERT INTO gap_jobs (room_id, created_at, reason, boundary_token, upper_token, cursor_token, status)
                         VALUES (?1, ?2, 'limited_sync', ?3, ?4, ?4, 'open')
                         ON CONFLICT(room_id, upper_token) DO NOTHING",
                        params![room.room_id, received_at, boundary, upper],
                    )?;
                    outcome.gaps_opened += inserted as u64;
                    // Base backfill still starts at this timeline's
                    // prev_batch; the gap job only repairs the bounded live
                    // interval, it does not own the room cursor.
                    seed_room_history(conn, &room.room_id, Some(upper), received_at)?;
                }
                Some(upper) => {
                    // `prev_batch` equals the last committed global token: the
                    // interval has no span, so there is no gap to repair. Base
                    // backfill is still seeded if it never started.
                    seed_room_history(conn, &room.room_id, Some(upper), received_at)?;
                }
                None => {
                    // No repair token: record the gap as unresolved rather
                    // than pretending the timeline is complete.
                    conn.execute(
                        "INSERT INTO gap_jobs (room_id, created_at, reason, boundary_token, upper_token, cursor_token, status, close_reason)
                         VALUES (?1, ?2, 'limited_sync_no_prev_batch', ?3, NULL, NULL, 'unresolved', 'no repair token available')",
                        params![room.room_id, received_at, boundary],
                    )?;
                    outcome.gaps_opened += 1;
                    seed_room_history(conn, &room.room_id, None, received_at)?;
                }
            },
        }
    } else {
        seed_room_history(conn, &room.room_id, room.prev_batch.as_deref(), received_at)?;
    }

    Ok(())
}

/// Seed a room's base backfill cursor.
///
/// A missing row is inserted with `token` (which may be absent, leaving the
/// room honestly unseeded). An existing row is only ever moved from the
/// never-started empty state: no token, no pages, not complete and not
/// stalled. A later explicit `prev_batch` may therefore seed such a row, but
/// in-progress, completed and stalled backfill is never rewound or reset, and
/// the absence of a token is never treated as completion.
fn seed_room_history(
    conn: &Connection,
    room_id: &str,
    token: Option<&str>,
    at: i64,
) -> Result<(), StoreError> {
    conn.execute(
        "INSERT INTO room_history (room_id, token, complete, stalled, pages, updated_at)
         VALUES (?1, ?2, 0, 0, 0, ?3)
         ON CONFLICT(room_id) DO UPDATE SET
           token = excluded.token,
           updated_at = excluded.updated_at
         WHERE room_history.token IS NULL
           AND room_history.pages = 0
           AND room_history.complete = 0
           AND room_history.stalled = 0
           AND excluded.token IS NOT NULL",
        params![room_id, token, at],
    )?;
    Ok(())
}

/// Insert or refresh one event and refresh every projection it can affect.
/// Returns `true` when the event was already present.
fn store_event(
    conn: &Connection,
    event: &NormalizedEvent,
    received_at: i64,
) -> Result<bool, StoreError> {
    let prior = get_event_row(conn, &event.room_id, &event.event_id)?;
    let prior_exists = prior.is_some();

    // Derived bundle caches are insert-only: they never rewrite an existing
    // row's canonical payload. A fetched event (sync/history) is authoritative
    // over a bundle, and the first bundle wins over conflicting replays. A
    // later standalone fetch still replaces a bundle-only representation
    // through the normal upsert below, keeping redaction/suppression state
    // monotonic (prior redaction is folded into `redacted`).
    if event.source == Source::Bundle && prior_exists {
        return Ok(true);
    }

    // Promotion: a fetched standalone event replacing a derived bundle-only
    // row. The fetched payload becomes authoritative, so derived relation/edit
    // fields that it does not carry are cleared rather than coalesced, and the
    // stale edit's former parent is re-projected.
    let prior_is_bundle = prior
        .as_ref()
        .is_some_and(|p| p.source == Source::Bundle.as_str());
    let promotion = prior_is_bundle && event.source != Source::Bundle;
    let prior_edit_parent = prior.as_ref().and_then(|p| p.edit_target.clone());

    let pending_redaction: Option<(String, Option<i64>, i64)> = conn
        .query_row(
            "SELECT redaction_event_id, origin_server_ts, received_at FROM redactions
             WHERE room_id = ?1 AND target_event_id = ?2
             ORDER BY received_at ASC, redaction_event_id ASC LIMIT 1",
            params![event.room_id, event.event_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;

    let prior_redacted = prior.as_ref().map(|p| p.redacted).unwrap_or(false);
    // A replacement whose original has been redacted (directly, through a
    // pending redaction, or because the original arrived already redacted) is
    // suppressed as well, even though no redaction names the replacement.
    let replacement_target_redacted = match &event.edit_target {
        Some(target) => target_is_redacted(conn, &event.room_id, target)?,
        None => false,
    };
    let redacted = event.redacted
        || prior_redacted
        || pending_redaction.is_some()
        || replacement_target_redacted;

    let raw_json = if redacted {
        if event.redacted {
            event.raw_json.clone()
        } else {
            let value: Value = serde_json::from_str(&event.raw_json).unwrap_or(Value::Null);
            let version = room_version(conn, &event.room_id)?;
            event::prune_redacted(&value, &event.event_type, version.as_deref())
        }
    } else {
        event.raw_json.clone()
    };
    let body_text = if redacted {
        None
    } else {
        event.body_text.clone()
    };
    let (redacted_by, redacted_at) = match &pending_redaction {
        Some((redaction_event_id, _, redaction_received_at)) => (
            Some(redaction_event_id.clone()),
            Some(*redaction_received_at),
        ),
        None if event.redacted => (None, Some(event.received_at)),
        None => (None, None),
    };

    conn.execute(
        "INSERT INTO events (
            room_id, event_id, event_type, sender, state_key, origin_server_ts,
            received_at, last_seen_at, source, raw_json, body_text,
            relation_type, relates_to_event_id, edit_target, edit_attempt, thread_root_id,
            redacted, redacted_by, redacted_at, encrypted)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20)
         ON CONFLICT(room_id, event_id) DO UPDATE SET
            event_type = excluded.event_type,
            sender = COALESCE(excluded.sender, events.sender),
            state_key = COALESCE(excluded.state_key, events.state_key),
            origin_server_ts = COALESCE(excluded.origin_server_ts, events.origin_server_ts),
            received_at = MIN(events.received_at, excluded.received_at),
            last_seen_at = excluded.last_seen_at,
            source = CASE WHEN events.source = 'bundle' THEN excluded.source ELSE events.source END,
            raw_json = excluded.raw_json,
            body_text = excluded.body_text,
            relation_type = CASE WHEN events.source = 'bundle'
                THEN excluded.relation_type ELSE COALESCE(excluded.relation_type, events.relation_type) END,
            relates_to_event_id = CASE WHEN events.source = 'bundle'
                THEN excluded.relates_to_event_id ELSE COALESCE(excluded.relates_to_event_id, events.relates_to_event_id) END,
            edit_target = CASE WHEN events.source = 'bundle'
                THEN excluded.edit_target ELSE COALESCE(excluded.edit_target, events.edit_target) END,
            edit_attempt = CASE WHEN events.source = 'bundle'
                THEN excluded.edit_attempt ELSE MAX(events.edit_attempt, excluded.edit_attempt) END,
            thread_root_id = CASE WHEN events.source = 'bundle'
                THEN excluded.thread_root_id ELSE COALESCE(excluded.thread_root_id, events.thread_root_id) END,
            redacted = excluded.redacted,
            redacted_by = COALESCE(excluded.redacted_by, events.redacted_by),
            redacted_at = COALESCE(excluded.redacted_at, events.redacted_at),
            encrypted = MAX(events.encrypted, excluded.encrypted)",
        params![
            event.room_id,
            event.event_id,
            event.event_type,
            event.sender,
            event.state_key,
            event.origin_server_ts,
            received_at,
            received_at,
            event.source.as_str(),
            raw_json,
            body_text,
            event.relation_type,
            event.relates_to_event_id,
            event.edit_target,
            event.edit_attempt,
            event.thread_root_id,
            redacted,
            redacted_by,
            redacted_at,
            event.event_type == event::ENCRYPTED,
        ],
    )?;

    // Whenever a redacted message original is stored (a plain redaction
    // arriving later, a pending redaction finally finding its target, or an
    // already-redacted representation), suppress any replacement edits that
    // already point at it. This keeps their bodies out of the archive even
    // when the edit was seen before the redaction.
    if redacted && event::is_message_type(&event.event_type) && !event.edit_attempt {
        suppress_replacement_bodies(conn, &event.room_id, &event.event_id, received_at)?;
    }

    let mut refresh: Vec<String> = Vec::new();
    if event.event_type == event::REDACTION {
        if let Some(target) = &event.redaction_target {
            let target_row = get_event_row(conn, &event.room_id, target)?;
            conn.execute(
                "INSERT OR IGNORE INTO redactions (room_id, target_event_id, redaction_event_id, origin_server_ts, received_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![event.room_id, target, event.event_id, event.origin_server_ts, received_at],
            )?;
            mark_redacted(conn, &event.room_id, target, &event.event_id, received_at)?;
            // Suppress edits even when the target event itself has not been
            // fetched yet; the redactions row is authoritative.
            suppress_replacement_bodies(conn, &event.room_id, target, received_at)?;
            refresh.push(target.clone());
            if let Some(parent) = target_row.and_then(|row| row.edit_target) {
                refresh.push(parent);
            }
        }
    }
    if let Some(target) = &event.edit_target {
        refresh.push(target.clone());
    }
    if event.redacted || promotion {
        if let Some(parent) = prior_edit_parent {
            refresh.push(parent);
        }
    }
    if event::is_message_type(&event.event_type) && !event.edit_attempt {
        refresh.push(event.event_id.clone());
    }

    refresh.sort();
    refresh.dedup();
    for target in refresh {
        project_event(conn, &event.room_id, &target, received_at)?;
    }

    // Store validated bundled replacements through the same event path, keyed
    // by their own event id. They are plain edits: redaction state, target
    // suppression and replay idempotence all apply unchanged.
    for bundled in &event.bundled_replacements {
        store_event(conn, bundled, received_at)?;
    }
    Ok(prior_exists)
}

/// Mark an event redacted: prune its stored raw JSON and clear its body.
fn mark_redacted(
    conn: &Connection,
    room_id: &str,
    target: &str,
    redaction_event_id: &str,
    at: i64,
) -> Result<(), StoreError> {
    let row: Option<(String, String)> = conn
        .query_row(
            "SELECT event_type, raw_json FROM events WHERE room_id = ?1 AND event_id = ?2",
            params![room_id, target],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if let Some((event_type, raw_json)) = row {
        let value: Value = serde_json::from_str(&raw_json).unwrap_or(Value::Null);
        let version = room_version(conn, room_id)?;
        let pruned = event::prune_redacted(&value, &event_type, version.as_deref());
        conn.execute(
            "UPDATE events SET redacted = 1, body_text = NULL, raw_json = ?3,
                    redacted_by = COALESCE(redacted_by, ?4),
                    redacted_at = COALESCE(redacted_at, ?5)
             WHERE room_id = ?1 AND event_id = ?2",
            params![room_id, target, pruned, redaction_event_id, at],
        )?;
    }
    Ok(())
}

/// Whether `target` is known to be redacted in `room_id`: either its stored row
/// is redacted or a redaction naming it is recorded (possibly before the target
/// itself was fetched). Strictly room-scoped, so equal event ids in different
/// rooms never leak into each other.
fn target_is_redacted(conn: &Connection, room_id: &str, target: &str) -> Result<bool, StoreError> {
    Ok(conn
        .query_row(
            "SELECT 1 FROM events WHERE room_id = ?1 AND event_id = ?2 AND redacted = 1
             UNION ALL
             SELECT 1 FROM redactions WHERE room_id = ?1 AND target_event_id = ?2
             LIMIT 1",
            params![room_id, target],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .is_some())
}

/// Suppress the bodies of every replacement edit that targets a redacted
/// original. The edit rows keep their relation columns, so an older replay of
/// the edit is recognized and suppressed again instead of restoring the text.
/// This is our local application retention rule; it cannot and does not recall
/// remote federation copies.
fn suppress_replacement_bodies(
    conn: &Connection,
    room_id: &str,
    original_event_id: &str,
    at: i64,
) -> Result<(), StoreError> {
    let version = room_version(conn, room_id)?;
    let edits: Vec<(String, String, String)> = {
        let mut stmt = conn.prepare(
            "SELECT event_id, event_type, raw_json FROM events
             WHERE room_id = ?1 AND edit_target = ?2 AND redacted = 0",
        )?;
        let rows = stmt.query_map(params![room_id, original_event_id], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;
        let mut edits = Vec::new();
        for row in rows {
            edits.push(row?);
        }
        edits
    };
    for (event_id, event_type, raw_json) in edits {
        let value: Value = serde_json::from_str(&raw_json).unwrap_or(Value::Null);
        let pruned = event::prune_redacted(&value, &event_type, version.as_deref());
        conn.execute(
            "UPDATE events SET redacted = 1, body_text = NULL, raw_json = ?3,
                    redacted_at = COALESCE(redacted_at, ?4)
             WHERE room_id = ?1 AND event_id = ?2 AND redacted = 0",
            params![room_id, event_id, pruned, at],
        )?;
    }
    Ok(())
}

/// Recompute the current-message projection for one event id.
fn project_event(
    conn: &Connection,
    room_id: &str,
    event_id: &str,
    now: i64,
) -> Result<(), StoreError> {
    let Some(event) = get_event_row(conn, room_id, event_id)? else {
        return Ok(());
    };
    // Edits are never messages of their own.
    if event.edit_target.is_some() {
        return Ok(());
    }
    if !event::is_message_type(&event.event_type) {
        return Ok(());
    }

    let redaction: Option<(Option<i64>, i64)> = conn
        .query_row(
            "SELECT origin_server_ts, received_at FROM redactions
             WHERE room_id = ?1 AND target_event_id = ?2
             ORDER BY received_at DESC, redaction_event_id DESC LIMIT 1",
            params![room_id, event_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;

    if event.redacted || redaction.is_some() {
        let redaction_ts = redaction
            .and_then(|(origin, _)| origin)
            .or(event.redacted_at);
        upsert_projection(
            conn,
            room_id,
            event_id,
            &event.event_type,
            event.sender.as_deref(),
            event.origin_server_ts,
            None,
            None,
            true,
            redaction_ts,
            event.relation_type.as_deref(),
            event.relates_to_event_id.as_deref(),
            event.thread_root_id.as_deref(),
            false,
            now,
        )?;
        return Ok(());
    }

    if event.event_type == event::ENCRYPTED {
        upsert_projection(
            conn,
            room_id,
            event_id,
            &event.event_type,
            event.sender.as_deref(),
            event.origin_server_ts,
            None,
            None,
            false,
            None,
            event.relation_type.as_deref(),
            event.relates_to_event_id.as_deref(),
            event.thread_root_id.as_deref(),
            true,
            now,
        )?;
        return Ok(());
    }

    // Pick the latest valid replacement among replacements by
    // (origin_server_ts, event_id), with the event id breaking ties. The
    // replacement is never compared against the original's timestamp: a
    // sender's clock may lag, but a later replacement is still authoritative.
    // A missing sender must not match another missing sender, so only a
    // non-null sender equal to the original's is accepted.
    let best_edit: Option<(String, String)> = conn
        .query_row(
            "SELECT event_id, body_text FROM events
             WHERE room_id = ?1 AND event_type = ?2 AND edit_target = ?3 AND redacted = 0
               AND body_text IS NOT NULL AND sender IS NOT NULL AND sender = ?4
               AND NOT EXISTS (
                   SELECT 1 FROM redactions r
                   WHERE r.room_id = events.room_id AND r.target_event_id = events.event_id
               )
             ORDER BY COALESCE(origin_server_ts, 0) DESC, event_id DESC LIMIT 1",
            params![room_id, event::MESSAGE, event_id, event.sender.as_deref()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;

    let (body, latest_edit_event_id) = match best_edit {
        Some((edit_id, edit_body)) => (Some(edit_body), Some(edit_id)),
        None => (event.body_text.clone(), None),
    };

    upsert_projection(
        conn,
        room_id,
        event_id,
        &event.event_type,
        event.sender.as_deref(),
        event.origin_server_ts,
        body.as_deref(),
        latest_edit_event_id.as_deref(),
        false,
        None,
        event.relation_type.as_deref(),
        event.relates_to_event_id.as_deref(),
        event.thread_root_id.as_deref(),
        false,
        now,
    )
}

#[allow(clippy::too_many_arguments)]
fn upsert_projection(
    conn: &Connection,
    room_id: &str,
    event_id: &str,
    event_type: &str,
    sender: Option<&str>,
    origin_server_ts: Option<i64>,
    body: Option<&str>,
    latest_edit_event_id: Option<&str>,
    redacted: bool,
    redaction_ts: Option<i64>,
    relation_type: Option<&str>,
    relates_to_event_id: Option<&str>,
    thread_root_id: Option<&str>,
    encrypted: bool,
    now: i64,
) -> Result<(), StoreError> {
    conn.execute(
        "INSERT INTO current_messages (
            room_id, event_id, event_type, sender, origin_server_ts, body, latest_edit_event_id,
            redacted, redaction_ts, relation_type, relates_to_event_id, thread_root_id, encrypted, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
         ON CONFLICT(room_id, event_id) DO UPDATE SET
            event_type = excluded.event_type,
            sender = COALESCE(excluded.sender, current_messages.sender),
            origin_server_ts = COALESCE(excluded.origin_server_ts, current_messages.origin_server_ts),
            body = CASE WHEN current_messages.redacted = 1 THEN NULL ELSE excluded.body END,
            latest_edit_event_id = CASE WHEN current_messages.redacted = 1 THEN NULL ELSE excluded.latest_edit_event_id END,
            redacted = MAX(current_messages.redacted, excluded.redacted),
            redaction_ts = COALESCE(excluded.redaction_ts, current_messages.redaction_ts),
            relation_type = COALESCE(excluded.relation_type, current_messages.relation_type),
            relates_to_event_id = COALESCE(excluded.relates_to_event_id, current_messages.relates_to_event_id),
            thread_root_id = COALESCE(excluded.thread_root_id, current_messages.thread_root_id),
            encrypted = MAX(current_messages.encrypted, excluded.encrypted),
            updated_at = excluded.updated_at",
        params![
            room_id,
            event_id,
            event_type,
            sender,
            origin_server_ts,
            body,
            latest_edit_event_id,
            redacted,
            redaction_ts,
            relation_type,
            relates_to_event_id,
            thread_root_id,
            encrypted,
            now
        ],
    )?;
    Ok(())
}

fn mark_base_stalled(
    conn: &Connection,
    room_id: &str,
    reason: &str,
    at: i64,
) -> Result<(), StoreError> {
    conn.execute(
        "UPDATE room_history SET stalled = 1, last_error = ?2, updated_at = ?3 WHERE room_id = ?1",
        params![room_id, reason, at],
    )?;
    Ok(())
}

/// The validated work item a page is about to update.
enum AppliedWork {
    Base,
    Gap {
        gap_id: i64,
        boundary: Option<String>,
    },
}

/// Nothing applied: a late page whose work item no longer expects this token.
fn stale_outcome() -> HistoryApplyOutcome {
    HistoryApplyOutcome {
        status: Some(HistoryStatus::Stale),
        ..Default::default()
    }
}

/// Durable state of a room's base backfill, used to validate eligibility.
struct BaseState {
    token: Option<String>,
    complete: bool,
    stalled: bool,
}

fn base_state(conn: &Connection, room_id: &str) -> Result<Option<BaseState>, StoreError> {
    Ok(conn
        .query_row(
            "SELECT token, complete, stalled FROM room_history WHERE room_id = ?1",
            params![room_id],
            |row| {
                Ok(BaseState {
                    token: row.get(0)?,
                    complete: row.get(1)?,
                    stalled: row.get(2)?,
                })
            },
        )
        .optional()?)
}

/// The `history_visited` ledger key of a work item: base backfill uses work id
/// 0; each gap repair job uses its own `gap_id`. Ledgers never overlap, so a
/// cycle in one work item cannot stall another.
fn ledger_key(work: HistoryWork) -> (&'static str, i64) {
    match work {
        HistoryWork::Base => ("base", 0),
        HistoryWork::Gap(gap_id) => ("gap", gap_id),
    }
}

fn token_visited(
    conn: &Connection,
    room_id: &str,
    work_kind: &str,
    work_id: i64,
    token: &str,
) -> Result<bool, StoreError> {
    Ok(conn
        .query_row(
            "SELECT 1 FROM history_visited
             WHERE room_id = ?1 AND work_kind = ?2 AND work_id = ?3 AND token = ?4",
            params![room_id, work_kind, work_id, token],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .is_some())
}

fn record_visited(
    conn: &Connection,
    room_id: &str,
    work_kind: &str,
    work_id: i64,
    token: &str,
    at: i64,
) -> Result<(), StoreError> {
    conn.execute(
        "INSERT OR IGNORE INTO history_visited (room_id, work_kind, work_id, token, visited_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![room_id, work_kind, work_id, token, at],
    )?;
    Ok(())
}

fn clear_visited(
    conn: &Connection,
    room_id: &str,
    work_kind: &str,
    work_id: i64,
) -> Result<(), StoreError> {
    conn.execute(
        "DELETE FROM history_visited WHERE room_id = ?1 AND work_kind = ?2 AND work_id = ?3",
        params![room_id, work_kind, work_id],
    )?;
    Ok(())
}

struct GapRow {
    room_id: String,
    status: String,
    cursor_token: Option<String>,
    boundary_token: Option<String>,
}

fn gap_row(conn: &Connection, gap_id: i64) -> Result<Option<GapRow>, StoreError> {
    Ok(conn
        .query_row(
            "SELECT room_id, status, cursor_token, boundary_token FROM gap_jobs WHERE gap_id = ?1",
            params![gap_id],
            |row| {
                Ok(GapRow {
                    room_id: row.get(0)?,
                    status: row.get(1)?,
                    cursor_token: row.get(2)?,
                    boundary_token: row.get(3)?,
                })
            },
        )
        .optional()?)
}

fn close_gap(
    conn: &Connection,
    gap_id: i64,
    status: &str,
    reason: &str,
    at: i64,
) -> Result<(), StoreError> {
    conn.execute(
        "UPDATE gap_jobs SET status = ?2, cursor_token = NULL, closed_at = ?3, close_reason = ?4
         WHERE gap_id = ?1 AND status = 'open'",
        params![gap_id, status, at, reason],
    )?;
    Ok(())
}
