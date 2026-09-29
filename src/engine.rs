//! Transport-independent ingestion engine.
//!
//! The engine owns the store and drives sync and history pagination. The real
//! Matrix adapter will implement [`Transport`] with matrix-sdk typed requests;
//! tests use a fake. No transaction is ever held across an `.await`: each
//! transport call completes before the store is touched.

use std::collections::HashSet;

use async_trait::async_trait;

use crate::event::{HistoryPage, SyncBatch};
use crate::store::{HistoryStatus, HistoryWork, Store, StoreError, SyncApplyOutcome};

/// One long-poll `/sync` request.
#[derive(Debug, Clone)]
pub struct SyncRequest {
    pub since: Option<String>,
    pub timeout_ms: u64,
}

/// One backward `/messages` request.
#[derive(Debug, Clone)]
pub struct HistoryRequest {
    pub room_id: String,
    pub from: String,
    /// Lower boundary of a bounded gap repair; `None` means archival backfill,
    /// which has no lower bound short of the start of accessible history.
    pub to: Option<String>,
    pub limit: u32,
}

/// Transport failures, classified for retry policy.
#[derive(Debug, Clone, thiserror::Error)]
pub enum TransportError {
    /// Credentials/session are invalid: no room work can succeed, so the whole
    /// run must stop instead of failing every remaining room.
    #[error("authentication failure: {0}")]
    Authentication(String),
    /// This one room is permanently inaccessible (forbidden/not found). The
    /// work item fails; later rooms must still progress.
    #[error("room unavailable: {0}")]
    RoomUnavailable(String),
    /// Rate limited: stop the run and back off for the hint.
    #[error("rate limited (retry_after_ms={retry_after_ms:?})")]
    RateLimited { retry_after_ms: Option<u64> },
    #[error("transient transport error: {0}")]
    Transient(String),
}

#[async_trait]
pub trait Transport: Send + Sync {
    async fn sync(&self, request: SyncRequest) -> Result<SyncBatch, TransportError>;
    async fn history(&self, request: HistoryRequest) -> Result<HistoryPage, TransportError>;
}

#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub sync_timeout_ms: u64,
    pub history_limit: u32,
    pub max_pages_per_room: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            sync_timeout_ms: 30_000,
            history_limit: 50,
            max_pages_per_room: 32,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("{0}")]
    Transport(#[from] TransportError),
}

#[derive(Debug)]
pub enum SyncPollOutcome {
    Applied(SyncApplyOutcome),
    RateLimited { retry_after_ms: Option<u64> },
    TransientFailure { message: String },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HistoryRunOutcome {
    pub rooms_visited: u64,
    pub pages_fetched: u64,
    pub events_stored: u64,
    pub rooms_completed: u64,
    pub rooms_stalled: u64,
    pub gaps_visited: u64,
    pub gaps_repaired: u64,
    /// Work items that failed permanently for one room only.
    pub items_failed: u64,
    /// Work items deferred after a transient error; they stay queued.
    pub items_deferred: u64,
    /// Set when a rate limit stopped the run early; the scheduler must back
    /// off for this long before retrying.
    pub retry_after_ms: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RoomHistoryOutcome {
    pub pages_fetched: u64,
    pub events_stored: u64,
    pub completed: bool,
    pub stalled: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GapRepairOutcome {
    pub pages_fetched: u64,
    pub events_stored: u64,
    pub repaired: bool,
    pub unresolved: bool,
}

pub struct Engine<T: Transport> {
    transport: T,
    store: Store,
    config: EngineConfig,
}

impl<T: Transport> Engine<T> {
    pub fn new(transport: T, store: Store, config: EngineConfig) -> Self {
        Self {
            transport,
            store,
            config,
        }
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    pub fn store_mut(&mut self) -> &mut Store {
        &mut self.store
    }

    pub fn transport(&self) -> &T {
        &self.transport
    }

    /// Run one sync poll. Success commits events plus the `next_batch` token
    /// atomically. Permanent errors abort; transient errors and rate limits are
    /// returned so the caller can back off.
    pub async fn poll_sync_once(
        &mut self,
        received_at: i64,
    ) -> Result<SyncPollOutcome, EngineError> {
        let request = SyncRequest {
            since: self.store.since_token()?,
            timeout_ms: self.config.sync_timeout_ms,
        };
        match self.transport.sync(request).await {
            Ok(batch) => {
                let outcome = self.store.apply_sync_batch(&batch, received_at)?;
                Ok(SyncPollOutcome::Applied(outcome))
            }
            Err(TransportError::RateLimited { retry_after_ms }) => {
                self.store.record_sync_failure("rate limited")?;
                Ok(SyncPollOutcome::RateLimited { retry_after_ms })
            }
            Err(TransportError::Transient(message)) => {
                self.store.record_sync_failure(&message)?;
                Ok(SyncPollOutcome::TransientFailure { message })
            }
            Err(
                error @ (TransportError::Authentication(_) | TransportError::RoomUnavailable(_)),
            ) => {
                self.store.record_sync_failure(&error.to_string())?;
                Err(EngineError::Transport(error))
            }
        }
    }

    /// Advance every pending history work item once: each room's base archival
    /// backfill, then each open bounded gap repair. A room-local permanent
    /// failure is recorded and skipped so later rooms progress; a rate limit
    /// stops the run with a retry hint; an authentication failure stops the
    /// whole run.
    pub async fn run_history_once(
        &mut self,
        received_at: i64,
    ) -> Result<HistoryRunOutcome, EngineError> {
        let mut outcome = HistoryRunOutcome::default();

        let positions = self.store.rooms_needing_history()?;
        for position in positions {
            outcome.rooms_visited += 1;
            match self
                .run_room_history_once(&position.room_id, received_at)
                .await
            {
                Ok(room) => {
                    outcome.pages_fetched += room.pages_fetched;
                    outcome.events_stored += room.events_stored;
                    if room.completed {
                        outcome.rooms_completed += 1;
                    }
                    if room.stalled {
                        outcome.rooms_stalled += 1;
                    }
                }
                Err(EngineError::Transport(error)) => {
                    if !self.handle_history_error(
                        &position.room_id,
                        None,
                        &error,
                        received_at,
                        &mut outcome,
                    )? {
                        return Ok(outcome);
                    }
                }
                Err(error) => return Err(error),
            }
        }

        let gaps = self.store.open_gap_positions()?;
        for gap in gaps {
            outcome.gaps_visited += 1;
            match self.run_gap_repair_once(gap.gap_id, received_at).await {
                Ok(repair) => {
                    outcome.pages_fetched += repair.pages_fetched;
                    outcome.events_stored += repair.events_stored;
                    if repair.repaired {
                        outcome.gaps_repaired += 1;
                    }
                }
                Err(EngineError::Transport(error)) => {
                    if !self.handle_history_error(
                        &gap.room_id,
                        Some(gap.gap_id),
                        &error,
                        received_at,
                        &mut outcome,
                    )? {
                        return Ok(outcome);
                    }
                }
                Err(error) => return Err(error),
            }
        }

        Ok(outcome)
    }

    /// Advance one room's base archival backfill. Stops on completion, on a
    /// repeated token (stall, persisted) and on a cycled token (stall,
    /// persisted); never loops on a stalled server. Gap repair jobs are
    /// separate work items and are not touched.
    pub async fn run_room_history_once(
        &mut self,
        room_id: &str,
        received_at: i64,
    ) -> Result<RoomHistoryOutcome, EngineError> {
        let mut outcome = RoomHistoryOutcome::default();
        let mut visited: HashSet<String> = HashSet::new();

        while (outcome.pages_fetched as usize) < self.config.max_pages_per_room {
            if self.store.room_history_complete(room_id)? {
                outcome.completed = true;
                break;
            }
            let Some(token) = self.store.room_history_token(room_id)? else {
                break;
            };
            if !visited.insert(token.clone()) {
                self.store.mark_history_stalled(
                    room_id,
                    "history pagination cycled to a previously seen token",
                    received_at,
                )?;
                outcome.stalled = true;
                break;
            }

            let page = self
                .transport
                .history(HistoryRequest {
                    room_id: room_id.to_owned(),
                    from: token.clone(),
                    to: None,
                    limit: self.config.history_limit,
                })
                .await?;

            let applied = self.store.apply_history_page(
                room_id,
                HistoryWork::Base,
                &token,
                &page,
                received_at,
            )?;
            outcome.pages_fetched += 1;
            outcome.events_stored += applied.events_seen.saturating_sub(applied.events_duplicate);

            match applied.status {
                Some(HistoryStatus::Advanced) => continue,
                Some(HistoryStatus::Completed) => {
                    outcome.completed = true;
                    break;
                }
                Some(HistoryStatus::Stalled) | None => {
                    outcome.stalled = true;
                    break;
                }
                // The durable cursor moved under us; re-read and continue.
                Some(HistoryStatus::Stale) => continue,
            }
        }
        Ok(outcome)
    }

    /// Repair one bounded gap from its own durable cursor. The request carries
    /// the job's saved lower boundary (`to`); the page is applied only while
    /// the cursor still matches. Exhaustion, stall or completion of this job
    /// never completes another gap or the base backfill.
    pub async fn run_gap_repair_once(
        &mut self,
        gap_id: i64,
        received_at: i64,
    ) -> Result<GapRepairOutcome, EngineError> {
        let mut outcome = GapRepairOutcome::default();
        let mut visited: HashSet<String> = HashSet::new();

        while (outcome.pages_fetched as usize) < self.config.max_pages_per_room {
            let Some(gap) = self.store.open_gap_position(gap_id)? else {
                break;
            };
            if !visited.insert(gap.token.clone()) {
                self.store.mark_gap_unresolved(
                    gap_id,
                    "history pagination cycled to a previously seen token",
                    received_at,
                )?;
                outcome.unresolved = true;
                break;
            }

            let page = self
                .transport
                .history(HistoryRequest {
                    room_id: gap.room_id.clone(),
                    from: gap.token.clone(),
                    to: gap.to_token.clone(),
                    limit: self.config.history_limit,
                })
                .await?;

            let applied = self.store.apply_history_page(
                &gap.room_id,
                HistoryWork::Gap(gap_id),
                &gap.token,
                &page,
                received_at,
            )?;
            outcome.pages_fetched += 1;
            outcome.events_stored += applied.events_seen.saturating_sub(applied.events_duplicate);

            match applied.status {
                Some(HistoryStatus::Advanced) => continue,
                Some(HistoryStatus::Completed) => {
                    outcome.repaired = true;
                    break;
                }
                Some(HistoryStatus::Stalled) | None => {
                    outcome.unresolved = true;
                    break;
                }
                // The durable cursor moved under us; re-read and continue.
                Some(HistoryStatus::Stale) => continue,
            }
        }
        Ok(outcome)
    }

    /// Classify a room-scoped history failure. `Ok(true)` means the run can
    /// continue with other work items, `Ok(false)` means it should stop early
    /// with a retry hint recorded, and `Err` is reserved for authentication
    /// failure, which no later room can survive.
    fn handle_history_error(
        &self,
        room_id: &str,
        gap_id: Option<i64>,
        error: &TransportError,
        received_at: i64,
        outcome: &mut HistoryRunOutcome,
    ) -> Result<bool, EngineError> {
        match error {
            TransportError::Authentication(_) => Err(EngineError::Transport(error.clone())),
            TransportError::RateLimited { retry_after_ms } => {
                outcome.retry_after_ms = outcome.retry_after_ms.or(*retry_after_ms);
                outcome.items_deferred += 1;
                Ok(false)
            }
            TransportError::Transient(_) => {
                if gap_id.is_none() {
                    self.store
                        .record_history_error(room_id, &error.to_string(), received_at)?;
                }
                outcome.items_deferred += 1;
                Ok(true)
            }
            TransportError::RoomUnavailable(_) => {
                match gap_id {
                    Some(gap_id) => {
                        self.store
                            .mark_gap_unresolved(gap_id, &error.to_string(), received_at)?;
                    }
                    None => {
                        self.store.mark_history_stalled(
                            room_id,
                            &error.to_string(),
                            received_at,
                        )?;
                    }
                }
                outcome.items_failed += 1;
                Ok(true)
            }
        }
    }
}
