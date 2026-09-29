//! Transport-independent ingestion engine.
//!
//! The engine owns the store and drives sync and history pagination. The real
//! Matrix adapter will implement [`Transport`] with matrix-sdk typed requests;
//! tests use a fake. No transaction is ever held across an `.await`: each
//! transport call completes before the store is touched.

use std::collections::HashSet;

use async_trait::async_trait;

use crate::event::{HistoryPage, SyncBatch};
use crate::store::{HistoryStatus, Store, StoreError, SyncApplyOutcome};

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
    pub limit: u32,
}

/// Transport failures, classified for retry policy. Permanent failures must
/// abort and be surfaced; rate limits carry the server's retry hint.
#[derive(Debug, Clone, thiserror::Error)]
pub enum TransportError {
    #[error("permanent access or authentication error: {0}")]
    Permanent(String),
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
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RoomHistoryOutcome {
    pub pages_fetched: u64,
    pub events_stored: u64,
    pub completed: bool,
    pub stalled: bool,
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
            Err(error @ TransportError::Permanent(_)) => {
                self.store.record_sync_failure(&error.to_string())?;
                Err(EngineError::Transport(error))
            }
        }
    }

    /// Advance history for every room that has unfinished, non-stalled work.
    pub async fn run_history_once(
        &mut self,
        received_at: i64,
    ) -> Result<HistoryRunOutcome, EngineError> {
        let positions = self.store.rooms_needing_history()?;
        let mut outcome = HistoryRunOutcome::default();
        for position in positions {
            outcome.rooms_visited += 1;
            let room = self
                .run_room_history_once(&position.room_id, received_at)
                .await?;
            outcome.pages_fetched += room.pages_fetched;
            outcome.events_stored += room.events_stored;
            if room.completed {
                outcome.rooms_completed += 1;
            }
            if room.stalled {
                outcome.rooms_stalled += 1;
            }
        }
        Ok(outcome)
    }

    /// Advance one room's backward pagination. Stops on completion, on a
    /// repeated token (stall, persisted) and on a cycled token (stall,
    /// persisted); never loops on a stalled server.
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
                    from: token,
                    limit: self.config.history_limit,
                })
                .await?;

            let applied = self.store.apply_history_page(room_id, &page, received_at)?;
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
            }
        }
        Ok(outcome)
    }
}
