//! Live runtime: startup binding plus the concurrent sync/history coordinator.
//!
//! One task owns the [`Engine`] and therefore the single SQLite writer. Network
//! futures are polled in `select!` and only their finished results are applied,
//! so a held `/sync` long poll never blocks paced `/messages` backfill and an
//! active backfill never blocks a new live batch. No DB transaction is open
//! across an `.await`, and every apply commits before the next request starts,
//! so cancellation leaves a valid committed checkpoint.

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use matrix_sdk::ruma::{OwnedRoomId, OwnedServerName};
use tokio::time::{sleep, sleep_until, Instant};

use crate::config::{Config, RoomSelector};
use crate::engine::{
    Engine, EngineError, HistoryFailure, HistoryRequest, Transport, TransportError,
};
use crate::event::{HistoryPage, SyncBatch};
use crate::matrix::{
    parse_alias, parse_room_id, parse_server_name, AdapterError, MatrixTransport,
    RoomCreateMetadata,
};
use crate::store::{now_unix_ms, HistoryWork, Store, StoreError};

/// Startup retry policy for per-room alias/join/metadata work.
#[derive(Debug, Clone)]
pub struct StartupPolicy {
    pub max_attempts: u32,
    pub transient_min: Duration,
    pub transient_max: Duration,
    pub rate_limit_fallback: Duration,
}

impl Default for StartupPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            transient_min: Duration::from_millis(500),
            transient_max: Duration::from_secs(30),
            rate_limit_fallback: Duration::from_secs(30),
        }
    }
}

/// Scheduler policy. Defaults are conservative: one history request per second
/// overall, bounded exponential transient backoff with jitter, and a mandatory
/// fallback when a rate limit carries no hint.
#[derive(Debug, Clone)]
pub struct RunSettings {
    /// `follow` leaves this false; `run` enables paced history work.
    pub history: bool,
    pub history_interval: Duration,
    pub transient_min: Duration,
    pub transient_max: Duration,
    pub rate_limit_fallback: Duration,
    /// How long to wait before re-checking for new history work when none is
    /// currently available.
    pub idle_poll: Duration,
    /// Floor between syncs after an empty or unchanged response, so a broken or
    /// chattering server cannot cause a tight loop; the checkpoint is still
    /// committed.
    pub empty_sync_floor: Duration,
}

impl RunSettings {
    pub fn follow() -> Self {
        Self {
            history: false,
            ..Self::default()
        }
    }

    pub fn run(history_interval_ms: u64) -> Self {
        Self {
            history: true,
            history_interval: Duration::from_millis(history_interval_ms.max(1)),
            ..Self::default()
        }
    }
}

impl Default for RunSettings {
    fn default() -> Self {
        Self {
            history: true,
            history_interval: Duration::from_secs(1),
            transient_min: Duration::from_millis(500),
            transient_max: Duration::from_secs(60),
            rate_limit_fallback: Duration::from_secs(30),
            idle_poll: Duration::from_secs(5),
            empty_sync_floor: Duration::from_secs(1),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RunSummary {
    pub sync_batches: u64,
    pub sync_events: u64,
    pub history_pages: u64,
    pub history_events: u64,
    pub rooms_completed: u64,
    pub gaps_repaired: u64,
    pub deferred: u64,
    pub unavailable: u64,
    pub rate_limits: u64,
    /// Results dropped because the room was no longer admitted (removed from
    /// the allowlist, not ready, or disabled mid-flight). Never committed.
    pub rejected: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error(transparent)]
    Engine(#[from] EngineError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Adapter(#[from] AdapterError),
    #[error(
        "archive/config identity {expected} does not match the token's server identity {found}"
    )]
    IdentityMismatch { expected: String, found: String },
    /// A dedicated-device pilot must verify the device binding; a server that
    /// does not report one cannot be used.
    #[error("the homeserver did not report a device id for the token; cannot verify the configured device binding {0}")]
    DeviceUnverified(String),
    #[error("configured room list is empty")]
    EmptyRooms,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct InitializeReport {
    /// Ready rooms (allowlisted, configured, version known).
    pub rooms: Vec<String>,
    pub joined: Vec<String>,
    pub versions_learned: Vec<String>,
    /// Rooms that could not be made ready, with a bounded reason.
    pub unready: Vec<(String, String)>,
}

/// Validate the token's identity, register the explicit room allowlist, pin
/// aliases, perform operator-requested joins and learn room versions. A
/// room-local startup failure (alias/join/metadata) marks only that room
/// unready and lets the others proceed; authentication remains global.
pub async fn initialize(
    adapter: &MatrixTransport,
    config: &Config,
    store: &mut Store,
) -> Result<InitializeReport, RuntimeError> {
    initialize_with_policy(adapter, config, store, &StartupPolicy::default()).await
}

pub async fn initialize_with_policy(
    adapter: &MatrixTransport,
    config: &Config,
    store: &mut Store,
    policy: &StartupPolicy,
) -> Result<InitializeReport, RuntimeError> {
    let who = adapter.whoami().await?;
    if who.user_id.as_str() != config.user_id {
        return Err(RuntimeError::IdentityMismatch {
            expected: config.user_id.clone(),
            found: who.user_id.to_string(),
        });
    }
    match &who.device_id {
        None => {
            return Err(RuntimeError::DeviceUnverified(format!(
                "{}/{}",
                config.user_id, config.device_id
            )));
        }
        Some(device) if device.as_str() != config.device_id => {
            return Err(RuntimeError::IdentityMismatch {
                expected: format!("{}/{}", config.user_id, config.device_id),
                found: format!("{}/{}", who.user_id, device),
            });
        }
        Some(_) => {}
    }

    let now = now_unix_ms();
    let mut report = InitializeReport::default();
    let mut resolved: Vec<ResolvedRoom> = Vec::with_capacity(config.rooms.len());
    // A final exhausted retry can leave a rate-limit pause that must be honored
    // before any later startup request (including the first live sync).
    let mut cooldown_until: Option<Instant> = None;

    for room in &config.rooms {
        await_cooldown(&mut cooldown_until).await;
        let base_via: Result<Vec<OwnedServerName>, _> = room
            .via
            .iter()
            .map(|server| parse_server_name(server))
            .collect();
        let base_via = base_via?;
        match &room.selector {
            RoomSelector::Id(id) => match parse_room_id(id) {
                Ok(room_id) => {
                    // A candidate is tracked for status but is not admitted to
                    // the data set until its metadata validates.
                    store.register_configured_room(room_id.as_str(), None, now)?;
                    resolved.push(ResolvedRoom {
                        room_id,
                        via: base_via,
                        join: room.join,
                    });
                }
                Err(error) => {
                    report.unready.push((id.clone(), bounded_reason(&error)));
                }
            },
            RoomSelector::Alias(alias) => match parse_alias(alias) {
                Ok(alias_id) => {
                    let mut attempt = 0u32;
                    loop {
                        match adapter.resolve_alias(&alias_id).await {
                            Ok(found) => {
                                // Exact alias identity: the pin key is not
                                // normalized, so #Room and #room stay distinct.
                                if let Err(error) =
                                    store.pin_alias(alias, found.room_id.as_str(), now)
                                {
                                    return Err(RuntimeError::Store(error));
                                }
                                store.register_configured_room(
                                    found.room_id.as_str(),
                                    Some(alias),
                                    now,
                                )?;
                                let mut via = found.servers;
                                via.extend(base_via.iter().cloned());
                                resolved.push(ResolvedRoom {
                                    room_id: found.room_id,
                                    via,
                                    join: room.join,
                                });
                                break;
                            }
                            Err(error) => match startup_backoff(policy, attempt, &error) {
                                StartupStep::Retry(delay) => {
                                    attempt += 1;
                                    sleep(delay).await;
                                }
                                StartupStep::GiveUp(pause) => {
                                    if let Some(delay) = pause {
                                        extend_startup_cooldown(&mut cooldown_until, delay);
                                    }
                                    if let AdapterError::Authentication(_)
                                    | AdapterError::Build
                                    | AdapterError::Transport(_) = error
                                    {
                                        return Err(RuntimeError::Adapter(error));
                                    }
                                    report.unready.push((alias.clone(), bounded_reason(&error)));
                                    break;
                                }
                            },
                        }
                    }
                }
                Err(error) => report.unready.push((alias.clone(), bounded_reason(&error))),
            },
        }
    }

    // The current config is the authoritative allowlist: clear the configured
    // flag on rooms that are no longer listed, so historical rows cannot be
    // mistaken for current configuration.
    let ids: Vec<String> = resolved.iter().map(|r| r.room_id.to_string()).collect();
    store.set_configured_rooms(&ids, now)?;
    if resolved.is_empty() {
        return Err(RuntimeError::EmptyRooms);
    }

    for room in &resolved {
        await_cooldown(&mut cooldown_until).await;
        let room_id = room.room_id.clone();
        let display = room_id.to_string();

        // Explicit operator opt-in comes FIRST: a public-join or
        // joined-history room may refuse state until the bot is a member, so
        // metadata cannot be fetched before the join. It is gated only by the
        // observed-departure rule; a successful join is still not permission to
        // ingest without validated metadata.
        let mut join_failed: Option<String> = None;
        if room.join && store.own_membership(room_id.as_str())?.is_none() {
            let mut attempt = 0u32;
            loop {
                match adapter.join_room(&room_id, &room.via, &room_id).await {
                    Ok(joined) => {
                        report.joined.push(joined.to_string());
                        store.clear_room_metadata_error(room_id.as_str(), now)?;
                        break;
                    }
                    Err(error) => match startup_backoff(policy, attempt, &error) {
                        StartupStep::Retry(delay) => {
                            attempt += 1;
                            sleep(delay).await;
                        }
                        StartupStep::GiveUp(pause) => {
                            if let Some(delay) = pause {
                                extend_startup_cooldown(&mut cooldown_until, delay);
                            }
                            if let AdapterError::Authentication(_)
                            | AdapterError::Build
                            | AdapterError::Transport(_) = error
                            {
                                return Err(RuntimeError::Adapter(error));
                            }
                            join_failed = Some(bounded_reason(&error));
                            break;
                        }
                    },
                }
            }
        } else if own_membership_is_join(store, room_id.as_str())? {
            // We are in the room; any prior join-refusal note is stale.
            store.clear_room_metadata_error(room_id.as_str(), now)?;
        }

        // Metadata after a successful (or not-required) join, validated
        // strictly; a malformed response must never become a guessed version. A
        // failed join defers the room instead of proceeding, so a final join
        // 429 pause is never bypassed by same-room metadata.
        if join_failed.is_none() && store.room_version_of(room_id.as_str())?.is_none() {
            let mut attempt = 0u32;
            loop {
                match adapter.room_create_metadata(&room_id).await {
                    Ok(RoomCreateMetadata::StateEvent(event)) => {
                        store.apply_room_control_state(room_id.as_str(), &[event], now)?;
                        report.versions_learned.push(display.clone());
                        break;
                    }
                    Ok(RoomCreateMetadata::VersionOnly(version)) => {
                        store.set_room_version_control(room_id.as_str(), &version, now)?;
                        report.versions_learned.push(display.clone());
                        break;
                    }
                    Err(error) => match startup_backoff(policy, attempt, &error) {
                        StartupStep::Retry(delay) => {
                            attempt += 1;
                            sleep(delay).await;
                        }
                        StartupStep::GiveUp(pause) => {
                            if let Some(delay) = pause {
                                extend_startup_cooldown(&mut cooldown_until, delay);
                            }
                            if let AdapterError::Authentication(_)
                            | AdapterError::Build
                            | AdapterError::Transport(_) = error
                            {
                                return Err(RuntimeError::Adapter(error));
                            }
                            let reason = bounded_reason(&error);
                            store.record_room_metadata_error(room_id.as_str(), &reason, now)?;
                            report.unready.push((display.clone(), reason));
                            break;
                        }
                    },
                }
            }
        }

        // A failed explicit join keeps the room unready even when state
        // metadata happens to be readable; the note is recorded after metadata
        // so a successful version validation cannot erase it.
        if let Some(reason) = join_failed {
            store.record_room_metadata_error(
                room_id.as_str(),
                &format!("not ready: {reason}"),
                now,
            )?;
            if !report.unready.iter().any(|(room, _)| room == &display) {
                report.unready.push((display.clone(), reason));
            }
        }

        // Only a validated, ready room enters the data-admission set and gets a
        // seeded cursor. Candidate/error rooms stay tracked for status only.
        if store.room_version_of(room_id.as_str())?.is_some()
            && store.room_metadata_error(room_id.as_str())?.is_none()
        {
            adapter.allow_room(room_id.clone());
            store.seed_configured_room_history(room_id.as_str(), now)?;
            report.rooms.push(display);
        } else {
            let reason = store
                .room_metadata_error(room_id.as_str())?
                .unwrap_or_else(|| "room is not ready".to_owned());
            if !report.unready.iter().any(|(room, _)| room == &display) {
                report.unready.push((display, reason));
            }
        }
    }

    // Never start live sync while a final exhausted retry's pause is pending.
    await_cooldown(&mut cooldown_until).await;
    Ok(report)
}

async fn await_cooldown(deadline: &mut Option<Instant>) {
    if let Some(until) = *deadline {
        let now = Instant::now();
        if until > now {
            sleep(until - now).await;
        }
        *deadline = None;
    }
}

fn extend_startup_cooldown(deadline: &mut Option<Instant>, delay: Duration) {
    let target = Instant::now() + delay;
    *deadline = Some(deadline.map_or(target, |current| std::cmp::max(current, target)));
}

fn own_membership_is_join(store: &Store, room_id: &str) -> Result<bool, StoreError> {
    Ok(store.own_membership(room_id)?.as_deref() == Some("join"))
}

fn bounded_reason(error: &AdapterError) -> String {
    match error {
        AdapterError::Authentication(_) => "authentication rejected".to_owned(),
        AdapterError::RoomUnavailable(status) => format!("inaccessible ({status})"),
        AdapterError::Transient(_) => "temporarily unavailable".to_owned(),
        AdapterError::RateLimited { .. } => "rate limited".to_owned(),
        AdapterError::Protocol(reason) => (*reason).to_owned(),
        AdapterError::Build => "client build failed".to_owned(),
        AdapterError::Transport(error) => error.to_string(),
    }
}

enum StartupStep {
    /// Retry after this delay (attempts remain).
    Retry(Duration),
    /// Stop retrying this room. `Some(delay)` preserves a final rate-limit or
    /// transient pause that must be honored before any later request even
    /// though this room is now deferred; `None` means the failure is permanent.
    GiveUp(Option<Duration>),
}

/// Bounded retry classification. The final allowed attempt still yields its
/// pause through [`StartupStep::GiveUp`], so exhausted retries never silently
/// discard a `Retry-After`.
fn startup_backoff(policy: &StartupPolicy, attempt: u32, error: &AdapterError) -> StartupStep {
    let delay = match error {
        AdapterError::RateLimited { retry_after_ms } => {
            rate_delay(*retry_after_ms, policy.rate_limit_fallback)
        }
        AdapterError::Transient(_) => {
            transient_delay(policy.transient_min, policy.transient_max, attempt + 1)
        }
        _ => return StartupStep::GiveUp(None),
    };
    if attempt + 1 >= policy.max_attempts {
        StartupStep::GiveUp(Some(delay))
    } else {
        StartupStep::Retry(delay)
    }
}

struct ResolvedRoom {
    room_id: OwnedRoomId,
    via: Vec<OwnedServerName>,
    join: bool,
}

type SyncFuture<'a> = Pin<Box<dyn Future<Output = Result<SyncBatch, TransportError>> + Send + 'a>>;
type HistoryFuture<'a> =
    Pin<Box<dyn Future<Output = Result<HistoryPage, TransportError>> + Send + 'a>>;

/// Run the coordinator until `shutdown` resolves.
pub async fn run<T, S>(
    mut engine: Engine<T>,
    settings: RunSettings,
    shutdown: S,
) -> Result<RunSummary, RuntimeError>
where
    T: Transport + Clone,
    S: Future<Output = ()>,
{
    let transport = engine.transport().clone();
    tokio::pin!(shutdown);

    let mut summary = RunSummary::default();
    let mut sync_request = engine.sync_request()?;
    let mut sync_fut: Option<SyncFuture<'_>> = Some(transport.sync(sync_request.clone()));
    let mut next_sync_at = Instant::now();

    let mut queue = WorkQueue::default();
    let mut history_in_flight: Option<(HistoryWork, String, HistoryRequest)> = None;
    let mut history_fut: Option<HistoryFuture<'_>> = None;
    let mut next_history_at = Instant::now();
    let mut waiting_for_history_work = false;
    let mut backoff_until = Instant::now();
    let mut transient_attempt: u32 = 0;

    loop {
        let now = Instant::now();
        if now >= backoff_until {
            if sync_fut.is_none() && now >= next_sync_at {
                sync_request = engine.sync_request()?;
                sync_fut = Some(transport.sync(sync_request.clone()));
            }
            if settings.history && history_fut.is_none() && now >= next_history_at {
                match queue.next(&engine)? {
                    Some((work, room, request)) => {
                        waiting_for_history_work = false;
                        history_fut = Some(transport.history(request.clone()));
                        history_in_flight = Some((work, room, request));
                        next_history_at = now + settings.history_interval;
                    }
                    None => {
                        waiting_for_history_work = true;
                        next_history_at = now + settings.idle_poll;
                    }
                }
            }
        }

        let far = now + Duration::from_secs(3_600);
        let sync_deadline = if sync_fut.is_none() {
            std::cmp::max(next_sync_at, backoff_until)
        } else {
            far
        };
        let history_deadline = if settings.history && history_fut.is_none() {
            std::cmp::max(next_history_at, backoff_until)
        } else {
            far
        };
        let wake_deadline = std::cmp::min(sync_deadline, history_deadline);

        // Fair selection: no plane is prioritized, so an unbounded eager stream
        // of ready sync batches cannot starve history or shutdown.
        let event = tokio::select! {
            result = async {
                match sync_fut.as_mut() {
                    Some(future) => Event::Sync(future.await),
                    None => std::future::pending().await,
                }
            } => result,
            result = async {
                match history_fut.as_mut() {
                    Some(future) => Event::History(future.await),
                    None => std::future::pending().await,
                }
            } => result,
            _ = sleep_until(wake_deadline) => Event::Wake,
            _ = &mut shutdown => Event::Stop,
        };

        match event {
            Event::Stop => return Ok(summary),
            Event::Wake => {}
            Event::Sync(result) => {
                sync_fut = None;
                let now = Instant::now();
                let previous_since = sync_request.since.clone();
                match engine.apply_sync_result(result, now_unix_ms())? {
                    crate::engine::SyncPollOutcome::Applied(outcome) => {
                        summary.sync_batches += 1;
                        summary.sync_events += outcome.events_seen;
                        transient_attempt = 0;
                        let current = engine.sync_request()?;
                        // Pace empty responses even when the token changed, and
                        // unchanged tokens, so a chattering server cannot spin.
                        let useful = outcome.events_seen > 0 || outcome.gaps_opened > 0;
                        next_sync_at = if !useful || current.since == previous_since {
                            now + settings.empty_sync_floor
                        } else {
                            now
                        };
                        // New live data may have seeded history or opened gaps.
                        // Wake an idle discovery wait, but never shorten the
                        // pacing interval after an actual history request.
                        if waiting_for_history_work {
                            next_history_at = now;
                        }
                    }
                    crate::engine::SyncPollOutcome::RateLimited { retry_after_ms } => {
                        summary.rate_limits += 1;
                        let delay = rate_delay(retry_after_ms, settings.rate_limit_fallback);
                        extend_backoff(&mut backoff_until, now, delay);
                        next_sync_at = backoff_until;
                        next_history_at = std::cmp::max(next_history_at, backoff_until);
                    }
                    crate::engine::SyncPollOutcome::TransientFailure { .. } => {
                        summary.deferred += 1;
                        transient_attempt = transient_attempt.saturating_add(1);
                        extend_backoff(
                            &mut backoff_until,
                            now,
                            transient_delay(
                                settings.transient_min,
                                settings.transient_max,
                                transient_attempt,
                            ),
                        );
                        next_sync_at = backoff_until;
                    }
                }
            }
            Event::History(result) => {
                history_fut = None;
                let now = Instant::now();
                let Some((work, room, request)) = history_in_flight.take() else {
                    continue;
                };
                next_history_at = now + settings.history_interval;
                match history_result(
                    &mut engine,
                    work,
                    &room,
                    &request,
                    result,
                    now_unix_ms(),
                    &mut summary,
                )? {
                    HistoryDisposition::Continue => transient_attempt = 0,
                    HistoryDisposition::Deferred => {
                        transient_attempt = transient_attempt.saturating_add(1);
                        extend_backoff(
                            &mut backoff_until,
                            now,
                            transient_delay(
                                settings.transient_min,
                                settings.transient_max,
                                transient_attempt,
                            ),
                        );
                    }
                    HistoryDisposition::RateLimited { retry_after_ms } => {
                        extend_backoff(
                            &mut backoff_until,
                            now,
                            rate_delay(retry_after_ms, settings.rate_limit_fallback),
                        );
                    }
                    HistoryDisposition::Rejected => {
                        summary.rejected += 1;
                    }
                    HistoryDisposition::Unavailable => {}
                }
            }
        }
        // Stay cooperative so shutdown and any other task on the runtime are
        // scheduled even under a long stream of immediately-ready results.
        tokio::task::yield_now().await;
    }
}

/// A global cooldown is never shortened by a later result, including a success
/// or a shorter rate-limit hint from the other plane.
fn extend_backoff(current: &mut Instant, now: Instant, delay: Duration) {
    *current = std::cmp::max(*current, now + delay);
}

enum Event {
    Sync(Result<SyncBatch, TransportError>),
    History(Result<HistoryPage, TransportError>),
    Wake,
    Stop,
}

/// Per-room round-robin scheduler.
///
/// Durable jobs stay in SQLite; memory here scales with the *explicitly
/// configured room set*, not the job backlog. Each visit to a room alternates
/// between its base work and one rotating open gap chosen by a bounded key-set
/// query, so a continuing base or a continuing gap relinquishes its turn and
/// cannot monopolize a ready window. `refresh_rooms` picks up newly ready rooms
/// within one rotation.
#[derive(Default)]
struct WorkQueue {
    rooms: VecDeque<String>,
    cursors: HashMap<String, RoomCursor>,
}

#[derive(Default, Clone, Copy)]
struct RoomCursor {
    serve_base_next: bool,
    gap_after: Option<i64>,
    /// Alternates the gap turn between the newest open gap (a bounded
    /// fresh-work opportunity) and the ascending key-set rotation.
    prefer_newest_gap: bool,
    /// The gap job served last, so a continuing newest job cannot be picked by
    /// every fresh turn and starve the ascending rotation.
    last_gap_id: Option<i64>,
}

impl WorkQueue {
    fn next<T: Transport>(
        &mut self,
        engine: &Engine<T>,
    ) -> Result<Option<(HistoryWork, String, HistoryRequest)>, StoreError> {
        self.refresh_rooms(engine)?;
        let room_count = self.rooms.len();
        for _ in 0..room_count {
            let Some(room) = self.rooms.pop_front() else {
                break;
            };
            self.rooms.push_back(room.clone());
            if !history_admitted(engine, &room)? {
                continue;
            }
            let cursor = self.cursors.entry(room.clone()).or_insert(RoomCursor {
                serve_base_next: true,
                gap_after: None,
                prefer_newest_gap: true,
                last_gap_id: None,
            });
            let prefer_base = cursor.serve_base_next;
            cursor.serve_base_next = !prefer_base;

            // Serve the preferred work kind, then fall back to the other kind
            // within the same visit. A base-only room therefore never waits an
            // idle poll between pages, and a gap-only room is served every
            // visit.
            if prefer_base {
                if let Some(request) = engine.base_history_request(&room)? {
                    return Ok(Some((HistoryWork::Base, room, request)));
                }
                if let Some((gap_id, request)) = self.next_gap(engine, &room)? {
                    return Ok(Some((HistoryWork::Gap(gap_id), room, request)));
                }
            } else {
                if let Some((gap_id, request)) = self.next_gap(engine, &room)? {
                    return Ok(Some((HistoryWork::Gap(gap_id), room, request)));
                }
                if let Some(request) = engine.base_history_request(&room)? {
                    return Ok(Some((HistoryWork::Base, room, request)));
                }
            }
        }
        Ok(None)
    }

    /// One gap selection for a room: alternating newest-first and ascending
    /// key-set turns, each a single bounded query. The ascending cursor is only
    /// advanced by ascending turns, so the fresh opportunity cannot skip the
    /// backlog.
    fn next_gap<T: Transport>(
        &mut self,
        engine: &Engine<T>,
        room: &str,
    ) -> Result<Option<(i64, HistoryRequest)>, StoreError> {
        let Some(cursor) = self.cursors.get_mut(room) else {
            return Ok(None);
        };
        let prefer_newest = cursor.prefer_newest_gap;
        cursor.prefer_newest_gap = !prefer_newest;
        // A fresh turn takes the newest open gap, unless it is the one just
        // served (a continuing job) in which case it falls back to the
        // ascending rotation so the backlog cannot be starved.
        let mut effective_newest = prefer_newest;
        let mut page = if prefer_newest {
            let page = engine.store().open_gap_positions_for_room_newest(room, 1)?;
            if page.first().map(|gap| gap.gap_id) == cursor.last_gap_id {
                effective_newest = false;
                Vec::new()
            } else {
                page
            }
        } else {
            Vec::new()
        };
        if page.is_empty() && !effective_newest {
            page = ascending_gap_page(engine, cursor, room)?;
            if page.is_empty() && cursor.gap_after.is_some() {
                cursor.gap_after = None;
                page = ascending_gap_page(engine, cursor, room)?;
            }
        }
        if let Some(gap) = page.into_iter().next() {
            if !effective_newest {
                cursor.gap_after = Some(gap.gap_id);
            }
            cursor.last_gap_id = Some(gap.gap_id);
            if let Some(request) = engine.gap_history_request(gap.gap_id)? {
                return Ok(Some((gap.gap_id, request)));
            }
        }
        Ok(None)
    }

    /// Recompute the rotation from the current configured room set. Cursors for
    /// rooms that are still configured are preserved; removed rooms are dropped.
    fn refresh_rooms<T: Transport>(&mut self, engine: &Engine<T>) -> Result<(), StoreError> {
        let ids = engine.store().configured_room_ids()?;
        let current: HashSet<String> = ids.iter().cloned().collect();
        self.rooms.retain(|room| current.contains(room));
        let queued: HashSet<String> = self.rooms.iter().cloned().collect();
        let additions: Vec<String> = ids
            .iter()
            .filter(|id| !queued.contains(*id))
            .cloned()
            .collect();
        self.rooms.extend(additions);
        self.cursors.retain(|room, _| current.contains(room));
        Ok(())
    }
}

fn ascending_gap_page<T: Transport>(
    engine: &Engine<T>,
    cursor: &RoomCursor,
    room: &str,
) -> Result<Vec<crate::store::GapPosition>, StoreError> {
    engine
        .store()
        .open_gap_positions_for_room_page(room, cursor.gap_after, 1)
}

/// The one admission predicate used before scheduling a request and before
/// committing a result: the room must still be in the current allowlist,
/// configured, ready (version known, no not-ready reason) and pass the mutable
/// room policy (not left/banned/encrypted/upgraded).
fn history_admitted<T: Transport>(engine: &Engine<T>, room: &str) -> Result<bool, StoreError> {
    Ok(engine.transport().is_allowed_room(room)
        && engine.store().room_configured(room)?
        && engine.store().room_metadata_error(room)?.is_none()
        && engine.store().room_version_of(room)?.is_some()
        && engine.store().room_history_allowed(room)?)
}

enum HistoryDisposition {
    Continue,
    Deferred,
    RateLimited {
        retry_after_ms: Option<u64>,
    },
    Unavailable,
    /// The result was not admitted (removed/not-ready/disabled) and was dropped
    /// without touching any cursor or event.
    Rejected,
}

fn history_result<T: Transport>(
    engine: &mut Engine<T>,
    work: HistoryWork,
    room: &str,
    request: &HistoryRequest,
    result: Result<HistoryPage, TransportError>,
    received_at: i64,
    summary: &mut RunSummary,
) -> Result<HistoryDisposition, RuntimeError> {
    // Global transport outcomes are classified FIRST, independently of whether
    // this work item can still be committed: a late 401 from a room that just
    // left must still halt the run, and a late 429 must still impose the global
    // cooldown on every other room. Only data application and local work-item
    // changes are governed by room eligibility.
    match &result {
        Err(TransportError::Authentication(_)) | Err(TransportError::Fatal(_)) => {
            let error = result.expect_err("matched an error above");
            return Err(RuntimeError::Engine(EngineError::Transport(error)));
        }
        Err(TransportError::RateLimited { retry_after_ms }) => {
            summary.rate_limits += 1;
            return Ok(HistoryDisposition::RateLimited {
                retry_after_ms: *retry_after_ms,
            });
        }
        _ => {}
    }
    if !history_admitted(engine, room)? {
        // The room stopped being eligible after the request was issued. The
        // response is dropped, not committed: no event, cursor or ledger
        // changes.
        return Ok(HistoryDisposition::Rejected);
    }
    match result {
        Ok(page) => match engine.commit_history_page(work, request, page, received_at) {
            Ok(committed) => {
                if committed.counted {
                    summary.history_pages += 1;
                    summary.history_events += committed.events_stored;
                    if let Some(crate::store::HistoryStatus::Completed) = committed.outcome.status {
                        if matches!(work, HistoryWork::Base) {
                            summary.rooms_completed += 1;
                        } else {
                            summary.gaps_repaired += 1;
                        }
                    }
                }
                Ok(HistoryDisposition::Continue)
            }
            Err(error) if is_room_local_store_error(&error) => {
                let reason = bounded_store_reason(&error);
                match work {
                    HistoryWork::Base => {
                        engine
                            .store()
                            .mark_history_stalled(room, &reason, received_at)?
                    }
                    HistoryWork::Gap(gap_id) => {
                        engine
                            .store()
                            .mark_gap_unresolved(gap_id, &reason, received_at)?
                    }
                }
                summary.unavailable += 1;
                Ok(HistoryDisposition::Unavailable)
            }
            Err(error) => Err(error.into()),
        },
        Err(error) => match engine.record_history_error(work, room, &error, received_at)? {
            HistoryFailure::Deferred => {
                summary.deferred += 1;
                Ok(HistoryDisposition::Deferred)
            }
            HistoryFailure::Unavailable => {
                summary.unavailable += 1;
                Ok(HistoryDisposition::Unavailable)
            }
            HistoryFailure::RateLimited { retry_after_ms } => {
                summary.rate_limits += 1;
                Ok(HistoryDisposition::RateLimited { retry_after_ms })
            }
            HistoryFailure::Authentication => {
                Err(RuntimeError::Engine(EngineError::Transport(error)))
            }
        },
    }
}

fn is_room_local_store_error(error: &EngineError) -> bool {
    matches!(
        error,
        EngineError::Store(
            StoreError::MalformedEvent { .. } | StoreError::MalformedRoomVersion { .. }
        )
    )
}

fn bounded_store_reason(error: &EngineError) -> String {
    match error {
        EngineError::Store(StoreError::MalformedEvent { .. }) => {
            "malformed event in room history".to_owned()
        }
        EngineError::Store(StoreError::MalformedRoomVersion { .. }) => {
            "malformed room version".to_owned()
        }
        other => other.to_string(),
    }
}

fn rate_delay(hint: Option<u64>, fallback: Duration) -> Duration {
    match hint {
        Some(ms) => Duration::from_millis(ms).min(Duration::from_secs(3_600)),
        None => fallback,
    }
}

fn transient_delay(min: Duration, max: Duration, attempt: u32) -> Duration {
    jittered_backoff(min, max, attempt)
}

fn jittered_backoff(min: Duration, max: Duration, attempt: u32) -> Duration {
    let shift = attempt.min(16);
    let base = min.saturating_mul(1u32 << shift).min(max);
    base + jitter(base, attempt)
}

/// Bounded deterministic jitter: up to a quarter of the base delay.
fn jitter(base: Duration, attempt: u32) -> Duration {
    let quarter = (base.as_millis() / 4) as u64;
    if quarter == 0 {
        return Duration::ZERO;
    }
    let fraction = u64::from(attempt.wrapping_mul(2_654_435_761) % 1_000);
    Duration::from_millis(quarter * fraction / 1_000)
}
