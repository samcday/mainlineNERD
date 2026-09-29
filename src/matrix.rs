//! Live Matrix transport built on matrix-sdk 0.19 and public typed Ruma
//! requests.
//!
//! Only public SDK request/response types are used (`Client::send`), the SDK's
//! persistent caches and E2EE machinery are disabled, and all state that
//! matters is handed to our own SQLite store. The adapter never sends room
//! messages, receipts, presence, typing or invites; it only reads `/sync`,
//! `/messages`, `/whoami`, alias resolution, room create state and an explicit
//! operator-requested join.
//!
//! Diagnostics never include URLs, pagination tokens, server payloads or raw
//! config text: errors are stable categories plus an HTTP status.

use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use js_int::UInt;
use matrix_sdk::authentication::matrix::MatrixSession;
use matrix_sdk::config::RequestConfig;
use matrix_sdk::ruma::api::client::account::whoami;
use matrix_sdk::ruma::api::client::alias::get_alias;
use matrix_sdk::ruma::api::client::filter::{
    Filter as DataFilter, FilterDefinition, LazyLoadOptions, RoomEventFilter, RoomFilter,
};
use matrix_sdk::ruma::api::client::membership::join_room_by_id_or_alias;
use matrix_sdk::ruma::api::client::message::get_message_events;
use matrix_sdk::ruma::api::client::state::get_state_event_for_key;
use matrix_sdk::ruma::api::client::sync::sync_events;
use matrix_sdk::ruma::api::error::{ErrorKind, RetryAfter};
use matrix_sdk::ruma::events::{
    AnySyncStateEvent, AnySyncTimelineEvent, AnyTimelineEvent, StateEventType,
};
use matrix_sdk::ruma::presence::PresenceState;
use matrix_sdk::ruma::serde::Raw;
use matrix_sdk::ruma::{
    OwnedDeviceId, OwnedRoomAliasId, OwnedRoomId, OwnedRoomOrAliasId, OwnedServerName, OwnedUserId,
    RoomAliasId, RoomId, RoomVersionId, UserId,
};
use matrix_sdk::{Client, HttpError, SessionMeta, SessionTokens};
use serde_json::Value;

use crate::config::{Config, Token};
use crate::engine::{HistoryRequest, SyncRequest, Transport, TransportError};
use crate::event::{self, HistoryPage, SyncBatch, SyncRoomUpdate};

/// Startup failures that happen before any archive work is scheduled. The
/// variants mirror the steady-state transport classification so a startup 429
/// or 5xx is retried/deferred instead of being reported as a permanent error.
#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    #[error("building the Matrix client failed")]
    Build,
    #[error("authentication rejected by the homeserver ({0})")]
    Authentication(String),
    #[error("room not found or inaccessible ({0})")]
    RoomUnavailable(String),
    #[error("temporarily unavailable ({0})")]
    Transient(String),
    #[error("rate limited (retry_after_ms={retry_after_ms:?})")]
    RateLimited { retry_after_ms: Option<u64> },
    #[error("invalid homeserver response: {0}")]
    Protocol(&'static str),
    #[error(transparent)]
    Transport(#[from] TransportError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WhoAmI {
    pub user_id: OwnedUserId,
    pub device_id: Option<OwnedDeviceId>,
}

/// A resolved alias plus the server routing hints the homeserver returned.
#[derive(Debug, Clone)]
pub struct ResolvedAlias {
    pub room_id: OwnedRoomId,
    pub servers: Vec<OwnedServerName>,
}

/// Validated `m.room.create` metadata.
#[derive(Debug, Clone)]
pub enum RoomCreateMetadata {
    /// A genuine, validated `m.room.create` state event. It is applied through
    /// the normal event path; no id is invented.
    StateEvent(Value),
    /// A validated content-only response: an explicit control-metadata path,
    /// never a fabricated create event. The version is already validated and
    /// is never a guessed default.
    VersionOnly(String),
}

#[derive(Debug, Default)]
struct Allowlist {
    rooms: RwLock<HashSet<OwnedRoomId>>,
}

impl Allowlist {
    fn insert(&self, room_id: OwnedRoomId) {
        self.rooms.write().unwrap().insert(room_id);
    }

    fn contains(&self, room_id: &RoomId) -> bool {
        self.rooms.read().unwrap().contains(room_id)
    }
}

/// The live Matrix transport. Cheap to clone; every clone shares the SDK client
/// and the allowlist.
#[derive(Clone)]
pub struct MatrixTransport {
    client: Client,
    user_id: OwnedUserId,
    allow: Arc<Allowlist>,
    default_request_config: RequestConfig,
    sync_request_config: RequestConfig,
}

impl MatrixTransport {
    /// Build a client from explicit config values and restore the supplied
    /// token. No E2EE, no persistent SDK cache and no well-known lookup.
    pub async fn connect(config: &Config, token: &Token) -> Result<Self, AdapterError> {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .user_agent("mainlinenerd-ingest/0.1")
            .build()
            .map_err(|_| AdapterError::Build)?;

        let default_request_config = RequestConfig::new()
            .disable_retry()
            .timeout(Duration::from_secs(30));
        let sync_request_config =
            RequestConfig::new()
                .disable_retry()
                .timeout(Duration::from_millis(
                    config.sync_timeout_ms.saturating_add(10_000),
                ));

        let client = Client::builder()
            .homeserver_url(&config.homeserver)
            .disable_well_known_lookup(true)
            .request_config(default_request_config)
            .http_client(http)
            .build()
            .await
            .map_err(|_| AdapterError::Build)?;

        let user_id =
            OwnedUserId::try_from(config.user_id.as_str()).map_err(|_| AdapterError::Build)?;
        let session = MatrixSession {
            meta: SessionMeta {
                user_id: user_id.clone(),
                device_id: OwnedDeviceId::from(config.device_id.as_str()),
            },
            tokens: SessionTokens {
                access_token: token.as_str().to_owned(),
                refresh_token: None,
            },
        };
        client
            .matrix_auth()
            .restore_session(session, matrix_sdk::store::RoomLoadSettings::default())
            .await
            .map_err(|_| AdapterError::Build)?;

        // Discover the server's supported versions once so that version-aware
        // typed path builders never issue a surprising request mid-run.
        client
            .fetch_server_versions(Some(default_request_config))
            .await
            .map_err(adapter_error)?;

        Ok(Self {
            client,
            user_id,
            allow: Arc::new(Allowlist::default()),
            default_request_config,
            sync_request_config,
        })
    }

    /// Configured user id the client is authenticated as.
    pub fn user_id(&self) -> &UserId {
        &self.user_id
    }

    /// Add a resolved, operator-configured room to the local allowlist.
    pub fn allow_room(&self, room_id: OwnedRoomId) {
        self.allow.insert(room_id);
    }

    pub fn is_allowed(&self, room_id: &RoomId) -> bool {
        self.allow.contains(room_id)
    }

    /// `/account/whoami`: the identity and (when reported) the server-side
    /// device binding. A missing device id is surfaced as `None`; the caller
    /// decides whether the binding can be verified.
    pub async fn whoami(&self) -> Result<WhoAmI, AdapterError> {
        let response = self
            .client
            .send(whoami::v3::Request::new())
            .with_request_config(Some(self.default_request_config))
            .await
            .map_err(adapter_error)?;
        Ok(WhoAmI {
            user_id: response.user_id,
            device_id: response.device_id,
        })
    }

    /// Resolve an explicit alias to its canonical room id and routing hints.
    pub async fn resolve_alias(&self, alias: &RoomAliasId) -> Result<ResolvedAlias, AdapterError> {
        let response = self
            .client
            .send(get_alias::v3::Request::new(alias.to_owned()))
            .with_request_config(Some(self.default_request_config))
            .await
            .map_err(adapter_error)?;
        Ok(ResolvedAlias {
            room_id: response.room_id,
            servers: response.servers,
        })
    }

    /// Explicitly join one operator-configured room by its pinned canonical id,
    /// with optional routing hints. This is the only write the adapter can
    /// perform, and it is never called after an observed departure. The
    /// response must name the expected room; an unexpected id is refused.
    pub async fn join_room(
        &self,
        room_id: &RoomId,
        via: &[OwnedServerName],
        expected: &RoomId,
    ) -> Result<OwnedRoomId, AdapterError> {
        let room_or_alias = OwnedRoomOrAliasId::try_from(room_id.as_str())
            .map_err(|_| AdapterError::Protocol("room id is not joinable"))?;
        let mut request = join_room_by_id_or_alias::v3::Request::new(room_or_alias);
        request.via = via.to_vec();
        let response = self
            .client
            .send(request)
            .with_request_config(Some(self.default_request_config))
            .await
            .map_err(adapter_error)?;
        if response.room_id != expected {
            return Err(AdapterError::Protocol(
                "join returned an unexpected room id",
            ));
        }
        Ok(response.room_id)
    }

    /// Fetch the room's `m.room.create` state as a validated full event when
    /// the server honours `format=event`, or as validated content-only
    /// metadata otherwise. Malformed, unsupported or mis-addressed responses
    /// are refused rather than turned into a guessed version.
    pub async fn room_create_metadata(
        &self,
        room_id: &RoomId,
    ) -> Result<RoomCreateMetadata, AdapterError> {
        let mut request = get_state_event_for_key::v3::Request::new(
            room_id.to_owned(),
            StateEventType::RoomCreate,
            String::new(),
        );
        request.format = get_state_event_for_key::v3::StateEventFormat::Event;
        let response = self
            .client
            .send(request)
            .with_request_config(Some(self.default_request_config))
            .await
            .map_err(adapter_error)?;
        let value: Value = serde_json::from_str(response.event_or_content.get())
            .map_err(|_| AdapterError::Protocol("room create metadata is not valid JSON"))?;
        classify_create_metadata(room_id, value)
    }

    fn sync_filter(&self) -> FilterDefinition {
        let rooms: Vec<OwnedRoomId> = self.allow.rooms.read().unwrap().iter().cloned().collect();
        let mut filter = FilterDefinition::default();
        // No presence or account-data publication/collection is requested.
        filter.presence = DataFilter::ignore_all();
        filter.account_data = DataFilter::ignore_all();

        let mut room = RoomFilter::default();
        // A server-side narrowing aid only; `is_allowed` and the local
        // selection rule are the real boundary.
        room.rooms = Some(rooms);
        room.include_leave = true;

        let mut timeline = RoomEventFilter::default();
        timeline.limit = Some(UInt::from(20u8));
        room.timeline = timeline;

        let mut state = RoomEventFilter::default();
        // Lazy loading keeps the server from sending a full roster; our own
        // membership is still extracted as control metadata and dropped.
        state.lazy_load_options = LazyLoadOptions::Enabled {
            include_redundant_members: false,
        };
        room.state = state;

        room.ephemeral = RoomEventFilter::ignore_all();
        room.account_data = RoomEventFilter::ignore_all();
        filter.room = room;
        filter
    }
}

#[async_trait]
impl Transport for MatrixTransport {
    async fn sync(&self, request: SyncRequest) -> Result<SyncBatch, TransportError> {
        let mut sync_request = sync_events::v3::Request::new();
        sync_request.since = request.since;
        sync_request.timeout = Some(Duration::from_millis(request.timeout_ms));
        // Never mark the bot online as a side effect of polling.
        sync_request.set_presence = PresenceState::Offline;
        sync_request.filter = Some(sync_events::v3::Filter::FilterDefinition(
            self.sync_filter(),
        ));

        let response = self
            .client
            .send(sync_request)
            .with_request_config(Some(self.sync_request_config))
            .await
            .map_err(|error| api_error_to_transport(error, Scope::Sync))?;

        let mut rooms = Vec::new();
        for (room_id, joined) in &response.rooms.join {
            if !self.is_allowed(room_id) {
                continue;
            }
            rooms.push(self.joined_room(room_id, joined));
        }
        for (room_id, left) in &response.rooms.leave {
            if !self.is_allowed(room_id) {
                continue;
            }
            rooms.push(self.left_room(room_id, left));
        }
        // Invites and knocks are ignored: no invites are auto-accepted and no
        // discovery crawl is performed.
        Ok(SyncBatch {
            next_batch: response.next_batch,
            rooms,
        })
    }

    fn is_allowed_room(&self, room_id: &str) -> bool {
        RoomId::parse(room_id)
            .map(|room_id| self.is_allowed(&room_id))
            .unwrap_or(false)
    }

    async fn history(&self, request: HistoryRequest) -> Result<HistoryPage, TransportError> {
        let room_id = RoomId::parse(request.room_id.as_str()).map_err(|_| {
            TransportError::Fatal("history request has an invalid room id".to_owned())
        })?;
        let mut message_request = get_message_events::v3::Request::backward(room_id.to_owned());
        message_request.from = Some(request.from.clone());
        message_request.to = request.to.clone();
        message_request.limit = UInt::from(request.limit.min(u32::from(u16::MAX)) as u16);
        // Efficiency aid only: member events are also dropped by the local
        // selection rule below.
        let mut filter = RoomEventFilter::default();
        filter.not_types = vec![event::ROOM_MEMBER.to_owned()];
        message_request.filter = filter;

        let response = self
            .client
            .send(message_request)
            .with_request_config(Some(self.default_request_config))
            .await
            .map_err(|error| api_error_to_transport(error, Scope::History))?;

        // Backfilled membership or power-level events must never change current
        // state or enter the archive, whatever the server returns.
        let raw: Vec<Value> = response
            .chunk
            .iter()
            .map(raw_event_value::<AnyTimelineEvent>)
            .collect();
        let mut ignored = None;
        Ok(HistoryPage {
            start: response.start,
            end: response.end,
            chunk: select_events(raw, None, &mut ignored),
        })
    }
}

impl MatrixTransport {
    fn joined_room(
        &self,
        room_id: &OwnedRoomId,
        room: &sync_events::v3::JoinedRoom,
    ) -> SyncRoomUpdate {
        let state_events: Vec<Raw<AnySyncStateEvent>> = match &room.state {
            sync_events::v3::State::Before(state) | sync_events::v3::State::After(state) => {
                state.events.clone()
            }
            _ => Vec::new(),
        };
        let mut own_membership = None;
        let state = select_events(
            state_events
                .iter()
                .map(raw_event_value::<AnySyncStateEvent>)
                .collect(),
            Some(self.user_id.as_str()),
            &mut own_membership,
        );
        let timeline = select_events(
            room.timeline
                .events
                .iter()
                .map(raw_event_value::<AnySyncTimelineEvent>)
                .collect(),
            Some(self.user_id.as_str()),
            &mut own_membership,
        );
        SyncRoomUpdate {
            room_id: room_id.to_string(),
            timeline,
            state,
            prev_batch: room.timeline.prev_batch.clone(),
            limited: room.timeline.limited,
            own_membership,
        }
    }

    fn left_room(&self, room_id: &OwnedRoomId, room: &sync_events::v3::LeftRoom) -> SyncRoomUpdate {
        let state_events: Vec<Raw<AnySyncStateEvent>> = match &room.state {
            sync_events::v3::State::Before(state) | sync_events::v3::State::After(state) => {
                state.events.clone()
            }
            _ => Vec::new(),
        };
        let mut own_membership = None;
        let state = select_events(
            state_events
                .iter()
                .map(raw_event_value::<AnySyncStateEvent>)
                .collect(),
            Some(self.user_id.as_str()),
            &mut own_membership,
        );
        let timeline = select_events(
            room.timeline
                .events
                .iter()
                .map(raw_event_value::<AnySyncTimelineEvent>)
                .collect(),
            Some(self.user_id.as_str()),
            &mut own_membership,
        );
        SyncRoomUpdate {
            room_id: room_id.to_string(),
            timeline,
            state,
            prev_batch: None,
            limited: false,
            own_membership: own_membership.or_else(|| Some("leave".to_owned())),
        }
    }
}

fn raw_event_value<T>(raw: &Raw<T>) -> Value {
    serde_json::to_value(raw).unwrap_or(Value::Null)
}

/// The event types the identity-minimized pilot archives. Everything else
/// (member rosters, power-level maps, names, topics, join rules, ...) is
/// dropped locally so a server that ignores the request filter cannot widen the
/// archive. Room policy and version are derived from create/encryption/
/// tombstone state; messages, redactions and opaque encrypted events carry the
/// technical signal.
fn is_archived_event_type(event_type: &str) -> bool {
    matches!(
        event_type,
        event::MESSAGE
            | event::ENCRYPTED
            | event::REDACTION
            | event::ROOM_CREATE
            | event::ROOM_ENCRYPTION
            | event::ROOM_TOMBSTONE
    )
}

/// Apply the local archival-selection rule. When `own` is `Some`, our own last
/// membership in a genuine member state event is recorded as control metadata
/// before the event is dropped. When `own` is `None` (backfill) membership is
/// only dropped and can never change current membership.
fn select_events(
    values: Vec<Value>,
    own: Option<&str>,
    membership: &mut Option<String>,
) -> Vec<Value> {
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        let Some(event_type) = value.get("type").and_then(Value::as_str) else {
            continue;
        };
        if event_type == event::ROOM_MEMBER {
            if let Some(own) = own {
                if value.get("state_key").and_then(Value::as_str) == Some(own) {
                    if let Some(observed) = value
                        .get("content")
                        .and_then(Value::as_object)
                        .and_then(|content| content.get("membership"))
                        .and_then(Value::as_str)
                    {
                        *membership = Some(observed.to_owned());
                    }
                }
            }
            continue;
        }
        if !is_archived_event_type(event_type) {
            continue;
        }
        out.push(value);
    }
    out
}

fn classify_create_metadata(
    room_id: &RoomId,
    value: Value,
) -> Result<RoomCreateMetadata, AdapterError> {
    let object = value.as_object().ok_or(AdapterError::Protocol(
        "room create metadata is not an object",
    ))?;

    let looks_like_event = object.contains_key("type") || object.contains_key("event_id");
    if looks_like_event {
        if object.get("type").and_then(Value::as_str) != Some(event::ROOM_CREATE) {
            return Err(AdapterError::Protocol(
                "metadata is not an m.room.create state event",
            ));
        }
        if object.get("state_key").and_then(Value::as_str) != Some("") {
            return Err(AdapterError::Protocol(
                "m.room.create metadata has the wrong state key",
            ));
        }
        if object
            .get("event_id")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
        {
            return Err(AdapterError::Protocol(
                "m.room.create metadata has no event id",
            ));
        }
        // A present room_id must be a matching string. A null, number, array
        // or object is a wrong type, not an absent field, and is refused
        // without echoing the malformed value.
        if let Some(room) = object.get("room_id") {
            match room.as_str() {
                Some(room) if room == room_id.as_str() => {}
                Some(_) => {
                    return Err(AdapterError::Protocol(
                        "m.room.create metadata is for another room",
                    ));
                }
                None => {
                    return Err(AdapterError::Protocol(
                        "m.room.create metadata has a non-string room id",
                    ));
                }
            }
        }
        let content =
            object
                .get("content")
                .and_then(Value::as_object)
                .ok_or(AdapterError::Protocol(
                    "m.room.create metadata has no content",
                ))?;
        validate_room_version(content.get("room_version"))?;
        return Ok(RoomCreateMetadata::StateEvent(value));
    }

    // Content-only: no event identity is present, so never invent one.
    let version = validate_room_version(object.get("room_version"))?;
    Ok(RoomCreateMetadata::VersionOnly(version))
}

/// Validate an `m.room_version` value. An absent value is room version 1 per
/// the spec; anything present must be a string naming a version this build
/// recognizes. Non-string values, unknown custom versions and malformed
/// strings are refused, never coerced.
fn validate_room_version(value: Option<&Value>) -> Result<String, AdapterError> {
    match value {
        None => Ok("1".to_owned()),
        Some(value) => {
            let version = value
                .as_str()
                .ok_or(AdapterError::Protocol("room version must be a string"))?;
            let id: RoomVersionId = version
                .parse()
                .map_err(|_| AdapterError::Protocol("unsupported room version"))?;
            if id.rules().is_none() {
                return Err(AdapterError::Protocol("unsupported room version"));
            }
            Ok(version.to_owned())
        }
    }
}

/// Whether an error should be treated as room-scoped or run-scoped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scope {
    Sync,
    History,
}

fn api_error_to_transport(error: HttpError, scope: Scope) -> TransportError {
    if let Some(api) = error.as_client_api_error() {
        let status = api.status_code;
        if let Some(ErrorKind::LimitExceeded(data)) = api.error_kind() {
            return TransportError::RateLimited {
                retry_after_ms: data.retry_after.as_ref().map(retry_after_ms),
            };
        }
        if matches!(api.error_kind(), Some(ErrorKind::UnknownToken(_))) || status.as_u16() == 401 {
            return TransportError::Authentication("401 unauthorized".to_owned());
        }
        return transport_from_status(status.as_u16(), scope);
    }
    if error.as_uiaa_response().is_some() {
        return TransportError::Authentication("interactive auth required".to_owned());
    }
    match error {
        HttpError::Reqwest(error) => TransportError::Transient(reqwest_category(&error)),
        HttpError::IntoHttp(_) => {
            TransportError::Fatal("could not construct the request".to_owned())
        }
        _ => TransportError::Transient("homeserver returned an unexpected error".to_owned()),
    }
}

fn transport_from_status(status: u16, scope: Scope) -> TransportError {
    match status {
        401 => TransportError::Authentication("401 unauthorized".to_owned()),
        403 | 404 => match scope {
            Scope::History => TransportError::RoomUnavailable(format!("{status}")),
            Scope::Sync => TransportError::Fatal(format!("sync failed with {status}")),
        },
        // A bad pagination token or range is room-local: it stalls this work
        // item instead of stopping every other room.
        400 | 422 if scope == Scope::History => {
            TransportError::RoomUnavailable(format!("{status}"))
        }
        429 => TransportError::RateLimited {
            retry_after_ms: None,
        },
        500..=599 => TransportError::Transient(format!("server error {status}")),
        _ => TransportError::Fatal(format!("unexpected status {status}")),
    }
}

/// A bounded category for a reqwest failure. The `reqwest::Error` Display can
/// contain the full request URL (including pagination tokens) and the upstream
/// message, so it is never stringified.
fn reqwest_category(error: &reqwest::Error) -> String {
    if error.is_timeout() {
        "request timed out".to_owned()
    } else if error.is_connect() {
        "connection failed".to_owned()
    } else if error.is_request() {
        "request failed".to_owned()
    } else if error.is_body() || error.is_decode() {
        "invalid response body".to_owned()
    } else {
        "network error".to_owned()
    }
}

fn retry_after_ms(retry_after: &RetryAfter) -> u64 {
    match retry_after {
        RetryAfter::Delay(delay) => delay.as_millis() as u64,
        RetryAfter::DateTime(time) => time
            .duration_since(SystemTime::now())
            .map(|delay| delay.as_millis() as u64)
            .unwrap_or(0),
    }
}

fn adapter_error(error: HttpError) -> AdapterError {
    if let Some(api) = error.as_client_api_error() {
        let status = api.status_code;
        if let Some(ErrorKind::LimitExceeded(data)) = api.error_kind() {
            return AdapterError::RateLimited {
                retry_after_ms: data.retry_after.as_ref().map(retry_after_ms),
            };
        }
        if matches!(api.error_kind(), Some(ErrorKind::UnknownToken(_))) || status.as_u16() == 401 {
            return AdapterError::Authentication("401 unauthorized".to_owned());
        }
        return match status.as_u16() {
            403 | 404 => AdapterError::RoomUnavailable(status.to_string()),
            429 => AdapterError::RateLimited {
                retry_after_ms: None,
            },
            500..=599 => AdapterError::Transient(format!("server error {status}")),
            _ => AdapterError::Protocol("unexpected homeserver response"),
        };
    }
    if error.as_uiaa_response().is_some() {
        return AdapterError::Authentication("interactive auth required".to_owned());
    }
    match error {
        HttpError::Reqwest(error) => AdapterError::Transient(reqwest_category(&error)),
        HttpError::IntoHttp(_) => AdapterError::Protocol("could not construct the request"),
        _ => AdapterError::Transient("homeserver returned an unexpected error".to_owned()),
    }
}

/// Parse an operator-configured alias string, rejecting invalid ones without
/// echoing the input.
pub fn parse_alias(alias: &str) -> Result<OwnedRoomAliasId, AdapterError> {
    OwnedRoomAliasId::try_from(alias)
        .map_err(|_| AdapterError::Protocol("invalid room alias in the allowlist"))
}

/// Parse an operator-configured room id string.
pub fn parse_room_id(room_id: &str) -> Result<OwnedRoomId, AdapterError> {
    OwnedRoomId::try_from(room_id)
        .map_err(|_| AdapterError::Protocol("invalid room id in the allowlist"))
}

/// Parse an operator-configured server name for join routing hints.
pub fn parse_server_name(server: &str) -> Result<OwnedServerName, AdapterError> {
    OwnedServerName::try_from(server)
        .map_err(|_| AdapterError::Protocol("invalid room via hint in the allowlist"))
}
