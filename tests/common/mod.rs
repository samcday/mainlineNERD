//! Shared fixtures: a scripted fake transport, a controllable transport with
//! holdable futures for the concurrency tests, and a loopback Matrix HTTP mock
//! for the real SDK boundary.
#![allow(dead_code)]

use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use axum::extract::{Path as AxumPath, Query, Request, State};
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::{get, post};
use axum::Router;
use mainlinenerd_ingest::engine::{HistoryRequest, SyncRequest, Transport, TransportError};
use mainlinenerd_ingest::event::{HistoryPage, SyncBatch, SyncRoomUpdate};
use mainlinenerd_ingest::store::{ArchiveIdentity, Store};
use rusqlite::Connection;
use serde_json::{json, Value};
use tokio::sync::{watch, Notify};

pub const ROOM: &str = "!room:hs.example.org";
pub const ROOM2: &str = "!room2:hs.example.org";
pub const ROOM3: &str = "!room3:hs.example.org";
pub const ALICE: &str = "@alice:hs.example.org";
pub const BOB: &str = "@bob:hs.example.org";

pub fn identity() -> ArchiveIdentity {
    ArchiveIdentity {
        homeserver: "https://hs.example.org".to_owned(),
        user_id: "@ingest:hs.example.org".to_owned(),
        device_id: "MLN".to_owned(),
    }
}

pub fn open_store(path: &Path) -> Store {
    Store::open(path, &identity()).expect("open store")
}

pub fn db(path: &Path) -> Connection {
    Connection::open(path).expect("open sqlite directly")
}

pub fn scalar_i64(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |row| row.get(0)).expect("scalar")
}

pub fn scalar_string(conn: &Connection, sql: &str) -> Option<String> {
    conn.query_row(sql, [], |row| row.get(0)).expect("scalar")
}

// ---------------------------------------------------------------------------
// Event fixtures
// ---------------------------------------------------------------------------

pub fn message(id: &str, ts: i64, body: &str) -> Value {
    message_from(id, ts, body, ALICE)
}

pub fn message_from(id: &str, ts: i64, body: &str, sender: &str) -> Value {
    json!({
        "type": "m.room.message",
        "event_id": id,
        "sender": sender,
        "origin_server_ts": ts,
        "content": { "msgtype": "m.text", "body": body }
    })
}

pub fn edit(id: &str, ts: i64, target: &str, new_body: &str) -> Value {
    json!({
        "type": "m.room.message",
        "event_id": id,
        "sender": ALICE,
        "origin_server_ts": ts,
        "content": {
            "msgtype": "m.text",
            "body": format!("* {new_body}"),
            "m.new_content": { "msgtype": "m.text", "body": new_body },
            "m.relates_to": { "rel_type": "m.replace", "event_id": target }
        }
    })
}

pub fn edit_from(id: &str, ts: i64, target: &str, new_body: &str, sender: &str) -> Value {
    let mut value = edit(id, ts, target, new_body);
    value["sender"] = json!(sender);
    value
}

pub fn invalid_edit(id: &str, ts: i64, target: &str) -> Value {
    json!({
        "type": "m.room.message",
        "event_id": id,
        "sender": ALICE,
        "origin_server_ts": ts,
        "content": {
            "msgtype": "m.text",
            "body": "* broken",
            "m.relates_to": { "rel_type": "m.replace", "event_id": target }
        }
    })
}

pub fn redaction(id: &str, ts: i64, target: &str, v11: bool) -> Value {
    if v11 {
        json!({
            "type": "m.room.redaction",
            "event_id": id,
            "sender": ALICE,
            "origin_server_ts": ts,
            "content": { "redacts": target }
        })
    } else {
        json!({
            "type": "m.room.redaction",
            "event_id": id,
            "sender": ALICE,
            "origin_server_ts": ts,
            "redacts": target,
            "content": {}
        })
    }
}

/// A plain message carrying a valid replacement under
/// `unsigned.m.relations.m.replace`. The bundle is transport metadata, not a
/// separately fetched event.
pub fn message_with_bundled_edit(
    id: &str,
    ts: i64,
    body: &str,
    edit_id: &str,
    edit_ts: i64,
    new_body: &str,
) -> Value {
    json!({
        "type": "m.room.message",
        "event_id": id,
        "sender": ALICE,
        "origin_server_ts": ts,
        "content": { "msgtype": "m.text", "body": body },
        "unsigned": {
            "m.relations": {
                "m.replace": {
                    "type": "m.room.message",
                    "event_id": edit_id,
                    "sender": ALICE,
                    "origin_server_ts": edit_ts,
                    "content": {
                        "msgtype": "m.text",
                        "body": format!("* {new_body}"),
                        "m.new_content": { "msgtype": "m.text", "body": new_body },
                        "m.relates_to": { "rel_type": "m.replace", "event_id": id }
                    }
                }
            }
        }
    })
}

pub fn already_redacted_message(id: &str, ts: i64) -> Value {
    json!({
        "type": "m.room.message",
        "event_id": id,
        "sender": ALICE,
        "origin_server_ts": ts,
        "unsigned": { "redacted_because": { "type": "m.room.redaction", "event_id": "$red" } },
        "content": {}
    })
}

pub fn create_room(version: &str) -> Value {
    json!({
        "type": "m.room.create",
        "event_id": "$create",
        "sender": ALICE,
        "state_key": "",
        "origin_server_ts": 1,
        "content": { "creator": ALICE, "room_version": version }
    })
}

pub fn encryption_event() -> Value {
    json!({
        "type": "m.room.encryption",
        "event_id": "$enc",
        "sender": ALICE,
        "state_key": "",
        "origin_server_ts": 2,
        "content": { "algorithm": "m.megolm.v1.aes-sha2" }
    })
}

pub fn tombstone(successor: &str) -> Value {
    json!({
        "type": "m.room.tombstone",
        "event_id": "$tomb",
        "sender": ALICE,
        "state_key": "",
        "origin_server_ts": 3,
        "content": { "body": "upgraded", "replacement_room": successor }
    })
}

pub fn member(id: &str, ts: i64) -> Value {
    json!({
        "type": "m.room.member",
        "event_id": id,
        "sender": ALICE,
        "state_key": ALICE,
        "origin_server_ts": ts,
        "content": { "membership": "join", "displayname": "Alice" }
    })
}

// ---------------------------------------------------------------------------
// Transport fixtures
// ---------------------------------------------------------------------------

pub fn room_update(room_id: &str) -> SyncRoomUpdate {
    SyncRoomUpdate {
        room_id: room_id.to_owned(),
        ..Default::default()
    }
}

pub fn sync_batch(next_batch: &str, rooms: Vec<SyncRoomUpdate>) -> SyncBatch {
    SyncBatch {
        next_batch: next_batch.to_owned(),
        rooms,
    }
}

pub fn history_page(start: &str, end: Option<&str>, chunk: Vec<Value>) -> HistoryPage {
    HistoryPage {
        start: start.to_owned(),
        end: end.map(str::to_owned),
        chunk,
    }
}

/// Scripted transport: `/sync` responses come from one queue, `/messages`
/// responses from per-`from`-token queues. Every request is recorded.
pub struct FakeTransport {
    sync: Mutex<VecDeque<Result<SyncBatch, TransportError>>>,
    history: Mutex<HashMap<String, VecDeque<Result<HistoryPage, TransportError>>>>,
    pub sync_requests: Mutex<Vec<SyncRequest>>,
    pub history_requests: Mutex<Vec<HistoryRequest>>,
}

impl FakeTransport {
    pub fn new() -> Self {
        Self {
            sync: Mutex::new(VecDeque::new()),
            history: Mutex::new(HashMap::new()),
            sync_requests: Mutex::new(Vec::new()),
            history_requests: Mutex::new(Vec::new()),
        }
    }

    pub fn push_sync(&self, batch: SyncBatch) {
        self.sync.lock().unwrap().push_back(Ok(batch));
    }

    pub fn push_sync_error(&self, error: TransportError) {
        self.sync.lock().unwrap().push_back(Err(error));
    }

    pub fn push_history(&self, from: &str, page: HistoryPage) {
        self.history
            .lock()
            .unwrap()
            .entry(from.to_owned())
            .or_default()
            .push_back(Ok(page));
    }

    pub fn push_history_error(&self, from: &str, error: TransportError) {
        self.history
            .lock()
            .unwrap()
            .entry(from.to_owned())
            .or_default()
            .push_back(Err(error));
    }

    pub fn history_call_count(&self) -> usize {
        self.history_requests.lock().unwrap().len()
    }
}

#[async_trait]
impl Transport for FakeTransport {
    async fn sync(&self, request: SyncRequest) -> Result<SyncBatch, TransportError> {
        self.sync_requests.lock().unwrap().push(request);
        self.sync.lock().unwrap().pop_front().unwrap_or_else(|| {
            Err(TransportError::Transient(
                "no scripted /sync response".to_owned(),
            ))
        })
    }

    async fn history(&self, request: HistoryRequest) -> Result<HistoryPage, TransportError> {
        self.history_requests.lock().unwrap().push(request.clone());
        let mut map = self.history.lock().unwrap();
        let queue = map.get_mut(&request.from).ok_or_else(|| {
            TransportError::Transient(format!(
                "no scripted /messages response for {}",
                request.from
            ))
        })?;
        queue.pop_front().unwrap_or_else(|| {
            Err(TransportError::Transient(format!(
                "scripted /messages queue exhausted for {}",
                request.from
            )))
        })
    }
}

// ---------------------------------------------------------------------------
// Controllable transport: futures can be held open on demand, so the runtime's
// concurrency and pacing can be observed deterministically under paused time.
// ---------------------------------------------------------------------------

#[derive(Default)]
struct ControlInner {
    sync: Mutex<VecDeque<Result<SyncBatch, TransportError>>>,
    history: Mutex<HashMap<String, VecDeque<Result<HistoryPage, TransportError>>>>,
    sync_requests: Mutex<Vec<SyncRequest>>,
    sync_request_times: Mutex<Vec<tokio::time::Instant>>,
    history_requests: Mutex<Vec<HistoryRequest>>,
    history_request_times: Mutex<Vec<tokio::time::Instant>>,
    hold_sync: AtomicBool,
    hold_history: AtomicBool,
    sync_gate: Notify,
    history_gate: Notify,
    sync_seen: watch::Sender<usize>,
    history_seen: watch::Sender<usize>,
}

/// A fake transport whose network futures can be held until released.
#[derive(Clone)]
pub struct ControllableTransport {
    inner: Arc<ControlInner>,
}

impl Default for ControllableTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl ControllableTransport {
    pub fn new() -> Self {
        let (sync_seen, _) = watch::channel(0);
        let (history_seen, _) = watch::channel(0);
        Self {
            inner: Arc::new(ControlInner {
                sync_seen,
                history_seen,
                ..Default::default()
            }),
        }
    }

    pub fn push_sync(&self, batch: SyncBatch) {
        self.inner.sync.lock().unwrap().push_back(Ok(batch));
    }

    pub fn push_sync_error(&self, error: TransportError) {
        self.inner.sync.lock().unwrap().push_back(Err(error));
    }

    pub fn push_history(&self, from: &str, page: HistoryPage) {
        self.inner
            .history
            .lock()
            .unwrap()
            .entry(from.to_owned())
            .or_default()
            .push_back(Ok(page));
    }

    pub fn push_history_error(&self, from: &str, error: TransportError) {
        self.inner
            .history
            .lock()
            .unwrap()
            .entry(from.to_owned())
            .or_default()
            .push_back(Err(error));
    }

    pub fn hold_sync(&self, hold: bool) {
        self.inner.hold_sync.store(hold, Ordering::SeqCst);
        if !hold {
            // notify_one stores a permit, so a release that races the waiter
            // still wakes it.
            self.inner.sync_gate.notify_one();
        }
    }

    pub fn hold_history(&self, hold: bool) {
        self.inner.hold_history.store(hold, Ordering::SeqCst);
        if !hold {
            self.inner.history_gate.notify_one();
        }
    }

    pub fn history_call_count(&self) -> usize {
        self.inner.history_requests.lock().unwrap().len()
    }

    pub fn sync_call_count(&self) -> usize {
        self.inner.sync_requests.lock().unwrap().len()
    }

    pub fn history_requests(&self) -> Vec<HistoryRequest> {
        self.inner.history_requests.lock().unwrap().clone()
    }

    /// Virtual timestamps of every history request, in order.
    pub fn history_request_times(&self) -> Vec<tokio::time::Instant> {
        self.inner.history_request_times.lock().unwrap().clone()
    }

    /// Virtual timestamps of every sync request, in order.
    pub fn sync_request_times(&self) -> Vec<tokio::time::Instant> {
        self.inner.sync_request_times.lock().unwrap().clone()
    }

    pub async fn wait_for_history(&self, count: usize) {
        let mut rx = self.inner.history_seen.subscribe();
        while *rx.borrow_and_update() < count {
            let _ = rx.changed().await;
        }
    }

    pub async fn wait_for_syncs(&self, count: usize) {
        let mut rx = self.inner.sync_seen.subscribe();
        while *rx.borrow_and_update() < count {
            let _ = rx.changed().await;
        }
    }
}

#[async_trait]
impl Transport for ControllableTransport {
    async fn sync(&self, request: SyncRequest) -> Result<SyncBatch, TransportError> {
        self.inner
            .sync_request_times
            .lock()
            .unwrap()
            .push(tokio::time::Instant::now());
        self.inner.sync_requests.lock().unwrap().push(request);
        self.inner.sync_seen.send_modify(|n| *n += 1);
        // Deliver a scripted response first; an empty queue optionally parks
        // the request so a test can inspect state before releasing it.
        if let Some(response) = self.inner.sync.lock().unwrap().pop_front() {
            return response;
        }
        while self.inner.hold_sync.load(Ordering::SeqCst) {
            self.inner.sync_gate.notified().await;
        }
        self.inner
            .sync
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| {
                Err(TransportError::Transient(
                    "no scripted /sync response".to_owned(),
                ))
            })
    }

    async fn history(&self, request: HistoryRequest) -> Result<HistoryPage, TransportError> {
        self.inner
            .history_request_times
            .lock()
            .unwrap()
            .push(tokio::time::Instant::now());
        self.inner
            .history_requests
            .lock()
            .unwrap()
            .push(request.clone());
        self.inner.history_seen.send_modify(|n| *n += 1);
        while self.inner.hold_history.load(Ordering::SeqCst) {
            self.inner.history_gate.notified().await;
        }
        let mut map = self.inner.history.lock().unwrap();
        let queue = map.get_mut(&request.from).ok_or_else(|| {
            TransportError::Transient(format!(
                "no scripted /messages response for {}",
                request.from
            ))
        })?;
        queue.pop_front().unwrap_or_else(|| {
            Err(TransportError::Transient(format!(
                "scripted /messages queue exhausted for {}",
                request.from
            )))
        })
    }
}

// ---------------------------------------------------------------------------
// Loopback Matrix HTTP mock for the real SDK boundary.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct MockRequest {
    pub method: String,
    pub path: String,
    pub query: String,
    /// Wall-clock instant the request was received; used to assert pacing.
    pub at: std::time::Instant,
}

impl MockRequest {
    pub fn path_with_query(&self) -> String {
        if self.query.is_empty() {
            self.path.clone()
        } else {
            format!("{}?{}", self.path, self.query)
        }
    }
}

#[derive(Debug, Clone)]
pub struct MockResponse {
    pub status: u16,
    pub body: Value,
    pub headers: Vec<(String, String)>,
}

impl MockResponse {
    pub fn json(body: Value) -> Self {
        Self {
            status: 200,
            body,
            headers: Vec::new(),
        }
    }

    pub fn status(status: u16, body: Value) -> Self {
        Self {
            status,
            body,
            headers: Vec::new(),
        }
    }

    pub fn matrix_error(status: u16, errcode: &str, message: &str) -> Self {
        Self::status(status, json!({ "errcode": errcode, "error": message }))
    }

    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_owned(), value.to_owned()));
        self
    }
}

#[derive(Default)]
pub struct MockState {
    pub requests: Mutex<Vec<MockRequest>>,
    pub whoami: Mutex<Option<Value>>,
    pub versions: Mutex<Option<Value>>,
    pub sync: Mutex<VecDeque<MockResponse>>,
    pub messages: Mutex<HashMap<String, VecDeque<MockResponse>>>,
    pub aliases: Mutex<HashMap<String, String>>,
    pub create_state: Mutex<HashMap<String, VecDeque<MockResponse>>>,
    pub alias_servers: Mutex<HashMap<String, Vec<String>>>,
    /// Repro F50: when set for an alias, the directory lookup answers with this
    /// response instead (e.g. a 502 when the alias' server is unreachable).
    pub alias_failures: Mutex<HashMap<String, MockResponse>>,
    pub join_result: Mutex<Option<String>>,
    pub join_response: Mutex<Option<MockResponse>>,
    pub join_requires_via: AtomicBool,
    /// When set, `m.room.create` state is refused until a join has happened, as
    /// a public-join/joined-history room may behave.
    pub metadata_requires_join: AtomicBool,
    pub joined: AtomicBool,
    pub hold_sync: AtomicBool,
    pub sync_gate: Notify,
    pub sync_seen: AtomicUsize,
    /// Extra current-state content served at `/rooms/{room}/state/{type}/`
    /// (empty state key), keyed by (room, type). Missing entries are 404.
    pub room_state: Mutex<HashMap<(String, String), Value>>,
}

impl MockState {
    pub fn set_room_state(&self, room_id: &str, event_type: &str, content: Value) {
        self.room_state
            .lock()
            .unwrap()
            .insert((room_id.to_owned(), event_type.to_owned()), content);
    }

    pub fn push_sync(&self, response: MockResponse) {
        self.sync.lock().unwrap().push_back(response);
    }

    pub fn push_messages(&self, from: &str, response: MockResponse) {
        self.messages
            .lock()
            .unwrap()
            .entry(from.to_owned())
            .or_default()
            .push_back(response);
    }

    pub fn set_alias(&self, alias: &str, room_id: &str) {
        self.set_alias_with_servers(alias, room_id, &[]);
    }

    pub fn set_alias_with_servers(&self, alias: &str, room_id: &str, servers: &[&str]) {
        self.aliases
            .lock()
            .unwrap()
            .insert(alias.to_owned(), room_id.to_owned());
        self.alias_servers.lock().unwrap().insert(
            alias.to_owned(),
            servers.iter().map(|server| (*server).to_owned()).collect(),
        );
    }

    pub fn set_create_state(&self, room_id: &str, response: MockResponse) {
        let mut queues = self.create_state.lock().unwrap();
        let queue = queues.entry(room_id.to_owned()).or_default();
        queue.clear();
        queue.push_back(response);
    }

    pub fn push_create_state(&self, room_id: &str, response: MockResponse) {
        self.create_state
            .lock()
            .unwrap()
            .entry(room_id.to_owned())
            .or_default()
            .push_back(response);
    }

    pub fn recorded(&self) -> Vec<MockRequest> {
        self.requests.lock().unwrap().clone()
    }

    pub fn hold_sync(&self, hold: bool) {
        self.hold_sync.store(hold, Ordering::SeqCst);
        if !hold {
            self.sync_gate.notify_one();
        }
    }

    pub async fn wait_for_syncs(&self, count: usize) {
        while self.sync_seen.load(Ordering::SeqCst) < count {
            let _ = tokio::time::timeout(
                std::time::Duration::from_millis(50),
                self.sync_gate.notified(),
            )
            .await;
        }
    }
}

/// A running loopback mock homeserver.
pub struct MockServer {
    pub base_url: String,
    pub state: Arc<MockState>,
    handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl MockServer {
    pub async fn start() -> Self {
        let state = Arc::new(MockState::default());
        let app = mock_router(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock server");
        let addr = listener.local_addr().expect("mock server address");
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self {
            base_url: format!("http://{addr}"),
            state,
            handle: Mutex::new(Some(handle)),
        }
    }

    /// Stop serving so a test can observe a network failure. The listener is
    /// dropped when the serve task is aborted.
    pub fn stop(&self) {
        if let Some(handle) = self.handle.lock().unwrap().take() {
            handle.abort();
        }
    }
}

fn mock_router(state: Arc<MockState>) -> Router {
    Router::new()
        .route("/_matrix/client/versions", get(mock_versions))
        .route("/_matrix/client/v3/account/whoami", get(mock_whoami))
        .route("/_matrix/client/v3/sync", get(mock_sync))
        .route(
            "/_matrix/client/v3/rooms/{room}/messages",
            get(mock_messages),
        )
        .route("/_matrix/client/v3/directory/room/{alias}", get(mock_alias))
        .route("/_matrix/client/v3/join/{room}", post(mock_join))
        .route(
            "/_matrix/client/v3/rooms/{room}/state/m.room.create",
            get(mock_create_state),
        )
        // An empty state key makes ruma emit a trailing slash; real
        // homeservers accept both forms.
        .route(
            "/_matrix/client/v3/rooms/{room}/state/m.room.create/",
            get(mock_create_state),
        )
        .route(
            "/_matrix/client/v3/rooms/{room}/state/{event_type}",
            get(mock_room_state),
        )
        .route(
            "/_matrix/client/v3/rooms/{room}/state/{event_type}/",
            get(mock_room_state),
        )
        .route(
            "/_matrix/client/v3/rooms/{room}/state",
            get(mock_full_state),
        )
        .layer(middleware::from_fn_with_state(
            state.clone(),
            record_request,
        ))
        .with_state(state)
}

async fn record_request(
    State(state): State<Arc<MockState>>,
    request: Request,
    next: Next,
) -> Response {
    state.requests.lock().unwrap().push(MockRequest {
        method: request.method().to_string(),
        path: request.uri().path().to_owned(),
        query: request.uri().query().unwrap_or("").to_owned(),
        at: std::time::Instant::now(),
    });
    next.run(request).await
}

fn render(response: MockResponse) -> Response {
    let mut builder = Response::builder()
        .status(StatusCode::from_u16(response.status).unwrap_or(StatusCode::OK))
        .header("content-type", "application/json");
    for (name, value) in &response.headers {
        builder = builder.header(name.as_str(), value.as_str());
    }
    builder
        .body(Body::from(response.body.to_string()))
        .unwrap_or_else(|_| Response::new(Body::from("{}")))
}

async fn mock_versions(State(state): State<Arc<MockState>>) -> Response {
    let body = state
        .versions
        .lock()
        .unwrap()
        .clone()
        .unwrap_or_else(|| json!({ "versions": ["v1.11"] }));
    render(MockResponse::json(body))
}

async fn mock_whoami(State(state): State<Arc<MockState>>) -> Response {
    let body = state
        .whoami
        .lock()
        .unwrap()
        .clone()
        .unwrap_or_else(|| json!({ "user_id": "@ingest:hs.example.org", "device_id": "MLN" }));
    render(MockResponse::json(body))
}

async fn mock_sync(State(state): State<Arc<MockState>>) -> Response {
    state.sync_seen.fetch_add(1, Ordering::SeqCst);
    state.sync_gate.notify_waiters();
    if let Some(response) = state.sync.lock().unwrap().pop_front() {
        return render(response);
    }
    // Nothing scripted: optionally park the request so a test can inspect the
    // committed checkpoint before releasing it with a final response.
    while state.hold_sync.load(Ordering::SeqCst) {
        state.sync_gate.notified().await;
    }
    let response = state
        .sync
        .lock()
        .unwrap()
        .pop_front()
        .unwrap_or_else(|| MockResponse::json(json!({ "next_batch": "empty", "rooms": {} })));
    render(response)
}

async fn mock_messages(
    State(state): State<Arc<MockState>>,
    AxumPath(room): AxumPath<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let from = params.get("from").cloned().unwrap_or_default();
    let response = state
        .messages
        .lock()
        .unwrap()
        .get_mut(&from)
        .and_then(|queue| queue.pop_front());
    match response {
        Some(response) => render(response),
        None => render(MockResponse::matrix_error(
            404,
            "M_NOT_FOUND",
            &format!("no scripted messages for {room} from {from}"),
        )),
    }
}

async fn mock_alias(
    State(state): State<Arc<MockState>>,
    AxumPath(alias): AxumPath<String>,
) -> Response {
    if let Some(failure) = state.alias_failures.lock().unwrap().get(&alias).cloned() {
        return render(failure);
    }
    let room_id = state.aliases.lock().unwrap().get(&alias).cloned();
    let servers = state
        .alias_servers
        .lock()
        .unwrap()
        .get(&alias)
        .cloned()
        .unwrap_or_default();
    match room_id {
        Some(room_id) => render(MockResponse::json(
            json!({ "room_id": room_id, "servers": servers }),
        )),
        None => render(MockResponse::matrix_error(
            404,
            "M_NOT_FOUND",
            "alias not found",
        )),
    }
}

async fn mock_join(
    State(state): State<Arc<MockState>>,
    AxumPath(room): AxumPath<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    // An unhinted join is rejected by this cold mock, exercising `via`.
    if state.join_requires_via.load(Ordering::SeqCst)
        && params
            .get("via")
            .map(String::as_str)
            .unwrap_or("")
            .is_empty()
    {
        return render(MockResponse::matrix_error(
            403,
            "M_FORBIDDEN",
            "cold homeserver needs a via hint",
        ));
    }
    if let Some(response) = state.join_response.lock().unwrap().clone() {
        return render(response);
    }
    state.joined.store(true, Ordering::SeqCst);
    let room_id = state.join_result.lock().unwrap().clone().unwrap_or(room);
    render(MockResponse::json(json!({ "room_id": room_id })))
}

async fn mock_room_state(
    State(state): State<Arc<MockState>>,
    AxumPath((room, event_type)): AxumPath<(String, String)>,
) -> Response {
    match state
        .room_state
        .lock()
        .unwrap()
        .get(&(room, event_type))
        .cloned()
    {
        Some(content) => render(MockResponse::json(content)),
        None => render(MockResponse::matrix_error(404, "M_NOT_FOUND", "no such state")),
    }
}

async fn mock_full_state(
    State(state): State<Arc<MockState>>,
    AxumPath(room): AxumPath<String>,
) -> Response {
    let events: Vec<Value> = state
        .room_state
        .lock()
        .unwrap()
        .iter()
        .filter(|((r, _), _)| r == &room)
        .map(|((_, ty), content)| {
            json!({
                "type": ty,
                "event_id": format!("$state-{ty}"),
                "sender": ALICE,
                "state_key": "",
                "origin_server_ts": 2,
                "content": content
            })
        })
        .collect();
    render(MockResponse::json(Value::Array(events)))
}

async fn mock_create_state(
    State(state): State<Arc<MockState>>,
    AxumPath(room): AxumPath<String>,
) -> Response {
    if state.metadata_requires_join.load(Ordering::SeqCst) && !state.joined.load(Ordering::SeqCst) {
        return render(MockResponse::matrix_error(403, "M_FORBIDDEN", "join first"));
    }
    let mut queues = state.create_state.lock().unwrap();
    let response = queues
        .get_mut(&room)
        .and_then(|queue| queue.pop_front())
        .unwrap_or_else(|| MockResponse::json(json!({ "room_version": "11" })));
    render(response)
}
