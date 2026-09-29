# mainlineNERD ingestion MVP: architecture, guarantees and limits

## Scope

Passive Matrix ingestion for an explicit room allowlist. One SQLite archive, one
writer. No event sending, no receipts, no invites, no media download, no E2EE key
import, no summarizer. Text projection only: raw events are always stored, and a
derived `current_messages` view is maintained for later LLM consumption.

The live adapter is `src/matrix.rs`, built on matrix-sdk 0.19.1 and public typed
Ruma request/response APIs (`Client::send`). The single-writer coordinator in
`src/runtime.rs` runs one long-poll `/sync` and paced `/messages` work
concurrently and applies every finished result through the store.

## Modules

| File | Responsibility |
| --- | --- |
| `src/event.rs` | Wire-envelope validation, relation/edit/redaction extraction, room-version dependent redaction pruning. No Matrix state renderer. |
| `src/store.rs` | SQLite schema, atomic batch/page commits, projection maintenance, allowlist pins, status and JSONL export. |
| `src/engine.rs` | `Transport` trait plus the request/apply primitives shared by the sequential driver and the concurrent coordinator. No transaction is held across an `.await`. |
| `src/config.rs` | Validated TOML config, homeserver URL rules, storage safety and token indirection. |
| `src/matrix.rs` | The live Matrix transport: typed `/sync`, `/messages`, `/whoami`, alias resolution, room create state and explicit joins. |
| `src/runtime.rs` | Startup binding (`initialize`) and the concurrent sync/history coordinator with pacing, backoff and rate-limit handling. |
| `src/main.rs` | `status`, `export`, `follow` (live sync only) and `run` (live sync plus history backfill). |

## Tables

- `archive_meta` — homeserver/user/device binding, set on first open.
- `sync_progress` — the opaque global `since` token, last success, failure count.
- `rooms` — room id, room version, predecessor/successor, encrypted and
  operator-action flags, plus `configured` (operator allowlist membership),
  `configured_alias` (informational metadata) and `own_membership` (our own
  last observed membership; `leave`/`ban` disables work for the room).
- `room_pins` — the canonical room id an explicit alias resolved to when it was
  first seen. A later resolution to a different room is refused, so alias drift
  cannot widen the allowlist.
- `room_history` — per-room base archival backfill cursor: opaque history token,
  `complete`, `stalled`, page count, last error. A row whose token is absent is
  honestly unseeded (not complete); a later explicit `prev_batch` may fill in a
  never-started empty row, but progress is never rewound.
- `gap_jobs` — bounded repair jobs, one per limited-sync gap: `boundary_token`
  (lower bound: the previously committed global sync token), `upper_token`
  (where repair starts: this timeline's `prev_batch`), `cursor_token` (the
  job's own durable repair cursor), status `open|repaired|unresolved`.
  `close_reason` also carries the last non-terminal error recorded against an
  open job, so `status` can attribute a transient gap failure to the gap.
- `history_visited` — durable per-work-item ledger of page tokens already
  visited (base backfill and each gap job are independent keys). It makes
  `p1 -> p2 -> p1` cycle detection survive calls and restarts; entries are
  written in the same transaction as the page they describe, so a failed,
  malformed or stale page leaves no trace.
- `events` — normalized envelope per `(room_id, event_id)`: type, sender,
  state key, origin timestamp, first/last observed timestamps, source
  (`sync|history|bundle`), raw JSON (redacted-pruned), extracted body text,
  relation metadata, redaction state.
- `redactions` — `(room, target, redaction event)` rows. Persistent deletion
  suppression.
- `current_messages` — one row per original message, with effective body,
  latest winning edit, relation/thread metadata, redacted flag and redaction
  timestamp. Edits never appear as rows of their own.

Application tables are independent of any matrix-sdk cache. The adapter uses a
memory-only SDK store (the `sqlite` feature is disabled) and never reuses SDK
store tables; our SQLite archive is the only source of truth for cursors.

## Live adapter and runtime

- The adapter is built from explicit config: a validated homeserver base URL
  (HTTPS required except for loopback hosts), the expected user/device binding
  and the room allowlist. HTTP redirects are disabled, URL credentials, query
  strings and fragments are rejected, and the config points the client at the
  configured homeserver rather than any link found in a message.
- Only public typed Ruma requests via `Client::send` are used: `/sync`,
  `/messages`, `/whoami`, `/directory/room/{alias}`, `/rooms/{id}/state/{type}`
  and an operator-requested `/join`. E2EE and automatic key forwarding are off.
  `RequestConfig::disable_retry()` and explicit timeouts give the owned
  scheduler control of pacing and error accounting.
- `/sync` is a long poll with `set_presence=offline`, a restrictive filter (only
  allowlisted rooms; no ephemeral/account data) and a small timeline limit.
  Server filters are an efficiency aid; the adapter additionally applies one
  local archival-selection rule to both sync and history: only
  `m.room.message`, `m.room.encrypted`, `m.room.redaction`, `m.room.create`,
  `m.room.encryption` and `m.room.tombstone` are archived. Rosters, power-level
  maps and other state are dropped whatever the server sends. Our own *current*
  membership is extracted as control metadata from sync; backfilled membership
  can never change it.
- Room version is established from genuine room state. If a room has no version
  yet, `initialize` fetches `m.room.create` and validates it strictly: a full
  event must really be the requested `m.room.create` state event, and a content
  object must have an absent version (v1) or an exact version this build
  recognizes. Non-object responses, non-string versions, a present non-string
  room id, wrong event types or state keys, unknown versions and mis-addressed
  events leave the room unready with a bounded reason; no guessed version is
  ever stored, and a corrected response recovers on a later start.
- For `join = true`, the explicit operator join happens *before* metadata, since
  a public-join or joined-history room may refuse state until the bot is a
  member. A successful join is not permission to ingest: only validated, ready
  rooms are admitted to the data set, and unready candidates are tracked for
  status only. A global sync token still advances for the healthy admitted
  scope while another candidate is deferred.
- The runtime owns one `Engine` and therefore one SQLite writer. It polls a
  held `/sync` future and at most one history future in a fair `select!` (no
  plane is prioritized) and yields between iterations, so an unbounded eager
  stream of ready sync batches cannot starve backfill, shutdown or other tasks.
  A held long poll never blocks backfill and an active backfill never blocks a
  live batch.
- Pending jobs stay in SQLite and the scheduler keeps only the configured room
  rotation plus a per-room cursor: each visit prefers the room's base work or
  one open gap (alternating) and falls back to the other kind within the same
  visit, so a base-only room pages back to back and a gap-only room is served
  every visit. Gap turns alternate between a bounded newest-gap opportunity and
  an ascending key-set rotation: a freshly opened gap is served within a couple
  of turns while older ids still advance, and the ascending cursor is never
  advanced by the fresh turn, so the backlog is not skipped. Memory scales with
  the explicit room set rather than the job backlog, and continuing jobs
  relinquish their turn. This bounds latency, not lossless throughput: what is
  archived still depends on server and retention limits. A removed configuration
  entry is cleared from the current allowlist and excluded from discovery and
  status even though its historical rows remain.
- History is paced globally (default one request per second) and served
  round-robin across rooms and their work. Global transport outcomes are
  classified before room eligibility: a late 401 from a room that just left
  still halts the run and a late 429 still imposes the global cooldown on every
  other room. Only data application and local work-item changes are rejected by
  room eligibility; a rejected page touches no event, cursor or ledger. A global
  cooldown is never shortened by another result (including a success or a
  shorter hint from the other plane). Transient
  failures back off with bounded exponential delay and jitter; a 429 honours the
  server's `Retry-After` header (including an HTTP date) or the Matrix retry
  hint, with an explicit conservative fallback when neither is present; a rate
  limit always halts history before another room is attempted. Empty sync
  responses are paced even when the token changes, and their checkpoint is
  still committed. A 401 stops the whole run; a 403 or not-found affects only
  that room.
- A configured room that is idle and absent from `/sync` still gets a base
  cursor: a never-started cursor is seeded from the committed global token.
  In-progress, stalled and complete work is never rewound.
- Rooms whose own membership is `leave`/`ban`, encrypted rooms and upgrade
  successors are flagged for operator action instead of being expanded. No
  automatic join ever follows an observed departure, and an explicit join uses
  the pinned canonical id with the server hints returned for an alias (or
  configured `via` hints); a join response naming another room is refused.
- Startup is isolated per room: an alias, join or metadata failure marks only
  that room unready (with a bounded status reason) and the others proceed, with
  bounded retries for transient/rate-limited failures. A final exhausted retry
  still preserves its `Retry-After`/transient pause before any later startup or
  live request. Authentication and client build failures remain global. A
  missing `/whoami` device id fails the dedicated-device binding clearly instead
  of being accepted.
- Permanent room failures stay terminal until an operator restores access. The
  explicit `run --retry-stalled` control re-enables stalled never-completed base
  work and reopens `unresolved` bounded gap repairs that still hold both a saved
  cursor and a saved lower boundary, preserving both tokens, the upper boundary,
  event data and the visited-token ledger. It is limited to currently
  configured, still-eligible rooms, commits its base/gap changes atomically and
  reports honest base and gap counts. It never joins, never clears membership/
  encryption/tombstone/readiness policy, never rewrites cursors and never
  reopens repaired/completed jobs or cursor-less/unbounded unfinished work.
- Diagnostics are stable categories plus an HTTP status. Request URLs (which
  carry pagination tokens), server error text, raw config snippets and the
  access token never reach `Display`, logs, status or stderr.
- CTRL-C drops the in-flight network futures and returns; every applied batch or
  page was already committed, so the durable checkpoint is always valid and the
  same positions are requested on reopen and applied exactly once.

## Transactional guarantees

- Schema bootstrap creates every table and records `user_version` in one
  transaction; a failure in a late statement rolls the whole bootstrap back, so
  a half-initialized archive cannot be opened later. An existing nonzero
  `user_version` that is not the current schema version is rejected before any
  DDL or DML in both writable and read-only opens: older archives get an
  actionable error that tells the operator to keep the archive and point this
  build at a new database path, or to use a compatible older build to export
  its contents. They are never reset, migrated or deleted in place, and newer
  archives are refused. Writes use `synchronous=FULL`, matching the
  committed-batch guarantee; journal mode is switched only after the schema is
  accepted.
- A `/sync` batch commits all room events, any new bounded gap repair jobs and
  the global `next_batch` token in one transaction, before the token advances.
- A `/messages` page commits its events, the next value of exactly one
  work-item cursor and that work item's visited-token entries in one
  transaction.
- Any malformed envelope aborts and rolls back the whole batch/page; neither
  events nor cursors advance. The error names the room and event.
- Replaying overlapping events is idempotent by `(room_id, event_id)`.
  Re-fetching a redaction, an already-redacted representation or a newer edit
  is applied even when a row already exists; an un-redacted replay can never
  resurrect a body.

## Cursor and gap discipline

- Progress is always an opaque token. Timestamps are metadata only and are never
  used as stream coverage or cursor ordering.
- Each room has one base archival backfill cursor. A limited sync never rewinds
  or resets it: backfill keeps walking back from the earliest known live
  position even while a gap is being repaired.
- The first sync has no previous committed token, so a limited initial snapshot
  is archival backfill, not a missing-live interval. Afterwards a limited sync
  creates a bounded repair job with its own durable cursor: `boundary_token` is
  the previously committed global sync token (lower bound), `upper_token` is
  this timeline's `prev_batch`, and `cursor_token` is where repair has reached.
  A limited sync whose `prev_batch` equals the last committed global token has
  no span: no repair job is created, but base history is still seeded if it
  never started. A limited sync with no `prev_batch` is recorded `unresolved`
  and never claims coverage. A room with completed base history can still have
  open repair jobs.
- Repair requests carry their lower bound as `to`. A response updates only the
  work item actually requested: exhaustion of one gap never completes another
  gap or the base backfill. The job closes at its boundary token or at the
  start of accessible history.
- A response is validated against the durable cursor of its work item before
  anything is stored, including the work item's eligibility: a base that is
  complete or stalled and a gap that is closed refuse a late page even when
  `expected_from` still equals their last token. Such a page is stale and
  changes no event, cursor or visited-token state, so an out-of-order in-flight
  response cannot overwrite newer progress. The engine re-checks that
  eligibility before every request, so a stalled or completed base is not
  fetched at all, and a response applied as stale is never counted as a
  committed page. Every work item resumes independently after reopen.
- An empty `/messages` page with a fresh `end` token is not completion: the
  engine keeps paging, for gap jobs as well as backfill. `end` absent means the
  start of accessible history. A page whose `end` equals its `start` is a
  stall; that work item is marked `stalled` (base) or `unresolved` (gap)
  without advancing its cursor. Successful page tokens are persisted per work
  item in `history_visited`, so a returned `end` this work item already visited
  (for example `p1 -> p2 -> p1`, even with a one-page budget or across a
  restart) stalls instead of looping, without affecting any other cursor.

## History failure isolation

- A room-local permanent failure (forbidden/not found) is recorded on that room
  and later rooms still progress; the failing room is stalled with the error
  surfaced in `status`.
- A transient failure defers only that work item: its cursor is untouched and
  it stays queued for a later run. The error is recorded against the work item
  that actually failed: base backfill uses `room_history.last_error`, while a
  gap records on `gap_jobs.close_reason` and leaves the base untouched, so
  `status` reports the failing work item.
- An authentication failure aborts the whole run; the returned error carries
  the pages already committed before the failure. A rate limit stops the run
  early: the outcome sets an explicit `rate_limited` flag (a scheduler MUST
  back off) plus an optional `retry_after_ms` hint, and no later room or gap is
  requested.
- Pages committed before a mid-work-item transport failure are not lost: the
  per-work-item counts are folded into the run outcome (or the abort error)
  whether the failure was transient, unavailable, rate-limited or
  authentication. Counts always describe committed pages, never uncommitted
  data.

## Projection rules

- Projected types: `m.room.message` and `m.room.encrypted` (placeholder with no
  body). Everything else is stored and surfaced but never projected.
- A valid edit is an `m.room.message` with `m.relates_to.rel_type = m.replace`,
  an `event_id`, and an `m.new_content` carrying at least a `msgtype` and a
  `body`. Invalid edits are stored, never projected and never become messages.
- Winning edit: latest by `(origin_server_ts, event_id)` among same-sender,
  non-redacted edits, with the event id breaking ties. A replacement is not
  compared against the original's timestamp, so a sender's clock skew does not
  discard a later replacement. A missing sender never compares equal to another
  missing sender.
- Redaction targets resolve with the maintained Ruma room-version rules: room
  version 11+ reads only `content.redacts`, earlier versions read only the
  top-level `redacts` field. A wrong-field-only or conflicting event never
  deletes the other field's target. The room version must be exact and
  recognized (`11garbage`, custom versions and absent versions are not
  prefixes or guesses); a redaction that carries a target candidate but needs
  unknown rules is an explicit normalization error that rolls the whole
  batch/page back rather than guessing a target or silently dropping the
  deletion. A genuine empty-`state_key` `m.room.create` with an absent
  `room_version` defaults to v1 per the spec; every present non-string value is
  a malformed-state error that rolls the batch back, never a coerced version
  string. The future adapter must establish the supported room version from
  room state before ingesting redactions.
- Redacted content is pruned with ruma-common's maintained redaction algorithm
  using the room version's `RedactionRules`, and the body is removed from both
  the projection and the stored raw JSON. The whole envelope is rebuilt, so
  `content`, `unsigned` aggregations (for example `m.relations` or
  `prev_content`) and extension fields cannot retain removed text.
  `unsigned.redacted_because` is reduced to a sanitized provenance marker
  without any free-form payload. An unknown or malformed room version, or a
  value canonical JSON cannot represent, fails closed to a provenance-only
  envelope with empty content instead of returning the original payload.
- Redactions and edits may arrive before their targets (backward pagination).
  Pending suppression lives in `redactions`, so a body fetched later is stored
  already redacted.
- Redacting an original suppresses the bodies of its replacement edits, whether
  the edit was already stored, arrives later during backfill, or the original
  arrives already redacted. Relation columns are kept text-free so an older
  replay is recognized and suppressed again. This is a local application
  retention rule; it does not recall remote federation copies.
- Already-redacted representations (`unsigned.redacted_because`, empty content
  for message types) are stored redacted and never projected, applying the same
  redaction rule as a direct redaction.
- A replacement cached under `unsigned.m.relations.m.replace` of a plain message
  is archived as its own event (source `bundle`, keyed by its own event id)
  through the same store/redaction path when it self-identifies consistently:
  same room context, same non-missing sender, message type, target equal to the
  enclosing event, and valid replacement content. Bundles of bundles are never
  expanded, and an invalid bundle neither alters the original nor becomes a
  separate claimed message, and a bundle may never reuse the enclosing event's
  id. Bundle rows are insert-only: a fetched `sync`/`history` event outranks a
  derived `bundle` for the canonical payload, the first bundle outranks
  conflicting replays, and a later standalone fetch replaces a bundle-only
  representation while redaction/suppression state stays monotonic. On that
  promotion the fetched payload is authoritative: derived relation/edit fields
  it does not carry are cleared rather than coalesced, and the stale edit's
  former parent is re-projected. This is canonicalization of duplicated
  transport metadata, not deletion of independent messages.
- The opaque server-generated `unsigned` block is never archived verbatim.
  Relation caches (`unsigned.m.relations`) and `prev_content` are dropped from
  every stored raw event (a validated bundled replacement is ingested
  separately), and only a sanitized `redacted_because` marker survives on a
  redacted representation.
- Room version, encrypted-room and successor flags are only taken from genuine
  state events with the expected empty `state_key`; a timeline event merely
  shaped like `m.room.create`, `m.room.encryption` or `m.room.tombstone` cannot
  change them. An isolated `m.room.encrypted` timeline event is surfaced as an
  opaque encrypted event and never enables room-wide E2EE.

## Privacy and passivity

- No credentials are stored in the archive; only opaque sync/history tokens in
  the progress tables. `status` prints neither tokens nor bodies. The access
  token is read from the configured environment variable at startup and is
  wrapped so it can never appear in a `Debug` value, log line or panic message;
  the config file stores only the variable name.
- `initialize` validates `/whoami` against the configured user and device before
  any room is registered or any batch applied. A missing device id is rejected
  because this dedicated-device pilot cannot verify that binding.
  The client is configured from the explicit homeserver and redirects are
  disabled, so it never follows an untrusted link or credential redirect.
- New archives and exports live under an owner-private data directory. An
  existing data directory that is group/world accessible is refused with the
  required `chmod 700` rather than silently widened; a missing one is created
  `0700`. The archive must be a regular file, never a symlink.
- Redaction removes bodies from the raw JSON that is kept, so exports cannot
  resurrect them. Encrypted rooms and room-upgrade successors are flagged for
  operator action instead of being expanded automatically.
- Raw export is the archived event representation, not a byte-for-byte transport
  dump. Opaque `unsigned` caches are canonicalized out or ingested as their own
  bundled events, so removing a replacement also removes every archived copy of
  its text, including a stale one replayed inside an enclosing original.
- `export --out` never truncates: the destination is opened with `create_new`
  and is refused when it already exists as a file, symlink or hardlink path
  (including the archive itself). A newly created export is owner-only (`0600`)
  on Unix; `--out` is optional and stdout remains supported.

## Known limits

- The adapter is read-only apart from an explicit operator-requested join of a
  configured room. No messages, receipts, presence, typing, invites, discovery
  crawl, media download, profile/roster archive or key requests are ever sent.
- No E2EE: encrypted rooms are flagged and skipped, and no key import or
  plaintext assumption is made. There is no room-upgrade history stitching and
  no automatic follow of a successor.
- Backfill has no arbitrary age cap but is paced; it archives what the server
  makes accessible and does not claim global Matrix coverage or recall of
  external copies.
- Edit validity is structural (`m.new_content.msgtype` and `.body` present)
  rather than a full ruma re-deserialization; rich HTML sanitization is out of
  scope.
- Room version is learned from a genuine empty-`state_key` `m.room.create`
  state event; an absent version means v1, and a present non-string value is a
  malformed-state error that rolls the batch back (it is never coerced into a
  possibly recognized version). Redaction uses ruma-common's exact
  `RoomVersionRules`; an unknown or malformed version fails closed for
  redacted-payload pruning (a provenance-only envelope) and is an explicit
  error when a redaction target must be resolved.
- Stalled rooms are not retried automatically; an operator must clear the
  stall after fixing access (documented, surfaced in `status`).
- Only forward pagination gaps (limited sync) are modeled; no room-upgrade
  history stitching.
- Only one level of `unsigned.m.relations.m.replace` bundles is recovered.
  Deeper or vendor-specific bundles are discarded rather than expanded, and a
  bundle that does not self-identify consistently is ignored.
