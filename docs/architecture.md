# mainlineNERD ingestion MVP: architecture, guarantees and limits

## Scope

Passive Matrix ingestion for an explicit room allowlist. One SQLite archive, one
writer. No event sending, no receipts, no invites, no media download, no E2EE key
import, no summarizer. Text projection only: raw events are always stored, and a
derived `current_messages` view is maintained for later LLM consumption.

The real Matrix adapter is a separate task. In this checkpoint the engine's
`Transport` trait is implemented only by the test fake, so the runtime algorithm
(transaction boundaries, paging stalls, gap repair) is exercised without
credentials or network.

## Modules

| File | Responsibility |
| --- | --- |
| `src/event.rs` | Wire-envelope validation, relation/edit/redaction extraction, room-version dependent redaction pruning. No Matrix state renderer. |
| `src/store.rs` | SQLite schema, atomic batch/page commits, projection maintenance, status and JSONL export. |
| `src/engine.rs` | `Transport` trait, sync polling, paced history pagination, error classification. No transaction is held across an `.await`. |
| `src/main.rs` | `status`, `export`; `run`/`follow` currently fail with an explicit "adapter not implemented" error. |

## Tables

- `archive_meta` — homeserver/user/device binding, set on first open.
- `sync_progress` — the opaque global `since` token, last success, failure count.
- `rooms` — room id, room version, predecessor/successor, encrypted and
  operator-action flags.
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

Application tables are independent of any matrix-sdk cache; the adapter task must
not reuse SDK store tables.

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
  the progress tables. `status` prints neither tokens nor bodies.
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

## Known limits (this checkpoint)

- No live adapter: no real `/sync`, `/messages`, alias resolution, session
  handling or retry scheduler. The engine exposes retry hints
  (`TransportError::RateLimited { retry_after_ms }`) for the adapter driver.
- JSONL export is available; no config file/CLI identity plumbing yet. The
  adapter task will add `config.example.toml` and session binding at startup.
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
