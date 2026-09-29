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
  `complete`, `stalled`, page count, last error.
- `gap_jobs` — bounded repair jobs, one per limited-sync gap: `boundary_token`
  (lower bound: the previously committed global sync token), `upper_token`
  (where repair starts: this timeline's `prev_batch`), `cursor_token` (the
  job's own durable repair cursor), status `open|repaired|unresolved`.
- `events` — normalized envelope per `(room_id, event_id)`: type, sender,
  state key, origin timestamp, first/last observed timestamps, source
  (`sync|history`), raw JSON (redacted-pruned), extracted body text, relation
  metadata, redaction state.
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
  a half-initialized archive cannot be opened later. Writes use
  `synchronous=FULL`, matching the committed-batch guarantee.
- A `/sync` batch commits all room events, any new bounded gap repair jobs and
  the global `next_batch` token in one transaction, before the token advances.
- A `/messages` page commits its events plus the next value of exactly one
  work-item cursor in one transaction.
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
  A limited sync with no `prev_batch` is recorded `unresolved` and never claims
  coverage. A room with completed base history can still have open repair jobs.
- Repair requests carry their lower bound as `to`. A response updates only the
  work item actually requested: exhaustion of one gap never completes another
  gap or the base backfill. The job closes at its boundary token or at the
  start of accessible history.
- A response is validated against the durable cursor of its work item before
  anything is stored. A page whose cursor has already moved is stale and
  changes nothing, so an out-of-order in-flight response cannot overwrite newer
  progress. Every work item resumes independently after reopen.
- An empty `/messages` page with a fresh `end` token is not completion: the
  engine keeps paging, for gap jobs as well as backfill. `end` absent means the
  start of accessible history. A page whose `end` equals its `start` is a
  stall; that work item is marked `stalled` (base) or `unresolved` (gap)
  without advancing its cursor. The engine remembers visited tokens per work
  item, so a `p1 -> p2 -> p1` cycle stops instead of looping.

## History failure isolation

- A room-local permanent failure (forbidden/not found) is recorded on that room
  and later rooms still progress; the failing room is stalled with the error
  surfaced in `status`.
- A transient failure defers only that work item: its cursor is untouched and
  it stays queued for a later run.
- An authentication failure stops the whole run; a rate limit stops the run
  early with a partial outcome and a retry hint instead of hammering remaining
  rooms.

## Projection rules

- Projected types: `m.room.message` and `m.room.encrypted` (placeholder with no
  body). Everything else is stored and surfaced but never projected.
- A valid edit is an `m.room.message` with `m.relates_to.rel_type = m.replace`,
  an `event_id`, and an `m.new_content.body`. Invalid edits are stored, never
  projected and never become messages.
- Winning edit: latest by `(origin_server_ts, event_id)` among same-sender,
  non-redacted edits strictly newer than the original.
- Redaction targets resolve with room-version rules: `content.redacts` for room
  version 11+, the top-level `redacts` field before that, with a fallback when
  only the other field is present.
- Redacted content is pruned per the room-version redaction rules (for example
  `m.room.member` keeps `membership`, `m.room.create` keeps its content only in
  v11+), and the body is removed from both the projection and the stored raw
  JSON. Redacting an edit falls back to the previous edit or the original body.
- Redactions and edits may arrive before their targets (backward pagination).
  Pending suppression lives in `redactions`, so a body fetched later is stored
  already redacted.
- Already-redacted representations (`unsigned.redacted_because`, empty content
  for message types) are stored redacted and never projected.

## Privacy and passivity

- No credentials are stored in the archive; only opaque sync/history tokens in
  the progress tables. `status` prints neither tokens nor bodies.
- Redaction removes bodies from the raw JSON that is kept, so exports cannot
  resurrect them. Encrypted rooms and room-upgrade successors are flagged for
  operator action instead of being expanded automatically.

## Known limits (this checkpoint)

- No live adapter: no real `/sync`, `/messages`, alias resolution, session
  handling or retry scheduler. The engine exposes retry hints
  (`TransportError::RateLimited { retry_after_ms }`) for the adapter driver.
- JSONL export is available; no config file/CLI identity plumbing yet. The
  adapter task will add `config.example.toml` and session binding at startup.
- Edit validity is structural (`m.new_content.body` present) rather than a full
  ruma re-deserialization; rich HTML sanitization is out of scope.
- Room version is learned from `m.room.create`; unknown versions fall back to
  pre-v11 redaction rules with a documented `content.redacts` preference.
- Stalled rooms are not retried automatically; an operator must clear the
  stall after fixing access (documented, surfaced in `status`).
- Only forward pagination gaps (limited sync) are modeled; no room-upgrade
  history stitching.
