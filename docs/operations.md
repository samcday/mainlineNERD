# mainlineNERD operations

This document covers configuration, the CLI, and what the live runtime does and
does not do. See [architecture.md](architecture.md) for the data model and
guarantees.

## Configuration

Copy `config.example.toml` and edit it. No secret belongs in the file: the bot
access token is read from the environment variable named by `token_env`
(`MLN_ACCESS_TOKEN` by default).

| Key | Meaning |
| --- | --- |
| `homeserver` | Explicit base URL. HTTPS is required; plain HTTP is accepted only for loopback hosts (`127.0.0.1`, `::1`, `localhost`). Credentials, query strings and fragments are rejected. |
| `user_id` | The exact account the token must belong to. |
| `device_id` | The expected device binding. Device ids are opaque: the value is preserved exactly as written and compared exactly (no trimming) against `/whoami`; a missing or different device is rejected. The value is never echoed in errors. |
| `token_env` | Environment variable holding the token. |
| `data_dir` | Owner-private directory for the archive. Created `0700` if absent; an existing group/world-accessible directory is refused. |
| `database` | Relative to `data_dir` unless absolute. |
| `history_interval_ms` | Global pacing between history requests (default 1000). |
| `history_limit` | `/messages` page size (1-1000). |
| `sync_timeout_ms` | Long-poll timeout requested from `/sync`. |
| `[[rooms]]` | Exactly one of `id` or `alias`, plus optional `join = true`. |

Aliases are case-sensitive identifiers and are resolved once and pinned to their
canonical room id; a later resolution to a different room is refused. Alias
entries retain the routing hints the homeserver returns, and exact room ids can
set optional `via` hints for a cold homeserver. `join = true` performs an
explicit join of that configured room only when the archive has never observed a
membership for it; a recorded leave or ban is never auto-reversed. A join that
returns another room id is refused.

For a room with `join = true`, the explicit join is performed before metadata is
fetched, because a public-join or joined-history room may refuse state until the
bot is a member. Within a run, only validated, ready rooms are admitted to the
data set (sync filtering and the runtime's admission predicate); unready
candidates are tracked in `status` but their data is never archived. A room is
*ready* only when its `m.room.create` metadata validates to an exact supported
room version. A non-object response, a non-string or unknown version,
a wrong event type or state key, or a mis-addressed event leaves the room
unready with a bounded reason in `status`; nothing is collected for it and a
corrected response recovers on a later start. Startup failures are isolated per
room with bounded retries for transient/rate-limited errors; authentication and
client-build failures stop the whole run.

## Commands

```sh
# Live sync only.
MLN_ACCESS_TOKEN=... mln-ingest --config config.toml follow

# Live sync plus paced history backfill.
MLN_ACCESS_TOKEN=... mln-ingest --config config.toml run

# One-shot recovery after access/membership was restored outside this tool:
# re-enable stalled base work and reopen unresolved bounded gaps (saved cursor
# and lower bound preserved) for currently configured, still-eligible rooms, at
# their saved cursors. It never joins, clears membership/policy flags, rewrites
# cursors or reopens repaired/completed or cursor-less unfinished work, and it
# reports honest base/gap counts.
MLN_ACCESS_TOKEN=... mln-ingest --config config.toml run --retry-stalled

# Offline: neither needs a token and neither touches the network.
mln-ingest --db archive.sqlite3 status
mln-ingest --db archive.sqlite3 status --json
mln-ingest --db archive.sqlite3 export --kind messages --out messages.jsonl
mln-ingest --db archive.sqlite3 export --kind events
```

`--config` (or `MLN_CONFIG`) is required for `follow`/`run`; `--db` (or
`MLN_DB`) is used for `status`/`export`. `export --out` refuses to overwrite an
existing file, symlink or hardlink and creates new files owner-only (`0600`).

## Startup sequence

1. Load and validate the config; check that `data_dir` is safe.
2. Read the token from the named environment variable.
3. Build the client (memory-only SDK store, E2EE and key forwarding disabled,
   redirects disabled) and fetch the server's supported versions.
4. Open or create the archive, bound to the configured homeserver/user/device.
5. `/whoami` must match both the configured user and device.
6. Register the allowlist, resolve and pin aliases, perform explicit joins, and
   learn room versions from `m.room.create` where the archive lacks one.
7. Start the coordinator.

Any failure in steps 1-5 refuses the run before a single event is ingested.

## Runtime behaviour

- One long-poll `/sync` and at most one `/messages` request are in flight; a
  single writer applies each finished result and commits it immediately. A held
  sync never blocks backfill and active backfill never blocks a new live batch.
- History is paced globally (default one request per second) and served
  round-robin across rooms and gap jobs. Pending jobs stay in SQLite and only a
  small bounded ready set is materialized, so a busy base backfill does not
  starve a newly opened gap or a newly seeded room.
- A committed global sync token seeds a never-started base cursor for an idle
  configured room in the same transaction that commits the token, so history
  starts in that run even if the room never appears in `/sync`.
- Transient failures back off with bounded exponential delay and jitter. A 429
  honours `Retry-After` (seconds or HTTP date) or the Matrix retry hint, and
  falls back to a conservative delay when neither is present; it always halts
  history before another room is attempted.
- A 401 stops the whole run; a 403 or not-found affects only that room, which is
  stalled in `status` until an operator acts.
- An observed own `leave`/`ban`, an encrypted room, or a room with a known
  successor is flagged and skipped; no work is scheduled and no join is retried.
- CTRL-C stops the run after the last committed batch or page; the durable
  checkpoint is always valid, and reopening resumes from it.

`status` reports the last successful sync, per-room base progress, open and
unresolved gaps, membership/inactive/encrypted/upgraded flags and bounded,
sanitized errors. It never prints tokens or message bodies, and it makes no
promise of globally complete Matrix coverage or recall of external copies.

## Testing

`cargo test` runs the synthetic suite: store/engine regressions, config and
storage-validation tests, adapter tests against a loopback mock homeserver
exercising the real matrix-sdk boundary, and runtime tests that use a
controllable fake transport under Tokio paused time (held futures, pacing,
backoff, failure isolation, shutdown). No test contacts a real Matrix server.
