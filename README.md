# mainlineNERD

Nattering Engineers' Ramblings, Distilled. The eventual goal is passive Matrix
ingestion into SQLite. Today this repository holds only a one-shot startup
check: it is **not a collector yet**, so it ingests nothing, keeps no events and
has no backfill.

## Startup check

One run of `mainlinenerd`:

1. builds a `matrix-sdk` client with E2EE compiled out and presence Offline,
2. restores the supplied session,
3. verifies the token, user and device with `/whoami`,
4. performs exactly one initial `/sync` (one-shot; not a loop),
5. only then writes startup bookkeeping to SQLite:
   `startup_state(id, homeserver, user_id, last_successful_startup)`.

A bad token or an identity mismatch exits non-zero and writes nothing: no row
and no database file. Nothing stores a `since` cursor (a resume token without
events would be meaningless) and no messages are archived. Retries are the
`matrix-sdk` defaults; there is no custom retry or scheduler code.

"One-shot" is not "time-bounded": the `/sync` request only caps the server-side
long-poll, and SDK retries can outlast it. Wall-clock limits (readiness waits and
a job timeout) live in the fixture and CI, not in the check.

The access token is read from `MATRIX_ACCESS_TOKEN` only; it is never accepted
as an argument, logged or stored.

```sh
export MATRIX_ACCESS_TOKEN=...   # not committed
cargo run --locked -- \
  --homeserver http://127.0.0.1:8008 \
  --user @smoke:smoke.localhost \
  --device MAINLINENERDSMOKE \
  --db out/startup.db
```

## Smoke test

`scripts/local-smoke.sh` runs the real binary against a real, isolated Synapse
container: fresh config, a synthetic throwaway account, then assertions on the
success receipt, the SQLite row and Synapse's own access log (exactly one
successful initial `/sync` with `set_presence=offline` and no `since` token),
followed by checks that an invalid token and a mismatched user/device fail
without leaving a startup record. Each run gets its own private `out/smoke-*`
directory and matching container name: the container is loopback-only and
removed on exit, the run directory is removed on success and kept for debugging
on failure. Nothing pre-existing is deleted. Needs docker or podman plus `curl`,
`jq`, `python3`, `sqlite3` and `openssl`.

The pinned image is `ghcr.io/element-hq/synapse:v1.162.0` (`SYNAPSE_IMAGE`
overrides it). CI (`.github/workflows/smoke.yml`) runs the same script with
bounded readiness waits and a job timeout, on the Rust version pinned in
`rust-toolchain.toml`.
