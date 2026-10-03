# Startup check

The eventual goal of mainlineNERD is passive Matrix ingestion into SQLite.
Today this repository holds only a one-shot startup check: it is **not a
collector yet**, so it keeps no events and has no backfill.

## Behaviour

One run of `mainlinenerd`:

1. builds a `matrix-sdk` client with E2EE compiled out and presence Offline,
2. restores the supplied session,
3. verifies the token, user and device with `/whoami`,
4. performs one initial `/sync` operation (not a loop),
5. only then writes startup bookkeeping to SQLite:
   `startup_state(id, homeserver, user_id, last_successful_startup)`.

A bad token or identity mismatch exits non-zero without writing a startup
record or creating a database at a fresh path. Nothing stores a `since` cursor
(a resume token without events would be meaningless) and no messages are archived.
Retries are the SDK defaults; there is no custom retry or scheduler code.

"One-shot" is not "time-bounded": the `/sync` request only caps the server-side
long-poll, and SDK retries can outlast it. CI provides the outer wall-clock limit.

The access token comes from `MATRIX_ACCESS_TOKEN` only, never an argument or
database field. Remote homeserver URLs require HTTPS; plain HTTP is accepted
only for `localhost` or loopback IP addresses. Hostless URLs and embedded
credentials are rejected. The client ignores ambient proxies and does not
follow redirects, preserving the selected transport.

```sh
export MATRIX_ACCESS_TOKEN=...   # not committed
cargo run --locked -- \
  --homeserver http://127.0.0.1:8008 \
  --user @smoke:smoke.localhost \
  --device MAINLINENERDSMOKE \
  --db target/startup.db
```

## Real-Synapse smoke test

`cargo xtask smoke` runs the real binary against a real, isolated Synapse with
a synthetic throwaway account. It checks:

- the successful receipt, verified identity and UTC timestamp in our SQLite;
- exactly one successful initial `/sync` in Synapse's access log, with
  `set_presence=offline` and no `since` token;
- invalid-token, wrong-user and wrong-device rejection without a database or sync;
- successful startup despite unusable proxy settings, with proxy bypasses cleared.

We query our SQLite database, not Synapse's internal schema. The server-side
proof comes from its HTTP access log. The Rust fixture uses HTTP, JSON and
SQLite libraries directly; it needs Docker or Podman and the standard Unix `id`
utility, not `curl`, `jq`, `python3`, `sqlite3` or `openssl`.

Each run owns a private `target/smoke-*` directory and a matching loopback-only
container. Normal completion or a reported error removes that container. State
is removed on success and retained on failure for diagnosis. Nothing
pre-existing is deleted. Forced termination can prevent cleanup; the runner
prints the container name and keeps the state path in its error output.

The image version is pinned once in `xtask/src/main.rs`. Options:

- `SYNAPSE_IMAGE`: trial a different image without changing the default pin.
- `CONTAINER_RUNTIME`: select `docker` or `podman`.
- `SMOKE_READY_TIMEOUT`: readiness wait in seconds (90 locally, 120 in CI).
- `SMOKE_PORT`: optionally choose the published loopback port.
- `BIN`: substitute a binary for regression experiments.

CI runs the same xtask with a 22-minute smoke-step limit and a 30-minute job
limit. Rust is pinned in `rust-toolchain.toml` and the workflow.

## Development

```sh
cargo fmt --all --check
cargo test --locked --workspace
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo xtask smoke
```

For a Synapse upgrade, change the xtask's image pin and run the real smoke test.
Preserve the sync, identity and success-only bookkeeping assertions when
adapting to an upstream change; a build-only check is not sufficient.
