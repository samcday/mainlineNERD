# mainlineNERD
Nattering Engineers' Ramblings, Distilled. Passive Matrix ingestion into SQLite: follow curated rooms, backfill accessible history, then extract technical signal.

`follow` runs live sync only. `run` also backfills accessible history for the
explicit configured room allowlist. `status` and `export` work offline.

```sh
cp config.example.toml config.toml   # edit homeserver, user/device, rooms
export MLN_ACCESS_TOKEN=...          # never put the token in the config
cargo run -- --config config.toml run
```

The adapter uses matrix-sdk 0.19.1 typed Ruma requests. It is read-only except
for an explicit operator-requested join of a configured room: no messages,
receipts, presence, typing, invites, E2EE, media or discovery. Setup, commands
and behaviour are in [docs/operations.md](docs/operations.md); the data model
and guarantees are in [docs/architecture.md](docs/architecture.md).

Run `cargo test` (offline with `--offline`); no test contacts a real Matrix
server.
