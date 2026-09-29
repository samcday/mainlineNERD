# mainlineNERD
Nattering Engineers' Ramblings, Distilled. Passive Matrix ingestion into SQLite: follow curated rooms, backfill accessible history, then extract technical signal.

In progress: a durable event store, replay-safe pagination, and synthetic tests.
The live Matrix adapter is next; this checkout does not collect from rooms yet.

Run `cargo test`. See [the ingestion design](docs/architecture.md) for guarantees,
limitations, and the `status` / `export` commands.
