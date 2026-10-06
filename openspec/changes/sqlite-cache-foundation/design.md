# Design

## Context

There is no Rust implementation to preserve. See `proposal.md` for the outcome
and `specs/durable-cache/spec.md` for the acceptance contract. This change creates
only the storage boundary that the local Synapse change will exercise.

## Goals / Non-Goals

Use one database and a small explicit API. Avoid an event graph, Matrix state
resolution, an append-only version archive, or an abstract ingestion framework.
The store does not infer public visibility from a payload or account identity.

## Decisions

Use `rusqlite` with bundled SQLite and transactions. It gives a small synchronous
API without a service or async pool. SQLx is viable, but its async integration is
not needed to prove storage. A later web server can keep blocking database work
off its async executor.

Keep one current record per room/event key. Store JSON payloads without flattening
formatted text or relation references. Keep page order in a separate reference
table. Page keys include the room and request identity. Keep continuation tokens
opaque, and never infer timeline order solely from event timestamps.

Commit records, page references, and progress together. Use SQLite foreign keys,
WAL journaling, and full synchronous commits. Keep schema version checks ahead of
initialization writes. An unsupported schema is an error, not permission to clear
the cache.

Keep last-success metadata separate from the latest failure status. Do not store
credentials, raw HTTP errors, or source-response logs in these records.
The next change owns network error classification and public eligibility.

Use a tombstone at the stable event key for removal. Readers resolve page entries
through the event table, so they cannot serve an obsolete page-body copy. A
tombstone wins over later replay. This is logical removal, not secure disk erasure.

Use Rust integration tests named for the `CACHE-*` scenarios. A test child process
signals transaction boundaries before the parent kills it. This is more reliable
than timing a crash with sleeps. Plain Rust tests avoid a second step-definition
layer while these scenarios only exercise a library.

## Risks / Trade-offs

Process-crash tests do not prove survival of failing storage hardware. State that
limit in the test report. Backup, restore, and future schema migrations need
separate work before production use.

The cache can preserve rich JSON, but that does not make it safe HTML or public
data. No public serving or Matrix networking belongs in this change.

Do not benchmark against an invented production data volume. The scenario counts
are small correctness fixtures. Report test duration and database size as
measurements, not service-level guarantees.
