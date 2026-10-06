# Proposal

## Why

As an operator, I need cached public history to survive process restarts without
asking the upstream for the same data again. Prove the storage contract before
adding Matrix requests or a website.

## What Changes

- Add a small Rust library backed by SQLite.
- Store stable room/event references, rich payloads, page order, cursors, and
  observation status without maintaining Matrix's state-resolution machinery.
- Commit page contents and their cursor together. Make repeated writes safe.
- Retain data when the source becomes unavailable.
- Provide targeted logical removal that cannot be undone by replaying an old page.
- Add executable restart, interruption, replay, and removal tests with matching CI.

## Capabilities

### New Capabilities

- `durable-cache`: Persistent room/event pages, atomic writes, observation status,
  and targeted removal.

### Modified Capabilities

None. There are no implemented main specifications yet.

## Impact

This introduces the first Rust crate, SQLite dependency, integration tests, and
Rust CI checks. It does not create a homeserver dependency.

## Boundaries

No network client, public UI, background collection, search index, media storage,
or historical-version publication. The storage API is not a public-access
decision. The following change tests that decision against local Synapse.

Removal means the application no longer returns the body. Physical erasure from
free database pages, backups, or exported files is not part of this change.

## Approval and dependencies

Planning follows Sam's request for a cache-first project with synthetic local
tests. Approval of these specific acceptance scenarios is pending.
Implementation approval reference: none yet.

Depends on the OpenSpec workflow setup. `local-synapse-cache-contract` depends on
this change's cache API and test results.
