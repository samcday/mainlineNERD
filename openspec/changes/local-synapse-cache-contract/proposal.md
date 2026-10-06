# Proposal

## Why

As an operator, I need evidence that repeated reads use the cache and that private
history does not enter the public cache. A real, disposable Synapse lets us test
those rules without touching anyone's conversations.

## What Changes

- Add a small Rust `xtask` command to create, seed, test, and remove a local fixture.
- Use a pinned stock Synapse image and disposable identities in an isolated network.
- Add a thin Matrix read-through adapter using maintained protocol/client libraries.
- Test public-history eligibility independently of the reader account's privileges.
- Count requests to prove cache reuse and bounded pagination.
- Test unavailable history, authentication loss, throttling, and observed redactions.
- Run the same integration command locally and in a standard Linux CI job.

## Capabilities

### New Capabilities

- `matrix-read-through`: Bounded retrieval of eligible history into the cache,
  with explicit coverage and failure results.
- `local-matrix-tests`: Reproducible synthetic Synapse tests with network isolation
  and reliable cleanup.

### Modified Capabilities

None. This uses `durable-cache` without changing its accepted storage contract.

## Impact

This depends on `sqlite-cache-foundation`. It adds a Matrix request adapter, test
fixtures, a task runner, and an integration CI job. It does not deploy a service.

## Boundaries

No matrix.org access, production account setup, web UI, media collection, search,
room discovery, or claim of complete history. No automatic room joins or account
sync by the reader. Fixture setup uses a separate administrator identity.

Production request limits, collection delay, community notice, and full removal
policy remain separate decisions. Fixture limits are test inputs, not promises
about acceptable matrix.org traffic.

## Approval and dependencies

Sam authorized synthetic local tests and requested this planning direction.
Approval of these specific acceptance scenarios is pending.
Implementation approval reference: none yet.

Do not claim the proposed historical-visibility check works until the fixture
proves it. If it fails, report the result and return to design rather than changing
the expected public/private boundary.
