# Design

## Context

Use the cache API from `sqlite-cache-foundation`. The repository currently has no
Matrix reader or integration fixture. See `proposal.md` for scope and the two
delta specs for acceptance. The source research below is not runtime proof.

## Goals / Non-Goals

Test actual Matrix authorization and cache behavior, not just a mock's assumptions.
Keep the reader a library exercised by the harness, not a deployed web service.
Production rate policy, a full circuit breaker, media, and a collection schedule
belong to later changes before any live deployment.

## Decisions

Use stock Synapse in a disposable container. Pin its release and image digest in
the implementation. Build the Rust test client before running it with Synapse on
an internal network. A local URL alone is not network isolation.

Use a small Rust `xtask` to manage the fixture and invoke the test executable.
Target a Docker-compatible engine on Linux. CI uses Docker on a standard runner.
Do not install or reconfigure a host daemon from the task runner. Missing tools
must fail with instructions. Use resource labels and cleanup guards for resources
created by that run, never broad container or directory deletion.

Use Ruma protocol types and one shared HTTP client. A single transport boundary
counts every wire attempt, applies deadlines and cooldowns, and disables hidden
redirects or retries. Prefer existing retry/backoff components. A full Matrix SDK
is an alternative, but its automatic retry behavior must not bypass our budget.

Seed rooms with a separate setup client. The reader has no write, join, or sync
operation. Keep counters for reader traffic separate from setup traffic. A small
local fault adapter can produce deterministic throttling, timeout, and server
failure responses. It does not replace real Synapse authorization tests.

Prove public eligibility before connecting admission to the durable cache.
The candidate is `/context/{eventId}` with `limit=0` and a filter for historical
`m.room.history_visibility`. Require matching room/event identifiers and explicit
`world_readable` state. Missing evidence rejects the event.

Test this candidate with clean, invited, and former-member accounts across history
visibility transitions. Do not accept current room state or "never joined" as
proof. If the candidate fails, stop and return to design. Do not admit private
messages to obtain a successful demo.

Build an explicit event projection, not a raw response proxy. Omit context state,
unvalidated bundled relations, quoted reply fallbacks, and nested redaction
details. Preserve a redacted flag without exposing the private redaction event.
Other related events need their own eligibility proof before admission.

Persist eligible projections and page progress through the existing cache API.
Separate reads from explicit refreshes, so ordinary cache hits do no upstream
work. Record an unsuccessful refresh without erasing an earlier public page.
Apply observed redactions through the cache tombstone operation.

Use Rust tests with `MATRIX-*` and `FIXTURE-*` IDs in their names or case labels.
Expose `cargo xtask test-synapse` as the planned full integration command. It must
report all required scenarios and fail if any are omitted. This command does not
exist until implementation.

Allow `--case <ID>` for focused local work. Reject unknown or unimplemented IDs
instead of reporting zero tests as success. CI runs the full suite without this
filter. FIXTURE-04 is the comparison of local and CI evidence, not a nested CI run.

## Risks / Trade-offs

Historical visibility validation can add one request per uncached event. Count
those calls rather than hiding the cost. The fixture's two-page/ten-attempt limit
is a correctness test, not a recommended matrix.org quota.

Local Synapse proves behavior for the pinned fixture version only. It does not
prove matrix.org configuration, federation completeness, or publication permission.
Any later live pilot needs separate approval.

Cleanup guards cannot survive a killed host or container daemon. Label resources
so the documented recovery procedure can identify them without deleting unrelated
work. Test normal exits, failures, and handled interruption.

## Source references

- [Synapse membership filtering](https://github.com/element-hq/synapse/blob/v1.162.0/synapse/visibility.py#L497-L569)
- [Context and historical state](https://github.com/element-hq/synapse/blob/v1.162.0/synapse/handlers/room.py#L2010-L2168)
- [Bundled relations](https://github.com/element-hq/synapse/blob/v1.162.0/synapse/handlers/relations.py#L434-L569)
- [Nested redaction metadata](https://github.com/element-hq/synapse/blob/v1.162.0/rust/src/events/serialize.rs#L225-L250)
