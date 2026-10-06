# Matrix read-through

## Purpose

Retrieve eligible public history through a bounded Matrix client and reuse it
from the durable cache. Report missing coverage and upstream failures without
treating an account's private access as permission to publish.

## ADDED Requirements

### Requirement: Explicit room scope and read-only access

The reader MUST retrieve only explicitly configured rooms. It MUST NOT join,
register, send messages, or perform account-wide sync. It MUST use only the
supplied session and reject unconfigured rooms before making an upstream request.

#### Scenario: MATRIX-01 Reader stays within its scope

- **GIVEN** two synthetic rooms, only one configured for the reader
- **WHEN** the reader requests both rooms
- **THEN** the configured room can be read and the other is rejected locally
- **AND** the reader's request log contains zero join, registration, send, or sync calls
- **AND** no request is sent for the unconfigured room

### Requirement: Public eligibility independent of account privileges

The reader MUST admit a new event only with evidence of its historical public
visibility. Missing or ambiguous evidence MUST fail closed for that event.
Unvalidated nested events and metadata MUST NOT enter the public cache or results.

#### Scenario: MATRIX-02 Mixed history and contaminated credentials

- **GIVEN** a room with shared, joined, invited, and world-readable history periods
- **AND** clean, invited-but-never-joined, and joined-then-left reader identities
- **WHEN** each reader retrieves the same selected history
- **THEN** the eligible public test messages appear for each reader
- **AND** no private test message body enters the cache or returned data
- **AND** a current world-readable flag or empty joined-room list is not accepted as proof

#### Scenario: MATRIX-03 Nested private data stays private

- **GIVEN** a public event with a later private edit, reply, and redaction reason
- **WHEN** the source response includes nested relation or redaction data
- **THEN** no body, reason, sender, or identifier is copied from an unvalidated nested event
- **AND** unvalidated context state, reply fallback quotes, and raw unsigned data are omitted
- **AND** a known redacted original is represented without its old body

### Requirement: Durable reuse without mandatory revalidation

A request for a complete, cached page MUST read locally unless an explicit refresh
was requested. Outages and privacy changes MUST NOT erase previously public cache
entries. Failed refreshes MUST preserve the previous successful observation time.

#### Scenario: MATRIX-04 Repeat reads and unavailable updates

- **GIVEN** an eligible page fetched from local Synapse and committed to SQLite
- **WHEN** 20 readers request that cached page without requesting a refresh
- **THEN** all receive the cached page and cause zero additional upstream requests
- **WHEN** Synapse is stopped, the reader restarts, and a refresh is attempted
- **THEN** the cached page remains available with an update-unavailable status
- **AND** the same preservation rule holds after the fixture room becomes private

### Requirement: Bounded pagination and honest coverage

Each read action MUST enforce configured page and total HTTP-attempt limits.
Visibility checks and retries MUST consume that attempt budget. Empty pages with
continuation tokens MUST remain resumable. Exhaustion MUST mean upstream-available
history ended, not that the archive is complete.

#### Scenario: MATRIX-05 Stop at the configured boundary

- **GIVEN** a fixture containing more history than two pages can retrieve
- **AND** a read action limited to two message pages and ten total HTTP attempts
- **WHEN** the reader encounters an empty filtered page with a continuation token
- **THEN** it can continue within those limits without declaring the room empty
- **AND** it makes at most two message-page calls and ten HTTP attempts in total
- **AND** it reports partial coverage and a continuation when the budget ends first

### Requirement: Explicit upstream failure handling

Authentication loss and forbidden access MUST be reported as unavailable coverage,
not successful empty history. A throttling response MUST delay further source
requests as instructed. Retries MUST be bounded and MUST NOT switch credentials.

#### Scenario: MATRIX-06 Failures do not manufacture empty history

- **GIVEN** fixture responses for invalid credentials, forbidden access, throttling,
  server failure, and timeout
- **WHEN** reads use a maximum of three attempts and a finite action deadline
- **THEN** authentication and forbidden responses are not retried
- **AND** no request starts before an instructed two-second throttle delay expires
- **AND** Matrix delay fields, HTTP Retry-After seconds, and HTTP dates are tested
- **AND** server failures and timeouts cannot exceed the attempt limit or deadline
- **AND** results distinguish failure from a successful empty page without exposing tokens

### Requirement: Apply observed redactions to cached bodies

When a refresh observes that an admitted event is redacted, the reader MUST use the
cache's removal operation. It MUST NOT publish unvalidated redaction metadata or
claim immediate detection of changes that it has not fetched.

#### Scenario: MATRIX-07 Redaction removes a cached body

- **GIVEN** a cached public message that the fixture later redacts
- **WHEN** an explicit refresh observes that redaction and the reader restarts
- **THEN** every cache read returns the stable identifier without the old body
- **AND** replaying an earlier page does not restore the body
