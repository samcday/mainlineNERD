# Durable cache

## Purpose

Keep room history and pagination progress across process restarts, with stable
references and explicit observation status. Support later removal without
turning the cache into a second Matrix state engine.

## ADDED Requirements

### Requirement: Persistent event and page records

The cache MUST retain room/event identifiers, supplied rich payloads, page order,
pagination tokens, and observation times after a successful write and reopen.
Pages MUST reference event records rather than hold independent body copies.

#### Scenario: CACHE-01 Read after restart

- **GIVEN** 100 synthetic events across two rooms, including formatted text,
  reply and replacement references, stored through the cache API
- **WHEN** the writer exits normally and a new process opens the same database
- **THEN** all 100 identifiers and payloads match the supplied records
- **AND** each page retains its order, tokens, and observation time
- **AND** reading one room returns no records from the other room

### Requirement: Atomic page writes

The cache MUST commit a page's event records, membership, observation metadata,
and pagination progress in one transaction. Success MUST be reported only after
the transaction commits.

#### Scenario: CACHE-02 Interrupted uncommitted page

- **GIVEN** a committed first page and its continuation token
- **WHEN** a child writer is killed after writing second-page rows but before commit
- **THEN** reopening returns the complete first page and its original token
- **AND** no second-page rows or progress become visible

#### Scenario: CACHE-03 Interrupted after acknowledgement

- **GIVEN** a child writer reports that a second page committed successfully
- **WHEN** the child is killed without a clean shutdown and the database is reopened
- **THEN** the complete second page and its continuation token remain readable

### Requirement: Repeatable writes without duplication

Replaying a page MUST NOT duplicate event records or page entries. Overlapping
pages MUST refer to the same stored event for the same room/event key.

#### Scenario: CACHE-04 Replay and overlap

- **GIVEN** two ten-event pages in one room with five shared event identifiers
- **WHEN** both pages are written ten times
- **THEN** the room contains exactly 15 event records
- **AND** each page contains exactly ten ordered references without duplicates

### Requirement: Preserve data when observation fails

The cache MUST distinguish a missing page, a known empty page, and a cached page
whose source is unavailable. A failed observation MUST NOT erase stored content
or advance pagination. It MUST retain the last successful observation time.

#### Scenario: CACHE-05 Unavailable source is not empty history

- **GIVEN** a stored page and its successful observation time
- **WHEN** a later observation records authentication loss, denied access, or outage
- **THEN** the stored page and token are unchanged
- **AND** the new failure status and the old successful observation time are readable
- **AND** missing, known-empty, and unavailable results remain distinct after restart

### Requirement: Targeted logical removal

Removing a room/event key MUST replace its body with a tombstone, a retained
identifier without content. Every page referencing it MUST return that tombstone.
Replaying old data MUST NOT restore the body.

#### Scenario: CACHE-06 Removal survives replay and restart

- **GIVEN** an event referenced by two cached pages
- **WHEN** its body is removed, an old page is replayed, and the process restarts
- **THEN** both page reads and direct event reads return the identifier without its body
- **AND** unrelated events remain unchanged

### Requirement: Safe schema handling

The cache MUST initialize an empty database and reject an unsupported newer schema
with an explicit error. It MUST NOT recreate or reset an existing incompatible
database.

#### Scenario: CACHE-07 Unsupported schema

- **GIVEN** a database marked with a newer schema version and a sentinel record
- **WHEN** the cache attempts to open it
- **THEN** opening fails with an unsupported-schema error
- **AND** the schema version and sentinel record remain unchanged
