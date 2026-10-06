# Local Matrix tests

## Purpose

Provide one repeatable way to test the cache against a real disposable Synapse
without using real identities, room history, or external Matrix services.

## ADDED Requirements

### Requirement: Isolated synthetic fixture

The test command MUST use a pinned stock Synapse image, fresh storage, and generated
test identities. The reader and homeserver MUST have no external network route
during the tests. Dependency and image downloads MUST finish before that phase.

#### Scenario: FIXTURE-01 Isolated repeatable run

- **WHEN** the integration command runs twice from clean fixture storage
- **THEN** both runs execute the same named scenarios against the pinned Synapse version
- **AND** all room content and identities come from the test setup
- **AND** attempts to reach a controlled address outside the fixture network fail
- **AND** the reader's audit shows zero real-service requests

### Requirement: Cleanup and actionable failure

The test command MUST remove the resources it created after success, test failure,
or a handled interruption. Missing prerequisites, failed startup, and timeout
MUST return nonzero rather than skip the suite. Logs MUST exclude credentials.

#### Scenario: FIXTURE-02 Failure does not leave a running fixture

- **GIVEN** a forced assertion failure or startup failure
- **WHEN** the integration command exits
- **THEN** it returns nonzero and names the failed phase
- **AND** no fixture containers, networks, or temporary data remain
- **AND** captured output contains no passwords, access tokens, or authorization headers

#### Scenario: FIXTURE-03 Unavailable engine is a failed check

- **GIVEN** the required container engine is unavailable
- **WHEN** the integration command runs
- **THEN** it returns nonzero with a prerequisite error
- **AND** it does not report the integration scenarios as passing or successfully skipped

### Requirement: Same verification locally and in CI

The integration command MUST run the same scenarios locally and on a standard
GitHub-hosted Linux runner. The job MUST have a finite timeout and no real-service
credentials. A missing or skipped integration job MUST block completion.

#### Scenario: FIXTURE-04 Required integration evidence

- **WHEN** the implementation PR reaches final review
- **THEN** the local run and the CI run report every required scenario's result
- **AND** the report records the tested commit, fixture image digest, and reader request counts
- **AND** any failed, missing, or unexplained skipped scenario prevents completion
