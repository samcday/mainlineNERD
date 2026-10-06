# Tasks

Implementation is not approved yet and depends on `sqlite-cache-foundation`.
The commands below are targets to introduce with this change. A selected case
must fail if unknown or unimplemented. CI must run the full command without a filter.

## 1. Isolated fixture

- [ ] 1.1 Add the pinned Synapse fixture, Rust task runner, network isolation,
  cleanup, and setup documentation. Prove FIXTURE-01, FIXTURE-02, and FIXTURE-03
  with `cargo xtask test-synapse --case <ID>` for each ID, including the forced
  failure and missing-engine checks. No real-service traffic or leaked tokens.

## 2. Public-read boundary

- [ ] 2.1 Add the bounded read-only transport and historical-visibility check.
  Prove MATRIX-01 and MATRIX-02 with `cargo xtask test-synapse --case <ID>` for
  each ID. Document the used endpoints and count every reader HTTP attempt.
  Stop if the public/private test cannot pass without weakening its expected result.
- [ ] 2.2 Add the explicit safe projection and nested-data tests.
  Prove MATRIX-03 with `cargo xtask test-synapse --case MATRIX-03`; document which
  source fields are retained, independently checked, or omitted.

## 3. Read-through and failure behavior

- [ ] 3.1 Connect eligible reads to the durable cache and bounded pagination.
  Prove MATRIX-04 and MATRIX-05 with `cargo xtask test-synapse --case <ID>` for
  each ID. Document cache-only reads, explicit refresh, and partial coverage.
- [ ] 3.2 Add deterministic fault responses and observed-redaction handling.
  Prove MATRIX-06 and MATRIX-07 with `cargo xtask test-synapse --case <ID>` for
  each ID. Document error states and the limit on detecting unfetched redactions.

## 4. Local and CI agreement

- [ ] 4.1 Add a standard Linux integration CI job with a 20-minute timeout.
  Prove FIXTURE-04 by running `cargo xtask test-synapse` locally and in CI on
  the same final commit. Require all MATRIX-01 through MATRIX-07 and FIXTURE-01
  through FIXTURE-03 cases, with no omitted or unexplained skipped cases.
  Rerun the foundation's Rust checks and `npm run check:specs`. Record the image
  digest, scenario results, request counts, durations, and code size in evidence
  notes without credentials.

## Workflow follow-up

- Archive only after all tasks and the cache dependency are verified.
- Include the resulting main specs and archive in the implementation PR before
  final Delta `/review`. Obtain explicit `/land` authorization.
- Return to Sam before real-room access, production collection, or a release.
