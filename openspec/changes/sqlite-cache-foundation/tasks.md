# Tasks

Implementation is not approved yet. The commands below are targets to introduce
with this change, not commands that exist in the current repository.
The `cache_contract` integration target must identify its `CACHE-*` scenarios.

## 1. Persistent records and schema

- [ ] 1.1 Add the Rust cache crate, pinned toolchain/dependencies, schema handling,
  and public API documentation. Prove CACHE-01 and CACHE-07 with
  `cargo test --locked --test cache_contract`.
- [ ] 1.2 Add the standard Linux Rust CI job with build, formatting, lint, and
  cache-test steps. For CACHE-01 and CACHE-07, run `cargo build --workspace --locked`,
  `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`,
  and `cargo test --workspace --locked` locally and in CI. Document the commands.

## 2. Transaction and replay behavior

- [ ] 2.1 Implement atomic page writes and deterministic child-process interruption
  tests. Prove CACHE-02 and CACHE-03 with `cargo test --locked --test cache_contract`;
  document the process-crash guarantee and its storage-hardware limits.
- [ ] 2.2 Implement replay-safe event and page writes with overlap tests.
  Prove CACHE-04 with `cargo test --locked --test cache_contract` and document
  page identity and ordering.

## 3. Observation status and removal

- [ ] 3.1 Implement separate success/failure status and tombstone removal.
  Prove CACHE-05 and CACHE-06 with `cargo test --locked --test cache_contract`;
  document the difference between logical removal and physical disk erasure.

## 4. Complete contract verification

- [ ] 4.1 Run the complete Rust checks from task 1.2 and `npm run check:specs`.
  Confirm CACHE-01 through CACHE-07 all execute and pass on the final commit in
  local checks and CI. Record test duration, fixture database size, and handwritten
  Rust lines separately from generated files in `refs/notes/evidence`.

## Workflow follow-up

- After every task has evidence, archive this completed change and include its
  main-spec updates before final Delta `/review`.
- Request `/land` only for the identified, explicitly authorized change or stack.
  Do not archive the separate pending Synapse proposal.
