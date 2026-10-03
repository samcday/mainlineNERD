# mainlineNERD

Nattering Engineers' Ramblings, Distilled. Read `docs/startup-check.md` for usage.

## Scope

- This is a one-shot startup check, not a collector. No event archive, resume
  cursor, backfill, discovery, or custom retry/scheduler framework.
- Use the Matrix SDK. Verify `/whoami` and complete the initial offline sync
  before writing SQLite startup bookkeeping.
- Keep changes small. Do not carry over PR #1's prototype architecture.
- Tests use an isolated Synapse and synthetic sessions only. Do not inspect
  existing credentials or contact public rooms without explicit approval.
- Never commit tokens or fixture secrets. Keep `Cargo.lock` for locked builds;
  omit generated lockfile contents from guided excerpts.

## Verification

```sh
cargo fmt --all --check
cargo test --locked --workspace
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo xtask smoke
```

- `xtask` owns the Synapse image pin. Upgrades must pass the real fixture,
  including our SQLite receipt, Synapse access-log assertions and rejection cases.
- Fixture state belongs under ignored `target/`, created at runtime, never via
  file-editing tools. Don't record scratch output in Delta or resurrect `out/`.
- Cleanup must touch only resources created by that test invocation.
