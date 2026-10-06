# From a story to a release

OpenSpec keeps the agreed behavior beside the code. It does not run the acceptance
tests, approve a PR, or grant permission to merge.

The boardroom owns outcomes and publication policy. CTO threads choose the design
and divide the work. Workers implement approved tasks. Independent review and CI
provide evidence. Sam approves changes in scope and each software release.

## Start here

Node.js 20.19 or newer runs the development tools. The product will be Rust.
Install the pinned OpenSpec version and inspect the first changes:

```sh
export OPENSPEC_TELEMETRY=0
npm ci --ignore-scripts
npm run openspec -- list
npm run openspec -- status --change sqlite-cache-foundation
npm run check:specs
```

The initial CI job validates specifications only. There is no Rust implementation
or working Synapse fixture yet. A green specification check proves neither.

Start with `sqlite-cache-foundation`. It proves storage without a Matrix service.
Then implement `local-synapse-cache-contract` on top of that cache. Both proposals
await approval of their detailed acceptance behavior before implementation.

## The short OpenSpec course

Each change contains four kinds of document:

- `proposal.md` states the outcome, boundaries, dependencies, and approval.
- `specs/` contains requirements with named, measurable scenarios.
- `design.md` records consequential technical choices and alternatives.
- `tasks.md` names small implementation steps and their verification.

The shared skills live in `.agents/skills/openspec-*`. Delta reads that directory.
After skill discovery refreshes, `/openspec-propose` prepares a change and
`/openspec-apply-change` implements its approved tasks. Use `/openspec-explore`
for questions before committing to a design.

These are agent skills, not shell subcommands. If a harness does not discover
them, ask it to read the matching `SKILL.md` and use the local CLI. For example:

```sh
npm run openspec -- instructions apply --change sqlite-cache-foundation --json
```

Run bare `openspec` commands from generated instructions through
`npm run openspec --` so they use this repository's pinned version.

ZCode has an upstream adapter. To generate its local, ignored command files, run
`npm run openspec -- init --tools agents,zcode --profile core --no-animation`.
It exposes names such as `/opsx:propose`. MiMoCode command discovery is unverified.
Give it the same files and task brief rather than assuming that slash commands work.
Do not change personal profiles or global configuration to set up this repository.

## Work on one change

1. Propose the change. Include measurable examples and explicit non-goals.
2. Get Sam's approval of the scope and acceptance behavior. Record the approval
   reference in the proposal. Do not treat generated tasks as authorization.
3. Implement one bounded task on a topic branch. Add its tests with the code.
4. Run the documented checks. Store detailed evidence on the tested commit.
   A failed, skipped, or absent required check is not completion.
5. After all tasks pass, sync and archive the change through
   `/openspec-archive-change`. Include those file changes in the implementation PR.
6. Request Delta `/review` on the complete PR. Address blocking findings and rerun
   checks. If expected behavior must change, return to Sam before changing it.
7. Request `/land` for the identified PR or stack. Land dependencies first and
   verify each final commit. Do not bypass a conflict, failed check, or review.

The archive is a record, not an approval. `openspec/specs/` describes completed
capabilities after their changes land. Pending behavior stays in `changes/`.
Do not sync an unimplemented proposal into the main specifications.

For a stack, name the dependency PR in each dependent PR. After its base lands,
update from `main` by merging, not by rewriting published history. Recheck the
result before landing. A general goal does not authorize arbitrary future merges.

## A goal for any harness

Give a worker the approved change name and one task or independent task group.
Its goal is to implement that behavior, provide test evidence, and open a small PR.
It can continue with another approved, unclaimed task. It must stop when blocked,
when review capacity is full, or when the approved queue is empty.

The board needs the result, remaining failures, operating measurements, and
decisions that need approval. It does not need a stream of generated code.
Do not build a separate agent scheduler or planning database.

## CI and releases

Use standard GitHub-hosted Linux runners. Public repository runner time is free.
GPL is not the reason. Larger runners and excess storage can still incur charges.
See [GitHub Actions billing](https://docs.github.com/en/billing/concepts/product-billing/github-actions).

Keep CI read-only, bounded by job timeouts, and free of service credentials.
Cancel superseded runs. Keep fixture logs small and retain them only when needed.
Do not use `pull_request_target` to execute untrusted contributor code.

The first Rust change must add real build, formatting, lint, and cache-test jobs.
The Synapse change must add its real isolated integration job. Do not add a
conditional green job that silently succeeds while the implementation is absent.

GitHub branch protections are not configured by these files. Once the checks run,
Sam can approve making them required in repository settings. The review and
landing instructions require them even before GitHub enforces them.

Before each software release, present the tested commit, demonstration, known
coverage gaps, migration/rollback instructions, and source/license information.
Ask Sam to approve that release explicitly. Landing a PR does not publish it.

## Maintain the tooling

OpenSpec is a locked development dependency. Update it in its own PR.
Regenerate the shared skills with the new local CLI and review the generated diff:

```sh
npm run openspec -- init --tools agents --profile core --no-animation
npm run check:specs
```

Keep project rules in `AGENTS.md` and `openspec/config.yaml`, not in generated
skills. Preserve the upstream MIT notice in `THIRD_PARTY.md`. The OpenSpec
documentation is at <https://github.com/Fission-AI/OpenSpec/tree/v1.14.1/docs>.
