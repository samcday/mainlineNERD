---
name: land
description: >-
  Land changes in samcday/mainlineNERD through a verified GitHub squash merge.
  Invoke only when the user explicitly requests landing or merging the relevant
  change, not for review, preparation, passing checks, or skill installation.
disable-model-invocation: true
metadata:
  delta-action: land
---

# Land mainlineNERD changes

An explicit landing request authorizes this whole workflow. Proceed through
preparation, verification, merging, and confirmation without asking for the
same merge permission again. Installation of this skill alone is not a landing
request. Stop for genuine blockers or ambiguous scope.

## Scope and preflight

- Work only in `samcday/mainlineNERD`. Do not change the separate skills
  repository or resurrect the archived prototypes.
- Read applicable instructions and inspect the current branch, working tree,
  staged changes, diff, and any existing PR. Preserve unrelated work. If changes
  in the same file cannot be separated confidently, ask the user.
- Inspect Git remotes and GitHub authentication. Use the source remote for
  `samcday/mainlineNERD`, normally `origin`, never the `local` backlink.
- The destination is `main`. Confirm that it remains the repository's default
  branch, squash merging is allowed, and automatic branch deletion is disabled.
  If these settings changed, pause rather than guessing or changing them.
- Inspect current branch protections, effective rules, required checks, and
  review requirements. Do not assume the absence of requirements from an earlier
  run still holds. Honour any additional requirements established for this change.
- Do not alter signing settings or bypass contribution requirements. There was
  no repository-specific signing, CLA, DCO, or submission-template requirement
  when this skill was created; newly introduced requirements still apply.

Useful read-only commands:

```sh
git --no-optional-locks status --short --branch
git remote -v
gh api repos/samcday/mainlineNERD
gh api repos/samcday/mainlineNERD/branches/main
gh api repos/samcday/mainlineNERD/rules/branches/main
```

## Prepare the topic branch and PR

1. Use the current topic branch. If on `main` or detached, create a descriptive
   topic branch first. Do not land an `archive/` branch without the user explicitly
   identifying that archived work as the intended change.
2. Review and stage only the requested files. Commit relevant uncommitted work
   with a concise, descriptive message. Preserve supplied human wording and
   required submission metadata; do not generate text where policy requires
   human-authored text.
3. Fetch the verified source remote's `main`. Incorporate newer destination
   changes with a merge into the topic branch, not a rebase or force-push.
   Prefix editor-capable Git commands with `GIT_EDITOR=true`, and provide commit
   messages or `--no-edit` so the terminal cannot wait for an editor.
4. **Stop on every conflict, including apparently trivial conflicts.** Do not
   resolve, choose a side, or discard the conflicting state automatically.
   Report the files and operation, say that landing has not completed, and wait
   for the user's direction.
5. Push the topic branch to the verified source remote. Open or reuse its PR
   against `main`; do not reopen the rejected prototype PRs. When creating one,
   supply `gh pr create` with explicit `--repo`, `--base`, `--head`, `--title`,
   and `--body-file` values instead of using interactive prompts. Describe the
   actual change, verification conclusions, and any relevant evidence-note
   reference. Update an existing relevant issue when appropriate; do not create
   an issue without being asked.

## Verify the exact change

The current baseline contains only `README.md` and `.gitignore`; neither defines
a build or test command. For documentation, ignore rules, and skill-only changes:

- Run `git diff --check` for pending edits, `git diff --cached --check` for staged
  edits, and `git diff --check <source-remote>/main...HEAD` for the complete PR.
- Read the resulting diff for scope, accuracy, links and unintended content.
  Validate changed skill frontmatter and preserve its intended invocation
  restrictions without executing the skill as a validation step.
- Do not invent Cargo, formatter, or test invocations from archived branches.

When the change introduces implementation, CI, or contribution requirements,
read their current definitions and run the applicable verified local checks.
Find the actual task/script/manifest supporting each repository-specific
invocation, confirm its arguments and runtime requirements, and record a
repository-relative source reference with the verification result. Documentation
alone is not enough when the implementation disagrees. If verification cannot be
established, report the blocker instead of substituting a guessed command.

For the PR:

1. If the PR is a draft and the change is ready locally, mark it ready with
   `gh pr ready <number> --repo samcday/mainlineNERD`. The landing request supplies
   that intent. Include any checks or reviews triggered by readiness in the gate
   below.
2. Record the exact published head SHA and destination SHA. Ensure the published
   head includes all intended changes, including any conflict resolution or
   review fixes. Do not rely on results for an earlier revision.
3. Inspect review submissions and unresolved comments. Address genuine blocking
   findings within the approved scope; ask about disputed requirements or changes
   requiring a design decision. An acknowledgement or completed bot check is not
   an approval.
4. Verify **every required check and review has passed for that head**, including
   requirements imposed by repository policy or the user's instructions even
   when GitHub does not enforce them. Required checks that are pending, failing,
   missing, cancelled, skipped without a verified exemption, or unverifiable are
   blockers. Do not treat an empty check list as evidence that an expected check
   passed.
5. Limit a running-check watch to fifteen minutes:
   `timeout 15m gh pr checks <number> --repo samcday/mainlineNERD --required --watch`.
   Confirm that `timeout` supports this invocation; if unavailable, report the
   verification blocker rather than starting an unbounded watch.
   If the watch times out or verification remains incomplete, report that the
   change has not landed. Starting checks is not successful completion.
6. Recheck the PR head and destination before merging. If either changed, update
   the topic branch where necessary and repeat the applicable verification.

## Merge and confirm

Once the final head satisfies all applicable checks and reviews, make a normal
GitHub squash merge:

```sh
gh pr merge <number> --repo samcday/mainlineNERD \
  --squash --match-head-commit <verified-head-sha>
```

Do not use `--admin`, bypass protections, force-push, directly push to `main`, or
delete branches. If GitHub requires a merge queue, follow its normal protected
queue path only after the pre-merge requirements pass; being enqueued is not
being merged. Do not change repository settings to make a merge succeed.

Confirm that GitHub reports the PR as `MERGED`, obtain its resulting merge
commit, fetch the source remote's `main`, and verify that the result is reachable:

```sh
gh pr view <number> --repo samcday/mainlineNERD \
  --json state,mergedAt,mergeCommit,headRefOid,baseRefName,url
git merge-base --is-ancestor <merge-commit> <source-remote>/main
```

If required post-merge checks exist, verify those too. Do not report successful
completion from a queued request or an unverified destination.

Preserve important existing `refs/notes/evidence` notes across the squash by
attaching appropriate evidence to the resulting commit and publishing the notes
without overwriting unrelated notes. Keep conclusions and note pointers in PR
text rather than raw captures. Never publish secrets, real-room transcripts or
unapproved private evidence. If note publication or another completion step is
blocked after the merge, report precisely what landed and what remains unfinished.

Leave source and archive branches intact. Finish with the PR link, resulting
commit, destination, verification outcome, and any remaining blocker. Do not
reset unrelated work or imply that another checkout has been updated.
