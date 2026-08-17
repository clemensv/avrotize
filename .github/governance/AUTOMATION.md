# Automation Workflow Contract

This contract maps current Avrotize automation and projects governance
responsibilities without changing existing build, test, publication, or release
behavior.

## Responsibility map

| Workflow | Current responsibility | Authority | Failure semantics |
| --- | --- | --- | --- |
| `build_deploy.yml` | Fourteen Python test groups, VS Code extension tests, and tag-only PyPI/extension publication | Technical evidence; release execution on an owner-created tag | Test and extension failures block deployment. Publication failures remain failures. |
| `python-runtime-versions-test.yml` | Build artifacts and exercise Python 3.10-3.14 installation/CLI smoke coverage | Compatibility signal | Build and matrix job failures propagate, but the current basic pytest command is a non-gating smoke signal. It is not represented as full evidence. |
| `validate-mcp-server-json.yml` | Validate the MCP registry manifest | MCP publication evidence | Validation failure fails the workflow. |
| `dependabot-auto-merge.yml` | Read-only policy guard summarizing Dependabot PR eligibility | Read-only summary only; no merge, approve, or branch deletion | Non-Dependabot PRs are ignored in summary. |
| `issue-intake.yml` | Normalize GitHub Issue Form submissions into deterministic intake records | Intake normalization only; no comments, labels, or project writes | Corrupt config fails closed; incomplete/unknown issues produce explicit non-ready records and exit successfully. |
| `dependabot-intake.yml` | Normalize Dependabot PR metadata into intake records using REST file metadata only, classifying each matched ecosystem separately | Intake normalization only; never checks out or runs PR head | Corrupt config fails closed; non-Dependabot PRs produce ignored records. |
| `repro-bug.yml` | Authorize, mark, run, and publish one policy-bounded CLI reproduction of an eligible bug report | Evidence and governed state labels only; never authorizes work, merge, or release | Denied or erroring authorization stops the run and mutates nothing. Readiness, policy, timeout, and resource refusals produce `BLOCKED` evidence. Infrastructure failure publishes `repro-blocked` and states that no evidence exists. |
| `repro-label-reconciliation.yml` | Manually reconcile the six governed reproduction labels on the repository from the checked-in catalog | Repository label name, color, and description only | Unauthorized actors, corrupt catalogs, and unexpected label API statuses fail the workflow. Issue state is never read or changed. |
| `governance-observe.yml` | Validate governance surfaces, command-capability profile parity, workflow static safety, and exact checkout revision | Advisory observation only | Findings are annotations and summaries; observe mode exits successfully and cannot satisfy or block a merge gate. |

A green workflow proves only its named responsibility.

## Deterministic and Copilot boundaries

Deterministic automation owns parsing, schema checks, path impact, head-SHA
comparison, dependency state, test selection, artifact hashes, lifecycle
projection, stale-verdict detection, budgets, and guard decisions.

Copilot may activate only when a checked-in workflow contract defines the
ambiguity, lowest adequate model, prompt version, structured output,
platform-configured AIC guardrails, and prohibited actions. It may classify or
recommend `PASS`, `REVISE`, or `ESCALATE`; it may not authorize work, change
priority, approve compatibility or risk, merge, publish, delete evidence, or
exercise owner authority. No current governance workflow invokes Copilot, so
its baseline and actual AIC are zero.
AIC is accepted only as the usage quantity reported by the GitHub/Copilot
execution platform. Workflow code does not derive AIC from tokens or any other
input.

No semantic or AI phase of guarded reproduction exists today. If one is ever
authorized, it must read the already-recorded evidence record only, must run
read-only after the terminal label is set, must not re-execute commands or
change any label or comment, and its AIC must come from platform telemetry with
sample size, P50, and P95 left `TBD` until representative runs exist.

## Exact-head evidence and stale approvals

Evidence records the PR head SHA, not a mutable branch name. Pull-request
automation must check out `github.event.pull_request.head.sha`, use
`persist-credentials: false`, and compare `git rev-parse HEAD` with that value.
A changed head invalidates head-bound approvals and evidence. Observe mode may
report stale records but cannot delete or rewrite them.

Before enforcement, a merge guard will deterministically require:

- an authorized linked item or a recorded owner exception;
- complete metadata and affected commands, shared semantics, generated targets, and public surfaces;
- required evidence produced from the current head;
- current-head domain, outcome, compatibility, security, and release verdicts;
- all existing required checks passing;
- no unresolved hard dependency.

The guard will fail closed on missing, malformed, stale, or unreachable facts.
It will not turn warnings, `continue-on-error`, `|| true`, skipped required
scope, or manual labels into passing evidence.

## Release guard

A later release guard will require an owner-approved version and compatibility
class, full required matrix evidence, package/extension checks from the tagged
revision, changelog and migration material, immutable tag identity, provenance,
artifact digests, and rollback. Publishing must promote the verified artifacts
built from the approved tag rather than rebuild them.

## Permissions, mutations, and concurrency

- Observe mode uses only `contents: read`; it has no secrets and makes no mutation.
- Future PR readers may add `pull-requests: read` only when review metadata is required.
- Mutating reconcile workflows require the narrow permission for the declared
  derived state and serialize on the issue, PR, or release they mutate.
- Superseded pull-request heads should cancel in-progress analysis.
- Untrusted pull-request code is never executed in a privileged
  `pull_request_target` context.
- Required commands propagate nonzero status. Infrastructure retries are bounded
  and apply only to classified transient failures.

## GitHub Actions projections

The following calibration uses the five most recent successful pull-request
runs observed on 2026-08-17. Elapsed minutes are wall-clock time. Runner minutes
sum active job durations.

| Workflow | Fan-out/jobs | Observed elapsed | Observed runner minutes |
| --- | ---: | ---: | ---: |
| Build/deploy PR evidence | 15 | 9.68-24.43; median 19.35 | 58.28-62.02; median 60.82 |
| Python runtime matrix | 7 (build, 5 runtimes, summary) | 1.75-14.65; median 12.70 | 4.05-4.45; median 4.37 |
| MCP manifest validation | 1 | 0.18-3.05; median 1.08 | 0.07-0.13; median 0.12 |
| Governance observe | 1 | projected 1 | projected 1 |
| Issue intake | 1 | projected 2 | projected 2 |
| Dependabot intake | 1 | projected 2 | projected 2 |
| Guarded bug reproduction | 4 (authorize, mark, reproduce, publish) | projected 8 | projected 12 |
| Reproduction label reconciliation | 2 (authorize, reconcile) | projected 3 | projected 4 |

Intake, reproduction, and label projections are declared projections, not
observations: these workflows have no recorded runs yet. Replace them with
platform-reported values after the first representative runs, and treat the
per-contract `runner_minutes_ceiling` as the enforcement bound in the meantime.

Initial Avrotize capability budgets distinguish elapsed from summed runner time:

| Scope | Elapsed | Typical runner | P95 runner | Ceiling |
| --- | ---: | ---: | ---: | ---: |
| Documentation-only PR | 3 | 5 | 10 | 15 |
| One command implementation | 10 | 20 | 45 | 60 |
| Shared schema/IDL transformation helper | 15 | 60 | 120 | 180 |
| Avrotize Schema or JSON Structure semantic change | 25 | 180 | 360 | 480 |
| Full command and generated-target matrix | 25 | 180 | 300 | 420 |
| Supported-runtime matrix | 15 | 40 | 80 | 120 |
| Package and extension release | 35 | 220 | 380 | 480 |

Parallel fan-out can reduce elapsed time but not summed runner time. Projections
include matrix children, reusable workflows, retries, and descendants. Record
the runner class with each observation.

Controls are cancellation of superseded heads, content-hash deduplication,
fail-safe impact filters, explicit cache keys, immutable build artifacts,
matrix/time limits, classified retries, and separate budgets for required and
optional diagnostics. If required scope cannot finish, report
`BLOCKED: ACTIONS-BUDGET-INSUFFICIENT`, unfinished scope, and a revised
projection; never silently skip it.

Telemetry records exact SHA, event and dedupe IDs, jobs/commands, runner classes,
queue/setup/active/elapsed time, runner minutes, cache/artifact reuse,
cancellations/skips/retries/timeouts, projected values, platform run IDs,
platform-reported AIC, optional token counts, and disposition. AIC projections
remain `TBD` until representative platform telemetry exists, then use observed
P50 and P95 distributions. Recalibrate after 20 representative runs, a material
matrix, runner, cache, or model change, or sustained P95 deviation greater than
25%.

## Intake automation

Issue and Dependabot intake workflows normalize incoming work items into
deterministic, versioned JSON records and Markdown summaries. They:

- Never authorize implementation, schedule work, approve compatibility, or
  permit merge.
- Never write comments, labels, project cards, or any mutation.
- Never check out or execute untrusted PR head content.
- Use `contents: read` (and `pull-requests: read` for Dependabot file metadata)
  only.
- Produce explicit non-ready/manual-triage records for incomplete, unknown, or
  freeform input and exit successfully.
- Fail closed on corrupt workflow config, dependabot.yml, or issue form contract.

The Dependabot policy guard (formerly auto-merge) is a read-only summary that
reports eligibility without merging, approving, or deleting branches.

## Security alert intake (future)

Dependabot security alerts require distinct event triggers and security access
not available through standard Actions workflow events. Security alert intake
is a separately authorized future control requiring:

- Explicit repository-owner authorization for security-event API access.
- A dedicated event source (not the PR or issue event).
- Separate authority boundary from dependency-update intake.

Until separately authorized, security alert intake is not implemented and must
not be approximated by scheduled broad-access polling or fabricated coverage.

## Guarded bug reproduction

`repro-bug.yml` runs four ordered jobs and each one holds only the permission it
needs.

1. **authorize** (`contents: read`, `issues: read`): resolves the trusted default
   branch revision, fetches `tools/governance_authorize.py` at that revision,
   calls the collaborator permission endpoint with an explicit HTTP status, and
   evaluates the request deterministically. Event, action, exact
   `repro-requested` label, actor identity, re-run actor agreement, and a numeric
   dispatch issue number are all checked before any permission call. An API
   failure is `ERROR` and fails the workflow; it is never silently downgraded to
   a denial. The decision artifact is uploaded even when denied. Issue metadata
   (revision, URL, body digest) is resolved only after the decision.
2. **mark-in-progress** (`issues: write`): removes the request and every terminal
   governed label, then adds `repro-in-progress`. Every label API status other
   than success or a benign `404` fails the job.
3. **reproduce** (`contents: read`, `issues: read`): checks out the authorized
   trusted SHA with `persist-credentials: false`, verifies `git rev-parse HEAD`
   against it, installs the pinned requirements plus the local package, records
   the resolved `avrotize` executable and version, re-fetches the issue, and
   re-verifies `updated_at` and the body digest against the authorized values. A
   changed revision produces `BLOCKED` evidence without execution.
4. **publish-final** (`issues: write`, `if: always()`): downloads the evidence
   artifact when reproduction succeeded, maps the recorded outcome to exactly one
   catalog label, removes all six governed labels, adds the final label, and
   comments the run URL, artifact name, trusted SHA, and authorized issue
   revision. If reproduction did not complete it publishes `repro-blocked` and
   says explicitly that no execution evidence exists.

The reproduction engine (`tools/governance_repro.py`) never invokes a shell,
builds argv from the checked-in policy, discards reporter-supplied paths, runs
with a minimal sanitized environment in a temporary workspace outside the
repository, bounds output and produced files, refuses symbolic links, and emits
schema-validated evidence. Policy refusals are `BLOCKED` evidence with exit 0;
corrupt policy, schema, catalog, or authorization is an infrastructure failure
with a nonzero exit.

The bug form renders the invocation field with `render: shell`, so GitHub writes
that section as a fenced block. The engine unwraps a single leading fence from
the invocation, command, minimal input, and exact expected output fields before
parsing or comparing them, so form rendering never changes an outcome.

Hosted runners cannot have their network disabled, so network safety is enforced
by policy instead: only commands that perform no network or database discovery
are allowlisted, `j2s` is excluded because it dereferences remote `$ref` targets,
and URLs are rejected in every argument. Fixture data may legitimately contain
`/`, `$`, `&`, and URLs because it is written to a file and never executed.

Concurrency serializes on the issue number and the request that started the run
(`repro-bug-<issue>-<label or workflow_dispatch>`) with cancellation, so a
repeated `repro-requested` supersedes an older reproduction of the same issue.
The request is part of the key deliberately: GitHub cannot filter the `labeled`
trigger, so every unrelated label creates a run whose jobs are all skipped, and
an issue-only key would let those runs cancel an authorized reproduction that is
already executing. A cancelled run can leave `repro-in-progress` in place; the
superseding run's final job replaces it.

## Reproduction label catalog

`.github/governance/repro-label-catalog.json` is the single source of the six
governed labels, their colors, descriptions, kind, and outcome mapping.
`repro-label-reconciliation.yml` is manual-dispatch only: it authorizes the
actor, checks out the trusted default branch, validates the catalog against its
schema, then reads, creates, or updates each repository label idempotently. It
takes no issue number and never touches issue state.
