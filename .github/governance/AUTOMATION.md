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
| `dependabot-auto-merge.yml` | Merge eligible Dependabot PRs only after the named build workflow succeeds | Narrow delegated dependency merge | Non-Dependabot and workflow-changing PRs are ineligible; failed or pending external checks prevent merge. |
| `governance-observe.yml` | Validate governance surfaces, conversion-profile parity, and exact checkout revision | Advisory observation only | Findings are annotations and summaries; observe mode exits successfully and cannot satisfy or block a merge gate. |

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

## Exact-head evidence and stale approvals

Evidence records the PR head SHA, not a mutable branch name. Pull-request
automation must check out `github.event.pull_request.head.sha`, use
`persist-credentials: false`, and compare `git rev-parse HEAD` with that value.
A changed head invalidates head-bound approvals and evidence. Observe mode may
report stale records but cannot delete or rewrite them.

Before enforcement, a merge guard will deterministically require:

- an authorized linked item or a recorded owner exception;
- complete metadata and affected conversion cells;
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

Initial conversion-profile budgets distinguish elapsed from summed runner time:

| Scope | Elapsed | Typical runner | P95 runner | Ceiling |
| --- | ---: | ---: | ---: | ---: |
| Documentation-only PR | 3 | 5 | 10 | 15 |
| One conversion cell | 10 | 20 | 45 | 60 |
| Shared importer/exporter helper | 15 | 60 | 120 | 180 |
| Core intermediate-model change | 25 | 180 | 360 | 480 |
| Full conversion matrix | 25 | 180 | 300 | 420 |
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

Telemetry records exact SHA, event and dedupe IDs, jobs/cells, runner classes,
queue/setup/active/elapsed time, runner minutes, cache/artifact reuse,
cancellations/skips/retries/timeouts, projected values, platform run IDs,
platform-reported AIC, optional token counts, and disposition. AIC projections
remain `TBD` until representative platform telemetry exists, then use observed
P50 and P95 distributions. Recalibrate after 20 representative runs, a material
matrix, runner, cache, or model change, or sustained P95 deviation greater than
25%.
