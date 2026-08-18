# Automation Workflow Contract

This contract maps Avrotize automation and governance responsibilities without
changing existing build, test, publication, or release behavior. The
machine-readable companion is
[`workflow-contracts.json`](workflow-contracts.json).

## Responsibility map

| Workflow | Responsibility | Authority and failure semantics |
| --- | --- | --- |
| `build_deploy.yml` | Python test groups, generated-target and VS Code extension evidence, tag-only publication | Existing failures and publication behavior are preserved. |
| `python-runtime-versions-test.yml` | Python 3.10-3.14 build/install/CLI smoke matrix | Existing runtime signal; not represented as broader compatibility approval. |
| `validate-mcp-server-json.yml` | MCP registry manifest validation | Validation failure remains a workflow failure. |
| `issue-intake.yml` | Normalize future Bug report and Feature or transformation request events | Read-only intake. Incomplete and freeform bodies produce visible non-ready records. Processor or schema corruption fails. |
| `dependabot-intake.yml` | Normalize future Dependabot PR metadata and changed-file metadata | Read-only intake. It never runs the PR head, approves, or merges. A head race produces `superseded`. |
| `governance-observe.yml` | Report deterministic governance findings | Advisory findings remain non-blocking; validator crashes still fail the workflow. |
| `governance-ci.yml` | Run the strict validator and every governance test on the exact PR head | Hard-failing quality check; no warning fallback or swallowed test failure. Passing does not authorize merge. |
| `repro-bug.yml` | Authorize an issue content snapshot and prepare manual reproduction evidence | Future issue-label/comment mutation only. It never installs dependencies or executes Avrotize or reporter fixtures. |

A green workflow proves only its named responsibility.

## Deterministic and Copilot boundaries

Deterministic automation owns parsing, schema checks, exact revision identity,
dependency metadata, path impact, artifact digests, stale-head detection, and
guard decisions. No current governance workflow invokes Copilot.

Any later Copilot step needs a checked-in contract defining its ambiguity,
structured output, prohibited actions, and platform controls. It cannot authorize
work, set priority, approve compatibility or risk, merge, publish, mutate
reproduction state, or exercise owner authority. AIC is accepted only as
GitHub/Copilot platform-reported telemetry. Until representative runs exist,
sample size, P50, and P95 are `TBD`. Token telemetry is operational only and is
never converted into AIC.

## Exact revisions and stale evidence

- `governance-ci.yml` checks out and verifies
  `github.event.pull_request.head.sha`; its only third-party Python test dependency
  is binary-only and hash-pinned in `requirements-ci.txt`.
- Issue intake resolves one default-branch processor SHA, checks out that exact
  SHA, and records the processor SHA plus form-contract, command-registry, and
  capability-profile and derived surface-registry digests.
- Dependabot intake checks out the exact event base SHA. It compares the current
  PR head with the event head before and after REST changed-file retrieval and
  hashes stable metadata including filename, previous filename, status, blob SHA,
  additions, deletions, and changes. A mismatch emits a `superseded` record.
- Reproduction authorization binds repository, issue number, title, and body in
  an immutable canonical snapshot with separate title, body, and combined
  digests. GitHub label/comment mutations may change `issue.updated_at` but do not
  invalidate that content snapshot. A title or body edit does.

Later merge or release enforcement must fail closed on missing, malformed, stale,
or unreachable facts. It must not turn warnings, exception labels,
`continue-on-error`, `|| true`, or skipped required scope into passing evidence.
No such merge or release guard is implemented by this adoption.

## Permissions and untrusted content

- Intake and governance quality use `contents: read`; Dependabot intake adds
  `pull-requests: read`.
- `pull_request_target` is used for Dependabot metadata only. It checks out the
  trusted base SHA and never checks out or executes the Dependabot head.
- Guarded reproduction queries the label-event sender's collaborator permission
  before issue-content processing or processor checkout. Only exact `maintain` or
  `admin` role results proceed. API errors, actor ambiguity, and rerun actor
  mismatch fail closed.
- The reproduction preparation job has only `contents: read` and `issues: read`.
  The two state-publication jobs alone have `issues: write`.
- Intake and reproduction workflows receive no repository secrets and never
  execute issue, Dependabot-head, or reporter-provided code. Governance CI is a
  separate read-only, secret-free quality boundary: it intentionally checks out
  the exact pull-request head and executes only the governance validator and
  governance tests from that head.

## Intake automation

### Issue intake

Future `issues: opened`, `edited`, and `reopened` events produce a versioned JSON
record and readable summary. The fence-aware parser uses the checked-in
Issue Form heading contract, so `###` inside fenced examples is content rather
than a new field. Exact identifiers are resolved per surface from
`commands.json`, its Python entry points, MCP tool decorators, and the VS Code
`Convert to` registry; substring matching is not used. Records bind a digest of
that derived surface registry. All checked-in choices are recognized, including
`Generated project or code`.

Bug records normalize the concrete command/API, Avrotize Schema, JSON Structure,
or direct path, source and result representations, flags/options, minimal input,
actual and expected result, environment/toolchain, regression, and declared
expected-result kind. Feature or transformation records normalize the requested
command/transformation, representations, preserved semantics, options, target
validation/runtime expectation, and documentation example. Unknown, duplicate,
malformed, or freeform bodies become explicit manual-triage records.

### Dependabot intake

Future Dependabot PR `opened`, `reopened`, `synchronize`, and
`ready_for_review` events are accepted only when both the PR author and event
sender are `dependabot[bot]`. Parsing supports the eight configured ecosystems,
directories, and commit prefixes such as `deps(python):`, `deps(nuget):`, and
`deps(actions):`. It does not require the optional `updated-dependencies` body
block.

Records include dependency/version data when determinable, direct/transitive
status when determinable, ecosystem/directory, manifests and lockfiles, Avrotize
capability domains, runtime/dev/test/build/docs/CI/editor-extension exposure,
generated-output/toolchain implications, major/unknown version risk, and required
validation scope. No version or severity implies exploitability, safe merge, or
implementation authorization.

Dependabot auto-merge is removed. The owner reviews the weekly Monday Dependabot
queue at least weekly so each configured ecosystem's five-PR limit does not
silently stall updates. Security-alert intake is not implemented: it needs a
separately authorized event source and security access rather than broad polling
or fabricated coverage.

## Guarded reproduction preparation

`repro-bug.yml` has no manual dispatch. It runs only for the exact
`repro-requested` label event, whose sender is the requesting actor.

1. **authorize** (`contents: read`, `issues: read`) queries collaborator
   permission before issue content. It first writes a minimal allow/deny gate record
   containing no issue body; denied requests fail after that record is uploaded and
   make no mutation. Allowed requests resolve the trusted default-branch processor
   SHA, evaluate actor/event/rerun identity, and compare the label event's title/body
   to an immediate REST re-fetch before authorizing the event snapshot.
2. **mark-in-progress** (`issues: write`) clears contradictory result labels and
   the request label, then adds `repro-in-progress`.
3. **prepare** (`contents: read`, `issues: read`) checks out and verifies the
   trusted SHA, re-fetches the issue, recomputes the snapshot digests, parses the
   same Bug report contract as intake, and emits schema-validated evidence.
4. **publish-final** (`issues: write`, step-level `always()`) creates a bounded
   schema-validated fallback record if prepared evidence is missing, corrupt, or
   mismatched to the issue/run/processor/content identity; uploads terminal evidence;
   then re-reads and reconciles governed labels with three bounded attempts. Producer
   jobs pass their exact run-attempt artifact names to consumers, including failed-job
   reruns. If the trusted terminal validator itself fails, a fixed-shape, structurally
   checked emergency `BLOCKED` record is uploaded and the job remains failed.
   Transient label API failures and concurrent changes are retried; failure to restore
   one governed state is surfaced. GitHub does not provide atomic multi-label
   replacement.

Automated command execution was intentionally removed. Avrotize does not provide
an adequate locked/hash-pinned automation environment, and a GitHub-hosted runner
cannot guarantee denied egress or enforce the required memory, PID, and filesystem
isolation for hostile parser input. The workflow therefore never installs
dependencies, materializes reporter fixtures, executes Avrotize, compiles
generated code, or decides `CONFIRMED`/`NOT_REPRODUCED`. Complete eligible reports
end in `repro-needs-review`; incomplete, changed, unknown, or non-bug reports end
in `repro-blocked`. Owners perform and adjudicate any later reproduction in an
approved isolated environment.

Artifact identities include issue number, workflow run ID, and run attempt.
Authorization, preparation, and terminal artifacts are retained for 30 days. The
issue comment links the workflow run and records the evidence digest, trusted SHA,
authorized content digest, and run attempt. Exact evidence—not label text—is
authoritative.

The six labels are declared in
[`repro-label-catalog.json`](repro-label-catalog.json). They must be provisioned
manually by an owner before first use. No write-capable label-reconciliation
workflow or selected-ref `workflow_dispatch` backdoor is included. Dependabot PRs
never use issue reproduction labels.

Reproduction preparation, labels, comments, and evidence do not schedule work,
authorize implementation, accept compatibility, approve merge, or approve
release. Repository owners retain ultimate authority.

## GitHub Actions projections

Elapsed time is wall-clock duration. Summed runner minutes add every job and
matrix child. Billable-minute equivalent is platform billing telemetry, not
elapsed time. Matrix fan-out may reduce elapsed time while increasing summed
runner consumption.

| Workflow | Fan-out/jobs | Elapsed P50/P95 | Summed runner P50/P95 | Billable equivalent |
| --- | ---: | --- | --- | --- |
| Build/deploy PR evidence | 15 | observed median 19.35; P95 TBD | observed median 60.82; P95 TBD | TBD |
| Python runtime matrix | 7 | observed median 12.70; P95 TBD | observed median 4.37; P95 TBD | TBD |
| MCP manifest validation | 1 | observed median 1.08; P95 TBD | observed median 0.12; P95 TBD | TBD |
| Governance observe | 1 | TBD | TBD | TBD |
| Governance CI | 1 | TBD | TBD | TBD |
| Issue intake | 1 | TBD | TBD | TBD |
| Dependabot intake | 1 | TBD | TBD | TBD |
| Reproduction preparation | 4 | TBD | TBD | TBD |

New workflow values remain `TBD` until representative completed runs exist. Record
sample size, runner class, queue/setup/active/elapsed time, summed runner minutes,
matrix fan-out, cache/artifact reuse, cancellation, retries, timeouts, exact SHA,
and platform run ID. Recalibrate after a material matrix, runner, cache, or model
change.
