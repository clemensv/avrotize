# Contributing to Avrotize

Read [GOVERNANCE.md](GOVERNANCE.md) before proposing or implementing work.
New work normally starts with the bug or feature form and becomes implementable
only when the repository owner marks it ready and ranks it.

## Contribution flow

1. File one structured work item. Name the exact command, Python API, MCP tool,
   VS Code action, or generated target; describe the input, output, flags,
   expected semantics, runtime/toolchain, and observable outcome.
2. Wait for owner authorization. Intake is not implementation authorization.
3. Use one item, branch, and pull request. Add newly discovered scope to the
   authorized item or file it separately.
4. Complete the PR template, including the exact head SHA and all affected
   commands, Python API, CLI, MCP, VS Code, package, specification, and generated
   surfaces.
5. Run the smallest evidence set covering the changed commands, shared schema
   semantics, and generated targets. Do not suppress a required command failure.

## Command and generated-output evidence

For changed transformations, include positive, invalid, and boundary fixtures;
expected schema/model semantics or output; meaningful round trips; and
supported flag defaults. Generation changes also compile or run the affected
generated target and exercise serialization where applicable. Interface changes
contract-test the Python API, CLI, MCP, or VS Code surface and update help and
documentation.

Golden-file changes require semantic review. Do not approve snapshots merely
because a tool regenerated them. Identify the source generator, command,
version/toolchain, and fixture so generated-file provenance is reproducible.

Changes to shared Avrotize Schema or JSON Structure behavior are normally
campaigns and require representative schema transformations, generated
language targets, data-platform outputs, and an explicit compatibility and
migration strategy.

## Reporting a reproducible bug

The bug form asks two optional questions that decide whether maintainers can run
guarded reproduction on your report:

- **Expected command result** — choose `Successful completion (exit 0)`,
  `Command failure (nonzero exit)`, `Exact output match`, or
  `Human semantic review`. Automation compares only structured facts, so an
  undeclared expectation can only end in "needs review".
- **Exact expected output** — required only when you choose `Exact output match`.
  Paste the exact expected file content or standard output.

Guarded reproduction additionally needs the Avrotize CLI surface, a command in
`.github/governance/repro-command-policy.json`, and the minimal input pasted
inline (ideally in a fenced block). Attachments, links, and file names cannot be
reproduced automatically. Reporter-supplied input and output paths are discarded
and replaced with workspace paths, so only your flags and the input content
matter.

## Requesting guarded reproduction (maintainers)

Apply the exact `repro-requested` label, or dispatch **Guarded bug reproduction**
with the issue number. Maintain or admin permission is required, the decision is
made before any checkout or issue content read, and a denial changes nothing on
the issue. The run replaces the governed labels with `repro-in-progress`, then
publishes exactly one of `repro-confirmed`, `repro-not-reproduced`,
`repro-blocked`, or `repro-needs-review` with a comment linking the run, the
evidence artifact, the trusted source revision, and the authorized issue
revision. Evidence is a record, not an authorization: implementation still needs
owner authorization under [GOVERNANCE.md](GOVERNANCE.md).

Run **Reconcile reproduction label catalog** manually when the six governed
labels are missing or drift from `.github/governance/repro-label-catalog.json`.
It reconciles repository labels only and never touches issue state.

## Local checks

Use the existing checks that match the change:

```powershell
# Targeted Python test
python -m pytest test\test_<affected_area>.py

# Package build
python -m build --sdist --wheel --outdir dist

# VS Code extension
Push-Location vscode\avrotize
npm ci
npm test
Pop-Location

# Governance validator and governance suites
python tools\validate_governance.py
python -m pytest test\test_governance_validator.py test\test_governance_schema.py `
  test\test_governance_intake.py test\test_governance_authorize.py `
  test\test_governance_repro.py test\test_governance_workflows.py
```

The GitHub Actions matrix remains authoritative for its named responsibility.
A warning-shaped success or ignored exit code is not evidence for a required
gate.

## Compatibility and release notes

Classify the change as patch, minor, or major under
[GOVERNANCE.md](GOVERNANCE.md). Describe affected users, accepted inputs,
defaults, generated APIs, outputs, runtime floors, and migration. Update
[CHANGELOG.md](CHANGELOG.md) for notable user-visible, compatibility, security,
dependency, publication, or governance changes.

Security reports follow [SECURITY.md](SECURITY.md); support requests follow
[SUPPORT.md](SUPPORT.md).
