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

The bug form asks two optional questions that help maintainers prepare a manual
reproduction:

- **Expected command result** — choose `Successful completion (exit 0)`,
  `Command failure (nonzero exit)`, `Exact output match`, or
  `Human semantic review`. Automation records this declaration but does not
  adjudicate the result.
- **Exact expected output** — required only when you choose `Exact output match`.
  Paste the exact expected file content or standard output.

Preparation additionally needs a concrete Avrotize command/API surface, a
command from `avrotize/commands.json`, necessary source and result
representations, flags/options, and minimal input pasted inline. Automation
parses and records these facts but never executes them.

## Requesting guarded reproduction (maintainers)

Apply the exact `repro-requested` label. Maintain or admin permission is required
for the label-event sender, and authorization occurs before checkout or issue
content processing. There is no manual dispatch. The workflow snapshots the
title/body revision and publishes only `repro-blocked` or
`repro-needs-review`, with a comment recording the run, evidence digest, trusted
source revision, and authorized content digest. It never executes Avrotize.
Evidence is not implementation authorization.

Owners provision the six labels from
`.github/governance/repro-label-catalog.json` through repository administration.
No write-capable label-reconciliation workflow is included.

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

### Adding or removing a command

`.github/governance/avrotize-capabilities.json` pins the command registry: it
records `expected_command_count` and a per-group count in `expected_groups`.
Any change to `avrotize/commands.json` therefore makes
`python tools\validate_governance.py` report drift until the profile is updated
in the same pull request. That is the intended detection, not a bug. A new
command group also needs an entry in `command_group_areas`, and a new
`7_Utility` command needs one in `utility_command_areas`, mapped to a domain
declared in `responsibility_domains`.

## Compatibility and release notes

Classify the change as patch, minor, or major under
[GOVERNANCE.md](GOVERNANCE.md). Describe affected users, accepted inputs,
defaults, generated APIs, outputs, runtime floors, and migration. Update
[CHANGELOG.md](CHANGELOG.md) for notable user-visible, compatibility, security,
dependency, publication, or governance changes.

Security reports follow [SECURITY.md](SECURITY.md); support requests follow
[SUPPORT.md](SUPPORT.md).
