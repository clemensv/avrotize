# Avrotize Governance

This file is the authoritative governance policy for Avrotize. It adopts the
conversion-matrix profile staged in
[`clemensv/project-governance`](https://github.com/clemensv/project-governance)
at commit `7e843f2d92054ecd73a3476ab9bf0a3adeeb45cd`, tailored to this repository.
If this policy, contributor templates, and automation disagree, this policy
controls and the ambiguity fails closed until the repository owner resolves it.

## Mission

Avrotize provides reliable, testable conversion among schema languages, data
models, generated programming-language APIs, database representations, and
public tool surfaces. Governance protects semantic fidelity, compatibility,
security, reproducible publication, and evidence tied to the exact revision
being approved.

## Authority

| Decision | Authority | Required record |
| --- | --- | --- |
| Backlog priority and rank | Repository owner | Ranked issue or explicit owner direction |
| Work authorization | Repository owner | Ready issue, or a recorded bootstrap/emergency exception |
| Normal merge | Repository owner or a future explicitly delegated maintainer | Current-head evidence and required reviews |
| Repository-owner merge exception | Repository owner only | Actor, head SHA, reason, unresolved gates, accepted risk, and follow-up |
| Release and compatibility classification | Repository owner | Approved version, changelog, immutable tag, and artifact evidence |
| Human approval | The reviewer named for the applicable domain or risk | Verdict bound to the exact PR head |
| Emergency action | Repository owner | Scope, evidence, risk, rollback, and ranked permanent follow-up |

The same person may hold more than one role, but authorization, review, merge,
and release remain distinct recorded decisions. Automation and agents may not
infer, impersonate, or exercise owner authority.

The owner may merge a change when ordinary gates are incomplete. This is an
override, not a claim that missing gates passed. It must be explicit and
auditable, and it does not permit falsifying evidence, suppressing failures, or
publishing an unapproved artifact.

## Planning model

Avrotize uses continuous ranked flow, not mandatory sprints. The repository
owner controls rank and may change cadence or WIP when the reason is recorded.

- Each item has one primary delivery lane and may affect several lanes.
- Default WIP is one active item per lane. Review and blocked work count toward
  WIP until the owner records an exception.
- `BLOCKED` means authorized work cannot advance because of a hard dependency.
  `PARKED` means work is intentionally removed from active flow.
- Campaigns coordinate coupled changes across several conversion cells or
  lanes. A campaign declares its matrix, compatibility strategy, sequence, exit
  criteria, and disposition of incomplete cells.
- Release milestones group compatibility and publication decisions. They do not
  authorize unranked implementation and are not delivery sprints.

## Delivery lanes

Lanes are durable responsibility and review domains, not assumed teams.
CODEOWNERS routes review but does not grant merge or release authority.

| Lane | Scope |
| --- | --- |
| Core model | Shared Python type-system, normalization, references, and intermediate schema behavior |
| Importers | Source-format parsers, inference, invalid-input handling, and normalized representations |
| Schema exporters and specifications | Target schema emitters, semantic equivalence, validators, round trips, and normative specs |
| Code generators | Generated C++, C#, Go, Java, JavaScript, Python, Rust, TypeScript, and runtime contracts |
| Data platforms | SQL, NoSQL, Kusto, Parquet, Iceberg, TMSL, and related mappings |
| Public interfaces | Python API, command registry and CLI, MCP server/manifests, and VS Code extension |
| Packaging and release | Python packages, Structurize, extension packaging, documentation, changelog, CI, and publication |

## Work classes

| Class | Meaning | Minimum review |
| --- | --- | --- |
| Defect | Restores documented behavior without an intended contract change | Domain and outcome review |
| Capability | Adds a conversion cell, option, interface, or supported target | Domain, outcome, and compatibility review |
| Compatibility | Changes accepted input, defaults, schemas, generated APIs, CLI/MCP contracts, runtime floors, or output semantics | Compatibility and release review; migration plan when breaking |
| Security | Changes trust boundaries, parsers, generated readers, credentials, or publication security | Security/risk review |
| Maintenance | Dependencies, refactoring, build, tests, or tooling with no intended public change | Domain review and evidence proportional to impact |
| Documentation/governance | Documentation or observe-mode policy automation with no runtime change | Policy/domain review |
| Release | Classifies, assembles, approves, or publishes immutable artifacts | Release authority |
| Emergency | Minimum safe restoration under recorded owner authority | Exact evidence, rollback, and permanent follow-up |

## Conversion impact matrix

The primary governed unit is a conversion capability cell:

`source format -> intermediate model -> target format/language -> options -> generated runtime/toolchain`

The checked-in profile is
[`.github/governance/conversion-matrix.json`](.github/governance/conversion-matrix.json).
Every authorized item and PR identifies changed cells and neighboring cells
affected through shared parsers, models, exporters, generators, fixtures,
runtimes, or public interfaces.

The impact declaration covers, as applicable:

- source format and version;
- importer/parser and normalized representation;
- target schema, language, database, or analytical model;
- options, defaults, generated runtime, and toolchain version;
- Python runtime cohort;
- Python API, CLI, MCP, and VS Code surfaces;
- package or publication artifact;
- positive, negative, boundary, and meaningful round-trip fixtures.

Shared core-model changes require a campaign unless the owner records why the
impact is demonstrably local.

## Readiness

Before implementation, an authorized item records:

1. Observable outcome and work class.
2. Primary lane, affected cells/lanes, and continuous-flow rank.
3. Hard dependencies, accountable delivery owner, and known reviewers.
4. Frozen acceptance manifest: positive, invalid, and boundary fixtures;
   expected intermediate representation or semantic output; meaningful round
   trips; generated-code compiler/runtime targets; supported options/defaults;
   documentation examples; and existing-fixture compatibility.
5. Evidence plan and risk across users, operations, security, data,
   compatibility, and release.

New scope is added explicitly to the authorized item or filed and ranked
separately. Golden files are evidence and never self-approving snapshots.

## Lifecycle

`INTAKE -> READY -> ACTIVE -> REVIEW -> APPROVED -> MERGED -> RELEASED`

`BLOCKED` and `PARKED` are side states with the meanings above. Lifecycle state
is derived from repository facts. Missing or ambiguous facts produce
`INDETERMINATE`, not success.

- **INTAKE:** structured request exists but is not authorized.
- **READY:** authority, rank, acceptance, evidence, risks, and dependencies are complete.
- **ACTIVE:** one owner works on one branch and one PR for the item.
- **REVIEW:** exact-head technical evidence and required review requests exist.
- **APPROVED:** all mandatory verdicts apply to the current head.
- **MERGED:** the approved revision is present on the default branch.
- **RELEASED:** approved immutable artifacts were published from an approved tag.

## Compatibility and review

- **Patch:** restores documented semantics without an intended output-contract
  change; any output drift is explained.
- **Minor:** additive or opt-in behavior with compatible defaults and generated APIs.
- **Major:** incompatible defaults, CLI/MCP/API contracts, schema
  interpretation, generated APIs, accepted inputs, outputs, or runtime floors.

Release authority makes the final classification.

Evidence and approvals bind to the exact PR head SHA. A new commit makes prior
approval stale unless a future deterministic policy proves a narrowly defined
non-semantic exception. The stricter of declared scope and actual diff governs
required evidence. Public schema, CLI, MCP, Python API, generated API, or wire
behavior requires compatibility review; user-visible behavior requires outcome
review; security-sensitive behavior requires security/risk review.

## Merge and release

Normal merge requires an authorized item, complete PR metadata, exact-head
evidence, all applicable reviews, and green existing required checks. This
policy does not weaken or replace current build, test, MCP validation,
publication, or release workflows.

Releases:

1. Classify compatibility and approve the release scope.
2. Run the required conversion, runtime, interface, package, and extension evidence.
3. Update the changelog and migration guidance when required.
4. Build once from the approved immutable tag.
5. Record artifact provenance and digests, then promote those artifacts rather than rebuilding.
6. Preserve a rollback or corrective-release plan.

## Automation and AI cost

[`.github/governance/AUTOMATION.md`](.github/governance/AUTOMATION.md)
defines workflow responsibilities, exact-head evidence, stale approvals,
permissions, failure semantics, Actions projections, and later guards.
[`.github/governance/AI-COST-ACCOUNTING.md`](.github/governance/AI-COST-ACCOUNTING.md)
defines inference cost in AIC. Deterministic workflows are preferred and use
zero AIC. Copilot may advise only within an explicit workflow contract; it
cannot authorize work, approve risk, merge, release, or exercise owner
exceptions.

Governance automation begins in advisory observe mode. It becomes blocking only
after existing repository state can satisfy the deterministic rule reliably and
the owner approves enforcement.

## Emergency changes

Emergency work is limited to the minimum safe restoration. The owner records
the authority, exact revision, evidence available, accepted risk, rollback, and
a ranked permanent follow-up. Emergency handling does not silently redefine
compatibility or release policy.

## Bootstrap provision

This governance adoption is an owner-directed bootstrap exception because
issue governance and its structured authorization surfaces did not yet exist.
The adoption PR itself is the authoritative work record. It must identify the
owner authorization, exact head, evidence, unresolved enforcement, and deferred
follow-up. It does not close or fix an issue and does not mutate pre-existing
issues or pull requests.

Later rollout follows observe, reconcile, and enforce phases documented in
[`.github/governance/ADOPTION.md`](.github/governance/ADOPTION.md).
