# Governance Adoption Phases

This owner-directed bootstrap introduces governance without modifying existing
issues or pull requests and without making a new governance check blocking.

## Phase 1: observe

- Publish authority, lanes, lifecycle, compatibility, contributor, and workflow contracts.
- Add structured intake and PR surfaces for new work.
- Run deterministic, read-only validation in advisory mode.
- Record existing inconsistencies without relabeling, closing, retargeting, or
  commenting on existing issues or pull requests.
- Preserve every existing build, test, publication, and release behavior.

## Phase 2: reconcile

After findings are understood, separately authorize idempotent mutations of
derived state only, such as future lifecycle fields or stale-state cleanup.
Serialize on the smallest resource, preserve an audit record, and never alter
priority, authorization, approval, or release authority through inference.
Existing issues and pull requests are reconciled only under an explicit later
owner direction.

## Phase 3: enforce

Make deterministic checks required only after current repository state can
satisfy them reliably. Candidate gates are authorized linkage, metadata,
declared/diff impact, exact-head evidence, changed-cell/runtime/interface
checks, current-head reviews, merge guard, and immutable release
provenance/approval.

Enforcement must not create a green path through falsified metadata, exception
labels, suppressed failures, weakened tests, or skipped required scope. AI
advice remains non-authoritative unless a narrow owner-authored policy explicitly
delegates a bounded decision.
