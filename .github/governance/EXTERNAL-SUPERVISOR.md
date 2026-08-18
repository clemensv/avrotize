# External Copilot Delivery Supervisor

This contract defines how an owner-launched Copilot project session may
coordinate Avrotize delivery. It is not a GitHub Actions workflow and it is not
the issue-intake Copilot. Issue intake remains a zero-tool, read-only suggestion
pass with no authority to rank, authorize, dispatch, mutate, merge, or release.

## Current mode and future authority levels

The checked-in contract is permanently observe/validation-only for this policy
revision. No delegation can grant it operational authority. The delegated
operational row below defines a future-policy contract only; enabling it
requires the separately reviewed trust infrastructure listed under activation.

| Level | The external supervisor may | It may not |
| --- | --- | --- |
| Advisory | Analyze intake/dependencies, propose rank/domain/acceptance/reviewers, report WIP or stale evidence, recommend actions | Mutate repository state or authorize anything |
| Future delegated operational | Under a later reviewed policy, select from the owner-approved ordered READY set; create/resume isolated issue sessions; send bounded instructions; redirect/replace within budgets; open/update scoped PRs or evidence when explicitly allowed; request review; record cycles/recovery | Activate under this policy revision, expand scope, change owner intent, treat child claims as evidence, or cross a denied action |
| Owner-only | The supervisor may prepare an exact-head decision packet | Set rank/priority, authorize READY, change acceptance, grant WIP/risk/compatibility/emergency exceptions, approve/merge, tag/release/publish, or grant/amend/revoke delegation |

The complete action lists live in
[`external-supervisor-policy.json`](external-supervisor-policy.json).
Unknown actions and fields fail closed.

## Durable facts and state

Repository lifecycle and external execution state are separate:

- Repository: `INTAKE -> READY -> ACTIVE -> REVIEW -> APPROVED -> MERGED -> RELEASED`
  with `BLOCKED`, `PARKED`, and `INDETERMINATE`.
- External session: `PLANNED`, `DISPATCH_REQUIRED`, `DISPATCHED`, `RUNNING`,
  `IDLE`, `EVIDENCE_READY`, `REVIEW_WAIT`, `REDIRECT_REQUIRED`, `BLOCKED`,
  `FAILED`, `CLOSED`, `UNKNOWN`, `INDETERMINATE`, or `REVOKED`.

A session state never advances repository lifecycle. A session, branch, commit,
or child `success=true` claim is not completion evidence. Exact-head evidence,
current checks, and current reviews are independently verified from durable
repository facts. A head change invalidates earlier evidence and approval.

The deterministic standard-library engine is
[`tools/governance_supervisor.py`](../../tools/governance_supervisor.py). It
validates and derives records; it does not call Copilot, create sessions, or
mutate GitHub. External project sessions execute only the plan that the engine
emits through app-native session APIs.

The checked-in policy is observe/validation-only: projected dispatch records
carry no delegated operational authority, mutation list, child-session
messaging, edit/commit/push, PR, or review-request tools. They must not be
executed. This keeps the portable contract reviewable without pretending that
caller-authored local JSON can prove owner intent, canonical chain ownership,
or live platform facts.

## Selection, WIP, and dispatch

Selection follows the owner-approved READY order and the union of owner-frozen
and currently observed dependency facts. A known blocked item may be passed over
for the next independent READY item; an unknown dependency,
acceptance drift, scope mismatch, or missing authoritative fact stops
reconciliation. Default WIP is one executing item per Avrotize responsibility
domain. A matching worker still executing from an earlier dispatch cycle
occupies that slot; repository lifecycle, ownership, non-executing stale
sessions, and unknown state do not. Delegation cannot grant a WIP exception.
Non-terminal, non-executing sessions reserve the domain from a new child create
until exact resume/redirect/completion/recovery, without being reported as
executing WIP. A `BLOCKED` child named as the replacement target remains history
but does not collide with its independently observed replacement.

Each child packet binds the delegation digest, policy commit/digest, cycle and decision,
issue/PR, exact base/head, responsibility domain/WIP slot, frozen acceptance
digest, evidence requirements, tools/mutations, one deterministic issue branch
and expected old SHA, budgets, expected output,
owner-only prohibitions, and fail states. A dispatch receipt is accepted only
after the session inventory independently reports the same non-supervisor child
as currently `RUNNING` with the exact cycle-record and dispatch-packet digests.
Raw Git push is not an allowed tool; ref mutation must use the scoped push broker,
which denies default branches, tags, force-push, and unrelated refs. Receipt
creation revalidates active delegation authority, exact action/target/head
semantics, and the cycle-time window.

## Durability and recovery

Cycle, dispatch, recovery, and owner-decision packet records are strict,
versioned, immutable, and payload-digested. Corrections create a new record
whose `audit.supersedes` binds the prior payload digest; existing records are
not edited. Per-record verification requires each declared superseded digest to
be supplied as a verified prior record. This is append-by-supersession, not a
cryptographic append-only ledger. Parked and failed history is preserved.
After a crash, reconstruction uses only the owner delegation, prior cycle
records, GitHub facts, Git state, and current project-session inventory—not chat
memory. Every cycle after sequence 1 requires its immediately preceding sealed
cycle. An unreceipted prior dispatch remains sticky across successor cycles
until a non-supervisor worker for the exact decision, issue, cycle, delegation,
and policy is observed to have received the dispatch, or an explicit recovery
record binds a fresh session inventory proving no matching live worker exists.
Sequence 1 is
bound to the delegation's immutable initial cycle ID. Each successor cycle ID
is derived from its predecessor's sealed digest, sequence, delegation, current
facts, session inventory, and recovery record, preventing reset, reuse, or forks
with different durable inputs from minting colliding dispatch identities.

Snapshots are owner-bounded by `max_snapshot_age_seconds`, record their exact
observation time, and reject future or stale facts. Reconciliation stops on
expired/revoked/missing delegation, policy or prompt drift, dirty worktree
evidence, stale heads/reviews, unknown dependencies, WIP or session collision,
missing session state, scope expansion, exhausted concurrent-session,
monotonic session-creation, redirect, cycle-time, or platform-reported AIC budgets,
audit-write failure, failed checks, or unverified child success.

## Observe-mode launch and future activation

Nothing in this PR activates or schedules a supervisor, and no delegation can
turn this policy revision into an operational supervisor.
Delegated Git mutation also remains unavailable unless the owner-launched
environment supplies a broker that enforces the packet's exact ref and
compare-and-swap constraints; absence of that broker is `BLOCKED`, never a
reason to fall back to raw push.
Operational activation additionally requires a separately reviewed
policy commit that verifies the owner-controlled delegation ref, independently
authenticated GitHub and project-session collectors, and exactly one atomic
successor for the canonical audit-chain tip.

To launch an observe/validation session, the repository owner:

1. Chooses the exact merged commit containing the policy and computes the SHA-256
   of that commit's policy blob.
2. Creates a delegation matching
   [`external-supervisor-delegation.schema.json`](schemas/external-supervisor-delegation.schema.json),
   including owner login, expiry/revocation, issue/PR/domain scope, ordered READY
   set, frozen acceptance digests, initial cycle ID, allowed actions, immutable
   denied owner-only actions, and limits.
3. Commits the exact delegation bytes to an owner-controlled audit branch. The
   engine requires that delegation commit and repository-relative blob path and
   rejects worktree-only or altered authority. The external session must write
   validated projection records without executing them.
4. Launches an Avrotize project session with
   [`external-supervisor-kickoff-v1.txt`](prompts/external-supervisor-kickoff-v1.txt)
   and the delegation identity, commit, and repository-relative path.
5. Retains every approval, merge, tag, release, publication, exception, rank,
   READY, acceptance, and delegation decision.

Revocation or expiry immediately ends operational authority. Observe-only
analysis may continue, but no delegated action may proceed.
