"""Deterministic external delivery-supervisor reconciliation for Avrotize.

This standard-library tool validates owner delegation and durable repository/session
snapshots, then emits immutable plan, dispatch, recovery, or owner-decision records.
It never calls Copilot, creates sessions, mutates GitHub, approves, merges, or releases.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

try:  # pragma: no cover - import shape depends on invocation style
    from tools import governance_schema
except ImportError:  # pragma: no cover - direct script execution
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from tools import governance_schema


ROOT = Path(__file__).resolve().parent.parent
POLICY_PATH = ROOT / ".github" / "governance" / "external-supervisor-policy.json"
DELEGATION_SCHEMA_PATH = (
    ROOT
    / ".github"
    / "governance"
    / "schemas"
    / "external-supervisor-delegation.schema.json"
)
RECORD_SCHEMA_PATH = (
    ROOT
    / ".github"
    / "governance"
    / "schemas"
    / "external-supervisor-cycle.schema.json"
)
CAPABILITY_PATH = ROOT / ".github" / "governance" / "avrotize-capabilities.json"
SHA1_RE = re.compile(r"^[0-9a-f]{40}$")

ACTIVE_SESSION_STATES = frozenset(
    {"DISPATCHED", "RUNNING", "IDLE", "EVIDENCE_READY", "REVIEW_WAIT", "REDIRECT_REQUIRED"}
)
RESERVING_SESSION_STATES = ACTIVE_SESSION_STATES | frozenset(
    {"PLANNED", "DISPATCH_REQUIRED", "UNKNOWN", "INDETERMINATE"}
)
TERMINAL_DEPENDENCY_STATES = frozenset({"MERGED", "RELEASED"})
EXPECTED_OUTPUT = [
    "commit-sha",
    "changed-paths",
    "commands-and-results",
    "evidence-artifacts",
    "unresolved-risks",
    "exact-head-status",
]

SNAPSHOT_TOP_KEYS = frozenset(
    {
        "repository",
        "observed_at",
        "audit_writable",
        "elapsed_seconds",
        "platform_reported_aic",
        "items",
    }
)
SNAPSHOT_ITEM_KEYS = frozenset(
    {
        "issue_number",
        "pull_request",
        "domain",
        "lifecycle",
        "dependencies",
        "acceptance_manifest_digest",
        "base_sha",
        "head_sha",
        "worktree_clean",
        "checks_passed",
        "evidence_verified",
        "evidence_head_sha",
        "reviews_current",
        "approval_head_sha",
        "active",
    }
)
INVENTORY_TOP_KEYS = frozenset({"observed_at", "supervisor_session_id", "sessions"})
SESSION_KEYS = frozenset(
    {
        "session_id",
        "role",
        "issue_number",
        "state",
        "delegation_id",
        "policy_commit",
        "cycle_id",
        "decision_id",
        "cycle_record_digest",
        "dispatch_packet_digest",
        "branch_ref",
        "head_sha",
        "dirty",
        "executing",
        "redirects",
        "success_claimed",
    }
)


class SupervisorError(RuntimeError):
    """Fail-closed reconciliation error with a stable reason code."""

    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


def _reject_duplicate_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise SupervisorError("DUPLICATE_JSON_KEY", f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def load_json(path: Path) -> dict[str, Any]:
    """Load a JSON object and reject duplicate keys."""
    try:
        value = json.loads(
            path.read_text(encoding="utf-8"), object_pairs_hook=_reject_duplicate_pairs
        )
    except OSError as exc:
        raise SupervisorError("MISSING_INPUT", f"cannot read {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise SupervisorError("INVALID_JSON", f"cannot parse {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise SupervisorError("INVALID_JSON", f"{path} must contain one JSON object")
    return value


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def digest_json(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def digest_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def text_blob_bytes(path: Path) -> bytes:
    """Return the LF-normalized UTF-8 bytes stored in Git for governed text files."""
    return path.read_text(encoding="utf-8").encode("utf-8")


def parse_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SupervisorError("INVALID_TIMESTAMP", f"invalid UTC timestamp {value!r}") from exc
    if parsed.tzinfo is None:
        raise SupervisorError("INVALID_TIMESTAMP", f"timestamp {value!r} lacks timezone")
    return parsed.astimezone(timezone.utc)


def format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _assert_exact_keys(value: dict[str, Any], allowed: frozenset[str], label: str) -> None:
    extra = sorted(set(value) - allowed)
    missing = sorted(allowed - set(value))
    if extra:
        raise SupervisorError("UNSUPPORTED_INPUT", f"{label} has unknown fields {extra}")
    if missing:
        raise SupervisorError("MISSING_INPUT", f"{label} lacks fields {missing}")


def _is_sha(value: Any, length: int) -> bool:
    return (
        isinstance(value, str)
        and len(value) == length
        and all(character in "0123456789abcdef" for character in value)
    )


def _validate_schema(
    value: dict[str, Any], schema_path: Path, label: str
) -> None:
    governance_schema.validate_or_raise(value, schema_path, label)


def validate_policy(policy: dict[str, Any], policy_bytes: bytes) -> None:
    """Validate policy semantics and checked-in prompt/domain bindings."""
    required = {
        "schema_version",
        "policy_id",
        "repository",
        "mode",
        "policy_path",
        "delegation_schema",
        "record_schema",
        "kickoff_prompt",
        "responsibility_domains",
        "repository_lifecycle_states",
        "external_session_states",
        "advisory_actions",
        "delegable_operational_actions",
        "owner_only_actions",
        "required_child_evidence",
        "fail_states",
        "invariants",
    }
    if set(policy) != required:
        raise SupervisorError(
            "POLICY_SHAPE_MISMATCH",
            f"policy fields differ: missing={sorted(required - set(policy))}, "
            f"extra={sorted(set(policy) - required)}",
        )
    if policy.get("schema_version") != 1:
        raise SupervisorError("POLICY_VERSION_MISMATCH", "only policy schema version 1 is supported")
    if policy.get("mode") != "observe-validation-only":
        raise SupervisorError("POLICY_MODE_MISMATCH", "policy must fail closed before delegation")
    owner_only = policy.get("owner_only_actions")
    delegable = policy.get("delegable_operational_actions")
    advisory = policy.get("advisory_actions")
    if not all(isinstance(value, list) and len(value) == len(set(value)) for value in (owner_only, delegable, advisory)):
        raise SupervisorError("POLICY_ACTION_MISMATCH", "policy action lists must be unique arrays")
    if set(owner_only) & (set(delegable) | set(advisory)):
        raise SupervisorError("AUTHORITY_OVERLAP", "owner-only actions overlap delegated/advisory actions")
    if policy["invariants"].get("actions_workflow") is not None:
        raise SupervisorError("PRIVILEGED_WORKFLOW_FORBIDDEN", "external supervision must not be an Actions workflow")
    if policy["invariants"].get("aic_source") != "github-copilot-platform":
        raise SupervisorError("AIC_SOURCE_INVALID", "AIC must be platform-reported")

    prompt = policy.get("kickoff_prompt")
    if not isinstance(prompt, dict) or set(prompt) != {"path", "sha256"}:
        raise SupervisorError("PROMPT_BINDING_INVALID", "kickoff prompt binding is malformed")
    prompt_path = ROOT / str(prompt["path"])
    try:
        prompt_digest = digest_bytes(text_blob_bytes(prompt_path))
    except OSError as exc:
        raise SupervisorError("PROMPT_MISSING", f"cannot read kickoff prompt: {exc}") from exc
    if prompt_digest != prompt["sha256"]:
        raise SupervisorError("PROMPT_DIGEST_MISMATCH", "kickoff prompt differs from policy digest")

    capabilities = load_json(CAPABILITY_PATH)
    domains = capabilities.get("responsibility_domains")
    if not isinstance(domains, dict) or policy["responsibility_domains"] != list(domains):
        raise SupervisorError(
            "DOMAIN_CATALOG_MISMATCH",
            "external supervisor domains must match the Avrotize capability profile",
        )
    if digest_bytes(policy_bytes) != digest_bytes(text_blob_bytes(POLICY_PATH)):
        raise SupervisorError("POLICY_BYTES_MISMATCH", "validated policy bytes are not the checked-in policy")


def verify_delegation(
    policy: dict[str, Any],
    delegation: dict[str, Any],
    *,
    policy_bytes: bytes,
    policy_commit: str,
    delegation_bytes: bytes,
    now: datetime,
) -> None:
    """Validate delegation schema, authority, scope, limits, and policy binding."""
    _validate_schema(delegation, DELEGATION_SCHEMA_PATH, "external supervisor delegation")
    bound_delegation = json.loads(
        delegation_bytes.decode("utf-8"),
        object_pairs_hook=_reject_duplicate_pairs,
    )
    if bound_delegation != delegation:
        raise SupervisorError(
            "DELEGATION_DIGEST_MISMATCH",
            "delegation bytes differ from the validated delegation object",
        )
    validate_policy(policy, policy_bytes)
    if delegation["repository"] != policy["repository"]:
        raise SupervisorError("OUT_OF_SCOPE", "delegation repository differs from policy")
    repository_owner = delegation["repository"].split("/", 1)[0]
    if delegation["authenticated_owner_login"].casefold() != repository_owner.casefold():
        raise SupervisorError(
            "OWNER_IDENTITY_MISMATCH",
            "authenticated owner login does not match the repository owner",
        )
    binding = delegation["policy_binding"]
    if binding["commit_sha"] != policy_commit:
        raise SupervisorError("POLICY_COMMIT_MISMATCH", "delegation does not bind the selected policy commit")
    if binding["blob_sha256"] != digest_bytes(policy_bytes):
        raise SupervisorError("POLICY_DIGEST_MISMATCH", "delegation policy blob digest is stale or forged")
    if binding["policy_path"] != policy["policy_path"]:
        raise SupervisorError("POLICY_PATH_MISMATCH", "delegation binds a different policy path")

    authorized_at = parse_timestamp(delegation["authorized_at"])
    expires_at = parse_timestamp(delegation["expires_at"])
    if expires_at <= authorized_at:
        raise SupervisorError("INVALID_EXPIRY", "delegation expires before it is authorized")
    if now < authorized_at:
        raise SupervisorError("DELEGATION_NOT_YET_VALID", "delegation authorization time is in the future")
    revocation = delegation["revocation"]
    if revocation["revoked"]:
        if revocation["revoked_at"] is None:
            raise SupervisorError("REVOCATION_INVALID", "revoked delegation lacks revoked_at")
        raise SupervisorError("DELEGATION_REVOKED", "owner revoked this delegation")
    if revocation["revoked_at"] is not None or revocation["reason"] is not None:
        raise SupervisorError("REVOCATION_INVALID", "active delegation contains revocation details")
    if now >= expires_at:
        raise SupervisorError("DELEGATION_EXPIRED", "delegation has expired")

    owner_only = policy["owner_only_actions"]
    if delegation["denied_owner_only_actions"] != owner_only:
        raise SupervisorError(
            "OWNER_ONLY_DENIAL_MISMATCH",
            "delegation must preserve the exact ordered owner-only denial list",
        )
    allowed = delegation["allowed_actions"]
    if not set(allowed) <= set(policy["delegable_operational_actions"]):
        raise SupervisorError("UNDELEGABLE_ACTION", "delegation contains an undelegable action")
    if set(allowed) & set(owner_only):
        raise SupervisorError("OWNER_AUTHORITY_ESCALATION", "owner-only action appeared in allowed actions")

    scope = delegation["scope"]
    if not set(scope["domains"]) <= set(policy["responsibility_domains"]):
        raise SupervisorError("OUT_OF_SCOPE", "delegation contains an unknown responsibility domain")
    positions = [item["position"] for item in scope["ready_order"]]
    if positions != list(range(1, len(positions) + 1)):
        raise SupervisorError("READY_ORDER_INVALID", "READY positions must be contiguous and ordered")
    issue_numbers = [item["issue_number"] for item in scope["ready_order"]]
    if len(issue_numbers) != len(set(issue_numbers)):
        raise SupervisorError("READY_ORDER_INVALID", "READY order contains duplicate issues")
    for item in scope["ready_order"]:
        if item["issue_number"] not in scope["issues"]:
            raise SupervisorError("OUT_OF_SCOPE", "READY item is outside issue scope")
        if item["domain"] not in scope["domains"]:
            raise SupervisorError("OUT_OF_SCOPE", "READY item domain is outside delegation scope")
        if item["pull_request"] is not None and item["pull_request"] not in scope["pull_requests"]:
            raise SupervisorError("OUT_OF_SCOPE", "READY item PR is outside delegation scope")
        if item["issue_number"] in item["dependencies"]:
            raise SupervisorError("DEPENDENCY_CYCLE", "READY item depends on itself")

    limits = delegation["limits"]
    if limits["max_active_per_domain"] != policy["invariants"]["default_max_active_per_domain"]:
        raise SupervisorError(
            "WIP_EXCEPTION_FORBIDDEN",
            "delegation cannot grant an owner-only WIP exception",
        )
    aic = limits["platform_reported_aic_guardrail"]
    if aic["enabled"] != (aic["max_aic"] is not None):
        raise SupervisorError("AIC_GUARDRAIL_INVALID", "AIC enabled/max_aic values disagree")


def verify_git_blob(
    repo_root: Path,
    *,
    commit_sha: str,
    blob_path: str,
    expected_bytes: bytes,
) -> None:
    """Verify the selected commit contains the exact governed blob."""
    committed_bytes = read_git_blob(
        repo_root,
        commit_sha=commit_sha,
        blob_path=blob_path,
    )
    if committed_bytes != expected_bytes:
        raise SupervisorError("GOVERNED_BLOB_MISMATCH", "commit blob differs from supplied governed bytes")


def read_git_blob(
    repo_root: Path,
    *,
    commit_sha: str,
    blob_path: str,
) -> bytes:
    """Read a governed blob only after confirming its source is a Git commit."""
    if not _is_sha(commit_sha, 40):
        raise SupervisorError("POLICY_COMMIT_INVALID", "policy commit must be a full SHA-1")
    object_type = subprocess.run(
        ["git", "cat-file", "-t", commit_sha],
        cwd=repo_root,
        check=False,
        capture_output=True,
    )
    if object_type.returncode != 0 or object_type.stdout.strip() != b"commit":
        raise SupervisorError(
            "POLICY_COMMIT_INVALID",
            "referenced policy object must be a Git commit",
        )
    result = subprocess.run(
        ["git", "show", f"{commit_sha}:{blob_path}"],
        cwd=repo_root,
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        raise SupervisorError("POLICY_COMMIT_UNREADABLE", result.stderr.decode("utf-8", "replace").strip())
    return result.stdout


def validate_snapshot(
    snapshot: dict[str, Any],
    inventory: dict[str, Any],
    policy: dict[str, Any],
    delegation: dict[str, Any],
) -> None:
    """Reject unbounded, ambiguous, or out-of-scope durable fact snapshots."""
    _assert_exact_keys(snapshot, SNAPSHOT_TOP_KEYS, "repository snapshot")
    _assert_exact_keys(inventory, INVENTORY_TOP_KEYS, "session inventory")
    if snapshot["repository"] != delegation["repository"]:
        raise SupervisorError("OUT_OF_SCOPE", "snapshot repository differs from delegation")
    if not isinstance(snapshot["items"], list) or len(snapshot["items"]) > 100:
        raise SupervisorError("SNAPSHOT_TOO_LARGE", "repository snapshot exceeds 100 items")
    if not isinstance(inventory["sessions"], list) or len(inventory["sessions"]) > 100:
        raise SupervisorError("SNAPSHOT_TOO_LARGE", "session inventory exceeds 100 sessions")
    parse_timestamp(snapshot["observed_at"])
    parse_timestamp(inventory["observed_at"])
    if not isinstance(snapshot["audit_writable"], bool):
        raise SupervisorError("UNSUPPORTED_INPUT", "audit_writable must be boolean")
    if not isinstance(snapshot["elapsed_seconds"], int) or snapshot["elapsed_seconds"] < 0:
        raise SupervisorError("UNSUPPORTED_INPUT", "elapsed_seconds must be a non-negative integer")
    aic = snapshot["platform_reported_aic"]
    if not isinstance(aic, dict) or set(aic) != {
        "reported",
        "value",
        "measurement_scope",
    }:
        raise SupervisorError("UNSUPPORTED_INPUT", "platform_reported_aic has invalid shape")
    if aic["measurement_scope"] != "current-cycle-total":
        raise SupervisorError("UNSUPPORTED_INPUT", "AIC measurement scope must be current-cycle-total")
    if not isinstance(aic["reported"], bool):
        raise SupervisorError("UNSUPPORTED_INPUT", "AIC reported must be boolean")
    if aic["reported"] != (isinstance(aic["value"], int) and aic["value"] >= 0):
        if not (aic["reported"] is False and aic["value"] is None):
            raise SupervisorError("UNSUPPORTED_INPUT", "AIC report/value fields disagree")

    seen_issues: set[int] = set()
    for item in snapshot["items"]:
        if not isinstance(item, dict):
            raise SupervisorError("UNSUPPORTED_INPUT", "snapshot item must be an object")
        _assert_exact_keys(item, SNAPSHOT_ITEM_KEYS, "repository snapshot item")
        issue = item["issue_number"]
        if not isinstance(issue, int) or issue < 1 or issue in seen_issues:
            raise SupervisorError("UNSUPPORTED_INPUT", "snapshot issue numbers must be unique positive integers")
        seen_issues.add(issue)
        if item["domain"] not in policy["responsibility_domains"]:
            raise SupervisorError("OUT_OF_SCOPE", f"issue {issue} has an unknown domain")
        if item["lifecycle"] not in policy["repository_lifecycle_states"]:
            raise SupervisorError("UNSUPPORTED_INPUT", f"issue {issue} has an unknown lifecycle")
        if not isinstance(item["dependencies"], list) or len(item["dependencies"]) > 50:
            raise SupervisorError("SNAPSHOT_TOO_LARGE", f"issue {issue} has too many dependencies")
        if (
            any(
                isinstance(dependency, bool)
                or not isinstance(dependency, int)
                or dependency < 1
                for dependency in item["dependencies"]
            )
            or len(item["dependencies"]) != len(set(item["dependencies"]))
        ):
            raise SupervisorError(
                "UNSUPPORTED_INPUT",
                f"issue {issue} dependencies must be unique positive integers",
            )
        for field in ("acceptance_manifest_digest",):
            if not _is_sha(item[field], 64):
                raise SupervisorError("UNSUPPORTED_INPUT", f"issue {issue} has invalid {field}")
        if not _is_sha(item["base_sha"], 40):
            raise SupervisorError("UNSUPPORTED_INPUT", f"issue {issue} has invalid base_sha")
        for field in ("head_sha", "evidence_head_sha", "approval_head_sha"):
            if item[field] is not None and not _is_sha(item[field], 40):
                raise SupervisorError("UNSUPPORTED_INPUT", f"issue {issue} has invalid {field}")
        for field in (
            "worktree_clean",
            "checks_passed",
            "evidence_verified",
            "reviews_current",
            "active",
        ):
            if not isinstance(item[field], bool):
                raise SupervisorError("UNSUPPORTED_INPUT", f"issue {issue} field {field} must be boolean")

    supervisor_id = inventory["supervisor_session_id"]
    if not isinstance(supervisor_id, str) or not supervisor_id:
        raise SupervisorError("MISSING_SESSION_STATE", "supervisor_session_id is required")
    seen_sessions: set[str] = set()
    for session in inventory["sessions"]:
        if not isinstance(session, dict):
            raise SupervisorError("UNSUPPORTED_INPUT", "session inventory entry must be an object")
        _assert_exact_keys(session, SESSION_KEYS, "session inventory entry")
        session_id = session["session_id"]
        if not isinstance(session_id, str) or not session_id or session_id in seen_sessions:
            raise SupervisorError("UNSUPPORTED_INPUT", "session IDs must be unique non-empty strings")
        seen_sessions.add(session_id)
        if session["role"] not in {"worker", "reviewer", "supervisor"}:
            raise SupervisorError("UNSUPPORTED_INPUT", f"session {session_id} has unknown role")
        if session["state"] not in policy["external_session_states"]:
            raise SupervisorError("MISSING_SESSION_STATE", f"session {session_id} has unknown state")
        if session["issue_number"] is not None and not isinstance(session["issue_number"], int):
            raise SupervisorError("UNSUPPORTED_INPUT", f"session {session_id} has invalid issue number")
        if session["head_sha"] is not None and not (
            isinstance(session["head_sha"], str) and SHA1_RE.fullmatch(session["head_sha"])
        ):
            raise SupervisorError("UNSUPPORTED_INPUT", f"session {session_id} has invalid head SHA")
        if not isinstance(session["delegation_id"], str) or not session["delegation_id"]:
            raise SupervisorError("UNSUPPORTED_INPUT", f"session {session_id} has invalid delegation ID")
        if not isinstance(session["policy_commit"], str) or not SHA1_RE.fullmatch(
            session["policy_commit"]
        ):
            raise SupervisorError("UNSUPPORTED_INPUT", f"session {session_id} has invalid policy commit")
        for field, prefix in (("cycle_id", "cycle-"), ("decision_id", "decision-")):
            value = session[field]
            if value is not None and (
                not isinstance(value, str) or not value.startswith(prefix)
            ):
                raise SupervisorError("UNSUPPORTED_INPUT", f"session {session_id} has invalid {field}")
        for field in ("cycle_record_digest", "dispatch_packet_digest"):
            value = session[field]
            if value is not None and not _is_sha(value, 64):
                raise SupervisorError(
                    "UNSUPPORTED_INPUT",
                    f"session {session_id} has invalid {field}",
                )
        if not isinstance(session["redirects"], int) or session["redirects"] < 0:
            raise SupervisorError("UNSUPPORTED_INPUT", f"session {session_id} has invalid redirect count")
        if not all(
            isinstance(session[field], bool)
            for field in ("dirty", "executing", "success_claimed")
        ):
            raise SupervisorError("UNSUPPORTED_INPUT", f"session {session_id} boolean fields are invalid")
        if session["executing"] is True and session["state"] != "RUNNING":
            raise SupervisorError(
                "UNSUPPORTED_INPUT",
                f"session {session_id} may execute only while RUNNING",
            )


def seal_record(
    record: dict[str, Any], *, supersedes: Sequence[str] = ()
) -> dict[str, Any]:
    """Seal a record with a digest over the complete payload using a zero placeholder."""
    sealed = copy.deepcopy(record)
    sealed["audit"] = {
        "immutable": True,
        "payload_digest": "0" * 64,
        "supersedes": list(supersedes),
    }
    sealed["audit"]["payload_digest"] = digest_json(sealed)
    _validate_schema(sealed, RECORD_SCHEMA_PATH, sealed.get("record_kind", "supervisor record"))
    return sealed


def _verify_record_payload(record: dict[str, Any]) -> None:
    _validate_schema(record, RECORD_SCHEMA_PATH, record.get("record_kind", "supervisor record"))
    expected = record["audit"]["payload_digest"]
    candidate = copy.deepcopy(record)
    candidate["audit"]["payload_digest"] = "0" * 64
    if digest_json(candidate) != expected:
        raise SupervisorError("AUDIT_DIGEST_MISMATCH", "durable record was modified after sealing")


def verify_record(
    record: dict[str, Any], *, prior_records: Sequence[dict[str, Any]] = ()
) -> None:
    _verify_record_payload(record)
    expected_superseded = set(record["audit"]["supersedes"])
    provided_superseded: set[str] = set()
    for prior in prior_records:
        _verify_record_payload(prior)
        provided_superseded.add(prior["audit"]["payload_digest"])
    if expected_superseded != provided_superseded:
        raise SupervisorError(
            "AUDIT_SUPERSESSION_MISMATCH",
            "supersession references must match verified prior records",
        )


def _minimal_worker_sessions(inventory: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "session_id": session["session_id"],
            "delegation_id": session["delegation_id"],
            "policy_commit": session["policy_commit"],
            "cycle_record_digest": session["cycle_record_digest"],
            "dispatch_packet_digest": session["dispatch_packet_digest"],
            "branch_ref": session["branch_ref"],
            "issue_number": session["issue_number"],
            "role": session["role"],
            "state": session["state"],
            "head_sha": session["head_sha"],
            "dirty": session["dirty"],
            "executing": session["executing"],
            "redirects": session["redirects"],
        }
        for session in inventory["sessions"]
        if session["role"] == "worker"
    ]


def _lifecycle_items(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "issue_number": item["issue_number"],
            "pull_request": item["pull_request"],
            "domain": item["domain"],
            "observed_state": item["lifecycle"],
            "head_sha": item["head_sha"],
            "evidence_head_sha": item["evidence_head_sha"],
            "approval_head_sha": item["approval_head_sha"],
        }
        for item in snapshot["items"]
    ]


def _decision_id(cycle_id: str, delegation_id: str) -> str:
    seed = digest_json(
        {
            "cycle_id": cycle_id,
            "delegation_id": delegation_id,
        }
    )
    return f"decision-{seed[:20]}"


def next_cycle_id(
    previous_record: dict[str, Any],
    sequence: int,
    *,
    delegation: dict[str, Any],
    delegation_binding: dict[str, Any],
    snapshot: dict[str, Any],
    inventory: dict[str, Any],
    recovery_record: dict[str, Any] | None,
) -> str:
    seed = digest_json(
        {
            "previous_record_digest": previous_record["audit"]["payload_digest"],
            "sequence": sequence,
            "delegation_digest": digest_json(delegation),
            "delegation_binding": delegation_binding,
            "snapshot_digest": digest_json(snapshot),
            "session_inventory_digest": digest_json(inventory),
            "recovery_record_digest": (
                recovery_record["audit"]["payload_digest"]
                if recovery_record is not None
                else None
            ),
        }
    )
    return f"cycle-{seed[:20]}"


def _dispatch_packet(
    policy: dict[str, Any],
    delegation: dict[str, Any],
    item: dict[str, Any],
    ready: dict[str, Any],
    delegation_binding: dict[str, Any],
    *,
    cycle_id: str,
    decision_id: str,
    target_session_id: str | None = None,
) -> dict[str, Any]:
    allowed = delegation["allowed_actions"]
    aic = delegation["limits"]["platform_reported_aic_guardrail"]
    observe_only = policy["invariants"]["operational_dispatch_mode"] == "observe-only"
    branch_seed = digest_json(
        {
            "delegation_id": delegation["delegation_id"],
            "issue_number": item["issue_number"],
            "base_sha": item["base_sha"],
        }
    )
    branch_ref = (
        f"refs/heads/copilot-supervisor/issue-{item['issue_number']}-{branch_seed[:12]}"
    )
    return {
        "delegation_id": delegation["delegation_id"],
        "delegation_digest": digest_json(delegation),
        "delegation_binding": copy.deepcopy(delegation_binding),
        "policy_binding": {
            "commit_sha": delegation["policy_binding"]["commit_sha"],
            "blob_sha256": delegation["policy_binding"]["blob_sha256"],
        },
        "cycle_id": cycle_id,
        "decision_id": decision_id,
        "target_session_id": target_session_id,
        "repository": delegation["repository"],
        "issue_number": item["issue_number"],
        "pull_request": item["pull_request"],
        "base_sha": item["base_sha"],
        "head_sha": item["head_sha"],
        "launch_head_sha": item["head_sha"] or item["base_sha"],
        "domain": item["domain"],
        "wip_slot": 1,
        "acceptance_manifest_digest": ready["acceptance_manifest"]["digest"],
        "required_evidence": list(policy["required_child_evidence"]),
        "allowed_tools": (
            ["repository-read"]
            if observe_only
            else [
                "repository-read",
                "scoped-repository-edit",
                "test-runner",
                "scoped-git-commit",
                "scoped-git-push-broker",
                "project-session-messaging",
            ]
        ),
        "mutation_constraints": {
            "allowed_branch_ref": branch_ref,
            "expected_old_sha": item["head_sha"] or item["base_sha"],
            "denied_ref_prefixes": [
                "refs/heads/master",
                "refs/heads/main",
                "refs/tags/",
            ],
            "force_push": False,
        },
        "allowed_mutations": [] if observe_only else list(allowed),
        "budgets": {
            "max_child_sessions": delegation["limits"]["max_child_sessions"],
            "max_session_creations": delegation["limits"]["max_session_creations"],
            "max_redirects_per_item": delegation["limits"]["max_redirects_per_item"],
            "max_cycle_seconds": delegation["limits"]["max_cycle_seconds"],
            "max_snapshot_age_seconds": delegation["limits"]["max_snapshot_age_seconds"],
            "max_platform_reported_aic": aic["max_aic"],
        },
        "expected_output": list(EXPECTED_OUTPUT),
        "prohibitions": list(policy["owner_only_actions"]),
        "fail_states": list(policy["fail_states"]),
    }


def reconcile(
    policy: dict[str, Any],
    delegation: dict[str, Any],
    snapshot: dict[str, Any],
    inventory: dict[str, Any],
    delegation_binding: dict[str, Any],
    *,
    now: datetime,
    cycle_id: str,
    sequence: int,
    previous_record: dict[str, Any] | None = None,
    recovery_record: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Derive one immutable execution-cycle plan from durable facts."""
    validate_snapshot(snapshot, inventory, policy, delegation)
    if sequence > 1 and previous_record is None:
        raise SupervisorError(
            "MISSING_PREVIOUS_RECORD",
            "every successor cycle requires its immediate predecessor",
        )
    previous_record_digest = None
    recovery_record_digest = None
    if previous_record is not None:
        verify_record(previous_record)
        if previous_record["record_kind"] != "external-supervisor-cycle":
            raise SupervisorError(
                "PREVIOUS_RECORD_INVALID",
                "reconciliation predecessor must be a cycle record",
            )
        if (
            previous_record["execution"]["supervisor_session_id"]
            != inventory["supervisor_session_id"]
        ):
            raise SupervisorError(
                "SUPERVISOR_IDENTITY_CHANGED",
                "supervisor identity cannot change within a delegation lineage",
            )
        if previous_record["sequence"] != sequence - 1:
            raise SupervisorError(
                "PREVIOUS_RECORD_INVALID",
                "cycle sequence must immediately follow its predecessor",
            )
        if (
            previous_record["delegation_id"] != delegation["delegation_id"]
            or previous_record["input_binding"]["delegation_digest"]
            != digest_json(delegation)
            or previous_record["delegation_binding"] != delegation_binding
            or previous_record["policy_binding"]["commit_sha"]
            != delegation["policy_binding"]["commit_sha"]
            or previous_record["policy_binding"]["blob_sha256"]
            != delegation["policy_binding"]["blob_sha256"]
        ):
            raise SupervisorError(
                "PREVIOUS_RECORD_INVALID",
                "cycle predecessor has different authority bindings",
            )
        previous_record_digest = previous_record["audit"]["payload_digest"]
    if recovery_record is not None:
        if previous_record is None:
            raise SupervisorError(
                "RECOVERY_RECORD_INVALID",
                "recovery continuation requires its predecessor cycle",
            )
        verify_record(recovery_record, prior_records=[previous_record])
        if recovery_record["record_kind"] != "external-supervisor-recovery":
            raise SupervisorError(
                "RECOVERY_RECORD_INVALID",
                "reconciliation recovery input must be a recovery record",
            )
        if (
            recovery_record["delegation_id"] != delegation["delegation_id"]
            or recovery_record["delegation_digest"] != digest_json(delegation)
            or recovery_record["delegation_binding"] != delegation_binding
            or recovery_record["policy_binding"]["commit_sha"]
            != delegation["policy_binding"]["commit_sha"]
            or recovery_record["policy_binding"]["blob_sha256"]
            != delegation["policy_binding"]["blob_sha256"]
        ):
            raise SupervisorError(
                "RECOVERY_AUTHORITY_MISMATCH",
                "recovery record has different authority bindings",
            )
        recovery_record_digest = recovery_record["audit"]["payload_digest"]
    if sequence == 1 and cycle_id != delegation["initial_cycle_id"]:
        raise SupervisorError(
            "INITIAL_CYCLE_MISMATCH",
            "sequence 1 must use the owner-bound initial cycle ID",
        )
    if sequence > 1 and previous_record is not None:
        expected_cycle_id = next_cycle_id(
            previous_record,
            sequence,
            delegation=delegation,
            delegation_binding=delegation_binding,
            snapshot=snapshot,
            inventory=inventory,
            recovery_record=recovery_record,
        )
        if cycle_id != expected_cycle_id:
            raise SupervisorError(
                "CYCLE_ID_MISMATCH",
                "cycle ID must be derived from the predecessor digest and sequence",
            )
    decision_id = _decision_id(cycle_id, delegation["delegation_id"])
    decision: dict[str, Any] = {
        "decision_id": decision_id,
        "state": "INDETERMINATE",
        "reason_code": "NO_DECISION",
        "selected_issue_number": None,
        "domain": None,
        "action": "NONE",
        "dispatch_packet": None,
        "owner_decision_required": False,
        "owner_decisions_allowed": [],
    }

    def stop(state: str, reason: str) -> None:
        decision.update(state=state, reason_code=reason)

    limits = delegation["limits"]
    sessions = inventory["sessions"]
    delegated_worker_sessions = [
        session
        for session in sessions
        if session["role"] == "worker"
        and session["session_id"] != inventory["supervisor_session_id"]
        and session["delegation_id"] == delegation["delegation_id"]
        and session["policy_commit"] == delegation["policy_binding"]["commit_sha"]
    ]
    active_sessions = [
        session
        for session in delegated_worker_sessions
        if session["state"] in RESERVING_SESSION_STATES
    ]
    observed_session_creations = len(
        {session["session_id"] for session in delegated_worker_sessions}
    )
    previous_session_creations = (
        previous_record["budget"]["session_creations_used"]
        if previous_record is not None
        else 0
    )
    session_creations_used = max(
        observed_session_creations,
        previous_session_creations,
    )
    current_redirects: dict[int, int] = {}
    for session in delegated_worker_sessions:
        issue = session["issue_number"]
        if issue is not None:
            current_redirects[issue] = (
                current_redirects.get(issue, 0) + session["redirects"]
            )
    previous_redirects: dict[int, int] = {}
    if previous_record is not None:
        previous_redirects = {
            entry["issue_number"]: entry["count"]
            for entry in previous_record["budget"]["redirect_counts"]
        }
    redirect_counter_regressed = False
    redirect_counter_regressed = any(
        issue in current_redirects and current_redirects[issue] < count
        for issue, count in previous_redirects.items()
    )
    redirect_high_water = {
        issue: max(current_redirects.get(issue, 0), previous_redirects.get(issue, 0))
        for issue in set(current_redirects) | set(previous_redirects)
    }
    redirects_used = sum(redirect_high_water.values())
    aic = snapshot["platform_reported_aic"]
    snapshot_observed_at = parse_timestamp(snapshot["observed_at"])
    inventory_observed_at = parse_timestamp(inventory["observed_at"])
    inventory_age = (now - inventory_observed_at).total_seconds()
    inventory_freshness_error = None
    if inventory_age < 0:
        inventory_freshness_error = ("INDETERMINATE", "SESSION_INVENTORY_FROM_FUTURE")
    elif inventory_age > limits["max_snapshot_age_seconds"]:
        inventory_freshness_error = ("INDETERMINATE", "SESSION_INVENTORY_STALE")
    pending_dispatch = (
        copy.deepcopy(previous_record["pending_dispatch"])
        if previous_record is not None
        else None
    )
    if recovery_record is not None:
        if (
            recovery_record["pending_dispatch"] != pending_dispatch
            or recovery_record["session_inventory_digest"] != digest_json(inventory)
            or recovery_record["session_inventory_observed_at"]
            != inventory["observed_at"]
        ):
            raise SupervisorError(
                "RECOVERY_INVENTORY_MISMATCH",
                "recovery must bind the exact pending dispatch and current inventory",
            )
        pending_dispatch = None
    if inventory_freshness_error is None and pending_dispatch is not None:
        dispatched_session_observed = (
            policy["mode"] == "operational-attested"
            and previous_record["authority"]["delegated_operational"]
            and any(
            session["role"] == "worker"
            and session["session_id"] != inventory["supervisor_session_id"]
            and session["issue_number"] == pending_dispatch["issue_number"]
            and session["delegation_id"] == delegation["delegation_id"]
            and session["policy_commit"]
            == delegation["policy_binding"]["commit_sha"]
            and session["cycle_id"] == pending_dispatch["cycle_id"]
            and session["decision_id"] == pending_dispatch["decision_id"]
            and session["dispatch_packet_digest"]
            == pending_dispatch["dispatch_packet_digest"]
            and session["cycle_record_digest"]
            == previous_record["audit"]["payload_digest"]
            and session["state"]
            not in {"PLANNED", "DISPATCH_REQUIRED", "UNKNOWN", "INDETERMINATE"}
            for session in sessions
        )
        )
        if dispatched_session_observed:
            pending_dispatch = None

    if inventory_freshness_error is not None:
        stop(*inventory_freshness_error)
    elif pending_dispatch is not None:
        stop("INDETERMINATE", "PRIOR_DISPATCH_UNRECEIPTED")
    elif redirect_counter_regressed:
        stop("INDETERMINATE", "REDIRECT_COUNTER_REGRESSION")
    elif any(
        session["state"] in {"UNKNOWN", "INDETERMINATE"}
        for session in active_sessions
    ):
        stop("INDETERMINATE", "MISSING_SESSION_STATE")
    elif not snapshot["audit_writable"]:
        stop("BLOCKED", "AUDIT_WRITE_UNAVAILABLE")
    elif snapshot_observed_at > now:
        stop("INDETERMINATE", "SNAPSHOT_FROM_FUTURE")
    elif (now - snapshot_observed_at).total_seconds() > limits["max_snapshot_age_seconds"]:
        stop("INDETERMINATE", "SNAPSHOT_STALE")
    elif snapshot["elapsed_seconds"] >= limits["max_cycle_seconds"]:
        stop("BLOCKED", "CYCLE_TIME_BUDGET_EXHAUSTED")
    elif session_creations_used > limits["max_session_creations"]:
        stop("BLOCKED", "SESSION_CREATION_LIMIT_EXCEEDED")
    elif len(active_sessions) > limits["max_child_sessions"]:
        stop("BLOCKED", "CHILD_SESSION_LIMIT_EXCEEDED")
    elif any(
        session["session_id"] == inventory["supervisor_session_id"]
        and session["role"] != "supervisor"
        for session in sessions
    ):
        stop("BLOCKED", "SUPERVISOR_SELF_DISPATCH")
    else:
        aic_guardrail = limits["platform_reported_aic_guardrail"]
        if aic_guardrail["enabled"] and not aic["reported"]:
            stop("INDETERMINATE", "AIC_TELEMETRY_UNAVAILABLE")
        elif aic_guardrail["enabled"] and aic["value"] >= aic_guardrail["max_aic"]:
            stop("BLOCKED", "AIC_BUDGET_EXHAUSTED")
        else:
            by_issue = {item["issue_number"]: item for item in snapshot["items"]}
            ready_validation_error = None
            for frozen_ready in delegation["scope"]["ready_order"]:
                frozen_item = by_issue.get(frozen_ready["issue_number"])
                if frozen_item is None:
                    ready_validation_error = ("INDETERMINATE", "MISSING_READY_FACTS")
                    break
                if frozen_item["issue_number"] not in delegation["scope"]["issues"]:
                    ready_validation_error = ("BLOCKED", "OUT_OF_SCOPE")
                    break
                if (
                    frozen_item["pull_request"] != frozen_ready["pull_request"]
                    or (
                        frozen_item["pull_request"] is not None
                        and frozen_item["pull_request"]
                        not in delegation["scope"]["pull_requests"]
                    )
                    or frozen_item["domain"] != frozen_ready["domain"]
                    or frozen_item["domain"] not in delegation["scope"]["domains"]
                ):
                    ready_validation_error = ("BLOCKED", "OUT_OF_SCOPE")
                    break
                if (
                    frozen_item["acceptance_manifest_digest"]
                    != frozen_ready["acceptance_manifest"]["digest"]
                ):
                    ready_validation_error = (
                        "BLOCKED",
                        "ACCEPTANCE_MANIFEST_DRIFT",
                    )
                    break
            unknown_dependencies = sorted(
                {
                    dependency
                    for ready in delegation["scope"]["ready_order"]
                    for dependency in ready["dependencies"]
                    if dependency not in by_issue
                }
                | {
                    dependency
                    for snapshot_item in snapshot["items"]
                    for dependency in snapshot_item["dependencies"]
                    if dependency not in by_issue
                }
            )
            live_workers = [
                session
                for session in sessions
                if session["role"] == "worker"
                and session["session_id"] != inventory["supervisor_session_id"]
                and session["state"] in RESERVING_SESSION_STATES
            ]
            missing_session_facts = [
                session["session_id"]
                for session in live_workers
                if session["issue_number"] is None
                or session["issue_number"] not in by_issue
            ]
            stale_session_bindings = [
                session["session_id"]
                for session in live_workers
                if session["issue_number"] in by_issue
                and (
                    session["delegation_id"] != delegation["delegation_id"]
                    or session["policy_commit"]
                    != delegation["policy_binding"]["commit_sha"]
                )
            ]
            domain_active: dict[str, list[int]] = {}
            domain_reserved: dict[str, list[int]] = {}
            for session in inventory["sessions"]:
                session_item = by_issue.get(session["issue_number"])
                if (
                    session_item is not None
                    and session["role"] == "worker"
                    and session["session_id"] != inventory["supervisor_session_id"]
                    and session["state"] in RESERVING_SESSION_STATES
                    and session["delegation_id"] == delegation["delegation_id"]
                    and session["policy_commit"]
                    == delegation["policy_binding"]["commit_sha"]
                ):
                    domain_reserved.setdefault(session_item["domain"], []).append(
                        session_item["issue_number"]
                    )
                if (
                    session_item is not None
                    and session["role"] == "worker"
                    and session["session_id"] != inventory["supervisor_session_id"]
                    and session["state"] == "RUNNING"
                    and session["executing"] is True
                    and session["delegation_id"] == delegation["delegation_id"]
                    and session["policy_commit"] == delegation["policy_binding"]["commit_sha"]
                ):
                    domain_active.setdefault(session_item["domain"], []).append(
                        session_item["issue_number"]
                    )
            collisions = {
                domain: issues
                for domain, issues in domain_active.items()
                if len(set(issues)) > limits["max_active_per_domain"]
            }
            if ready_validation_error is not None:
                stop(*ready_validation_error)
            elif missing_session_facts:
                stop("INDETERMINATE", "MISSING_SESSION_FACTS")
            elif stale_session_bindings:
                stop("INDETERMINATE", "STALE_SESSION_BINDING")
            elif unknown_dependencies:
                stop("INDETERMINATE", "UNKNOWN_DEPENDENCY")
            elif collisions:
                stop("BLOCKED", "WIP_COLLISION")
            else:
                ready = None
                item = None
                skipped_running = False
                skipped_terminal = False
                skipped_blocked = False
                for candidate_ready in delegation["scope"]["ready_order"]:
                    candidate = by_issue[candidate_ready["issue_number"]]
                    if any(
                        by_issue[dependency]["lifecycle"] not in TERMINAL_DEPENDENCY_STATES
                        for dependency in set(candidate_ready["dependencies"])
                        | set(candidate["dependencies"])
                    ):
                        continue
                    if candidate["lifecycle"] in {"MERGED", "RELEASED"}:
                        skipped_terminal = True
                        continue
                    if candidate["lifecycle"] in {"BLOCKED", "PARKED"}:
                        skipped_blocked = True
                        continue
                    if any(
                        session["role"] == "worker"
                        and session["issue_number"] == candidate["issue_number"]
                        and session["delegation_id"] == delegation["delegation_id"]
                        and session["policy_commit"]
                        == delegation["policy_binding"]["commit_sha"]
                        and session["state"] == "RUNNING"
                        and session["executing"] is True
                        and session["success_claimed"] is False
                        for session in sessions
                    ):
                        skipped_running = True
                        continue
                    occupied_by = set(domain_active.get(candidate["domain"], []))
                    if occupied_by - {candidate["issue_number"]}:
                        continue
                    reserved_by = set(domain_reserved.get(candidate["domain"], []))
                    if reserved_by - {candidate["issue_number"]}:
                        continue
                    ready = candidate_ready
                    item = candidate
                    break

                if ready is None or item is None:
                    if decision["reason_code"] == "NO_DECISION":
                        if skipped_running:
                            stop("RUNNING", "EXISTING_CHILDREN_RUNNING")
                        elif skipped_blocked:
                            stop("BLOCKED", "NO_ELIGIBLE_READY_ITEM")
                        elif skipped_terminal:
                            stop("CLOSED", "READY_SET_TERMINAL")
                        else:
                            stop("BLOCKED", "NO_ELIGIBLE_READY_ITEM")
                else:
                    decision["selected_issue_number"] = item["issue_number"]
                    decision["domain"] = item["domain"]
                    item_sessions = [
                        session
                        for session in sessions
                        if session["role"] == "worker"
                        and session["issue_number"] == item["issue_number"]
                        and session["delegation_id"] == delegation["delegation_id"]
                        and session["policy_commit"] == delegation["policy_binding"]["commit_sha"]
                    ]
                    live_sessions = [
                        session
                        for session in item_sessions
                        if session["state"] in RESERVING_SESSION_STATES
                    ]
                    recovery_sessions = [
                        session
                        for session in item_sessions
                        if session["state"] in {"BLOCKED", "FAILED"}
                    ]
                    item_redirects = redirect_high_water.get(item["issue_number"], 0)
                    current = (
                        live_sessions[0]
                        if len(live_sessions) == 1
                        else recovery_sessions[0]
                        if not live_sessions and len(recovery_sessions) == 1
                        else None
                    )
                    if len(live_sessions) > 1:
                        stop("BLOCKED", "SESSION_COLLISION")
                    elif not live_sessions and len(recovery_sessions) > 1:
                        stop("INDETERMINATE", "SESSION_HISTORY_AMBIGUOUS")
                    elif current and current["session_id"] == inventory["supervisor_session_id"]:
                        stop("BLOCKED", "SUPERVISOR_SELF_DISPATCH")
                    elif current and current["dirty"]:
                        stop("INDETERMINATE", "DIRTY_WORKTREE_NOT_EVIDENCE")
                    elif current and current["success_claimed"] and not item["evidence_verified"]:
                        stop("BLOCKED", "CHILD_SUCCESS_UNVERIFIED")
                    elif item["head_sha"] is not None and (
                            (item["evidence_head_sha"] is not None and item["evidence_head_sha"] != item["head_sha"])
                            or (item["approval_head_sha"] is not None and item["approval_head_sha"] != item["head_sha"])
                        ):
                        stop("INDETERMINATE", "STALE_HEAD")
                    elif not item["checks_passed"] and item["lifecycle"] in {"REVIEW", "APPROVED"}:
                        stop("BLOCKED", "FAILED_CHECKS")
                    elif not item["reviews_current"] and item["lifecycle"] in {"REVIEW", "APPROVED"}:
                        stop("BLOCKED", "STALE_REVIEW")
                    elif item["lifecycle"] in {"MERGED", "RELEASED"}:
                        decision.update(
                                state="CLOSED",
                                reason_code="REPOSITORY_LIFECYCLE_TERMINAL",
                                action="NONE",
                            )
                    elif item["lifecycle"] == "APPROVED":
                        if (
                            item["head_sha"] is None
                            or not item["evidence_verified"]
                            or item["evidence_head_sha"] != item["head_sha"]
                            or item["approval_head_sha"] != item["head_sha"]
                            or current is None
                            or (
                                current["head_sha"] != item["head_sha"]
                            )
                        ):
                            stop("INDETERMINATE", "MISSING_EXACT_HEAD")
                        else:
                            decision.update(
                                state="REVIEW_WAIT",
                                reason_code="OWNER_MERGE_DECISION_REQUIRED",
                                owner_decision_required=True,
                                owner_decisions_allowed=["merge"],
                            )
                    elif current and current["state"] == "FAILED":
                        if item_redirects >= limits["max_redirects_per_item"]:
                            stop("BLOCKED", "REDIRECT_LIMIT_EXHAUSTED")
                        elif "redirect-child-session" not in delegation["allowed_actions"]:
                            stop("BLOCKED", "REDIRECT_NOT_DELEGATED")
                        else:
                            decision.update(
                                    state="DISPATCH_REQUIRED",
                                    reason_code="FAILED_CHILD_REDIRECT_ALLOWED",
                                    action="REDIRECT_CHILD",
                                    dispatch_packet=_dispatch_packet(
                                        policy,
                                        delegation,
                                        item,
                                        ready,
                                        delegation_binding,
                                        cycle_id=cycle_id,
                                        decision_id=decision_id,
                                        target_session_id=current["session_id"],
                                    ),
                                )
                    elif current and current["state"] == "RUNNING":
                        if not current["executing"]:
                            stop("INDETERMINATE", "RUNNING_SESSION_NOT_EXECUTING")
                        else:
                            decision.update(
                                state="RUNNING",
                                reason_code="CHILD_RUNNING",
                            )
                    elif current and current["state"] == "DISPATCHED":
                        decision.update(
                            state="DISPATCHED",
                            reason_code="DISPATCH_RECEIPT_REQUIRED",
                        )
                    elif current and current["state"] in {
                        "PLANNED",
                        "DISPATCH_REQUIRED",
                        "UNKNOWN",
                        "INDETERMINATE",
                    }:
                        stop("INDETERMINATE", "MISSING_SESSION_STATE")
                    elif current and current["state"] == "IDLE":
                        required = {
                            "resume-isolated-child-session",
                            "send-bounded-child-instructions",
                        }
                        if not required <= set(delegation["allowed_actions"]):
                            stop("BLOCKED", "RESUME_ACTION_NOT_DELEGATED")
                        else:
                            decision.update(
                                state="DISPATCH_REQUIRED",
                                reason_code="IDLE_CHILD_RESUME_ALLOWED",
                                action="RESUME_CHILD",
                                dispatch_packet=_dispatch_packet(
                                    policy,
                                    delegation,
                                    item,
                                    ready,
                                    delegation_binding,
                                    cycle_id=cycle_id,
                                    decision_id=decision_id,
                                    target_session_id=current["session_id"],
                                ),
                            )
                    elif current and current["state"] == "REDIRECT_REQUIRED":
                        if item_redirects >= limits["max_redirects_per_item"]:
                            stop("BLOCKED", "REDIRECT_LIMIT_EXHAUSTED")
                        elif "redirect-child-session" not in delegation["allowed_actions"]:
                            stop("BLOCKED", "REDIRECT_NOT_DELEGATED")
                        else:
                            decision.update(
                                state="DISPATCH_REQUIRED",
                                reason_code="CHILD_REDIRECT_REQUIRED",
                                action="REDIRECT_CHILD",
                                dispatch_packet=_dispatch_packet(
                                    policy,
                                    delegation,
                                    item,
                                    ready,
                                    delegation_binding,
                                    cycle_id=cycle_id,
                                    decision_id=decision_id,
                                    target_session_id=current["session_id"],
                                ),
                            )
                    elif current and current["state"] == "BLOCKED":
                        required = {
                            "replace-child-session",
                            "send-bounded-child-instructions",
                        }
                        if item_redirects >= limits["max_redirects_per_item"]:
                            stop("BLOCKED", "REDIRECT_LIMIT_EXHAUSTED")
                        elif len(active_sessions) >= limits["max_child_sessions"]:
                            stop("BLOCKED", "CHILD_SESSION_LIMIT_EXHAUSTED")
                        elif session_creations_used >= limits["max_session_creations"]:
                            stop("BLOCKED", "SESSION_CREATION_LIMIT_EXHAUSTED")
                        elif not required <= set(delegation["allowed_actions"]):
                            stop("BLOCKED", "REPLACE_ACTION_NOT_DELEGATED")
                        else:
                            decision.update(
                                state="DISPATCH_REQUIRED",
                                reason_code="BLOCKED_CHILD_REPLACEMENT_ALLOWED",
                                action="REPLACE_CHILD",
                                dispatch_packet=_dispatch_packet(
                                    policy,
                                    delegation,
                                    item,
                                    ready,
                                    delegation_binding,
                                    cycle_id=cycle_id,
                                    decision_id=decision_id,
                                    target_session_id=current["session_id"],
                                ),
                            )
                    elif current and current["state"] == "EVIDENCE_READY":
                        if item["head_sha"] is None:
                            stop("INDETERMINATE", "MISSING_EXACT_HEAD")
                        elif current["head_sha"] != item["head_sha"]:
                            stop("INDETERMINATE", "STALE_HEAD")
                        elif not item["worktree_clean"]:
                            stop("INDETERMINATE", "DIRTY_WORKTREE_NOT_EVIDENCE")
                        elif not item["evidence_verified"] or item["evidence_head_sha"] != item["head_sha"]:
                            stop("INDETERMINATE", "STALE_HEAD")
                        elif not item["checks_passed"]:
                            stop("BLOCKED", "FAILED_CHECKS")
                        else:
                            decision.update(
                                    state="REVIEW_WAIT",
                                    reason_code="EXACT_HEAD_EVIDENCE_READY",
                                    action=(
                                        "REQUEST_REVIEW"
                                        if "request-review" in delegation["allowed_actions"]
                                        else "NONE"
                                    ),
                                )
                    elif current and current["state"] == "REVIEW_WAIT":
                        if (
                            item["head_sha"] is None
                            or current["head_sha"] != item["head_sha"]
                            or not item["evidence_verified"]
                            or item["evidence_head_sha"] != item["head_sha"]
                        ):
                            stop("INDETERMINATE", "STALE_HEAD")
                        else:
                            decision.update(
                                state="REVIEW_WAIT",
                                reason_code="CHILD_REVIEW_WAIT",
                                action=(
                                    "REQUEST_REVIEW"
                                    if "request-review" in delegation["allowed_actions"]
                                    else "NONE"
                                ),
                            )
                    elif item["lifecycle"] == "ACTIVE":
                        stop("UNKNOWN", "MISSING_SESSION_STATE")
                    elif item["lifecycle"] == "REVIEW":
                        if item["head_sha"] is None:
                            stop("INDETERMINATE", "MISSING_EXACT_HEAD")
                        elif not item["evidence_verified"] or item["evidence_head_sha"] != item["head_sha"]:
                            stop("INDETERMINATE", "STALE_HEAD")
                        else:
                            decision.update(
                                    state="REVIEW_WAIT",
                                    reason_code="REVIEW_PENDING_OWNER_OR_REVIEWER",
                                    owner_decision_required=True,
                                    owner_decisions_allowed=["approve-pull-request"],
                                    action=(
                                        "REQUEST_REVIEW"
                                        if "request-review" in delegation["allowed_actions"]
                                        else "NONE"
                                    ),
                                )
                    elif item["lifecycle"] != "READY":
                        stop("BLOCKED", "OWNER_READY_AUTHORIZATION_REQUIRED")
                    elif domain_active.get(item["domain"]):
                        stop("BLOCKED", "WIP_SLOT_UNAVAILABLE")
                    elif len(active_sessions) >= limits["max_child_sessions"]:
                        stop("BLOCKED", "CHILD_SESSION_LIMIT_EXHAUSTED")
                    elif session_creations_used >= limits["max_session_creations"]:
                        stop("BLOCKED", "SESSION_CREATION_LIMIT_EXHAUSTED")
                    else:
                        required = {
                                "select-owner-approved-ready-item",
                                "create-isolated-child-session",
                                "send-bounded-child-instructions",
                            }
                        if not required <= set(delegation["allowed_actions"]):
                            stop("BLOCKED", "DISPATCH_ACTION_NOT_DELEGATED")
                        else:
                            decision.update(
                                    state="DISPATCH_REQUIRED",
                                    reason_code="OWNER_APPROVED_READY_ITEM_SELECTED",
                                    action="CREATE_CHILD",
                                    dispatch_packet=_dispatch_packet(
                                        policy,
                                        delegation,
                                        item,
                                        ready,
                                        delegation_binding,
                                        cycle_id=cycle_id,
                                        decision_id=decision_id,
                                    ),
                                )

    if (
        decision["action"] in {"REDIRECT_CHILD", "REPLACE_CHILD"}
        and decision["selected_issue_number"] is not None
    ):
        issue = decision["selected_issue_number"]
        redirect_high_water[issue] = redirect_high_water.get(issue, 0) + 1
        redirects_used = sum(redirect_high_water.values())
    if decision["action"] in {"CREATE_CHILD", "REPLACE_CHILD"}:
        session_creations_used += 1
    if (
        decision["state"] == "DISPATCH_REQUIRED"
        and decision["action"]
        in {"CREATE_CHILD", "RESUME_CHILD", "REDIRECT_CHILD", "REPLACE_CHILD"}
    ):
        pending_dispatch = {
            "cycle_id": cycle_id,
            "decision_id": decision["decision_id"],
            "issue_number": decision["selected_issue_number"],
            "dispatch_packet_digest": digest_json(decision["dispatch_packet"]),
        }

    record = {
        "schema_version": 1,
        "record_kind": "external-supervisor-cycle",
        "cycle_id": cycle_id,
        "sequence": sequence,
        "created_at": format_timestamp(now),
        "repository": delegation["repository"],
        "delegation_id": delegation["delegation_id"],
        "delegation_binding": copy.deepcopy(delegation_binding),
        "policy_binding": {
            "commit_sha": delegation["policy_binding"]["commit_sha"],
            "blob_sha256": delegation["policy_binding"]["blob_sha256"],
        },
        "input_binding": {
            "delegation_digest": digest_json(delegation),
            "snapshot_digest": digest_json(snapshot),
            "snapshot_observed_at": snapshot["observed_at"],
            "session_inventory_digest": digest_json(inventory),
            "previous_record_digest": previous_record_digest,
            "recovery_record_digest": recovery_record_digest,
        },
        "authority": {
            "advisory": True,
            "delegated_operational": (
                policy["invariants"]["operational_dispatch_mode"] != "observe-only"
                and decision["state"]
                not in {"BLOCKED", "INDETERMINATE", "UNKNOWN", "REVOKED"}
            ),
            "owner_only_actions_denied": list(policy["owner_only_actions"]),
            "scope_issues": list(delegation["scope"]["issues"]),
            "scope_pull_requests": list(delegation["scope"]["pull_requests"]),
        },
        "repository_lifecycle": _lifecycle_items(snapshot),
        "execution": {
            "supervisor_session_id": inventory["supervisor_session_id"],
            "state": decision["state"],
            "worker_sessions": _minimal_worker_sessions(inventory),
        },
        "decision": decision,
        "pending_dispatch": pending_dispatch,
        "budget": {
            "active_child_sessions": len(active_sessions),
            "session_creations_used": session_creations_used,
            "redirects_used": redirects_used,
            "redirect_counts": [
                {"issue_number": issue, "count": redirect_high_water[issue]}
                for issue in sorted(redirect_high_water)
            ],
            "elapsed_seconds": snapshot["elapsed_seconds"],
            "platform_reported_aic": aic,
        },
    }
    return seal_record(record)


def create_dispatch_receipt(
    cycle: dict[str, Any],
    inventory: dict[str, Any],
    *,
    policy: dict[str, Any],
    delegation: dict[str, Any],
    delegation_binding: dict[str, Any],
    child_session_id: str,
    now: datetime,
) -> dict[str, Any]:
    """Bind a dispatch to the same decision and an independently RUNNING child."""
    if cycle.get("record_kind") != "external-supervisor-cycle":
        raise SupervisorError(
            "DISPATCH_RECEIPT_SOURCE_INVALID",
            "dispatch receipt source must be a cycle record",
        )
    verify_record(cycle)
    if (
        policy.get("mode") != "operational-attested"
        or not cycle["authority"]["delegated_operational"]
    ):
        raise SupervisorError(
            "OBSERVE_ONLY",
            "observe/validation-only projections cannot receive execution receipts",
        )
    _validate_schema(
        delegation,
        DELEGATION_SCHEMA_PATH,
        "external supervisor delegation",
    )
    if (
        cycle["delegation_id"] != delegation["delegation_id"]
        or cycle["input_binding"]["delegation_digest"] != digest_json(delegation)
        or cycle["delegation_binding"] != delegation_binding
        or cycle["policy_binding"]["commit_sha"]
        != delegation["policy_binding"]["commit_sha"]
        or cycle["policy_binding"]["blob_sha256"]
        != delegation["policy_binding"]["blob_sha256"]
    ):
        raise SupervisorError(
            "RECEIPT_AUTHORITY_MISMATCH",
            "dispatch receipt authority differs from the cycle",
        )
    if delegation["revocation"]["revoked"]:
        raise SupervisorError("DELEGATION_REVOKED", "owner revoked this delegation")
    if now < parse_timestamp(delegation["authorized_at"]) or now >= parse_timestamp(
        delegation["expires_at"]
    ):
        raise SupervisorError(
            "DELEGATION_EXPIRED",
            "delegation is not active at receipt time",
        )
    cycle_created_at = parse_timestamp(cycle["created_at"])
    receipt_age = (now - cycle_created_at).total_seconds()
    if receipt_age < 0 or receipt_age > delegation["limits"]["max_cycle_seconds"]:
        raise SupervisorError(
            "RECEIPT_CYCLE_STALE",
            "dispatch receipt is outside the delegated cycle window",
        )
    if cycle["record_kind"] != "external-supervisor-cycle":
        raise SupervisorError("RECEIPT_CYCLE_INVALID", "dispatch receipt requires a cycle record")
    decision = cycle["decision"]
    packet = decision["dispatch_packet"]
    if decision["state"] != "DISPATCH_REQUIRED" or packet is None:
        raise SupervisorError("RECEIPT_DECISION_INVALID", "cycle has no dispatchable decision")
    _assert_exact_keys(inventory, INVENTORY_TOP_KEYS, "session inventory")
    inventory_age = (now - parse_timestamp(inventory["observed_at"])).total_seconds()
    if inventory_age < 0 or inventory_age > delegation["limits"]["max_snapshot_age_seconds"]:
        raise SupervisorError(
            "RECEIPT_SESSION_INVENTORY_STALE",
            "dispatch receipt requires a fresh session inventory",
        )
    cycle_record_digest = cycle["audit"]["payload_digest"]
    dispatch_packet_digest = digest_json(packet)
    target_session_id = packet["target_session_id"]
    if decision["action"] in {"RESUME_CHILD", "REDIRECT_CHILD"}:
        if child_session_id != target_session_id:
            raise SupervisorError(
                "DISPATCH_RECEIPT_MISMATCH",
                "resume or redirect receipt must bind the exact selected session",
            )
    elif decision["action"] == "REPLACE_CHILD":
        if target_session_id is None or child_session_id == target_session_id:
            raise SupervisorError(
                "DISPATCH_RECEIPT_MISMATCH",
                "replacement receipt must bind a new child and name the replaced session",
            )
    elif decision["action"] == "CREATE_CHILD" and target_session_id is not None:
        raise SupervisorError(
            "DISPATCH_RECEIPT_MISMATCH",
            "new child dispatch must not name an existing target session",
        )
    if child_session_id == inventory["supervisor_session_id"]:
        raise SupervisorError("SUPERVISOR_SELF_DISPATCH", "supervisor cannot satisfy its own dispatch")
    matches = [
        session for session in inventory["sessions"] if session.get("session_id") == child_session_id
    ]
    if len(matches) != 1:
        raise SupervisorError("MISSING_SESSION_STATE", "child session is absent or duplicated")
    session = matches[0]
    _assert_exact_keys(session, SESSION_KEYS, "session inventory entry")
    decision_matches = [
        candidate
        for candidate in inventory["sessions"]
        if candidate.get("role") == "worker"
        and candidate.get("issue_number") == packet["issue_number"]
        and candidate.get("delegation_id") == packet["delegation_id"]
        and candidate.get("policy_commit") == packet["policy_binding"]["commit_sha"]
        and candidate.get("cycle_id") == packet["cycle_id"]
        and candidate.get("decision_id") == packet["decision_id"]
        and candidate.get("cycle_record_digest") == cycle_record_digest
        and candidate.get("dispatch_packet_digest") == dispatch_packet_digest
        and candidate.get("state") == "RUNNING"
        and candidate.get("executing") is True
    ]
    if len(decision_matches) != 1:
        raise SupervisorError(
            "DISPATCH_RECEIPT_COLLISION",
            "dispatch decision must bind exactly one executing child",
        )
    expected = {
        "role": "worker",
        "issue_number": packet["issue_number"],
        "state": "RUNNING",
        "executing": True,
        "delegation_id": packet["delegation_id"],
        "policy_commit": packet["policy_binding"]["commit_sha"],
        "cycle_id": packet["cycle_id"],
        "decision_id": packet["decision_id"],
        "cycle_record_digest": cycle_record_digest,
        "dispatch_packet_digest": dispatch_packet_digest,
        "branch_ref": packet["mutation_constraints"]["allowed_branch_ref"],
    }
    mismatches = [key for key, value in expected.items() if session.get(key) != value]
    if mismatches:
        raise SupervisorError(
            "DISPATCH_RECEIPT_MISMATCH",
            f"child does not match same-cycle dispatch fields {mismatches}",
        )
    if session["head_sha"] != packet["launch_head_sha"]:
        raise SupervisorError(
            "DISPATCH_RECEIPT_HEAD_MISMATCH",
            "child head differs from the exact launch head bound by the dispatch packet",
        )
    seed = digest_json(
        {
            "cycle": cycle["audit"]["payload_digest"],
            "session": session,
            "inventory": digest_json(inventory),
        }
    )
    return seal_record(
        {
            "schema_version": 1,
            "record_kind": "external-supervisor-dispatch",
            "receipt_id": f"receipt-{seed[:20]}",
            "created_at": format_timestamp(now),
            "delegation_id": packet["delegation_id"],
            "delegation_digest": cycle["input_binding"]["delegation_digest"],
            "delegation_binding": copy.deepcopy(delegation_binding),
            "policy_binding": packet["policy_binding"],
            "cycle_id": packet["cycle_id"],
            "decision_id": packet["decision_id"],
            "cycle_record_digest": cycle_record_digest,
            "dispatch_packet_digest": dispatch_packet_digest,
            "dispatched_head_sha": packet["launch_head_sha"],
            "dispatched_branch_ref": packet["mutation_constraints"]["allowed_branch_ref"],
            "child_session_id": child_session_id,
            "issue_number": packet["issue_number"],
            "state": "RUNNING",
            "executing": True,
            "session_inventory_digest": digest_json(inventory),
        }
    )


def create_recovery_record(
    delegation: dict[str, Any],
    *,
    cause: str,
    state: str,
    previous_records: Sequence[dict[str, Any]],
    inventory: dict[str, Any],
    delegation_binding: dict[str, Any],
    now: datetime,
) -> dict[str, Any]:
    """Create a crash-recovery record that depends only on durable facts."""
    if len(previous_records) != 1:
        raise SupervisorError(
            "RECOVERY_PREDECESSOR_COUNT",
            "recovery must supersede exactly one immediate predecessor",
        )
    previous_record_digests = []
    _assert_exact_keys(inventory, INVENTORY_TOP_KEYS, "session inventory")
    inventory_age = (now - parse_timestamp(inventory["observed_at"])).total_seconds()
    if inventory_age < 0 or inventory_age > delegation["limits"]["max_snapshot_age_seconds"]:
        raise SupervisorError(
            "RECOVERY_SESSION_INVENTORY_STALE",
            "recovery requires a fresh session inventory",
        )
    for previous in previous_records:
        if previous.get("record_kind") != "external-supervisor-cycle":
            raise SupervisorError(
                "RECOVERY_PREVIOUS_RECORD_INVALID",
                "recovery predecessor must be a cycle record",
            )
        verify_record(previous)
        if (
            previous["delegation_id"] != delegation["delegation_id"]
            or previous["input_binding"]["delegation_digest"]
            != digest_json(delegation)
            or previous["delegation_binding"] != delegation_binding
            or previous["policy_binding"]["commit_sha"]
            != delegation["policy_binding"]["commit_sha"]
            or previous["policy_binding"]["blob_sha256"]
            != delegation["policy_binding"]["blob_sha256"]
        ):
            raise SupervisorError(
                "RECOVERY_AUTHORITY_MISMATCH",
                "recovery may supersede only records under the same authority",
            )
        previous_record_digests.append(previous["audit"]["payload_digest"])
    latest = previous_records[-1] if previous_records else None
    pending_dispatch = latest.get("pending_dispatch") if latest is not None else None
    if pending_dispatch is not None:
        matching_workers = [
            session
            for session in inventory["sessions"]
            if session.get("role") == "worker"
            and session.get("session_id") != inventory["supervisor_session_id"]
            and session.get("delegation_id") == delegation["delegation_id"]
            and session.get("policy_commit")
            == delegation["policy_binding"]["commit_sha"]
            and session.get("issue_number") == pending_dispatch["issue_number"]
            and session.get("cycle_id") == pending_dispatch["cycle_id"]
            and session.get("decision_id") == pending_dispatch["decision_id"]
            and session.get("dispatch_packet_digest")
            == pending_dispatch["dispatch_packet_digest"]
            and session.get("cycle_record_digest") == latest["audit"]["payload_digest"]
            and session.get("state") not in {"CLOSED", "REVOKED"}
        ]
        if matching_workers:
            raise SupervisorError(
                "RECOVERY_DISPATCH_STILL_OBSERVED",
                "recovery cannot clear a dispatch while its worker is still observed",
            )
    seed = digest_json(
        {
            "delegation": delegation["delegation_id"],
            "cause": cause,
            "pending_dispatch": pending_dispatch,
            "session_inventory_digest": digest_json(inventory),
            "session_inventory_observed_at": inventory["observed_at"],
            "previous": previous_record_digests,
            "created_at": format_timestamp(now),
        }
    )
    return seal_record(
        {
            "schema_version": 1,
            "record_kind": "external-supervisor-recovery",
            "recovery_id": f"recovery-{seed[:20]}",
            "created_at": format_timestamp(now),
            "delegation_id": delegation["delegation_id"],
            "delegation_digest": digest_json(delegation),
            "delegation_binding": copy.deepcopy(delegation_binding),
            "policy_binding": {
                "commit_sha": delegation["policy_binding"]["commit_sha"],
                "blob_sha256": delegation["policy_binding"]["blob_sha256"],
            },
            "cause": cause,
            "pending_dispatch": pending_dispatch,
            "session_inventory_digest": digest_json(inventory),
            "session_inventory_observed_at": inventory["observed_at"],
            "previous_record_digests": previous_record_digests,
            "reconstructed_from": [
                "delegation",
                "cycle-records",
                "github-facts",
                "git-state",
                "session-inventory",
            ],
            "parked_and_failed_history_preserved": True,
            "state": state,
        },
        supersedes=previous_record_digests,
    )


def create_owner_decision_packet(
    cycle: dict[str, Any],
    *,
    policy: dict[str, Any],
    delegation: dict[str, Any],
    delegation_binding: dict[str, Any],
    issue_number: int,
    pull_request: int | None,
    exact_head_sha: str | None,
    requested_owner_decisions: list[str],
    reason: str,
    now: datetime,
) -> dict[str, Any]:
    """Request owner-only decisions without making them."""
    if cycle.get("record_kind") != "external-supervisor-cycle":
        raise SupervisorError(
            "OWNER_DECISION_SOURCE_INVALID",
            "owner decision source must be a cycle record",
        )
    verify_record(cycle)
    _validate_schema(
        delegation,
        DELEGATION_SCHEMA_PATH,
        "external supervisor delegation",
    )
    if (
        cycle["repository"] != policy["repository"]
        or cycle["delegation_id"] != delegation["delegation_id"]
        or cycle["input_binding"]["delegation_digest"] != digest_json(delegation)
        or cycle["delegation_binding"] != delegation_binding
        or cycle["policy_binding"]["commit_sha"]
        != delegation["policy_binding"]["commit_sha"]
        or cycle["policy_binding"]["blob_sha256"]
        != delegation["policy_binding"]["blob_sha256"]
    ):
        raise SupervisorError(
            "OWNER_DECISION_AUTHORITY_MISMATCH",
            "owner packet authority differs from the validated cycle",
        )
    allowed = set(cycle["authority"]["owner_only_actions_denied"])
    if not requested_owner_decisions or not set(requested_owner_decisions) <= allowed:
        raise SupervisorError("OWNER_DECISION_INVALID", "packet may request only owner-only decisions")
    matching_facts = [
        item
        for item in cycle["repository_lifecycle"]
        if item["issue_number"] == issue_number
        and item["pull_request"] == pull_request
        and item["head_sha"] == exact_head_sha
    ]
    if len(matching_facts) != 1:
        raise SupervisorError(
            "OWNER_DECISION_INVALID",
            "owner decision packet must bind exact facts from the cycle",
        )
    exact_head_actions = {
        "approve-pull-request",
        "merge",
        "tag-release",
        "publish-release",
        "approve-compatibility-exception",
        "approve-risk-exception",
        "approve-emergency-exception",
    }
    decision_bound_actions = allowed - {"tag-release", "publish-release"}
    requested = set(requested_owner_decisions)
    observed_state = matching_facts[0]["observed_state"]
    if (
        issue_number not in cycle["authority"]["scope_issues"]
        or (
            pull_request is not None
            and pull_request not in cycle["authority"]["scope_pull_requests"]
        )
    ):
        raise SupervisorError(
            "OWNER_DECISION_INVALID",
            "owner decision packet target is outside the delegated scope",
        )
    if requested & exact_head_actions and exact_head_sha is None:
        raise SupervisorError(
            "OWNER_DECISION_INVALID",
            "approval, merge, and release decisions require a non-null exact head",
        )
    if requested & decision_bound_actions:
        if not cycle["decision"]["owner_decision_required"] or (
            cycle["decision"]["selected_issue_number"] != issue_number
        ) or not requested <= set(cycle["decision"]["owner_decisions_allowed"]):
            raise SupervisorError(
                "OWNER_DECISION_INVALID",
                "decision must be explicitly requested for the same issue by the source cycle",
            )
    if "approve-pull-request" in requested and observed_state not in {
        "REVIEW",
        "APPROVED",
    }:
        raise SupervisorError(
            "OWNER_DECISION_INVALID",
            "pull-request approval requires REVIEW or APPROVED lifecycle facts",
        )
    if "merge" in requested and observed_state != "APPROVED":
        raise SupervisorError(
            "OWNER_DECISION_INVALID",
            "merge requires exact-head APPROVED lifecycle facts",
        )
    if requested & {"tag-release", "publish-release"} and observed_state not in {
        "MERGED",
        "RELEASED",
    }:
        raise SupervisorError(
            "OWNER_DECISION_INVALID",
            "release decisions require MERGED or RELEASED lifecycle facts",
        )
    seed = digest_json(
        {
            "cycle": cycle["audit"]["payload_digest"],
            "issue": issue_number,
            "head": exact_head_sha,
            "requested": requested_owner_decisions,
        }
    )
    return seal_record(
        {
            "schema_version": 1,
            "record_kind": "external-supervisor-owner-decision-packet",
            "packet_id": f"owner-packet-{seed[:20]}",
            "created_at": format_timestamp(now),
            "delegation_id": cycle["delegation_id"],
            "delegation_digest": cycle["input_binding"]["delegation_digest"],
            "delegation_binding": cycle["delegation_binding"],
            "policy_binding": cycle["policy_binding"],
            "source_cycle_id": cycle["cycle_id"],
            "source_cycle_digest": cycle["audit"]["payload_digest"],
            "source_decision_id": cycle["decision"]["decision_id"],
            "observed_lifecycle": observed_state,
            "reason": reason,
            "issue_number": issue_number,
            "pull_request": pull_request,
            "exact_head_sha": exact_head_sha,
            "requested_owner_decisions": requested_owner_decisions,
            "supervisor_made_decision": False,
        }
    )


def _write_record(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(record, indent=2, ensure_ascii=False) + "\n"
    try:
        with path.open("x", encoding="utf-8", newline="\n") as stream:
            stream.write(content)
    except FileExistsError:
        if path.read_text(encoding="utf-8") != content:
            raise SupervisorError(
                "AUDIT_RECORD_EXISTS",
                "immutable audit record path already contains different bytes",
            )


def _load_and_verify_authority(
    args: argparse.Namespace,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], bytes, datetime]:
    policy_path = Path(args.policy)
    policy_bytes = text_blob_bytes(policy_path)
    policy = json.loads(
        policy_bytes.decode("utf-8"),
        object_pairs_hook=_reject_duplicate_pairs,
    )
    delegation_path = Path(args.delegation)
    delegation_bytes = text_blob_bytes(delegation_path)
    delegation = json.loads(
        delegation_bytes.decode("utf-8"),
        object_pairs_hook=_reject_duplicate_pairs,
    )
    delegation_relative_path = Path(args.delegation_path)
    if (
        delegation_relative_path.is_absolute()
        or ".." in delegation_relative_path.parts
        or delegation_relative_path.suffix != ".json"
    ):
        raise SupervisorError(
            "DELEGATION_PATH_INVALID",
            "delegation path must be a repository-relative JSON path",
        )
    now = parse_timestamp(args.now)
    verify_delegation(
        policy,
        delegation,
        policy_bytes=policy_bytes,
        policy_commit=args.policy_commit,
        delegation_bytes=delegation_bytes,
        now=now,
    )
    verify_git_blob(
        Path(args.repo_root),
        commit_sha=args.policy_commit,
        blob_path=delegation["policy_binding"]["policy_path"],
        expected_bytes=policy_bytes,
    )
    prompt_path = policy["kickoff_prompt"]["path"]
    committed_prompt_bytes = read_git_blob(
        Path(args.repo_root),
        commit_sha=args.policy_commit,
        blob_path=prompt_path,
    )
    if digest_bytes(committed_prompt_bytes) != policy["kickoff_prompt"]["sha256"]:
        raise SupervisorError(
            "PROMPT_DIGEST_MISMATCH",
            "policy commit prompt blob differs from the policy digest",
        )
    verify_git_blob(
        Path(args.repo_root),
        commit_sha=args.delegation_commit,
        blob_path=args.delegation_path,
        expected_bytes=delegation_bytes,
    )
    delegation_binding = {
        "commit_sha": args.delegation_commit,
        "path": delegation_relative_path.as_posix(),
        "blob_sha256": digest_bytes(delegation_bytes),
    }
    return policy, delegation, delegation_binding, policy_bytes, now


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    reconcile_parser = sub.add_parser("reconcile", help="Emit one deterministic cycle record.")
    for name, default in (
        ("policy", str(POLICY_PATH)),
        ("delegation", None),
        ("snapshot", None),
        ("sessions", None),
        ("policy-commit", None),
        ("delegation-commit", None),
        ("delegation-path", None),
        ("repo-root", str(ROOT)),
        ("now", None),
        ("cycle-id", None),
        ("output", None),
    ):
        reconcile_parser.add_argument(f"--{name}", required=default is None, default=default)
    reconcile_parser.add_argument("--sequence", type=int, required=True)
    reconcile_parser.add_argument("--previous-record")
    reconcile_parser.add_argument("--recovery-record")

    receipt_parser = sub.add_parser("receipt", help="Verify and emit a dispatch receipt.")
    for name, default in (
        ("policy", str(POLICY_PATH)),
        ("delegation", None),
        ("policy-commit", None),
        ("delegation-commit", None),
        ("delegation-path", None),
        ("repo-root", str(ROOT)),
    ):
        receipt_parser.add_argument(f"--{name}", required=default is None, default=default)
    receipt_parser.add_argument("--cycle", required=True)
    receipt_parser.add_argument("--sessions", required=True)
    receipt_parser.add_argument("--child-session-id", required=True)
    receipt_parser.add_argument("--now", required=True)
    receipt_parser.add_argument("--output", required=True)

    recovery_parser = sub.add_parser("recover", help="Emit a durable recovery record.")
    for name, default in (
        ("policy", str(POLICY_PATH)),
        ("delegation", None),
        ("policy-commit", None),
        ("delegation-commit", None),
        ("delegation-path", None),
        ("sessions", None),
        ("repo-root", str(ROOT)),
        ("now", None),
        ("output", None),
    ):
        recovery_parser.add_argument(f"--{name}", required=default is None, default=default)
    recovery_parser.add_argument(
        "--cause",
        required=True,
        choices=["CRASH_RESTART", "STALE_SESSION", "FAILED_CHILD", "AUDIT_RECONCILIATION"],
    )
    recovery_parser.add_argument(
        "--state",
        required=True,
        choices=[
            "PLANNED",
            "BLOCKED",
            "UNKNOWN",
            "INDETERMINATE",
            "REDIRECT_REQUIRED",
        ],
    )
    recovery_parser.add_argument("--previous-record", action="append", default=[])

    owner_parser = sub.add_parser("owner-packet", help="Emit an owner-only decision request.")
    for name, default in (
        ("policy", str(POLICY_PATH)),
        ("delegation", None),
        ("policy-commit", None),
        ("delegation-commit", None),
        ("delegation-path", None),
        ("repo-root", str(ROOT)),
    ):
        owner_parser.add_argument(f"--{name}", required=default is None, default=default)
    owner_parser.add_argument("--cycle", required=True)
    owner_parser.add_argument("--issue-number", required=True, type=int)
    owner_parser.add_argument("--pull-request", type=int)
    owner_parser.add_argument("--exact-head-sha")
    owner_parser.add_argument("--requested-decision", action="append", required=True)
    owner_parser.add_argument("--reason", required=True)
    owner_parser.add_argument("--now", required=True)
    owner_parser.add_argument("--output", required=True)

    verify_parser = sub.add_parser("verify-record", help="Verify record schema and audit digest.")
    verify_parser.add_argument("--record", required=True)
    verify_parser.add_argument("--prior-record", action="append", default=[])
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "reconcile":
            policy, delegation, delegation_binding, _, now = _load_and_verify_authority(args)
            previous = None
            if args.previous_record:
                previous = load_json(Path(args.previous_record))
            recovery = (
                load_json(Path(args.recovery_record))
                if args.recovery_record
                else None
            )
            record = reconcile(
                policy,
                delegation,
                load_json(Path(args.snapshot)),
                load_json(Path(args.sessions)),
                delegation_binding,
                now=now,
                cycle_id=args.cycle_id,
                sequence=args.sequence,
                previous_record=previous,
                recovery_record=recovery,
            )
            _write_record(Path(args.output), record)
        elif args.command == "receipt":
            policy, delegation, delegation_binding, _, now = _load_and_verify_authority(args)
            record = create_dispatch_receipt(
                load_json(Path(args.cycle)),
                load_json(Path(args.sessions)),
                policy=policy,
                delegation=delegation,
                delegation_binding=delegation_binding,
                child_session_id=args.child_session_id,
                now=now,
            )
            _write_record(Path(args.output), record)
        elif args.command == "recover":
            _, delegation, delegation_binding, _, now = _load_and_verify_authority(args)
            previous_records: list[dict[str, Any]] = []
            for path in args.previous_record:
                previous = load_json(Path(path))
                verify_record(previous)
                previous_records.append(previous)
            record = create_recovery_record(
                delegation,
                cause=args.cause,
                state=args.state,
                previous_records=previous_records,
                inventory=load_json(Path(args.sessions)),
                delegation_binding=delegation_binding,
                now=now,
            )
            _write_record(Path(args.output), record)
        elif args.command == "owner-packet":
            policy, delegation, delegation_binding, _, now = _load_and_verify_authority(args)
            record = create_owner_decision_packet(
                load_json(Path(args.cycle)),
                policy=policy,
                delegation=delegation,
                delegation_binding=delegation_binding,
                issue_number=args.issue_number,
                pull_request=args.pull_request,
                exact_head_sha=args.exact_head_sha,
                requested_owner_decisions=args.requested_decision,
                reason=args.reason,
                now=now,
            )
            _write_record(Path(args.output), record)
        else:
            verify_record(
                load_json(Path(args.record)),
                prior_records=[
                    load_json(Path(path)) for path in args.prior_record
                ],
            )
    except (SupervisorError, governance_schema.SchemaError, OSError) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
