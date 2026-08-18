"""Deterministic authorization evaluation for guarded bug reproduction.

The workflow performs the untrusted-input-free parts (reading event context and
calling the collaborator permission endpoint) and hands this helper a single
JSON request document. All decision logic lives here so it can be unit tested
without GitHub, without a checkout, and without reading issue content.

Subcommands
-----------
``evaluate``   Decide ALLOW / DENY / ERROR from event, actor, and permission API data.
``snapshot``   Capture immutable issue title/body content after authorization.
``verify``     Recompute and compare the current issue content before preparation.

Exit codes for ``evaluate``: ``0`` ALLOW, ``10`` DENY, ``20`` ERROR. ``ERROR``
means the decision could not be made (API failure or malformed response) and the
workflow must fail loudly rather than treating it as a denial.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Sequence

REPO_ROOT = Path(__file__).resolve().parent.parent
AUTHORIZATION_SCHEMA = (
    REPO_ROOT / ".github" / "governance" / "schemas" / "repro-authorization-record.schema.json"
)

SCHEMA_VERSION = 1
RECORD_KIND = "repro-authorization"

#: Only this exact label requests a guarded reproduction.
REQUEST_LABEL = "repro-requested"
#: Only these collaborator roles may request guarded execution.
#:
#: These are matched against the API's granular ``role_name`` field. The legacy
#: ``permission`` field cannot express "maintain" -- it reports maintainers as
#: "write" -- so it is only trusted for an exact "admin" match. Accepting
#: "write" there would silently widen this gate to every write collaborator.
ALLOWED_PERMISSIONS = ("maintain", "admin")

AUTHORITY_STATEMENT = (
    "Authorization evaluation records who may request guarded reproduction. It does not "
    "authorize implementation, merge, release, or any repository mutation beyond governed "
    "reproduction state labels. Repository owner retains authority."
)

DECISION_ALLOW = "ALLOW"
DECISION_DENY = "DENY"
DECISION_ERROR = "ERROR"

EXIT_ALLOW = 0
EXIT_DENY = 10
EXIT_ERROR = 20

_REASONS = {
    "AUTHORIZED": "Actor holds maintain or admin permission for an eligible request.",
    "EVENT_NOT_ELIGIBLE": "Event is not an eligible guarded reproduction trigger.",
    "ACTION_NOT_ELIGIBLE": "Issue event action is not 'labeled'.",
    "LABEL_NOT_ELIGIBLE": f"Applied label is not exactly '{REQUEST_LABEL}'.",
    "ACTOR_MISSING": "Request carries no actor login.",
    "ACTOR_AMBIGUOUS": "Event sender and workflow actor disagree.",
    "RERUN_ACTOR_MISMATCH": "Re-run was started by a different actor than the recorded actor.",
    "TRIGGERING_ACTOR_MISSING": "Request carries no triggering actor login.",
    "ISSUE_NUMBER_INVALID": "Issue number is missing or not a positive integer.",
    "PERMISSION_RESPONSE_MISSING": "No collaborator permission response was supplied.",
    "PERMISSION_API_ERROR": "Collaborator permission endpoint returned an unusable status.",
    "PERMISSION_RESPONSE_MALFORMED": "Collaborator permission response has no permission field.",
    "PERMISSION_NOT_A_COLLABORATOR": "Actor is not a collaborator on this repository.",
    "PERMISSION_INSUFFICIENT": "Actor permission is below maintain.",
}


class AuthorizationInputError(RuntimeError):
    """Raised when the request document itself is unusable."""


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _clean(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _coerce_issue_number(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, str):
        candidate = value.strip()
        if candidate.isdigit():
            number = int(candidate)
            return number if number > 0 else None
    return None


def _decide_permission(response: Any) -> tuple[str, str, int | None, str | None]:
    """Map a collaborator permission API response to (decision, reason_code, status, permission)."""
    if not isinstance(response, dict):
        return DECISION_ERROR, "PERMISSION_RESPONSE_MISSING", None, None
    raw_status = response.get("http_status")
    status = raw_status if isinstance(raw_status, int) and not isinstance(raw_status, bool) else None
    if status is None:
        return DECISION_ERROR, "PERMISSION_RESPONSE_MISSING", None, None
    if status == 404:
        return DECISION_DENY, "PERMISSION_NOT_A_COLLABORATOR", status, None
    if status != 200:
        return DECISION_ERROR, "PERMISSION_API_ERROR", status, None
    body = response.get("body")
    if not isinstance(body, dict):
        return DECISION_ERROR, "PERMISSION_RESPONSE_MALFORMED", status, None
    role_name = body.get("role_name")
    permission = body.get("permission")
    if isinstance(role_name, str) and role_name.strip():
        level = role_name.strip().lower()
    elif isinstance(permission, str) and permission.strip():
        # Without role_name only "admin" is unambiguous; "write" may be either a
        # plain write collaborator or a maintainer, so it must not pass.
        level = permission.strip().lower()
        if level != "admin":
            return DECISION_DENY, "PERMISSION_INSUFFICIENT", status, level
    else:
        return DECISION_ERROR, "PERMISSION_RESPONSE_MALFORMED", status, None
    if level in ALLOWED_PERMISSIONS:
        return DECISION_ALLOW, "AUTHORIZED", status, level
    return DECISION_DENY, "PERMISSION_INSUFFICIENT", status, level


def evaluate(request: dict[str, Any]) -> dict[str, Any]:
    """Evaluate an authorization request into a deterministic decision record."""
    if not isinstance(request, dict):
        raise AuthorizationInputError("authorization request must be a JSON object")

    event_name = _clean(request.get("event_name"))
    action = _clean(request.get("action"))
    label_value = request.get("label_name")
    label_name = label_value if isinstance(label_value, str) else ""
    sender_login = _clean(request.get("sender_login"))
    workflow_actor = _clean(request.get("actor"))
    triggering_actor = _clean(request.get("triggering_actor"))
    run_attempt_raw = request.get("run_attempt", 1)
    run_attempt = run_attempt_raw if isinstance(run_attempt_raw, int) and not isinstance(run_attempt_raw, bool) else _coerce_issue_number(run_attempt_raw) or 1

    decision = DECISION_DENY
    reason_code = "EVENT_NOT_ELIGIBLE"
    actor = ""
    issue_number: int | None = None
    permission_status: int | None = None
    permission_level: str | None = None
    permission_evaluated = False

    if event_name == "issues":
        actor = sender_login
        issue_number = _coerce_issue_number(request.get("issue_number_event"))
        if action != "labeled":
            reason_code = "ACTION_NOT_ELIGIBLE"
        elif label_name != REQUEST_LABEL:
            reason_code = "LABEL_NOT_ELIGIBLE"
        elif not actor:
            reason_code = "ACTOR_MISSING"
        elif workflow_actor and workflow_actor != actor:
            reason_code = "ACTOR_AMBIGUOUS"
        elif issue_number is None:
            reason_code = "ISSUE_NUMBER_INVALID"
        else:
            reason_code = ""
    else:
        reason_code = "EVENT_NOT_ELIGIBLE"

    if reason_code == "":
        if not triggering_actor:
            reason_code = "TRIGGERING_ACTOR_MISSING"
        elif triggering_actor != actor:
            reason_code = "RERUN_ACTOR_MISMATCH"

    if reason_code == "":
        permission_evaluated = True
        decision, reason_code, permission_status, permission_level = _decide_permission(
            request.get("permission_response")
        )

    record: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "record_kind": RECORD_KIND,
        "decision": decision,
        "actor_authorized": decision == DECISION_ALLOW,
        "reason_code": reason_code,
        "reason": _REASONS.get(reason_code, "Unclassified authorization outcome."),
        "request": {
            "repository": _clean(request.get("repository")),
            "event_name": event_name,
            "action": action,
            "label_name": label_name,
            "actor": actor,
            "sender_login": sender_login,
            "workflow_actor": workflow_actor,
            "triggering_actor": triggering_actor,
            "run_attempt": run_attempt,
            "run_id": _clean(request.get("run_id")),
            "issue_number": issue_number if issue_number is not None else 0,
        },
        "permission": {
            "evaluated": permission_evaluated,
            "http_status": permission_status if permission_status is not None else 0,
            "level": permission_level or "",
            "allowed_levels": list(ALLOWED_PERMISSIONS),
        },
        "authority": {
            "authorized": False,
            "statement": AUTHORITY_STATEMENT,
        },
    }
    return record


def render_summary(record: dict[str, Any]) -> str:
    """Render a deterministic Markdown summary for a decision record."""
    request = record["request"]
    permission = record["permission"]
    lines = [
        "## Guarded reproduction authorization",
        "",
        f"- **Decision**: {record['decision']}",
        f"- **Reason code**: `{record['reason_code']}`",
        f"- **Reason**: {record['reason']}",
        f"- **Event**: `{request['event_name']}`"
        + (f" / `{request['action']}`" if request["action"] else ""),
        f"- **Requested label**: `{request['label_name'] or 'n/a'}`",
        f"- **Actor**: `{request['actor'] or 'unknown'}`",
        f"- **Triggering actor**: `{request['triggering_actor'] or 'unknown'}`",
        f"- **Run attempt**: {request['run_attempt']}",
        f"- **Issue**: #{request['issue_number']}",
        f"- **Permission checked**: {'yes' if permission['evaluated'] else 'no'}",
        f"- **Permission level**: `{permission['level'] or 'not-determined'}`"
        + (f" (HTTP {permission['http_status']})" if permission["evaluated"] else ""),
        "",
        f"> {AUTHORITY_STATEMENT}",
    ]
    return "\n".join(lines) + "\n"


def build_snapshot(
    issue: dict[str, Any],
    expected_issue_number: int,
    repository: str,
) -> dict[str, Any]:
    """Capture the immutable content facts authorized for preparation.

    GitHub updates ``issue.updated_at`` for label and comment mutations, so it
    cannot represent the protected reporter-authored revision. The snapshot is
    bound only to repository, issue number, title, and body.
    """
    if not isinstance(issue, dict):
        raise AuthorizationInputError("issue payload must be a JSON object")
    number = _coerce_issue_number(issue.get("number"))
    if number is None:
        raise AuthorizationInputError("issue payload has no usable number")
    if expected_issue_number and number != expected_issue_number:
        raise AuthorizationInputError(
            f"issue payload number {number} does not match authorized issue {expected_issue_number}"
        )
    if "pull_request" in issue:
        raise AuthorizationInputError("requested item is a pull request, not an issue")
    repository = _clean(repository)
    if not repository:
        raise AuthorizationInputError("repository identity is required")
    title = issue.get("title")
    body = issue.get("body")
    title_text = title if isinstance(title, str) else ""
    body_text = body if isinstance(body, str) else ""
    canonical = json.dumps(
        {
            "repository": repository,
            "issue_number": number,
            "title": title_text,
            "body": body_text,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return {
        "schema_version": 1,
        "record_kind": "repro-content-snapshot",
        "repository": repository,
        "issue_number": number,
        "issue_url": issue.get("html_url") if isinstance(issue.get("html_url"), str) else "",
        "issue_state": issue.get("state") if isinstance(issue.get("state"), str) else "",
        "title": title_text,
        "body": body_text,
        "title_digest": _sha256_text(title_text),
        "body_digest": _sha256_text(body_text),
        "content_digest": _sha256_text(canonical),
        "title_bytes": len(title_text.encode("utf-8")),
        "body_bytes": len(body_text.encode("utf-8")),
    }


def verify_snapshot(issue: dict[str, Any], snapshot: dict[str, Any]) -> dict[str, Any]:
    """Compare current title/body content to the authorized immutable snapshot."""
    if not isinstance(snapshot, dict):
        raise AuthorizationInputError("authorized snapshot must be a JSON object")
    expected_number = _coerce_issue_number(snapshot.get("issue_number"))
    if expected_number is None:
        raise AuthorizationInputError("authorized snapshot has no usable issue number")
    current = build_snapshot(issue, expected_number, _clean(snapshot.get("repository")))
    fields = ("repository", "issue_number", "title_digest", "body_digest", "content_digest")
    matches = all(current.get(field) == snapshot.get(field) for field in fields)
    return {
        "matches": matches,
        "repository": current["repository"],
        "issue_number": current["issue_number"],
        "title_digest": current["title_digest"],
        "body_digest": current["body_digest"],
        "content_digest": current["content_digest"],
        "authorized_title_digest": snapshot.get("title_digest", ""),
        "authorized_body_digest": snapshot.get("body_digest", ""),
        "authorized_content_digest": snapshot.get("content_digest", ""),
    }


def _write_outputs(record: dict[str, Any], json_path: Path | None, markdown_path: Path | None) -> None:
    payload = json.dumps(record, indent=2, sort_keys=False) + "\n"
    if json_path:
        json_path.parent.mkdir(parents=True, exist_ok=True)
        json_path.write_text(payload, encoding="utf-8")
    else:
        sys.stdout.write(payload)
    if markdown_path:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(render_summary(record), encoding="utf-8")


def _validate_record(record: dict[str, Any]) -> None:
    """Validate against the checked-in schema when it is available."""
    if not AUTHORIZATION_SCHEMA.is_file():
        return
    sys.path.insert(0, str(REPO_ROOT))
    try:
        from tools import governance_schema
    finally:
        if sys.path and sys.path[0] == str(REPO_ROOT):
            sys.path.pop(0)
    governance_schema.validate_or_raise(record, AUTHORIZATION_SCHEMA, "authorization record")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="mode", required=True)

    evaluate_parser = subparsers.add_parser("evaluate", help="Evaluate an authorization request.")
    evaluate_parser.add_argument("--request", required=True, type=Path)
    evaluate_parser.add_argument("--output-json", type=Path)
    evaluate_parser.add_argument("--output-markdown", type=Path)

    snapshot_parser = subparsers.add_parser("snapshot", help="Capture authorized issue content.")
    snapshot_parser.add_argument("--issue", required=True, type=Path)
    snapshot_parser.add_argument("--expected-issue-number", type=int, required=True)
    snapshot_parser.add_argument("--repository", required=True)
    snapshot_parser.add_argument("--output-json", type=Path)

    verify_parser = subparsers.add_parser("verify", help="Verify current issue content against a snapshot.")
    verify_parser.add_argument("--issue", required=True, type=Path)
    verify_parser.add_argument("--snapshot", required=True, type=Path)
    verify_parser.add_argument("--output-json", type=Path)

    args = parser.parse_args(argv)

    if args.mode == "evaluate":
        try:
            request = json.loads(args.request.read_text(encoding="utf-8"))
            record = evaluate(request)
            _validate_record(record)
        except (OSError, json.JSONDecodeError, AuthorizationInputError) as exc:
            print(f"::error::authorization request is unusable: {exc}", file=sys.stderr)
            return EXIT_ERROR
        _write_outputs(record, args.output_json, args.output_markdown)
        if record["decision"] == DECISION_ALLOW:
            return EXIT_ALLOW
        if record["decision"] == DECISION_DENY:
            return EXIT_DENY
        return EXIT_ERROR

    issue = json.loads(args.issue.read_text(encoding="utf-8"))
    if args.mode == "snapshot":
        output = build_snapshot(issue, args.expected_issue_number, args.repository)
    else:
        snapshot = json.loads(args.snapshot.read_text(encoding="utf-8"))
        output = verify_snapshot(issue, snapshot)
    payload = json.dumps(output, indent=2) + "\n"
    if args.output_json:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(payload, encoding="utf-8")
    else:
        sys.stdout.write(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
