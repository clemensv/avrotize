"""Prepare revision-bound evidence for maintainer-requested bug reproduction.

This module never installs dependencies or executes Avrotize. GitHub-hosted
runners cannot provide the repository's required combination of locked Python
dependencies, disabled egress, and enforceable memory/PID/filesystem quotas, so
the workflow stops at deterministic evidence preparation for manual execution
in an owner-approved environment.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any, Sequence

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools import governance_authorize, governance_intake, governance_schema  # noqa: E402

SCHEMA_VERSION = 2
RECORD_KIND = "repro-preparation-evidence"
GOVERNANCE_DIR = REPO_ROOT / ".github" / "governance"
EVIDENCE_SCHEMA = GOVERNANCE_DIR / "schemas" / "repro-evidence-record.schema.json"
TERMINAL_FALLBACK_SCHEMA = (
    GOVERNANCE_DIR / "schemas" / "repro-terminal-fallback.schema.json"
)
ISSUE_FORM_CONTRACT = GOVERNANCE_DIR / "issue-form-contract.json"
LABEL_CATALOG = GOVERNANCE_DIR / "repro-label-catalog.json"
COMMANDS_JSON = REPO_ROOT / "avrotize" / "commands.json"
CAPABILITIES_JSON = GOVERNANCE_DIR / "avrotize-capabilities.json"

AUTHORITY_STATEMENT = (
    "Preparation evidence does not authorize scheduling, implementation, compatibility acceptance, "
    "merge, or release. Manual reproduction and every later decision remain under repository-owner authority."
)


class PreparationError(RuntimeError):
    """Corrupt configuration or unusable authorization."""


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _file_digest(path: Path, label: str) -> str:
    if not path.is_file():
        raise PreparationError(f"required {label} is missing: {path}")
    return _sha256_text(path.read_text(encoding="utf-8"))


def _safe_code(value: str) -> str:
    candidate = re.sub(r"[^A-Z0-9_]", "_", value.upper()).strip("_")
    return candidate[:64] if candidate else "UNCLASSIFIED"


def _read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PreparationError(f"cannot read {label}: {exc}") from exc
    if not isinstance(value, dict):
        raise PreparationError(f"{label} must be a JSON object")
    return value


def prepare(
    issue: dict[str, Any],
    authorization: dict[str, Any],
    snapshot: dict[str, Any],
    options: dict[str, Any],
) -> tuple[dict[str, Any], str]:
    """Build a preparation record without executing reporter-controlled input."""
    if authorization.get("decision") != "ALLOW":
        raise PreparationError("preparation requires an ALLOW authorization record")

    repository = str(options.get("repository") or "")
    issue_number = int(issue.get("number") or 0)
    if repository != snapshot.get("repository") or issue_number != snapshot.get("issue_number"):
        raise PreparationError("issue identity does not match the authorized snapshot")

    verification = governance_authorize.verify_snapshot(issue, snapshot)
    form_type = "unclassified"
    intake_status = "manual-triage"
    missing_fields: list[str] = []
    command_known: bool | None = None
    surface = ""
    command = ""

    if not verification["matches"]:
        status = "BLOCKED"
        reason_code = "ISSUE_CONTENT_CHANGED"
        reason = "issue title or body changed after authorization"
    else:
        intake, _ = governance_intake.normalize_issue(
            json.dumps(
                {
                    "action": "labeled",
                    "issue": issue,
                    "repository": {"full_name": repository},
                    "sender": {"login": authorization["request"]["actor"]},
                },
                sort_keys=True,
            ),
            str(options.get("processor_sha") or options.get("trusted_sha") or "local-worktree"),
        )
        classification = intake["classification"]
        facts = intake["normalized_facts"]
        form_type = str(classification["form_type"])
        intake_status = str(classification["status"])
        missing_fields = list(classification["missing_fields"])
        command_known = facts.get("command_known")
        surface = str(facts.get("surface") or "")
        command = str(facts.get("command") or "")
        reproduction_details = {
            "exact command or API area": facts.get("command")
            if command_known is True
            else None,
            "small example or steps": (
                None
                if governance_intake._is_placeholder(
                    str(facts.get("input_reproducer") or "")
                )
                else facts.get("input_reproducer")
            ),
            "version and environment": (
                None
                if governance_intake._is_placeholder(
                    str(facts.get("environment") or "")
                )
                else facts.get("environment")
            ),
        }
        reproduction_missing = [
            label for label, value in reproduction_details.items() if not value
        ]
        if form_type != "bug":
            status, reason_code, reason = (
                "BLOCKED",
                "NOT_A_BUG_REPORT",
                "This preparation path is only for Bug reports.",
            )
        elif intake_status != "complete":
            status, reason_code, reason = (
                "BLOCKED",
                "REPORT_NOT_COMPLETE",
                "A maintainer needs one more basic problem detail before preparing reproduction evidence.",
            )
        elif command_known is not True:
            status, reason_code, reason = (
                "BLOCKED",
                "COMMAND_NOT_RECOGNIZED",
                "A maintainer needs the exact Avrotize command or API area before preparing reproduction evidence.",
            )
        elif reproduction_missing:
            missing_fields.extend(reproduction_missing)
            status, reason_code, reason = (
                "BLOCKED",
                "REPRODUCTION_DETAILS_NEEDED",
                "A maintainer needs a small example and relevant environment details before manual reproduction.",
            )
        else:
            status, reason_code, reason = (
                "NEEDS_REVIEW",
                "MANUAL_REPRODUCTION_REQUIRED",
                "The evidence record is ready for a maintainer to review manually; no command was executed.",
            )

    final_label = "repro-blocked" if status == "BLOCKED" else "repro-needs-review"
    run_attempt = int(options.get("run_attempt") or 1)
    record = {
        "schema_version": SCHEMA_VERSION,
        "record_kind": RECORD_KIND,
        "issue_number": issue_number,
        "request": {
            "repository": repository,
            "actor": str(authorization["request"].get("actor") or ""),
            "run_id": str(options.get("run_id") or ""),
            "run_attempt": run_attempt,
            "run_url": str(options.get("run_url") or ""),
            "authorization_digest": _sha256_text(json.dumps(authorization, sort_keys=True)),
        },
        "authorized_content": {
            "title_digest": str(snapshot.get("title_digest") or ""),
            "body_digest": str(snapshot.get("body_digest") or ""),
            "content_digest": str(snapshot.get("content_digest") or ""),
            "verification": verification,
        },
        "processor": {
            "trusted_sha": str(options.get("trusted_sha") or ""),
            "default_branch": str(options.get("default_branch") or ""),
            "issue_form_contract_digest": _file_digest(ISSUE_FORM_CONTRACT, "issue form contract"),
            "command_registry_digest": _file_digest(COMMANDS_JSON, "command registry"),
            "capability_digest": _file_digest(CAPABILITIES_JSON, "capability profile"),
            "surface_registry_digest": _sha256_text(
                json.dumps(
                    governance_intake._load_surface_registry(),
                    sort_keys=True,
                    separators=(",", ":"),
                )
            ),
            "label_catalog_digest": _file_digest(LABEL_CATALOG, "label catalog"),
        },
        "readiness": {
            "form_type": form_type,
            "intake_status": intake_status,
            "missing_fields": missing_fields,
            "surface": surface,
            "command": command,
            "command_known": command_known,
        },
        "execution": {
            "performed": False,
            "reason": (
                "Automated execution is disabled because GitHub-hosted runners do not provide the "
                "required locked environment, disabled egress, and enforceable resource isolation."
            ),
        },
        "result": {
            "status": status,
            "reason_code": _safe_code(reason_code),
            "reason": reason,
            "final_label": final_label,
        },
        "artifact": {
            "name": str(
                options.get("artifact_name")
                or f"repro-preparation-{issue_number}-{options.get('run_id', '')}-{run_attempt}"
            ),
            "retention_days": int(options.get("retention_days") or 30),
        },
        "authority": {"authorized": False, "statement": AUTHORITY_STATEMENT},
    }
    governance_schema.validate_or_raise(record, EVIDENCE_SCHEMA, "reproduction preparation evidence")
    return record, render_summary(record)


def render_summary(record: dict[str, Any]) -> str:
    result = record["result"]
    verification = record["authorized_content"]["verification"]
    outcome = (
        "Ready for maintainer review"
        if result["status"] == "NEEDS_REVIEW"
        else "More information or a working preparation run is needed"
    )
    return "\n".join(
        [
            "## Reproduction preparation",
            "",
            f"- **Issue**: #{record['issue_number']}",
            f"- **Result**: {outcome}",
            f"- **Reason**: {result['reason']}",
            f"- **Content snapshot matched**: {'yes' if verification['matches'] else 'no'}",
            f"- **Trusted processor SHA**: `{record['processor']['trusted_sha']}`",
            "- **Reporter input and Avrotize commands**: not executed",
            "",
            f"> {AUTHORITY_STATEMENT}",
            "",
        ]
    )


def validate_prepared_evidence(
    record: dict[str, Any], expected: dict[str, Any]
) -> list[str]:
    """Validate schema and exact producer/run/content identity."""
    errors = governance_schema.validate(
        record, governance_schema.load_schema(EVIDENCE_SCHEMA)
    )
    verification = record.get("authorized_content", {}).get("verification", {})
    checks = {
        "issue number": record.get("issue_number") == expected["issue_number"],
        "repository": record.get("request", {}).get("repository")
        == expected["repository"],
        "run id": record.get("request", {}).get("run_id") == expected["run_id"],
        "producer attempt": record.get("request", {}).get("run_attempt")
        == expected["preparation_attempt"],
        "trusted processor": record.get("processor", {}).get("trusted_sha")
        == expected["trusted_sha"],
        "title digest": record.get("authorized_content", {}).get("title_digest")
        == expected["title_digest"],
        "body digest": record.get("authorized_content", {}).get("body_digest")
        == expected["body_digest"],
        "content digest": record.get("authorized_content", {}).get("content_digest")
        == expected["content_digest"],
        "verification result": verification.get("matches") is True,
        "verification repository": verification.get("repository")
        == expected["repository"],
        "verification issue number": verification.get("issue_number")
        == expected["issue_number"],
        "verification current title digest": verification.get("title_digest")
        == expected["title_digest"],
        "verification current body digest": verification.get("body_digest")
        == expected["body_digest"],
        "verification current content digest": verification.get("content_digest")
        == expected["content_digest"],
        "verification authorized title digest": verification.get(
            "authorized_title_digest"
        )
        == expected["title_digest"],
        "verification authorized body digest": verification.get(
            "authorized_body_digest"
        )
        == expected["body_digest"],
        "verification digest": verification.get("authorized_content_digest")
        == expected["content_digest"],
        "artifact name": record.get("artifact", {}).get("name")
        == expected["preparation_artifact"],
    }
    errors.extend(
        f"identity mismatch: {label}" for label, matches in checks.items() if not matches
    )
    result = record.get("result", {})
    if (result.get("status"), result.get("final_label")) not in {
        ("BLOCKED", "repro-blocked"),
        ("NEEDS_REVIEW", "repro-needs-review"),
    }:
        errors.append("result status and final label are inconsistent")
    return errors


def build_terminal_fallback(expected: dict[str, Any]) -> dict[str, Any]:
    """Build and schema-validate fail-closed terminal evidence."""
    reason = (
        "prepared evidence was missing, corrupt, or did not match the authorized "
        "run identity"
    )
    record = {
        "schema_version": 1,
        "record_kind": "repro-terminal-fallback",
        "issue_number": expected["issue_number"],
        "request": {
            "repository": expected["repository"],
            "run_id": expected["run_id"],
            "run_attempt": expected["run_attempt"],
            "run_url": expected["run_url"],
        },
        "processor": {"trusted_sha": expected["trusted_sha"]},
        "authorized_content": {
            "title_digest": expected["title_digest"],
            "body_digest": expected["body_digest"],
            "content_digest": expected["content_digest"],
        },
        "upstream": {
            "prepare_result": expected["prepare_result"],
            "mark_result": expected["mark_result"],
            "preparation_artifact": expected["preparation_artifact"],
        },
        "execution": {"performed": False, "reason": reason},
        "result": {
            "status": "BLOCKED",
            "reason_code": "PREPARATION_EVIDENCE_UNAVAILABLE",
            "reason": reason,
            "final_label": "repro-blocked",
        },
        "artifact": {
            "name": expected["terminal_artifact"],
            "retention_days": 30,
        },
        "authority": {
            "authorized": False,
            "statement": (
                "Reproduction evidence does not authorize implementation, scheduling, "
                "compatibility acceptance, merge, or release."
            ),
        },
    }
    governance_schema.validate_or_raise(
        record, TERMINAL_FALLBACK_SCHEMA, "reproduction terminal fallback"
    )
    return record


def finalize_terminal(
    evidence: dict[str, Any] | None, expected: dict[str, Any]
) -> tuple[dict[str, Any], str, dict[str, str]]:
    """Accept exact validated preparation evidence or emit a validated fallback."""
    if evidence is not None and not validate_prepared_evidence(evidence, expected):
        record = evidence
    else:
        record = build_terminal_fallback(expected)
    encoded = json.dumps(record, indent=2) + "\n"
    digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    result = record["result"]
    ready = result["status"] == "NEEDS_REVIEW"
    outcome = (
        "Evidence is ready for maintainer review"
        if ready
        else "Preparation needs a maintainer's attention"
    )
    next_step = (
        "The evidence is available for maintainer review. "
        "No action is needed from the reporter unless a maintainer follows up."
        if ready
        else "A maintainer can review the run and ask for one specific detail if needed. "
        "No action is needed from the reporter unless a maintainer follows up."
    )
    missing = record.get("readiness", {}).get("missing_fields", [])
    follow_up = (
        " If a maintainer follows up, the useful details are: "
        + ", ".join(str(value) for value in missing)
        + "."
        if not ready and missing
        else ""
    )
    operation_marker = (
        f"<!-- avrotize-repro:{expected['repository']}:"
        f"issue-{expected['issue_number']}:run-{expected['run_id']}:"
        f"attempt-{expected['run_attempt']} -->"
    )
    comment = "\n".join(
        [
            operation_marker,
            "## Reproduction preparation update",
            "",
            f"**{outcome}.** {result['reason']} {next_step}{follow_up}",
            "",
            f"[View the workflow run and evidence]({expected['run_url']}). "
            "No Avrotize command, attachment, or reporter example was executed.",
            "",
            "<details>",
            "<summary>Technical record</summary>",
            "",
            f"- Record status: `{result['status']}` (`{result['reason_code']}`)",
            f"- Evidence digest: `{digest}`",
            f"- Trusted processor SHA: `{expected['trusted_sha']}`",
            f"- Authorized content digest: `{expected['content_digest']}`",
            f"- Run attempt: `{expected['run_attempt']}`",
            "",
            "</details>",
        ]
    )
    metadata = {
        "label": result["final_label"],
        "status": result["status"],
        "digest": digest,
    }
    return record, comment + "\n", metadata


def _terminal_main(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(description="Finalize reproduction preparation evidence")
    parser.add_argument("--evidence", type=Path)
    parser.add_argument("--issue-number", required=True, type=int)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--trusted-sha", required=True)
    parser.add_argument("--title-digest", required=True)
    parser.add_argument("--body-digest", required=True)
    parser.add_argument("--content-digest", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--run-attempt", required=True, type=int)
    parser.add_argument("--run-url", required=True)
    parser.add_argument("--preparation-artifact", default="")
    parser.add_argument("--preparation-attempt", type=int, default=0)
    parser.add_argument("--terminal-artifact", required=True)
    parser.add_argument("--prepare-result", required=True)
    parser.add_argument("--mark-result", required=True)
    parser.add_argument("--output-json", required=True, type=Path)
    parser.add_argument("--output-comment", required=True, type=Path)
    parser.add_argument("--output-metadata", required=True, type=Path)
    args = parser.parse_args(argv)
    evidence: dict[str, Any] | None = None
    if args.evidence and args.evidence.is_file():
        try:
            value = json.loads(args.evidence.read_text(encoding="utf-8"))
            evidence = value if isinstance(value, dict) else None
        except (OSError, json.JSONDecodeError):
            evidence = None
    expected = {
        "issue_number": args.issue_number,
        "repository": args.repository,
        "trusted_sha": args.trusted_sha,
        "title_digest": args.title_digest,
        "body_digest": args.body_digest,
        "content_digest": args.content_digest,
        "run_id": args.run_id,
        "run_attempt": args.run_attempt,
        "run_url": args.run_url,
        "preparation_artifact": args.preparation_artifact,
        "preparation_attempt": args.preparation_attempt,
        "terminal_artifact": args.terminal_artifact,
        "prepare_result": args.prepare_result,
        "mark_result": args.mark_result,
    }
    record, comment, metadata = finalize_terminal(evidence, expected)
    args.output_json.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    args.output_comment.write_text(comment, encoding="utf-8")
    args.output_metadata.write_text(
        json.dumps(metadata, indent=2) + "\n", encoding="utf-8"
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(argv) if argv is not None else sys.argv[1:]
    if arguments and arguments[0] == "terminal":
        return _terminal_main(arguments[1:])
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--issue", required=True, type=Path)
    parser.add_argument("--authorization", required=True, type=Path)
    parser.add_argument("--snapshot", required=True, type=Path)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--trusted-sha", required=True)
    parser.add_argument("--default-branch", required=True)
    parser.add_argument("--processor-sha", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--run-attempt", required=True, type=int)
    parser.add_argument("--run-url", required=True)
    parser.add_argument("--artifact-name", required=True)
    parser.add_argument("--retention-days", type=int, default=30)
    parser.add_argument("--output-json", required=True, type=Path)
    parser.add_argument("--output-markdown", required=True, type=Path)
    args = parser.parse_args(arguments)

    record, summary = prepare(
        _read_json(args.issue, "issue payload"),
        _read_json(args.authorization, "authorization record"),
        _read_json(args.snapshot, "authorized content snapshot"),
        {
            "repository": args.repository,
            "trusted_sha": args.trusted_sha,
            "default_branch": args.default_branch,
            "processor_sha": args.processor_sha,
            "run_id": args.run_id,
            "run_attempt": args.run_attempt,
            "run_url": args.run_url,
            "artifact_name": args.artifact_name,
            "retention_days": args.retention_days,
        },
    )
    args.output_json.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    args.output_markdown.write_text(summary, encoding="utf-8")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except PreparationError as exc:
        print(f"::error::reproduction preparation failed: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
