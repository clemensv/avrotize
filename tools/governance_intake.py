"""Revision-bound governance intake and read-only semantic assistance for Avrotize.

CLI modes:
  python tools/governance_intake.py issue-preflight --event EVENT_JSON
  python tools/governance_intake.py issue   --event EVENT_JSON
  python tools/governance_intake.py dependabot --event EVENT_JSON --files FILES_JSON

Standard library only. Copilot execution remains outside this trusted processor;
the processor prepares inert stdin and validates all returned JSON before use.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any, Sequence

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools import governance_schema  # noqa: E402  (path bootstrap above enables direct execution)

ISSUE_FORM_CONTRACT = REPO_ROOT / ".github" / "governance" / "issue-form-contract.json"
COPILOT_INTAKE_POLICY = REPO_ROOT / ".github" / "governance" / "copilot-intake-policy.json"
COPILOT_CLI_LOCKFILE = REPO_ROOT / ".github" / "governance" / "copilot-cli" / "package-lock.json"
COMMANDS_JSON = REPO_ROOT / "avrotize" / "commands.json"
MCP_SERVER = REPO_ROOT / "avrotize" / "mcp_server.py"
VSCODE_PACKAGE = REPO_ROOT / "vscode" / "avrotize" / "package.json"
DEPENDABOT_CONFIG = REPO_ROOT / ".github" / "dependabot.yml"
CAPABILITIES_JSON = REPO_ROOT / ".github" / "governance" / "avrotize-capabilities.json"
SCHEMA_DIR = REPO_ROOT / ".github" / "governance" / "schemas"
ISSUE_RECORD_SCHEMA = SCHEMA_DIR / "issue-intake-record.schema.json"
ISSUE_SEMANTIC_SCHEMA = SCHEMA_DIR / "issue-semantic-assistance.schema.json"
DEPENDABOT_RECORD_SCHEMA = SCHEMA_DIR / "dependabot-intake-record.schema.json"

AUTHORITY_STATEMENT = (
    "Intake assistance does not authorize implementation, schedule work, approve "
    "compatibility, or permit merge or release. Repository owner retains authority."
)

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def _sha256(data: str) -> str:
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


#: Maximum rendered length of any reporter-derived fragment in a Markdown summary.
MAX_SUMMARY_FRAGMENT = 200

_SUMMARY_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def _summary_safe(value: Any, limit: int = MAX_SUMMARY_FRAGMENT) -> str:
    """Render a reporter-derived value as inert single-line Markdown.

    Issue-form text reaches the run summary (commands, unexpected headings,
    surface errors). Step summaries are rendered as Markdown, so collapse
    whitespace, neutralize characters that could close a code span, break a
    table row, or start a heading/quote, and truncate. Presentation hygiene
    only; this is not an authorization control.
    """
    text = _SUMMARY_CONTROL_RE.sub(" ", str(value).replace("\x00", " "))
    text = " ".join(text.split())
    text = text.replace("\\", "\\\\")
    for character in ("`", "|", "[", "]", "(", ")", "*", "_", "~", "#", "!"):
        text = text.replace(character, "\\" + character)
    text = text.replace("<", "&lt;").replace(">", "&gt;")
    if len(text) > limit:
        text = text[:limit] + "..."
    return text


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _load_json_strict(path: Path) -> Any:
    """Load JSON, raising on any error (corrupt config = infrastructure failure)."""
    text = path.read_text(encoding="utf-8")
    return _loads_json_strict(text)


def _loads_json_strict(text: str) -> Any:
    """Parse RFC JSON while rejecting duplicate keys and non-finite numbers."""
    def object_no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise ValueError(f"duplicate JSON property: {key}")
            value[key] = item
        return value

    def reject_constant(value: str) -> None:
        raise ValueError(f"non-finite JSON number: {value}")

    return json.loads(
        text,
        object_pairs_hook=object_no_duplicates,
        parse_constant=reject_constant,
    )


def _load_command_entries() -> list[dict[str, Any]]:
    """Load complete command entries from commands.json."""
    if not COMMANDS_JSON.is_file():
        raise RuntimeError(f"Commands registry not found: {COMMANDS_JSON}")
    data = _load_json_strict(COMMANDS_JSON)
    if not isinstance(data, list):
        raise RuntimeError(f"Commands registry is not an array: {COMMANDS_JSON}")
    for item in data:
        if (
            not isinstance(item, dict)
            or not isinstance(item.get("command"), str)
            or not isinstance(item.get("function"), dict)
            or not isinstance(item["function"].get("name"), str)
        ):
            raise RuntimeError(f"Corrupt entry in commands registry: {item!r}")
    return data


def _load_commands() -> set[str]:
    """Load exact CLI command names from commands.json."""
    return {item["command"] for item in _load_command_entries()}


def _load_surface_registry() -> dict[str, dict[str, str]]:
    """Build exact surface identifiers from authoritative checked-in registries.

    Each value maps a case-sensitive accepted identifier to its canonical CLI
    command when one exists, or to an empty string for non-transform MCP tools.
    """
    entries = _load_command_entries()
    cli = {item["command"]: item["command"] for item in entries}
    python_api: dict[str, str] = {}
    for item in entries:
        function_name = item["function"]["name"]
        python_api[function_name] = item["command"]
        python_api[function_name.rsplit(".", 1)[-1]] = item["command"]

    if not MCP_SERVER.is_file():
        raise RuntimeError(f"MCP server registry not found: {MCP_SERVER}")
    try:
        tree = ast.parse(MCP_SERVER.read_text(encoding="utf-8"), filename=str(MCP_SERVER))
    except SyntaxError as exc:
        raise RuntimeError(f"MCP server registry is not valid Python: {exc}") from exc
    mcp_tools: dict[str, str] = {}
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        is_tool = any(
            isinstance(decorator, ast.Call)
            and isinstance(decorator.func, ast.Attribute)
            and decorator.func.attr == "tool"
            for decorator in node.decorator_list
        )
        if is_tool:
            mcp_tools[node.name] = node.name

    vscode_data = _load_json_strict(VSCODE_PACKAGE)
    try:
        menu_items = vscode_data["contributes"]["menus"]["convertSubmenu"]
    except (KeyError, TypeError) as exc:
        raise RuntimeError("VS Code package lacks the Convert to command registry") from exc
    if not isinstance(menu_items, list):
        raise RuntimeError("VS Code Convert to command registry is not an array")
    vscode: dict[str, str] = {}
    for item in menu_items:
        if not isinstance(item, dict) or not isinstance(item.get("command"), str):
            raise RuntimeError(f"Corrupt VS Code command entry: {item!r}")
        command_id = item["command"]
        canonical = command_id.removeprefix("avrotize.")
        if canonical not in cli:
            raise RuntimeError(f"VS Code command is absent from commands.json: {command_id}")
        vscode[command_id] = canonical
        title = item.get("title")
        if isinstance(title, str) and title:
            vscode[f"Convert to > {title}"] = canonical

    generated = dict(cli)
    generated.update(python_api)
    generated.update(vscode)
    return {
        "Avrotize CLI": cli,
        "Structurize CLI": cli,
        "Python API": python_api,
        "MCP server": mcp_tools,
        "VS Code extension": vscode,
        "Generated project or code": generated,
        "": generated | mcp_tools,
    }


_CALLABLE_IDENTIFIER_RE = re.compile(
    r"^([A-Za-z_][A-Za-z0-9_.]*)(?:\s*\([^()\r\n]*\))?$"
)
_CLI_IDENTIFIER_RE = re.compile(
    r"^(?:(?:avrotize|structurize)\s+)?([a-z][a-z0-9-]*)$"
)


def _canonical_command(
    value: str | None, surface: str | None, registry: dict[str, dict[str, str]]
) -> str | None:
    """Resolve an exact surface identifier without substring or fuzzy matching."""
    if not value:
        return None
    identifier = value.strip()
    surface_key = "" if surface in (None, "", "I'm not sure") else surface
    surface_registry = registry.get(surface_key, {})
    if identifier in surface_registry:
        return surface_registry[identifier]

    cli_match = _CLI_IDENTIFIER_RE.fullmatch(identifier)
    if cli_match and cli_match.group(1) in surface_registry:
        return surface_registry[cli_match.group(1)]

    callable_match = _CALLABLE_IDENTIFIER_RE.fullmatch(identifier)
    if callable_match and callable_match.group(1) in surface_registry:
        return surface_registry[callable_match.group(1)]
    return None


def _load_issue_form_contract() -> dict[str, Any]:
    """Load the issue form contract. Corrupt = infrastructure failure."""
    contract = _load_json_strict(ISSUE_FORM_CONTRACT)
    if not isinstance(contract, dict) or "forms" not in contract:
        raise RuntimeError("Malformed issue form contract")
    return contract


def _validate_record(record: dict[str, Any], schema_path: Path) -> list[str]:
    """Validate a record against a checked-in schema with the deep stdlib validator.

    A missing schema file is an infrastructure failure: records must never be
    written without structural validation.
    """
    if not schema_path.is_file():
        raise RuntimeError(f"Required record schema is missing: {schema_path}")
    schema = governance_schema.load_schema(schema_path)
    return governance_schema.validate(record, schema)


def _validate_or_raise(record: dict[str, Any], schema_path: Path, label: str) -> None:
    """Validate before any write; structural failure is an infrastructure failure."""
    errors = _validate_record(record, schema_path)
    if errors:
        raise RuntimeError(f"{label} failed schema validation: " + "; ".join(errors))


_PROMPT_INJECTION_PATTERNS = (
    re.compile(r"\bignore\s+(?:all\s+|any\s+|the\s+)?(?:previous|prior|above)\s+instructions?\b", re.IGNORECASE),
    re.compile(r"\b(?:reveal|print|leak|expose)\b.{0,48}\b(?:token|secret|github_token|system prompt)\b", re.IGNORECASE | re.DOTALL),
    re.compile(
        r"(?:\b(?:copilot|assistant|you)\b.{0,32}\b(?:call|invoke|use|run)\b"
        r"|(?:\bcall\b|\binvoke\b|\buse\b)\s+(?:the\s+|a\s+)?).*?"
        r"\b(?:tool|shell|terminal)\b",
        re.IGNORECASE | re.DOTALL,
    ),
    re.compile(r"\b(?:emit|set|apply|assign|decide)\b.{0,32}\b(?:label|priority|severity|rank|assignee|schedule)\b", re.IGNORECASE | re.DOTALL),
    re.compile(r"\b(?:return|respond|answer)\b.{0,24}\bmarkdown\b.{0,24}\b(?:instead|not json)\b", re.IGNORECASE | re.DOTALL),
    re.compile(r"\b(?:fabricate|invent|make up)\b.{0,32}\bcommand\b", re.IGNORECASE | re.DOTALL),
)

_UNSUPPORTED_OUTPUT_PATTERNS = (
    re.compile(r"\bpriority\s*[:=]\s*\w+", re.IGNORECASE),
    re.compile(r"\bseverity\s*[:=]\s*\w+", re.IGNORECASE),
    re.compile(r"\b(?:apply|set|add|remove)\s+(?:the\s+)?label\b", re.IGNORECASE),
    re.compile(r"\b(?:assign|schedule|accept|reject|approve|merge|release)\s+(?:this|the)\b", re.IGNORECASE),
    re.compile(r"\b(?:valid|invalid|compliant|noncompliant)\s+(?:issue|report|reporter|contributor)\b", re.IGNORECASE),
)


def _load_copilot_intake_policy() -> dict[str, Any]:
    """Load and validate the trusted Copilot issue-intake policy."""
    policy = _load_json_strict(COPILOT_INTAKE_POLICY)
    try:
        cli = policy["cli"]
        request = policy["request"]
        boundary = policy["tool_boundary"]
        artifact = policy["artifact"]
    except (KeyError, TypeError) as exc:
        raise RuntimeError("Copilot intake policy is incomplete") from exc
    expected = {
        "package": "@github/copilot",
        "install_scripts": False,
    }
    if any(cli.get(key) != value for key, value in expected.items()):
        raise RuntimeError("Copilot intake CLI policy is not locked down")
    if not isinstance(cli.get("version"), str) or not re.fullmatch(r"\d+\.\d+\.\d+(?:-\d+)?", cli["version"]):
        raise RuntimeError("Copilot intake CLI version must be exact")
    if not isinstance(cli.get("integrity"), str) or not cli["integrity"].startswith("sha512-"):
        raise RuntimeError("Copilot intake CLI integrity must be SHA-512")
    if cli.get("lockfile") != ".github/governance/copilot-cli/package-lock.json":
        raise RuntimeError("Copilot intake lockfile path changed")
    lockfile_digest = _sha256(COPILOT_CLI_LOCKFILE.read_text(encoding="utf-8"))
    if cli.get("lockfile_sha256") != lockfile_digest:
        raise RuntimeError("Copilot intake lockfile digest does not match policy")
    if request.get("max_ai_credits") != 30:
        raise RuntimeError("Copilot intake max_ai_credits must remain the conservative minimum of 30")
    if not isinstance(request.get("timeout_seconds"), int) or request["timeout_seconds"] <= 0:
        raise RuntimeError("Copilot intake timeout must be a positive integer")
    if request.get("max_output_bytes") != 32768:
        raise RuntimeError("Copilot intake max_output_bytes must remain 32768")
    confidence = request.get("minimum_confidence")
    if not isinstance(confidence, (int, float)) or isinstance(confidence, bool) or not 0 <= confidence <= 1:
        raise RuntimeError("Copilot intake minimum_confidence must be between 0 and 1")
    if boundary.get("available_tools") != [] or boundary.get("builtin_mcp") is not False:
        raise RuntimeError("Copilot intake must expose no tools or built-in MCP server")
    if boundary.get("custom_instructions") is not False or boundary.get("remote_export") is not False:
        raise RuntimeError("Copilot intake must disable custom instructions and remote export")
    if set(boundary.get("denied_tools", [])) != {"shell", "write", "read", "url", "memory"}:
        raise RuntimeError("Copilot intake denied tool set changed")
    if any(artifact.get(key) is not False for key in ("raw_title", "raw_body", "raw_model_response")):
        raise RuntimeError("Copilot intake artifact policy must exclude raw content")
    return policy


def _copilot_resources() -> tuple[dict[str, Any], str, str, str, dict[str, Any]]:
    """Return trusted semantic policy/template/schema resources and their digests."""
    policy = _load_copilot_intake_policy()
    request = policy["request"]
    prompt_path = REPO_ROOT / request["prompt_template"]
    output_schema_path = REPO_ROOT / request["output_schema"]
    if prompt_path != REPO_ROOT / ".github" / "governance" / "prompts" / "issue-semantic-assistance-v1.txt":
        raise RuntimeError("Copilot intake prompt path is not the reviewed version")
    if output_schema_path != ISSUE_SEMANTIC_SCHEMA:
        raise RuntimeError("Copilot intake output schema path is not the reviewed schema")
    prompt_text = prompt_path.read_text(encoding="utf-8")
    schema_text = output_schema_path.read_text(encoding="utf-8")
    output_schema = governance_schema.load_schema(output_schema_path)
    policy_text = COPILOT_INTAKE_POLICY.read_text(encoding="utf-8")
    return policy, prompt_text, schema_text, policy_text, output_schema


def _capability_areas() -> set[str]:
    """Return exact checked-in Avrotize capability-area identifiers."""
    profile = _load_json_strict(CAPABILITIES_JSON)
    areas: set[str] = set()
    for key in ("command_group_areas", "responsibility_domains"):
        values = profile.get(key)
        if isinstance(values, dict):
            areas.update(str(name) for name in values)
    utilities = profile.get("utility_command_areas")
    if isinstance(utilities, dict):
        areas.update(str(value) for value in utilities.values())
    return areas


def _prompt_injection_indicator(title: str, body: str) -> bool:
    """Conservatively detect issue text that tries to redirect the model."""
    combined = f"{title}\n{body}"
    return any(pattern.search(combined) for pattern in _PROMPT_INJECTION_PATTERNS)


def prepare_issue_assistance(event_json: str) -> tuple[dict[str, Any], str]:
    """Build a no-tools Copilot prompt and a content-free deterministic preflight record."""
    event = json.loads(event_json)
    issue = event.get("issue", event)
    title = str(issue.get("title") or "")
    body = str(issue.get("body") or "")
    policy, prompt_template, schema_text, policy_text, _ = _copilot_resources()
    request = policy["request"]
    contract = _load_issue_form_contract()
    surfaces = list(contract.get("surface_choices", []))
    commands = sorted(_load_commands())
    areas = sorted(_capability_areas())
    input_characters = len(title) + len(body)
    if input_characters > int(request["max_input_characters"]):
        eligible = False
        reason = "input-too-large"
    elif _prompt_injection_indicator(title, body):
        eligible = False
        reason = "prompt-injection-indicator"
    else:
        eligible = True
        reason = "eligible"

    registry = {
        "surfaces": surfaces,
        "areas": areas,
        "commands": commands,
    }
    untrusted_issue = {
        "title": title,
        "body": body,
    }
    prompt = "\n\n".join(
        (
            prompt_template.rstrip(),
            "OUTPUT_SCHEMA_JSON\n"
            + json.dumps(_loads_json_strict(schema_text), sort_keys=True, separators=(",", ":")),
            "REGISTRY_JSON\n" + json.dumps(registry, sort_keys=True, ensure_ascii=False, separators=(",", ":")),
            "UNTRUSTED_ISSUE_JSON\n"
            + json.dumps(untrusted_issue, sort_keys=True, ensure_ascii=False, separators=(",", ":")),
        )
    ) + "\n"
    preflight = {
        "schema_version": 1,
        "eligible": eligible,
        "reason": reason,
        "title_digest": _sha256(title),
        "body_digest": _sha256(body),
        "policy_digest": _sha256(policy_text),
        "lockfile_digest": _sha256_bytes(COPILOT_CLI_LOCKFILE.read_bytes()),
        "output_schema_digest": _sha256(schema_text),
        "prompt_template_digest": _sha256(prompt_template),
        "prompt_version": request["prompt_version"],
        "model": request["model"],
        "max_ai_credits": request["max_ai_credits"],
        "timeout_seconds": request["timeout_seconds"],
        "input_characters": input_characters,
    }
    return preflight, prompt


def _sanitize_semantic_text(value: Any, limit: int) -> str:
    """Bound model text and remove controls without interpreting it as Markdown."""
    text = _SUMMARY_CONTROL_RE.sub(" ", str(value).replace("\x00", " "))
    text = " ".join(text.split())
    return text[:limit]


def _contains_unsupported_authority_output(value: Any) -> bool:
    """Reject model text that crosses the maintainer-assistance authority boundary."""
    if isinstance(value, dict):
        return any(_contains_unsupported_authority_output(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_unsupported_authority_output(item) for item in value)
    if not isinstance(value, str):
        return False
    return any(pattern.search(value) for pattern in _UNSUPPORTED_OUTPUT_PATTERNS)


def _semantic_fallback(
    policy: dict[str, Any],
    reason: str,
    attempted: bool,
    exit_code: int | None,
) -> dict[str, Any]:
    request = policy["request"]
    return {
        "status": "unavailable",
        "reason": reason,
        "prompt_version": request["prompt_version"],
        "model": request["model"],
        "execution": {
            "attempted": attempted,
            "exit_code": exit_code,
            "timeout_seconds": request["timeout_seconds"],
            "max_ai_credits": request["max_ai_credits"],
            "platform_reported_aic": {
                "reported": False,
                "value": None,
                "source": "not-exposed-by-cli-output",
            },
        },
        "result": None,
        "result_digest": None,
    }


def _normalize_semantic_assistance(
    preflight: dict[str, Any] | None,
    raw_output: str | None,
    exit_code: int | None,
    stderr: str | None,
    surface_registry: dict[str, dict[str, str]],
) -> dict[str, Any]:
    """Validate and cross-check Copilot output before it enters an intake record."""
    policy, prompt_template, schema_text, policy_text, output_schema = _copilot_resources()
    if preflight is None:
        return _semantic_fallback(policy, "not-requested", False, None)

    expected_digests = {
        "policy_digest": _sha256(policy_text),
        "lockfile_digest": _sha256_bytes(COPILOT_CLI_LOCKFILE.read_bytes()),
        "output_schema_digest": _sha256(schema_text),
        "prompt_template_digest": _sha256(prompt_template),
    }
    for key, expected in expected_digests.items():
        if preflight.get(key) != expected:
            raise RuntimeError(f"Copilot intake preflight {key} does not match trusted resources")

    eligible = preflight.get("eligible") is True
    if not eligible:
        reason = str(preflight.get("reason") or "copilot-unavailable")
        if reason not in {"prompt-injection-indicator", "input-too-large"}:
            reason = "copilot-unavailable"
        return _semantic_fallback(policy, reason, False, None)

    attempted = exit_code is not None
    if exit_code == 124:
        return _semantic_fallback(policy, "timeout", True, exit_code)
    if exit_code not in (0, None):
        details = (stderr or "").lower()
        reason = (
            "aic-guardrail-exhausted"
            if "ai credit" in details and ("limit" in details or "exhaust" in details)
            else "copilot-unavailable"
        )
        return _semantic_fallback(policy, reason, True, exit_code)
    if raw_output is None:
        return _semantic_fallback(policy, "copilot-unavailable", attempted, exit_code)

    if raw_output == "__COPILOT_OUTPUT_TOO_LARGE__":
        return _semantic_fallback(policy, "unsupported-output", True, exit_code)
    try:
        candidate = _loads_json_strict(raw_output)
    except (json.JSONDecodeError, ValueError):
        return _semantic_fallback(policy, "invalid-json", True, exit_code)
    errors = governance_schema.validate(candidate, output_schema)
    if errors:
        return _semantic_fallback(policy, "schema-violation", True, exit_code)
    if _contains_unsupported_authority_output(candidate):
        return _semantic_fallback(policy, "unsupported-output", True, exit_code)

    known_surfaces = set(_load_issue_form_contract().get("surface_choices", []))
    known_areas = _capability_areas()
    enriched_candidates: list[dict[str, Any]] = []
    unknown_registry_suggestion = False
    low_confidence = float(candidate["report_kind"]["confidence"]) < float(
        policy["request"]["minimum_confidence"]
    )
    for item in candidate["candidates"]:
        surface = item.get("surface")
        area = item.get("area")
        command = item.get("command")
        surface_known = None if surface is None else surface in known_surfaces
        area_known = None if area is None else area in known_areas
        canonical = _canonical_command(
            command,
            surface if surface_known else None,
            surface_registry,
        )
        command_known = None if command is None else canonical is not None
        if surface_known is False or area_known is False or command_known is False:
            unknown_registry_suggestion = True
        confidence = float(item["confidence"])
        low_confidence = low_confidence or confidence < float(
            policy["request"]["minimum_confidence"]
        )
        enriched_candidates.append(
            {
                "suggested_surface": (
                    _sanitize_semantic_text(surface, 80) if surface is not None else None
                ),
                "suggested_area": (
                    _sanitize_semantic_text(area, 100) if area is not None else None
                ),
                "suggested_command": (
                    _sanitize_semantic_text(command, 120) if command is not None else None
                ),
                "confidence": confidence,
                "evidence": _sanitize_semantic_text(item["evidence"], 180),
                "surface_known": surface_known,
                "area_known": area_known,
                "command_known": command_known,
                "canonical_command": canonical,
            }
        )

    accepted = {
        "summary": _sanitize_semantic_text(candidate["summary"], 500),
        "report_kind": {
            "value": candidate["report_kind"]["value"],
            "confidence": float(candidate["report_kind"]["confidence"]),
            "evidence": [
                _sanitize_semantic_text(value, 180)
                for value in candidate["report_kind"]["evidence"]
            ],
        },
        "candidates": enriched_candidates,
        "missing_details": [
            _sanitize_semantic_text(value, 180) for value in candidate["missing_details"]
        ],
        "duplicate_search_terms": [
            _sanitize_semantic_text(value, 80)
            for value in candidate["duplicate_search_terms"]
        ],
        "needs_human_review": bool(candidate["needs_human_review"]),
    }
    if not accepted["summary"] or any(
        not item
        for item in (
            accepted["report_kind"]["evidence"]
            + accepted["missing_details"]
            + accepted["duplicate_search_terms"]
            + [entry["evidence"] for entry in accepted["candidates"]]
        )
    ):
        return _semantic_fallback(policy, "unsupported-output", True, exit_code)

    if unknown_registry_suggestion:
        status, reason = "needs-human-review", "unknown-registry-suggestion"
    elif low_confidence:
        status, reason = "needs-human-review", "low-confidence"
    elif accepted["needs_human_review"]:
        status, reason = "needs-human-review", "model-requested-review"
    else:
        status, reason = "needs-human-review", "suggestions-ready"
    request = policy["request"]
    result_digest = _sha256(
        json.dumps(accepted, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    )
    return {
        "status": status,
        "reason": reason,
        "prompt_version": request["prompt_version"],
        "model": request["model"],
        "execution": {
            "attempted": True,
            "exit_code": exit_code,
            "timeout_seconds": request["timeout_seconds"],
            "max_ai_credits": request["max_ai_credits"],
            "platform_reported_aic": {
                "reported": False,
                "value": None,
                "source": "not-exposed-by-cli-output",
            },
        },
        "result": accepted,
        "result_digest": result_digest,
    }


def _flatten_paginated_files(data: Any) -> list[dict[str, Any]]:
    """Flatten nested page arrays from gh api --paginate --slurp.

    --paginate --slurp produces [[page1_items...], [page2_items...], ...].
    Also handles flat arrays (from fixtures or single-page responses).
    """
    if not isinstance(data, list):
        raise ValueError(f"Expected list for files data, got {type(data).__name__}")
    if not data:
        return []
    # Detect nested: if first element is a list, treat as pages
    if isinstance(data[0], list):
        flat: list[dict[str, Any]] = []
        for page in data:
            if not isinstance(page, list):
                raise ValueError("Inconsistent paginated structure: expected list of lists")
            for item in page:
                if isinstance(item, dict):
                    flat.append(item)
        return flat
    # Already flat
    return [item for item in data if isinstance(item, dict)]


# ---------------------------------------------------------------------------
# Issue intake
# ---------------------------------------------------------------------------

_HEADING_RE = re.compile(r"^### (.+)$", re.MULTILINE)
_PLACEHOLDER_PATTERNS = [
    re.compile(r"^(N/?A|n/?a|none|placeholder|todo|tbd|xxx|\.\.\.)$", re.IGNORECASE),
    re.compile(r"^_No response_$"),
]


def _is_placeholder(value: str) -> bool:
    """Detect placeholder/non-meaningful content."""
    stripped = value.strip()
    if not stripped:
        return True
    for pat in _PLACEHOLDER_PATTERNS:
        if pat.match(stripped):
            return True
    return False


def _parse_issue_body(body: str) -> dict[str, str]:
    """Parse rendered Issue Form sections without treating fenced content as headings."""
    sections: dict[str, str] = {}
    heading = ""
    content: list[str] = []
    fence_marker = ""

    def store() -> None:
        if not heading:
            return
        value = "\n".join(content).strip()
        if value == "_No response_":
            value = ""
        if heading in sections:
            sections[f"__duplicate_heading__:{heading}"] = value
        else:
            sections[heading] = value

    for line in body.splitlines():
        stripped = line.lstrip()
        if fence_marker:
            content.append(line)
            if stripped.startswith(fence_marker):
                fence_marker = ""
            continue
        fence = re.match(r"^(`{3,}|~{3,})", stripped)
        if fence:
            fence_marker = fence.group(1)[0] * len(fence.group(1))
            content.append(line)
            continue
        match = re.fullmatch(r"### (.+)", line)
        if match:
            store()
            heading = match.group(1).strip()
            content = []
            continue
        if heading:
            content.append(line)
    store()
    return sections


def _detect_form_type(title: str, contract: dict[str, Any]) -> tuple[str, dict[str, Any] | None]:
    """Detect form type from title prefix. Unknown/freeform => unclassified."""
    for form in contract["forms"]:
        prefix = form["title_prefix"]
        if title.startswith(prefix):
            return form["type"], form
    return "unclassified", None


def _heading_to_field_id(heading: str, form_spec: dict[str, Any]) -> str | None:
    """Map a heading label to its field id using the contract."""
    headings = form_spec.get("headings", [])
    field_ids = form_spec.get("field_ids", [])
    for i, h in enumerate(headings):
        if h == heading and i < len(field_ids):
            return field_ids[i]
    return None


def _field_id_to_heading(field_id: str, form_spec: dict[str, Any] | None) -> str:
    """Return the contributor-facing label for a stable form field ID."""
    if form_spec is None:
        return field_id.replace("_", " ")
    field_ids = list(form_spec.get("field_ids", []))
    headings = list(form_spec.get("headings", []))
    try:
        index = field_ids.index(field_id)
    except ValueError:
        return field_id.replace("_", " ")
    return headings[index] if index < len(headings) else field_id.replace("_", " ")


def _resolve_semantic_paths(command: str | None, surface: str | None) -> list[str]:
    """Determine which semantic paths a command/surface touches."""
    if not command:
        return []
    paths: list[str] = []
    # Extract command token handling "avrotize <cmd>" and flags
    tokens = command.strip().split()
    # Skip leading "avrotize" or "python -m avrotize" etc
    cmd_tokens = [t for t in tokens if not t.startswith("-") and t != "avrotize" and t != "python" and t != "-m"]
    cmd = cmd_tokens[0] if cmd_tokens else ""

    if cmd.startswith("a2") or cmd.endswith("2a") or "avrotools" in command or "avrovalidator" in command:
        paths.append("Avrotize Schema")
    if cmd.startswith("s2") or cmd.endswith("2s") or "structure" in command.lower() or "jstruct" in command.lower():
        paths.append("JSON Structure")
    if not paths:
        paths.append("Direct command behavior")
    return paths


def _validate_surface(surface: str | None, contract: dict[str, Any]) -> tuple[bool, str | None]:
    """Validate a surface against the exact checked-in Issue Form choices."""
    if not surface:
        return True, None
    choices = contract.get("surface_choices")
    if not isinstance(choices, list) or not choices or not all(isinstance(choice, str) for choice in choices):
        raise RuntimeError("Issue form contract lacks surface_choices")
    value = surface.strip()
    if value in choices:
        return True, None
    return False, f"unsupported surface: {value}"


def _analyze_heading_set(sections: dict[str, str], form_spec: dict[str, Any]) -> tuple[list[str], list[str], list[str]]:
    """Compare rendered headings against the exact contract heading set.

    Returns (unexpected_headings, missing_required_headings, missing_optional_headings).
    """
    declared = list(form_spec.get("headings", []))
    field_ids = list(form_spec.get("field_ids", []))
    required_fields = set(form_spec.get("required_semantic_fields", []))
    present = set(sections)
    unexpected = sorted(present - set(declared))
    missing_required: list[str] = []
    missing_optional: list[str] = []
    for index, heading in enumerate(declared):
        if heading in present:
            continue
        field_id = field_ids[index] if index < len(field_ids) else ""
        if field_id in required_fields:
            missing_required.append(heading)
        else:
            missing_optional.append(heading)
    return unexpected, missing_required, missing_optional


def _normalize_expected_result(choice: str, contract: dict[str, Any]) -> tuple[str, str]:
    """Map the structured expected-result dropdown to a deterministic expectation kind."""
    raw = (choice or "").strip()
    if not raw or _is_placeholder(raw):
        return "", "undeclared"
    mapping = contract.get("expected_result_choices")
    if mapping is None:
        return raw, "undeclared"
    if not isinstance(mapping, dict):
        raise RuntimeError("Issue form contract expected_result_choices must be an object")
    kind = mapping.get(raw)
    if not isinstance(kind, str):
        return raw, "undeclared"
    return raw, kind


def normalize_issue(
    event_json: str,
    processor_sha: str = "local-worktree",
    *,
    semantic_preflight: dict[str, Any] | None = None,
    semantic_output: str | None = None,
    semantic_exit_code: int | None = None,
    semantic_stderr: str | None = None,
    minimize_content: bool = False,
) -> tuple[dict[str, Any], str]:
    """Normalize an issue event into a record and markdown summary.

    Returns (record_dict, markdown_str).
    Raises on corrupt config/infrastructure. Incomplete/unknown exits successfully.
    """
    event = json.loads(event_json)
    contract = _load_issue_form_contract()
    surface_registry = _load_surface_registry()
    policy, prompt_template, semantic_schema_text, policy_text, _ = _copilot_resources()

    issue = event.get("issue", event)
    title = issue.get("title", "")
    body = issue.get("body") or ""
    number = issue.get("number", 0)
    action = event.get("action", "opened")
    repository = event.get("repository", {}).get("full_name", "")
    url = issue.get("html_url", "")
    sender = event.get("sender", {}).get("login", "")

    body_digest = _sha256(body)
    title_digest = _sha256(title)
    contract_text = ISSUE_FORM_CONTRACT.read_text(encoding="utf-8")
    commands_text = COMMANDS_JSON.read_text(encoding="utf-8")
    capabilities_text = CAPABILITIES_JSON.read_text(encoding="utf-8")
    contract_digest = _sha256(contract_text)
    command_registry_digest = _sha256(commands_text)
    capability_digest = _sha256(capabilities_text)
    semantic_policy_digest = _sha256(policy_text)
    copilot_lockfile_digest = _sha256(COPILOT_CLI_LOCKFILE.read_text(encoding="utf-8"))
    semantic_output_schema_digest = _sha256(semantic_schema_text)
    semantic_prompt_digest = _sha256(prompt_template)
    surface_registry_digest = _sha256(
        json.dumps(surface_registry, sort_keys=True, separators=(",", ":"))
    )
    source_digest = _sha256(
        ":".join(
            (
                _sha256(event_json),
                processor_sha,
                contract_digest,
                command_registry_digest,
                capability_digest,
                surface_registry_digest,
                semantic_policy_digest,
                copilot_lockfile_digest,
                semantic_output_schema_digest,
                semantic_prompt_digest,
            )
        )
    )
    if semantic_preflight is not None:
        if semantic_preflight.get("title_digest") != title_digest:
            raise RuntimeError("Copilot intake preflight title digest does not match the event")
        if semantic_preflight.get("body_digest") != body_digest:
            raise RuntimeError("Copilot intake preflight body digest does not match the event")

    form_type, form_spec = _detect_form_type(title, contract)

    # Parse sections
    sections = _parse_issue_body(body)

    # Determine completeness
    status = "manual-triage"
    missing_fields: list[str] = []
    supplemental_headings: list[str] = []
    repeated_headings: list[str] = []
    missing_headings: list[str] = []
    surface_error: str | None = None
    canonical_command: str | None = None
    normalized: dict[str, Any] = {
        "command": None, "command_known": None, "surface": None,
        "semantic_paths": [], "source_representation": None,
        "result_representation": None, "flags_options": None,
        "input_reproducer": None, "actual_behavior": None,
        "expected_behavior": None, "expected_result_choice": None,
        "expected_result_kind": "undeclared", "expected_output": None,
        "environment": None,
        "regression": None, "semantics": None,
        "validation_expectations": None, "documentation": None,
    }

    if form_type == "unclassified":
        status = "manual-triage"
    elif form_spec is not None:
        # Map sections to field IDs
        field_values: dict[str, str] = {}
        for heading, content in sections.items():
            fid = _heading_to_field_id(heading, form_spec)
            if fid:
                field_values[fid] = content

        unexpected_headings, missing_required_headings, missing_optional_headings = _analyze_heading_set(
            sections, form_spec
        )
        repeated_headings = sorted(
            heading.removeprefix("__duplicate_heading__:")
            for heading in unexpected_headings
            if heading.startswith("__duplicate_heading__:")
        )
        supplemental_headings = sorted(
            heading
            for heading in unexpected_headings
            if not heading.startswith("__duplicate_heading__:")
        )
        missing_headings = missing_required_headings + missing_optional_headings

        # Empty, duplicate, or non-contract content remains available for a human read.
        if not body.strip() or not sections:
            status = "manual-triage"
        else:
            # Check required fields - reject placeholders
            for req in form_spec.get("required_semantic_fields", []):
                val = field_values.get(req, "")
                if _is_placeholder(val):
                    missing_fields.append(req)

            # Validate surface option
            surface_val = field_values.get("surface", "")
            if surface_val:
                valid, err = _validate_surface(surface_val, contract)
                if not valid:
                    surface_error = err

            if repeated_headings:
                status = "manual-triage"
            elif missing_fields:
                status = "incomplete"
            elif surface_error:
                status = "manual-triage"
            else:
                status = "complete"

            # Extract normalized facts
            surface_raw = field_values.get("surface") or None
            if surface_raw and not _is_placeholder(surface_raw):
                normalized["surface"] = surface_raw
            cmd_raw = field_values.get("command", "")
            if cmd_raw and not _is_placeholder(cmd_raw):
                normalized["command"] = cmd_raw
                canonical_command = _canonical_command(
                    cmd_raw, normalized["surface"], surface_registry
                )
                normalized["command_known"] = canonical_command is not None
            else:
                canonical_command = None
            normalized["semantic_paths"] = _resolve_semantic_paths(
                canonical_command or normalized["command"], normalized["surface"]
            )
            reproducer = field_values.get("reproducer") or field_values.get("input") or None
            normalized["source_representation"] = field_values.get("input") or reproducer
            normalized["result_representation"] = field_values.get("output") or None
            normalized["flags_options"] = (
                field_values.get("invocation")
                or field_values.get("options")
                or reproducer
                or None
            )
            normalized["input_reproducer"] = reproducer
            normalized["actual_behavior"] = field_values.get("actual") or None
            normalized["expected_behavior"] = (
                field_values.get("expected")
                or field_values.get("problem")
                or field_values.get("outcome")
                or None
            )
            expected_choice, expected_kind = _normalize_expected_result(
                field_values.get("expected_result", ""), contract
            )
            normalized["expected_result_choice"] = expected_choice or None
            normalized["expected_result_kind"] = expected_kind
            expected_output_raw = field_values.get("expected_output", "")
            normalized["expected_output"] = (
                None if _is_placeholder(expected_output_raw) else expected_output_raw
            )
            normalized["environment"] = field_values.get("environment") or None
            normalized["regression"] = field_values.get("regression") or None
            normalized["semantics"] = field_values.get("semantics") or field_values.get("details") or None
            normalized["validation_expectations"] = field_values.get("validation") or None
            normalized["documentation"] = (
                field_values.get("documentation") or field_values.get("example") or None
            )

    if minimize_content:
        if normalized["command_known"] is True:
            normalized["command"] = canonical_command
        else:
            normalized["command"] = None
            normalized["command_known"] = None
        for key in (
            "source_representation",
            "result_representation",
            "flags_options",
            "input_reproducer",
            "actual_behavior",
            "expected_behavior",
            "expected_result_choice",
            "expected_output",
            "environment",
            "regression",
            "semantics",
            "validation_expectations",
            "documentation",
        ):
            normalized[key] = None
        normalized["expected_result_kind"] = "undeclared"

    semantic_assistance = _normalize_semantic_assistance(
        semantic_preflight,
        semantic_output,
        semantic_exit_code,
        semantic_stderr,
        surface_registry,
    )
    record: dict[str, Any] = {
        "schema_version": 2,
        "record_kind": "issue-intake",
        "event_identity": {
            "issue_number": number,
            "repository": repository,
            "url": url,
            "event_type": action,
            "sender": sender,
            "processor_sha": processor_sha,
            "title_digest": title_digest,
            "body_digest": body_digest,
            "contract_digest": contract_digest,
            "command_registry_digest": command_registry_digest,
            "capability_digest": capability_digest,
            "surface_registry_digest": surface_registry_digest,
            "semantic_policy_digest": semantic_policy_digest,
            "copilot_lockfile_digest": copilot_lockfile_digest,
            "semantic_output_schema_digest": semantic_output_schema_digest,
            "semantic_prompt_digest": semantic_prompt_digest,
            "source_digest": source_digest,
            "update": action in ("edited", "reopened"),
        },
        "classification": {
            "form_type": form_type,
            "status": status,
            "missing_fields": missing_fields,
            "supplemental_headings": [
                _sanitize_semantic_text(value, 120) for value in supplemental_headings
            ],
            "repeated_headings": [
                _sanitize_semantic_text(value, 120) for value in repeated_headings
            ],
            "missing_headings": missing_headings,
        },
        "normalized_facts": normalized,
        "semantic_assistance": semantic_assistance,
        "privacy": {
            "raw_title_stored": False,
            "raw_body_stored": False,
            "raw_model_response_stored": False,
            "artifact_content": (
                "digests-and-bounded-derived-output"
                if minimize_content
                else "includes-normalized-form-values"
            ),
        },
        "authority": {
            "authorized": False,
            "statement": AUTHORITY_STATEMENT,
        },
    }
    _validate_or_raise(record, ISSUE_RECORD_SCHEMA, "issue intake record")

    status_text = {
        "complete": "Ready for a maintainer to read",
        "incomplete": "More information may help",
        "manual-triage": "Needs a maintainer look",
    }.get(status, "Needs a maintainer look")
    md_lines = [
        "## Issue details check",
        "",
        f"- **Issue**: #{number}",
        f"- **Event**: {action}",
        f"- **Form**: {form_type}",
        f"- **Result**: {status_text}",
        f"- **Body digest**: `{body_digest[:16]}...`",
    ]
    if missing_fields:
        missing_labels = [
            _field_id_to_heading(field_id, form_spec) for field_id in missing_fields
        ]
        md_lines.append(
            f"- **A maintainer may ask for**: {', '.join(_summary_safe(label) for label in missing_labels)}"
        )
    if supplemental_headings:
        md_lines.append(
            f"- **Additional sections kept for review**: {', '.join(_summary_safe(h) for h in supplemental_headings)}"
        )
    if repeated_headings:
        md_lines.append(
            f"- **Repeated sections need a maintainer look**: {', '.join(_summary_safe(h) for h in repeated_headings)}"
        )
    if missing_headings:
        md_lines.append(
            f"- **Sections not present**: {', '.join(_summary_safe(h) for h in missing_headings)}"
        )
    if surface_error:
        md_lines.append(f"- **Area needs a quick human check**: {_summary_safe(surface_error)}")
    if normalized["command"]:
        known_str = "yes" if normalized["command_known"] else "no/unknown"
        md_lines.append(f"- **Command**: `{_summary_safe(normalized['command'])}` (known: {known_str})")
    if normalized["semantic_paths"]:
        md_lines.append(f"- **Related Avrotize paths**: {', '.join(normalized['semantic_paths'])}")
    assistance_status = {
        "needs-human-review": "Untrusted semantic suggestions are available for maintainer review; they are not decisions",
        "unavailable": "Semantic suggestions were unavailable; the issue remains ready for a human read",
    }[semantic_assistance["status"]]
    md_lines.append(f"- **Copilot assistance**: {assistance_status}")
    semantic_result = semantic_assistance["result"]
    if isinstance(semantic_result, dict):
        md_lines.append(
            "> **Untrusted Copilot suggestion (not a decision):** "
            + _summary_safe(semantic_result["summary"], 500)
        )
        report_kind = semantic_result["report_kind"]
        md_lines.append(
            f"- **Model-suggested report kind**: {_summary_safe(report_kind['value'])} "
            f"(confidence {float(report_kind['confidence']):.2f})"
        )
        known_candidates = [
            item
            for item in semantic_result["candidates"]
            if item["surface_known"] is True
            or item["area_known"] is True
            or item["command_known"] is True
        ]
        if known_candidates:
            rendered = []
            for item in known_candidates:
                values = [
                    item["suggested_surface"] if item["surface_known"] is True else None,
                    item["suggested_area"] if item["area_known"] is True else None,
                    item["canonical_command"] if item["command_known"] is True else None,
                ]
                rendered.append(
                    " / ".join(_summary_safe(value) for value in values if value)
                    + f" ({float(item['confidence']):.2f})"
                )
            md_lines.append(f"- **Model-suggested Avrotize areas**: {', '.join(rendered)}")
        if semantic_result["missing_details"]:
            md_lines.append(
                "- **Model-suggested details that might help later**: "
                + "; ".join(
                    _summary_safe(value) for value in semantic_result["missing_details"]
                )
            )
    md_lines.extend([
        "",
        f"> {AUTHORITY_STATEMENT}",
    ])
    markdown = "\n".join(md_lines) + "\n"

    return record, markdown


# ---------------------------------------------------------------------------
# Dependabot intake
# ---------------------------------------------------------------------------

# Minimal dependabot.yml parser (standard library only)
def _parse_dependabot_config(config_path: Path | None = None) -> list[dict[str, Any]]:
    """Parse dependabot.yml to extract update entries. Fails on malformed config."""
    path = config_path or DEPENDABOT_CONFIG
    if not path.is_file():
        raise RuntimeError(f"Dependabot config not found: {path}")
    text = path.read_text(encoding="utf-8")
    entries: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("- package-ecosystem:"):
            if current is not None:
                entries.append(current)
            eco = stripped.split(":", 1)[1].strip().strip('"').strip("'")
            current = {"package-ecosystem": eco, "directory": "/"}
        elif stripped.startswith("directory:") and current is not None:
            current["directory"] = stripped.split(":", 1)[1].strip().strip('"').strip("'")
        elif stripped.startswith("prefix:") and current is not None:
            current["prefix"] = stripped.split(":", 1)[1].strip().strip('"').strip("'")
    if current is not None:
        entries.append(current)
    if not entries:
        raise RuntimeError("Malformed dependabot.yml: no update entries found")
    return entries


# Ecosystem -> manifest file patterns (basename matches)
_ECOSYSTEM_MANIFESTS: dict[str, list[str]] = {
    "pip": ["requirements.txt", "setup.py", "pyproject.toml", "Pipfile", "setup.cfg"],
    "nuget": [".csproj", ".fsproj", "packages.config", "Directory.Packages.props"],
    "maven": ["pom.xml"],
    "npm": ["package.json"],
    "gomod": ["go.mod"],
    "cargo": ["Cargo.toml"],
    "github-actions": [],  # workflows are the manifests for github-actions
}

# github-actions manifest path pattern
_GITHUB_ACTIONS_PATH_PREFIX = ".github/workflows/"

_ECOSYSTEM_LOCKFILES: dict[str, list[str]] = {
    "pip": ["Pipfile.lock", "poetry.lock"],
    "npm": ["package-lock.json", "yarn.lock", "pnpm-lock.yaml"],
    "gomod": ["go.sum"],
    "cargo": ["Cargo.lock"],
}

# Ecosystem/directory -> domain mapping. Multiple ecosystems intentionally share
# "/" in dependabot.yml, so directory alone is not a stable classifier.
_ENTRY_DOMAIN: dict[tuple[str, str], list[str]] = {
    ("pip", "/"): ["root-python-package"],
    ("github-actions", "/"): ["ci"],
    ("pip", "/avrotize/dependencies/python/py312"): ["generated-output", "toolchain"],
    ("nuget", "/avrotize/dependencies/cs/net100"): ["generated-output", "toolchain"],
    ("maven", "/avrotize/dependencies/java/jdk21"): ["generated-output", "toolchain"],
    ("npm", "/avrotize/dependencies/typescript/node22"): ["generated-output", "toolchain"],
    ("gomod", "/avrotize/dependencies/go/go121"): ["generated-output", "toolchain"],
    ("cargo", "/avrotize/dependencies/rust/stable"): ["generated-output", "toolchain"],
}

_ENTRY_EXPOSURE: dict[tuple[str, str], list[str]] = {
    ("pip", "/"): ["runtime", "build", "test"],
    ("github-actions", "/"): ["ci"],
    ("pip", "/avrotize/dependencies/python/py312"): ["generated-output", "toolchain", "compiler-runtime-test"],
    ("nuget", "/avrotize/dependencies/cs/net100"): ["generated-output", "toolchain", "compiler-runtime-test"],
    ("maven", "/avrotize/dependencies/java/jdk21"): ["generated-output", "toolchain", "compiler-runtime-test"],
    ("npm", "/avrotize/dependencies/typescript/node22"): ["generated-output", "toolchain", "compiler-runtime-test"],
    ("gomod", "/avrotize/dependencies/go/go121"): ["generated-output", "toolchain", "compiler-runtime-test"],
    ("cargo", "/avrotize/dependencies/rust/stable"): ["generated-output", "toolchain", "compiler-runtime-test"],
}

# Expanded path-based domain resolution
_PATH_DOMAIN_RULES: list[tuple[str, str, list[str]]] = [
    # (path_prefix, domain, exposure)
    (".github/workflows/", "ci", ["ci"]),
    ("vscode/avrotize/", "editor-extension", ["editor-extension"]),
    ("structurize/", "structurize-package", ["runtime"]),
    ("docs/", "documentation", ["documentation"]),
]

# Dependabot body metadata regex patterns
_BODY_DEP_NAME_RE = re.compile(r"-?\s*dependency-name:\s*(.+)")
_BODY_DEP_TYPE_RE = re.compile(r"dependency-type:\s*(.+)")
_BODY_UPDATE_TYPE_RE = re.compile(r"update-type:\s*(.+)")


def _parse_dependabot_body_metadata(body: str) -> list[dict[str, str]]:
    """Parse dependency metadata from Dependabot PR body.

    Extracts dependency-name, dependency-type, update-type blocks.
    """
    deps: list[dict[str, str]] = []
    current: dict[str, str] = {}
    for line in body.splitlines():
        line = line.strip()
        m = _BODY_DEP_NAME_RE.match(line)
        if m:
            if current:
                deps.append(current)
            current = {"dependency-name": m.group(1).strip()}
            continue
        m = _BODY_DEP_TYPE_RE.match(line)
        if m and current:
            current["dependency-type"] = m.group(1).strip()
            continue
        m = _BODY_UPDATE_TYPE_RE.match(line)
        if m and current:
            current["update-type"] = m.group(1).strip()
            continue
    if current:
        deps.append(current)
    return deps


def _strip_dependabot_prefix(title: str, configured_prefixes: Sequence[str]) -> tuple[str, str]:
    for prefix in sorted(configured_prefixes, key=len, reverse=True):
        marker = f"{prefix}:"
        if title.lower().startswith(marker.lower()):
            return title[len(marker):].strip(), prefix
    return title.strip(), ""


def _parse_dependabot_title(
    title: str, configured_prefixes: Sequence[str] = ()
) -> list[dict[str, Any]]:
    """Extract dependency name and versions from Dependabot PR title."""
    deps: list[dict[str, Any]] = []
    title, _ = _strip_dependabot_prefix(title, configured_prefixes)
    # Single dependency bump
    m = re.match(
        r"(?:Bump|Update)\s+(.+?)\s+(?:requirement\s+)?from\s+[~>=<]*\s*(\S+)\s+to\s+[~>=<]*\s*(\S+)",
        title, re.IGNORECASE,
    )
    if m:
        deps.append({
            "name": m.group(1).strip(),
            "old_version": m.group(2).strip(),
            "new_version": m.group(3).strip(),
        })
        return deps

    # Group update
    m = re.match(r"Bump the .+ group .+ with (\d+) updates?", title, re.IGNORECASE)
    if m:
        return deps  # details from body metadata

    # Fallback - "Bump X to Y"
    m = re.match(r"(?:Bump|Update)\s+(.+?)\s+to\s+[~>=<]*\s*(\S+)", title, re.IGNORECASE)
    if m:
        deps.append({
            "name": m.group(1).strip(),
            "old_version": None,
            "new_version": m.group(2).strip(),
        })
    return deps


def _classify_version_bump(old_ver: str | None, new_ver: str | None) -> str:
    """Classify as major/minor/patch/unknown."""
    if not old_ver or not new_ver:
        return "unknown"
    old_parts = old_ver.lstrip("v").split(".")
    new_parts = new_ver.lstrip("v").split(".")
    try:
        if int(new_parts[0]) > int(old_parts[0]):
            return "major"
        if len(new_parts) > 1 and len(old_parts) > 1 and int(new_parts[1]) > int(old_parts[1]):
            return "minor"
        return "patch"
    except (ValueError, IndexError):
        return "unknown"


def _is_ecosystem_manifest(filename: str, ecosystem: str) -> bool:
    """Check if a file is a manifest for the given ecosystem."""
    if ecosystem == "github-actions":
        return filename.startswith(_GITHUB_ACTIONS_PATH_PREFIX) and (
            filename.endswith(".yml") or filename.endswith(".yaml")
        )
    basename = filename.rsplit("/", 1)[-1] if "/" in filename else filename
    patterns = _ECOSYSTEM_MANIFESTS.get(ecosystem, [])
    return any(basename.endswith(p) or basename == p for p in patterns)


def _is_ecosystem_lockfile(filename: str, ecosystem: str) -> bool:
    """Check if a file is a lockfile for the given ecosystem."""
    basename = filename.rsplit("/", 1)[-1] if "/" in filename else filename
    patterns = _ECOSYSTEM_LOCKFILES.get(ecosystem, [])
    return any(basename.endswith(p) or basename == p for p in patterns)


def _file_in_directory(filename: str, directory: str) -> bool:
    """Check if a file path is within a config directory."""
    dir_prefix = directory.lstrip("/").rstrip("/")
    if dir_prefix == "":
        return True  # root matches everything
    return filename.startswith(dir_prefix + "/") or filename == dir_prefix


def _assign_files_to_entries(
    changed_files: list[dict[str, Any]], config_entries: list[dict[str, Any]]
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """Assign every changed file to the most specific matching dependabot.yml entry.

    Returns (assignments keyed by ``"ecosystem:directory"``, unmatched filenames).
    Each assignment holds the config entry and the filenames that matched it, so
    multi-ecosystem pull requests are classified per ecosystem instead of forcing
    one primary ecosystem onto every file.
    """
    assignments: dict[str, dict[str, Any]] = {}
    unmatched: list[str] = []

    for file_info in changed_files:
        filename = file_info.get("filename", "")
        if not filename:
            continue

        best_entry: dict[str, Any] | None = None
        best_specificity = -1

        for entry in config_entries:
            directory = entry["directory"]
            ecosystem = entry["package-ecosystem"]

            if not _file_in_directory(filename, directory):
                continue

            # Check if file is actually a manifest or lockfile for this ecosystem
            if not (_is_ecosystem_manifest(filename, ecosystem) or
                    _is_ecosystem_lockfile(filename, ecosystem)):
                continue

            # Specificity = length of directory prefix
            specificity = len(directory.lstrip("/").rstrip("/"))
            if specificity > best_specificity:
                best_entry = entry
                best_specificity = specificity

        if best_entry is None:
            unmatched.append(filename)
            continue

        key = f"{best_entry['package-ecosystem']}:{best_entry['directory']}"
        bucket = assignments.setdefault(key, {"entry": best_entry, "files": []})
        bucket["files"].append(file_info)

    return assignments, unmatched


def _match_config_entries(
    changed_files: list[dict[str, Any]], config_entries: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Match changed files against dependabot.yml entries.

    Uses directory + ecosystem-compatible manifest/lockfile patterns.
    Returns all matched config entries (most-specific directory wins per file).
    Handles duplicate root entries (pip / vs github-actions /).
    """
    assignments, _ = _assign_files_to_entries(changed_files, config_entries)
    return [bucket["entry"] for bucket in assignments.values()]


def _classify_files(
    file_infos: list[dict[str, Any]], ecosystem: str
) -> tuple[list[str], list[str], list[str], list[dict[str, Any]]]:
    """Classify changed files into manifests, lockfiles, other_files.

    Returns (manifests, lockfiles, other_files, file_metadata).
    Unknown files go to other_files, not manifests.
    """
    manifests: list[str] = []
    lockfiles: list[str] = []
    other_files: list[str] = []
    file_metadata: list[dict[str, Any]] = []

    for info in file_infos:
        filename = info.get("filename", "")
        status = info.get("status", "unknown")
        file_metadata.append({
            "filename": filename,
            "status": status,
            "previous_filename": info.get("previous_filename"),
        })

        if _is_ecosystem_lockfile(filename, ecosystem):
            lockfiles.append(filename)
        elif _is_ecosystem_manifest(filename, ecosystem):
            manifests.append(filename)
        else:
            other_files.append(filename)

    return manifests, lockfiles, other_files, file_metadata


def _determine_dependency_type(
    body_metadata: list[dict[str, str]],
    manifests: list[str],
    lockfiles: list[str],
    dep_name: str,
) -> str:
    """Determine direct/transitive/indeterminate.

    Priority: body metadata first, then manifest/lockfile evidence.
    """
    # Check body metadata first
    for meta in body_metadata:
        if meta.get("dependency-name", "").strip() == dep_name:
            dep_type_raw = meta.get("dependency-type", "")
            if "indirect" in dep_type_raw:
                return "transitive"
            if "direct" in dep_type_raw:
                return "direct"

    # Fallback to file evidence
    if lockfiles and not manifests:
        return "transitive"
    if manifests and not lockfiles:
        return "direct"
    if manifests:
        return "direct"
    return "indeterminate"


def _resolve_domains_and_exposure(
    ecosystem: str, directory: str, changed_files: list[str]
) -> tuple[list[str], list[str], list[str]]:
    """Resolve domains, exposure, and required validation scope from actual paths.

    Returns (domains, exposure_categories, validation_scope).
    """
    domains: set[str] = set()
    exposure: set[str] = set()
    validation: set[str] = set()

    # Base from directory
    entry_key = (ecosystem, directory)
    dir_domains = _ENTRY_DOMAIN.get(entry_key, ["dependency-management"])
    domains.update(dir_domains)
    dir_exposure = _ENTRY_EXPOSURE.get(entry_key, ["runtime"])
    exposure.update(dir_exposure)

    # Expand from actual file paths
    for f in changed_files:
        for prefix, domain, exp_cats in _PATH_DOMAIN_RULES:
            if f.startswith(prefix):
                domains.add(domain)
                exposure.update(exp_cats)

    # Build concrete validation scope
    validation.add("technical-evidence")
    if "runtime" in exposure:
        validation.add("python-package-tests")
    if "build" in exposure:
        validation.add("package-build")
    if "test" in exposure:
        validation.add("test-suite")
    if "generated-output" in exposure or "toolchain" in exposure:
        validation.add("generated-output-verification")
    if "compiler-runtime-test" in exposure:
        validation.add("target-compiler-runtime-test")
    if "ci" in exposure:
        validation.add("ci-workflow-validation")
    if "editor-extension" in exposure:
        validation.add("vscode-extension-tests")
    if "documentation" in exposure:
        validation.add("documentation-build")

    return sorted(domains), sorted(exposure), sorted(validation)


def _identity_checks(
    pr: dict[str, Any],
    sender: str,
    configured_prefixes: Sequence[str],
) -> dict[str, Any]:
    """Compute Dependabot identity metadata checks."""
    author = pr.get("user", {}).get("login", "")
    head_ref = pr.get("head", {}).get("ref", "")
    title = pr.get("title", "")
    stripped_title, prefix = _strip_dependabot_prefix(title, configured_prefixes)
    return {
        "author_is_dependabot_bot": author == "dependabot[bot]",
        "sender_is_dependabot_bot": sender == "dependabot[bot]",
        "head_ref_prefix": head_ref.startswith("dependabot/"),
        "title_matches_pattern": bool(re.match(
            r"^(Bump|Update)\s+", stripped_title, re.IGNORECASE
        )),
        "configured_title_prefix": prefix,
    }


def normalize_dependabot(
    event_json: str,
    files_json: str,
    processor_sha: str = "local-worktree",
) -> tuple[dict[str, Any], str]:
    """Normalize a Dependabot PR event into a record and markdown summary.

    Identity: requires pull_request.user.login == "dependabot[bot]".
    Branch-name substring NEVER authenticates the bot.
    """
    event = json.loads(event_json)
    files_data = json.loads(files_json)
    source_digest = _sha256(event_json)

    pr = event.get("pull_request", event)
    number = pr.get("number", 0)
    title = pr.get("title", "")
    head_sha = pr.get("head", {}).get("sha", "")
    base_sha = pr.get("base", {}).get("sha", "")
    author = pr.get("user", {}).get("login", "")
    head_ref = pr.get("head", {}).get("ref", "")
    action = event.get("action", "opened")
    repository = event.get("repository", {}).get("full_name", "")
    sender = event.get("sender", {}).get("login", "")
    body = pr.get("body", "") or ""
    observation = event.get("intake_observation", {})

    # Flatten paginated files
    flat_files = _flatten_paginated_files(files_data)
    stable_file_metadata = [
        {
            "filename": str(info.get("filename") or ""),
            "previous_filename": info.get("previous_filename"),
            "status": str(info.get("status") or "unknown"),
            "sha": str(info.get("sha") or ""),
            "additions": int(info.get("additions") or 0),
            "deletions": int(info.get("deletions") or 0),
            "changes": int(info.get("changes") or 0),
        }
        for info in flat_files
        if info.get("filename")
    ]

    # Compute digests
    files_digest = _sha256(
        json.dumps(stable_file_metadata, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    )
    if not DEPENDABOT_CONFIG.is_file():
        raise RuntimeError(f"Dependabot config not found: {DEPENDABOT_CONFIG}")
    config_text = DEPENDABOT_CONFIG.read_text(encoding="utf-8")
    config_digest = _sha256(config_text)
    if not CAPABILITIES_JSON.is_file():
        raise RuntimeError(f"Capability profile not found: {CAPABILITIES_JSON}")
    capabilities_text = CAPABILITIES_JSON.read_text(encoding="utf-8")
    capability_digest = _sha256(capabilities_text)
    config_entries = _parse_dependabot_config()
    configured_prefixes = [
        entry["prefix"] for entry in config_entries if isinstance(entry.get("prefix"), str)
    ]
    identity_checks = _identity_checks(pr, sender, configured_prefixes)
    is_dependabot = (
        identity_checks["author_is_dependabot_bot"]
        and identity_checks["sender_is_dependabot_bot"]
    )
    observed_before = str(observation.get("head_before") or head_sha)
    observed_after = str(observation.get("head_after") or head_sha)
    head_verified_before = observed_before == head_sha
    head_verified_after = observed_after == head_sha
    combined_source = (
        f"{source_digest}:{files_digest}:{config_digest}:{capability_digest}:"
        f"{processor_sha}:{observed_before}:{observed_after}"
    )
    combined_digest = _sha256(combined_source)

    if not is_dependabot:
        record: dict[str, Any] = {
            "schema_version": 1,
            "record_kind": "dependabot-intake",
            "pr_number": number,
            "event_identity": {
                "pr_number": number,
                "repository": repository,
                "event_type": action,
                "head_sha": head_sha,
                "base_sha": base_sha,
                "author": author,
                "sender": sender,
                "processor_sha": processor_sha,
                "identity_checks": identity_checks,
                "observed_head_before": observed_before,
                "observed_head_after": observed_after,
                "head_verified_before": head_verified_before,
                "head_verified_after": head_verified_after,
                "files_digest": files_digest,
                "config_digest": config_digest,
                "capability_digest": capability_digest,
                "source_digest": combined_digest,
            },
            "classification": {
                "is_dependabot": False,
                "status": "ignored",
                "reason": "unauthorized",
                "missing_info": [],
            },
            "normalized_facts": {},
            "authority": {
                "authorized": False,
                "statement": AUTHORITY_STATEMENT,
            },
        }
        _validate_or_raise(record, DEPENDABOT_RECORD_SCHEMA, "dependabot intake record")
        markdown = (
            "## Dependabot Intake Summary\n\n"
            f"- **PR**: #{number}\n"
            f"- **Status**: ignored (not a Dependabot PR)\n"
            f"- **Author**: {author}\n\n"
            f"> {AUTHORITY_STATEMENT}\n"
        )
        return record, markdown

    if not head_verified_before or not head_verified_after:
        record = {
            "schema_version": 1,
            "record_kind": "dependabot-intake",
            "pr_number": number,
            "event_identity": {
                "pr_number": number,
                "repository": repository,
                "event_type": action,
                "head_sha": head_sha,
                "base_sha": base_sha,
                "author": author,
                "sender": sender,
                "processor_sha": processor_sha,
                "identity_checks": identity_checks,
                "observed_head_before": observed_before,
                "observed_head_after": observed_after,
                "head_verified_before": head_verified_before,
                "head_verified_after": head_verified_after,
                "files_digest": files_digest,
                "config_digest": config_digest,
                "capability_digest": capability_digest,
                "source_digest": combined_digest,
            },
            "classification": {
                "is_dependabot": True,
                "status": "superseded",
                "reason": "head-changed-during-retrieval",
                "missing_info": ["stable-head"],
            },
            "normalized_facts": {},
            "authority": {"authorized": False, "statement": AUTHORITY_STATEMENT},
        }
        _validate_or_raise(record, DEPENDABOT_RECORD_SCHEMA, "dependabot intake record")
        markdown = (
            "## Dependabot Intake Summary\n\n"
            f"- **PR**: #{number}\n"
            "- **Status**: superseded; head changed during metadata retrieval\n"
            f"- **Event head**: `{head_sha}`\n"
            f"- **Before files**: `{observed_before}`\n"
            f"- **After files**: `{observed_after}`\n\n"
            f"> {AUTHORITY_STATEMENT}\n"
        )
        return record, markdown

    # Assign every changed file to its own most-specific ecosystem entry
    assignments, unmatched_files = _assign_files_to_entries(flat_files, config_entries)
    matched_entries = [bucket["entry"] for bucket in assignments.values()]

    # Primary entry: most-specific directory
    primary_entry: dict[str, Any] | None = None
    if matched_entries:
        primary_entry = max(matched_entries, key=lambda e: len(e["directory"]))

    ecosystem = primary_entry["package-ecosystem"] if primary_entry else "unknown"
    directory = primary_entry["directory"] if primary_entry else "/"
    multi_ecosystem = len({entry["package-ecosystem"] for entry in matched_entries}) > 1

    # Parse dependencies from title and body
    deps_from_title = _parse_dependabot_title(title, configured_prefixes)
    body_metadata = _parse_dependabot_body_metadata(body)

    # Per-ecosystem classification: each matched entry classifies only its own files
    ecosystem_groups: list[dict[str, Any]] = []
    manifests: list[str] = []
    lockfiles: list[str] = []
    other_files: list[str] = []
    domain_set: set[str] = set()
    exposure_set: set[str] = set()
    validation_set: set[str] = set()

    for key in sorted(assignments):
        bucket = assignments[key]
        entry = bucket["entry"]
        entry_ecosystem = entry["package-ecosystem"]
        entry_directory = entry["directory"]
        group_manifests, group_lockfiles, group_other, _ = _classify_files(
            bucket["files"], entry_ecosystem
        )
        group_files = group_manifests + group_lockfiles + group_other
        group_domains, group_exposure, group_validation = _resolve_domains_and_exposure(
            entry_ecosystem, entry_directory, group_files
        )
        ecosystem_groups.append({
            "ecosystem": entry_ecosystem,
            "directory": entry_directory,
            "manifests_changed": group_manifests,
            "lockfiles_changed": group_lockfiles,
            "other_files": group_other,
            "domains": group_domains,
            "exposure_categories": group_exposure,
            "required_validation_scope": group_validation,
        })
        manifests.extend(group_manifests)
        lockfiles.extend(group_lockfiles)
        other_files.extend(group_other)
        domain_set.update(group_domains)
        exposure_set.update(group_exposure)
        validation_set.update(group_validation)

    other_files.extend(unmatched_files)
    file_metadata = stable_file_metadata

    dependency_ecosystem = ecosystem if not multi_ecosystem else "indeterminate"
    dependency_directory = directory if not multi_ecosystem else "multiple"

    # Build dependency records (merge title + body metadata)
    dependencies: list[dict[str, Any]] = []
    if deps_from_title:
        for dep in deps_from_title:
            dep_name = dep["name"]
            dep_type = _determine_dependency_type(body_metadata, manifests, lockfiles, dep_name)

            # Check body metadata for update-type
            update_type_from_body: str | None = None
            for meta in body_metadata:
                if meta.get("dependency-name", "").strip() == dep_name:
                    ut_raw = meta.get("update-type", "")
                    if "major" in ut_raw:
                        update_type_from_body = "major"
                    elif "minor" in ut_raw:
                        update_type_from_body = "minor"
                    elif "patch" in ut_raw:
                        update_type_from_body = "patch"
                    break

            update_type = update_type_from_body or _classify_version_bump(
                dep.get("old_version"), dep.get("new_version")
            )
            dependencies.append({
                "name": dep_name,
                "old_version": dep.get("old_version"),
                "new_version": dep.get("new_version"),
                "ecosystem": dependency_ecosystem,
                "directory": dependency_directory,
                "update_type": update_type,
                "dependency_type": dep_type,
            })
    elif body_metadata:
        # Group updates: deps come from body only
        for meta in body_metadata:
            dep_name = meta.get("dependency-name", "unknown")
            dep_type_raw = meta.get("dependency-type", "")
            dep_type = "indeterminate"
            if "indirect" in dep_type_raw:
                dep_type = "transitive"
            elif "direct" in dep_type_raw:
                dep_type = "direct"

            ut_raw = meta.get("update-type", "")
            update_type = "unknown"
            if "major" in ut_raw:
                update_type = "major"
            elif "minor" in ut_raw:
                update_type = "minor"
            elif "patch" in ut_raw:
                update_type = "patch"

            dependencies.append({
                "name": dep_name,
                "old_version": None,
                "new_version": None,
                "ecosystem": dependency_ecosystem,
                "directory": dependency_directory,
                "update_type": update_type,
                "dependency_type": dep_type,
            })

    # Domains and exposure aggregated from every matched ecosystem group
    if ecosystem_groups:
        domains = sorted(domain_set)
        exposure_cats = sorted(exposure_set)
        validation_scope = sorted(validation_set)
    else:
        all_changed = other_files
        domains, exposure_cats, validation_scope = _resolve_domains_and_exposure(
            ecosystem, "/", all_changed
        )

    major_risk = any(d.get("update_type") == "major" for d in dependencies)
    unknown_risk = any(d.get("update_type") == "unknown" for d in dependencies)

    if major_risk:
        validation_scope = sorted(set(validation_scope) | {"compatibility-review", "review-required"})

    # Missing info
    missing_info: list[str] = []
    if not dependencies:
        missing_info.append("dependency-details")
    if not matched_entries:
        missing_info.append("config-entry-match")

    status = "complete" if not missing_info else "incomplete"

    # Config entries for record (all matched, not just primary)
    config_entries_record = [
        {"ecosystem": e["package-ecosystem"], "directory": e["directory"]}
        for e in matched_entries
    ]

    record = {
        "schema_version": 1,
        "record_kind": "dependabot-intake",
        "pr_number": number,
        "event_identity": {
            "pr_number": number,
            "repository": repository,
            "event_type": action,
            "head_sha": head_sha,
            "base_sha": base_sha,
            "author": author,
            "sender": sender,
            "processor_sha": processor_sha,
            "identity_checks": identity_checks,
            "observed_head_before": observed_before,
            "observed_head_after": observed_after,
            "head_verified_before": head_verified_before,
            "head_verified_after": head_verified_after,
            "files_digest": files_digest,
            "config_digest": config_digest,
            "capability_digest": capability_digest,
            "source_digest": combined_digest,
        },
        "classification": {
            "is_dependabot": True,
            "status": status,
            "missing_info": missing_info,
        },
        "normalized_facts": {
            "dependencies": dependencies,
            "config_entries": config_entries_record,
            "config_entry": config_entries_record[0] if config_entries_record else {
                "ecosystem": "unknown", "directory": "/",
            },
            "ecosystems": ecosystem_groups,
            "multi_ecosystem": multi_ecosystem,
            "unmatched_files": unmatched_files,
            "manifests_changed": manifests,
            "lockfiles_changed": lockfiles,
            "other_files": other_files,
            "file_metadata": file_metadata,
            "domains": domains,
            "exposure": {
                "categories": exposure_cats,
                "generated_output_implications": "generated-output" in exposure_cats,
                "toolchain_implications": "toolchain" in exposure_cats,
            },
            "major_version_risk": major_risk,
            "unknown_version_risk": unknown_risk,
            "review_required": major_risk or unknown_risk,
            "safe_merge_inferred": False,
            "exploitability_inferred": False,
            "required_validation_scope": validation_scope,
        },
        "authority": {
            "authorized": False,
            "statement": AUTHORITY_STATEMENT,
        },
    }
    _validate_or_raise(record, DEPENDABOT_RECORD_SCHEMA, "dependabot intake record")

    # Markdown summary
    md_lines = [
        "## Dependabot Intake Summary",
        "",
        f"- **PR**: #{number}",
        f"- **Event**: {action}",
        f"- **Status**: {status}",
        f"- **Ecosystem**: {ecosystem}",
        f"- **Directory**: `{directory}`",
        f"- **Head SHA**: `{head_sha[:12]}...`" if head_sha else "- **Head SHA**: unknown",
    ]
    if dependencies:
        md_lines.append("- **Dependencies**:")
        for dep in dependencies:
            md_lines.append(
                f"  - `{dep['name']}`: {dep.get('old_version', '?')} -> {dep.get('new_version', '?')} "
                f"({dep['update_type']}, {dep['dependency_type']})"
            )
    if domains:
        md_lines.append(f"- **Domains**: {', '.join(domains)}")
    if ecosystem_groups:
        md_lines.append("- **Ecosystem classification**:")
        for group in ecosystem_groups:
            md_lines.append(
                f"  - `{group['ecosystem']}` in `{group['directory']}`: "
                f"{len(group['manifests_changed'])} manifest(s), "
                f"{len(group['lockfiles_changed'])} lockfile(s), "
                f"{len(group['other_files'])} other file(s)"
            )
    if multi_ecosystem:
        md_lines.append("- **Multi-ecosystem**: each ecosystem classified separately")
    if unmatched_files:
        md_lines.append(f"- **Unmatched files**: {len(unmatched_files)}")
    if major_risk:
        md_lines.append("- **Major version risk**: review-required")
    if unknown_risk:
        md_lines.append("- **Unknown version risk**: review-required")
    md_lines.append("- **Safe merge inferred**: no (never inferred)")
    md_lines.append("- **Exploitability inferred**: no (never inferred)")
    if missing_info:
        md_lines.append(f"- **Missing**: {', '.join(missing_info)}")
    md_lines.extend([
        "",
        f"> {AUTHORITY_STATEMENT}",
    ])
    markdown = "\n".join(md_lines) + "\n"

    return record, markdown


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="mode")

    issue_parser = subparsers.add_parser("issue", help="Normalize a GitHub issue event.")
    issue_parser.add_argument("--event", required=True, type=Path, help="Path to issue event JSON.")
    issue_parser.add_argument("--output-json", type=Path, help="Write record JSON to this path.")
    issue_parser.add_argument("--output-md", type=Path, help="Write summary Markdown to this path.")
    issue_parser.add_argument("--processor-sha", default="local-worktree")
    issue_parser.add_argument("--semantic-preflight", type=Path)
    issue_parser.add_argument("--copilot-output", type=Path)
    issue_parser.add_argument("--copilot-stderr", type=Path)
    issue_parser.add_argument("--copilot-exit-code", type=int)
    issue_parser.add_argument("--minimize-content", action="store_true")

    preflight_parser = subparsers.add_parser(
        "issue-preflight",
        help="Validate trusted resources and prepare a no-tools Copilot prompt.",
    )
    preflight_parser.add_argument("--event", required=True, type=Path)
    preflight_parser.add_argument("--output-json", required=True, type=Path)
    preflight_parser.add_argument("--output-prompt", required=True, type=Path)
    preflight_parser.add_argument("--processor-sha", default="local-worktree")

    dep_parser = subparsers.add_parser("dependabot", help="Normalize a Dependabot PR event.")
    dep_parser.add_argument("--event", required=True, type=Path, help="Path to PR event JSON.")
    dep_parser.add_argument("--files", required=True, type=Path, help="Path to changed-files JSON.")
    dep_parser.add_argument("--output-json", type=Path, help="Write record JSON to this path.")
    dep_parser.add_argument("--output-md", type=Path, help="Write summary Markdown to this path.")
    dep_parser.add_argument("--processor-sha", default="local-worktree")

    args = parser.parse_args(argv)

    if not args.mode:
        parser.print_help()
        return 1

    if args.mode == "issue-preflight":
        event_text = args.event.read_text(encoding="utf-8")
        preflight, prompt = prepare_issue_assistance(event_text)
        # Validate the complete fallback record before any Copilot request consumes AIC.
        normalize_issue(
            event_text,
            args.processor_sha,
            semantic_preflight=preflight,
            minimize_content=True,
        )
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(
            json.dumps(preflight, indent=2, sort_keys=False) + "\n",
            encoding="utf-8",
        )
        args.output_prompt.parent.mkdir(parents=True, exist_ok=True)
        args.output_prompt.write_text(
            prompt if preflight["eligible"] else "",
            encoding="utf-8",
        )
        return 0

    if args.mode == "issue":
        event_text = args.event.read_text(encoding="utf-8")
        semantic_preflight = (
            _load_json_strict(args.semantic_preflight)
            if args.semantic_preflight
            else None
        )
        semantic_output = (
            args.copilot_output.read_text(encoding="utf-8")
            if args.copilot_output and args.copilot_output.is_file()
            else None
        )
        semantic_stderr = (
            args.copilot_stderr.read_text(encoding="utf-8", errors="replace")
            if args.copilot_stderr and args.copilot_stderr.is_file()
            else None
        )
        record, markdown = normalize_issue(
            event_text,
            args.processor_sha,
            semantic_preflight=semantic_preflight,
            semantic_output=semantic_output,
            semantic_exit_code=args.copilot_exit_code,
            semantic_stderr=semantic_stderr,
            minimize_content=args.minimize_content,
        )
    elif args.mode == "dependabot":
        event_text = args.event.read_text(encoding="utf-8")
        files_text = args.files.read_text(encoding="utf-8")
        record, markdown = normalize_dependabot(event_text, files_text, args.processor_sha)
    else:
        parser.print_help()
        return 1

    schema_dir = REPO_ROOT / ".github" / "governance" / "schemas"
    schema_path = None
    if args.mode == "issue":
        schema_path = schema_dir / "issue-intake-record.schema.json"
    elif args.mode == "dependabot":
        schema_path = schema_dir / "dependabot-intake-record.schema.json"
    if schema_path is not None:
        validation_errors = _validate_record(record, schema_path)
        if validation_errors:
            print("Schema validation errors:", file=sys.stderr)
            for error in validation_errors:
                print(f"  - {error}", file=sys.stderr)
            return 2

    record_json = json.dumps(record, indent=2, sort_keys=False)

    if args.output_json:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(record_json + "\n", encoding="utf-8")
    else:
        print(record_json)

    if args.output_md:
        args.output_md.parent.mkdir(parents=True, exist_ok=True)
        args.output_md.write_text(markdown, encoding="utf-8")
    else:
        print(markdown, file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
