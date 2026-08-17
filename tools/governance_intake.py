"""Deterministic governance intake normalizer for Avrotize.

Two CLI modes:
  python tools/governance_intake.py issue   --event EVENT_JSON
  python tools/governance_intake.py dependabot --event EVENT_JSON --files FILES_JSON

Standard library only. Outputs versioned JSON records and Markdown summaries.
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

from tools import governance_schema  # noqa: E402  (path bootstrap above enables direct execution)

ISSUE_FORM_CONTRACT = REPO_ROOT / ".github" / "governance" / "issue-form-contract.json"
COMMANDS_JSON = REPO_ROOT / "avrotize" / "commands.json"
DEPENDABOT_CONFIG = REPO_ROOT / ".github" / "dependabot.yml"
CAPABILITIES_JSON = REPO_ROOT / ".github" / "governance" / "avrotize-capabilities.json"
SCHEMA_DIR = REPO_ROOT / ".github" / "governance" / "schemas"
ISSUE_RECORD_SCHEMA = SCHEMA_DIR / "issue-intake-record.schema.json"
DEPENDABOT_RECORD_SCHEMA = SCHEMA_DIR / "dependabot-intake-record.schema.json"

AUTHORITY_STATEMENT = (
    "Intake normalization does not authorize implementation, schedule work, "
    "approve compatibility, or permit merge. Repository owner retains authority."
)

# Supported surface options from the issue form contract (substring match)
_SUPPORTED_SURFACE_TOKENS = {"cli", "python", "mcp", "vs code", "vscode"}


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def _sha256(data: str) -> str:
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _load_json_strict(path: Path) -> Any:
    """Load JSON, raising on any error (corrupt config = infrastructure failure)."""
    text = path.read_text(encoding="utf-8")
    return json.loads(text)


def _load_commands() -> set[str]:
    """Load known command names from commands.json. Corrupt registry = hard fail."""
    if not COMMANDS_JSON.is_file():
        raise RuntimeError(f"Commands registry not found: {COMMANDS_JSON}")
    data = _load_json_strict(COMMANDS_JSON)
    if not isinstance(data, list):
        raise RuntimeError(f"Commands registry is not an array: {COMMANDS_JSON}")
    result: set[str] = set()
    for item in data:
        if not isinstance(item, dict) or "command" not in item:
            raise RuntimeError(f"Corrupt entry in commands registry: {item!r}")
        result.add(item["command"])
    return result


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
    """Parse GitHub Issue Form rendered body into heading->content map."""
    sections: dict[str, str] = {}
    headings = list(_HEADING_RE.finditer(body))
    for i, match in enumerate(headings):
        heading = match.group(1).strip()
        start = match.end()
        end = headings[i + 1].start() if i + 1 < len(headings) else len(body)
        content = body[start:end].strip()
        if content == "_No response_":
            content = ""
        sections[heading] = content
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


def _resolve_semantic_paths(command: str | None, surface: str | None, known_commands: set[str]) -> list[str]:
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


def _validate_surface(surface: str | None) -> tuple[bool, str | None]:
    """Validate surface contains a recognized token."""
    if not surface:
        return True, None
    lower = surface.strip().lower()
    if any(token in lower for token in _SUPPORTED_SURFACE_TOKENS):
        return True, None
    return False, f"unsupported surface: {surface.strip()}"


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
    mapping = contract.get("expected_result_choices")
    if not isinstance(mapping, dict) or not mapping:
        raise RuntimeError("Issue form contract lacks expected_result_choices mapping")
    raw = (choice or "").strip()
    if not raw or _is_placeholder(raw):
        return "", "undeclared"
    kind = mapping.get(raw)
    if not isinstance(kind, str):
        return raw, "undeclared"
    return raw, kind


def normalize_issue(event_json: str) -> tuple[dict[str, Any], str]:
    """Normalize an issue event into a record and markdown summary.

    Returns (record_dict, markdown_str).
    Raises on corrupt config/infrastructure. Incomplete/unknown exits successfully.
    """
    event = json.loads(event_json)
    contract = _load_issue_form_contract()
    known_commands = _load_commands()

    issue = event.get("issue", event)
    title = issue.get("title", "")
    body = issue.get("body") or ""
    number = issue.get("number", 0)
    action = event.get("action", "opened")
    repository = event.get("repository", {}).get("full_name", "")
    url = issue.get("html_url", "")
    sender = event.get("sender", {}).get("login", "")

    body_digest = _sha256(body)
    source_digest = _sha256(event_json)

    form_type, form_spec = _detect_form_type(title, contract)

    # Parse sections
    sections = _parse_issue_body(body)

    # Determine completeness
    status = "manual-triage"
    missing_fields: list[str] = []
    unexpected_headings: list[str] = []
    missing_headings: list[str] = []
    surface_error: str | None = None
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
        missing_headings = missing_required_headings + missing_optional_headings

        # Check for malformed: known title prefix but no meaningful body content
        if not body.strip() or not sections:
            status = "malformed"
        else:
            # Check required fields - reject placeholders
            for req in form_spec.get("required_semantic_fields", []):
                val = field_values.get(req, "")
                if _is_placeholder(val):
                    missing_fields.append(req)

            # Validate surface option
            surface_val = field_values.get("surface", "")
            if surface_val:
                valid, err = _validate_surface(surface_val)
                if not valid:
                    surface_error = err

            if unexpected_headings:
                status = "malformed"
            elif missing_fields:
                status = "incomplete"
            elif surface_error:
                status = "malformed"
            else:
                status = "complete"

            # Extract normalized facts
            cmd_raw = field_values.get("command", "")
            if cmd_raw and not _is_placeholder(cmd_raw):
                normalized["command"] = cmd_raw
                # Extract command token with flag awareness
                tokens = cmd_raw.strip().split(",")[0].strip().split()
                cmd_tokens = [t for t in tokens if not t.startswith("-") and t != "avrotize" and t != "python" and t != "-m"]
                cmd_token = cmd_tokens[0] if cmd_tokens else ""
                if "." in cmd_token:
                    parts = cmd_token.split(".")
                    cmd_token = parts[-1] if len(parts) > 1 else cmd_token
                normalized["command_known"] = cmd_token in known_commands or any(
                    cmd_token in c for c in known_commands
                )

            surface_raw = field_values.get("surface") or None
            if surface_raw and not _is_placeholder(surface_raw):
                normalized["surface"] = surface_raw
            normalized["semantic_paths"] = _resolve_semantic_paths(
                normalized["command"], normalized["surface"], known_commands
            )
            normalized["source_representation"] = field_values.get("input") or None
            normalized["result_representation"] = field_values.get("output") or None
            normalized["flags_options"] = field_values.get("invocation") or field_values.get("options") or None
            normalized["input_reproducer"] = field_values.get("input") or None
            normalized["actual_behavior"] = field_values.get("actual") or None
            normalized["expected_behavior"] = field_values.get("expected") or field_values.get("outcome") or None
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
            normalized["semantics"] = field_values.get("semantics") or None
            normalized["validation_expectations"] = field_values.get("validation") or None
            normalized["documentation"] = field_values.get("documentation") or None

    record: dict[str, Any] = {
        "schema_version": 1,
        "record_kind": "issue-intake",
        "event_identity": {
            "issue_number": number,
            "repository": repository,
            "url": url,
            "event_type": action,
            "sender": sender,
            "body_digest": body_digest,
            "source_digest": source_digest,
            "update": action in ("edited", "reopened"),
        },
        "classification": {
            "form_type": form_type,
            "status": status,
            "missing_fields": missing_fields,
            "unexpected_headings": unexpected_headings,
            "missing_headings": missing_headings,
        },
        "normalized_facts": normalized,
        "authority": {
            "authorized": False,
            "statement": AUTHORITY_STATEMENT,
        },
    }
    _validate_or_raise(record, ISSUE_RECORD_SCHEMA, "issue intake record")

    # Markdown summary
    md_lines = [
        "## Issue Intake Summary",
        "",
        f"- **Issue**: #{number}",
        f"- **Event**: {action}",
        f"- **Form type**: {form_type}",
        f"- **Status**: {status}",
        f"- **Body digest**: `{body_digest[:16]}...`",
    ]
    if missing_fields:
        md_lines.append(f"- **Missing fields**: {', '.join(missing_fields)}")
    if unexpected_headings:
        md_lines.append(f"- **Unexpected headings**: {', '.join(unexpected_headings)}")
    if missing_headings:
        md_lines.append(f"- **Missing headings**: {', '.join(missing_headings)}")
    if surface_error:
        md_lines.append(f"- **Surface error**: {surface_error}")
    if normalized["command"]:
        known_str = "yes" if normalized["command_known"] else "no/unknown"
        md_lines.append(f"- **Command**: `{normalized['command']}` (known: {known_str})")
    if normalized["semantic_paths"]:
        md_lines.append(f"- **Semantic paths**: {', '.join(normalized['semantic_paths'])}")
    if form_type == "bug":
        md_lines.append(f"- **Declared expectation**: {normalized['expected_result_kind']}")
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

# Directory -> domain mapping
_DIRECTORY_DOMAIN: dict[str, list[str]] = {
    "/": ["root-python-package"],
    "/avrotize/dependencies/python/py312": ["generated-output", "toolchain"],
    "/avrotize/dependencies/cs/net100": ["generated-output", "toolchain"],
    "/avrotize/dependencies/java/jdk21": ["generated-output", "toolchain"],
    "/avrotize/dependencies/typescript/node22": ["generated-output", "toolchain"],
    "/avrotize/dependencies/go/go121": ["generated-output", "toolchain"],
    "/avrotize/dependencies/rust/stable": ["generated-output", "toolchain"],
}

# Directory -> exposure categories
_DIRECTORY_EXPOSURE: dict[str, list[str]] = {
    "/": ["runtime", "build", "test"],
    "/avrotize/dependencies/python/py312": ["generated-output", "toolchain", "compiler-runtime-test"],
    "/avrotize/dependencies/cs/net100": ["generated-output", "toolchain", "compiler-runtime-test"],
    "/avrotize/dependencies/java/jdk21": ["generated-output", "toolchain", "compiler-runtime-test"],
    "/avrotize/dependencies/typescript/node22": ["generated-output", "toolchain", "compiler-runtime-test"],
    "/avrotize/dependencies/go/go121": ["generated-output", "toolchain", "compiler-runtime-test"],
    "/avrotize/dependencies/rust/stable": ["generated-output", "toolchain", "compiler-runtime-test"],
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


def _parse_dependabot_title(title: str) -> list[dict[str, Any]]:
    """Extract dependency name and versions from Dependabot PR title."""
    deps: list[dict[str, Any]] = []
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
    directory: str, changed_files: list[str]
) -> tuple[list[str], list[str], list[str]]:
    """Resolve domains, exposure, and required validation scope from actual paths.

    Returns (domains, exposure_categories, validation_scope).
    """
    domains: set[str] = set()
    exposure: set[str] = set()
    validation: set[str] = set()

    # Base from directory
    dir_domains = _DIRECTORY_DOMAIN.get(directory, ["root-python-package"])
    domains.update(dir_domains)
    dir_exposure = _DIRECTORY_EXPOSURE.get(directory, ["runtime"])
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


def _identity_checks(pr: dict[str, Any]) -> dict[str, Any]:
    """Compute Dependabot identity metadata checks."""
    author = pr.get("user", {}).get("login", "")
    head_ref = pr.get("head", {}).get("ref", "")
    title = pr.get("title", "")
    return {
        "author_is_dependabot_bot": author == "dependabot[bot]",
        "head_ref_prefix": head_ref.startswith("dependabot/"),
        "title_matches_pattern": bool(re.match(
            r"^(Bump|Update)\s+", title, re.IGNORECASE
        )),
    }


def normalize_dependabot(event_json: str, files_json: str) -> tuple[dict[str, Any], str]:
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

    # Flatten paginated files
    flat_files = _flatten_paginated_files(files_data)
    changed_filenames = [f.get("filename", "") for f in flat_files if f.get("filename")]

    # Identity: ONLY author login determines Dependabot identity
    is_dependabot = (author == "dependabot[bot]")
    identity_checks = _identity_checks(pr)

    # Compute digests
    files_digest = _sha256(json.dumps(changed_filenames, sort_keys=True))
    if not DEPENDABOT_CONFIG.is_file():
        raise RuntimeError(f"Dependabot config not found: {DEPENDABOT_CONFIG}")
    config_text = DEPENDABOT_CONFIG.read_text(encoding="utf-8")
    config_digest = _sha256(config_text)
    if not CAPABILITIES_JSON.is_file():
        raise RuntimeError(f"Capability profile not found: {CAPABILITIES_JSON}")
    capabilities_text = CAPABILITIES_JSON.read_text(encoding="utf-8")
    capability_digest = _sha256(capabilities_text)
    combined_source = f"{source_digest}:{files_digest}:{config_digest}:{capability_digest}"
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
                "identity_checks": identity_checks,
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

    # Parse config
    config_entries = _parse_dependabot_config()

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
    deps_from_title = _parse_dependabot_title(title)
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
            entry_directory, group_files
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
    file_metadata = [
        {
            "filename": info.get("filename", ""),
            "status": info.get("status", "unknown"),
            "previous_filename": info.get("previous_filename"),
        }
        for info in flat_files
    ]

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
        domains, exposure_cats, validation_scope = _resolve_domains_and_exposure("/", all_changed)

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
            "identity_checks": identity_checks,
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

    dep_parser = subparsers.add_parser("dependabot", help="Normalize a Dependabot PR event.")
    dep_parser.add_argument("--event", required=True, type=Path, help="Path to PR event JSON.")
    dep_parser.add_argument("--files", required=True, type=Path, help="Path to changed-files JSON.")
    dep_parser.add_argument("--output-json", type=Path, help="Write record JSON to this path.")
    dep_parser.add_argument("--output-md", type=Path, help="Write summary Markdown to this path.")

    args = parser.parse_args(argv)

    if not args.mode:
        parser.print_help()
        return 1

    if args.mode == "issue":
        event_text = args.event.read_text(encoding="utf-8")
        record, markdown = normalize_issue(event_text)
    elif args.mode == "dependabot":
        event_text = args.event.read_text(encoding="utf-8")
        files_text = args.files.read_text(encoding="utf-8")
        record, markdown = normalize_dependabot(event_text, files_text)
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
