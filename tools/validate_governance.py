"""Deterministically validate Avrotize governance surfaces."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

try:  # pragma: no cover - import shape depends on invocation style
    from tools import governance_schema
except ImportError:  # pragma: no cover - direct script execution
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from tools import governance_schema


REQUIRED_FILES = (
    ".gitattributes",
    "GOVERNANCE.md",
    "CONTRIBUTING.md",
    "SECURITY.md",
    "SUPPORT.md",
    ".github/CODEOWNERS",
    ".github/pull_request_template.md",
    ".github/ISSUE_TEMPLATE/config.yml",
    ".github/ISSUE_TEMPLATE/bug.yml",
    ".github/ISSUE_TEMPLATE/feature.yml",
    ".github/ISSUE_TEMPLATE/question.yml",
    ".github/governance/AUTOMATION.md",
    ".github/governance/AI-USAGE-ACCOUNTING.md",
    ".github/governance/ADOPTION.md",
    ".github/governance/EXTERNAL-SUPERVISOR.md",
    ".github/governance/requirements-ci.txt",
    ".github/governance/avrotize-capabilities.json",
    ".github/governance/workflow-contracts.json",
    ".github/governance/issue-form-contract.json",
    ".github/governance/copilot-intake-policy.json",
    ".github/governance/external-supervisor-policy.json",
    ".github/governance/copilot-cli/package.json",
    ".github/governance/copilot-cli/package-lock.json",
    ".github/governance/prompts/external-supervisor-kickoff-v2.txt",
    ".github/governance/prompts/issue-semantic-assistance-v1.txt",
    ".github/governance/repro-label-catalog.json",
    ".github/governance/schemas/issue-intake-record.schema.json",
    ".github/governance/schemas/issue-semantic-assistance.schema.json",
    ".github/governance/schemas/dependabot-intake-record.schema.json",
    ".github/governance/schemas/repro-evidence-record.schema.json",
    ".github/governance/schemas/repro-terminal-fallback.schema.json",
    ".github/governance/schemas/repro-authorization-record.schema.json",
    ".github/governance/schemas/repro-label-catalog.schema.json",
    ".github/workflows/governance-ci.yml",
    ".github/workflows/issue-intake.yml",
    ".github/workflows/dependabot-intake.yml",
    ".github/workflows/repro-bug.yml",
    "tools/governance_intake.py",
    "tools/governance_repro.py",
    "tools/governance_authorize.py",
    "tools/governance_schema.py",
)

#: Governance workflows whose structure this validator polices in full.
GOVERNANCE_WORKFLOWS = (
    ".github/workflows/issue-intake.yml",
    ".github/workflows/dependabot-intake.yml",
    ".github/workflows/governance-ci.yml",
    ".github/workflows/repro-bug.yml",
    ".github/workflows/governance-observe.yml",
)

#: Action versions the repository standardizes on.
PINNED_ACTION_VERSIONS = {
    "actions/checkout": "v7",
    "actions/setup-python": "v7",
    "actions/upload-artifact": "v7",
    "actions/download-artifact": "v8",
}

GOVERNED_REPRO_LABELS = (
    "repro-requested",
    "repro-in-progress",
    "repro-confirmed",
    "repro-not-reproduced",
    "repro-blocked",
    "repro-needs-review",
)

ISSUE_FORM_REQUIREMENTS = {
    "bug.yml": (
        "id: problem",
        "id: actual",
        "id: reproducer",
        "id: surface",
        "id: command",
        "id: environment",
        "id: additional",
    ),
    "feature.yml": (
        "id: outcome",
        "id: example",
        "id: command",
        "id: details",
    ),
    "question.yml": ("id: message",),
}

UNRESOLVED_MARKERS = ("{{TODO", "<TODO>", "[TODO]", "REPLACE_ME")

OBSOLETE_AUTHORITY_RUNTIME_PATHS = (
    "tools/governance_supervisor.py",
    "test/test_governance_supervisor.py",
    ".github/governance/schemas/external-supervisor-delegation.schema.json",
    ".github/governance/schemas/external-supervisor-cycle.schema.json",
    "test/fixtures/governance/supervisor/valid-delegation.json",
    "test/fixtures/governance/supervisor/repository-snapshot.json",
    "test/fixtures/governance/supervisor/session-inventory.json",
)

EXTERNAL_SUPERVISOR_ROUTINE_ACTIONS = frozenset(
    {
        "issue-labels",
        "issue-comments",
        "issue-assignments",
        "deterministic-lifecycle-reconciliation",
        "bounded-coordination-and-dispatch",
    }
)

EXTERNAL_SUPERVISOR_OWNER_ONLY_ACTIONS = frozenset(
    {
        "set-backlog-rank",
        "set-priority",
        "authorize-ready",
        "change-acceptance-manifest",
        "grant-wip-exception",
        "classify-compatibility",
        "approve-compatibility-exception",
        "approve-risk-exception",
        "approve-emergency-exception",
        "merge",
        "tag-release",
        "publish-release",
        "amend-governance-policy",
        "change-authority-or-delegation",
    }
)

EXTERNAL_SUPERVISOR_PROHIBITED_ACTIONS = frozenset(
    {
        *EXTERNAL_SUPERVISOR_OWNER_ONLY_ACTIONS,
        "approve-pull-request",
    }
)


@dataclass(frozen=True)
class Finding:
    path: str
    message: str


def _load_json(path: Path, findings: list[Finding]) -> object | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        findings.append(Finding(path.as_posix(), f"cannot load JSON: {exc}"))
        return None


def _is_exact_action_list(value: object, expected: frozenset[str]) -> bool:
    return (
        isinstance(value, list)
        and len(value) == len(expected)
        and all(isinstance(action, str) for action in value)
        and set(value) == expected
    )


def _action_lists_overlap(left: object, right: object) -> bool:
    return (
        isinstance(left, list)
        and isinstance(right, list)
        and all(isinstance(action, str) for action in (*left, *right))
        and not set(left).isdisjoint(right)
    )


def _validate_required_files(root: Path, findings: list[Finding]) -> None:
    for relative in REQUIRED_FILES:
        path = root / relative
        if not path.is_file():
            findings.append(Finding(relative, "required governance surface is missing"))
            continue
        if path.suffix in {".md", ".yml", ".yaml"}:
            text = path.read_text(encoding="utf-8")
            for marker in UNRESOLVED_MARKERS:
                if marker in text:
                    findings.append(Finding(relative, f"contains unresolved marker {marker!r}"))


def _validate_issue_forms(root: Path, findings: list[Finding]) -> None:
    forms_root = root / ".github" / "ISSUE_TEMPLATE"
    for filename, required_fragments in ISSUE_FORM_REQUIREMENTS.items():
        path = forms_root / filename
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8")
        for fragment in required_fragments:
            if fragment not in text:
                findings.append(Finding(path.relative_to(root).as_posix(), f"missing required field {fragment!r}"))


def _validate_capability_profile(root: Path, findings: list[Finding]) -> None:
    profile_path = root / ".github" / "governance" / "avrotize-capabilities.json"
    profile = _load_json(profile_path, findings)
    if not isinstance(profile, dict):
        return

    registry_relative = profile.get("command_registry")
    if not isinstance(registry_relative, str):
        findings.append(Finding(profile_path.relative_to(root).as_posix(), "command_registry must be a path string"))
        return

    registry_path = root / registry_relative
    commands = _load_json(registry_path, findings)
    if not isinstance(commands, list):
        return

    names: list[str] = []
    groups: Counter[str] = Counter()
    for index, command in enumerate(commands):
        if not isinstance(command, dict):
            findings.append(Finding(registry_relative, f"command at index {index} is not an object"))
            continue
        name = command.get("command")
        group = command.get("group")
        if not isinstance(name, str) or not name:
            findings.append(Finding(registry_relative, f"command at index {index} has no name"))
        else:
            names.append(name)
        if not isinstance(group, str) or not group:
            findings.append(Finding(registry_relative, f"command {name or index!r} has no group"))
        else:
            groups[group] += 1

    duplicates = sorted(name for name, count in Counter(names).items() if count > 1)
    if duplicates:
        findings.append(Finding(registry_relative, f"duplicate command names: {', '.join(duplicates)}"))

    expected_count = profile.get("expected_command_count")
    if expected_count != len(commands):
        findings.append(
            Finding(
                profile_path.relative_to(root).as_posix(),
                f"expected_command_count is {expected_count!r}, registry contains {len(commands)}",
            )
        )

    expected_groups = profile.get("expected_groups")
    if not isinstance(expected_groups, dict):
        findings.append(Finding(profile_path.relative_to(root).as_posix(), "expected_groups must be an object"))
        expected_groups = None
    elif dict(sorted(groups.items())) != dict(sorted(expected_groups.items())):
        findings.append(
            Finding(
                profile_path.relative_to(root).as_posix(),
                f"expected_groups {expected_groups!r} do not match registry groups {dict(groups)!r}",
            )
        )

    command_group_areas = profile.get("command_group_areas")
    # Fall back to the registry's own groups when the declared mapping is
    # unusable, so a malformed profile still yields findings instead of raising.
    declared_groups = expected_groups if isinstance(expected_groups, dict) else groups
    expected_area_groups = set(declared_groups) - {"7_Utility"}
    if not isinstance(command_group_areas, dict):
        findings.append(Finding(profile_path.relative_to(root).as_posix(), "command_group_areas must be an object"))
    else:
        assigned_groups = [
            group
            for area_groups in command_group_areas.values()
            if isinstance(area_groups, list)
            for group in area_groups
        ]
        duplicate_groups = sorted(group for group, count in Counter(assigned_groups).items() if count > 1)
        if duplicate_groups:
            findings.append(
                Finding(
                    profile_path.relative_to(root).as_posix(),
                    f"command groups assigned more than once: {', '.join(duplicate_groups)}",
                )
            )
        if set(assigned_groups) != expected_area_groups:
            findings.append(
                Finding(
                    profile_path.relative_to(root).as_posix(),
                    f"command_group_areas must cover registry groups {sorted(expected_area_groups)!r}",
                )
            )

    utility_commands = {
        command["command"]
        for command in commands
        if isinstance(command, dict)
        and command.get("group") == "7_Utility"
        and isinstance(command.get("command"), str)
    }
    utility_command_areas = profile.get("utility_command_areas")
    if not isinstance(utility_command_areas, dict):
        findings.append(Finding(profile_path.relative_to(root).as_posix(), "utility_command_areas must be an object"))
    elif set(utility_command_areas) != utility_commands or not all(
        isinstance(area, str) and area for area in utility_command_areas.values()
    ):
        findings.append(
            Finding(
                profile_path.relative_to(root).as_posix(),
                f"utility_command_areas must classify exactly {sorted(utility_commands)!r}",
            )
        )

    responsibility_domains = profile.get("responsibility_domains")
    if not isinstance(responsibility_domains, dict) or not responsibility_domains:
        findings.append(Finding(profile_path.relative_to(root).as_posix(), "responsibility_domains must be a non-empty object"))
    elif isinstance(command_group_areas, dict):
        classified_areas = set(command_group_areas)
        undefined_areas = sorted(classified_areas - set(responsibility_domains))
        if undefined_areas:
            findings.append(
                Finding(
                    profile_path.relative_to(root).as_posix(),
                    f"command classifications reference undefined responsibility domains: {', '.join(undefined_areas)}",
                )
            )

    surfaces = profile.get("surfaces")
    if not isinstance(surfaces, dict):
        findings.append(Finding(profile_path.relative_to(root).as_posix(), "surfaces must be an object"))
    else:
        for surface, relative in surfaces.items():
            if not isinstance(relative, str) or not (root / relative).is_file():
                findings.append(Finding(str(relative), f"declared {surface!r} surface does not exist"))


def _validate_workflow_contracts(root: Path, findings: list[Finding]) -> None:
    contract_path = root / ".github" / "governance" / "workflow-contracts.json"
    document = _load_json(contract_path, findings)
    if not isinstance(document, dict) or not isinstance(document.get("contracts"), list):
        findings.append(Finding(contract_path.relative_to(root).as_posix(), "contracts must be an array"))
        return

    ids: list[str] = []
    for index, contract in enumerate(document["contracts"]):
        if not isinstance(contract, dict):
            findings.append(Finding(contract_path.relative_to(root).as_posix(), f"contract {index} is not an object"))
            continue
        contract_id = contract.get("id")
        if not isinstance(contract_id, str) or not contract_id:
            findings.append(Finding(contract_path.relative_to(root).as_posix(), f"contract {index} has no id"))
        else:
            ids.append(contract_id)
        for field in ("purpose", "authority_owner", "events", "inputs", "deterministic", "copilot", "actions", "permissions", "result"):
            if field not in contract:
                findings.append(Finding(contract_path.relative_to(root).as_posix(), f"contract {contract_id or index!r} lacks {field!r}"))
        copilot = contract.get("copilot")
        if isinstance(copilot, dict):
            if copilot.get("aic_source") != "github-copilot-platform":
                findings.append(
                    Finding(
                        contract_path.relative_to(root).as_posix(),
                        f"contract {contract_id or index!r} must use platform-reported AIC",
                    )
                )
            observed = copilot.get("observed_run_aic")
            if not isinstance(observed, dict) or not all(field in observed for field in ("sample_size", "p50", "p95")):
                findings.append(
                    Finding(
                        contract_path.relative_to(root).as_posix(),
                        f"contract {contract_id or index!r} lacks observed AIC sample size, P50, or P95",
                    )
                )
            elif observed["sample_size"] == 0 and (observed["p50"] != "TBD" or observed["p95"] != "TBD"):
                findings.append(
                    Finding(
                        contract_path.relative_to(root).as_posix(),
                        f"contract {contract_id or index!r} must keep AIC P50 and P95 uncalibrated without observations",
                    )
                )
            guardrails = copilot.get("guardrails")
            if not isinstance(guardrails, dict) or not all(field in guardrails for field in ("per_run", "daily")):
                findings.append(
                    Finding(
                        contract_path.relative_to(root).as_posix(),
                        f"contract {contract_id or index!r} lacks per-run or daily AIC guardrails",
                    )
                )
            if copilot.get("token_telemetry") != "operational-only":
                findings.append(
                    Finding(
                        contract_path.relative_to(root).as_posix(),
                        f"contract {contract_id or index!r} must keep token telemetry operational-only",
                    )
                )
        implementation = contract.get("implementation")
        if implementation is not None and (not isinstance(implementation, str) or not (root / implementation).is_file()):
            findings.append(Finding(str(implementation), f"contract {contract_id or index!r} implementation does not exist"))

    duplicates = sorted(contract_id for contract_id, count in Counter(ids).items() if count > 1)
    if duplicates:
        findings.append(Finding(contract_path.relative_to(root).as_posix(), f"duplicate contract ids: {', '.join(duplicates)}"))


def _rendered_form_labels(text: str) -> list[str]:
    """Return the headings a GitHub Issue Form renders, in order.

    Every non-``markdown`` field renders its ``label`` as a ``### `` heading,
    including ``checkboxes``. Checkbox option labels use ``- label:`` and are not
    headings.
    """
    labels: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("- label:") or not stripped.startswith("label:"):
            continue
        labels.append(stripped.split("label:", 1)[1].strip().strip("'\""))
    return labels


def _validate_issue_form_contract(root: Path, findings: list[Finding]) -> None:
    """Validate that the issue form contract matches the actual YAML form files."""
    contract_path = root / ".github" / "governance" / "issue-form-contract.json"
    contract = _load_json(contract_path, findings)
    if not isinstance(contract, dict):
        return
    forms = contract.get("forms")
    if not isinstance(forms, list):
        findings.append(Finding(contract_path.relative_to(root).as_posix(), "forms must be an array"))
        return

    for form in forms:
        form_file = form.get("file", "")
        form_path = root / form_file
        if not form_path.is_file():
            findings.append(Finding(form_file, "issue form file referenced by contract does not exist"))
            continue
        text = form_path.read_text(encoding="utf-8")
        declared_headings = list(form.get("headings", []))
        field_ids = list(form.get("field_ids", []))
        required_semantic_fields = form.get("required_semantic_fields")
        if len(declared_headings) != len(field_ids):
            findings.append(Finding(form_file, "contract headings and field_ids must be index aligned"))
        if not isinstance(required_semantic_fields, list) or not required_semantic_fields:
            findings.append(
                Finding(
                    form_file,
                    "contract required_semantic_fields must be a non-empty array",
                )
            )
        elif any(field_id not in field_ids for field_id in required_semantic_fields):
            findings.append(
                Finding(
                    form_file,
                    "contract required_semantic_fields must reference declared field_ids",
                )
            )
        for fid in field_ids:
            if f"id: {fid}" not in text:
                findings.append(Finding(form_file, f"contract declares field id '{fid}' not found in YAML"))
        rendered = _rendered_form_labels(text)
        if rendered != declared_headings:
            missing = [label for label in rendered if label not in declared_headings]
            extra = [label for label in declared_headings if label not in rendered]
            findings.append(
                Finding(
                    form_file,
                    "contract heading set must equal the rendered form labels in order"
                    + (f"; undeclared rendered labels: {missing}" if missing else "")
                    + (f"; declared labels the form does not render: {extra}" if extra else ""),
                )
            )


def _validate_intake_workflow_safety(root: Path, findings: list[Finding]) -> None:
    """Verify intake/guard workflows have no write permissions or merge commands.

    Also checks: persist-credentials false, no PR-head checkout/execution,
    correct event types, trusted checkout refs.
    """
    intake_workflows = [
        ".github/workflows/issue-intake.yml",
        ".github/workflows/dependabot-intake.yml",
        ".github/workflows/repro-bug.yml",
    ]
    for wf_relative in intake_workflows:
        wf_path = root / wf_relative
        if not wf_path.is_file():
            continue
        text = wf_path.read_text(encoding="utf-8")
        if "contents: write" in text:
            findings.append(Finding(wf_relative, "intake/guard workflow must not have contents:write"))
        if "pull-requests: write" in text:
            findings.append(Finding(wf_relative, "intake/guard workflow must not have pull-requests:write"))
        if "gh pr merge" in text:
            findings.append(Finding(wf_relative, "intake/guard workflow must not contain 'gh pr merge'"))
        if "gh pr review" in text:
            findings.append(Finding(wf_relative, "intake/guard workflow must not contain 'gh pr review'"))
        # persist-credentials must be false on all checkouts
        if "actions/checkout" in text and "persist-credentials: false" not in text:
            findings.append(Finding(wf_relative, "checkout must use persist-credentials: false"))
        # No PR head checkout
        if "github.event.pull_request.head.sha" in text and "ref:" in text:
            # Only allowed in base SHA context
            pass
        if "github.event.pull_request.head.ref" in text and "ref:" in text:
            findings.append(Finding(wf_relative, "must not checkout PR head ref"))

    # Schema files must exist
    schema_dir = root / ".github" / "governance" / "schemas"
    required_schemas = [
        "issue-intake-record.schema.json",
        "issue-semantic-assistance.schema.json",
        "dependabot-intake-record.schema.json",
    ]
    for schema_name in required_schemas:
        if not (schema_dir / schema_name).is_file():
            findings.append(Finding(f".github/governance/schemas/{schema_name}", "required schema file missing"))

    # Validate contract-workflow event parity
    contracts_path = root / ".github" / "governance" / "workflow-contracts.json"
    contracts_data = _load_json(contracts_path, findings)
    if isinstance(contracts_data, dict):
        for contract in contracts_data.get("contracts", []):
            impl_path = contract.get("implementation", "")
            if not isinstance(impl_path, str) or not impl_path:
                continue
            impl_file = root / impl_path
            if not impl_file.is_file():
                continue
            wf_text = impl_file.read_text(encoding="utf-8")
            # Check permissions parity
            contract_perms = contract.get("permissions", [])
            for perm in contract_perms:
                key, _, value = perm.partition(":")
                if value == "write" and f"{key}: write" not in wf_text:
                    pass  # contract may declare it but workflow shouldn't have it for intake
                if value == "read" and key == "pull-requests" and "pull-requests: read" not in wf_text:
                    if "pulls/" in wf_text or "pull_request" in wf_text:
                        findings.append(Finding(impl_path, f"uses PR API but missing pull-requests:read permission"))


def _validate_copilot_issue_intake(root: Path, findings: list[Finding]) -> None:
    """Keep Copilot issue assistance read-only, zero-tool, pinned, and schema-bound."""
    workflow_relative = ".github/workflows/issue-intake.yml"
    workflow_path = root / workflow_relative
    policy_relative = ".github/governance/copilot-intake-policy.json"
    policy_path = root / policy_relative
    semantic_schema_relative = ".github/governance/schemas/issue-semantic-assistance.schema.json"
    semantic_schema_path = root / semantic_schema_relative
    if not workflow_path.is_file() or not policy_path.is_file() or not semantic_schema_path.is_file():
        return

    workflow = workflow_path.read_text(encoding="utf-8")
    policy = _load_json(policy_path, findings)
    semantic_schema = _load_json(semantic_schema_path, findings)
    if not isinstance(policy, dict) or not isinstance(semantic_schema, dict):
        return
    try:
        governance_schema.assert_supported_schema(semantic_schema, semantic_schema_relative)
    except governance_schema.SchemaError as exc:
        findings.append(Finding(semantic_schema_relative, str(exc)))

    try:
        cli = policy["cli"]
        request = policy["request"]
        boundary = policy["tool_boundary"]
        artifact = policy["artifact"]
    except (KeyError, TypeError) as exc:
        findings.append(Finding(policy_relative, f"incomplete Copilot intake policy: {exc}"))
        return

    required_fragments = (
        "copilot-requests: write",
        "contents: read",
        "actions/setup-node@v7",
        f'COPILOT_CLI_VERSION: "{cli.get("version")}"',
        f'COPILOT_LOCKFILE_SHA256: "{cli.get("lockfile_sha256")}"',
        f'COPILOT_MODEL: "{request.get("model")}"',
        f'COPILOT_MAX_AI_CREDITS: "{request.get("max_ai_credits")}"',
        f'COPILOT_TIMEOUT_SECONDS: "{request.get("timeout_seconds")}"',
        f'COPILOT_MAX_OUTPUT_BYTES: "{request.get("max_output_bytes")}"',
        "npm ci --prefix .github/governance/copilot-cli",
        ".github/governance/copilot-cli/node_modules/.bin/copilot",
        "--ignore-scripts",
        "--silent",
        "--max-ai-credits=",
        "--no-ask-user",
        "--no-auto-update",
        "--no-custom-instructions",
        "--disable-builtin-mcps",
        "--no-remote",
        "--no-remote-export",
        "--disallow-temp-dir",
        "--available-tools=",
        "--deny-tool='shell,write,read,url,memory'",
        "--secret-env-vars=GITHUB_TOKEN",
        "--log-level=none",
        "--stream=off",
        "< copilot-prompt.txt",
        "GITHUB_TOKEN: ${{ github.token }}",
        "--semantic-preflight copilot-preflight.json",
        "--minimize-content",
        "if: always() && steps.preflight.outcome == 'success'",
        "if: always() && steps.finalize.outcome == 'success'",
    )
    for fragment in required_fragments:
        if fragment not in workflow:
            findings.append(Finding(workflow_relative, f"missing Copilot intake control {fragment!r}"))

    forbidden_fragments = (
        "--allow-all",
        "--allow-tool",
        "--allow-url",
        "--yolo",
        "issues: write",
        "contents: write",
        "pull-requests: write",
        "actions: write",
        "secrets.",
        "github.event.issue.body }}",
        "github.event.issue.title }}",
        "copilot-session-",
        "--share",
    )
    for fragment in forbidden_fragments:
        if fragment in workflow:
            findings.append(Finding(workflow_relative, f"forbidden Copilot intake capability {fragment!r}"))


    if cli.get("package") != "@github/copilot" or cli.get("install_scripts") is not False:
        findings.append(Finding(policy_relative, "Copilot CLI package must be official and install scripts disabled"))
    if not isinstance(cli.get("version"), str) or not cli.get("version"):
        findings.append(Finding(policy_relative, "Copilot CLI version must be exact"))
    if not isinstance(cli.get("integrity"), str) or not cli["integrity"].startswith("sha512-"):
        findings.append(Finding(policy_relative, "Copilot CLI integrity must be SHA-512"))
    lock_relative = cli.get("lockfile")
    lock_path = root / str(lock_relative)
    package_path = lock_path.with_name("package.json")
    if lock_relative != ".github/governance/copilot-cli/package-lock.json":
        findings.append(Finding(policy_relative, "Copilot CLI lockfile path changed"))
    if not lock_path.is_file() or not package_path.is_file():
        findings.append(Finding(policy_relative, "Copilot CLI package and lockfile must exist"))
    else:
        lock_digest = hashlib.sha256(
            lock_path.read_text(encoding="utf-8").encode("utf-8")
        ).hexdigest()
        if cli.get("lockfile_sha256") != lock_digest:
            findings.append(Finding(policy_relative, "Copilot CLI lockfile digest does not match policy"))
        package = _load_json(package_path, findings)
        lock = _load_json(lock_path, findings)
        if isinstance(package, dict):
            dependency = package.get("dependencies", {}).get("@github/copilot")
            if dependency != cli.get("version"):
                findings.append(Finding(package_path.relative_to(root).as_posix(), "Copilot dependency must use the exact policy version"))
        if isinstance(lock, dict):
            if lock.get("lockfileVersion") != 3:
                findings.append(Finding(lock_path.relative_to(root).as_posix(), "Copilot lockfile must use lockfileVersion 3"))
            packages = lock.get("packages")
            if not isinstance(packages, dict):
                findings.append(Finding(lock_path.relative_to(root).as_posix(), "Copilot lockfile packages map is missing"))
            else:
                copilot_entry = packages.get("node_modules/@github/copilot")
                if not isinstance(copilot_entry, dict) or copilot_entry.get("version") != cli.get("version"):
                    findings.append(Finding(lock_path.relative_to(root).as_posix(), "locked Copilot package version does not match policy"))
                elif copilot_entry.get("integrity") != cli.get("integrity"):
                    findings.append(Finding(lock_path.relative_to(root).as_posix(), "locked Copilot package integrity does not match policy"))
                for package_name, entry in packages.items():
                    if package_name == "" or not isinstance(entry, dict):
                        continue
                    if entry.get("link") is True:
                        findings.append(Finding(lock_path.relative_to(root).as_posix(), f"lockfile package {package_name!r} must not be a mutable link"))
                        continue
                    if not isinstance(entry.get("version"), str):
                        findings.append(Finding(lock_path.relative_to(root).as_posix(), f"lockfile package {package_name!r} lacks an exact version"))
                    resolved = entry.get("resolved")
                    if not isinstance(resolved, str) or not resolved.startswith("https://registry.npmjs.org/"):
                        findings.append(Finding(lock_path.relative_to(root).as_posix(), f"lockfile package {package_name!r} has an unapproved source"))
                    integrity = entry.get("integrity")
                    if not isinstance(integrity, str) or not integrity.startswith("sha512-"):
                        findings.append(Finding(lock_path.relative_to(root).as_posix(), f"lockfile package {package_name!r} lacks SHA-512 integrity"))
                    if entry.get("hasInstallScript") is True:
                        findings.append(Finding(lock_path.relative_to(root).as_posix(), f"lockfile package {package_name!r} declares an install script"))
    if request.get("max_ai_credits") != 30:
        findings.append(Finding(policy_relative, "Copilot intake must use the minimum 30-AIC session guardrail"))
    if request.get("timeout_seconds") != 120:
        findings.append(Finding(policy_relative, "Copilot intake timeout must remain 120 seconds"))
    if request.get("max_output_bytes") != 32768:
        findings.append(Finding(policy_relative, "Copilot intake output cap must remain 32768 bytes"))
    if boundary.get("available_tools") != []:
        findings.append(Finding(policy_relative, "Copilot intake must expose no tools"))
    if set(boundary.get("denied_tools", [])) != {"shell", "write", "read", "url", "memory"}:
        findings.append(Finding(policy_relative, "Copilot intake denied tool set changed"))
    if boundary.get("builtin_mcp") is not False:
        findings.append(Finding(policy_relative, "Copilot intake must disable built-in MCP"))
    if any(artifact.get(key) is not False for key in ("raw_title", "raw_body", "raw_model_response")):
        findings.append(Finding(policy_relative, "Copilot intake artifacts must omit raw issue and model content"))


def _validate_external_supervisor(root: Path, findings: list[Finding]) -> None:
    """Structurally validate the trusted-host contract without asserting authority."""
    base = ".github/governance"
    policy_relative = f"{base}/external-supervisor-policy.json"
    prompt_relative = f"{base}/prompts/external-supervisor-kickoff-v2.txt"

    for relative in OBSOLETE_AUTHORITY_RUNTIME_PATHS:
        if (root / relative).exists():
            findings.append(
                Finding(
                    relative,
                    "obsolete local authority runtime artifact must remain removed",
                )
            )

    policy = _load_json(root / policy_relative, findings)
    contracts = _load_json(root / f"{base}/workflow-contracts.json", findings)
    prompt_path = root / prompt_relative
    if (
        not isinstance(policy, dict)
        or not isinstance(contracts, dict)
        or not prompt_path.is_file()
    ):
        return

    expected_policy_keys = {
        "schema_version",
        "policy_id",
        "repository",
        "mode",
        "policy_path",
        "kickoff_prompt",
        "host_operational_authority",
    }
    if set(policy) != expected_policy_keys:
        findings.append(Finding(policy_relative, "external supervisor policy has missing or unknown top-level fields"))

    try:
        prompt_digest = policy["kickoff_prompt"]["sha256"]
        host_authority = policy["host_operational_authority"]
    except (KeyError, TypeError) as exc:
        findings.append(Finding(policy_relative, f"incomplete external supervisor policy: {exc}"))
        return
    if policy.get("schema_version") != 2 or policy.get("mode") != "external-host-admin-operations":
        findings.append(Finding(policy_relative, "external supervisor policy must enable version 2 trusted-host admin operations"))
    expected_prompt = {
        "path": prompt_relative,
        "sha256": prompt_digest,
    }
    if (
        policy.get("policy_id") != "avrotize-external-delivery-supervisor-v2"
        or policy.get("repository") != "clemensv/avrotize"
        or policy.get("policy_path") != policy_relative
        or policy.get("kickoff_prompt") != expected_prompt
        or not isinstance(prompt_digest, str)
        or len(prompt_digest) != 64
        or any(character not in "0123456789abcdef" for character in prompt_digest)
    ):
        findings.append(
            Finding(
                policy_relative,
                "external supervisor policy identity or prompt binding is invalid",
            )
        )

    expected_host_keys = {
        "enabled",
        "trust_boundaries",
        "repository",
        "required_repository_permission",
        "fresh_identity_verification",
        "routine_actions",
        "owner_only_actions",
        "supervisor_prohibited_actions",
        "mutation_rules",
        "credential_rules",
        "host_responsibilities",
    }
    if not isinstance(host_authority, dict) or set(host_authority) != expected_host_keys:
        findings.append(Finding(policy_relative, "external supervisor host authority has missing or unknown fields"))
    else:
        mutation_rules = host_authority.get("mutation_rules")
        credential_rules = host_authority.get("credential_rules")
        if (
            host_authority.get("enabled") is not True
            or host_authority.get("trust_boundaries")
            != ["trusted-copilot-session-host", "github"]
            or host_authority.get("repository") != "clemensv/avrotize"
            or host_authority.get("required_repository_permission") != "admin"
            or host_authority.get("fresh_identity_verification") is not True
            or not _is_exact_action_list(
                host_authority.get("routine_actions"),
                EXTERNAL_SUPERVISOR_ROUTINE_ACTIONS,
            )
            or host_authority.get("host_responsibilities")
            != ["app-native-session-inventory", "owner-instruction-provenance"]
        ):
            findings.append(Finding(policy_relative, "external supervisor host authority does not match the trusted-host admin contract"))
        if not isinstance(mutation_rules, dict) or set(mutation_rules) != {
            "fresh_github_read_before_mutation",
            "idempotent_operations",
            "use_native_compare_and_swap_when_available",
            "reread_and_reconcile_when_compare_and_swap_is_unavailable",
            "only_retry_safe_operations",
            "comments_require_stable_operation_marker",
            "manual_confirmation_cannot_replace_comment_marker",
            "github_live_state_and_audit_history_are_authoritative",
        } or any(value is not True for value in mutation_rules.values()):
            findings.append(Finding(policy_relative, "external supervisor mutation rules are incomplete"))
        if not isinstance(credential_rules, dict) or set(credential_rules) != {
            "use_authenticated_host_tooling_directly",
            "never_pass_credentials_to_children",
            "never_pass_credentials_to_repository_scripts",
        } or any(value is not True for value in credential_rules.values()):
            findings.append(Finding(policy_relative, "external supervisor credential rules are incomplete"))

    if isinstance(host_authority, dict):
        host_routine = host_authority.get("routine_actions")
        host_owner_only = host_authority.get("owner_only_actions")
        host_prohibited = host_authority.get("supervisor_prohibited_actions")
        if not _is_exact_action_list(
            host_owner_only,
            EXTERNAL_SUPERVISOR_OWNER_ONLY_ACTIONS,
        ):
            findings.append(
                Finding(
                    policy_relative,
                    "external supervisor policy owner-only actions must match the complete exact required set",
                )
            )
        if not _is_exact_action_list(
            host_prohibited,
            EXTERNAL_SUPERVISOR_PROHIBITED_ACTIONS,
        ):
            findings.append(
                Finding(
                    policy_relative,
                    "external supervisor policy supervisor-prohibited actions must match the complete exact required set",
                )
            )
        if _action_lists_overlap(host_routine, host_owner_only):
            findings.append(
                Finding(
                    policy_relative,
                    "external supervisor routine actions overlap owner-only actions",
                )
            )
        if _action_lists_overlap(host_routine, host_prohibited):
            findings.append(
                Finding(
                    policy_relative,
                    "external supervisor routine actions overlap supervisor-prohibited actions",
                )
            )

    prompt_bytes = prompt_path.read_bytes()
    if hashlib.sha256(prompt_bytes).hexdigest() != prompt_digest:
        findings.append(Finding(prompt_relative, "external supervisor kickoff prompt digest does not match policy"))
    attributes_path = root / ".gitattributes"
    if attributes_path.is_file():
        attributes = attributes_path.read_text(encoding="utf-8").splitlines()
        expected_attribute = f"{prompt_relative} text eol=lf"
        if expected_attribute not in attributes:
            findings.append(
                Finding(
                    ".gitattributes",
                    "external supervisor v2 kickoff prompt must be governed as LF text",
                )
            )
        if any("external-supervisor-kickoff-v1.txt" in line for line in attributes):
            findings.append(
                Finding(
                    ".gitattributes",
                    "deleted external supervisor v1 kickoff prompt remains governed",
                )
            )

    contract = next(
        (
            value
            for value in contracts.get("contracts", [])
            if isinstance(value, dict) and value.get("id") == "external-delivery-supervisor"
        ),
        None,
    )
    if contract is None:
        findings.append(Finding(f"{base}/workflow-contracts.json", "missing external delivery supervisor contract"))
    elif contract.get("implementation") is not None or contract.get("permissions") != []:
        findings.append(Finding(f"{base}/workflow-contracts.json", "external supervisor must have no Actions implementation or permissions"))
    else:
        contract_actions = contract.get("actions", {})
        if not isinstance(contract_actions, dict):
            findings.append(
                Finding(
                    f"{base}/workflow-contracts.json",
                    "external supervisor workflow contract actions must be an object",
                )
            )
        else:
            contract_routine = contract_actions.get("mutations")
            contract_owner_only = contract_actions.get("owner_only")
            contract_prohibited = contract_actions.get("supervisor_prohibited")
            if not _is_exact_action_list(
                contract_routine,
                EXTERNAL_SUPERVISOR_ROUTINE_ACTIONS,
            ):
                findings.append(
                    Finding(
                        f"{base}/workflow-contracts.json",
                        "external supervisor workflow contract has incorrect routine mutations",
                    )
                )
            if not _is_exact_action_list(
                contract_owner_only,
                EXTERNAL_SUPERVISOR_OWNER_ONLY_ACTIONS,
            ):
                findings.append(
                    Finding(
                        f"{base}/workflow-contracts.json",
                        "external supervisor workflow contract owner-only actions must match the complete exact required set",
                    )
                )
            if not _is_exact_action_list(
                contract_prohibited,
                EXTERNAL_SUPERVISOR_PROHIBITED_ACTIONS,
            ):
                findings.append(
                    Finding(
                        f"{base}/workflow-contracts.json",
                        "external supervisor workflow contract supervisor-prohibited actions must match the complete exact required set",
                    )
                )
            if _action_lists_overlap(contract_routine, contract_owner_only):
                findings.append(
                    Finding(
                        f"{base}/workflow-contracts.json",
                        "external supervisor workflow routine actions overlap owner-only actions",
                    )
                )
            if _action_lists_overlap(contract_routine, contract_prohibited):
                findings.append(
                    Finding(
                        f"{base}/workflow-contracts.json",
                        "external supervisor workflow routine actions overlap supervisor-prohibited actions",
                    )
                )

    required_document_text = {
        f"{base}/EXTERNAL-SUPERVISOR.md": (
            "freshly verifies that its active GitHub identity has",
            "GitHub live state and GitHub's audit/history are authoritative",
            "stable operation marker",
            "owner-only and non-delegable",
            "applicable named human domain or risk reviewer",
            "not thereby reserved exclusively",
            "non-authoritative structural validation",
            "No repository authority runtime",
        ),
        "GOVERNANCE.md": (
            "Freshly verified GitHub repository admin for routine operations only",
            "issue-intake Copilot pass",
            "separate authenticated external-supervisor admin exception",
            "Every automated",
            "policy changes, and",
            "applicable named human domain or risk reviewer",
            "does not reserve approval exclusively",
        ),
        f"{base}/AUTOMATION.md": (
            "freshly verifying that its active GitHub identity has",
            "without claiming atomic compare-and-swap",
            "non-authoritative structural validation",
            "necessarily owner-only",
        ),
        f"{base}/ADOPTION.md": (
            "freshly verified as repository `admin`",
            "Every automated issue comment requires a stable operation marker",
            "owner-instruction provenance",
            "not necessarily owner-only",
        ),
        prompt_relative: (
            "immediately before each mutation",
            "stable operation marker",
            "cannot replace that marker",
            "applicable named human",
            "not necessarily reserved",
            "Repository Python is non-authoritative structural validation only",
        ),
    }
    for relative, fragments in required_document_text.items():
        document_path = root / relative
        if not document_path.is_file():
            continue
        text = document_path.read_text(encoding="utf-8")
        for fragment in fragments:
            if fragment not in text:
                findings.append(Finding(relative, f"external supervisor contract is missing {fragment!r}"))

    supervisor_workflows = list((root / ".github" / "workflows").glob("*supervisor*.yml"))
    if supervisor_workflows:
        findings.append(Finding(".github/workflows", "external supervisor must not have a privileged Actions workflow"))



def _indent(line: str) -> int:
    return len(line) - len(line.lstrip(" "))


def _run_block_lines(text: str) -> list[tuple[int, str]]:
    """Return (line number, content) for every line inside a ``run:`` block."""
    collected: list[tuple[int, str]] = []
    lines = text.splitlines()
    index = 0
    while index < len(lines):
        line = lines[index]
        stripped = line.strip()
        if stripped.startswith("run:") or stripped.startswith("- run:"):
            base = _indent(line)
            inline = stripped.split("run:", 1)[1].strip()
            if inline and inline not in {"|", ">-", ">", "|-"}:
                collected.append((index + 1, inline))
                index += 1
                continue
            index += 1
            while index < len(lines):
                body = lines[index]
                if body.strip() and _indent(body) <= base:
                    break
                collected.append((index + 1, body))
                index += 1
            continue
        index += 1
    return collected


def _job_blocks(text: str) -> dict[str, list[str]]:
    """Split a workflow into job name -> block lines using two-space job indentation."""
    lines = text.splitlines()
    jobs: dict[str, list[str]] = {}
    in_jobs = False
    current: str | None = None
    for line in lines:
        if not line.strip() or line.lstrip().startswith("#"):
            if current:
                jobs[current].append(line)
            continue
        if _indent(line) == 0:
            in_jobs = line.startswith("jobs:")
            current = None
            continue
        if not in_jobs:
            continue
        if _indent(line) == 2 and line.rstrip().endswith(":"):
            current = line.strip().rstrip(":")
            jobs[current] = []
            continue
        if current:
            jobs[current].append(line)
    return jobs


def _validate_action_versions(root: Path, findings: list[Finding]) -> None:
    """Every action must be version-pinned, and shared actions must use the repository major."""
    for path in sorted((root / ".github" / "workflows").glob("*.yml")):
        relative = path.relative_to(root).as_posix()
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            stripped = line.strip()
            if not stripped.startswith("uses:") and not stripped.startswith("- uses:"):
                continue
            reference = stripped.split("uses:", 1)[1].strip()
            if "@" not in reference:
                findings.append(Finding(relative, f"line {number}: action {reference!r} is not version pinned"))
                continue
            action, _, version = reference.partition("@")
            expected = PINNED_ACTION_VERSIONS.get(action)
            if expected and version != expected:
                findings.append(
                    Finding(relative, f"line {number}: {action} must be pinned to {expected}, found {version}")
                )


def _validate_governance_workflow_safety(root: Path, findings: list[Finding]) -> None:
    """Structural safety rules for every governance workflow."""
    for relative in GOVERNANCE_WORKFLOWS:
        path = root / relative
        if not path.is_file():
            findings.append(Finding(relative, "governance workflow is missing"))
            continue
        text = path.read_text(encoding="utf-8")

        if "|| true" in text or "|| :" in text:
            findings.append(Finding(relative, "must not suppress command failure with '|| true'"))
        if "continue-on-error: true" in text:
            findings.append(Finding(relative, "must not use continue-on-error: true"))
        if "\npermissions:" not in text:
            findings.append(Finding(relative, "must declare workflow-level permissions"))
        if "actions/checkout" in text and "persist-credentials: false" not in text:
            findings.append(Finding(relative, "checkout must use persist-credentials: false"))
        if "github.event.pull_request.head.ref" in text:
            findings.append(Finding(relative, "must not reference an untrusted pull-request head ref"))
        for issue_ref in ("ref: ${{ github.event.issue", "ref: ${{ inputs."):
            if issue_ref in text:
                findings.append(Finding(relative, "must not take a git ref from issue or dispatch input"))
        if "actions/upload-artifact" in text and "retention-days:" not in text:
            findings.append(Finding(relative, "artifact uploads must declare retention-days"))

        for job_name, block in _job_blocks(text).items():
            joined = "\n".join(block)
            if "timeout-minutes:" not in joined:
                findings.append(Finding(relative, f"job {job_name!r} must declare timeout-minutes"))
            if "permissions:" not in joined:
                findings.append(Finding(relative, f"job {job_name!r} must declare explicit permissions"))

        for number, line in _run_block_lines(text):
            for forbidden in ("${{ github.event.", "${{ inputs.", "${{ needs."):
                if forbidden in line:
                    findings.append(
                        Finding(
                            relative,
                            f"line {number}: shell body interpolates {forbidden}...; pass it through env instead",
                        )
                    )
            for content_ref in ("github.event.issue.body", "github.event.issue.title"):
                if content_ref in line:
                    findings.append(Finding(relative, f"line {number}: must not use issue content in a shell body"))


def _validate_repro_workflow(root: Path, findings: list[Finding]) -> None:
    """Validate authorization-first, preparation-only reproduction automation."""
    relative = ".github/workflows/repro-bug.yml"
    path = root / relative
    if not path.is_file():
        findings.append(Finding(relative, "guarded reproduction workflow is missing"))
        return
    text = path.read_text(encoding="utf-8")

    if "pull_request_target" in text:
        findings.append(Finding(relative, "guarded reproduction must never run in a pull_request_target context"))
    if "types: [labeled]" not in text:
        findings.append(Finding(relative, "issues trigger must be exactly types: [labeled]"))
    if "github.event.label.name == 'repro-requested'" not in text:
        findings.append(Finding(relative, "must gate on the exact repro-requested label"))
    if "workflow_dispatch" in text:
        findings.append(Finding(relative, "must not expose privileged workflow_dispatch"))
    for forbidden in ("pip install", "requirements.txt", "python -m avrotize", "docker run"):
        if forbidden in text:
            findings.append(Finding(relative, f"preparation-only workflow must not contain {forbidden!r}"))
    if "dependabot[bot]" not in text:
        findings.append(Finding(relative, "must not run for Dependabot actors"))
    if "github.actor == github.event.sender.login" not in text:
        findings.append(Finding(relative, "must bind workflow actor to the label-event sender"))

    concurrency = ""
    for line in text.splitlines():
        if line.strip().startswith("group: repro-preparation-"):
            concurrency = line.strip()
    if not concurrency:
        findings.append(Finding(relative, "must serialize on a repro-preparation-<issue> concurrency group"))
    else:
        for forbidden in ("run_id", "run_attempt", "updated_at", "title", "body"):
            if forbidden in concurrency:
                findings.append(
                    Finding(relative, f"concurrency group must not include {forbidden}")
                )
    if "cancel-in-progress: false" not in text:
        findings.append(Finding(relative, "must not cancel an authorized preparation already in progress"))

    jobs = _job_blocks(text)
    expected_jobs = ["authorize", "mark-in-progress", "prepare", "publish-final"]
    if list(jobs) != expected_jobs:
        findings.append(Finding(relative, f"jobs must be exactly {expected_jobs}, found {list(jobs)}"))
        return

    authorize = "\n".join(jobs["authorize"])
    if "actions/checkout" in authorize:
        findings.append(Finding(relative, "authorization must not check out repository code"))
    if "governance_repro.py" in authorize:
        findings.append(Finding(relative, "authorization must not run the reproduction engine"))
    if "pip install" in authorize:
        findings.append(Finding(relative, "authorization must not install packages before deciding"))
    permission_index = authorize.find("collaborators/${encoded_actor}/permission")
    issue_index = authorize.find('repos/${REPO}/issues/${ISSUE_NUMBER}')
    if permission_index < 0 or (issue_index >= 0 and issue_index < permission_index):
        findings.append(Finding(relative, "collaborator permission must be queried before issue content"))
    for digest in ("title_digest", "body_digest", "content_digest"):
        if digest not in authorize:
            findings.append(Finding(relative, f"authorization must emit immutable {digest}"))
    for required in (
        'event.get("issue")',
        "current_snapshot",
        "issue title or body changed after the label event",
    ):
        if required not in authorize:
            findings.append(
                Finding(relative, f"authorization must bind label-event content via {required!r}")
            )
    if "updated_at" in authorize:
        findings.append(Finding(relative, "authorization must not bind aggregate issue.updated_at"))

    mark = "\n".join(jobs["mark-in-progress"])
    if "needs: authorize" not in mark:
        findings.append(Finding(relative, "mark-in-progress must depend on authorization"))
    if "issues: write" not in mark:
        findings.append(Finding(relative, "mark-in-progress needs issues: write"))
    if "repro-in-progress" not in mark:
        findings.append(Finding(relative, "mark-in-progress must apply the in-progress label"))

    prepare = "\n".join(jobs["prepare"])
    if "needs: [authorize, mark-in-progress]" not in prepare:
        findings.append(Finding(relative, "prepare must depend on authorization and state marking"))
    if "issues: write" in prepare:
        findings.append(Finding(relative, "prepare must not hold issues: write"))
    if "governance_authorize.py verify" not in prepare:
        findings.append(Finding(relative, "prepare must re-verify the immutable title/body snapshot"))
    if "needs.authorize.outputs.trusted_sha" not in prepare:
        findings.append(Finding(relative, "prepare must check out the authorized trusted revision"))
    if "needs.authorize.outputs.artifact_name" not in prepare:
        findings.append(
            Finding(relative, "prepare must consume the producer-selected authorization artifact")
        )
    publish = "\n".join(jobs["publish-final"])
    if "if: always()" not in publish:
        findings.append(Finding(relative, "publish-final must run even when reproduction fails"))
    if "issues: write" not in publish:
        findings.append(Finding(relative, "publish-final needs issues: write"))
    if "contents: read" not in publish:
        findings.append(Finding(relative, "publish-final needs contents: read for the trusted validator"))
    if "ref: ${{ needs.authorize.outputs.trusted_sha }}" not in publish:
        findings.append(Finding(relative, "publish-final must check out the authorized trusted validator"))
    if "needs.prepare.outputs.artifact_name" not in publish:
        findings.append(
            Finding(relative, "publish-final must consume the producer-selected preparation artifact")
        )
    for required in (
        "governance_repro.py terminal",
        'test "$(git rev-parse HEAD)" = "${TRUSTED_SHA}"',
        'VALIDATOR_VERIFIED: ${{ steps.terminal-checkout.outputs.verified }}',
        '--issue-number "${ISSUE_NUMBER}"',
        '--preparation-artifact "${PREPARATION_ARTIFACT}"',
        '--preparation-attempt "${PREPARATION_ATTEMPT:-0}"',
    ):
        if required not in publish:
            findings.append(Finding(relative, f"publish-final must validate evidence via {required!r}"))
    for label in GOVERNED_REPRO_LABELS:
        if label not in publish:
            findings.append(Finding(relative, f"publish-final must reconcile governed label {label}"))
    for required in ("run_attempt", "for attempt in 1 2 3"):
        if required not in publish:
            findings.append(Finding(relative, f"publish-final must include {required!r}"))
    for required in (
        'marker="<!-- avrotize-repro:',
        'grep -Fqx -- "${marker}" terminal-comment.md',
        'issues/${ISSUE_NUMBER}/comments',
        'grep -Fqx -- "${marker}" existing-comments.txt',
    ):
        if required not in publish:
            findings.append(
                Finding(
                    relative,
                    f"publish-final must deduplicate its marked issue comment via {required!r}",
                )
            )
    repro_tool = (root / "tools/governance_repro.py").read_text(encoding="utf-8")
    for required in (
        "validate_prepared_evidence",
        "repro-terminal-fallback.schema.json",
        "PREPARATION_EVIDENCE_UNAVAILABLE",
        "<!-- avrotize-repro:",
    ):
        if required not in repro_tool:
            findings.append(
                Finding("tools/governance_repro.py", f"terminal validation must include {required!r}")
            )
    if '"CONFIRMED"' in publish or '"NOT_REPRODUCED"' in publish:
        findings.append(Finding(relative, "preparation automation must not claim an adjudicated outcome"))


def _validate_repro_policy_and_catalog(root: Path, findings: list[Finding]) -> None:
    """Validate preparation label catalog, schema, and evidence parity."""
    catalog_path = root / ".github/governance/repro-label-catalog.json"
    catalog_schema = root / ".github/governance/schemas/repro-label-catalog.schema.json"
    evidence_schema_path = root / ".github/governance/schemas/repro-evidence-record.schema.json"

    catalog = _load_json(catalog_path, findings)
    if not isinstance(catalog, dict):
        return

    try:
        governance_schema.validate_or_raise(catalog, catalog_schema, "reproduction label catalog")
    except governance_schema.SchemaError as exc:
        findings.append(Finding(catalog_schema.relative_to(root).as_posix(), str(exc)))

    catalog_names = [label.get("name") for label in catalog.get("labels", [])]
    if sorted(name for name in catalog_names if isinstance(name, str)) != sorted(GOVERNED_REPRO_LABELS):
        findings.append(
            Finding(
                catalog_path.relative_to(root).as_posix(),
                f"catalog must declare exactly {sorted(GOVERNED_REPRO_LABELS)}",
            )
        )

    preparation_labels = ["repro-blocked", "repro-needs-review"]
    evidence_schema = _load_json(evidence_schema_path, findings)
    if isinstance(evidence_schema, dict):
        final_label = (
            evidence_schema.get("properties", {})
            .get("result", {})
            .get("properties", {})
            .get("final_label", {})
        )
        if sorted(final_label.get("enum", [])) != preparation_labels:
            findings.append(
                Finding(
                    evidence_schema_path.relative_to(root).as_posix(),
                    f"preparation final_label enum must equal {preparation_labels}",
                )
            )
    if not set(preparation_labels).issubset(set(catalog_names)):
        findings.append(Finding(catalog_path.relative_to(root).as_posix(), "preparation labels missing from catalog"))


def _validate_governance_ci(root: Path, findings: list[Finding]) -> None:
    relative = ".github/workflows/governance-ci.yml"
    path = root / relative
    if not path.is_file():
        findings.append(Finding(relative, "hard-failing governance CI workflow is missing"))
        return
    text = path.read_text(encoding="utf-8")
    required = (
        "github.event.pull_request.head.sha",
        "python tools/validate_governance.py --strict",
        'python -m unittest discover -s test -p "test_governance*.py" -v',
        "persist-credentials: false",
        "--require-hashes",
        "--only-binary=:all:",
        ".github/governance/requirements-ci.txt",
    )
    for fragment in required:
        if fragment not in text:
            findings.append(Finding(relative, f"missing exact-head quality fragment {fragment!r}"))
    for forbidden in ("continue-on-error", "|| true", "--advisory"):
        if forbidden in text:
            findings.append(Finding(relative, f"quality job must not contain {forbidden!r}"))


def _validate_contract_parity(root: Path, findings: list[Finding]) -> None:
    """Every contract must match the workflow it claims to describe."""
    contracts_path = root / ".github" / "governance" / "workflow-contracts.json"
    document = _load_json(contracts_path, findings)
    if not isinstance(document, dict):
        return
    relative = contracts_path.relative_to(root).as_posix()
    for contract in document.get("contracts", []):
        if not isinstance(contract, dict):
            continue
        implementation = contract.get("implementation")
        if not isinstance(implementation, str) or not implementation:
            continue
        workflow = root / implementation
        if not workflow.is_file():
            findings.append(Finding(implementation, "contract implementation workflow does not exist"))
            continue
        text = workflow.read_text(encoding="utf-8")
        for permission in contract.get("permissions", []):
            key, _, value = str(permission).partition(":")
            if f"{key}: {value}" not in text:
                findings.append(
                    Finding(relative, f"contract {contract.get('id')!r} declares {permission!r}, workflow does not")
                )
        for event in contract.get("events", []):
            trigger = str(event).split(":", 1)[0]
            if trigger and trigger not in text:
                findings.append(
                    Finding(relative, f"contract {contract.get('id')!r} declares event {event!r}, workflow does not")
                )
        mutations = contract.get("mutations", [])
        if not mutations and ("issues: write" in text or "contents: write" in text):
            findings.append(
                Finding(relative, f"contract {contract.get('id')!r} declares no mutations but the workflow can write")
            )


def _validate_expected_sha(root: Path, expected_sha: str | None, findings: list[Finding]) -> None:
    if not expected_sha:
        return
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        findings.append(Finding(".git", f"cannot resolve checked-out revision: {result.stderr.strip()}"))
        return
    actual_sha = result.stdout.strip()
    if actual_sha.lower() != expected_sha.strip().lower():
        findings.append(Finding(".git", f"checked-out revision {actual_sha} does not match expected {expected_sha}"))


def validate_repo(root: Path, expected_sha: str | None = None) -> list[Finding]:
    findings: list[Finding] = []
    _validate_required_files(root, findings)
    _validate_issue_forms(root, findings)
    _validate_capability_profile(root, findings)
    _validate_workflow_contracts(root, findings)
    _validate_issue_form_contract(root, findings)
    _validate_intake_workflow_safety(root, findings)
    _validate_copilot_issue_intake(root, findings)
    _validate_external_supervisor(root, findings)
    _validate_action_versions(root, findings)
    _validate_governance_workflow_safety(root, findings)
    _validate_repro_workflow(root, findings)
    _validate_repro_policy_and_catalog(root, findings)
    _validate_governance_ci(root, findings)
    _validate_contract_parity(root, findings)
    _validate_expected_sha(root, expected_sha, findings)
    return findings


def _escape_annotation(value: str) -> str:
    return value.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def _write_summary(path: Path, findings: Sequence[Finding], expected_sha: str | None, advisory: bool) -> None:
    lines = [
        "## Governance validation",
        "",
        f"- Mode: {'advisory observe' if advisory else 'strict'}",
        f"- Expected revision: `{expected_sha or 'not supplied'}`",
        f"- Findings: {len(findings)}",
        "",
    ]
    if findings:
        lines.extend(["| Path | Finding |", "| --- | --- |"])
        lines.extend(f"| `{finding.path}` | {finding.message} |" for finding in findings)
    else:
        lines.append("All deterministic governance checks passed.")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--expected-sha")
    parser.add_argument("--strict", action="store_true", help="Fail on deterministic findings (the default).")
    parser.add_argument("--advisory", action="store_true", help="Report findings without a nonzero exit status.")
    parser.add_argument("--summary", type=Path, help="Write a Markdown summary to this path.")
    args = parser.parse_args(argv)
    if args.strict and args.advisory:
        parser.error("--strict and --advisory are mutually exclusive")

    root = args.repo_root.resolve()
    findings = validate_repo(root, args.expected_sha)
    annotation_level = "warning" if args.advisory else "error"
    for finding in findings:
        print(
            f"::{annotation_level} file={_escape_annotation(finding.path)}::"
            f"{_escape_annotation(finding.message)}"
        )

    if args.summary:
        _write_summary(args.summary, findings, args.expected_sha, args.advisory)

    if not findings:
        print("Governance validation passed.")
    elif args.advisory:
        print(f"Governance observe mode reported {len(findings)} finding(s).")
    else:
        print(f"Governance validation failed with {len(findings)} finding(s).", file=sys.stderr)

    return 0 if args.advisory or not findings else 1


if __name__ == "__main__":
    raise SystemExit(main())
