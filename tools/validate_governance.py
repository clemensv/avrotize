"""Deterministically validate Avrotize governance surfaces."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence


REQUIRED_FILES = (
    "GOVERNANCE.md",
    "CONTRIBUTING.md",
    "SECURITY.md",
    "SUPPORT.md",
    ".github/CODEOWNERS",
    ".github/pull_request_template.md",
    ".github/ISSUE_TEMPLATE/config.yml",
    ".github/ISSUE_TEMPLATE/bug.yml",
    ".github/ISSUE_TEMPLATE/feature.yml",
    ".github/governance/AUTOMATION.md",
    ".github/governance/AI-USAGE-ACCOUNTING.md",
    ".github/governance/ADOPTION.md",
    ".github/governance/avrotize-capabilities.json",
    ".github/governance/workflow-contracts.json",
)

ISSUE_FORM_REQUIREMENTS = {
    "bug.yml": (
        "id: version",
        "id: surface",
        "id: command",
        "id: invocation",
        "id: input",
        "id: output",
        "id: actual",
        "id: expected",
        "id: environment",
        "id: regression",
    ),
    "feature.yml": (
        "id: outcome",
        "id: command",
        "id: input",
        "id: output",
        "id: semantics",
        "id: options",
        "id: validation",
        "id: documentation",
    ),
}

UNRESOLVED_MARKERS = ("{{TODO", "<TODO>", "[TODO]", "REPLACE_ME")


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
    elif dict(sorted(groups.items())) != dict(sorted(expected_groups.items())):
        findings.append(
            Finding(
                profile_path.relative_to(root).as_posix(),
                f"expected_groups {expected_groups!r} do not match registry groups {dict(groups)!r}",
            )
        )

    command_group_areas = profile.get("command_group_areas")
    expected_area_groups = set(expected_groups or {}) - {"7_Utility"}
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
    parser.add_argument("--advisory", action="store_true", help="Report findings without a nonzero exit status.")
    parser.add_argument("--summary", type=Path, help="Write a Markdown summary to this path.")
    args = parser.parse_args(argv)

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
