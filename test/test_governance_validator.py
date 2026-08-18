from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path

from tools import validate_governance


SOURCE = Path(__file__).resolve().parent.parent
COPIED = (
    ".github/governance/repro-label-catalog.json",
    ".github/governance/schemas/repro-evidence-record.schema.json",
    ".github/governance/schemas/repro-terminal-fallback.schema.json",
    ".github/governance/schemas/repro-authorization-record.schema.json",
    ".github/governance/schemas/repro-label-catalog.schema.json",
    ".github/workflows/governance-ci.yml",
    ".github/workflows/governance-observe.yml",
    ".github/workflows/issue-intake.yml",
    ".github/workflows/dependabot-intake.yml",
    ".github/workflows/repro-bug.yml",
    "tools/governance_repro.py",
)


def write(path: Path, content: str = "content\n") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def form_yaml(field_ids: list[str]) -> str:
    lines = ["name: Synthetic form", "body:"]
    for field_id in field_ids:
        lines.extend(
            [
                "  - type: input",
                f"    id: {field_id}",
                "    attributes:",
                f"      label: Field {field_id}",
            ]
        )
    return "\n".join(lines) + "\n"


def repository(root: Path) -> Path:
    for relative in validate_governance.REQUIRED_FILES:
        if not relative.endswith(".json"):
            write(root / relative)

    for filename, fragments in validate_governance.ISSUE_FORM_REQUIREMENTS.items():
        ids = [fragment.split(": ")[1] for fragment in fragments]
        write(root / ".github" / "ISSUE_TEMPLATE" / filename, form_yaml(ids))

    commands = [
        {"command": "a2j", "group": "schemas"},
        {"command": "pcf", "group": "7_Utility"},
        {"command": "mcp", "group": "7_Utility"},
    ]
    write(root / "avrotize" / "commands.json", json.dumps(commands))
    write(root / "pyproject.toml")
    profile = {
        "command_registry": "avrotize/commands.json",
        "expected_command_count": 3,
        "expected_groups": {"schemas": 1, "7_Utility": 2},
        "command_group_areas": {"schema-transformations": ["schemas"]},
        "utility_command_areas": {
            "mcp": "command-access",
            "pcf": "schema-transformations",
        },
        "responsibility_domains": {"schema-transformations": ["avrotize/**"]},
        "surfaces": {
            "cli": "avrotize/commands.json",
            "package": "pyproject.toml",
        },
    }
    write(
        root / ".github" / "governance" / "avrotize-capabilities.json",
        json.dumps(profile),
    )

    forms = []
    for form_type, filename in (("bug", "bug.yml"), ("feature", "feature.yml")):
        ids = [
            fragment.split(": ")[1]
            for fragment in validate_governance.ISSUE_FORM_REQUIREMENTS[filename]
        ]
        forms.append(
            {
                "type": form_type,
                "file": f".github/ISSUE_TEMPLATE/{filename}",
                "title_prefix": f"[{form_type.title()}]",
                "headings": [f"Field {field_id}" for field_id in ids],
                "field_ids": ids,
                "required_semantic_fields": [ids[0]],
            }
        )
    write(
        root / ".github" / "governance" / "issue-form-contract.json",
        json.dumps(
            {
                "schema_version": 2,
                "surface_choices": ["I'm not sure", "Avrotize CLI"],
                "forms": forms,
            }
        ),
    )
    for schema in ("issue-intake-record.schema.json", "dependabot-intake-record.schema.json"):
        write(
            root / ".github" / "governance" / "schemas" / schema,
            json.dumps({"type": "object"}),
        )
    write(
        root / ".github" / "governance" / "workflow-contracts.json",
        json.dumps(
            {
                "contracts": [
                    {
                        "id": "observe",
                        "implementation": None,
                        "purpose": "observe",
                        "authority_owner": "owner",
                        "events": ["pull_request"],
                        "inputs": {},
                        "deterministic": {},
                        "actions": {},
                        "permissions": ["contents:read"],
                        "result": {},
                        "copilot": {
                            "aic_source": "github-copilot-platform",
                            "observed_run_aic": {
                                "sample_size": 0,
                                "p50": "TBD",
                                "p95": "TBD",
                            },
                            "guardrails": {"per_run": "TBD", "daily": "TBD"},
                            "token_telemetry": "operational-only",
                        },
                    }
                ]
            }
        ),
    )
    for relative in COPIED:
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(SOURCE / relative, destination)
    return root


class ValidatorTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = repository(Path(temporary.name))

    def messages(self) -> list[str]:
        return [finding.message for finding in validate_governance.validate_repo(self.root)]

    def mutate(self, relative: str, old: str, new: str) -> None:
        path = self.root / relative
        text = path.read_text(encoding="utf-8")
        self.assertIn(old, text)
        path.write_text(text.replace(old, new, 1), encoding="utf-8")

    def test_valid_repository_has_no_findings(self) -> None:
        self.assertEqual(validate_governance.validate_repo(self.root), [])

    def test_issue_form_contract_requires_semantic_field_declarations(self) -> None:
        path = self.root / ".github" / "governance" / "issue-form-contract.json"
        contract = json.loads(path.read_text(encoding="utf-8"))
        del contract["forms"][0]["required_semantic_fields"]
        path.write_text(json.dumps(contract), encoding="utf-8")
        self.assertTrue(
            any("required_semantic_fields" in value for value in self.messages())
        )

    def test_registry_and_surface_drift_are_reported(self) -> None:
        path = self.root / ".github" / "governance" / "avrotize-capabilities.json"
        profile = json.loads(path.read_text())
        profile["expected_command_count"] = 2
        path.write_text(json.dumps(profile))
        (self.root / "pyproject.toml").unlink()
        messages = self.messages()
        self.assertTrue(any("expected_command_count" in value for value in messages))
        self.assertTrue(any("declared 'package' surface" in value for value in messages))

    def test_platform_reported_aic_rules_are_enforced(self) -> None:
        path = self.root / ".github" / "governance" / "workflow-contracts.json"
        document = json.loads(path.read_text())
        document["contracts"][0]["copilot"]["aic_source"] = "derived"
        document["contracts"][0]["copilot"]["observed_run_aic"]["p50"] = 10
        path.write_text(json.dumps(document))
        messages = self.messages()
        self.assertTrue(any("platform-reported AIC" in value for value in messages))
        self.assertTrue(any("uncalibrated" in value for value in messages))

    def test_advisory_and_strict_cli_modes(self) -> None:
        (self.root / "GOVERNANCE.md").unlink()
        with redirect_stdout(StringIO()) as output:
            self.assertEqual(
                validate_governance.main(
                    ["--repo-root", str(self.root), "--advisory"]
                ),
                0,
            )
        self.assertIn("::warning file=GOVERNANCE.md::", output.getvalue())
        with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
            self.assertEqual(
                validate_governance.main(["--repo-root", str(self.root), "--strict"]),
                1,
            )

    def test_write_permissions_and_merge_commands_are_rejected(self) -> None:
        path = self.root / ".github" / "workflows" / "dependabot-intake.yml"
        path.write_text(path.read_text() + "\n# pull-requests: write\n# gh pr merge\n")
        messages = self.messages()
        self.assertTrue(any("pull-requests:write" in value for value in messages))
        self.assertTrue(any("gh pr merge" in value for value in messages))

    def test_unpinned_action_and_missing_checkout_guard_are_rejected(self) -> None:
        self.mutate(
            ".github/workflows/issue-intake.yml",
            "uses: actions/checkout@v7",
            "uses: actions/checkout",
        )
        self.mutate(
            ".github/workflows/issue-intake.yml",
            "          persist-credentials: false\n",
            "",
        )
        messages = self.messages()
        self.assertTrue(any("not version pinned" in value for value in messages))
        self.assertTrue(any("persist-credentials" in value for value in messages))

    def test_privileged_dispatch_and_execution_regressions_are_rejected(self) -> None:
        path = self.root / ".github" / "workflows" / "repro-bug.yml"
        path.write_text(
            path.read_text()
            + "\n# workflow_dispatch\n# pip install -r requirements.txt\n"
        )
        messages = self.messages()
        self.assertTrue(any("workflow_dispatch" in value for value in messages))
        self.assertTrue(any("preparation-only" in value for value in messages))

    def test_mutable_issue_timestamp_is_rejected(self) -> None:
        self.mutate(
            ".github/workflows/repro-bug.yml",
            "      body_digest: ${{ steps.snapshot.outputs.body_digest }}",
            "      updated_at: ${{ github.event.issue.updated_at }}",
        )
        self.assertTrue(any("updated_at" in value for value in self.messages()))

    def test_governance_ci_cannot_swallow_failures(self) -> None:
        path = self.root / ".github" / "workflows" / "governance-ci.yml"
        path.write_text(path.read_text() + "\n# continue-on-error\n")
        self.assertTrue(any("quality job" in value for value in self.messages()))

    def test_corrupt_schema_is_reported_not_crashed(self) -> None:
        path = (
            self.root
            / ".github"
            / "governance"
            / "schemas"
            / "repro-label-catalog.schema.json"
        )
        path.write_text("{not-json")
        findings = validate_governance.validate_repo(self.root)
        self.assertTrue(any("cannot load schema" in value.message for value in findings))


if __name__ == "__main__":
    unittest.main()
