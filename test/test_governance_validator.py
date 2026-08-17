import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path

from tools import validate_governance


def _write(path: Path, content: str = "content\n") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _repository(tmp_path: Path) -> Path:
    for relative in validate_governance.REQUIRED_FILES:
        if not relative.endswith(".json"):
            _write(tmp_path / relative)

    for filename, fragments in validate_governance.ISSUE_FORM_REQUIREMENTS.items():
        _write(tmp_path / ".github" / "ISSUE_TEMPLATE" / filename, "\n".join(fragments))

    commands = [
        {"command": "a2x", "group": "schemas"},
        {"command": "x2a", "group": "schemas"},
        {"command": "mcp", "group": "7_Utility"},
    ]
    _write(tmp_path / "avrotize" / "commands.json", json.dumps(commands))

    surfaces = {
        "cli": "avrotize/commands.json",
        "package": "pyproject.toml",
    }
    _write(tmp_path / "pyproject.toml")
    profile = {
        "command_registry": "avrotize/commands.json",
        "expected_command_count": 3,
        "expected_groups": {"schemas": 2, "7_Utility": 1},
        "command_group_areas": {"schema-transformations": ["schemas"]},
        "utility_command_areas": {"mcp": "command-access"},
        "responsibility_domains": {"schema-transformations": ["avrotize/**"]},
        "surfaces": surfaces,
    }
    _write(
        tmp_path / ".github" / "governance" / "avrotize-capabilities.json",
        json.dumps(profile),
    )

    contracts = {
        "contracts": [
            {
                "id": "observe",
                "implementation": None,
                "purpose": "observe",
                "authority_owner": "owner",
                "events": ["pull_request"],
                "inputs": {},
                "deterministic": {},
                "copilot": {
                    "aic_source": "github-copilot-platform",
                    "observed_run_aic": {
                        "sample_size": 0,
                        "p50": "TBD",
                        "p95": "TBD",
                    },
                    "guardrails": {
                        "per_run": "TBD",
                        "daily": "TBD",
                    },
                    "token_telemetry": "operational-only",
                },
                "actions": {},
                "permissions": ["contents:read"],
                "result": {},
            }
        ]
    }
    _write(
        tmp_path / ".github" / "governance" / "workflow-contracts.json",
        json.dumps(contracts),
    )
    return tmp_path


class GovernanceValidatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.root = _repository(Path(self.temporary_directory.name))

    def test_valid_repository_has_no_findings(self) -> None:
        self.assertEqual(validate_governance.validate_repo(self.root), [])

    def test_registry_drift_is_reported(self) -> None:
        profile_path = self.root / ".github" / "governance" / "avrotize-capabilities.json"
        profile = json.loads(profile_path.read_text(encoding="utf-8"))
        profile["expected_command_count"] = 4
        profile_path.write_text(json.dumps(profile), encoding="utf-8")

        findings = validate_governance.validate_repo(self.root)

        self.assertTrue(any("expected_command_count" in finding.message for finding in findings))

    def test_unclassified_utility_command_is_reported(self) -> None:
        profile_path = self.root / ".github" / "governance" / "avrotize-capabilities.json"
        profile = json.loads(profile_path.read_text(encoding="utf-8"))
        profile["utility_command_areas"] = {}
        profile_path.write_text(json.dumps(profile), encoding="utf-8")

        findings = validate_governance.validate_repo(self.root)

        self.assertTrue(any("utility_command_areas must classify exactly" in finding.message for finding in findings))

    def test_uncovered_command_group_is_reported(self) -> None:
        profile_path = self.root / ".github" / "governance" / "avrotize-capabilities.json"
        profile = json.loads(profile_path.read_text(encoding="utf-8"))
        profile["command_group_areas"].pop("schema-transformations")
        profile_path.write_text(json.dumps(profile), encoding="utf-8")

        findings = validate_governance.validate_repo(self.root)

        self.assertTrue(any("command_group_areas must cover registry groups" in finding.message for finding in findings))

    def test_undefined_responsibility_domain_is_reported(self) -> None:
        profile_path = self.root / ".github" / "governance" / "avrotize-capabilities.json"
        profile = json.loads(profile_path.read_text(encoding="utf-8"))
        profile["command_group_areas"]["unknown-domain"] = profile["command_group_areas"].pop(
            "schema-transformations"
        )
        profile_path.write_text(json.dumps(profile), encoding="utf-8")

        findings = validate_governance.validate_repo(self.root)

        self.assertTrue(any("undefined responsibility domains" in finding.message for finding in findings))

    def test_missing_declared_surface_is_reported(self) -> None:
        (self.root / "pyproject.toml").unlink()

        findings = validate_governance.validate_repo(self.root)

        self.assertTrue(any("declared 'package' surface does not exist" in finding.message for finding in findings))

    def test_non_platform_aic_source_is_reported(self) -> None:
        contract_path = self.root / ".github" / "governance" / "workflow-contracts.json"
        document = json.loads(contract_path.read_text(encoding="utf-8"))
        document["contracts"][0]["copilot"]["aic_source"] = "derived"
        contract_path.write_text(json.dumps(document), encoding="utf-8")

        findings = validate_governance.validate_repo(self.root)

        self.assertTrue(any("must use platform-reported AIC" in finding.message for finding in findings))

    def test_missing_empirical_aic_distribution_is_reported(self) -> None:
        contract_path = self.root / ".github" / "governance" / "workflow-contracts.json"
        document = json.loads(contract_path.read_text(encoding="utf-8"))
        del document["contracts"][0]["copilot"]["observed_run_aic"]["p95"]
        contract_path.write_text(json.dumps(document), encoding="utf-8")

        findings = validate_governance.validate_repo(self.root)

        self.assertTrue(any("lacks observed AIC sample size, P50, or P95" in finding.message for finding in findings))

    def test_unobserved_aic_distribution_must_remain_uncalibrated(self) -> None:
        contract_path = self.root / ".github" / "governance" / "workflow-contracts.json"
        document = json.loads(contract_path.read_text(encoding="utf-8"))
        document["contracts"][0]["copilot"]["observed_run_aic"]["p50"] = 10
        contract_path.write_text(json.dumps(document), encoding="utf-8")

        findings = validate_governance.validate_repo(self.root)

        self.assertTrue(any("must keep AIC P50 and P95 uncalibrated" in finding.message for finding in findings))

    def test_advisory_mode_reports_but_succeeds(self) -> None:
        (self.root / "GOVERNANCE.md").unlink()
        stdout = StringIO()
        with redirect_stdout(stdout):
            result = validate_governance.main(["--repo-root", str(self.root), "--advisory"])

        self.assertEqual(result, 0)
        self.assertIn("::warning file=GOVERNANCE.md::", stdout.getvalue())

    def test_strict_mode_fails_on_findings(self) -> None:
        (self.root / "GOVERNANCE.md").unlink()
        with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
            result = validate_governance.main(["--repo-root", str(self.root)])

        self.assertEqual(result, 1)


if __name__ == "__main__":
    unittest.main()
