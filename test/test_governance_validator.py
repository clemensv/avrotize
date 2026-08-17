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
    ]
    _write(tmp_path / "avrotize" / "commands.json", json.dumps(commands))

    surfaces = {
        "cli": "avrotize/commands.json",
        "package": "pyproject.toml",
    }
    _write(tmp_path / "pyproject.toml")
    profile = {
        "command_registry": "avrotize/commands.json",
        "expected_command_count": 2,
        "expected_groups": {"schemas": 2},
        "surfaces": surfaces,
    }
    _write(
        tmp_path / ".github" / "governance" / "conversion-matrix.json",
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
                "copilot": {},
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
        profile_path = self.root / ".github" / "governance" / "conversion-matrix.json"
        profile = json.loads(profile_path.read_text(encoding="utf-8"))
        profile["expected_command_count"] = 3
        profile_path.write_text(json.dumps(profile), encoding="utf-8")

        findings = validate_governance.validate_repo(self.root)

        self.assertTrue(any("expected_command_count" in finding.message for finding in findings))

    def test_missing_declared_surface_is_reported(self) -> None:
        (self.root / "pyproject.toml").unlink()

        findings = validate_governance.validate_repo(self.root)

        self.assertTrue(any("declared 'package' surface does not exist" in finding.message for finding in findings))

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
