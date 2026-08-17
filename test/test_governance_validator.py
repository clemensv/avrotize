import json
import shutil
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path

from tools import validate_governance

SOURCE_ROOT = Path(__file__).resolve().parent.parent

#: Governance artifacts copied verbatim so the fixture exercises the real rules.
COPIED_ARTIFACTS = (
    ".github/governance/repro-command-policy.json",
    ".github/governance/repro-label-catalog.json",
    ".github/governance/schemas/repro-evidence-record.schema.json",
    ".github/governance/schemas/repro-authorization-record.schema.json",
    ".github/governance/schemas/repro-command-policy.schema.json",
    ".github/governance/schemas/repro-label-catalog.schema.json",
    ".github/workflows/issue-intake.yml",
    ".github/workflows/dependabot-intake.yml",
    ".github/workflows/dependabot-auto-merge.yml",
    ".github/workflows/governance-observe.yml",
    ".github/workflows/repro-bug.yml",
    ".github/workflows/repro-label-reconciliation.yml",
)


def _write(path: Path, content: str = "content\n") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _form_yaml(field_ids: list[str]) -> str:
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


def _repository(tmp_path: Path) -> Path:
    for relative in validate_governance.REQUIRED_FILES:
        if not relative.endswith(".json"):
            _write(tmp_path / relative)

    for filename, fragments in validate_governance.ISSUE_FORM_REQUIREMENTS.items():
        field_ids = [fragment.split(": ")[1] for fragment in fragments]
        _write(tmp_path / ".github" / "ISSUE_TEMPLATE" / filename, _form_yaml(field_ids))

    commands = [
        {"command": "a2j", "group": "schemas"},
        {"command": "a2asn", "group": "schemas"},
        {"command": "s2asn", "group": "schemas"},
        {"command": "pcf", "group": "7_Utility"},
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
        "expected_command_count": 5,
        "expected_groups": {"schemas": 3, "7_Utility": 2},
        "command_group_areas": {"schema-transformations": ["schemas"]},
        "utility_command_areas": {"mcp": "command-access", "pcf": "schema-transformations"},
        "responsibility_domains": {"schema-transformations": ["avrotize/**"]},
        "surfaces": surfaces,
    }
    _write(
        tmp_path / ".github" / "governance" / "avrotize-capabilities.json",
        json.dumps(profile),
    )

    # Issue form contract referencing the test form files
    issue_form_contract = {
        "schema_version": 1,
        "expected_result_choices": {"Successful completion (exit 0)": "success"},
        "forms": [
            {
                "type": "bug",
                "file": ".github/ISSUE_TEMPLATE/bug.yml",
                "title_prefix": "[Bug]",
                "headings": [
                    f"Field {frag.split(': ')[1]}"
                    for frag in validate_governance.ISSUE_FORM_REQUIREMENTS["bug.yml"]
                ],
                "field_ids": [frag.split(": ")[1] for frag in validate_governance.ISSUE_FORM_REQUIREMENTS["bug.yml"]],
            },
            {
                "type": "feature",
                "file": ".github/ISSUE_TEMPLATE/feature.yml",
                "title_prefix": "[Feature]",
                "headings": [
                    f"Field {frag.split(': ')[1]}"
                    for frag in validate_governance.ISSUE_FORM_REQUIREMENTS["feature.yml"]
                ],
                "field_ids": [frag.split(": ")[1] for frag in validate_governance.ISSUE_FORM_REQUIREMENTS["feature.yml"]],
            },
        ],
    }
    _write(
        tmp_path / ".github" / "governance" / "issue-form-contract.json",
        json.dumps(issue_form_contract),
    )
    _write(
        tmp_path / ".github" / "governance" / "schemas" / "issue-intake-record.schema.json",
        json.dumps({"type": "object"}),
    )
    _write(
        tmp_path / ".github" / "governance" / "schemas" / "dependabot-intake-record.schema.json",
        json.dumps({"type": "object"}),
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

    for relative in COPIED_ARTIFACTS:
        destination = tmp_path / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(SOURCE_ROOT / relative, destination)
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

    def test_malformed_expected_groups_is_reported_without_crashing(self) -> None:
        profile_path = self.root / ".github" / "governance" / "avrotize-capabilities.json"
        profile = json.loads(profile_path.read_text(encoding="utf-8"))
        profile["expected_groups"] = 8
        profile_path.write_text(json.dumps(profile), encoding="utf-8")

        findings = validate_governance.validate_repo(self.root)

        self.assertTrue(any("expected_groups must be an object" in finding.message for finding in findings))

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

    def test_write_permission_detected(self) -> None:
        """Intake workflow with contents:write must be flagged."""
        wf_path = self.root / ".github" / "workflows" / "issue-intake.yml"
        wf_path.parent.mkdir(parents=True, exist_ok=True)
        _write(wf_path, "permissions:\n  contents: write\n")
        findings = validate_governance.validate_repo(self.root)
        self.assertTrue(any("contents:write" in f.message for f in findings))

    def test_pr_write_permission_detected(self) -> None:
        """Intake workflow with pull-requests:write must be flagged."""
        wf_path = self.root / ".github" / "workflows" / "dependabot-intake.yml"
        wf_path.parent.mkdir(parents=True, exist_ok=True)
        _write(wf_path, "permissions:\n  pull-requests: write\n")
        findings = validate_governance.validate_repo(self.root)
        self.assertTrue(any("pull-requests:write" in f.message for f in findings))

    def test_merge_command_detected(self) -> None:
        """Intake workflow containing 'gh pr merge' must be flagged."""
        wf_path = self.root / ".github" / "workflows" / "dependabot-auto-merge.yml"
        wf_path.parent.mkdir(parents=True, exist_ok=True)
        _write(wf_path, "run: gh pr merge --squash\npermissions:\n  contents: read\n")
        findings = validate_governance.validate_repo(self.root)
        self.assertTrue(any("gh pr merge" in f.message for f in findings))

    def test_missing_persist_credentials_detected(self) -> None:
        """Checkout without persist-credentials: false must be flagged."""
        wf_path = self.root / ".github" / "workflows" / "issue-intake.yml"
        wf_path.parent.mkdir(parents=True, exist_ok=True)
        _write(wf_path, "uses: actions/checkout@v7\nwith:\n  ref: main\npermissions:\n  contents: read\n")
        findings = validate_governance.validate_repo(self.root)
        self.assertTrue(any("persist-credentials" in f.message for f in findings))

    def test_missing_schema_file_detected(self) -> None:
        """Missing required schema file must be flagged."""
        schema_path = self.root / ".github" / "governance" / "schemas" / "issue-intake-record.schema.json"
        if schema_path.exists():
            schema_path.unlink()
        findings = validate_governance.validate_repo(self.root)
        self.assertTrue(any("required schema file missing" in f.message for f in findings))


class WorkflowStaticAnalysisTests(unittest.TestCase):
    """Negative tests for the static workflow rules."""

    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.root = _repository(Path(self.temporary_directory.name))
        self.repro = self.root / ".github" / "workflows" / "repro-bug.yml"

    def _mutate(self, path: Path, old: str, new: str) -> None:
        text = path.read_text(encoding="utf-8")
        self.assertIn(old, text)
        path.write_text(text.replace(old, new, 1), encoding="utf-8")

    def _messages(self) -> list[str]:
        return [finding.message for finding in validate_governance.validate_repo(self.root)]

    def test_unpinned_action_is_reported(self) -> None:
        self._mutate(self.repro, "uses: actions/checkout@v7", "uses: actions/checkout")
        self.assertTrue(any("is not version pinned" in message for message in self._messages()))

    def test_wrong_action_major_is_reported(self) -> None:
        self._mutate(self.repro, "uses: actions/setup-python@v7", "uses: actions/setup-python@v5")
        self.assertTrue(any("must be pinned to v7" in message for message in self._messages()))

    def test_suppressed_failure_is_reported(self) -> None:
        self._mutate(self.repro, "test -s governance_authorize.py", "test -s governance_authorize.py || true")
        self.assertTrue(any("'|| true'" in message for message in self._messages()))

    def test_continue_on_error_is_reported(self) -> None:
        self._mutate(self.repro, "      - name: Enforce authorization decision", "      - continue-on-error: true\n      - name: Enforce authorization decision")
        self.assertTrue(any("continue-on-error" in message for message in self._messages()))

    def test_missing_job_timeout_is_reported(self) -> None:
        self._mutate(self.repro, "    timeout-minutes: 5\n    permissions:\n      contents: read\n      issues: read", "    permissions:\n      contents: read\n      issues: read")
        self.assertTrue(any("must declare timeout-minutes" in message for message in self._messages()))

    def test_concurrency_with_run_id_is_reported(self) -> None:
        self._mutate(
            self.repro,
            "  group: repro-bug-${{ github.event.issue.number || inputs.issue_number }}",
            "  group: repro-bug-${{ github.event.issue.number || inputs.issue_number }}-${{ github.run_id }}",
        )
        self.assertTrue(any("would not cancel" in message for message in self._messages()))

    def test_undeclared_rendered_form_label_is_reported(self) -> None:
        contract_path = self.root / ".github" / "governance" / "issue-form-contract.json"
        contract = json.loads(contract_path.read_text(encoding="utf-8"))
        contract["forms"] = [
            {
                "type": "bug",
                "file": ".github/ISSUE_TEMPLATE/bug.yml",
                "title_prefix": "[Bug]",
                "headings": ["Only heading"],
                "field_ids": ["version"],
                "required_semantic_fields": [],
            }
        ]
        contract_path.write_text(json.dumps(contract), encoding="utf-8")
        form = self.root / ".github" / "ISSUE_TEMPLATE" / "bug.yml"
        form.write_text(
            "body:\n"
            "  - type: input\n"
            "    id: version\n"
            "    attributes:\n"
            "      label: Only heading\n"
            "  - type: checkboxes\n"
            "    id: confirmation\n"
            "    attributes:\n"
            "      label: Confirmation\n"
            "      options:\n"
            "        - label: I confirm\n",
            encoding="utf-8",
        )
        messages = self._messages()
        self.assertTrue(any("undeclared rendered labels: ['Confirmation']" in message for message in messages))

    def test_issue_supplied_ref_is_reported(self) -> None:
        self._mutate(
            self.repro,
            "          ref: ${{ needs.authorize.outputs.trusted_sha }}",
            "          ref: ${{ github.event.issue.title }}",
        )
        self.assertTrue(any("git ref from issue" in message for message in self._messages()))

    def test_issue_content_in_shell_is_reported(self) -> None:
        self._mutate(
            self.repro,
            "          actual=$(git rev-parse HEAD)",
            "          actual=\"${{ github.event.issue.body }}\"",
        )
        self.assertTrue(any("issue content in a shell body" in message for message in self._messages()))

    def test_pull_request_target_in_repro_is_reported(self) -> None:
        self._mutate(self.repro, "on:\n  issues:", "on:\n  pull_request_target:\n    types: [opened]\n  issues:")
        self.assertTrue(any("pull_request_target" in message for message in self._messages()))

    def test_publish_final_must_always_run(self) -> None:
        self._mutate(self.repro, "    if: always() && needs.authorize.result == 'success'", "    if: needs.authorize.result == 'success'")
        self.assertTrue(any("publish-final must run even when reproduction fails" in message for message in self._messages()))

    def test_reproduce_with_issue_write_is_reported(self) -> None:
        self._mutate(
            self.repro,
            "    permissions:\n      contents: read\n      issues: read\n    steps:\n      - name: Checkout trusted revision",
            "    permissions:\n      contents: read\n      issues: write\n    steps:\n      - name: Checkout trusted revision",
        )
        self.assertTrue(any("reproduce must not hold issues: write" in message for message in self._messages()))

    def test_missing_retention_days_is_reported(self) -> None:
        text = self.repro.read_text(encoding="utf-8").replace("          retention-days: 14\n", "")
        self.repro.write_text(text, encoding="utf-8")
        self.assertTrue(any("retention-days" in message for message in self._messages()))

    def test_label_reconciliation_must_stay_manual(self) -> None:
        path = self.root / ".github" / "workflows" / "repro-label-reconciliation.yml"
        self._mutate(path, "on:\n  workflow_dispatch:", "on:\n  schedule:\n    - cron: '0 0 * * *'\n  workflow_dispatch:")
        self.assertTrue(any("manual dispatch only" in message for message in self._messages()))

    def test_label_reconciliation_must_not_touch_issues(self) -> None:
        path = self.root / ".github" / "workflows" / "repro-label-reconciliation.yml"
        self._mutate(path, "${GITHUB_API_URL}/repos/${REPO}/labels", "${GITHUB_API_URL}/repos/${REPO}/issues/1/labels")
        self.assertTrue(any("must not touch issue state" in message for message in self._messages()))

    def test_policy_command_outside_registry_is_reported(self) -> None:
        path = self.root / ".github" / "governance" / "repro-command-policy.json"
        policy = json.loads(path.read_text(encoding="utf-8"))
        policy["commands"][0]["command"] = "notacommand"
        path.write_text(json.dumps(policy), encoding="utf-8")
        self.assertTrue(any("policy allows unknown command" in message for message in self._messages()))

    def test_policy_schema_violation_is_reported(self) -> None:
        path = self.root / ".github" / "governance" / "repro-command-policy.json"
        policy = json.loads(path.read_text(encoding="utf-8"))
        policy["limits"]["timeout_seconds"] = 100000
        path.write_text(json.dumps(policy), encoding="utf-8")
        self.assertTrue(any("failed schema validation" in message for message in self._messages()))

    def test_catalog_label_drift_is_reported(self) -> None:
        path = self.root / ".github" / "governance" / "repro-label-catalog.json"
        catalog = json.loads(path.read_text(encoding="utf-8"))
        catalog["labels"][0]["name"] = "repro-other"
        path.write_text(json.dumps(catalog), encoding="utf-8")
        self.assertTrue(any("catalog must declare exactly" in message for message in self._messages()))

    def test_evidence_label_enum_drift_is_reported(self) -> None:
        path = self.root / ".github" / "governance" / "schemas" / "repro-evidence-record.schema.json"
        schema = json.loads(path.read_text(encoding="utf-8"))
        schema["properties"]["result"]["properties"]["final_label"]["enum"] = ["repro-confirmed"]
        path.write_text(json.dumps(schema), encoding="utf-8")
        self.assertTrue(any("final_label enum must equal" in message for message in self._messages()))

    def test_contract_permission_drift_is_reported(self) -> None:
        path = self.root / ".github" / "governance" / "workflow-contracts.json"
        document = json.loads(path.read_text(encoding="utf-8"))
        document["contracts"][0]["implementation"] = ".github/workflows/repro-bug.yml"
        document["contracts"][0]["permissions"] = ["contents:write"]
        document["contracts"][0]["mutations"] = ["labels"]
        path.write_text(json.dumps(document), encoding="utf-8")
        self.assertTrue(any("declares 'contents:write', workflow does not" in message for message in self._messages()))

    def test_contract_without_mutations_but_write_permission_is_reported(self) -> None:
        path = self.root / ".github" / "governance" / "workflow-contracts.json"
        document = json.loads(path.read_text(encoding="utf-8"))
        document["contracts"][0]["implementation"] = ".github/workflows/repro-bug.yml"
        document["contracts"][0]["permissions"] = []
        document["contracts"][0]["mutations"] = []
        document["contracts"][0]["events"] = ["issues"]
        path.write_text(json.dumps(document), encoding="utf-8")
        self.assertTrue(any("declares no mutations but the workflow can write" in message for message in self._messages()))

    def test_authorize_job_must_not_check_out(self) -> None:
        self._mutate(
            self.repro,
            "      - name: Resolve trusted default-branch revision",
            "      - uses: actions/checkout@v7\n        with:\n          persist-credentials: false\n      - name: Resolve trusted default-branch revision",
        )
        self.assertTrue(any("authorization must not check out" in message for message in self._messages()))

    def test_real_repository_passes_strict_validation(self) -> None:
        self.assertEqual(validate_governance.validate_repo(SOURCE_ROOT), [])


if __name__ == "__main__":
    unittest.main()
