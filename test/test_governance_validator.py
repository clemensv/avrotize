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
    ".gitattributes",
    "GOVERNANCE.md",
    ".github/governance/ADOPTION.md",
    ".github/governance/AUTOMATION.md",
    ".github/governance/EXTERNAL-SUPERVISOR.md",
    ".github/governance/copilot-intake-policy.json",
    ".github/governance/external-supervisor-policy.json",
    ".github/governance/copilot-cli/package.json",
    ".github/governance/copilot-cli/package-lock.json",
    ".github/governance/prompts/external-supervisor-kickoff-v2.txt",
    ".github/governance/prompts/issue-semantic-assistance-v1.txt",
    ".github/governance/repro-label-catalog.json",
    ".github/governance/schemas/issue-semantic-assistance.schema.json",
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

EXPECTED_EXTERNAL_SUPERVISOR_OWNER_ONLY_ACTIONS = frozenset(
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
EXPECTED_EXTERNAL_SUPERVISOR_PROHIBITED_ACTIONS = frozenset(
    {
        *EXPECTED_EXTERNAL_SUPERVISOR_OWNER_ONLY_ACTIONS,
        "approve-pull-request",
    }
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
    capability_profile = json.loads(
        (
            SOURCE
            / ".github"
            / "governance"
            / "avrotize-capabilities.json"
        ).read_text(encoding="utf-8")
    )
    domains = list(capability_profile["responsibility_domains"])
    profile = {
        "command_registry": "avrotize/commands.json",
        "expected_command_count": 3,
        "expected_groups": {"schemas": 1, "7_Utility": 2},
        "command_group_areas": {
            domains[0]: ["schemas"]
        },
        "utility_command_areas": {
            "mcp": "command-access",
            "pcf": domains[0],
        },
        "responsibility_domains": {
            domain: ["avrotize/**"]
            for domain in domains
        },
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
                    },
                    {
                        "id": "external-delivery-supervisor",
                        "implementation": None,
                        "purpose": "validate owner delegation",
                        "authority_owner": "owner",
                        "events": ["owner-launched project session"],
                        "inputs": {},
                        "deterministic": {},
                        "actions": {
                            "mutations": sorted(
                                validate_governance.EXTERNAL_SUPERVISOR_ROUTINE_ACTIONS
                            ),
                            "owner_only": sorted(
                                validate_governance.EXTERNAL_SUPERVISOR_OWNER_ONLY_ACTIONS
                            ),
                            "supervisor_prohibited": sorted(
                                validate_governance.EXTERNAL_SUPERVISOR_PROHIBITED_ACTIONS
                            ),
                        },
                        "permissions": [],
                        "result": {},
                        "copilot": {
                            "aic_source": "github-copilot-platform",
                            "observed_run_aic": {
                                "sample_size": 0,
                                "p50": "TBD",
                                "p95": "TBD",
                            },
                            "guardrails": {
                                "per_run": "owner delegation",
                                "daily": "TBD",
                            },
                            "token_telemetry": "operational-only",
                        },
                    },
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

    def test_copilot_issue_intake_requires_zero_tool_boundary(self) -> None:
        path = self.root / ".github" / "workflows" / "issue-intake.yml"
        text = path.read_text(encoding="utf-8")
        text = text.replace("--available-tools=", "--allow-all", 1)
        path.write_text(text, encoding="utf-8")
        messages = self.messages()
        self.assertTrue(any("missing Copilot intake control" in value for value in messages))
        self.assertTrue(any("forbidden Copilot intake capability" in value for value in messages))

    def test_copilot_issue_intake_requires_permission(self) -> None:
        self.mutate(
            ".github/workflows/issue-intake.yml",
            "copilot-requests: write",
            "copilot-requests: read",
        )
        self.assertTrue(
            any("copilot-requests: write" in value for value in self.messages())
        )

    def test_copilot_policy_and_workflow_version_must_match(self) -> None:
        self.mutate(
            ".github/workflows/issue-intake.yml",
            'COPILOT_CLI_VERSION: "1.0.80"',
            'COPILOT_CLI_VERSION: "1.0.79"',
        )
        self.assertTrue(
            any("COPILOT_CLI_VERSION" in value for value in self.messages())
        )

    def test_external_supervisor_prompt_digest_is_enforced(self) -> None:
        path = (
            self.root
            / ".github"
            / "governance"
            / "prompts"
            / "external-supervisor-kickoff-v2.txt"
        )
        path.write_text(path.read_text(encoding="utf-8") + "\ndrift\n", encoding="utf-8")
        self.assertTrue(
            any("kickoff prompt digest" in value for value in self.messages())
        )

    def test_external_supervisor_prompt_digest_uses_raw_bytes(self) -> None:
        path = (
            self.root
            / ".github"
            / "governance"
            / "prompts"
            / "external-supervisor-kickoff-v2.txt"
        )
        path.write_bytes(path.read_bytes().replace(b"\n", b"\r\n"))
        self.assertTrue(
            any("kickoff prompt digest" in value for value in self.messages())
        )

    def test_external_supervisor_prompt_requires_lf_attribute(self) -> None:
        self.mutate(
            ".gitattributes",
            ".github/governance/prompts/external-supervisor-kickoff-v2.txt text eol=lf",
            ".github/governance/prompts/external-supervisor-kickoff-v1.txt text eol=lf",
        )
        messages = self.messages()
        self.assertTrue(any("governed as LF text" in value for value in messages))
        self.assertTrue(any("v1 kickoff prompt remains governed" in value for value in messages))

    def test_external_supervisor_requires_fresh_admin_host_authority(self) -> None:
        path = self.root / ".github" / "governance" / "external-supervisor-policy.json"
        policy = json.loads(path.read_text(encoding="utf-8"))
        policy["host_operational_authority"]["required_repository_permission"] = "write"
        path.write_text(json.dumps(policy), encoding="utf-8")
        self.assertTrue(
            any("trusted-host admin contract" in value for value in self.messages())
        )

    def test_external_supervisor_requires_every_owner_only_action(self) -> None:
        path = self.root / ".github" / "governance" / "external-supervisor-policy.json"
        original = json.loads(path.read_text(encoding="utf-8"))
        actions = original["host_operational_authority"]["owner_only_actions"]
        self.assertEqual(
            set(actions),
            EXPECTED_EXTERNAL_SUPERVISOR_OWNER_ONLY_ACTIONS,
        )
        self.assertEqual(
            validate_governance.EXTERNAL_SUPERVISOR_OWNER_ONLY_ACTIONS,
            EXPECTED_EXTERNAL_SUPERVISOR_OWNER_ONLY_ACTIONS,
        )
        for removed in EXPECTED_EXTERNAL_SUPERVISOR_OWNER_ONLY_ACTIONS:
            with self.subTest(removed=removed):
                policy = json.loads(json.dumps(original))
                policy["host_operational_authority"]["owner_only_actions"].remove(
                    removed
                )
                path.write_text(json.dumps(policy), encoding="utf-8")
                self.assertTrue(
                    any(
                        "owner-only actions must match the complete exact required set"
                        in value
                        for value in self.messages()
                    )
                )
        path.write_text(json.dumps(original), encoding="utf-8")

    def test_external_supervisor_requires_every_prohibited_action(self) -> None:
        path = self.root / ".github" / "governance" / "external-supervisor-policy.json"
        original = json.loads(path.read_text(encoding="utf-8"))
        actions = original["host_operational_authority"][
            "supervisor_prohibited_actions"
        ]
        self.assertIn("approve-pull-request", actions)
        self.assertEqual(
            set(actions),
            EXPECTED_EXTERNAL_SUPERVISOR_PROHIBITED_ACTIONS,
        )
        self.assertEqual(
            validate_governance.EXTERNAL_SUPERVISOR_PROHIBITED_ACTIONS,
            EXPECTED_EXTERNAL_SUPERVISOR_PROHIBITED_ACTIONS,
        )
        for removed in EXPECTED_EXTERNAL_SUPERVISOR_PROHIBITED_ACTIONS:
            with self.subTest(removed=removed):
                policy = json.loads(json.dumps(original))
                policy["host_operational_authority"][
                    "supervisor_prohibited_actions"
                ].remove(removed)
                path.write_text(json.dumps(policy), encoding="utf-8")
                self.assertTrue(
                    any(
                        "supervisor-prohibited actions must match the complete exact required set"
                        in value
                        for value in self.messages()
                    )
                )
        path.write_text(json.dumps(original), encoding="utf-8")

    def test_external_supervisor_rejects_added_or_misclassified_actions(self) -> None:
        path = self.root / ".github" / "governance" / "external-supervisor-policy.json"
        original = json.loads(path.read_text(encoding="utf-8"))
        cases = (
            ("owner_only_actions", "approve-pull-request", "owner-only actions"),
            (
                "supervisor_prohibited_actions",
                "unrecognized-reserved-action",
                "supervisor-prohibited actions",
            ),
        )
        for field, addition, expected in cases:
            with self.subTest(field=field, addition=addition):
                policy = json.loads(json.dumps(original))
                policy["host_operational_authority"][field].append(addition)
                path.write_text(json.dumps(policy), encoding="utf-8")
                self.assertTrue(
                    any(
                        f"{expected} must match the complete exact required set"
                        in value
                        for value in self.messages()
                    )
                )
        path.write_text(json.dumps(original), encoding="utf-8")

    def test_external_supervisor_routine_actions_cannot_overlap_reserved_sets(
        self,
    ) -> None:
        path = self.root / ".github" / "governance" / "external-supervisor-policy.json"
        original = json.loads(path.read_text(encoding="utf-8"))
        cases = (
            ("merge", "overlap owner-only actions"),
            ("approve-pull-request", "overlap supervisor-prohibited actions"),
        )
        for addition, expected in cases:
            with self.subTest(addition=addition):
                policy = json.loads(json.dumps(original))
                policy["host_operational_authority"]["routine_actions"].append(
                    addition
                )
                path.write_text(json.dumps(policy), encoding="utf-8")
                self.assertTrue(any(expected in value for value in self.messages()))
        path.write_text(json.dumps(original), encoding="utf-8")

    def test_external_supervisor_workflow_requires_complete_action_sets(self) -> None:
        path = self.root / ".github" / "governance" / "workflow-contracts.json"
        original = json.loads(path.read_text(encoding="utf-8"))
        contract = next(
            value
            for value in original["contracts"]
            if value["id"] == "external-delivery-supervisor"
        )
        cases = (
            (
                "owner_only",
                EXPECTED_EXTERNAL_SUPERVISOR_OWNER_ONLY_ACTIONS,
                "owner-only actions must match the complete exact required set",
            ),
            (
                "supervisor_prohibited",
                EXPECTED_EXTERNAL_SUPERVISOR_PROHIBITED_ACTIONS,
                "supervisor-prohibited actions must match the complete exact required set",
            ),
        )
        for field, required, expected in cases:
            self.assertEqual(set(contract["actions"][field]), required)
            for removed in contract["actions"][field]:
                with self.subTest(field=field, removed=removed):
                    document = json.loads(json.dumps(original))
                    candidate = next(
                        value
                        for value in document["contracts"]
                        if value["id"] == "external-delivery-supervisor"
                    )
                    candidate["actions"][field].remove(removed)
                    path.write_text(json.dumps(document), encoding="utf-8")
                    self.assertTrue(
                        any(expected in value for value in self.messages())
                    )
        path.write_text(json.dumps(original), encoding="utf-8")

    def test_external_supervisor_workflow_routine_actions_cannot_overlap_reserved_sets(
        self,
    ) -> None:
        path = self.root / ".github" / "governance" / "workflow-contracts.json"
        original = json.loads(path.read_text(encoding="utf-8"))
        cases = (
            ("merge", "workflow routine actions overlap owner-only actions"),
            (
                "approve-pull-request",
                "workflow routine actions overlap supervisor-prohibited actions",
            ),
        )
        for addition, expected in cases:
            with self.subTest(addition=addition):
                document = json.loads(json.dumps(original))
                contract = next(
                    value
                    for value in document["contracts"]
                    if value["id"] == "external-delivery-supervisor"
                )
                contract["actions"]["mutations"].append(addition)
                path.write_text(json.dumps(document), encoding="utf-8")
                self.assertTrue(any(expected in value for value in self.messages()))
        path.write_text(json.dumps(original), encoding="utf-8")

    def test_external_supervisor_requires_retry_safe_mutations(self) -> None:
        path = self.root / ".github" / "governance" / "external-supervisor-policy.json"
        policy = json.loads(path.read_text(encoding="utf-8"))
        policy["host_operational_authority"]["mutation_rules"][
            "only_retry_safe_operations"
        ] = False
        path.write_text(json.dumps(policy), encoding="utf-8")
        self.assertTrue(
            any("mutation rules are incomplete" in value for value in self.messages())
        )

    def test_external_supervisor_requires_comment_marker_without_exception(self) -> None:
        path = self.root / ".github" / "governance" / "external-supervisor-policy.json"
        policy = json.loads(path.read_text(encoding="utf-8"))
        policy["host_operational_authority"]["mutation_rules"][
            "manual_confirmation_cannot_replace_comment_marker"
        ] = False
        path.write_text(json.dumps(policy), encoding="utf-8")
        self.assertTrue(
            any("mutation rules are incomplete" in value for value in self.messages())
        )

    def test_external_supervisor_document_contract_is_enforced(self) -> None:
        self.mutate(
            ".github/governance/EXTERNAL-SUPERVISOR.md",
            "stable operation marker",
            "operation marker",
        )
        self.assertTrue(
            any("stable operation marker" in value for value in self.messages())
        )

    def test_obsolete_authority_runtime_artifacts_are_rejected(self) -> None:
        write(self.root / "tools" / "governance_supervisor.py")
        self.assertTrue(
            any(
                "obsolete local authority runtime artifact" in value
                for value in self.messages()
            )
        )

    def test_reproduction_comment_requires_marker_deduplication(self) -> None:
        self.mutate(
            ".github/workflows/repro-bug.yml",
            'grep -Fqx -- "${marker}" terminal-comment.md',
            'grep -Fqx -- "${comment}" terminal-comment.md',
        )
        self.assertTrue(
            any(
                "deduplicate its marked issue comment" in value
                for value in self.messages()
            )
        )

    def test_external_supervisor_cannot_gain_a_privileged_workflow(self) -> None:
        write(
            self.root / ".github" / "workflows" / "external-supervisor.yml",
            "name: forbidden\n",
        )
        self.assertTrue(
            any("must not have a privileged Actions workflow" in value for value in self.messages())
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
