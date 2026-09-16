"""Structural safety tests for governance workflows."""

from __future__ import annotations

import json
import unittest
from pathlib import Path

import yaml
from yaml.constructor import ConstructorError


ROOT = Path(__file__).resolve().parent.parent
WORKFLOWS = ROOT / ".github" / "workflows"
GOVERNANCE = ROOT / ".github" / "governance"
GOVERNANCE_WORKFLOWS = (
    "governance-ci.yml",
    "governance-observe.yml",
    "issue-intake.yml",
    "dependabot-intake.yml",
    "repro-bug.yml",
)


class UniqueKeyLoader(yaml.SafeLoader):
    """Safe YAML loader that rejects duplicate mapping keys."""


def _construct_unique_mapping(
    loader: UniqueKeyLoader, node: yaml.nodes.MappingNode, deep: bool = False
) -> dict:
    loader.flatten_mapping(node)
    mapping: dict = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                f"found duplicate key {key!r}",
                key_node.start_mark,
            )
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_unique_mapping
)


def load(name: str) -> dict:
    return yaml.load(
        (WORKFLOWS / name).read_text(encoding="utf-8"), Loader=UniqueKeyLoader
    )


def triggers(document: dict) -> dict:
    return document.get("on", document.get(True))


def job_text(job: dict) -> str:
    return "\n".join(str(step.get("run", "")) for step in job.get("steps", []))


class WorkflowBaselineTests(unittest.TestCase):
    def test_yaml_loader_rejects_duplicate_keys(self) -> None:
        with self.assertRaises(ConstructorError):
            yaml.load("jobs: {}\njobs: {}\n", Loader=UniqueKeyLoader)

    def test_every_workflow_parses(self) -> None:
        for path in sorted(WORKFLOWS.glob("*.yml")):
            with self.subTest(path=path.name):
                document = yaml.load(
                    path.read_text(encoding="utf-8"), Loader=UniqueKeyLoader
                )
                self.assertIsInstance(document, dict)
                self.assertIsInstance(document.get("jobs"), dict)

    def test_governance_jobs_have_permissions_and_timeouts(self) -> None:
        for name in GOVERNANCE_WORKFLOWS:
            for job_name, job in load(name)["jobs"].items():
                with self.subTest(workflow=name, job=job_name):
                    self.assertIsInstance(job.get("permissions"), dict)
                    self.assertIsInstance(job.get("timeout-minutes"), int)

    def test_governance_quality_never_swallows_failure(self) -> None:
        text = (WORKFLOWS / "governance-ci.yml").read_text(encoding="utf-8")
        self.assertNotIn("continue-on-error", text)
        self.assertNotIn("|| true", text)
        self.assertNotIn("--advisory", text)

    def test_removed_privileged_or_misleading_workflows_stay_removed(self) -> None:
        self.assertFalse((WORKFLOWS / "repro-label-reconciliation.yml").exists())
        self.assertFalse((WORKFLOWS / "dependabot-auto-merge.yml").exists())


class ExactHeadGovernanceCiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workflow = load("governance-ci.yml")
        self.job = self.workflow["jobs"]["governance-quality"]

    def test_runs_on_governance_pull_request_changes(self) -> None:
        on = triggers(self.workflow)
        self.assertEqual(set(on), {"pull_request"})
        self.assertIn(".github/**", on["pull_request"]["paths"])
        self.assertIn("test/test_governance*.py", on["pull_request"]["paths"])

    def test_exact_head_checkout_and_verification(self) -> None:
        checkout = self.job["steps"][0]
        self.assertEqual(checkout["with"]["ref"], "${{ github.event.pull_request.head.sha }}")
        self.assertFalse(checkout["with"]["persist-credentials"])
        self.assertIn('test "$(git rev-parse HEAD)" = "${EXPECTED_HEAD}"', job_text(self.job))

    def test_strict_validator_and_every_governance_module_hard_fail(self) -> None:
        body = job_text(self.job)
        self.assertIn("--require-hashes", body)
        self.assertIn("--only-binary=:all:", body)
        requirements = (GOVERNANCE / "requirements-ci.txt").read_text(encoding="utf-8")
        self.assertIn("PyYAML==", requirements)
        self.assertIn("--hash=sha256:", requirements)
        self.assertIn("python tools/validate_governance.py --strict", body)
        self.assertIn(
            'python -m unittest discover -s test -p "test_governance*.py" -v',
            body,
        )


class ReproductionWorkflowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workflow = load("repro-bug.yml")
        self.jobs = self.workflow["jobs"]

    def test_only_exact_label_trigger_exists(self) -> None:
        on = triggers(self.workflow)
        self.assertEqual(on, {"issues": {"types": ["labeled"]}})
        self.assertNotIn("workflow_dispatch", (WORKFLOWS / "repro-bug.yml").read_text())
        condition = self.jobs["authorize"]["if"]
        self.assertIn("github.event.label.name == 'repro-requested'", condition)
        self.assertIn("github.actor == github.event.sender.login", condition)

    def test_job_graph_and_permissions(self) -> None:
        self.assertEqual(
            list(self.jobs), ["authorize", "mark-in-progress", "prepare", "publish-final"]
        )
        self.assertEqual(self.workflow["permissions"], {})
        self.assertEqual(
            self.jobs["authorize"]["permissions"], {"contents": "read", "issues": "read"}
        )
        self.assertEqual(
            self.jobs["mark-in-progress"]["permissions"], {"issues": "write"}
        )
        self.assertEqual(
            self.jobs["prepare"]["permissions"], {"contents": "read", "issues": "read"}
        )
        self.assertEqual(
            self.jobs["publish-final"]["permissions"],
            {"contents": "read", "issues": "write"},
        )

    def test_permission_is_queried_before_processor_or_issue_content(self) -> None:
        steps = self.jobs["authorize"]["steps"]
        self.assertEqual(steps[0]["name"], "Query requesting actor permission")
        first = steps[0]["run"]
        self.assertIn("/collaborators/${encoded_actor}/permission", first)
        self.assertNotIn("/issues/", first)
        self.assertNotIn("checkout", first)
        self.assertEqual(steps[1]["name"], "Record minimal permission gate")
        self.assertIn("repro-minimal-permission-gate", steps[1]["run"])
        self.assertIn("RERUN_ACTOR_MISMATCH", steps[1]["run"])
        self.assertEqual(steps[2]["name"], "Resolve trusted default-branch processor")
        self.assertEqual(
            steps[2]["if"], "steps.gate.outputs.authorized == 'true'"
        )
        self.assertEqual(steps[5]["name"], "Capture authorized title and body snapshot")
        self.assertEqual(steps[-1]["name"], "Enforce permission gate")
        self.assertEqual(steps[-1]["if"], "always()")

    def test_authorization_binds_title_and_body_not_updated_at(self) -> None:
        text = (WORKFLOWS / "repro-bug.yml").read_text(encoding="utf-8")
        for digest in ("title_digest", "body_digest", "content_digest"):
            self.assertIn(digest, text)
        self.assertNotIn("updated_at", text)
        self.assertIn('event.get("issue")', text)
        self.assertIn("current_snapshot", text)
        self.assertIn("issue title or body changed after the label event", text)

    def test_prepare_uses_only_trusted_sha_and_never_executes_avrotize(self) -> None:
        prepare = self.jobs["prepare"]
        checkout = prepare["steps"][0]
        self.assertEqual(checkout["with"]["ref"], "${{ needs.authorize.outputs.trusted_sha }}")
        self.assertFalse(checkout["with"]["persist-credentials"])
        body = job_text(prepare)
        self.assertIn("governance_authorize.py verify", body)
        self.assertIn("governance_repro.py", body)
        for forbidden in (
            "pip install",
            "requirements.txt",
            "python -m avrotize",
            "avrotize ",
            "docker run",
        ):
            self.assertNotIn(forbidden, body)

    def test_artifacts_include_run_attempt_and_use_30_day_retention(self) -> None:
        text = (WORKFLOWS / "repro-bug.yml").read_text(encoding="utf-8")
        self.assertIn("${{ github.run_attempt }}", text)
        uploads = [
            step
            for job in self.jobs.values()
            for step in job["steps"]
            if "upload-artifact" in str(step.get("uses", ""))
        ]
        self.assertGreaterEqual(len(uploads), 3)
        self.assertTrue(all(step["with"]["retention-days"] == 30 for step in uploads))
        self.assertEqual(
            self.jobs["prepare"]["steps"][2]["with"]["name"],
            "${{ needs.authorize.outputs.artifact_name }}",
        )
        publish_download = next(
            step
            for step in self.jobs["publish-final"]["steps"]
            if step["name"] == "Download prepared evidence"
        )
        self.assertEqual(
            publish_download["with"]["name"],
            "${{ needs.prepare.outputs.artifact_name }}",
        )

    def test_terminal_evidence_and_reconciliation_always_run(self) -> None:
        steps = self.jobs["publish-final"]["steps"]
        for name in (
            "Download prepared evidence",
            "Determine terminal state and auditable fallback",
            "Upload terminal evidence",
            "Reconcile one terminal governed state",
            "Publish evidence reference",
        ):
            step = next(value for value in steps if value["name"] == name)
            self.assertEqual(step["if"], "always()")
        body = job_text(self.jobs["publish-final"])
        self.assertIn("governance_repro.py terminal", body)
        self.assertIn('test "$(git rev-parse HEAD)" = "${TRUSTED_SHA}"', body)
        self.assertIn("VALIDATOR_VERIFIED", body)
        self.assertIn('--issue-number "${ISSUE_NUMBER}"', body)
        self.assertIn('--preparation-artifact "${PREPARATION_ARTIFACT}"', body)
        self.assertIn('--preparation-attempt "${PREPARATION_ATTEMPT:-0}"', body)
        self.assertIn("terminal_validator_failed=true", body)
        self.assertIn("trusted terminal evidence validation failed", body)
        self.assertIn("for attempt in 1 2 3", body)
        self.assertIn("attempt_failed=true", body)
        self.assertIn("continue", body)
        self.assertIn("*) FINAL_LABEL=repro-blocked", body)
        self.assertIn("after-labels.txt", body)
        self.assertIn("single governed-state invariant", body)
        self.assertNotIn("atomic", body.lower())

    def test_terminal_automation_cannot_claim_confirmed(self) -> None:
        workflow_body = job_text(self.jobs["publish-final"])
        helper = (ROOT / "tools" / "governance_repro.py").read_text(encoding="utf-8")
        self.assertIn("governance_repro.py terminal", workflow_body)
        self.assertIn('("BLOCKED", "repro-blocked")', helper)
        self.assertIn('("NEEDS_REVIEW", "repro-needs-review")', helper)
        self.assertNotIn('"CONFIRMED"', helper)
        self.assertNotIn('"NOT_REPRODUCED"', helper)


class IssueIntakeWorkflowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workflow = load("issue-intake.yml")
        self.job = self.workflow["jobs"]["normalize"]

    def test_future_issue_events_only_and_no_mutations(self) -> None:
        self.assertEqual(
            triggers(self.workflow),
            {"issues": {"types": ["opened", "edited", "reopened"]}},
        )
        self.assertEqual(self.workflow["permissions"], {})
        self.assertEqual(
            self.job["permissions"],
            {"contents": "read", "copilot-requests": "write"},
        )

    def test_exact_trusted_processor_and_revision_identity(self) -> None:
        body = job_text(self.job)
        self.assertIn("repos/${REPO}/commits/${DEFAULT_BRANCH}", body)
        checkout = self.job["steps"][1]
        self.assertEqual(checkout["with"]["ref"], "${{ steps.processor.outputs.sha }}")
        self.assertIn("--processor-sha", body)
        self.assertIn("${{ github.run_attempt }}", json.dumps(self.workflow))
        self.assertFalse(self.workflow["concurrency"]["cancel-in-progress"])

    def test_copilot_is_noninteractive_zero_tool_and_read_only(self) -> None:
        body = job_text(self.job)
        for required in (
            "--silent",
            "--no-ask-user",
            "--available-tools=",
            "--deny-tool='shell,write,read,url,memory'",
            "--disable-builtin-mcps",
            "--no-custom-instructions",
            "--no-remote-export",
            "--max-ai-credits=",
            "< copilot-prompt.txt",
            "--minimize-content",
        ):
            self.assertIn(required, body)
        for forbidden in (
            "--allow-all",
            "--allow-tool",
            "--allow-url",
            "--yolo",
            "issues: write",
            "contents: write",
            "secrets.",
        ):
            self.assertNotIn(forbidden, body)

    def test_copilot_package_and_artifacts_are_pinned_and_private(self) -> None:
        body = job_text(self.job)
        self.assertIn("npm ci --prefix .github/governance/copilot-cli", body)
        self.assertIn("COPILOT_LOCKFILE_SHA256", body)
        self.assertIn(
            ".github/governance/copilot-cli/node_modules/.bin/copilot",
            body,
        )
        self.assertIn("--ignore-scripts", body)
        upload = self.job["steps"][-1]
        paths = upload["with"]["path"]
        self.assertIn("intake-record.json", paths)
        self.assertIn("copilot-preflight.json", paths)
        self.assertNotIn("event.json", paths)
        self.assertNotIn("copilot-prompt.txt", paths)
        self.assertNotIn("copilot-response", paths)


class DependabotIntakeWorkflowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workflow = load("dependabot-intake.yml")
        self.job = self.workflow["jobs"]["normalize"]

    def test_actor_and_sender_identity_are_required(self) -> None:
        condition = self.job["if"]
        self.assertIn("pull_request.user.login == 'dependabot[bot]'", condition)
        self.assertIn("event.sender.login == 'dependabot[bot]'", condition)
        self.assertEqual(
            triggers(self.workflow)["pull_request_target"]["types"],
            ["opened", "reopened", "synchronize", "ready_for_review"],
        )

    def test_metadata_only_exact_head_binding(self) -> None:
        checkout = self.job["steps"][0]
        self.assertEqual(checkout["with"]["ref"], "${{ github.event.pull_request.base.sha }}")
        self.assertFalse(checkout["with"]["persist-credentials"])
        body = job_text(self.job)
        self.assertIn("head_before=", body)
        self.assertIn("head_after=", body)
        self.assertIn("/pulls/${PR_NUMBER}/files", body)
        self.assertIn("intake_observation", body)
        self.assertIn("--processor-sha", body)
        for forbidden in ("pip install", "npm install", "dotnet", "mvn ", "go test", "cargo"):
            self.assertNotIn(forbidden, body)

    def test_permissions_and_artifact_identity(self) -> None:
        self.assertEqual(
            self.job["permissions"], {"contents": "read", "pull-requests": "read"}
        )
        upload = self.job["steps"][-1]
        name = upload["with"]["name"]
        self.assertIn("pull_request.head.sha", name)
        self.assertIn("github.run_attempt", name)
        self.assertEqual(upload["with"]["retention-days"], 30)


class LabelCatalogTests(unittest.TestCase):
    def test_required_states_are_unique_and_noncontradictory(self) -> None:
        catalog = json.loads(
            (GOVERNANCE / "repro-label-catalog.json").read_text(encoding="utf-8")
        )
        labels = {item["name"]: item for item in catalog["labels"]}
        self.assertTrue(
            {
                "repro-requested",
                "repro-in-progress",
                "repro-confirmed",
                "repro-not-reproduced",
                "repro-blocked",
                "repro-needs-review",
            }.issubset(labels)
        )
        self.assertEqual(len(labels), len(catalog["labels"]))
        self.assertEqual(
            set(labels),
            {
                "repro-requested",
                "repro-in-progress",
                "repro-confirmed",
                "repro-not-reproduced",
                "repro-blocked",
                "repro-needs-review",
            },
        )


if __name__ == "__main__":
    unittest.main()
