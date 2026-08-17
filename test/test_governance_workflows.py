"""Structural tests for governance workflow YAML.

These parse the workflows and assert the safety contract that the stdlib
validator enforces textually, so both layers must agree.
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
WORKFLOWS = REPO_ROOT / ".github" / "workflows"
GOVERNANCE = REPO_ROOT / ".github" / "governance"

GOVERNANCE_WORKFLOW_NAMES = (
    "issue-intake.yml",
    "dependabot-intake.yml",
    "dependabot-auto-merge.yml",
    "governance-observe.yml",
    "repro-bug.yml",
    "repro-label-reconciliation.yml",
)


def load(name: str) -> dict:
    return yaml.safe_load((WORKFLOWS / name).read_text(encoding="utf-8"))


def triggers(document: dict) -> dict:
    # PyYAML parses the bare key `on` as boolean True.
    return document.get("on", document.get(True))


def steps(job: dict) -> list[dict]:
    return job.get("steps", [])


class WorkflowParsingTests(unittest.TestCase):
    def test_every_workflow_parses(self) -> None:
        for path in sorted(WORKFLOWS.glob("*.yml")):
            with self.subTest(workflow=path.name):
                document = yaml.safe_load(path.read_text(encoding="utf-8"))
                self.assertIsInstance(document, dict)
                self.assertIn("jobs", document)

    def test_governance_workflows_declare_job_permissions_and_timeouts(self) -> None:
        for name in GOVERNANCE_WORKFLOW_NAMES:
            document = load(name)
            for job_name, job in document["jobs"].items():
                with self.subTest(workflow=name, job=job_name):
                    self.assertIsInstance(job.get("permissions"), dict)
                    self.assertIsInstance(job.get("timeout-minutes"), int)

    def test_governance_workflows_never_suppress_failure(self) -> None:
        for name in GOVERNANCE_WORKFLOW_NAMES:
            text = (WORKFLOWS / name).read_text(encoding="utf-8")
            with self.subTest(workflow=name):
                self.assertNotIn("|| true", text)
                self.assertNotIn("continue-on-error", text)


class ReproWorkflowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.document = load("repro-bug.yml")
        self.jobs = self.document["jobs"]

    def test_triggers_are_exact(self) -> None:
        on = triggers(self.document)
        self.assertEqual(set(on), {"issues", "workflow_dispatch"})
        self.assertEqual(on["issues"], {"types": ["labeled"]})
        self.assertEqual(set(on["workflow_dispatch"]["inputs"]), {"issue_number"})
        self.assertEqual(on["workflow_dispatch"]["inputs"]["issue_number"]["type"], "string")

    def test_workflow_permissions_are_empty(self) -> None:
        self.assertEqual(self.document["permissions"], {})

    def test_job_order_and_dependencies(self) -> None:
        self.assertEqual(list(self.jobs), ["authorize", "mark-in-progress", "reproduce", "publish-final"])
        self.assertEqual(self.jobs["mark-in-progress"]["needs"], "authorize")
        self.assertEqual(self.jobs["reproduce"]["needs"], ["authorize", "mark-in-progress"])
        self.assertEqual(self.jobs["publish-final"]["needs"], ["authorize", "mark-in-progress", "reproduce"])

    def test_job_permissions_are_least_privilege(self) -> None:
        self.assertEqual(self.jobs["authorize"]["permissions"], {"contents": "read", "issues": "read"})
        self.assertEqual(self.jobs["mark-in-progress"]["permissions"], {"issues": "write"})
        self.assertEqual(self.jobs["reproduce"]["permissions"], {"contents": "read", "issues": "read"})
        self.assertEqual(self.jobs["publish-final"]["permissions"], {"issues": "write"})

    def test_concurrency_cancels_previous_runs_for_the_same_request(self) -> None:
        concurrency = self.document["concurrency"]
        self.assertTrue(concurrency["cancel-in-progress"])
        self.assertEqual(
            concurrency["group"],
            "repro-bug-${{ github.event.issue.number || inputs.issue_number }}"
            "-${{ github.event.label.name || 'workflow_dispatch' }}",
        )
        for volatile in ("run_id", "run_attempt", "updated_at"):
            self.assertNotIn(volatile, concurrency["group"])

    def test_authorization_gate_is_exact(self) -> None:
        condition = self.jobs["authorize"]["if"]
        self.assertIn("github.event.label.name == 'repro-requested'", condition)
        self.assertIn("github.actor != 'dependabot[bot]'", condition)
        self.assertIn("github.event.sender.login != 'dependabot[bot]'", condition)

    def test_authorize_job_does_not_check_out_or_install(self) -> None:
        for step in steps(self.jobs["authorize"]):
            self.assertNotIn("actions/checkout", step.get("uses", ""))
            self.assertNotIn("pip install", step.get("run", ""))

    def test_authorize_uploads_decision_even_when_denied(self) -> None:
        upload = [step for step in steps(self.jobs["authorize"]) if "upload-artifact" in step.get("uses", "")]
        self.assertEqual(len(upload), 1)
        self.assertEqual(upload[0]["if"], "always()")
        self.assertEqual(upload[0]["with"]["retention-days"], 14)

    def test_authorize_enforces_before_reading_issue_metadata(self) -> None:
        names = [step.get("name", "") for step in steps(self.jobs["authorize"])]
        self.assertLess(names.index("Enforce authorization decision"), names.index("Resolve authorized issue metadata"))

    def test_reproduce_checks_out_the_trusted_revision(self) -> None:
        checkout = [step for step in steps(self.jobs["reproduce"]) if "actions/checkout" in step.get("uses", "")]
        self.assertEqual(len(checkout), 1)
        self.assertEqual(checkout[0]["with"]["ref"], "${{ needs.authorize.outputs.trusted_sha }}")
        self.assertFalse(checkout[0]["with"]["persist-credentials"])

    def test_reproduce_verifies_the_authorized_revision(self) -> None:
        run_bodies = "\n".join(step.get("run", "") for step in steps(self.jobs["reproduce"]))
        self.assertIn("--expected-updated-at", run_bodies)
        self.assertIn("--expected-body-digest", run_bodies)
        self.assertIn("git rev-parse HEAD", run_bodies)

    def test_reproduce_uploads_evidence_with_retention(self) -> None:
        upload = [step for step in steps(self.jobs["reproduce"]) if "upload-artifact" in step.get("uses", "")]
        self.assertEqual(len(upload), 1)
        self.assertEqual(upload[0]["with"]["retention-days"], 14)
        self.assertEqual(upload[0]["with"]["if-no-files-found"], "error")

    def test_publish_final_always_runs_after_authorization(self) -> None:
        condition = self.jobs["publish-final"]["if"]
        self.assertTrue(condition.startswith("always()"))
        self.assertIn("needs.authorize.result == 'success'", condition)

    def test_publish_final_downloads_evidence_only_on_success(self) -> None:
        download = [step for step in steps(self.jobs["publish-final"]) if "download-artifact" in step.get("uses", "")]
        self.assertEqual(len(download), 1)
        self.assertEqual(download[0]["if"], "needs.reproduce.result == 'success'")

    def test_publish_final_reconciles_every_governed_label(self) -> None:
        catalog = json.loads((GOVERNANCE / "repro-label-catalog.json").read_text(encoding="utf-8"))
        body = "\n".join(step.get("run", "") for step in steps(self.jobs["publish-final"]))
        for label in catalog["labels"]:
            self.assertIn(label["name"], body)

    def test_publish_final_maps_status_to_catalog_label(self) -> None:
        catalog = json.loads((GOVERNANCE / "repro-label-catalog.json").read_text(encoding="utf-8"))
        mapping = {label["outcome"]: label["name"] for label in catalog["labels"] if label["outcome"]}
        body = "\n".join(step.get("run", "") for step in steps(self.jobs["publish-final"]))
        for outcome, label in mapping.items():
            self.assertIn(f'"{outcome}": "{label}"', body)

    def test_publish_final_publishes_codes_not_reporter_text(self) -> None:
        body = "\n".join(step.get("run", "") for step in steps(self.jobs["publish-final"]))
        self.assertIn('evidence["result"]["reason_code"]', body)
        self.assertNotIn('evidence["result"]["reason"]', body)
        self.assertIn('re.fullmatch(r"[A-Z][A-Z0-9_]{2,63}", reason_code)', body)

    def test_no_step_interpolates_issue_content(self) -> None:
        for job_name, job in self.jobs.items():
            for step in steps(job):
                with self.subTest(job=job_name, step=step.get("name")):
                    self.assertNotIn("github.event.issue.body", step.get("run", ""))
                    self.assertNotIn("github.event.issue.title", step.get("run", ""))

    def test_mark_in_progress_clears_contradictory_labels(self) -> None:
        body = "\n".join(step.get("run", "") for step in steps(self.jobs["mark-in-progress"]))
        for label in ("repro-requested", "repro-confirmed", "repro-not-reproduced", "repro-blocked", "repro-needs-review"):
            self.assertIn(label, body)
        self.assertIn('{"labels":["repro-in-progress"]}', body)


class LabelReconciliationWorkflowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.document = load("repro-label-reconciliation.yml")
        self.jobs = self.document["jobs"]

    def test_manual_dispatch_only(self) -> None:
        on = triggers(self.document)
        self.assertEqual(set(on), {"workflow_dispatch"})
        self.assertIsNone(on["workflow_dispatch"])

    def test_permissions_are_least_privilege(self) -> None:
        self.assertEqual(self.document["permissions"], {})
        self.assertEqual(self.jobs["authorize"]["permissions"], {})
        self.assertEqual(self.jobs["reconcile"]["permissions"], {"contents": "read", "issues": "write"})

    def test_reconcile_requires_authorization_first(self) -> None:
        self.assertEqual(self.jobs["reconcile"]["needs"], "authorize")
        for step in steps(self.jobs["authorize"]):
            self.assertNotIn("actions/checkout", step.get("uses", ""))

    def test_reconciles_repository_labels_not_issue_state(self) -> None:
        text = (WORKFLOWS / "repro-label-reconciliation.yml").read_text(encoding="utf-8")
        self.assertNotIn("/issues/", text)
        self.assertIn("repro-label-catalog.json", text)
        self.assertIn("/labels", text)

    def test_creates_and_updates_labels_idempotently(self) -> None:
        body = "\n".join(step.get("run", "") for step in steps(self.jobs["reconcile"]))
        self.assertIn("api GET", body)
        self.assertIn("api PATCH", body)
        self.assertIn("api POST", body)


class IntakeWorkflowTests(unittest.TestCase):
    def test_intake_workflows_are_read_only(self) -> None:
        for name in ("issue-intake.yml", "dependabot-intake.yml", "dependabot-auto-merge.yml", "governance-observe.yml"):
            document = load(name)
            for job_name, job in document["jobs"].items():
                with self.subTest(workflow=name, job=job_name):
                    self.assertNotIn("write", set(job["permissions"].values()))

    def test_dependabot_intake_never_checks_out_pr_head(self) -> None:
        document = load("dependabot-intake.yml")
        checkout = [
            step
            for step in steps(document["jobs"]["normalize"])
            if "actions/checkout" in step.get("uses", "")
        ]
        self.assertEqual(len(checkout), 1)
        self.assertIn("base.sha", checkout[0]["with"]["ref"])
        self.assertFalse(checkout[0]["with"]["persist-credentials"])


class ContractParityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.contracts = json.loads((GOVERNANCE / "workflow-contracts.json").read_text(encoding="utf-8"))["contracts"]

    def _contract(self, contract_id: str) -> dict:
        for contract in self.contracts:
            if contract["id"] == contract_id:
                return contract
        raise AssertionError(f"contract {contract_id} is missing")

    def test_repro_contract_matches_workflow(self) -> None:
        contract = self._contract("guarded-bug-reproduction")
        document = load("repro-bug.yml")
        declared = set()
        for job in document["jobs"].values():
            declared.update(f"{key}:{value}" for key, value in job["permissions"].items())
        self.assertEqual(set(contract["permissions"]), declared)
        self.assertEqual(contract["concurrency"], "repro-bug-${issue_number}-${requesting_label_or_dispatch}")
        self.assertEqual(contract["artifact_retention_days"], 14)
        self.assertTrue(contract["mutations"])
        self.assertFalse(contract["copilot"]["enabled"])

    def test_reconciliation_contract_matches_workflow(self) -> None:
        contract = self._contract("repro-label-reconciliation")
        self.assertEqual(contract["events"], ["workflow_dispatch"])
        self.assertEqual(set(contract["permissions"]), {"contents:read", "issues:write"})
        self.assertFalse(contract["copilot"]["enabled"])

    def test_no_contract_enables_copilot(self) -> None:
        for contract in self.contracts:
            with self.subTest(contract=contract["id"]):
                self.assertFalse(contract.get("copilot", {}).get("enabled", False))
                self.assertEqual(contract["copilot"]["aic_source"], "github-copilot-platform")


if __name__ == "__main__":
    unittest.main()
