from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from tools import governance_authorize


def request(**overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "repository": "clemensv/avrotize",
        "event_name": "issues",
        "action": "labeled",
        "label_name": "repro-requested",
        "sender_login": "maintainer",
        "actor": "maintainer",
        "triggering_actor": "maintainer",
        "run_attempt": 1,
        "run_id": "123",
        "issue_number_event": 42,
        "permission_response": {
            "http_status": 200,
            "body": {"permission": "write", "role_name": "maintain"},
        },
    }
    value.update(overrides)
    return value


class AuthorizationTests(unittest.TestCase):
    def test_maintain_and_admin_are_allowed(self) -> None:
        for role in ("maintain", "admin"):
            with self.subTest(role=role):
                value = request(
                    permission_response={
                        "http_status": 200,
                        "body": {"permission": "write", "role_name": role},
                    }
                )
                record = governance_authorize.evaluate(value)
                self.assertEqual(record["decision"], "ALLOW")
                self.assertTrue(record["actor_authorized"])

    def test_read_triage_write_are_denied(self) -> None:
        for role in ("read", "triage", "write"):
            with self.subTest(role=role):
                value = request(
                    permission_response={
                        "http_status": 200,
                        "body": {"permission": role, "role_name": role},
                    }
                )
                record = governance_authorize.evaluate(value)
                self.assertEqual(record["decision"], "DENY")
                self.assertEqual(record["reason_code"], "PERMISSION_INSUFFICIENT")

    def test_legacy_write_without_role_is_denied(self) -> None:
        value = request(
            permission_response={"http_status": 200, "body": {"permission": "write"}}
        )
        self.assertEqual(governance_authorize.evaluate(value)["decision"], "DENY")

    def test_api_failure_is_error(self) -> None:
        for status in (0, 403, 500):
            with self.subTest(status=status):
                record = governance_authorize.evaluate(
                    request(permission_response={"http_status": status, "body": {}})
                )
                self.assertEqual(record["decision"], "ERROR")

    def test_404_is_denied(self) -> None:
        record = governance_authorize.evaluate(
            request(permission_response={"http_status": 404, "body": {}})
        )
        self.assertEqual(record["decision"], "DENY")
        self.assertEqual(record["reason_code"], "PERMISSION_NOT_A_COLLABORATOR")

    def test_only_exact_label_event_is_eligible(self) -> None:
        cases = [
            ("event_name", "workflow_dispatch", "EVENT_NOT_ELIGIBLE"),
            ("action", "opened", "ACTION_NOT_ELIGIBLE"),
            ("label_name", "Repro-Requested", "LABEL_NOT_ELIGIBLE"),
            ("label_name", "repro-requested ", "LABEL_NOT_ELIGIBLE"),
        ]
        for key, value, reason in cases:
            with self.subTest(key=key, value=value):
                record = governance_authorize.evaluate(request(**{key: value}))
                self.assertEqual(record["decision"], "DENY")
                self.assertEqual(record["reason_code"], reason)
                self.assertFalse(record["permission"]["evaluated"])

    def test_actor_ambiguity_and_rerun_mismatch_fail_closed(self) -> None:
        ambiguous = governance_authorize.evaluate(request(actor="different"))
        self.assertEqual(ambiguous["reason_code"], "ACTOR_AMBIGUOUS")
        mismatch = governance_authorize.evaluate(request(run_attempt=2, triggering_actor="admin"))
        self.assertEqual(mismatch["reason_code"], "RERUN_ACTOR_MISMATCH")
        missing = governance_authorize.evaluate(request(triggering_actor=""))
        self.assertEqual(missing["reason_code"], "TRIGGERING_ACTOR_MISSING")

    def test_invalid_issue_number_is_denied(self) -> None:
        for value in (0, -1, True, "abc", None):
            with self.subTest(value=value):
                record = governance_authorize.evaluate(request(issue_number_event=value))
                self.assertEqual(record["reason_code"], "ISSUE_NUMBER_INVALID")


class SnapshotTests(unittest.TestCase):
    def setUp(self) -> None:
        self.issue = {
            "number": 42,
            "title": "a2p recursive record failure",
            "body": "### Command or API\navrotize a2p",
            "updated_at": "2026-01-01T00:00:00Z",
            "html_url": "https://github.com/clemensv/avrotize/issues/42",
            "state": "open",
            "labels": [{"name": "repro-requested"}],
        }
        self.snapshot = governance_authorize.build_snapshot(
            self.issue, 42, "clemensv/avrotize"
        )

    def test_snapshot_binds_repository_number_title_and_body(self) -> None:
        canonical = json.dumps(
            {
                "repository": "clemensv/avrotize",
                "issue_number": 42,
                "title": self.issue["title"],
                "body": self.issue["body"],
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        self.assertEqual(
            self.snapshot["content_digest"],
            hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
        )

    def test_label_timestamp_and_comment_mutations_do_not_invalidate(self) -> None:
        current = dict(self.issue)
        current["updated_at"] = "2026-01-01T01:00:00Z"
        current["labels"] = [{"name": "repro-in-progress"}]
        current["comments"] = 5
        result = governance_authorize.verify_snapshot(current, self.snapshot)
        self.assertTrue(result["matches"])

    def test_title_or_body_edit_invalidates(self) -> None:
        for field, value in (("title", "changed title"), ("body", "changed body")):
            with self.subTest(field=field):
                current = dict(self.issue)
                current[field] = value
                self.assertFalse(
                    governance_authorize.verify_snapshot(current, self.snapshot)["matches"]
                )

    def test_issue_number_repository_and_pull_request_guards(self) -> None:
        with self.assertRaises(governance_authorize.AuthorizationInputError):
            governance_authorize.build_snapshot(self.issue, 41, "clemensv/avrotize")
        with self.assertRaises(governance_authorize.AuthorizationInputError):
            governance_authorize.build_snapshot(self.issue, 42, "")
        with self.assertRaises(governance_authorize.AuthorizationInputError):
            governance_authorize.build_snapshot(
                {**self.issue, "pull_request": {"url": "x"}},
                42,
                "clemensv/avrotize",
            )

    def test_null_title_and_body_hash_empty_string(self) -> None:
        value = governance_authorize.build_snapshot(
            {"number": 42, "title": None, "body": None},
            42,
            "clemensv/avrotize",
        )
        empty = hashlib.sha256(b"").hexdigest()
        self.assertEqual(value["title_digest"], empty)
        self.assertEqual(value["body_digest"], empty)


class CliTests(unittest.TestCase):
    def test_evaluate_exit_codes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "request.json"
            for expected, value in (
                (0, request()),
                (10, request(label_name="wrong")),
                (
                    20,
                    request(permission_response={"http_status": 500, "body": {}}),
                ),
            ):
                with self.subTest(expected=expected):
                    path.write_text(json.dumps(value), encoding="utf-8")
                    self.assertEqual(
                        governance_authorize.main(["evaluate", "--request", str(path)]),
                        expected,
                    )

    def test_snapshot_and_verify_subcommands(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            issue_path = root / "issue.json"
            snapshot_path = root / "snapshot.json"
            verify_path = root / "verify.json"
            issue_path.write_text(
                json.dumps({"number": 7, "title": "x", "body": "y"}),
                encoding="utf-8",
            )
            self.assertEqual(
                governance_authorize.main(
                    [
                        "snapshot",
                        "--issue",
                        str(issue_path),
                        "--expected-issue-number",
                        "7",
                        "--repository",
                        "clemensv/avrotize",
                        "--output-json",
                        str(snapshot_path),
                    ]
                ),
                0,
            )
            self.assertEqual(
                governance_authorize.main(
                    [
                        "verify",
                        "--issue",
                        str(issue_path),
                        "--snapshot",
                        str(snapshot_path),
                        "--output-json",
                        str(verify_path),
                    ]
                ),
                0,
            )
            self.assertTrue(json.loads(verify_path.read_text())["matches"])


if __name__ == "__main__":
    unittest.main()
