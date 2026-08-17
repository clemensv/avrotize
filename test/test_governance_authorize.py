"""Tests for deterministic guarded-reproduction authorization."""

from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from tools import governance_authorize, governance_schema

SCHEMA = Path(__file__).resolve().parent.parent / ".github" / "governance" / "schemas" / "repro-authorization-record.schema.json"


def _label_request(**overrides):
    request = {
        "repository": "clemensv/avrotize",
        "event_name": "issues",
        "action": "labeled",
        "label_name": "repro-requested",
        "sender_login": "maintainer",
        "actor": "maintainer",
        "triggering_actor": "maintainer",
        "run_attempt": 1,
        "run_id": "12345",
        "issue_number_event": 42,
        "issue_number_input": "",
        "permission_response": {
            "http_status": 200,
            "body": {"permission": "write", "role_name": "maintain"},
        },
    }
    request.update(overrides)
    return request


def _dispatch_request(**overrides):
    request = {
        "repository": "clemensv/avrotize",
        "event_name": "workflow_dispatch",
        "action": "",
        "label_name": "",
        "sender_login": "",
        "actor": "maintainer",
        "triggering_actor": "maintainer",
        "run_attempt": 1,
        "run_id": "999",
        "issue_number_event": "",
        "issue_number_input": "77",
        "permission_response": {
            "http_status": 200,
            "body": {"permission": "admin", "role_name": "admin"},
        },
    }
    request.update(overrides)
    return request


class PermissionLevelTests(unittest.TestCase):
    def test_maintain_is_authorized(self) -> None:
        record = governance_authorize.evaluate(_label_request())
        self.assertEqual(record["decision"], "ALLOW")
        self.assertEqual(record["reason_code"], "AUTHORIZED")
        self.assertTrue(record["actor_authorized"])

    def test_admin_is_authorized(self) -> None:
        record = governance_authorize.evaluate(
            _label_request(permission_response={"http_status": 200, "body": {"permission": "admin"}})
        )
        self.assertEqual(record["decision"], "ALLOW")

    def test_write_is_denied(self) -> None:
        record = governance_authorize.evaluate(
            _label_request(permission_response={"http_status": 200, "body": {"permission": "write"}})
        )
        self.assertEqual(record["decision"], "DENY")
        self.assertEqual(record["reason_code"], "PERMISSION_INSUFFICIENT")

    def test_triage_is_denied(self) -> None:
        record = governance_authorize.evaluate(
            _label_request(permission_response={"http_status": 200, "body": {"permission": "triage"}})
        )
        self.assertEqual(record["reason_code"], "PERMISSION_INSUFFICIENT")

    def test_read_is_denied(self) -> None:
        record = governance_authorize.evaluate(
            _label_request(permission_response={"http_status": 200, "body": {"permission": "read"}})
        )
        self.assertEqual(record["reason_code"], "PERMISSION_INSUFFICIENT")

    def test_none_permission_is_denied(self) -> None:
        record = governance_authorize.evaluate(
            _label_request(permission_response={"http_status": 200, "body": {"permission": "none"}})
        )
        self.assertEqual(record["decision"], "DENY")

    def test_not_a_collaborator_is_denied(self) -> None:
        record = governance_authorize.evaluate(
            _label_request(permission_response={"http_status": 404, "body": {"message": "Not Found"}})
        )
        self.assertEqual(record["decision"], "DENY")
        self.assertEqual(record["reason_code"], "PERMISSION_NOT_A_COLLABORATOR")

    def test_server_error_is_infrastructure_error(self) -> None:
        record = governance_authorize.evaluate(
            _label_request(permission_response={"http_status": 500, "body": {}})
        )
        self.assertEqual(record["decision"], "ERROR")
        self.assertEqual(record["reason_code"], "PERMISSION_API_ERROR")

    def test_transport_failure_is_infrastructure_error(self) -> None:
        record = governance_authorize.evaluate(
            _label_request(permission_response={"http_status": 0, "body": None})
        )
        self.assertEqual(record["decision"], "ERROR")

    def test_forbidden_is_infrastructure_error(self) -> None:
        record = governance_authorize.evaluate(
            _label_request(permission_response={"http_status": 403, "body": {"message": "Forbidden"}})
        )
        self.assertEqual(record["decision"], "ERROR")
        self.assertEqual(record["reason_code"], "PERMISSION_API_ERROR")

    def test_malformed_body_is_infrastructure_error(self) -> None:
        record = governance_authorize.evaluate(
            _label_request(permission_response={"http_status": 200, "body": {}})
        )
        self.assertEqual(record["decision"], "ERROR")
        self.assertEqual(record["reason_code"], "PERMISSION_RESPONSE_MALFORMED")

    def test_role_name_maintain_is_authorized(self) -> None:
        record = governance_authorize.evaluate(
            _label_request(
                permission_response={
                    "http_status": 200,
                    "body": {"permission": "write", "role_name": "maintain"},
                }
            )
        )
        self.assertEqual(record["decision"], "ALLOW")
        self.assertEqual(record["permission"]["level"], "maintain")

    def test_plain_write_collaborator_is_denied(self) -> None:
        record = governance_authorize.evaluate(
            _label_request(
                permission_response={
                    "http_status": 200,
                    "body": {"permission": "write", "role_name": "write"},
                }
            )
        )
        self.assertEqual(record["decision"], "DENY")
        self.assertEqual(record["reason_code"], "PERMISSION_INSUFFICIENT")

    def test_custom_role_is_denied(self) -> None:
        record = governance_authorize.evaluate(
            _label_request(
                permission_response={
                    "http_status": 200,
                    "body": {"permission": "write", "role_name": "custom-elevated"},
                }
            )
        )
        self.assertEqual(record["decision"], "DENY")
        self.assertEqual(record["reason_code"], "PERMISSION_INSUFFICIENT")

    def test_write_without_role_name_is_denied(self) -> None:
        record = governance_authorize.evaluate(
            _label_request(permission_response={"http_status": 200, "body": {"permission": "write"}})
        )
        self.assertEqual(record["decision"], "DENY")
        self.assertEqual(record["reason_code"], "PERMISSION_INSUFFICIENT")

    def test_admin_without_role_name_is_authorized(self) -> None:
        record = governance_authorize.evaluate(
            _label_request(permission_response={"http_status": 200, "body": {"permission": "admin"}})
        )
        self.assertEqual(record["decision"], "ALLOW")

    def test_missing_response_is_infrastructure_error(self) -> None:
        record = governance_authorize.evaluate(_label_request(permission_response=None))
        self.assertEqual(record["decision"], "ERROR")
        self.assertEqual(record["reason_code"], "PERMISSION_RESPONSE_MISSING")

    def test_permission_case_is_normalized(self) -> None:
        record = governance_authorize.evaluate(
            _label_request(permission_response={"http_status": 200, "body": {"permission": "  Admin "}})
        )
        self.assertEqual(record["decision"], "ALLOW")
        self.assertEqual(record["permission"]["level"], "admin")


class EventEligibilityTests(unittest.TestCase):
    def test_wrong_label_is_denied_without_permission_check(self) -> None:
        record = governance_authorize.evaluate(_label_request(label_name="bug"))
        self.assertEqual(record["reason_code"], "LABEL_NOT_ELIGIBLE")
        self.assertFalse(record["permission"]["evaluated"])

    def test_label_prefix_is_not_accepted(self) -> None:
        record = governance_authorize.evaluate(_label_request(label_name="repro-requested-urgent"))
        self.assertEqual(record["reason_code"], "LABEL_NOT_ELIGIBLE")

    def test_non_labeled_action_is_denied(self) -> None:
        record = governance_authorize.evaluate(_label_request(action="opened"))
        self.assertEqual(record["reason_code"], "ACTION_NOT_ELIGIBLE")

    def test_unrelated_event_is_denied(self) -> None:
        record = governance_authorize.evaluate(_label_request(event_name="push"))
        self.assertEqual(record["reason_code"], "EVENT_NOT_ELIGIBLE")

    def test_empty_actor_is_denied(self) -> None:
        record = governance_authorize.evaluate(_label_request(sender_login="", actor=""))
        self.assertEqual(record["reason_code"], "ACTOR_MISSING")
        self.assertFalse(record["permission"]["evaluated"])

    def test_actor_ambiguity_is_denied(self) -> None:
        record = governance_authorize.evaluate(_label_request(actor="someone-else"))
        self.assertEqual(record["reason_code"], "ACTOR_AMBIGUOUS")

    def test_rerun_by_other_actor_is_denied(self) -> None:
        record = governance_authorize.evaluate(_label_request(run_attempt=2, triggering_actor="other"))
        self.assertEqual(record["reason_code"], "RERUN_ACTOR_MISMATCH")
        self.assertFalse(record["permission"]["evaluated"])

    def test_missing_triggering_actor_is_denied(self) -> None:
        record = governance_authorize.evaluate(_label_request(triggering_actor=""))
        self.assertEqual(record["reason_code"], "TRIGGERING_ACTOR_MISSING")

    def test_label_actor_is_the_event_sender(self) -> None:
        record = governance_authorize.evaluate(_label_request(sender_login="labeler", actor="labeler", triggering_actor="labeler"))
        self.assertEqual(record["request"]["actor"], "labeler")

    def test_missing_issue_number_on_label_event_is_denied(self) -> None:
        record = governance_authorize.evaluate(_label_request(issue_number_event=None))
        self.assertEqual(record["reason_code"], "ISSUE_NUMBER_INVALID")


class DispatchTests(unittest.TestCase):
    def test_numeric_issue_is_authorized(self) -> None:
        record = governance_authorize.evaluate(_dispatch_request())
        self.assertEqual(record["decision"], "ALLOW")
        self.assertEqual(record["request"]["issue_number"], 77)

    def test_dispatch_actor_is_workflow_actor(self) -> None:
        record = governance_authorize.evaluate(_dispatch_request(sender_login="ignored"))
        self.assertEqual(record["request"]["actor"], "maintainer")

    def test_non_numeric_issue_is_denied(self) -> None:
        record = governance_authorize.evaluate(_dispatch_request(issue_number_input="42; rm -rf /"))
        self.assertEqual(record["reason_code"], "ISSUE_NUMBER_INVALID")

    def test_empty_issue_is_denied(self) -> None:
        record = governance_authorize.evaluate(_dispatch_request(issue_number_input=""))
        self.assertEqual(record["reason_code"], "ISSUE_NUMBER_INVALID")

    def test_zero_issue_is_denied(self) -> None:
        record = governance_authorize.evaluate(_dispatch_request(issue_number_input="0"))
        self.assertEqual(record["reason_code"], "ISSUE_NUMBER_INVALID")

    def test_negative_issue_is_denied(self) -> None:
        record = governance_authorize.evaluate(_dispatch_request(issue_number_input="-5"))
        self.assertEqual(record["reason_code"], "ISSUE_NUMBER_INVALID")

    def test_dispatch_rerun_mismatch_is_denied(self) -> None:
        record = governance_authorize.evaluate(_dispatch_request(triggering_actor="other"))
        self.assertEqual(record["reason_code"], "RERUN_ACTOR_MISMATCH")


class RecordShapeTests(unittest.TestCase):
    def test_record_validates_against_schema(self) -> None:
        for request in (_label_request(), _dispatch_request(), _label_request(label_name="bug")):
            with self.subTest(request=request["event_name"] + request["label_name"]):
                record = governance_authorize.evaluate(request)
                governance_schema.validate_or_raise(record, SCHEMA, "authorization record")

    def test_authority_is_never_granted(self) -> None:
        record = governance_authorize.evaluate(_label_request())
        self.assertFalse(record["authority"]["authorized"])
        self.assertIn("does not", record["authority"]["statement"])

    def test_summary_is_deterministic(self) -> None:
        record = governance_authorize.evaluate(_label_request())
        self.assertEqual(governance_authorize.render_summary(record), governance_authorize.render_summary(record))
        self.assertIn("Decision", governance_authorize.render_summary(record))

    def test_denied_record_records_no_issue_content(self) -> None:
        record = governance_authorize.evaluate(_label_request(label_name="bug"))
        self.assertNotIn("body", json.dumps(record))


class MetadataTests(unittest.TestCase):
    def test_body_digest_matches_sha256(self) -> None:
        issue = {"number": 42, "body": "Line one\r\nLine two", "updated_at": "2026-08-17T10:00:00Z", "html_url": "u", "state": "open"}
        metadata = governance_authorize.build_metadata(issue, 42)
        self.assertEqual(metadata["issue_body_digest"], hashlib.sha256("Line one\r\nLine two".encode("utf-8")).hexdigest())
        self.assertEqual(metadata["issue_updated_at"], "2026-08-17T10:00:00Z")

    def test_null_body_hashes_empty_string(self) -> None:
        metadata = governance_authorize.build_metadata({"number": 1, "body": None}, 1)
        self.assertEqual(metadata["issue_body_digest"], hashlib.sha256(b"").hexdigest())

    def test_pull_request_is_rejected(self) -> None:
        with self.assertRaises(governance_authorize.AuthorizationInputError):
            governance_authorize.build_metadata({"number": 1, "pull_request": {"url": "x"}}, 1)

    def test_issue_number_mismatch_is_rejected(self) -> None:
        with self.assertRaises(governance_authorize.AuthorizationInputError):
            governance_authorize.build_metadata({"number": 2}, 1)


class CliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory(dir=str(Path(__file__).resolve().parent))
        self.addCleanup(self.temporary_directory.cleanup)
        self.root = Path(self.temporary_directory.name)

    def _run(self, request) -> tuple[int, Path]:
        request_path = self.root / "request.json"
        record_path = self.root / "authorization.json"
        summary_path = self.root / "authorization.md"
        request_path.write_text(json.dumps(request), encoding="utf-8")
        code = governance_authorize.main(
            [
                "evaluate",
                "--request",
                str(request_path),
                "--output-json",
                str(record_path),
                "--output-markdown",
                str(summary_path),
            ]
        )
        return code, record_path

    def test_allow_exits_zero(self) -> None:
        code, record_path = self._run(_label_request())
        self.assertEqual(code, governance_authorize.EXIT_ALLOW)
        self.assertEqual(json.loads(record_path.read_text(encoding="utf-8"))["decision"], "ALLOW")

    def test_deny_exits_ten_and_still_writes_evidence(self) -> None:
        code, record_path = self._run(_label_request(label_name="bug"))
        self.assertEqual(code, governance_authorize.EXIT_DENY)
        self.assertTrue(record_path.is_file())
        self.assertEqual(json.loads(record_path.read_text(encoding="utf-8"))["decision"], "DENY")

    def test_api_error_exits_twenty(self) -> None:
        code, _ = self._run(_label_request(permission_response={"http_status": 502, "body": {}}))
        self.assertEqual(code, governance_authorize.EXIT_ERROR)

    def test_corrupt_request_exits_twenty(self) -> None:
        request_path = self.root / "request.json"
        request_path.write_text("{not json", encoding="utf-8")
        code = governance_authorize.main(["evaluate", "--request", str(request_path)])
        self.assertEqual(code, governance_authorize.EXIT_ERROR)

    def test_metadata_subcommand_writes_facts(self) -> None:
        issue_path = self.root / "issue.json"
        output_path = self.root / "metadata.json"
        issue_path.write_text(
            json.dumps({"number": 7, "body": "text", "updated_at": "t", "html_url": "u", "state": "open"}),
            encoding="utf-8",
        )
        code = governance_authorize.main(
            ["metadata", "--issue", str(issue_path), "--expected-issue-number", "7", "--output-json", str(output_path)]
        )
        self.assertEqual(code, 0)
        metadata = json.loads(output_path.read_text(encoding="utf-8"))
        self.assertEqual(metadata["issue_number"], 7)
        self.assertEqual(len(metadata["issue_body_digest"]), 64)


if __name__ == "__main__":
    unittest.main()
