"""Comprehensive tests for the guarded bug reproduction engine."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from tools import governance_repro, governance_schema

REPO_ROOT = Path(__file__).resolve().parent.parent
EVIDENCE_SCHEMA = REPO_ROOT / ".github" / "governance" / "schemas" / "repro-evidence-record.schema.json"

FENCED_FIXTURE = '```json\n{"type": "record", "name": "X", "namespace": "n", "fields": []}\n```'


def build_body(
    *,
    surface: str = "Avrotize CLI",
    command: str = "avrotize a2j",
    invocation: str = "avrotize a2j schema.avsc --out result.json",
    fence_invocation: bool = True,
    fixture: str = FENCED_FIXTURE,
    expected_result: str = "Successful completion (exit 0)",
    expected_output: str = "_No response_",
    version: str = "3.9.0",
    actual: str = "Traceback (most recent call last): ValueError",
    expected: str = "A JSON schema should be written",
) -> str:
    # The bug form declares `render: shell` on the invocation textarea, so GitHub
    # always writes that section as a fenced block. Model that faithfully.
    rendered_invocation = f"```shell\n{invocation}\n```" if fence_invocation else invocation
    return "\n".join(
        [
            "### Avrotize or Structurize version",
            version,
            "### Where did you run it?",
            surface,
            "### Exact command, Python function, MCP tool, or VS Code action",
            command,
            "### Invocation and flags",
            rendered_invocation,
            "### Input kind, dialect, and minimal input",
            fixture,
            "### Output kind, language, and runtime",
            "JSON Schema draft-07",
            "### Actual output or error",
            actual,
            "### Expected output or behavior",
            expected,
            "### Expected command result",
            expected_result,
            "### Exact expected output",
            expected_output,
            "### Environment and toolchain",
            "Ubuntu 24.04, Python 3.12",
            "### Compatibility regression",
            "Worked in 3.8.0",
            # GitHub renders the checkboxes field as a heading plus a task list.
            "### Confirmation",
            "- [X] I supplied a minimal non-sensitive reproducer.\n- [X] I searched for an existing report of this behavior.",
        ]
    )


def build_issue(body: str | None = None, **overrides) -> dict:
    issue = {
        "number": 42,
        "title": "[Bug] a2j conversion fails",
        "body": body if body is not None else build_body(),
        "updated_at": "2026-08-17T10:00:00Z",
        "html_url": "https://github.com/clemensv/avrotize/issues/42",
        "state": "open",
    }
    issue.update(overrides)
    return issue


def build_authorization(**overrides) -> dict:
    record = {
        "decision": "ALLOW",
        "request": {
            "actor": "maintainer",
            "event_name": "issues",
            "label_name": "repro-requested",
        },
    }
    record.update(overrides)
    return record


class FakeProcess:
    """Stand-in for a spawned child process. No real command is ever executed."""

    pid = 31337

    def __init__(self, plan, cwd, stdout_handle, stderr_handle):
        self._plan = plan
        self._cwd = Path(cwd)
        self._stdout = stdout_handle
        self._stderr = stderr_handle
        self._waits = 0
        self.killed = False

    def _emit(self) -> None:
        self._stdout.write(self._plan.get("stdout", b""))
        self._stderr.write(self._plan.get("stderr", b""))
        self._stdout.flush()
        self._stderr.flush()
        for name, content in self._plan.get("files", {}).items():
            target = self._cwd / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
        for link_name, link_target in self._plan.get("symlinks", {}).items():
            try:
                (self._cwd / link_name).symlink_to(link_target)
            except (OSError, NotImplementedError):
                self._plan["symlink_unavailable"] = True

    def wait(self, timeout=None):
        self._waits += 1
        if self._plan.get("timeout"):
            if self._waits == 1:
                raise subprocess.TimeoutExpired(cmd="avrotize", timeout=timeout or 0)
            return -9
        self._emit()
        return self._plan.get("returncode", 0)

    def kill(self):
        self.killed = True


class FakeSpawn:
    def __init__(self, plan):
        self.plan = plan
        self.argv: list[str] = []
        self.env: dict[str, str] = {}
        self.cwd = ""

    def __call__(self, argv, cwd, env, stdout_handle, stderr_handle):
        self.argv = list(argv)
        self.env = dict(env)
        self.cwd = str(cwd)
        return FakeProcess(self.plan, cwd, stdout_handle, stderr_handle)


class SummarySafeTests(unittest.TestCase):
    """Reporter-derived fragments must not be able to shape summary Markdown."""

    def test_markdown_control_characters_are_neutralized(self) -> None:
        rendered = governance_repro.summary_safe("a2j``` | **x** <img src=x>\n\n## Heading\n> quote")
        self.assertNotIn("`", rendered)
        self.assertNotIn("|", rendered)
        self.assertNotIn("<", rendered)
        self.assertNotIn("\n", rendered)

    def test_control_characters_are_stripped(self) -> None:
        self.assertEqual(governance_repro.summary_safe("a\x00b\x1bc"), "a b c")

    def test_long_fragment_is_truncated(self) -> None:
        rendered = governance_repro.summary_safe("x" * 400)
        self.assertTrue(rendered.endswith("..."))
        self.assertLessEqual(len(rendered), governance_repro.MAX_SUMMARY_FRAGMENT + 3)

    def test_ordinary_reason_text_is_readable(self) -> None:
        self.assertEqual(
            governance_repro.summary_safe("option '--bogus' is not declared for a2j"),
            "option '--bogus' is not declared for a2j",
        )


class ReproTestCase(unittest.TestCase):
    """Base case wiring a temporary workspace and a fake process spawner."""

    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory(dir=str(Path(__file__).resolve().parent))
        self.addCleanup(self.temporary_directory.cleanup)
        self.workspace_root = Path(self.temporary_directory.name)
        original_popen = governance_repro._popen
        self.addCleanup(setattr, governance_repro, "_popen", original_popen)

    def run_engine(self, issue=None, authorization=None, plan=None, **option_overrides):
        plan = plan if plan is not None else {"returncode": 0}
        spawn = FakeSpawn(plan)
        governance_repro._popen = spawn
        options = {
            "repository": "clemensv/avrotize",
            "avrotize_executable": "/opt/hostedtoolcache/bin/avrotize",
            "avrotize_version": "3.9.0",
            "expected_updated_at": "2026-08-17T10:00:00Z",
            "trusted_sha": "b" * 40,
            "default_branch": "master",
            "run_id": "555",
            "run_attempt": 1,
            "run_url": "https://github.com/clemensv/avrotize/actions/runs/555",
            "workspace_root": str(self.workspace_root),
            "artifact_name": "repro-evidence-42",
            "retention_days": 14,
        }
        options.update(option_overrides)
        record, summary = governance_repro.reproduce(
            issue if issue is not None else build_issue(),
            authorization if authorization is not None else build_authorization(),
            options,
        )
        return record, summary, spawn


class ReadinessAndPolicyTests(ReproTestCase):
    def test_complete_report_is_executed(self) -> None:
        record, _, spawn = self.run_engine()
        self.assertTrue(record["execution"]["executed"])
        self.assertEqual(record["readiness"]["command"], "a2j")
        self.assertEqual(spawn.argv[0], "/opt/hostedtoolcache/bin/avrotize")
        self.assertEqual(spawn.argv[1], "a2j")

    def test_non_bug_form_is_blocked(self) -> None:
        record, _, _ = self.run_engine(issue=build_issue(title="[Feature] add a2j flag"))
        self.assertEqual(record["result"]["status"], "BLOCKED")
        self.assertEqual(record["result"]["reason_code"], "NOT_A_BUG_REPORT")
        self.assertFalse(record["execution"]["executed"])

    def test_incomplete_report_is_blocked(self) -> None:
        body = build_body().replace("Ubuntu 24.04, Python 3.12", "_No response_")
        record, _, _ = self.run_engine(issue=build_issue(body))
        self.assertEqual(record["result"]["reason_code"], "REPORT_NOT_COMPLETE")
        self.assertIn("environment", record["readiness"]["missing_fields"])

    def test_unknown_heading_is_blocked(self) -> None:
        body = build_body() + "\n### Unexpected extra heading\nsomething"
        record, _, _ = self.run_engine(issue=build_issue(body))
        self.assertEqual(record["result"]["status"], "BLOCKED")
        self.assertEqual(record["result"]["reason_code"], "REPORT_NOT_COMPLETE")

    def test_non_cli_surface_is_blocked(self) -> None:
        record, _, _ = self.run_engine(issue=build_issue(build_body(surface="Python API")))
        self.assertEqual(record["result"]["reason_code"], "SURFACE_NOT_ELIGIBLE")

    def test_structurize_surface_is_blocked(self) -> None:
        record, _, _ = self.run_engine(issue=build_issue(build_body(surface="Structurize CLI")))
        self.assertEqual(record["result"]["reason_code"], "SURFACE_NOT_ELIGIBLE")

    def test_command_outside_policy_is_blocked(self) -> None:
        body = build_body(command="avrotize a2k", invocation="avrotize a2k schema.avsc --out out.kql")
        record, _, _ = self.run_engine(issue=build_issue(body))
        self.assertEqual(record["result"]["reason_code"], "COMMAND_NOT_IN_POLICY")

    def test_network_capable_command_is_blocked(self) -> None:
        body = build_body(command="avrotize j2s", invocation="avrotize j2s schema.json --out out.struct.json")
        record, _, _ = self.run_engine(issue=build_issue(body))
        self.assertEqual(record["result"]["reason_code"], "COMMAND_NOT_IN_POLICY")

    def test_unknown_command_is_blocked(self) -> None:
        body = build_body(command="avrotize nope", invocation="avrotize nope schema.avsc")
        record, _, _ = self.run_engine(issue=build_issue(body))
        self.assertEqual(record["result"]["reason_code"], "COMMAND_NOT_IN_POLICY")

    def test_invocation_command_mismatch_is_blocked(self) -> None:
        body = build_body(command="avrotize a2j", invocation="avrotize a2asn schema.avsc")
        record, _, _ = self.run_engine(issue=build_issue(body))
        self.assertEqual(record["result"]["reason_code"], "COMMAND_MISMATCH")

    def test_stdout_only_command_is_allowed(self) -> None:
        body = build_body(
            command="avrotize pcf",
            invocation="avrotize pcf schema.avsc",
            expected_result="Exact output match",
            expected_output="canonical",
        )
        record, _, spawn = self.run_engine(issue=build_issue(body), plan={"returncode": 0, "stdout": b"canonical\n"})
        self.assertEqual(record["result"]["status"], "NOT_REPRODUCED")
        self.assertNotIn("--out", spawn.argv)


class RenderedInvocationTests(ReproTestCase):
    """The bug form renders the invocation as a fenced block; the engine must unwrap it."""

    def test_bug_form_still_renders_the_invocation_as_shell(self) -> None:
        form = (REPO_ROOT / ".github" / "ISSUE_TEMPLATE" / "bug.yml").read_text(encoding="utf-8")
        self.assertIn("id: invocation", form)
        invocation_block = form.split("id: invocation", 1)[1].split("- type:", 1)[0]
        self.assertIn("render: shell", invocation_block)

    def test_github_rendered_fenced_invocation_executes(self) -> None:
        body = build_body(fence_invocation=True)
        self.assertIn("```shell", body)
        self.assertIn("### Confirmation", body)
        record, _, spawn = self.run_engine(issue=build_issue(body))
        self.assertTrue(record["execution"]["executed"])
        self.assertEqual(spawn.argv[1], "a2j")

    def test_rendered_checkbox_section_does_not_block_the_report(self) -> None:
        record, _, _ = self.run_engine(issue=build_issue(build_body()))
        self.assertEqual(record["readiness"]["intake_status"], "complete")
        self.assertTrue(record["readiness"]["eligible"])

    def test_unfenced_invocation_still_executes(self) -> None:
        record, _, _ = self.run_engine(issue=build_issue(build_body(fence_invocation=False)))
        self.assertTrue(record["execution"]["executed"])

    def test_fenced_invocation_options_are_still_policed(self) -> None:
        body = build_body(invocation="avrotize a2j schema.avsc --naming klingon")
        record, _, _ = self.run_engine(issue=build_issue(body))
        self.assertEqual(record["result"]["reason_code"], "OPTION_VALUE_NOT_ALLOWED")

    def test_fenced_command_field_is_unwrapped(self) -> None:
        body = build_body(command="```\navrotize a2j\n```")
        record, _, _ = self.run_engine(issue=build_issue(body))
        self.assertEqual(record["readiness"]["command"], "a2j")
        self.assertTrue(record["execution"]["executed"])

    def test_checked_in_bug_fixture_reaches_command_policy(self) -> None:
        fixture = json.loads(
            (REPO_ROOT / "test" / "fixtures" / "governance" / "issue_bug_complete.json").read_text(encoding="utf-8")
        )
        issue = build_issue(fixture["issue"]["body"], title=fixture["issue"]["title"])
        record, _, _ = self.run_engine(issue=issue)
        self.assertEqual(record["readiness"]["intake_status"], "complete")
        self.assertEqual(record["readiness"]["command"], "a2p")
        self.assertEqual(record["result"]["reason_code"], "COMMAND_NOT_IN_POLICY")


class InvocationSafetyTests(ReproTestCase):
    def _blocked_reason(self, invocation: str, command: str = "avrotize a2j") -> str:
        body = build_body(command=command, invocation=invocation)
        record, _, spawn = self.run_engine(issue=build_issue(body))
        self.assertEqual(record["result"]["status"], "BLOCKED")
        self.assertFalse(record["execution"]["executed"])
        self.assertEqual(spawn.argv, [])
        return record["result"]["reason_code"]

    def test_command_substitution_is_blocked(self) -> None:
        self.assertEqual(
            self._blocked_reason("avrotize a2j schema.avsc --naming $(whoami)"),
            "TOKEN_SHELL_METACHARACTER",
        )

    def test_pipe_is_blocked(self) -> None:
        self.assertEqual(
            self._blocked_reason("avrotize a2j schema.avsc --naming a|b"),
            "TOKEN_SHELL_METACHARACTER",
        )

    def test_backtick_is_blocked(self) -> None:
        self.assertEqual(
            self._blocked_reason("avrotize a2asn schema.avsc --module `id`", command="avrotize a2asn"),
            "TOKEN_SHELL_METACHARACTER",
        )

    def test_semicolon_chain_is_blocked(self) -> None:
        self.assertEqual(
            self._blocked_reason("avrotize a2asn schema.avsc --module x;rm", command="avrotize a2asn"),
            "TOKEN_SHELL_METACHARACTER",
        )

    def test_at_sign_is_blocked(self) -> None:
        self.assertEqual(
            self._blocked_reason("avrotize a2asn schema.avsc --module @args", command="avrotize a2asn"),
            "TOKEN_AT_SIGN",
        )

    def test_url_option_value_is_blocked(self) -> None:
        self.assertEqual(
            self._blocked_reason("avrotize a2asn schema.avsc --module https://evil.example/x", command="avrotize a2asn"),
            "TOKEN_URL",
        )

    def test_absolute_path_option_value_is_blocked(self) -> None:
        self.assertEqual(
            self._blocked_reason("avrotize a2asn schema.avsc --module /etc/passwd", command="avrotize a2asn"),
            "TOKEN_ABSOLUTE_PATH",
        )

    def test_traversal_option_value_is_blocked(self) -> None:
        self.assertEqual(
            self._blocked_reason("avrotize a2asn schema.avsc --module ../../etc/passwd", command="avrotize a2asn"),
            "TOKEN_PATH_REFERENCE",
        )

    def test_relative_path_option_value_is_blocked(self) -> None:
        self.assertEqual(
            self._blocked_reason("avrotize a2asn schema.avsc --module sub/dir", command="avrotize a2asn"),
            "TOKEN_PATH_REFERENCE",
        )

    def test_control_character_in_option_value_is_blocked(self) -> None:
        self.assertEqual(
            self._blocked_reason("avrotize a2asn schema.avsc --module A\x07B", command="avrotize a2asn"),
            "TOKEN_CONTROL_CHARACTER",
        )

    def test_assignment_form_is_blocked(self) -> None:
        self.assertEqual(self._blocked_reason("avrotize a2j schema.avsc --naming=pascal"), "OPTION_ASSIGNMENT_FORM")

    def test_undeclared_option_is_blocked(self) -> None:
        self.assertEqual(self._blocked_reason("avrotize a2j schema.avsc --unknown-flag x"), "OPTION_NOT_ALLOWED")

    def test_short_option_is_blocked(self) -> None:
        self.assertEqual(self._blocked_reason("avrotize a2j schema.avsc -n pascal"), "OPTION_NOT_ALLOWED")

    def test_choice_outside_policy_is_blocked(self) -> None:
        self.assertEqual(self._blocked_reason("avrotize a2j schema.avsc --naming klingon"), "OPTION_VALUE_NOT_ALLOWED")

    def test_string_option_pattern_is_enforced(self) -> None:
        self.assertEqual(
            self._blocked_reason("avrotize a2asn schema.avsc --module 9bad", command="avrotize a2asn"),
            "OPTION_VALUE_NOT_ALLOWED",
        )

    def test_missing_option_value_is_blocked(self) -> None:
        self.assertEqual(self._blocked_reason("avrotize a2j schema.avsc --naming"), "OPTION_VALUE_MISSING")

    def test_valid_string_option_is_accepted(self) -> None:
        body = build_body(command="avrotize a2asn", invocation="avrotize a2asn schema.avsc --module MyModule")
        record, _, spawn = self.run_engine(issue=build_issue(body))
        self.assertTrue(record["execution"]["executed"])
        self.assertIn("--module", spawn.argv)
        self.assertIn("MyModule", spawn.argv)

    def test_multiple_positionals_are_blocked(self) -> None:
        self.assertEqual(self._blocked_reason("avrotize a2j one.avsc two.avsc"), "MULTIPLE_POSITIONAL_ARGUMENTS")

    def test_foreign_executable_is_blocked(self) -> None:
        self.assertEqual(self._blocked_reason("python -m avrotize a2j schema.avsc"), "EXECUTABLE_NOT_ALLOWED")

    def test_structurize_executable_is_blocked(self) -> None:
        self.assertEqual(self._blocked_reason("structurize a2j schema.avsc"), "EXECUTABLE_NOT_ALLOWED")

    def test_unparsable_invocation_is_blocked(self) -> None:
        self.assertEqual(self._blocked_reason('avrotize a2j "unclosed'), "INVOCATION_UNPARSABLE")

    def test_too_many_tokens_is_blocked(self) -> None:
        long_invocation = "avrotize a2j " + " ".join(["--naming pascal"] * 20)
        self.assertEqual(self._blocked_reason(long_invocation), "INVOCATION_TOO_LONG")

    def test_reporter_paths_are_replaced_not_used(self) -> None:
        body = build_body(invocation="avrotize a2j /home/reporter/schema.avsc --out /var/output/result.json")
        record, _, spawn = self.run_engine(issue=build_issue(body))
        self.assertTrue(record["execution"]["executed"])
        self.assertNotIn("/home/reporter/schema.avsc", spawn.argv)
        self.assertNotIn("/var/output/result.json", spawn.argv)
        self.assertEqual(len(record["execution"]["input_substitutions"]), 2)

    def test_deprecated_input_alias_is_replaced(self) -> None:
        body = build_body(invocation="avrotize a2j --avsc reporter.avsc --out out.json")
        record, _, spawn = self.run_engine(issue=build_issue(body))
        self.assertTrue(record["execution"]["executed"])
        self.assertNotIn("--avsc", spawn.argv)
        self.assertNotIn("reporter.avsc", spawn.argv)

    def test_argv_is_recorded_with_workspace_redacted(self) -> None:
        record, _, _ = self.run_engine()
        self.assertTrue(any(part.startswith("<workspace>") for part in record["execution"]["argv"]))
        self.assertFalse(any(str(self.workspace_root) in part for part in record["execution"]["argv"]))
        self.assertEqual(len(record["execution"]["argv_digest"]), 64)


class FixtureTests(ReproTestCase):
    def test_inline_json_with_slashes_and_shell_characters_is_accepted(self) -> None:
        fixture = '```json\n{"$id": "https://example.com/a/b", "doc": "a & b $HOME | x", "type": "record"}\n```'
        record, _, _ = self.run_engine(issue=build_issue(build_body(fixture=fixture)))
        self.assertTrue(record["execution"]["executed"])
        self.assertGreater(record["execution"]["fixture"]["bytes"], 0)

    def test_fixture_digest_matches_content(self) -> None:
        fixture_text = '{"type": "record", "name": "X", "namespace": "n", "fields": []}'
        record, _, _ = self.run_engine()
        self.assertEqual(
            record["execution"]["fixture"]["digest"],
            hashlib.sha256(fixture_text.encode("utf-8")).hexdigest(),
        )

    def test_missing_fixture_is_blocked(self) -> None:
        record, _, _ = self.run_engine(issue=build_issue(build_body(fixture="_No response_")))
        self.assertEqual(record["result"]["reason_code"], "REPORT_NOT_COMPLETE")

    def test_url_only_fixture_is_blocked(self) -> None:
        record, _, _ = self.run_engine(issue=build_issue(build_body(fixture="https://example.com/schema.avsc")))
        self.assertEqual(record["result"]["reason_code"], "FIXTURE_NOT_INLINE")

    def test_filename_only_fixture_is_blocked(self) -> None:
        record, _, _ = self.run_engine(issue=build_issue(build_body(fixture="schema.avsc")))
        self.assertEqual(record["result"]["reason_code"], "FIXTURE_NOT_INLINE")

    def test_attachment_reference_is_blocked(self) -> None:
        record, _, _ = self.run_engine(issue=build_issue(build_body(fixture="see attached file")))
        self.assertEqual(record["result"]["reason_code"], "FIXTURE_NOT_INLINE")

    def test_oversized_fixture_is_blocked(self) -> None:
        payload = "x" * (64 * 1024 + 10)
        record, _, _ = self.run_engine(issue=build_issue(build_body(fixture=f"```\n{payload}\n```")))
        self.assertEqual(record["result"]["reason_code"], "FIXTURE_TOO_LARGE")

    def test_control_character_fixture_is_blocked(self) -> None:
        record, _, _ = self.run_engine(issue=build_issue(build_body(fixture='```\n{"a": "\x07bell"}\n```')))
        self.assertEqual(record["result"]["reason_code"], "FIXTURE_CONTROL_CHARACTER")

    def test_fixture_extension_follows_policy(self) -> None:
        record, _, spawn = self.run_engine()
        self.assertEqual(record["execution"]["fixture"]["extension"], ".avsc")
        self.assertTrue(any(part.endswith("input.avsc") for part in spawn.argv))

    def test_structure_fixture_extension_is_compound(self) -> None:
        body = build_body(command="avrotize s2asn", invocation="avrotize s2asn schema.struct.json --out out.asn1")
        record, _, _ = self.run_engine(issue=build_issue(body))
        self.assertEqual(record["execution"]["fixture"]["extension"], ".struct.json")


class OutcomeMappingTests(ReproTestCase):
    def test_expected_success_with_failure_is_confirmed(self) -> None:
        record, _, _ = self.run_engine(plan={"returncode": 2, "stderr": b"boom"})
        self.assertEqual(record["result"]["status"], "CONFIRMED")
        self.assertEqual(record["result"]["reason_code"], "EXPECTED_SUCCESS_GOT_FAILURE")
        self.assertEqual(record["result"]["final_label"], "repro-confirmed")

    def test_expected_success_with_success_needs_review(self) -> None:
        record, _, _ = self.run_engine(plan={"returncode": 0, "files": {"output.json": b"{}"}})
        self.assertEqual(record["result"]["status"], "NEEDS_REVIEW")
        self.assertEqual(record["result"]["reason_code"], "SUCCESS_WITHOUT_EXACT_OUTPUT")
        self.assertEqual(record["result"]["final_label"], "repro-needs-review")

    def test_expected_failure_with_failure_is_not_reproduced(self) -> None:
        body = build_body(expected_result="Command failure (nonzero exit)")
        record, _, _ = self.run_engine(issue=build_issue(body), plan={"returncode": 1})
        self.assertEqual(record["result"]["status"], "NOT_REPRODUCED")
        self.assertEqual(record["result"]["reason_code"], "EXPECTED_FAILURE_OBSERVED")
        self.assertEqual(record["result"]["final_label"], "repro-not-reproduced")

    def test_expected_failure_with_success_is_confirmed(self) -> None:
        body = build_body(expected_result="Command failure (nonzero exit)")
        record, _, _ = self.run_engine(issue=build_issue(body), plan={"returncode": 0})
        self.assertEqual(record["result"]["status"], "CONFIRMED")
        self.assertEqual(record["result"]["reason_code"], "EXPECTED_FAILURE_GOT_SUCCESS")

    def test_exact_output_match_is_not_reproduced(self) -> None:
        body = build_body(expected_result="Exact output match", expected_output='{"type":"object"}')
        record, _, _ = self.run_engine(
            issue=build_issue(body), plan={"returncode": 0, "files": {"output.json": b'{"type":"object"}\n'}}
        )
        self.assertEqual(record["result"]["status"], "NOT_REPRODUCED")
        self.assertTrue(record["result"]["comparison"]["performed"])
        self.assertTrue(record["result"]["comparison"]["match"])
        self.assertEqual(len(record["result"]["comparison"]["expected_digest"]), 64)

    def test_exact_output_mismatch_is_confirmed(self) -> None:
        body = build_body(expected_result="Exact output match", expected_output='{"type":"object"}')
        record, _, _ = self.run_engine(
            issue=build_issue(body), plan={"returncode": 0, "files": {"output.json": b'{"type":"array"}\n'}}
        )
        self.assertEqual(record["result"]["status"], "CONFIRMED")
        self.assertEqual(record["result"]["reason_code"], "EXACT_OUTPUT_MISMATCH")
        self.assertFalse(record["result"]["comparison"]["match"])

    def test_exact_output_without_text_needs_review(self) -> None:
        body = build_body(expected_result="Exact output match")
        record, _, _ = self.run_engine(issue=build_issue(body), plan={"returncode": 0, "files": {"output.json": b"{}"}})
        self.assertEqual(record["result"]["status"], "NEEDS_REVIEW")
        self.assertEqual(record["result"]["reason_code"], "EXACT_OUTPUT_NOT_SUPPLIED")

    def test_exact_output_with_multiple_files_needs_review(self) -> None:
        body = build_body(expected_result="Exact output match", expected_output="x")
        record, _, _ = self.run_engine(
            issue=build_issue(body),
            plan={"returncode": 0, "files": {"output.json": b"x", "extra.json": b"y"}},
        )
        self.assertEqual(record["result"]["reason_code"], "EXACT_OUTPUT_TARGET_AMBIGUOUS")

    def test_exact_output_with_no_file_needs_review(self) -> None:
        body = build_body(expected_result="Exact output match", expected_output="x")
        record, _, _ = self.run_engine(issue=build_issue(body), plan={"returncode": 0})
        self.assertEqual(record["result"]["reason_code"], "EXACT_OUTPUT_TARGET_AMBIGUOUS")

    def test_success_with_exact_output_compares(self) -> None:
        body = build_body(expected_result="Successful completion (exit 0)", expected_output='{"ok":true}')
        record, _, _ = self.run_engine(
            issue=build_issue(body), plan={"returncode": 0, "files": {"output.json": b'{"ok":true}'}}
        )
        self.assertEqual(record["result"]["status"], "NOT_REPRODUCED")
        self.assertEqual(record["result"]["reason_code"], "EXACT_OUTPUT_MATCH")

    def test_fenced_exact_output_is_unwrapped_before_comparison(self) -> None:
        body = build_body(
            expected_result="Exact output match",
            expected_output='```json\n{"type":"object"}\n```',
        )
        record, _, _ = self.run_engine(
            issue=build_issue(body), plan={"returncode": 0, "files": {"output.json": b'{"type":"object"}\n'}}
        )
        self.assertEqual(record["result"]["status"], "NOT_REPRODUCED")
        self.assertTrue(record["result"]["comparison"]["match"])

    def test_trailing_newline_difference_is_not_a_mismatch(self) -> None:
        body = build_body(expected_result="Exact output match", expected_output='{"a":1}')
        record, _, _ = self.run_engine(
            issue=build_issue(body), plan={"returncode": 0, "files": {"output.json": b'{"a":1}\r\n'}}
        )
        self.assertEqual(record["result"]["status"], "NOT_REPRODUCED")

    def test_human_review_choice_needs_review(self) -> None:
        body = build_body(expected_result="Human semantic review")
        record, _, _ = self.run_engine(issue=build_issue(body), plan={"returncode": 0})
        self.assertEqual(record["result"]["reason_code"], "EXPECTATION_HUMAN_REVIEW")

    def test_undeclared_expectation_needs_review(self) -> None:
        body = build_body(expected_result="_No response_")
        record, _, _ = self.run_engine(issue=build_issue(body), plan={"returncode": 0})
        self.assertEqual(record["result"]["status"], "NEEDS_REVIEW")
        self.assertEqual(record["result"]["reason_code"], "EXPECTATION_NOT_DECLARED")

    def test_reporter_prose_is_never_compared(self) -> None:
        body = build_body(
            expected_result="_No response_",
            actual="ValueError: unexpected token",
            expected="ValueError: unexpected token",
        )
        record, summary, _ = self.run_engine(
            issue=build_issue(body), plan={"returncode": 0, "stdout": b"ValueError: unexpected token"}
        )
        self.assertEqual(record["result"]["status"], "NEEDS_REVIEW")
        self.assertNotIn("unexpected token", json.dumps(record["result"]))
        self.assertNotIn("Traceback", summary)


class ResourceLimitTests(ReproTestCase):
    def test_timeout_is_blocked(self) -> None:
        record, _, _ = self.run_engine(plan={"timeout": True})
        self.assertEqual(record["result"]["status"], "BLOCKED")
        self.assertEqual(record["result"]["reason_code"], "EXECUTION_TIMEOUT")
        self.assertTrue(record["execution"]["timed_out"])
        self.assertEqual(record["execution"]["resource_status"], "timeout")

    def test_stream_budget_is_enforced(self) -> None:
        record, _, _ = self.run_engine(plan={"returncode": 0, "stdout": b"a" * (1024 * 1024 + 8)})
        self.assertEqual(record["result"]["reason_code"], "STREAM_LIMIT_EXCEEDED")
        self.assertEqual(record["execution"]["resource_status"], "stream-limit-exceeded")
        self.assertTrue(record["execution"]["stdout"]["truncated"])
        self.assertLessEqual(len(record["execution"]["stdout"]["excerpt"]), 4000)

    def test_output_byte_budget_is_enforced(self) -> None:
        record, _, _ = self.run_engine(plan={"returncode": 0, "files": {"big.json": b"z" * (4 * 1024 * 1024 + 16)}})
        self.assertEqual(record["result"]["reason_code"], "OUTPUT_LIMIT_EXCEEDED")
        self.assertTrue(record["execution"]["outputs"]["limit_exceeded"])

    def test_output_file_count_budget_is_enforced(self) -> None:
        files = {f"file{index}.json": b"{}" for index in range(40)}
        record, _, _ = self.run_engine(plan={"returncode": 0, "files": files})
        self.assertEqual(record["result"]["reason_code"], "OUTPUT_LIMIT_EXCEEDED")
        self.assertEqual(record["execution"]["outputs"]["file_count"], 40)
        self.assertLessEqual(len(record["execution"]["outputs"]["files"]), 32)

    def test_symlink_output_is_rejected(self) -> None:
        plan = {"returncode": 0, "files": {"output.json": b"{}"}, "symlinks": {"link.json": "target.json"}}
        record, _, _ = self.run_engine(plan=plan)
        if plan.get("symlink_unavailable"):
            self.skipTest("symlink creation is not permitted on this platform")
        self.assertEqual(record["result"]["reason_code"], "OUTPUT_SYMLINK_REJECTED")

    def test_output_manifest_records_digests(self) -> None:
        record, _, _ = self.run_engine(plan={"returncode": 2, "files": {"nested/out.json": b"abc"}})
        manifest = record["execution"]["outputs"]
        self.assertEqual(manifest["file_count"], 1)
        self.assertEqual(manifest["files"][0]["path"], "nested/out.json")
        self.assertEqual(manifest["files"][0]["bytes"], 3)
        self.assertEqual(manifest["files"][0]["digest"], hashlib.sha256(b"abc").hexdigest())

    def test_stream_digests_and_excerpts_are_recorded(self) -> None:
        record, _, _ = self.run_engine(plan={"returncode": 1, "stdout": b"hello\n", "stderr": b"warn\n"})
        self.assertEqual(record["execution"]["stdout"]["digest"], hashlib.sha256(b"hello\n").hexdigest())
        self.assertEqual(record["execution"]["stderr"]["excerpt"].strip(), "warn")
        self.assertEqual(record["execution"]["stderr"]["bytes"], 5)

    def test_excerpt_cut_is_reported_as_truncated(self) -> None:
        payload = b"y" * 9000
        record, _, _ = self.run_engine(plan={"returncode": 1, "stdout": payload})
        stdout = record["execution"]["stdout"]
        self.assertEqual(stdout["bytes"], 9000)
        self.assertEqual(len(stdout["excerpt"]), 4000)
        self.assertTrue(stdout["truncated"])
        self.assertEqual(stdout["digest"], hashlib.sha256(payload).hexdigest())

    def test_short_stream_is_not_marked_truncated(self) -> None:
        record, _, _ = self.run_engine(plan={"returncode": 1, "stdout": b"short output"})
        self.assertFalse(record["execution"]["stdout"]["truncated"])


class ExecutionEnvironmentTests(ReproTestCase):
    def test_environment_is_minimal_and_sanitized(self) -> None:
        os.environ["GITHUB_TOKEN"] = "should-not-leak"
        os.environ["ACTIONS_RUNTIME_TOKEN"] = "should-not-leak"
        self.addCleanup(os.environ.pop, "GITHUB_TOKEN", None)
        self.addCleanup(os.environ.pop, "ACTIONS_RUNTIME_TOKEN", None)
        _, _, spawn = self.run_engine()
        self.assertNotIn("GITHUB_TOKEN", spawn.env)
        self.assertNotIn("ACTIONS_RUNTIME_TOKEN", spawn.env)
        self.assertNotIn("should-not-leak", json.dumps(spawn.env))
        self.assertEqual(spawn.env["PYTHONHASHSEED"], "0")
        self.assertEqual(spawn.env["LC_ALL"], "C.UTF-8")

    def test_child_runs_in_workspace_output_directory(self) -> None:
        _, _, spawn = self.run_engine()
        self.assertTrue(spawn.cwd.startswith(str(self.workspace_root)))
        self.assertTrue(spawn.cwd.endswith("out"))

    def test_workspace_is_removed_after_run(self) -> None:
        self.run_engine()
        self.assertEqual(list(self.workspace_root.iterdir()), [])


class RevisionAndIdentityTests(ReproTestCase):
    def test_changed_updated_at_blocks_without_execution(self) -> None:
        record, _, spawn = self.run_engine(expected_updated_at="2026-08-17T09:00:00Z")
        self.assertEqual(record["result"]["reason_code"], "ISSUE_REVISED_AFTER_AUTHORIZATION")
        self.assertFalse(record["execution"]["executed"])
        self.assertFalse(record["request"]["revision_verified"])
        self.assertEqual(spawn.argv, [])

    def test_changed_body_digest_blocks_without_execution(self) -> None:
        record, _, _ = self.run_engine(expected_body_digest="a" * 64)
        self.assertEqual(record["result"]["reason_code"], "ISSUE_REVISED_AFTER_AUTHORIZATION")
        self.assertFalse(record["execution"]["executed"])

    def test_matching_body_digest_allows_execution(self) -> None:
        issue = build_issue()
        digest = hashlib.sha256(issue["body"].encode("utf-8")).hexdigest()
        record, _, _ = self.run_engine(issue=issue, expected_body_digest=digest)
        self.assertTrue(record["execution"]["executed"])
        self.assertTrue(record["request"]["revision_verified"])

    def test_identity_and_source_are_recorded(self) -> None:
        record, _, _ = self.run_engine()
        self.assertEqual(record["request"]["actor"], "maintainer")
        self.assertEqual(record["request"]["requested_label"], "repro-requested")
        self.assertEqual(record["request"]["run_id"], "555")
        self.assertEqual(record["request"]["run_attempt"], 1)
        self.assertEqual(record["request"]["issue_url"], "https://github.com/clemensv/avrotize/issues/42")
        self.assertEqual(record["source"]["trusted_sha"], "b" * 40)
        self.assertEqual(record["source"]["default_branch"], "master")
        self.assertEqual(len(record["source"]["policy_digest"]), 64)
        self.assertEqual(len(record["source"]["command_registry_digest"]), 64)
        self.assertEqual(len(record["source"]["label_catalog_digest"]), 64)
        self.assertEqual(len(record["source"]["issue_form_contract_digest"]), 64)
        self.assertEqual(len(record["request"]["authorization_digest"]), 64)

    def test_environment_versions_are_recorded(self) -> None:
        record, _, _ = self.run_engine()
        self.assertEqual(record["environment"]["avrotize_version"], "3.9.0")
        self.assertEqual(record["environment"]["avrotize_executable"], "/opt/hostedtoolcache/bin/avrotize")
        self.assertTrue(record["environment"]["python_version"])
        self.assertEqual(record["environment"]["engine_version"], governance_repro.ENGINE_VERSION)

    def test_unauthorized_record_is_infrastructure_failure(self) -> None:
        with self.assertRaises(governance_repro.InfrastructureError):
            self.run_engine(authorization={"decision": "DENY", "request": {}})

    def test_missing_executable_is_infrastructure_failure(self) -> None:
        with self.assertRaises(governance_repro.InfrastructureError):
            self.run_engine(avrotize_executable="")


class EvidenceShapeTests(ReproTestCase):
    def test_evidence_validates_against_schema(self) -> None:
        for plan in (
            {"returncode": 0, "files": {"output.json": b"{}"}},
            {"returncode": 3, "stderr": b"error"},
            {"timeout": True},
        ):
            with self.subTest(plan=str(plan)):
                record, _, _ = self.run_engine(plan=plan)
                governance_schema.validate_or_raise(record, EVIDENCE_SCHEMA, "evidence record")

    def test_blocked_evidence_validates_against_schema(self) -> None:
        record, _, _ = self.run_engine(issue=build_issue(build_body(surface="Python API")))
        governance_schema.validate_or_raise(record, EVIDENCE_SCHEMA, "evidence record")

    def test_authority_is_never_granted(self) -> None:
        record, summary, _ = self.run_engine()
        self.assertFalse(record["authority"]["authorized"])
        self.assertIn("does not authorize", summary)

    def test_final_label_matches_catalog(self) -> None:
        catalog = governance_repro.load_label_catalog()
        mapping = {label["outcome"]: label["name"] for label in catalog["labels"] if label["outcome"]}
        record, _, _ = self.run_engine(plan={"returncode": 5})
        self.assertEqual(record["result"]["final_label"], mapping[record["result"]["status"]])

    def test_artifact_metadata_is_recorded(self) -> None:
        record, _, _ = self.run_engine()
        self.assertEqual(record["artifact"]["name"], "repro-evidence-42")
        self.assertEqual(record["artifact"]["retention_days"], 14)

    def test_summary_reports_outcome_and_source(self) -> None:
        _, summary, _ = self.run_engine()
        self.assertIn("Guarded bug reproduction evidence", summary)
        self.assertIn("Final label", summary)
        self.assertIn("Trusted source", summary)


class PolicyLoadingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory(dir=str(Path(__file__).resolve().parent))
        self.addCleanup(self.temporary_directory.cleanup)
        self.root = Path(self.temporary_directory.name)

    def test_real_policy_loads(self) -> None:
        policy = governance_repro.load_policy()
        self.assertEqual({entry["command"] for entry in policy["commands"]}, {"a2j", "a2asn", "s2asn", "pcf"})

    def test_missing_policy_is_infrastructure_failure(self) -> None:
        with self.assertRaises(governance_repro.InfrastructureError):
            governance_repro.load_policy(self.root / "absent.json")

    def test_corrupt_policy_is_infrastructure_failure(self) -> None:
        path = self.root / "policy.json"
        path.write_text("{ not json", encoding="utf-8")
        with self.assertRaises(governance_repro.InfrastructureError):
            governance_repro.load_policy(path)

    def test_schema_violating_policy_is_infrastructure_failure(self) -> None:
        policy = json.loads((REPO_ROOT / ".github/governance/repro-command-policy.json").read_text(encoding="utf-8"))
        policy["commands"][0]["output_mode"] = "network"
        path = self.root / "policy.json"
        path.write_text(json.dumps(policy), encoding="utf-8")
        with self.assertRaises(governance_repro.InfrastructureError):
            governance_repro.load_policy(path)

    def test_policy_command_must_exist_in_registry(self) -> None:
        policy = json.loads((REPO_ROOT / ".github/governance/repro-command-policy.json").read_text(encoding="utf-8"))
        policy["commands"][0]["command"] = "notreal"
        path = self.root / "policy.json"
        path.write_text(json.dumps(policy), encoding="utf-8")
        with self.assertRaises(governance_repro.InfrastructureError):
            governance_repro.load_policy(path)

    def test_corrupt_catalog_is_infrastructure_failure(self) -> None:
        path = self.root / "catalog.json"
        path.write_text(json.dumps({"schema_version": 1, "labels": []}), encoding="utf-8")
        with self.assertRaises(governance_repro.InfrastructureError):
            governance_repro.load_label_catalog(path)


class CliTests(ReproTestCase):
    def test_cli_writes_evidence_and_exits_zero(self) -> None:
        governance_repro._popen = FakeSpawn({"returncode": 1, "stderr": b"failure"})
        issue_path = self.workspace_root / "issue.json"
        auth_path = self.workspace_root / "authorization.json"
        json_path = self.workspace_root / "evidence.json"
        markdown_path = self.workspace_root / "evidence.md"
        issue_path.write_text(json.dumps(build_issue()), encoding="utf-8")
        auth_path.write_text(json.dumps(build_authorization()), encoding="utf-8")
        code = governance_repro.main(
            [
                "--issue",
                str(issue_path),
                "--authorization",
                str(auth_path),
                "--repository",
                "clemensv/avrotize",
                "--expected-updated-at",
                "2026-08-17T10:00:00Z",
                "--trusted-sha",
                "c" * 40,
                "--default-branch",
                "master",
                "--avrotize-executable",
                "/usr/bin/avrotize",
                "--avrotize-version",
                "3.9.0",
                "--workspace-root",
                str(self.workspace_root),
                "--output-json",
                str(json_path),
                "--output-markdown",
                str(markdown_path),
            ]
        )
        self.assertEqual(code, 0)
        record = json.loads(json_path.read_text(encoding="utf-8"))
        self.assertEqual(record["result"]["status"], "CONFIRMED")
        self.assertTrue(markdown_path.is_file())

    def test_cli_rejects_unreadable_issue(self) -> None:
        auth_path = self.workspace_root / "authorization.json"
        auth_path.write_text(json.dumps(build_authorization()), encoding="utf-8")
        with self.assertRaises(governance_repro.InfrastructureError):
            governance_repro.main(
                ["--issue", str(self.workspace_root / "missing.json"), "--authorization", str(auth_path)]
            )

    def test_repeated_runs_are_deterministic(self) -> None:
        first, _, _ = self.run_engine(plan={"returncode": 4, "stdout": b"x"})
        second, _, _ = self.run_engine(plan={"returncode": 4, "stdout": b"x"})
        for record in (first, second):
            record["execution"]["duration_seconds"] = 0.0
        self.assertEqual(json.dumps(first, sort_keys=True), json.dumps(second, sort_keys=True))


if __name__ == "__main__":
    unittest.main()
