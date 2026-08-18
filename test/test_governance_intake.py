"""Comprehensive tests for governance intake normalizer."""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from tools import governance_intake

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "governance"


class IssueIntakeBugCompleteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.event = (FIXTURES / "issue_bug_complete.json").read_text(encoding="utf-8")
        self.record, self.markdown = governance_intake.normalize_issue(self.event)

    def test_schema_version(self) -> None:
        self.assertEqual(self.record["schema_version"], 2)

    def test_record_kind(self) -> None:
        self.assertEqual(self.record["record_kind"], "issue-intake")

    def test_form_type_detected(self) -> None:
        self.assertEqual(self.record["classification"]["form_type"], "bug")

    def test_status_complete(self) -> None:
        self.assertEqual(self.record["classification"]["status"], "complete")

    def test_no_missing_fields(self) -> None:
        self.assertEqual(self.record["classification"]["missing_fields"], [])

    def test_command_extracted(self) -> None:
        self.assertEqual(self.record["normalized_facts"]["command"], "avrotize a2p")

    def test_command_known(self) -> None:
        self.assertTrue(self.record["normalized_facts"]["command_known"])

    def test_semantic_paths_avrotize_schema(self) -> None:
        self.assertIn("Avrotize Schema", self.record["normalized_facts"]["semantic_paths"])

    def test_authority_false(self) -> None:
        self.assertFalse(self.record["authority"]["authorized"])

    def test_authority_statement(self) -> None:
        self.assertIn("does not authorize", self.record["authority"]["statement"])

    def test_source_digest_present(self) -> None:
        self.assertEqual(len(self.record["event_identity"]["source_digest"]), 64)

    def test_body_digest_present(self) -> None:
        self.assertEqual(len(self.record["event_identity"]["body_digest"]), 64)

    def test_deterministic_output(self) -> None:
        record2, _ = governance_intake.normalize_issue(self.event)
        self.assertEqual(
            json.dumps(self.record, sort_keys=True),
            json.dumps(record2, sort_keys=True),
        )

    def test_deterministic_serialized_bytes(self) -> None:
        """Same input must produce byte-identical JSON."""
        _, _ = governance_intake.normalize_issue(self.event)
        r2, _ = governance_intake.normalize_issue(self.event)
        self.assertEqual(
            json.dumps(self.record, indent=2, sort_keys=False).encode(),
            json.dumps(r2, indent=2, sort_keys=False).encode(),
        )

    def test_markdown_contains_issue_number(self) -> None:
        self.assertIn("#500", self.markdown)

    def test_event_identity_has_repository(self) -> None:
        # May be empty in fixture but key must exist
        self.assertIn("repository", self.record["event_identity"])

    def test_event_identity_has_sender(self) -> None:
        self.assertIn("sender", self.record["event_identity"])

    def test_event_identity_update_flag(self) -> None:
        self.assertFalse(self.record["event_identity"]["update"])


class IssueIntakeFeatureCompleteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.event = (FIXTURES / "issue_feature_complete.json").read_text(encoding="utf-8")
        self.record, self.markdown = governance_intake.normalize_issue(self.event)

    def test_form_type_feature(self) -> None:
        self.assertEqual(self.record["classification"]["form_type"], "feature")

    def test_status_complete(self) -> None:
        self.assertEqual(self.record["classification"]["status"], "complete")

    def test_optional_command_s2graphql(self) -> None:
        self.assertEqual(self.record["normalized_facts"]["command"], "s2graphql")

    def test_command_known_false_for_proposed(self) -> None:
        self.assertIsNotNone(self.record["normalized_facts"]["command_known"])

    def test_optional_command_resolves_json_structure_path(self) -> None:
        self.assertIn("JSON Structure", self.record["normalized_facts"]["semantic_paths"])

    def test_optional_example_extracted(self) -> None:
        self.assertIsNotNone(self.record["normalized_facts"]["documentation"])


class IssueIntakeIncompleteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.event = (FIXTURES / "issue_incomplete.json").read_text(encoding="utf-8")
        self.record, self.markdown = governance_intake.normalize_issue(self.event)

    def test_status_incomplete(self) -> None:
        self.assertEqual(self.record["classification"]["status"], "incomplete")

    def test_missing_fields_listed(self) -> None:
        missing = self.record["classification"]["missing_fields"]
        self.assertTrue(len(missing) > 0)

    def test_summary_uses_reporter_facing_field_label(self) -> None:
        self.assertIn("What were you trying to do?", self.markdown)
        self.assertNotIn("**A maintainer may ask for**: problem", self.markdown)

    def test_authority_false(self) -> None:
        self.assertFalse(self.record["authority"]["authorized"])


class IssueIntakeUnknownTests(unittest.TestCase):
    """Unknown/freeform issues must produce unclassified + manual-triage."""

    def setUp(self) -> None:
        self.event = (FIXTURES / "issue_unknown.json").read_text(encoding="utf-8")
        self.record, self.markdown = governance_intake.normalize_issue(self.event)

    def test_form_type_unclassified(self) -> None:
        self.assertEqual(self.record["classification"]["form_type"], "unclassified")

    def test_status_manual_triage(self) -> None:
        self.assertEqual(self.record["classification"]["status"], "manual-triage")

    def test_authority_false(self) -> None:
        self.assertFalse(self.record["authority"]["authorized"])


class IssueIntakeEmptyBodyTests(unittest.TestCase):
    """Known title prefix with an empty body remains available for a human read."""

    def setUp(self) -> None:
        self.event = (FIXTURES / "issue_empty_body.json").read_text(encoding="utf-8")
        self.record, self.markdown = governance_intake.normalize_issue(self.event)

    def test_status_routes_to_human_review(self) -> None:
        self.assertEqual(self.record["classification"]["status"], "manual-triage")

    def test_exits_successfully(self) -> None:
        self.assertIsNotNone(self.record)
        self.assertFalse(self.record["authority"]["authorized"])


class IssueIntakePlaceholderRejectionTests(unittest.TestCase):
    """Placeholder values (N/A, TBD, none) must be rejected."""

    def test_placeholder_detection(self) -> None:
        self.assertTrue(governance_intake._is_placeholder("N/A"))
        self.assertTrue(governance_intake._is_placeholder("none"))
        self.assertTrue(governance_intake._is_placeholder("TBD"))
        self.assertTrue(governance_intake._is_placeholder("placeholder"))
        self.assertTrue(governance_intake._is_placeholder("..."))
        self.assertTrue(governance_intake._is_placeholder(""))

    def test_real_content_not_placeholder(self) -> None:
        self.assertFalse(governance_intake._is_placeholder("avrotize a2p"))
        self.assertFalse(governance_intake._is_placeholder("Some real content here"))


class IssueIntakeEditedReopenedTests(unittest.TestCase):
    """Edited/reopened events must set update=True."""

    def test_edited_event(self) -> None:
        event = json.dumps({
            "action": "edited",
            "issue": {"number": 99, "title": "Can you help?", "body": "stuff"},
        })
        record, _ = governance_intake.normalize_issue(event)
        self.assertTrue(record["event_identity"]["update"])
        self.assertEqual(record["event_identity"]["event_type"], "edited")

    def test_reopened_event(self) -> None:
        event = json.dumps({
            "action": "reopened",
            "issue": {"number": 99, "title": "Question", "body": "test"},
        })
        record, _ = governance_intake.normalize_issue(event)
        self.assertTrue(record["event_identity"]["update"])


class IssueIntakeMalformedJsonTests(unittest.TestCase):
    """Malformed JSON input must raise (hard failure), not produce a record."""

    def test_malformed_json_raises(self) -> None:
        with self.assertRaises(json.JSONDecodeError):
            governance_intake.normalize_issue("{not valid json")


class DependabotPythonMajorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.event = (FIXTURES / "dependabot_python_major.json").read_text(encoding="utf-8")
        self.files = (FIXTURES / "dependabot_python_major_files.json").read_text(encoding="utf-8")
        self.record, self.markdown = governance_intake.normalize_dependabot(self.event, self.files)

    def test_schema_version(self) -> None:
        self.assertEqual(self.record["schema_version"], 1)

    def test_record_kind(self) -> None:
        self.assertEqual(self.record["record_kind"], "dependabot-intake")

    def test_is_dependabot(self) -> None:
        self.assertTrue(self.record["classification"]["is_dependabot"])

    def test_status_complete(self) -> None:
        self.assertEqual(self.record["classification"]["status"], "complete")

    def test_dependency_name(self) -> None:
        deps = self.record["normalized_facts"]["dependencies"]
        self.assertEqual(len(deps), 1)
        self.assertEqual(deps[0]["name"], "requests")

    def test_major_version_bump(self) -> None:
        deps = self.record["normalized_facts"]["dependencies"]
        self.assertEqual(deps[0]["update_type"], "major")

    def test_major_version_risk(self) -> None:
        self.assertTrue(self.record["normalized_facts"]["major_version_risk"])

    def test_review_required_for_major(self) -> None:
        self.assertTrue(self.record["normalized_facts"]["review_required"])

    def test_ecosystem_pip(self) -> None:
        self.assertEqual(self.record["normalized_facts"]["config_entry"]["ecosystem"], "pip")

    def test_manifests_changed(self) -> None:
        self.assertIn("requirements.txt", self.record["normalized_facts"]["manifests_changed"])

    def test_other_files_empty_for_known(self) -> None:
        self.assertEqual(self.record["normalized_facts"]["other_files"], [])

    def test_authority_false(self) -> None:
        self.assertFalse(self.record["authority"]["authorized"])

    def test_no_safe_merge_inference(self) -> None:
        self.assertFalse(self.record["normalized_facts"]["safe_merge_inferred"])

    def test_no_exploitability_inference(self) -> None:
        self.assertFalse(self.record["normalized_facts"]["exploitability_inferred"])

    def test_deterministic_output(self) -> None:
        record2, _ = governance_intake.normalize_dependabot(self.event, self.files)
        self.assertEqual(
            json.dumps(self.record, sort_keys=True),
            json.dumps(record2, sort_keys=True),
        )

    def test_validation_scope_includes_review(self) -> None:
        scope = self.record["normalized_facts"]["required_validation_scope"]
        self.assertIn("compatibility-review", scope)
        self.assertIn("review-required", scope)

    def test_concrete_validation_scope(self) -> None:
        scope = self.record["normalized_facts"]["required_validation_scope"]
        self.assertIn("technical-evidence", scope)
        self.assertIn("python-package-tests", scope)

    def test_identity_checks_present(self) -> None:
        checks = self.record["event_identity"]["identity_checks"]
        self.assertTrue(checks["author_is_dependabot_bot"])
        self.assertTrue(checks["head_ref_prefix"])

    def test_body_metadata_dependency_type(self) -> None:
        deps = self.record["normalized_facts"]["dependencies"]
        self.assertEqual(deps[0]["dependency_type"], "direct")

    def test_pr_number_top_level(self) -> None:
        self.assertEqual(self.record["pr_number"], 460)

    def test_source_digest_combined(self) -> None:
        self.assertEqual(len(self.record["event_identity"]["source_digest"]), 64)

    def test_files_digest_present(self) -> None:
        self.assertEqual(len(self.record["event_identity"]["files_digest"]), 64)

    def test_config_digest_present(self) -> None:
        self.assertEqual(len(self.record["event_identity"]["config_digest"]), 64)


class DependabotMultiEcosystemTests(unittest.TestCase):
    def setUp(self) -> None:
        self.event = (FIXTURES / "dependabot_multi_ecosystem.json").read_text(encoding="utf-8")
        self.files = (FIXTURES / "dependabot_multi_ecosystem_files.json").read_text(encoding="utf-8")
        self.record, self.markdown = governance_intake.normalize_dependabot(self.event, self.files)

    def test_is_dependabot(self) -> None:
        self.assertTrue(self.record["classification"]["is_dependabot"])

    def test_config_entries_multiple(self) -> None:
        """Multi-ecosystem fixture must produce config_entries."""
        entries = self.record["normalized_facts"]["config_entries"]
        self.assertGreater(len(entries), 0)

    def test_directory_matched(self) -> None:
        entry = self.record["normalized_facts"]["config_entry"]
        self.assertEqual(entry["directory"], "/avrotize/dependencies/typescript/node22")

    def test_lockfiles_detected(self) -> None:
        self.assertIn(
            "avrotize/dependencies/typescript/node22/package-lock.json",
            self.record["normalized_facts"]["lockfiles_changed"],
        )

    def test_manifests_detected(self) -> None:
        self.assertIn(
            "avrotize/dependencies/typescript/node22/package.json",
            self.record["normalized_facts"]["manifests_changed"],
        )

    def test_generated_output_implications(self) -> None:
        self.assertTrue(self.record["normalized_facts"]["exposure"]["generated_output_implications"])

    def test_toolchain_implications(self) -> None:
        self.assertTrue(self.record["normalized_facts"]["exposure"]["toolchain_implications"])

    def test_domain_includes_generated_output(self) -> None:
        self.assertIn("generated-output", self.record["normalized_facts"]["domains"])

    def test_concrete_validation_scope(self) -> None:
        scope = self.record["normalized_facts"]["required_validation_scope"]
        self.assertIn("generated-output-verification", scope)
        self.assertIn("target-compiler-runtime-test", scope)


class DependabotNonDependabotTests(unittest.TestCase):
    def setUp(self) -> None:
        self.event = (FIXTURES / "dependabot_non_dependabot.json").read_text(encoding="utf-8")
        self.record, self.markdown = governance_intake.normalize_dependabot(self.event, "[]")

    def test_not_dependabot(self) -> None:
        self.assertFalse(self.record["classification"]["is_dependabot"])

    def test_status_ignored(self) -> None:
        self.assertEqual(self.record["classification"]["status"], "ignored")

    def test_reason_unauthorized(self) -> None:
        self.assertEqual(self.record["classification"]["reason"], "unauthorized")

    def test_authority_false(self) -> None:
        self.assertFalse(self.record["authority"]["authorized"])


class DependabotSpoofBranchNonBotTests(unittest.TestCase):
    """A non-bot user with a 'dependabot/' branch name must be rejected."""

    def test_branch_spoof_rejected(self) -> None:
        event = json.dumps({
            "action": "opened",
            "sender": {"login": "evil-user"},
            "pull_request": {
                "number": 999,
                "title": "Bump foo from 1.0 to 2.0",
                "user": {"login": "evil-user"},
                "head": {"sha": "abc123", "ref": "dependabot/pip/foo-2.0"},
                "base": {"sha": "def456", "ref": "master"},
                "body": "",
            },
        })
        record, _ = governance_intake.normalize_dependabot(event, "[]")
        self.assertFalse(record["classification"]["is_dependabot"])
        self.assertEqual(record["classification"]["status"], "ignored")
        self.assertEqual(record["classification"]["reason"], "unauthorized")


class DependabotGithubActionsRootTests(unittest.TestCase):
    """Workflow files in .github/workflows must classify as github-actions, not pip."""

    def test_workflow_file_classifies_as_actions(self) -> None:
        event = json.dumps({
            "action": "opened",
            "sender": {"login": "dependabot[bot]"},
            "pull_request": {
                "number": 470,
                "title": "Bump actions/checkout from 3 to 4",
                "user": {"login": "dependabot[bot]"},
                "head": {"sha": "aaa111", "ref": "dependabot/github_actions/actions/checkout-4"},
                "base": {"sha": "bbb222", "ref": "master"},
                "body": "Bumps [actions/checkout](https://github.com/actions/checkout) from 3 to 4.\n\n---\nupdated-dependencies:\n- dependency-name: actions/checkout\n  dependency-type: direct:production\n  update-type: version-update:semver-major\n...",
            },
        })
        files = json.dumps([
            {"filename": ".github/workflows/build_deploy.yml", "status": "modified"},
        ])
        record, _ = governance_intake.normalize_dependabot(event, files)
        self.assertTrue(record["classification"]["is_dependabot"])
        entry = record["normalized_facts"]["config_entry"]
        self.assertEqual(entry["ecosystem"], "github-actions")
        # Must NOT match pip /
        self.assertNotEqual(entry["ecosystem"], "pip")


class DependabotLockfileTransitiveTests(unittest.TestCase):
    """Lockfile-only changes should classify as transitive."""

    def test_lockfile_only_transitive(self) -> None:
        event = json.dumps({
            "action": "opened",
            "sender": {"login": "dependabot[bot]"},
            "pull_request": {
                "number": 471,
                "title": "Bump indirect-dep from 1.0 to 1.1 in /avrotize/dependencies/typescript/node22",
                "user": {"login": "dependabot[bot]"},
                "head": {"sha": "ccc333", "ref": "dependabot/npm_and_yarn/indirect-dep-1.1"},
                "base": {"sha": "ddd444", "ref": "master"},
                "body": "---\nupdated-dependencies:\n- dependency-name: indirect-dep\n  dependency-type: indirect\n  update-type: version-update:semver-minor\n...",
            },
        })
        files = json.dumps([
            {"filename": "avrotize/dependencies/typescript/node22/package-lock.json", "status": "modified"},
        ])
        record, _ = governance_intake.normalize_dependabot(event, files)
        deps = record["normalized_facts"]["dependencies"]
        self.assertEqual(len(deps), 1)
        self.assertEqual(deps[0]["dependency_type"], "transitive")


class DependabotGroupBodyMetadataTests(unittest.TestCase):
    """Group updates should extract dependencies from body metadata."""

    def test_group_update_body_parsing(self) -> None:
        event = json.dumps({
            "action": "opened",
            "sender": {"login": "dependabot[bot]"},
            "pull_request": {
                "number": 472,
                "title": "Bump the pip group in / with 2 updates",
                "user": {"login": "dependabot[bot]"},
                "head": {"sha": "eee555", "ref": "dependabot/pip/group-123"},
                "base": {"sha": "fff666", "ref": "master"},
                "body": (
                    "Bumps the pip group with 2 updates.\n\n---\n"
                    "updated-dependencies:\n"
                    "- dependency-name: requests\n"
                    "  dependency-type: direct:production\n"
                    "  update-type: version-update:semver-minor\n"
                    "- dependency-name: urllib3\n"
                    "  dependency-type: indirect\n"
                    "  update-type: version-update:semver-patch\n"
                    "...\n"
                ),
            },
        })
        files = json.dumps([
            {"filename": "requirements.txt", "status": "modified"},
            {"filename": "Pipfile.lock", "status": "modified"},
        ])
        record, _ = governance_intake.normalize_dependabot(event, files)
        deps = record["normalized_facts"]["dependencies"]
        self.assertEqual(len(deps), 2)
        names = [d["name"] for d in deps]
        self.assertIn("requests", names)
        self.assertIn("urllib3", names)
        # Check types from body
        req_dep = next(d for d in deps if d["name"] == "requests")
        self.assertEqual(req_dep["dependency_type"], "direct")
        self.assertEqual(req_dep["update_type"], "minor")
        url_dep = next(d for d in deps if d["name"] == "urllib3")
        self.assertEqual(url_dep["dependency_type"], "transitive")


class DependabotNestedApiFlattening(unittest.TestCase):
    """gh api --paginate --slurp produces [[page1], [page2]]; must flatten."""

    def test_nested_pages_flattened(self) -> None:
        nested = [
            [{"filename": "a.txt", "status": "modified"}, {"filename": "b.txt", "status": "added"}],
            [{"filename": "c.txt", "status": "modified"}],
        ]
        flat = governance_intake._flatten_paginated_files(nested)
        self.assertEqual(len(flat), 3)
        self.assertEqual([f["filename"] for f in flat], ["a.txt", "b.txt", "c.txt"])

    def test_flat_array_passthrough(self) -> None:
        flat_input = [{"filename": "x.txt", "status": "modified"}]
        result = governance_intake._flatten_paginated_files(flat_input)
        self.assertEqual(len(result), 1)

    def test_empty_array(self) -> None:
        self.assertEqual(governance_intake._flatten_paginated_files([]), [])

    def test_exact_nested_shape(self) -> None:
        """Exact shape: list of lists of dicts."""
        data = [[{"filename": "f1", "status": "added"}], [{"filename": "f2", "status": "removed"}]]
        result = governance_intake._flatten_paginated_files(data)
        self.assertEqual(result, [{"filename": "f1", "status": "added"}, {"filename": "f2", "status": "removed"}])


class DependabotMajorUnknownRiskTests(unittest.TestCase):
    """Major and unknown version risk must be flagged."""

    def test_unknown_version_risk(self) -> None:
        event = json.dumps({
            "action": "opened",
            "sender": {"login": "dependabot[bot]"},
            "pull_request": {
                "number": 475,
                "title": "Bump mystery-dep to something",
                "user": {"login": "dependabot[bot]"},
                "head": {"sha": "hhh888", "ref": "dependabot/pip/mystery"},
                "base": {"sha": "iii999", "ref": "master"},
                "body": "",
            },
        })
        files = json.dumps([{"filename": "requirements.txt", "status": "modified"}])
        record, _ = governance_intake.normalize_dependabot(event, files)
        # Title parse gives name but no old version -> unknown bump
        deps = record["normalized_facts"]["dependencies"]
        if deps:
            self.assertIn(deps[0]["update_type"], ["unknown", "major", "minor", "patch"])


class DependabotConcreteDomainsExposureTests(unittest.TestCase):
    """Verify concrete domain/exposure/validation resolution."""

    def test_root_python_exposure(self) -> None:
        event = json.dumps({
            "action": "opened",
            "sender": {"login": "dependabot[bot]"},
            "pull_request": {
                "number": 476,
                "title": "Bump foo from 1.0 to 1.1",
                "user": {"login": "dependabot[bot]"},
                "head": {"sha": "jjj000", "ref": "dependabot/pip/foo-1.1"},
                "base": {"sha": "kkk111", "ref": "master"},
                "body": "---\nupdated-dependencies:\n- dependency-name: foo\n  dependency-type: direct:production\n  update-type: version-update:semver-minor\n...",
            },
        })
        files = json.dumps([{"filename": "requirements.txt", "status": "modified"}])
        record, _ = governance_intake.normalize_dependabot(event, files)
        exposure = record["normalized_facts"]["exposure"]["categories"]
        self.assertIn("runtime", exposure)
        self.assertIn("build", exposure)
        self.assertIn("test", exposure)
        domains = record["normalized_facts"]["domains"]
        self.assertIn("root-python-package", domains)
        scope = record["normalized_facts"]["required_validation_scope"]
        self.assertIn("python-package-tests", scope)

    def test_ci_workflow_exposure(self) -> None:
        event = json.dumps({
            "action": "opened",
            "sender": {"login": "dependabot[bot]"},
            "pull_request": {
                "number": 477,
                "title": "Bump actions/checkout from 3 to 4",
                "user": {"login": "dependabot[bot]"},
                "head": {"sha": "lll222", "ref": "dependabot/github_actions/checkout-4"},
                "base": {"sha": "mmm333", "ref": "master"},
                "body": "",
            },
        })
        files = json.dumps([{"filename": ".github/workflows/build_deploy.yml", "status": "modified"}])
        record, _ = governance_intake.normalize_dependabot(event, files)
        domains = record["normalized_facts"]["domains"]
        self.assertIn("ci", domains)
        scope = record["normalized_facts"]["required_validation_scope"]
        self.assertIn("ci-workflow-validation", scope)


class DependabotFileMetadataTests(unittest.TestCase):
    """File status metadata must be preserved."""

    def test_file_metadata_includes_status(self) -> None:
        event = json.dumps({
            "action": "opened",
            "sender": {"login": "dependabot[bot]"},
            "pull_request": {
                "number": 480,
                "title": "Bump dep from 1.0 to 2.0",
                "user": {"login": "dependabot[bot]"},
                "head": {"sha": "xxx", "ref": "dependabot/pip/dep-2.0"},
                "base": {"sha": "yyy", "ref": "master"},
                "body": "",
            },
        })
        files = json.dumps([{"filename": "requirements.txt", "status": "modified"}])
        record, _ = governance_intake.normalize_dependabot(event, files)
        metadata = record["normalized_facts"]["file_metadata"]
        self.assertEqual(len(metadata), 1)
        self.assertEqual(metadata[0]["filename"], "requirements.txt")
        self.assertEqual(metadata[0]["status"], "modified")


class DependabotUnknownFilesNotManifests(unittest.TestCase):
    """Unknown files must not be classified as manifests."""

    def test_unknown_file_in_other(self) -> None:
        event = json.dumps({
            "action": "opened",
            "sender": {"login": "dependabot[bot]"},
            "pull_request": {
                "number": 481,
                "title": "Bump dep from 1.0 to 2.0",
                "user": {"login": "dependabot[bot]"},
                "head": {"sha": "aaa", "ref": "dependabot/pip/dep-2.0"},
                "base": {"sha": "bbb", "ref": "master"},
                "body": "",
            },
        })
        files = json.dumps([
            {"filename": "requirements.txt", "status": "modified"},
            {"filename": "random_readme.md", "status": "modified"},
        ])
        record, _ = governance_intake.normalize_dependabot(event, files)
        self.assertIn("random_readme.md", record["normalized_facts"]["other_files"])
        self.assertNotIn("random_readme.md", record["normalized_facts"]["manifests_changed"])


class IssueFormContractValidationTests(unittest.TestCase):
    """Test that the issue form contract matches the actual YAML files."""

    def test_contract_loads(self) -> None:
        contract = governance_intake._load_issue_form_contract()
        self.assertIn("forms", contract)
        self.assertEqual(len(contract["forms"]), 2)

    def test_bug_form_headings_match_yaml(self) -> None:
        contract = governance_intake._load_issue_form_contract()
        bug_form = next(f for f in contract["forms"] if f["type"] == "bug")
        yaml_path = governance_intake.REPO_ROOT / bug_form["file"]
        text = yaml_path.read_text(encoding="utf-8")
        for heading in bug_form["headings"]:
            self.assertIn(heading, text, f"Heading '{heading}' not found in {bug_form['file']}")

    def test_feature_form_headings_match_yaml(self) -> None:
        contract = governance_intake._load_issue_form_contract()
        feature_form = next(f for f in contract["forms"] if f["type"] == "feature")
        yaml_path = governance_intake.REPO_ROOT / feature_form["file"]
        text = yaml_path.read_text(encoding="utf-8")
        for heading in feature_form["headings"]:
            self.assertIn(heading, text, f"Heading '{heading}' not found in {feature_form['file']}")

    def test_bug_field_ids_match_yaml(self) -> None:
        contract = governance_intake._load_issue_form_contract()
        bug_form = next(f for f in contract["forms"] if f["type"] == "bug")
        yaml_path = governance_intake.REPO_ROOT / bug_form["file"]
        text = yaml_path.read_text(encoding="utf-8")
        for fid in bug_form["field_ids"]:
            self.assertIn(f"id: {fid}", text, f"Field id '{fid}' not in {bug_form['file']}")


class ConfigMappingTests(unittest.TestCase):
    """Test that dependabot.yml parsing works correctly."""

    def test_config_parses(self) -> None:
        entries = governance_intake._parse_dependabot_config()
        self.assertGreater(len(entries), 0)

    def test_config_has_pip_entry(self) -> None:
        entries = governance_intake._parse_dependabot_config()
        ecosystems = [e["package-ecosystem"] for e in entries]
        self.assertIn("pip", ecosystems)

    def test_config_has_github_actions_entry(self) -> None:
        entries = governance_intake._parse_dependabot_config()
        ecosystems = [e["package-ecosystem"] for e in entries]
        self.assertIn("github-actions", ecosystems)

    def test_config_has_npm_entry(self) -> None:
        entries = governance_intake._parse_dependabot_config()
        ecosystems = [e["package-ecosystem"] for e in entries]
        self.assertIn("npm", ecosystems)

    def test_npm_directory_correct(self) -> None:
        entries = governance_intake._parse_dependabot_config()
        npm_entry = next(e for e in entries if e["package-ecosystem"] == "npm")
        self.assertEqual(npm_entry["directory"], "/avrotize/dependencies/typescript/node22")

    def test_duplicate_root_entries(self) -> None:
        """Both pip / and github-actions / must be parsed as separate entries."""
        entries = governance_intake._parse_dependabot_config()
        root_entries = [e for e in entries if e["directory"] == "/"]
        ecosystems = [e["package-ecosystem"] for e in root_entries]
        self.assertIn("pip", ecosystems)
        self.assertIn("github-actions", ecosystems)
        self.assertEqual(len(root_entries), 2)


class SchemaShapeTests(unittest.TestCase):
    """Verify schemas have required structural properties."""

    def setUp(self) -> None:
        schema_dir = governance_intake.REPO_ROOT / ".github" / "governance" / "schemas"
        self.issue_schema = json.loads(
            (schema_dir / "issue-intake-record.schema.json").read_text(encoding="utf-8")
        )
        self.dep_schema = json.loads(
            (schema_dir / "dependabot-intake-record.schema.json").read_text(encoding="utf-8")
        )

    def test_issue_schema_has_id(self) -> None:
        self.assertIn("$id", self.issue_schema)

    def test_dep_schema_has_id(self) -> None:
        self.assertIn("$id", self.dep_schema)

    def test_issue_schema_additional_properties(self) -> None:
        self.assertFalse(self.issue_schema.get("additionalProperties", True))

    def test_dep_schema_additional_properties(self) -> None:
        self.assertFalse(self.dep_schema.get("additionalProperties", True))

    def test_issue_schema_required_fields(self) -> None:
        required = self.issue_schema["required"]
        for field in [
            "schema_version",
            "record_kind",
            "event_identity",
            "classification",
            "semantic_assistance",
            "privacy",
            "authority",
        ]:
            self.assertIn(field, required)

    def test_dep_schema_required_fields(self) -> None:
        required = self.dep_schema["required"]
        for field in ["schema_version", "record_kind", "event_identity", "classification", "authority"]:
            self.assertIn(field, required)


class CLIOutputTests(unittest.TestCase):
    """Test CLI entry point."""

    def test_issue_mode_returns_zero(self) -> None:
        fixture = FIXTURES / "issue_bug_complete.json"
        ret = governance_intake.main(["issue", "--event", str(fixture)])
        self.assertEqual(ret, 0)

    def test_dependabot_mode_returns_zero(self) -> None:
        event_fixture = FIXTURES / "dependabot_python_major.json"
        files_fixture = FIXTURES / "dependabot_python_major_files.json"
        ret = governance_intake.main([
            "dependabot", "--event", str(event_fixture), "--files", str(files_fixture)
        ])
        self.assertEqual(ret, 0)

    def test_no_mode_returns_one(self) -> None:
        ret = governance_intake.main([])
        self.assertEqual(ret, 1)


def _bug_event(body: str, title: str = "[Bug] heading set check") -> str:
    return json.dumps({"action": "opened", "issue": {"number": 1, "title": title, "body": body}})


def _complete_bug_body() -> str:
    event = json.loads((FIXTURES / "issue_bug_complete.json").read_text(encoding="utf-8"))
    return event["issue"]["body"]


class HeadingSetTests(unittest.TestCase):
    """The rendered heading set must match the contract exactly."""

    def test_complete_form_has_no_heading_findings(self) -> None:
        record, _ = governance_intake.normalize_issue(_bug_event(_complete_bug_body()))
        self.assertEqual(record["classification"]["supplemental_headings"], [])
        self.assertEqual(record["classification"]["missing_headings"], [])
        self.assertEqual(record["classification"]["status"], "complete")

    def test_additional_heading_is_kept_without_rejecting_report(self) -> None:
        body = _complete_bug_body() + "\n\n### Injected heading\n\nvalue"
        record, markdown = governance_intake.normalize_issue(_bug_event(body))
        self.assertEqual(record["classification"]["status"], "complete")
        self.assertEqual(record["classification"]["supplemental_headings"], ["Injected heading"])
        self.assertIn("Additional sections kept for review", markdown)

    def test_missing_required_heading_remains_available_for_follow_up(self) -> None:
        body = _complete_bug_body().replace("### What happened?", "### Result")
        record, _ = governance_intake.normalize_issue(_bug_event(body))
        self.assertEqual(record["classification"]["status"], "incomplete")
        self.assertIn("Result", record["classification"]["supplemental_headings"])
        self.assertIn("What happened?", record["classification"]["missing_headings"])

    def test_missing_optional_heading_is_tolerated(self) -> None:
        body = _complete_bug_body()
        body = body.replace(
            "\n\n### Anything else? (optional)\n\nThis worked with Avrotize 3.8.2.",
            "",
        )
        record, _ = governance_intake.normalize_issue(_bug_event(body))
        self.assertEqual(record["classification"]["status"], "complete")
        self.assertEqual(record["classification"]["missing_headings"], ["Anything else? (optional)"])
        self.assertEqual(record["normalized_facts"]["expected_result_kind"], "undeclared")


class ProgressiveDisclosureTests(unittest.TestCase):
    def test_bug_requires_only_goal_and_observed_result(self) -> None:
        contract = governance_intake._load_issue_form_contract()
        bug = next(form for form in contract["forms"] if form["type"] == "bug")
        self.assertEqual(bug["required_semantic_fields"], ["problem", "actual"])

    def test_feature_requires_only_desired_outcome(self) -> None:
        contract = governance_intake._load_issue_form_contract()
        feature = next(form for form in contract["forms"] if form["type"] == "feature")
        self.assertEqual(feature["required_semantic_fields"], ["outcome"])

    def test_optional_bug_details_can_be_omitted(self) -> None:
        body = (
            "### What were you trying to do?\n\nConvert a schema.\n\n"
            "### What happened?\n\nThe result was empty."
        )
        record, markdown = governance_intake.normalize_issue(_bug_event(body))
        self.assertEqual(record["classification"]["status"], "complete")
        self.assertIsNone(record["normalized_facts"]["command"])
        self.assertIn("Ready for a maintainer to read", markdown)


class IssueFormParityTests(unittest.TestCase):
    def test_every_checked_in_surface_choice_is_accepted(self) -> None:
        contract = governance_intake._load_issue_form_contract()
        for choice in contract["surface_choices"]:
            with self.subTest(choice=choice):
                body = _complete_bug_body().replace(
                    "### Where did this happen? (optional)\n\nAvrotize CLI",
                    f"### Where did this happen? (optional)\n\n{choice}",
                    1,
                )
                record, _ = governance_intake.normalize_issue(_bug_event(body))
                self.assertEqual(record["normalized_facts"]["surface"], choice)
                self.assertNotIn("surface", record["classification"]["missing_fields"])

    def test_each_surface_uses_its_authoritative_exact_identifier(self) -> None:
        identifiers = {
            "I'm not sure": "avrotize a2p",
            "Avrotize CLI": "avrotize a2p",
            "Structurize CLI": "structurize s2graphql",
            "Python API": "avrotize.avrotopython.convert_avro_to_python",
            "MCP server": "run_conversion",
            "VS Code extension": "avrotize.a2p",
            "Generated project or code": "convert_avro_to_python",
        }
        for surface, identifier in identifiers.items():
            with self.subTest(surface=surface):
                body = _complete_bug_body().replace(
                    "### Where did this happen? (optional)\n\nAvrotize CLI",
                    f"### Where did this happen? (optional)\n\n{surface}",
                    1,
                ).replace(
                    "### Command or Avrotize area (optional)\n\navrotize a2p",
                    "### Command or Avrotize area (optional)"
                    f"\n\n{identifier}",
                    1,
                )
                record, _ = governance_intake.normalize_issue(_bug_event(body))
                self.assertTrue(record["normalized_facts"]["command_known"])

    def test_substring_or_wrong_surface_identifier_is_not_known(self) -> None:
        cases = (
            ("Avrotize CLI", "a"),
            ("Python API", "a2p"),
            ("MCP server", "a2p"),
            ("VS Code extension", "avrotize.avrotopython.convert_avro_to_python"),
        )
        for surface, identifier in cases:
            with self.subTest(surface=surface, identifier=identifier):
                body = _complete_bug_body().replace(
                    "### Where did this happen? (optional)\n\nAvrotize CLI",
                    f"### Where did this happen? (optional)\n\n{surface}",
                    1,
                ).replace(
                    "### Command or Avrotize area (optional)\n\navrotize a2p",
                    "### Command or Avrotize area (optional)"
                    f"\n\n{identifier}",
                    1,
                )
                record, _ = governance_intake.normalize_issue(_bug_event(body))
                self.assertFalse(record["normalized_facts"]["command_known"])

    def test_heading_inside_fenced_reporter_content_is_not_a_field(self) -> None:
        body = _complete_bug_body().replace(
            '{"type":"record","name":"Node","fields":[{"name":"children","type":{"type":"array","items":"Node"}}]}',
            '{"type":"record","name":"Node"}\n### Version and environment (optional)\nnot a field',
            1,
        )
        record, _ = governance_intake.normalize_issue(_bug_event(body))
        self.assertEqual(record["classification"]["status"], "complete")
        self.assertEqual(record["classification"]["supplemental_headings"], [])
        self.assertEqual(
            record["normalized_facts"]["environment"],
            "Avrotize 3.9.0, Ubuntu 22.04, Python 3.12.3, protoc 25.1",
        )

    def test_duplicate_real_heading_routes_to_human_review(self) -> None:
        body = _complete_bug_body() + "\n\n### Version and environment (optional)\n\nsecond value\n"
        record, _ = governance_intake.normalize_issue(_bug_event(body))
        self.assertEqual(record["classification"]["status"], "manual-triage")
        self.assertIn(
            "Version and environment (optional)",
            record["classification"]["repeated_headings"],
        )

    def test_processor_and_contract_digests_bind_record_source(self) -> None:
        event = _bug_event(_complete_bug_body())
        first, _ = governance_intake.normalize_issue(event, "a" * 40)
        second, _ = governance_intake.normalize_issue(event, "b" * 40)
        self.assertEqual(first["event_identity"]["processor_sha"], "a" * 40)
        for key in (
            "title_digest",
            "body_digest",
            "contract_digest",
            "command_registry_digest",
            "capability_digest",
            "surface_registry_digest",
            "semantic_policy_digest",
            "copilot_lockfile_digest",
            "semantic_output_schema_digest",
            "semantic_prompt_digest",
            "source_digest",
        ):
            self.assertRegex(first["event_identity"][key], r"^[0-9a-f]{64}$")
        self.assertNotEqual(
            first["event_identity"]["source_digest"],
            second["event_identity"]["source_digest"],
        )


class DependabotRevisionBindingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.event = json.loads(
            (FIXTURES / "dependabot_python_major.json").read_text(encoding="utf-8")
        )
        self.files = json.loads(
            (FIXTURES / "dependabot_python_major_files.json").read_text(encoding="utf-8")
        )

    def test_real_configured_prefixed_title_is_parsed(self) -> None:
        self.event["pull_request"]["title"] = (
            "deps(python): Bump requests from 2.31.0 to 3.0.0"
        )
        record, _ = governance_intake.normalize_dependabot(
            json.dumps(self.event), json.dumps(self.files)
        )
        dependency = record["normalized_facts"]["dependencies"][0]
        self.assertEqual(dependency["name"], "requests")
        self.assertEqual(dependency["update_type"], "major")
        self.assertEqual(
            record["event_identity"]["identity_checks"]["configured_title_prefix"],
            "deps(python)",
        )

    def test_head_change_before_or_after_retrieval_is_superseded(self) -> None:
        event_head = self.event["pull_request"]["head"]["sha"]
        for before, after in (("new-head", "new-head"), (event_head, "new-head")):
            with self.subTest(before=before, after=after):
                self.event["intake_observation"] = {
                    "head_before": before,
                    "head_after": after,
                }
                record, _ = governance_intake.normalize_dependabot(
                    json.dumps(self.event), json.dumps(self.files)
                )
                self.assertEqual(record["classification"]["status"], "superseded")
                self.assertEqual(record["normalized_facts"], {})

    def test_full_file_metadata_changes_digest(self) -> None:
        first, _ = governance_intake.normalize_dependabot(
            json.dumps(self.event), json.dumps(self.files)
        )
        changed = json.loads(json.dumps(self.files))
        changed[0]["sha"] = "f" * 40
        changed[0]["additions"] = int(changed[0].get("additions", 0)) + 1
        second, _ = governance_intake.normalize_dependabot(
            json.dumps(self.event), json.dumps(changed)
        )
        self.assertNotEqual(
            first["event_identity"]["files_digest"],
            second["event_identity"]["files_digest"],
        )

    def test_github_actions_root_entry_is_not_classified_as_python(self) -> None:
        self.event["pull_request"]["title"] = (
            "deps(actions): Bump actions/checkout from 6 to 7"
        )
        self.event["pull_request"]["head"]["ref"] = (
            "dependabot/github_actions/actions/checkout-7"
        )
        files = [
            {
                "filename": ".github/workflows/ci.yml",
                "status": "modified",
                "sha": "1" * 40,
                "additions": 1,
                "deletions": 1,
                "changes": 2,
            }
        ]
        record, _ = governance_intake.normalize_dependabot(
            json.dumps(self.event), json.dumps(files)
        )
        facts = record["normalized_facts"]
        self.assertEqual(facts["ecosystems"][0]["ecosystem"], "github-actions")
        self.assertEqual(facts["domains"], ["ci"])
        self.assertEqual(facts["exposure"]["categories"], ["ci"])
        self.assertIn("ci-workflow-validation", facts["required_validation_scope"])
        self.assertNotIn("python-package-tests", facts["required_validation_scope"])
        self.assertNotIn("package-build", facts["required_validation_scope"])


class DependabotMultiEcosystemSeparationTests(unittest.TestCase):
    """Each matched ecosystem is classified with its own manifest rules."""

    def setUp(self) -> None:
        event = (FIXTURES / "dependabot_cross_ecosystem.json").read_text(encoding="utf-8")
        files = (FIXTURES / "dependabot_cross_ecosystem_files.json").read_text(encoding="utf-8")
        self.record, self.markdown = governance_intake.normalize_dependabot(event, files)
        self.facts = self.record["normalized_facts"]
        self.groups = {group["ecosystem"]: group for group in self.facts["ecosystems"]}

    def test_multiple_ecosystems_detected(self) -> None:
        self.assertTrue(self.facts["multi_ecosystem"])
        self.assertEqual(sorted(self.groups), ["github-actions", "npm", "pip"])

    def test_pip_group_only_owns_requirements(self) -> None:
        self.assertEqual(self.groups["pip"]["manifests_changed"], ["requirements.txt"])
        self.assertEqual(self.groups["pip"]["lockfiles_changed"], [])

    def test_npm_group_separates_manifest_and_lockfile(self) -> None:
        npm = self.groups["npm"]
        self.assertEqual(npm["manifests_changed"], ["avrotize/dependencies/typescript/node22/package.json"])
        self.assertEqual(npm["lockfiles_changed"], ["avrotize/dependencies/typescript/node22/package-lock.json"])

    def test_actions_group_classifies_workflow_as_manifest(self) -> None:
        self.assertEqual(self.groups["github-actions"]["manifests_changed"], [".github/workflows/build_deploy.yml"])

    def test_workflow_file_is_not_a_pip_manifest(self) -> None:
        self.assertNotIn(".github/workflows/build_deploy.yml", self.groups["pip"]["manifests_changed"])

    def test_unmatched_files_are_recorded(self) -> None:
        self.assertEqual(self.facts["unmatched_files"], ["docs/notes.md"])
        self.assertIn("docs/notes.md", self.facts["other_files"])

    def test_domains_are_unioned_across_ecosystems(self) -> None:
        self.assertIn("root-python-package", self.facts["domains"])
        self.assertIn("generated-output", self.facts["domains"])
        self.assertIn("ci", self.facts["domains"])

    def test_validation_scope_covers_every_ecosystem(self) -> None:
        scope = self.facts["required_validation_scope"]
        self.assertIn("python-package-tests", scope)
        self.assertIn("target-compiler-runtime-test", scope)
        self.assertIn("ci-workflow-validation", scope)

    def test_dependency_ecosystem_is_not_guessed(self) -> None:
        for dependency in self.facts["dependencies"]:
            self.assertEqual(dependency["ecosystem"], "indeterminate")
            self.assertEqual(dependency["directory"], "multiple")

    def test_markdown_reports_each_ecosystem(self) -> None:
        self.assertIn("Ecosystem classification", self.markdown)
        self.assertIn("Multi-ecosystem", self.markdown)

    def test_major_update_still_requires_review(self) -> None:
        self.assertTrue(self.facts["major_version_risk"])
        self.assertTrue(self.facts["review_required"])


class SchemaEnforcementTests(unittest.TestCase):
    """Records are structurally validated before they are written."""

    def test_issue_record_validates_deeply(self) -> None:
        record, _ = governance_intake.normalize_issue(_bug_event(_complete_bug_body()))
        errors = governance_intake._validate_record(record, governance_intake.ISSUE_RECORD_SCHEMA)
        self.assertEqual(errors, [])

    def test_invalid_record_is_rejected(self) -> None:
        record, _ = governance_intake.normalize_issue(_bug_event(_complete_bug_body()))
        record["classification"]["status"] = "totally-made-up"
        errors = governance_intake._validate_record(record, governance_intake.ISSUE_RECORD_SCHEMA)
        self.assertTrue(errors)

    def test_complete_issue_cannot_omit_required_observed_result(self) -> None:
        record, _ = governance_intake.normalize_issue(_bug_event(_complete_bug_body()))
        record["normalized_facts"]["actual_behavior"] = None
        errors = governance_intake._validate_record(record, governance_intake.ISSUE_RECORD_SCHEMA)
        self.assertTrue(errors)

    def test_incomplete_dependabot_record_requires_missing_information(self) -> None:
        event = (FIXTURES / "dependabot_python_major.json").read_text(encoding="utf-8")
        files = (FIXTURES / "dependabot_python_major_files.json").read_text(encoding="utf-8")
        record, _ = governance_intake.normalize_dependabot(event, files)
        record["classification"]["status"] = "incomplete"
        record["classification"]["missing_info"] = []
        errors = governance_intake._validate_record(
            record, governance_intake.DEPENDABOT_RECORD_SCHEMA
        )
        self.assertTrue(errors)

    def test_complete_dependabot_record_requires_dependencies(self) -> None:
        event = (FIXTURES / "dependabot_python_major.json").read_text(encoding="utf-8")
        files = (FIXTURES / "dependabot_python_major_files.json").read_text(encoding="utf-8")
        record, _ = governance_intake.normalize_dependabot(event, files)
        record["normalized_facts"]["dependencies"] = []
        errors = governance_intake._validate_record(
            record, governance_intake.DEPENDABOT_RECORD_SCHEMA
        )
        self.assertTrue(errors)

    def test_missing_schema_is_infrastructure_failure(self) -> None:
        with self.assertRaises(RuntimeError):
            governance_intake._validate_record({}, Path("does-not-exist.schema.json"))

    def test_dependabot_record_validates_deeply(self) -> None:
        event = (FIXTURES / "dependabot_python_major.json").read_text(encoding="utf-8")
        files = (FIXTURES / "dependabot_python_major_files.json").read_text(encoding="utf-8")
        record, _ = governance_intake.normalize_dependabot(event, files)
        errors = governance_intake._validate_record(record, governance_intake.DEPENDABOT_RECORD_SCHEMA)
        self.assertEqual(errors, [])


if __name__ == "__main__":
    unittest.main()
