"""Participation-UX contracts for governance contributor surfaces."""

from __future__ import annotations

import json
import unittest
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parent.parent
ISSUE_FORMS = ROOT / ".github" / "ISSUE_TEMPLATE"


def load_form(name: str) -> dict:
    return yaml.safe_load((ISSUE_FORMS / name).read_text(encoding="utf-8"))


class LightweightIssueFormTests(unittest.TestCase):
    def required_ids(self, name: str) -> list[str]:
        return [
            item["id"]
            for item in load_form(name)["body"]
            if item["type"] != "markdown"
            and item.get("validations", {}).get("required") is True
        ]

    def test_required_reporter_fields_are_minimal(self) -> None:
        self.assertEqual(self.required_ids("bug.yml"), ["problem", "actual"])
        self.assertEqual(self.required_ids("feature.yml"), ["outcome"])
        self.assertEqual(self.required_ids("question.yml"), ["message"])

    def test_forms_have_no_confirmation_checklists(self) -> None:
        for name in ("bug.yml", "feature.yml", "question.yml"):
            with self.subTest(name=name):
                fields = [
                    item for item in load_form(name)["body"] if item["type"] != "markdown"
                ]
                self.assertNotIn("checkboxes", {item["type"] for item in fields})
                self.assertNotIn("title", {item.get("id") for item in fields})

    def test_optional_surface_accepts_uncertainty_and_generated_code(self) -> None:
        surface = next(
            item for item in load_form("bug.yml")["body"] if item.get("id") == "surface"
        )
        self.assertFalse(surface["validations"]["required"])
        self.assertIn("I'm not sure", surface["attributes"]["options"])
        self.assertIn("Generated project or code", surface["attributes"]["options"])

    def test_not_sure_chooser_route_opens_fallback_directly(self) -> None:
        config = load_form("config.yml")
        fallback = next(
            link
            for link in config["contact_links"]
            if link["name"] == "Not sure which form to use?"
        )
        self.assertTrue(fallback["url"].endswith("template=question.yml"))

    def test_contract_required_fields_match_lightweight_forms(self) -> None:
        contract = json.loads(
            (ROOT / ".github" / "governance" / "issue-form-contract.json").read_text(
                encoding="utf-8"
            )
        )
        required = {
            form["type"]: form["required_semantic_fields"] for form in contract["forms"]
        }
        self.assertEqual(required, {"bug": ["problem", "actual"], "feature": ["outcome"]})


class WelcomingTextTests(unittest.TestCase):
    def test_contributing_invites_imperfect_work_and_maintainer_help(self) -> None:
        text = (ROOT / "CONTRIBUTING.md").read_text(encoding="utf-8").lower()
        for phrase in (
            "do not need to be perfect",
            "maintainers can help",
            "draft and incomplete pull requests are welcome",
            "do not delay the report",
        ):
            self.assertIn(phrase, text)

    def test_support_disclaims_every_assurance_category(self) -> None:
        text = (ROOT / "SUPPORT.md").read_text(encoding="utf-8").lower()
        self.assertIn("best-effort", text)
        self.assertIn("no guarantee", text)
        for assurance in (
            "response",
            "review",
            "acceptance",
            "triage",
            "resolution",
            "fix",
            "release",
            "compatibility",
            "availability",
            "maintenance",
            "continued support",
            "outcome",
        ):
            self.assertIn(assurance, text)
        for prohibited in (
            "support covers",
            "will respond",
            "response time",
            "service level",
            "resolution time",
            "next owner-approved",
        ):
            self.assertNotIn(prohibited, text)

    def test_pull_request_template_has_two_essential_prompts(self) -> None:
        text = (ROOT / ".github" / "pull_request_template.md").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("- [ ]", text)
        self.assertNotIn("authorization", text.lower())
        self.assertEqual(text.count("\n## "), 3)
        self.assertIn("Anything else? (optional)", text)

    def test_reproduction_feedback_is_actionable_and_non_accusatory(self) -> None:
        helper = (ROOT / "tools" / "governance_repro.py").read_text(encoding="utf-8")
        workflow = (
            ROOT / ".github" / "workflows" / "repro-bug.yml"
        ).read_text(encoding="utf-8")
        for text in (helper, workflow):
            self.assertIn("No action is needed from the reporter", text)
            self.assertNotIn("failed governance", text.lower())
            self.assertNotIn("A maintainer will review", text)
        self.assertIn("one specific detail", helper)


if __name__ == "__main__":
    unittest.main()
