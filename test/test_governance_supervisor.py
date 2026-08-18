"""Authority, reconciliation, and durability tests for external supervision."""

from __future__ import annotations

import copy
import json
import subprocess
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from tools import governance_schema, governance_supervisor


ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "test" / "fixtures" / "governance" / "supervisor"
POLICY_PATH = ROOT / ".github" / "governance" / "external-supervisor-policy.json"
PROMPT_PATH = ROOT / ".github" / "governance" / "prompts" / "external-supervisor-kickoff-v1.txt"
DELEGATION_SCHEMA = (
    ROOT
    / ".github"
    / "governance"
    / "schemas"
    / "external-supervisor-delegation.schema.json"
)
RECORD_SCHEMA = (
    ROOT
    / ".github"
    / "governance"
    / "schemas"
    / "external-supervisor-cycle.schema.json"
)
NOW = datetime(2026, 2, 1, 12, 0, 0, tzinfo=timezone.utc)
POLICY_COMMIT = "a" * 40


def load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


class SupervisorTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.policy_bytes = governance_supervisor.text_blob_bytes(POLICY_PATH)
        self.policy = load(POLICY_PATH)
        self.delegation = load(FIXTURES / "valid-delegation.json")
        self.snapshot = load(FIXTURES / "repository-snapshot.json")
        self.inventory = load(FIXTURES / "session-inventory.json")

    def verify_delegation(self) -> None:
        governance_supervisor.verify_delegation(
            self.policy,
            self.delegation,
            policy_bytes=self.policy_bytes,
            policy_commit=POLICY_COMMIT,
            delegation_bytes=json.dumps(self.delegation).encode("utf-8"),
            now=NOW,
        )

    def delegation_binding(self) -> dict:
        return {
            "commit_sha": POLICY_COMMIT,
            "path": "governance/delegations/test.json",
            "blob_sha256": governance_supervisor.digest_bytes(
                json.dumps(self.delegation).encode("utf-8")
            ),
        }

    def cycle(
        self,
        *,
        cycle_id: str | None = None,
        sequence: int = 1,
        previous_record: dict | None = None,
        recovery_record: dict | None = None,
    ) -> dict:
        self.verify_delegation()
        if cycle_id is None:
            cycle_id = (
                self.delegation["initial_cycle_id"]
                if sequence == 1
                else governance_supervisor.next_cycle_id(
                    previous_record,
                    sequence,
                    delegation=self.delegation,
                    delegation_binding=self.delegation_binding(),
                    snapshot=self.snapshot,
                    inventory=self.inventory,
                    recovery_record=recovery_record,
                )
            )
        result = governance_supervisor.reconcile(
            self.policy,
            self.delegation,
            self.snapshot,
            self.inventory,
            self.delegation_binding(),
            now=NOW,
            cycle_id=cycle_id,
            sequence=sequence,
            previous_record=previous_record,
            recovery_record=recovery_record,
        )
        self.last_cycle = result
        return result

    def add_worker(
        self,
        *,
        session_id: str = "worker-101",
        state: str = "RUNNING",
        issue_number: int = 101,
        cycle_id: str | None = "cycle-example-001",
        decision_id: str | None = None,
        dirty: bool = False,
        head_sha: str | None = None,
        branch_ref: str | None = None,
        executing: bool | None = None,
        redirects: int = 0,
        success_claimed: bool = False,
    ) -> dict:
        bound_cycle = getattr(self, "last_cycle", None)
        cycle_record_digest = None
        dispatch_packet_digest = None
        if (
            bound_cycle is not None
            and decision_id == bound_cycle["decision"]["decision_id"]
            and cycle_id == bound_cycle["cycle_id"]
        ):
            cycle_record_digest = bound_cycle["audit"]["payload_digest"]
            dispatch_packet_digest = governance_supervisor.digest_json(
                bound_cycle["decision"]["dispatch_packet"]
            )
            if head_sha is None:
                head_sha = bound_cycle["decision"]["dispatch_packet"][
                    "launch_head_sha"
                ]
            if branch_ref is None:
                branch_ref = bound_cycle["decision"]["dispatch_packet"][
                    "mutation_constraints"
                ]["allowed_branch_ref"]
        if head_sha is None:
            head_sha = "5" * 40
        if branch_ref is None:
            branch_ref = f"refs/heads/observed/issue-{issue_number}"
        session = {
            "session_id": session_id,
            "role": "worker",
            "issue_number": issue_number,
            "state": state,
            "delegation_id": self.delegation["delegation_id"],
            "policy_commit": POLICY_COMMIT,
            "cycle_id": cycle_id,
            "decision_id": decision_id,
            "cycle_record_digest": cycle_record_digest,
            "dispatch_packet_digest": dispatch_packet_digest,
            "branch_ref": branch_ref,
            "head_sha": head_sha,
            "dirty": dirty,
            "executing": state == "RUNNING" if executing is None else executing,
            "redirects": redirects,
            "success_claimed": success_claimed,
        }
        self.inventory["sessions"].append(session)
        return session

    def item(self, issue_number: int) -> dict:
        return next(
            item for item in self.snapshot["items"] if item["issue_number"] == issue_number
        )

    def operational_receipt_cycle(self, cycle: dict) -> dict:
        candidate = copy.deepcopy(cycle)
        candidate.pop("audit")
        candidate["authority"]["delegated_operational"] = True
        packet = candidate["decision"]["dispatch_packet"]
        packet["allowed_tools"] = [
            "repository-read",
            "scoped-repository-edit",
            "test-runner",
            "scoped-git-commit",
            "scoped-git-push-broker",
            "project-session-messaging",
        ]
        packet["allowed_mutations"] = list(self.delegation["allowed_actions"])
        candidate["pending_dispatch"]["dispatch_packet_digest"] = (
            governance_supervisor.digest_json(packet)
        )
        activated = governance_supervisor.seal_record(candidate)
        self.last_cycle = activated
        self.receipt_policy = copy.deepcopy(self.policy)
        self.receipt_policy["mode"] = "operational-attested"
        return activated


class DelegationTests(SupervisorTestCase):
    def test_valid_delegation_is_strict_and_active(self) -> None:
        self.verify_delegation()
        schema = governance_schema.load_schema(DELEGATION_SCHEMA)
        self.assertEqual(governance_schema.validate(self.delegation, schema), [])

    def test_unknown_delegation_field_is_rejected(self) -> None:
        schema = governance_schema.load_schema(DELEGATION_SCHEMA)
        self.delegation["surprise"] = True
        errors = governance_schema.validate(self.delegation, schema)
        self.assertTrue(any("unexpected properties" in error for error in errors))

    def test_delegation_bytes_cannot_change_after_owner_binding(self) -> None:
        bound_bytes = json.dumps(self.delegation).encode("utf-8")
        self.delegation["expires_at"] = "2026-12-01T00:00:00Z"
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError,
            "DELEGATION_DIGEST_MISMATCH",
        ):
            governance_supervisor.verify_delegation(
                self.policy,
                self.delegation,
                policy_bytes=self.policy_bytes,
                policy_commit=POLICY_COMMIT,
                delegation_bytes=bound_bytes,
                now=NOW,
            )

    def test_missing_expired_and_revoked_delegation_fail_closed(self) -> None:
        missing = copy.deepcopy(self.delegation)
        del missing["delegation_id"]
        schema = governance_schema.load_schema(DELEGATION_SCHEMA)
        self.assertTrue(governance_schema.validate(missing, schema))

        self.delegation["expires_at"] = "2026-01-31T00:00:00Z"
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError, "DELEGATION_EXPIRED"
        ):
            self.verify_delegation()

        self.delegation = load(FIXTURES / "valid-delegation.json")
        self.delegation["revocation"] = {
            "revoked": True,
            "revoked_at": "2026-01-15T00:00:00Z",
            "reason": "Owner revoked",
        }
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError, "DELEGATION_REVOKED"
        ):
            self.verify_delegation()

    def test_authenticated_owner_must_match_repository_owner(self) -> None:
        self.delegation["authenticated_owner_login"] = "not-the-owner"
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError, "OWNER_IDENTITY_MISMATCH"
        ):
            self.verify_delegation()

    def test_owner_binds_the_initial_cycle_id(self) -> None:
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError, "INITIAL_CYCLE_MISMATCH"
        ):
            self.cycle(cycle_id="cycle-reset-0001")

    def test_policy_commit_and_blob_digest_are_exact(self) -> None:
        self.delegation["policy_binding"]["commit_sha"] = "b" * 40
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError, "POLICY_COMMIT_MISMATCH"
        ):
            self.verify_delegation()

        self.delegation = load(FIXTURES / "valid-delegation.json")
        self.delegation["policy_binding"]["blob_sha256"] = "f" * 64
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError, "POLICY_DIGEST_MISMATCH"
        ):
            self.verify_delegation()

    def test_policy_commit_blob_is_verified_from_git(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            subprocess.run(["git", "init", "-q"], cwd=root, check=True)
            subprocess.run(
                ["git", "config", "user.email", "supervisor@example.invalid"],
                cwd=root,
                check=True,
            )
            subprocess.run(
                ["git", "config", "user.name", "Supervisor Test"],
                cwd=root,
                check=True,
            )
            target = root / self.policy["policy_path"]
            target.parent.mkdir(parents=True)
            target.write_bytes(self.policy_bytes)
            prompt_target = root / self.policy["kickoff_prompt"]["path"]
            prompt_target.parent.mkdir(parents=True, exist_ok=True)
            prompt_target.write_bytes(
                governance_supervisor.text_blob_bytes(PROMPT_PATH)
            )
            subprocess.run(["git", "add", "."], cwd=root, check=True)
            subprocess.run(["git", "commit", "-qm", "policy"], cwd=root, check=True)
            commit = subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=root, text=True
            ).strip()
            governance_supervisor.verify_git_blob(
                root,
                commit_sha=commit,
                blob_path=self.policy["policy_path"],
                expected_bytes=self.policy_bytes,
            )
            prompt_target.write_text("tampered working tree\n", encoding="utf-8")
            committed_prompt = governance_supervisor.read_git_blob(
                root,
                commit_sha=commit,
                blob_path=self.policy["kickoff_prompt"]["path"],
            )
            self.assertEqual(
                governance_supervisor.digest_bytes(committed_prompt),
                self.policy["kickoff_prompt"]["sha256"],
            )
            with self.assertRaisesRegex(
                governance_supervisor.SupervisorError, "GOVERNED_BLOB_MISMATCH"
            ):
                governance_supervisor.verify_git_blob(
                    root,
                    commit_sha=commit,
                    blob_path=self.policy["policy_path"],
                    expected_bytes=self.policy_bytes + b"\n",
                )

    def test_owner_only_actions_cannot_be_allowed(self) -> None:
        self.delegation["allowed_actions"].append("merge")
        schema = governance_schema.load_schema(DELEGATION_SCHEMA)
        self.assertTrue(governance_schema.validate(self.delegation, schema))
        with self.assertRaises((governance_schema.SchemaError, governance_supervisor.SupervisorError)):
            self.verify_delegation()

    def test_owner_only_denial_list_is_immutable(self) -> None:
        self.delegation["denied_owner_only_actions"].reverse()
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError, "OWNER_ONLY_DENIAL_MISMATCH"
        ):
            self.verify_delegation()

    def test_delegation_cannot_grant_wip_exception(self) -> None:
        self.delegation["limits"]["max_active_per_domain"] = 2
        with self.assertRaises((governance_schema.SchemaError, governance_supervisor.SupervisorError)):
            self.verify_delegation()

    def test_scope_expansion_and_unknown_dependencies_fail(self) -> None:
        self.delegation["scope"]["ready_order"][0]["issue_number"] = 999
        with self.assertRaisesRegex(governance_supervisor.SupervisorError, "OUT_OF_SCOPE"):
            self.verify_delegation()

        self.delegation = load(FIXTURES / "valid-delegation.json")
        self.delegation["scope"]["ready_order"][0]["dependencies"] = [999]
        cycle = self.cycle()
        self.assertEqual(cycle["decision"]["state"], "INDETERMINATE")
        self.assertEqual(cycle["decision"]["reason_code"], "UNKNOWN_DEPENDENCY")

    def test_prompt_digest_and_domain_catalog_drift_are_detected(self) -> None:
        changed = copy.deepcopy(self.policy)
        changed["kickoff_prompt"]["sha256"] = "0" * 64
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError, "PROMPT_DIGEST_MISMATCH"
        ):
            governance_supervisor.validate_policy(changed, self.policy_bytes)

        changed = copy.deepcopy(self.policy)
        changed["responsibility_domains"].reverse()
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError, "DOMAIN_CATALOG_MISMATCH"
        ):
            governance_supervisor.validate_policy(changed, self.policy_bytes)


class ReconciliationTests(SupervisorTestCase):
    def test_selects_first_owner_approved_ready_item(self) -> None:
        cycle = self.cycle()
        self.assertEqual(cycle["decision"]["state"], "DISPATCH_REQUIRED")
        self.assertEqual(cycle["decision"]["selected_issue_number"], 101)
        packet = cycle["decision"]["dispatch_packet"]
        self.assertEqual(packet["wip_slot"], 1)
        self.assertEqual(
            packet["acceptance_manifest_digest"],
            self.delegation["scope"]["ready_order"][0]["acceptance_manifest"]["digest"],
        )
        self.assertEqual(packet["prohibitions"], self.policy["owner_only_actions"])
        self.assertEqual(packet["required_evidence"], self.policy["required_child_evidence"])
        self.assertNotIn("merge", packet["allowed_mutations"])

    def test_wip_one_active_item_per_domain(self) -> None:
        active = copy.deepcopy(self.item(101))
        active["issue_number"] = 103
        active["pull_request"] = None
        active["lifecycle"] = "ACTIVE"
        active["active"] = True
        active["acceptance_manifest_digest"] = "e" * 64
        self.snapshot["items"].append(active)
        self.add_worker(session_id="worker-103", issue_number=103)
        cycle = self.cycle()
        self.assertEqual(cycle["decision"]["state"], "DISPATCH_REQUIRED")
        self.assertEqual(cycle["decision"]["selected_issue_number"], 102)

    def test_stale_or_nonexecuting_session_does_not_occupy_wip(self) -> None:
        active = copy.deepcopy(self.item(101))
        active["issue_number"] = 103
        active["pull_request"] = None
        active["lifecycle"] = "ACTIVE"
        active["active"] = True
        active["acceptance_manifest_digest"] = "e" * 64
        self.snapshot["items"].append(active)
        self.add_worker(
            session_id="worker-103",
            issue_number=103,
            cycle_id="cycle-older-0001",
            executing=False,
        )
        cycle = self.cycle()
        self.assertEqual(cycle["decision"]["state"], "DISPATCH_REQUIRED")
        self.assertEqual(cycle["decision"]["selected_issue_number"], 102)

    def test_prior_cycle_executing_worker_occupies_current_wip(self) -> None:
        active = copy.deepcopy(self.item(101))
        active["issue_number"] = 103
        active["pull_request"] = None
        active["lifecycle"] = "ACTIVE"
        active["active"] = True
        active["acceptance_manifest_digest"] = "e" * 64
        self.snapshot["items"].append(active)
        self.add_worker(
            session_id="worker-103",
            issue_number=103,
            cycle_id="cycle-older-0001",
            executing=True,
        )
        cycle = self.cycle()
        self.assertEqual(cycle["decision"]["state"], "DISPATCH_REQUIRED")
        self.assertEqual(cycle["decision"]["selected_issue_number"], 102)

    def test_all_ready_entries_are_validated_before_selection(self) -> None:
        self.item(102)["domain"] = "programming-language-model-generation"
        cycle = self.cycle()
        self.assertEqual(cycle["decision"]["state"], "BLOCKED")
        self.assertEqual(cycle["decision"]["reason_code"], "OUT_OF_SCOPE")
        self.assertIsNone(cycle["decision"]["selected_issue_number"])

    def test_idle_child_is_resumed_by_exact_session_id(self) -> None:
        competing = copy.deepcopy(self.item(101))
        competing["issue_number"] = 103
        competing["pull_request"] = None
        competing["acceptance_manifest_digest"] = "e" * 64
        self.snapshot["items"].append(competing)
        self.delegation["scope"]["issues"].append(103)
        for entry in self.delegation["scope"]["ready_order"]:
            entry["position"] += 1
        competing_ready = copy.deepcopy(self.delegation["scope"]["ready_order"][0])
        competing_ready.update(
            issue_number=103,
            pull_request=None,
            position=1,
            dependencies=[],
        )
        competing_ready["acceptance_manifest"]["digest"] = "e" * 64
        self.delegation["scope"]["ready_order"].insert(0, competing_ready)
        self.add_worker(state="IDLE")
        cycle = self.cycle()
        self.assertEqual(cycle["decision"]["action"], "RESUME_CHILD")
        self.assertEqual(cycle["decision"]["selected_issue_number"], 101)
        self.assertEqual(
            cycle["decision"]["dispatch_packet"]["target_session_id"],
            "worker-101",
        )

    def test_missing_or_stale_live_session_facts_fail_closed(self) -> None:
        self.add_worker(session_id="worker-103", issue_number=103)
        cycle = self.cycle()
        self.assertEqual(cycle["decision"]["reason_code"], "MISSING_SESSION_FACTS")

        self.inventory = load(FIXTURES / "session-inventory.json")
        worker = self.add_worker()
        worker["policy_commit"] = "b" * 40
        cycle = self.cycle()
        self.assertEqual(cycle["decision"]["reason_code"], "STALE_SESSION_BINDING")

    def test_invalid_session_identifiers_are_rejected_without_crashing(self) -> None:
        worker = self.add_worker()
        worker["issue_number"] = {"bad": 1}
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError, "UNSUPPORTED_INPUT"
        ):
            self.cycle()

    def test_observe_policy_does_not_accept_projected_replacement(self) -> None:
        self.add_worker(state="BLOCKED", executing=False)
        first = self.operational_receipt_cycle(self.cycle())
        self.assertEqual(first["decision"]["action"], "REPLACE_CHILD")
        self.add_worker(
            session_id="worker-101-replacement",
            cycle_id=first["cycle_id"],
            decision_id=first["decision"]["decision_id"],
            redirects=1,
        )
        second = self.cycle(
            sequence=2,
            previous_record=first,
        )
        self.assertEqual(
            second["decision"]["reason_code"],
            "PRIOR_DISPATCH_UNRECEIPTED",
        )
        self.assertIsNone(second["decision"]["selected_issue_number"])
        self.assertEqual(second["decision"]["action"], "NONE")

    def test_known_blocked_dependency_selects_next_independent_ready_item(self) -> None:
        dependency = self.item(100)
        dependency["lifecycle"] = "ACTIVE"
        dependency["active"] = True
        cycle = self.cycle()
        self.assertEqual(cycle["decision"]["state"], "DISPATCH_REQUIRED")
        self.assertEqual(cycle["decision"]["selected_issue_number"], 102)

    def test_dirty_worktree_is_never_exact_head_evidence(self) -> None:
        item = self.item(101)
        item["lifecycle"] = "ACTIVE"
        item["active"] = True
        item["head_sha"] = "5" * 40
        self.add_worker(state="EVIDENCE_READY", executing=False, dirty=True)
        cycle = self.cycle()
        self.assertEqual(cycle["decision"]["state"], "INDETERMINATE")
        self.assertEqual(cycle["decision"]["reason_code"], "DIRTY_WORKTREE_NOT_EVIDENCE")

    def test_head_advance_invalidates_evidence_and_review(self) -> None:
        item = self.item(101)
        item.update(
            lifecycle="REVIEW",
            active=True,
            head_sha="6" * 40,
            evidence_verified=True,
            evidence_head_sha="5" * 40,
            reviews_current=True,
            approval_head_sha="5" * 40,
        )
        self.add_worker(
            state="REVIEW_WAIT",
            executing=False,
            issue_number=101,
            head_sha="5" * 40,
        )
        cycle = self.cycle()
        self.assertEqual(cycle["decision"]["state"], "INDETERMINATE")
        self.assertEqual(cycle["decision"]["reason_code"], "STALE_HEAD")

    def test_child_success_without_independent_evidence_is_blocked(self) -> None:
        item = self.item(101)
        item.update(lifecycle="ACTIVE", active=True, head_sha="5" * 40)
        self.add_worker(success_claimed=True)
        cycle = self.cycle()
        self.assertEqual(cycle["decision"]["state"], "BLOCKED")
        self.assertEqual(cycle["decision"]["reason_code"], "CHILD_SUCCESS_UNVERIFIED")

    def test_session_state_never_advances_repository_lifecycle(self) -> None:
        item = self.item(101)
        item["lifecycle"] = "READY"
        self.add_worker(state="RUNNING")
        cycle = self.cycle()
        lifecycle = next(
            value
            for value in cycle["repository_lifecycle"]
            if value["issue_number"] == 101
        )
        self.assertEqual(lifecycle["observed_state"], "READY")
        self.assertNotIn("derived_state", lifecycle)

    def test_reconciliation_is_idempotent_for_identical_durable_facts(self) -> None:
        first = self.cycle()
        second = self.cycle()
        self.assertEqual(first, second)

    def test_unreceipted_prior_dispatch_blocks_next_cycle(self) -> None:
        first = self.cycle()
        second = self.cycle(
            sequence=2,
            previous_record=first,
        )
        self.assertEqual(
            second["decision"]["reason_code"],
            "PRIOR_DISPATCH_UNRECEIPTED",
        )

    def test_unreceipted_dispatch_remains_sticky_across_successor_cycles(self) -> None:
        first = self.cycle()
        second = self.cycle(sequence=2, previous_record=first)
        third = self.cycle(sequence=3, previous_record=second)
        self.assertEqual(
            third["decision"]["reason_code"],
            "PRIOR_DISPATCH_UNRECEIPTED",
        )
        self.assertEqual(third["pending_dispatch"], first["pending_dispatch"])

    def test_pending_dispatch_requires_exact_received_worker_observation(self) -> None:
        first = self.cycle()
        self.add_worker(
            issue_number=102,
            cycle_id=first["cycle_id"],
            decision_id=first["decision"]["decision_id"],
        )
        second = self.cycle(sequence=2, previous_record=first)
        self.assertEqual(
            second["decision"]["reason_code"],
            "PRIOR_DISPATCH_UNRECEIPTED",
        )

    def test_observe_policy_cannot_clear_fabricated_operational_dispatch(self) -> None:
        first = self.cycle()
        candidate = copy.deepcopy(first)
        candidate.pop("audit")
        candidate["authority"]["delegated_operational"] = True
        fabricated = governance_supervisor.seal_record(candidate)
        self.last_cycle = fabricated
        self.add_worker(
            cycle_id=fabricated["cycle_id"],
            decision_id=fabricated["decision"]["decision_id"],
        )
        second = self.cycle(sequence=2, previous_record=fabricated)
        self.assertEqual(
            second["decision"]["reason_code"],
            "PRIOR_DISPATCH_UNRECEIPTED",
        )

    def test_successor_cycle_id_is_bound_to_predecessor(self) -> None:
        first = self.cycle()
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError, "CYCLE_ID_MISMATCH"
        ):
            self.cycle(
                cycle_id=self.delegation["initial_cycle_id"],
                sequence=2,
                previous_record=first,
            )

    def test_successor_requires_predecessor_and_stable_supervisor(self) -> None:
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError, "MISSING_PREVIOUS_RECORD"
        ):
            self.cycle(cycle_id="cycle-missing-predecessor", sequence=2)

        first = self.cycle()
        self.inventory["supervisor_session_id"] = "different-supervisor"
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError, "SUPERVISOR_IDENTITY_CHANGED"
        ):
            self.cycle(sequence=2, previous_record=first)

    def test_successor_cycle_identity_changes_with_durable_inputs(self) -> None:
        first = self.cycle()
        second_id = governance_supervisor.next_cycle_id(
            first,
            2,
            delegation=self.delegation,
            delegation_binding=self.delegation_binding(),
            snapshot=self.snapshot,
            inventory=self.inventory,
            recovery_record=None,
        )
        changed_snapshot = copy.deepcopy(self.snapshot)
        changed_snapshot["items"][2]["lifecycle"] = "BLOCKED"
        changed_id = governance_supervisor.next_cycle_id(
            first,
            2,
            delegation=self.delegation,
            delegation_binding=self.delegation_binding(),
            snapshot=changed_snapshot,
            inventory=self.inventory,
            recovery_record=None,
        )
        self.assertNotEqual(second_id, changed_id)

    def test_explicit_recovery_can_continue_an_unreceipted_dispatch(self) -> None:
        first = self.cycle()
        recovery = governance_supervisor.create_recovery_record(
            self.delegation,
            cause="CRASH_RESTART",
            state="PLANNED",
            previous_records=[first],
            inventory=self.inventory,
            delegation_binding=self.delegation_binding(),
            now=NOW,
        )
        second = self.cycle(
            sequence=2,
            previous_record=first,
            recovery_record=recovery,
        )
        self.assertEqual(second["decision"]["action"], "CREATE_CHILD")

    def test_unknown_worker_state_fails_closed(self) -> None:
        self.add_worker(state="UNKNOWN", executing=False)
        cycle = self.cycle()
        self.assertEqual(cycle["decision"]["reason_code"], "MISSING_SESSION_STATE")

    def test_stale_or_future_inventory_cannot_dispatch(self) -> None:
        for observed_at, reason in (
            ("2026-02-01T10:59:59Z", "SESSION_INVENTORY_STALE"),
            ("2026-02-01T12:00:01Z", "SESSION_INVENTORY_FROM_FUTURE"),
        ):
            with self.subTest(reason=reason):
                self.inventory = load(FIXTURES / "session-inventory.json")
                self.inventory["observed_at"] = observed_at
                cycle = self.cycle()
                self.assertEqual(cycle["decision"]["state"], "INDETERMINATE")
                self.assertEqual(cycle["decision"]["reason_code"], reason)
                self.assertEqual(cycle["decision"]["action"], "NONE")

    def test_terminal_or_blocked_head_does_not_stall_independent_ready_work(self) -> None:
        for lifecycle in ("MERGED", "BLOCKED", "PARKED"):
            with self.subTest(lifecycle=lifecycle):
                self.snapshot = load(FIXTURES / "repository-snapshot.json")
                self.item(101)["lifecycle"] = lifecycle
                cycle = self.cycle()
                self.assertEqual(cycle["decision"]["selected_issue_number"], 102)
                self.assertEqual(cycle["decision"]["action"], "CREATE_CHILD")

    def test_redirect_counters_cannot_reset_between_cycles(self) -> None:
        worker = self.add_worker(redirects=1)
        self.item(102)["lifecycle"] = "BLOCKED"
        first = self.cycle()
        worker["redirects"] = 0
        second = self.cycle(
            sequence=2,
            previous_record=first,
        )
        self.assertEqual(
            second["decision"]["reason_code"],
            "REDIRECT_COUNTER_REGRESSION",
        )

    def test_emitted_redirect_increments_durable_high_water(self) -> None:
        item = self.item(101)
        item.update(lifecycle="ACTIVE", active=True, head_sha="5" * 40)
        self.add_worker(state="FAILED", redirects=0)
        cycle = self.cycle()
        self.assertEqual(cycle["decision"]["action"], "REDIRECT_CHILD")
        self.assertEqual(
            cycle["budget"]["redirect_counts"],
            [{"issue_number": 101, "count": 1}],
        )
        self.assertEqual(cycle["budget"]["redirects_used"], 1)

    def test_snapshot_dependencies_participate_in_ready_selection(self) -> None:
        self.item(101)["dependencies"] = [102]
        cycle = self.cycle()
        self.assertEqual(cycle["decision"]["selected_issue_number"], 102)

    def test_ready_pr_binding_is_exact(self) -> None:
        self.item(101)["pull_request"] = 452
        cycle = self.cycle()
        self.assertEqual(cycle["decision"]["reason_code"], "OUT_OF_SCOPE")

    def test_malformed_dependencies_fail_closed(self) -> None:
        for dependencies in ([{"issue": 100}], [True], [100, 100], [0], [-1]):
            with self.subTest(dependencies=dependencies):
                self.snapshot = load(FIXTURES / "repository-snapshot.json")
                self.item(101)["dependencies"] = dependencies
                with self.assertRaisesRegex(
                    governance_supervisor.SupervisorError, "UNSUPPORTED_INPUT"
                ):
                    self.cycle()

    def test_dirty_executing_item_does_not_stall_independent_ready_work(self) -> None:
        self.add_worker(dirty=True)
        cycle = self.cycle()
        self.assertEqual(cycle["decision"]["selected_issue_number"], 102)

    def test_evidence_ready_requires_non_null_exact_head(self) -> None:
        self.item(101).update(lifecycle="ACTIVE", active=True)
        worker = self.add_worker(
            state="EVIDENCE_READY",
            executing=False,
        )
        worker["head_sha"] = None
        cycle = self.cycle()
        self.assertEqual(cycle["decision"]["reason_code"], "MISSING_EXACT_HEAD")

    def test_review_and_approved_require_non_null_exact_head(self) -> None:
        for lifecycle in ("REVIEW", "APPROVED"):
            with self.subTest(lifecycle=lifecycle):
                self.snapshot = load(FIXTURES / "repository-snapshot.json")
                self.item(101).update(
                    lifecycle=lifecycle,
                    checks_passed=True,
                    evidence_verified=True,
                    reviews_current=True,
                )
                cycle = self.cycle()
                self.assertEqual(
                    cycle["decision"]["reason_code"],
                    "MISSING_EXACT_HEAD",
                )

    def test_review_states_require_worker_and_repository_head_match(self) -> None:
        for state in ("EVIDENCE_READY", "REVIEW_WAIT"):
            with self.subTest(state=state):
                self.snapshot = load(FIXTURES / "repository-snapshot.json")
                self.inventory = load(FIXTURES / "session-inventory.json")
                self.item(101).update(
                    lifecycle="REVIEW" if state == "REVIEW_WAIT" else "ACTIVE",
                    active=True,
                    head_sha="5" * 40,
                    checks_passed=True,
                    evidence_verified=True,
                    evidence_head_sha="5" * 40,
                    reviews_current=True,
                )
                self.add_worker(
                    state=state,
                    executing=False,
                    head_sha="6" * 40,
                )
                cycle = self.cycle()
                self.assertEqual(cycle["decision"]["reason_code"], "STALE_HEAD")

    def test_approved_merge_requires_current_worker_head_match(self) -> None:
        for worker_head in (None, "6" * 40):
            with self.subTest(worker_head=worker_head):
                self.snapshot = load(FIXTURES / "repository-snapshot.json")
                self.inventory = load(FIXTURES / "session-inventory.json")
                self.item(101).update(
                    lifecycle="APPROVED",
                    active=True,
                    head_sha="5" * 40,
                    checks_passed=True,
                    evidence_verified=True,
                    evidence_head_sha="5" * 40,
                    reviews_current=True,
                    approval_head_sha="5" * 40,
                )
                worker = self.add_worker(
                    state="REVIEW_WAIT",
                    executing=False,
                    head_sha=worker_head or "5" * 40,
                )
                worker["head_sha"] = worker_head
                cycle = self.cycle()
                self.assertEqual(
                    cycle["decision"]["reason_code"],
                    "MISSING_EXACT_HEAD",
                )

    def test_missing_session_for_active_item_is_unknown(self) -> None:
        item = self.item(101)
        item.update(lifecycle="ACTIVE", active=True, head_sha="5" * 40)
        cycle = self.cycle()
        self.assertEqual(cycle["decision"]["state"], "UNKNOWN")
        self.assertEqual(cycle["decision"]["reason_code"], "MISSING_SESSION_STATE")

    def test_failed_checks_and_audit_write_failure_stop_execution(self) -> None:
        item = self.item(101)
        item.update(
            lifecycle="REVIEW",
            active=True,
            head_sha="5" * 40,
            evidence_verified=True,
            evidence_head_sha="5" * 40,
        )
        cycle = self.cycle()
        self.assertEqual(cycle["decision"]["reason_code"], "FAILED_CHECKS")

        self.snapshot = load(FIXTURES / "repository-snapshot.json")
        self.snapshot["audit_writable"] = False
        cycle = self.cycle()
        self.assertEqual(cycle["decision"]["reason_code"], "AUDIT_WRITE_UNAVAILABLE")

    def test_stale_or_future_snapshot_stops_reconciliation(self) -> None:
        self.snapshot["observed_at"] = "2026-02-01T11:54:59Z"
        cycle = self.cycle()
        self.assertEqual(cycle["decision"]["reason_code"], "SNAPSHOT_STALE")

        self.snapshot["observed_at"] = "2026-02-01T12:00:01Z"
        cycle = self.cycle()
        self.assertEqual(cycle["decision"]["reason_code"], "SNAPSHOT_FROM_FUTURE")

    def test_exact_child_session_limit_blocks_new_dispatch(self) -> None:
        self.delegation["limits"]["max_child_sessions"] = 1
        self.add_worker(session_id="worker-102", issue_number=102)
        cycle = self.cycle()
        self.assertEqual(
            cycle["decision"]["reason_code"],
            "CHILD_SESSION_LIMIT_EXHAUSTED",
        )

    def test_session_creation_limit_survives_closed_history(self) -> None:
        self.delegation["limits"]["max_session_creations"] = 1
        self.add_worker(
            session_id="worker-102-closed",
            issue_number=102,
            state="CLOSED",
        )
        cycle = self.cycle()
        self.assertEqual(
            cycle["decision"]["reason_code"],
            "SESSION_CREATION_LIMIT_EXHAUSTED",
        )

    def test_session_creation_limit_survives_archived_inventory(self) -> None:
        self.delegation["limits"]["max_session_creations"] = 1
        first = self.cycle()
        recovery = governance_supervisor.create_recovery_record(
            self.delegation,
            cause="CRASH_RESTART",
            state="PLANNED",
            previous_records=[first],
            inventory=self.inventory,
            delegation_binding=self.delegation_binding(),
            now=NOW,
        )
        second = self.cycle(
            sequence=2,
            previous_record=first,
            recovery_record=recovery,
        )
        self.assertEqual(
            second["decision"]["reason_code"],
            "SESSION_CREATION_LIMIT_EXHAUSTED",
        )

    def test_redirect_limit_is_summed_across_item_history(self) -> None:
        item = self.item(101)
        item.update(lifecycle="ACTIVE", active=True, head_sha="5" * 40)
        self.add_worker(
            session_id="worker-101-old",
            state="CLOSED",
            redirects=1,
        )
        self.add_worker(state="FAILED", redirects=1)
        cycle = self.cycle()
        self.assertEqual(cycle["decision"]["reason_code"], "REDIRECT_LIMIT_EXHAUSTED")

    def test_stale_review_stops_owner_decision(self) -> None:
        item = self.item(101)
        item.update(
            lifecycle="APPROVED",
            active=True,
            head_sha="5" * 40,
            checks_passed=True,
            evidence_verified=True,
            evidence_head_sha="5" * 40,
            reviews_current=False,
            approval_head_sha="5" * 40,
        )
        cycle = self.cycle()
        self.assertEqual(cycle["decision"]["reason_code"], "STALE_REVIEW")

    def test_redirect_session_time_and_aic_limits(self) -> None:
        item = self.item(101)
        item.update(lifecycle="ACTIVE", active=True, head_sha="5" * 40)
        self.add_worker(state="FAILED", redirects=2)
        cycle = self.cycle()
        self.assertEqual(cycle["decision"]["reason_code"], "REDIRECT_LIMIT_EXHAUSTED")

        self.snapshot = load(FIXTURES / "repository-snapshot.json")
        self.inventory = load(FIXTURES / "session-inventory.json")
        self.snapshot["elapsed_seconds"] = 3600
        cycle = self.cycle()
        self.assertEqual(cycle["decision"]["reason_code"], "CYCLE_TIME_BUDGET_EXHAUSTED")

        self.snapshot = load(FIXTURES / "repository-snapshot.json")
        self.delegation["limits"]["platform_reported_aic_guardrail"] = {
            "enabled": True,
            "max_aic": 30,
            "source": "github-copilot-platform",
        }
        self.snapshot["platform_reported_aic"] = {
            "reported": True,
            "value": 30,
            "measurement_scope": "current-cycle-total",
        }
        cycle = self.cycle()
        self.assertEqual(cycle["decision"]["reason_code"], "AIC_BUDGET_EXHAUSTED")

    def test_supervisor_cannot_count_as_worker(self) -> None:
        item = self.item(101)
        item.update(lifecycle="ACTIVE", active=True, head_sha="5" * 40)
        self.add_worker(session_id=self.inventory["supervisor_session_id"])
        cycle = self.cycle()
        self.assertEqual(cycle["decision"]["reason_code"], "SUPERVISOR_SELF_DISPATCH")

    def test_approved_head_requests_owner_decision_not_merge(self) -> None:
        item = self.item(101)
        item.update(
            lifecycle="APPROVED",
            active=True,
            head_sha="5" * 40,
            checks_passed=True,
            evidence_verified=True,
            evidence_head_sha="5" * 40,
            reviews_current=True,
            approval_head_sha="5" * 40,
        )
        self.add_worker(
            state="REVIEW_WAIT",
            executing=False,
            issue_number=101,
            head_sha="5" * 40,
        )
        cycle = self.cycle()
        self.assertEqual(cycle["decision"]["state"], "REVIEW_WAIT")
        self.assertTrue(cycle["decision"]["owner_decision_required"])
        self.assertEqual(cycle["decision"]["action"], "NONE")


class DispatchAndDurabilityTests(SupervisorTestCase):
    def test_observe_mode_dispatch_grants_no_mutation_tools(self) -> None:
        cycle = self.cycle()
        packet = cycle["decision"]["dispatch_packet"]
        self.assertNotIn("git-push", packet["allowed_tools"])
        self.assertEqual(packet["allowed_tools"], ["repository-read"])
        self.assertEqual(packet["allowed_mutations"], [])
        self.assertFalse(cycle["authority"]["delegated_operational"])
        self.assertTrue(
            packet["mutation_constraints"]["allowed_branch_ref"].startswith(
                "refs/heads/copilot-supervisor/issue-101-"
            )
        )
        self.assertFalse(packet["mutation_constraints"]["force_push"])
        self.assertIn(
            "refs/tags/",
            packet["mutation_constraints"]["denied_ref_prefixes"],
        )
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError, "OBSERVE_ONLY"
        ):
            governance_supervisor.create_dispatch_receipt(
                cycle,
                self.inventory,
                policy=getattr(self, "receipt_policy", self.policy),
                delegation=self.delegation,
                delegation_binding=self.delegation_binding(),
                child_session_id="worker-101",
                now=NOW,
            )

    def test_same_cycle_non_supervisor_dispatch_receipt(self) -> None:
        cycle = self.operational_receipt_cycle(self.cycle())
        packet = cycle["decision"]["dispatch_packet"]
        self.add_worker(
            cycle_id=cycle["cycle_id"],
            decision_id=cycle["decision"]["decision_id"],
        )
        receipt = governance_supervisor.create_dispatch_receipt(
            cycle,
            self.inventory,
            policy=getattr(self, "receipt_policy", self.policy),
            delegation=self.delegation,
            delegation_binding=self.delegation_binding(),
            child_session_id="worker-101",
            now=NOW,
        )
        self.assertEqual(receipt["state"], "RUNNING")
        self.assertEqual(receipt["issue_number"], packet["issue_number"])
        self.assertEqual(receipt["dispatched_head_sha"], packet["launch_head_sha"])
        self.assertEqual(
            receipt["dispatched_branch_ref"],
            packet["mutation_constraints"]["allowed_branch_ref"],
        )
        governance_supervisor.verify_record(receipt)

    def test_nonexecuting_child_cannot_satisfy_receipt(self) -> None:
        cycle = self.operational_receipt_cycle(self.cycle())
        self.add_worker(
            cycle_id=cycle["cycle_id"],
            decision_id=cycle["decision"]["decision_id"],
            executing=False,
        )
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError, "DISPATCH_RECEIPT_(?:MISMATCH|COLLISION)"
        ):
            governance_supervisor.create_dispatch_receipt(
                cycle,
                self.inventory,
                policy=getattr(self, "receipt_policy", self.policy),
                delegation=self.delegation,
                delegation_binding=self.delegation_binding(),
                child_session_id="worker-101",
                now=NOW,
            )

    def test_receipt_rejects_wrong_dispatch_binding_or_head(self) -> None:
        item = self.item(101)
        item.update(lifecycle="ACTIVE", active=True, head_sha="5" * 40)
        worker = self.add_worker(state="FAILED")
        cycle = self.operational_receipt_cycle(self.cycle())
        worker.update(
            state="RUNNING",
            executing=True,
            cycle_id=cycle["cycle_id"],
            decision_id=cycle["decision"]["decision_id"],
            cycle_record_digest=cycle["audit"]["payload_digest"],
            dispatch_packet_digest=governance_supervisor.digest_json(
                cycle["decision"]["dispatch_packet"]
            ),
            branch_ref=cycle["decision"]["dispatch_packet"][
                "mutation_constraints"
            ]["allowed_branch_ref"],
        )
        worker["head_sha"] = "6" * 40
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError,
            "DISPATCH_RECEIPT_HEAD_MISMATCH",
        ):
            governance_supervisor.create_dispatch_receipt(
                cycle,
                self.inventory,
                policy=getattr(self, "receipt_policy", self.policy),
                delegation=self.delegation,
                delegation_binding=self.delegation_binding(),
                child_session_id="worker-101",
                now=NOW,
            )

        worker["dispatch_packet_digest"] = governance_supervisor.digest_json(
            cycle["decision"]["dispatch_packet"]
        )
        worker["branch_ref"] = "refs/heads/wrong-scope"
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError,
            "DISPATCH_RECEIPT_MISMATCH",
        ):
            governance_supervisor.create_dispatch_receipt(
                cycle,
                self.inventory,
                policy=getattr(self, "receipt_policy", self.policy),
                delegation=self.delegation,
                delegation_binding=self.delegation_binding(),
                child_session_id="worker-101",
                now=NOW,
            )

        worker["head_sha"] = "5" * 40
        worker["dispatch_packet_digest"] = "0" * 64
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError,
            "DISPATCH_RECEIPT_COLLISION",
        ):
            governance_supervisor.create_dispatch_receipt(
                cycle,
                self.inventory,
                policy=getattr(self, "receipt_policy", self.policy),
                delegation=self.delegation,
                delegation_binding=self.delegation_binding(),
                child_session_id="worker-101",
                now=NOW,
            )

    def test_recovery_cannot_clear_observed_pending_worker(self) -> None:
        cycle = self.cycle()
        self.add_worker(
            cycle_id=cycle["cycle_id"],
            decision_id=cycle["decision"]["decision_id"],
        )
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError,
            "RECOVERY_DISPATCH_STILL_OBSERVED",
        ):
            governance_supervisor.create_recovery_record(
                self.delegation,
                cause="CRASH_RESTART",
                state="PLANNED",
                previous_records=[cycle],
                inventory=self.inventory,
                delegation_binding=self.delegation_binding(),
                now=NOW,
            )

    def test_duplicate_children_cannot_satisfy_one_dispatch(self) -> None:
        cycle = self.operational_receipt_cycle(self.cycle())
        for session_id in ("worker-101-a", "worker-101-b"):
            self.add_worker(
                session_id=session_id,
                cycle_id=cycle["cycle_id"],
                decision_id=cycle["decision"]["decision_id"],
            )
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError, "DISPATCH_RECEIPT_COLLISION"
        ):
            governance_supervisor.create_dispatch_receipt(
                cycle,
                self.inventory,
                policy=getattr(self, "receipt_policy", self.policy),
                delegation=self.delegation,
                delegation_binding=self.delegation_binding(),
                child_session_id="worker-101-a",
                now=NOW,
            )

    def test_receipt_revalidates_delegation_expiry(self) -> None:
        cycle = self.operational_receipt_cycle(self.cycle())
        self.add_worker(
            cycle_id=cycle["cycle_id"],
            decision_id=cycle["decision"]["decision_id"],
        )
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError, "DELEGATION_EXPIRED"
        ):
            governance_supervisor.create_dispatch_receipt(
                cycle,
                self.inventory,
                policy=getattr(self, "receipt_policy", self.policy),
                delegation=self.delegation,
                delegation_binding=self.delegation_binding(),
                child_session_id="worker-101",
                now=governance_supervisor.parse_timestamp(
                    self.delegation["expires_at"]
                ),
            )

    def test_receipt_rejects_delegation_mutation(self) -> None:
        cycle = self.operational_receipt_cycle(self.cycle())
        self.add_worker(
            cycle_id=cycle["cycle_id"],
            decision_id=cycle["decision"]["decision_id"],
        )
        self.delegation["expires_at"] = "2026-12-01T00:00:00Z"
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError, "RECEIPT_AUTHORITY_MISMATCH"
        ):
            governance_supervisor.create_dispatch_receipt(
                cycle,
                self.inventory,
                policy=getattr(self, "receipt_policy", self.policy),
                delegation=self.delegation,
                delegation_binding=self.delegation_binding(),
                child_session_id="worker-101",
                now=NOW,
            )

    def test_record_schema_rejects_owner_only_allowed_mutation(self) -> None:
        cycle = self.cycle()
        invalid_values = {
            "allowed_mutations": ["merge"],
            "allowed_tools": ["shell"],
        }
        for field, value in invalid_values.items():
            with self.subTest(field=field):
                candidate = copy.deepcopy(cycle)
                candidate.pop("audit")
                candidate["decision"]["dispatch_packet"][field] = value
                with self.assertRaisesRegex(
                    governance_schema.SchemaError, "failed schema validation"
                ):
                    governance_supervisor.seal_record(candidate)

    def test_supervisor_and_wrong_cycle_cannot_satisfy_receipt(self) -> None:
        cycle = self.operational_receipt_cycle(self.cycle())
        self.add_worker(
            session_id=self.inventory["supervisor_session_id"],
            cycle_id=cycle["cycle_id"],
            decision_id=cycle["decision"]["decision_id"],
        )
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError, "SUPERVISOR_SELF_DISPATCH"
        ):
            governance_supervisor.create_dispatch_receipt(
                cycle,
                self.inventory,
                policy=getattr(self, "receipt_policy", self.policy),
                delegation=self.delegation,
                delegation_binding=self.delegation_binding(),
                child_session_id=self.inventory["supervisor_session_id"],
                now=NOW,
            )

        self.inventory = load(FIXTURES / "session-inventory.json")
        self.add_worker(
            cycle_id="cycle-wrong-0001",
            decision_id=cycle["decision"]["decision_id"],
        )
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError, "DISPATCH_RECEIPT_(?:MISMATCH|COLLISION)"
        ):
            governance_supervisor.create_dispatch_receipt(
                cycle,
                self.inventory,
                policy=getattr(self, "receipt_policy", self.policy),
                delegation=self.delegation,
                delegation_binding=self.delegation_binding(),
                child_session_id="worker-101",
                now=NOW,
            )

    def test_audit_record_is_immutable(self) -> None:
        cycle = self.cycle()
        governance_supervisor.verify_record(cycle)
        cycle["decision"]["reason_code"] = "ALTERED"
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError, "AUDIT_DIGEST_MISMATCH"
        ):
            governance_supervisor.verify_record(cycle)

    def test_audit_writer_never_overwrites_different_bytes(self) -> None:
        cycle = self.cycle()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "cycle.json"
            governance_supervisor._write_record(path, cycle)
            governance_supervisor._write_record(path, cycle)
            changed = copy.deepcopy(cycle)
            changed["decision"]["reason_code"] = "ALTERED"
            with self.assertRaisesRegex(
                governance_supervisor.SupervisorError, "AUDIT_RECORD_EXISTS"
            ):
                governance_supervisor._write_record(path, changed)

    def test_crash_recovery_uses_only_durable_facts(self) -> None:
        cycle = self.cycle()
        recovery = governance_supervisor.create_recovery_record(
            self.delegation,
            cause="CRASH_RESTART",
            state="PLANNED",
            previous_records=[cycle],
            inventory=self.inventory,
            delegation_binding=self.delegation_binding(),
            now=NOW,
        )
        self.assertEqual(
            recovery["reconstructed_from"],
            [
                "delegation",
                "cycle-records",
                "github-facts",
                "git-state",
                "session-inventory",
            ],
        )
        self.assertTrue(recovery["parked_and_failed_history_preserved"])
        self.assertEqual(
            recovery["audit"]["supersedes"],
            [cycle["audit"]["payload_digest"]],
        )
        governance_supervisor.verify_record(recovery, prior_records=[cycle])

    def test_wrong_record_kinds_fail_with_controlled_errors(self) -> None:
        cycle = self.cycle()
        recovery = governance_supervisor.create_recovery_record(
            self.delegation,
            cause="CRASH_RESTART",
            state="PLANNED",
            previous_records=[cycle],
            inventory=self.inventory,
            delegation_binding=self.delegation_binding(),
            now=NOW,
        )
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError,
            "RECOVERY_PREVIOUS_RECORD_INVALID",
        ):
            governance_supervisor.create_recovery_record(
                self.delegation,
                cause="CRASH_RESTART",
                state="PLANNED",
                previous_records=[recovery],
                inventory=self.inventory,
                delegation_binding=self.delegation_binding(),
                now=NOW,
            )
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError,
            "OWNER_DECISION_SOURCE_INVALID",
        ):
            governance_supervisor.create_owner_decision_packet(
                recovery,
                policy=self.policy,
                delegation=self.delegation,
                delegation_binding=self.delegation_binding(),
                issue_number=101,
                pull_request=451,
                exact_head_sha="5" * 40,
                requested_owner_decisions=["merge"],
                reason="Wrong record kind.",
                now=NOW,
            )
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError,
            "DISPATCH_RECEIPT_SOURCE_INVALID",
        ):
            governance_supervisor.create_dispatch_receipt(
                recovery,
                self.inventory,
                policy=self.policy,
                delegation=self.delegation,
                delegation_binding=self.delegation_binding(),
                child_session_id="worker-101",
                now=NOW,
            )

    def test_recovery_cannot_supersede_different_authority(self) -> None:
        cycle = self.cycle()
        foreign = copy.deepcopy(self.delegation)
        foreign["delegation_id"] = "delegation-foreign-001"
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError, "RECOVERY_AUTHORITY_MISMATCH"
        ):
            governance_supervisor.create_recovery_record(
                foreign,
                cause="CRASH_RESTART",
                state="PLANNED",
                previous_records=[cycle],
                inventory=self.inventory,
                delegation_binding=self.delegation_binding(),
                now=NOW,
            )

    def test_owner_packet_requests_but_never_makes_merge_or_release(self) -> None:
        self.item(101).update(
            lifecycle="APPROVED",
            active=True,
            head_sha="5" * 40,
            checks_passed=True,
            evidence_verified=True,
            evidence_head_sha="5" * 40,
            reviews_current=True,
            approval_head_sha="5" * 40,
        )
        self.item(102).update(
            lifecycle="MERGED",
            head_sha="4" * 40,
            checks_passed=True,
            evidence_verified=True,
            evidence_head_sha="4" * 40,
            reviews_current=True,
            approval_head_sha="4" * 40,
        )
        self.add_worker(
            state="REVIEW_WAIT",
            executing=False,
            issue_number=101,
            head_sha="5" * 40,
        )
        cycle = self.cycle()
        packet = governance_supervisor.create_owner_decision_packet(
            cycle,
            policy=self.policy,
            delegation=self.delegation,
            delegation_binding=self.delegation_binding(),
            issue_number=101,
            pull_request=451,
            exact_head_sha="5" * 40,
            requested_owner_decisions=["merge"],
            reason="Exact-head evidence is ready for the owner's decision.",
            now=NOW,
        )
        self.assertFalse(packet["supervisor_made_decision"])
        self.assertEqual(packet["requested_owner_decisions"], ["merge"])
        self.assertEqual(packet["source_cycle_id"], cycle["cycle_id"])
        self.assertEqual(
            packet["source_cycle_digest"],
            cycle["audit"]["payload_digest"],
        )
        self.assertEqual(packet["observed_lifecycle"], "APPROVED")
        governance_supervisor.verify_record(packet)

        release_packet = governance_supervisor.create_owner_decision_packet(
            cycle,
            policy=self.policy,
            delegation=self.delegation,
            delegation_binding=self.delegation_binding(),
            issue_number=102,
            pull_request=452,
            exact_head_sha="4" * 40,
            requested_owner_decisions=["publish-release"],
            reason="Merged exact head is eligible for an owner release decision.",
            now=NOW,
        )
        self.assertEqual(
            release_packet["requested_owner_decisions"],
            ["publish-release"],
        )
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError, "OWNER_DECISION_INVALID"
        ):
            governance_supervisor.create_owner_decision_packet(
                cycle,
                policy=self.policy,
                delegation=self.delegation,
                delegation_binding=self.delegation_binding(),
                issue_number=102,
                pull_request=452,
                exact_head_sha="4" * 40,
                requested_owner_decisions=["merge"],
                reason="A different item cannot borrow the selected decision.",
                now=NOW,
            )
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError, "OWNER_DECISION_INVALID"
        ):
            governance_supervisor.create_owner_decision_packet(
                cycle,
                policy=self.policy,
                delegation=self.delegation,
                delegation_binding=self.delegation_binding(),
                issue_number=101,
                pull_request=451,
                exact_head_sha="5" * 40,
                requested_owner_decisions=["approve-risk-exception"],
                reason="Merge readiness cannot authorize an unrelated exception.",
                now=NOW,
            )
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError, "OWNER_DECISION_INVALID"
        ):
            governance_supervisor.create_owner_decision_packet(
                cycle,
                policy=self.policy,
                delegation=self.delegation,
                delegation_binding=self.delegation_binding(),
                issue_number=101,
                pull_request=451,
                exact_head_sha="5" * 40,
                requested_owner_decisions=["set-priority"],
                reason="Merge readiness cannot authorize priority changes.",
                now=NOW,
            )
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError, "OWNER_DECISION_INVALID"
        ):
            governance_supervisor.create_owner_decision_packet(
                cycle,
                policy=self.policy,
                delegation=self.delegation,
                delegation_binding=self.delegation_binding(),
                issue_number=9999,
                pull_request=8888,
                exact_head_sha="f" * 40,
                requested_owner_decisions=["merge"],
                reason="Unbound facts must fail.",
                now=NOW,
            )

        self.snapshot = load(FIXTURES / "repository-snapshot.json")
        self.item(101)["head_sha"] = "5" * 40
        ready_cycle = self.cycle()
        with self.assertRaisesRegex(
            governance_supervisor.SupervisorError, "OWNER_DECISION_INVALID"
        ):
            governance_supervisor.create_owner_decision_packet(
                ready_cycle,
                policy=self.policy,
                delegation=self.delegation,
                delegation_binding=self.delegation_binding(),
                issue_number=101,
                pull_request=451,
                exact_head_sha="5" * 40,
                requested_owner_decisions=["merge"],
                reason="READY is not merge-eligible.",
                now=NOW,
            )

    def test_every_durable_record_schema_rejects_unknown_fields(self) -> None:
        cycle = self.cycle()
        schema = governance_schema.load_schema(RECORD_SCHEMA)
        cycle["unknown"] = True
        errors = governance_schema.validate(cycle, schema)
        self.assertTrue(errors)


class IntakeBoundaryTests(SupervisorTestCase):
    def test_workflow_internal_copilot_remains_zero_tool_observe_only(self) -> None:
        workflow = (
            ROOT / ".github" / "workflows" / "issue-intake.yml"
        ).read_text(encoding="utf-8")
        for fragment in (
            "contents: read",
            "copilot-requests: write",
            "--available-tools=",
            "--deny-tool='shell,write,read,url,memory'",
            "--disable-builtin-mcps",
            "--no-remote",
        ):
            self.assertIn(fragment, workflow)
        for forbidden in ("issues: write", "pull-requests: write", "contents: write"):
            self.assertNotIn(forbidden, workflow)
        self.assertEqual(
            self.policy["invariants"]["actions_workflow"],
            None,
        )
        self.assertTrue(
            set(self.policy["owner_only_actions"]).isdisjoint(
                self.policy["delegable_operational_actions"]
            )
        )


if __name__ == "__main__":
    unittest.main()
