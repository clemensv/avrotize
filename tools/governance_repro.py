"""Guarded bug reproduction engine for governed Avrotize CLI reports.

The engine turns an authorized issue into structured evidence. It never runs a
shell, never executes generated output, never compares natural-language text,
and never authorizes anything. Execution is bounded by an explicit checked-in
command policy (``.github/governance/repro-command-policy.json``).

Failure taxonomy
----------------
``PolicyBlocked``       Readiness, policy, or resource refusal. Evidence is still
                        produced with status ``BLOCKED`` and the CLI exits 0.
``InfrastructureError`` Corrupt policy/schema/catalog, unusable authorization, or
                        an internal defect. No evidence is claimed and the CLI
                        exits nonzero so the workflow fails visibly.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import shlex
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Sequence

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools import governance_intake, governance_schema  # noqa: E402  (path bootstrap above)

ENGINE_VERSION = "2"
SCHEMA_VERSION = 1
RECORD_KIND = "repro-evidence"

GOVERNANCE_DIR = REPO_ROOT / ".github" / "governance"
COMMANDS_JSON = REPO_ROOT / "avrotize" / "commands.json"
POLICY_PATH = GOVERNANCE_DIR / "repro-command-policy.json"
POLICY_SCHEMA = GOVERNANCE_DIR / "schemas" / "repro-command-policy.schema.json"
LABEL_CATALOG_PATH = GOVERNANCE_DIR / "repro-label-catalog.json"
LABEL_CATALOG_SCHEMA = GOVERNANCE_DIR / "schemas" / "repro-label-catalog.schema.json"
ISSUE_FORM_CONTRACT = GOVERNANCE_DIR / "issue-form-contract.json"
EVIDENCE_SCHEMA = GOVERNANCE_DIR / "schemas" / "repro-evidence-record.schema.json"

EXCERPT_CHARACTERS = 4000
EMPTY_DIGEST = hashlib.sha256(b"").hexdigest()

AUTHORITY_STATEMENT = (
    "Guarded reproduction records evidence only. It does not authorize implementation, merge, "
    "release, or repository mutation beyond governed reproduction state labels. Repository owner "
    "retains authority."
)

#: Characters that must never appear in a token that reaches the process argv.
_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_FIXTURE_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_METACHAR_RE = re.compile(r"[|&;<>()`$\\!*?\[\]{}~\"'#\n\r\t]")
_OPTION_NAME_RE = re.compile(r"^--[a-z][a-z0-9-]*$")
_ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")
_FENCE_RE = re.compile(r"```[A-Za-z0-9_.+-]*\r?\n(.*?)```", re.DOTALL)
_ATTACHMENT_RE = re.compile(
    r"\b(see attach|attached file|attachment|download (?:it |the )?from|available at http|"
    r"github\.com/user-attachments|gist\.github\.com)",
    re.IGNORECASE,
)
_URL_RE = re.compile(r"[a-zA-Z][a-zA-Z0-9+.-]*://")
_ABSOLUTE_PATH_RE = re.compile(r"^(?:/|~|[A-Za-z]:[\\/])")


class PolicyBlocked(Exception):
    """Readiness, policy, or resource refusal. Produces BLOCKED evidence."""

    def __init__(self, reason_code: str, reason: str) -> None:
        super().__init__(f"{reason_code}: {reason}")
        self.reason_code = reason_code
        self.reason = reason


class InfrastructureError(RuntimeError):
    """Corrupt configuration or internal defect. Never produces evidence."""


# ---------------------------------------------------------------------------
# Digests and loading
# ---------------------------------------------------------------------------


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(65536)
            if not chunk:
                break
            size += len(chunk)
            digest.update(chunk)
    return digest.hexdigest(), size


def _digest_of_file(path: Path, label: str) -> str:
    if not path.is_file():
        raise InfrastructureError(f"required {label} is missing: {path}")
    return _sha256_text(path.read_text(encoding="utf-8"))


def load_policy(policy_path: Path = POLICY_PATH) -> dict[str, Any]:
    """Load and deeply validate the command policy. Corrupt policy is infrastructure failure."""
    if not policy_path.is_file():
        raise InfrastructureError(f"reproduction command policy is missing: {policy_path}")
    try:
        policy = json.loads(policy_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise InfrastructureError(f"reproduction command policy is not valid JSON: {exc}") from exc
    try:
        governance_schema.validate_or_raise(policy, POLICY_SCHEMA, "reproduction command policy")
    except governance_schema.SchemaError as exc:
        raise InfrastructureError(str(exc)) from exc
    names = [entry["command"] for entry in policy["commands"]]
    if len(names) != len(set(names)):
        raise InfrastructureError("reproduction command policy declares duplicate commands")
    registry = _load_command_registry()
    unknown = sorted(set(names) - registry)
    if unknown:
        raise InfrastructureError(
            f"reproduction command policy allows commands absent from commands.json: {unknown}"
        )
    return policy


def _load_command_registry() -> set[str]:
    if not COMMANDS_JSON.is_file():
        raise InfrastructureError(f"command registry is missing: {COMMANDS_JSON}")
    try:
        data = json.loads(COMMANDS_JSON.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise InfrastructureError(f"command registry is not valid JSON: {exc}") from exc
    if not isinstance(data, list):
        raise InfrastructureError("command registry must be an array")
    return {item["command"] for item in data if isinstance(item, dict) and isinstance(item.get("command"), str)}


def load_label_catalog(catalog_path: Path = LABEL_CATALOG_PATH) -> dict[str, Any]:
    """Load and deeply validate the governed label catalog."""
    if not catalog_path.is_file():
        raise InfrastructureError(f"reproduction label catalog is missing: {catalog_path}")
    try:
        catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise InfrastructureError(f"reproduction label catalog is not valid JSON: {exc}") from exc
    try:
        governance_schema.validate_or_raise(catalog, LABEL_CATALOG_SCHEMA, "reproduction label catalog")
    except governance_schema.SchemaError as exc:
        raise InfrastructureError(str(exc)) from exc
    return catalog


def label_for_outcome(catalog: dict[str, Any], status: str) -> str:
    for label in catalog["labels"]:
        if label["outcome"] == status:
            return label["name"]
    raise InfrastructureError(f"label catalog has no label for outcome {status!r}")


# ---------------------------------------------------------------------------
# Token, fixture, and argv policy
# ---------------------------------------------------------------------------


def assert_token_safe(token: str, context: str) -> None:
    """Reject any token that could leave the argv boundary or reach a shell."""
    if _CONTROL_RE.search(token):
        raise PolicyBlocked("TOKEN_CONTROL_CHARACTER", f"{context} contains a control character")
    if "\x00" in token:
        raise PolicyBlocked("TOKEN_CONTROL_CHARACTER", f"{context} contains a NUL byte")
    if _METACHAR_RE.search(token):
        raise PolicyBlocked("TOKEN_SHELL_METACHARACTER", f"{context} contains a shell metacharacter")
    if "@" in token:
        raise PolicyBlocked("TOKEN_AT_SIGN", f"{context} contains '@', which can denote a response file or host")
    if _URL_RE.search(token):
        raise PolicyBlocked("TOKEN_URL", f"{context} contains a URL")
    if _ABSOLUTE_PATH_RE.search(token):
        raise PolicyBlocked("TOKEN_ABSOLUTE_PATH", f"{context} contains an absolute path")
    if ".." in token or "/" in token or "\\" in token:
        raise PolicyBlocked("TOKEN_PATH_REFERENCE", f"{context} contains a path reference")
    if "=" in token and token.startswith("-"):
        raise PolicyBlocked(
            "OPTION_ASSIGNMENT_FORM",
            f"{context} uses --flag=value form, which guarded reproduction does not accept",
        )


def assert_discarded_value_safe(token: str, context: str) -> None:
    """Reporter-supplied input/output paths are discarded, so only inert-data rules apply."""
    if _CONTROL_RE.search(token) or "\x00" in token:
        raise PolicyBlocked("TOKEN_CONTROL_CHARACTER", f"{context} contains a control character")


def unwrap_fenced_block(text: str) -> str:
    """Return the first fenced code block's content, or the text unchanged.

    GitHub Issue Forms wrap any ``render:``-ed textarea in a fenced block, so the
    invocation field always arrives fenced. Reporters also fence pasted input by
    hand. Both are unwrapped the same way.
    """
    match = _FENCE_RE.search(text or "")
    return match.group(1) if match else (text or "")


def extract_fixture(field_text: str) -> str:
    """Extract the inline reproducer from the issue field, preferring a fenced block."""
    if not isinstance(field_text, str) or not field_text.strip():
        raise PolicyBlocked("FIXTURE_MISSING", "the report supplies no minimal input")
    candidate = unwrap_fenced_block(field_text).strip("\n").strip()
    if not candidate:
        raise PolicyBlocked("FIXTURE_MISSING", "the report supplies no minimal input")
    single_token = len(candidate.split()) == 1
    if single_token and (_URL_RE.search(candidate) or "." in candidate or "/" in candidate or "\\" in candidate):
        raise PolicyBlocked(
            "FIXTURE_NOT_INLINE",
            "the minimal input names a file or URL instead of inline data",
        )
    if len(candidate) < 32 and _ATTACHMENT_RE.search(field_text):
        raise PolicyBlocked(
            "FIXTURE_NOT_INLINE",
            "the minimal input refers to an attachment or download instead of inline data",
        )
    return candidate


def validate_fixture(fixture: str, max_bytes: int) -> bytes:
    """Fixture data is never executed: only encoding, control characters, and size are policed."""
    try:
        data = fixture.encode("utf-8")
        data.decode("utf-8")
    except UnicodeError as exc:
        raise PolicyBlocked("FIXTURE_NOT_UTF8", f"the minimal input is not valid UTF-8: {exc}") from exc
    if _FIXTURE_CONTROL_RE.search(fixture):
        raise PolicyBlocked("FIXTURE_CONTROL_CHARACTER", "the minimal input contains control characters")
    if len(data) > max_bytes:
        raise PolicyBlocked(
            "FIXTURE_TOO_LARGE",
            f"the minimal input is {len(data)} bytes, above the {max_bytes} byte limit",
        )
    return data


def normalize_command_name(command_field: str) -> str:
    """Extract the CLI subcommand name from the reported command field."""
    text = unwrap_fenced_block(command_field or "").strip()
    if not text:
        raise PolicyBlocked("COMMAND_MISSING", "the report names no command")
    first = text.split(",")[0].strip()
    try:
        tokens = shlex.split(first)
    except ValueError as exc:
        raise PolicyBlocked("COMMAND_UNPARSABLE", f"the reported command cannot be parsed: {exc}") from exc
    tokens = [token for token in tokens if not token.startswith("-")]
    if not tokens:
        raise PolicyBlocked("COMMAND_MISSING", "the report names no command")
    if len(tokens) > 1 and tokens[0] in {"avrotize", "structurize"}:
        return tokens[1]
    return tokens[0]


def select_policy_command(policy: dict[str, Any], command_name: str) -> dict[str, Any]:
    for entry in policy["commands"]:
        if entry["command"] == command_name:
            return entry
    raise PolicyBlocked(
        "COMMAND_NOT_IN_POLICY",
        f"command {command_name!r} is not in the guarded reproduction allowlist; a maintainer must reproduce it manually",
    )


def build_argv(
    policy: dict[str, Any],
    command_spec: dict[str, Any],
    invocation: str,
    executable: str,
    fixture_path: Path,
    output_path: Path | None,
) -> tuple[list[str], list[str]]:
    """Build a fully policy-controlled argv from the reported invocation.

    Returns ``(argv, substitutions)``. Reporter-supplied input and output paths
    are discarded and replaced with workspace-controlled paths.
    """
    limits = policy["limits"]
    try:
        tokens = shlex.split(unwrap_fenced_block(invocation).strip())
    except ValueError as exc:
        raise PolicyBlocked("INVOCATION_UNPARSABLE", f"the invocation cannot be parsed: {exc}") from exc
    if not tokens:
        raise PolicyBlocked("INVOCATION_MISSING", "the report supplies no invocation")
    if len(tokens) > limits["max_invocation_tokens"]:
        raise PolicyBlocked(
            "INVOCATION_TOO_LONG",
            f"the invocation has {len(tokens)} tokens, above the {limits['max_invocation_tokens']} token limit",
        )
    if tokens[0] not in policy["executables"]:
        raise PolicyBlocked(
            "EXECUTABLE_NOT_ALLOWED",
            f"invocation must start with one of {policy['executables']}, got {tokens[0]!r}",
        )
    if len(tokens) < 2:
        raise PolicyBlocked("INVOCATION_MISSING_COMMAND", "the invocation names no subcommand")
    if tokens[1] != command_spec["command"]:
        raise PolicyBlocked(
            "COMMAND_MISMATCH",
            f"invocation subcommand {tokens[1]!r} does not match reported command {command_spec['command']!r}",
        )

    reporter_options = {option["name"]: option for option in command_spec["reporter_options"]}
    input_aliases = set(command_spec["input_option_aliases"])
    reserved = set(policy["reserved_option_names"])
    output_option = command_spec.get("output_option", "")

    accepted: list[str] = []
    substitutions: list[str] = []
    positional_seen = False
    index = 2
    while index < len(tokens):
        token = tokens[index]
        if token.startswith("-"):
            if not _OPTION_NAME_RE.match(token):
                assert_token_safe(token, f"option {token!r}")
                raise PolicyBlocked("OPTION_NOT_ALLOWED", f"option {token!r} is not a governed long option")
            if token in input_aliases or token == output_option or token in reserved:
                if index + 1 >= len(tokens):
                    raise PolicyBlocked(
                        "OPTION_VALUE_MISSING", f"option {token!r} has no value in the reported invocation"
                    )
                assert_discarded_value_safe(tokens[index + 1], f"value of {token!r}")
                substitutions.append(
                    f"discarded reporter path supplied via {token} and used a workspace-controlled path"
                )
                index += 2
                continue
            option = reporter_options.get(token)
            if option is None:
                raise PolicyBlocked(
                    "OPTION_NOT_ALLOWED",
                    f"option {token!r} is not declared for {command_spec['command']} in the reproduction policy",
                )
            assert_token_safe(token, f"option {token!r}")
            if option["kind"] == "flag":
                accepted.append(token)
                index += 1
                continue
            if index + 1 >= len(tokens):
                raise PolicyBlocked("OPTION_VALUE_MISSING", f"option {token!r} has no value")
            value = tokens[index + 1]
            assert_token_safe(value, f"value of {token!r}")
            if len(value) > limits["max_option_value_length"]:
                raise PolicyBlocked("OPTION_VALUE_TOO_LONG", f"value of {token!r} is too long")
            if option["kind"] == "choice":
                if value not in option.get("choices", []):
                    raise PolicyBlocked(
                        "OPTION_VALUE_NOT_ALLOWED",
                        f"value {value!r} for {token!r} is not one of {option.get('choices', [])}",
                    )
            elif option["kind"] == "string":
                pattern = option.get("pattern")
                max_length = option.get("max_length", limits["max_option_value_length"])
                if len(value) > max_length or (pattern and re.fullmatch(pattern, value) is None):
                    raise PolicyBlocked(
                        "OPTION_VALUE_NOT_ALLOWED", f"value {value!r} for {token!r} is not policy conforming"
                    )
            else:  # pragma: no cover - schema restricts kinds
                raise InfrastructureError(f"unsupported option kind {option['kind']!r}")
            accepted.extend([token, value])
            index += 2
            continue

        if positional_seen:
            raise PolicyBlocked(
                "MULTIPLE_POSITIONAL_ARGUMENTS",
                "guarded reproduction accepts exactly one input, and the invocation supplies more",
            )
        assert_discarded_value_safe(token, "positional input path")
        substitutions.append("discarded reporter positional input path and used the workspace fixture")
        positional_seen = True
        index += 1

    argv = [executable, command_spec["command"], *accepted, str(fixture_path)]
    if command_spec["output_mode"] == "option":
        if not output_path:  # pragma: no cover - caller always provides one
            raise InfrastructureError("output path is required for option output mode")
        argv.extend([command_spec["output_option"], str(output_path)])
    return argv, substitutions


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------


def _child_environment(workspace: Path) -> dict[str, str]:
    """Minimal, deterministic environment. No secrets and no CI context are inherited."""
    keep = ("PATH", "SYSTEMROOT", "WINDIR", "COMSPEC", "PATHEXT", "NUMBER_OF_PROCESSORS", "PROCESSOR_ARCHITECTURE")
    env = {name: os.environ[name] for name in keep if name in os.environ}
    scratch = workspace / "scratch"
    scratch.mkdir(parents=True, exist_ok=True)
    env.update(
        {
            "HOME": str(workspace),
            "USERPROFILE": str(workspace),
            "TMPDIR": str(scratch),
            "TEMP": str(scratch),
            "TMP": str(scratch),
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PYTHONIOENCODING": "utf-8",
            "PYTHONHASHSEED": "0",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONNOUSERSITE": "1",
            "NO_COLOR": "1",
        }
    )
    return env


def _spawn_kwargs() -> dict[str, Any]:
    if os.name == "nt":
        return {"creationflags": getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)}
    return {"start_new_session": True}


def _terminate_tree(process: Any) -> None:
    """Kill the child and, where the platform allows it, its whole process group."""
    if os.name != "nt":
        try:
            os.killpg(os.getpgid(process.pid), 9)
            return
        except (OSError, AttributeError):
            pass
    try:
        process.kill()
    except OSError:  # pragma: no cover - process already gone
        pass


def _popen(argv: list[str], cwd: Path, env: dict[str, str], stdout_handle: Any, stderr_handle: Any) -> Any:
    """Single spawn seam. Never uses a shell; tests replace this function."""
    return subprocess.Popen(  # noqa: S603 - argv is policy-built and shell is never used
        argv,
        cwd=str(cwd),
        stdin=subprocess.DEVNULL,
        stdout=stdout_handle,
        stderr=stderr_handle,
        env=env,
        shell=False,
        **_spawn_kwargs(),
    )


def execute(argv: list[str], workspace: Path, out_dir: Path, timeout_seconds: int) -> dict[str, Any]:
    """Run the guarded command without a shell and capture bounded evidence."""
    stdout_path = workspace / "streams" / "stdout.bin"
    stderr_path = workspace / "streams" / "stderr.bin"
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    env = _child_environment(workspace)
    started = time.monotonic()
    timed_out = False
    returncode: int | None = None
    with stdout_path.open("wb") as stdout_handle, stderr_path.open("wb") as stderr_handle:
        process = _popen(argv, out_dir, env, stdout_handle, stderr_handle)
        try:
            returncode = process.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            timed_out = True
            _terminate_tree(process)
            try:
                returncode = process.wait(timeout=10)
            except subprocess.TimeoutExpired:  # pragma: no cover - kill failed
                returncode = None
    duration = round(max(time.monotonic() - started, 0.0), 3)
    return {
        "returncode": returncode,
        "timed_out": timed_out,
        "duration_seconds": duration,
        "stdout_path": stdout_path,
        "stderr_path": stderr_path,
    }


def _redact_argv(argv: list[str], workspace: Path) -> list[str]:
    """Record argv with the ephemeral workspace prefix redacted so evidence is stable."""
    prefixes = (str(workspace), str(workspace).replace("\\", "/"))
    redacted: list[str] = []
    for part in argv:
        text = str(part)
        for prefix in prefixes:
            text = text.replace(prefix, "<workspace>")
        redacted.append(text)
    return redacted


def _sanitize(text: str, workspace: Path) -> tuple[str, bool]:
    cleaned = _ANSI_RE.sub("", text)
    cleaned = cleaned.replace(str(workspace), "<workspace>")
    cleaned = cleaned.replace(str(workspace).replace("\\", "/"), "<workspace>")
    cleaned = cleaned.replace("\r\n", "\n").replace("\r", "\n")
    cleaned = "".join(char if char == "\n" or char >= " " else " " for char in cleaned)
    if len(cleaned) > EXCERPT_CHARACTERS:
        return cleaned[:EXCERPT_CHARACTERS], True
    return cleaned, False


def summarize_stream(path: Path, workspace: Path, budget: int) -> dict[str, Any]:
    if not path.is_file():
        return {"digest": EMPTY_DIGEST, "bytes": 0, "truncated": False, "excerpt": ""}
    digest, size = _sha256_file(path)
    with path.open("rb") as handle:
        head = handle.read(min(budget, 262144))
    excerpt, excerpt_truncated = _sanitize(head.decode("utf-8", errors="replace"), workspace)
    return {
        "digest": digest,
        "bytes": size,
        "truncated": size > len(head) or excerpt_truncated,
        "excerpt": excerpt,
    }


def collect_outputs(out_dir: Path, limits: dict[str, Any]) -> tuple[dict[str, Any], str]:
    """Build the produced-file manifest. Nothing produced is ever executed or compiled."""
    files: list[dict[str, Any]] = []
    total_bytes = 0
    violation = ""
    for path in sorted(out_dir.rglob("*")):
        if path.is_symlink():
            violation = "OUTPUT_SYMLINK_REJECTED"
            continue
        if not path.is_file():
            continue
        digest, size = _sha256_file(path)
        total_bytes += size
        files.append(
            {
                "path": path.relative_to(out_dir).as_posix(),
                "bytes": size,
                "digest": digest,
            }
        )
    limit_exceeded = (
        len(files) > limits["max_produced_files"] or total_bytes > limits["max_produced_bytes"]
    )
    if limit_exceeded and not violation:
        violation = "OUTPUT_LIMIT_EXCEEDED"
    manifest = {
        "file_count": len(files),
        "total_bytes": total_bytes,
        "limit_exceeded": limit_exceeded,
        "files": files[: limits["max_produced_files"]],
    }
    return manifest, violation


# ---------------------------------------------------------------------------
# Outcome classification
# ---------------------------------------------------------------------------


def _normalize_comparison_text(text: str) -> str:
    return text.replace("\r\n", "\n").replace("\r", "\n").strip()


def _empty_comparison() -> dict[str, Any]:
    return {"performed": False, "target": "", "match": False, "expected_digest": "", "actual_digest": ""}


def _compare_exact_output(
    expected_output: str,
    command_spec: dict[str, Any],
    outputs: dict[str, Any],
    out_dir: Path,
    stdout_path: Path,
) -> tuple[str, dict[str, Any]]:
    """Compare the single deterministic output target with the declared exact output."""
    comparison = _empty_comparison()
    if command_spec["output_mode"] == "option":
        if outputs["file_count"] != 1:
            return "AMBIGUOUS", comparison
        target_path = out_dir / outputs["files"][0]["path"]
        target_label = f"output file {outputs['files'][0]['path']}"
    else:
        if outputs["file_count"] != 0:
            return "AMBIGUOUS", comparison
        target_path = stdout_path
        target_label = "standard output"
    if not target_path.is_file():
        return "AMBIGUOUS", comparison
    actual_text = _normalize_comparison_text(target_path.read_bytes().decode("utf-8", errors="replace"))
    expected_text = _normalize_comparison_text(expected_output)
    comparison.update(
        {
            "performed": True,
            "target": target_label,
            "match": actual_text == expected_text,
            "expected_digest": _sha256_text(expected_text),
            "actual_digest": _sha256_text(actual_text),
        }
    )
    return ("MATCH" if comparison["match"] else "MISMATCH"), comparison


def classify_outcome(
    expectation_kind: str,
    expected_output: str | None,
    execution: dict[str, Any],
    outputs: dict[str, Any],
    violation: str,
    command_spec: dict[str, Any],
    out_dir: Path,
    stdout_path: Path,
    stdout_bytes: int,
    stderr_bytes: int,
    limits: dict[str, Any],
) -> tuple[str, str, str, dict[str, Any], str]:
    """Map structured execution evidence to an outcome.

    Returns ``(status, reason_code, reason, comparison, resource_status)``. Only
    structured facts are used: returncode, timeout, resource limits, produced
    output, and the reporter's structured expectation. Natural-language actual
    and expected text is never compared.
    """
    if execution["timed_out"]:
        return (
            "BLOCKED",
            "EXECUTION_TIMEOUT",
            f"execution exceeded the {command_spec['timeout_seconds']}s policy timeout",
            _empty_comparison(),
            "timeout",
        )
    if stdout_bytes + stderr_bytes > limits["max_stream_bytes"]:
        return (
            "BLOCKED",
            "STREAM_LIMIT_EXCEEDED",
            "captured output exceeded the policy stream budget, so evidence is not deterministic",
            _empty_comparison(),
            "stream-limit-exceeded",
        )
    if violation == "OUTPUT_SYMLINK_REJECTED":
        return (
            "BLOCKED",
            "OUTPUT_SYMLINK_REJECTED",
            "the command produced a symbolic link, which guarded reproduction refuses to record",
            _empty_comparison(),
            "output-limit-exceeded",
        )
    if violation == "OUTPUT_LIMIT_EXCEEDED":
        return (
            "BLOCKED",
            "OUTPUT_LIMIT_EXCEEDED",
            "produced output exceeded the policy file count or size budget",
            _empty_comparison(),
            "output-limit-exceeded",
        )

    resource_status = "within-limits"
    returncode = execution["returncode"]
    failed = returncode != 0

    if expectation_kind == "human_review":
        return (
            "NEEDS_REVIEW",
            "EXPECTATION_HUMAN_REVIEW",
            "the report asks for human semantic review, which automation never adjudicates",
            _empty_comparison(),
            resource_status,
        )
    if expectation_kind == "undeclared":
        return (
            "NEEDS_REVIEW",
            "EXPECTATION_NOT_DECLARED",
            "the report declares no structured expected command result",
            _empty_comparison(),
            resource_status,
        )

    if expectation_kind == "exact_output":
        if not expected_output:
            return (
                "NEEDS_REVIEW",
                "EXACT_OUTPUT_NOT_SUPPLIED",
                "the report requests exact output matching but supplies no exact expected output",
                _empty_comparison(),
                resource_status,
            )
        verdict, comparison = _compare_exact_output(
            expected_output, command_spec, outputs, out_dir, stdout_path
        )
        if verdict == "AMBIGUOUS":
            return (
                "NEEDS_REVIEW",
                "EXACT_OUTPUT_TARGET_AMBIGUOUS",
                "exactly one comparison target is required and the run did not produce one",
                comparison,
                resource_status,
            )
        if verdict == "MATCH":
            return (
                "NOT_REPRODUCED",
                "EXACT_OUTPUT_MATCH",
                "the produced output matches the declared exact expected output",
                comparison,
                resource_status,
            )
        return (
            "CONFIRMED",
            "EXACT_OUTPUT_MISMATCH",
            "the produced output differs from the declared exact expected output",
            comparison,
            resource_status,
        )

    if expectation_kind == "success":
        if failed:
            return (
                "CONFIRMED",
                "EXPECTED_SUCCESS_GOT_FAILURE",
                f"the report expects successful completion and the command exited {returncode}",
                _empty_comparison(),
                resource_status,
            )
        if expected_output:
            verdict, comparison = _compare_exact_output(
                expected_output, command_spec, outputs, out_dir, stdout_path
            )
            if verdict == "AMBIGUOUS":
                return (
                    "NEEDS_REVIEW",
                    "EXACT_OUTPUT_TARGET_AMBIGUOUS",
                    "exactly one comparison target is required and the run did not produce one",
                    comparison,
                    resource_status,
                )
            if verdict == "MATCH":
                return (
                    "NOT_REPRODUCED",
                    "EXACT_OUTPUT_MATCH",
                    "the command succeeded and the produced output matches the declared exact expected output",
                    comparison,
                    resource_status,
                )
            return (
                "CONFIRMED",
                "EXACT_OUTPUT_MISMATCH",
                "the command succeeded but the produced output differs from the declared exact expected output",
                comparison,
                resource_status,
            )
        return (
            "NEEDS_REVIEW",
            "SUCCESS_WITHOUT_EXACT_OUTPUT",
            "the command succeeded as expected and no exact output was declared to compare",
            _empty_comparison(),
            resource_status,
        )

    if expectation_kind == "failure":
        if failed:
            return (
                "NOT_REPRODUCED",
                "EXPECTED_FAILURE_OBSERVED",
                f"the report expects a nonzero exit and the command exited {returncode}",
                _empty_comparison(),
                resource_status,
            )
        return (
            "CONFIRMED",
            "EXPECTED_FAILURE_GOT_SUCCESS",
            "the report expects a nonzero exit and the command succeeded",
            _empty_comparison(),
            resource_status,
        )

    raise InfrastructureError(f"unsupported expectation kind {expectation_kind!r}")


# ---------------------------------------------------------------------------
# Evidence assembly
# ---------------------------------------------------------------------------


def _empty_execution(timeout_seconds: int) -> dict[str, Any]:
    empty_stream = {"digest": EMPTY_DIGEST, "bytes": 0, "truncated": False, "excerpt": ""}
    return {
        "executed": False,
        "argv": [],
        "argv_digest": _sha256_text("[]"),
        "timeout_seconds": timeout_seconds,
        "returncode": None,
        "timed_out": False,
        "duration_seconds": 0.0,
        "resource_status": "not-executed",
        "input_substitutions": [],
        "fixture": {"digest": EMPTY_DIGEST, "bytes": 0, "source_field": "input", "extension": ""},
        "stdout": dict(empty_stream),
        "stderr": dict(empty_stream),
        "outputs": {"file_count": 0, "total_bytes": 0, "limit_exceeded": False, "files": []},
    }


def _empty_readiness() -> dict[str, Any]:
    return {
        "form_type": "",
        "intake_status": "",
        "missing_fields": [],
        "surface": "",
        "command": "",
        "eligible": False,
        "expectation": {
            "kind": "undeclared",
            "declared_choice": "",
            "exact_output_provided": False,
            "exact_output_digest": "",
        },
        "notes": [],
    }


def _issue_event_json(issue: dict[str, Any], repository: str) -> str:
    return json.dumps(
        {
            "action": "workflow_dispatch",
            "issue": issue,
            "repository": {"full_name": repository},
            "sender": {"login": ""},
        },
        sort_keys=True,
    )


def reproduce(
    issue: dict[str, Any],
    authorization: dict[str, Any],
    options: dict[str, Any],
    policy: dict[str, Any] | None = None,
    catalog: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], str]:
    """Produce evidence for one authorized issue. Returns ``(record, markdown)``."""
    policy = policy or load_policy(Path(options.get("policy_path") or POLICY_PATH))
    catalog = catalog or load_label_catalog()
    limits = policy["limits"]

    if not isinstance(authorization, dict) or authorization.get("decision") != "ALLOW":
        raise InfrastructureError("guarded reproduction requires an ALLOW authorization record")

    issue_number = int(issue.get("number") or 0)
    repository = str(options.get("repository") or "")
    updated_at = str(issue.get("updated_at") or "")
    body = issue.get("body") if isinstance(issue.get("body"), str) else ""
    body_digest = _sha256_text(body)

    expected_updated_at = str(options.get("expected_updated_at") or "")
    expected_body_digest = str(options.get("expected_body_digest") or "")
    revision_verified = True
    revision_note = "issue revision matches the authorized revision"
    if expected_updated_at and expected_updated_at != updated_at:
        revision_verified = False
        revision_note = "issue updated_at changed after authorization"
    elif expected_body_digest and expected_body_digest != body_digest:
        revision_verified = False
        revision_note = "issue body digest changed after authorization"

    request = {
        "repository": repository,
        "issue_number": issue_number,
        "issue_url": str(issue.get("html_url") or ""),
        "issue_updated_at": updated_at,
        "issue_body_digest": body_digest,
        "actor": str(authorization["request"].get("actor") or ""),
        "event_name": str(authorization["request"].get("event_name") or ""),
        "requested_label": str(authorization["request"].get("label_name") or ""),
        "run_id": str(options.get("run_id") or ""),
        "run_attempt": int(options.get("run_attempt") or 0),
        "run_url": str(options.get("run_url") or ""),
        "authorization_digest": _sha256_text(json.dumps(authorization, sort_keys=True)),
        "revision_verified": revision_verified,
        "revision_note": revision_note,
    }
    source = {
        "trusted_sha": str(options.get("trusted_sha") or ""),
        "default_branch": str(options.get("default_branch") or ""),
        "policy_version": str(policy["policy_version"]),
        "policy_digest": _digest_of_file(Path(options.get("policy_path") or POLICY_PATH), "command policy"),
        "command_registry_digest": _digest_of_file(COMMANDS_JSON, "command registry"),
        "issue_form_contract_digest": _digest_of_file(ISSUE_FORM_CONTRACT, "issue form contract"),
        "label_catalog_digest": _digest_of_file(LABEL_CATALOG_PATH, "label catalog"),
    }
    environment = {
        "engine_version": ENGINE_VERSION,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "avrotize_executable": str(options.get("avrotize_executable") or ""),
        "avrotize_version": str(options.get("avrotize_version") or "unknown"),
    }

    readiness = _empty_readiness()
    execution = _empty_execution(limits["timeout_seconds"])
    status = "BLOCKED"
    reason_code = "UNCLASSIFIED"
    reason = "guarded reproduction produced no classified outcome"
    comparison = _empty_comparison()

    if not revision_verified:
        status, reason_code, reason = "BLOCKED", "ISSUE_REVISED_AFTER_AUTHORIZATION", revision_note
        readiness["notes"].append("issue content was not parsed because the authorized revision changed")
    else:
        try:
            record_intake, _ = governance_intake.normalize_issue(_issue_event_json(issue, repository))
            classification = record_intake["classification"]
            facts = record_intake["normalized_facts"]
            readiness.update(
                {
                    "form_type": str(classification["form_type"]),
                    "intake_status": str(classification["status"]),
                    "missing_fields": list(classification["missing_fields"]),
                    "surface": str(facts.get("surface") or ""),
                }
            )
            expectation_kind = str(facts.get("expected_result_kind") or "undeclared")
            raw_expected_output = facts.get("expected_output")
            expected_output = (
                unwrap_fenced_block(raw_expected_output).strip()
                if isinstance(raw_expected_output, str) and raw_expected_output.strip()
                else None
            )
            readiness["expectation"] = {
                "kind": expectation_kind,
                "declared_choice": str(facts.get("expected_result_choice") or ""),
                "exact_output_provided": bool(expected_output),
                "exact_output_digest": _sha256_text(_normalize_comparison_text(expected_output)) if expected_output else "",
            }

            if classification["form_type"] != "bug":
                raise PolicyBlocked("NOT_A_BUG_REPORT", "only complete bug reports are eligible for guarded reproduction")
            if classification["status"] != "complete":
                raise PolicyBlocked(
                    "REPORT_NOT_COMPLETE",
                    f"the bug report intake status is {classification['status']!r}",
                )
            surface = str(facts.get("surface") or "").strip()
            if surface not in policy["eligible_surfaces"]:
                raise PolicyBlocked(
                    "SURFACE_NOT_ELIGIBLE",
                    f"surface {surface!r} is not eligible; guarded reproduction runs {policy['eligible_surfaces']} only",
                )
            command_name = normalize_command_name(str(facts.get("command") or ""))
            readiness["command"] = command_name
            command_spec = select_policy_command(policy, command_name)
            readiness["eligible"] = True

            fixture_text = extract_fixture(str(facts.get("input_reproducer") or ""))
            fixture_bytes = validate_fixture(fixture_text, limits["max_fixture_bytes"])

            executable = str(options.get("avrotize_executable") or "")
            if not executable:
                raise InfrastructureError("trusted avrotize executable path was not supplied")

            workspace_root = options.get("workspace_root")
            with tempfile.TemporaryDirectory(prefix="avrotize-repro-", dir=workspace_root) as workspace_str:
                workspace = Path(workspace_str)
                out_dir = workspace / "out"
                out_dir.mkdir(parents=True, exist_ok=True)
                fixture_path = workspace / f"input{command_spec['input_extension']}"
                fixture_path.write_bytes(fixture_bytes)
                output_path = (
                    out_dir / f"output{command_spec.get('output_extension', '')}"
                    if command_spec["output_mode"] == "option"
                    else None
                )
                argv, substitutions = build_argv(
                    policy, command_spec, str(facts.get("flags_options") or ""), executable, fixture_path, output_path
                )
                run = execute(argv, workspace, out_dir, int(command_spec["timeout_seconds"]))
                stdout_summary = summarize_stream(run["stdout_path"], workspace, limits["max_stream_bytes"])
                stderr_summary = summarize_stream(run["stderr_path"], workspace, limits["max_stream_bytes"])
                outputs, violation = collect_outputs(out_dir, limits)
                status, reason_code, reason, comparison, resource_status = classify_outcome(
                    expectation_kind,
                    expected_output,
                    run,
                    outputs,
                    violation,
                    command_spec,
                    out_dir,
                    run["stdout_path"],
                    stdout_summary["bytes"],
                    stderr_summary["bytes"],
                    limits,
                )
                execution.update(
                    {
                        "executed": True,
                        "argv": _redact_argv(argv, workspace),
                        "argv_digest": _sha256_text(json.dumps(_redact_argv(argv, workspace), sort_keys=False)),
                        "timeout_seconds": int(command_spec["timeout_seconds"]),
                        "returncode": run["returncode"],
                        "timed_out": run["timed_out"],
                        "duration_seconds": run["duration_seconds"],
                        "resource_status": resource_status,
                        "input_substitutions": substitutions,
                        "fixture": {
                            "digest": _sha256_bytes(fixture_bytes),
                            "bytes": len(fixture_bytes),
                            "source_field": "input",
                            "extension": command_spec["input_extension"],
                        },
                        "stdout": stdout_summary,
                        "stderr": stderr_summary,
                        "outputs": outputs,
                    }
                )
        except PolicyBlocked as blocked:
            status, reason_code, reason = "BLOCKED", blocked.reason_code, blocked.reason

    record = {
        "schema_version": SCHEMA_VERSION,
        "record_kind": RECORD_KIND,
        "issue_number": issue_number,
        "request": request,
        "source": source,
        "environment": environment,
        "readiness": readiness,
        "execution": execution,
        "result": {
            "status": status,
            "reason_code": reason_code,
            "reason": reason,
            "final_label": label_for_outcome(catalog, status),
            "comparison": comparison,
        },
        "artifact": {
            "name": str(options.get("artifact_name") or f"repro-evidence-{issue_number}"),
            "retention_days": int(options.get("retention_days") or 14),
        },
        "authority": {"authorized": False, "statement": AUTHORITY_STATEMENT},
    }

    try:
        governance_schema.validate_or_raise(record, EVIDENCE_SCHEMA, "reproduction evidence record")
    except governance_schema.SchemaError as exc:
        raise InfrastructureError(str(exc)) from exc
    return record, render_summary(record)


def render_summary(record: dict[str, Any]) -> str:
    """Render the Markdown evidence summary. Reporter prose is never echoed."""
    request = record["request"]
    execution = record["execution"]
    result = record["result"]
    lines = [
        "## Guarded bug reproduction evidence",
        "",
        f"- **Issue**: #{record['issue_number']}",
        f"- **Outcome**: {result['status']} (`{result['reason_code']}`)",
        f"- **Reason**: {result['reason']}",
        f"- **Final label**: `{result['final_label']}`",
        f"- **Requested by**: `{request['actor'] or 'unknown'}` via `{request['event_name'] or 'unknown'}`",
        f"- **Issue revision**: `{request['issue_updated_at'] or 'unknown'}` "
        f"(body `{request['issue_body_digest'][:16]}...`, verified: {'yes' if request['revision_verified'] else 'no'})",
        f"- **Trusted source**: `{record['source']['trusted_sha'] or 'unknown'}` "
        f"on `{record['source']['default_branch'] or 'unknown'}`",
        f"- **Policy**: `{record['source']['policy_version']}` (`{record['source']['policy_digest'][:16]}...`)",
        f"- **Environment**: Python {record['environment']['python_version']}, "
        f"avrotize {record['environment']['avrotize_version']}, {record['environment']['platform']}",
        f"- **Readiness**: form `{record['readiness']['form_type'] or 'unknown'}`, "
        f"intake `{record['readiness']['intake_status'] or 'unknown'}`, "
        f"expectation `{record['readiness']['expectation']['kind']}`",
        "",
        "### Execution",
        "",
        f"- **Executed**: {'yes' if execution['executed'] else 'no'}",
        f"- **argv**: `{json.dumps(execution['argv'])}`",
        f"- **Exit code**: {execution['returncode'] if execution['returncode'] is not None else 'n/a'}"
        f" (timed out: {'yes' if execution['timed_out'] else 'no'}, {execution['duration_seconds']}s)",
        f"- **Resources**: {execution['resource_status']}",
        f"- **Fixture**: {execution['fixture']['bytes']} bytes, `{execution['fixture']['digest'][:16]}...`",
        f"- **stdout**: {execution['stdout']['bytes']} bytes, `{execution['stdout']['digest'][:16]}...`",
        f"- **stderr**: {execution['stderr']['bytes']} bytes, `{execution['stderr']['digest'][:16]}...`",
        f"- **Produced files**: {execution['outputs']['file_count']} "
        f"({execution['outputs']['total_bytes']} bytes)",
    ]
    for produced in execution["outputs"]["files"]:
        lines.append(f"  - `{produced['path']}` {produced['bytes']} bytes `{produced['digest'][:16]}...`")
    if execution["input_substitutions"]:
        lines.append("- **Input handling**:")
        lines.extend(f"  - {note}" for note in execution["input_substitutions"])
    comparison = result["comparison"]
    if comparison["performed"]:
        lines.extend(
            [
                "",
                "### Exact output comparison",
                "",
                f"- **Target**: {comparison['target']}",
                f"- **Match**: {'yes' if comparison['match'] else 'no'}",
                f"- **Expected digest**: `{comparison['expected_digest'][:16]}...`",
                f"- **Actual digest**: `{comparison['actual_digest'][:16]}...`",
            ]
        )
    lines.extend(["", f"> {AUTHORITY_STATEMENT}"])
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _read_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise InfrastructureError(f"cannot read {label} at {path}: {exc}") from exc


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--issue", required=True, type=Path, help="Issue payload JSON from the REST API.")
    parser.add_argument("--authorization", required=True, type=Path, help="Authorization record JSON.")
    parser.add_argument("--policy", type=Path, default=POLICY_PATH)
    parser.add_argument("--repository", default="")
    parser.add_argument("--expected-updated-at", default="")
    parser.add_argument("--expected-body-digest", default="")
    parser.add_argument("--trusted-sha", default="")
    parser.add_argument("--default-branch", default="")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--run-attempt", default="0")
    parser.add_argument("--run-url", default="")
    parser.add_argument("--avrotize-executable", default="")
    parser.add_argument("--avrotize-version", default="unknown")
    parser.add_argument("--artifact-name", default="")
    parser.add_argument("--retention-days", default="14")
    parser.add_argument("--workspace-root", default="")
    parser.add_argument("--output-json", type=Path)
    parser.add_argument("--output-markdown", type=Path)
    args = parser.parse_args(argv)

    issue = _read_json(args.issue, "issue payload")
    authorization = _read_json(args.authorization, "authorization record")
    if not isinstance(issue, dict):
        raise InfrastructureError("issue payload must be a JSON object")

    options = {
        "repository": args.repository,
        "policy_path": args.policy,
        "expected_updated_at": args.expected_updated_at,
        "expected_body_digest": args.expected_body_digest,
        "trusted_sha": args.trusted_sha,
        "default_branch": args.default_branch,
        "run_id": args.run_id,
        "run_attempt": int(args.run_attempt or 0),
        "run_url": args.run_url,
        "avrotize_executable": args.avrotize_executable,
        "avrotize_version": args.avrotize_version,
        "artifact_name": args.artifact_name,
        "retention_days": int(args.retention_days or 14),
        "workspace_root": args.workspace_root or None,
    }
    record, summary = reproduce(issue, authorization, options)

    payload = json.dumps(record, indent=2) + "\n"
    if args.output_json:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(payload, encoding="utf-8")
    else:
        sys.stdout.write(payload)
    if args.output_markdown:
        args.output_markdown.parent.mkdir(parents=True, exist_ok=True)
        args.output_markdown.write_text(summary, encoding="utf-8")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except InfrastructureError as error:  # pragma: no cover - exercised in workflow
        print(f"::error::guarded reproduction infrastructure failure: {error}", file=sys.stderr)
        raise SystemExit(2) from error
