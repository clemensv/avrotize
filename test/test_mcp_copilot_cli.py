"""
Integration tests for the Avrotize MCP server via the GitHub Copilot CLI.

These tests launch the Copilot CLI with a prompt that triggers MCP tool calls,
then parse the JSONL event stream to verify the server responded correctly.

NOT intended for CI/CD — requires a local Copilot CLI installation and
authenticated session. Run locally with:

    pytest test/test_mcp_copilot_cli.py -v

Skip marker: all tests are marked @pytest.mark.copilot_cli so you can
exclude them in CI with: pytest -m "not copilot_cli"
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

COPILOT_LOADER = Path(r"C:\ProgramData\global-npm\node_modules\@github\copilot\npm-loader.js")
REPO_ROOT = Path(__file__).resolve().parent.parent
MCP_CONFIG = json.dumps({
    "mcpServers": {
        "avrotize": {
            "command": "python",
            "args": ["-m", "avrotize.mcp_server"],
            "transport": "stdio",
        }
    }
})

MODEL = os.environ.get("COPILOT_TEST_MODEL", "gpt-5-mini")
TIMEOUT_SECONDS = int(os.environ.get("COPILOT_TEST_TIMEOUT", "300"))


def _copilot_available() -> bool:
    """Return True if the Copilot CLI loader script exists and node is on PATH."""
    if not COPILOT_LOADER.exists():
        return False
    return shutil.which("node") is not None


skip_no_copilot = pytest.mark.skipif(
    not _copilot_available(),
    reason="GitHub Copilot CLI not installed",
)

pytestmark = [pytest.mark.copilot_cli, skip_no_copilot]


def run_copilot_prompt(prompt: str, extra_args: list[str] | None = None) -> list[dict[str, Any]]:
    """
    Run a Copilot CLI prompt with the Avrotize MCP server attached.

    Returns a list of parsed JSONL event dicts.
    """
    cmd = [
        "node", str(COPILOT_LOADER),
        "-p", prompt,
        "--output-format", "json",
        "--additional-mcp-config", MCP_CONFIG,
        "--allow-all-tools",
        "--model", MODEL,
        "--no-custom-instructions",
        "--disable-builtin-mcps",
        "--no-ask-user",
        "-s",
    ]
    if extra_args:
        cmd.extend(extra_args)

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            timeout=TIMEOUT_SECONDS,
            cwd=str(REPO_ROOT),
        )
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout.decode("utf-8", errors="replace") if exc.stdout else ""
        pytest.fail(f"Copilot CLI timed out after {TIMEOUT_SECONDS}s. Partial output: {stdout[:500]}")

    stdout = result.stdout.decode("utf-8", errors="replace") if result.stdout else ""
    events: list[dict[str, Any]] = []
    for line in stdout.strip().splitlines():
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return events


def extract_events(events: list[dict], event_type: str) -> list[dict]:
    """Filter events by type."""
    return [e for e in events if e.get("type") == event_type]


def get_tool_calls(events: list[dict]) -> list[dict]:
    """Return all tool execution_start events."""
    return extract_events(events, "tool.execution_start")


def get_tool_results(events: list[dict]) -> list[dict]:
    """Return all tool execution_complete events."""
    return extract_events(events, "tool.execution_complete")


def get_final_message(events: list[dict]) -> str | None:
    """Return the content of the last assistant.message event."""
    msgs = extract_events(events, "assistant.message")
    if msgs:
        return msgs[-1].get("data", {}).get("content", "")
    return None


def get_exit_code(events: list[dict]) -> int | None:
    """Return the exit code from the result event."""
    results = extract_events(events, "result")
    if results:
        return results[-1].get("exitCode", results[-1].get("data", {}).get("exitCode"))
    return None


def assert_mcp_loaded(events: list[dict]):
    """Assert the MCP server was loaded."""
    loaded = extract_events(events, "session.mcp_servers_loaded")
    assert loaded, "MCP server was never loaded"


def assert_tool_was_called(events: list[dict], tool_substr: str) -> dict:
    """Assert a tool whose name contains tool_substr was called. Returns the execution_complete event."""
    calls = get_tool_calls(events)
    matching = [c for c in calls if tool_substr in json.dumps(c)]
    assert matching, (
        f"Expected a tool call containing '{tool_substr}', "
        f"got: {[json.dumps(c.get('data',{}))[:100] for c in calls]}"
    )
    # Find the corresponding completion
    completions = get_tool_results(events)
    if completions:
        return completions[-1]
    return matching[0]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestMcpDescribeCapabilities:
    """Test the describe_capabilities tool via Copilot CLI."""

    def test_describe_capabilities_returns_server_info(self):
        """Copilot should call describe_capabilities and get server metadata."""
        events = run_copilot_prompt(
            "Call the avrotize MCP server's describe_capabilities tool. "
            "Return ONLY the raw JSON that the tool returned."
        )
        assert_mcp_loaded(events)
        assert_tool_was_called(events, "describe_capabilities")

        msg = get_final_message(events)
        assert msg is not None, "No assistant message received"
        assert "avrotize" in msg.lower(), f"Response should mention avrotize: {msg[:200]}"


class TestMcpListConversions:
    """Test the list_conversions tool via Copilot CLI."""

    def test_list_conversions_returns_commands(self):
        """Copilot should call list_conversions and return available commands."""
        events = run_copilot_prompt(
            "Call the avrotize MCP server's list_conversions tool. "
            "List the first 10 command names from the result."
        )
        assert_mcp_loaded(events)
        assert_tool_was_called(events, "list_conversions")

        msg = get_final_message(events)
        assert msg is not None, "No assistant message received"
        # Should contain at least some known command names
        known_commands = ["p2a", "a2p", "j2a", "a2j", "j2s", "s2j"]
        found = [cmd for cmd in known_commands if cmd in msg]
        assert found, f"Response should contain known commands like p2a, a2j: {msg[:300]}"


class TestMcpGetConversion:
    """Test the get_conversion tool via Copilot CLI."""

    def test_get_conversion_returns_metadata(self):
        """Copilot should call get_conversion for a specific command."""
        events = run_copilot_prompt(
            "Use the avrotize__get_conversion MCP tool with the argument "
            "command='j2a'. Return only the JSON output from the tool."
        )
        assert_mcp_loaded(events)
        assert_tool_was_called(events, "get_conversion")

        msg = get_final_message(events)
        assert msg is not None, "No assistant message received"
        assert "j2a" in msg.lower(), f"Response should reference j2a: {msg[:200]}"


class TestMcpRunConversion:
    """Test the run_conversion tool via Copilot CLI."""

    def test_convert_inline_json_schema_to_avro(self):
        """Copilot should convert an inline JSON Schema to Avro via run_conversion."""
        json_schema = json.dumps({
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"}
            },
            "required": ["name"]
        })

        events = run_copilot_prompt(
            f"Use the avrotize MCP server's run_conversion tool to convert "
            f"this JSON Schema to Avro. Use command='j2a' and pass the schema "
            f"as input_content. Here is the schema: {json_schema}\n"
            f"Show the resulting Avro schema."
        )
        assert_mcp_loaded(events)
        assert_tool_was_called(events, "run_conversion")

        msg = get_final_message(events)
        assert msg is not None, "No assistant message received"
        # Avro schemas have "type": "record" and "fields"
        assert "record" in msg.lower() or "fields" in msg.lower(), (
            f"Response should contain Avro schema elements: {msg[:300]}"
        )

    def test_convert_file_proto_to_avro(self):
        """Copilot should convert a .proto file to Avro via run_conversion."""
        proto_file = REPO_ROOT / "test" / "proto" / "address.proto"
        if not proto_file.exists():
            pytest.skip(f"Test fixture {proto_file} not found")

        output_dir = Path(tempfile.mkdtemp(prefix="avrotize_mcp_test_"))
        output_path = output_dir / "address.avsc"

        try:
            events = run_copilot_prompt(
                f"Use the avrotize MCP server's run_conversion tool. "
                f"Convert the protobuf file at '{proto_file}' to Avro schema. "
                f"Use command='p2a', input_path='{proto_file}', "
                f"output_path='{output_path}'. Show the result."
            )
            assert_mcp_loaded(events)
            assert_tool_was_called(events, "run_conversion")
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

    def test_convert_cddl_to_json_structure(self):
        """Copilot should convert a CDDL file to JSON Structure via run_conversion."""
        cddl_content = """\
; Simple CDDL
person = {
    name: tstr
    age: uint
}
"""
        events = run_copilot_prompt(
            f"Use the avrotize MCP server's run_conversion tool to convert "
            f"this CDDL schema to JSON Structure format. Use command='cddl2s' "
            f"and pass the schema as input_content:\n{cddl_content}\n"
            f"Show the result."
        )
        assert_mcp_loaded(events)
        assert_tool_was_called(events, "run_conversion")

        msg = get_final_message(events)
        assert msg is not None, "No assistant message received"


class TestMcpEndToEnd:
    """End-to-end tests combining multiple MCP tool calls."""

    def test_discover_and_convert(self):
        """Copilot should discover the right conversion, then run it."""
        events = run_copilot_prompt(
            "I have a JSON Schema and I want Avro. First, use the avrotize "
            "MCP server to find the right conversion command for JSON Schema "
            "to Avro (hint: list_conversions or get_conversion). Then use "
            "run_conversion with that command and this inline schema: "
            '{"type":"object","properties":{"id":{"type":"integer"},"label":{"type":"string"}}} '
            "Show me the Avro output.",
            extra_args=["--autopilot", "--max-autopilot-continues", "5"],
        )
        assert_mcp_loaded(events)
        tool_results = get_tool_results(events)
        assert len(tool_results) >= 1, "Expected at least one tool call"

        msg = get_final_message(events)
        assert msg is not None, "No assistant message received"

    def test_conversion_with_namespace(self):
        """Copilot should pass options like namespace to run_conversion."""
        events = run_copilot_prompt(
            "Use the avrotize MCP server's run_conversion tool. Convert this "
            "JSON Schema to Avro with command='j2a' and the option "
            "namespace='com.example.test'. Input content: "
            '{"type":"object","properties":{"value":{"type":"number"}}} '
            "Show the result."
        )
        assert_mcp_loaded(events)
        assert_tool_was_called(events, "run_conversion")

        msg = get_final_message(events)
        assert msg is not None, "No assistant message received"
