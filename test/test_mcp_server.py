"""Deterministic, CI-safe unit tests for the Avrotize MCP server.

Unlike ``test_mcp_copilot_cli.py`` (which drives the real GitHub Copilot CLI
and is skipped in CI), this suite exercises ``avrotize.mcp_server`` directly:

* the JSON-RPC / MCP protocol layer (``_handle_message``),
* tool dispatch (``_dispatch_tool``) and the four exposed tools,
* the argument-plumbing helpers,
* one real, pure-Python end-to-end conversion (``cddl2s``),
* the stdio transport loop, and
* a load-time budget: the server must be ready (respond to ``initialize``)
  in under 2 seconds — the whole reason the lightweight JSON-RPC transport
  exists instead of the heavy ``mcp`` library import.

No network, no external CLIs, no LLM — safe for CI.
"""

import argparse
import io
import json
import subprocess
import sys
import time
from pathlib import Path

import pytest

from avrotize import mcp_server
from avrotize.mcp_server import (
    _MCP_PROTOCOL_VERSION,
    _build_namespace,
    _coerce_value,
    _command_dest,
    _describe_capabilities,
    _dispatch_tool,
    _execute_conversion,
    _find_command,
    _find_primary_input_arg,
    _get_version,
    _handle_message,
    _inject_input_into_namespace,
    _list_commands,
    _resolve_input_path,
    _run_stdio_loop,
    _tool_schemas,
    run_mcp_server,
    run_mcp_server_fastmcp,
)

REPO_ROOT = Path(__file__).resolve().parent.parent

TOOL_NAMES = {"describe_capabilities", "list_conversions", "get_conversion", "run_conversion"}

SAMPLE_CDDL = "person = {\n  name: tstr\n  age: uint\n}\n"

# A synthetic command definition used to exercise the arg-plumbing helpers
# without depending on the exact shape of any real command in commands.json.
_SAMPLE_COMMAND = {
    "args": [
        {"name": "input", "type": "str", "nargs": "?"},
        {"name": "--namespace", "type": "str"},
        {"name": "--count", "type": "int", "default": 5},
        {"name": "--flag", "type": "bool"},
        {"name": "--items", "type": "str", "nargs": "+"},
    ]
}


def _req(method, req_id=1, params=None):
    msg = {"jsonrpc": "2.0", "method": method}
    if req_id is not None:
        msg["id"] = req_id
    if params is not None:
        msg["params"] = params
    return msg


# ---------------------------------------------------------------------------
# JSON-RPC / MCP protocol layer  (_handle_message)
# ---------------------------------------------------------------------------

class TestHandleMessage:
    def test_initialize(self):
        resp = _handle_message(_req("initialize"))
        assert resp["jsonrpc"] == "2.0"
        assert resp["id"] == 1
        result = resp["result"]
        assert result["protocolVersion"] == _MCP_PROTOCOL_VERSION
        assert result["serverInfo"]["name"] == "avrotize"
        assert isinstance(result["serverInfo"]["version"], str)
        assert "tools" in result["capabilities"]

    def test_notifications_initialized_returns_none(self):
        assert _handle_message({"jsonrpc": "2.0", "method": "notifications/initialized"}) is None

    def test_ping(self):
        resp = _handle_message(_req("ping", req_id=7))
        assert resp["id"] == 7
        assert resp["result"] == {}

    def test_tools_list_returns_four_tools(self):
        resp = _handle_message(_req("tools/list"))
        names = {t["name"] for t in resp["result"]["tools"]}
        assert names == TOOL_NAMES

    def test_tools_call_describe_capabilities(self):
        resp = _handle_message(_req("tools/call", params={"name": "describe_capabilities", "arguments": {}}))
        result = resp["result"]
        assert result.get("isError") in (None, False)
        assert result["content"][0]["type"] == "text"
        payload = json.loads(result["content"][0]["text"])
        assert payload["server"] == "avrotize"

    def test_tools_call_run_conversion_cddl2s(self):
        resp = _handle_message(_req("tools/call", params={
            "name": "run_conversion",
            "arguments": {"command": "cddl2s", "input_content": SAMPLE_CDDL},
        }))
        result = resp["result"]
        assert result.get("isError") in (None, False)
        payload = json.loads(result["content"][0]["text"])
        assert payload["success"] is True
        assert payload["command"] == "cddl2s"
        assert payload["output_content"]
        assert isinstance(json.loads(payload["output_content"]), dict)

    def test_tools_call_unknown_tool_is_error(self):
        resp = _handle_message(_req("tools/call", params={"name": "does_not_exist", "arguments": {}}))
        assert resp["result"]["isError"] is True
        assert "does_not_exist" in resp["result"]["content"][0]["text"]

    def test_unknown_method_with_id_is_method_not_found(self):
        resp = _handle_message(_req("bogus/method", req_id=3))
        assert resp["error"]["code"] == -32601
        assert "bogus/method" in resp["error"]["message"]

    def test_unknown_notification_returns_none(self):
        # No id -> notification -> silently ignored.
        assert _handle_message({"jsonrpc": "2.0", "method": "some/notification"}) is None


# ---------------------------------------------------------------------------
# Tool dispatch and schemas
# ---------------------------------------------------------------------------

class TestDispatchTool:
    def test_describe_capabilities(self):
        out = _dispatch_tool("describe_capabilities", {})
        assert out["server"] == "avrotize"
        assert out["command_count"] == len(out["commands"])

    def test_list_conversions(self):
        out = _dispatch_tool("list_conversions", {})
        assert isinstance(out, list) and out
        assert all("command" in c for c in out)

    def test_get_conversion_valid(self):
        out = _dispatch_tool("get_conversion", {"command": "cddl2s"})
        assert out["command"] == "cddl2s"
        assert out["group"]

    def test_get_conversion_invalid_raises(self):
        with pytest.raises(ValueError):
            _dispatch_tool("get_conversion", {"command": "no-such-command"})

    def test_run_conversion(self):
        out = _dispatch_tool("run_conversion", {"command": "cddl2s", "input_content": SAMPLE_CDDL})
        assert out["success"] is True

    def test_unknown_tool_raises(self):
        with pytest.raises(ValueError):
            _dispatch_tool("no_such_tool", {})


def test_tool_schemas_shape():
    schemas = _tool_schemas()
    by_name = {s["name"]: s for s in schemas}
    assert set(by_name) == TOOL_NAMES
    assert by_name["get_conversion"]["inputSchema"]["required"] == ["command"]
    assert by_name["run_conversion"]["inputSchema"]["required"] == ["command"]
    for schema in schemas:
        assert schema["inputSchema"]["type"] == "object"


# ---------------------------------------------------------------------------
# Command discovery
# ---------------------------------------------------------------------------

def test_list_commands_excludes_mcp():
    names = {c["command"] for c in _list_commands()}
    assert "mcp" not in names
    assert "cddl2s" in names


def test_describe_capabilities_excludes_mcp_and_counts():
    described = _describe_capabilities()
    assert "mcp" not in described["commands"]
    assert described["command_count"] == len(described["commands"])
    assert set(described["tools"]) == TOOL_NAMES


def test_find_command_valid_and_invalid():
    assert _find_command("cddl2s")["command"] == "cddl2s"
    assert _find_command("does-not-exist") is None


# ---------------------------------------------------------------------------
# Argument-plumbing helpers
# ---------------------------------------------------------------------------

class TestCoerceValue:
    @pytest.mark.parametrize(
        "value,expected",
        [(True, True), (1, True), (0, False), ("true", True), ("yes", True),
         ("on", True), ("1", True), ("0", False), ("off", False), ("no", False)],
    )
    def test_bool(self, value, expected):
        assert _coerce_value(value, "bool") is expected

    def test_numeric_and_str(self):
        assert _coerce_value("42", "int") == 42
        assert _coerce_value("3.5", "float") == 3.5
        assert _coerce_value(7, "str") == "7"

    def test_none_passthrough(self):
        assert _coerce_value(None, "int") is None

    def test_unknown_type_passthrough(self):
        sentinel = ["x"]
        assert _coerce_value(sentinel, "list") is sentinel


def test_command_dest():
    assert _command_dest({"name": "input"}) == "input"
    assert _command_dest({"name": "--root-class-name"}) == "root_class_name"
    assert _command_dest({"name": "--ns.name"}) == "ns_name"
    assert _command_dest({"name": "--x", "dest": "custom"}) == "custom"


def test_build_namespace_defaults():
    ns = _build_namespace(_SAMPLE_COMMAND, {})
    assert ns.input is None
    assert ns.namespace is None
    assert ns.count == 5
    assert ns.flag is False
    assert ns.items is None


def test_build_namespace_overrides_and_coercion():
    ns = _build_namespace(_SAMPLE_COMMAND, {"namespace": "com.x", "count": "9", "flag": "true", "items": "a"})
    assert ns.namespace == "com.x"
    assert ns.count == 9
    assert ns.flag is True
    assert ns.items == ["a"]


def test_build_namespace_nargs_list_value():
    ns = _build_namespace(_SAMPLE_COMMAND, {"items": ["a", "b"]})
    assert ns.items == ["a", "b"]


def test_resolve_input_path_explicit_wins():
    ns = argparse.Namespace(input="A")
    assert _resolve_input_path(ns, "B") == "B"


def test_resolve_input_path_input_fallback():
    ns = argparse.Namespace(input="A")
    assert _resolve_input_path(ns, None) == "A"


def test_resolve_input_path_secondary_fallback():
    ns = argparse.Namespace(input=None, proto="P")
    assert _resolve_input_path(ns, None) == "P"


def test_find_primary_input_arg():
    cmd = {"args": [{"name": "--opt", "type": "str"}, {"name": "input", "type": "str"}]}
    assert _find_primary_input_arg(cmd)["name"] == "input"
    assert _find_primary_input_arg({"args": [{"name": "--only-option"}]}) is None


def test_inject_input_scalar():
    cmd = {"args": [{"name": "input", "type": "str"}]}
    ns = argparse.Namespace(input=None)
    _inject_input_into_namespace(cmd, ns, "F")
    assert ns.input == "F"

    ns_existing = argparse.Namespace(input="EXISTING")
    _inject_input_into_namespace(cmd, ns_existing, "F")
    assert ns_existing.input == "EXISTING"


def test_inject_input_nargs():
    cmd = {"args": [{"name": "input", "type": "str", "nargs": "+"}]}
    ns_empty = argparse.Namespace(input=None)
    _inject_input_into_namespace(cmd, ns_empty, "F")
    assert ns_empty.input == ["F"]

    ns_existing = argparse.Namespace(input=["E"])
    _inject_input_into_namespace(cmd, ns_existing, "F")
    assert ns_existing.input == ["F", "E"]


# ---------------------------------------------------------------------------
# End-to-end conversion (pure-Python cddl2s)
# ---------------------------------------------------------------------------

def test_execute_conversion_cddl2s_inline():
    result = _execute_conversion("cddl2s", input_content=SAMPLE_CDDL)
    assert result["success"] is True
    assert result["command"] == "cddl2s"
    doc = json.loads(result["output_content"])
    assert isinstance(doc, dict) and doc


def test_execute_conversion_unknown_command_raises():
    with pytest.raises(ValueError):
        _execute_conversion("no-such-cmd", input_content="x")


def test_execute_conversion_requires_input():
    with pytest.raises(ValueError):
        _execute_conversion("cddl2s")


# ---------------------------------------------------------------------------
# Entry points / transport validation
# ---------------------------------------------------------------------------

def test_run_mcp_server_rejects_non_stdio():
    with pytest.raises(ValueError):
        run_mcp_server("http")


def test_fastmcp_rejects_non_stdio():
    # Raises ValueError when the mcp lib is present, RuntimeError when it is not.
    with pytest.raises((ValueError, RuntimeError)):
        run_mcp_server_fastmcp("http")


def test_get_version_returns_str():
    version = _get_version()
    assert isinstance(version, str) and version


# ---------------------------------------------------------------------------
# stdio transport loop (in-process, no subprocess)
# ---------------------------------------------------------------------------

def test_run_stdio_loop_handles_requests_and_parse_error(monkeypatch):
    lines = [
        json.dumps({"jsonrpc": "2.0", "id": 1, "method": "initialize"}),
        "{ this is not valid json",
        json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}),
    ]
    fake_in = io.StringIO("\n".join(lines) + "\n")
    fake_out = io.StringIO()
    monkeypatch.setattr(mcp_server.sys, "stdin", fake_in)
    monkeypatch.setattr(mcp_server.sys, "stdout", fake_out)

    _run_stdio_loop()

    out_lines = [line for line in fake_out.getvalue().splitlines() if line.strip()]
    # initialize -> response; malformed -> parse error; notification -> no response.
    assert len(out_lines) == 2
    initialize_resp = json.loads(out_lines[0])
    parse_error_resp = json.loads(out_lines[1])
    assert initialize_resp["result"]["serverInfo"]["name"] == "avrotize"
    assert parse_error_resp["error"]["code"] == -32700
    assert parse_error_resp["id"] is None


# ---------------------------------------------------------------------------
# Load-time budget: the server must be ready in < 2 seconds
# ---------------------------------------------------------------------------
#
# The lightweight JSON-RPC transport exists specifically to avoid importing the
# heavy ``mcp`` library (uvicorn/starlette/httpx), which the module docstring
# notes costs ~2-3s.  Two complementary checks protect that budget:
#
#   * ``test_mcp_server_startup_avoids_heavy_imports`` — deterministic and
#     low-variance: it times the import *inside* the child process (excluding
#     interpreter-spawn/OS-scheduling noise) and asserts the heavy libraries
#     are never imported on the default path.  This is the reliable guard.
#   * ``test_mcp_server_ready_under_2s`` — the literal wall-clock check that the
#     server responds to ``initialize`` in under 2s.  Process spawn on a loaded
#     host is noisy, so it is best-of-N with a warm-up and an early exit; a
#     genuine heavy-import regression inflates *every* sample by seconds, so the
#     minimum still crosses the budget and fails.

_READY_BUDGET_S = 2.0
_EARLY_EXIT_S = 1.5
_MAX_ATTEMPTS = 10
_HEAVY_MODULES = ("mcp", "uvicorn", "starlette")


def _time_initialize_once():
    """Spawn the server, send ``initialize``, and time until it responds.

    Returns (elapsed_seconds, first_stdout_line, stderr).
    """
    request = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}) + "\n"
    start = time.perf_counter()
    proc = subprocess.Popen(
        [sys.executable, "-m", "avrotize.mcp_server"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=str(REPO_ROOT),
    )
    try:
        out, err = proc.communicate(input=request, timeout=30)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.communicate()
        raise
    elapsed = time.perf_counter() - start
    first_line = next((line for line in out.splitlines() if line.strip()), "")
    return elapsed, first_line, err


def _is_valid_initialize(line):
    try:
        return json.loads(line).get("result", {}).get("serverInfo", {}).get("name") == "avrotize"
    except (json.JSONDecodeError, TypeError, AttributeError):
        return False


def test_mcp_server_startup_avoids_heavy_imports():
    """Importing the server must be fast and must not pull in the heavy mcp libs.

    The timer runs inside the child process, so this excludes interpreter-spawn
    noise and is stable across machines and CI — it is the low-variance guard
    for the load-time budget.
    """
    probe = (
        "import time, sys, json;"
        "t = time.perf_counter();"
        "import avrotize.mcp_server;"
        "dt = time.perf_counter() - t;"
        "heavy = [m for m in ('mcp', 'uvicorn', 'starlette') if m in sys.modules];"
        "print(json.dumps({'import_s': dt, 'heavy': heavy}))"
    )
    result = subprocess.run(
        [sys.executable, "-c", probe],
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
        timeout=60,
    )
    assert result.returncode == 0, f"import probe failed:\n{result.stderr}"
    report = json.loads(result.stdout.strip().splitlines()[-1])
    assert report["heavy"] == [], (
        f"Lightweight startup path imported heavy libraries {report['heavy']}; "
        f"the default MCP server path must not import the `mcp` library."
    )
    assert report["import_s"] < _READY_BUDGET_S, (
        f"Importing avrotize.mcp_server took {report['import_s']:.3f}s, "
        f"exceeding the {_READY_BUDGET_S}s budget."
    )


def test_mcp_server_ready_under_2s():
    """End-to-end: the server responds to ``initialize`` in under 2 seconds.

    Best-of-N with a warm-up and early exit keeps this robust against transient
    host load while still failing hard on a genuine >2s startup regression
    (e.g. reintroducing the heavy ``mcp`` import path, which inflates every
    sample by seconds).
    """
    _time_initialize_once()  # warm-up (byte-compile, prime caches) — not timed

    best = float("inf")
    valid_response = False
    last_stderr = ""
    for _ in range(_MAX_ATTEMPTS):
        elapsed, first_line, stderr = _time_initialize_once()
        last_stderr = stderr
        valid_response = valid_response or _is_valid_initialize(first_line)
        best = min(best, elapsed)
        if best < _EARLY_EXIT_S:
            break

    assert valid_response, f"Server did not return a valid initialize response. stderr:\n{last_stderr}"
    assert best < _READY_BUDGET_S, (
        f"MCP server ready time {best:.3f}s exceeded the {_READY_BUDGET_S}s budget "
        f"(best of {_MAX_ATTEMPTS}). A regression to the heavy `mcp` import path is the usual cause."
    )
