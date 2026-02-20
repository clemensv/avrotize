"""MCP server integration for Avrotize conversions."""

from __future__ import annotations

import argparse
import os
import tempfile
from typing import Any, Dict, List

from avrotize.avrotize import dynamic_import, load_commands


def _command_dest(arg: Dict[str, Any]) -> str:
    name = arg["name"]
    if name.startswith("-"):
        return arg.get("dest", name.lstrip("-").replace("-", "_").replace(".", "_"))
    return name


def _coerce_value(value: Any, arg_type: str) -> Any:
    if value is None:
        return None
    if arg_type == "bool":
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        sval = str(value).strip().lower()
        return sval in {"1", "true", "yes", "y", "on"}
    if arg_type == "int":
        return int(value)
    if arg_type == "float":
        return float(value)
    return str(value) if arg_type == "str" else value


def _build_namespace(command: Dict[str, Any], options: Dict[str, Any]) -> argparse.Namespace:
    namespace = argparse.Namespace()
    normalized_options = {
        key.lstrip("-").replace("-", "_"): value
        for key, value in (options or {}).items()
    }

    for arg in command.get("args", []):
        dest = _command_dest(arg)
        default_value = arg.get("default", False if arg.get("type") == "bool" else None)
        setattr(namespace, dest, default_value)
        if dest in normalized_options:
            raw_value = normalized_options[dest]
            if arg.get("nargs") in {"+", "*"}:
                values = raw_value if isinstance(raw_value, list) else [raw_value]
                setattr(namespace, dest, [_coerce_value(v, arg["type"]) for v in values])
            else:
                setattr(namespace, dest, _coerce_value(raw_value, arg["type"]))

    return namespace


def _resolve_input_path(command_args: argparse.Namespace, explicit_input_path: str | None) -> str | None:
    if explicit_input_path:
        return explicit_input_path
    return (
        getattr(command_args, "input", None)
        or getattr(command_args, "avsc", None)
        or getattr(command_args, "proto", None)
        or getattr(command_args, "jsons", None)
        or getattr(command_args, "xsd", None)
        or getattr(command_args, "kusto_uri", None)
        or getattr(command_args, "parquet", None)
        or getattr(command_args, "asn", None)
        or getattr(command_args, "kstruct", None)
    )


def _find_primary_input_arg(command: Dict[str, Any]) -> Dict[str, Any] | None:
    return next(
        (arg for arg in command.get("args", []) if isinstance(arg.get("name"), str) and not arg["name"].startswith("-")),
        None,
    )


def _inject_input_into_namespace(command: Dict[str, Any], command_args: argparse.Namespace, input_value: str) -> None:
    primary_input_arg = _find_primary_input_arg(command)
    if not primary_input_arg:
        return

    dest = _command_dest(primary_input_arg)
    current_value = getattr(command_args, dest, None)

    if primary_input_arg.get("nargs") in {"+", "*"}:
        if current_value in (None, "", []):
            setattr(command_args, dest, [input_value])
        elif isinstance(current_value, list) and input_value not in current_value:
            setattr(command_args, dest, [input_value, *current_value])
        elif not isinstance(current_value, list):
            setattr(command_args, dest, [input_value, current_value])
    else:
        if current_value in (None, ""):
            setattr(command_args, dest, input_value)


def _find_command(command_name: str) -> Dict[str, Any] | None:
    return next((cmd for cmd in load_commands() if cmd.get("command") == command_name), None)


def _list_commands() -> List[Dict[str, Any]]:
    commands = load_commands()
    result: List[Dict[str, Any]] = []
    for command in commands:
        if command.get("command") == "mcp":
            continue
        result.append(
            {
                "command": command.get("command"),
                "description": command.get("description"),
                "group": command.get("group"),
                "args": [arg.get("name") for arg in command.get("args", [])],
            }
        )
    return result


def _describe_capabilities() -> Dict[str, Any]:
    commands = load_commands()
    command_names = [cmd.get("command") for cmd in commands if cmd.get("command") != "mcp"]
    return {
        "server": "avrotize",
        "purpose": "Schema conversion and schema-driven code generation",
        "use_when": [
            "Converting between schema formats (Avro, JSON Schema, Proto, XSD, Parquet, etc.)",
            "Generating code from Avro/JSON Structure schemas (C#, Java, Python, TypeScript, JavaScript, Rust, Go, C++)",
            "Inferring schemas from sample JSON/XML/CSV/parquet inputs",
        ],
        "do_not_use_when": [
            "Task is unrelated to schema conversion or code generation",
            "Task requires arbitrary code execution outside avrotize command set",
        ],
        "recommended_flow": [
            "Call describe_capabilities for high-level routing",
            "Call list_conversions to discover available commands",
            "Call get_conversion(command) to inspect required args/options",
            "Call run_conversion(...) to execute",
        ],
        "tools": {
            "describe_capabilities": "High-level guidance for when and how to use this server",
            "list_conversions": "List available conversion/code generation commands",
            "get_conversion": "Inspect metadata and args for a specific command",
            "run_conversion": "Execute a specific conversion command",
        },
        "command_count": len(command_names),
        "commands": command_names,
    }


def _execute_conversion(
    command_name: str,
    input_path: str | None = None,
    input_content: str | None = None,
    output_path: str | None = None,
    options: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    command = _find_command(command_name)
    if not command:
        raise ValueError(f"Unknown command '{command_name}'.")

    command_args = _build_namespace(command, options or {})
    temp_input_path = None
    temp_output_path = None

    try:
        resolved_input = _resolve_input_path(command_args, input_path)
        skip_input_file_handling = command.get("skip_input_file_handling", False)
        if input_content is not None and not resolved_input:
            temp_input = tempfile.NamedTemporaryFile(delete=False, mode="w", encoding="utf-8")
            temp_input.write(input_content)
            temp_input.flush()
            temp_input.close()
            temp_input_path = temp_input.name
            resolved_input = temp_input_path

        if not skip_input_file_handling and not resolved_input:
            if input_content is None:
                raise ValueError("This command requires input_path or input_content.")

        if resolved_input:
            _inject_input_into_namespace(command, command_args, resolved_input)

        if not output_path and any(v == "output_file_path" for v in command.get("function", {}).get("args", {}).values()):
            temp_output = tempfile.NamedTemporaryFile(delete=False, mode="w", encoding="utf-8")
            temp_output_path = temp_output.name
            temp_output.close()
            output_path = temp_output_path

        module_name, func_name = command["function"]["name"].rsplit(".", 1)
        func = dynamic_import(module_name, func_name)

        func_args: Dict[str, Any] = {}
        for arg_name, arg_value in command["function"]["args"].items():
            if arg_value == "input_file_path":
                func_args[arg_name] = resolved_input
            elif arg_value == "output_file_path":
                func_args[arg_name] = output_path
            elif isinstance(arg_value, str) and arg_value.startswith("args."):
                attr_name = arg_value[5:]
                if hasattr(command_args, attr_name):
                    func_args[arg_name] = getattr(command_args, attr_name)
            else:
                func_args[arg_name] = arg_value

        func(**func_args)

        output_content = None
        if temp_output_path and os.path.exists(temp_output_path):
            with open(temp_output_path, "r", encoding="utf-8") as out_file:
                output_content = out_file.read()

        return {
            "success": True,
            "command": command_name,
            "output_path": output_path,
            "output_content": output_content,
        }
    finally:
        if temp_input_path and os.path.exists(temp_input_path):
            os.remove(temp_input_path)
        if temp_output_path and os.path.exists(temp_output_path):
            os.remove(temp_output_path)


def run_mcp_server(transport: str = "stdio"):
    """Run avrotize as a local MCP server.

    Exposes conversion tools to MCP clients.
    """
    try:
        from mcp.server.fastmcp import FastMCP
    except ImportError as exc:
        raise RuntimeError(
            "MCP support is not installed. Install with: pip install mcp"
        ) from exc

    if transport != "stdio":
        raise ValueError("Only 'stdio' transport is currently supported.")

    mcp = FastMCP("avrotize")

    @mcp.tool()
    def describe_capabilities() -> Dict[str, Any]:
        """Describe when this server should be used and how to invoke it."""
        return _describe_capabilities()

    @mcp.tool()
    def list_conversions() -> List[Dict[str, Any]]:
        """List available Avrotize conversion commands."""
        return _list_commands()

    @mcp.tool()
    def get_conversion(command: str) -> Dict[str, Any]:
        """Get metadata for a specific conversion command."""
        cmd = _find_command(command)
        if not cmd:
            raise ValueError(f"Unknown command '{command}'.")
        return {
            "command": cmd.get("command"),
            "description": cmd.get("description"),
            "group": cmd.get("group"),
            "args": cmd.get("args", []),
        }

    @mcp.tool()
    def run_conversion(
        command: str,
        input_path: str = "",
        input_content: str = "",
        output_path: str = "",
        options: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        """Run a conversion command and return conversion output information."""
        return _execute_conversion(
            command_name=command,
            input_path=input_path or None,
            input_content=input_content or None,
            output_path=output_path or None,
            options=options or {},
        )

    try:
        mcp.run(transport="stdio")
    except TypeError:
        mcp.run()
