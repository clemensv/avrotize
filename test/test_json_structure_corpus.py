import json
import os
import shutil
import subprocess
import warnings
from pathlib import Path
from typing import Any

import avro.schema
import pytest

from avrotize.avrotojstruct import convert_avro_to_json_structure


def find_jstruct() -> str | None:
    configured = os.environ.get("JSTRUCT")
    if configured:
        return configured

    executable = shutil.which("jstruct")
    if executable:
        return executable

    checkout_root = Path(__file__).resolve().parents[2]
    executable_name = "jstruct.exe" if os.name == "nt" else "jstruct"
    for profile in ("release", "debug"):
        candidate = checkout_root / "json-structure" / "sdk" / "rust" / "target" / profile / executable_name
        if candidate.is_file():
            return str(candidate)
    return None


def test_all_json_structure_schemas_are_valid() -> None:
    jstruct = find_jstruct()
    if not jstruct:
        pytest.fail("jstruct is required; install it or set JSTRUCT to its executable path")

    test_root = Path(__file__).parent
    schemas = sorted(test_root.rglob("*.struct.json"))
    assert schemas, "No JSON Structure schemas found"

    result = subprocess.run(
        [jstruct, "check", "--format", "text", *(str(schema) for schema in schemas)],
        capture_output=True,
        text=True,
        check=False,
    )
    output = "\n".join(part for part in (result.stdout.strip(), result.stderr.strip()) if part)
    assert result.returncode == 0, output


def _named_avro_types(node: Any, namespace: str | None = None) -> set[str]:
    result = set()
    if isinstance(node, list):
        for item in node:
            result.update(_named_avro_types(item, namespace))
    elif isinstance(node, dict):
        category = node.get("type")
        current_namespace = node.get("namespace", namespace)
        if category in ("record", "enum", "fixed") and node.get("name"):
            name = node["name"]
            if "." in name:
                current_namespace, name = name.rsplit(".", 1)
            result.add(f"{current_namespace}.{name}" if current_namespace else name)
            if category == "record":
                for field in node.get("fields", []):
                    result.update(_named_avro_types(field.get("type"), current_namespace))
        elif isinstance(category, (dict, list)):
            result.update(_named_avro_types(category, current_namespace))
        elif category == "array":
            result.update(_named_avro_types(node.get("items"), current_namespace))
        elif category == "map":
            result.update(_named_avro_types(node.get("values"), current_namespace))
    return result


def _definition_count(definitions: dict[str, Any]) -> int:
    count = 0
    for value in definitions.values():
        if isinstance(value, dict) and "type" in value:
            count += 1
        elif isinstance(value, dict):
            count += _definition_count(value)
    return count


def _assert_reserved_definition_altnames(definitions: dict[str, Any]) -> bool:
    found = False
    for key, value in definitions.items():
        if not isinstance(value, dict):
            continue
        if "type" in value:
            if key in ("type_", "definitions_"):
                assert value.get("name") == key
                assert value.get("altnames", {}).get("avro") == key[:-1]
                found = True
        else:
            found = _assert_reserved_definition_altnames(value) or found
    return found


def test_all_valid_avro_schemas_convert_to_valid_json_structure(tmp_path: Path) -> None:
    jstruct = find_jstruct()
    if not jstruct:
        pytest.fail("jstruct is required; install it or set JSTRUCT to its executable path")

    test_root = Path(__file__).parent
    outputs = []
    for source_path in sorted(test_root.rglob("*.avsc")):
        source_text = source_path.read_text(encoding="utf-8")
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", avro.errors.IgnoredLogicalType)
                avro.schema.parse(source_text)
        except avro.errors.SchemaParseException:
            continue

        source = json.loads(source_text)
        output_path = tmp_path / source_path.relative_to(test_root).with_suffix(".struct.json")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        convert_avro_to_json_structure(str(source_path), str(output_path))
        output = json.loads(output_path.read_text(encoding="utf-8"))
        assert _definition_count(output.get("definitions", {})) >= len(_named_avro_types(source))
        if _assert_reserved_definition_altnames(output.get("definitions", {})):
            assert output["$schema"] == "https://json-structure.org/meta/extended/v0/#"
            assert "JSONStructureAlternateNames" in output.get("$uses", [])
        outputs.append(output_path)

    assert outputs, "No valid Avro schemas found"
    result = subprocess.run(
        [jstruct, "check", "--format", "text", *(str(output) for output in outputs)],
        capture_output=True,
        text=True,
        check=False,
    )
    output = "\n".join(part for part in (result.stdout.strip(), result.stderr.strip()) if part)
    assert result.returncode == 0, output