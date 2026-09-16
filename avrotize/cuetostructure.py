"""CUE subset to JSON Structure bridge via Avrotize Schema."""

from __future__ import annotations

import os
import tempfile
import json
import copy
from typing import Any

from avrotize.avrotojstruct import convert_avro_to_json_structure
from avrotize.cuetoavro import convert_cue_to_avro


_PRIMITIVE_AVRO_TYPES = {"null", "boolean", "int", "long", "float", "double", "bytes", "string"}


def _inline_named_references(avro_schema: Any) -> Any:
    if not isinstance(avro_schema, list):
        return avro_schema
    named = {schema["name"]: schema for schema in avro_schema if isinstance(schema, dict) and schema.get("name")}
    if not avro_schema:
        return avro_schema
    root = copy.deepcopy(avro_schema[-1])

    def visit(node: Any, stack: set[str]) -> Any:
        if isinstance(node, str):
            short_name = node.split(".")[-1]
            if node not in _PRIMITIVE_AVRO_TYPES and short_name in named and short_name not in stack:
                return visit(copy.deepcopy(named[short_name]), stack | {short_name})
            return node
        if isinstance(node, list):
            return [visit(item, stack) for item in node]
        if isinstance(node, dict):
            result = copy.deepcopy(node)
            if result.get("type") == "record" and result.get("name"):
                stack = stack | {result["name"].split(".")[-1]}
            for key in ("type", "items", "values"):
                if key in result:
                    result[key] = visit(result[key], stack)
            if "fields" in result:
                result["fields"] = [visit(field, stack) for field in result["fields"]]
            return result
        return node

    return visit(root, {root.get("name", "").split(".")[-1]})


def convert_cue_to_json_structure(
    cue_file_path: str,
    json_structure_path: str,
    namespace: str | None = None,
) -> None:
    """Convert CUE subset schema to JSON Structure through an Avro temporary file."""
    output_dir = os.path.dirname(os.path.abspath(json_structure_path)) or os.getcwd()
    with tempfile.NamedTemporaryFile("w", suffix=".avsc", delete=False, dir=output_dir, encoding="utf-8") as temp_file:
        temp_avro = temp_file.name
    try:
        convert_cue_to_avro(cue_file_path, temp_avro, namespace=namespace)
        with open(temp_avro, "r", encoding="utf-8") as avro_file:
            avro_schema = json.load(avro_file)
        with open(temp_avro, "w", encoding="utf-8") as avro_file:
            json.dump(_inline_named_references(avro_schema), avro_file, indent=2)
        convert_avro_to_json_structure(temp_avro, json_structure_path)
    finally:
        if os.path.exists(temp_avro):
            os.remove(temp_avro)
