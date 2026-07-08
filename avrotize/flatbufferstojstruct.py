"""Convert FlatBuffers .fbs schemas to JSON Structure via Avrotize Schema."""

from __future__ import annotations

import copy
import json
import os
import tempfile
from typing import Any

from avrotize.avrotojstruct import convert_avro_to_json_structure
from avrotize.flatbufferstoavro import convert_flatbuffers_to_avro


def _inline_root_schema(avro_schema: Any) -> Any:
    if not isinstance(avro_schema, list):
        return avro_schema
    by_name: dict[str, dict[str, Any]] = {}
    for schema in avro_schema:
        if isinstance(schema, dict) and schema.get("type") in {"record", "enum"}:
            qn = f"{schema.get('namespace')}.{schema['name']}" if schema.get("namespace") else schema["name"]
            by_name[qn] = schema
            by_name[schema["name"]] = schema
    root = next((s for s in avro_schema if isinstance(s, dict) and s.get("type") == "record" and s.get("root_type")), None)
    if root is None:
        root = next((s for s in reversed(avro_schema) if isinstance(s, dict) and s.get("type") == "record"), avro_schema[0] if avro_schema else {})

    primitives = {"null", "boolean", "int", "long", "float", "double", "bytes", "string"}

    def inline_type(value: Any, stack: set[str]) -> Any:
        if isinstance(value, str):
            if value in primitives or value not in by_name or value in stack:
                return value
            schema = by_name[value]
            qn = f"{schema.get('namespace')}.{schema['name']}" if schema.get("namespace") else schema["name"]
            return inline_schema(schema, stack | {value, qn, schema["name"]})
        if isinstance(value, list):
            return [inline_type(item, stack) for item in value]
        if isinstance(value, dict):
            out = copy.deepcopy(value)
            typ = out.get("type")
            if typ == "array":
                out["items"] = inline_type(out.get("items"), stack)
            elif typ == "map":
                out["values"] = inline_type(out.get("values"), stack)
            elif isinstance(typ, str) and typ not in primitives:
                return inline_type(typ, stack)
            return out
        return value

    def inline_schema(schema: dict[str, Any], stack: set[str]) -> dict[str, Any]:
        out = copy.deepcopy(schema)
        if out.get("type") == "record":
            for field in out.get("fields", []):
                field["type"] = inline_type(field.get("type"), stack)
        return out

    return inline_schema(root, set())


def convert_flatbuffers_to_json_structure(fbs_file_path: str, json_structure_file: str, namespace: str | None = None):
    temp_dir = os.path.dirname(os.path.abspath(json_structure_file)) or os.getcwd()
    with tempfile.NamedTemporaryFile(delete=False, suffix=".avsc", dir=temp_dir, mode="w", encoding="utf-8") as temp:
        temp_path = temp.name
    try:
        convert_flatbuffers_to_avro(fbs_file_path, temp_path, namespace=namespace)
        with open(temp_path, "r", encoding="utf-8") as f:
            avro_schema = json.load(f)
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(_inline_root_schema(avro_schema), f, indent=2)
        convert_avro_to_json_structure(temp_path, json_structure_file)
    finally:
        try:
            os.remove(temp_path)
        except OSError:
            pass
