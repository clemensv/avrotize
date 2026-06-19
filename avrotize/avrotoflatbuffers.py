"""Convert Avrotize (Avro) schemas to FlatBuffers .fbs schemas."""

from __future__ import annotations

import json
import os
import re
from typing import Any


def _sanitize(name: str) -> str:
    cleaned = re.sub(r"\W", "_", name)
    if not cleaned or cleaned[0].isdigit():
        cleaned = f"_{cleaned}"
    return cleaned


class AvroToFlatBuffersConverter:
    primitive_map = {
        "boolean": "bool", "int": "int", "long": "long", "float": "float",
        "double": "double", "bytes": "[ubyte]", "string": "string", "null": "null",
    }

    def __init__(self, namespace: str | None = None):
        self.namespace_override = namespace
        self.named: dict[str, dict[str, Any]] = {}
        self.union_defs: dict[str, list[str]] = {}

    def convert_file(self, avro_schema_path: str) -> str:
        with open(avro_schema_path, "r", encoding="utf-8") as f:
            schema = json.load(f)
        schemas = schema if isinstance(schema, list) else [schema]
        for item in schemas:
            if isinstance(item, dict) and item.get("type") in {"record", "enum"}:
                qn = self.qualified_name(item)
                self.named[qn] = item
                self.named[item["name"]] = item
        namespace = self.namespace_override or self.first_namespace(schemas)
        lines: list[str] = []
        if namespace:
            lines.append(f"namespace {namespace};")
            lines.append("")
        for item in schemas:
            if isinstance(item, dict) and item.get("type") == "enum":
                lines.extend(self.render_enum(item)); lines.append("")
        record_lines: list[str] = []
        for item in schemas:
            if isinstance(item, dict) and item.get("type") == "record":
                record_lines.extend(self.render_record(item)); record_lines.append("")
        for name, members in self.union_defs.items():
            lines.append(f"union {name} {{ {', '.join(members)} }}")
            lines.append("")
        lines.extend(record_lines)
        root = self.find_root(schemas)
        if root:
            lines.append(f"root_type {root};")
        return "\n".join(lines).rstrip() + "\n"

    @staticmethod
    def qualified_name(schema: dict[str, Any]) -> str:
        return f"{schema.get('namespace')}.{schema['name']}" if schema.get("namespace") else schema["name"]

    @staticmethod
    def first_namespace(schemas: list[Any]) -> str:
        for item in schemas:
            if isinstance(item, dict) and item.get("namespace"):
                return item["namespace"]
        return ""

    @staticmethod
    def find_root(schemas: list[Any]) -> str | None:
        for item in schemas:
            if isinstance(item, dict) and item.get("type") == "record" and item.get("root_type"):
                return item["name"]
        for item in reversed(schemas):
            if isinstance(item, dict) and item.get("type") == "record":
                return item["name"]
        return None

    def render_enum(self, schema: dict[str, Any]) -> list[str]:
        ordinals = schema.get("ordinals", {})
        parts = []
        for i, symbol in enumerate(schema.get("symbols", [])):
            value = ordinals.get(symbol, i)
            parts.append(f"{_sanitize(symbol)} = {value}")
        return [f"enum {schema['name']} : int {{ {', '.join(parts)} }}"]

    def render_record(self, schema: dict[str, Any]) -> list[str]:
        lines = [f"table {schema['name']} {{"]
        for field in schema.get("fields", []):
            fbs_type, nullable = self.avro_type_to_fbs(field["type"], schema["name"], field["name"])
            default = ""
            avro_default = field.get("fbsDefault", field.get("default"))
            if avro_default is not None and not isinstance(field["type"], list):
                default = f" = {self.format_default(avro_default)}"
            attr = ""
            if not nullable and self.can_be_required(fbs_type):
                attr = " (required)"
            lines.append(f"  {_sanitize(field['name'])}: {fbs_type}{default}{attr};")
        lines.append("}")
        return lines

    def can_be_required(self, fbs_type: str) -> bool:
        if fbs_type.startswith("[") or fbs_type in {"bool", "byte", "ubyte", "short", "ushort", "int", "uint", "long", "ulong", "float", "double"}:
            return False
        named = self.named.get(fbs_type)
        if isinstance(named, dict) and named.get("type") == "enum":
            return False
        return True

    @staticmethod
    def format_default(value: Any) -> str:
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, str):
            return f'"{value}"'
        return str(value)

    def avro_type_to_fbs(self, avro_type: Any, record_name: str, field_name: str) -> tuple[str, bool]:
        nullable = False
        if isinstance(avro_type, list):
            nullable = "null" in avro_type
            non_null = [t for t in avro_type if t != "null"]
            if len(non_null) == 1:
                fbs, _ = self.avro_type_to_fbs(non_null[0], record_name, field_name)
                return fbs, nullable
            members = [self.avro_type_to_fbs(t, record_name, field_name)[0] for t in non_null]
            union_name = f"{record_name}_{field_name}_Union"
            self.union_defs[union_name] = [m for m in members if not m.startswith("[")]
            return union_name, nullable
        if isinstance(avro_type, str):
            return self.primitive_map.get(avro_type, avro_type.split(".")[-1]), nullable
        if isinstance(avro_type, dict):
            typ = avro_type.get("type")
            if typ == "array":
                item, _ = self.avro_type_to_fbs(avro_type.get("items"), record_name, field_name)
                return f"[{item}]", nullable
            if typ == "map":
                return "string", nullable
            if typ == "enum" or typ == "record":
                self.named[avro_type["name"]] = avro_type
                return avro_type["name"], nullable
            if typ == "bytes":
                return "[ubyte]", nullable
            return self.primitive_map.get(typ, typ), nullable
        return "string", nullable


def convert_avro_to_flatbuffers(avro_schema_path: str, fbs_file_path: str, namespace: str | None = None):
    if not os.path.exists(avro_schema_path):
        raise FileNotFoundError(f"Avro schema file {avro_schema_path} does not exist.")
    converter = AvroToFlatBuffersConverter(namespace)
    fbs_schema = converter.convert_file(avro_schema_path)
    with open(fbs_file_path, "w", encoding="utf-8") as f:
        f.write(fbs_schema)
