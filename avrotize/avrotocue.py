"""Avrotize Schema to CUE subset converter."""

from __future__ import annotations

import json
import re
from typing import Any


_AVRO_TO_CUE = {
    "string": "string",
    "int": "int",
    "long": "int",
    "float": "float",
    "double": "number",
    "boolean": "bool",
    "bytes": "bytes",
    "null": "null",
}


class AvroToCueConverter:
    """Emits CUE definitions for the same practical subset parsed by cuetoavro."""

    def __init__(self, namespace: str | None = None) -> None:
        self.namespace = namespace
        self.emitted: set[str] = set()

    def convert(self, avro_schema: Any) -> str:
        schemas = avro_schema if isinstance(avro_schema, list) else [avro_schema]
        namespace = self.namespace or self.find_namespace(schemas)
        lines: list[str] = []
        package = self.package_from_namespace(namespace)
        if package:
            lines.extend([f"package {package}", ""])
        for schema in schemas:
            if isinstance(schema, dict) and schema.get("type") == "record":
                lines.extend(self.record_definition(schema))
                lines.append("")
        if not any(isinstance(schema, dict) and schema.get("type") == "record" for schema in schemas):
            lines.extend(["#Document: {}", ""])
        return "\n".join(lines).rstrip() + "\n"

    @staticmethod
    def find_namespace(schemas: list[Any]) -> str | None:
        for schema in schemas:
            if isinstance(schema, dict) and schema.get("namespace"):
                return schema["namespace"]
        return None

    @staticmethod
    def package_from_namespace(namespace: str | None) -> str | None:
        if not namespace:
            return None
        name = namespace.split(".")[-1]
        name = re.sub(r"[^A-Za-z0-9_]", "_", name)
        if not name or name[0].isdigit():
            name = f"_{name}"
        return name

    def record_definition(self, record: dict[str, Any]) -> list[str]:
        name = record.get("name", "Document").split(".")[-1]
        lines = [f"#{name}: {{"]
        for field in record.get("fields", []):
            lines.append(f"  {self.field_to_cue(field)}")
        lines.append("}")
        return lines

    def field_to_cue(self, field: dict[str, Any]) -> str:
        field_name = field.get("name", "field")
        cue_type, nullable = self.type_to_cue(field.get("type", "string"))
        suffix = "?" if nullable else ""
        default = field.get("default", _NO_DEFAULT)
        if default is not _NO_DEFAULT:
            cue_type = f"{cue_type} | *{self.literal_to_cue(default)}"
        return f"{field_name}{suffix}: {cue_type}"

    def type_to_cue(self, avro_type: Any) -> tuple[str, bool]:
        if isinstance(avro_type, list):
            non_null = [item for item in avro_type if item != "null"]
            nullable = len(non_null) != len(avro_type)
            if len(non_null) == 1:
                cue_type, _ = self.type_to_cue(non_null[0])
                return cue_type, nullable
            choices = [self.type_to_cue(item)[0] for item in non_null]
            if nullable:
                choices.append("null")
            return " | ".join(choices) if choices else "null", nullable
        if isinstance(avro_type, str):
            if avro_type in _AVRO_TO_CUE:
                return _AVRO_TO_CUE[avro_type], False
            return f"#{avro_type.split('.')[-1]}", False
        if isinstance(avro_type, dict):
            type_name = avro_type.get("type")
            if isinstance(type_name, str) and type_name in _AVRO_TO_CUE:
                return _AVRO_TO_CUE[type_name], False
            if type_name == "array":
                item_type, _ = self.type_to_cue(avro_type.get("items", "string"))
                return f"[...{item_type}]", False
            if type_name == "map":
                value_type, _ = self.type_to_cue(avro_type.get("values", "string"))
                return f"{{ [string]: {value_type} }}", False
            if type_name == "enum":
                values = avro_type.get("x-cue-symbols", {})
                symbols = [values.get(symbol, symbol) for symbol in avro_type.get("symbols", [])]
                return " | ".join(self.literal_to_cue(symbol) for symbol in symbols), False
            if type_name == "record":
                fields = avro_type.get("fields", [])
                if not fields:
                    return "{}", False
                inner = ["{"]
                for field in fields:
                    inner.append(f"    {self.field_to_cue(field)}")
                inner.append("  }")
                return "\n".join(inner), False
            if isinstance(type_name, str):
                return self.type_to_cue(type_name)
        return "string", False

    @staticmethod
    def literal_to_cue(value: Any) -> str:
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        return json.dumps(str(value))


class _NoDefault:
    pass


_NO_DEFAULT = _NoDefault()


def convert_avro_to_cue(avro_schema_path: str, cue_file_path: str, namespace: str | None = None) -> None:
    """Convert an Avrotize/Avro schema file to a CUE subset file."""
    with open(avro_schema_path, "r", encoding="utf-8") as avro_file:
        avro_schema = json.load(avro_file)
    converter = AvroToCueConverter(namespace=namespace)
    cue_text = converter.convert(avro_schema)
    with open(cue_file_path, "w", encoding="utf-8") as cue_file:
        cue_file.write(cue_text)
