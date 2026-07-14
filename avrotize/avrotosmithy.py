"""Convert Avrotize Schema (Avro) data shapes to Smithy 2.0 IDL."""

from __future__ import annotations

import json
import os
import re
from typing import Any


AVRO_TO_SMITHY_PRIMITIVES = {
    "null": "Document",
    "boolean": "Boolean",
    "bytes": "Blob",
    "string": "String",
    "int": "Integer",
    "long": "Long",
    "float": "Float",
    "double": "Double",
}


class AvroToSmithyConverter:
    """Convert Avro schemas to Smithy data shapes."""

    def __init__(self, namespace: str | None = None) -> None:
        self.namespace = namespace
        self.generated_shapes: list[str] = []
        self.generated_names: set[str] = set()
        self.collection_types: dict[str, str] = {}

    def convert_schema(self, avro_schema: Any) -> str:
        schemas = avro_schema if self._is_schema_list(avro_schema) else [avro_schema]
        self.namespace = self.namespace or self._discover_namespace(schemas) or "smithy.generated"
        self._index_collection_wrappers(schemas)
        lines = ['$version: "2.0"', f"namespace {self.namespace}", ""]
        body: list[str] = []
        for schema in schemas:
            body.extend(self.shape_to_smithy(schema))
            if body and body[-1] != "":
                body.append("")
        if self.generated_shapes:
            body.extend(self.generated_shapes)
        return "\n".join(lines + body).rstrip() + "\n"

    @staticmethod
    def _discover_namespace(schemas: list[Any]) -> str | None:
        for schema in schemas:
            if isinstance(schema, dict) and schema.get("namespace"):
                return schema["namespace"]
        return None

    @staticmethod
    def _is_union(schema: Any) -> bool:
        return isinstance(schema, list) and any(item != "null" for item in schema)

    @staticmethod
    def _is_schema_list(schema: Any) -> bool:
        return isinstance(schema, list) and all(isinstance(item, dict) and "type" in item and "name" in item for item in schema)

    def _index_collection_wrappers(self, schemas: list[Any]) -> None:
        for schema in schemas:
            if not isinstance(schema, dict) or schema.get("type") != "record" or not schema.get("fields"):
                continue
            marker = schema.get("smithyShape")
            field_type = schema["fields"][0].get("type")
            if marker == "list" and isinstance(field_type, dict) and field_type.get("type") == "array":
                self.collection_types[self._signature(field_type)] = schema["name"]
            elif marker == "map" and isinstance(field_type, dict) and field_type.get("type") == "map":
                self.collection_types[self._signature(field_type)] = schema["name"]

    @staticmethod
    def _signature(avro_type: Any) -> str:
        return json.dumps(avro_type, sort_keys=True)

    def shape_to_smithy(self, schema: Any) -> list[str]:
        if isinstance(schema, list):
            name = self.unique_name("RootUnion")
            return self.union_shape_lines(name, schema)
        if isinstance(schema, str):
            return []
        schema_type = schema.get("type")
        if schema_type == "record" and schema.get("smithyShape") == "union":
            return self.record_union_lines(schema)
        if schema_type == "record" and schema.get("smithyShape") == "list":
            return self.record_list_lines(schema)
        if schema_type == "record" and schema.get("smithyShape") == "map":
            return self.record_map_lines(schema)
        if schema_type == "record":
            return self.record_lines(schema)
        if schema_type == "enum":
            return self.enum_lines(schema)
        if schema_type == "array":
            return self.list_lines(schema.get("name", self.unique_name("RootList")), schema.get("items", "string"))
        if schema_type == "map":
            return self.map_lines(schema.get("name", self.unique_name("RootMap")), schema.get("values", "string"))
        return []

    def record_lines(self, schema: dict[str, Any]) -> list[str]:
        lines = self.doc_lines(schema)
        lines.append(f"structure {schema['name']} {{")
        for field in schema.get("fields", []):
            lines.extend(self.field_lines(schema["name"], field))
        lines.append("}")
        return lines

    def record_union_lines(self, schema: dict[str, Any]) -> list[str]:
        lines = self.doc_lines(schema)
        lines.append(f"union {schema['name']} {{")
        for field in schema.get("fields", []):
            lines.extend(self.member_lines(schema["name"], field, include_required=False))
        lines.append("}")
        return lines

    def record_list_lines(self, schema: dict[str, Any]) -> list[str]:
        field = schema.get("fields", [{}])[0]
        field_type = field.get("type", {}) if isinstance(field, dict) else {}
        items = field_type.get("items", "string") if isinstance(field_type, dict) else "string"
        return self.list_lines(schema["name"], items, schema)

    def record_map_lines(self, schema: dict[str, Any]) -> list[str]:
        field = schema.get("fields", [{}])[0]
        field_type = field.get("type", {}) if isinstance(field, dict) else {}
        values = field_type.get("values", "string") if isinstance(field_type, dict) else "string"
        return self.map_lines(schema["name"], values, schema)

    def enum_lines(self, schema: dict[str, Any]) -> list[str]:
        lines = self.doc_lines(schema)
        ordinals = schema.get("ordinals")
        if ordinals:
            lines.append(f"intEnum {schema['name']} {{")
            for idx, symbol in enumerate(schema.get("symbols", [])):
                lines.append(f"    {symbol} = {ordinals.get(symbol, idx)}")
        else:
            lines.append(f"enum {schema['name']} {{")
            for symbol in schema.get("symbols", []):
                lines.append(f"    {symbol}")
        lines.append("}")
        return lines

    def list_lines(self, name: str, items: Any, schema: dict[str, Any] | None = None) -> list[str]:
        lines = self.doc_lines(schema or {})
        lines.append(f"list {name} {{")
        lines.append(f"    member: {self.avro_type_to_smithy(name, 'member', items)}")
        lines.append("}")
        return lines

    def map_lines(self, name: str, values: Any, schema: dict[str, Any] | None = None) -> list[str]:
        lines = self.doc_lines(schema or {})
        lines.append(f"map {name} {{")
        lines.append("    key: String")
        lines.append(f"    value: {self.avro_type_to_smithy(name, 'value', values)}")
        lines.append("}")
        return lines

    def union_shape_lines(self, name: str, union_type: list[Any]) -> list[str]:
        lines = [f"union {name} {{"]
        for index, branch in enumerate([item for item in union_type if item != "null"]):
            member_name = self.member_name_from_type(branch, index)
            lines.append(f"    {member_name}: {self.avro_type_to_smithy(name, member_name, branch)}")
        lines.append("}")
        return lines

    def field_lines(self, record_name: str, field: dict[str, Any]) -> list[str]:
        field_type = field.get("type", "string")
        nullable, non_null = self.unwrap_nullable(field_type)
        if isinstance(non_null, list):
            union_name = self.emit_union(record_name, field["name"], non_null)
            non_null = union_name
        lines = self.member_doc_lines(field)
        if not nullable:
            lines.append("    @required")
        if "default" in field and field.get("default") is not None:
            lines.append(f"    @default({self.format_literal(field['default'])})")
        lines.append(f"    {field['name']}: {self.avro_type_to_smithy(record_name, field['name'], non_null)}")
        return lines

    def member_lines(self, record_name: str, field: dict[str, Any], include_required: bool) -> list[str]:
        nullable, non_null = self.unwrap_nullable(field.get("type", "string"))
        lines = self.member_doc_lines(field)
        if include_required and not nullable:
            lines.append("    @required")
        lines.append(f"    {field['name']}: {self.avro_type_to_smithy(record_name, field['name'], non_null)}")
        return lines

    def avro_type_to_smithy(self, owner_name: str, field_name: str, avro_type: Any) -> str:
        if isinstance(avro_type, list):
            nullable, non_null = self.unwrap_nullable(avro_type)
            if isinstance(non_null, list):
                return self.emit_union(owner_name, field_name, non_null)
            return self.avro_type_to_smithy(owner_name, field_name, non_null)
        if isinstance(avro_type, str):
            return AVRO_TO_SMITHY_PRIMITIVES.get(avro_type, avro_type.split(".")[-1])
        if isinstance(avro_type, dict):
            if avro_type.get("logicalType") == "timestamp-millis" and avro_type.get("type") == "long":
                return "Timestamp"
            typ = avro_type.get("type")
            if typ == "array":
                existing = self.collection_types.get(self._signature(avro_type))
                if existing:
                    return existing
                name = self.unique_name(f"{owner_name}{self.pascal(field_name)}List")
                self.generated_shapes.extend(self.list_lines(name, avro_type.get("items", "string")))
                self.generated_shapes.append("")
                return name
            if typ == "map":
                existing = self.collection_types.get(self._signature(avro_type))
                if existing:
                    return existing
                name = self.unique_name(f"{owner_name}{self.pascal(field_name)}Map")
                self.generated_shapes.extend(self.map_lines(name, avro_type.get("values", "string")))
                self.generated_shapes.append("")
                return name
            if typ == "record":
                nested_name = avro_type.get("name", self.unique_name(f"{owner_name}{self.pascal(field_name)}"))
                if nested_name not in self.generated_names:
                    self.generated_names.add(nested_name)
                    self.generated_shapes.extend(self.shape_to_smithy(avro_type))
                    self.generated_shapes.append("")
                return nested_name
            if typ == "enum":
                enum_name = avro_type.get("name", self.unique_name(f"{owner_name}{self.pascal(field_name)}Enum"))
                if enum_name not in self.generated_names:
                    self.generated_names.add(enum_name)
                    self.generated_shapes.extend(self.enum_lines(avro_type))
                    self.generated_shapes.append("")
                return enum_name
            return self.avro_type_to_smithy(owner_name, field_name, typ)
        return "Document"

    def emit_union(self, owner_name: str, field_name: str, union_type: list[Any]) -> str:
        name = self.unique_name(f"{owner_name}{self.pascal(field_name)}Union")
        self.generated_shapes.extend(self.union_shape_lines(name, union_type))
        self.generated_shapes.append("")
        return name

    @staticmethod
    def unwrap_nullable(avro_type: Any) -> tuple[bool, Any]:
        if isinstance(avro_type, list):
            non_null = [item for item in avro_type if item != "null"]
            if len(non_null) == 1:
                return len(non_null) != len(avro_type), non_null[0]
            return "null" in avro_type, non_null
        return False, avro_type

    def unique_name(self, base: str) -> str:
        base = self.pascal(base)
        name = base
        counter = 2
        while name in self.generated_names:
            name = f"{base}{counter}"
            counter += 1
        self.generated_names.add(name)
        return name

    @staticmethod
    def member_name_from_type(avro_type: Any, index: int) -> str:
        if isinstance(avro_type, str):
            base = avro_type.split(".")[-1]
        elif isinstance(avro_type, dict):
            base = avro_type.get("name") or avro_type.get("type", "choice")
        else:
            base = "choice"
        name = re.sub(r"[^A-Za-z0-9_]", "_", base)
        if not re.match(r"[A-Za-z_]", name):
            name = f"choice{index + 1}"
        return name[:1].lower() + name[1:]

    @staticmethod
    def doc_lines(schema: dict[str, Any]) -> list[str]:
        doc = schema.get("doc")
        return [f"@documentation({AvroToSmithyConverter.format_literal(doc)})"] if doc else []

    @staticmethod
    def member_doc_lines(field: dict[str, Any]) -> list[str]:
        doc = field.get("doc")
        return [f"    @documentation({AvroToSmithyConverter.format_literal(doc)})"] if doc else []

    @staticmethod
    def format_literal(value: Any) -> str:
        if isinstance(value, str):
            return json.dumps(value)
        if value is True:
            return "true"
        if value is False:
            return "false"
        if value is None:
            return "null"
        return str(value)

    @staticmethod
    def pascal(name: str) -> str:
        parts = re.split(r"[^A-Za-z0-9]+", name)
        return "".join(part[:1].upper() + part[1:] for part in parts if part) or "Shape"


def convert_avro_to_smithy_schema(avro_schema: Any, namespace: str | None = None) -> str:
    """Convert an Avro schema object to Smithy IDL text."""
    return AvroToSmithyConverter(namespace).convert_schema(avro_schema)


def convert_avro_to_smithy(avro_schema_path: str, smithy_file_path: str, namespace: str | None = None) -> None:
    """Convert an Avrotize Schema JSON file to Smithy IDL."""
    with open(avro_schema_path, "r", encoding="utf-8") as avro_file:
        avro_schema = json.load(avro_file)
    smithy_text = convert_avro_to_smithy_schema(avro_schema, namespace)
    output_dir = os.path.dirname(os.path.abspath(smithy_file_path))
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    with open(smithy_file_path, "w", encoding="utf-8") as smithy_file:
        smithy_file.write(smithy_text)
