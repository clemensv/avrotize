"""Convert SurrealQL schema definitions to Avrotize Schema."""

import json
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List

from avrotize.common import avro_name

JsonNode = Dict[str, "JsonNode"] | List["JsonNode"] | str | bool | int | float | None


@dataclass
class FieldDefinition:
    """Parsed SurrealQL field definition."""

    path: str
    type_expr: str
    doc: str | None = None


@dataclass
class TableDefinition:
    """Parsed SurrealQL table definition."""

    name: str
    schemafull: bool = True
    fields: List[FieldDefinition] = field(default_factory=list)


@dataclass
class FieldNode:
    """Tree node used to reconstruct nested Avro records from dotted field paths."""

    name: str
    type_expr: str | None = None
    doc: str | None = None
    children: Dict[str, "FieldNode"] = field(default_factory=dict)
    array_item: "FieldNode | None" = None


class SurrealToAvroConverter:
    """Converter for SurrealQL DEFINE TABLE/FIELD schema statements."""

    KEYWORDS = "DEFAULT|ASSERT|READONLY|COMMENT|PERMISSIONS|FLEXIBLE|VALUE"

    def __init__(self, namespace: str | None = None) -> None:
        self.namespace = namespace

    def parse(self, surrealql: str) -> List[TableDefinition]:
        """Parse supported SurrealQL schema statements."""
        tables: Dict[str, TableDefinition] = {}
        for statement in self._split_statements(self._strip_line_comments(surrealql)):
            table_match = re.match(
                r"^DEFINE\s+TABLE\s+(`[^`]+`|[A-Za-z_][\w:-]*)\s+(SCHEMAFULL|SCHEMALESS)\b",
                statement,
                flags=re.IGNORECASE | re.DOTALL,
            )
            if table_match:
                table_name = self._clean_identifier(table_match.group(1))
                tables[table_name] = TableDefinition(
                    name=table_name,
                    schemafull=table_match.group(2).upper() == "SCHEMAFULL",
                )
                continue

            field_match = re.match(
                r"^DEFINE\s+FIELD\s+(.+?)\s+ON\s+(?:TABLE\s+)?(`[^`]+`|[A-Za-z_][\w:-]*)\b(.*)$",
                statement,
                flags=re.IGNORECASE | re.DOTALL,
            )
            if field_match:
                path = self._clean_path(field_match.group(1).strip())
                table_name = self._clean_identifier(field_match.group(2))
                tail = field_match.group(3).strip()
                type_expr = self._extract_type(tail)
                doc = self._extract_comment(tail)
                table = tables.setdefault(table_name, TableDefinition(name=table_name))
                table.fields.append(FieldDefinition(path=path, type_expr=type_expr, doc=doc))
        return list(tables.values())

    def convert_schema(self, surrealql: str) -> JsonNode:
        """Convert SurrealQL text to an Avrotize Schema document."""
        records = [self._table_to_record(table) for table in self.parse(surrealql)]
        if not records:
            raise ValueError("No supported SurrealQL DEFINE TABLE or DEFINE FIELD statements found")
        return records[0] if len(records) == 1 else records

    def _table_to_record(self, table: TableDefinition) -> Dict[str, JsonNode]:
        root = FieldNode(table.name)
        for field_def in table.fields:
            self._insert_field(root, field_def)
        record: Dict[str, JsonNode] = {
            "type": "record",
            "name": avro_name(table.name),
            "fields": self._node_children_to_fields(root, avro_name(table.name)),
        }
        if self.namespace:
            record["namespace"] = self.namespace
        if not table.schemafull:
            record["doc"] = "SurrealDB SCHEMALESS table; Avro captures declared fields only."
        return record

    def _insert_field(self, root: FieldNode, field_def: FieldDefinition) -> None:
        parts = [part for part in field_def.path.split(".") if part]
        node = root
        for index, part in enumerate(parts):
            is_array_item = part.endswith("[*]")
            name = part[:-3] if is_array_item else part
            if name not in node.children:
                node.children[name] = FieldNode(name)
            node = node.children[name]
            if is_array_item:
                if node.array_item is None:
                    node.array_item = FieldNode("item")
                node = node.array_item
            if index == len(parts) - 1:
                node.type_expr = field_def.type_expr
                node.doc = field_def.doc

    def _node_children_to_fields(self, node: FieldNode, parent_name: str) -> List[Dict[str, JsonNode]]:
        return [self._node_to_field(child, parent_name) for child in node.children.values()]

    def _node_to_field(self, node: FieldNode, parent_name: str) -> Dict[str, JsonNode]:
        field_type = self._node_to_type(node, parent_name)
        field_schema: Dict[str, JsonNode] = {"name": avro_name(node.name), "type": field_type}
        if node.doc:
            field_schema["doc"] = node.doc
        if isinstance(field_type, list) and field_type and field_type[0] == "null":
            field_schema["default"] = None
        return field_schema

    def _node_to_type(self, node: FieldNode, parent_name: str) -> JsonNode:
        type_expr = node.type_expr or ("array" if node.array_item else "object" if node.children else "string")
        nullable, base_type = self._unwrap_option(type_expr)
        if node.array_item is not None:
            item_type = self._node_to_type(node.array_item, self._nested_name(parent_name, node.name))
            avro_type: JsonNode = {"type": "array", "items": self._non_nullable(item_type)}
        elif node.children and self._base_type_name(base_type) in {"object", "array", "set"}:
            if self._base_type_name(base_type) in {"array", "set"}:
                item_record = self._record_for_node(node, self._nested_name(parent_name, node.name))
                avro_type = {"type": "array", "items": item_record}
            else:
                avro_type = self._record_for_node(node, self._nested_name(parent_name, node.name))
        else:
            avro_type = self._surreal_type_to_avro(base_type, self._nested_name(parent_name, node.name))
        if nullable:
            return ["null", avro_type]
        return avro_type

    def _record_for_node(self, node: FieldNode, record_name: str) -> Dict[str, JsonNode]:
        return {
            "type": "record",
            "name": avro_name(record_name),
            "fields": self._node_children_to_fields(node, avro_name(record_name)),
        }

    def _surreal_type_to_avro(self, type_expr: str, record_name: str) -> JsonNode:
        nullable, base_type = self._unwrap_option(type_expr)
        if nullable:
            return ["null", self._surreal_type_to_avro(base_type, record_name)]
        name = self._base_type_name(base_type)
        inner = self._generic_inner(base_type)
        if name == "string":
            return "string"
        if name == "int":
            return "long"
        if name in {"float", "number"}:
            return "double"
        if name == "decimal":
            return {"type": "bytes", "logicalType": "decimal", "precision": 38, "scale": 9}
        if name == "bool":
            return "boolean"
        if name == "datetime":
            return {"type": "long", "logicalType": "timestamp-millis"}
        if name == "duration":
            return "string"
        if name == "uuid":
            return {"type": "string", "logicalType": "uuid"}
        if name == "bytes":
            return "bytes"
        if name == "object":
            return {"type": "record", "name": avro_name(record_name), "fields": []}
        if name in {"array", "set"}:
            items = self._surreal_type_to_avro(inner, f"{record_name}_item") if inner else "string"
            return {"type": "array", "items": self._non_nullable(items)}
        if name == "record":
            target = inner or "record"
            return {"type": "string", "surrealType": f"record<{target}>", "doc": "SurrealDB record references are represented as record id strings."}
        if name == "geometry":
            return "string"
        return "string"

    @staticmethod
    def _non_nullable(avro_type: JsonNode) -> JsonNode:
        if isinstance(avro_type, list):
            non_null = [item for item in avro_type if item != "null"]
            return non_null[0] if len(non_null) == 1 else non_null
        return avro_type

    @staticmethod
    def _unwrap_option(type_expr: str) -> tuple[bool, str]:
        stripped = type_expr.strip()
        if stripped.lower().startswith("option<") and stripped.endswith(">"):
            return True, stripped[7:-1].strip()
        return False, stripped

    @staticmethod
    def _base_type_name(type_expr: str) -> str:
        return type_expr.strip().split("<", 1)[0].strip().lower()

    @staticmethod
    def _generic_inner(type_expr: str) -> str | None:
        text = type_expr.strip()
        start = text.find("<")
        if start == -1 or not text.endswith(">"):
            return None
        return text[start + 1 : -1].strip()

    @staticmethod
    def _nested_name(parent_name: str, field_name: str) -> str:
        return f"{parent_name}_{avro_name(field_name)}"

    @classmethod
    def _extract_type(cls, tail: str) -> str:
        match = re.search(rf"\bTYPE\s+(.+?)(?=\s+(?:{cls.KEYWORDS})\b|$)", tail, flags=re.IGNORECASE | re.DOTALL)
        if not match:
            return "string"
        return match.group(1).strip()

    @staticmethod
    def _extract_comment(tail: str) -> str | None:
        match = re.search(r"\bCOMMENT\s+(['\"])(.*?)\1", tail, flags=re.IGNORECASE | re.DOTALL)
        return match.group(2) if match else None

    @staticmethod
    def _clean_identifier(identifier: str) -> str:
        return identifier.strip().strip("`")

    @classmethod
    def _clean_path(cls, path: str) -> str:
        return ".".join(cls._clean_identifier(part) for part in path.split("."))

    @staticmethod
    def _strip_line_comments(text: str) -> str:
        lines = []
        for line in text.splitlines():
            lines.append(re.split(r"\s+(?://|--)\s*", line, maxsplit=1)[0])
        return "\n".join(lines)

    @staticmethod
    def _split_statements(text: str) -> List[str]:
        statements: List[str] = []
        current: List[str] = []
        quote: str | None = None
        depth = 0
        for char in text:
            if quote:
                current.append(char)
                if char == quote:
                    quote = None
                continue
            if char in {"'", '"'}:
                quote = char
                current.append(char)
                continue
            if char == "<":
                depth += 1
            elif char == ">" and depth:
                depth -= 1
            if char == ";" and depth == 0:
                statement = "".join(current).strip()
                if statement:
                    statements.append(statement)
                current = []
            else:
                current.append(char)
        statement = "".join(current).strip()
        if statement:
            statements.append(statement)
        return statements


def convert_surrealql_to_avro(surrealql_file_path: str, avro_schema_path: str, namespace: str | None = None):
    """Convert a SurrealQL schema file to an Avrotize Schema file."""
    with open(surrealql_file_path, "r", encoding="utf-8") as surrealql_file:
        surrealql = surrealql_file.read()
    schema = SurrealToAvroConverter(namespace=namespace).convert_schema(surrealql)
    with open(avro_schema_path, "w", encoding="utf-8") as avro_file:
        json.dump(schema, avro_file, indent=2)
