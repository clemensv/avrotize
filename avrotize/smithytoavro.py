"""Convert Smithy 2.0 IDL data shapes to Avrotize Schema (Avro)."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from typing import Any

from avrotize.dependency_resolver import sort_messages_by_dependencies

AvroType = dict[str, Any] | list[Any] | str

_IDENTIFIER_RE = re.compile(r"@[A-Za-z_][A-Za-z0-9_.$#-]*|[A-Za-z_$][A-Za-z0-9_.$#-]*|\"(?:\\.|[^\"])*\"|-?\d+(?:\.\d+)?|[{}()\[\]:,=]")
_NAME_RE = re.compile(r"[^A-Za-z0-9_]")

SMITHY_TO_AVRO_PRIMITIVES: dict[str, AvroType] = {
    "Blob": "bytes",
    "Boolean": "boolean",
    "String": "string",
    "Byte": "int",
    "Short": "int",
    "Integer": "int",
    "Long": "long",
    "Float": "float",
    "Double": "double",
    "BigInteger": "string",
    "BigDecimal": "double",
    "Timestamp": {"type": "long", "logicalType": "timestamp-millis"},
    "Document": "string",
}


@dataclass
class SmithyShape:
    kind: str
    name: str
    traits: dict[str, Any] = field(default_factory=dict)
    members: list[dict[str, Any]] = field(default_factory=list)
    member_type: str | None = None
    key_type: str | None = None
    value_type: str | None = None
    enum_values: list[tuple[str, int | None]] = field(default_factory=list)


class SmithyIdlParser:
    """Small Smithy IDL parser for phase-1 data-shape conversion."""

    def __init__(self, text: str) -> None:
        self.tokens = self._tokenize(text)
        self.index = 0
        self.namespace: str | None = None
        self.shapes: list[SmithyShape] = []

    @staticmethod
    def _strip_comments(text: str) -> str:
        result: list[str] = []
        i = 0
        in_string = False
        while i < len(text):
            ch = text[i]
            nxt = text[i + 1] if i + 1 < len(text) else ""
            if ch == '"' and (i == 0 or text[i - 1] != "\\"):
                in_string = not in_string
                result.append(ch)
                i += 1
            elif not in_string and ch == "/" and nxt == "/":
                while i < len(text) and text[i] not in "\r\n":
                    i += 1
            elif not in_string and ch == "#":
                while i < len(text) and text[i] not in "\r\n":
                    i += 1
            else:
                result.append(ch)
                i += 1
        return "".join(result)

    @classmethod
    def _tokenize(cls, text: str) -> list[str]:
        return [m.group(0) for m in _IDENTIFIER_RE.finditer(cls._strip_comments(text))]

    def peek(self, offset: int = 0) -> str | None:
        pos = self.index + offset
        return self.tokens[pos] if pos < len(self.tokens) else None

    def pop(self) -> str:
        tok = self.peek()
        if tok is None:
            raise ValueError("Unexpected end of Smithy IDL")
        self.index += 1
        return tok

    def consume(self, token: str) -> bool:
        if self.peek() == token:
            self.index += 1
            return True
        return False

    def expect(self, token: str) -> None:
        actual = self.pop()
        if actual != token:
            raise ValueError(f"Expected {token!r}, got {actual!r}")

    def parse(self) -> tuple[str | None, list[SmithyShape]]:
        pending_traits: dict[str, Any] = {}
        while self.peek() is not None:
            tok = self.peek()
            if tok == ",":
                self.pop()
                continue
            if tok and tok.startswith("@"):
                pending_traits.update(self.parse_trait())
                continue
            if tok == "$version":
                self.pop()
                if self.consume(":") or self.consume("="):
                    self.pop()
                continue
            if tok == "metadata":
                self.parse_metadata()
                pending_traits = {}
                continue
            if tok == "namespace":
                self.pop()
                self.namespace = self.pop()
                pending_traits = {}
                continue
            if tok in {"structure", "union", "list", "map", "enum", "intEnum"}:
                self.shapes.append(self.parse_shape(pending_traits))
                pending_traits = {}
                continue
            if tok in {"service", "operation", "resource"}:
                self.skip_statement_or_block()
                pending_traits = {}
                continue
            if tok == "apply":
                self.parse_apply()
                pending_traits = {}
                continue
            self.pop()
            pending_traits = {}
        return self.namespace, self.shapes


    def parse_metadata(self) -> None:
        self.pop()
        if self.peek() is not None:
            self.pop()
        if self.consume(":") or self.consume("="):
            if self.peek() == "[":
                self.skip_bracketed("[", "]")
            elif self.peek() == "{":
                self.skip_braces()
            elif self.peek() is not None:
                self.pop()
        self.consume(",")

    def parse_apply(self) -> None:
        self.pop()
        if self.peek() is not None:
            self.pop()
        while self.peek() and self.peek().startswith("@"):
            self.parse_trait()
        self.consume(",")

    def parse_trait(self) -> dict[str, Any]:
        name = self.pop()[1:]
        value: Any = True
        if self.consume("("):
            collected: list[str] = []
            depth = 1
            while depth and self.peek() is not None:
                tok = self.pop()
                if tok == "(":
                    depth += 1
                elif tok == ")":
                    depth -= 1
                    if depth == 0:
                        break
                collected.append(tok)
            value = self._trait_value(collected)
        return {name.split("#")[-1].split(".")[-1]: value}

    @staticmethod
    def _trait_value(tokens: list[str]) -> Any:
        if not tokens:
            return True
        if len(tokens) == 1:
            return _literal(tokens[0])
        if tokens[0] in {"[", "{"}:
            return " ".join(tokens)
        values: dict[str, Any] = {}
        i = 0
        while i < len(tokens):
            key = tokens[i]
            if i + 2 < len(tokens) and tokens[i + 1] in {":", "="}:
                values[key] = _literal(tokens[i + 2])
                i += 3
            else:
                i += 1
        return values or " ".join(tokens)

    def parse_shape(self, traits: dict[str, Any]) -> SmithyShape:
        kind = self.pop()
        name = sanitize_name(self.pop())
        while self.peek() == "with":
            self.pop()
            self.skip_bracketed("[", "]")
        shape = SmithyShape(kind=kind, name=name, traits=traits.copy())
        self.expect("{")
        if kind == "structure":
            shape.members = self.parse_members(until="}")
        elif kind == "union":
            shape.members = self.parse_members(until="}", union_members=True)
        elif kind == "list":
            self.parse_collection(shape, list_mode=True)
        elif kind == "map":
            self.parse_collection(shape, list_mode=False)
        elif kind in {"enum", "intEnum"}:
            shape.enum_values = self.parse_enum_values(kind == "intEnum")
        self.expect("}")
        return shape

    def parse_members(self, until: str, union_members: bool = False) -> list[dict[str, Any]]:
        members: list[dict[str, Any]] = []
        pending_traits: dict[str, Any] = {}
        while self.peek() is not None and self.peek() != until:
            tok = self.peek()
            if tok == ",":
                self.pop()
                continue
            if tok and tok.startswith("@"):
                pending_traits.update(self.parse_trait())
                continue
            name = sanitize_member_name(self.pop())
            if not self.consume(":"):
                pending_traits = {}
                continue
            type_name = self.pop()
            members.append({"name": name, "type": type_name, "traits": pending_traits.copy()})
            pending_traits = {}
            self.consume(",")
        return members

    def parse_collection(self, shape: SmithyShape, list_mode: bool) -> None:
        pending_traits: dict[str, Any] = {}
        while self.peek() is not None and self.peek() != "}":
            tok = self.peek()
            if tok == ",":
                self.pop()
                continue
            if tok and tok.startswith("@"):
                pending_traits.update(self.parse_trait())
                continue
            name = self.pop()
            if self.consume(":"):
                value = self.pop()
                if list_mode and name == "member":
                    shape.member_type = value
                    shape.members.append({"name": "member", "type": value, "traits": pending_traits.copy()})
                elif not list_mode and name == "key":
                    shape.key_type = value
                elif not list_mode and name == "value":
                    shape.value_type = value
                    shape.members.append({"name": "value", "type": value, "traits": pending_traits.copy()})
            pending_traits = {}
            self.consume(",")

    def parse_enum_values(self, int_enum: bool) -> list[tuple[str, int | None]]:
        values: list[tuple[str, int | None]] = []
        while self.peek() is not None and self.peek() != "}":
            if self.peek() == ",":
                self.pop()
                continue
            if self.peek() and self.peek().startswith("@"):  # member traits ignored for enum values
                self.parse_trait()
                continue
            name = sanitize_enum_symbol(self.pop())
            number: int | None = None
            if int_enum and self.consume("="):
                number = int(float(self.pop()))
            values.append((name, number))
            self.consume(",")
        return values

    def skip_bracketed(self, start: str, end: str) -> None:
        if not self.consume(start):
            return
        depth = 1
        while depth and self.peek() is not None:
            tok = self.pop()
            if tok == start:
                depth += 1
            elif tok == end:
                depth -= 1

    def skip_statement_or_block(self) -> None:
        first = self.pop()
        while self.peek() is not None:
            tok = self.peek()
            if tok == "{":
                self.skip_braces()
                return
            if first in {"service", "operation", "resource"} and tok in {"structure", "union", "list", "map", "enum", "intEnum", "service", "operation", "resource", "namespace", "apply", "metadata"}:
                return
            self.pop()
            if tok == ",":
                return

    def skip_braces(self) -> None:
        self.expect("{")
        depth = 1
        while depth and self.peek() is not None:
            tok = self.pop()
            if tok == "{":
                depth += 1
            elif tok == "}":
                depth -= 1


def _literal(token: str) -> Any:
    if token.startswith('"') and token.endswith('"'):
        return bytes(token[1:-1], "utf-8").decode("unicode_escape")
    if token == "true":
        return True
    if token == "false":
        return False
    if token == "null":
        return None
    if re.fullmatch(r"-?\d+", token):
        return int(token)
    if re.fullmatch(r"-?\d+\.\d+", token):
        return float(token)
    return token


def sanitize_name(name: str) -> str:
    name = name.split("#")[-1].split(".")[-1]
    name = _NAME_RE.sub("_", name)
    if not name or not re.match(r"[A-Za-z_]", name):
        name = f"Shape_{name}"
    return name


def sanitize_member_name(name: str) -> str:
    name = _NAME_RE.sub("_", name.split("#")[-1])
    if not name or not re.match(r"[A-Za-z_]", name):
        name = f"field_{name}"
    return name


def sanitize_enum_symbol(name: str) -> str:
    name = sanitize_name(name).upper() if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name) else name
    if not re.match(r"[A-Za-z_]", name):
        name = f"VALUE_{name}"
    return name


class SmithyToAvroConverter:
    """Convert parsed Smithy data shapes to Avrotize Schema."""

    def __init__(self, namespace: str | None = None) -> None:
        self.namespace = namespace
        self.shapes: dict[str, SmithyShape] = {}

    def convert_text(self, text: str) -> list[dict[str, Any]]:
        parsed_namespace, shapes = SmithyIdlParser(text).parse()
        self.namespace = self.namespace or parsed_namespace or "smithy"
        self.shapes = {shape.name: shape for shape in shapes}
        schemas = [self.shape_to_avro(shape) for shape in shapes]
        return sort_messages_by_dependencies([schema for schema in schemas if schema is not None])

    def shape_to_avro(self, shape: SmithyShape) -> dict[str, Any]:
        if shape.kind == "structure":
            return self.structure_to_avro(shape)
        if shape.kind == "union":
            return self.union_to_avro(shape)
        if shape.kind == "enum":
            return self.enum_to_avro(shape, int_enum=False)
        if shape.kind == "intEnum":
            return self.enum_to_avro(shape, int_enum=True)
        if shape.kind == "list":
            return self.collection_wrapper_to_avro(shape, "list")
        if shape.kind == "map":
            return self.collection_wrapper_to_avro(shape, "map")
        raise ValueError(f"Unsupported Smithy shape kind: {shape.kind}")

    def base_named_schema(self, shape: SmithyShape, avro_type: str = "record") -> dict[str, Any]:
        schema: dict[str, Any] = {"type": avro_type, "name": shape.name, "namespace": self.namespace}
        doc = trait_doc(shape.traits)
        if doc:
            schema["doc"] = doc
        return schema

    def structure_to_avro(self, shape: SmithyShape) -> dict[str, Any]:
        schema = self.base_named_schema(shape)
        schema["fields"] = [self.member_to_field(member) for member in shape.members]
        deps = self.dependencies_for_members(shape.members)
        if deps:
            schema["dependencies"] = deps
        return schema

    def union_to_avro(self, shape: SmithyShape) -> dict[str, Any]:
        schema = self.base_named_schema(shape)
        schema["smithyShape"] = "union"
        schema["fields"] = []
        for member in shape.members:
            field_schema = self.member_to_field(member, force_optional=True)
            schema["fields"].append(field_schema)
        deps = self.dependencies_for_members(shape.members)
        if deps:
            schema["dependencies"] = deps
        return schema

    def enum_to_avro(self, shape: SmithyShape, int_enum: bool) -> dict[str, Any]:
        schema = self.base_named_schema(shape, "enum")
        schema["symbols"] = [symbol for symbol, _ in shape.enum_values]
        if int_enum:
            schema["ordinals"] = {symbol: (idx if value is None else value) for idx, (symbol, value) in enumerate(shape.enum_values)}
            schema["smithyShape"] = "intEnum"
        return schema

    def collection_wrapper_to_avro(self, shape: SmithyShape, kind: str) -> dict[str, Any]:
        schema = self.base_named_schema(shape)
        schema["smithyShape"] = kind
        if kind == "list":
            field_type = {"type": "array", "items": self.smithy_type_to_avro(shape.member_type or "Document")}
            members = shape.members or [{"name": "member", "type": shape.member_type or "Document", "traits": {}}]
            doc = trait_doc(members[0].get("traits", {})) if members else None
            field_schema: dict[str, Any] = {"name": "member", "type": field_type}
            if doc:
                field_schema["doc"] = doc
            schema["fields"] = [field_schema]
        else:
            if shape.key_type and self.simple_type_name(shape.key_type) != "String":
                note = "Smithy map keys must be String for Avro map conversion; original key type was " + shape.key_type
                schema["doc"] = (schema.get("doc", "") + " " + note).strip()
            field_type = {"type": "map", "values": self.smithy_type_to_avro(shape.value_type or "Document")}
            schema["fields"] = [{"name": "entries", "type": field_type}]
        deps = self.dependencies_for_members(shape.members)
        if deps:
            schema["dependencies"] = deps
        return schema

    def member_to_field(self, member: dict[str, Any], force_optional: bool = False) -> dict[str, Any]:
        traits = member.get("traits", {})
        avro_type = self.smithy_type_to_avro(member["type"])
        required = bool(traits.get("required")) and not force_optional
        default_provided = "default" in traits
        field_schema: dict[str, Any] = {"name": member["name"]}
        doc = trait_doc(traits)
        if doc:
            field_schema["doc"] = doc
        if required:
            field_schema["type"] = avro_type
            if default_provided:
                field_schema["default"] = avro_default(traits["default"], avro_type)
        else:
            if default_provided and traits["default"] is not None:
                field_schema["type"] = [avro_type, "null"]
                field_schema["default"] = avro_default(traits["default"], avro_type)
            else:
                field_schema["type"] = ["null", avro_type]
                field_schema["default"] = None
        return field_schema

    def smithy_type_to_avro(self, smithy_type: str) -> AvroType:
        name = self.simple_type_name(smithy_type)
        if name in SMITHY_TO_AVRO_PRIMITIVES:
            value = SMITHY_TO_AVRO_PRIMITIVES[name]
            return dict(value) if isinstance(value, dict) else value
        shape = self.shapes.get(sanitize_name(name))
        if shape and shape.kind == "list":
            return {"type": "array", "items": self.smithy_type_to_avro(shape.member_type or "Document")}
        if shape and shape.kind == "map":
            return {"type": "map", "values": self.smithy_type_to_avro(shape.value_type or "Document")}
        return sanitize_name(name)

    @staticmethod
    def simple_type_name(smithy_type: str) -> str:
        return smithy_type.split("#")[-1].split(".")[-1]

    def dependencies_for_members(self, members: list[dict[str, Any]]) -> list[str]:
        deps: list[str] = []
        for member in members:
            self.collect_dependency(member.get("type"), deps)
        return deps

    def collect_dependency(self, smithy_type: str | None, deps: list[str]) -> None:
        if not smithy_type:
            return
        name = sanitize_name(self.simple_type_name(smithy_type))
        if name in SMITHY_TO_AVRO_PRIMITIVES:
            return
        shape = self.shapes.get(name)
        if shape and shape.kind in {"list", "map"}:
            for member in shape.members:
                self.collect_dependency(member.get("type"), deps)
            return
        qname = f"{self.namespace}.{name}" if self.namespace else name
        if qname not in deps:
            deps.append(qname)


def trait_doc(traits: dict[str, Any]) -> str | None:
    parts: list[str] = []
    if isinstance(traits.get("documentation"), str):
        parts.append(traits["documentation"])
    if traits.get("deprecated"):
        parts.append("Deprecated.")
    if traits.get("tags"):
        parts.append(f"Tags: {traits['tags']}")
    return " ".join(parts) or None


def avro_default(value: Any, avro_type: AvroType) -> Any:
    if isinstance(avro_type, dict) and avro_type.get("logicalType") == "timestamp-millis":
        return int(value) if isinstance(value, (int, float, str)) and str(value).lstrip("-").isdigit() else value
    return value


def convert_smithy_to_avro_schema(smithy_text: str, namespace: str | None = None) -> list[dict[str, Any]]:
    """Convert Smithy IDL text to an Avrotize Schema object."""
    return SmithyToAvroConverter(namespace).convert_text(smithy_text)


def convert_smithy_to_avro(smithy_file_path: str, avro_schema_path: str, namespace: str | None = None) -> None:
    """Convert a Smithy IDL file to an Avrotize Schema JSON file."""
    with open(smithy_file_path, "r", encoding="utf-8") as smithy_file:
        smithy_text = smithy_file.read()
    avro_schema = convert_smithy_to_avro_schema(smithy_text, namespace)
    output_dir = os.path.dirname(os.path.abspath(avro_schema_path))
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    with open(avro_schema_path, "w", encoding="utf-8") as avro_file:
        json.dump(avro_schema, avro_file, indent=2)
        avro_file.write("\n")

