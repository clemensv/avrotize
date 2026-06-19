"""Convert FlatBuffers .fbs schemas to Avrotize (Avro) schemas."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from typing import Any

from avrotize.common import pascal


@dataclass
class FbsField:
    name: str
    type_name: str
    is_vector: bool = False
    default: Any = None
    has_default: bool = False
    attributes: set[str] = field(default_factory=set)


@dataclass
class FbsType:
    name: str
    kind: str
    fields: list[FbsField] = field(default_factory=list)


@dataclass
class FbsEnum:
    name: str
    base_type: str = "int"
    values: list[tuple[str, int | None]] = field(default_factory=list)


@dataclass
class FbsUnion:
    name: str
    members: list[str] = field(default_factory=list)


@dataclass
class FbsSchema:
    namespace: str = ""
    tables: list[FbsType] = field(default_factory=list)
    structs: list[FbsType] = field(default_factory=list)
    enums: list[FbsEnum] = field(default_factory=list)
    unions: list[FbsUnion] = field(default_factory=list)
    root_type: str | None = None


_TOKEN_RE = re.compile(r'"(?:\\.|[^"\\])*"|[A-Za-z_][A-Za-z0-9_\.]*|-?\d+(?:\.\d+)?|[:;{},=\[\]()]')


class FlatBuffersParser:
    def __init__(self, text: str):
        text = re.sub(r"//.*?$", "", text, flags=re.MULTILINE)
        text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)
        self.tokens = _TOKEN_RE.findall(text)
        self.pos = 0

    def peek(self) -> str | None:
        return self.tokens[self.pos] if self.pos < len(self.tokens) else None

    def pop(self, expected: str | None = None) -> str:
        tok = self.peek()
        if tok is None:
            raise ValueError("Unexpected end of FlatBuffers schema")
        if expected is not None and tok != expected:
            raise ValueError(f"Expected '{expected}', got '{tok}'")
        self.pos += 1
        return tok

    def accept(self, token: str) -> bool:
        if self.peek() == token:
            self.pos += 1
            return True
        return False

    def parse(self) -> FbsSchema:
        schema = FbsSchema()
        while self.peek() is not None:
            tok = self.peek()
            if tok == "namespace":
                self.pop(); schema.namespace = self.pop(); self.accept(";")
            elif tok == "table":
                schema.tables.append(self.parse_type("table"))
            elif tok == "struct":
                schema.structs.append(self.parse_type("struct"))
            elif tok == "enum":
                schema.enums.append(self.parse_enum())
            elif tok == "union":
                schema.unions.append(self.parse_union())
            elif tok == "root_type":
                self.pop(); schema.root_type = self.pop(); self.accept(";")
            elif tok in {"file_identifier", "file_extension", "attribute", "rpc_service", "include"}:
                self.skip_statement_or_block()
            else:
                self.pop()
        return schema

    def skip_statement_or_block(self) -> None:
        while self.peek() is not None:
            tok = self.pop()
            if tok == "{":
                depth = 1
                while self.peek() is not None and depth:
                    t = self.pop()
                    if t == "{": depth += 1
                    elif t == "}": depth -= 1
                self.accept(";")
                return
            if tok == ";":
                return

    def parse_type(self, kind: str) -> FbsType:
        self.pop(kind)
        typ = FbsType(self.pop(), kind)
        self.pop("{")
        while self.peek() and self.peek() != "}":
            if self.peek() in {";", ","}:
                self.pop(); continue
            typ.fields.append(self.parse_field())
        self.pop("}")
        return typ

    def parse_field(self) -> FbsField:
        name = self.pop()
        self.pop(":")
        is_vector = self.accept("[")
        type_name = self.pop()
        if is_vector:
            self.pop("]")
        has_default = False
        default: Any = None
        if self.accept("="):
            default_token = self.pop()
            default, has_default = self.parse_default(default_token), True
        attrs: set[str] = set()
        if self.accept("("):
            while self.peek() and self.peek() != ")":
                attr = self.pop()
                if attr not in {",", ":"}:
                    attrs.add(attr)
                    if self.peek() == ":":
                        self.pop(); self.pop()
                self.accept(",")
            self.pop(")")
        self.accept(";") or self.accept(",")
        return FbsField(name, type_name, is_vector, default, has_default, attrs)

    @staticmethod
    def parse_default(token: str) -> Any:
        if token.startswith('"'):
            return token[1:-1]
        if token in {"true", "false"}:
            return token == "true"
        if re.match(r"^-?\d+$", token):
            return int(token)
        if re.match(r"^-?\d+\.\d+$", token):
            return float(token)
        return token

    def parse_enum(self) -> FbsEnum:
        self.pop("enum")
        enum = FbsEnum(self.pop())
        if self.accept(":"):
            enum.base_type = self.pop()
        self.pop("{")
        next_value = 0
        while self.peek() and self.peek() != "}":
            if self.peek() in {",", ";"}:
                self.pop(); continue
            name = self.pop()
            value = None
            if self.accept("="):
                value = int(self.pop())
                next_value = value + 1
            else:
                value = next_value
                next_value += 1
            enum.values.append((name, value))
            self.accept(",") or self.accept(";")
        self.pop("}")
        return enum

    def parse_union(self) -> FbsUnion:
        self.pop("union")
        union = FbsUnion(self.pop())
        self.pop("{")
        while self.peek() and self.peek() != "}":
            if self.peek() in {",", ";"}:
                self.pop(); continue
            name = self.pop()
            if self.accept(":"):
                name = self.pop()
            union.members.append(name)
            self.accept(",") or self.accept(";")
        self.pop("}")
        return union


class FlatBuffersToAvroConverter:
    scalar_map = {
        "bool": "boolean", "byte": "int", "int8": "int", "ubyte": "int", "uint8": "int",
        "short": "int", "int16": "int", "ushort": "int", "uint16": "int",
        "int": "int", "int32": "int", "uint": "long", "uint32": "long",
        "long": "long", "int64": "long", "ulong": "long", "uint64": "long",
        "float": "float", "float32": "float", "double": "double", "float64": "double",
        "string": "string",
    }

    def __init__(self, namespace: str | None = None):
        self.namespace = namespace
        self.enums: dict[str, FbsEnum] = {}
        self.unions: dict[str, FbsUnion] = {}
        self.named: set[str] = set()

    def convert_file(self, fbs_file_path: str) -> list[dict[str, Any]]:
        with open(fbs_file_path, "r", encoding="utf-8") as f:
            schema = FlatBuffersParser(f.read()).parse()
        namespace = self.namespace if self.namespace is not None else schema.namespace
        if not namespace:
            namespace = pascal(os.path.basename(fbs_file_path).rsplit('.', 1)[0])
        self.enums = {e.name: e for e in schema.enums}
        self.unions = {u.name: u for u in schema.unions}
        self.named = {e.name for e in schema.enums} | {t.name for t in schema.tables} | {s.name for s in schema.structs}
        avro: list[dict[str, Any]] = []
        for enum in schema.enums:
            avro.append(self.convert_enum(enum, namespace))
        for typ in [*schema.structs, *schema.tables]:
            avro.append(self.convert_type(typ, namespace, schema.root_type == typ.name))
        return self.sort_by_dependencies(avro)


    @staticmethod
    def sort_by_dependencies(schemas: list[dict[str, Any]]) -> list[dict[str, Any]]:
        by_name: dict[str, dict[str, Any]] = {}
        for schema in schemas:
            qn = f"{schema.get('namespace')}.{schema['name']}" if schema.get('namespace') else schema['name']
            by_name[qn] = schema
            by_name[schema['name']] = schema
        ordered: list[dict[str, Any]] = []
        visiting: set[int] = set()
        visited: set[int] = set()

        primitives = {"null", "boolean", "int", "long", "float", "double", "bytes", "string"}

        def refs(avro_type: Any) -> list[str]:
            if isinstance(avro_type, str):
                return [] if avro_type in primitives else [avro_type]
            if isinstance(avro_type, list):
                out: list[str] = []
                for item in avro_type:
                    out.extend(refs(item))
                return out
            if isinstance(avro_type, dict):
                typ = avro_type.get("type")
                if typ == "array":
                    return refs(avro_type.get("items"))
                if typ == "map":
                    return refs(avro_type.get("values"))
                if typ in {"record", "enum"}:
                    return []
                return refs(typ)
            return []

        def visit(schema: dict[str, Any]) -> None:
            key = id(schema)
            if key in visited or key in visiting:
                return
            visiting.add(key)
            for field in schema.get("fields", []):
                for ref in refs(field.get("type")):
                    dep = by_name.get(ref)
                    if dep is not None and dep is not schema:
                        visit(dep)
            visiting.remove(key)
            visited.add(key)
            if schema not in ordered:
                ordered.append(schema)

        for schema in schemas:
            visit(schema)
        return ordered

    def convert_enum(self, enum: FbsEnum, namespace: str) -> dict[str, Any]:
        return {
            "type": "enum",
            "name": enum.name,
            "namespace": namespace,
            "symbols": [name for name, _ in enum.values],
            "ordinals": {name: value for name, value in enum.values},
            "doc": f"FlatBuffers enum base type {enum.base_type}; integer values are preserved in the non-standard ordinals annotation."
        }

    def convert_type(self, typ: FbsType, namespace: str, is_root: bool) -> dict[str, Any]:
        record: dict[str, Any] = {"type": "record", "name": typ.name, "namespace": namespace, "fields": []}
        notes = []
        if typ.kind == "struct":
            notes.append("FlatBuffers struct converted to an Avro record; fixed inline memory layout is not represented in Avro.")
        if is_root:
            notes.append("FlatBuffers root_type.")
            record["root_type"] = True
        if notes:
            record["doc"] = " ".join(notes)
        for f in typ.fields:
            field_type = self.convert_field_type(f, namespace)
            avro_field: dict[str, Any] = {"name": f.name, "type": field_type}
            if typ.kind == "table" and "required" not in f.attributes:
                avro_field["type"] = ["null", *field_type] if isinstance(field_type, list) else ["null", field_type]
                avro_field["default"] = None
            elif f.has_default:
                avro_field["default"] = f.default
            if f.has_default and typ.kind == "table" and "required" not in f.attributes:
                avro_field["fbsDefault"] = f.default
            if "required" in f.attributes:
                avro_field["fbsRequired"] = True
            record["fields"].append(avro_field)
        return record

    def convert_field_type(self, f: FbsField, namespace: str) -> Any:
        base = self.convert_type_name(f.type_name, namespace)
        if f.is_vector:
            if f.type_name in {"ubyte", "uint8"}:
                return "bytes"
            return {"type": "array", "items": base}
        return base

    def convert_type_name(self, type_name: str, namespace: str) -> Any:
        if type_name in self.scalar_map:
            return self.scalar_map[type_name]
        if type_name in self.unions:
            return [self.qualify(member, namespace) for member in self.unions[type_name].members]
        return self.qualify(type_name, namespace)

    @staticmethod
    def qualify(type_name: str, namespace: str) -> str:
        return type_name if "." in type_name or not namespace else f"{namespace}.{type_name}"


def convert_flatbuffers_to_avro(fbs_file_path: str, avro_schema_path: str, namespace: str | None = None):
    if not os.path.exists(fbs_file_path):
        raise FileNotFoundError(f"FlatBuffers schema file {fbs_file_path} does not exist.")
    converter = FlatBuffersToAvroConverter(namespace)
    avro_schema = converter.convert_file(fbs_file_path)
    with open(avro_schema_path, "w", encoding="utf-8") as avro_file:
        json.dump(avro_schema, avro_file, indent=2)


