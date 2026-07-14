"""Convert Cap'n Proto .capnp schemas to Avrotize/Avro schemas."""

from __future__ import annotations

import json
import os
import re
import tempfile
from dataclasses import dataclass, field
from typing import Any

from avrotize.common import avro_name, unique_name
from avrotize.dependency_resolver import sort_and_inline_dependencies


@dataclass
class CapnpField:
    name: str
    ordinal: int
    type_name: str
    group: "CapnpStruct | None" = None
    union: str | None = None


@dataclass
class CapnpEnum:
    name: str
    values: list[tuple[str, int]] = field(default_factory=list)


@dataclass
class CapnpStruct:
    name: str
    fields: list[CapnpField] = field(default_factory=list)
    structs: list["CapnpStruct"] = field(default_factory=list)
    enums: list[CapnpEnum] = field(default_factory=list)


@dataclass
class CapnpSchema:
    structs: list[CapnpStruct] = field(default_factory=list)
    enums: list[CapnpEnum] = field(default_factory=list)
    file_id: str | None = None


_TOKEN_RE = re.compile(
    r"@0x[0-9A-Fa-f]+|0x[0-9A-Fa-f]+|@[0-9]+|[A-Za-z_][A-Za-z0-9_\.]*|[0-9][A-Za-z0-9_\.]*|[{}():;=,\[\]]|\S"
)


def _strip_comments(text: str) -> str:
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    text = re.sub(r"//.*", "", text)
    return re.sub(r"(?m)^\s*#.*$", "", text)


class _Parser:
    def __init__(self, text: str) -> None:
        self.tokens = _TOKEN_RE.findall(_strip_comments(text))
        self.pos = 0
        self.file_id: str | None = None

    def peek(self) -> str | None:
        return self.tokens[self.pos] if self.pos < len(self.tokens) else None

    def pop(self, expected: str | None = None) -> str:
        if self.pos >= len(self.tokens):
            raise ValueError("Unexpected end of Cap'n Proto schema")
        token = self.tokens[self.pos]
        if expected is not None and token != expected:
            raise ValueError(f"Expected {expected!r}, found {token!r}")
        self.pos += 1
        return token

    def skip_statement(self) -> None:
        depth = 0
        while self.peek() is not None:
            token = self.pop()
            if token == "{":
                depth += 1
            elif token == "}":
                if depth == 0:
                    self.pos -= 1
                    return
                depth -= 1
            elif token == ";" and depth == 0:
                return

    def skip_block_or_statement(self) -> None:
        if self.peek() == "{":
            self.pop("{")
            depth = 1
            while depth and self.peek() is not None:
                token = self.pop()
                if token == "{":
                    depth += 1
                elif token == "}":
                    depth -= 1
        else:
            self.skip_statement()

    def parse(self) -> CapnpSchema:
        schema = CapnpSchema(file_id=None)
        if self.peek() and self.peek().startswith("@0x"):
            schema.file_id = self.pop()[1:]
            if self.peek() == ";":
                self.pop(";")
        while self.peek() is not None:
            token = self.peek()
            if token == "struct":
                schema.structs.append(self.parse_struct())
            elif token == "enum":
                schema.enums.append(self.parse_enum())
            elif token and token.startswith("@0x"):
                schema.file_id = self.pop()[1:]
                if self.peek() == ";":
                    self.pop(";")
            else:
                self.pop()
                self.skip_block_or_statement()
        schema.file_id = schema.file_id or self.file_id
        return schema

    def parse_struct(self) -> CapnpStruct:
        self.pop("struct")
        name = self.pop()
        struct = CapnpStruct(name=name)
        self.pop("{")
        while self.peek() is not None and self.peek() != "}":
            token = self.peek()
            if token == "struct":
                struct.structs.append(self.parse_struct())
            elif token == "enum":
                struct.enums.append(self.parse_enum())
            elif token == "union":
                self.parse_union_fields(struct, "union")
            elif token in {"interface", "const", "annotation", "using", "import"}:
                self.pop()
                self.skip_block_or_statement()
            else:
                field = self.parse_field()
                if field:
                    struct.fields.append(field)
        self.pop("}")
        return struct

    def parse_enum(self) -> CapnpEnum:
        self.pop("enum")
        name = self.pop()
        enum = CapnpEnum(name=name)
        self.pop("{")
        while self.peek() is not None and self.peek() != "}":
            name_token = self.pop()
            if self.peek() and self.peek().startswith("@"):
                ordinal = int(self.pop()[1:])
                enum.values.append((name_token, ordinal))
            self.skip_statement()
        self.pop("}")
        return enum

    def parse_union_fields(self, struct: CapnpStruct, union_name: str) -> None:
        self.pop("union")
        self.pop("{")
        while self.peek() is not None and self.peek() != "}":
            field = self.parse_field()
            if field:
                field.union = union_name
                struct.fields.append(field)
        self.pop("}")

    def parse_type(self) -> str:
        parts: list[str] = []
        depth = 0
        while self.peek() is not None:
            token = self.peek()
            if token in {";", "="} and depth == 0:
                break
            if token == "{" and depth == 0:
                break
            token = self.pop()
            if token == "(":
                depth += 1
            elif token == ")":
                depth -= 1
            parts.append(token)
        return "".join(parts)

    def parse_field(self) -> CapnpField | None:
        if self.peek() in {";", "}"}:
            self.pop()
            return None
        name = self.pop()
        if not (self.peek() and self.peek().startswith("@")):
            self.skip_block_or_statement()
            return None
        ordinal = int(self.pop()[1:])
        self.pop(":")
        type_name = self.parse_type()
        group: CapnpStruct | None = None
        union_name: str | None = None
        if type_name == "group" and self.peek() == "{":
            group = CapnpStruct(name=_pascal(name))
            self.pop("{")
            while self.peek() is not None and self.peek() != "}":
                if self.peek() == "union":
                    self.parse_union_fields(group, "union")
                else:
                    nested_field = self.parse_field()
                    if nested_field:
                        group.fields.append(nested_field)
            self.pop("}")
        elif type_name == "union" and self.peek() == "{":
            group = CapnpStruct(name=_pascal(name))
            union_name = name
            self.pop("{")
            while self.peek() is not None and self.peek() != "}":
                nested_field = self.parse_field()
                if nested_field:
                    nested_field.union = name
                    group.fields.append(nested_field)
            self.pop("}")
            type_name = "group"
        else:
            self.skip_statement()
        return CapnpField(name=name, ordinal=ordinal, type_name=type_name, group=group, union=union_name)


def _pascal(name: str) -> str:
    words = re.split(r"[^A-Za-z0-9]+", name)
    return "".join(word[:1].upper() + word[1:] for word in words if word) or "Value"


def _derive_namespace(path: str, namespace: str | None) -> str:
    if namespace:
        return namespace
    return re.sub(r"[^A-Za-z0-9_.]", "_", os.path.splitext(os.path.basename(path))[0])


class CapnpToAvroConverter:
    primitive_map: dict[str, Any] = {
        "Void": "null",
        "Bool": "boolean",
        "Int8": "int",
        "Int16": "int",
        "Int32": "int",
        "Int64": "long",
        "UInt8": "long",
        "UInt16": "long",
        "UInt32": "long",
        "UInt64": "long",
        "Float32": "float",
        "Float64": "double",
        "Text": "string",
        "Data": "bytes",
    }

    def __init__(self, namespace: str) -> None:
        self.namespace = namespace
        self.definitions: list[dict[str, Any]] = []
        self.known: dict[str, str] = {}
        self._used_type_names: dict[str, set[str]] = {}
        self._name_map: dict[tuple[str, str], str] = {}

    def convert(self, schema: CapnpSchema) -> list[dict[str, Any]]:
        for enum in schema.enums:
            self._register_named_type(enum.name, self.namespace)
        for struct in schema.structs:
            self._register_struct_names(struct, self.namespace)
        for struct in schema.structs:
            self._struct_to_avro(struct, self.namespace)
        for enum in schema.enums:
            self.definitions.append(self._enum_to_avro(enum, self.namespace))
        return sort_and_inline_dependencies(self.definitions)

    def _has_named_dependencies(self, struct: CapnpStruct) -> bool:
        for field in struct.fields:
            type_name = field.type_name
            while type_name.startswith("List(") and type_name.endswith(")"):
                type_name = type_name[5:-1]
            clean = type_name.lstrip(".").split(".")[-1]
            if field.group is None and clean not in self.primitive_map and clean:
                return True
        return False

    def _register_struct_names(self, struct: CapnpStruct, namespace: str) -> None:
        struct_name = self._register_named_type(struct.name, namespace)
        nested_ns = f"{namespace}.{struct_name}_types"
        for enum in struct.enums:
            self._register_named_type(enum.name, nested_ns)
        for nested in struct.structs:
            self._register_struct_names(nested, nested_ns)

    def _register_named_type(self, name: str, namespace: str) -> str:
        key = (namespace, name)
        if key not in self._name_map:
            used = self._used_type_names.setdefault(namespace, set())
            self._name_map[key] = unique_name(avro_name(name), used)
        sanitized = self._name_map[key]
        self.known[name] = f"{namespace}.{sanitized}"
        self.known[f"{namespace}.{name}"] = f"{namespace}.{sanitized}"
        return sanitized

    def _avro_type_name(self, name: str, namespace: str) -> str:
        return self._name_map.get((namespace, name)) or self._register_named_type(name, namespace)

    def _enum_to_avro(self, enum: CapnpEnum, namespace: str) -> dict[str, Any]:
        used_symbols: set[str] = set()
        symbols = [unique_name(avro_name(name), used_symbols) for name, _ in enum.values]
        ordinal_symbols = symbols or ["unknown"]
        return {
            "type": "enum",
            "name": self._avro_type_name(enum.name, namespace),
            "namespace": namespace,
            "symbols": ordinal_symbols,
            "capnpOrdinals": {symbol: ordinal for symbol, (_, ordinal) in zip(symbols, enum.values)},
        }

    def _struct_to_avro(self, struct: CapnpStruct, namespace: str) -> dict[str, Any]:
        record_name = self._avro_type_name(struct.name, namespace)
        nested_ns = f"{namespace}.{record_name}_types"
        for enum in struct.enums:
            self.definitions.append(self._enum_to_avro(enum, nested_ns))
        for nested in struct.structs:
            self._struct_to_avro(nested, nested_ns)
        used_fields: set[str] = set()
        record = {
            "type": "record",
            "name": record_name,
            "namespace": namespace,
            "fields": [self._field_to_avro(field, nested_ns, used_fields) for field in sorted(struct.fields, key=lambda f: f.ordinal)],
        }
        self.definitions.append(record)
        return record

    def _field_to_avro(self, field: CapnpField, nested_ns: str, used_fields: set[str]) -> dict[str, Any]:
        avro_type = self._type_to_avro(field.type_name, field.group, nested_ns)
        result: dict[str, Any] = {
            "name": unique_name(avro_name(field.name), used_fields),
            "type": self._nullable(avro_type),
            "default": None,
            "capnpOrdinal": field.ordinal,
        }
        if field.union:
            result["capnpUnion"] = field.union
        return result

    def _nullable(self, avro_type: Any) -> Any:
        if avro_type == "null":
            return "null"
        if isinstance(avro_type, list):
            return avro_type if "null" in avro_type else ["null", *avro_type]
        return ["null", avro_type]

    def _type_to_avro(self, type_name: str, group: CapnpStruct | None, nested_ns: str) -> Any:
        if group is not None:
            used = self._used_type_names.setdefault(nested_ns, set())
            group_name = unique_name(avro_name(group.name), used)
            used_fields: set[str] = set()
            return {
                "type": "record",
                "name": group_name,
                "namespace": nested_ns,
                "fields": [
                    self._field_to_avro(f, f"{nested_ns}.{group_name}_types", used_fields)
                    for f in sorted(group.fields, key=lambda f: f.ordinal)
                ],
            }
        if type_name.startswith("List(") and type_name.endswith(")"):
            item_type = type_name[5:-1]
            return {"type": "array", "items": self._type_to_avro(item_type, None, nested_ns)}
        clean = type_name.lstrip(".")
        if clean in self.primitive_map:
            return self.primitive_map[clean]
        return self.known.get(clean.split(".")[-1], clean)


def parse_capnproto_schema(text: str) -> CapnpSchema:
    return _Parser(text).parse()


def convert_capnproto_to_avro(capnp_file_path: str, avro_schema_path: str, namespace: str | None = None) -> None:
    """Convert a Cap'n Proto .capnp schema file to an Avrotize/Avro schema file."""
    with open(capnp_file_path, "r", encoding="utf-8") as capnp_file:
        schema = parse_capnproto_schema(capnp_file.read())
    converter = CapnpToAvroConverter(_derive_namespace(capnp_file_path, namespace))
    avro_schema = converter.convert(schema)
    os.makedirs(os.path.dirname(os.path.abspath(avro_schema_path)) or ".", exist_ok=True)
    with open(avro_schema_path, "w", encoding="utf-8") as avro_file:
        json.dump(avro_schema, avro_file, indent=2)


def convert_capnproto_to_json_structure(
    capnp_file_path: str,
    json_structure_file: str,
    namespace: str | None = None,
    naming_mode: str = "default",
    avro_encoding: bool = False,
) -> None:
    """Convert Cap'n Proto to JSON Structure by bridging through Avrotize/Avro."""
    from avrotize.avrotojstruct import convert_avro_to_json_structure

    temp_parent = os.path.dirname(os.path.abspath(json_structure_file)) or os.getcwd()
    os.makedirs(temp_parent, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="avrotize-capnp-", dir=temp_parent) as temp_dir:
        avro_file = os.path.join(temp_dir, "schema.avsc")
        convert_capnproto_to_avro(capnp_file_path, avro_file, namespace=namespace)
        convert_avro_to_json_structure(avro_file, json_structure_file, naming_mode=naming_mode, avro_encoding=avro_encoding)
