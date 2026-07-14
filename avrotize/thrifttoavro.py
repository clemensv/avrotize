
"""Apache Thrift IDL to Avrotize Schema converter."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from typing import Any

from avrotize.common import pascal
from avrotize.dependency_resolver import sort_messages_by_dependencies

AvroSchema = dict[str, Any] | list[Any] | str


@dataclass
class ThriftType:
    name: str | None = None
    args: list["ThriftType"] = field(default_factory=list)


@dataclass
class ThriftField:
    field_id: int
    requiredness: str | None
    type: ThriftType
    name: str
    default: Any = None
    has_default: bool = False


@dataclass
class ThriftDefinition:
    kind: str
    name: str
    fields: list[ThriftField] = field(default_factory=list)
    values: list[tuple[str, int | None]] = field(default_factory=list)
    target: ThriftType | None = None


@dataclass
class ThriftDocument:
    namespaces: dict[str, str] = field(default_factory=dict)
    includes: list[str] = field(default_factory=list)
    definitions: list[ThriftDefinition] = field(default_factory=list)


@dataclass
class Token:
    kind: str
    value: str


_PRIMITIVES = {
    "bool": "boolean",
    "byte": "int",
    "i8": "int",
    "i16": "int",
    "i32": "int",
    "i64": "long",
    "double": "double",
    "string": "string",
    "binary": "bytes",
}


def _strip_comments(text: str) -> str:
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    text = re.sub(r"//.*?$", "", text, flags=re.M)
    text = re.sub(r"#.*?$", "", text, flags=re.M)
    return text


def _decode_string_literal(raw: str) -> str:
    """Decode Thrift string-literal escapes without mangling non-ASCII UTF-8.

    ``str.decode("unicode_escape")`` operates byte-wise and therefore corrupts
    multi-byte UTF-8 sequences (e.g. ``café`` -> ``cafÃ©``). Re-encoding the
    latin-1 view back to bytes and decoding as UTF-8 restores the original
    characters; escapes such as ``\\n`` or ``\\uXXXX`` are still honoured.
    """
    decoded = raw.encode("utf-8").decode("unicode_escape")
    try:
        return decoded.encode("latin-1").decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError):
        return decoded


def _tokenize(text: str) -> list[Token]:
    tokens: list[Token] = []
    pattern = re.compile(
        r"""("(?:\\.|[^"\\])*")|('(?:\\.|[^'\\])*')|([A-Za-z_][A-Za-z0-9_.]*)|(-?\d+(?:\.\d+)?)|([{}<>:;,=\[\]])|(\S)"""
    )
    for match in pattern.finditer(_strip_comments(text)):
        dq_string, sq_string, ident, number, punct, other = match.groups()
        string = dq_string if dq_string is not None else sq_string
        if string is not None:
            tokens.append(Token("string", _decode_string_literal(string[1:-1])))
        elif ident is not None:
            tokens.append(Token("ident", ident))
        elif number is not None:
            tokens.append(Token("number", number))
        elif punct is not None:
            tokens.append(Token(punct, punct))
        elif other and not other.isspace():
            tokens.append(Token(other, other))
    tokens.append(Token("eof", ""))
    return tokens


class ThriftParser:
    def __init__(self, text: str) -> None:
        self.tokens = _tokenize(text)
        self.pos = 0

    def peek(self, value: str | None = None) -> Token:
        token = self.tokens[self.pos]
        if value is None or token.value == value or token.kind == value:
            return token
        return Token("", "")

    def pop(self, expected: str | None = None) -> Token:
        token = self.tokens[self.pos]
        if expected is not None and token.value != expected and token.kind != expected:
            raise ValueError(f"Expected {expected}, found {token.value or token.kind}")
        self.pos += 1
        return token

    def consume(self, value: str) -> bool:
        if self.peek(value).kind:
            self.pop()
            return True
        return False

    def parse(self) -> ThriftDocument:
        doc = ThriftDocument()
        while not self.peek("eof").kind:
            if self.consume("namespace"):
                scope = self.pop().value
                doc.namespaces[scope] = self.pop().value
                self._terminator()
            elif self.consume("include"):
                doc.includes.append(self.pop("string").value)
                self._terminator()
            elif self.consume("typedef"):
                target = self.parse_type()
                name = self.pop("ident").value
                doc.definitions.append(ThriftDefinition("typedef", name, target=target))
                self._terminator()
            elif self.consume("enum"):
                doc.definitions.append(self.parse_enum())
            elif self.consume("struct"):
                doc.definitions.append(self.parse_record("struct"))
            elif self.consume("union"):
                doc.definitions.append(self.parse_record("union"))
            elif self.consume("exception"):
                doc.definitions.append(self.parse_record("exception"))
            elif self.consume("const"):
                self._skip_const()
            elif self.consume("service"):
                self._skip_block()
            else:
                self.pop()
        return doc

    def parse_enum(self) -> ThriftDefinition:
        name = self.pop("ident").value
        values: list[tuple[str, int | None]] = []
        self.pop("{")
        ordinal = 0
        while not self.consume("}"):
            symbol = self.pop("ident").value
            explicit: int | None = None
            if self.consume("="):
                explicit = int(float(self.pop("number").value))
                ordinal = explicit
            values.append((symbol, explicit if explicit is not None else ordinal))
            ordinal += 1
            self._terminator()
        self._terminator()
        return ThriftDefinition("enum", name, values=values)

    def parse_record(self, kind: str) -> ThriftDefinition:
        name = self.pop("ident").value
        fields: list[ThriftField] = []
        self.pop("{")
        next_id = 1
        while not self.consume("}"):
            if self.peek("eof").kind:
                raise ValueError("Unexpected end of file in record")
            field_id = next_id
            if self.peek("number").kind and self.tokens[self.pos + 1].value == ":":
                field_id = int(float(self.pop("number").value))
                self.pop(":")
            next_id = field_id + 1
            requiredness = None
            if self.peek("ident").value in {"required", "optional"}:
                requiredness = self.pop().value
            field_type = self.parse_type()
            name_token = self.pop("ident")
            has_default = False
            default: Any = None
            if self.consume("="):
                has_default = True
                default = self.parse_default()
            fields.append(ThriftField(field_id, requiredness, field_type, name_token.value, default, has_default))
            self._terminator()
        self._terminator()
        return ThriftDefinition(kind, name, fields=fields)

    def parse_type(self) -> ThriftType:
        name = self.pop("ident").value
        args: list[ThriftType] = []
        if self.consume("<"):
            args.append(self.parse_type())
            if self.consume(","):
                args.append(self.parse_type())
            self.pop(">")
        return ThriftType(name, args)

    def parse_default(self) -> Any:
        token = self.pop()
        if token.kind == "string":
            return token.value
        if token.kind == "number":
            return float(token.value) if "." in token.value else int(token.value)
        if token.value in {"true", "false"}:
            return token.value == "true"
        if token.value in {"[", "{"}:
            open_value = token.value
            close = "]" if open_value == "[" else "}"
            depth = 1
            while depth and not self.peek("eof").kind:
                t = self.pop()
                if t.value == open_value:
                    depth += 1
                elif t.value == close:
                    depth -= 1
            return [] if close == "]" else {}
        return token.value

    def _terminator(self) -> None:
        while self.peek(";").kind or self.peek(",").kind:
            self.pop()

    def _skip_const(self) -> None:
        self.parse_type()
        if self.peek("ident").kind:
            self.pop()
        if self.consume("="):
            self.parse_default()
        self._terminator()

    def _skip_statement(self) -> None:
        depth = 0
        while not self.peek("eof").kind:
            t = self.pop()
            if t.value == "{":
                depth += 1
            elif t.value == "}":
                if depth == 0:
                    return
                depth -= 1
            elif depth == 0 and t.value in {";", ","}:
                return

    def _skip_block(self) -> None:
        while not self.peek("eof").kind and not self.consume("{"):
            self.pop()
        depth = 1
        while depth and not self.peek("eof").kind:
            t = self.pop()
            if t.value == "{":
                depth += 1
            elif t.value == "}":
                depth -= 1
        self._terminator()


def sanitize_name(name: str) -> str:
    clean = re.sub(r"\W", "_", name)
    if not clean or clean[0].isdigit():
        clean = f"_{clean}"
    return clean


def sanitize_namespace(namespace: str | None) -> str:
    if not namespace:
        return ""
    return ".".join(sanitize_name(part) for part in namespace.split(".") if part)


class ThriftToAvroConverter:
    def __init__(self) -> None:
        self.namespace = ""
        self.typedefs: dict[str, ThriftType] = {}
        self.named_types: set[str] = set()

    def convert(self, thrift_file_path: str, namespace: str | None = None) -> list[dict[str, Any]]:
        with open(thrift_file_path, "r", encoding="utf-8") as thrift_file:
            doc = ThriftParser(thrift_file.read()).parse()
        self.namespace = sanitize_namespace(namespace or doc.namespaces.get("*") or next(iter(doc.namespaces.values()), None) or pascal(os.path.basename(thrift_file_path).replace(".thrift", "")))
        self.typedefs = {d.name: d.target for d in doc.definitions if d.kind == "typedef" and d.target}
        self.named_types = {sanitize_name(d.name) for d in doc.definitions if d.kind in {"struct", "union", "exception", "enum"}}
        avro_schema: list[dict[str, Any]] = []
        for definition in doc.definitions:
            if definition.kind == "enum":
                avro_schema.append(self.convert_enum(definition))
            elif definition.kind in {"struct", "union", "exception"}:
                avro_schema.append(self.convert_record(definition))
        return sort_messages_by_dependencies(avro_schema)

    def convert_enum(self, definition: ThriftDefinition) -> dict[str, Any]:
        symbols: list[str] = []
        ordinals: dict[str, int] = {}
        for symbol, ordinal in definition.values:
            clean = sanitize_name(symbol)
            if clean in symbols:
                clean = f"{clean}_{len(symbols)}"
            symbols.append(clean)
            if ordinal is not None:
                ordinals[clean] = ordinal
        schema: dict[str, Any] = {
            "type": "enum",
            "name": sanitize_name(definition.name),
            "namespace": self.namespace,
            "symbols": symbols,
        }
        if ordinals:
            schema["ordinals"] = ordinals
        return schema

    def convert_record(self, definition: ThriftDefinition) -> dict[str, Any]:
        record: dict[str, Any] = {
            "type": "record",
            "name": sanitize_name(definition.name),
            "namespace": self.namespace,
            "fields": [],
        }
        if definition.kind == "union":
            record["doc"] = "Converted from a Thrift union; fields are nullable and at most one should be set."
        elif definition.kind == "exception":
            record["doc"] = "Converted from a Thrift exception."
        dependencies: list[str] = []
        for thrift_field in definition.fields:
            field_type = self.avro_type(thrift_field.type, sanitize_name(definition.name), sanitize_name(thrift_field.name), dependencies)
            is_optional = thrift_field.requiredness == "optional" or definition.kind == "union"
            if is_optional:
                field_type = ["null", field_type]
            avro_field: dict[str, Any] = {"name": sanitize_name(thrift_field.name), "type": field_type, "thrift_id": thrift_field.field_id}
            if is_optional:
                avro_field["default"] = None
            elif thrift_field.has_default:
                avro_field["default"] = thrift_field.default
            record["fields"].append(avro_field)
        if dependencies:
            record["dependencies"] = sorted(set(dependencies))
        return record

    def resolve_typedef(self, thrift_type: ThriftType) -> ThriftType:
        seen: set[str] = set()
        current = thrift_type
        while current.name in self.typedefs and current.name not in seen:
            seen.add(current.name or "")
            current = self.typedefs[current.name or ""]
        return current

    def avro_type(self, thrift_type: ThriftType, record_name: str, field_name: str, dependencies: list[str]) -> AvroSchema:
        thrift_type = self.resolve_typedef(thrift_type)
        name = thrift_type.name or "string"
        if name in _PRIMITIVES:
            return _PRIMITIVES[name]
        if name in {"list", "set"}:
            item_type = self.avro_type(thrift_type.args[0], record_name, field_name, dependencies) if thrift_type.args else "string"
            return {"type": "array", "items": item_type}
        if name == "map":
            key_type = self.resolve_typedef(thrift_type.args[0]) if thrift_type.args else ThriftType("string")
            value_type = self.avro_type(thrift_type.args[1], record_name, field_name, dependencies) if len(thrift_type.args) > 1 else "string"
            if key_type.name == "string":
                return {"type": "map", "values": value_type}
            entry_name = sanitize_name(f"{record_name}_{field_name}_Entry")
            return {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": entry_name,
                    "namespace": self.namespace,
                    "fields": [
                        {"name": "key", "type": self.avro_type(key_type, record_name, f"{field_name}_key", dependencies)},
                        {"name": "value", "type": value_type},
                    ],
                    "doc": "Map with a non-string Thrift key represented as key/value entries.",
                },
            }
        clean = sanitize_name(name.split(".")[-1])
        if clean in self.named_types:
            qname = f"{self.namespace}.{clean}" if self.namespace else clean
            dependencies.append(qname)
            return qname
        return "string"


def convert_thrift_to_avro(thrift_file_path: str, avro_schema_path: str, namespace: str | None = None):
    """Convert Apache Thrift IDL to Avrotize Schema."""
    if not os.path.exists(thrift_file_path):
        raise FileNotFoundError(f"Thrift file {thrift_file_path} does not exist.")
    converter = ThriftToAvroConverter()
    avro_schema = converter.convert(thrift_file_path, namespace)
    with open(avro_schema_path, "w", encoding="utf-8") as avro_file:
        json.dump(avro_schema, avro_file, indent=2)
