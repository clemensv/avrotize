"""CUE subset to Avrotize Schema converter."""

# pylint: disable=too-many-lines,too-many-return-statements,too-many-branches,too-many-locals,too-many-statements

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any

from avrotize.common import avro_name, avro_namespace


_PRIMITIVES = {
    "string": "string",
    "int": "long",
    "float": "double",
    "number": "double",
    "bool": "boolean",
    "bytes": "bytes",
    "null": "null",
}


@dataclass
class Token:
    kind: str
    value: str
    pos: int


class CueSubsetParser:
    """Small, forgiving parser for the documented CUE schema subset."""

    def __init__(self, text: str) -> None:
        self.tokens = self._tokenize(self._strip_comments(text))
        self.index = 0
        self.warnings: list[str] = []

    @staticmethod
    def _strip_comments(text: str) -> str:
        text = re.sub(r"/\*.*?\*/", lambda m: "\n" * m.group(0).count("\n"), text, flags=re.S)
        return re.sub(r"//.*", "", text)

    @staticmethod
    def _tokenize(text: str) -> list[Token]:
        spec = [
            ("ELLIPSIS", r"\.\.\."),
            ("STRING", r'"(?:\\.|[^"\\])*"'),
            ("NUMBER", r"-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?"),
            ("IDENT", r"[A-Za-z_][A-Za-z0-9_]*"),
            ("NEWLINE", r"\r?\n"),
            ("SKIP", r"[ \t\r]+"),
            ("PUNCT", r"[#{}\[\]:?,;|*]"),
            ("UNSUPPORTED", r"."),
        ]
        regex = re.compile("|".join(f"(?P<{name}>{pattern})" for name, pattern in spec))
        tokens: list[Token] = []
        for match in regex.finditer(text):
            kind = match.lastgroup or "UNSUPPORTED"
            value = match.group()
            if kind == "SKIP":
                continue
            if kind == "PUNCT":
                kind = value
            tokens.append(Token(kind, value, match.start()))
        tokens.append(Token("EOF", "", len(text)))
        return tokens

    def current(self) -> Token:
        return self.tokens[self.index]

    def peek(self, offset: int = 1) -> Token:
        pos = min(self.index + offset, len(self.tokens) - 1)
        return self.tokens[pos]

    def advance(self) -> Token:
        token = self.current()
        self.index += 1
        return token

    def match(self, *kinds: str) -> Token | None:
        if self.current().kind in kinds:
            return self.advance()
        return None

    def expect(self, kind: str) -> Token | None:
        if self.current().kind == kind:
            return self.advance()
        self.warnings.append(f"expected {kind!r} near {self.current().value!r}")
        return None

    def skip_separators(self) -> None:
        while self.current().kind in {"NEWLINE", ",", ";"}:
            self.advance()

    def skip_to_separator(self) -> None:
        depth = 0
        while self.current().kind != "EOF":
            if depth == 0 and self.current().kind in {"NEWLINE", ",", ";", "}"}:
                return
            if self.current().kind in {"{", "["}:
                depth += 1
            elif self.current().kind in {"}", "]"}:
                if depth == 0:
                    return
                depth -= 1
            self.advance()

    def parse(self) -> dict[str, Any]:
        package = None
        definitions: dict[str, dict[str, Any]] = {}
        fields: list[dict[str, Any]] = []
        self.skip_separators()
        while self.current().kind != "EOF":
            if self.current().kind == "IDENT" and self.current().value == "package":
                self.advance()
                if self.current().kind == "IDENT":
                    package = self.advance().value
                self.skip_to_separator()
                self.skip_separators()
                continue
            if self.current().kind == "#" and self.peek().kind == "IDENT":
                self.advance()
                name = self.advance().value
                self.expect(":")
                if self.current().kind == "{":
                    definitions[name] = self.parse_struct()
                else:
                    self.warnings.append(f"definition #{name} is not a struct and was ignored")
                    self.skip_to_separator()
                self.skip_separators()
                continue
            field = self.parse_field()
            if field:
                fields.append(field)
            else:
                if self.current().kind not in {"EOF", "}"}:
                    self.warnings.append(f"ignored unsupported top-level construct near {self.current().value!r}")
                    self.skip_to_separator()
            self.skip_separators()
        return {"package": package, "definitions": definitions, "fields": fields, "warnings": self.warnings}

    def parse_struct(self) -> dict[str, Any]:
        self.expect("{")
        self.skip_separators()
        if self.current().kind == "[":
            self.advance()
            key = self.advance()
            if key.kind == "IDENT" and key.value == "string":
                self.expect("]")
                self.expect(":")
                values = self.parse_expr()
                self.skip_separators()
                self.expect("}")
                return {"kind": "map", "values": values}
            self.warnings.append("only [string]: T open structs are supported as maps")
            self.skip_to_separator()
        fields: list[dict[str, Any]] = []
        while self.current().kind not in {"}", "EOF"}:
            self.skip_separators()
            if self.current().kind in {"}", "EOF"}:
                break
            field = self.parse_field()
            if field:
                fields.append(field)
            else:
                self.warnings.append(f"ignored unsupported struct construct near {self.current().value!r}")
                self.skip_to_separator()
            self.skip_separators()
        self.expect("}")
        return {"kind": "struct", "fields": fields}

    def parse_field(self) -> dict[str, Any] | None:
        if self.current().kind == "IDENT":
            name = self.advance().value
        elif self.current().kind == "STRING":
            name = json.loads(self.advance().value)
        else:
            return None
        optional = bool(self.match("?"))
        if not self.match(":"):
            return None
        expr = self.parse_expr()
        if self.current().kind not in {"NEWLINE", ",", ";", "}", "EOF"}:
            self.warnings.append(f"field {name!r} has unsupported trailing constraint tokens")
            self.skip_to_separator()
        return {"name": name, "optional": optional, "type": expr}

    def parse_expr(self) -> dict[str, Any]:
        branches: list[dict[str, Any]] = []
        default: Any = None
        default_seen = False
        while True:
            is_default = bool(self.match("*"))
            branch = self.parse_primary()
            branches.append(branch)
            if is_default:
                default = self.literal_default(branch)
                default_seen = True
            if not self.match("|"):
                break
        if len(branches) == 1 and not default_seen:
            return branches[0]
        return {"kind": "disjunction", "branches": branches, "has_default": default_seen, "default": default}

    def parse_primary(self) -> dict[str, Any]:
        token = self.current()
        if token.kind == "IDENT":
            value = self.advance().value
            if value in _PRIMITIVES:
                return {"kind": "primitive", "name": value}
            if value in {"true", "false"}:
                return {"kind": "literal", "value": value == "true"}
            self.warnings.append(f"unknown identifier {value!r} mapped as a string")
            return {"kind": "unsupported", "text": value}
        if token.kind == "STRING":
            return {"kind": "literal", "value": json.loads(self.advance().value)}
        if token.kind == "NUMBER":
            raw = self.advance().value
            return {"kind": "literal", "value": float(raw) if any(c in raw for c in ".eE") else int(raw)}
        if token.kind == "#":
            self.advance()
            if self.current().kind == "IDENT":
                return {"kind": "ref", "name": self.advance().value}
            return {"kind": "unsupported", "text": "#"}
        if token.kind == "{":
            return self.parse_struct()
        if token.kind == "[":
            self.advance()
            if self.match("ELLIPSIS"):
                items = self.parse_expr()
                self.expect("]")
                return {"kind": "array", "items": items}
            self.warnings.append("only open list forms like [...T] are supported")
            self.skip_to_separator()
            return {"kind": "array", "items": {"kind": "unsupported", "text": "list"}}
        if token.kind == "IDENT" and token.value == "null":
            self.advance()
            return {"kind": "primitive", "name": "null"}
        self.warnings.append(f"unsupported expression token {token.value!r} mapped as string")
        self.advance()
        return {"kind": "unsupported", "text": token.value}

    @staticmethod
    def literal_default(branch: dict[str, Any]) -> Any:
        if branch.get("kind") == "literal":
            return branch.get("value")
        if branch.get("kind") == "primitive" and branch.get("name") == "null":
            return None
        return None


class CueToAvroConverter:
    """Converts the documented CUE schema subset to Avrotize Schema."""

    def __init__(self, namespace: str | None = None) -> None:
        self.namespace = avro_namespace(namespace) if namespace else None
        self.warnings: list[str] = []

    def convert_text(self, text: str, root_name: str = "Document") -> dict[str, Any] | list[Any]:
        parsed = CueSubsetParser(text).parse()
        self.warnings = parsed["warnings"]
        namespace = self.namespace or (avro_namespace(parsed["package"]) if parsed["package"] else None)
        records = []
        for name, struct in parsed["definitions"].items():
            records.append(self.struct_to_record(name, struct, namespace))
        if parsed["fields"]:
            records.append(self.struct_to_record(root_name, {"kind": "struct", "fields": parsed["fields"]}, namespace))
        if not records:
            records.append(self.struct_to_record(root_name, {"kind": "struct", "fields": []}, namespace))
        for record in records:
            self.add_warnings(record)
        return records[0] if len(records) == 1 else records

    def add_warnings(self, schema: dict[str, Any]) -> None:
        if self.warnings and isinstance(schema, dict):
            note = "CUE conversion notes: " + "; ".join(dict.fromkeys(self.warnings[:5]))
            schema["doc"] = (schema.get("doc", "") + ("\n" if schema.get("doc") else "") + note).strip()

    def struct_to_record(self, name: str, struct: dict[str, Any], namespace: str | None) -> dict[str, Any]:
        fields = []
        for field in struct.get("fields", []):
            fields.append(self.field_to_avro(field, avro_name(name), namespace))
        record: dict[str, Any] = {"type": "record", "name": avro_name(name), "fields": fields}
        if namespace:
            record["namespace"] = namespace
        return record

    def field_to_avro(self, field: dict[str, Any], parent_name: str, namespace: str | None) -> dict[str, Any]:
        field_name = avro_name(field["name"])
        avro_type = self.type_to_avro(field["type"], f"{parent_name}_{field_name}", namespace)
        avro_field: dict[str, Any] = {"name": field_name, "type": avro_type}
        if field_name != field["name"]:
            avro_field["altnames"] = {"cue": field["name"]}
        default = self.default_for(field["type"])
        if field.get("optional"):
            avro_field["type"] = self.nullable(avro_type)
            avro_field["default"] = None
        elif default is not _NO_DEFAULT:
            avro_field["default"] = self.default_for_avro(avro_field["type"], default)
        return avro_field

    def type_to_avro(self, cue_type: dict[str, Any], name_hint: str, namespace: str | None) -> Any:
        kind = cue_type.get("kind")
        if kind == "primitive":
            return _PRIMITIVES.get(cue_type["name"], "string")
        if kind == "literal":
            return self.literal_type(cue_type["value"])
        if kind == "ref":
            return avro_name(cue_type["name"])
        if kind == "array":
            return {"type": "array", "items": self.type_to_avro(cue_type["items"], f"{name_hint}_item", namespace)}
        if kind == "map":
            return {"type": "map", "values": self.type_to_avro(cue_type["values"], f"{name_hint}_value", namespace)}
        if kind == "struct":
            return self.struct_to_record(name_hint, cue_type, namespace)
        if kind == "disjunction":
            enum_schema = self.enum_from_string_literals(cue_type, name_hint)
            if enum_schema:
                return enum_schema
            union: list[Any] = []
            for branch in cue_type["branches"]:
                self.add_union_branch(union, self.type_to_avro(branch, name_hint, namespace))
            return union[0] if len(union) == 1 else union
        return "string"

    @staticmethod
    def literal_type(value: Any) -> str:
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "boolean"
        if isinstance(value, int) and not isinstance(value, bool):
            return "long"
        if isinstance(value, float):
            return "double"
        return "string"

    def enum_from_string_literals(self, cue_type: dict[str, Any], name_hint: str) -> dict[str, Any] | None:
        values = [branch.get("value") for branch in cue_type["branches"] if branch.get("kind") == "literal"]
        if len(values) != len(cue_type["branches"]) or not all(isinstance(value, str) for value in values):
            return None
        if len(values) < 2:
            return None
        used: set[str] = set()
        symbols: list[str] = []
        for value in values:
            symbol = avro_name(str(value)).upper()
            if symbol in used:
                symbol = f"{symbol}_{len(used)}"
            used.add(symbol)
            symbols.append(symbol)
        enum_schema: dict[str, Any] = {"type": "enum", "name": avro_name(f"{name_hint}_enum"), "symbols": symbols}
        default = cue_type.get("default")
        if isinstance(default, str):
            enum_schema["default"] = symbols[values.index(default)]
        enum_schema["x-cue-symbols"] = dict(zip(symbols, values))
        return enum_schema

    @staticmethod
    def add_union_branch(union: list[Any], branch: Any) -> None:
        if isinstance(branch, list):
            for item in branch:
                CueToAvroConverter.add_union_branch(union, item)
            return
        key = json.dumps(branch, sort_keys=True) if isinstance(branch, dict) else str(branch)
        existing = {json.dumps(item, sort_keys=True) if isinstance(item, dict) else str(item) for item in union}
        if key not in existing:
            union.append(branch)

    @staticmethod
    def nullable(avro_type: Any) -> list[Any]:
        if isinstance(avro_type, list):
            return ["null"] + [item for item in avro_type if item != "null"]
        if avro_type == "null":
            return ["null"]
        return ["null", avro_type]

    @staticmethod
    def default_for(cue_type: dict[str, Any]) -> Any:
        if cue_type.get("kind") == "disjunction" and cue_type.get("has_default"):
            return cue_type.get("default")
        return _NO_DEFAULT

    @staticmethod
    def default_for_avro(avro_type: Any, default: Any) -> Any:
        if isinstance(avro_type, dict) and avro_type.get("type") == "enum":
            mapping = avro_type.get("x-cue-symbols", {})
            for symbol, original in mapping.items():
                if original == default:
                    return symbol
        return default


class _NoDefault:
    pass


_NO_DEFAULT = _NoDefault()


def convert_cue_to_avro(cue_file_path: str, avro_schema_path: str, namespace: str | None = None) -> None:
    """Convert a CUE schema subset file to an Avrotize/Avro schema file."""
    with open(cue_file_path, "r", encoding="utf-8") as cue_file:
        cue_text = cue_file.read()
    root_name = avro_name(os.path.splitext(os.path.basename(cue_file_path))[0] or "Document")
    converter = CueToAvroConverter(namespace=namespace)
    avro_schema = converter.convert_text(cue_text, root_name=root_name)
    with open(avro_schema_path, "w", encoding="utf-8") as avro_file:
        json.dump(avro_schema, avro_file, indent=2)
        avro_file.write("\n")
