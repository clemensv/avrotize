# pylint: disable=line-too-long

""" AvroToASN1 class for converting Avrotize (Avro) schema to an ASN.1 module. """

import json
import os
import re
from typing import Dict, List, Union

from avrotize.common import is_generic_avro_type

JsonNode = Union[Dict, List, str, int, float, bool, None]


class AvroToASN1:
    """ Converts an Avrotize (Avro) schema to an ASN.1 (X.680) module. """

    def __init__(self, module_name: str = 'AvrotizeSchema'):
        self.module_name = self._to_type_reference(module_name)
        # Ordered list of top-level "Name ::= ..." assignment strings.
        self.assignments: List[str] = []
        # ASN.1 type names already emitted (global uniqueness for typereferences).
        self.known_types: Dict[str, str] = {}
        # Avro full name -> ASN.1 type name.
        self.type_name_map: Dict[str, str] = {}
        self.used_type_names: set = set()

    # ------------------------------------------------------------------ names

    @staticmethod
    def _sanitize(name: str) -> str:
        """Turn an arbitrary name into an ASN.1-legal token body (letters, digits, hyphens)."""
        if not name:
            return 'x'
        # Keep only the last dotted segment for namespaced names.
        name = name.split('.')[-1]
        # Replace any run of illegal characters with a single hyphen.
        name = re.sub(r'[^A-Za-z0-9]+', '-', name)
        # Collapse consecutive hyphens and strip leading/trailing hyphens.
        name = re.sub(r'-+', '-', name).strip('-')
        if not name:
            return 'x'
        # ASN.1 tokens must start with a letter.
        if not name[0].isalpha():
            name = 'x-' + name
        return name

    def _to_type_reference(self, name: str) -> str:
        """ASN.1 typereference: must start with an upper-case letter."""
        token = self._sanitize(name)
        return token[0].upper() + token[1:]

    def _to_identifier(self, name: str) -> str:
        """ASN.1 identifier (member / enum value): must start with a lower-case letter."""
        token = self._sanitize(name)
        letters = [c for c in token if c.isalpha()]
        if letters and all(c.isupper() for c in letters):
            # ALL-CAPS tokens (e.g. Avro enum symbols) read best fully lower-cased.
            token = token.lower()
        return token[0].lower() + token[1:]

    def _unique_type_name(self, name: str) -> str:
        """Return a module-unique ASN.1 typereference for the given name."""
        base = self._to_type_reference(name)
        candidate = base
        counter = 1
        while candidate in self.used_type_names:
            counter += 1
            candidate = f"{base}{counter}"
        self.used_type_names.add(candidate)
        return candidate

    # ------------------------------------------------------------- primitives

    def _map_primitive(self, avro_type: Union[str, dict]) -> str:
        """Map an Avro primitive (or logical) type to an ASN.1 built-in type."""
        if isinstance(avro_type, dict):
            logical_type = avro_type.get('logicalType')
            base = avro_type.get('type')
            if logical_type == 'date':
                return 'DATE'
            if logical_type in ('timestamp-millis', 'timestamp-micros',
                                'local-timestamp-millis', 'local-timestamp-micros'):
                return 'DATE-TIME'
            if logical_type in ('time-millis', 'time-micros'):
                return 'TIME-OF-DAY'
            if logical_type == 'decimal':
                return 'REAL'
            if logical_type == 'uuid':
                return 'UTF8String'
            if logical_type == 'duration':
                return 'UTF8String'
            return self._map_primitive(base)

        mapping = {
            'null': 'NULL',
            'boolean': 'BOOLEAN',
            'int': 'INTEGER',
            'long': 'INTEGER',
            'float': 'REAL',
            'double': 'REAL',
            'bytes': 'OCTET STRING',
            'string': 'UTF8String',
        }
        return mapping.get(avro_type, '')

    @staticmethod
    def _is_primitive(avro_type: Union[str, dict]) -> bool:
        if isinstance(avro_type, dict):
            return 'logicalType' in avro_type or avro_type.get('type') in {
                'null', 'boolean', 'int', 'long', 'float', 'double', 'bytes', 'string'}
        return avro_type in {'null', 'boolean', 'int', 'long', 'float', 'double', 'bytes', 'string'}

    # ------------------------------------------------------------- expressions

    def type_expression(self, avro_type: JsonNode) -> str:
        """Return an ASN.1 type expression for an Avro type (defining named types as a side effect)."""
        if isinstance(avro_type, str):
            if self._is_primitive(avro_type):
                return self._map_primitive(avro_type)
            # Reference to a previously defined named type.
            return self.type_name_map.get(avro_type, self._to_type_reference(avro_type))

        if isinstance(avro_type, list):
            return self._union_expression(avro_type)

        if isinstance(avro_type, dict):
            if 'logicalType' in avro_type:
                return self._map_primitive(avro_type)
            kind = avro_type.get('type')
            if kind == 'record' or kind == 'error':
                return self._define_record(avro_type)
            if kind == 'enum':
                return self._define_enum(avro_type)
            if kind == 'fixed':
                return self._define_fixed(avro_type)
            if kind == 'array':
                items = avro_type.get('items', 'string')
                return f"SEQUENCE OF {self.type_expression(items)}"
            if kind == 'map':
                values = avro_type.get('values', 'string')
                return f"SEQUENCE OF SEQUENCE {{ key UTF8String, value {self.type_expression(values)} }}"
            if self._is_primitive(avro_type):
                return self._map_primitive(avro_type)
            if isinstance(kind, (dict, list)):
                return self.type_expression(kind)
            if isinstance(kind, str):
                return self.type_name_map.get(kind, self._to_type_reference(kind))
        return 'UTF8String'

    def _union_expression(self, union: List[JsonNode]) -> str:
        """Map an Avro union to an ASN.1 type expression (nullability is handled by the caller)."""
        if is_generic_avro_type(union):
            return 'ANY'
        non_null = [t for t in union if t != 'null']
        if len(non_null) == 0:
            return 'NULL'
        if len(non_null) == 1:
            return self.type_expression(non_null[0])
        members = []
        used: set = set()
        for i, branch in enumerate(non_null):
            alt = self._alternative_name(branch, i, used)
            members.append(f"{alt} {self.type_expression(branch)}")
        return "CHOICE { " + ", ".join(members) + " }"

    def _alternative_name(self, branch: JsonNode, index: int, used: set) -> str:
        """Derive a unique ASN.1 CHOICE alternative identifier from a union branch."""
        if isinstance(branch, str):
            base = self._to_identifier(branch)
        elif isinstance(branch, dict):
            if 'name' in branch:
                base = self._to_identifier(branch['name'])
            else:
                base = self._to_identifier(str(branch.get('type', f'choice{index}')))
        else:
            base = f'choice{index}'
        candidate = base
        counter = 1
        while candidate in used:
            counter += 1
            candidate = f"{base}{counter}"
        used.add(candidate)
        return candidate

    # --------------------------------------------------------------- defining

    def _define_record(self, record: dict) -> str:
        """Emit an ASN.1 SEQUENCE assignment for an Avro record, returning its type name."""
        full_name = self._full_name(record)
        if full_name in self.type_name_map:
            return self.type_name_map[full_name]

        type_name = self._unique_type_name(record.get('name', 'Record'))
        # Register before recursing so self-references resolve.
        self.type_name_map[full_name] = type_name

        fields = record.get('fields', [])
        used_members: set = set()
        members = [(self._member_definition(field, used_members), field.get('doc'))
                   for field in fields]

        if members:
            lines: List[str] = []
            for i, (core, doc) in enumerate(members):
                comma = "," if i < len(members) - 1 else ""
                if doc:
                    lines.append(f"    -- {self._comment(doc)}")
                lines.append(f"    {core}{comma}")
            body = "SEQUENCE {\n" + "\n".join(lines) + "\n}"
        else:
            body = "SEQUENCE {}"
        self._append_assignment(type_name, body, record.get('doc'))
        return type_name

    def _member_definition(self, field: dict, used_members: set) -> str:
        """Build a single ASN.1 SEQUENCE member (without comment) from an Avro field."""
        name = self._unique_identifier(field['name'], used_members)
        field_type = field['type']
        optional = isinstance(field_type, list) and 'null' in field_type
        expr = self.type_expression(field_type)
        member = f"{name} {expr}"
        if optional:
            member += " OPTIONAL"
        return member

    def _define_enum(self, enum: dict) -> str:
        """Emit an ASN.1 ENUMERATED assignment for an Avro enum, returning its type name."""
        full_name = self._full_name(enum)
        if full_name in self.type_name_map:
            return self.type_name_map[full_name]

        type_name = self._unique_type_name(enum.get('name', 'Enum'))
        self.type_name_map[full_name] = type_name

        values: List[str] = []
        used_symbols: set = set()
        for i, symbol in enumerate(enum.get('symbols', [])):
            sym = self._unique_identifier(symbol, used_symbols)
            values.append(f"{sym}({i})")
        body = "ENUMERATED { " + ", ".join(values) + " }" if values else "ENUMERATED { unspecified(0) }"
        self._append_assignment(type_name, body, enum.get('doc'))
        return type_name

    def _define_fixed(self, fixed: dict) -> str:
        """Emit an ASN.1 OCTET STRING (SIZE(n)) assignment for an Avro fixed type."""
        full_name = self._full_name(fixed)
        if full_name in self.type_name_map:
            return self.type_name_map[full_name]

        type_name = self._unique_type_name(fixed.get('name', 'Fixed'))
        self.type_name_map[full_name] = type_name
        size = fixed.get('size', 0)
        self._append_assignment(type_name, f"OCTET STRING (SIZE({size}))", fixed.get('doc'))
        return type_name

    # ------------------------------------------------------------- utilities

    def _unique_identifier(self, name: str, used: set) -> str:
        base = self._to_identifier(name)
        candidate = base
        counter = 1
        while candidate in used:
            counter += 1
            candidate = f"{base}-{counter}"
        used.add(candidate)
        return candidate

    @staticmethod
    def _full_name(schema: dict) -> str:
        name = schema.get('name', '')
        namespace = schema.get('namespace', '')
        return f"{namespace}.{name}" if namespace else name

    @staticmethod
    def _comment(text: str) -> str:
        """ASN.1 line comment body: no line breaks, no double-hyphen terminator."""
        return re.sub(r'\s+', ' ', str(text)).replace('--', '- -').strip()

    def _append_assignment(self, type_name: str, body: str, doc: str = None) -> None:
        assignment = ''
        if doc:
            assignment += f"-- {self._comment(doc)}\n"
        assignment += f"{type_name} ::= {body}"
        self.assignments.append(assignment)
        self.known_types[type_name] = body

    # ----------------------------------------------------------------- render

    def convert(self, avro_schema: JsonNode) -> str:
        """Convert an Avro schema (dict or list of named types) to an ASN.1 module string."""
        schemas = avro_schema if isinstance(avro_schema, list) else [avro_schema]
        for schema in schemas:
            if not isinstance(schema, dict):
                continue
            kind = schema.get('type')
            if kind in ('record', 'error'):
                self._define_record(schema)
            elif kind == 'enum':
                self._define_enum(schema)
            elif kind == 'fixed':
                self._define_fixed(schema)
        return self.render()

    def render(self) -> str:
        """Render the accumulated assignments as a complete ASN.1 module."""
        lines = [f"{self.module_name} DEFINITIONS AUTOMATIC TAGS ::= BEGIN", ""]
        for assignment in self.assignments:
            lines.append(assignment)
            lines.append("")
        lines.append("END")
        return "\n".join(lines) + "\n"


def convert_avro_to_asn1(avro_schema_path: str, asn1_file_path: str, module_name: str = '') -> None:
    """Convert an Avrotize schema file to an ASN.1 module file."""
    with open(avro_schema_path, 'r', encoding='utf-8') as avro_file:
        avro_schema = json.load(avro_file)

    if not module_name:
        module_name = os.path.splitext(os.path.basename(asn1_file_path))[0] or 'AvrotizeSchema'

    converter = AvroToASN1(module_name)
    asn1_text = converter.convert(avro_schema)

    out_dir = os.path.dirname(asn1_file_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)
    with open(asn1_file_path, 'w', encoding='utf-8') as asn1_file:
        asn1_file.write(asn1_text)
