# pylint: disable=line-too-long

""" StructureToASN1 class for converting JSON Structure schema to an ASN.1 module. """

import json
import os
import re
from typing import Any, Dict, List, Optional, Union

JsonNode = Union[Dict, List, str, int, float, bool, None]


class StructureToASN1:
    """ Converts a JSON Structure schema directly to an ASN.1 (X.680) module. """

    def __init__(self, module_name: str = 'StructureSchema'):
        self.module_name = self._to_type_reference(module_name)
        self.assignments: List[str] = []
        self.used_type_names: set = set()
        self.definitions: Dict[str, Any] = {}
        # JSON Structure definition name -> emitted ASN.1 type name.
        self.def_emitted: Dict[str, str] = {}
        # Identity map for inline (anonymous) hoisted objects.
        self.inline_emitted: Dict[int, str] = {}

    # ------------------------------------------------------------------ names

    @staticmethod
    def _sanitize(name: str) -> str:
        """Turn an arbitrary name into an ASN.1-legal token body (letters, digits, hyphens)."""
        if not name:
            return 'x'
        name = name.split('/')[-1].split('.')[-1]
        name = re.sub(r'[^A-Za-z0-9]+', '-', name)
        name = re.sub(r'-+', '-', name).strip('-')
        if not name:
            return 'x'
        if not name[0].isalpha():
            name = 'x-' + name
        return name

    def _to_type_reference(self, name: str) -> str:
        token = self._sanitize(name)
        return token[0].upper() + token[1:]

    def _to_identifier(self, name: str) -> str:
        token = self._sanitize(name)
        letters = [c for c in token if c.isalpha()]
        if letters and all(c.isupper() for c in letters):
            # ALL-CAPS tokens (e.g. enum-style symbols) read best fully lower-cased.
            token = token.lower()
        return token[0].lower() + token[1:]

    def _unique_type_name(self, name: str) -> str:
        base = self._to_type_reference(name)
        candidate = base
        counter = 1
        while candidate in self.used_type_names:
            counter += 1
            candidate = f"{base}{counter}"
        self.used_type_names.add(candidate)
        return candidate

    def _unique_identifier(self, name: str, used: set) -> str:
        base = self._to_identifier(name)
        candidate = base
        counter = 1
        while candidate in used:
            counter += 1
            candidate = f"{base}-{counter}"
        used.add(candidate)
        return candidate

    # ------------------------------------------------------------- primitives

    _PRIMITIVES = {
        'null': 'NULL',
        'boolean': 'BOOLEAN',
        'string': 'UTF8String',
        'integer': 'INTEGER',
        'number': 'REAL',
        'int8': 'INTEGER (-128..127)',
        'uint8': 'INTEGER (0..255)',
        'int16': 'INTEGER (-32768..32767)',
        'uint16': 'INTEGER (0..65535)',
        'int32': 'INTEGER (-2147483648..2147483647)',
        'uint32': 'INTEGER (0..4294967295)',
        'int64': 'INTEGER (-9223372036854775808..9223372036854775807)',
        'uint64': 'INTEGER (0..18446744073709551615)',
        'int128': 'INTEGER (-170141183460469231731687303715884105728..170141183460469231731687303715884105727)',
        'uint128': 'INTEGER (0..340282366920938463463374607431768211455)',
        'float8': 'REAL',
        'float': 'REAL',
        'float32': 'REAL',
        'float64': 'REAL',
        'double': 'REAL',
        'binary32': 'REAL',
        'binary64': 'REAL',
        'decimal': 'REAL',
        'binary': 'OCTET STRING',
        'bytes': 'OCTET STRING',
        'date': 'DATE',
        'time': 'TIME-OF-DAY',
        'datetime': 'DATE-TIME',
        'timestamp': 'DATE-TIME',
        'duration': 'UTF8String',
        'uuid': 'UTF8String',
        'uri': 'UTF8String',
        'jsonpointer': 'UTF8String',
        'any': 'ANY',
    }

    def _is_primitive(self, structure_type: Any) -> bool:
        return isinstance(structure_type, str) and structure_type in self._PRIMITIVES

    def _map_primitive(self, structure_type: str) -> str:
        return self._PRIMITIVES.get(structure_type, 'UTF8String')

    def _constrained_primitive(self, kind: str, type_def: Dict) -> str:
        """Map a primitive typedef, honouring `enum`/`const` as an ASN.1 constrained type.

        Integer-valued closed sets become ``INTEGER (v1 | v2 | ...)`` which preserves the
        exact wire values. String-valued closed sets become an ``ENUMERATED`` (the idiomatic
        ASN.1 construct for a named symbol set); the enum symbols carry the string labels and
        their ordinals mirror the JSON Structure array index.
        """
        base = self._map_primitive(kind)
        builtin = base.split(' (')[0]
        values = None
        if 'const' in type_def:
            values = [type_def['const']]
        elif isinstance(type_def.get('enum'), list) and type_def['enum']:
            values = type_def['enum']
        if not values:
            return base
        if builtin == 'INTEGER' and all(isinstance(v, int) and not isinstance(v, bool) for v in values):
            return "INTEGER (" + " | ".join(str(v) for v in values) + ")"
        if all(isinstance(v, str) for v in values):
            used: set = set()
            symbols = []
            for index, value in enumerate(values):
                symbol = self._unique_identifier(value, used)
                symbols.append(f"{symbol}({index})")
            return "ENUMERATED { " + ", ".join(symbols) + " }"
        return base

    # ------------------------------------------------------------- references

    @staticmethod
    def _ref_name(ref: str) -> str:
        return ref.split('/')[-1]

    def _ref_path(self, ref: str) -> str:
        """Return the definitions-relative pointer path used as a stable dedup key."""
        if ref.startswith('#/definitions/'):
            return ref[len('#/definitions/'):]
        if ref.startswith('#/'):
            return ref[len('#/'):]
        return self._ref_name(ref)

    def _resolve_ref(self, ref: str) -> Optional[Dict]:
        """Resolve a local JSON Pointer, walking nested definition namespaces."""
        return self._lookup_path(self._ref_path(ref))

    def _lookup_path(self, path: str) -> Optional[Dict]:
        """Walk a `/`-separated definitions-relative path and return the node, or None."""
        node: Any = self.definitions
        for segment in path.split('/'):
            if isinstance(node, dict) and segment in node:
                node = node[segment]
            else:
                return None
        return node if isinstance(node, dict) else None

    def ensure_definition(self, def_name: str, type_def: Optional[Dict] = None) -> str:
        """Emit (once) a top-level assignment for a named definition and return its ASN.1 name."""
        if def_name in self.def_emitted:
            return self.def_emitted[def_name]
        if type_def is None:
            type_def = self._lookup_path(def_name)
        asn1_name = self._unique_type_name(def_name)
        self.def_emitted[def_name] = asn1_name
        if type_def is None:
            # Dangling reference: emit a permissive placeholder so the module stays valid.
            self._append_assignment(asn1_name, 'ANY', None)
            return asn1_name
        body = self._definition_body(type_def, asn1_name)
        self._append_assignment(asn1_name, body, self._doc_of(type_def))
        return asn1_name

    def _definition_body(self, type_def: Dict, asn1_name: str) -> str:
        """Return the ASN.1 right-hand-side for a named definition."""
        kind = type_def.get('type') if isinstance(type_def, dict) else type_def
        if kind == 'object':
            return self._object_sequence_body(type_def)
        return self.type_expression(type_def, hint_name=asn1_name, as_definition=True)

    # ------------------------------------------------------------- expressions

    def type_expression(self, type_def: JsonNode, hint_name: Optional[str] = None,
                        as_definition: bool = False) -> str:
        """Return an ASN.1 type expression for a JSON Structure type node."""
        if isinstance(type_def, str):
            if self._is_primitive(type_def):
                return self._map_primitive(type_def)
            # Reference to a named definition.
            return self.ensure_definition(type_def)

        if isinstance(type_def, list):
            return self._union_expression(type_def, hint_name)

        if isinstance(type_def, dict):
            if '$ref' in type_def:
                ref = type_def['$ref']
                return self.ensure_definition(self._ref_path(ref), self._resolve_ref(ref))

            kind = type_def.get('type')
            # Canonical type reference / nested typedef: `{"type": {"$ref": ...}}` or
            # `{"type": {"type": "object", ...}}`.
            if isinstance(kind, dict):
                return self.type_expression(kind, hint_name)
            if isinstance(kind, list):
                return self._union_expression(kind, hint_name)
            if kind == 'object':
                if as_definition:
                    return self._object_sequence_body(type_def)
                return self._hoist_object(type_def, hint_name)
            if kind == 'array':
                items = type_def.get('items', 'any')
                return f"SEQUENCE OF {self.type_expression(items, self._child(hint_name, 'Item'))}"
            if kind == 'set':
                items = type_def.get('items', 'any')
                return f"SET OF {self.type_expression(items, self._child(hint_name, 'Item'))}"
            if kind == 'map':
                values = type_def.get('values', 'any')
                value_expr = self.type_expression(values, self._child(hint_name, 'Value'))
                return f"SEQUENCE OF SEQUENCE {{ key UTF8String, value {value_expr} }}"
            if kind == 'tuple':
                return self._tuple_expression(type_def, hint_name)
            if kind == 'choice':
                return self._choice_expression(type_def, hint_name)
            if self._is_primitive(kind):
                return self._constrained_primitive(kind, type_def)
            if isinstance(kind, str) and kind:
                # Named type reference expressed as {"type": "SomeName"}.
                return self.ensure_definition(kind)
        return 'ANY'

    def _union_expression(self, union: List[JsonNode], hint_name: Optional[str]) -> str:
        non_null = [t for t in union if t != 'null']
        if len(non_null) == 0:
            return 'NULL'
        if len(non_null) == 1:
            return self.type_expression(non_null[0], hint_name)
        members = []
        used: set = set()
        for i, branch in enumerate(non_null):
            alt = self._alternative_name(branch, i, used)
            members.append(f"{alt} {self.type_expression(branch, self._child(hint_name, 'Alt'))}")
        return "CHOICE { " + ", ".join(members) + " }"

    def _tuple_expression(self, type_def: Dict, hint_name: Optional[str]) -> str:
        used: set = set()
        members = []
        order = type_def.get('tuple')
        properties = type_def.get('properties')
        if isinstance(order, list) and all(isinstance(o, str) for o in order) and isinstance(properties, dict):
            # JSON Structure named-tuple idiom: `tuple` lists ordered property names.
            for name in order:
                item = properties.get(name, 'any')
                member = self._unique_identifier(name, used)
                members.append(f"{member} {self.type_expression(item, self._child(hint_name, name))}")
        else:
            # Positional list-of-types idiom (items / prefixItems / tuple-of-typedefs).
            items = order or type_def.get('items') or type_def.get('prefixItems') or []
            if isinstance(items, dict):
                items = [items]
            for i, item in enumerate(items):
                member = self._unique_identifier(f"item{i}", used)
                members.append(f"{member} {self.type_expression(item, self._child(hint_name, f'Item{i}'))}")
        return "SEQUENCE { " + ", ".join(members) + " }" if members else "SEQUENCE {}"

    def _choice_expression(self, type_def: Dict, hint_name: Optional[str]) -> str:
        choices = type_def.get('choices', [])
        if isinstance(choices, dict):
            entries = list(choices.items())
        else:
            entries = [(f"option{i+1}", branch) for i, branch in enumerate(choices)]
        used: set = set()
        members = []
        for name, branch in entries:
            alt = self._unique_identifier(name, used)
            members.append(f"{alt} {self.type_expression(branch, self._child(hint_name, name))}")
        return "CHOICE { " + ", ".join(members) + " }" if members else "CHOICE { unspecified NULL }"

    def _alternative_name(self, branch: JsonNode, index: int, used: set) -> str:
        if isinstance(branch, str):
            base = branch
        elif isinstance(branch, dict):
            if '$ref' in branch:
                base = self._ref_name(branch['$ref'])
            elif isinstance(branch.get('type'), dict) and '$ref' in branch['type']:
                base = self._ref_name(branch['type']['$ref'])
            else:
                type_val = branch.get('type')
                base = branch.get('name') or (type_val if isinstance(type_val, str) else f'choice{index}')
        else:
            base = f'choice{index}'
        return self._unique_identifier(base, used)

    # --------------------------------------------------------------- objects

    def _hoist_object(self, type_def: Dict, hint_name: Optional[str]) -> str:
        """Emit an anonymous inline object as a top-level SEQUENCE assignment; return its name."""
        key = id(type_def)
        if key in self.inline_emitted:
            return self.inline_emitted[key]
        name = type_def.get('name') or hint_name or 'Object'
        asn1_name = self._unique_type_name(name)
        self.inline_emitted[key] = asn1_name
        body = self._object_sequence_body(type_def)
        self._append_assignment(asn1_name, body, self._doc_of(type_def))
        return asn1_name

    def _collect_object(self, type_def: Dict, seen: Optional[set] = None) -> tuple:
        """Collect an object's properties + required set, merging any `$extends` bases."""
        if seen is None:
            seen = set()
        properties: Dict[str, Any] = {}
        required: set = set()
        extends = type_def.get('$extends')
        if extends:
            bases = extends if isinstance(extends, list) else [extends]
            for base_ref in bases:
                if not isinstance(base_ref, str) or base_ref in seen:
                    continue
                seen.add(base_ref)
                base_def = self._resolve_ref(base_ref)
                if isinstance(base_def, dict):
                    base_props, base_required = self._collect_object(base_def, seen)
                    properties.update(base_props)
                    required |= base_required
        for prop_name, prop_def in (type_def.get('properties', {}) or {}).items():
            properties[prop_name] = prop_def
        required |= set(type_def.get('required', []) or [])
        return properties, required

    def _object_sequence_body(self, type_def: Dict) -> str:
        """Build a multi-line ASN.1 SEQUENCE body from a JSON Structure object."""
        properties, required = self._collect_object(type_def)
        parent_hint = type_def.get('name')
        used_members: set = set()

        entries: List[tuple] = []
        for prop_name, prop_def in properties.items():
            member = self._unique_identifier(prop_name, used_members)
            nullable = self._is_nullable(prop_def)
            optional = (prop_name not in required) or nullable
            expr = self.type_expression(prop_def, self._child(parent_hint, prop_name))
            doc = self._doc_of(prop_def)
            entries.append((member, expr, optional, doc))

        if not entries:
            return "SEQUENCE {}"

        lines: List[str] = []
        for i, (member, expr, optional, doc) in enumerate(entries):
            comma = "," if i < len(entries) - 1 else ""
            if doc:
                lines.append(f"    -- {self._comment(doc)}")
            opt = " OPTIONAL" if optional else ""
            lines.append(f"    {member} {expr}{opt}{comma}")
        return "SEQUENCE {\n" + "\n".join(lines) + "\n}"

    @staticmethod
    def _is_nullable(prop_def: Any) -> bool:
        if isinstance(prop_def, list):
            return 'null' in prop_def
        if isinstance(prop_def, dict):
            t = prop_def.get('type')
            return isinstance(t, list) and 'null' in t
        return False

    # ------------------------------------------------------------- utilities

    def _child(self, parent: Optional[str], child: str) -> str:
        base = parent if parent else 'Type'
        return f"{base}-{child}"

    @staticmethod
    def _doc_of(node: Any) -> Optional[str]:
        if isinstance(node, dict):
            return node.get('description') or node.get('doc')
        return None

    @staticmethod
    def _comment(text: str) -> str:
        return re.sub(r'\s+', ' ', str(text)).replace('--', '- -').strip()

    def _append_assignment(self, type_name: str, body: str, doc: Optional[str] = None) -> None:
        assignment = ''
        if doc:
            assignment += f"-- {self._comment(doc)}\n"
        assignment += f"{type_name} ::= {body}"
        self.assignments.append(assignment)

    # ----------------------------------------------------------------- render

    def convert(self, structure_schema: Dict) -> str:
        """Convert a JSON Structure schema to an ASN.1 module string."""
        self.definitions = structure_schema.get('definitions', {}) or {}

        top_type = structure_schema.get('type')
        if top_type:
            root_name = structure_schema.get('name', 'Root')
            if top_type == 'object':
                asn1_name = self._unique_type_name(root_name)
                self.def_emitted.setdefault(root_name, asn1_name)
                body = self._object_sequence_body(structure_schema)
                self._append_assignment(asn1_name, body, self._doc_of(structure_schema))
            else:
                asn1_name = self._unique_type_name(root_name)
                body = self._definition_body(structure_schema, asn1_name)
                self._append_assignment(asn1_name, body, self._doc_of(structure_schema))

        # Emit any definitions not already emitted on demand, descending into
        # nested namespaces (dicts without a `type` key are namespaces, not types).
        self._emit_all(self.definitions, '')

        return self.render()

    def _emit_all(self, node: Dict, prefix: str) -> None:
        for name, value in node.items():
            if not isinstance(value, dict):
                continue
            path = f"{prefix}{name}" if not prefix else f"{prefix}/{name}"
            if 'type' in value or '$ref' in value or 'choices' in value:
                self.ensure_definition(path, value)
            else:
                # Namespace container: recurse.
                self._emit_all(value, path)

    def render(self) -> str:
        lines = [f"{self.module_name} DEFINITIONS AUTOMATIC TAGS ::= BEGIN", ""]
        for assignment in self.assignments:
            lines.append(assignment)
            lines.append("")
        lines.append("END")
        return "\n".join(lines) + "\n"


def convert_structure_to_asn1(structure_schema_path: str, asn1_file_path: str, module_name: str = '') -> None:
    """Convert a JSON Structure schema file to an ASN.1 module file."""
    with open(structure_schema_path, 'r', encoding='utf-8') as structure_file:
        structure_schema = json.load(structure_file)

    if not module_name:
        module_name = os.path.splitext(os.path.basename(asn1_file_path))[0] or 'StructureSchema'

    converter = StructureToASN1(module_name)
    asn1_text = converter.convert(structure_schema)

    out_dir = os.path.dirname(asn1_file_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)
    with open(asn1_file_path, 'w', encoding='utf-8') as asn1_file:
        asn1_file.write(asn1_text)
