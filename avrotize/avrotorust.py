import json
import os
import hashlib
import re
from typing import Dict, List, Union
from avrotize.common import (
    is_generic_avro_type,
    is_any_value_type,
    render_template,
    pascal,
    camel,
    snake,
)
from avrotize.rust_xml import xml_wire_name, xml_enum_wire_value

INDENT = '    '

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | None


class AvroToRust:
    """Converts Avro schema to Rust structs, including Serde and Avro marshalling methods"""

    def __init__(self, base_package: str = '') -> None:
        self.base_package = base_package.replace('.', '/').lower()
        self.output_dir = os.getcwd()
        self.generated_types_avro_namespace: Dict[str, str] = {}
        self.generated_types_rust_package: Dict[str, str] = {}
        self.generated_union_fields: Dict[str, List[Dict]] = {}
        self.generated_struct_avro_test_values: Dict[str, List[str]] = {}
        self.avro_named_types: Dict[str, Dict] = {}
        self.avro_short_names: Dict[str, List[str]] = {}
        self.avro_type_fullnames: Dict[int, str] = {}
        self.union_path_identities: Dict[str, str] = {}
        self.union_alias_candidates: Dict[tuple[str, str], List[str]] = {}
        self.planned_alias_contents: Dict[str, str] = {}
        self.generated_aliases_to_remove: List[tuple[str, str]] = []
        self.avro_annotation = False
        self.serde_annotation = False
        self.xml_annotation = False
        
    reserved_words = [
            'as', 'break', 'const', 'continue', 'crate', 'else', 'enum', 'extern', 'false', 'fn', 'for', 'if', 'impl',
            'in', 'let', 'loop', 'match', 'mod', 'move', 'mut', 'pub', 'ref', 'return', 'self', 'Self', 'static',
            'struct', 'super', 'trait', 'true', 'type', 'unsafe', 'use', 'where', 'while', 'async', 'await', 'dyn',
        ]

    def safe_identifier(self, name: str) -> str:
        """Converts a name to a safe Rust identifier"""
        if name in AvroToRust.reserved_words:
            return f"{name}_"
        return name
    
    def escaped_identifier(self, name: str) -> str:
        """Converts a name to a safe Rust identifier with a leading r# prefix"""
        if name != "crate" and name in AvroToRust.reserved_words:
            return f"r#{name}"
        return name
    
    def safe_package(self, package: str) -> str:
        """Converts a package name to a safe Rust package name"""
        elements = package.split('::')
        return '::'.join([self.escaped_identifier(element) for element in elements])

    def map_primitive_to_rust(self, avro_fullname: str, is_optional: bool) -> str:
        """Maps Avro primitive types to Rust types"""
        # Handle AnyValue (extensible any type) regardless of namespace qualification
        if is_any_value_type(avro_fullname):
            return 'Option<serde_json::Value>' if is_optional else 'serde_json::Value'
        optional_mapping = {
            'null': 'None',
            'boolean': 'Option<bool>',
            'int': 'Option<i32>',
            'long': 'Option<i64>',
            'float': 'Option<f32>',
            'double': 'Option<f64>',
            'bytes': 'Option<Vec<u8>>',
            'string': 'Option<String>',
        }
        required_mapping = {
            'null': 'None',
            'boolean': 'bool',
            'int': 'i32',
            'long': 'i64',
            'float': 'f32',
            'double': 'f64',
            'bytes': 'Vec<u8>',
            'string': 'String',
        }
        rust_fullname = avro_fullname
        if '.' in rust_fullname:
            type_name = pascal(avro_fullname.split('.')[-1])
            package_name = '::'.join(avro_fullname.split('.')[:-1]).lower()
            rust_fullname = self.safe_package(self.concat_package(package_name, type_name))
        if rust_fullname in self.generated_types_rust_package:
            return rust_fullname
        else:
            return required_mapping.get(avro_fullname, avro_fullname) if not is_optional else optional_mapping.get(avro_fullname, avro_fullname)

    def concat_package(self, package: str, name: str) -> str:
        """Concatenates package and name using a double colon separator"""
        if package:
            return f"crate::{package.lower()}::{name.lower()}::{name}"
        return f"crate::{name.lower()}::{name}"

    def convert_avro_type_to_rust(
        self,
        field_name: str,
        avro_type: Union[str, Dict, List],
        namespace: str,
        nullable: bool = False,
        path=None,
    ) -> str:
        """Converts Avro type to Rust type"""
        ns = namespace.replace('.', '::').lower()
        type_name = ''
        if isinstance(avro_type, str):
            if is_any_value_type(avro_type):
                type_name = self.map_primitive_to_rust(avro_type, nullable)
            else:
                named_type = self.resolve_avro_named_type(avro_type, namespace)
                if named_type and named_type.get('type') in ('record', 'enum', 'fixed'):
                    if named_type.get('type') == 'fixed':
                        if named_type.get('logicalType') == 'decimal':
                            return 'f64'
                        if 'logicalType' not in named_type:
                            return 'Vec<u8>'
                    named_fullname = self.avro_type_fullnames[id(named_type)]
                    named_namespace = named_fullname.rpartition('.')[0]
                    rust_namespace = named_namespace.replace('.', '::').lower()
                    rust_name = self.safe_identifier(
                        pascal(named_fullname.rsplit('.', 1)[-1])
                    )
                    type_name = self.safe_package(
                        self.concat_package(rust_namespace, rust_name)
                    )
                else:
                    type_name = self.map_primitive_to_rust(avro_type, nullable)
        elif isinstance(avro_type, list):
            if is_generic_avro_type(avro_type):
                return 'serde_json::Value' if self.serde_annotation or self.xml_annotation else 'std::collections::HashMap<String, String>'
            non_null_types = [t for t in avro_type if t != 'null']
            if len(non_null_types) == 0:
                type_name = '()'
            elif len(non_null_types) == 1:
                if isinstance(non_null_types[0], str):
                    inner_type = self.convert_avro_type_to_rust(
                        field_name,
                        non_null_types[0],
                        namespace,
                        path=path,
                    )
                    type_name = (
                        inner_type
                        if inner_type.startswith('Option<')
                        else f'Option<{inner_type}>'
                    )
                else:
                    type_name = self.convert_avro_type_to_rust(
                        field_name,
                        non_null_types[0],
                        namespace,
                        path=path,
                    )
            else:
                type_name = self.generate_union_enum(
                    field_name,
                    avro_type,
                    namespace,
                    path=path,
                )
        elif isinstance(avro_type, dict):
            if avro_type['type'] in ['record', 'enum']:
                type_name = self.generate_class_or_enum(
                    avro_type,
                    namespace,
                    path=path,
                )
            elif avro_type['type'] == 'fixed':
                if avro_type.get('logicalType') == 'decimal':
                    return 'f64'
                if 'logicalType' not in avro_type:
                    return 'Vec<u8>'
            elif avro_type['type'] == 'bytes' and 'logicalType' in avro_type:
                if avro_type.get('logicalType') == 'decimal':
                    return 'f64'
            elif avro_type['type'] == 'array':
                item_type = self.convert_avro_type_to_rust(
                    field_name,
                    avro_type['items'],
                    namespace,
                    path=(path or []) + [('array', 'items')],
                )
                return f"Vec<{item_type}>"
            elif avro_type['type'] == 'map':
                values_type = self.convert_avro_type_to_rust(
                    field_name,
                    avro_type['values'],
                    namespace,
                    path=(path or []) + [('map', 'values')],
                )
                return f"std::collections::HashMap<String, {values_type}>"
            elif 'logicalType' in avro_type:
                if avro_type['logicalType'] == 'date':
                    return 'chrono::NaiveDate'
                elif avro_type['logicalType'] == 'time-millis' or avro_type['logicalType'] == 'time-micros':
                    return 'chrono::NaiveTime'
                elif avro_type['logicalType'] == 'timestamp-millis' or avro_type['logicalType'] == 'timestamp-micros':
                    return 'chrono::NaiveDateTime'
                elif avro_type['logicalType'] == 'uuid':
                    return 'uuid::Uuid'
            else:
                type_name = self.convert_avro_type_to_rust(
                    field_name,
                    avro_type['type'],
                    namespace,
                    path=path,
                )
        if type_name:
            return type_name
        return 'serde_json::Value' if self.serde_annotation or self.xml_annotation else 'std::collections::HashMap<String, String>'

    def generate_class_or_enum(
        self,
        avro_schema: Dict,
        parent_namespace: str = '',
        path=None,
    ) -> str:
        """Generates a Rust struct or enum from an Avro schema"""
        fullname, namespace, _ = self.canonical_avro_name(
            avro_schema['name'],
            avro_schema.get('namespace', parent_namespace),
        )
        type_path = path or [('record', fullname)]
        if avro_schema['type'] == 'record':
            return self.generate_struct(avro_schema, namespace, type_path)
        elif avro_schema['type'] == 'enum':
            return self.generate_enum(avro_schema, namespace)
        return 'serde_json::Value'

    @staticmethod
    def canonical_avro_name(name: str, namespace: str = ''):
        """Returns Avro fullname, namespace, and short name."""
        if '.' in name:
            fullname = name
            resolved_namespace, _, short_name = name.rpartition('.')
            return fullname, resolved_namespace, short_name
        fullname = f'{namespace}.{name}' if namespace else name
        return fullname, namespace, name

    def resolve_avro_named_type(self, name: str, namespace: str = ''):
        """Resolves a named reference using Avro namespace rules."""
        if '.' in name:
            return self.avro_named_types.get(name)
        contextual_name = f'{namespace}.{name}' if namespace else name
        if contextual_name in self.avro_named_types:
            return self.avro_named_types[contextual_name]
        return None

    def union_name_from_path(self, path) -> str:
        """Builds a collision-proof Rust union name from typed path segments."""
        segment_codes = {
            'record': 'R',
            'field': 'F',
            'branch': 'B',
            'array': 'A',
            'map': 'M',
        }
        framed = []
        for segment_type, value in path:
            encoded = str(value).encode('utf-8').hex()
            framed.append(
                f'{segment_codes[segment_type]}{len(encoded)}X{encoded}'
            )
        identity = ''.join(framed)
        digest = hashlib.sha256(identity.encode('ascii')).hexdigest()
        existing = self.union_path_identities.get(digest)
        if existing is not None and existing != identity:
            raise RuntimeError('SHA-256 collision in generated Rust union identity')
        self.union_path_identities[digest] = identity
        return 'UnionPath' + digest

    def index_avro_named_types(self, node, parent_namespace=''):
        """Indexes named Avro types by canonical fullname."""
        if isinstance(node, list):
            for item in node:
                self.index_avro_named_types(item, parent_namespace)
            return
        if not isinstance(node, dict):
            return

        node_type = node.get('type')
        namespace = parent_namespace
        if node_type in ('record', 'enum', 'fixed') and node.get('name'):
            fullname, namespace, short_name = self.canonical_avro_name(
                node['name'],
                node.get('namespace', parent_namespace),
            )
            self.avro_named_types[fullname] = node
            self.avro_type_fullnames[id(node)] = fullname
            candidates = self.avro_short_names.setdefault(short_name, [])
            if fullname not in candidates:
                candidates.append(fullname)
        if node_type == 'record':
            for field in node.get('fields', []):
                self.index_avro_named_types(field.get('type'), namespace)
        elif node_type == 'array':
            self.index_avro_named_types(node.get('items'), namespace)
        elif node_type == 'map':
            self.index_avro_named_types(node.get('values'), namespace)
        elif isinstance(node_type, (dict, list)):
            self.index_avro_named_types(node_type, namespace)

    def inline_avro_references(
        self,
        node,
        namespace='',
        resolving=None,
        defined_names=None,
    ):
        """Inline named types so each generated module owns a self-contained schema."""
        resolving = set() if resolving is None else resolving
        defined_names = set() if defined_names is None else defined_names
        if isinstance(node, str):
            resolved = self.resolve_avro_named_type(node, namespace)
            if not resolved:
                return node
            full_name = self.avro_type_fullnames[id(resolved)]
            resolved_namespace = full_name.rpartition('.')[0]
            if full_name in resolving:
                return full_name
            if full_name in defined_names:
                return full_name
            defined_names.add(full_name)
            return self.inline_avro_references(
                resolved,
                resolved_namespace,
                resolving | {full_name},
                defined_names,
            )
        if isinstance(node, list):
            return [
                self.inline_avro_references(
                    item,
                    namespace,
                    resolving,
                    defined_names,
                )
                for item in node
            ]
        if not isinstance(node, dict):
            return node

        result = dict(node)
        node_type = node.get('type')
        current_namespace = namespace
        current_resolving = resolving
        if node_type in ('record', 'enum', 'fixed') and node.get('name'):
            full_name, current_namespace, short_name = self.canonical_avro_name(
                node['name'],
                node.get('namespace', namespace),
            )
            result['name'] = short_name
            if current_namespace:
                result['namespace'] = current_namespace
            if full_name in defined_names and full_name not in resolving:
                return full_name
            defined_names.add(full_name)
            current_resolving = resolving | {full_name}

        if node_type == 'record':
            result['fields'] = [
                {
                    **field,
                    'type': self.inline_avro_references(
                        field.get('type'),
                        current_namespace,
                        current_resolving,
                        defined_names,
                    ),
                }
                for field in node.get('fields', [])
            ]
        elif node_type == 'array':
            result['items'] = self.inline_avro_references(
                node.get('items'),
                current_namespace,
                current_resolving,
                defined_names,
            )
        elif node_type == 'map':
            result['values'] = self.inline_avro_references(
                node.get('values'),
                current_namespace,
                current_resolving,
                defined_names,
            )
        elif isinstance(node_type, (dict, list)):
            result['type'] = self.inline_avro_references(
                node_type,
                current_namespace,
                current_resolving,
                defined_names,
            )
        return result

    def collect_xml_field_metadata(
        self, avro_type
    ) -> tuple[
        set[str],
        set[str],
        set[str],
        set[tuple[str, str]],
        set[tuple[str, str, str]],
        set[tuple[str, str]],
    ]:
        """Collects nested XML element, attribute, and map field names."""
        elements: set[str] = set()
        attributes: set[str] = set()
        maps: set[str] = set()
        relationships: set[tuple[str, str]] = set()
        namespaces: set[tuple[str, str, str]] = set()
        attribute_owners: set[tuple[str, str]] = set()
        visited: set[tuple[int, str]] = set()

        def nested_records(node, inherited_namespace=''):
            if isinstance(node, str):
                resolved = self.resolve_avro_named_type(
                    node,
                    inherited_namespace,
                )
                return [resolved] if resolved and resolved.get('type') == 'record' else []
            if isinstance(node, list):
                return [
                    record
                    for item in node
                    for record in nested_records(item, inherited_namespace)
                ]
            if not isinstance(node, dict):
                return []
            node_type = node.get('type')
            if node_type == 'record':
                return [node]
            if node_type == 'array':
                return nested_records(node.get('items'), inherited_namespace)
            if isinstance(node_type, (dict, list)):
                return nested_records(node_type, inherited_namespace)
            return []

        def visit(
            node,
            parent_element=None,
            inherited_xml_namespace='',
            inherited_avro_namespace='',
        ):
            if isinstance(node, str):
                resolved = self.resolve_avro_named_type(
                    node,
                    inherited_avro_namespace,
                )
                if resolved:
                    visit(
                        resolved,
                        parent_element,
                        inherited_xml_namespace,
                        inherited_avro_namespace,
                    )
                return
            if isinstance(node, list):
                for item in node:
                    visit(
                        item,
                        parent_element,
                        inherited_xml_namespace,
                        inherited_avro_namespace,
                    )
            elif isinstance(node, dict):
                node_type = node.get('type')
                if node_type == 'record':
                    visit_key = (id(node), parent_element or '')
                    if visit_key in visited:
                        return
                    visited.add(visit_key)
                    _, record_avro_namespace, _ = self.canonical_avro_name(
                        node['name'],
                        node.get('namespace', inherited_avro_namespace),
                    )
                    record_xml_namespace = node.get(
                        'xmlns',
                        inherited_xml_namespace,
                    )
                    for nested_field in node.get('fields', []):
                        name = xml_wire_name(nested_field['name'], nested_field)
                        if nested_field.get('xmlkind', 'element') == 'attribute':
                            attributes.add(name)
                            attribute_owners.add((parent_element or '', name))
                        else:
                            elements.add(name)
                            parent = parent_element or ''
                            if parent_element:
                                relationships.add((parent_element, name))
                            nested_type = nested_field.get('type')
                            if isinstance(nested_type, dict) and nested_type.get('type') == 'map':
                                maps.add(name)
                            records = nested_records(
                                nested_type,
                                record_avro_namespace,
                            )
                            field_namespaces = {
                                record.get(
                                    'xmlns',
                                    record_xml_namespace,
                                )
                                for record in records
                            } or {record_xml_namespace}
                            namespaces.update((parent, name, namespace) for namespace in field_namespaces)
                        visit(
                            nested_field.get('type'),
                            name,
                            record_xml_namespace,
                            record_avro_namespace,
                        )
                elif node_type == 'array':
                    visit(
                        node.get('items'),
                        parent_element,
                        inherited_xml_namespace,
                        inherited_avro_namespace,
                    )
                elif node_type == 'map':
                    visit(
                        node.get('values'),
                        parent_element,
                        inherited_xml_namespace,
                        inherited_avro_namespace,
                    )
                elif isinstance(node_type, (dict, list)):
                    visit(
                        node_type,
                        parent_element,
                        inherited_xml_namespace,
                        inherited_avro_namespace,
                    )

        initial_avro_namespace = ''
        if isinstance(avro_type, dict) and avro_type.get('name'):
            _, initial_avro_namespace, _ = self.canonical_avro_name(
                avro_type['name'],
                avro_type.get('namespace', ''),
            )
        visit(avro_type, inherited_avro_namespace=initial_avro_namespace)
        return elements, attributes, maps, relationships, namespaces, attribute_owners

    def generate_struct(
        self,
        avro_schema: Dict,
        parent_namespace: str,
        path=None,
    ) -> str:
        """Generates a Rust struct from an Avro record schema"""
        fullname, parent_namespace, short_name = self.canonical_avro_name(
            avro_schema['name'],
            avro_schema.get('namespace', parent_namespace),
        )
        struct_name = self.safe_identifier(pascal(short_name))
        record_path = path or [('record', fullname)]
        fields = []
        for field in avro_schema.get('fields', []):
            original_field_name = field['name']
            field_name = self.safe_identifier(snake(original_field_name))
            field_path = record_path + [('field', original_field_name)]
            field_type = self.convert_avro_type_to_rust(
                field_name,
                field['type'],
                parent_namespace,
                path=field_path,
            )
            xml_name = xml_wire_name(original_field_name, field)
            xml_kind = field.get('xmlkind', 'element')
            serde_name = f"@{xml_name}" if self.xml_annotation and xml_kind == 'attribute' else (
                xml_name if self.xml_annotation else original_field_name
            )
            serde_rename = field_name != serde_name
            # Check if this is a generated type (enum, union, or record) where random values may match default
            is_generated_type = field_type in self.generated_types_rust_package or '::' in field_type
            base_field_type = (
                field_type[7:-1]
                if field_type.startswith('Option<') else field_type
            )
            avro_union_fields = (
                self.generated_union_fields.get(base_field_type, [])
                if self.avro_annotation else []
            )
            source_null_index = (
                field['type'].index('null')
                if isinstance(field['type'], list)
                and 'null' in field['type']
                and any(item != 'null' for item in field['type'])
                and not is_generic_avro_type(field['type'])
                and not field_type.startswith('Option<')
                else -1
            )
            avro_decode = (
                self.render_avro_decode_value(
                    field['type'],
                    field_type,
                    parent_namespace,
                    'field_value',
                    [0],
                )
                if self.avro_annotation else ''
            )
            avro_encode = (
                self.render_avro_encode_value(
                    field['type'],
                    field_type,
                    parent_namespace,
                    f'&self.{field_name}',
                    [0],
                )
                if self.avro_annotation else ''
            )
            fields.append({
                'original_name': original_field_name,
                'json_name': original_field_name,
                'serde_name': serde_name,
                'serde_alias': original_field_name if self.xml_annotation and (self.serde_annotation or self.avro_annotation) and original_field_name != serde_name else '',
                'xml_name': xml_name,
                'xml_kind': xml_kind,
                'name': field_name,
                'type': field_type,
                'serde_rename': serde_rename,
                'is_optional': field_type.startswith('Option<'),
                'random_value': self.generate_random_value_for_avro(
                    field_type,
                    field['type'],
                    parent_namespace,
                ),
                'is_generated_type': is_generated_type,
                'is_avro_union': bool(avro_union_fields),
                'avro_union_type': base_field_type,
                'avro_union_fields': avro_union_fields,
                'avro_null_index': source_null_index,
                'avro_decode': avro_decode,
                'avro_encode': avro_encode,
            })
        
        ns = parent_namespace.replace('.', '::').lower()
        qualified_struct_name = self.safe_package(self.concat_package(ns, struct_name))
        avro_test_instances = []
        if self.avro_annotation:
            for field, field_schema in zip(fields, avro_schema.get('fields', [])):
                for test_value in self.generate_avro_test_values(
                    field_schema['type'],
                    field['type'],
                    parent_namespace,
                ):
                    avro_test_instances.append(
                        f'''{{
            let mut instance = {struct_name}::generate_random_instance();
            instance.{field["name"]} = {test_value};
            instance
        }}'''
                    )
        self.generated_struct_avro_test_values[
            qualified_struct_name
        ] = [
            instance.replace(
                f'{struct_name}::generate_random_instance()',
                f'{qualified_struct_name}::generate_random_instance()',
            )
            for instance in avro_test_instances
        ]
        if not 'namespace' in avro_schema:
            avro_schema['namespace'] = parent_namespace
        avro_schema_str = json.dumps(
            self.inline_avro_references(avro_schema, parent_namespace)
        )
        avro_schema_str = avro_schema_str.replace('"', '§')
        avro_schema_str = f"\",\n{INDENT*2}\"".join(
            [avro_schema_str[i:i+80] for i in range(0, len(avro_schema_str), 80)])
        avro_schema_str = avro_schema_str.replace('§', '\\"')
        avro_schema_str = f"concat!(\"{avro_schema_str}\")"
        (
            descendant_elements,
            descendant_attributes,
            descendant_maps,
            descendant_relationships,
            element_namespaces,
            attribute_owners,
        ) = self.collect_xml_field_metadata(avro_schema)

        context = {
            'avro_annotation': self.avro_annotation,
            'serde_annotation': self.serde_annotation,
            'xml_annotation': self.xml_annotation,
            'doc': avro_schema.get('doc', ''),
            'struct_name': struct_name,
            'xml_name': xml_wire_name(avro_schema['name'], avro_schema),
            'xml_namespace': avro_schema.get('xmlns', ''),
            'xml_descendant_elements': sorted(descendant_elements),
            'xml_descendant_attributes': sorted(descendant_attributes),
            'xml_descendant_maps': sorted(descendant_maps),
            'xml_descendant_relationships': sorted(descendant_relationships),
            'xml_element_namespaces': sorted(element_namespaces),
            'xml_attribute_owners': sorted(attribute_owners),
            'fields': fields,
            'avro_schema': avro_schema_str,
            'avro_test_instances': avro_test_instances,
            'json_match_predicates': [self.get_is_json_match_clause(f['original_name'], f['type']) for f in fields]
        }

        file_name = self.to_file_name(qualified_struct_name)
        target_file = os.path.join(self.output_dir, "src", file_name + ".rs")
        render_template('avrotorust/dataclass_struct.rs.jinja', target_file, **context)
        self.write_mod_rs(parent_namespace)

        self.generated_types_avro_namespace[qualified_struct_name] = "struct"
        self.generated_types_rust_package[qualified_struct_name] = "struct"

        return qualified_struct_name

    def generate_avro_test_values(
        self,
        avro_type,
        rust_type: str,
        namespace: str,
    ) -> List[str]:
        """Generates deterministic values for every nested Avro union branch."""
        if isinstance(avro_type, list):
            if is_generic_avro_type(avro_type):
                if rust_type != 'serde_json::Value':
                    return [
                        'std::iter::once(('
                        '"key".to_string(), '
                        '"value".to_string())).collect()'
                    ]
                return [
                    'serde_json::json!("value")',
                    'serde_json::json!(true)',
                    'serde_json::json!({"key": "value"})',
                ]
            non_null_types = [item for item in avro_type if item != 'null']
            if len(non_null_types) > 1:
                union_type = (
                    rust_type[7:-1]
                    if rust_type.startswith('Option<') else rust_type
                )
                values = []
                union_fields = self.generated_union_fields.get(union_type, [])
                for field in union_fields:
                    branch_values = self.generate_avro_test_values(
                        field['avro_type'],
                        field['type'],
                        namespace,
                    ) or [field['random_value']]
                    values.extend(
                        f'{union_type}::{field["name"]}({value})'
                        for value in branch_values
                    )
                return values
            if len(non_null_types) == 1 and rust_type.startswith('Option<'):
                inner_type = rust_type[7:-1]
                inner_values = self.generate_avro_test_values(
                    non_null_types[0],
                    inner_type,
                    namespace,
                )
                return ['None'] + [f'Some({value})' for value in inner_values]

        resolved_type = avro_type
        if isinstance(avro_type, str):
            if is_any_value_type(avro_type):
                return []
            resolved_type = (
                self.resolve_avro_named_type(avro_type, namespace)
                or avro_type
            )

        if isinstance(resolved_type, dict):
            node_type = resolved_type.get('type')
            if node_type == 'record':
                return self.generated_struct_avro_test_values.get(rust_type, [])
            if node_type == 'array':
                inner_type = rust_type[4:-1]
                return [
                    f'vec![{value}]'
                    for value in self.generate_avro_test_values(
                        resolved_type['items'],
                        inner_type,
                        namespace,
                    )
                ]
            if node_type == 'map':
                inner_type = rust_type.split(', ', 1)[1][:-1]
                return [
                    (
                        'std::iter::once(('
                        '"branch".to_string(), '
                        f'{value})).collect()'
                    )
                    for value in self.generate_avro_test_values(
                        resolved_type['values'],
                        inner_type,
                        namespace,
                    )
                ]
        return []

    def render_avro_encode_value(
        self,
        avro_type,
        rust_type: str,
        namespace: str,
        value_expression: str,
        counter: List[int],
    ) -> str:
        """Renders recursive Rust encoding with explicit Avro union indexes."""
        counter[0] += 1
        suffix = counter[0]

        if isinstance(avro_type, list):
            if is_generic_avro_type(avro_type):
                standalone_schema = self.inline_avro_references(
                    avro_type,
                    namespace,
                )
                schema_literal = json.dumps(json.dumps(standalone_schema))
                return (
                    f'apache_avro::to_value({value_expression})?.resolve('
                    '&apache_avro::Schema::parse_str('
                    f'{schema_literal})?)?'
                )
            non_null_types = [item for item in avro_type if item != 'null']
            if len(non_null_types) > 1:
                union_type = (
                    rust_type[7:-1]
                    if rust_type.startswith('Option<') else rust_type
                )
                union_fields = self.generated_union_fields.get(union_type, [])
                arms = []
                for union_field in union_fields:
                    arms.append(
                        f'''{union_type}::{union_field["name"]}(value) =>
                            apache_avro::types::Value::Union(
                                {union_field["source_avro_index"]},
                                Box::new({union_field["avro_encode"]}),
                            )'''
                    )
                arms_text = ',\n'.join(arms)
                return f'''match {value_expression} {{
                    {arms_text},
                }}'''

            if len(non_null_types) == 1:
                inner_type = (
                    rust_type[7:-1]
                    if rust_type.startswith('Option<') else rust_type
                )
                inner_encode = self.render_avro_encode_value(
                    non_null_types[0],
                    inner_type,
                    namespace,
                    (
                        'value'
                        if rust_type.startswith('Option<')
                        else value_expression
                    ),
                    counter,
                )
                null_index = avro_type.index('null')
                value_index = next(
                    index for index, item in enumerate(avro_type)
                    if item != 'null'
                )
                if rust_type.startswith('Option<'):
                    return f'''match {value_expression} {{
                        None => apache_avro::types::Value::Union(
                            {null_index},
                            Box::new(apache_avro::types::Value::Null),
                        ),
                        Some(value) => apache_avro::types::Value::Union(
                            {value_index},
                            Box::new({inner_encode}),
                        ),
                    }}'''
                return f'''apache_avro::types::Value::Union(
                    {value_index},
                    Box::new({inner_encode}),
                )'''

        resolved_type = avro_type
        if isinstance(avro_type, str):
            if is_any_value_type(avro_type):
                return f'apache_avro::to_value({value_expression})?'
            if avro_type == 'bytes':
                return (
                    'apache_avro::types::Value::Bytes('
                    f'({value_expression}).clone())'
                )
            resolved_type = (
                self.resolve_avro_named_type(avro_type, namespace)
                or avro_type
            )

        if isinstance(resolved_type, dict):
            node_type = resolved_type.get('type')
            if node_type == 'bytes' and 'logicalType' not in resolved_type:
                return (
                    'apache_avro::types::Value::Bytes('
                    f'({value_expression}).clone())'
                )
            if node_type == 'fixed' and 'logicalType' not in resolved_type:
                return (
                    '{\n'
                    f'    let bytes = ({value_expression}).clone();\n'
                    f'    if bytes.len() != {resolved_type["size"]} {{\n'
                    '        return Err("invalid Avro fixed size".into());\n'
                    '    }\n'
                    '    apache_avro::types::Value::Fixed('
                    f'{resolved_type["size"]}, bytes)\n'
                    '}'
                )
            if node_type == 'record':
                return f'({value_expression}).to_avro_value()?'
            if node_type == 'array':
                inner_type = rust_type[4:-1]
                item_name = f'item_{suffix}'
                values_name = f'values_{suffix}'
                inner_encode = self.render_avro_encode_value(
                    resolved_type['items'],
                    inner_type,
                    namespace,
                    item_name,
                    counter,
                )
                return f'''{{
                    let mut {values_name} = Vec::with_capacity(
                        ({value_expression}).len()
                    );
                    for {item_name} in ({value_expression}).iter() {{
                        {values_name}.push({inner_encode});
                    }}
                    apache_avro::types::Value::Array({values_name})
                }}'''
            if node_type == 'map':
                inner_type = rust_type.split(', ', 1)[1][:-1]
                item_name = f'item_{suffix}'
                values_name = f'values_{suffix}'
                inner_encode = self.render_avro_encode_value(
                    resolved_type['values'],
                    inner_type,
                    namespace,
                    item_name,
                    counter,
                )
                return f'''{{
                    let mut {values_name} = std::collections::HashMap::new();
                    for (key, {item_name}) in ({value_expression}).iter() {{
                        {values_name}.insert(key.clone(), {inner_encode});
                    }}
                    apache_avro::types::Value::Map({values_name})
                }}'''

        return f'apache_avro::to_value({value_expression})?'

    def render_avro_decode_value(
        self,
        avro_type,
        rust_type: str,
        namespace: str,
        value_expression: str,
        counter: List[int],
    ) -> str:
        """Renders index-aware Rust decoding for a generated field value."""
        counter[0] += 1
        suffix = counter[0]

        if isinstance(avro_type, list):
            if is_generic_avro_type(avro_type):
                if rust_type != 'serde_json::Value':
                    return f'''match {value_expression} {{
                        apache_avro::types::Value::Union(_, value) => {{
                            match value.as_ref() {{
                                apache_avro::types::Value::Map(items) => {{
                                    let mut result = std::collections::HashMap::new();
                                    for (key, item) in items {{
                                        let item = match item {{
                                            apache_avro::types::Value::Union(_, inner) =>
                                                inner.as_ref(),
                                            other => other,
                                        }};
                                        result.insert(
                                            key.clone(),
                                            apache_avro::from_value(item)?,
                                        );
                                    }}
                                    result
                                }},
                                _ => return Err(
                                    "expected an Avro map for generic value".into()
                                ),
                            }}
                        }},
                        _ => return Err(
                            "expected an Avro union for generic value".into()
                        ),
                    }}'''
                return f'apache_avro::from_value({value_expression})?'
            non_null_types = [item for item in avro_type if item != 'null']
            if len(non_null_types) > 1:
                union_type = (
                    rust_type[7:-1]
                    if rust_type.startswith('Option<') else rust_type
                )
                union_fields = self.generated_union_fields.get(union_type, [])
                null_index = avro_type.index('null') if 'null' in avro_type else -1
                arms = []
                if null_index >= 0:
                    if rust_type.startswith('Option<'):
                        arms.append(f'{null_index} => None')
                    else:
                        arms.append(
                            f'''{null_index} => return Err(
                                "nullable Avro union null is unsupported by the generated Rust union API".into()
                            )'''
                        )
                for union_field in union_fields:
                    decoded = (
                        f'{union_type}::from_avro_branch('
                        f'{union_field["avro_index"]}, value)?'
                    )
                    if rust_type.startswith('Option<'):
                        decoded = f'Some({decoded})'
                    arms.append(
                        f'{union_field["source_avro_index"]} => {decoded}'
                    )
                arms_text = ',\n'.join(arms)
                return f'''match {value_expression} {{
                    apache_avro::types::Value::Union(index, value) => match index {{
                        {arms_text},
                        _ => return Err(format!(
                            "unsupported Avro union branch {{}}",
                            index,
                        ).into()),
                    }},
                    _ => return Err("expected an Avro union value".into()),
                }}'''

            if len(non_null_types) == 1:
                inner_rust_type = (
                    rust_type[7:-1]
                    if rust_type.startswith('Option<') else rust_type
                )
                inner_decode = self.render_avro_decode_value(
                    non_null_types[0],
                    inner_rust_type,
                    namespace,
                    'value.as_ref()',
                    counter,
                )
                null_index = avro_type.index('null')
                value_index = next(
                    index for index, item in enumerate(avro_type)
                    if item != 'null'
                )
                if rust_type.startswith('Option<'):
                    return f'''match {value_expression} {{
                        apache_avro::types::Value::Union(index, value) => match index {{
                            {null_index} => None,
                            {value_index} => Some({inner_decode}),
                            _ => return Err(format!(
                                "unsupported Avro optional branch {{}}",
                                index,
                            ).into()),
                        }},
                        _ => return Err("expected an Avro optional value".into()),
                    }}'''
                return f'''match {value_expression} {{
                    apache_avro::types::Value::Union(index, value) => match index {{
                        {null_index} => return Err(
                            "nullable Avro complex null is unsupported by the generated Rust API".into()
                        ),
                        {value_index} => {inner_decode},
                        _ => return Err(format!(
                            "unsupported Avro optional branch {{}}",
                            index,
                        ).into()),
                    }},
                    _ => return Err("expected an Avro optional value".into()),
                }}'''

        resolved_type = avro_type
        if isinstance(avro_type, str):
            if is_any_value_type(avro_type):
                return f'apache_avro::from_value({value_expression})?'
            if avro_type == 'bytes':
                return f'''match {value_expression} {{
                    apache_avro::types::Value::Bytes(bytes) => bytes.clone(),
                    _ => return Err("expected an Avro bytes value".into()),
                }}'''
            resolved_type = (
                self.resolve_avro_named_type(avro_type, namespace)
                or avro_type
            )

        if isinstance(resolved_type, dict):
            node_type = resolved_type.get('type')
            if node_type == 'bytes' and 'logicalType' not in resolved_type:
                return f'''match {value_expression} {{
                    apache_avro::types::Value::Bytes(bytes) => bytes.clone(),
                    _ => return Err("expected an Avro bytes value".into()),
                }}'''
            if node_type == 'fixed' and 'logicalType' not in resolved_type:
                return f'''match {value_expression} {{
                    apache_avro::types::Value::Fixed(size, bytes)
                        if *size == {resolved_type["size"]}
                            && bytes.len() == {resolved_type["size"]} =>
                        bytes.clone(),
                    _ => return Err("expected an Avro fixed value".into()),
                }}'''
            if node_type == 'record':
                return f'{rust_type}::from_avro_value({value_expression})?'
            if node_type == 'array':
                inner_rust_type = rust_type[4:-1]
                item_name = f'item_{suffix}'
                values_name = f'values_{suffix}'
                inner_decode = self.render_avro_decode_value(
                    resolved_type['items'],
                    inner_rust_type,
                    namespace,
                    item_name,
                    counter,
                )
                return f'''match {value_expression} {{
                    apache_avro::types::Value::Array(items) => {{
                        let mut {values_name} = Vec::with_capacity(items.len());
                        for {item_name} in items {{
                            {values_name}.push({inner_decode});
                        }}
                        {values_name}
                    }},
                    _ => return Err("expected an Avro array value".into()),
                }}'''
            if node_type == 'map':
                inner_rust_type = rust_type.split(', ', 1)[1][:-1]
                item_name = f'item_{suffix}'
                values_name = f'values_{suffix}'
                inner_decode = self.render_avro_decode_value(
                    resolved_type['values'],
                    inner_rust_type,
                    namespace,
                    item_name,
                    counter,
                )
                return f'''match {value_expression} {{
                    apache_avro::types::Value::Map(items) => {{
                        let mut {values_name} = std::collections::HashMap::new();
                        for (key, {item_name}) in items {{
                            {values_name}.insert(key.clone(), {inner_decode});
                        }}
                        {values_name}
                    }},
                    _ => return Err("expected an Avro map value".into()),
                }}'''

        return f'apache_avro::from_value({value_expression})?'
    
    def get_is_json_match_clause(self, field_name: str, field_type: str, for_union=False) -> str:
        """Generates the is_json_match clause for a field"""
        ref = f'node[\"{field_name}\"]' if not for_union else 'node'
        
        # Check if type is optional - if so, we need to allow null values
        is_optional = field_type.startswith('Option<')
        base_type = field_type[7:-1] if is_optional else field_type
        null_check = f" || {ref}.is_null()" if is_optional else ""
        
        # serde_json::Value can be any JSON type, so always return true
        if base_type == 'serde_json::Value':
            return "true"
        
        if base_type == 'String':
            return f"({ref}.is_string(){null_check})"
        elif base_type == 'bool':
            return f"({ref}.is_boolean(){null_check})"
        elif base_type == 'i32':
            return f"({ref}.is_i64(){null_check})"
        elif base_type == 'i64':
            return f"({ref}.is_i64(){null_check})"
        elif base_type == 'f32':
            return f"({ref}.is_f64(){null_check})"
        elif base_type == 'f64':
            return f"({ref}.is_f64(){null_check})"
        elif base_type == 'Vec<u8>':
            return f"({ref}.is_array(){null_check})"
        elif base_type == '()':
            return f"({ref}.is_null(){null_check})"
        elif base_type == 'std::collections::HashMap<String, String>':
            return f"({ref}.is_object(){null_check})"
        elif base_type.startswith('std::collections::HashMap<String, '):
            return f"({ref}.is_object(){null_check})"
        elif base_type.startswith('Vec<'):
            return f"({ref}.is_array(){null_check})"
        # chrono types - check for string (ISO 8601 format) or number (timestamp)
        elif 'chrono::NaiveDateTime' in base_type or 'NaiveDateTime' in base_type:
            return f"({ref}.is_string() || {ref}.is_i64(){null_check})"
        elif 'chrono::NaiveDate' in base_type or 'NaiveDate' in base_type:
            return f"({ref}.is_string() || {ref}.is_i64(){null_check})"
        elif 'chrono::NaiveTime' in base_type or 'NaiveTime' in base_type:
            return f"({ref}.is_string() || {ref}.is_i64(){null_check})"
        # uuid type - check for string
        elif 'uuid::Uuid' in base_type or 'Uuid' in base_type:
            return f"({ref}.is_string(){null_check})"
        else:
            # Custom types - call their is_json_match method
            if is_optional:
                return f"({base_type}::is_json_match(&{ref}) || {ref}.is_null())"
            return f"{base_type}::is_json_match(&{ref})"


    def generate_enum(self, avro_schema: Dict, parent_namespace: str) -> str:
        """Generates a Rust enum from an Avro enum schema"""
        symbols = [{
            'name': symbol,
            'value': xml_enum_wire_value(symbol, avro_schema) if self.xml_annotation else symbol,
            'json_value': symbol,
        } for symbol in avro_schema.get('symbols', [])]
        _, parent_namespace, short_name = self.canonical_avro_name(
            avro_schema['name'],
            avro_schema.get('namespace', parent_namespace),
        )
        enum_name = self.safe_identifier(pascal(short_name))
        ns = parent_namespace.replace('.', '::').lower()
        qualified_enum_name = self.safe_package(self.concat_package(ns, enum_name))
        
        if not 'namespace' in avro_schema:
            avro_schema['namespace'] = parent_namespace
        avro_schema_str = json.dumps(
            self.inline_avro_references(avro_schema, parent_namespace)
        )
        avro_schema_str = avro_schema_str.replace('"', '§')
        avro_schema_str = f"\",\n{INDENT*2}\"".join(
            [avro_schema_str[i:i+80] for i in range(0, len(avro_schema_str), 80)])
        avro_schema_str = avro_schema_str.replace('§', '\\"')
        avro_schema_str = f"concat!(\"{avro_schema_str}\")"

        context = {
            'avro_annotation': self.avro_annotation,
            'serde_annotation': self.serde_annotation,
            'xml_annotation': self.xml_annotation,
            'enum_name': enum_name,
            'symbols': symbols,
            'xml_name': xml_wire_name(avro_schema['name'], avro_schema),
            'avro_schema': avro_schema_str,
        }

        file_name = self.to_file_name(qualified_enum_name)
        target_file = os.path.join(self.output_dir, "src", file_name + ".rs")
        render_template('avrotorust/dataclass_enum.rs.jinja', target_file, **context)
        self.write_mod_rs(parent_namespace)

        self.generated_types_avro_namespace[qualified_enum_name] = "enum"
        self.generated_types_rust_package[qualified_enum_name] = "enum"

        return qualified_enum_name

    def generate_union_enum(
        self,
        field_name: str,
        avro_type: List,
        namespace: str,
        path=None,
    ) -> str:
        """Generates a union enum for Rust"""
        ns = namespace.replace('.', '::').lower()
        union_enum_name = self.union_name_from_path(
            path or [('field', field_name)]
        )
        union_avro_branches = [
            (source_index, avro_branch)
            for source_index, avro_branch in enumerate(avro_type)
            if avro_branch != 'null'
        ]
        union_avro_types = [
            avro_branch for _, avro_branch in union_avro_branches
        ]
        union_types = [
            self.convert_avro_type_to_rust(
                field_name + "Option" + str(source_index),
                avro_branch,
                namespace,
                path=(path or []) + [('branch', str(source_index))],
            )
            for i, (source_index, avro_branch) in enumerate(union_avro_branches)
        ]
        avro_schema_str = json.dumps(
            self.inline_avro_references(union_avro_types, namespace)
        )
        avro_schema_str = avro_schema_str.replace('"', '§')
        avro_schema_str = f"\",\n{INDENT*2}\"".join(
            [avro_schema_str[i:i+80] for i in range(0, len(avro_schema_str), 80)])
        avro_schema_str = avro_schema_str.replace('§', '\\"')
        avro_schema_str = f"concat!(\"{avro_schema_str}\")"
        
        # Track seen predicates to identify structurally identical variants
        seen_predicates: set = set()
        # Track seen variant names to deduplicate
        seen_names: dict = {}
        union_fields = []
        for i, t in enumerate(union_types):
            predicate = self.get_is_json_match_clause(
                field_name,
                t,
                for_union=True,
            )
            predicate_key = self.get_json_shape_signature(
                union_avro_types[i],
                namespace,
            )
            # Mark if this is the first variant with this predicate structure
            is_first_with_predicate = predicate_key not in seen_predicates
            seen_predicates.add(predicate_key)
            
            # Deduplicate variant names
            variant_name = pascal(t.rsplit('::',1)[-1])
            if variant_name in seen_names:
                seen_names[variant_name] += 1
                variant_name = f"{variant_name}{seen_names[variant_name]}"
            else:
                seen_names[variant_name] = 1
            
            union_fields.append({
                'name': variant_name, 
                'type': t, 
                'avro_type': union_avro_types[i],
                'avro_index': i,
                'source_avro_index': union_avro_branches[i][0],
                'avro_decode': self.render_avro_decode_value(
                    union_avro_types[i],
                    t,
                    namespace,
                    'value',
                    [0],
                ),
                'avro_encode': self.render_avro_encode_value(
                    union_avro_types[i],
                    t,
                    namespace,
                    'value',
                    [0],
                ),
                'random_value': self.generate_random_value_for_avro(
                    t,
                    union_avro_types[i],
                    namespace,
                ),
                'default_value': 'Default::default()',
                'json_match_predicate': predicate,
                'is_first_with_predicate': is_first_with_predicate,
            })
        scalar_kinds = {
            'String': 'string',
            'bool': 'bool',
            'i8': 'integer', 'i16': 'integer', 'i32': 'integer', 'i64': 'integer',
            'u8': 'integer', 'u16': 'integer', 'u32': 'integer', 'u64': 'integer',
            'isize': 'integer', 'usize': 'integer',
            'f32': 'float', 'f64': 'float',
        }
        present_scalar_kinds = {scalar_kinds[field['type']] for field in union_fields if field['type'] in scalar_kinds}
        predicate_counts = {
            predicate: sum(1 for field in union_fields if field['json_match_predicate'] == predicate)
            for predicate in {field['json_match_predicate'] for field in union_fields}
        }
        for field in union_fields:
            scalar_kind = scalar_kinds.get(field['type'])
            field['xml_scalar_kind'] = scalar_kind or ''
            field['xml_guard_string'] = scalar_kind == 'string' and len(present_scalar_kinds) > 1
            field['xml_reject_value'] = (
                (scalar_kind is not None and scalar_kind != 'string' and 'string' in present_scalar_kinds)
                or (scalar_kind == 'integer' and 'float' in present_scalar_kinds)
                or predicate_counts[field['json_match_predicate']] > 1
            )
            field['xml_safe_for_random'] = not field['xml_reject_value']
        xml_string_guards = {
            'bool': 'bool' in present_scalar_kinds,
            'integer': 'integer' in present_scalar_kinds,
            'float': 'float' in present_scalar_kinds,
        }
        
        qualified_union_enum_name = self.safe_package(self.concat_package(ns, union_enum_name))
        legacy_name = pascal(field_name) + 'Union'
        self.union_alias_candidates.setdefault(
            (namespace, legacy_name),
            [],
        ).append(qualified_union_enum_name)
        context = {
            'serde_annotation': self.serde_annotation,
            'avro_annotation': self.avro_annotation,
            'xml_annotation': self.xml_annotation,
            'union_enum_name': union_enum_name,
            'union_fields': union_fields,
            'xml_string_guards': xml_string_guards,
            'avro_schema': avro_schema_str,
            'json_match_predicates': [
                field['json_match_predicate']
                for field in union_fields
            ],
        }

        file_name = self.to_file_name(qualified_union_enum_name)
        target_file = os.path.join(self.output_dir, "src", file_name + ".rs")
        render_template('avrotorust/dataclass_union.rs.jinja', target_file, **context)
        self.generated_types_avro_namespace[qualified_union_enum_name] = "union"
        self.generated_types_rust_package[qualified_union_enum_name] = "union"
        self.generated_union_fields[qualified_union_enum_name] = union_fields
        self.write_mod_rs(namespace)

        return qualified_union_enum_name

    def write_union_aliases(self):
        """Emits legacy Avro union names when they are unambiguous."""
        for alias_path, namespace in self.generated_aliases_to_remove:
            if os.path.exists(alias_path):
                os.remove(alias_path)
                self.write_mod_rs(namespace)
        for (namespace, legacy_name), targets in sorted(
            self.union_alias_candidates.items()
        ):
            unique_targets = sorted(set(targets))
            if len(unique_targets) != 1:
                continue
            ns = namespace.replace('.', '::').lower()
            qualified_alias = self.safe_package(
                self.concat_package(ns, legacy_name)
            )
            if qualified_alias in self.generated_types_rust_package:
                continue
            target_file = os.path.join(
                self.output_dir,
                'src',
                self.to_file_name(qualified_alias) + '.rs',
            )
            module_directory = os.path.splitext(target_file)[0]
            alias_content = self.union_alias_content(
                legacy_name,
                unique_targets[0],
            )
            if os.path.isdir(module_directory):
                continue
            if os.path.exists(target_file):
                with open(target_file, 'r', encoding='utf-8') as alias_file:
                    existing_content = alias_file.read()
                if existing_content == alias_content:
                    self.generated_types_rust_package[qualified_alias] = 'alias'
                    self.write_mod_rs(namespace)
                    continue
            os.makedirs(os.path.dirname(target_file), exist_ok=True)
            temporary_file = target_file + '.tmp'
            with open(temporary_file, 'w', encoding='utf-8') as alias_file:
                alias_file.write(alias_content)
            os.replace(temporary_file, target_file)
            self.generated_types_rust_package[qualified_alias] = 'alias'
            self.write_mod_rs(namespace)

    @staticmethod
    def union_alias_content(legacy_name: str, target: str) -> str:
        """Returns the complete generated legacy alias source."""
        return (
            f'pub type {legacy_name} = {target};\n\n'
            '#[cfg(test)]\n'
            'mod tests {\n'
            '    use super::*;\n\n'
            '    #[test]\n'
            '    fn legacy_alias_compiles() {\n'
            f'        let _ = {legacy_name}::default();\n'
            '    }\n'
            '}\n'
        )

    @staticmethod
    def is_generated_union_alias(content: str, legacy_name: str) -> bool:
        """Checks whether source is a previously generated alias file."""
        pattern = (
            rf'^pub type {re.escape(legacy_name)} = '
            r'crate::[A-Za-z0-9_#:]+;\n\n'
            r'#\[cfg\(test\)\]\n'
            r'mod tests \{\n'
            r'    use super::\*;\n\n'
            r'    #\[test\]\n'
            r'    fn legacy_alias_compiles\(\) \{\n'
            rf'        let _ = {re.escape(legacy_name)}::default\(\);\n'
            r'    \}\n'
            r'\}\n$'
        )
        return re.fullmatch(pattern, content) is not None

    @staticmethod
    def is_legacy_generated_union(content: str, legacy_name: str) -> bool:
        """Checks whether source is a previously generated union module."""
        markers = (
            f'pub enum {legacy_name} {{',
            f'impl Default for {legacy_name} {{',
            'pub fn is_json_match(',
            'pub fn generate_random_instance()',
            f'fn test_union_variants_{legacy_name.lower()}()',
        )
        return all(marker in content for marker in markers)

    def get_json_shape_signature(
        self,
        avro_type,
        namespace: str,
        resolving=None,
    ):
        """Returns a stable structural signature for untagged JSON matching."""
        resolving = () if resolving is None else resolving
        if isinstance(avro_type, str):
            resolved = self.resolve_avro_named_type(avro_type, namespace)
            if resolved:
                fullname = self.avro_type_fullnames[id(resolved)]
                if fullname in resolving:
                    return ('ref', resolving.index(fullname))
                return self.get_json_shape_signature(
                    resolved,
                    fullname.rpartition('.')[0],
                    resolving,
                )
            if avro_type in ('int', 'long'):
                return 'integer'
            if avro_type in ('float', 'double'):
                return 'number'
            return avro_type
        if isinstance(avro_type, list):
            return (
                'union',
                tuple(
                    self.get_json_shape_signature(
                        item,
                        namespace,
                        resolving,
                    )
                    for item in avro_type
                ),
            )
        if not isinstance(avro_type, dict):
            return str(avro_type)
        node_type = avro_type.get('type')
        if node_type == 'record':
            fullname, record_namespace, _ = self.canonical_avro_name(
                avro_type['name'],
                avro_type.get('namespace', namespace),
            )
            if fullname in resolving:
                return ('ref', resolving.index(fullname))
            nested_resolving = resolving + (fullname,)
            return (
                'record',
                tuple(
                    (
                        field['name'],
                        self.get_json_shape_signature(
                            field['type'],
                            record_namespace,
                            nested_resolving,
                        ),
                    )
                    for field in avro_type.get('fields', [])
                ),
            )
        if node_type == 'enum':
            return 'string'
        if node_type == 'array':
            return (
                'array',
                self.get_json_shape_signature(
                    avro_type['items'],
                    namespace,
                    resolving,
                ),
            )
        if node_type == 'map':
            return (
                'map',
                self.get_json_shape_signature(
                    avro_type['values'],
                    namespace,
                    resolving,
                ),
            )
        return self.get_json_shape_signature(
            node_type,
            namespace,
            resolving,
        )

    def to_file_name(self, qualified_name):
        """Converts a qualified union enum name to a file name"""
        if qualified_name.startswith('crate::'):
            qualified_name = qualified_name[(len('crate::')):]
        qualified_name = qualified_name.replace('r#', '')
        return qualified_name.rsplit('::',1)[0].replace('::', os.sep).lower()

    def generate_random_value_for_avro(
        self,
        rust_type: str,
        avro_type,
        namespace: str,
    ) -> str:
        """Generates a random value that respects schema-specific sizes."""
        resolved_type = avro_type
        if isinstance(avro_type, str):
            resolved_type = (
                self.resolve_avro_named_type(avro_type, namespace)
                or avro_type
            )
        if isinstance(resolved_type, dict):
            node_type = resolved_type.get('type')
            if node_type == 'fixed' and 'logicalType' not in resolved_type:
                return (
                    'vec![rand::Rng::gen::<u8>(&mut rng); '
                    f'{resolved_type["size"]}]'
                )
            if node_type == 'array':
                inner_type = rust_type[4:-1]
                inner_value = self.generate_random_value_for_avro(
                    inner_type,
                    resolved_type['items'],
                    namespace,
                )
                return f'(0..3).map(|_| {inner_value}).collect()'
            if node_type == 'map':
                inner_type = rust_type.split(', ', 1)[1][:-1]
                inner_value = self.generate_random_value_for_avro(
                    inner_type,
                    resolved_type['values'],
                    namespace,
                )
                return (
                    '(0..3).map(|_| ('
                    'format!("key_{}", rand::Rng::gen::<u32>(&mut rng)), '
                    f'{inner_value})).collect()'
                )
        if isinstance(avro_type, list):
            non_null_types = [
                item for item in avro_type if item != 'null'
            ]
            if len(non_null_types) == 1:
                inner_type = (
                    rust_type[7:-1]
                    if rust_type.startswith('Option<') else rust_type
                )
                return self.generate_random_value_for_avro(
                    inner_type,
                    non_null_types[0],
                    namespace,
                )
            return self.generate_random_value(rust_type)
        return self.generate_random_value(rust_type)
    
    def generate_random_value(self, rust_type: str) -> str:
        """Generates a random value for a given Rust type"""
        if rust_type == 'String' or rust_type == 'Option<String>':
            return 'format!("random_string_{}", rand::Rng::gen::<u32>(&mut rng))'
        elif rust_type == 'bool' or rust_type == 'Option<bool>':
            return 'rand::Rng::gen::<bool>(&mut rng)'
        elif rust_type == 'i32' or rust_type == 'Option<i32>':
            return 'rand::Rng::gen_range(&mut rng, 0..100)'
        elif rust_type == 'i64' or rust_type == 'Option<i64>':
            return 'rand::Rng::gen_range(&mut rng, 0..100) as i64'
        elif rust_type == 'f32' or rust_type == 'Option<f32>':
            return '(rand::Rng::gen::<f32>(&mut rng)*1000.0).round()/1000.0'
        elif rust_type == 'f64' or rust_type == 'Option<f64>':
            return '(rand::Rng::gen::<f64>(&mut rng)*1000.0).round()/1000.0'
        elif rust_type == 'Vec<u8>' or rust_type == 'Option<Vec<u8>>':
            return 'vec![rand::Rng::gen::<u8>(&mut rng); 10]'
        elif rust_type == 'chrono::NaiveDate':
            return 'chrono::NaiveDate::from_ymd(rand::Rng::gen_range(&mut rng, 2000..2023), rand::Rng::gen_range(&mut rng, 1..13), rand::Rng::gen_range(&mut rng, 1..29))'
        elif rust_type == 'chrono::NaiveTime':
            return 'chrono::NaiveTime::from_hms(rand::Rng::gen_range(&mut rng, 0..24),rand::Rng::gen_range(&mut rng, 0..60), rand::Rng::gen_range(&mut rng, 0..60))'
        elif rust_type == 'chrono::NaiveDateTime':
            return 'chrono::NaiveDateTime::new(chrono::NaiveDate::from_ymd(rand::Rng::gen_range(&mut rng, 2000..2023), rand::Rng::gen_range(&mut rng, 1..13), rand::Rng::gen_range(&mut rng, 1..29)), chrono::NaiveTime::from_hms(rand::Rng::gen_range(&mut rng, 0..24), rand::Rng::gen_range(&mut rng, 0..60), rand::Rng::gen_range(&mut rng, 0..60)))'
        elif rust_type == 'uuid::Uuid':
            return 'uuid::Uuid::new_v4()'
        elif rust_type.startswith('Option<'):
            return self.generate_random_value(rust_type[7:-1])
        elif rust_type.startswith('std::collections::HashMap<String, '):
            inner_type = rust_type.split(', ')[1][:-1]
            return f'(0..3).map(|_| (format!("key_{{}}", rand::Rng::gen::<u32>(&mut rng)), {self.generate_random_value(inner_type)})).collect()'
        elif rust_type.startswith('Vec<'):
            inner_type = rust_type[4:-1]
            return f'(0..3).map(|_| {self.generate_random_value(inner_type)}).collect()'
        elif rust_type in self.generated_types_rust_package:
            return f'{rust_type}::generate_random_instance()'
        else:
            return 'Default::default()'

    def write_mod_rs(self, namespace: str):
        """Writes the mod.rs file for a Rust module"""
        if not namespace:
            return
        directories = [part.lower() for part in namespace.split('.')]
        for i in range(len(directories)):
            sub_package = '::'.join(directories[:i + 1])
            directory_path = os.path.join(
                self.output_dir, "src", sub_package.replace('.', os.sep).replace('::', os.sep))
            if not os.path.exists(directory_path):
                os.makedirs(directory_path, exist_ok=True)
            mod_rs_path = os.path.join(directory_path, "mod.rs")
            
            types = sorted(
                file.replace('.rs', '')
                for file in os.listdir(directory_path)
                if file.endswith('.rs') and file != "mod.rs"
            )
            mod_statements = '\n'.join(f'pub mod {self.escaped_identifier(typ.lower())};' for typ in types)
            mods = sorted(
                directory
                for directory in os.listdir(directory_path)
                if os.path.isdir(os.path.join(directory_path, directory))
            )
            mod_statements += '\n' + '\n'.join(f'pub mod {self.escaped_identifier(mod.lower())};' for mod in mods)

            with open(mod_rs_path, 'w', encoding='utf-8') as file:
                file.write(mod_statements)

    def write_cargo_toml(self):
        """Writes the Cargo.toml file for the Rust project"""
        dependencies = []
        if self.serde_annotation or self.avro_annotation or self.xml_annotation:
            dependencies.append('serde = { version = "1.0", features = ["derive"] }')
        dependencies.append('serde_json = "1.0"')
        dependencies.append('chrono = { version = "0.4", features = ["serde"] }')
        dependencies.append('uuid = { version = "1.11", features = ["serde", "v4"] }')
        if self.avro_annotation or self.serde_annotation or self.xml_annotation:
            dependencies.append('flate2 = "1.0"')
        if self.xml_annotation:
            dependencies.append('quick-xml = { version = "0.38", features = ["serialize"] }')
        if self.avro_annotation:
            dependencies.append('apache-avro = "0.17"')
            dependencies.append('lazy_static = "1.4"')
        dependencies.append('rand = "0.8"')

        cargo_toml_content =  f"[package]\n"
        cargo_toml_content += f"name = \"{self.base_package.replace('/', '_')}\"\n"
        cargo_toml_content += f"version = \"0.1.0\"\n"
        cargo_toml_content += f"edition = \"2021\"\n\n"
        cargo_toml_content += f"[dependencies]\n"
        cargo_toml_content += "\n".join(f"{dependency}" for dependency in dependencies)
        cargo_toml_path = os.path.join(self.output_dir, "Cargo.toml")
        with open(cargo_toml_path, 'w', encoding='utf-8') as file:
            file.write(cargo_toml_content)

    def write_lib_rs(self):
        """Writes the lib.rs file for the Rust project"""
        modules = {
            self.to_file_name(name).split(os.sep)[0]
            for name in self.generated_types_rust_package
        }
        mod_statements = '\n'.join(
            f'pub mod {self.escaped_identifier(module)};'
            for module in sorted(modules)
        )
        if self.xml_annotation:
            mod_statements = 'pub(crate) mod xml_support;\n' + mod_statements
        
        lib_rs_content = f"""
// This is the library entry point

{mod_statements}
"""
        lib_rs_path = os.path.join(self.output_dir, "src", "lib.rs")
        if not os.path.exists(os.path.dirname(lib_rs_path)):
            os.makedirs(os.path.dirname(lib_rs_path), exist_ok=True)
        with open(lib_rs_path, 'w', encoding='utf-8') as file:
            file.write(lib_rs_content)

    def write_xml_support_rs(self):
        """Writes shared XML validation and bounded decompression helpers."""
        if self.xml_annotation:
            render_template(
                'rust/xml_support.rs.jinja',
                os.path.join(self.output_dir, "src", "xml_support.rs"),
            )

    def convert_schema(self, schema: JsonNode, output_dir: str):
        """Converts Avro schema to Rust"""
        if not isinstance(schema, list):
            schema = [schema]
        self.index_avro_named_types(schema)
        self.output_dir = output_dir
        self.validate_rust_generation_plan(schema)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        for avro_schema in (x for x in schema if isinstance(x, dict)):
            self.generate_class_or_enum(avro_schema)

        self.write_union_aliases()
        self.write_cargo_toml()
        self.write_xml_support_rs()
        self.write_lib_rs()

    def validate_rust_generation_plan(self, schema: JsonNode):
        """Validates every planned Rust source path before creating output."""
        planned = {}
        alias_candidates = {}
        visited_named = set()

        def add(path, kind, identity, description):
            path = tuple(part.lower() for part in path)
            existing = planned.get(path)
            artifact = {
                'kind': kind,
                'identity': identity,
                'description': description,
            }
            if existing is not None and (
                existing['kind'] != kind
                or existing['identity'] != identity
                or kind not in ('union', 'infrastructure')
            ):
                path_text = '::'.join(path)
                first, second = sorted((
                    existing['description'],
                    description,
                ))
                raise ValueError(
                    'Rust generation plan has an exact path collision at '
                    f"'{path_text}': '{first}' and '{second}'"
                )
            planned[path] = artifact

        def visit(node, namespace='', path=None, field_name=''):
            if isinstance(node, str):
                return
            if isinstance(node, list):
                if is_generic_avro_type(node):
                    return
                non_null = [
                    (index, item)
                    for index, item in enumerate(node)
                    if item != 'null'
                ]
                if len(non_null) > 1:
                    union_name = self.union_name_from_path(
                        path or [('field', field_name)]
                    )
                    namespace_parts = (
                        tuple(namespace.lower().split('.'))
                        if namespace else ()
                    )
                    union_output_path = (
                        namespace_parts + (union_name.lower(),)
                    )
                    union_identity = union_name
                    add(
                        union_output_path,
                        'union',
                        union_identity,
                        f"generated union {union_name} at "
                        f"{path or [('field', field_name)]}",
                    )
                    legacy_name = pascal(field_name) + 'Union'
                    alias_candidates.setdefault(
                        (namespace_parts, legacy_name),
                        set(),
                    ).add(union_name)
                    for source_index, branch in non_null:
                        visit(
                            branch,
                            namespace,
                            (path or []) + [
                                ('branch', str(source_index))
                            ],
                            field_name + 'Option' + str(source_index),
                        )
                elif len(non_null) == 1:
                    visit(
                        non_null[0][1],
                        namespace,
                        path,
                        field_name,
                    )
                return
            if not isinstance(node, dict):
                return

            node_type = node.get('type')
            if node_type in ('record', 'enum', 'fixed') and node.get('name'):
                fullname, node_namespace, short_name = (
                    self.canonical_avro_name(
                        node['name'],
                        node.get('namespace', namespace),
                    )
                )
                namespace = node_namespace
                if node_type in ('record', 'enum'):
                    namespace_parts = (
                        tuple(namespace.lower().split('.'))
                        if namespace else ()
                    )
                    add(
                        namespace_parts + (
                            self.safe_identifier(
                                pascal(short_name)
                            ).lower(),
                        ),
                        'named',
                        fullname,
                        f"named type {fullname}",
                    )
                if fullname in visited_named:
                    return
                visited_named.add(fullname)
                if node_type == 'record':
                    record_path = path or [('record', fullname)]
                    for field in node.get('fields', []):
                        visit(
                            field.get('type'),
                            namespace,
                            record_path + [
                                ('field', field['name'])
                            ],
                            self.safe_identifier(snake(field['name'])),
                        )
                return
            if node_type == 'array':
                visit(
                    node.get('items'),
                    namespace,
                    (path or []) + [('array', 'items')],
                    field_name,
                )
            elif node_type == 'map':
                visit(
                    node.get('values'),
                    namespace,
                    (path or []) + [('map', 'values')],
                    field_name,
                )
            elif isinstance(node_type, (dict, list)):
                visit(node_type, namespace, path, field_name)

        for top_level in schema:
            visit(top_level)

        for (namespace_parts, legacy_name), targets in sorted(
            alias_candidates.items()
        ):
            alias_name = legacy_name.lower()
            relative_parts = list(namespace_parts) + [
                alias_name + '.rs'
            ]
            alias_path = os.path.join(
                self.output_dir,
                'src',
                *relative_parts,
            )
            alias_directory = os.path.splitext(alias_path)[0]
            namespace = '::'.join(namespace_parts)
            existing_content = None
            if os.path.isdir(alias_directory):
                raise ValueError(
                    'Existing directory conflicts with planned legacy '
                    f"union alias: '{alias_path}'"
                )
            if os.path.exists(alias_path):
                with open(
                    alias_path,
                    'r',
                    encoding='utf-8',
                ) as alias_file:
                    existing_content = alias_file.read()

            if len(targets) != 1:
                if existing_content is not None:
                    if (
                        self.is_generated_union_alias(
                            existing_content,
                            legacy_name,
                        )
                        or self.is_legacy_generated_union(
                            existing_content,
                            legacy_name,
                        )
                    ):
                        self.generated_aliases_to_remove.append(
                            (alias_path, namespace)
                        )
                    else:
                        raise ValueError(
                            'Existing file conflicts with ambiguous legacy '
                            f"union alias: '{alias_path}'"
                        )
                continue

            if len(targets) == 1:
                union_name = next(iter(targets))
                qualified_target = self.safe_package(
                    self.concat_package(namespace, union_name)
                )
                add(
                    namespace_parts + (alias_name,),
                    'alias',
                    qualified_target,
                    f"legacy union alias {legacy_name}",
                )
                alias_content = self.union_alias_content(
                    legacy_name,
                    qualified_target,
                )
                if existing_content is not None:
                    if (
                        existing_content != alias_content
                        and not self.is_generated_union_alias(
                            existing_content,
                            legacy_name,
                        )
                        and not self.is_legacy_generated_union(
                            existing_content,
                            legacy_name,
                        )
                    ):
                        raise ValueError(
                            'Existing file conflicts with planned '
                            f"legacy union alias: '{alias_path}'"
                        )
                self.planned_alias_contents[alias_path] = alias_content

        source_paths = list(planned)
        add(
            ('lib',),
            'infrastructure',
            'lib.rs',
            'generated infrastructure lib.rs',
        )
        for source_path in source_paths:
            for length in range(1, len(source_path)):
                add(
                    source_path[:length] + ('mod',),
                    'infrastructure',
                    '::'.join(source_path[:length]) + '::mod.rs',
                    'generated infrastructure mod.rs for '
                    + '::'.join(source_path[:length]),
                )

        normalized_paths = sorted(planned)
        for file_path, other_path in zip(
            normalized_paths,
            normalized_paths[1:],
        ):
            if len(file_path) < len(other_path) and (
                other_path[:len(file_path)] == file_path
            ):
                path_text = '::'.join(file_path)
                first, second = sorted(
                    (
                        planned[file_path]['description'],
                        planned[other_path]['description'],
                    )
                )
                raise ValueError(
                    'Rust generation plan requires the same path '
                    f"'{path_text}' as both a file and directory: "
                    f"'{first}' and '{second}'"
                )

    def convert(self, avro_schema_path: str, output_dir: str):
        """Converts Avro schema to Rust"""
        with open(avro_schema_path, 'r', encoding='utf-8') as file:
            schema = json.load(file)
        self.convert_schema(schema, output_dir)


def convert_avro_to_rust(avro_schema_path, rust_file_path, package_name='', avro_annotation=False, serde_annotation=False, xml_annotation=False):
    """Converts Avro schema to Rust structs

    Args:
        avro_schema_path (str): Avro input schema path  
        rust_file_path (str): Output Rust file path 
        package_name (str): Base package name
        avro_annotation (bool): Include Avro annotations
        serde_annotation (bool): Include Serde annotations
        xml_annotation (bool): Include quick-xml compatible Serde annotations
    """
    
    if not package_name:
        package_name = os.path.splitext(os.path.basename(avro_schema_path))[0].lower().replace('-', '_')
        
    avrotorust = AvroToRust()
    avrotorust.base_package = package_name
    avrotorust.avro_annotation = avro_annotation
    avrotorust.serde_annotation = serde_annotation
    avrotorust.xml_annotation = xml_annotation
    avrotorust.convert(avro_schema_path, rust_file_path)


def convert_avro_schema_to_rust(avro_schema: JsonNode, output_dir: str, package_name='', avro_annotation=False, serde_annotation=False, xml_annotation=False):
    """Converts Avro schema to Rust structs

    Args:
        avro_schema (JsonNode): Avro schema as a dictionary or list of dictionaries
        output_dir (str): Output directory path 
        package_name (str): Base package name
        avro_annotation (bool): Include Avro annotations
        serde_annotation (bool): Include Serde annotations
        xml_annotation (bool): Include quick-xml compatible Serde annotations
    """
    avrotorust = AvroToRust()
    avrotorust.base_package = package_name
    avrotorust.avro_annotation = avro_annotation
    avrotorust.serde_annotation = serde_annotation
    avrotorust.xml_annotation = xml_annotation
    avrotorust.convert_schema(avro_schema, output_dir)
