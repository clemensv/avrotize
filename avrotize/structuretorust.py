# pylint: disable=line-too-long

""" StructureToRust class for converting JSON Structure schema to Rust structs """

import json
import os
import re
from typing import Any, Dict, List, Set, Tuple, Union, Optional

from avrotize.common import (
    pascal,
    snake,
    render_template,
    json_wire_name,
    json_enum_wire_value,
)
from avrotize.rust_xml import xml_wire_name, xml_enum_wire_value

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | None

INDENT = '    '


class StructureToRust:
    """ Converts JSON Structure schema to Rust structs """

    def __init__(self, base_package: str = '', serde_annotation: bool = False, xml_annotation: bool = False) -> None:
        self.base_package = base_package.replace('.', '/').lower()
        self.serde_annotation = serde_annotation
        self.xml_annotation = xml_annotation
        self.output_dir = os.getcwd()
        self.schema_doc: JsonNode = None
        self.generated_types_rust_package: Dict[str, str] = {}
        self.generated_structure_types: Dict[str, Dict[str, Union[str, Dict, List]]] = {}
        self.type_dict: Dict[str, Dict] = {}
        self.definitions: Dict[str, Any] = {}
        self.schema_registry: Dict[str, Dict] = {}

    reserved_words = [
        'as', 'break', 'const', 'continue', 'crate', 'else', 'enum', 'extern', 'false', 'fn', 'for', 'if', 'impl',
        'in', 'let', 'loop', 'match', 'mod', 'move', 'mut', 'pub', 'ref', 'return', 'self', 'Self', 'static',
        'struct', 'super', 'trait', 'true', 'type', 'unsafe', 'use', 'where', 'while', 'async', 'await', 'dyn',
    ]

    def safe_identifier(self, name: str) -> str:
        """Converts a name to a safe Rust identifier"""
        if name in StructureToRust.reserved_words:
            return f"{name}_"
        return name

    def escaped_identifier(self, name: str) -> str:
        """Converts a name to a safe Rust identifier with a leading r# prefix"""
        if name != "crate" and name in StructureToRust.reserved_words:
            return f"r#{name}"
        return name

    def safe_package(self, package: str) -> str:
        """Converts a package name to a safe Rust package name"""
        elements = package.split('::')
        return '::'.join([self.escaped_identifier(element) for element in elements])

    def get_qualified_name(self, namespace: str, name: str) -> str:
        """ Concatenates namespace and name with a dot separator """
        return f"{namespace}.{name}" if namespace != '' else name

    def sanitize_namespace(self, namespace: str) -> str:
        """Converts a namespace to a valid Rust module path by replacing dots with underscores"""
        return namespace.replace('.', '_')

    def concat_namespace(self, namespace: str, name: str) -> str:
        """ Concatenates namespace and name with a dot separator """
        if namespace and name:
            return f"{namespace}.{name}"
        elif namespace:
            return namespace
        else:
            return name

    def concat_package(self, package: str, name: str) -> str:
        """Concatenates package and name using a double colon separator"""
        return f"crate::{package.lower()}::{name.lower()}::{name}" if package else name

    def map_primitive_to_rust(self, structure_type: str, is_optional: bool = False) -> str:
        """ Maps JSON Structure primitive types to Rust types """
        optional_mapping = {
            'null': 'None',
            'boolean': 'Option<bool>',
            'string': 'Option<String>',
            'integer': 'Option<i32>',
            'number': 'Option<f64>',
            'int8': 'Option<i8>',
            'uint8': 'Option<u8>',
            'int16': 'Option<i16>',
            'uint16': 'Option<u16>',
            'int32': 'Option<i32>',
            'uint32': 'Option<u32>',
            'int64': 'Option<i64>',
            'uint64': 'Option<u64>',
            'int128': 'Option<i128>',
            'uint128': 'Option<u128>',
            'float8': 'Option<f32>',
            'float': 'Option<f32>',
            'double': 'Option<f64>',
            'binary32': 'Option<f32>',
            'binary64': 'Option<f64>',
            'decimal': 'Option<f64>',
            'binary': 'Option<Vec<u8>>',
            'date': 'Option<chrono::NaiveDate>',
            'time': 'Option<chrono::NaiveTime>',
            'datetime': 'Option<chrono::DateTime<chrono::Utc>>',
            'timestamp': 'Option<chrono::DateTime<chrono::Utc>>',
            'duration': 'Option<chrono::Duration>',
            'uuid': 'Option<uuid::Uuid>',
            'uri': 'Option<String>',
            'jsonpointer': 'Option<String>',
            'any': 'Option<serde_json::Value>',
        }
        required_mapping = {
            'null': 'None',
            'boolean': 'bool',
            'string': 'String',
            'integer': 'i32',
            'number': 'f64',
            'int8': 'i8',
            'uint8': 'u8',
            'int16': 'i16',
            'uint16': 'u16',
            'int32': 'i32',
            'uint32': 'u32',
            'int64': 'i64',
            'uint64': 'u64',
            'int128': 'i128',
            'uint128': 'u128',
            'float8': 'f32',
            'float': 'f32',
            'double': 'f64',
            'binary32': 'f32',
            'binary64': 'f64',
            'decimal': 'f64',
            'binary': 'Vec<u8>',
            'date': 'chrono::NaiveDate',
            'time': 'chrono::NaiveTime',
            'datetime': 'chrono::DateTime<chrono::Utc>',
            'timestamp': 'chrono::DateTime<chrono::Utc>',
            'duration': 'chrono::Duration',
            'uuid': 'uuid::Uuid',
            'uri': 'String',
            'jsonpointer': 'String',
            'any': 'serde_json::Value',
        }
        
        rust_fullname = structure_type
        if '.' in rust_fullname:
            type_name = pascal(structure_type.split('.')[-1])
            package_name = '::'.join(structure_type.split('.')[:-1]).lower()
            rust_fullname = self.safe_package(self.concat_package(package_name, type_name))
        
        if rust_fullname in self.generated_types_rust_package:
            return rust_fullname
        else:
            return required_mapping.get(structure_type, 'serde_json::Value') if not is_optional else optional_mapping.get(structure_type, 'Option<serde_json::Value>')

    def resolve_ref(self, ref: str, context_schema: Optional[Dict] = None) -> Optional[Dict]:
        """ Resolves a $ref to the actual schema definition """
        if not ref.startswith('#/'):
            if ref in self.schema_registry:
                return self.schema_registry[ref]
            return None

        path = ref[2:].split('/')
        schema = context_schema if context_schema else self.schema_doc
        for part in path:
            if not isinstance(schema, dict) or part not in schema:
                return None
            schema = schema[part]
        return schema

    def register_schema_ids(self, schema: Dict, base_uri: str = '') -> None:
        """ Recursively registers schemas with $id keywords """
        if not isinstance(schema, dict):
            return

        if '$id' in schema:
            schema_id = schema['$id']
            if base_uri and not schema_id.startswith(('http://', 'https://', 'urn:')):
                from urllib.parse import urljoin
                schema_id = urljoin(base_uri, schema_id)
            self.schema_registry[schema_id] = schema
            base_uri = schema_id

        if 'definitions' in schema:
            for def_name, def_schema in schema['definitions'].items():
                if isinstance(def_schema, dict):
                    self.register_schema_ids(def_schema, base_uri)

        if 'properties' in schema:
            for prop_name, prop_schema in schema['properties'].items():
                if isinstance(prop_schema, dict):
                    self.register_schema_ids(prop_schema, base_uri)

        for key in ['items', 'values', 'additionalProperties']:
            if key in schema and isinstance(schema[key], dict):
                self.register_schema_ids(schema[key], base_uri)

    def convert_structure_type_to_rust(self, class_name: str, field_name: str, structure_type: JsonNode, parent_namespace: str, nullable: bool = False) -> str:
        """ Converts JSON Structure type to Rust type """
        ns = self.sanitize_namespace(parent_namespace).replace('.', '::').lower()
        
        if isinstance(structure_type, str):
            return self.map_primitive_to_rust(structure_type, nullable)
        elif isinstance(structure_type, list):
            # Handle type unions
            non_null_types = [t for t in structure_type if t != 'null']
            has_null = 'null' in structure_type
            
            if len(non_null_types) == 1:
                inner_type = self.convert_structure_type_to_rust(class_name, field_name, non_null_types[0], parent_namespace, False)
                if has_null:
                    if inner_type.startswith('Option<'):
                        return inner_type
                    return f'Option<{inner_type}>'
                return inner_type
            else:
                # Multiple non-null types - generate a union enum
                return self.generate_union_enum(field_name, structure_type, parent_namespace)
        elif isinstance(structure_type, dict):
            # Handle $ref
            if '$ref' in structure_type:
                ref_schema = self.resolve_ref(structure_type['$ref'], self.schema_doc if isinstance(self.schema_doc, dict) else None)
                if ref_schema:
                    ref_path = structure_type['$ref'].split('/')
                    type_name = ref_path[-1]
                    ref_namespace = '.'.join(ref_path[2:-1]) if len(ref_path) > 3 else parent_namespace
                    return self.generate_class_or_choice(ref_schema, ref_namespace, write_file=True, explicit_name=type_name)
                return 'serde_json::Value'

            # Handle enum keyword
            if 'enum' in structure_type:
                return self.generate_enum(structure_type, field_name, parent_namespace, write_file=True)

            # Handle type keyword
            if 'type' not in structure_type:
                return 'serde_json::Value'

            struct_type = structure_type['type']

            # Handle complex types
            if struct_type == 'object':
                return self.generate_class(structure_type, parent_namespace, write_file=True)
            elif struct_type == 'array':
                items_type = self.convert_structure_type_to_rust(
                    class_name, field_name+'Item', structure_type.get('items', {'type': 'any'}), parent_namespace)
                return f"Vec<{items_type}>"
            elif struct_type == 'set':
                items_type = self.convert_structure_type_to_rust(
                    class_name, field_name+'Item', structure_type.get('items', {'type': 'any'}), parent_namespace)
                return f"std::collections::HashSet<{items_type}>"
            elif struct_type == 'map':
                values_type = self.convert_structure_type_to_rust(
                    class_name, field_name+'Value', structure_type.get('values', {'type': 'any'}), parent_namespace)
                return f"std::collections::HashMap<String, {values_type}>"
            elif struct_type == 'choice':
                return self.generate_choice(structure_type, parent_namespace, write_file=True)
            elif struct_type == 'tuple':
                return self.generate_tuple(structure_type, parent_namespace, write_file=True)
            else:
                return self.convert_structure_type_to_rust(class_name, field_name, struct_type, parent_namespace, nullable)
        
        return 'serde_json::Value'

    def generate_class_or_choice(self, structure_schema: Dict, parent_namespace: str, write_file: bool = True, explicit_name: str = '') -> str:
        """ Generates a Class or Choice """
        struct_type = structure_schema.get('type', 'object')
        if struct_type == 'object':
            return self.generate_class(structure_schema, parent_namespace, write_file, explicit_name=explicit_name)
        elif struct_type == 'choice':
            return self.generate_choice(structure_schema, parent_namespace, write_file, explicit_name=explicit_name)
        elif struct_type == 'tuple':
            return self.generate_tuple(structure_schema, parent_namespace, write_file, explicit_name=explicit_name)
        return 'serde_json::Value'

    def collect_xml_field_metadata(
        self, structure_type
    ) -> tuple[
        set[str],
        set[str],
        set[str],
        set[tuple[str, str]],
        set[tuple[str, str, str]],
        set[tuple[str, str]],
    ]:
        """Collects nested XML element, attribute, and map property names."""
        elements: set[str] = set()
        attributes: set[str] = set()
        maps: set[str] = set()
        relationships: set[tuple[str, str]] = set()
        namespaces: set[tuple[str, str, str]] = set()
        attribute_owners: set[tuple[str, str]] = set()
        visited: set[tuple[int, str]] = set()

        def nested_objects(node):
            if isinstance(node, list):
                return [record for item in node for record in nested_objects(item)]
            if not isinstance(node, dict):
                return []
            if '$ref' in node:
                resolved = self.resolve_ref(
                    node['$ref'],
                    self.schema_doc if isinstance(self.schema_doc, dict) else None,
                )
                return nested_objects(resolved)
            node_type = node.get('type')
            if node_type in ('object', 'tuple'):
                return [node]
            if node_type in ('array', 'set'):
                return nested_objects(node.get('items'))
            if node_type == 'choice':
                return [
                    record
                    for choice in node.get('choices', {}).values()
                    for record in nested_objects(choice)
                ]
            return []

        def visit(node, parent_element=None, inherited_namespace=''):
            if isinstance(node, list):
                for item in node:
                    visit(item, parent_element, inherited_namespace)
            elif isinstance(node, dict):
                if '$ref' in node:
                    resolved = self.resolve_ref(
                        node['$ref'],
                        self.schema_doc if isinstance(self.schema_doc, dict) else None,
                    )
                    if resolved:
                        visit(resolved, parent_element, inherited_namespace)
                    return
                node_type = node.get('type')
                if node_type in ('object', 'tuple'):
                    visit_key = (id(node), parent_element or '')
                    if visit_key in visited:
                        return
                    visited.add(visit_key)
                    record_namespace = node.get('xmlns', inherited_namespace)
                    for prop_name, prop_schema in node.get('properties', {}).items():
                        name = xml_wire_name(prop_name, prop_schema)
                        if prop_schema.get('xmlkind', 'element') == 'attribute':
                            attributes.add(name)
                            attribute_owners.add((parent_element or '', name))
                        else:
                            elements.add(name)
                            parent = parent_element or ''
                            if parent_element:
                                relationships.add((parent_element, name))
                            if prop_schema.get('type') == 'map':
                                maps.add(name)
                            records = nested_objects(prop_schema)
                            field_namespaces = {
                                record.get('xmlns', record_namespace) for record in records
                            } or {record_namespace}
                            namespaces.update((parent, name, namespace) for namespace in field_namespaces)
                        visit(prop_schema, name, record_namespace)
                elif node_type in ('array', 'set'):
                    visit(node.get('items'), parent_element, inherited_namespace)
                elif node_type == 'map':
                    visit(node.get('values'), parent_element, inherited_namespace)
                elif node_type == 'choice':
                    for choice in node.get('choices', {}).values():
                        visit(choice, parent_element, inherited_namespace)

        visit(structure_type)
        return elements, attributes, maps, relationships, namespaces, attribute_owners

    def add_xml_union_metadata(self, variants: list[dict]) -> dict[str, bool]:
        """Marks union variants whose XML lexical forms cannot round-trip unambiguously."""
        scalar_kinds = {
            'String': 'string',
            'bool': 'bool',
            'i8': 'integer', 'i16': 'integer', 'i32': 'integer', 'i64': 'integer',
            'u8': 'integer', 'u16': 'integer', 'u32': 'integer', 'u64': 'integer',
            'isize': 'integer', 'usize': 'integer',
            'f32': 'float', 'f64': 'float',
        }
        present_scalar_kinds = {scalar_kinds[variant['type']] for variant in variants if variant['type'] in scalar_kinds}
        type_counts = {
            rust_type: sum(1 for variant in variants if variant['type'] == rust_type)
            for rust_type in {variant['type'] for variant in variants}
        }
        kind_counts = {
            kind: sum(1 for variant in variants if scalar_kinds.get(variant['type']) == kind)
            for kind in present_scalar_kinds
        }
        for variant in variants:
            scalar_kind = scalar_kinds.get(variant['type'])
            variant['xml_scalar_kind'] = scalar_kind or ''
            variant['xml_guard_string'] = scalar_kind == 'string' and len(present_scalar_kinds) > 1
            variant['xml_reject_value'] = (
                type_counts[variant['type']] > 1
                or (scalar_kind is not None and kind_counts[scalar_kind] > 1)
                or (scalar_kind is not None and scalar_kind != 'string' and 'string' in present_scalar_kinds)
                or (scalar_kind == 'integer' and 'float' in present_scalar_kinds)
            )
            variant['xml_safe_for_random'] = not variant['xml_reject_value']
        return {
            'bool': 'bool' in present_scalar_kinds,
            'integer': 'integer' in present_scalar_kinds,
            'float': 'float' in present_scalar_kinds,
        }

    def generate_class(self, structure_schema: Dict, parent_namespace: str, write_file: bool, explicit_name: str = '') -> str:
        """ Generates a Rust struct from JSON Structure object type """
        # Get name and namespace
        class_name = pascal(explicit_name if explicit_name else structure_schema.get('name', 'UnnamedClass'))
        schema_namespace = structure_schema.get('namespace', parent_namespace)
        namespace = self.sanitize_namespace(schema_namespace.lower())
        
        qualified_struct_name = self.safe_package(self.concat_package(namespace, class_name))
        if qualified_struct_name in self.generated_types_rust_package:
            return qualified_struct_name

        # Check if this is an abstract type
        is_abstract = structure_schema.get('abstract', False)

        # Handle inheritance ($extends)
        base_class = None
        if '$extends' in structure_schema:
            base_ref = structure_schema['$extends']
            if isinstance(self.schema_doc, dict):
                base_schema = self.resolve_ref(base_ref, self.schema_doc)
                if base_schema:
                    ref_path = base_ref.split('/')
                    base_name = ref_path[-1]
                    ref_namespace = '.'.join(ref_path[2:-1]) if len(ref_path) > 3 else parent_namespace
                    base_class = self.generate_class(base_schema, ref_namespace, write_file=True, explicit_name=base_name)

        # Generate properties
        properties = structure_schema.get('properties', {})
        required_props = structure_schema.get('required', [])

        fields = []
        for prop_name, prop_schema in properties.items():
            # Skip const fields for now (they would need to be class constants)
            if 'const' in prop_schema:
                continue
            
            json_name = json_wire_name(prop_name, prop_schema)
            xml_name = xml_wire_name(prop_name, prop_schema)
            original_field_name = xml_name if self.xml_annotation else json_name
            field_name = self.safe_identifier(snake(prop_name))
            
            # Determine if required
            is_required = prop_name in required_props if not isinstance(required_props, list) or \
                         len(required_props) == 0 or not isinstance(required_props[0], list) else \
                         any(prop_name in req_set for req_set in required_props)
            
            # Get property type
            prop_type = self.convert_structure_type_to_rust(class_name, field_name, prop_schema, schema_namespace, not is_required)
            
            # Add Option wrapper if not required and doesn't already have it
            if not is_required and not prop_type.startswith('Option<'):
                prop_type = f'Option<{prop_type}>'
            
            xml_kind = prop_schema.get('xmlkind', 'element')
            serde_name = f"@{xml_name}" if self.xml_annotation and xml_kind == 'attribute' else original_field_name
            serde_rename = field_name != serde_name
            
            # Get source type - handle nullable unions like ["int64", "null"]
            raw_type = prop_schema.get('type', 'string')
            if isinstance(raw_type, str):
                source_type = raw_type
            elif isinstance(raw_type, list):
                non_null_types = [t for t in raw_type if t != 'null']
                source_type = non_null_types[0] if len(non_null_types) == 1 and isinstance(non_null_types[0], str) else 'object'
            else:
                source_type = 'object'
            is_generated_type = prop_type in self.generated_types_rust_package or '::' in prop_type
            fields.append({
                'original_name': original_field_name,
                'json_name': json_name,
                'serde_name': serde_name,
                'serde_alias': json_name if self.serde_annotation and self.xml_annotation and json_name != serde_name else '',
                'xml_name': xml_name,
                'xml_kind': xml_kind,
                'name': field_name,
                'type': prop_type,
                'is_optional': prop_type.startswith('Option<'),
                'source_type': source_type,
                'serde_rename': serde_rename,
                'is_generated_type': is_generated_type,
                'random_value': self.generate_random_value(prop_type)
            })

        # Get docstring
        doc = structure_schema.get('description', structure_schema.get('doc', class_name))
        (
            descendant_elements,
            descendant_attributes,
            descendant_maps,
            descendant_relationships,
            element_namespaces,
            attribute_owners,
        ) = self.collect_xml_field_metadata(structure_schema)

        # Prepare context for template
        context = {
            'serde_annotation': self.serde_annotation,
            'xml_annotation': self.xml_annotation,
            'doc': doc,
            'struct_name': self.safe_identifier(class_name),
            'xml_name': xml_wire_name(explicit_name if explicit_name else structure_schema.get('name', 'UnnamedClass'), structure_schema),
            'xml_namespace': structure_schema.get('xmlns', ''),
            'xml_descendant_elements': sorted(descendant_elements),
            'xml_descendant_attributes': sorted(descendant_attributes),
            'xml_descendant_maps': sorted(descendant_maps),
            'xml_descendant_relationships': sorted(descendant_relationships),
            'xml_element_namespaces': sorted(element_namespaces),
            'xml_attribute_owners': sorted(attribute_owners),
            'fields': fields,
            'is_abstract': is_abstract,
        }

        file_name = self.to_file_name(qualified_struct_name)
        target_file = os.path.join(self.output_dir, "src", file_name + ".rs")
        render_template('structuretorust/dataclass_struct.rs.jinja', target_file, **context)
        self.write_mod_rs(namespace)

        self.generated_types_rust_package[qualified_struct_name] = "struct"
        self.generated_structure_types[qualified_struct_name] = structure_schema

        return qualified_struct_name

    def generate_enum(self, structure_schema: Dict, field_name: str, parent_namespace: str, write_file: bool) -> str:
        """ Generates a Rust enum from JSON Structure enum keyword """
        enum_values = structure_schema.get('enum', [])
        if not enum_values:
            return 'serde_json::Value'

        # Determine enum name from field name
        enum_name = pascal(structure_schema.get('name', field_name + 'Enum'))
        schema_namespace = structure_schema.get('namespace', parent_namespace)
        namespace = self.sanitize_namespace(schema_namespace.lower())
        
        qualified_enum_name = self.safe_package(self.concat_package(namespace, enum_name))
        if qualified_enum_name in self.generated_types_rust_package:
            return qualified_enum_name

        # Convert enum values to valid Rust identifiers
        symbols = []
        for value in enum_values:
            wire = xml_enum_wire_value(value, structure_schema) if self.xml_annotation else json_enum_wire_value(value, structure_schema)
            if isinstance(value, str):
                symbol = pascal(value.replace('-', '_').replace(' ', '_'))
                json_wire = json_enum_wire_value(value, structure_schema)
                symbols.append({
                    'name': symbol,
                    'value': wire,
                    'json_value': json_wire if symbol != json_wire else snake(symbol),
                })
            else:
                symbols.append({
                    'name': f"Value{value}",
                    'value': str(value),
                    'json_value': json_enum_wire_value(value, structure_schema),
                })

        doc = structure_schema.get('description', structure_schema.get('doc', enum_name))

        context = {
            'serde_annotation': self.serde_annotation,
            'xml_annotation': self.xml_annotation,
            'enum_name': self.safe_identifier(enum_name),
            'xml_name': xml_wire_name(structure_schema.get('name', field_name + 'Enum'), structure_schema),
            'symbols': symbols,
            'doc': doc,
        }

        file_name = self.to_file_name(qualified_enum_name)
        target_file = os.path.join(self.output_dir, "src", file_name + ".rs")
        render_template('structuretorust/dataclass_enum.rs.jinja', target_file, **context)
        self.write_mod_rs(namespace)

        self.generated_types_rust_package[qualified_enum_name] = "enum"
        self.generated_structure_types[qualified_enum_name] = structure_schema

        return qualified_enum_name

    def generate_choice(self, structure_schema: Dict, parent_namespace: str, write_file: bool, explicit_name: str = '') -> str:
        """ Generates a discriminated union (choice) type """
        choice_name = explicit_name if explicit_name else structure_schema.get('name', 'UnnamedChoice')
        schema_namespace = structure_schema.get('namespace', parent_namespace)
        namespace = self.sanitize_namespace(schema_namespace.lower())
        
        qualified_name = self.safe_package(self.concat_package(namespace, pascal(choice_name)))
        if qualified_name in self.generated_types_rust_package:
            return qualified_name

        choices = structure_schema.get('choices', {})
        
        # Generate types for each choice
        choice_types = []
        for choice_key, choice_schema in choices.items():
            if isinstance(choice_schema, dict):
                if '$ref' in choice_schema:
                    # Resolve reference and generate the type
                    ref_schema = self.resolve_ref(choice_schema['$ref'], self.schema_doc if isinstance(self.schema_doc, dict) else None)
                    if ref_schema:
                        ref_path = choice_schema['$ref'].split('/')
                        ref_name = ref_path[-1]
                        ref_namespace = '.'.join(ref_path[2:-1]) if len(ref_path) > 3 else parent_namespace
                        type_ref = self.generate_class(ref_schema, ref_namespace, write_file=True, explicit_name=ref_name)
                        type_name = type_ref.split('::')[-1]
                        choice_types.append({
                            'variant_name': pascal(choice_key),
                            'type': type_name,
                            'tag': choice_key,
                            'random_value': self.generate_random_value(type_ref),
                        })
                elif 'type' in choice_schema:
                    # Generate inline type
                    rust_type = self.convert_structure_type_to_rust(choice_name, choice_key, choice_schema, schema_namespace)
                    choice_types.append({
                        'variant_name': pascal(choice_key),
                        'type': rust_type,
                        'tag': choice_key,
                        'random_value': self.generate_random_value(rust_type),
                    })

        doc = structure_schema.get('description', structure_schema.get('doc', choice_name))
        xml_string_guards = self.add_xml_union_metadata(choice_types)

        context = {
            'serde_annotation': self.serde_annotation,
            'xml_annotation': self.xml_annotation,
            'union_enum_name': self.safe_identifier(pascal(choice_name)),
            'variants': choice_types,
            'xml_string_guards': xml_string_guards,
            'doc': doc,
        }

        file_name = self.to_file_name(qualified_name)
        target_file = os.path.join(self.output_dir, "src", file_name + ".rs")
        render_template('structuretorust/dataclass_union.rs.jinja', target_file, **context)
        self.write_mod_rs(namespace)

        self.generated_types_rust_package[qualified_name] = "choice"
        self.generated_structure_types[qualified_name] = structure_schema

        return qualified_name

    def generate_tuple(self, structure_schema: Dict, parent_namespace: str, write_file: bool, explicit_name: str = '') -> str:
        """ Generates a Rust tuple type from JSON Structure tuple """
        # For tuples, we generate a struct with numbered fields
        tuple_name = explicit_name if explicit_name else structure_schema.get('name', 'UnnamedTuple')
        return self.generate_class(structure_schema, parent_namespace, write_file, explicit_name=tuple_name)

    def generate_union_enum(self, field_name: str, structure_type: List, namespace: str) -> str:
        """Generates a union enum for Rust"""
        ns = self.sanitize_namespace(namespace.replace('.', '::').lower())
        union_enum_name = pascal(field_name) + 'Union'
        
        non_null_types = [t for t in structure_type if t != 'null']
        has_null = 'null' in structure_type
        
        union_types = []
        for i, t in enumerate(non_null_types):
            type_name = self.convert_structure_type_to_rust(field_name + "Option" + str(i), field_name + "Option" + str(i), t, namespace)
            variant_name = pascal(type_name.rsplit('::',1)[-1].replace('<', '').replace('>', '').replace(',', ''))
            union_types.append({
                'variant_name': variant_name,
                'type': type_name,
                'random_value': self.generate_random_value(type_name),
            })
        xml_string_guards = self.add_xml_union_metadata(union_types)
        
        qualified_union_enum_name = self.safe_package(self.concat_package(ns, union_enum_name))
        
        context = {
            'serde_annotation': self.serde_annotation,
            'xml_annotation': self.xml_annotation,
            'union_enum_name': union_enum_name,
            'variants': union_types,
            'xml_string_guards': xml_string_guards,
            'doc': f'Union type for {field_name}',
        }

        file_name = self.to_file_name(qualified_union_enum_name)
        target_file = os.path.join(self.output_dir, "src", file_name + ".rs").lower()
        render_template('structuretorust/dataclass_union.rs.jinja', target_file, **context)
        self.generated_types_rust_package[qualified_union_enum_name] = "union"
        self.write_mod_rs(namespace)

        return qualified_union_enum_name

    def to_file_name(self, qualified_name):
        """Converts a qualified union enum name to a file name"""
        if qualified_name.startswith('crate::'):
            qualified_name = qualified_name[(len('crate::')):]
        qualified_name = qualified_name.replace('r#', '')
        return qualified_name.rsplit('::',1)[0].replace('::', os.sep).lower()

    def generate_random_value(self, rust_type: str) -> str:
        """Generates a random value for a given Rust type"""
        if rust_type == 'String' or rust_type == 'Option<String>':
            return 'format!("random_string_{}", rand::Rng::gen::<u32>(&mut rng))'
        elif rust_type == 'bool' or rust_type == 'Option<bool>':
            return 'rand::Rng::gen::<bool>(&mut rng)'
        elif rust_type in ['i8', 'Option<i8>']:
            return 'rand::Rng::gen_range(&mut rng, 1..101) as i8'
        elif rust_type in ['u8', 'Option<u8>']:
            return 'rand::Rng::gen_range(&mut rng, 1..101) as u8'
        elif rust_type in ['i16', 'Option<i16>']:
            return 'rand::Rng::gen_range(&mut rng, 1..101) as i16'
        elif rust_type in ['u16', 'Option<u16>']:
            return 'rand::Rng::gen_range(&mut rng, 1..101) as u16'
        elif rust_type in ['i32', 'Option<i32>']:
            return 'rand::Rng::gen_range(&mut rng, 1..101)'
        elif rust_type in ['u32', 'Option<u32>']:
            return 'rand::Rng::gen_range(&mut rng, 1..101) as u32'
        elif rust_type in ['i64', 'Option<i64>']:
            return 'rand::Rng::gen_range(&mut rng, 1..101) as i64'
        elif rust_type in ['u64', 'Option<u64>']:
            return 'rand::Rng::gen_range(&mut rng, 1..101) as u64'
        elif rust_type in ['i128', 'Option<i128>']:
            return 'rand::Rng::gen_range(&mut rng, 1..101) as i128'
        elif rust_type in ['u128', 'Option<u128>']:
            return 'rand::Rng::gen_range(&mut rng, 1..101) as u128'
        elif rust_type in ['f32', 'Option<f32>']:
            return '(rand::Rng::gen::<f32>(&mut rng)*1000.0).round()/1000.0'
        elif rust_type in ['f64', 'Option<f64>']:
            return '(rand::Rng::gen::<f64>(&mut rng)*1000.0).round()/1000.0'
        elif rust_type == 'Vec<u8>' or rust_type == 'Option<Vec<u8>>':
            return 'vec![rand::Rng::gen::<u8>(&mut rng); 10]'
        elif rust_type == 'chrono::NaiveDate' or rust_type == 'Option<chrono::NaiveDate>':
            return 'chrono::NaiveDate::from_ymd_opt(rand::Rng::gen_range(&mut rng, 2000..2023), rand::Rng::gen_range(&mut rng, 1..13), rand::Rng::gen_range(&mut rng, 1..29)).unwrap()'
        elif rust_type == 'chrono::NaiveTime' or rust_type == 'Option<chrono::NaiveTime>':
            return 'chrono::NaiveTime::from_hms_opt(rand::Rng::gen_range(&mut rng, 0..24),rand::Rng::gen_range(&mut rng, 0..60), rand::Rng::gen_range(&mut rng, 0..60)).unwrap()'
        elif rust_type == 'chrono::DateTime<chrono::Utc>' or rust_type == 'Option<chrono::DateTime<chrono::Utc>>':
            return 'chrono::Utc::now()'
        elif rust_type == 'chrono::Duration' or rust_type == 'Option<chrono::Duration>':
            return 'chrono::Duration::seconds(rand::Rng::gen_range(&mut rng, 0..86400))'
        elif rust_type == 'uuid::Uuid' or rust_type == 'Option<uuid::Uuid>':
            return 'uuid::Uuid::new_v4()'
        elif rust_type.startswith('std::collections::HashMap<String, '):
            inner_type = rust_type.split(', ')[1][:-1]
            return f'(0..3).map(|_| (format!("key_{{}}", rand::Rng::gen::<u32>(&mut rng)), {self.generate_random_value(inner_type)})).collect()'
        elif rust_type.startswith('std::collections::HashSet<'):
            inner_type = rust_type[27:-1]
            return f'(0..3).map(|_| {self.generate_random_value(inner_type)}).collect()'
        elif rust_type.startswith('Vec<'):
            inner_type = rust_type[4:-1]
            return f'(0..3).map(|_| {self.generate_random_value(inner_type)}).collect()'
        elif rust_type in self.generated_types_rust_package:
            return f'{rust_type}::generate_random_instance()'
        else:
            return 'Default::default()'

    def write_mod_rs(self, namespace: str):
        """Writes the mod.rs file for a Rust module"""
        # Sanitize namespace to replace dots with underscores
        sanitized_namespace = self.sanitize_namespace(namespace)
        directories = sanitized_namespace.split('.')
        for i in range(len(directories)):
            sub_package = '::'.join(directories[:i + 1])
            directory_path = os.path.join(
                self.output_dir, "src", sub_package.replace('.', os.sep).replace('::', os.sep))
            if not os.path.exists(directory_path):
                os.makedirs(directory_path, exist_ok=True)
            mod_rs_path = os.path.join(directory_path, "mod.rs")
            
            types = [file.replace('.rs', '') for file in os.listdir(directory_path) if file.endswith('.rs') and file != "mod.rs"]
            mod_statements = '\n'.join(f'pub mod {self.escaped_identifier(typ.lower())};' for typ in types)
            mods = [dir for dir in os.listdir(directory_path) if os.path.isdir(os.path.join(directory_path, dir))]
            mod_statements += '\n' + '\n'.join(f'pub mod {self.escaped_identifier(mod.lower())};' for mod in mods)

            with open(mod_rs_path, 'w', encoding='utf-8') as file:
                file.write(mod_statements)

    def write_cargo_toml(self):
        """Writes the Cargo.toml file for the Rust project"""
        dependencies = []
        if self.serde_annotation or self.xml_annotation:
            dependencies.append('serde = { version = "1.0", features = ["derive"] }')
            dependencies.append('serde_json = "1.0"')
        if self.xml_annotation:
            dependencies.append('quick-xml = { version = "0.38", features = ["serialize"] }')
        dependencies.append('chrono = { version = "0.4", features = ["serde"] }')
        dependencies.append('uuid = { version = "1.11", features = ["serde", "v4"] }')
        dependencies.append('flate2 = "1.0"')
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
        modules = {name[(len('crate::')):].split('::')[0].replace('.', '_') for name in self.generated_types_rust_package if name.startswith('crate::')}
        mod_statements = '\n'.join(f'pub mod {self.escaped_identifier(module)};' for module in sorted(modules))
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

    def process_definitions(self, definitions: Dict, namespace_path: str) -> None:
        """ Processes the definitions section recursively """
        for name, definition in definitions.items():
            if isinstance(definition, dict):
                if 'type' in definition:
                    # This is a type definition
                    current_namespace = self.concat_namespace(namespace_path, '')
                    check_namespace = self.sanitize_namespace(current_namespace.lower())
                    check_name = pascal(name)
                    check_ref = self.safe_package(self.concat_package(check_namespace, check_name))
                    if check_ref not in self.generated_types_rust_package:
                        self.generate_class_or_choice(definition, current_namespace, write_file=True, explicit_name=name)
                else:
                    # This is a namespace
                    new_namespace = self.concat_namespace(namespace_path, name)
                    self.process_definitions(definition, new_namespace)

    def convert_schema(self, schema: JsonNode, output_dir: str):
        """Converts JSON Structure schema to Rust"""
        if not isinstance(schema, list):
            schema = [schema]
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        self.output_dir = output_dir

        # Register all schema IDs first
        for structure_schema in (s for s in schema if isinstance(s, dict)):
            self.register_schema_ids(structure_schema)

        # Process each schema
        for structure_schema in (s for s in schema if isinstance(s, dict)):
            self.schema_doc = structure_schema
            
            # Store definitions for later use
            if 'definitions' in structure_schema:
                self.definitions = structure_schema['definitions']

            # Process root type FIRST
            if 'type' in structure_schema:
                self.generate_class_or_choice(structure_schema, '', write_file=True)
            elif '$root' in structure_schema:
                root_ref = structure_schema['$root']
                root_schema = self.resolve_ref(root_ref, structure_schema)
                if root_schema:
                    ref_path = root_ref.split('/')
                    type_name = ref_path[-1]
                    ref_namespace = '.'.join(ref_path[2:-1]) if len(ref_path) > 3 else ''
                    self.generate_class_or_choice(root_schema, ref_namespace, write_file=True, explicit_name=type_name)

            # Process definitions
            if 'definitions' in structure_schema:
                self.process_definitions(self.definitions, '')

        self.write_cargo_toml()
        self.write_xml_support_rs()
        self.write_lib_rs()

    def convert(self, structure_schema_path: str, output_dir: str):
        """Converts JSON Structure schema to Rust"""
        with open(structure_schema_path, 'r', encoding='utf-8') as file:
            schema = json.load(file)
        self.convert_schema(schema, output_dir)


def convert_structure_to_rust(structure_schema_path: str, rust_file_path: str, package_name: str = '', serde_annotation: bool = False, xml_annotation: bool = False):
    """Converts JSON Structure schema to Rust structs

    Args:
        structure_schema_path (str): JSON Structure input schema path
        rust_file_path (str): Output Rust file path
        package_name (str): Base package name
        serde_annotation (bool): Include Serde annotations
        xml_annotation (bool): Include quick-xml compatible Serde annotations
    """
    if not package_name:
        package_name = os.path.splitext(os.path.basename(structure_schema_path))[0].lower().replace('-', '_')

    structtorust = StructureToRust()
    structtorust.base_package = package_name
    structtorust.serde_annotation = serde_annotation
    structtorust.xml_annotation = xml_annotation
    structtorust.convert(structure_schema_path, rust_file_path)


def convert_structure_schema_to_rust(structure_schema: JsonNode, output_dir: str, package_name: str = '', serde_annotation: bool = False, xml_annotation: bool = False):
    """Converts JSON Structure schema to Rust structs

    Args:
        structure_schema (JsonNode): JSON Structure schema as a dictionary or list of dictionaries
        output_dir (str): Output directory path
        package_name (str): Base package name
        serde_annotation (bool): Include Serde annotations
        xml_annotation (bool): Include quick-xml compatible Serde annotations
    """
    structtorust = StructureToRust()
    structtorust.base_package = package_name
    structtorust.serde_annotation = serde_annotation
    structtorust.xml_annotation = xml_annotation
    structtorust.convert_schema(structure_schema, output_dir)
