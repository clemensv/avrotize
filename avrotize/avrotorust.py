import json
import os
import hashlib
import re
from collections import defaultdict, deque
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


class JsonSignature:
    """A bounded graph describing generated JSON matcher or value shapes."""

    __slots__ = (
        'root',
        'nodes',
        'node_count',
        'edge_count',
        '_canonical',
        '_hash',
        'canonical_visit_count',
        'canonical_refinement_count',
    )

    def __init__(self, root: int, nodes: list[tuple]) -> None:
        self.root = root
        self.nodes = tuple(nodes)
        self.node_count = len(self.nodes)
        self.edge_count = sum(
            len(data)
            if kind in ('record', 'record_match', 'union')
            else int(data is not None)
            for kind, data in self.nodes
        )
        self.canonical_visit_count = 0
        self.canonical_refinement_count = 0
        self._canonical = self._canonicalize()
        self._hash = hash(self._canonical)

    def __hash__(self) -> int:
        return self._hash

    def __eq__(self, other) -> bool:
        if not isinstance(other, JsonSignature):
            return NotImplemented
        return self._canonical == other._canonical

    def __repr__(self) -> str:
        return repr(self._canonical)

    def _canonicalize(self):
        """Returns the coarsest structural quotient without recursive walks."""
        adjacency = []
        base_edge_labels = []
        reverse_graph = [[] for _ in self.nodes]
        for node_id, (kind, data) in enumerate(self.nodes):
            if data is None:
                children = ()
                edge_labels = ()
            elif kind in ('record', 'record_match'):
                children = tuple(child for _, child in data)
                edge_labels = tuple(
                    ('field', index, name)
                    for index, (name, _) in enumerate(data)
                )
            elif kind == 'union':
                children = data
                edge_labels = tuple(
                    ('branch', index)
                    for index in range(len(data))
                )
            else:
                children = (data,)
                edge_labels = (('value',),)
            adjacency.append(children)
            base_edge_labels.append(edge_labels)
            for child in children:
                reverse_graph[child].append(node_id)

        visited = set()
        finish_order = []
        for start in range(len(self.nodes)):
            if start in visited:
                continue
            visited.add(start)
            stack = [(start, 0)]
            while stack:
                node_id, child_index = stack[-1]
                children = adjacency[node_id]
                if child_index < len(children):
                    child = children[child_index]
                    stack[-1] = (node_id, child_index + 1)
                    if child not in visited:
                        visited.add(child)
                        stack.append((child, 0))
                else:
                    finish_order.append(node_id)
                    stack.pop()

        components = []
        component_of = [-1] * len(self.nodes)
        for start in reversed(finish_order):
            if component_of[start] != -1:
                continue
            component_id = len(components)
            component = []
            stack = [start]
            component_of[start] = component_id
            while stack:
                node_id = stack.pop()
                component.append(node_id)
                for predecessor in reverse_graph[node_id]:
                    if component_of[predecessor] == -1:
                        component_of[predecessor] = component_id
                        stack.append(predecessor)
            components.append(component)

        cyclic_components = [
            len(component) > 1
            or component[0] in adjacency[component[0]]
            for component in components
        ]
        reverse = [[] for _ in self.nodes]
        descriptors = []
        for node_id, ((kind, _), children, edge_labels) in enumerate(zip(
            self.nodes,
            adjacency,
            base_edge_labels,
        )):
            component_id = component_of[node_id]
            scoped_labels = tuple(
                edge_label + (
                    'internal'
                    if component_of[child] == component_id
                    else 'external',
                )
                for child, edge_label in zip(children, edge_labels)
            )
            descriptors.append((
                kind,
                cyclic_components[component_id],
                scoped_labels,
            ))
            for child, edge_label in zip(children, scoped_labels):
                reverse[child].append((node_id, edge_label))

        descriptor_blocks = {}
        blocks = []
        classes = [-1] * len(self.nodes)
        for node_id, descriptor in enumerate(descriptors):
            block_id = descriptor_blocks.get(descriptor)
            if block_id is None:
                block_id = len(blocks)
                descriptor_blocks[descriptor] = block_id
                blocks.append(set())
            blocks[block_id].add(node_id)
            classes[node_id] = block_id

        worklist = list(range(len(blocks)))
        queued = set(worklist)
        worklist_index = 0
        while worklist_index < len(worklist):
            splitter_id = worklist[worklist_index]
            worklist_index += 1
            queued.discard(splitter_id)
            splitter = tuple(blocks[splitter_id])
            predecessors_by_label = defaultdict(list)
            for target in splitter:
                self.canonical_refinement_count += len(reverse[target])
                for predecessor, edge_label in reverse[target]:
                    predecessors_by_label[edge_label].append(predecessor)

            for predecessors in predecessors_by_label.values():
                affected_blocks = defaultdict(list)
                for predecessor in predecessors:
                    affected_blocks[classes[predecessor]].append(predecessor)

                for block_id, inside_nodes in affected_blocks.items():
                    members = blocks[block_id]
                    if len(inside_nodes) == len(members):
                        continue
                    if len(inside_nodes) * 2 <= len(members):
                        smaller = set(inside_nodes)
                        members.difference_update(smaller)
                        larger = members
                        self.canonical_refinement_count += len(smaller)
                    else:
                        larger = set(inside_nodes)
                        smaller = members.difference(larger)
                        self.canonical_refinement_count += (
                            len(inside_nodes) + len(members)
                        )

                    blocks[block_id] = larger
                    new_block_id = len(blocks)
                    blocks.append(smaller)
                    for node_id in smaller:
                        classes[node_id] = new_block_id

                    queued.add(new_block_id)
                    worklist.append(new_block_id)

        representatives = {}
        for node_id, class_id in enumerate(classes):
            representatives.setdefault(class_id, node_id)
        root_class = classes[self.root]
        canonical_ids = {root_class: 0}
        pending = [root_class]
        definitions = []
        pending_index = 0
        while pending_index < len(pending):
            class_id = pending[pending_index]
            pending_index += 1
            self.canonical_visit_count += 1
            kind, data = self.nodes[representatives[class_id]]

            def canonical_id(child_id):
                child_class = classes[child_id]
                if child_class not in canonical_ids:
                    canonical_ids[child_class] = len(canonical_ids)
                    pending.append(child_class)
                return canonical_ids[child_class]

            if data is None:
                definition = (kind, None)
            elif kind in ('record', 'record_match'):
                definition = (
                    kind,
                    tuple(
                        (name, canonical_id(child_id))
                        for name, child_id in data
                    ),
                )
            elif kind == 'union':
                definition = (
                    kind,
                    tuple(canonical_id(child_id) for child_id in data),
                )
            else:
                definition = (kind, canonical_id(data))
            definitions.append(definition)
        return tuple(definitions)


class _JsonSignatureRef:
    """Identity cursor used while comparing nodes from signature graphs."""

    __slots__ = ('signature', 'node_id')

    def __init__(self, signature: JsonSignature, node_id: int) -> None:
        self.signature = signature
        self.node_id = node_id


class AvroToRust:
    """Converts Avro schema to Rust structs, including Serde and Avro marshalling methods"""

    def __init__(self, base_package: str = '') -> None:
        self.base_package = base_package.replace('.', '/').lower()
        self.output_dir = os.getcwd()
        self.avro_annotation = False
        self.serde_annotation = False
        self.xml_annotation = False
        self.reset_run_state()

    def reset_run_state(self):
        """Resets state that belongs to one conversion output."""
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
        self.generated_aliases_to_remove: List[
            tuple[str, str, tuple[str, ...]]
        ] = []
        self.planned_source_paths: set[tuple[str, ...]] = set()
        self.union_schema_targets: Dict[tuple, str] = {}
        self.union_targets_in_progress: set[tuple] = set()
        self.json_round_trip_safety = None
        
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
                    named_type = self.resolve_avro_named_type(
                        non_null_types[0],
                        namespace,
                    )
                    if named_type and named_type.get('type') in (
                        'record',
                        'enum',
                        'fixed',
                    ):
                        type_name = inner_type
                    else:
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
        self.json_round_trip_safety = None
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
                            if self.xml_type_contains_collection(
                                nested_type,
                                'map',
                            ):
                                if isinstance(nested_type, list):
                                    elements.add('entries')
                                    relationships.add((name, 'entries'))
                                    maps.add('entries')
                                else:
                                    maps.add(name)
                            if (
                                isinstance(nested_type, list)
                                and self.xml_type_contains_collection(
                                    nested_type,
                                    'array',
                                )
                            ):
                                elements.add('item')
                                relationships.add((name, 'item'))
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

    def xml_type_contains_collection(
        self,
        avro_type,
        collection_type: str,
    ) -> bool:
        """Checks direct union branches for an XML collection wire shape."""
        pending = [avro_type]
        while pending:
            node = pending.pop()
            if isinstance(node, list):
                pending.extend(node)
            elif isinstance(node, dict):
                node_type = node.get('type')
                if node_type == collection_type:
                    return True
                if isinstance(node_type, (dict, list)):
                    pending.append(node_type)
        return False

    def xml_union_representation(
        self,
        avro_type,
        rust_type: str,
        namespace: str,
    ) -> str:
        """Returns the XML wire wrapper used by a union variant."""
        if rust_type.startswith('Vec<'):
            return 'sequence'
        if rust_type.startswith('std::collections::HashMap<'):
            return 'map'
        resolved = (
            self.resolve_avro_named_type(avro_type, namespace)
            if isinstance(avro_type, str)
            else avro_type
        )
        if isinstance(resolved, dict) and resolved.get('type') == 'record':
            return 'record'
        return 'text'

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
                'xml_repeatable': (
                    not isinstance(field['type'], list)
                    and self.xml_type_contains_collection(
                        field['type'],
                        'array',
                    )
                ),
                'xml_map': (
                    not isinstance(field['type'], list)
                    and self.xml_type_contains_collection(
                        field['type'],
                        'map',
                    )
                ),
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
            'json_round_trip_safe': self.is_json_round_trip_safe(
                avro_schema,
                parent_namespace,
            ),
            'json_match_predicates': [
                self.get_is_json_match_clause(
                    (
                        field['serde_name'],
                        field['serde_alias'],
                    ),
                    field['type'],
                )
                for field in fields
            ],
            'json_value_match_predicates': [
                self.get_is_json_match_clause(
                    (
                        field['serde_name'],
                        field['serde_alias'],
                    ),
                    field['type'],
                    exact_nested=True,
                )
                for field in fields
            ],
            'xml_canonical_match_predicates': [
                self.get_is_xml_canonical_match_clause(field)
                for field in fields
            ],
            'xml_shape_predicates': [
                self.get_is_xml_shape_clause(field)
                for field in fields
            ],
            'xml_match_predicates': [
                self.get_is_xml_match_clause(
                    (
                        field['serde_name'],
                        field['serde_alias'],
                    ),
                    field['type'],
                )
                for field in fields
            ],
            'legacy_xml_shape_predicates': [
                self.get_legacy_is_xml_shape_clause(field)
                for field in fields
            ],
            'legacy_xml_match_predicates': [
                self.get_legacy_is_xml_match_clause(
                    field['serde_name'],
                    field['type'],
                )
                for field in fields
            ],
        }

        file_name = self.to_file_name(qualified_struct_name)
        target_file = self.output_path("src", file_name + ".rs")
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
                return (
                    f'({value_expression}).to_avro_source_value()?'
                )

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
                return (
                    f'{union_type}::from_avro_source_value('
                    f'{value_expression}, depth + 1)?'
                )

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
                return (
                    f'{rust_type}::from_avro_value_at('
                    f'{value_expression}, depth + 1)?'
                )
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
    
    @staticmethod
    def _rust_inner_type(rust_type: str, prefix: str) -> str | None:
        """Returns the inner type when `rust_type` has the requested wrapper."""
        if rust_type.startswith(prefix) and rust_type.endswith('>'):
            return rust_type[len(prefix):-1]
        return None

    def get_value_match_expression(
        self,
        reference: str,
        field_type: str,
        xml: bool = False,
        canonical_xml: bool = False,
        exact_nested: bool = False,
    ) -> str:
        """Builds an allocation-free exact predicate for a generated Rust type."""
        optional_type = self._rust_inner_type(field_type, 'Option<')
        if optional_type is not None:
            inner = self.get_value_match_expression(
                reference,
                optional_type,
                xml,
                canonical_xml,
                exact_nested,
            )
            null_test = (
                f'{reference}.is_xml_null()'
                if xml else f'{reference}.is_null()'
            )
            return f'({null_test} || {inner})'

        if field_type == 'serde_json::Value':
            return 'true'

        if xml:
            if field_type == 'String':
                return f'{reference}.is_xml_text()'
            if field_type == 'bool':
                return f'{reference}.matches_xml_bool()'
            if field_type in {
                'i8', 'i16', 'i32', 'i64', 'i128', 'isize',
                'u8', 'u16', 'u32', 'u64', 'u128', 'usize',
                'f32', 'f64',
            }:
                return (
                    f'{reference}.matches_xml_number::<{field_type}>()'
                )
            if field_type == '()':
                return f'{reference}.is_xml_unit()'
            if (
                'chrono::' in field_type
                or 'uuid::Uuid' in field_type
                or field_type == 'Uuid'
            ):
                return (
                    f'{reference}.matches_xml_from_str::<{field_type}>()'
                )
            vector_type = self._rust_inner_type(field_type, 'Vec<')
            if vector_type is not None:
                item_match = self.get_value_match_expression(
                    'item',
                    vector_type,
                    True,
                    canonical_xml,
                    exact_nested,
                )
                return (
                    f'{reference}.xml_sequence_matches('
                    f'|item| {item_match})'
                )
            map_type = self._rust_inner_type(
                field_type,
                'std::collections::HashMap<String, ',
            )
            if map_type is not None:
                value_match = self.get_value_match_expression(
                    'value',
                    map_type,
                    True,
                    canonical_xml,
                    exact_nested,
                )
                return (
                    f'{reference}.xml_map_matches('
                    f'|value| {value_match})'
                )
            method = (
                'is_xml_value_canonical_match'
                if canonical_xml
                else 'is_xml_value_match'
            )
            return f'{field_type}::{method}({reference})'

        if field_type == 'String':
            return f'{reference}.is_string()'
        if field_type == 'bool':
            return f'{reference}.is_boolean()'
        if field_type in {'i8', 'i16', 'i32', 'isize'}:
            return (
                f'{reference}.as_i64().map_or(false, '
                f'|value| {field_type}::try_from(value).is_ok())'
            )
        if field_type == 'i64':
            return f'{reference}.as_i64().is_some()'
        if field_type in {'u8', 'u16', 'u32', 'usize'}:
            return (
                f'{reference}.as_u64().map_or(false, '
                f'|value| {field_type}::try_from(value).is_ok())'
            )
        if field_type == 'u64':
            return f'{reference}.as_u64().is_some()'
        if field_type == 'f32':
            return f'{reference}.as_f64().is_some()'
        if field_type == 'f64':
            return f'{reference}.as_f64().is_some()'
        if field_type == '()':
            return f'{reference}.is_null()'
        if (
            'chrono::' in field_type
            or 'uuid::Uuid' in field_type
            or field_type == 'Uuid'
        ):
            return (
                f'{reference}.as_str().map_or(false, '
                f'|value| value.parse::<{field_type}>().is_ok())'
            )
        vector_type = self._rust_inner_type(field_type, 'Vec<')
        if vector_type is not None:
            item_match = self.get_value_match_expression(
                'item',
                vector_type,
                exact_nested=exact_nested,
            )
            return (
                f'{reference}.as_array().map_or(false, '
                f'|items| items.iter().all(|item| {item_match}))'
            )
        map_type = self._rust_inner_type(
            field_type,
            'std::collections::HashMap<String, ',
        )
        if map_type is not None:
            value_match = self.get_value_match_expression(
                'value',
                map_type,
                exact_nested=exact_nested,
            )
            return (
                f'{reference}.as_object().map_or(false, '
                f'|values| values.values().all(|value| {value_match}))'
            )
        generated_kind = self.generated_types_rust_package.get(field_type)
        method = (
            'is_json_value_match'
            if exact_nested and generated_kind in {'union', 'struct', 'enum'}
            else 'is_json_match'
        )
        return f'{field_type}::{method}({reference})'

    def get_is_json_match_clause(
        self,
        field_name: str | tuple[str, ...],
        field_type: str,
        for_union=False,
        exact_nested=False,
    ) -> str:
        """Generates an exact borrowed JSON match clause for a field."""
        if for_union:
            return self.get_value_match_expression(
                'node',
                field_type,
                exact_nested=exact_nested,
            )

        field_names = (
            field_name if isinstance(field_name, tuple) else (field_name,)
        )
        lookups = [
            f'node.get("{name}")'
            for name in dict.fromkeys(field_names)
            if name
        ]
        value_match = self.get_value_match_expression(
            'value',
            field_type,
            exact_nested=exact_nested,
        )
        missing_matches = field_type.startswith('Option<')
        lookup = '.or_else(|| '.join(lookups) + ')' * (len(lookups) - 1)
        duplicate_guard = ' + '.join(
            f'usize::from(node.get("{name}").is_some())'
            for name in dict.fromkeys(field_names)
            if name
        )
        return (
            f'(({duplicate_guard}) <= 1 && {lookup}.map_or('
            f'{str(missing_matches).lower()}, |value| {value_match}))'
        )

    def get_is_xml_canonical_match_clause(self, field) -> str:
        """Matches only canonical XML wire names and enum spellings."""
        canonical_name = field['serde_name']
        alias_name = field['serde_alias']
        value_match = self.get_value_match_expression(
            'value',
            field['type'],
            True,
            True,
        )
        missing_matches = field['type'].startswith('Option<')
        alias_guard = (
            f'!node.contains_key("{alias_name}") && '
            if alias_name and alias_name != canonical_name else ''
        )
        return (
            f'({alias_guard}node.get("{canonical_name}").map_or('
            f'{str(missing_matches).lower()}, |value| {value_match}))'
        )

    def get_is_xml_match_clause(
        self,
        field_name: str | tuple[str, ...],
        field_type: str,
        for_union=False,
    ) -> str:
        """Generates an exact borrowed XML match clause for a field."""
        if for_union:
            return self.get_value_match_expression('node', field_type, True)

        field_names = (
            field_name if isinstance(field_name, tuple) else (field_name,)
        )
        lookups = [
            f'node.get("{name}")'
            for name in dict.fromkeys(field_names)
            if name
        ]
        value_match = self.get_value_match_expression(
            'value',
            field_type,
            True,
        )
        missing_matches = field_type.startswith('Option<')
        lookup = '.or_else(|| '.join(lookups) + ')' * (len(lookups) - 1)
        duplicate_guard = ' + '.join(
            f'usize::from(node.contains_key("{name}"))'
            for name in dict.fromkeys(field_names)
            if name
        )
        return (
            f'(({duplicate_guard}) <= 1 && {lookup}.map_or('
            f'{str(missing_matches).lower()}, |value| {value_match}))'
        )

    def get_legacy_is_xml_match_clause(
        self,
        field_name: str,
        field_type: str,
        for_union=False,
    ) -> str:
        """Preserves the public normalized-JSON XML predicate API."""
        reference = 'node' if for_union else f'node["{field_name}"]'
        optional_type = self._rust_inner_type(field_type, 'Option<')
        base_type = optional_type or field_type
        null_check = f' || {reference}.is_null()' if optional_type else ''
        if base_type == 'serde_json::Value':
            return 'true'
        if base_type == 'String':
            return f'({reference}.is_string(){null_check})'
        if base_type == 'bool':
            return f'({reference}.is_boolean(){null_check})'
        if base_type in {
            'i8', 'i16', 'i32', 'i64', 'isize',
            'u8', 'u16', 'u32', 'u64', 'usize',
        }:
            return f'({reference}.is_i64(){null_check})'
        if base_type in {'f32', 'f64'}:
            return f'({reference}.is_f64(){null_check})'
        if base_type == 'Vec<u8>' or base_type.startswith('Vec<'):
            return f'({reference}.is_array(){null_check})'
        if base_type == '()':
            return f'({reference}.is_null(){null_check})'
        if base_type.startswith('std::collections::HashMap<String, '):
            return f'({reference}.is_object(){null_check})'
        if 'chrono::' in base_type:
            return (
                f'({reference}.is_string() || '
                f'{reference}.is_i64(){null_check})'
            )
        if 'uuid::Uuid' in base_type or base_type == 'Uuid':
            return f'({reference}.is_string(){null_check})'
        if optional_type:
            return (
                f'({base_type}::is_xml_match(&{reference})'
                f' || {reference}.is_null())'
            )
        return f'{base_type}::is_xml_match(&{reference})'

    @staticmethod
    def get_legacy_is_xml_shape_clause(field) -> str:
        """Preserves public normalized-JSON XML shape predicates."""
        reference = f'node.get("{field["serde_name"]}")'
        base_type = (
            field['type'][7:-1]
            if field['type'].startswith('Option<')
            else field['type']
        )
        primitive = (
            base_type in {
                'String', 'bool', 'i8', 'i16', 'i32', 'i64',
                'u8', 'u16', 'u32', 'u64', 'isize', 'usize',
                'f32', 'f64', 'Vec<u8>', '()', 'serde_json::Value',
            }
            or base_type.startswith('Vec<')
            or base_type.startswith('std::collections::HashMap<')
            or 'chrono::' in base_type
            or 'uuid::Uuid' in base_type
        )
        if field['is_optional']:
            if primitive:
                return 'true'
            return (
                f'{reference}.map_or(true, |value| '
                f'{base_type}::is_xml_shape(value))'
            )
        if primitive:
            return f'{reference}.is_some()'
        return (
            f'{reference}.map_or(false, |value| '
            f'{base_type}::is_xml_shape(value))'
        )

    @staticmethod
    def get_is_xml_shape_clause(field) -> str:
        """Checks XML wire keys without reclassifying concrete values."""
        field_names = (field['serde_name'],)
        lookup = '.or_else(|| '.join(
            f'node.get("{name}")' for name in field_names
        ) + ')' * (len(field_names) - 1)
        duplicate_guard = ' + '.join(
            f'usize::from(node.contains_key("{name}"))'
            for name in field_names
        )
        base_type = (
            field['type'][7:-1]
            if field['type'].startswith('Option<')
            else field['type']
        )
        primitive = (
            base_type in {
                'String',
                'bool',
                'i8',
                'i16',
                'i32',
                'i64',
                'u8',
                'u16',
                'u32',
                'u64',
                'isize',
                'usize',
                'f32',
                'f64',
                'Vec<u8>',
                '()',
                'serde_json::Value',
            }
            or base_type.startswith('Vec<')
            or base_type.startswith('std::collections::HashMap<')
            or 'chrono::' in base_type
            or 'uuid::Uuid' in base_type
        )
        if field['is_optional']:
            if primitive:
                return f'({duplicate_guard}) <= 1'
            return (
                f'(({duplicate_guard}) <= 1 && '
                f'{lookup}.map_or(true, |value| '
                f'{base_type}::is_xml_value_shape(value))'
                f')'
            )
        if primitive:
            return f'(({duplicate_guard}) == 1)'
        return (
            f'(({duplicate_guard}) == 1 && '
            f'{lookup}.map_or(false, |value| '
            f'{base_type}::is_xml_value_shape(value))'
            f')'
        )


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
        target_file = self.output_path("src", file_name + ".rs")
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
        structural_identity = json.dumps(
            self.inline_avro_references(union_avro_types, namespace),
            sort_keys=True,
        )
        source_indexes = tuple(
            source_index for source_index, _ in union_avro_branches
        )
        source_null_index = (
            avro_type.index('null')
            if self.avro_annotation and 'null' in avro_type else -1
        )
        target_key = (
            namespace.lower(),
            structural_identity,
            source_indexes,
            source_null_index,
        )
        existing_target = self.union_schema_targets.get(target_key)
        if existing_target is None:
            existing_target = self.safe_package(
                self.concat_package(ns, union_enum_name)
            )
            self.union_schema_targets[target_key] = existing_target
            is_new_target = True
        else:
            is_new_target = False
            legacy_name = pascal(field_name) + 'Union'
            self.union_alias_candidates.setdefault(
                (namespace.lower(), legacy_name),
                [],
            ).append(existing_target)
            if target_key in self.union_targets_in_progress:
                return existing_target

        self.union_targets_in_progress.add(target_key)
        union_types = [
            self.convert_avro_type_to_rust(
                field_name + "Option" + str(source_index),
                avro_branch,
                namespace,
                path=(path or []) + [('branch', str(source_index))],
            )
            for _, (source_index, avro_branch) in enumerate(
                union_avro_branches
            )
        ]
        self.union_targets_in_progress.remove(target_key)
        if not is_new_target:
            return existing_target
        avro_schema_str = json.dumps(
            self.inline_avro_references(union_avro_types, namespace)
        )
        avro_schema_str = avro_schema_str.replace('"', '§')
        avro_schema_str = f"\",\n{INDENT*2}\"".join(
            [avro_schema_str[i:i+80] for i in range(0, len(avro_schema_str), 80)])
        avro_schema_str = avro_schema_str.replace('§', '\\"')
        avro_schema_str = f"concat!(\"{avro_schema_str}\")"
        source_avro_schema_str = json.dumps(
            self.inline_avro_references(avro_type, namespace)
        )
        source_avro_schema_str = source_avro_schema_str.replace('"', '§')
        source_avro_schema_str = f"\",\n{INDENT*2}\"".join(
            [
                source_avro_schema_str[i:i+80]
                for i in range(0, len(source_avro_schema_str), 80)
            ]
        )
        source_avro_schema_str = source_avro_schema_str.replace('§', '\\"')
        source_avro_schema_str = f"concat!(\"{source_avro_schema_str}\")"
        
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
            value_predicate = self.get_is_json_match_clause(
                field_name,
                t,
                for_union=True,
                exact_nested=True,
            )
            xml_predicate = self.get_is_xml_match_clause(
                field_name,
                t,
                for_union=True,
            )
            canonical_xml_predicate = self.get_value_match_expression(
                'node',
                t,
                True,
                True,
            )
            legacy_xml_predicate = self.get_legacy_is_xml_match_clause(
                field_name,
                t,
                for_union=True,
            )
            predicate_key = self.get_json_match_signature(
                union_avro_types[i],
                namespace,
            )
            shape_signature = self.get_json_shape_signature(
                union_avro_types[i],
                namespace,
            )
            default_shape_signature = self.get_json_default_shape_signature(
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
            
            xml_representation = self.xml_union_representation(
                union_avro_types[i],
                t,
                namespace,
            )
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
                'json_value_match_predicate': value_predicate,
                'xml_match_predicate': xml_predicate,
                'legacy_xml_match_predicate': legacy_xml_predicate,
                'xml_borrowed_match_predicate': (
                    f'content.get("item").map_or('
                    f'content.is_empty_xml_map(), |node| '
                    f'{xml_predicate})'
                    if xml_representation == 'sequence'
                    else (
                        f'content.get("entries").map_or(false, |node| '
                        f'{xml_predicate})'
                        if xml_representation == 'map'
                        else xml_predicate
                    )
                ),
                'xml_canonical_match_predicate': (
                    (
                        f'content.get("item").map_or('
                        f'content.is_empty_xml_map(), |node| '
                        f'{canonical_xml_predicate})'
                        if xml_representation == 'sequence'
                        else (
                            f'content.get("entries").map_or(false, |node| '
                            f'{canonical_xml_predicate})'
                            if xml_representation == 'map'
                            else canonical_xml_predicate
                        )
                    )
                ),
                'xml_representation': xml_representation,
                'json_match_signature': predicate_key,
                'json_shape_signature': shape_signature,
                'json_default_shape_signature': default_shape_signature,
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
        for field in union_fields:
            json_ambiguous = sum(
                1 for candidate in union_fields
                if self.json_match_accepts_shape(
                    candidate['json_match_signature'],
                    field['json_shape_signature'],
                )
            ) > 1
            scalar_kind = scalar_kinds.get(field['type'])
            field['xml_scalar_kind'] = scalar_kind or ''
            field['xml_guard_string'] = scalar_kind == 'string' and len(present_scalar_kinds) > 1
            field['xml_reject_value'] = (
                (scalar_kind is not None and scalar_kind != 'string' and 'string' in present_scalar_kinds)
                or (scalar_kind == 'integer' and 'float' in present_scalar_kinds)
            )
            field['xml_check_value_ambiguity'] = json_ambiguous
            default_is_ambiguous = sum(
                1 for candidate in union_fields
                if self.json_match_accepts_shape(
                    candidate['json_match_signature'],
                    field['json_default_shape_signature'],
                )
            ) > 1
            field['json_default_is_ambiguous'] = default_is_ambiguous
            field['xml_safe_for_random'] = not (
                field['xml_reject_value']
                or default_is_ambiguous
            )
            field['xml_random_value'] = (
                'i64::MAX'
                if field['type'] == 'i64'
                and any(
                    candidate['type'] == 'i32'
                    for candidate in union_fields
                )
                else (
                    field['default_value']
                    if field['xml_check_value_ambiguity']
                    else field['random_value']
                )
            )
            field['json_ambiguous'] = json_ambiguous
            field['json_round_trip_safe'] = (
                not field['json_ambiguous']
                and self.is_json_round_trip_safe(
                    field['avro_type'],
                    namespace,
                )
            )
        xml_string_guards = {
            'bool': 'bool' in present_scalar_kinds,
            'integer': 'integer' in present_scalar_kinds,
            'float': 'float' in present_scalar_kinds,
        }
        
        qualified_union_enum_name = self.safe_package(self.concat_package(ns, union_enum_name))
        legacy_name = pascal(field_name) + 'Union'
        self.union_alias_candidates.setdefault(
            (namespace.lower(), legacy_name),
            [],
        ).append(qualified_union_enum_name)
        context = {
            'serde_annotation': self.serde_annotation,
            'avro_annotation': self.avro_annotation,
            'xml_annotation': self.xml_annotation,
            'union_enum_name': union_enum_name,
            'union_fields': union_fields,
            'xml_string_guards': xml_string_guards,
            'xml_has_typed_scalar_parse': bool(
                present_scalar_kinds & {'bool', 'integer', 'float'}
            ),
            'xml_has_record_variant': any(
                field['xml_representation'] == 'record'
                for field in union_fields
            ),
            'avro_schema': avro_schema_str,
            'source_avro_schema': source_avro_schema_str,
            'source_null_index': (
                avro_type.index('null') if 'null' in avro_type else -1
            ),
            'ambiguous_json_field': next(
                (
                    field for field in union_fields
                    if field['json_default_is_ambiguous']
                ),
                None,
            ),
            'json_match_predicates': [
                field['json_match_predicate']
                for field in union_fields
            ],
        }

        file_name = self.to_file_name(qualified_union_enum_name)
        target_file = self.output_path("src", file_name + ".rs")
        render_template('avrotorust/dataclass_union.rs.jinja', target_file, **context)
        self.generated_types_avro_namespace[qualified_union_enum_name] = "union"
        self.generated_types_rust_package[qualified_union_enum_name] = "union"
        self.generated_union_fields[qualified_union_enum_name] = union_fields
        self.write_mod_rs(namespace)

        return qualified_union_enum_name

    def write_union_aliases(self):
        """Emits legacy Avro union names when they are unambiguous."""
        for (
            alias_path,
            namespace,
            normalized_path,
        ) in self.generated_aliases_to_remove:
            if (
                normalized_path not in self.planned_source_paths
                and os.path.exists(alias_path)
            ):
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
            target_file = self.output_path(
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

    @staticmethod
    def normalize_json_union_signature(signatures):
        """Returns a flattened, deduplicated union signature."""
        flattened = []
        for signature in signatures:
            if (
                isinstance(signature, tuple)
                and signature
                and signature[0] == 'union'
            ):
                flattened.extend(signature[1])
            else:
                flattened.append(signature)
        unique = tuple(dict.fromkeys(flattened))
        if len(unique) == 1:
            return unique[0]
        return ('union', unique)

    def generated_json_union_branches(self, avro_type, namespace: str):
        """Returns branches represented by the generated Rust field type."""
        non_null_types = [item for item in avro_type if item != 'null']
        if not non_null_types:
            return ('null',)
        if len(non_null_types) > 1:
            return tuple(non_null_types)

        branch = non_null_types[0]
        if (
            isinstance(branch, str)
            and not is_any_value_type(branch)
            and self.resolve_avro_named_type(branch, namespace) is None
        ):
            return (branch, 'null')
        return (branch,)

    def _build_json_signature(
        self,
        avro_type,
        namespace: str,
        mode: str,
    ) -> JsonSignature:
        """Builds a cycle-safe graph with one node per named schema node."""
        nodes: list[tuple | None] = []
        atoms = {}
        named_nodes = {}
        anonymous_nodes = {}
        pending = []
        generic_type_cache = {}
        acyclic_type_cache = {}

        def add_atom(kind: str) -> int:
            if kind not in atoms:
                atoms[kind] = len(nodes)
                nodes.append((kind, None))
            return atoms[kind]

        def add_pending(key, kind, payload, current_namespace):
            if key in anonymous_nodes:
                return anonymous_nodes[key]
            node_id = len(nodes)
            anonymous_nodes[key] = node_id
            nodes.append(None)
            pending.append(
                (kind, node_id, payload, current_namespace)
            )
            return node_id

        def is_acyclic(root) -> bool:
            active = set()
            finished = set()
            stack = [(root, False)]
            while stack:
                value, leaving = stack.pop()
                if not isinstance(value, (dict, list)):
                    continue
                value_id = id(value)
                if leaving:
                    active.discard(value_id)
                    finished.add(value_id)
                    continue
                if value_id in active:
                    return False
                if value_id in finished:
                    continue
                active.add(value_id)
                stack.append((value, True))
                children = value.values() if isinstance(value, dict) else value
                stack.extend((child, False) for child in children)
            return True

        def ensure(node, current_namespace: str) -> int:
            while True:
                if isinstance(node, str):
                    if is_any_value_type(node):
                        return add_atom(
                            'null' if mode == 'default' else 'any'
                        )
                    resolved = self.resolve_avro_named_type(
                        node,
                        current_namespace,
                    )
                    if resolved:
                        fullname = self.avro_type_fullnames[id(resolved)]
                        if resolved.get('type') == 'record':
                            if fullname in named_nodes:
                                return named_nodes[fullname]
                            node_id = len(nodes)
                            named_nodes[fullname] = node_id
                            nodes.append(None)
                            pending.append((
                                'record',
                                node_id,
                                resolved,
                                fullname.rpartition('.')[0],
                            ))
                            return node_id
                        if (
                            resolved.get('type') == 'fixed'
                            and resolved.get('logicalType') == 'decimal'
                        ):
                            return add_atom('number')
                        if resolved.get('type') == 'enum':
                            symbols = resolved.get('symbols', [])
                            if mode == 'match' and self.xml_annotation:
                                symbols = symbols + [
                                    xml_enum_wire_value(
                                        symbol,
                                        resolved,
                                    )
                                    for symbol in symbols
                                ]
                            if mode == 'default':
                                symbols = symbols[:1]
                            return add_atom(
                                'enum:' + '\x1f'.join(sorted(symbols))
                            )
                        return add_atom(
                            'array'
                            if resolved.get('type') == 'fixed'
                            else resolved.get('type', node)
                        )
                    if node in ('int', 'long'):
                        return add_atom('integer')
                    if node in ('float', 'double'):
                        return add_atom('number')
                    if mode == 'match' and node == 'bytes':
                        return add_atom('array')
                    return add_atom(node)

                if isinstance(node, list):
                    union_key = ('union', id(node), current_namespace)
                    if union_key in anonymous_nodes:
                        return anonymous_nodes[union_key]
                    is_generic = generic_type_cache.get(id(node))
                    if is_generic is None:
                        acyclic = is_acyclic(node)
                        acyclic_type_cache[id(node)] = acyclic
                        is_generic = acyclic and is_generic_avro_type(node)
                        generic_type_cache[id(node)] = is_generic
                    if is_generic and mode != 'default':
                        if self.serde_annotation or self.xml_annotation:
                            return add_atom('any')
                        if mode == 'match':
                            return add_pending(
                                (
                                    'generic-map-match',
                                    id(node),
                                    current_namespace,
                                ),
                                'map_match',
                                'string',
                                current_namespace,
                            )
                        key = ('generic-map', id(node), current_namespace)
                        return add_pending(
                            key,
                            'map',
                            'string',
                            current_namespace,
                        )
                    generated_branches = self.generated_json_union_branches(
                        node,
                        current_namespace,
                    )
                    if mode == 'default':
                        if not acyclic_type_cache[id(node)]:
                            return add_pending(
                                union_key,
                                'union',
                                generated_branches,
                                current_namespace,
                            )
                        if (
                            'null' in generated_branches
                            or not generated_branches
                        ):
                            return add_atom('null')
                        node = generated_branches[0]
                        continue
                    if len(generated_branches) == 1:
                        node = generated_branches[0]
                        continue
                    return add_pending(
                        union_key,
                        'union',
                        generated_branches,
                        current_namespace,
                    )

                if not isinstance(node, dict):
                    return add_atom(str(node))

                node_type = node.get('type')
                if node_type == 'record':
                    fullname, record_namespace, _ = (
                        self.canonical_avro_name(
                            node['name'],
                            node.get('namespace', current_namespace),
                        )
                    )
                    if fullname in named_nodes:
                        return named_nodes[fullname]
                    node_id = len(nodes)
                    named_nodes[fullname] = node_id
                    nodes.append(None)
                    pending.append((
                        'record',
                        node_id,
                        node,
                        record_namespace,
                    ))
                    return node_id
                if node_type == 'enum':
                    symbols = node.get('symbols', [])
                    if mode == 'match' and self.xml_annotation:
                        symbols = symbols + [
                            xml_enum_wire_value(symbol, node)
                            for symbol in symbols
                        ]
                    if mode == 'default':
                        symbols = symbols[:1]
                    return add_atom(
                        'enum:' + '\x1f'.join(sorted(symbols))
                    )
                if node_type == 'array':
                    if mode == 'match':
                        return add_atom('array')
                    return add_pending(
                        ('array', id(node), current_namespace),
                        'array',
                        node['items'],
                        current_namespace,
                    )
                if node_type == 'map':
                    if mode == 'match':
                        return add_pending(
                            ('map-match', id(node), current_namespace),
                            'map_match',
                            node['values'],
                            current_namespace,
                        )
                    return add_pending(
                        ('map', id(node), current_namespace),
                        'map',
                        node['values'],
                        current_namespace,
                    )
                if node.get('logicalType') in {
                    'date',
                    'time-millis',
                    'time-micros',
                    'timestamp-millis',
                    'timestamp-micros',
                }:
                    return add_atom('string')
                if (
                    node_type in ('fixed', 'bytes')
                    and node.get('logicalType') == 'decimal'
                ):
                    return add_atom('number')
                if node_type in ('fixed', 'bytes'):
                    return add_atom('array')
                node = node_type

        root = ensure(avro_type, namespace)
        pending_index = 0
        while pending_index < len(pending):
            kind, node_id, payload, current_namespace = pending[pending_index]
            pending_index += 1
            if kind == 'record':
                fields = tuple(
                    (
                        (
                            tuple(dict.fromkeys((
                                field['name'],
                                (
                                    f"@{xml_wire_name(field['name'], field)}"
                                    if field.get(
                                        'xmlkind',
                                        'element',
                                    ) == 'attribute'
                                    else xml_wire_name(
                                        field['name'],
                                        field,
                                    )
                                ),
                            )))
                            if mode == 'match' and self.xml_annotation
                            else field['name']
                        ),
                        ensure(field['type'], current_namespace),
                    )
                    for field in payload.get('fields', [])
                )
                nodes[node_id] = (
                    'record_match' if mode == 'match' else 'record',
                    fields,
                )
            elif kind == 'union':
                branch_ids = []
                for branch in payload:
                    branch_id = ensure(branch, current_namespace)
                    if branch_id not in branch_ids:
                        branch_ids.append(branch_id)
                nodes[node_id] = ('union', tuple(branch_ids))
            else:
                nodes[node_id] = (
                    kind,
                    ensure(payload, current_namespace),
                )

        return JsonSignature(root, nodes)

    def get_json_shape_signature(
        self,
        avro_type,
        namespace: str,
        resolving=None,
    ):
        """Returns a bounded graph for generated untagged JSON values."""
        del resolving
        return self._build_json_signature(avro_type, namespace, 'shape')

    def get_json_default_shape_signature(
        self,
        avro_type,
        namespace: str,
        resolving=None,
    ):
        """Returns a bounded graph for the generated Rust Default value."""
        del resolving
        return self._build_json_signature(avro_type, namespace, 'default')

    def evaluate_json_round_trip_safe(
        self,
        avro_type,
        namespace: str,
        named_safety,
        owner=None,
    ) -> bool:
        """Evaluates JSON safety using the current named-type fixed point."""
        if isinstance(avro_type, str):
            resolved = self.resolve_avro_named_type(avro_type, namespace)
            if not resolved:
                return True
            fullname = self.avro_type_fullnames[id(resolved)]
            return named_safety.get(fullname, True)
        if isinstance(avro_type, list):
            branches = [item for item in avro_type if item != 'null']
            if len(branches) <= 1:
                return (
                    not branches
                    or self.evaluate_json_round_trip_safe(
                        branches[0],
                        namespace,
                        named_safety,
                    )
                )
            match_signatures = [
                self.get_json_match_signature(
                    branch,
                    namespace,
                )
                for branch in branches
            ]
            shape_signatures = [
                self.get_json_shape_signature(
                    branch,
                    namespace,
                )
                for branch in branches
            ]
            return any(
                sum(
                    1 for match_signature in match_signatures
                    if self.json_match_accepts_shape(
                        match_signature,
                        shape_signature,
                    )
                ) == 1
                and self.evaluate_json_round_trip_safe(
                    branch,
                    namespace,
                    named_safety,
                )
                for branch, shape_signature
                in zip(branches, shape_signatures)
            )
        if not isinstance(avro_type, dict):
            return True

        node_type = avro_type.get('type')
        if node_type == 'record':
            fullname, record_namespace, _ = self.canonical_avro_name(
                avro_type['name'],
                avro_type.get('namespace', namespace),
            )
            if fullname != owner and fullname in named_safety:
                return named_safety[fullname]
            return all(
                self.evaluate_json_round_trip_safe(
                    field['type'],
                    record_namespace,
                    named_safety,
                )
                for field in avro_type.get('fields', [])
            )
        if node_type == 'array':
            return self.evaluate_json_round_trip_safe(
                avro_type['items'],
                namespace,
                named_safety,
            )
        if node_type == 'map':
            return self.evaluate_json_round_trip_safe(
                avro_type['values'],
                namespace,
                named_safety,
            )
        if isinstance(node_type, (dict, list)):
            return self.evaluate_json_round_trip_safe(
                node_type,
                namespace,
                named_safety,
            )
        return True

    def ensure_json_round_trip_safety(self):
        """Computes the greatest fixed point for named-type JSON safety."""
        if self.json_round_trip_safety is not None:
            return
        safety = {
            fullname: True
            for fullname in self.avro_named_types
        }
        reverse_dependencies = {
            fullname: set()
            for fullname in self.avro_named_types
        }

        def collect_dependencies(owner, schema):
            dependencies = set()
            pending = [(schema, owner.rpartition('.')[0])]
            while pending:
                node, namespace = pending.pop()
                if isinstance(node, str):
                    resolved = self.resolve_avro_named_type(node, namespace)
                    if resolved:
                        dependencies.add(
                            self.avro_type_fullnames[id(resolved)]
                        )
                    continue
                if isinstance(node, list):
                    pending.extend((item, namespace) for item in node)
                    continue
                if not isinstance(node, dict):
                    continue
                node_type = node.get('type')
                if node_type == 'record':
                    fullname, record_namespace, _ = (
                        self.canonical_avro_name(
                            node['name'],
                            node.get('namespace', namespace),
                        )
                    )
                    if fullname != owner:
                        dependencies.add(fullname)
                        continue
                    pending.extend(
                        (field['type'], record_namespace)
                        for field in node.get('fields', [])
                    )
                elif node_type == 'array':
                    pending.append((node.get('items'), namespace))
                elif node_type == 'map':
                    pending.append((node.get('values'), namespace))
                elif isinstance(node_type, (dict, list)):
                    pending.append((node_type, namespace))
                elif isinstance(node_type, str):
                    resolved = self.resolve_avro_named_type(
                        node_type,
                        namespace,
                    )
                    if resolved:
                        dependencies.add(
                            self.avro_type_fullnames[id(resolved)]
                        )
            return dependencies

        for owner, schema in self.avro_named_types.items():
            for dependency in collect_dependencies(owner, schema):
                if dependency != owner:
                    reverse_dependencies.setdefault(
                        dependency,
                        set(),
                    ).add(owner)

        pending = sorted(self.avro_named_types)
        queued = set(pending)
        pending_index = 0
        while pending_index < len(pending):
            fullname = pending[pending_index]
            pending_index += 1
            queued.discard(fullname)
            if not safety[fullname]:
                continue
            schema = self.avro_named_types[fullname]
            namespace = fullname.rpartition('.')[0]
            if self.evaluate_json_round_trip_safe(
                schema,
                namespace,
                safety,
                owner=fullname,
            ):
                continue
            safety[fullname] = False
            for dependent in sorted(
                reverse_dependencies.get(fullname, ())
            ):
                if safety[dependent] and dependent not in queued:
                    pending.append(dependent)
                    queued.add(dependent)
        self.json_round_trip_safety = safety

    def is_json_round_trip_safe(
        self,
        avro_type,
        namespace: str,
        resolving=None,
    ) -> bool:
        """Checks whether generated untagged JSON has a deterministic branch."""
        del resolving
        self.ensure_json_round_trip_safety()
        if isinstance(avro_type, str):
            resolved = self.resolve_avro_named_type(avro_type, namespace)
            if resolved:
                fullname = self.avro_type_fullnames[id(resolved)]
                return self.json_round_trip_safety[fullname]
        if isinstance(avro_type, dict) and avro_type.get('name'):
            fullname = self.avro_type_fullnames.get(id(avro_type))
            if fullname in self.json_round_trip_safety:
                return self.json_round_trip_safety[fullname]
        return self.evaluate_json_round_trip_safe(
            avro_type,
            namespace,
            self.json_round_trip_safety,
        )

    def get_json_match_signature(
        self,
        avro_type,
        namespace: str,
        resolving=None,
    ):
        """Returns a bounded graph accepted by a generated union matcher."""
        del resolving
        return self._build_json_signature(avro_type, namespace, 'match')

    @staticmethod
    def _json_signature_root(signature):
        if isinstance(signature, JsonSignature):
            return _JsonSignatureRef(signature, signature.root)
        return signature

    @staticmethod
    def _json_signature_key(signature):
        if isinstance(signature, _JsonSignatureRef):
            return (
                'graph',
                id(signature.signature),
                signature.node_id,
            )
        if isinstance(signature, tuple):
            return ('legacy', id(signature))
        return ('atom', signature)

    @staticmethod
    def _json_signature_view(signature):
        if isinstance(signature, _JsonSignatureRef):
            kind, data = signature.signature.nodes[signature.node_id]
            if kind in ('record', 'record_match'):
                return kind, tuple(
                    (
                        name,
                        _JsonSignatureRef(signature.signature, child_id),
                    )
                    for name, child_id in data
                )
            if kind == 'union':
                return kind, tuple(
                    _JsonSignatureRef(signature.signature, child_id)
                    for child_id in data
                )
            if data is not None:
                return kind, _JsonSignatureRef(signature.signature, data)
            return kind, None
        if isinstance(signature, tuple) and signature:
            return (
                signature[0],
                signature[1] if len(signature) > 1 else None,
            )
        return signature, None

    @staticmethod
    def json_match_accepts_shape(
        match_signature,
        shape_signature,
        _memo=None,
        _stats=None,
    ) -> bool:
        """Checks matcher overlap across all realizable serialized shapes."""
        if _memo is None:
            _memo = {}
        start = (
            AvroToRust._json_signature_root(match_signature),
            AvroToRust._json_signature_root(shape_signature),
        )

        def pair_key(pair):
            return tuple(
                AvroToRust._json_signature_key(signature)
                for signature in pair
            )

        start_key = pair_key(start)
        if start_key in _memo:
            if _stats is not None:
                _stats.update({
                    'equation_count': 0,
                    'equation_evaluations': 0,
                    'queue_pushes': 0,
                    'dependency_notifications': 0,
                })
            return _memo[start_key]

        equations = {}
        operands = {start_key: start}
        pending = [start_key]
        while pending:
            key = pending.pop()
            if key in equations:
                continue
            match_node, shape_node = operands[key]
            shape_kind, shape_data = AvroToRust._json_signature_view(
                shape_node
            )
            match_kind, match_data = AvroToRust._json_signature_view(
                match_node
            )
            dependencies = None
            operator = None
            if match_kind == 'any' or shape_kind == 'any':
                equations[key] = True
            elif shape_kind == 'union':
                operator = 'any'
                dependencies = [
                    (match_node, branch_shape)
                    for branch_shape in shape_data
                ]
            elif match_kind == 'union':
                operator = 'any'
                dependencies = [
                    (branch_match, shape_node)
                    for branch_match in match_data
                ]
            elif match_kind == 'record_match':
                if shape_kind == 'record':
                    operator = 'all'
                    value_fields = dict(shape_data)
                    dependencies = [
                        (
                            field_match,
                            next(
                                (
                                    value_fields[name]
                                    for name in (
                                        field_name
                                        if isinstance(field_name, tuple)
                                        else (field_name,)
                                    )
                                    if name in value_fields
                                ),
                                'null',
                            ),
                        )
                        for field_name, field_match in match_data
                    ]
                elif shape_kind == 'map':
                    operator = 'all_any'
                    dependencies = [
                        (
                            (field_match, 'null'),
                            (field_match, shape_data),
                        )
                        for _, field_match in match_data
                    ]
                else:
                    equations[key] = False
            elif match_kind == 'object':
                equations[key] = shape_kind in ('map', 'record')
            elif match_kind == 'map_match':
                operator = 'all'
                if shape_kind == 'map':
                    dependencies = [(match_data, shape_data)]
                elif shape_kind == 'record':
                    dependencies = [
                        (match_data, field_shape)
                        for _, field_shape in shape_data
                    ]
                else:
                    equations[key] = False
            elif match_kind == 'number':
                equations[key] = shape_kind in ('integer', 'number')
            elif match_kind == 'array':
                equations[key] = shape_kind in (
                    'array',
                    'bytes',
                    'fixed',
                )
            elif (
                isinstance(match_kind, str)
                and match_kind.startswith('enum:')
            ):
                match_symbols = set(match_kind[5:].split('\x1f'))
                if (
                    isinstance(shape_kind, str)
                    and shape_kind.startswith('enum:')
                ):
                    shape_symbols = set(shape_kind[5:].split('\x1f'))
                    equations[key] = bool(
                        match_symbols & shape_symbols
                    )
                else:
                    equations[key] = shape_kind == 'string'
            elif (
                match_kind == 'string'
                and isinstance(shape_kind, str)
                and shape_kind.startswith('enum:')
            ):
                equations[key] = True
            else:
                equations[key] = (
                    match_kind == shape_kind
                    and (
                        match_data == shape_data
                        if match_kind == 'ref'
                        else True
                    )
                )
            if dependencies is not None:
                dependency_keys = []
                for dependency in dependencies:
                    alternatives = (
                        dependency
                        if operator == 'all_any'
                        else (dependency,)
                    )
                    alternative_keys = []
                    for alternative in alternatives:
                        dependency_key = pair_key(alternative)
                        alternative_keys.append(dependency_key)
                        operands.setdefault(dependency_key, alternative)
                        if dependency_key not in equations:
                            pending.append(dependency_key)
                    dependency_keys.append(
                        tuple(alternative_keys)
                        if operator == 'all_any'
                        else alternative_keys[0]
                    )
                equations[key] = (operator, tuple(dependency_keys))

        values = {
            key: equation if isinstance(equation, bool) else False
            for key, equation in equations.items()
        }
        dependents = defaultdict(set)
        remaining = {}
        satisfied_groups = defaultdict(set)
        queue = deque()
        queued = set()
        queue_pushes = 0
        equation_evaluations = 0
        dependency_notifications = 0

        def publish(key):
            nonlocal queue_pushes
            if values[key] and key not in queued:
                queued.add(key)
                queue.append(key)
                queue_pushes += 1

        for key, equation in equations.items():
            if isinstance(equation, bool):
                if equation:
                    publish(key)
                continue
            equation_evaluations += 1
            operator, dependencies = equation
            if operator == 'any':
                dependency_keys = set(dependencies)
                for dependency in dependency_keys:
                    dependents[dependency].add((key, None))
                if any(values[dependency] for dependency in dependency_keys):
                    values[key] = True
                    publish(key)
            elif operator == 'all':
                dependency_keys = set(dependencies)
                for dependency in dependency_keys:
                    dependents[dependency].add((key, None))
                remaining[key] = len(dependency_keys)
                if remaining[key] == 0:
                    values[key] = True
                    publish(key)
            else:
                remaining[key] = len(dependencies)
                for group_index, alternatives in enumerate(dependencies):
                    alternative_keys = set(alternatives)
                    for dependency in alternative_keys:
                        dependents[dependency].add((key, group_index))
                    if any(
                        values[dependency]
                        for dependency in alternative_keys
                    ):
                        satisfied_groups[key].add(group_index)
                        remaining[key] -= 1
                if remaining[key] == 0:
                    values[key] = True
                    publish(key)

        while queue:
            dependency = queue.popleft()
            queued.remove(dependency)
            for key, group_index in dependents[dependency]:
                dependency_notifications += 1
                if values[key]:
                    continue
                operator, _ = equations[key]
                if operator == 'any':
                    values[key] = True
                elif operator == 'all':
                    remaining[key] -= 1
                    values[key] = remaining[key] == 0
                elif group_index not in satisfied_groups[key]:
                    satisfied_groups[key].add(group_index)
                    remaining[key] -= 1
                    values[key] = remaining[key] == 0
                if values[key]:
                    publish(key)

        if _stats is not None:
            _stats.update({
                'equation_count': len(equations),
                'equation_evaluations': equation_evaluations,
                'queue_pushes': queue_pushes,
                'dependency_notifications': dependency_notifications,
            })
        _memo.update(values)
        return values[start_key]

    def to_file_name(self, qualified_name):
        """Converts a qualified union enum name to a file name"""
        if qualified_name.startswith('crate::'):
            qualified_name = qualified_name[(len('crate::')):]
        qualified_name = qualified_name.replace('r#', '')
        return qualified_name.rsplit('::',1)[0].replace('::', os.sep).lower()

    def output_path(self, *relative_parts: str) -> str:
        """Returns a contained output path for generated artifacts."""
        output_root = os.path.abspath(self.output_dir)
        candidate = os.path.abspath(
            os.path.join(output_root, *relative_parts)
        )
        if os.path.commonpath((output_root, candidate)) != output_root:
            raise ValueError(
                f"generated Rust path escapes output directory: {candidate}"
            )
        return candidate

    @staticmethod
    def validate_rust_path_components(parts, owner: str):
        """Rejects generated relative path components that can escape."""
        for part in parts:
            if (
                part in ('', '.', '..')
                or '/' in part
                or '\\' in part
                or os.path.isabs(part)
            ):
                raise ValueError(
                    f"invalid generated Rust path component '{part}' "
                    f"for '{owner}'"
                )

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
            directory_path = self.output_path(
                "src",
                sub_package.replace('.', os.sep).replace('::', os.sep),
            )
            if not os.path.exists(directory_path):
                os.makedirs(directory_path, exist_ok=True)
            mod_rs_path = self.output_path(
                "src",
                sub_package.replace('.', os.sep).replace('::', os.sep),
                "mod.rs",
            )
            
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
        cargo_toml_path = self.output_path("Cargo.toml")
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
        lib_rs_path = self.output_path("src", "lib.rs")
        if not os.path.exists(os.path.dirname(lib_rs_path)):
            os.makedirs(os.path.dirname(lib_rs_path), exist_ok=True)
        with open(lib_rs_path, 'w', encoding='utf-8') as file:
            file.write(lib_rs_content)

    def write_xml_support_rs(self):
        """Writes shared XML validation and bounded decompression helpers."""
        if self.xml_annotation:
            render_template(
                'rust/xml_support.rs.jinja',
                self.output_path("src", "xml_support.rs"),
            )

    def convert_schema(self, schema: JsonNode, output_dir: str):
        """Converts Avro schema to Rust"""
        self.reset_run_state()
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
        union_identity_targets = {}

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
                    self.validate_rust_path_components(
                        namespace_parts + (union_name.lower(),),
                        union_name,
                    )
                    structural_identity = json.dumps(
                        self.inline_avro_references(
                            [item for _, item in non_null],
                            namespace,
                        ),
                        sort_keys=True,
                    )
                    identity_key = (
                        namespace.lower(),
                        structural_identity,
                        tuple(index for index, _ in non_null),
                        (
                            node.index('null')
                            if self.avro_annotation and 'null' in node else -1
                        ),
                    )
                    canonical_union_name = union_identity_targets.setdefault(
                        identity_key,
                        union_name,
                    )
                    union_output_path = (
                        namespace_parts
                        + (canonical_union_name.lower(),)
                    )
                    union_identity = (
                        structural_identity,
                        tuple(index for index, _ in non_null),
                        (
                            node.index('null')
                            if self.avro_annotation and 'null' in node else -1
                        ),
                    )
                    add(
                        union_output_path,
                        'union',
                        union_identity,
                        f"generated union {canonical_union_name} at "
                        f"{path or [('field', field_name)]}",
                    )
                    legacy_name = pascal(field_name) + 'Union'
                    alias_candidates.setdefault(
                        (namespace_parts, legacy_name),
                        set(),
                    ).add(canonical_union_name)
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
                    self.validate_rust_path_components(
                        namespace_parts + (
                            self.safe_identifier(
                                pascal(short_name)
                            ).lower(),
                        ),
                        fullname,
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
            alias_path = self.output_path(
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
                            (
                                alias_path,
                                namespace,
                                namespace_parts + (alias_name,),
                            )
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
        self.planned_source_paths = set(source_paths)
        add(
            ('lib',),
            'infrastructure',
            'lib.rs',
            'generated infrastructure lib.rs',
        )
        if self.xml_annotation:
            add(
                ('xml_support',),
                'infrastructure',
                'xml_support.rs',
                'generated infrastructure xml_support.rs',
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
