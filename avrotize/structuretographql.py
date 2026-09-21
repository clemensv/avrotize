# coding: utf-8
"""
Module to convert JSON Structure schema to GraphQL schema.
"""

import json
import os
import re
from typing import Dict, List, Optional, Set
from urllib.parse import unquote, urldefrag, urljoin, urlparse

from avrotize.common import get_longest_namespace_prefix

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | bool | int | None


class StructureToGraphQLConverter:
    """
    Class to convert JSON Structure schema to GraphQL schema.
    """

    BUILTIN_SCALARS = {'Boolean', 'Float', 'ID', 'Int', 'String'}
    CUSTOM_SCALARS = {'Binary', 'Date', 'DateTime', 'Decimal', 'Duration', 'JSON', 'Time', 'URI', 'UUID'}

    def __init__(self, structure_schema_path, graphql_schema_path):
        """
        Initialize the converter with file paths.

        :param structure_schema_path: Path to the JSON Structure schema file.
        :param graphql_schema_path: Path to save the GraphQL schema file.
        """
        self.structure_schema_path = structure_schema_path
        self.graphql_schema_path = graphql_schema_path
        self.records = {}  # qualified_name -> schema
        self.record_names = {}  # qualified_name -> simple_name
        self.enums = {}  # qualified_name -> schema
        self.enum_names = {}  # qualified_name -> simple_name
        self.scalars: Set[str] = set()
        self.schema_doc: JsonNode = None
        self.definitions: Dict = {}
        self.schema_registry: Dict[str, Dict] = {}
        self.schema_context: Dict[int, Dict] = {}
        self.schema_base_uri: Dict[int, str] = {}
        self.schema_names: Dict[int, str] = {}
        self.type_qualified_names: Dict[str, str] = {}
        self.type_document_ids: Dict[str, str] = {}
        self.extracting_schema_ids: Set[int] = set()
        self.extracted_schema_ids: Set[int] = set()
        self.longest_namespace_prefix = ""
        self.type_order = []  # Track order of type definitions for dependency ordering

    def convert(self: 'StructureToGraphQLConverter'):
        """
        Convert JSON Structure schema to GraphQL schema and save to file.
        """
        with open(self.structure_schema_path, 'r', encoding='utf-8') as file:
            structure_schemas: JsonNode = json.load(file)

        # Normalize to list
        if isinstance(structure_schemas, dict):
            structure_schemas = [structure_schemas]
        
        if not isinstance(structure_schemas, list):
            raise ValueError("Expected a single JSON Structure schema as a JSON object, or a list of schemas")

        self.schema_doc = structure_schemas
        
        # Register all schemas with $id
        for schema in structure_schemas:
            if isinstance(schema, dict):
                self.register_schema_ids(schema, context_schema=schema)

        # Extract named types from all schemas
        for schema in structure_schemas:
            if isinstance(schema, dict):
                # Process root type if it exists
                if 'type' in schema:
                    self.extract_named_types_from_structure(schema, schema.get('namespace', ''))
                elif '$root' in schema:
                    root_ref = schema['$root']
                    root_schema = self.resolve_ref(root_ref, schema)
                    if root_schema:
                        ref_path = self.reference_pointer_parts(root_ref, schema, root_schema)
                        ref_namespace = self.referenced_schema_namespace(root_schema, ref_path, '')
                        ref_name = ref_path[-1] if ref_path else str(root_schema.get('name', ''))
                        self.extract_named_types_from_structure(root_schema, ref_namespace, explicit_name=ref_name)

                # Process definitions
                if 'definitions' in schema:
                    self.definitions = schema['definitions']
                    self.process_definitions(self.definitions, '')

        self.assign_graphql_type_names()
        graphql_content = self.generate_graphql()

        with open(self.graphql_schema_path, "w", encoding="utf-8") as file:
            file.write(graphql_content)
            if not graphql_content.endswith('\n'):
                file.write('\n')

    def register_schema_ids(self, schema: Dict, base_uri: str = '', context_schema: Optional[Dict] = None) -> None:
        """Recursively registers schemas with $id keywords"""
        if not isinstance(schema, dict):
            return
        context_schema = context_schema if context_schema is not None else schema
        self.schema_context[id(schema)] = context_schema

        if '$id' in schema:
            schema_id = schema['$id']
            if base_uri and not schema_id.startswith(('http://', 'https://', 'urn:')):
                schema_id = urljoin(base_uri, schema_id)
            self.schema_registry[schema_id] = schema
            document_uri, fragment = urldefrag(schema_id)
            if not fragment:
                self.schema_registry[document_uri] = schema
            base_uri = schema_id
        self.schema_base_uri[id(schema)] = base_uri

        for value in schema.values():
            if isinstance(value, dict):
                self.register_schema_ids(value, base_uri, context_schema)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        self.register_schema_ids(item, base_uri, context_schema)

    def resolve_ref(self, ref: str, context_schema: Optional[Dict] = None) -> Optional[Dict]:
        """Resolves a $ref to the actual schema definition"""
        target = self._resolve_ref_once(ref, context_schema)
        followed: set[int] = set()
        while isinstance(target, dict) and '$root' in target and id(target) not in followed:
            followed.add(id(target))
            target = self._resolve_ref_once(str(target['$root']), target)
        return target if isinstance(target, dict) else None

    def _resolve_ref_once(self, ref: str, context_schema: Optional[Dict]) -> Optional[Dict]:
        context = context_schema if isinstance(context_schema, dict) else None
        owner = self.schema_context.get(id(context), context) if context is not None else None
        base_uri = self.schema_base_uri.get(id(context), '') if context is not None else ''
        if not base_uri and owner is not None:
            base_uri = self.schema_base_uri.get(id(owner), '')
        absolute_ref = urljoin(base_uri, ref)
        document_uri, fragment = urldefrag(absolute_ref)

        if document_uri:
            target: object = self.schema_registry.get(document_uri)
            if target is None:
                target = self.schema_registry.get(absolute_ref)
        else:
            target = owner if owner is not None else self.schema_doc

        candidates = target if isinstance(target, list) else [target]
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            resolved: object = candidate
            if fragment:
                for part in self._pointer_parts(fragment):
                    if not isinstance(resolved, dict) or part not in resolved:
                        break
                    resolved = resolved[part]
                else:
                    return resolved if isinstance(resolved, dict) else None
            else:
                return candidate
        return None

    @staticmethod
    def _pointer_parts(fragment: str) -> list[str]:
        if not fragment.startswith('/'):
            return []
        return [
            unquote(part).replace('~1', '/').replace('~0', '~')
            for part in fragment[1:].split('/')
        ]

    def reference_pointer_parts(self, ref: str, context_schema: Dict, resolved_schema: Dict) -> list[str]:
        base_uri = self.schema_base_uri.get(id(context_schema), '')
        _, fragment = urldefrag(urljoin(base_uri, ref))
        parts = self._pointer_parts(fragment)
        if parts:
            return parts
        owner = self.schema_context.get(id(resolved_schema))
        if isinstance(owner, dict) and '$root' in owner and owner is not context_schema:
            return self.reference_pointer_parts(str(owner['$root']), owner, resolved_schema)
        return []

    def referenced_schema_namespace(self, schema: Dict, ref_path: list[str], fallback: str) -> str:
        """Return the namespace owned by a resolved reference target."""
        if 'namespace' in schema:
            return str(schema.get('namespace') or '')
        if ref_path[:1] == ['definitions'] and len(ref_path) > 2:
            return '.'.join(ref_path[1:-1])
        owner = self.schema_context.get(id(schema))
        if isinstance(owner, dict) and 'namespace' in owner:
            return str(owner.get('namespace') or '')
        return fallback

    def process_definitions(self, definitions: Dict, namespace_path: str) -> None:
        """Processes the definitions section recursively"""
        for name, definition in definitions.items():
            if isinstance(definition, dict):
                if 'type' in definition or 'enum' in definition:
                    # This is a type definition
                    current_namespace = self.concat_namespace(namespace_path, '')
                    self.extract_named_types_from_structure(definition, current_namespace, explicit_name=name)
                else:
                    # This is a namespace
                    new_namespace = self.concat_namespace(namespace_path, name)
                    self.process_definitions(definition, new_namespace)

    def concat_namespace(self, namespace: str, name: str) -> str:
        """Concatenates namespace and name with a dot separator"""
        if namespace and name:
            return f"{namespace}.{name}"
        elif namespace:
            return namespace
        else:
            return name

    def qualified_name(self, schema: Dict[str, JsonNode], parent_namespace: str = '') -> str:
        """
        Get the full name of a record type.
        """
        name = str(schema.get('name', 'UnnamedType'))
        namespace = str(schema.get('namespace', parent_namespace))
        if namespace:
            return f"{namespace}.{name}"
        return name

    def schema_document_id(self, schema: Dict) -> str:
        """Return the owning document URI for a schema node, without a fragment."""
        base_uri = self.schema_base_uri.get(id(schema), '')
        if not base_uri:
            owner = self.schema_context.get(id(schema))
            if isinstance(owner, dict):
                base_uri = self.schema_base_uri.get(id(owner), '')
        document_uri, _ = urldefrag(base_uri)
        return document_uri

    def canonical_type_key(self, schema: Dict, qualified_name: str) -> str:
        """Keep document ownership in the private identity used by extracted types."""
        document_id = self.schema_document_id(schema)
        return f"{document_id}#{qualified_name}" if document_id else qualified_name

    @staticmethod
    def document_qualifier(document_id: str) -> str:
        """Derive a deterministic, readable collision qualifier from a document URI."""
        parsed = urlparse(document_id)
        path_name = unquote(parsed.path.rstrip('/').rsplit('/', 1)[-1])
        qualifier = os.path.splitext(path_name)[0] if path_name else parsed.netloc
        return qualifier or parsed.path or 'Document'

    @staticmethod
    def sanitize_graphql_name(name: str, fallback: str = 'UnnamedType') -> str:
        """Return a valid GraphQL name without changing internal schema identity."""
        sanitized = re.sub(r'[^A-Za-z0-9_]', '_', str(name)) or fallback
        if not sanitized[0].isalpha() and sanitized[0] != '_':
            sanitized = f'_{sanitized}'
        if sanitized.startswith('__'):
            sanitized = f'Type{sanitized}'
        return sanitized

    def assign_graphql_type_names(self) -> None:
        """Assign simple names where unique and namespace-qualified names on collisions."""
        entries = [
            ('record', type_key, name, schema)
            for type_key, schema in self.records.items()
            for name in [self.record_names[type_key]]
        ]
        entries.extend(
            ('enum', type_key, name, schema)
            for type_key, schema in self.enums.items()
            for name in [self.enum_names[type_key]]
        )
        by_simple_name: Dict[str, list[tuple[str, str, str, Dict]]] = {}
        for entry in entries:
            by_simple_name.setdefault(entry[2], []).append(entry)

        qualified_counts: Dict[str, int] = {}
        for _, type_key, _, _ in entries:
            qualified = self.type_qualified_names[type_key]
            qualified_counts[qualified] = qualified_counts.get(qualified, 0) + 1

        assigned: Dict[tuple[str, str], str] = {}
        used_names: Set[str] = self.BUILTIN_SCALARS | self.CUSTOM_SCALARS | self.scalars

        def reserve_name(name: str) -> str:
            base = self.sanitize_graphql_name(name)
            candidate = base
            index = 2
            while candidate in used_names:
                candidate = f"{base}_{index}"
                index += 1
            used_names.add(candidate)
            return candidate

        for simple_name in sorted(by_simple_name):
            group = by_simple_name[simple_name]
            if len(group) == 1:
                kind, type_key, _, _ = group[0]
                assigned[(kind, type_key)] = reserve_name(simple_name)

        for simple_name in sorted(by_simple_name):
            group = by_simple_name[simple_name]
            if len(group) == 1:
                continue
            for kind, type_key, _, _ in sorted(
                group,
                key=lambda entry: (
                    self.type_qualified_names[entry[1]],
                    self.type_document_ids[entry[1]],
                    entry[0],
                ),
            ):
                qualified = self.type_qualified_names[type_key]
                document_id = self.type_document_ids[type_key]
                if qualified_counts[qualified] > 1 and document_id:
                    qualified = f"{self.document_qualifier(document_id)}.{qualified}"
                assigned[(kind, type_key)] = reserve_name(qualified)

        for kind, type_key, _, schema in entries:
            graphql_name = assigned[(kind, type_key)]
            if kind == 'record':
                self.record_names[type_key] = graphql_name
            else:
                self.enum_names[type_key] = graphql_name
            self.schema_names[id(schema)] = graphql_name

    def extract_named_types_from_structure(self, schema: Dict, parent_namespace: str, explicit_name: str = ''):
        """
        Extract all named types (objects, enums) from a JSON Structure schema.
        """
        if not isinstance(schema, dict):
            return
        schema_id = id(schema)
        if schema_id in self.extracting_schema_ids:
            return
        self.extracting_schema_ids.add(schema_id)
        try:
            self._extract_named_types_from_structure(schema, parent_namespace, explicit_name)
        finally:
            self.extracting_schema_ids.remove(schema_id)

    def _extract_named_types_from_structure(self, schema: Dict, parent_namespace: str, explicit_name: str = ''):
        """Extract named types after guarding against active recursive references."""

        # Handle $ref FIRST before anything else
        if '$ref' in schema:
            ref_schema = self.resolve_ref(schema['$ref'], schema)
            if ref_schema:
                ref_path = self.reference_pointer_parts(schema['$ref'], schema, ref_schema)
                ref_namespace = self.referenced_schema_namespace(ref_schema, ref_path, parent_namespace)
                ref_name = ref_path[-1] if ref_path else self.schema_names.get(id(ref_schema), str(ref_schema.get('name', '')))
                self.extract_named_types_from_structure(ref_schema, ref_namespace, explicit_name=ref_name)
            return

        # Use explicit name if provided, otherwise get from schema
        name = explicit_name if explicit_name else schema.get('name', '')
        
        # Handle enum keyword
        if 'enum' in schema:
            if id(schema) in self.extracted_schema_ids:
                return
            self.schema_names[id(schema)] = name
            qualified = self.qualified_name({**schema, 'name': name, 'namespace': parent_namespace}, parent_namespace)
            type_key = self.canonical_type_key(schema, qualified)
            if type_key not in self.enums:
                self.enums[type_key] = schema
                self.enum_names[type_key] = name
                self.type_qualified_names[type_key] = qualified
                self.type_document_ids[type_key] = self.schema_document_id(schema)
                self.type_order.append(('enum', type_key))
            self.extracted_schema_ids.add(id(schema))
            return

        # Handle type keyword
        struct_type = schema.get('type')
        
        # A property whose ``type`` value is itself a schema object — the JSON Structure
        # Core canonical form for references, e.g. ``{"type": {"$ref": ...}}`` — or a union
        # array that contains such objects. Recurse so referenced named types are discovered
        # as dependencies BEFORE their referrer (preserving declaration order).
        if isinstance(struct_type, dict):
            self.extract_named_types_from_structure(struct_type, parent_namespace)
            return
        if isinstance(struct_type, list):
            for member in struct_type:
                if isinstance(member, dict):
                    self.extract_named_types_from_structure(member, parent_namespace)
            return
        
        if struct_type == 'object':
            if id(schema) in self.extracted_schema_ids:
                return
            self.schema_names[id(schema)] = name
            # Process nested properties FIRST to ensure dependencies come before this type
            if 'properties' in schema and isinstance(schema['properties'], dict):
                for prop_name, prop_schema in schema['properties'].items():
                    if isinstance(prop_schema, dict):
                        self.extract_named_types_from_structure(prop_schema, parent_namespace)
            
            # NOW add this type after all dependencies have been processed
            if name:
                qualified = self.qualified_name({**schema, 'name': name, 'namespace': parent_namespace}, parent_namespace)
                type_key = self.canonical_type_key(schema, qualified)
                if type_key not in self.records:
                    self.records[type_key] = schema
                    self.record_names[type_key] = name
                    self.type_qualified_names[type_key] = qualified
                    self.type_document_ids[type_key] = self.schema_document_id(schema)
                    self.type_order.append(('record', type_key))
                self.extracted_schema_ids.add(id(schema))
        
        elif struct_type == 'array' and 'items' in schema:
            if isinstance(schema['items'], dict):
                self.extract_named_types_from_structure(schema['items'], parent_namespace)
        
        elif struct_type == 'set' and 'items' in schema:
            if isinstance(schema['items'], dict):
                self.extract_named_types_from_structure(schema['items'], parent_namespace)
        
        elif struct_type == 'map' and 'values' in schema:
            if isinstance(schema['values'], dict):
                self.extract_named_types_from_structure(schema['values'], parent_namespace)
        
        elif struct_type == 'choice':
            # Process choice types
            choices = schema.get('choices', {})
            for choice_name, choice_schema in choices.items():
                if isinstance(choice_schema, dict):
                    if '$ref' in choice_schema:
                        ref_schema = self.resolve_ref(choice_schema['$ref'], choice_schema)
                        if ref_schema:
                            ref_path = self.reference_pointer_parts(choice_schema['$ref'], choice_schema, ref_schema)
                            ref_namespace = self.referenced_schema_namespace(ref_schema, ref_path, parent_namespace)
                            ref_name = ref_path[-1] if ref_path else str(ref_schema.get('name', ''))
                            self.extract_named_types_from_structure(ref_schema, ref_namespace, explicit_name=ref_name)
                    else:
                        self.extract_named_types_from_structure(choice_schema, parent_namespace)

    def generate_graphql(self):
        """
        Generate GraphQL content from the extracted types.

        :return: GraphQL content as a string.
        """
        self.scalars = {'JSON'}
        definitions = []
        for type_kind, qualified_name in self.type_order:
            if type_kind == 'enum' and qualified_name in self.enums:
                definitions.append(self.generate_graphql_enum({**self.enums[qualified_name], 'name': self.enum_names[qualified_name]}))
            elif type_kind == 'record' and qualified_name in self.records:
                definitions.append(self.generate_graphql_record({**self.records[qualified_name], 'name': self.record_names[qualified_name]}))

        scalar_definitions = [f"scalar {scalar}" for scalar in sorted(self.scalars)]
        return "\n".join([*scalar_definitions, '', *definitions])

    def generate_graphql_record(self, record):
        """
        Generate GraphQL content for a record.

        :param record: Record schema as a dictionary.
        :return: GraphQL content as a string.
        """
        name = record.get('name', 'UnnamedType')
        doc = record.get('description', record.get('doc', ''))
        
        # Add description as comment if present
        output = []
        if doc:
            output.append(f'"""\n{doc}\n"""')
        
        fields = []
        properties = record.get('properties', {})
        required_props = record.get('required', [])
        
        for prop_name, prop_schema in properties.items():
            field_type = self.get_graphql_type(prop_schema)
            
            # Add ! for required fields
            is_required = prop_name in required_props if isinstance(required_props, list) else False
            if is_required and not field_type.endswith('!'):
                field_type = f"{field_type}!"
            
            # Add field description as comment if present
            field_doc = prop_schema.get('description', prop_schema.get('doc', ''))
            if field_doc:
                fields.append(f'  """{field_doc}"""')
            fields.append(f"  {prop_name}: {field_type}")
        
        output.append(f"type {name} {{")
        if fields:
            output.extend(fields)
        else:
            output.append("  _empty: String")  # GraphQL doesn't allow empty types
        output.append("}")
        
        return "\n".join(output)

    def generate_graphql_enum(self, enum):
        """
        Generate GraphQL content for an enum.

        :param enum: Enum schema as a dictionary.
        :return: GraphQL content as a string.
        """
        name = enum.get('name', 'UnnamedEnum')
        doc = enum.get('description', enum.get('doc', ''))
        symbols = enum.get('enum', [])
        
        output = []
        if doc:
            output.append(f'"""\n{doc}\n"""')
        
        # GraphQL enum members must be valid identifiers
        enum_members = []
        for symbol in symbols:
            # Convert to valid GraphQL identifier
            member = str(symbol).replace('-', '_').replace(' ', '_').replace('.', '_')
            # Ensure it starts with a letter or underscore
            if member and not member[0].isalpha() and member[0] != '_':
                member = f"_{member}"
            enum_members.append(f"  {member}")
        
        output.append(f"enum {name} {{")
        if enum_members:
            output.extend(enum_members)
        else:
            output.append("  _EMPTY")
        output.append("}")
        
        return "\n".join(output)

    def get_graphql_type(self, structure_type):
        """
        Get GraphQL type from JSON Structure type.

        :param structure_type: JSON Structure type as a string, dict, or list.
        :return: GraphQL type as a string.
        """
        if isinstance(structure_type, list):
            # Handle type unions (e.g., ["null", "string"])
            non_null_types = [t for t in structure_type if t != "null"]
            if non_null_types:
                return self.get_graphql_type(non_null_types[0])
            return "String"
        
        if isinstance(structure_type, dict):
            # Handle $ref
            if '$ref' in structure_type:
                ref_schema = self.resolve_ref(structure_type['$ref'], structure_type)
                if ref_schema:
                    ref_path = self.reference_pointer_parts(structure_type['$ref'], structure_type, ref_schema)
                    ref_name = self.schema_names.get(id(ref_schema), ref_path[-1] if ref_path else ref_schema.get('name'))
                    ref_type = ref_schema.get('type')
                    if ref_type == 'object' or 'enum' in ref_schema:
                        return ref_name or structure_type['$ref'].split('/')[-1]
                    return self.get_graphql_type(ref_schema)
                    # Try to extract name from $ref path
                    ref_path = structure_type['$ref'].split('/')
                    # The last element is the type name
                    type_name = ref_path[-1]
                    return type_name
                return "JSON"
            
            # Handle enum
            if 'enum' in structure_type:
                # Return inline enum as String (GraphQL doesn't support inline enums)
                return "String"
            
            # Handle type keyword
            struct_type = structure_type.get('type')

            logical_type = structure_type.get('logicalType')
            logical_type_mapping = {
                'date': 'date',
                'rfc3339-date': 'date',
                'time': 'time',
                'timeMillis': 'time',
                'timeMicros': 'time',
                'time-millis': 'time',
                'time-micros': 'time',
                'rfc3339-time-millis': 'time',
                'rfc3339-time-micros': 'time',
                'datetime': 'datetime',
                'timestamp': 'timestamp',
                'timestampMillis': 'timestamp',
                'timestampMicros': 'timestamp',
                'timestamp-millis': 'timestamp',
                'timestamp-micros': 'timestamp',
                'localTimestampMillis': 'datetime',
                'localTimestampMicros': 'datetime',
                'local-timestamp-millis': 'datetime',
                'local-timestamp-micros': 'datetime',
                'rfc3339-timestamp-millis': 'datetime',
                'rfc3339-timestamp-micros': 'datetime',
                'rfc3339-local-timestamp-millis': 'datetime',
                'rfc3339-local-timestamp-micros': 'datetime',
                'duration': 'duration',
                'rfc3339-duration': 'duration',
                'decimal': 'decimal',
                'uuid': 'uuid',
            }
            if logical_type in logical_type_mapping:
                return self.get_graphql_primitive_type(logical_type_mapping[logical_type])
            
            # Handle type unions (e.g., ["string", "null"])
            if isinstance(struct_type, list):
                return self.get_graphql_type(struct_type)
            
            # Handle a type reference (or nested type object) that is the value of
            # ``type`` — the JSON Structure Core canonical form ``{"type": {"$ref": ...}}``.
            if isinstance(struct_type, dict):
                return self.get_graphql_type(struct_type)
            
            if struct_type == 'array':
                items_type = self.get_graphql_type(structure_type.get('items', {'type': 'any'}))
                return f"[{items_type}]"
            
            if struct_type == 'set':
                items_type = self.get_graphql_type(structure_type.get('items', {'type': 'any'}))
                return f"[{items_type}]"  # GraphQL doesn't have Set, use array
            
            if struct_type == 'map':
                return "JSON"  # GraphQL doesn't have Map, use custom scalar
            
            if struct_type == 'object':
                # Inline object - return JSON scalar
                return "JSON"
            
            if struct_type == 'choice':
                # Union types in GraphQL are more complex, return JSON for now
                return "JSON"
            
            if struct_type == 'tuple':
                # Tuples are represented as arrays in GraphQL
                return "JSON"
            
            if struct_type:
                return self.get_graphql_primitive_type(struct_type)
        
        if isinstance(structure_type, str):
            return self.get_graphql_primitive_type(structure_type)
        
        return "JSON"

    def get_graphql_primitive_type(self, structure_type):
        """
        Map JSON Structure primitive types to GraphQL types.

        :param structure_type: JSON Structure type as a string.
        :return: GraphQL type as a string.
        """
        type_mapping = {
            # JSON primitive types
            "string": "String",
            "number": "Float",
            "integer": "Int",
            "boolean": "Boolean",
            "null": "String",
            
            # Extended integer types
            "int8": "Int",
            "uint8": "Int",
            "int16": "Int",
            "uint16": "Int",
            "int32": "Int",
            "uint32": "Int",
            "int64": "Int",
            "uint64": "Int",
            "int128": "String",  # GraphQL Int is 32-bit, use String for larger
            "uint128": "String",
            
            # Floating point types
            "float8": "Float",
            "float": "Float",
            "double": "Float",
            "binary32": "Float",
            "binary64": "Float",
            "decimal": "Decimal",
            
            # Binary types
            "binary": "Binary",
            
            # Date/time types
            "date": "Date",
            "time": "Time",
            "datetime": "DateTime",
            "timestamp": "DateTime",
            "duration": "Duration",
            
            # Other special types
            "uuid": "UUID",
            "uri": "URI",
            "jsonpointer": "String",
            "any": "JSON"
        }
        
        graphql_type = type_mapping.get(structure_type, 'String')
        if graphql_type in self.CUSTOM_SCALARS:
            self.scalars.add(graphql_type)
        return graphql_type


def convert_structure_to_graphql(structure_schema_path, graphql_schema_path):
    """
    Convert a JSON Structure schema file to a GraphQL schema file.

    :param structure_schema_path: Path to the JSON Structure schema file.
    :param graphql_schema_path: Path to save the GraphQL schema file.
    """
    converter = StructureToGraphQLConverter(structure_schema_path, graphql_schema_path)
    converter.convert()
