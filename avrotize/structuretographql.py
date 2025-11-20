# coding: utf-8
"""
Module to convert JSON Structure schema to GraphQL schema.
"""

import json
import os
from typing import Dict, List, Optional, Set

from avrotize.common import get_longest_namespace_prefix

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | bool | int | None


class StructureToGraphQLConverter:
    """
    Class to convert JSON Structure schema to GraphQL schema.
    """

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
        self.scalars = {}
        self.schema_doc: JsonNode = None
        self.definitions: Dict = {}
        self.schema_registry: Dict[str, Dict] = {}
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
                self.register_schema_ids(schema)

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
                        ref_path = root_ref.split('/')
                        ref_namespace = '.'.join(ref_path[2:-1]) if len(ref_path) > 3 else ''
                        self.extract_named_types_from_structure(root_schema, ref_namespace)
                
                # Process definitions
                if 'definitions' in schema:
                    self.definitions = schema['definitions']
                    self.process_definitions(self.definitions, '')

        graphql_content = self.generate_graphql()

        with open(self.graphql_schema_path, "w", encoding="utf-8") as file:
            file.write(graphql_content)
            if not graphql_content.endswith('\n'):
                file.write('\n')

    def register_schema_ids(self, schema: Dict, base_uri: str = '') -> None:
        """Recursively registers schemas with $id keywords"""
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

    def resolve_ref(self, ref: str, context_schema: Optional[Dict] = None) -> Optional[Dict]:
        """Resolves a $ref to the actual schema definition"""
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
            return f"{namespace}_{name}".replace('.', '_')
        return name

    def extract_named_types_from_structure(self, schema: Dict, parent_namespace: str, explicit_name: str = ''):
        """
        Extract all named types (objects, enums) from a JSON Structure schema.
        """
        if not isinstance(schema, dict):
            return

    def extract_named_types_from_structure(self, schema: Dict, parent_namespace: str, explicit_name: str = ''):
        """
        Extract all named types (objects, enums) from a JSON Structure schema.
        """
        if not isinstance(schema, dict):
            return

        # Handle $ref FIRST before anything else
        if '$ref' in schema:
            # Use self.schema_doc as context for resolving refs
            context = self.schema_doc[0] if isinstance(self.schema_doc, list) and len(self.schema_doc) > 0 else self.schema_doc
            ref_schema = self.resolve_ref(schema['$ref'], context)
            if ref_schema:
                # Extract type name from $ref path for explicit naming
                ref_path = schema['$ref'].split('/')
                ref_name = ref_path[-1]
                self.extract_named_types_from_structure(ref_schema, parent_namespace, explicit_name=ref_name)
            return

        # Use explicit name if provided, otherwise get from schema
        name = explicit_name if explicit_name else schema.get('name', '')
        
        # Handle enum keyword
        if 'enum' in schema:
            qualified = self.qualified_name({**schema, 'name': name, 'namespace': parent_namespace}, parent_namespace)
            if qualified not in self.enums:
                self.enums[qualified] = schema
                self.enum_names[qualified] = name
                self.type_order.append(('enum', qualified))
            return

        # Handle type keyword
        struct_type = schema.get('type')
        
        if struct_type == 'object':
            # Process nested properties FIRST to ensure dependencies come before this type
            if 'properties' in schema and isinstance(schema['properties'], dict):
                for prop_name, prop_schema in schema['properties'].items():
                    if isinstance(prop_schema, dict):
                        self.extract_named_types_from_structure(prop_schema, parent_namespace)
            
            # NOW add this type after all dependencies have been processed
            if name:
                qualified = self.qualified_name({**schema, 'name': name, 'namespace': parent_namespace}, parent_namespace)
                if qualified not in self.records:
                    self.records[qualified] = schema
                    self.record_names[qualified] = name
                    self.type_order.append(('record', qualified))
        
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
                        ref_schema = self.resolve_ref(choice_schema['$ref'], schema)
                        if ref_schema:
                            self.extract_named_types_from_structure(ref_schema, parent_namespace)
                    else:
                        self.extract_named_types_from_structure(choice_schema, parent_namespace)

    def generate_graphql(self):
        """
        Generate GraphQL content from the extracted types.

        :return: GraphQL content as a string.
        """
        graphql = []

        # Generate scalars for custom types
        custom_scalars = set()
        
        # Add commonly used custom scalars
        if any('date' in str(record).lower() or 'datetime' in str(record).lower() or 'timestamp' in str(record).lower() 
               for record in self.records.values()):
            custom_scalars.add('scalar Date')
            custom_scalars.add('scalar DateTime')
        
        if any('uuid' in str(record).lower() for record in self.records.values()):
            custom_scalars.add('scalar UUID')
        
        if any('uri' in str(record).lower() or 'url' in str(record).lower() for record in self.records.values()):
            custom_scalars.add('scalar URI')
        
        if any('decimal' in str(record).lower() for record in self.records.values()):
            custom_scalars.add('scalar Decimal')
        
        if any('binary' in str(record).lower() or 'bytes' in str(record).lower() for record in self.records.values()):
            custom_scalars.add('scalar Binary')
        
        # Add JSON scalar for map types
        custom_scalars.add('scalar JSON')
        
        for scalar in sorted(custom_scalars):
            graphql.append(scalar)
        
        if custom_scalars:
            graphql.append('')  # Empty line after scalars

        # Generate types in dependency order
        for type_kind, qualified_name in self.type_order:
            if type_kind == 'enum' and qualified_name in self.enums:
                graphql.append(self.generate_graphql_enum(self.enums[qualified_name]))
            elif type_kind == 'record' and qualified_name in self.records:
                graphql.append(self.generate_graphql_record(self.records[qualified_name]))

        return "\n".join(graphql)

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
                # Use self.schema_doc as context for resolving refs
                context = self.schema_doc[0] if isinstance(self.schema_doc, list) and len(self.schema_doc) > 0 else self.schema_doc
                ref_schema = self.resolve_ref(structure_type['$ref'], context)
                if ref_schema:
                    # First try to get the name from the schema
                    ref_name = ref_schema.get('name')
                    if ref_name:
                        return ref_name
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
            
            # Handle type unions (e.g., ["string", "null"])
            if isinstance(struct_type, list):
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
            "time": "String",
            "datetime": "DateTime",
            "timestamp": "DateTime",
            "duration": "String",
            
            # Other special types
            "uuid": "UUID",
            "uri": "URI",
            "jsonpointer": "String",
            "any": "JSON"
        }
        
        return type_mapping.get(structure_type, 'String')


def convert_structure_to_graphql(structure_schema_path, graphql_schema_path):
    """
    Convert a JSON Structure schema file to a GraphQL schema file.

    :param structure_schema_path: Path to the JSON Structure schema file.
    :param graphql_schema_path: Path to save the GraphQL schema file.
    """
    converter = StructureToGraphQLConverter(structure_schema_path, graphql_schema_path)
    converter.convert()
