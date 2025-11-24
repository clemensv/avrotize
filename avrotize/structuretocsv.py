# coding: utf-8
"""
Module to convert JSON Structure schema to CSV schema.
"""

import json
import os
from typing import Any, Dict, List, Optional, Union

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | None


class StructureToCSVConverter:
    """
    Class to convert JSON Structure schema to CSV schema.
    """

    def __init__(self, structure_schema_path: str, csv_schema_path: str):
        """
        Initialize the converter with file paths.

        :param structure_schema_path: Path to the JSON Structure schema file.
        :param csv_schema_path: Path to save the CSV schema file.
        """
        self.structure_schema_path = structure_schema_path
        self.csv_schema_path = csv_schema_path
        self.schema_doc: Optional[Dict] = None
        self.schema_registry: Dict[str, Dict] = {}
        self.definitions: Dict[str, Any] = {}

    def convert(self):
        """
        Convert JSON Structure schema to CSV schema and save to file.
        """
        with open(self.structure_schema_path, 'r', encoding='utf-8') as file:
            structure_schema = json.load(file)

        # Store schema for reference resolution
        self.schema_doc = structure_schema
        
        # Register schema IDs for $ref resolution
        self.register_schema_ids(structure_schema)
        
        # Store definitions
        if 'definitions' in structure_schema:
            self.definitions = structure_schema['definitions']

        csv_schema = self.convert_structure_to_csv_schema(structure_schema)

        with open(self.csv_schema_path, 'w', encoding='utf-8') as file:
            json.dump(csv_schema, file, indent=2)

    def register_schema_ids(self, schema: Dict, base_uri: str = '') -> None:
        """
        Recursively registers schemas with $id keywords for cross-references.
        """
        if not isinstance(schema, dict):
            return

        # Register this schema if it has an $id
        if '$id' in schema:
            schema_id = schema['$id']
            # Handle relative URIs
            if base_uri and not schema_id.startswith(('http://', 'https://', 'urn:')):
                from urllib.parse import urljoin
                schema_id = urljoin(base_uri, schema_id)
            self.schema_registry[schema_id] = schema
            base_uri = schema_id

        # Recursively process definitions
        if 'definitions' in schema:
            for def_name, def_schema in schema['definitions'].items():
                if isinstance(def_schema, dict):
                    self.register_schema_ids(def_schema, base_uri)

        # Recursively process properties
        if 'properties' in schema:
            for prop_name, prop_schema in schema['properties'].items():
                if isinstance(prop_schema, dict):
                    self.register_schema_ids(prop_schema, base_uri)

        # Recursively process items, values, etc.
        for key in ['items', 'values', 'additionalProperties']:
            if key in schema and isinstance(schema[key], dict):
                self.register_schema_ids(schema[key], base_uri)

    def resolve_ref(self, ref: str, context_schema: Optional[Dict] = None) -> Optional[Dict]:
        """
        Resolves a $ref to the actual schema definition.
        """
        # Check if it's an absolute URI reference (schema with $id)
        if not ref.startswith('#/'):
            # Try to resolve from schema registry
            if ref in self.schema_registry:
                return self.schema_registry[ref]
            return None

        # Handle fragment-only references (internal to document)
        path = ref[2:].split('/')
        schema = context_schema if context_schema else self.schema_doc

        for part in path:
            if not isinstance(schema, dict) or part not in schema:
                return None
            schema = schema[part]

        return schema

    def convert_structure_to_csv_schema(self, structure_schema: Dict) -> Dict:
        """
        Convert a JSON Structure schema to a CSV schema.

        :param structure_schema: JSON Structure schema as a dictionary.
        :return: CSV schema as a dictionary.
        """
        csv_schema = {
            "fields": []
        }

        # Handle object type with properties
        if structure_schema.get('type') == 'object' and 'properties' in structure_schema:
            for prop_name, prop_schema in structure_schema['properties'].items():
                csv_field = self.convert_structure_field_to_csv_field(prop_name, prop_schema, structure_schema)
                csv_schema['fields'].append(csv_field)

        # Handle definitions
        elif 'definitions' in structure_schema:
            # Look for a root type or the first object type in definitions
            root_ref = structure_schema.get('$root')
            if root_ref:
                root_schema = self.resolve_ref(root_ref, structure_schema)
                if root_schema and root_schema.get('type') == 'object':
                    return self.convert_structure_to_csv_schema(root_schema)

            # Otherwise, try to find the first object type
            for def_name, def_schema in structure_schema.get('definitions', {}).items():
                if isinstance(def_schema, dict) and def_schema.get('type') == 'object':
                    return self.convert_structure_to_csv_schema(def_schema)

        return csv_schema

    def convert_structure_field_to_csv_field(self, field_name: str, field_schema: Dict, parent_schema: Dict) -> Dict:
        """
        Convert a JSON Structure field to a CSV field.

        :param field_name: Name of the field.
        :param field_schema: JSON Structure field schema as a dictionary.
        :param parent_schema: Parent schema for context (e.g., required fields).
        :return: CSV field as a dictionary.
        """
        csv_field = {
            "name": field_name,
            "type": self.convert_structure_type_to_csv_type(field_schema)
        }

        # Handle description
        if 'description' in field_schema:
            csv_field['description'] = field_schema['description']
        elif 'doc' in field_schema:
            csv_field['description'] = field_schema['doc']

        # Handle default values
        if 'default' in field_schema:
            csv_field['default'] = field_schema['default']

        # Handle const values
        if 'const' in field_schema:
            csv_field['const'] = field_schema['const']

        # Check if field is required
        required_props = parent_schema.get('required', [])
        is_required = field_name in required_props if isinstance(required_props, list) else False
        
        # Set nullable based on required status and type
        if not is_required:
            csv_field['nullable'] = True

        # Handle enum values
        if 'enum' in field_schema:
            csv_field['enum'] = field_schema['enum']

        # Handle format/constraints for specific types
        self.add_type_constraints(csv_field, field_schema)

        return csv_field

    def add_type_constraints(self, csv_field: Dict, field_schema: Dict) -> None:
        """
        Add type-specific constraints to CSV field based on JSON Structure annotations.
        """
        # String constraints
        if 'maxLength' in field_schema:
            csv_field['maxLength'] = field_schema['maxLength']
        if 'minLength' in field_schema:
            csv_field['minLength'] = field_schema['minLength']
        if 'pattern' in field_schema:
            csv_field['pattern'] = field_schema['pattern']

        # Numeric constraints
        if 'minimum' in field_schema:
            csv_field['minimum'] = field_schema['minimum']
        if 'maximum' in field_schema:
            csv_field['maximum'] = field_schema['maximum']
        if 'exclusiveMinimum' in field_schema:
            csv_field['exclusiveMinimum'] = field_schema['exclusiveMinimum']
        if 'exclusiveMaximum' in field_schema:
            csv_field['exclusiveMaximum'] = field_schema['exclusiveMaximum']

        # Decimal/numeric precision
        if 'precision' in field_schema:
            csv_field['precision'] = field_schema['precision']
        if 'scale' in field_schema:
            csv_field['scale'] = field_schema['scale']

        # Content encoding
        if 'contentEncoding' in field_schema:
            csv_field['contentEncoding'] = field_schema['contentEncoding']

    def convert_structure_type_to_csv_type(self, field_schema: Union[Dict, str, List]) -> str:
        """
        Convert a JSON Structure type to a CSV type.

        :param field_schema: JSON Structure type as a string, dict, or list (for unions).
        :return: CSV type as a string.
        """
        # Handle union types (array of types)
        if isinstance(field_schema, list):
            # Filter out null
            non_null_types = [t for t in field_schema if t != 'null']
            if len(non_null_types) == 1:
                # Simple nullable type
                return self.convert_structure_type_to_csv_type(non_null_types[0])
            else:
                # Complex union - use string as fallback
                return "string"

        # Handle string primitive types
        if isinstance(field_schema, str):
            return self.map_primitive_type_to_csv(field_schema)

        # Handle dict/object schemas
        if isinstance(field_schema, dict):
            # Handle $ref
            if '$ref' in field_schema:
                ref_schema = self.resolve_ref(field_schema['$ref'], self.schema_doc)
                if ref_schema:
                    return self.convert_structure_type_to_csv_type(ref_schema)
                return 'string'

            # Handle enum
            if 'enum' in field_schema:
                # Determine enum base type
                base_type = field_schema.get('type', 'string')
                return self.map_primitive_type_to_csv(base_type)

            # Handle type keyword
            if 'type' not in field_schema:
                return 'string'

            struct_type = field_schema['type']
            
            # Handle union types when type is a list within the dict
            if isinstance(struct_type, list):
                non_null_types = [t for t in struct_type if t != 'null']
                if len(non_null_types) == 1:
                    # Simple nullable type
                    return self.map_primitive_type_to_csv(non_null_types[0])
                else:
                    # Complex union - use string as fallback
                    return "string"

            # Handle compound types
            if struct_type == 'array':
                return 'string'  # CSV doesn't have native array support
            elif struct_type == 'set':
                return 'string'
            elif struct_type == 'map':
                return 'string'  # Maps become JSON strings in CSV
            elif struct_type == 'object':
                return 'string'  # Nested objects become JSON strings
            elif struct_type == 'choice':
                return 'string'  # Choice types become strings
            elif struct_type == 'tuple':
                return 'string'  # Tuples become strings
            else:
                # Primitive type specified in type field
                return self.map_primitive_type_to_csv(struct_type)

        return 'string'

    def map_primitive_type_to_csv(self, structure_type: str) -> str:
        """
        Maps JSON Structure primitive types to CSV types.

        :param structure_type: JSON Structure type name.
        :return: CSV type name.
        """
        type_mapping = {
            # JSON primitive types
            'null': 'string',
            'boolean': 'boolean',
            'string': 'string',
            'integer': 'integer',
            'number': 'number',
            
            # Extended integer types
            'int8': 'integer',
            'uint8': 'integer',
            'int16': 'integer',
            'uint16': 'integer',
            'int32': 'integer',
            'uint32': 'integer',
            'int64': 'integer',
            'uint64': 'integer',
            'int128': 'integer',
            'uint128': 'integer',
            
            # Floating point types
            'float8': 'number',
            'float': 'number',
            'double': 'number',
            'binary32': 'number',
            'binary64': 'number',
            'decimal': 'number',
            
            # Binary data
            'binary': 'string',  # Base64 encoded
            
            # Date/time types
            'date': 'string',
            'time': 'string',
            'datetime': 'string',
            'timestamp': 'string',
            'duration': 'string',
            
            # Special types
            'uuid': 'string',
            'uri': 'string',
            'jsonpointer': 'string',
            'any': 'string',
            
            # Compound types (when referenced directly)
            'object': 'string',
            'array': 'string',
            'set': 'string',
            'map': 'string',
            'choice': 'string',
            'tuple': 'string',
        }

        return type_mapping.get(structure_type, 'string')


def convert_structure_to_csv_schema(structure_schema_path: str, csv_schema_path: str):
    """
    Convert a JSON Structure schema file to a CSV schema file.

    :param structure_schema_path: Path to the JSON Structure schema file.
    :param csv_schema_path: Path to save the CSV schema file.
    """
    if not os.path.exists(structure_schema_path):
        raise FileNotFoundError(f"JSON Structure schema file not found: {structure_schema_path}")

    converter = StructureToCSVConverter(structure_schema_path, csv_schema_path)
    converter.convert()
