""" Convert a JSON Structure schema to a Parquet schema. """

import json
import sys
from typing import Dict, List, Optional, Set, Any
import pyarrow as pa
import pyarrow.parquet as pq

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | bool | int | None


class StructureToParquetConverter:
    """ Class to convert JSON Structure schema to Parquet schema."""

    def __init__(self: 'StructureToParquetConverter'):
        self.named_type_cache: Dict[str, JsonNode] = {}
        self.schema_doc: Optional[Dict[str, Any]] = None
        self.schema_registry: Dict[str, Dict] = {}  # Maps $id URIs to schemas
        self.definitions: Dict[str, Any] = {}

    def get_fullname(self, namespace: str, name: str) -> str:
        """ Get the full name of a type."""
        return f"{namespace}.{name}" if namespace else name

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

    def convert_structure_to_parquet(self, structure_schema_path, structure_record_type, parquet_file_path, emit_cloudevents_columns=False):
        """ Convert a JSON Structure schema to a Parquet schema."""
        schema_file = structure_schema_path
        if not schema_file:
            print("Please specify the JSON Structure schema file")
            sys.exit(1)
        with open(schema_file, "r", encoding="utf-8") as f:
            schema_json = f.read()

        # Parse the schema as a JSON object
        schema = json.loads(schema_json)
        self.schema_doc = schema
        self.cache_named_types(schema)

        # Register schema IDs for external references
        self.register_schema_ids(schema)

        # If we have a list of schemas, find the specified record type
        if isinstance(schema, list) and structure_record_type:
            schema = next(
                (x for x in schema if x.get("name") == structure_record_type or 
                 x.get("namespace", "")+"."+x.get("name", "") == structure_record_type), None)
            if schema is None:
                print(
                    f"No top-level record type {structure_record_type} found in the JSON Structure schema")
                sys.exit(1)
        elif not isinstance(schema, dict):
            print(
                "Expected a single JSON Structure schema as a JSON object, or a list of schema records")
            sys.exit(1)

        # Get the root type
        if 'type' in schema and schema['type'] == 'object':
            # Inline object at root
            table_name = schema.get("name", "Table")
            properties = schema.get("properties", {})
        elif '$root' in schema:
            # Root reference
            root_ref = schema['$root']
            root_schema = self.resolve_ref(root_ref, schema)
            if not root_schema or root_schema.get('type') != 'object':
                print(f"Root type must be an object type")
                sys.exit(1)
            table_name = root_schema.get("name", "Table")
            properties = root_schema.get("properties", {})
        else:
            print("JSON Structure schema must have 'type': 'object' or '$root' property")
            sys.exit(1)

        # Create a list to store the parquet schema
        parquet_schema = []

        # Get required fields
        required_fields = schema.get('required', []) if 'type' in schema else root_schema.get('required', [])

        # Append the parquet schema with the column names and types
        for field_name, field_schema in properties.items():
            is_required = field_name in required_fields
            column_type = self.convert_structure_type_to_parquet_type(field_schema, is_required)
            parquet_schema.append((field_name, column_type))

        if emit_cloudevents_columns:
            parquet_schema.extend([
                ("___type", pa.string()),
                ("___source", pa.string()),
                ("___id", pa.string()),
                ("___time", pa.timestamp('ns')),
                ("___subject", pa.string())
            ])

        # Create an empty table with the schema
        table = pa.Table.from_batches([], schema=pa.schema(parquet_schema))
        pq.write_table(table, parquet_file_path)

    def convert_structure_type_to_parquet_type(self, structure_type, is_required=True):
        """ Convert a JSON Structure type to a Parquet type."""
        if isinstance(structure_type, list):
            # Handle type unions
            non_null_types = [t for t in structure_type if t != 'null']
            has_null = 'null' in structure_type
            
            if len(non_null_types) == 1:
                return self.convert_structure_type_to_parquet_type(non_null_types[0], is_required and not has_null)
            elif len(non_null_types) > 1:
                # Multiple non-null types - create a struct with fields for each type
                struct_fields = []
                for i, struct_union_type in enumerate(non_null_types):
                    field_type = self.convert_structure_type_to_parquet_type(struct_union_type, True)
                    
                    if isinstance(struct_union_type, str):
                        field_name = f'{struct_union_type}Value'
                    elif isinstance(struct_union_type, dict):
                        if 'type' in struct_union_type:
                            type_val = struct_union_type['type']
                            if type_val == 'array':
                                field_name = 'ArrayValue'
                            elif type_val == 'object':
                                field_name = struct_union_type.get('name', f'Object{i}') + 'Value'
                            elif type_val == 'map':
                                field_name = 'MapValue'
                            else:
                                field_name = f'{type_val}Value'
                        else:
                            field_name = f'_{i}'
                    else:
                        field_name = f'_{i}'
                    struct_fields.append(pa.field(field_name, field_type))
                return pa.struct(struct_fields)
            else:
                # Only null type
                return pa.string()
                
        elif isinstance(structure_type, dict):
            # Handle $ref
            if '$ref' in structure_type:
                ref_schema = self.resolve_ref(structure_type['$ref'], self.schema_doc)
                if ref_schema:
                    return self.convert_structure_type_to_parquet_type(ref_schema, is_required)
                return pa.string()

            # Handle type keyword
            if 'type' not in structure_type:
                return pa.string()

            struct_type = structure_type['type']

            # Handle object type
            if struct_type == 'object':
                properties = structure_type.get('properties', {})
                if len(properties) == 0:
                    print(f"WARNING: No properties in object type {structure_type.get('name', 'unnamed')}")
                    return pa.string()
                required = structure_type.get('required', [])
                struct_fields = {}
                for prop_name, prop_schema in properties.items():
                    prop_required = prop_name in required
                    struct_fields[prop_name] = self.convert_structure_type_to_parquet_type(prop_schema, prop_required)
                return pa.struct(struct_fields)
            
            # Handle array type
            elif struct_type == 'array':
                items_type = self.convert_structure_type_to_parquet_type(
                    structure_type.get('items', {'type': 'any'}), True)
                return pa.list_(items_type)
            
            # Handle set type (JSON Structure specific)
            elif struct_type == 'set':
                items_type = self.convert_structure_type_to_parquet_type(
                    structure_type.get('items', {'type': 'any'}), True)
                return pa.list_(items_type)  # Parquet doesn't have native set, use list
            
            # Handle map type
            elif struct_type == 'map':
                values_type = self.convert_structure_type_to_parquet_type(
                    structure_type.get('values', {'type': 'any'}), True)
                return pa.map_(pa.string(), values_type)
            
            # Handle choice type (JSON Structure tagged union)
            elif struct_type == 'choice':
                choices = structure_type.get('choices', {})
                if not choices:
                    return pa.string()
                
                # Create a struct with a field for each choice
                struct_fields = []
                for choice_name, choice_schema in choices.items():
                    choice_type = self.convert_structure_type_to_parquet_type(choice_schema, True)
                    struct_fields.append(pa.field(choice_name, choice_type))
                return pa.struct(struct_fields)
            
            # Handle tuple type (JSON Structure specific)
            elif struct_type == 'tuple':
                elements = structure_type.get('elements', [])
                if not elements:
                    return pa.string()
                
                # Create a struct with indexed fields
                struct_fields = {}
                for i, element_schema in enumerate(elements):
                    element_type = self.convert_structure_type_to_parquet_type(element_schema, True)
                    struct_fields[f'_{i}'] = element_type
                return pa.struct(struct_fields)
            
            # Handle primitive types
            else:
                return self.map_scalar_type(struct_type, structure_type)
                
        elif isinstance(structure_type, str):
            # String type reference
            if structure_type in self.named_type_cache:
                return self.convert_structure_type_to_parquet_type(self.named_type_cache[structure_type], is_required)
            return self.map_scalar_type(structure_type, {})

        return pa.string()

    def map_scalar_type(self, type_name: str, structure_type: Dict) -> pa.DataType:
        """ Map a JSON Structure scalar type to a Parquet scalar type."""
        
        # Handle type annotations
        precision = structure_type.get('precision')
        scale = structure_type.get('scale')
        
        # JSON primitive types
        if type_name == "null":
            return pa.string()
        elif type_name == "boolean":
            return pa.bool_()
        elif type_name == "string":
            # Check for content encoding or format
            content_encoding = structure_type.get('contentEncoding')
            if content_encoding == 'base64':
                return pa.binary()
            return pa.string()
        elif type_name == "number":
            return pa.float64()
        elif type_name == "integer":
            return pa.int32()
        
        # JSON Structure extended primitive types
        elif type_name == "binary":
            return pa.binary()
        elif type_name == "int8":
            return pa.int8()
        elif type_name == "uint8":
            return pa.uint8()
        elif type_name == "int16":
            return pa.int16()
        elif type_name == "uint16":
            return pa.uint16()
        elif type_name == "int32":
            return pa.int32()
        elif type_name == "uint32":
            return pa.uint32()
        elif type_name == "int64":
            return pa.int64()
        elif type_name == "uint64":
            return pa.uint64()
        elif type_name == "int128":
            return pa.decimal128(38, 0)  # Use decimal for int128
        elif type_name == "uint128":
            return pa.decimal128(39, 0)  # Use decimal for uint128
        elif type_name in ["float8", "float", "binary32"]:
            return pa.float32()
        elif type_name in ["double", "binary64"]:
            return pa.float64()
        elif type_name == "decimal":
            if precision and scale is not None:
                return pa.decimal128(precision, scale)
            return pa.decimal128(38, 18)  # Default precision and scale
        elif type_name == "date":
            return pa.date32()
        elif type_name == "time":
            return pa.time64('ns')
        elif type_name in ["datetime", "timestamp"]:
            return pa.timestamp('ns')
        elif type_name == "duration":
            return pa.duration('ns')
        elif type_name == "uuid":
            return pa.string()  # Store UUID as string
        elif type_name == "uri":
            return pa.string()
        elif type_name == "jsonpointer":
            return pa.string()
        elif type_name == "any":
            return pa.string()  # Store as JSON string
        else:
            # Unknown type, default to string
            return pa.string()

    def cache_named_types(self, structure_type):
        """ Add an encountered type to the list of types."""
        if isinstance(structure_type, list):
            for item in structure_type:
                self.cache_named_types(item)
        elif isinstance(structure_type, dict):
            # Cache named types with their full name
            if structure_type.get("name"):
                namespace = structure_type.get("namespace", "")
                fullname = self.get_fullname(namespace, structure_type["name"])
                self.named_type_cache[fullname] = structure_type
            
            # Process definitions
            if "definitions" in structure_type:
                for def_name, def_schema in self._flatten_definitions(structure_type["definitions"]).items():
                    if isinstance(def_schema, dict):
                        namespace = def_schema.get("namespace", "")
                        name = def_schema.get("name", def_name.split('/')[-1])
                        fullname = self.get_fullname(namespace, name)
                        self.named_type_cache[fullname] = def_schema
            
            # Recursively cache nested types
            if "properties" in structure_type:
                for prop_schema in structure_type["properties"].values():
                    if isinstance(prop_schema, dict):
                        self.cache_named_types(prop_schema)
            
            for key in ["items", "values", "elements"]:
                if key in structure_type:
                    self.cache_named_types(structure_type[key])

    def _flatten_definitions(self, definitions: Dict[str, Any], prefix: str = '') -> Dict[str, Dict[str, Any]]:
        """
        Flatten nested definitions into a flat dictionary with paths as keys.
        """
        flattened = {}
        
        for key, value in definitions.items():
            path = f"{prefix}/{key}" if prefix else key
            
            if isinstance(value, dict):
                # Check if this is a type definition
                if 'type' in value or 'oneOf' in value or 'allOf' in value or '$ref' in value:
                    flattened[path] = value
                else:
                    # It's a namespace, recurse
                    flattened.update(self._flatten_definitions(value, path))
        
        return flattened

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

        for key in ['items', 'values', 'additionalProperties', 'elements']:
            if key in schema and isinstance(schema[key], dict):
                self.register_schema_ids(schema[key], base_uri)


def convert_structure_to_parquet(structure_schema_path, structure_record_type, parquet_file_path, emit_cloudevents_columns=False):
    """ Convert a JSON Structure schema to a Parquet schema."""
    converter = StructureToParquetConverter()
    converter.convert_structure_to_parquet(
        structure_schema_path, structure_record_type, parquet_file_path, emit_cloudevents_columns)
