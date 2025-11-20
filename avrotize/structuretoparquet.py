""" Convert a JSON Structure schema to a Parquet schema. """

import json
import sys
from typing import Dict, List, Optional, Any
import pyarrow as pa
import pyarrow.parquet as pq

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | bool | int | None


class StructureToParquetConverter:
    """ Class to convert JSON Structure schema to Parquet schema."""

    def __init__(self: 'StructureToParquetConverter'):
        self.named_type_cache: Dict[str, JsonNode] = {}
        self.schema_registry: Dict[str, Dict] = {}

    def get_fullname(self, namespace: str, name: str) -> str:
        """ Get the full name of a record type."""
        return f"{namespace}.{name}" if namespace else name

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
        self.cache_named_types(schema)
        self.register_schema_ids(schema)

        # Handle list of schemas or single schema
        if isinstance(schema, list):
            if structure_record_type:
                schema = next(
                    (x for x in schema if x.get("name") == structure_record_type or 
                     f"{x.get('namespace', '')}.{x.get('name', '')}" == structure_record_type), None)
                if schema is None:
                    print(f"No top-level record type {structure_record_type} found in the JSON Structure schema")
                    sys.exit(1)
            else:
                # Use first schema in list
                schema = schema[0]
        
        # Handle $root reference
        if "$root" in schema:
            root_ref = schema["$root"]
            schema = self.resolve_ref(root_ref, schema)
            if schema is None:
                print(f"Cannot resolve $root reference: {root_ref}")
                sys.exit(1)

        if not isinstance(schema, dict):
            print("Expected a JSON Structure schema as a JSON object, or a list of schema records")
            sys.exit(1)

        # Get the name and fields of the top-level record
        table_name = schema.get("name", "table")
        
        # Ensure we have an object type with properties
        if schema.get("type") != "object" or "properties" not in schema:
            print("JSON Structure schema must have type 'object' with 'properties'")
            sys.exit(1)
        
        properties = schema["properties"]

        # Create a list to store the parquet schema
        parquet_schema = []

        # Append the parquet schema with the column names and types
        for field_name, field_schema in properties.items():
            column_type = self.convert_structure_type_to_parquet_type(field_schema, schema.get("required", []))
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

    def resolve_ref(self, ref: str, context_schema: Optional[Dict] = None) -> Optional[Dict]:
        """ Resolves a $ref to the actual schema definition """
        # Check if it's an absolute URI reference (schema with $id)
        if not ref.startswith('#/'):
            # Try to resolve from schema registry
            if ref in self.schema_registry:
                return self.schema_registry[ref]
            return None
        
        # Handle fragment-only references (internal to document)
        path = ref[2:].split('/')
        schema = context_schema
        
        for part in path:
            if not isinstance(schema, dict) or part not in schema:
                return None
            schema = schema[part]
        
        return schema

    def register_schema_ids(self, schema: Dict, base_uri: str = '') -> None:
        """ Recursively registers schemas with $id keywords """
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
            base_uri = schema_id  # Update base URI for nested schemas
        
        # Recursively process definitions
        if 'definitions' in schema:
            for def_name, def_schema in schema['definitions'].items():
                if isinstance(def_schema, dict):
                    self.register_schema_ids(def_schema, base_uri)
                else:
                    # Nested namespace structure
                    self._register_nested_definitions(def_schema, base_uri)
        
        # Recursively process properties
        if 'properties' in schema:
            for prop_name, prop_schema in schema['properties'].items():
                if isinstance(prop_schema, dict):
                    self.register_schema_ids(prop_schema, base_uri)
        
        # Recursively process items, values, etc.
        for key in ['items', 'values', 'additionalProperties']:
            if key in schema and isinstance(schema[key], dict):
                self.register_schema_ids(schema[key], base_uri)

    def _register_nested_definitions(self, definitions: Any, base_uri: str = '') -> None:
        """ Recursively registers nested definition structures """
        if isinstance(definitions, dict):
            for key, value in definitions.items():
                if isinstance(value, dict):
                    if 'type' in value or '$ref' in value:
                        # This is a schema definition
                        self.register_schema_ids(value, base_uri)
                    else:
                        # This is a namespace level, recurse
                        self._register_nested_definitions(value, base_uri)

    def convert_structure_type_to_parquet_type(self, structure_type: JsonNode, required_fields: List[str] = None):
        """ Convert a JSON Structure type to a Parquet type."""
        if required_fields is None:
            required_fields = []
            
        if isinstance(structure_type, str):
            # Simple type reference
            return self.map_scalar_type(structure_type)
        elif isinstance(structure_type, list):
            # Type union - handle optional types (union with null)
            item_count = len(structure_type)
            if item_count == 1:
                return self.convert_structure_type_to_parquet_type(structure_type[0], required_fields)
            elif item_count == 2:
                first = structure_type[0]
                second = structure_type[1]
                if isinstance(first, str) and first == "null":
                    return self.convert_structure_type_to_parquet_type(second, required_fields)
                elif isinstance(second, str) and second == "null":
                    return self.convert_structure_type_to_parquet_type(first, required_fields)
                else:
                    # Non-null union - use struct representation
                    struct_fields = self.map_union_fields(structure_type)
                    return pa.struct(struct_fields)
            elif item_count > 0:
                # Multi-type union
                struct_fields = self.map_union_fields(structure_type)
                return pa.struct(struct_fields)
            else:
                print(f"WARNING: Empty union type {structure_type}")
                return pa.string()
        elif isinstance(structure_type, dict):
            # Handle $ref
            if '$ref' in structure_type:
                ref_schema = self.resolve_ref(structure_type['$ref'])
                if ref_schema:
                    return self.convert_structure_type_to_parquet_type(ref_schema, required_fields)
                return pa.string()
            
            # Get the type
            type_name = structure_type.get("type")
            
            if type_name == "object":
                # Nested object - convert to struct
                properties = structure_type.get("properties", {})
                if len(properties) == 0:
                    print(f"WARNING: No properties in object type")
                    return pa.string()
                nested_required = structure_type.get("required", [])
                return pa.struct({
                    prop_name: self.convert_structure_type_to_parquet_type(prop_schema, nested_required)
                    for prop_name, prop_schema in properties.items()
                })
            elif type_name == "array":
                items_schema = structure_type.get("items", {"type": "any"})
                return pa.list_(self.convert_structure_type_to_parquet_type(items_schema, required_fields))
            elif type_name == "set":
                # Sets are represented as arrays in Parquet
                items_schema = structure_type.get("items", {"type": "any"})
                return pa.list_(self.convert_structure_type_to_parquet_type(items_schema, required_fields))
            elif type_name == "map":
                values_schema = structure_type.get("values", {"type": "any"})
                return pa.map_(pa.string(), self.convert_structure_type_to_parquet_type(values_schema, required_fields))
            elif type_name == "tuple":
                # Tuples are represented as structs with positional field names
                properties = structure_type.get("properties", {})
                tuple_order = structure_type.get("tuple", list(properties.keys()))
                return pa.struct({
                    prop_name: self.convert_structure_type_to_parquet_type(properties[prop_name], required_fields)
                    for prop_name in tuple_order if prop_name in properties
                })
            elif type_name == "choice":
                # Choice types (discriminated unions) - represent as struct
                choices = structure_type.get("choices", {})
                struct_fields = []
                for choice_name, choice_schema in choices.items():
                    choice_type = self.convert_structure_type_to_parquet_type(choice_schema, required_fields)
                    struct_fields.append(pa.field(choice_name, choice_type))
                # Add discriminator field if selector is specified
                selector = structure_type.get("selector", "type")
                if selector not in [f.name for f in struct_fields]:
                    struct_fields.insert(0, pa.field(selector, pa.string()))
                return pa.struct(struct_fields)
            elif type_name in ["string", "number", "integer", "boolean", "null", "binary",
                             "int8", "uint8", "int16", "uint16", "int32", "uint32", "int64", "uint64",
                             "int128", "uint128", "float8", "float", "double", "binary32", "binary64",
                             "decimal", "date", "time", "datetime", "timestamp", "duration", 
                             "uuid", "uri", "jsonpointer", "any"]:
                # Handle annotations
                result_type = self.map_scalar_type(type_name)
                
                # Handle decimal with precision/scale
                if type_name == "decimal":
                    precision = structure_type.get("precision", 38)
                    scale = structure_type.get("scale", 18)
                    return pa.decimal128(precision, scale)
                
                return result_type
            else:
                # Unknown type
                return self.map_scalar_type(type_name)
        
        return pa.string()

    def cache_named_types(self, structure_type):
        """ Add an encountered type to the list of types."""
        if isinstance(structure_type, list):
            for item in structure_type:
                self.cache_named_types(item)
        if isinstance(structure_type, dict):
            if structure_type.get("name"):
                namespace = structure_type.get("namespace", "")
                self.named_type_cache[self.get_fullname(namespace, structure_type.get("name"))] = structure_type
            if "properties" in structure_type:
                for field_name, field_schema in structure_type.get("properties", {}).items():
                    if isinstance(field_schema, dict):
                        self.cache_named_types(field_schema)
            if "definitions" in structure_type:
                self._cache_nested_definitions(structure_type["definitions"])

    def _cache_nested_definitions(self, definitions: Any) -> None:
        """ Recursively caches nested definition structures """
        if isinstance(definitions, dict):
            for key, value in definitions.items():
                if isinstance(value, dict):
                    if 'type' in value or '$ref' in value or 'name' in value:
                        # This is a schema definition
                        self.cache_named_types(value)
                    else:
                        # This is a namespace level, recurse
                        self._cache_nested_definitions(value)

    def map_union_fields(self, structure_type):
        """ Map the fields of a union type to Parquet fields."""
        struct_fields = []
        for i, union_type in enumerate(structure_type):
            field_type = self.convert_structure_type_to_parquet_type(union_type)
            if isinstance(union_type, str):
                if "null" == union_type:
                    continue
                if union_type in self.named_type_cache:
                    union_type = self.named_type_cache[union_type]
            if isinstance(union_type, str):
                field_name = f'{union_type}Value'
            elif isinstance(union_type, dict):
                type_val = union_type.get("type")
                if type_val == "array":
                    field_name = 'arrayValue'
                elif type_val == "set":
                    field_name = 'setValue'
                elif type_val == "map":
                    field_name = 'mapValue'
                elif type_val == "object":
                    field_name = union_type.get("name", f'objectValue_{i}')
                elif "name" in union_type:
                    field_name = f'{union_type.get("name")}Value'
                else:
                    field_name = f'_{i}'
            else:
                field_name = f'_{i}'
            struct_fields.append(pa.field(field_name, field_type))
        return struct_fields

    def map_scalar_type(self, type_name: str):
        """ Map a JSON Structure scalar type to a Parquet scalar type."""
        # JSON primitive types
        if type_name == "null":
            return pa.string()
        elif type_name == "boolean":
            return pa.bool_()
        elif type_name == "string":
            return pa.string()
        elif type_name == "integer":
            return pa.int64()  # Default integer mapping
        elif type_name == "number":
            return pa.float64()  # Default number mapping
        # Extended primitive types
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
            # PyArrow doesn't have int128, use string representation
            return pa.string()
        elif type_name == "uint128":
            # PyArrow doesn't have uint128, use string representation
            return pa.string()
        elif type_name == "float8":
            return pa.float32()  # Approximation
        elif type_name == "float":
            return pa.float32()
        elif type_name == "double":
            return pa.float64()
        elif type_name == "binary32":
            return pa.float32()
        elif type_name == "binary64":
            return pa.float64()
        elif type_name == "decimal":
            return pa.decimal128(38, 18)  # Default precision and scale
        elif type_name == "date":
            return pa.date32()
        elif type_name == "time":
            return pa.time64('ns')
        elif type_name == "datetime":
            return pa.timestamp('ns')
        elif type_name == "timestamp":
            return pa.timestamp('ns')
        elif type_name == "duration":
            return pa.duration('ns')
        elif type_name == "uuid":
            return pa.string()  # UUID as string
        elif type_name == "uri":
            return pa.string()
        elif type_name == "jsonpointer":
            return pa.string()
        elif type_name == "any":
            return pa.string()  # Any type as string (could be JSON)
        else:
            # Unknown type, default to string
            return pa.string()


def convert_structure_to_parquet(structure_schema_path, structure_record_type, parquet_file_path, emit_cloudevents_columns=False):
    """ Convert a JSON Structure schema to a Parquet schema."""
    converter = StructureToParquetConverter()
    converter.convert_structure_to_parquet(
        structure_schema_path, structure_record_type, parquet_file_path, emit_cloudevents_columns)
