"""Convert JSON Structure schemas to Data Package format."""

import json
import sys
from typing import Dict, List, Optional, Set, cast
from datapackage import Package

from avrotize.common import get_longest_namespace_prefix

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | bool | int | None


class StructureToDataPackageConverter:
    """Class to convert JSON Structure schema to Data Package."""

    def __init__(self) -> None:
        self.named_type_cache: Dict[str, JsonNode] = {}
        self.schema_registry: Dict[str, Dict] = {}

    def get_fullname(self, namespace: str, name: str) -> str:
        """Get the full name of a record type."""
        return f"{namespace}.{name}" if namespace else name

    def resolve_ref(self, ref: str, context_schema: Optional[Dict] = None) -> Optional[Dict]:
        """Resolves a $ref to the actual schema definition."""
        # Check if it's an absolute URI reference (schema with $id)
        if not ref.startswith('#/'):
            if ref in self.schema_registry:
                return self.schema_registry[ref]
            return None
        
        # Handle fragment-only references (internal to document)
        path = ref[2:].split('/')
        schema = context_schema if context_schema else None
        
        if schema is None:
            return None
            
        for part in path:
            if not isinstance(schema, dict) or part not in schema:
                return None
            schema = schema[part]
        
        return cast(Dict, schema)

    def register_schema_ids(self, schema: Dict, base_uri: str = '') -> None:
        """Recursively registers schemas with $id keywords."""
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

    def convert_structure_to_datapackage(self, structure_schema_path: str, 
                                        structure_record_type: Optional[str], 
                                        datapackage_path: str) -> None:
        """Convert a JSON Structure schema to a Data Package."""
        with open(structure_schema_path, "r", encoding="utf-8") as f:
            schema_json = f.read()

        # Parse the schema as a JSON object
        schema = json.loads(schema_json)
        
        # Register schema IDs for cross-references
        if isinstance(schema, dict):
            self.register_schema_ids(schema)
        elif isinstance(schema, list):
            for s in schema:
                if isinstance(s, dict):
                    self.register_schema_ids(s)
        
        self.cache_named_types(schema)

        # Handle list of schemas or single schema
        if isinstance(schema, list):
            if structure_record_type:
                schema = next(
                    (x for x in schema 
                     if isinstance(x, dict) and 
                     (x.get("name") == structure_record_type or 
                      str(x.get("namespace", "")) + "." + str(x.get("name", "")) == structure_record_type)), 
                    None)
                if schema is None:
                    print(f"No top-level record type {structure_record_type} found in the JSON Structure schema")
                    sys.exit(1)
            schemas_to_convert = schema if isinstance(schema, list) else [schema]
        elif isinstance(schema, dict):
            # Single schema - convert it to a list
            if 'type' in schema and schema['type'] == 'object':
                schemas_to_convert = [schema]
            elif 'definitions' in schema:
                # Schema with definitions - extract object types from definitions
                schemas_to_convert = []
                self._extract_object_schemas(schema.get('definitions', {}), schemas_to_convert)
                # Also include root if it's an object
                if schema.get('type') == 'object':
                    schemas_to_convert.insert(0, schema)
            else:
                schemas_to_convert = [schema]
        else:
            print("Expected a single JSON Structure schema as a JSON object, or a list of schema records")
            sys.exit(1)

        # Calculate longest namespace prefix
        longest_namespace_prefix = self._get_longest_namespace_prefix(schemas_to_convert)
        self.create_datapackage_for_schemas(schemas_to_convert, datapackage_path, longest_namespace_prefix)

    def _extract_object_schemas(self, definitions: Dict, schemas_to_convert: List[Dict]) -> None:
        """Extract object type schemas from definitions recursively."""
        for name, definition in definitions.items():
            if isinstance(definition, dict):
                if definition.get('type') == 'object':
                    # Add name if not present
                    if 'name' not in definition:
                        definition['name'] = name
                    schemas_to_convert.append(definition)
                elif 'definitions' in definition:
                    # Nested definitions
                    self._extract_object_schemas(definition['definitions'], schemas_to_convert)

    def _get_longest_namespace_prefix(self, schemas: List[Dict]) -> str:
        """Calculate the longest common namespace prefix from schemas."""
        if not schemas:
            return ""
        
        namespaces = []
        for schema in schemas:
            if isinstance(schema, dict):
                ns = schema.get('namespace', '')
                if ns:
                    namespaces.append(ns)
        
        if not namespaces:
            return ""
        
        # Find common prefix
        if len(namespaces) == 1:
            return namespaces[0]
        
        # Split by dots and find common parts
        parts_lists = [ns.split('.') for ns in namespaces]
        common_parts = []
        
        for i in range(min(len(p) for p in parts_lists)):
            part = parts_lists[0][i]
            if all(p[i] == part for p in parts_lists):
                common_parts.append(part)
            else:
                break
        
        return '.'.join(common_parts)

    def create_datapackage_for_schemas(self, schemas: List[Dict], 
                                      datapackage_path: str, 
                                      namespace_prefix: str) -> None:
        """Create a Data Package for given schemas."""
        package = Package()
        data_package_resources = []

        for schema in schemas:
            if not isinstance(schema, dict):
                continue
                
            # Skip non-object types
            if schema.get('type') != 'object':
                continue
                
            name = str(schema.get("name", "UnnamedTable"))
            namespace = str(schema.get("namespace", ""))
            
            # Remove common namespace prefix
            if namespace.startswith(namespace_prefix):
                namespace = namespace[len(namespace_prefix):].strip(".")
            
            table_name = f"{namespace}_{name}" if namespace else name
            properties = schema.get("properties", {})

            # Create the Data Package schema
            resource_schema: Dict[str, List[JsonNode]] = {
                "fields": []
            }

            for prop_name, prop_schema in properties.items():
                column_name = prop_name
                column_type = self.convert_structure_type_to_datapackage_type(prop_schema, schema)
                field_schema = {"name": column_name, "type": column_type}
                
                # Add description from doc or description (only if prop_schema is a dict)
                if isinstance(prop_schema, dict):
                    if "description" in prop_schema:
                        field_schema["description"] = prop_schema["description"]
                    elif "doc" in prop_schema:
                        field_schema["description"] = prop_schema["doc"]
                    
                    # Add format constraints if applicable
                    self._add_field_constraints(field_schema, prop_schema)
                
                resource_schema["fields"].append(field_schema)

            resource = {
                "name": table_name,
                "data": [],  # Empty data array for schema-only package
                "schema": resource_schema
            }
            
            # Add resource description if available
            if "description" in schema:
                resource["description"] = schema["description"]
            elif "doc" in schema:
                resource["description"] = schema["doc"]
            
            data_package_resources.append(resource)

        # Add resources to the Data Package
        for resource in data_package_resources:
            package.add_resource(resource)

        # Save the Data Package
        package.descriptor["name"] = namespace_prefix if namespace_prefix else "datapackage"
        package.commit()

        with open(datapackage_path, "w", encoding="utf-8") as f:
            f.write(json.dumps(package.descriptor, indent=2))

    def _add_field_constraints(self, field_schema: Dict, prop_schema: Dict) -> None:
        """Add Data Package field constraints from JSON Structure annotations."""
        # Add format for specific types
        prop_type = prop_schema.get('type', '')
        
        # Date/time formats
        if prop_type == 'date':
            field_schema['format'] = 'date'
        elif prop_type == 'datetime' or prop_type == 'timestamp':
            field_schema['format'] = 'datetime'
        elif prop_type == 'time':
            field_schema['format'] = 'time'
        elif prop_type == 'duration':
            field_schema['format'] = 'duration'
        elif prop_type == 'uri':
            field_schema['format'] = 'uri'
        elif prop_type == 'uuid':
            field_schema['format'] = 'uuid'
        elif prop_type == 'binary':
            field_schema['format'] = 'binary'
        
        # String constraints
        if 'maxLength' in prop_schema:
            if 'constraints' not in field_schema:
                field_schema['constraints'] = {}
            field_schema['constraints']['maxLength'] = prop_schema['maxLength']
        
        if 'minLength' in prop_schema:
            if 'constraints' not in field_schema:
                field_schema['constraints'] = {}
            field_schema['constraints']['minLength'] = prop_schema['minLength']
        
        if 'pattern' in prop_schema:
            if 'constraints' not in field_schema:
                field_schema['constraints'] = {}
            field_schema['constraints']['pattern'] = prop_schema['pattern']
        
        # Numeric constraints
        if 'minimum' in prop_schema:
            if 'constraints' not in field_schema:
                field_schema['constraints'] = {}
            field_schema['constraints']['minimum'] = prop_schema['minimum']
        
        if 'maximum' in prop_schema:
            if 'constraints' not in field_schema:
                field_schema['constraints'] = {}
            field_schema['constraints']['maximum'] = prop_schema['maximum']
        
        # Enum values
        if 'enum' in prop_schema:
            if 'constraints' not in field_schema:
                field_schema['constraints'] = {}
            field_schema['constraints']['enum'] = prop_schema['enum']

    def convert_structure_type_to_datapackage_type(self, structure_type: JsonNode, 
                                                   context_schema: Optional[Dict] = None) -> str:
        """Convert a JSON Structure type to a Data Package type."""
        if isinstance(structure_type, list):
            # Union type
            item_count = len(structure_type)
            if item_count == 1:
                return self.convert_structure_type_to_datapackage_type(structure_type[0], context_schema)
            elif item_count == 2:
                # Check for nullable union (type + null)
                first = structure_type[0]
                second = structure_type[1]
                if isinstance(first, str) and first == "null":
                    return self.convert_structure_type_to_datapackage_type(second, context_schema)
                elif isinstance(second, str) and second == "null":
                    return self.convert_structure_type_to_datapackage_type(first, context_schema)
            # Complex union - default to string
            return "string"
        elif isinstance(structure_type, dict):
            # Handle $ref
            if '$ref' in structure_type:
                ref_schema = self.resolve_ref(structure_type['$ref'], context_schema)
                if ref_schema:
                    return self.convert_structure_type_to_datapackage_type(ref_schema, context_schema)
                return "string"
            
            # Handle enum
            if 'enum' in structure_type:
                # Enums are represented as strings with enum constraint
                return "string"
            
            # Get the type field
            type_name = structure_type.get("type")
            
            # Handle case where type itself is a dict with $ref
            if isinstance(type_name, dict):
                return self.convert_structure_type_to_datapackage_type(type_name, context_schema)
            
            if type_name == "array":
                return "array"
            elif type_name == "set":
                return "array"  # Sets are represented as arrays in Data Package
            elif type_name == "map":
                return "object"
            elif type_name == "object":
                return "object"
            elif type_name == "choice":
                return "string"  # Choices default to string
            elif type_name == "tuple":
                return "array"  # Tuples are arrays with fixed structure
            elif type_name:
                return self.map_scalar_type(type_name)
            else:
                return "string"
        elif isinstance(structure_type, str):
            # Check named type cache
            if structure_type in self.named_type_cache:
                return self.convert_structure_type_to_datapackage_type(
                    self.named_type_cache[structure_type], context_schema)
            return self.map_scalar_type(structure_type)

        return "string"

    def cache_named_types(self, structure_type: JsonNode) -> None:
        """Add an encountered type to the cache of named types."""
        if isinstance(structure_type, list):
            for item in structure_type:
                self.cache_named_types(item)
        elif isinstance(structure_type, dict):
            # Cache this type if it has a name
            if structure_type.get("name"):
                full_name = self.get_fullname(
                    str(structure_type.get("namespace", "")), 
                    str(structure_type.get("name")))
                self.named_type_cache[full_name] = structure_type
            
            # Recursively cache types in properties
            if "properties" in structure_type:
                for prop_name, prop_schema in structure_type["properties"].items():
                    if isinstance(prop_schema, dict):
                        self.cache_named_types(prop_schema)
            
            # Recursively cache types in definitions
            if "definitions" in structure_type:
                for def_name, def_schema in structure_type["definitions"].items():
                    if isinstance(def_schema, dict):
                        self.cache_named_types(def_schema)
            
            # Cache types in array items, map values, etc.
            for key in ['items', 'values', 'additionalProperties']:
                if key in structure_type and isinstance(structure_type[key], dict):
                    self.cache_named_types(structure_type[key])

    def map_scalar_type(self, type_name: str) -> str:
        """Map a JSON Structure scalar type to a Data Package scalar type."""
        # JSON Structure Core primitive types mapping
        scalar_type_mapping = {
            # JSON primitive types
            "null": "string",
            "boolean": "boolean",
            "string": "string",
            "number": "number",
            "integer": "integer",
            
            # Extended primitive types - integers
            "int8": "integer",
            "uint8": "integer",
            "int16": "integer",
            "uint16": "integer",
            "int32": "integer",
            "uint32": "integer",
            "int64": "integer",
            "uint64": "integer",
            "int128": "integer",
            "uint128": "integer",
            
            # Extended primitive types - floats
            "float8": "number",
            "float": "number",
            "double": "number",
            "binary32": "number",
            "binary64": "number",
            "decimal": "number",
            
            # Extended primitive types - other
            "binary": "string",  # Base64-encoded in JSON
            "date": "date",
            "time": "time",
            "datetime": "datetime",
            "timestamp": "datetime",
            "duration": "duration",
            "uuid": "string",
            "uri": "string",
            "jsonpointer": "string",
            
            # Special type
            "any": "any"
        }
        return scalar_type_mapping.get(type_name, "string")


def convert_structure_to_datapackage(structure_schema_path: str, 
                                     structure_record_type: Optional[str], 
                                     datapackage_path: str) -> None:
    """Convert a JSON Structure schema to a Data Package."""
    converter = StructureToDataPackageConverter()
    converter.convert_structure_to_datapackage(structure_schema_path, structure_record_type, datapackage_path)


# Example usage:
# convert_structure_to_datapackage("schema.struct.json", "MyRecord", "datapackage.json")
