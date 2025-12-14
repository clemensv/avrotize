"""Convert a JSON Structure schema to an Iceberg schema."""

import json
import sys
from typing import Dict, List, Any, Optional
import pyarrow as pa
from pyiceberg.schema import Schema, NestedField
from pyiceberg.io.pyarrow import PyArrowFileIO, schema_to_pyarrow
from pyiceberg.types import (
    BooleanType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    StringType,
    BinaryType,
    DateType,
    TimestampType,
    DecimalType,
    FixedType,
    ListType,
    MapType,
    StructType,
    TimeType
)

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | bool | int | None


def iceberg_type_to_json(iceberg_type) -> str | Dict:
    """
    Serialize an Iceberg type to JSON per Iceberg Table Spec Appendix C.
    
    Primitive types are serialized as strings. Complex types (struct, list, map)
    are serialized as JSON objects with their nested structure.
    """
    # Primitive types map to simple strings
    if isinstance(iceberg_type, BooleanType):
        return "boolean"
    elif isinstance(iceberg_type, IntegerType):
        return "int"
    elif isinstance(iceberg_type, LongType):
        return "long"
    elif isinstance(iceberg_type, FloatType):
        return "float"
    elif isinstance(iceberg_type, DoubleType):
        return "double"
    elif isinstance(iceberg_type, StringType):
        return "string"
    elif isinstance(iceberg_type, BinaryType):
        return "binary"
    elif isinstance(iceberg_type, DateType):
        return "date"
    elif isinstance(iceberg_type, TimeType):
        return "time"
    elif isinstance(iceberg_type, TimestampType):
        return "timestamp"
    elif isinstance(iceberg_type, DecimalType):
        return f"decimal({iceberg_type.precision},{iceberg_type.scale})"
    elif isinstance(iceberg_type, FixedType):
        return f"fixed[{iceberg_type.length}]"
    elif isinstance(iceberg_type, ListType):
        return {
            "type": "list",
            "element-id": iceberg_type.element_id,
            "element-required": iceberg_type.element_required,
            "element": iceberg_type_to_json(iceberg_type.element_type)
        }
    elif isinstance(iceberg_type, MapType):
        return {
            "type": "map",
            "key-id": iceberg_type.key_id,
            "key": iceberg_type_to_json(iceberg_type.key_type),
            "value-id": iceberg_type.value_id,
            "value-required": iceberg_type.value_required,
            "value": iceberg_type_to_json(iceberg_type.value_type)
        }
    elif isinstance(iceberg_type, StructType):
        return {
            "type": "struct",
            "fields": [
                {
                    "id": field.field_id,
                    "name": field.name,
                    "required": field.required,
                    "type": iceberg_type_to_json(field.field_type)
                }
                for field in iceberg_type.fields
            ]
        }
    else:
        # Fallback for unknown types
        return str(iceberg_type)


class StructureToIcebergConverter:
    """Class to convert JSON Structure schema to Iceberg schema."""

    def __init__(self: 'StructureToIcebergConverter'):
        self.named_type_cache: Dict[str, JsonNode] = {}
        self.id_counter = 0
        self.definitions: Dict[str, Any] = {}
        self.schema_doc: Optional[Dict[str, Any]] = None

    def get_id(self) -> int:
        """Get a unique ID for a field."""
        self.id_counter += 1
        return self.id_counter
    
    def get_fullname(self, namespace: str, name: str) -> str:
        """Get the full name of a record type."""
        return f"{namespace}.{name}" if namespace else name

    def convert_structure_to_iceberg(self, structure_schema_path: str, structure_record_type: Optional[str], output_path: str, emit_cloudevents_columns: bool=False, output_format: str="arrow"):
        """Convert a JSON Structure schema to an Iceberg schema.
        
        Args:
            structure_schema_path: Path to the JSON Structure schema file
            structure_record_type: Record type to convert (or None for the root)
            output_path: Path to write the Iceberg schema
            emit_cloudevents_columns: Whether to add CloudEvents columns
            output_format: Output format - 'arrow' for binary Arrow IPC (default), 'schema' for JSON
        """
        schema_file = structure_schema_path
        if not schema_file:
            print("Please specify the JSON Structure schema file")
            sys.exit(1)
        with open(schema_file, "r", encoding="utf-8") as f:
            schema_json = f.read()

        # Parse the schema as a JSON object
        schema = json.loads(schema_json)
        self.schema_doc = schema
        
        # Handle definitions if present
        if "definitions" in schema:
            self.definitions = schema["definitions"]
        
        # For JSON Structure, we expect an object type at the top level
        if schema.get("type") != "object":
            # Check if we have a $ref at the top level
            if "$ref" in schema:
                ref = schema["$ref"]
                schema = self.resolve_ref(ref)
            elif structure_record_type and "definitions" in schema:
                # Look for the type in definitions
                if structure_record_type in schema["definitions"]:
                    schema = schema["definitions"][structure_record_type]
                else:
                    print(f"No record type {structure_record_type} found in the JSON Structure schema definitions")
                    sys.exit(1)
            else:
                print("Expected a JSON Structure schema with type 'object' at the top level")
                sys.exit(1)

        # Get the name and properties of the top-level object
        table_name = schema.get("name", "Table")
        properties = schema.get("properties", {})
        required = schema.get("required", [])

        # Create a list to store the iceberg schema
        iceberg_fields: List[NestedField] = []

        # Append the iceberg schema with the column names and types
        for prop_name, prop_schema in properties.items():
            is_required = prop_name in required
            column_type = self.convert_structure_type_to_iceberg_type(prop_schema)
            iceberg_fields.append(
                NestedField(
                    field_id=self.get_id(),
                    name=prop_name,
                    field_type=column_type,
                    required=is_required
                ))

        if emit_cloudevents_columns:
            iceberg_fields.extend([
                NestedField(field_id=self.get_id(),
                      name="___type", field_type=StringType(), required=False),
                NestedField(field_id=self.get_id(),
                      name="___source", field_type=StringType(), required=False),
                NestedField(field_id=self.get_id(),
                      name="___id", field_type=StringType(), required=False),
                NestedField(field_id=self.get_id(),
                      name="___time", field_type=TimestampType(), required=False),
                NestedField(field_id=self.get_id(),
                      name="___subject", field_type=StringType(), required=False)
            ])

        iceberg_schema = Schema(*iceberg_fields)
        print(f"Iceberg schema created: {iceberg_schema}")

        if output_format == "arrow":
            # Write as binary PyArrow schema
            arrow_schema = schema_to_pyarrow(iceberg_schema)
            file_io = PyArrowFileIO()
            output_file = file_io.new_output("file://" + output_path)
            with output_file.create(overwrite=True) as f:
                pa.output_stream(f).write(arrow_schema.serialize().to_pybytes())
        else:
            # Write Iceberg schema as spec-compliant JSON (per Iceberg Table Spec Appendix C)
            schema_json = {
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {
                        "id": field.field_id,
                        "name": field.name,
                        "required": field.required,
                        "type": iceberg_type_to_json(field.field_type)
                    }
                    for field in iceberg_schema.fields
                ]
            }
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(schema_json, f, indent=2)

    def resolve_ref(self, ref: str) -> Dict[str, Any]:
        """Resolve a $ref reference."""
        if not ref.startswith("#/"):
            raise ValueError(f"Only local references are supported, got: {ref}")
        
        parts = ref[2:].split("/")
        current = self.schema_doc
        
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                raise ValueError(f"Could not resolve reference: {ref}")
        
        return current

    def convert_structure_type_to_iceberg_type(self, structure_type):
        """Convert a JSON Structure type to an Iceberg type."""
        # Handle $ref
        if isinstance(structure_type, dict) and "$ref" in structure_type:
            ref = structure_type["$ref"]
            resolved = self.resolve_ref(ref)
            return self.convert_structure_type_to_iceberg_type(resolved)
        
        # Handle array of types (e.g., ["string", "null"] for nullable types)
        if isinstance(structure_type, list):
            # Filter out null from the list
            non_null_types = [t for t in structure_type if t != "null"]
            if len(non_null_types) == 1:
                # Nullable type - just use the non-null type (Iceberg handles optionality with required flag)
                return self.convert_structure_type_to_iceberg_type(non_null_types[0])
            elif len(non_null_types) > 1:
                # Union of multiple non-null types - create a struct with alternatives
                fields = []
                for i, choice in enumerate(non_null_types):
                    choice_type = self.convert_structure_type_to_iceberg_type(choice)
                    fields.append(NestedField(
                        field_id=self.get_id(),
                        name=f"option_{i}",
                        field_type=choice_type,
                        required=False
                    ))
                return StructType(*fields)
            else:
                # Only null - return string as fallback
                return StringType()
        
        # Handle dictionary with type field
        if isinstance(structure_type, dict):
            type_name = structure_type.get("type")
            
            # Handle type being an array
            if isinstance(type_name, list):
                # This is like {"type": ["string", "null"]}
                return self.convert_structure_type_to_iceberg_type(type_name)
            
            # Handle array type
            if type_name == "array":
                items = structure_type.get("items", {"type": "string"})
                return ListType(
                    element_id=self.get_id(),
                    element_type=self.convert_structure_type_to_iceberg_type(items),
                    element_required=True
                )
            
            # Handle set type (treated as array in Iceberg)
            elif type_name == "set":
                items = structure_type.get("items", {"type": "string"})
                return ListType(
                    element_id=self.get_id(),
                    element_type=self.convert_structure_type_to_iceberg_type(items),
                    element_required=True
                )
            
            # Handle map type
            elif type_name == "map":
                values = structure_type.get("values", {"type": "string"})
                return MapType(
                    key_id=self.get_id(),
                    key_type=StringType(),
                    value_id=self.get_id(),
                    value_type=self.convert_structure_type_to_iceberg_type(values),
                    value_required=True
                )
            
            # Handle tuple type (treated as struct with indexed fields)
            elif type_name == "tuple":
                items = structure_type.get("items", [])
                fields = []
                for i, item in enumerate(items):
                    fields.append(NestedField(
                        field_id=self.get_id(),
                        name=f"field_{i}",
                        field_type=self.convert_structure_type_to_iceberg_type(item),
                        required=True
                    ))
                return StructType(*fields)
            
            # Handle object type
            elif type_name == "object":
                properties = structure_type.get("properties", {})
                required = structure_type.get("required", [])
                fields = []
                
                # Handle $extends if present
                if "$extends" in structure_type:
                    extends_ref = structure_type["$extends"]
                    base_schema = self.resolve_ref(extends_ref)
                    base_properties = base_schema.get("properties", {})
                    base_required = base_schema.get("required", [])
                    
                    # Add base properties first
                    for prop_name, prop_schema in base_properties.items():
                        is_required = prop_name in base_required
                        fields.append(NestedField(
                            field_id=self.get_id(),
                            name=prop_name,
                            field_type=self.convert_structure_type_to_iceberg_type(prop_schema),
                            required=is_required
                        ))
                
                # Add own properties
                for prop_name, prop_schema in properties.items():
                    is_required = prop_name in required
                    fields.append(NestedField(
                        field_id=self.get_id(),
                        name=prop_name,
                        field_type=self.convert_structure_type_to_iceberg_type(prop_schema),
                        required=is_required
                    ))
                
                return StructType(*fields)
            
            # Handle choice type (union)
            elif type_name == "choice":
                choices = structure_type.get("choices", [])
                if isinstance(choices, list):
                    # For inline choices, create a struct with alternatives
                    fields = []
                    for i, choice in enumerate(choices):
                        choice_type = self.convert_structure_type_to_iceberg_type(choice)
                        fields.append(NestedField(
                            field_id=self.get_id(),
                            name=f"option_{i}",
                            field_type=choice_type,
                            required=False
                        ))
                    return StructType(*fields)
                elif isinstance(choices, dict):
                    # For tagged choices, create a struct with named alternatives
                    fields = []
                    for choice_name, choice_schema in choices.items():
                        choice_type = self.convert_structure_type_to_iceberg_type(choice_schema)
                        fields.append(NestedField(
                            field_id=self.get_id(),
                            name=choice_name,
                            field_type=choice_type,
                            required=False
                        ))
                    return StructType(*fields)
                else:
                    return StringType()
            
            # Handle any type
            elif type_name == "any":
                return StringType()
            
            # Handle primitive types with annotations
            elif type_name:
                return self.map_iceberg_scalar_type(type_name, structure_type)
        
        # Handle string type name directly
        elif isinstance(structure_type, str):
            return self.map_iceberg_scalar_type(structure_type, {})

        return StringType()

    def map_iceberg_scalar_type(self, type_name: str, type_schema: Dict[str, Any]):
        """Map a JSON Structure scalar type to an Iceberg scalar type."""
        # Check for decimal with precision and scale
        if type_name == "decimal":
            precision = type_schema.get("precision", 38)
            scale = type_schema.get("scale", 18)
            return DecimalType(precision, scale)
        
        # Map other primitive types
        type_mapping = {
            'null': StringType(),  # Iceberg doesn't have a null type
            'boolean': BooleanType(),
            'string': StringType(),
            'int8': IntegerType(),  # Iceberg doesn't have byte type
            'uint8': IntegerType(),
            'int16': IntegerType(),  # Iceberg doesn't have short type
            'uint16': IntegerType(),
            'int32': IntegerType(),
            'uint32': LongType(),  # Use long for unsigned int32
            'int64': LongType(),
            'uint64': LongType(),  # Iceberg doesn't distinguish signed/unsigned
            'int128': StringType(),  # No native 128-bit support
            'uint128': StringType(),
            'integer': IntegerType(),  # Generic integer
            'number': DoubleType(),  # Generic number
            'float8': FloatType(),
            'float': FloatType(),
            'float32': FloatType(),
            'binary32': FloatType(),
            'double': DoubleType(),
            'float64': DoubleType(),
            'binary64': DoubleType(),
            'decimal': DecimalType(38, 18),
            'binary': BinaryType(),
            'bytes': BinaryType(),  # Binary data
            'date': DateType(),
            'time': TimeType(),
            'datetime': TimestampType(),
            'timestamp': TimestampType(),
            'duration': LongType(),  # Store as microseconds
            'uuid': StringType(),  # Store UUID as string
            'uri': StringType(),
            'jsonpointer': StringType(),
        }
        
        return type_mapping.get(type_name, StringType())


def convert_structure_to_iceberg(structure_schema_path, structure_record_type, output_path, emit_cloudevents_columns=False, output_format="arrow"):
    """Convert a JSON Structure schema to an Iceberg schema.
    
    Args:
        structure_schema_path: Path to the JSON Structure schema file
        structure_record_type: Record type to convert (or None for the root)
        output_path: Path to write the Iceberg schema
        emit_cloudevents_columns: Whether to add CloudEvents columns
        output_format: Output format - 'arrow' for binary Arrow IPC (default), 'schema' for JSON
    """
    converter = StructureToIcebergConverter()
    converter.convert_structure_to_iceberg(
        structure_schema_path, structure_record_type, output_path, emit_cloudevents_columns, output_format)
