"""
SPDX-FileCopyrightText: 2025-present Avrotize contributors
SPDX-License-Identifier: Apache-2.0

Reverse converter: JSON Structure → Avro Schema
"""
import json
import re
from typing import Any, Dict, List, Optional, Union


def convert_json_structure_to_avro(
    json_structure_file: str, avro_schema_file: str, naming_mode: str = "default"
) -> None:
    """
    Read a JSON Structure file and write the corresponding Avro schema file.
    """
    with open(json_structure_file, "r", encoding="utf-8") as f:
        jstruct = json.load(f)
    avro_schema = JStructToAvroConverter(naming_mode=naming_mode).convert(jstruct)
    with open(avro_schema_file, "w", encoding="utf-8") as f:
        json.dump(avro_schema, f, indent=2, ensure_ascii=False)


class JStructToAvroConverter:
    """Convert JSON Structure documents to Avro schemas."""

    def __init__(self, naming_mode: str = "default"):
        self.naming_mode = naming_mode
        self.definitions: Dict[str, Any] = {}
        self.visited: set[str] = set()
        self.type_registry: Dict[str, Any] = {}

    def convert(self, jstruct: Dict[str, Any]) -> Any:
        """
        Convert a JSON Structure document to an Avro schema.
        """
        # Extract definitions if present
        if "definitions" in jstruct:
            self.definitions = jstruct["definitions"]

        # Find the root schema
        if "$root" in jstruct:
            root_ref = jstruct["$root"]
            if root_ref.startswith("#/definitions/"):
                root_path = root_ref[14:]  # Remove "#/definitions/"
                root_schema = self._resolve_definition_path(root_path)
            else:
                raise ValueError(f"Unsupported root reference: {root_ref}")
        else:
            # Use the document itself as the root schema
            root_schema = jstruct

        return self._convert_type(root_schema)

    def _resolve_definition_path(self, path: str) -> Dict[str, Any]:
        """
        Resolve a definition path like "example/com/record" to the actual definition.
        """
        parts = path.split("/")
        current = self.definitions

        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                raise ValueError(f"Definition not found: {path}")

        return current

    def _convert_type(self, schema: Union[Dict[str, Any], str, List[Any]]) -> Any:
        """Convert a JSON Structure type to an Avro type."""
        if isinstance(schema, str):
            return self._convert_primitive_type(schema)
        if isinstance(schema, list):
            return self._convert_union_type(schema)
        if not isinstance(schema, dict):
            raise ValueError(f"Unsupported schema type: {type(schema)}")
        if "$ref" in schema:
            return self._resolve_reference(schema["$ref"])

        schema_type = schema.get("type")
        if schema_type == "object":
            return self._convert_record_type(schema)
        elif schema_type == "array":
            return self._convert_array_type(schema)
        elif schema_type == "map":
            return self._convert_map_type(schema)
        elif schema_type == "string" and "enum" in schema:
            return self._convert_enum_type(schema)
        elif schema_type == "choice":
            return self._convert_choice_type(schema)
        elif schema_type and schema_type in self._get_primitive_type_mappings():
            return self._convert_primitive_type(schema_type, schema)
        else:
            raise ValueError(f"Unsupported type: {schema_type}")

    def _resolve_reference(self, ref: str) -> Any:
        """Resolve a $ref to the actual type definition."""
        if ref.startswith("#/definitions/"):
            def_path = ref[14:]
            definition = self._resolve_definition_path(def_path)
            return self._convert_type(definition)
        else:
            raise ValueError(f"Unsupported reference: {ref}")

    def _convert_record_type(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Convert a JSON Structure object type to an Avro record."""
        record_name = schema.get("name", "Record")
        record_name = self._sanitize_name(record_name)
        type_key = f"record:{record_name}"
        if type_key in self.visited:
            return {"type": record_name}
        self.visited.add(type_key)
        avro_record = {"type": "record", "name": record_name, "fields": []}
        if "description" in schema:
            avro_record["doc"] = schema["description"]
        properties = schema.get("properties", {})
        required_fields = set(schema.get("required", []))
        for field_name, field_schema in properties.items():
            field_name = self._sanitize_name(field_name)
            field_type = self._convert_type(field_schema)
            avro_field = {"name": field_name, "type": field_type}
            if "default" in field_schema:
                avro_field["default"] = field_schema["default"]
            elif field_name not in required_fields:
                if isinstance(field_type, list):
                    if "null" not in field_type:
                        field_type.insert(0, "null")
                        avro_field["type"] = field_type
                        avro_field["default"] = None
                else:
                    avro_field["type"] = ["null", field_type]
                    avro_field["default"] = None
            if "description" in field_schema:
                avro_field["doc"] = field_schema["description"]
            avro_record["fields"].append(avro_field)
        self.visited.remove(type_key)
        return avro_record

    def _convert_array_type(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Convert a JSON Structure array type to an Avro array."""
        items_schema = schema.get("items")
        if items_schema is None:
            raise ValueError("Array type must have items property")
        return {"type": "array", "items": self._convert_type(items_schema)}

    def _convert_map_type(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Convert a JSON Structure map type to an Avro map."""
        values_schema = schema.get("values")
        if values_schema is None:
            raise ValueError("Map type must have values property")
        return {"type": "map", "values": self._convert_type(values_schema)}

    def _convert_enum_type(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Convert a JSON Structure string with enum to an Avro enum."""
        enum_name = schema.get("name", "Enum")
        enum_name = self._sanitize_name(enum_name)
        avro_enum = {"type": "enum", "name": enum_name, "symbols": schema["enum"]}
        if "description" in schema:
            avro_enum["doc"] = schema["description"]
        if "default" in schema:
            avro_enum["default"] = schema["default"]
        return avro_enum

    def _convert_choice_type(self, schema: Dict[str, Any]) -> List[Any]:
        """Convert a JSON Structure choice type to an Avro union."""
        choices = schema.get("choices", {})
        union_types = []
        for choice_name, choice_schema in choices.items():
            union_types.append(self._convert_type(choice_schema))
        return union_types

    def _convert_union_type(self, schema_list: List[Any]) -> List[Any]:
        """Convert a list of schemas to an Avro union."""
        union_types = []
        for schema in schema_list:
            union_types.append(self._convert_type(schema))
        return union_types    
    
    def _convert_primitive_type(
        self, type_name: str, schema: Optional[Dict[str, Any]] = None
    ) -> Union[str, Dict[str, Any]]:
        """Convert a JSON Structure primitive type to an Avro primitive type."""
        mappings = self._get_primitive_type_mappings()
        if type_name in mappings:
            base_type, logical_type = mappings[type_name]
            
            # If it's a direct Avro type with no logical type
            if logical_type is None:
                if schema and "logicalType" in schema:
                    # Explicit logical type in the schema takes precedence
                    schema_logical_type = schema["logicalType"]
                    return self._convert_logical_type(base_type, schema_logical_type, schema)
                return base_type
            else:
                # It's a JSON Structure type that maps to an Avro logical type
                return self._convert_logical_type(base_type, logical_type, schema or {})
        else:
            # Handle unknown types as custom logical types
            return self._convert_custom_type(type_name, schema or {})
                
    def _convert_custom_type(
        self, type_name: str, schema: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Convert unknown JSON Structure types to custom logical types."""
        # Determine the appropriate base Avro type based on the nature of the custom type
        if type_name in ["int32", "uint32"]:
            base_type = "int"
        elif type_name in ["int64", "uint64"]:
            base_type = "long"
        elif type_name in ["int128", "uint128"]:
            base_type = "string"  # Use string for very large integers
        elif type_name in ["uuid", "uri", "jsonpointer"]:
            base_type = "string"
        elif type_name in ["date", "time", "datetime", "duration"]:
            base_type = "string"
        elif type_name == "decimal":
            base_type = "bytes"
        elif type_name == "any":
            # Use a union of all primitive types for "any"
            return ["null", "boolean", "int", "long", "float", "double", "string", "bytes"]
        elif type_name == "set":
            # A set is represented as an array with the additional semantic of unique items
            if "items" in schema:
                items_type = self._convert_type(schema["items"])
                return {
                    "type": "array", 
                    "items": items_type,
                    "logicalType": "set"
                }
            else:
                raise ValueError("Set type must have items property")
        elif type_name == "tuple":
            # A tuple is represented as a record with positional fields
            if "items" in schema and isinstance(schema["items"], list):
                fields = []
                for i, item_schema in enumerate(schema["items"]):
                    field_name = f"item{i}"
                    field_type = self._convert_type(item_schema)
                    fields.append({"name": field_name, "type": field_type})
                
                tuple_name = schema.get("name", "Tuple")
                tuple_name = self._sanitize_name(tuple_name)
                
                return {
                    "type": "record",
                    "name": tuple_name,
                    "logicalType": "tuple",
                    "fields": fields
                }
            else:
                raise ValueError("Tuple type must have items as an array")
        elif type_name == "choice":
            # Handle 'choice' type by converting it to a union of types
            if "choices" in schema and isinstance(schema["choices"], dict):
                return self._convert_choice_type(schema)
            else:
                raise ValueError("Choice type must have choices property as an object")
        else:
            # For any other unknown type, determine the most appropriate base type
            # Default to string for most custom types
            base_type = "string"
        
        # Check if the schema has an explicit logicalType
        logical_type = schema.get("logicalType", type_name)
        
        # Create the logical type representation
        result = {"type": base_type, "logicalType": logical_type}
        
        # Copy any additional attributes from the schema that might be relevant
        for key, value in schema.items():
            if key not in ["type", "name", "description", "properties", "required", "logicalType"]:
                result[key] = value
                
        return result

    def _convert_logical_type(
        self, base_type: str, logical_type: str, schema: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Convert a logical type to Avro format."""
        result = {"type": base_type, "logicalType": logical_type}

        if logical_type == "decimal":
            if "precision" in schema:
                result["precision"] = schema["precision"]
            if "scale" in schema:
                result["scale"] = schema["scale"]
        elif logical_type in [
            "date",
            "time-millis",
            "time-micros",
            "timestamp-millis",
            "timestamp-micros",
            "local-timestamp-millis",
            "local-timestamp-micros",
        ]:
            # These logical types do not require additional parameters
            pass
        elif logical_type == "uuid":
            # UUID is a logical type for string
            if base_type != "string":
                raise ValueError(
                    f"UUID logical type must be used with string base type, not {base_type}"
                )
        
        # Copy any additional attributes from the schema that might be relevant
        for key, value in schema.items():
            if key not in ["type", "logicalType", "name", "description", "properties", "required"]:
                result[key] = value

        return result    
    
    def _get_primitive_type_mappings(self) -> Dict[str, tuple[str, Optional[str]]]:
        """
        Get the mapping from JSON Structure primitive types to Avro primitive types.
        Returns a tuple of (base_type, logical_type) where logical_type is None if
        no conversion is needed.
        """
        return {
            # Direct mappings with no logical type needed
            "string": ("string", None),
            "boolean": ("boolean", None),
            "float": ("float", None),
            "double": ("double", None),
            "binary": ("bytes", None),
            "null": ("null", None),
            
            # JSON Schema types with direct Avro mappings
            "number": ("double", None),
            "integer": ("int", None),
            "byte": ("bytes", None),
            
            # Simple mapped logical types
            "int32": ("int", "int32"),
            "int64": ("long", "int64"),
            "uint32": ("int", "uint32"),
            "uint64": ("long", "uint64"),
            "int128": ("string", "int128"),
            "uint128": ("string", "uint128"),
            
            # Date and time logical types
            "date": ("int", "date"),
            "time": ("int", "time-millis"),
            "datetime": ("long", "timestamp-millis"),
            "duration": ("string", "duration"),
            
            # Other logical types
            "uuid": ("string", "uuid"),
            "uri": ("string", "uri"),
            "decimal": ("bytes", "decimal"),
        }

    def _sanitize_name(self, name: str) -> str:
        """
        Sanitize a name to be valid for Avro.
        Avro names must start with [A-Za-z_] and contain only [A-Za-z0-9_].
        """
        # Replace invalid characters with underscores
        sanitized = re.sub(r"[^A-Za-z0-9_]", "_", name)

        # Ensure it starts with a letter or underscore
        if sanitized and not re.match(r"^[A-Za-z_]", sanitized):
            sanitized = "_" + sanitized

        # Ensure it's not empty
        if not sanitized:
            sanitized = "UnnamedType"

        return sanitized
