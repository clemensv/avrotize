"""Shared schema inference logic for JSON and XML data.

This module provides the core inference logic used by:
- json2a/json2s: Infer schema from JSON files
- xml2a/xml2s: Infer schema from XML files
- sql2a: Infer schema for JSON/XML columns in databases
"""

import copy
import json
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Tuple, Callable

from avrotize.common import avro_name, get_tree_hash

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | bool | int | float | None


class SchemaInferrer:
    """Base class for schema inference from JSON and XML data."""

    def __init__(self, namespace: str = '', type_name_prefix: str = ''):
        """Initialize the schema inferrer.

        Args:
            namespace: Namespace for generated types (Avro) or $id base (JSON Structure)
            type_name_prefix: Prefix for generated type names
        """
        self.namespace = namespace
        self.type_name_prefix = type_name_prefix
        self.generated_types: List[str] = []

    def fold_record_types(self, base_record: dict, new_record: dict) -> Tuple[bool, dict]:
        """Merges two record types by combining their fields.

        When two records have overlapping fields with compatible types, they
        are folded into a single record with all fields. Fields that don't
        appear in all records become optional (nullable).

        Args:
            base_record: The base record to merge into
            new_record: The new record to merge

        Returns:
            Tuple of (success, merged_record). If folding fails due to
            incompatible types, returns (False, new_record).
        """
        base_fields = copy.deepcopy(base_record).get("fields", [])
        new_fields = new_record.get("fields", [])

        for field in new_fields:
            base_field = next(
                (f for f in base_fields if f["name"] == field["name"]), None)
            if not base_field:
                base_fields.append(field)
            else:
                if isinstance(base_field["type"], str) and base_field["type"] != field["type"]:
                    return False, new_record
                elif isinstance(base_field["type"], dict) and isinstance(field["type"], dict):
                    if base_field["type"].get("type") == field["type"].get("type"):
                        result, record_type = self.fold_record_types(
                            base_field["type"], field["type"])
                        if not result:
                            return False, new_record
                        base_field["type"] = record_type
        base_record["fields"] = base_fields
        return True, base_record

    def consolidated_type_list(self, type_name: str, python_values: list, 
                                type_converter: Callable[[str, Any], JsonNode]) -> List[JsonNode]:
        """Consolidates a list of values into unique types.

        Eliminates duplicate types using tree hashing and attempts to fold
        compatible record types together.

        Args:
            type_name: Base name for generated types
            python_values: List of Python values to analyze
            type_converter: Function to convert Python values to schema types

        Returns:
            List of unique schema types
        """
        list_types = [type_converter(type_name, item) for item in python_values]

        # Eliminate duplicates using tree hashing
        tree_hashes = {}
        for item in list_types:
            tree_hash = get_tree_hash(item)
            if tree_hash.hash_value not in tree_hashes:
                tree_hashes[tree_hash.hash_value] = item
        list_types = list(tree_hashes.values())

        # Try to fold record types together
        unique_types = []
        prior_record = None
        for item in list_types:
            if isinstance(item, dict) and item.get("type") == "record":
                if prior_record is None:
                    prior_record = item
                else:
                    folded, record = self.fold_record_types(prior_record, item)
                    if not folded:
                        unique_types.append(item)
                    else:
                        prior_record = record
            else:
                unique_types.append(item)
        if prior_record is not None:
            unique_types.append(prior_record)

        # Consolidate array and map types
        array_types = [item["items"] for item in unique_types 
                       if isinstance(item, dict) and item.get("type") == "array"]
        map_types = [item["values"] for item in unique_types 
                     if isinstance(item, dict) and item.get("type") == "map"]
        list_types = [item for item in unique_types 
                      if not isinstance(item, dict) or item.get("type") not in ["array", "map"]]

        item_types: List[JsonNode] = []
        for item2 in array_types:
            if isinstance(item2, list):
                item_types.extend(item2)
            else:
                item_types.append(item2)
        if len(item_types) > 0:
            list_types.append({"type": "array", "items": item_types})

        value_types: List[JsonNode] = []
        for item3 in map_types:
            if isinstance(item3, list):
                value_types.extend(item3)
            else:
                value_types.append(item3)
        if len(value_types) > 0:
            list_types.append({"type": "map", "values": value_types})

        return list_types


class AvroSchemaInferrer(SchemaInferrer):
    """Infers Avro schemas from JSON and XML data."""

    def python_type_to_avro_type(self, type_name: str, python_value: Any) -> JsonNode:
        """Maps Python types to Avro types.

        Args:
            type_name: Name for the type being generated
            python_value: Python value to convert

        Returns:
            Avro schema type
        """
        simple_types = {
            int: "long",  # Use long for safety with large integers
            float: "double",
            str: "string",
            bool: "boolean",
            bytes: "bytes"
        }

        if python_value is None:
            return "null"

        if isinstance(python_value, dict):
            type_name_name = avro_name(type_name.rsplit('.', 1)[-1])
            type_name_namespace = (type_name.rsplit('.', 1)[0]) + "Types" if '.' in type_name else ''
            if self.namespace:
                type_namespace = self.namespace + ('.' if type_name_namespace else '') + type_name_namespace
            else:
                type_namespace = type_name_namespace
            record: Dict[str, JsonNode] = {
                "type": "record",
                "name": type_name_name,
            }
            if type_namespace:
                record["namespace"] = type_namespace
            fields: List[JsonNode] = []
            for key, value in python_value.items():
                original_key = key
                key = avro_name(key)
                field: Dict[str, JsonNode] = {
                    "name": key,
                    "type": self.python_type_to_avro_type(f"{type_name}.{key}", value)
                }
                if original_key != key:
                    field["altnames"] = {"json": original_key}
                fields.append(field)
            record["fields"] = fields
            return record

        if isinstance(python_value, list):
            if len(python_value) > 0:
                item_types = self.consolidated_type_list(
                    type_name, python_value, self.python_type_to_avro_type)
            else:
                item_types = ["string"]
            if len(item_types) == 1:
                return {"type": "array", "items": item_types[0]}
            else:
                return {"type": "array", "items": item_types}

        return simple_types.get(type(python_value), "string")

    def infer_from_json_values(self, type_name: str, values: List[Any]) -> JsonNode:
        """Infers Avro schema from a list of JSON values.

        Args:
            type_name: Name for the root type
            values: List of parsed JSON values

        Returns:
            Inferred Avro schema
        """
        if not values:
            return "string"

        unique_types = self.consolidated_type_list(
            type_name, values, self.python_type_to_avro_type)

        if len(unique_types) > 1:
            return unique_types
        elif len(unique_types) == 1:
            return unique_types[0]
        else:
            return "string"

    def infer_from_xml_values(self, type_name: str, xml_strings: List[str]) -> JsonNode:
        """Infers Avro schema from a list of XML strings.

        Args:
            type_name: Name for the root type
            xml_strings: List of XML strings to analyze

        Returns:
            Inferred Avro schema
        """
        xml_structures: List[Dict[str, Any]] = []
        for xml_str in xml_strings:
            try:
                structure = self._parse_xml_to_dict(xml_str)
                if structure:
                    xml_structures.append(structure)
            except ET.ParseError:
                pass

        if not xml_structures:
            return "string"

        unique_types = self.consolidated_type_list(
            type_name, xml_structures, self.python_type_to_avro_type)

        if len(unique_types) > 1:
            return unique_types
        elif len(unique_types) == 1:
            return unique_types[0]
        else:
            return "string"

    def _parse_xml_to_dict(self, xml_string: str) -> Dict[str, Any] | None:
        """Parses XML string to a dictionary structure for schema inference."""
        try:
            root = ET.fromstring(xml_string)
            return self._element_to_dict(root)
        except ET.ParseError:
            return None

    def _element_to_dict(self, element: ET.Element) -> Dict[str, Any]:
        """Converts an XML element to a dictionary."""
        result: Dict[str, Any] = {}

        # Handle attributes
        for attr_name, attr_value in element.attrib.items():
            # Strip namespace from attribute name
            attr_name = attr_name.split('}')[-1] if '}' in attr_name else attr_name
            result[f"@{attr_name}"] = attr_value

        # Handle text content
        if element.text and element.text.strip():
            if len(element) == 0 and not element.attrib:
                return element.text.strip()  # type: ignore
            result["#text"] = element.text.strip()

        # Handle child elements
        for child in element:
            child_tag = child.tag.split('}')[-1] if '}' in child.tag else child.tag
            child_dict = self._element_to_dict(child)

            if child_tag in result:
                # Convert to list if multiple children with same tag
                if not isinstance(result[child_tag], list):
                    result[child_tag] = [result[child_tag]]
                result[child_tag].append(child_dict)
            else:
                result[child_tag] = child_dict

        return result


class JsonStructureSchemaInferrer(SchemaInferrer):
    """Infers JSON Structure schemas from JSON and XML data."""

    # JSON Structure primitive type mapping
    # Use 'integer' for general integers (accepts native JSON numbers)
    # int64/uint64 etc. are string-encoded for JSON safety with large numbers
    PYTHON_TO_JSTRUCT_TYPES = {
        int: "integer",
        float: "double",
        str: "string",
        bool: "boolean",
        bytes: "binary"
    }

    def __init__(self, namespace: str = '', type_name_prefix: str = '', base_id: str = ''):
        """Initialize the JSON Structure schema inferrer.

        Args:
            namespace: Namespace for generated types
            type_name_prefix: Prefix for generated type names
            base_id: Base URI for $id generation
        """
        super().__init__(namespace, type_name_prefix)
        self.base_id = base_id or 'https://example.com/'
        self.definitions: Dict[str, Any] = {}

    def python_type_to_jstruct_type(self, type_name: str, python_value: Any) -> Dict[str, Any] | str:
        """Maps Python types to JSON Structure types.

        Args:
            type_name: Name for the type being generated
            python_value: Python value to convert

        Returns:
            JSON Structure schema type
        """
        if python_value is None:
            return "null"

        if isinstance(python_value, dict):
            # Generate an object type
            safe_name = avro_name(type_name.rsplit('.', 1)[-1])
            properties: Dict[str, Any] = {}
            required: List[str] = []

            for key, value in python_value.items():
                original_key = key
                safe_key = avro_name(key)
                prop_type = self.python_type_to_jstruct_type(f"{type_name}.{safe_key}", value)

                if isinstance(prop_type, str):
                    properties[safe_key] = {"type": prop_type}
                else:
                    properties[safe_key] = prop_type

                # Add altnames if key was transformed
                if original_key != safe_key:
                    properties[safe_key]["altnames"] = {"json": original_key}

                # All inferred properties are required unless null
                if prop_type != "null":
                    required.append(safe_key)

            result: Dict[str, Any] = {
                "type": "object",
                "name": safe_name,
                "properties": properties
            }
            if required:
                result["required"] = required

            return result

        if isinstance(python_value, list):
            if len(python_value) > 0:
                item_types = self.consolidated_jstruct_type_list(
                    type_name, python_value)
                # Simplify single-type arrays
                if len(item_types) == 1:
                    items = item_types[0]
                else:
                    # Use choice for multiple item types
                    items = {"type": "choice", "choices": item_types}
            else:
                items = {"type": "string"}

            if isinstance(items, str):
                return {"type": "array", "items": {"type": items}}
            elif isinstance(items, dict) and "type" not in items:
                return {"type": "array", "items": items}
            else:
                return {"type": "array", "items": items}

        return self.PYTHON_TO_JSTRUCT_TYPES.get(type(python_value), "string")

    def fold_jstruct_record_types(self, base_record: dict, new_record: dict) -> Tuple[bool, dict]:
        """Merges two JSON Structure object types by combining their properties.

        Args:
            base_record: The base object to merge into
            new_record: The new object to merge

        Returns:
            Tuple of (success, merged_object)
        """
        base_props = copy.deepcopy(base_record).get("properties", {})
        new_props = new_record.get("properties", {})
        base_required = set(base_record.get("required", []))
        new_required = set(new_record.get("required", []))

        for prop_name, prop_schema in new_props.items():
            if prop_name not in base_props:
                base_props[prop_name] = prop_schema
                # Property only in some records is not required
            else:
                # Property exists in both - check compatibility
                base_type = base_props[prop_name].get("type") if isinstance(base_props[prop_name], dict) else base_props[prop_name]
                new_type = prop_schema.get("type") if isinstance(prop_schema, dict) else prop_schema

                if base_type != new_type:
                    # Types differ - can't fold simply
                    if base_type == "object" and new_type == "object":
                        # Try to fold nested objects
                        success, merged = self.fold_jstruct_record_types(
                            base_props[prop_name], prop_schema)
                        if success:
                            base_props[prop_name] = merged
                        else:
                            return False, new_record
                    else:
                        return False, new_record

        # Update required - only properties in ALL records are required
        merged_required = base_required & new_required

        base_record["properties"] = base_props
        if merged_required:
            base_record["required"] = list(merged_required)
        elif "required" in base_record:
            del base_record["required"]

        return True, base_record

    def consolidated_jstruct_type_list(self, type_name: str, python_values: list) -> List[Any]:
        """Consolidates a list of values into unique JSON Structure types.

        Args:
            type_name: Base name for generated types
            python_values: List of Python values to analyze

        Returns:
            List of unique JSON Structure types
        """
        list_types = [self.python_type_to_jstruct_type(type_name, item) for item in python_values]

        # Eliminate duplicates using tree hashing
        tree_hashes = {}
        for item in list_types:
            tree_hash = get_tree_hash(item)
            if tree_hash.hash_value not in tree_hashes:
                tree_hashes[tree_hash.hash_value] = item
        list_types = list(tree_hashes.values())

        # Try to fold object types together
        unique_types = []
        prior_object = None
        for item in list_types:
            if isinstance(item, dict) and item.get("type") == "object":
                if prior_object is None:
                    prior_object = item
                else:
                    folded, obj = self.fold_jstruct_record_types(prior_object, item)
                    if not folded:
                        unique_types.append(item)
                    else:
                        prior_object = obj
            else:
                unique_types.append(item)
        if prior_object is not None:
            unique_types.append(prior_object)

        # Consolidate array and map types
        array_types = [item.get("items") for item in unique_types 
                       if isinstance(item, dict) and item.get("type") == "array"]
        map_types = [item.get("values") for item in unique_types 
                     if isinstance(item, dict) and item.get("type") == "map"]
        list_types = [item for item in unique_types 
                      if not isinstance(item, dict) or item.get("type") not in ["array", "map"]]

        item_types: List[Any] = []
        for item2 in array_types:
            if isinstance(item2, list):
                item_types.extend(item2)
            elif item2:
                item_types.append(item2)
        if item_types:
            if len(item_types) == 1:
                list_types.append({"type": "array", "items": item_types[0]})
            else:
                list_types.append({"type": "array", "items": {"type": "choice", "choices": item_types}})

        value_types: List[Any] = []
        for item3 in map_types:
            if isinstance(item3, list):
                value_types.extend(item3)
            elif item3:
                value_types.append(item3)
        if value_types:
            if len(value_types) == 1:
                list_types.append({"type": "map", "values": value_types[0]})
            else:
                list_types.append({"type": "map", "values": {"type": "choice", "choices": value_types}})

        return list_types

    def infer_from_json_values(self, type_name: str, values: List[Any]) -> Dict[str, Any]:
        """Infers JSON Structure schema from a list of JSON values.

        Args:
            type_name: Name for the root type
            values: List of parsed JSON values

        Returns:
            Complete JSON Structure schema with $schema and $id
        """
        if not values:
            return self._wrap_schema({"type": "string"}, type_name)

        unique_types = self.consolidated_jstruct_type_list(type_name, values)

        if len(unique_types) > 1:
            # Multiple types -> use choice
            schema = {"type": "choice", "choices": unique_types, "name": avro_name(type_name)}
        elif len(unique_types) == 1:
            schema = unique_types[0]
            if isinstance(schema, str):
                schema = {"type": schema}
            if "name" not in schema:
                schema["name"] = avro_name(type_name)
        else:
            schema = {"type": "string", "name": avro_name(type_name)}

        return self._wrap_schema(schema, type_name)

    def infer_from_xml_values(self, type_name: str, xml_strings: List[str]) -> Dict[str, Any]:
        """Infers JSON Structure schema from a list of XML strings.

        Args:
            type_name: Name for the root type
            xml_strings: List of XML strings to analyze

        Returns:
            Complete JSON Structure schema with $schema and $id
        """
        xml_structures: List[Dict[str, Any]] = []
        for xml_str in xml_strings:
            try:
                structure = self._parse_xml_to_dict(xml_str)
                if structure:
                    xml_structures.append(structure)
            except ET.ParseError:
                pass

        if not xml_structures:
            return self._wrap_schema({"type": "string"}, type_name)

        unique_types = self.consolidated_jstruct_type_list(type_name, xml_structures)

        if len(unique_types) > 1:
            schema = {"type": "choice", "choices": unique_types, "name": avro_name(type_name)}
        elif len(unique_types) == 1:
            schema = unique_types[0]
            if isinstance(schema, str):
                schema = {"type": schema}
            if "name" not in schema:
                schema["name"] = avro_name(type_name)
        else:
            schema = {"type": "string", "name": avro_name(type_name)}

        return self._wrap_schema(schema, type_name)

    def _wrap_schema(self, schema: Dict[str, Any], type_name: str) -> Dict[str, Any]:
        """Wraps a schema with JSON Structure metadata.

        Args:
            schema: The schema body
            type_name: Name for generating $id

        Returns:
            Complete JSON Structure schema
        """
        safe_name = avro_name(type_name)
        schema_id = f"{self.base_id.rstrip('/')}/{safe_name}"

        result = {
            "$schema": "https://json-structure.org/meta/core/v0/#",
            "$id": schema_id,
        }
        result.update(schema)
        return result

    def _parse_xml_to_dict(self, xml_string: str) -> Dict[str, Any] | None:
        """Parses XML string to a dictionary structure for schema inference."""
        try:
            root = ET.fromstring(xml_string)
            return self._element_to_dict(root)
        except ET.ParseError:
            return None

    def _element_to_dict(self, element: ET.Element) -> Dict[str, Any]:
        """Converts an XML element to a dictionary."""
        result: Dict[str, Any] = {}

        # Handle attributes (prefix with @ for XML attributes)
        for attr_name, attr_value in element.attrib.items():
            attr_name = attr_name.split('}')[-1] if '}' in attr_name else attr_name
            result[f"@{attr_name}"] = attr_value

        # Handle text content
        if element.text and element.text.strip():
            if len(element) == 0 and not element.attrib:
                return element.text.strip()  # type: ignore
            result["#text"] = element.text.strip()

        # Handle child elements
        for child in element:
            child_tag = child.tag.split('}')[-1] if '}' in child.tag else child.tag
            child_dict = self._element_to_dict(child)

            if child_tag in result:
                if not isinstance(result[child_tag], list):
                    result[child_tag] = [result[child_tag]]
                result[child_tag].append(child_dict)
            else:
                result[child_tag] = child_dict

        return result


# Convenience functions for direct use

def infer_avro_schema_from_json(
    json_values: List[Any],
    type_name: str = 'Document',
    namespace: str = ''
) -> JsonNode:
    """Infers Avro schema from JSON values.

    Args:
        json_values: List of parsed JSON values
        type_name: Name for the root type
        namespace: Avro namespace

    Returns:
        Inferred Avro schema
    """
    inferrer = AvroSchemaInferrer(namespace=namespace)
    return inferrer.infer_from_json_values(type_name, json_values)


def infer_avro_schema_from_xml(
    xml_strings: List[str],
    type_name: str = 'Document',
    namespace: str = ''
) -> JsonNode:
    """Infers Avro schema from XML strings.

    Args:
        xml_strings: List of XML strings
        type_name: Name for the root type
        namespace: Avro namespace

    Returns:
        Inferred Avro schema
    """
    inferrer = AvroSchemaInferrer(namespace=namespace)
    return inferrer.infer_from_xml_values(type_name, xml_strings)


def infer_jstruct_schema_from_json(
    json_values: List[Any],
    type_name: str = 'Document',
    base_id: str = 'https://example.com/'
) -> Dict[str, Any]:
    """Infers JSON Structure schema from JSON values.

    Args:
        json_values: List of parsed JSON values
        type_name: Name for the root type
        base_id: Base URI for $id generation

    Returns:
        Complete JSON Structure schema
    """
    inferrer = JsonStructureSchemaInferrer(base_id=base_id)
    return inferrer.infer_from_json_values(type_name, json_values)


def infer_jstruct_schema_from_xml(
    xml_strings: List[str],
    type_name: str = 'Document',
    base_id: str = 'https://example.com/'
) -> Dict[str, Any]:
    """Infers JSON Structure schema from XML strings.

    Args:
        xml_strings: List of XML strings
        type_name: Name for the root type
        base_id: Base URI for $id generation

    Returns:
        Complete JSON Structure schema
    """
    inferrer = JsonStructureSchemaInferrer(base_id=base_id)
    return inferrer.infer_from_xml_values(type_name, xml_strings)
