"""Shared schema inference logic for JSON and XML data.

This module provides the core inference logic used by:
- json2a/json2s: Infer schema from JSON files
- xml2a/xml2s: Infer schema from XML files
- sql2a: Infer schema for JSON/XML columns in databases
"""

import copy
import json
import re
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Tuple, Callable

from avrotize.common import avro_name, get_tree_hash

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | bool | int | float | None


class SchemaInferrer:
    """Base class for schema inference from JSON and XML data."""

    def __init__(self, namespace: str = '', type_name_prefix: str = '', altnames_key: str = 'json'):
        """Initialize the schema inferrer.

        Args:
            namespace: Namespace for generated types (Avro) or $id base (JSON Structure)
            type_name_prefix: Prefix for generated type names
            altnames_key: Key to use for altnames mapping (e.g., 'json', 'sql', 'xml')
        """
        self.namespace = namespace
        self.type_name_prefix = type_name_prefix
        self.altnames_key = altnames_key
        self.generated_types: List[str] = []

    def fold_record_types(self, base_record: dict, new_record: dict) -> Tuple[bool, dict]:
        """Merges two record types by combining their fields.

        When two records have overlapping fields with compatible types, they
        are folded into a single record with all fields. Fields that don't
        appear in all records become optional (nullable with null default).

        Args:
            base_record: The base record to merge into
            new_record: The new record to merge

        Returns:
            Tuple of (success, merged_record). If folding fails due to
            incompatible types, returns (False, new_record).
        """
        base_fields = copy.deepcopy(base_record).get("fields", [])
        new_fields = new_record.get("fields", [])
        
        # Track field names present in each record
        base_field_names = {f["name"] for f in base_fields}
        new_field_names = {f["name"] for f in new_fields}

        # Process fields from the new record
        for field in new_fields:
            base_field = next(
                (f for f in base_fields if f["name"] == field["name"]), None)
            if not base_field:
                # Field only in new record - add it as nullable
                new_field = copy.deepcopy(field)
                new_field["type"] = self._make_nullable(new_field["type"])
                new_field["default"] = None
                base_fields.append(new_field)
            else:
                # Field in both records - merge types
                merged_type = self._merge_field_types(base_field["type"], field["type"])
                if merged_type is None:
                    return False, new_record
                base_field["type"] = merged_type
        
        # Make fields that are only in base record nullable
        for base_field in base_fields:
            if base_field["name"] not in new_field_names and base_field["name"] in base_field_names:
                if not self._is_nullable(base_field["type"]):
                    base_field["type"] = self._make_nullable(base_field["type"])
                    base_field["default"] = None

        base_record["fields"] = base_fields
        return True, base_record

    def _is_nullable(self, avro_type: JsonNode) -> bool:
        """Check if an Avro type is nullable (contains null in union)."""
        if avro_type == "null":
            return True
        if isinstance(avro_type, list):
            return "null" in avro_type
        return False

    def _make_nullable(self, avro_type: JsonNode) -> JsonNode:
        """Make an Avro type nullable by wrapping in union with null."""
        if self._is_nullable(avro_type):
            return avro_type
        if avro_type == "null":
            return "null"
        if isinstance(avro_type, list):
            # Already a union, add null if not present
            if "null" not in avro_type:
                return ["null"] + list(avro_type)
            return avro_type
        # Wrap in union with null first (for Avro default null)
        return ["null", avro_type]

    def _merge_field_types(self, type1: JsonNode, type2: JsonNode) -> JsonNode | None:
        """Merge two Avro types into a compatible type.
        
        Returns the merged type, or None if types are incompatible.
        """
        # If types are identical, return as-is
        if type1 == type2:
            return type1
        
        # Handle null combinations - create nullable type
        if type1 == "null":
            return self._make_nullable(type2)
        if type2 == "null":
            return self._make_nullable(type1)
        
        # If one is already nullable and other is compatible base type
        if isinstance(type1, list) and "null" in type1:
            non_null_types = [t for t in type1 if t != "null"]
            if len(non_null_types) == 1 and non_null_types[0] == type2:
                return type1
            # Check if type2 is compatible with any non-null type
            for t in non_null_types:
                if t == type2:
                    return type1
                if isinstance(t, dict) and isinstance(type2, dict):
                    if t.get("type") == type2.get("type") == "record":
                        success, merged = self.fold_record_types(t, type2)
                        if success:
                            return ["null", merged]
            # Add type2 to the union
            return type1 + [type2] if type2 not in type1 else type1
        
        if isinstance(type2, list) and "null" in type2:
            non_null_types = [t for t in type2 if t != "null"]
            if len(non_null_types) == 1 and non_null_types[0] == type1:
                return type2
            # Add type1 to the union
            return type2 + [type1] if type1 not in type2 else type2
        
        # Both are primitives but different - try to create union
        if isinstance(type1, str) and isinstance(type2, str):
            # Create a nullable union with both types
            return ["null", type1, type2]
        
        # Both are records - try to fold
        if isinstance(type1, dict) and isinstance(type2, dict):
            if type1.get("type") == type2.get("type") == "record":
                success, merged = self.fold_record_types(type1, type2)
                if success:
                    return merged
            elif type1.get("type") == type2.get("type") == "array":
                # Merge array item types
                items1 = type1.get("items", "string")
                items2 = type2.get("items", "string")
                merged_items = self._merge_field_types(items1, items2)
                if merged_items is not None:
                    return {"type": "array", "items": merged_items}
        
        return None

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

    def __init__(self, namespace: str = '', type_name_prefix: str = '', altnames_key: str = 'json',
                 infer_choices: bool = False, choice_depth: int = 1):
        """Initialize the Avro schema inferrer.

        Args:
            namespace: Namespace for generated types
            type_name_prefix: Prefix for generated type names
            altnames_key: Key to use for altnames mapping
            infer_choices: Whether to detect discriminated unions (choice types)
            choice_depth: Maximum nesting depth for recursive choice inference (1 = root only)
        """
        super().__init__(namespace, type_name_prefix, altnames_key)
        self.infer_choices = infer_choices
        self.choice_depth = choice_depth
        self.current_depth = 0  # Track current recursion depth

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
                    field["altnames"] = {self.altnames_key: original_key}
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

        # Check for discriminated unions if enabled
        if self.infer_choices:
            choice_result = self._infer_choice_type(type_name, values)
            if choice_result is not None:
                return choice_result

        unique_types = self.consolidated_type_list(
            type_name, values, self.python_type_to_avro_type)

        if len(unique_types) > 1:
            # Try to merge all types into a single compatible type
            merged = unique_types[0]
            for t in unique_types[1:]:
                merged = self._merge_field_types(merged, t)
                if merged is None:
                    # Can't merge - return as union
                    return unique_types
            return merged
        elif len(unique_types) == 1:
            return unique_types[0]
        else:
            return "string"

    def _infer_choice_type(self, type_name: str, values: List[Any]) -> JsonNode | None:
        """Detect and generate schema for discriminated unions.
        
        Returns an Avro union schema if a discriminated union is detected,
        or None to fall back to standard inference.
        """
        from avrotize.choice_inference import infer_choice_type
        from avrotize.common import avro_name
        
        result = infer_choice_type(values)
        
        if not result.is_choice:
            return None
        
        # Handle nested discriminator (envelope pattern)
        if result.nested_discriminator:
            nested = result.nested_discriminator
            parent_field = nested.field_path.split('.')[0]
            
            # Build the envelope type with the nested union
            envelope_cluster = result.clusters[0] if result.clusters else None
            if not envelope_cluster:
                return None
            
            # Get representative document
            rep_doc = envelope_cluster.documents[0].data if envelope_cluster.documents else {}
            
            # Build variant types from nested clusters
            variant_types = []
            for value in sorted(nested.values):
                # Find cluster for this value
                cluster_docs = [c for c in nested.nested_clusters 
                               if any(d.field_values.get(nested.discriminator_field) == value 
                                     for d in c.documents)]
                if not cluster_docs:
                    continue
                cluster = cluster_docs[0]
                
                # Build variant record
                variant_name = avro_name(''.join(word.capitalize() for word in value.replace('_', ' ').split()))
                variant_namespace = self.namespace + '.' + avro_name(type_name) + 'Types' if self.namespace else avro_name(type_name) + 'Types'
                
                # Get fields from cluster's representative document
                variant_doc = cluster.documents[0].data if cluster.documents else {}
                variant_fields = []
                
                # Add discriminator field with default value
                variant_fields.append({
                    "name": avro_name(nested.discriminator_field),
                    "type": "string",
                    "default": value
                })
                
                # Add other fields
                for field_name in sorted(cluster.merged_signature):
                    if field_name == nested.discriminator_field:
                        continue
                    safe_name = avro_name(field_name)
                    # Find first non-null value from cluster documents for type inference
                    field_value = None
                    for doc in cluster.documents:
                        val = doc.data.get(field_name)
                        if val is not None:
                            field_value = val
                            break
                    if field_value is None:
                        field_value = variant_doc.get(field_name)
                    field_type = self.python_type_to_avro_type(f"{type_name}.{parent_field}.{safe_name}", field_value)
                    
                    is_required = field_name in cluster.required_fields
                    if not is_required:
                        field_type = self._make_nullable(field_type)
                    
                    field_def: Dict[str, JsonNode] = {"name": safe_name, "type": field_type}
                    if not is_required:
                        field_def["default"] = None
                    if field_name != safe_name:
                        field_def["altnames"] = {self.altnames_key: field_name}
                    variant_fields.append(field_def)
                
                variant_record: Dict[str, JsonNode] = {
                    "type": "record",
                    "name": variant_name,
                    "namespace": variant_namespace,
                    "fields": variant_fields
                }
                variant_types.append(variant_record)
            
            # Build envelope record
            envelope_fields = []
            for field_name in sorted(envelope_cluster.merged_signature):
                safe_name = avro_name(field_name)
                
                if field_name == parent_field:
                    # This is the union field
                    envelope_fields.append({
                        "name": safe_name,
                        "type": variant_types if len(variant_types) > 1 else variant_types[0]
                    })
                else:
                    field_value = rep_doc.get(field_name)
                    field_type = self.python_type_to_avro_type(f"{type_name}.{safe_name}", field_value)
                    
                    is_required = field_name in envelope_cluster.required_fields
                    if not is_required:
                        field_type = self._make_nullable(field_type)
                    
                    field_def = {"name": safe_name, "type": field_type}
                    if not is_required:
                        field_def["default"] = None
                    if field_name != safe_name:
                        field_def["altnames"] = {self.altnames_key: field_name}
                    envelope_fields.append(field_def)
            
            envelope_namespace = self.namespace if self.namespace else ''
            envelope_record: Dict[str, JsonNode] = {
                "type": "record",
                "name": avro_name(type_name),
                "fields": envelope_fields
            }
            if envelope_namespace:
                envelope_record["namespace"] = envelope_namespace
            
            return envelope_record
        
        # Handle top-level discriminated union
        if result.discriminator_field:
            variant_types = []
            
            for value in sorted(result.discriminator_values):
                # Find cluster for this value
                cluster_docs = [c for c in result.clusters 
                               if any(d.field_values.get(result.discriminator_field) == value 
                                     for d in c.documents)]
                if not cluster_docs:
                    continue
                cluster = cluster_docs[0]
                
                # Build variant record
                variant_name = avro_name(''.join(word.capitalize() for word in value.replace('_', ' ').split()))
                variant_namespace = self.namespace + '.' + avro_name(type_name) + 'Types' if self.namespace else avro_name(type_name) + 'Types'
                
                variant_doc = cluster.documents[0].data if cluster.documents else {}
                variant_fields = []
                
                # Add discriminator field with default value first
                disc_safe = avro_name(result.discriminator_field)
                variant_fields.append({
                    "name": disc_safe,
                    "type": "string",
                    "default": value
                })
                
                # Add other fields
                for field_name in sorted(cluster.merged_signature):
                    if field_name == result.discriminator_field:
                        continue
                    safe_name = avro_name(field_name)
                    # Find first non-null value from cluster documents for type inference
                    field_value = None
                    for doc in cluster.documents:
                        val = doc.data.get(field_name)
                        if val is not None:
                            field_value = val
                            break
                    if field_value is None:
                        field_value = variant_doc.get(field_name)
                    field_type = self.python_type_to_avro_type(f"{type_name}.{safe_name}", field_value)
                    
                    is_required = field_name in cluster.required_fields
                    if not is_required:
                        field_type = self._make_nullable(field_type)
                    
                    field_def: Dict[str, JsonNode] = {"name": safe_name, "type": field_type}
                    if not is_required:
                        field_def["default"] = None
                    if field_name != safe_name:
                        field_def["altnames"] = {self.altnames_key: field_name}
                    variant_fields.append(field_def)
                
                variant_record: Dict[str, JsonNode] = {
                    "type": "record",
                    "name": variant_name,
                    "namespace": variant_namespace,
                    "fields": variant_fields
                }
                variant_types.append(variant_record)
            
            if len(variant_types) == 1:
                return variant_types[0]
            return variant_types
        
        # Undiscriminated union - fall back to standard inference
        return None

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
            # Try to merge all types into a single compatible type
            merged = unique_types[0]
            for t in unique_types[1:]:
                merged = self._merge_field_types(merged, t)
                if merged is None:
                    # Can't merge - return as union
                    return unique_types
            return merged
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

    def __init__(self, namespace: str = '', type_name_prefix: str = '', base_id: str = '',
                 infer_choices: bool = False, choice_depth: int = 1, infer_enums: bool = False,
                 enum_max_values: int = 50, enum_max_ratio: float = 0.1):
        """Initialize the JSON Structure schema inferrer.

        Args:
            namespace: Namespace for generated types
            type_name_prefix: Prefix for generated type names
            base_id: Base URI for $id generation
            infer_choices: Whether to detect discriminated unions (choice types)
            choice_depth: Maximum nesting depth for recursive choice inference (1 = root only)
            infer_enums: Whether to detect enum types from repeated string values
            enum_max_values: Maximum unique values to consider as enum (default 50)
            enum_max_ratio: Maximum ratio of unique values to samples (default 0.1 = 10%)
        """
        super().__init__(namespace, type_name_prefix)
        self.base_id = base_id or 'https://example.com/'
        self.definitions: Dict[str, Any] = {}
        self.infer_choices = infer_choices
        self.choice_depth = choice_depth
        self.current_depth = 0  # Track current recursion depth
        self.infer_enums = infer_enums
        self.enum_max_values = enum_max_values
        self.enum_max_ratio = enum_max_ratio

    # Regex patterns for temporal types - compiled once for performance
    _DATETIME_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:?\d{2})?$')
    _DATE_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}$')
    _TIME_PATTERN = re.compile(r'^\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:?\d{2})?$')
    _ID_PATTERN = re.compile(r'^[A-Z]{2,5}-[A-Z]{2,5}-[A-Z0-9]+$')
    _NUMERIC_STRING_PATTERN = re.compile(r'^-?\d+\.?\d*$')

    def _infer_string_type(self, value: str) -> str:
        """Infer the specific type for a string value (datetime, date, time, or string).
        
        Args:
            value: String value to analyze
            
        Returns:
            Type string: 'datetime', 'date', 'time', or 'string'
        """
        if self._DATETIME_PATTERN.match(value):
            return 'datetime'
        if self._DATE_PATTERN.match(value):
            return 'date'
        if self._TIME_PATTERN.match(value):
            return 'time'
        return 'string'

    def _infer_type_from_values(self, values: List[Any]) -> Dict[str, Any] | str | None:
        """Analyze multiple values to determine the best type.
        
        For string values: detect datetime, date, time patterns, or enum candidates.
        For numeric values: use the largest value to determine int32 vs larger types.
        
        Args:
            values: List of values for the same field
            
        Returns:
            Type schema if a specialized type was detected, None otherwise
        """
        if not values:
            return None
        
        # Filter out None values
        non_null_values = [v for v in values if v is not None]
        if not non_null_values:
            return None
        
        # All strings? Check for temporal types or enum
        string_values = [v for v in non_null_values if isinstance(v, str)]
        if len(string_values) == len(non_null_values) and string_values:
            # First check enum (high cardinality constraint means rare values)
            is_enum, enum_values = self._is_enum_candidate(string_values)
            if is_enum:
                return {"type": "string", "enum": enum_values}
            
            # Check for temporal patterns - need consistency
            inferred_types = [self._infer_string_type(v) for v in string_values]
            type_counts = {}
            for t in inferred_types:
                type_counts[t] = type_counts.get(t, 0) + 1
            
            # If >80% of values are same temporal type, use it
            for temporal_type in ['datetime', 'date', 'time']:
                if type_counts.get(temporal_type, 0) / len(string_values) >= 0.8:
                    return temporal_type
            
            return None  # Plain string
        
        # All integers? Check for range to pick type
        int_values = [v for v in non_null_values if isinstance(v, int) and not isinstance(v, bool)]
        if len(int_values) == len(non_null_values) and int_values:
            min_val = min(int_values)
            max_val = max(int_values)
            # int32 range: -2147483648 to 2147483647
            if min_val >= -2147483648 and max_val <= 2147483647:
                return None  # Default integer inference handles this
            else:
                return 'double'  # Large values need double
        
        return None

    def _is_enum_candidate(self, values: List[Any]) -> tuple[bool, List[str]]:
        """Check if a list of values represents an enum type.
        
        Args:
            values: List of field values from documents
            
        Returns:
            Tuple of (is_enum, sorted_unique_values)
        """
        if not self.infer_enums:
            return False, []
        
        # Filter to non-null string values
        string_values = [v for v in values if isinstance(v, str)]
        if len(string_values) < 5:  # Need sufficient samples
            return False, []
        
        unique_values = set(string_values)
        num_unique = len(unique_values)
        num_samples = len(string_values)
        
        # Require at least 2 unique values - single-value enums are useless
        if num_unique < 2:
            return False, []
        
        # Check cardinality constraints
        if num_unique > self.enum_max_values:
            return False, []
        
        ratio = num_unique / num_samples
        if ratio > self.enum_max_ratio:
            return False, []
        
        # Check if values look like enum values (not IDs, timestamps, etc.)
        for val in unique_values:
            # Skip if looks like UUID (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
            if len(val) == 36 and val.count('-') == 4:
                return False, []
            # Skip if looks like an ID pattern (e.g., DFL-MAT-J041T9, DFL-CLU-000003)
            if self._ID_PATTERN.match(val):
                return False, []
            # Skip if looks like ISO timestamp
            if self._DATETIME_PATTERN.match(val):
                return False, []
            # Skip if too long (probably not an enum)
            if len(val) > 50:
                return False, []
            # Skip if looks like a path or URL
            if '/' in val and len(val) > 20:
                return False, []
            # Skip if looks like a numeric value as string (coordinates, etc.)
            if self._NUMERIC_STRING_PATTERN.match(val):
                return False, []
        
        return True, sorted(unique_values)

    def _infer_object_type_from_values(self, type_name: str, obj_values: List[Dict]) -> Dict[str, Any]:
        """Infer object type from multiple object values with multi-value analysis.
        
        This collects all values for each field across the objects and applies:
        - Enum detection for repeated string values
        - Datetime/date/time pattern detection
        - Proper numeric range analysis
        
        Args:
            type_name: Name for the generated type
            obj_values: List of dict values to analyze
            
        Returns:
            Object schema with enhanced type inference
        """
        if not obj_values:
            return {"type": "object", "name": avro_name(type_name), "properties": {}}
        
        # Collect all values for each field
        field_values: Dict[str, List[Any]] = {}
        field_presence: Dict[str, int] = {}  # Count how many objects have this field
        
        for obj in obj_values:
            for key, value in obj.items():
                if key not in field_values:
                    field_values[key] = []
                    field_presence[key] = 0
                field_values[key].append(value)
                field_presence[key] += 1
        
        properties: Dict[str, Any] = {}
        required: List[str] = []
        total_objects = len(obj_values)
        
        for field_name in sorted(field_values.keys()):
            safe_name = avro_name(field_name)
            values = field_values[field_name]
            
            # Check if this field is required (present in all objects)
            is_required = field_presence[field_name] == total_objects
            
            # Try multi-value type inference first
            inferred_type = self._infer_type_from_values(values)
            
            if inferred_type is not None:
                if isinstance(inferred_type, str):
                    properties[safe_name] = {"type": inferred_type}
                else:
                    properties[safe_name] = inferred_type
            else:
                # Fall back to standard single-value inference
                # Find first non-null value
                sample_value = None
                for v in values:
                    if v is not None:
                        sample_value = v
                        break
                
                if sample_value is None:
                    properties[safe_name] = {"type": "null"}
                else:
                    prop_type = self.python_type_to_jstruct_type(
                        f"{type_name}.{safe_name}", sample_value)
                    if isinstance(prop_type, str):
                        properties[safe_name] = {"type": prop_type}
                    else:
                        properties[safe_name] = prop_type
            
            # Add altnames if field was transformed
            if field_name != safe_name:
                properties[safe_name]["altnames"] = {self.altnames_key: field_name}
            
            # Track required fields
            if is_required and properties[safe_name].get("type") != "null":
                required.append(safe_name)
        
        result: Dict[str, Any] = {
            "type": "object",
            "name": avro_name(type_name),
            "properties": properties
        }
        if required:
            result["required"] = required
        
        return result

    def _apply_recursive_choice_inference(self, type_name: str, schema: Dict[str, Any], 
                                           source_values: List[Any], depth: int) -> Dict[str, Any]:
        """Recursively apply choice inference to nested object properties.
        
        For each property in the object, collects all values from source data
        and checks if they form a discriminated union.
        
        Args:
            type_name: Name for the current type
            schema: The schema to enhance with choice types
            source_values: Original Python values (dicts) to analyze
            depth: Current recursion depth
            
        Returns:
            Enhanced schema with choice types where detected
        """
        if not self.infer_choices:
            return schema
        if depth >= self.choice_depth:
            return schema
        if not isinstance(schema, dict) or schema.get("type") != "object":
            return schema
        
        properties = schema.get("properties", {})
        if not properties:
            return schema
        
        from avrotize.choice_inference import infer_choice_type
        
        # For each property, collect values from all source objects and check for choices
        enhanced_properties: Dict[str, Any] = {}
        for prop_name, prop_schema in properties.items():
            # Collect all values for this property across all source objects
            prop_values = []
            for source in source_values:
                if isinstance(source, dict) and prop_name in source:
                    prop_values.append(source[prop_name])
                elif isinstance(source, dict):
                    # Check for altnames - property might have been renamed
                    altname = prop_schema.get("altnames", {}).get(self.altnames_key)
                    if altname and altname in source:
                        prop_values.append(source[altname])
            
            if len(prop_values) < 2:
                enhanced_properties[prop_name] = prop_schema
                continue
            
            # Check if property values are all dicts (potential choice type)
            if not all(isinstance(v, dict) for v in prop_values):
                # Also recursively process arrays of objects
                if all(isinstance(v, list) for v in prop_values):
                    # Flatten array items and check for choices
                    all_items = [item for v in prop_values for item in v if isinstance(item, dict)]
                    if len(all_items) >= 2:
                        result = infer_choice_type(all_items)
                        if result.is_choice:
                            # Generate choice type for array items
                            choice_schema = self._build_choice_from_inference(
                                f"{type_name}.{prop_name}", result, all_items, depth + 1)
                            if choice_schema:
                                enhanced_properties[prop_name] = {
                                    "type": "array",
                                    "items": choice_schema
                                }
                                continue
                enhanced_properties[prop_name] = prop_schema
                continue
            
            # Check for discriminated union in property values
            result = infer_choice_type(prop_values)
            if result.is_choice:
                # Generate choice type
                choice_schema = self._build_choice_from_inference(
                    f"{type_name}.{prop_name}", result, prop_values, depth + 1)
                if choice_schema:
                    enhanced_properties[prop_name] = choice_schema
                    continue
            
            # Not a choice - but still recursively process nested objects
            if isinstance(prop_schema, dict) and prop_schema.get("type") == "object":
                enhanced_properties[prop_name] = self._apply_recursive_choice_inference(
                    f"{type_name}.{prop_name}", prop_schema, prop_values, depth + 1)
            else:
                enhanced_properties[prop_name] = prop_schema
        
        schema["properties"] = enhanced_properties
        return schema
    
    def _build_choice_from_inference(self, type_name: str, result: Any, 
                                      values: List[Any], depth: int) -> Dict[str, Any] | None:
        """Build a choice schema from choice inference result.
        
        This is similar to _infer_choice_type but works at any depth level
        and can be called recursively.
        
        Args:
            type_name: Name for the choice type
            result: ChoiceInferenceResult from infer_choice_type
            values: Source values for this property
            depth: Current depth for further recursion
            
        Returns:
            Choice schema or None if unable to build
        """
        if not result.is_choice or not result.discriminator_field:
            return None
        
        choices_map: Dict[str, Any] = {}
        
        for cluster in result.clusters:
            if not cluster.documents:
                continue
            
            # Get discriminator value for this cluster
            disc_value = None
            for doc in cluster.documents:
                disc_value = doc.field_values.get(result.discriminator_field)
                if disc_value:
                    break
            
            if not disc_value or not isinstance(disc_value, str):
                continue
            
            # Build variant name
            variant_name = avro_name(''.join(word.capitalize() 
                                            for word in disc_value.replace('_', ' ').replace('-', ' ').split()))
            
            # Get representative document for this variant
            variant_doc = cluster.documents[0].data if cluster.documents else {}
            
            # Build properties for this variant
            properties: Dict[str, Any] = {}
            required: List[str] = []
            
            # Add discriminator field with default value
            disc_safe_name = avro_name(result.discriminator_field)
            properties[disc_safe_name] = {
                "type": "string",
                "default": disc_value
            }
            if result.discriminator_field != disc_safe_name:
                properties[disc_safe_name]["altnames"] = {self.altnames_key: result.discriminator_field}
            required.append(disc_safe_name)
            
            # Collect all values for each field across this cluster's documents
            cluster_values_by_field: Dict[str, List[Any]] = {}
            for doc in cluster.documents:
                for field_name, field_value in doc.data.items():
                    if field_name == result.discriminator_field:
                        continue
                    if field_name not in cluster_values_by_field:
                        cluster_values_by_field[field_name] = []
                    cluster_values_by_field[field_name].append(field_value)
            
            # Build field schemas with potential recursive choice inference
            for field_name in sorted(cluster.merged_signature):
                if field_name == result.discriminator_field:
                    continue
                safe_name = avro_name(field_name)
                
                # Collect all values for this field
                field_values = cluster_values_by_field.get(field_name, [])
                
                # Check if field values form a discriminated union (only if we're within depth limit)
                if depth < self.choice_depth and len(field_values) >= 2:
                    # Filter to dict values only for choice inference
                    dict_values = [v for v in field_values if isinstance(v, dict)]
                    if len(dict_values) >= 2:
                        from avrotize.choice_inference import infer_choice_type as _infer
                        nested_choice_result = _infer(dict_values)
                        if nested_choice_result.is_choice:
                            # Build nested choice type
                            nested_choice = self._build_choice_from_inference(
                                f"{type_name}.{safe_name}", nested_choice_result, dict_values, depth + 1)
                            if nested_choice:
                                properties[safe_name] = nested_choice
                                if field_name != safe_name:
                                    if "altnames" not in properties[safe_name]:
                                        properties[safe_name]["altnames"] = {}
                                    properties[safe_name]["altnames"][self.altnames_key] = field_name
                                required.append(safe_name)
                                continue
                
                # Check if field values form an enum (small set of repeated string values)
                is_enum, enum_values = self._is_enum_candidate(field_values)
                if is_enum:
                    properties[safe_name] = {
                        "type": "string",
                        "enum": enum_values
                    }
                    if field_name != safe_name:
                        properties[safe_name]["altnames"] = {self.altnames_key: field_name}
                    if field_name in cluster.required_fields:
                        required.append(safe_name)
                    continue
                
                # Try multi-value type inference (datetime detection, numeric range)
                inferred_type = self._infer_type_from_values(field_values)
                if inferred_type is not None:
                    if isinstance(inferred_type, str):
                        properties[safe_name] = {"type": inferred_type}
                    else:
                        properties[safe_name] = inferred_type
                    if field_name != safe_name:
                        if "altnames" not in properties[safe_name]:
                            properties[safe_name]["altnames"] = {}
                        properties[safe_name]["altnames"][self.altnames_key] = field_name
                    if field_name in cluster.required_fields:
                        required.append(safe_name)
                    continue
                
                # No choice detected or depth limit reached - use standard type inference
                # Find first non-null value from cluster values for type inference
                field_value = None
                for fv in field_values:
                    if fv is not None:
                        field_value = fv
                        break
                # Fall back to first doc value if all are null
                if field_value is None:
                    field_value = variant_doc.get(field_name)
                field_type = self.python_type_to_jstruct_type(f"{type_name}.{safe_name}", field_value)
                
                # Still apply recursive processing for nested objects
                if depth < self.choice_depth and len(field_values) >= 2 and isinstance(field_type, dict) and field_type.get("type") == "object":
                    field_type = self._apply_recursive_choice_inference(
                        f"{type_name}.{safe_name}", field_type, field_values, depth + 1)
                
                if isinstance(field_type, str):
                    properties[safe_name] = {"type": field_type}
                else:
                    properties[safe_name] = field_type
                
                if field_name != safe_name:
                    if "altnames" not in properties[safe_name]:
                        properties[safe_name]["altnames"] = {}
                    properties[safe_name]["altnames"][self.altnames_key] = field_name
                
                # Use cluster.required_fields to determine if field appears in all documents
                if field_name in cluster.required_fields:
                    required.append(safe_name)
            
            variant_schema: Dict[str, Any] = {
                "type": "object",
                "name": variant_name,
                "properties": properties
            }
            if required:
                variant_schema["required"] = required
            
            choices_map[variant_name] = variant_schema
        
        if not choices_map:
            return None
        
        return {
            "type": "choice",
            "selector": result.discriminator_field,
            "choices": choices_map,
            "name": avro_name(type_name.rsplit('.', 1)[-1])
        }

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

        # Handle integers with proper range detection
        # bool is subclass of int in Python, so check bool first
        if isinstance(python_value, bool):
            return "boolean"
        
        if isinstance(python_value, int):
            # Check if value fits in int32 range
            if -2147483648 <= python_value <= 2147483647:
                return "integer"  # int32 alias
            else:
                # Per JSON Structure spec, int64 values are string-encoded
                # Since we're inferring from JSON native numbers (which can't exceed
                # double precision ~2^53), use 'double' for large integers from JSON
                # This allows validation of the source data as-is
                return "double"

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
                    properties[safe_key]["altnames"] = {self.altnames_key: original_key}

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
                    # choices must be a map with type names as keys
                    choices_map: Dict[str, Any] = {}
                    for it in item_types:
                        if isinstance(it, str):
                            choices_map[it] = {"type": it}
                        elif isinstance(it, dict):
                            # For object types, use name if available
                            name = it.get("name", f"type{len(choices_map)}")
                            choices_map[name] = it
                    items = {"type": "choice", "choices": choices_map}
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
        # If all values are objects, use multi-value analysis for properties
        if python_values and all(isinstance(v, dict) for v in python_values):
            result = self._infer_object_type_from_values(type_name, python_values)
            if result:
                return [result]
        
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
                # Build choices map from item types
                choices_map: Dict[str, Any] = {}
                for it in item_types:
                    if isinstance(it, str):
                        choices_map[it] = {"type": it}
                    elif isinstance(it, dict):
                        name = it.get("name", f"type{len(choices_map)}")
                        choices_map[name] = it
                list_types.append({"type": "array", "items": {"type": "choice", "choices": choices_map}})

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

        # Check for discriminated unions if enabled
        if self.infer_choices:
            choice_result = self._infer_choice_type(type_name, values)
            if choice_result is not None:
                return self._wrap_schema(choice_result, type_name)

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

    def _infer_choice_type(self, type_name: str, values: List[Any]) -> Dict[str, Any] | None:
        """Detect and generate schema for discriminated unions.
        
        Returns a JSON Structure choice schema if a discriminated union is detected,
        or None to fall back to standard inference.
        """
        from avrotize.choice_inference import infer_choice_type
        from avrotize.common import avro_name
        
        result = infer_choice_type(values)
        
        if not result.is_choice:
            return None
        
        # Handle nested discriminator (envelope pattern)
        if result.nested_discriminator:
            nested = result.nested_discriminator
            parent_field = nested.field_path.split('.')[0]
            
            envelope_cluster = result.clusters[0] if result.clusters else None
            if not envelope_cluster:
                return None
            
            rep_doc = envelope_cluster.documents[0].data if envelope_cluster.documents else {}
            
            # Build variant types from nested clusters
            variant_types = []
            for value in sorted(nested.values):
                cluster_docs = [c for c in nested.nested_clusters 
                               if any(d.field_values.get(nested.discriminator_field) == value 
                                     for d in c.documents)]
                if not cluster_docs:
                    continue
                cluster = cluster_docs[0]
                
                variant_name = avro_name(''.join(word.capitalize() for word in value.replace('_', ' ').split()))
                variant_doc = cluster.documents[0].data if cluster.documents else {}
                
                properties: Dict[str, Any] = {}
                required: List[str] = []
                
                # Add discriminator field with default
                properties[avro_name(nested.discriminator_field)] = {
                    "type": "string",
                    "default": value
                }
                required.append(avro_name(nested.discriminator_field))
                
                # Add other fields with potential recursive choice inference
                # Collect values for each field across all documents in this cluster
                cluster_values_by_field: Dict[str, List[Any]] = {}
                for doc in cluster.documents:
                    for fn, fv in doc.data.items():
                        if fn == nested.discriminator_field:
                            continue
                        if fn not in cluster_values_by_field:
                            cluster_values_by_field[fn] = []
                        cluster_values_by_field[fn].append(fv)
                
                for field_name in sorted(cluster.merged_signature):
                    if field_name == nested.discriminator_field:
                        continue
                    safe_name = avro_name(field_name)
                    
                    # Collect all values for this field
                    field_values = cluster_values_by_field.get(field_name, [])
                    
                    # Check if field values form a discriminated union
                    if self.choice_depth > 1 and len(field_values) >= 2:
                        dict_values = [v for v in field_values if isinstance(v, dict)]
                        if len(dict_values) >= 2:
                            from avrotize.choice_inference import infer_choice_type as _infer
                            nested_choice_result = _infer(dict_values)
                            if nested_choice_result.is_choice:
                                nested_choice = self._build_choice_from_inference(
                                    f"{type_name}.{parent_field}.{safe_name}", 
                                    nested_choice_result, dict_values, 2)
                                if nested_choice:
                                    properties[safe_name] = nested_choice
                                    if field_name != safe_name:
                                        if "altnames" not in properties[safe_name]:
                                            properties[safe_name]["altnames"] = {}
                                        properties[safe_name]["altnames"][self.altnames_key] = field_name
                                    if field_name in cluster.required_fields:
                                        required.append(safe_name)
                                    continue
                    
                    # Check if field values form an enum
                    is_enum, enum_values = self._is_enum_candidate(field_values)
                    if is_enum:
                        properties[safe_name] = {
                            "type": "string",
                            "enum": enum_values
                        }
                        if field_name != safe_name:
                            properties[safe_name]["altnames"] = {self.altnames_key: field_name}
                        if field_name in cluster.required_fields:
                            required.append(safe_name)
                        continue
                    
                    # Try multi-value type inference (datetime detection, numeric range)
                    inferred_type = self._infer_type_from_values(field_values)
                    if inferred_type is not None:
                        if isinstance(inferred_type, str):
                            properties[safe_name] = {"type": inferred_type}
                        else:
                            properties[safe_name] = inferred_type
                        if field_name != safe_name:
                            if "altnames" not in properties[safe_name]:
                                properties[safe_name]["altnames"] = {}
                            properties[safe_name]["altnames"][self.altnames_key] = field_name
                        if field_name in cluster.required_fields:
                            required.append(safe_name)
                        continue
                    
                    # No choice detected - standard type inference
                    # Find first non-null value from cluster values for type inference
                    field_value = None
                    for fv in field_values:
                        if fv is not None:
                            field_value = fv
                            break
                    if field_value is None:
                        field_value = variant_doc.get(field_name)
                    field_type = self.python_type_to_jstruct_type(f"{type_name}.{parent_field}.{safe_name}", field_value)
                    
                    if isinstance(field_type, str):
                        properties[safe_name] = {"type": field_type}
                    else:
                        properties[safe_name] = field_type
                    
                    if field_name != safe_name:
                        properties[safe_name]["altnames"] = {self.altnames_key: field_name}
                    
                    if field_name in cluster.required_fields:
                        required.append(safe_name)
                
                variant_record: Dict[str, Any] = {
                    "type": "object",
                    "name": variant_name,
                    "properties": properties
                }
                if required:
                    variant_record["required"] = required
                variant_types.append(variant_record)
            
            # Build envelope with choice payload
            envelope_properties: Dict[str, Any] = {}
            envelope_required: List[str] = []
            
            # Collect all values for envelope fields for multi-value inference
            envelope_values_by_field: Dict[str, List[Any]] = {}
            for doc in envelope_cluster.documents:
                for fn, fv in doc.data.items():
                    if fn not in envelope_values_by_field:
                        envelope_values_by_field[fn] = []
                    if fv is not None:
                        envelope_values_by_field[fn].append(fv)
            
            for field_name in sorted(envelope_cluster.merged_signature):
                safe_name = avro_name(field_name)
                
                if field_name == parent_field:
                    if len(variant_types) > 1:
                        # Build choices as a map (object) per JSON Structure spec
                        # Each value is a schema directly (the object type definition)
                        choices_map: Dict[str, Any] = {}
                        for vt in variant_types:
                            choices_map[vt["name"]] = vt
                        envelope_properties[safe_name] = {
                            "type": "choice",
                            "choices": choices_map
                        }
                    else:
                        envelope_properties[safe_name] = variant_types[0] if variant_types else {"type": "object"}
                else:
                    # Collect field values for multi-value inference
                    field_values = envelope_values_by_field.get(field_name, [])
                    
                    # Check if field values form an enum
                    is_enum, enum_values = self._is_enum_candidate(field_values)
                    if is_enum:
                        envelope_properties[safe_name] = {
                            "type": "string",
                            "enum": enum_values
                        }
                        if field_name != safe_name:
                            envelope_properties[safe_name]["altnames"] = {self.altnames_key: field_name}
                        if field_name in envelope_cluster.required_fields:
                            envelope_required.append(safe_name)
                        continue
                    
                    # Try multi-value type inference
                    inferred_type = self._infer_type_from_values(field_values)
                    if inferred_type is not None:
                        if isinstance(inferred_type, str):
                            envelope_properties[safe_name] = {"type": inferred_type}
                        else:
                            envelope_properties[safe_name] = inferred_type
                        if field_name != safe_name:
                            if "altnames" not in envelope_properties[safe_name]:
                                envelope_properties[safe_name]["altnames"] = {}
                            envelope_properties[safe_name]["altnames"][self.altnames_key] = field_name
                        if field_name in envelope_cluster.required_fields:
                            envelope_required.append(safe_name)
                        continue
                    
                    # Standard type inference from first non-null value
                    field_value = None
                    for fv in field_values:
                        if fv is not None:
                            field_value = fv
                            break
                    if field_value is None:
                        field_value = rep_doc.get(field_name)
                    field_type = self.python_type_to_jstruct_type(f"{type_name}.{safe_name}", field_value)
                    
                    if isinstance(field_type, str):
                        envelope_properties[safe_name] = {"type": field_type}
                    else:
                        envelope_properties[safe_name] = field_type
                    
                    if field_name != safe_name:
                        envelope_properties[safe_name]["altnames"] = {self.altnames_key: field_name}
                
                if field_name in envelope_cluster.required_fields:
                    envelope_required.append(safe_name)
            
            envelope_record: Dict[str, Any] = {
                "type": "object",
                "name": avro_name(type_name),
                "properties": envelope_properties
            }
            if envelope_required:
                envelope_record["required"] = envelope_required
            
            return envelope_record
        
        # Handle top-level discriminated union as inline union
        # Inline unions match the actual instance format where the discriminator
        # is a property value, not a key wrapper (tagged union)
        if result.discriminator_field:
            # Collect all fields from all variants to find common vs variant-specific
            all_variant_fields: Dict[str, Set[str]] = {}  # variant_value -> field names
            variant_docs: Dict[str, Dict[str, Any]] = {}  # variant_value -> sample doc
            
            for value in sorted(result.discriminator_values):
                cluster_docs = [c for c in result.clusters 
                               if any(d.field_values.get(result.discriminator_field) == value 
                                     for d in c.documents)]
                if not cluster_docs:
                    continue
                cluster = cluster_docs[0]
                all_variant_fields[value] = set(cluster.merged_signature)
                variant_docs[value] = cluster.documents[0].data if cluster.documents else {}
            
            if not all_variant_fields:
                return None
            
            # Find common fields (present in ALL variants)
            common_fields = set.intersection(*all_variant_fields.values()) if all_variant_fields else set()
            
            # Build abstract base type with common fields
            base_name = avro_name(type_name) + "Base"
            base_properties: Dict[str, Any] = {}
            base_required: List[str] = []
            
            # Use first variant's doc for type inference of common fields
            first_value = sorted(result.discriminator_values)[0]
            rep_doc = variant_docs.get(first_value, {})
            
            for field_name in sorted(common_fields):
                safe_name = avro_name(field_name)
                
                # Collect all values for this field across all clusters for enum detection
                all_field_values: List[Any] = []
                for cluster in result.clusters:
                    for doc in cluster.documents:
                        val = doc.data.get(field_name)
                        if val is not None:
                            all_field_values.append(val)
                
                # Check if field values form an enum
                is_enum, enum_values = self._is_enum_candidate(all_field_values)
                if is_enum:
                    base_properties[safe_name] = {
                        "type": "string",
                        "enum": enum_values
                    }
                    if field_name != safe_name:
                        base_properties[safe_name]["altnames"] = {self.altnames_key: field_name}
                    # Check if required in all clusters
                    all_required = all(
                        field_name in c.required_fields 
                        for c in result.clusters if c.merged_signature
                    )
                    if all_required and field_name != result.discriminator_field:
                        base_required.append(safe_name)
                    continue
                
                # Try multi-value type inference (datetime detection, numeric range)
                inferred_type = self._infer_type_from_values(all_field_values)
                if inferred_type is not None:
                    if isinstance(inferred_type, str):
                        base_properties[safe_name] = {"type": inferred_type}
                    else:
                        base_properties[safe_name] = inferred_type
                    if field_name != safe_name:
                        if "altnames" not in base_properties[safe_name]:
                            base_properties[safe_name]["altnames"] = {}
                        base_properties[safe_name]["altnames"][self.altnames_key] = field_name
                    # Check if required in all clusters
                    all_required = all(
                        field_name in c.required_fields 
                        for c in result.clusters if c.merged_signature
                    )
                    if all_required and field_name != result.discriminator_field:
                        base_required.append(safe_name)
                    continue
                
                # Find first non-null value for type inference
                field_value = None
                for val in all_field_values:
                    if val is not None:
                        field_value = val
                        break
                if field_value is None:
                    field_value = rep_doc.get(field_name)
                field_type = self.python_type_to_jstruct_type(f"{type_name}.{safe_name}", field_value)
                
                if isinstance(field_type, str):
                    base_properties[safe_name] = {"type": field_type}
                else:
                    base_properties[safe_name] = field_type
                
                if field_name != safe_name:
                    base_properties[safe_name]["altnames"] = {self.altnames_key: field_name}
                
                # Check if required in all clusters
                # Note: discriminator field is NOT required as it's handled by selector
                all_required = all(
                    field_name in c.required_fields 
                    for c in result.clusters if c.merged_signature
                )
                if all_required and field_name != result.discriminator_field:
                    base_required.append(safe_name)
            
            base_type: Dict[str, Any] = {
                "abstract": True,
                "type": "object",
                "name": base_name,
                "properties": base_properties
            }
            if base_required:
                base_type["required"] = base_required
            
            # Build variant types that extend the base
            definitions: Dict[str, Any] = {base_name: base_type}
            choices_map: Dict[str, Any] = {}
            
            for value in sorted(result.discriminator_values):
                if value not in all_variant_fields:
                    continue
                
                # Type name is PascalCase for definitions
                variant_name = avro_name(''.join(word.capitalize() for word in value.replace('_', ' ').split()))
                # Choice key must match actual selector value in instances
                choice_key = value
                
                variant_doc = variant_docs.get(value, {})
                variant_specific = all_variant_fields[value] - common_fields
                
                # Get cluster with all documents for this variant
                cluster_for_variant = next(
                    (c for c in result.clusters 
                     if any(d.field_values.get(result.discriminator_field) == value 
                           for d in c.documents)),
                    None
                )
                
                properties: Dict[str, Any] = {}
                required: List[str] = []
                
                # Add variant-specific fields only (common fields inherited from base)
                for field_name in sorted(variant_specific):
                    safe_name = avro_name(field_name)
                    
                    # Find the first non-null value across all documents in this variant
                    # to properly infer the type
                    field_value = None
                    if cluster_for_variant:
                        for doc in cluster_for_variant.documents:
                            val = doc.data.get(field_name)
                            if val is not None:
                                field_value = val
                                break
                    if field_value is None:
                        field_value = variant_doc.get(field_name)
                    
                    # Collect all values for enum detection
                    field_values = []
                    if cluster_for_variant:
                        for doc in cluster_for_variant.documents:
                            val = doc.data.get(field_name)
                            if val is not None:
                                field_values.append(val)
                    
                    # Check if field values form an enum
                    is_enum, enum_values = self._is_enum_candidate(field_values)
                    if is_enum:
                        properties[safe_name] = {
                            "type": "string",
                            "enum": enum_values
                        }
                        if field_name != safe_name:
                            properties[safe_name]["altnames"] = {self.altnames_key: field_name}
                        if cluster_for_variant and field_name in cluster_for_variant.required_fields:
                            required.append(safe_name)
                        continue
                    
                    # Try multi-value type inference (datetime detection, numeric range)
                    inferred_type = self._infer_type_from_values(field_values)
                    if inferred_type is not None:
                        if isinstance(inferred_type, str):
                            properties[safe_name] = {"type": inferred_type}
                        else:
                            properties[safe_name] = inferred_type
                        if field_name != safe_name:
                            if "altnames" not in properties[safe_name]:
                                properties[safe_name]["altnames"] = {}
                            properties[safe_name]["altnames"][self.altnames_key] = field_name
                        if cluster_for_variant and field_name in cluster_for_variant.required_fields:
                            required.append(safe_name)
                        continue
                    
                    field_type = self.python_type_to_jstruct_type(f"{type_name}.{safe_name}", field_value)
                    
                    if isinstance(field_type, str):
                        properties[safe_name] = {"type": field_type}
                    else:
                        properties[safe_name] = field_type
                    
                    if field_name != safe_name:
                        properties[safe_name]["altnames"] = {self.altnames_key: field_name}
                    
                    # Field is required only if present in all documents of this variant
                    if cluster_for_variant and field_name in cluster_for_variant.required_fields:
                        required.append(safe_name)
                
                variant_record: Dict[str, Any] = {
                    "type": "object",
                    "name": variant_name,
                    "$extends": f"#/definitions/{base_name}",
                    "properties": properties
                }
                if required:
                    variant_record["required"] = required
                
                # Apply recursive choice inference if depth allows
                if self.choice_depth > 1 and cluster_for_variant:
                    cluster_values = [d.data for d in cluster_for_variant.documents]
                    variant_record = self._apply_recursive_choice_inference(
                        f"{type_name}.{variant_name}", variant_record, cluster_values, 1)
                
                definitions[variant_name] = variant_record
                # Use actual discriminator value as choice key (must match selector in instances)
                choices_map[choice_key] = {"type": {"$ref": f"#/definitions/{variant_name}"}}
            
            if len(choices_map) == 1:
                # Single variant - just return it directly
                return list(definitions.values())[1]  # Skip base, return the variant
            
            # Build inline union choice type
            disc_safe = avro_name(result.discriminator_field)
            return {
                "type": "choice",
                "name": avro_name(type_name),
                "$extends": f"#/definitions/{base_name}",
                "selector": disc_safe,
                "choices": choices_map,
                "definitions": definitions
            }
        
        # Undiscriminated union - fall back to standard inference
        return None

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
