""" JSON Schema to JSON Structure converter. """

# pylint: disable=too-many-lines, line-too-long, too-many-branches, too-many-statements, too-many-locals, too-many-nested-blocks, too-many-arguments, too-many-instance-attributes, too-many-public-methods, too-many-boolean-expressions

import json
import os
import copy
import re
import urllib.parse
from urllib.parse import ParseResult, urlparse, unquote
from typing import Any, Dict, List, Tuple, Union, Optional
import jsonpointer
from jsonpointer import JsonPointerException
import requests

from avrotize.common import avro_name, avro_name_with_altname, avro_namespace, find_schema_node, generic_type, set_schema_node
from avrotize.dependency_resolver import inline_dependencies_of, sort_messages_by_dependencies

# JSON Structure primitive types
structure_primitive_types = [
    'null', 'string', 'int8', 'int16', 'int32', 'int64', 
    'uint8', 'uint16', 'uint32', 'uint64', 'float', 'double', 
    'decimal', 'boolean', 'bytes', 'date', 'time', 'datetime', 
    'duration', 'uuid', 'set', 'map', 'object', 'choice'
]


class JsonToStructureConverter:
    """
    Converts JSON Schema documents to JSON Structure format.

    Attributes:
    imported_types: A dictionary of imported type schemas.
    root_namespace: The namespace for the root schema.
    max_recursion_depth: The maximum recursion depth.
    types_with_unmerged_types: A list of types with unmerged types.
    content_cache: A dictionary for caching fetched URLs.
    utility_namespace: The namespace for utility types.
    preserve_composition: Flag to preserve composition keywords.
    detect_inheritance: Flag to detect inheritance patterns.    detect_discriminators: Flag to detect OpenAPI discriminator patterns.
    convert_empty_objects_to_maps: Flag to convert objects with only additionalProperties to maps.
    split_top_level_records: Flag to split top-level records.
    root_class_name: The name of the root class.
    """

    def __init__(self) -> None:
        self.imported_types: Dict[Any, Any] = {}
        self.root_namespace = 'example.com'
        self.max_recursion_depth = 40
        self.types_with_unmerged_types: List[dict] = []
        self.content_cache: Dict[str, str] = {}
        self.utility_namespace = 'utility.vasters.com'
        self.split_top_level_records = False
        self.root_class_name = 'document'
        self.type_registry: Dict[str, str] = {}  # Track type definitions for reference resolution
          # JSON Structure specific configuration
        self.preserve_composition = False  # Resolve composition keywords by default for JSON Structure compliance
        self.detect_inheritance = True
        self.detect_discriminators = True
        self.convert_empty_objects_to_maps = True

    def is_empty_type(self, structure_type):
        """
        Check if the JSON Structure type is an empty type.

        Parameters:
        structure_type (any): The JSON Structure type to check.

        Returns:
        bool: True if the type is empty, False otherwise.
        """
        if len(structure_type) == 0:
            return True
        if isinstance(structure_type, list):
            return all(self.is_empty_type(t) for t in structure_type)
        if isinstance(structure_type, dict):            
            if not 'type' in structure_type:
                return True
            if (structure_type['type'] == 'object' and (not 'properties' in structure_type or len(structure_type['properties']) == 0)) or \
               (structure_type['type'] == 'choice' and (not 'choices' in structure_type or len(structure_type['choices']) == 0)) or \
               (structure_type['type'] == 'set' and (not 'items' in structure_type or not structure_type['items'])) or \
               (structure_type['type'] == 'map' and (not 'values' in structure_type or not structure_type['values'])):
                return True
        return False

    def is_empty_json_type(self, json_type):
        """
        Check if the JSON type is an empty type.

        Parameters:
        json_type (any): The JSON type to check.

        Returns:
        bool: True if the JSON type is empty, False otherwise.
        """
        if len(json_type) == 0:
            return True
        if isinstance(json_type, list):
            return all(self.is_empty_json_type(t) for t in json_type)
        elif isinstance(json_type, dict):
            if not 'type' in json_type:
                return True
        return False

    def detect_numeric_type(self, schema: dict) -> str:
        """
        Analyze schema constraints to determine the appropriate numeric type.
        
        Args:
            schema (dict): The JSON schema object
            
        Returns:
            str: The appropriate JSON Structure numeric type
        """
        # Check for format hints first
        format_hint = schema.get('format')
        if format_hint:
            format_mapping = {
                'int8': 'int32',     # Use int32 instead of int8 for better compatibility
                'int16': 'int32',    # Use int32 instead of int16 for better compatibility  
                'int32': 'int32',
                'int64': 'int64',
                'uint8': 'int32',    # Use int32 instead of uint8 for better compatibility
                'uint16': 'int32',   # Use int32 instead of uint16 for better compatibility
                'uint32': 'int64',   # Use int64 instead of uint32 for better compatibility
                'uint64': 'int64',   # Use int64 instead of uint64 for better compatibility
                'float': 'float',
                'double': 'double'
            }
            if format_hint in format_mapping:
                return format_mapping[format_hint]
        
        # Analyze constraints for integer types
        if schema.get('type') == 'integer':
            minimum = schema.get('minimum', schema.get('exclusiveMinimum'))
            maximum = schema.get('maximum', schema.get('exclusiveMaximum'))
            
            # For integers with constraints, use conservative type mapping
            if minimum is not None and maximum is not None:
                # Both bounds specified
                if minimum >= -2147483648 and maximum <= 2147483647:
                    return 'int32'
                else:
                    return 'int64'
            elif minimum is not None and minimum >= 0:
                # Non-negative integers - use int32 for reasonable ranges
                if maximum is None or maximum <= 2147483647:
                    return 'int32'  # Conservative choice for age-like fields
                else:
                    return 'int64'
            else:
                # General integers or negative minimum
                return 'int32'  # Conservative default
        
        # For number type, check for decimal indicators
        elif schema.get('type') == 'number':
            if 'multipleOf' in schema:
                multiple_of = schema['multipleOf']
                if isinstance(multiple_of, float) or '.' in str(multiple_of):
                    return 'decimal'
            
            # Check for precision/scale hints in description or custom properties
            if 'precision' in schema or 'scale' in schema:
                return 'decimal'
                
            return 'double'  # Default for floating point
            
        return 'double'  # Default fallback

    def detect_temporal_type(self, schema: dict) -> str:
        """
        Detect temporal types based on format.
        
        Args:
            schema (dict): The JSON schema object
            
        Returns:
            str: The appropriate JSON Structure temporal type
        """
        format_hint = schema.get('format')
        if format_hint:
            temporal_mapping = {
                'date': 'date',
                'time': 'time', 
                'date-time': 'datetime',
                'duration': 'duration'
            }
            return temporal_mapping.get(format_hint, 'string')
        return 'string'

    def detect_collection_type(self, schema: dict) -> str:
        """
        Determine if array should be 'set' based on uniqueItems.
        
        Args:
            schema (dict): The JSON schema array object
            
        Returns:
            str: Either 'set' or array (for list)        """
        if schema.get('type') == 'array' and schema.get('uniqueItems', False):
            return 'set'
        return 'array'

    def should_convert_to_map(self, json_object: dict) -> bool:
        """
        Determine if object should be converted to map type.
        
        Args:
            json_object (dict): The JSON schema object
            
        Returns:
            bool: True if should be converted to map
        """
        if not self.convert_empty_objects_to_maps:
            return False
            
        # Convert if object has only additionalProperties and no properties
        if ('additionalProperties' in json_object and 
            (not 'properties' in json_object or len(json_object['properties']) == 0) and
            (not 'patternProperties' in json_object or len(json_object['patternProperties']) == 0)):
            return True
            
        # Convert if object has only patternProperties and no properties or additionalProperties
        if ('patternProperties' in json_object and
            (not 'properties' in json_object or len(json_object['properties']) == 0) and
            'additionalProperties' not in json_object):
            return True
            
        return False

    def detect_discriminator_pattern(self, schema: dict) -> dict | None:
        """
        Detect OpenAPI discriminator patterns for choice type.
        
        Args:
            schema (dict): The JSON schema object
            
        Returns:
            dict | None: Discriminator info if detected, None otherwise
        """
        if not self.detect_discriminators:
            return None
            
        # Check for OpenAPI discriminator
        if 'discriminator' in schema:
            discriminator = schema['discriminator']
            if isinstance(discriminator, dict) and 'propertyName' in discriminator:
                return {
                    'propertyName': discriminator['propertyName'],
                    'mapping': discriminator.get('mapping', {})
                }
        
        return None
        
        # Check for oneOf with discriminator-like pattern
        if 'oneOf' in schema:
            # Look for common property across all oneOf options that could be a discriminator
            oneof_options = schema['oneOf']
            if len(oneof_options) > 1:
                common_props = None
                for option in oneof_options:
                    if '$ref' in option:
                        continue  # Skip refs for now
                    if 'properties' in option:
                        props = set(option['properties'].keys())
                        if common_props is None:
                            common_props = props
                        else:
                            common_props = common_props.intersection(props)
                
                # If there's exactly one common property, it might be a discriminator
                if common_props and len(common_props) == 1:
                    prop_name = list(common_props)[0]
                    return {
                        'property': prop_name,
                        'mapping': {}  # Would need more analysis to populate
                    }
        
        return None

    def detect_inheritance_pattern(self, schema: dict, type_name: str = '') -> dict | None:
        """
        Detect simple inheritance patterns in allOf schemas.
        
        Only detects patterns with exactly 2 items where one is a $ref and the other 
        contains properties/required/other object schema keywords.
        Excludes self-referential patterns.
        
        Args:
            schema (dict): The JSON schema object
            type_name (str): The name of the current type (to detect self-references)
            
        Returns:
            dict | None: Inheritance info if detected, None otherwise
        """
        if not self.detect_inheritance or 'allOf' not in schema:
            return None
            
        allof_items = schema['allOf']
        
        # Only handle simple 2-item inheritance patterns
        if len(allof_items) != 2:
            return None
            
        # Look for pattern: [{"$ref": "..."}, {"properties": {...}}] or similar
        ref_item = None
        extension_item = None
        
        for item in allof_items:
            if '$ref' in item and len(item) == 1:  # Pure reference, no other properties
                ref_item = item
            elif ('type' in item or 'properties' in item or 'required' in item or 
                  'additionalProperties' in item) and '$ref' not in item:  # Pure extension, no ref
                extension_item = item
                
        # Only return inheritance info for simple base + extension pattern
        if ref_item and extension_item:
            base_ref = ref_item['$ref']
            
            # Check for self-referential patterns
            if base_ref.startswith('#/definitions/'):
                ref_type_name = base_ref[14:]  # Remove '#/definitions/'
                if ref_type_name == type_name:
                    # Self-referential pattern - don't convert to inheritance
                    return None
            
            return {
                'base_ref': base_ref,
                'extension': extension_item
            }
            
        return None

    def json_schema_primitive_to_structure_type(self, json_primitive: Optional[str | list], format: Optional[str], enum: Optional[list], record_name: str, field_name: str, namespace: str, dependencies: list, schema: dict) -> str | dict[str, Any] | list:
        """
        Convert a JSON Schema primitive type to JSON Structure primitive type.

        Args:
            json_primitive (str | list): The JSON Schema primitive type to be converted.
            format (str | None): The format of the JSON primitive type, if applicable.
            enum (list | None): The list of enum values, if applicable.
            record_name (str): The name of the record.
            field_name (str): The name of the field.
            namespace (str): The namespace of the type.
            dependencies (list): The list of dependencies.
            schema (dict): The complete schema object for analysis.        Returns:
            str | dict[str,Any] | list: The converted JSON Structure primitive type.        """
        
        if isinstance(json_primitive, list):
            if enum:
                # Handle enum with multiple types (convert to string enum)
                return {
                    'type': 'string',
                    'enum': list(enum)
                }
            else:
                # Handle union types
                union_types = []
                for item in json_primitive:
                    if isinstance(item, str):
                        converted = self.json_schema_primitive_to_structure_type(
                            item, format, None, record_name, field_name, namespace, dependencies, schema)
                        union_types.append(converted)
                    elif isinstance(item, dict):
                        item_format = item.get('format', format)
                        item_enum = item.get('enum', enum)
                        item_type = item.get('type', item)
                        converted = self.json_schema_primitive_to_structure_type(
                            item_type, item_format, item_enum, record_name, field_name, namespace, dependencies, item)
                        union_types.append(converted)
                # Always wrap as {"type": [ ... ]} for unions
                return {"type": self.flatten_union(union_types, None, field_name)}
        # ...existing code...
        structure_type = None
        
        if json_primitive == 'string':
            if format:
                if format in ('date', 'time', 'date-time', 'duration'):
                    structure_type = self.detect_temporal_type({'type': 'string', 'format': format})
                elif format == 'uuid':
                    structure_type = 'uuid'
                elif format == 'byte':
                    structure_type = 'string'  # Map bytes to string in JSON Structure                elif format == 'binary':
                    structure_type = 'string'  # Map binary to string in JSON Structure
                else:
                    structure_type = 'string'
            else:
                structure_type = 'string'
                
        elif json_primitive == 'integer':
            structure_type = self.detect_numeric_type({'type': 'integer', 'format': format, **schema})
            
        elif json_primitive == 'number':
            structure_type = self.detect_numeric_type({'type': 'number', 'format': format, **schema})
            
        elif json_primitive == 'boolean':
            structure_type = 'boolean'
            
        elif json_primitive == 'null':
            structure_type = 'null'
            
        else:
            # Handle case where type is not specified but enum is present
            if json_primitive is None and enum is not None:
                # Default to string type for enums without explicit type
                structure_type = 'string'
            else:                # Unknown type, keep as string reference
                if isinstance(json_primitive, str):
                    dependencies.append(json_primitive)
                structure_type = json_primitive or 'string'  # Ensure we never return None        # Always return proper schema objects, not simple strings
        if isinstance(structure_type, str):
            result: dict[str, Any] = {'type': structure_type}
            
            # Ensure map and set types are complete
            if structure_type == 'map':
                result['values'] = {'type': 'any'}  # Default values type per user instruction
            elif structure_type == 'set':
                result['items'] = {'type': 'any'}   # Default items type per user instruction
            
            # Handle enums
            if enum is not None:
                result['enum'] = list(enum)
                
            # Add constraints for string types
            if structure_type == 'string' and isinstance(schema, dict):
                if 'maxLength' in schema:
                    result['maxLength'] = schema['maxLength']
                if 'minLength' in schema:
                    result['minLength'] = schema['minLength']
                if 'pattern' in schema:
                    result['pattern'] = schema['pattern']
                    
            # Add precision/scale for decimal types
            elif structure_type == 'decimal' and isinstance(schema, dict):
                if 'multipleOf' in schema:
                    # Try to infer precision/scale from multipleOf
                    multiple_str = str(schema['multipleOf'])
                    if '.' in multiple_str:
                        scale = len(multiple_str.split('.')[1])
                        result['scale'] = str(scale)
                        
            return result
        
        # If already a dict or other complex type, return as-is
        return structure_type

    def _hoist_definition(self, schema, structure_schema, name_hint):
        """
        Hoist a compound schema to the top-level definitions and return a $ref.
        """
        if 'definitions' not in structure_schema:
            structure_schema['definitions'] = {}
        # Generate a unique name
        base = avro_name(name_hint or 'UnionType')
        idx = 1
        name = base
        while name in structure_schema['definitions']:
            idx += 1
            name = f"{base}{idx}"
        schema = dict(schema)  # Copy
        schema['name'] = name
        structure_schema['definitions'][name] = schema
        return {'$ref': f"#/definitions/{name}"}

    def _ensure_schema_object(self, value, structure_schema=None, name_hint=None, force_hoist_in_union=False):
        """
        Ensure that a value is wrapped as a proper JSON Structure schema object.
        
        Args:
            value: The value to wrap
            structure_schema: The structure schema for hoisting definitions
            name_hint: Hint for naming hoisted definitions
            force_hoist_in_union: Whether to hoist complex types in union contexts
            
        Returns:
            A proper schema object with type or $ref
        """        # Always return a schema object (dict with at least 'type' or '$ref') for use in properties/items/values
        if isinstance(value, dict):
            # Special handling for $ref
            if '$ref' in value and len(value) == 1:  # Pure $ref reference
                if force_hoist_in_union:
                    # In union contexts, $ref should be returned as-is (not wrapped in type)
                    return value
                else:
                    # In property contexts, wrap in type field for JSON Structure compliance
                    return {'type': value}
                
            # Check if this dict has composition keywords - preserve as-is for JSON Structure conditional composition
            if any(key in value for key in ['anyOf', 'oneOf', 'allOf']):
                return value
            
            # Ensure map types have values and set types have items
            if value.get('type') == 'map' and 'values' not in value:
                value = dict(value)  # Create a copy to avoid modifying original
                value['values'] = {'type': 'any'}  # Default to 'any' as per user instruction
            elif value.get('type') == 'set' and 'items' not in value:
                value = dict(value)  # Create a copy to avoid modifying original  
                value['items'] = {'type': 'any'}  # Default to 'any' as per user instruction
                  # If force_hoist_in_union, check if this is a simple primitive type that should be extracted
            if force_hoist_in_union and ('$ref' not in value):
                # Check if this is a simple primitive type like {"type": "int32"}
                if (len(value) == 1 and 'type' in value and 
                    value['type'] in ['string', 'boolean', 'integer', 'number', 'null', 'int32', 'int64', 'float', 'double', 'decimal', 'uuid', 'date', 'time', 'datetime', 'duration', 'bytes']):
                    # Return the primitive type string directly for JSON Structure compliance
                    return value['type']
                elif structure_schema is not None:
                    # For complex types, hoist to definitions
                    return self._hoist_definition(value, structure_schema, name_hint or 'UnionType')
            return value
        elif isinstance(value, str):
            # Handle special cases where string primitives represent incomplete complex types
            if value == 'map':
                # Convert incomplete map type to complete structure
                schema_obj = {'type': 'map', 'values': {'type': 'any'}}
            elif value == 'set':
                # Convert incomplete set type to complete structure
                schema_obj = {'type': 'set', 'items': {'type': 'any'}}
            else:
                schema_obj = {'type': value}
                
            # For JSON Structure unions, primitive types should be direct type strings, not hoisted
            if force_hoist_in_union:
                # Return the primitive type string directly for JSON Structure compliance
                # But only for actual primitives, not for complex types like map/set
                if value in ['string', 'boolean', 'integer', 'number', 'null', 'int32', 'int64', 'float', 'double', 'decimal', 'uuid', 'date', 'time', 'datetime', 'duration', 'bytes']:
                    return value
                else:
                    # For complex types like map/set, return the complete schema object
                    return schema_obj
            return schema_obj
        elif isinstance(value, list):
            # For unions, process each type appropriately
            result = []
            for idx, v in enumerate(value):
                if isinstance(v, str):
                    # Primitive types in unions should be direct strings
                    result.append(v)
                else:
                    # Complex types should be hoisted to definitions and referenced via $ref
                    obj = self._ensure_schema_object(v, structure_schema, f"{name_hint}_option_{idx}" if name_hint else None, force_hoist_in_union=True)
                    result.append(obj)
            return {"type": result}
        else:
            return {'type': 'string'}
    
    def _scan_for_uses(self, structure_schema: dict) -> list:
        """
        Scan the structure schema for extension feature usage and return the list of required $uses.
        """
        uses = set()
        def scan(obj):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if k == 'altnames':
                        uses.add('JSONStructureAlternateNames')
                    if k in {'unit', 'currency', 'symbol'}:
                        uses.add('JSONStructureUnits')
                    if k in {'pattern', 'minLength', 'maxLength', 'minimum', 'maximum', 'exclusiveMinimum', 'exclusiveMaximum', 'multipleOf', 'const', 'enum', 'required', 'propertyNames', 'keyNames'}:
                        uses.add('JSONStructureValidation')
                    if k in {'if', 'then', 'else', 'dependentRequired', 'dependentSchemas', 'anyOf', 'allOf', 'oneOf', 'not'}:
                        uses.add('JSONStructureConditionalComposition')
                    scan(v)
            elif isinstance(obj, list):
                for item in obj:
                    scan(item)
        scan(structure_schema)
        return sorted(uses)

    def _ensure_validation_extension_in_structure_schema(self, structure_schema) -> None:
        """
        Ensure that the JSONStructureValidation extension is included in the $uses array.
        This is handled automatically by the _scan_for_uses method when propertyNames or keyNames are detected.
        
        Args:
            structure_schema: The structure schema to update (dict or list)
        """
        # No action needed - the _scan_for_uses method automatically detects
        # propertyNames and keyNames and adds JSONStructureValidation to $uses
        pass

    def create_structure_object(self, properties: dict, required: list, record_name: str, namespace: str, dependencies: list, json_schema: dict, base_uri: str, structure_schema: dict, record_stack: list, recursion_depth: int = 1, original_schema: dict | None = None) -> dict:
        """
        Create a JSON Structure object type from properties.
        
        Args:
            properties (dict): The properties of the object
            required (list): List of required property names
            record_name (str): Name of the record
            namespace (str): Namespace
            dependencies (list): Dependencies list
            json_schema (dict): The full JSON schema
            base_uri (str): Base URI
            structure_schema (list): Structure schema list
            record_stack (list): Record stack for recursion detection
            recursion_depth (int): Current recursion depth
            original_schema (dict): The original JSON schema object containing additionalProperties
            
        Returns:
            dict: JSON Structure object definition        """        # Create the basic structure object
        structure_obj = {
            'type': 'object'
        }
        
        # Add required field if it's not empty
        if required:
            structure_obj['required'] = required
            
        # Add name if provided
        if record_name:
            structure_obj['name'] = avro_name(record_name)
            
        # Initialize properties dict only if we have properties to add
        has_properties = bool(properties)
        if has_properties:
            structure_obj['properties'] = {}
            
        # Process regular properties
        for prop_name, prop_schema in properties.items():
            prop_type = self.json_type_to_structure_type(
                prop_schema, record_name, prop_name, namespace, dependencies,
                json_schema, base_uri, structure_schema, record_stack, recursion_depth + 1
            )
            # Normalize property name if needed
            if not self.is_valid_identifier(prop_name):
                normalized_name = self.normalize_identifier(prop_name)
                prop_entry = self._ensure_schema_object(prop_type, structure_schema, normalized_name)
                # Always create a new dict to add altnames
                new_entry = {}
                if isinstance(prop_entry, dict):
                    new_entry.update(prop_entry)
                else:
                    new_entry['type'] = prop_entry
                new_entry['altnames'] = {'json': prop_name}
                structure_obj['properties'][normalized_name] = new_entry
            else:
                structure_obj['properties'][prop_name] = self._ensure_schema_object(prop_type, structure_schema, prop_name)        # Handle patternProperties and additionalProperties
        has_additional_schema = False
        if original_schema:
            # Check for patternProperties that coexist with properties/additionalProperties
            pattern_properties = original_schema.get('patternProperties')
            additional_props = original_schema.get('additionalProperties')
              # Special case: multiple patternProperties with no properties
            # Should create a type union of maps, not a single object with anyOf
            # This applies whether additionalProperties is false OR a schema
            if (pattern_properties and len(pattern_properties) > 1 and 
                (not 'properties' in original_schema or not original_schema['properties'])):
                # Return type union of maps instead of object
                return self.create_pattern_union_maps(
                    pattern_properties, additional_props, record_name, namespace, dependencies,
                    json_schema, base_uri, structure_schema, record_stack, recursion_depth
                )
            
            # Merge patternProperties into additionalProperties if both exist
            if pattern_properties and ('properties' in original_schema or additional_props is not None):
                # patternProperties coexists with properties/additionalProperties - merge into additionalProperties                # Get the pattern schema for values (merge all pattern schemas)
                if len(pattern_properties) == 1:
                    pattern_schema = list(pattern_properties.values())[0]
                else:
                    # Multiple patterns - create a union type instead of anyOf
                    schemas = list(pattern_properties.values())
                    # Convert each schema and create a proper union
                    converted_schemas = []
                    for idx, schema in enumerate(schemas):
                        converted_schema = self.json_type_to_structure_type(
                            schema, record_name, f'pattern_{idx}', namespace, dependencies,
                            json_schema, base_uri, structure_schema, record_stack, recursion_depth + 1
                        )
                        converted_schemas.append(converted_schema)
                    
                    if len(converted_schemas) == 1:
                        pattern_schema = converted_schemas[0]
                    else:
                        # Create union type array - hoist compound types if needed
                        hoisted_schemas = []
                        for idx, schema in enumerate(converted_schemas):
                            hoisted_schema = self._ensure_schema_object(schema, structure_schema, f'pattern_{idx}', force_hoist_in_union=True)
                            hoisted_schemas.append(hoisted_schema)
                        pattern_schema = {'type': hoisted_schemas}
                
                if additional_props is False:
                    # Override false additionalProperties with pattern schema
                    merged_additional = pattern_schema
                elif additional_props is True:
                    # Keep true (allow any additional properties)
                    merged_additional = True
                elif isinstance(additional_props, dict):
                    # Merge both schemas using a union type instead of anyOf
                    additional_converted = self.json_type_to_structure_type(
                        additional_props, record_name, 'additional', namespace, dependencies,
                        json_schema, base_uri, structure_schema, record_stack, recursion_depth + 1
                    )
                    
                    # Create union of additional props and pattern schema
                    additional_hoisted = self._ensure_schema_object(additional_converted, structure_schema, 'additional', force_hoist_in_union=True)
                    pattern_hoisted = self._ensure_schema_object(pattern_schema, structure_schema, 'pattern', force_hoist_in_union=True)
                    
                    merged_additional = {
                        'type': [additional_hoisted, pattern_hoisted]
                    }
                elif additional_props is None:
                    # No additionalProperties, use pattern schema
                    merged_additional = pattern_schema
                else:
                    merged_additional = pattern_schema
                  # Convert merged schema to structure type
                if merged_additional is not True and isinstance(merged_additional, dict):
                    additional_type = self.json_type_to_structure_type(
                        merged_additional, record_name, 'additionalProperty', namespace, dependencies,
                        json_schema, base_uri, structure_schema, record_stack, recursion_depth + 1
                    )
                    structure_obj['additionalProperties'] = self._ensure_schema_object(additional_type, structure_schema, 'additionalProperty', force_hoist_in_union=True)
                    has_additional_schema = True
                elif merged_additional is True:
                    structure_obj['additionalProperties'] = True
                    has_additional_schema = True
                  # Add propertyNames validation for the patterns
                patterns = list(pattern_properties.keys())
                if len(patterns) == 1:
                    # Single pattern - use it directly
                    pattern = patterns[0]
                    structure_obj['propertyNames'] = {
                        "type": "string",
                        "pattern": pattern
                    }
                else:
                    # Multiple patterns - in JSON Structure, we cannot use anyOf for propertyNames
                    # Skip propertyNames validation when there are multiple patterns
                    # The patterns are already handled via the merged additionalProperties schema
                    pass
                
                # Ensure $uses includes JSONStructureValidation
                self._ensure_validation_extension_in_structure_schema(structure_schema)
            
            elif additional_props is not None and additional_props is not False:
                # Handle additionalProperties without patternProperties
                if isinstance(additional_props, dict):
                    # Convert the additionalProperties schema to JSON Structure type
                    additional_type = self.json_type_to_structure_type(
                        additional_props, record_name, 'additionalProperty', namespace, dependencies,
                        json_schema, base_uri, structure_schema, record_stack, recursion_depth + 1
                    )
                    structure_obj['additionalProperties'] = self._ensure_schema_object(additional_type, structure_schema, 'additionalProperty')
                    has_additional_schema = True
                elif additional_props is True:
                    # True means any additional properties are allowed with any type
                    structure_obj['additionalProperties'] = True
                    has_additional_schema = True
        
        # For JSON Structure compliance: If we have no properties and no additionalProperties/extension,
        # add a default additionalProperties to make the object schema valid
        if not has_properties and not has_additional_schema and '$extends' not in structure_obj:
            # Add default additionalProperties to make the object valid per JSON Structure spec
            structure_obj['additionalProperties'] = True
        
        return structure_obj

    def create_structure_choice(self, discriminator_info: dict, oneof_options: list, record_name: str, namespace: str, dependencies: list, json_schema: dict, base_uri: str, structure_schema: dict, record_stack: list, recursion_depth: int = 1) -> dict:
        """
        Create a JSON Structure choice type from discriminator pattern.
        
        Args:
            discriminator_info (dict): Discriminator information
            oneof_options (list): List of oneOf options
            record_name (str): Name of the record
            namespace (str): Namespace
            dependencies (list): Dependencies list
            json_schema (dict): The full JSON schema
            base_uri (str): Base URI
            structure_schema (list): Structure schema list
            record_stack (list): Record stack for recursion detection
            recursion_depth (int): Current recursion depth
            
        Returns:
            dict: JSON Structure choice definition
        """
        # Handle both 'property' and 'propertyName' keys for compatibility
        discriminator_property = discriminator_info.get('property') or discriminator_info.get('propertyName')
        mapping = discriminator_info.get('mapping', {})
        
        choice_obj = {
            'type': 'choice',
            'discriminator': discriminator_property,
            'choices': {}
        }
        
        if record_name:
            choice_obj['name'] = avro_name(record_name)
        
        # Build reverse mapping from $ref to choice key
        ref_to_key = {}
        for key, ref in mapping.items():
            ref_to_key[ref] = key
            
        # Process each choice option
        for i, option in enumerate(oneof_options):
            if '$ref' in option:
                # Handle reference - use mapping to get the choice key
                ref = option['$ref']
                if ref in ref_to_key:
                    choice_key = ref_to_key[ref]
                else:
                    # Extract name from reference
                    if ref.startswith('#/definitions/'):
                        choice_key = ref[14:]  # Remove '#/definitions/' prefix
                    else:
                        choice_key = f"option_{i}"
                choice_obj['choices'][choice_key] = {'$ref': ref}
            else:
                # Convert option to structure type
                choice_key = f"option_{i}"
                choice_type = self.json_type_to_structure_type(
                    option, record_name, f"choice_{i}", namespace, dependencies,
                    json_schema, base_uri, structure_schema, record_stack, recursion_depth + 1
                )
                choice_obj['choices'][choice_key] = choice_type
                
        return choice_obj

    def create_structure_map(self, values_schema: dict, record_name: str, namespace: str, dependencies: list, json_schema: dict, base_uri: str, structure_schema: dict, record_stack: list, recursion_depth: int = 1) -> dict:
        """
        Create a JSON Structure map type.
        
        Args:
            values_schema (dict): Schema for map values
            record_name (str): Name of the record
            namespace (str): Namespace
            dependencies (list): Dependencies list
            json_schema (dict): The full JSON schema
            base_uri (str): Base URI
            structure_schema (list): Structure schema list
            record_stack (list): Record stack for recursion detection
            recursion_depth (int): Current recursion depth
              Returns:
            dict: JSON Structure map definition
        """
        values_type = self.json_type_to_structure_type(
            values_schema, record_name, 'value', namespace, dependencies,
            json_schema, base_uri, structure_schema, record_stack, recursion_depth + 1
        )
        # Always wrap as schema object        values_type = self._ensure_schema_object(values_type, structure_schema, 'value')
        return {
            'type': 'map',
            'values': values_type
        }

    def create_structure_map_with_pattern(self, values_schema: dict, pattern_properties: dict, record_name: str, namespace: str, dependencies: list, json_schema: dict, base_uri: str, structure_schema: dict, record_stack: list, recursion_depth: int = 1) -> dict:
        """
        Create a JSON Structure map type with pattern validation using keyNames.
        
        Args:
            values_schema (dict): Schema for map values
            pattern_properties (dict): The patternProperties object with patterns as keys
            record_name (str): Name of the record
            namespace (str): Namespace
            dependencies (list): Dependencies list
            json_schema (dict): The full JSON schema
            base_uri (str): Base URI
            structure_schema (list): Structure schema list
            record_stack (list): Record stack for recursion detection
            recursion_depth (int): Current recursion depth
            
        Returns:
            dict: JSON Structure map definition with keyNames validation
        """
        map_result = self.create_structure_map(
            values_schema, record_name, namespace, dependencies,
            json_schema, base_uri, structure_schema, record_stack, recursion_depth
        )
        
        if pattern_properties and len(pattern_properties) > 0:
            # Extract patterns and create keyNames validation schema
            patterns = list(pattern_properties.keys())
            
            if len(patterns) == 1:
                # Single pattern - use it directly
                pattern = patterns[0]
                map_result['keyNames'] = {
                    "type": "string",
                    "pattern": pattern
                }
            else:
                # Multiple patterns - combine with anyOf
                pattern_schemas = []
                for pattern in patterns:
                    pattern_schemas.append({
                        "type": "string",
                        "pattern": pattern
                    })
                map_result['keyNames'] = {
                    "anyOf": pattern_schemas
                }
            
            # Ensure $uses includes JSONStructureValidation
            self._ensure_validation_extension_in_structure_schema(structure_schema)
            
        return map_result

    def create_structure_array_or_set(self, items_schema: dict, is_set: bool, record_name: str, namespace: str, dependencies: list, json_schema: dict, base_uri: str, structure_schema: dict, record_stack: list, recursion_depth: int = 1) -> dict:
        """
        Create a JSON Structure array or set type.
        
        Args:
            items_schema (dict): Schema for array/set items
            is_set (bool): True for set, False for array
            record_name (str): Name of the record
            namespace (str): Namespace
            dependencies (list): Dependencies list
            json_schema (dict): The full JSON schema
            base_uri (str): Base URI
            structure_schema (list): Structure schema list
            record_stack (list): Record stack for recursion detection
            recursion_depth (int): Current recursion depth
            
        Returns:
            dict: JSON Structure array/set definition
        """
        items_type = self.json_type_to_structure_type(
            items_schema, record_name, 'item', namespace, dependencies,
            json_schema, base_uri, structure_schema, record_stack, recursion_depth + 1
        )
        # Always wrap as schema object
        items_type = self._ensure_schema_object(items_type, structure_schema, 'item')
        return {
            'type': 'set' if is_set else 'array',
            'items': items_type
        }

    def add_alternate_names(self, structure: dict, original_name: str) -> dict:
        """
        Add alternate names for different naming conventions.
        
        Args:
            structure (dict): The structure definition
            original_name (str): The original property/type name
            
        Returns:
            dict: Structure with altnames added
        """
        if not original_name:
            return structure
            
        altnames = {}
        
        # Add camelCase if original is snake_case
        if '_' in original_name:
            camel_case = ''.join(word.capitalize() if i > 0 else word 
                               for i, word in enumerate(original_name.split('_')))
            altnames['camelCase'] = camel_case
              # Add snake_case if original is camelCase
        elif any(c.isupper() for c in original_name):
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', original_name).lower()
            altnames['snake_case'] = snake_case
            
        if altnames:
            if not isinstance(structure, dict):
                structure = {'type': structure}
            structure['altnames'] = altnames
            
        return structure

    def add_validation_constraints(self, structure: dict, schema: dict) -> dict:
        """
        Convert JSON Schema validation constraints to JSON Structure format.
        
        Args:
            structure (dict): The structure definition
            schema (dict): The original JSON schema
            
        Returns:
            dict: Structure with validation constraints added
        """
        if not isinstance(structure, dict):
            structure = {'type': structure}
            
        # Copy validation constraints
        validation_keys = [
            'minimum', 'maximum', 'exclusiveMinimum', 'exclusiveMaximum',
            'minLength', 'maxLength', 'pattern', 'minItems', 'maxItems',
            'const', 'enum'
        ]
        
        # Check if this is an int64 type
        is_int64 = structure.get('type') == 'int64'
        
        for key in validation_keys:
            if key in schema:
                value = schema[key]
                # For int64 types, convert numeric minimum/maximum values to strings
                if is_int64 and key in ('minimum', 'maximum', 'exclusiveMinimum', 'exclusiveMaximum') and isinstance(value, (int, float)):
                    structure[key] = str(int(value))
                else:
                    structure[key] = value
                
        return structure

    def ensure_object_compliance(self, structure: dict) -> dict:
        """
        Ensure that object types comply with JSON Structure spec requirements.
        
        Args:
            structure (dict): The structure definition
            
        Returns:
            dict: Structure with JSON Structure compliance ensured
        """
        if not isinstance(structure, dict) or structure.get('type') != 'object':
            return structure
            
        # Check if this object type needs properties to be compliant
        has_properties = 'properties' in structure and structure['properties']
        has_additional_props = 'additionalProperties' in structure
        has_extensions = '$extends' in structure
        
        # If object has empty properties and no additionalProperties or extensions,
        # add additionalProperties: true to make it compliant
        if 'properties' in structure and not has_properties and not has_additional_props and not has_extensions:
            structure['additionalProperties'] = True
            
        return structure

    def flatten_union(self, type_list: list, structure_schema=None, name_hint=None) -> list:
        """
        Flatten the list of types in a union into a single list.

        Args:
            type_list (list): The list of types in a union.

        Returns:
            list: The flattened list of types.
        """
        flat_list = []
        for idx, t in enumerate(type_list):
            if isinstance(t, list):
                inner = self.flatten_union(t, structure_schema, name_hint)
                for u in inner:
                    obj = self._ensure_schema_object(u, structure_schema, f"{name_hint}_option_{idx}" if name_hint else None, force_hoist_in_union=True)
                    if obj not in flat_list:
                        flat_list.append(obj)
            else:
                # For primitive types in unions, extract the type string directly
                if isinstance(t, dict) and 'type' in t and t['type'] in ['string', 'boolean', 'integer', 'number', 'null'] and len(t) == 1:
                    # This is a simple primitive type object like {"type": "boolean"} - extract the type string
                    if t['type'] not in flat_list:
                        flat_list.append(t['type'])
                else:
                    # For complex types, use the normal processing
                    obj = self._ensure_schema_object(t, structure_schema, f"{name_hint}_option_{idx}" if name_hint else None, force_hoist_in_union=True)
                    if obj not in flat_list:
                        flat_list.append(obj)
        return flat_list

    def merge_structure_schemas(self, schemas: list, structure_schemas: list, type_name: str | None = None, deps: List[str] = []) -> str | list | dict:
        """Merge multiple JSON Structure type schemas into one."""
        
        if len(schemas) == 1:
            return schemas[0]
            
        merged_schema: dict = {}
        if type_name:
            merged_schema['name'] = type_name
            
        for schema in schemas:
            schema = copy.deepcopy(schema)
            if isinstance(schema, dict) and 'dependencies' in schema:
                deps1: List[str] = merged_schema.get('dependencies', [])
                deps1.extend(schema['dependencies'])
                merged_schema['dependencies'] = deps1
                
            if isinstance(schema, str):
                # Simple type reference
                if 'type' not in merged_schema:
                    merged_schema['type'] = schema
                elif merged_schema['type'] != schema:
                    # Type conflict, create union
                    if not isinstance(merged_schema['type'], list):
                        merged_schema['type'] = [merged_schema['type']]
                    if schema not in merged_schema['type']:
                        merged_schema['type'].append(schema)
                        
            elif isinstance(schema, dict):
                # Merge object schemas
                for key, value in schema.items():
                    if key == 'properties' and 'properties' in merged_schema:
                        # Merge properties
                        for prop_name, prop_schema in value.items():
                            if prop_name in merged_schema['properties']:
                                # Property exists, merge types
                                existing = merged_schema['properties'][prop_name]
                                merged_schema['properties'][prop_name] = self.merge_structure_schemas(
                                    [existing, prop_schema], structure_schemas, None, deps)
                            else:
                                merged_schema['properties'][prop_name] = prop_schema
                    elif key == 'required' and 'required' in merged_schema:
                        # Merge required arrays
                        merged_schema['required'] = list(set(merged_schema['required'] + value))
                    else:
                        merged_schema[key] = value
                        
        return merged_schema

    def json_type_to_structure_type(self, json_type: str | dict, record_name: str, field_name: str, namespace: str, dependencies: list, json_schema: dict, base_uri: str, structure_schema: dict, record_stack: list, recursion_depth=1) -> dict | list | str:
        """Convert a JSON Schema type to JSON Structure type."""
        
        try:
            if recursion_depth >= self.max_recursion_depth:
                print(f'WARNING: Maximum recursion depth reached for {record_name} at field {field_name}')
                return 'string'  # Fallback to string instead of generic_type()            structure_type: dict = {}
            local_name = avro_name(field_name if field_name else record_name)

            if isinstance(json_type, str):
                # Simple type reference
                return self.json_schema_primitive_to_structure_type(
                    json_type, None, None, record_name, field_name, namespace, dependencies, {})

            if isinstance(json_type, dict):                # Handle inheritance pattern first (only if inheritance detection is enabled)
                inheritance_info = self.detect_inheritance_pattern(json_type, record_name)
                if inheritance_info:
                    base_ref = inheritance_info['base_ref']
                    extension = inheritance_info['extension']
                      # Create abstract base type name
                    if base_ref.startswith('#/definitions/'):
                        base_type_name = base_ref[14:]  # Remove '#/definitions/'
                        abstract_base_name = avro_name(f"{base_type_name}Base")
                    else:
                        # Handle external references or other formats
                        abstract_base_name = avro_name(f"{record_name}Base")
                    
                    # Ensure the abstract base type exists
                    self._ensure_abstract_base_type(base_ref, abstract_base_name, structure_schema, json_schema, base_uri)
                    
                    structure_type = {
                        'type': 'object',
                        'name': record_name,
                        '$extends': f"#/definitions/{abstract_base_name}"
                    }                    
                    if 'properties' in extension and extension['properties']:
                        structure_type['properties'] = {}
                        for prop_name, prop_schema in extension['properties'].items():
                            prop_type = self.json_type_to_structure_type(
                                prop_schema, record_name, prop_name, namespace, dependencies,
                                json_schema, base_uri, structure_schema, record_stack, recursion_depth + 1
                            )
                            # Normalize property name if needed
                            if not self.is_valid_identifier(prop_name):
                                normalized_name = self.normalize_identifier(prop_name)
                                prop_entry = self._ensure_schema_object(prop_type, structure_schema, normalized_name)
                                # Always create a new dict to add altnames
                                new_entry = {}
                                if isinstance(prop_entry, dict):
                                    new_entry.update(prop_entry)
                                else:
                                    new_entry['type'] = prop_entry
                                new_entry['altnames'] = {'json': prop_name}
                                structure_type['properties'][normalized_name] = new_entry
                            else:
                                structure_type['properties'][prop_name] = self._ensure_schema_object(prop_type, structure_schema, prop_name)
                    
                    if 'required' in extension:
                        structure_type['required'] = extension['required']
                    
                    # Copy other extension properties (validation constraints, etc.)
                    for key, value in extension.items():
                        if key not in ['properties', 'required', 'type']:
                            structure_type[key] = value
                    
                    # Apply any remaining validation constraints from the original schema
                    structure_type = self.add_validation_constraints(structure_type, json_type)
                        
                    return structure_type

                # Handle discriminator pattern
                discriminator_info = self.detect_discriminator_pattern(json_type)
                if discriminator_info and 'oneOf' in json_type:
                    return self.create_structure_choice(
                        discriminator_info, json_type['oneOf'], record_name, namespace,
                        dependencies, json_schema, base_uri, structure_schema, record_stack, recursion_depth
                    )                # Handle $ref first (before checking for type)
                if '$ref' in json_type:
                    ref = json_type['$ref']
                    # Normalize references to use definitions instead of $defs
                    if ref.startswith('#/$defs/'):
                        ref = ref.replace('#/$defs/', '#/definitions/')
                    elif ref.startswith('#/definitions/'):
                        # Already correct format
                        pass
                    
                    # Handle nested JSON Pointer references like #/definitions/pipelineCommon/execution
                    if '/' in ref.split('#/definitions/')[-1] and ref.startswith('#/definitions/'):
                        try:
                            # Resolve the nested JSON Pointer reference
                            resolved_schema, _ = self.resolve_reference(json_type, base_uri, json_schema)
                            if resolved_schema != json_type:
                                # We successfully resolved a nested reference, process the resolved schema                                # Create a new definition name based on the nested path
                                ref_parts = ref.split('/')
                                if len(ref_parts) >= 4:  # ['#', 'definitions', 'parent', 'child', ...]
                                    parent_name = ref_parts[2]
                                    child_path = '/'.join(ref_parts[3:])
                                    new_def_name = avro_name(f"{parent_name}_{child_path.replace('/', '_')}")
                                    
                                    # Process the resolved schema recursively
                                    converted_schema = self.json_type_to_structure_type(
                                        resolved_schema, new_def_name, field_name, namespace, dependencies,
                                        json_schema, base_uri, structure_schema, record_stack, recursion_depth + 1
                                    )
                                    
                                    # Check if we need to add to definitions
                                    if isinstance(converted_schema, dict) and converted_schema.get('type') in ['object', 'array', 'choice']:
                                        # Set name for the definition and add to structure_schema definitions
                                        converted_schema['name'] = new_def_name
                                        if 'definitions' not in structure_schema:
                                            structure_schema['definitions'] = {}
                                        structure_schema['definitions'][new_def_name] = converted_schema
                                        return {'$ref': f"#/definitions/{new_def_name}"}
                                    else:
                                        # For simple types, return the converted schema directly
                                        return converted_schema
                        except Exception as e:
                            # If resolution fails, fall back to original reference
                            print(f"Failed to resolve nested reference {ref}: {e}")
                            pass
                    
                    # Check if we need to use type registry to normalize the reference
                    # Extract the definition name from the reference
                    if ref.startswith('#/definitions/'):
                        def_name = ref[14:]  # Remove '#/definitions/' prefix
                        if def_name in self.type_registry:
                            # Use the normalized reference from the registry
                            ref = self.type_registry[def_name]
                    
                    return {'$ref': ref}

                # Handle schemas without explicit type
                if json_type.get('type') is None:
                    if 'enum' in json_type:
                        # Enum-only schema - default to string type
                        enum_values = json_type.get('enum')
                        structure_type = self.json_schema_primitive_to_structure_type(
                            'string', json_type.get('format'), enum_values, record_name, field_name, namespace, dependencies, json_type
                        )
                        if isinstance(structure_type, dict):
                            structure_type = self.add_validation_constraints(structure_type, json_type)
                        return structure_type
                    elif 'properties' in json_type or 'additionalProperties' in json_type or 'patternProperties' in json_type:
                        # Object schema without explicit type - treat as object
                        # Apply constraint composition conversion if applicable
                        effective_schema = self._convert_constraint_composition_to_required(json_type)
                        properties = effective_schema.get('properties', {})
                        required = effective_schema.get('required', [])
                        return self.create_structure_object(
                            properties, required, record_name, namespace, dependencies,
                            json_schema, base_uri, structure_schema, record_stack, recursion_depth, effective_schema
                        )
                    elif self.has_composition_keywords(json_type):
                        # Handle composition keywords without explicit type
                        # Continue to composition handling below instead of returning empty object
                        pass
                    else:
                        # Other schema without type - default to generic object
                        # Create a generic object that allows any properties
                        return {
                            'type': 'object',
                            'properties': {}                        }
                
                if json_type.get('type') and isinstance(json_type['type'], str):
                    # Check if this schema also has composition keywords that should be preserved
                    if self.preserve_composition and self.has_composition_keywords(json_type):
                        # Skip primitive handling and continue to composition handling below
                        pass
                    else:
                        format_hint = json_type.get('format')
                        enum_values = json_type.get('enum')
                          # Special handling for objects
                        if json_type['type'] == 'object':
                            # Check if should convert to map
                            if self.should_convert_to_map(json_type):
                                # Handle patternProperties conversion to map
                                pattern_properties = json_type.get('patternProperties')
                                if pattern_properties:
                                    # Get the pattern schema for values (merge all pattern schemas)
                                    if len(pattern_properties) == 1:
                                        pattern_schema = list(pattern_properties.values())[0]
                                    else:
                                        # Multiple patterns - merge schemas using anyOf
                                        schemas = list(pattern_properties.values())
                                        pattern_schema = {'anyOf': schemas}
                                    
                                    # Convert patternProperties to map with keyNames validation
                                    return self.create_structure_map_with_pattern(
                                        pattern_schema, pattern_properties, record_name, namespace, dependencies,
                                        json_schema, base_uri, structure_schema, record_stack, recursion_depth
                                    )
                                else:
                                    # Handle additionalProperties conversion to map
                                    additional_props = json_type.get('additionalProperties', True)
                                    if isinstance(additional_props, dict):
                                        return self.create_structure_map(
                                            additional_props, record_name, namespace, dependencies,
                                            json_schema, base_uri, structure_schema, record_stack, recursion_depth
                                        )
                                    else:                                        return {
                                            'type': 'map',
                                            'values': {'type': 'string'}  # Default for boolean additionalProperties
                                        }
                            else:
                                # Regular object - first check for discriminated union patterns
                                if 'oneOf' in json_type:
                                    choice_info = self.detect_discriminated_union_pattern(json_type)
                                    if choice_info:
                                        # Convert to JSON Structure choice type
                                        choice_result = {
                                            'type': 'choice',
                                            'choices': choice_info['choices']
                                        }
                                        
                                        # Add selector if specified (for tagged unions)
                                        if choice_info.get('selector'):
                                            choice_result['selector'] = choice_info['selector']
                                        
                                        # Add name if we have one
                                        if record_name:
                                            choice_result['name'] = avro_name(record_name)
                                            
                                        return choice_result
                                  # Regular object without discriminated union
                                # Check if this is a bare object type that should be converted to "any"
                                if (not json_type.get('properties') and 
                                    not json_type.get('additionalProperties') and
                                    not json_type.get('patternProperties') and
                                    not json_type.get('required') and
                                    not json_type.get('$extends') and
                                    not any(k in json_type for k in ['allOf', 'anyOf', 'oneOf', 'if', 'then', 'else'])):
                                    # This is a bare "type": "object" which means "any object" in JSON Schema
                                    # Convert to "any" type in JSON Structure
                                    return {'type': 'any'}
                                
                                # Apply constraint composition conversion if applicable
                                effective_schema = self._convert_constraint_composition_to_required(json_type)
                                properties = effective_schema.get('properties', {})
                                required = effective_schema.get('required', [])
                                return self.create_structure_object(
                                    properties, required, record_name, namespace, dependencies,
                                    json_schema, base_uri, structure_schema, record_stack, recursion_depth, json_type
                                )
                          # Special handling for arrays
                        elif json_type['type'] == 'array':
                            items_schema = json_type.get('items', {'type': 'string'})
                            is_set = self.detect_collection_type(json_type) == 'set'
                            return self.create_structure_array_or_set(
                                items_schema, is_set, record_name, namespace, dependencies,
                                json_schema, base_uri, structure_schema, record_stack, recursion_depth
                            )
                        
                        # Special handling for maps
                        elif json_type['type'] == 'map':
                            values_schema = json_type.get('values', {'type': 'string'})
                            return self.create_structure_map(
                                values_schema, record_name, namespace, dependencies,
                                json_schema, base_uri, structure_schema, record_stack, recursion_depth
                            )
                        
                        # Special handling for sets
                        elif json_type['type'] == 'set':
                            items_schema = json_type.get('items', {'type': 'string'})
                            return self.create_structure_array_or_set(
                                items_schema, True, record_name, namespace, dependencies,
                                json_schema, base_uri, structure_schema, record_stack, recursion_depth
                            )
                        
                        else:
                            # Primitive type
                            structure_type = self.json_schema_primitive_to_structure_type(
                                json_type['type'], format_hint, enum_values, record_name, field_name, namespace, dependencies, json_type
                            )
                              # Add validation constraints
                            if isinstance(structure_type, str):
                                structure_type = self.add_validation_constraints({'type': structure_type}, json_type)
                                if len(structure_type) == 1:
                                    structure_type = structure_type['type']
                            elif isinstance(structure_type, dict):
                                structure_type = self.add_validation_constraints(structure_type, json_type)
                            return structure_type

                # Handle composition keywords - resolve when preserve_composition is False
                if not self.preserve_composition and self.has_composition_keywords(json_type):
                    return self.resolve_composition_keywords(
                        json_type, record_name, field_name, namespace, dependencies,
                        json_schema, base_uri, structure_schema, record_stack, recursion_depth
                    )

                # Handle composition keywords
                if self.preserve_composition:
                    if 'allOf' in json_type and not inheritance_info:
                        # Non-inheritance allOf - keep as-is or merge based on configuration
                        allof_schemas = []
                        for allof_item in json_type['allOf']:
                            converted = self.json_type_to_structure_type(
                                allof_item, record_name, field_name, namespace, dependencies,
                                json_schema, base_uri, structure_schema, record_stack, recursion_depth + 1
                            )
                            allof_schemas.append(converted)
                          # For now, merge them - JSON Structure supports allOf natively
                        return {
                            'allOf': allof_schemas
                        }
                    
                    if 'oneOf' in json_type and not discriminator_info:                        # Check if this is a discriminated union pattern
                        choice_info = self.detect_discriminated_union_pattern(json_type)
                        if choice_info:
                            # Convert to JSON Structure choice type (tagged union)
                            choice_result = {
                                'type': 'choice',
                                'choices': choice_info['choices']
                            }
                            
                            # Add selector if specified (for tagged unions)
                            if choice_info.get('selector'):
                                choice_result['selector'] = choice_info['selector']
                            
                            # Add name if we have one
                            if record_name:
                                choice_result['name'] = avro_name(record_name)
                                
                            return choice_result
                        
                        # Regular oneOf without discriminator
                        oneof_schemas = []
                        for oneof_item in json_type['oneOf']:
                            # For constraint-only schemas, preserve them but add type: object
                            if self._is_constraint_only_schema(oneof_item):
                                preserved_item = dict(oneof_item)
                                preserved_item['type'] = 'object'
                                # Add properties for required fields to make it valid JSON Structure
                                if 'required' in preserved_item and 'properties' not in preserved_item:
                                    preserved_item['properties'] = {}
                                    for req_field in preserved_item['required']:
                                        preserved_item['properties'][req_field] = {'type': 'any'}
                                # Allow additional properties since this is a constraint-only schema
                                preserved_item['additionalProperties'] = True
                                oneof_schemas.append(preserved_item)
                            else:
                                converted = self.json_type_to_structure_type(
                                    oneof_item, record_name, field_name, namespace, dependencies,
                                    json_schema, base_uri, structure_schema, record_stack, recursion_depth + 1
                                )
                                oneof_schemas.append(converted)
                        return {
                            'oneOf': oneof_schemas
                        }
                    
                    if 'anyOf' in json_type:                        # Check if this is a constraint-only anyOf pattern that should be converted to permutations
                        anyof_items = json_type['anyOf']
                        constraint_only = all(
                            self._is_constraint_only_schema(item) and 'required' in item
                            for item in anyof_items
                        )
                        
                        if constraint_only:
                            # Convert constraint-only anyOf to permutations and return as object with required
                            converted_schema = self._convert_constraint_anyof_to_permutations(json_type)
                            
                            # Use create_structure_object to properly handle patternProperties
                            result = self.create_structure_object(
                                json_type.get('properties', {}),
                                converted_schema.get('required', []),
                                record_name, namespace, dependencies,
                                json_schema, base_uri, structure_schema,
                                record_stack, recursion_depth, json_type
                            )
                              # Add other properties from the original schema (except structural properties)
                            for key, value in json_type.items():
                                if key not in ['anyOf', 'type', 'properties', 'required', 'patternProperties', 'additionalProperties']:
                                    result[key] = value
                            return result
                        else:
                            # Regular anyOf composition - preserve original structure when preserve_composition=True
                            anyof_schemas = []
                            for anyof_item in anyof_items:
                                converted = self.json_type_to_structure_type(
                                    anyof_item, record_name, field_name, namespace, dependencies,
                                    json_schema, base_uri, structure_schema, record_stack, recursion_depth + 1
                                )
                                anyof_schemas.append(converted)
                            
                            return {
                                'anyOf': anyof_schemas
                            }
                    
                    # Handle conditional schemas (if/then/else)
                    if 'if' in json_type:
                        # Preserve conditional schemas as-is in JSON Structure
                        result = {}
                        
                        # Process if clause
                        if_schema = self.json_type_to_structure_type(
                            json_type['if'], record_name, field_name, namespace, dependencies,
                            json_schema, base_uri, structure_schema, record_stack, recursion_depth + 1
                        )
                        result['if'] = if_schema
                        
                        # Process then clause if present
                        if 'then' in json_type:
                            then_schema = self.json_type_to_structure_type(
                                json_type['then'], record_name, field_name, namespace, dependencies,
                                json_schema, base_uri, structure_schema, record_stack, recursion_depth + 1
                            )
                            result['then'] = then_schema
                        
                        # Process else clause if present
                        if 'else' in json_type:
                            else_schema = self.json_type_to_structure_type(
                                json_type['else'], record_name, field_name, namespace, dependencies,
                                json_schema, base_uri, structure_schema, record_stack, recursion_depth + 1
                            )
                            result['else'] = else_schema
                          # Add any other properties from the original schema
                        for key, value in json_type.items():
                            if key not in ['if', 'then', 'else']:
                                if key in ['properties', 'required', 'type']:
                                    # Handle structural properties
                                    if key == 'properties':
                                        converted_props = {}
                                        for prop_name, prop_schema in value.items():
                                            prop_type = self.json_type_to_structure_type(
                                                prop_schema, record_name, prop_name, namespace, dependencies,
                                                json_schema, base_uri, structure_schema, record_stack, recursion_depth + 1
                                            )
                                            converted_props[prop_name] = self._ensure_schema_object(prop_type, structure_schema, prop_name)
                                        result[key] = converted_props
                                    else:
                                        result[key] = value
                                else:
                                    # Copy validation and other properties as-is
                                    result[key] = value
                        
                        # Ensure conditional object schemas are valid JSON Structure
                        if result.get('type') == 'object':
                            # If we have no properties but we're an object type, add additionalProperties
                            if 'properties' not in result and '$extends' not in result:
                                result['additionalProperties'] = True
                            # If we have empty properties, remove it and add additionalProperties instead
                            elif 'properties' in result and not result['properties']:
                                del result['properties']
                                if 'additionalProperties' not in result:
                                    result['additionalProperties'] = True
                        
                        return result

                # Handle const
                if 'const' in json_type:
                    const_value = json_type['const']
                    if isinstance(const_value, str):
                        return {
                            'type': 'string',
                            'const': const_value
                        }
                    elif isinstance(const_value, (int, float)):
                        return {
                            'type': 'int32' if isinstance(const_value, int) else 'double',
                            'const': const_value
                        }
                    elif isinstance(const_value, bool):
                        return {
                            'type': 'boolean',
                            'const': const_value
                        }# Fallback for unhandled cases
                if 'properties' in json_type:                    # Treat as object even without explicit type
                    properties = json_type['properties']
                    required = json_type.get('required', [])
                    return self.create_structure_object(
                        properties, required, record_name, namespace, dependencies,                        json_schema, base_uri, structure_schema, record_stack, recursion_depth, json_type
                    )

            # Fallback
            return 'string'
            
        except Exception as e:
            print(f'ERROR: Failed to convert type for {record_name}.{field_name}: {e}')
            return 'string'

    def fetch_content(self, url: str | ParseResult):
        """
        Fetches the content from the specified URL.

        Args:
            url (str or ParseResult): The URL to fetch the content from.

        Returns:
            str: The fetched content.

        Raises:
            requests.RequestException: If there is an error while making the HTTP request.
            Exception: If there is an error while reading the file.
        """
        # Parse the URL to determine the scheme
        if isinstance(url, str):
            parsed_url = urlparse(url)
        else:
            parsed_url = url

        if parsed_url.geturl() in self.content_cache:
            return self.content_cache[parsed_url.geturl()]
        scheme = parsed_url.scheme

        # Handle HTTP and HTTPS URLs
        if scheme in ['http', 'https']:
            response = requests.get(url if isinstance(
                url, str) else parsed_url.geturl(), timeout=30)
            # Raises an HTTPError if the response status code is 4XX/5XX
            response.raise_for_status()
            self.content_cache[parsed_url.geturl()] = response.text
            return response.text

        # Handle file URLs
        elif scheme == 'file':
            # Remove the leading 'file://' from the path for compatibility
            file_path = parsed_url.netloc
            if not file_path:
                file_path = parsed_url.path
            # On Windows, a file URL might start with a '/' but it's not part of the actual path
            if os.name == 'nt' and file_path.startswith('/'):
                file_path = file_path[1:]
            with open(file_path, 'r', encoding='utf-8') as file:
                text = file.read()
                self.content_cache[parsed_url.geturl()] = text
                return text
        else:
            raise NotImplementedError(f'Unsupported URL scheme: {scheme}')

    def resolve_reference(self, json_type: dict, base_uri: str, json_doc: dict) -> Tuple[Union[dict, Any], dict]:
        """
        Resolve a JSON Pointer reference or a JSON $ref reference.

        Args:
            json_type (dict): The JSON type containing the reference.
            base_uri (str): The base URI of the JSON document.
            json_doc (dict): The JSON document containing the reference.

        Returns:
            Tuple[Union[dict, Any], dict]: A tuple containing the resolved JSON schema and the original JSON schema document.

        Raises:
            Exception: If there is an error decoding JSON from the reference.
            Exception: If there is an error resolving the JSON Pointer reference.
        """
        try:
            ref = json_type['$ref']
            content = None
            url = urlparse(ref)
            if url.scheme:
                content = self.fetch_content(ref)
            elif url.path:
                file_uri = self.compose_uri(base_uri, url)
                content = self.fetch_content(file_uri)
            if content:
                try:
                    json_schema_doc = json_schema = json.loads(content)
                    # resolve the JSON Pointer reference, if any
                    if url.fragment:
                        json_schema = jsonpointer.resolve_pointer(
                            json_schema, url.fragment)
                    return json_schema, json_schema_doc
                except json.JSONDecodeError:
                    raise Exception(f'Error decoding JSON from {ref}')

            if url.fragment:
                json_pointer = unquote(url.fragment)
                ref_schema = jsonpointer.resolve_pointer(
                    json_doc, json_pointer)
                if ref_schema:
                    return ref_schema, json_doc
        except JsonPointerException as e:
            raise Exception(
                f'Error resolving JSON Pointer reference for {base_uri}')
        return json_type, json_doc

    def compose_uri(self, base_uri, url):
        """Compose a URI from a base URI and a relative URL."""
        if isinstance(url, str):
            url = urlparse(url)
            if url.scheme:
                return url.geturl()
        if not url.path and not url.netloc:
            return base_uri
        if base_uri.startswith('file'):
            parsed_file_uri = urlparse(base_uri)
            dir = os.path.dirname(
                parsed_file_uri.netloc if parsed_file_uri.netloc else parsed_file_uri.path)
            filename = os.path.join(dir, url.path)
            file_uri = f'file://{filename}'
        else:
            # combine the base URI with the URL
            file_uri = urllib.parse.urljoin(base_uri, url.geturl())
        return file_uri

    def convert_schema(self, json_schema_path: str, output_path: str | None = None):
        """
        Convert a JSON Schema file to JSON Structure format.
        
        Args:
            json_schema_path (str): Path to the input JSON Schema file
            output_path (str): Path for the output JSON Structure file (optional)
        """
        # Read the JSON Schema
        with open(json_schema_path, 'r', encoding='utf-8') as file:
            json_schema = json.load(file)
        
        # Convert to JSON Structure
        structure_schema = self.convert_json_schema_to_structure(json_schema, json_schema_path)
        
        # Determine output path
        if not output_path:
            base_name = os.path.splitext(json_schema_path)[0]
            output_path = f"{base_name}.structure.json"
          # Write the result
        with open(output_path, 'w', encoding='utf-8') as file:
            # Sort properties before writing
            # Sort properties before writing
            sorted_schema = self._sort_json_structure_properties(structure_schema)
            json.dump(sorted_schema, file, indent=2)
        
        print(f"Converted {json_schema_path} to {output_path}")
        return structure_schema
        
    
    def _mark_abstract_types(self, structure_schema: dict) -> None:
        """
        Mark abstract types in the structure schema.
        
        Args:
            structure_schema (dict): The structure schema to mark
        """
        if 'definitions' in structure_schema:
            for def_name, def_schema in structure_schema['definitions'].items():
                if isinstance(def_schema, dict):
                    # Mark types with only inheritance as abstract
                    if ('$extends' in def_schema and 
                        ('properties' not in def_schema or len(def_schema['properties']) == 0)):
                        def_schema['abstract'] = True
                    
                    # Mark choice types with discriminators as abstract  
                    if (def_schema.get('type') == 'choice' and 
                        'discriminator' in def_schema):
                        def_schema['abstract'] = True
    
    def jsons_to_structure(self, json_schema: Union[dict, list], namespace: str, base_uri: str) -> dict:
        """
        Convert a JSON Schema to JSON Structure format.
        
        Args:
            json_schema (dict | list): The JSON Schema to convert
            namespace (str): The target namespace  
            base_uri (str): Base URI for reference resolution
            
        Returns:
            dict: The converted JSON Structure schema
        """
        # Clear type registry for new conversion
        self.type_registry.clear()
        
        structure_schema: Dict[str, Any] = {
            "$schema": "https://json-structure.org/meta/extended/v0/#"
        }
        # Do NOT set $uses here; it will be set after scanning for actual usage

        # Handle schema with definitions/defs
        if isinstance(json_schema, dict) and ('definitions' in json_schema or '$defs' in json_schema):
            # Process definitions
            defs_key = '$defs' if '$defs' in json_schema else 'definitions'
            json_schema_defs = json_schema[defs_key]
            if json_schema_defs:
                structure_schema['definitions'] = {}
                  # First pass: populate type registry for reference resolution
                for def_name in json_schema_defs.keys():
                    normalized_def_name = avro_name(def_name)
                    self.type_registry[def_name] = f"#/definitions/{normalized_def_name}"                # Second pass: convert each definition
                for def_name, def_schema in json_schema_defs.items():
                    # Skip empty definitions or ones that are just plain values/strings
                    if not isinstance(def_schema, dict) or not def_schema:
                        continue
                    
                    # Check if this is a pure container definition (only contains nested schemas, no actual schema keywords)
                    schema_keywords = {'type', 'properties', 'items', 'additionalProperties', 'patternProperties', 
                                     'oneOf', 'anyOf', 'allOf', '$ref', 'required', 'enum', 'const', 'minimum',
                                     'maximum', 'minLength', 'maxLength', 'pattern', 'format', 'if', 'then', 'else'}
                    has_schema_keywords = any(key in def_schema for key in schema_keywords)                    # If it only contains nested object definitions (no schema keywords), handle as container
                    # These are typically namespace containers like "resourceTypes" that only organize other types
                    if not has_schema_keywords:
                        non_meta_items = {k: v for k, v in def_schema.items() 
                                        if not k.startswith('$') and k not in ['title', 'description', 'examples']}
                        if non_meta_items and all(isinstance(value, dict) for value in non_meta_items.values()):
                            # This looks like a pure container - but check if any references point to it
                            ref_target = f"#/definitions/{def_name}"
                            ref_target_normalized = f"#/definitions/{avro_name(def_name)}"
                            
                            # Search the entire schema for references to this definition
                            schema_str = json.dumps(json_schema)
                            if ref_target in schema_str or ref_target_normalized in schema_str:
                                # This container is being referenced, so we need to keep it as a valid object
                                # Create a minimal valid object type
                                dependencies = []
                                normalized_def_name, original_name = avro_name_with_altname(def_name)
                                container_def = {
                                    'type': 'object',
                                    'name': normalized_def_name,
                                    'additionalProperties': True  # Allow any properties to make it valid
                                }
                                if original_name is not None:
                                    container_def['altnames'] = {'json': original_name}
                                structure_schema['definitions'][normalized_def_name] = container_def
                                continue
                            else:
                                # Skip pure container definitions that aren't referenced
                                continue
                          # Process all dictionary definitions - this includes schemas with only descriptions
                    dependencies = []
                    normalized_def_name, original_name = avro_name_with_altname(def_name)
                    converted_def = self.json_type_to_structure_type(
                        def_schema, def_name, '', namespace, dependencies,
                        json_schema, base_uri, structure_schema, [], 1
                    )
                    if isinstance(converted_def, dict):
                        converted_def['name'] = normalized_def_name
                        # Add alternate name if the original was different
                        if original_name is not None:
                            if 'altnames' not in converted_def:
                                converted_def['altnames'] = {}
                            converted_def['altnames']['json'] = original_name
                        structure_schema['definitions'][normalized_def_name] = converted_def
                    else:
                        definition_obj = {
                            'type': converted_def,
                            'name': normalized_def_name
                        }
                        # Add alternate name if the original was different
                        if original_name is not None:
                            definition_obj['altnames'] = {'json': original_name}
                        structure_schema['definitions'][normalized_def_name] = definition_obj
        
        # Handle root-level schema type
        root_type_keys = ['type', 'properties', 'items', 'additionalProperties', 'oneOf', 'anyOf', 'allOf']
        has_root_type = any(key in json_schema for key in root_type_keys)
        if has_root_type and isinstance(json_schema, dict):
            dependencies = []
            root_converted = self.json_type_to_structure_type(
                json_schema, self.root_class_name, '', namespace, dependencies,
                json_schema, base_uri, structure_schema, [], 1
            )
            
            # Merge root type properties into schema
            if isinstance(root_converted, dict):
                for key, value in root_converted.items():
                    if key not in structure_schema:
                        structure_schema[key] = value
            else:
                structure_schema['type'] = root_converted
          # Handle schema metadata
        if isinstance(json_schema, dict):
            if '$id' in json_schema:
                structure_schema['$id'] = json_schema['$id']
            elif 'id' in json_schema:
                structure_schema['$id'] = json_schema['id']
            else:
                # Generate default $id if missing
                structure_schema['$id'] = f"https://example.com/{namespace.replace('.', '/')}.schema.json"
                  
            # Add description if present, or map title to description if no description exists
            if 'description' in json_schema:
                structure_schema['description'] = json_schema['description']
            elif 'title' in json_schema:
                structure_schema['description'] = json_schema['title']
        else:
            # Generate default $id for non-dict schemas
            structure_schema['$id'] = f"https://example.com/{namespace.replace('.', '/')}.schema.json"
        
        # Mark abstract types
        self._mark_abstract_types(structure_schema)
        
        return structure_schema
    
    def convert_json_schema_to_structure(self, json_schema: Union[dict, list], base_uri: str = "") -> dict:
        """
        Convert a JSON Schema dictionary to JSON Structure format.
        
        Args:
            json_schema (dict): The JSON Schema to convert
            base_uri (str): Base URI for reference resolution
            
        Returns:
            dict: The converted JSON Structure schema
            
        Raises:
            ValueError: If the input schema is invalid
            TypeError: If the schema format is not supported
        """
        if not isinstance(json_schema, (dict, list)):
            raise TypeError(f"Expected dict or list, got {type(json_schema)}")
            
        if isinstance(json_schema, dict) and not json_schema:
            raise ValueError("Empty schema dictionary provided")
            
        try:
            structure_schema = self.jsons_to_structure(json_schema, self.root_namespace, base_uri)
            # Always add a name to the root if it has a type and no name
            if 'type' in structure_schema and 'name' not in structure_schema:
                structure_schema['name'] = avro_name(self.root_class_name)            # Only add $uses if the feature is actually used
            used = self._scan_for_uses(structure_schema)
            if used:
                structure_schema['$uses'] = used
            elif '$uses' in structure_schema:
                del structure_schema['$uses']            # Final validation to ensure map and set types are complete
            # validation_errors = self.validate_structure_completeness(structure_schema)
            # if validation_errors:
            #     print(f"WARNING: Structure validation found incomplete types:")
            #     for error in validation_errors:
            #         print(f"  - {error}")
            
            # Validate and fix JSON Structure compliance
            structure_schema = self._validate_and_fix_json_structure_type(structure_schema)
                
            return structure_schema
        except Exception as e:
            raise ValueError(f"Failed to convert JSON Schema to JSON Structure: {e}") from e

    def is_valid_identifier(self, name: str) -> bool:
        """
        Check if a name is a valid identifier (for property names, etc.).
        """
        if not name or not isinstance(name, str):
            return False        # Check if it's a valid Python identifier (basic check)
        import re
        return bool(re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name))
    
    def normalize_identifier(self, name: str) -> str:
        """
        Normalize a name to be a valid identifier.
        """
        if not name or not isinstance(name, str):
            return 'property'
        
        # Replace invalid characters with underscores
        import re
        normalized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
          # Ensure it doesn't start with a number
        if normalized and normalized[0].isdigit():
            normalized = 'prop_' + normalized
        
        # Ensure it's not empty
        if not normalized:
            normalized = 'property'
            
        return normalized
    
    def _validate_and_fix_json_structure_type(self, structure_type: Any) -> Any:
        """
        Validate and fix a JSON Structure type to ensure compliance.
        
        This method post-processes generated JSON Structure schemas to fix common issues:
        - Converts "integer" type to "number" (JSON Structure doesn't support integer)
        - Ensures arrays have "items"
        - Ensures objects have "properties" 
        - Ensures map/set "values"/"items" are schema objects, not strings
        - Recursively fixes nested schemas
        """
        if not isinstance(structure_type, dict):
            return structure_type
            
        # Create a copy to avoid modifying the original
        structure_type = structure_type.copy()
        
        # Fix invalid types
        if structure_type.get('type') == 'integer':
            structure_type['type'] = 'number'  # JSON Structure doesn't have integer type
        
        # Ensure arrays have items
        elif structure_type.get('type') == 'array' and 'items' not in structure_type:
            structure_type['items'] = {'type': 'object'}  # Default to object items
            
        # Ensure objects have properties (unless they extend another type)
        elif (structure_type.get('type') == 'object' and 
              'properties' not in structure_type and 
              '$ref' not in structure_type and 
              '$extends' not in structure_type):
            structure_type['properties'] = {}  # Default to empty properties
            
        # Ensure map values are schema objects
        elif structure_type.get('type') == 'map' and 'values' in structure_type:
            values = structure_type['values']
            if isinstance(values, str):
                structure_type['values'] = {'type': values}
            elif isinstance(values, dict):
                structure_type['values'] = self._validate_and_fix_json_structure_type(values)
                
        # Ensure set items are schema objects
        elif structure_type.get('type') == 'set' and 'items' in structure_type:
            items = structure_type['items']
            if isinstance(items, str):
                structure_type['items'] = {'type': items}
            elif isinstance(items, dict):
                structure_type['items'] = self._validate_and_fix_json_structure_type(items)
            
        # Recursively validate nested structures - comprehensive approach
        if 'anyOf' in structure_type:
            structure_type['anyOf'] = [
                self._validate_and_fix_json_structure_type(item) 
                for item in structure_type['anyOf']
            ]
        elif 'oneOf' in structure_type:
            structure_type['oneOf'] = [
                self._validate_and_fix_json_structure_type(item) 
                for item in structure_type['oneOf']
            ]
        elif 'allOf' in structure_type:
            structure_type['allOf'] = [
                self._validate_and_fix_json_structure_type(item) 
                for item in structure_type['allOf']
            ]
        
        # Handle nested schemas in various contexts
        if 'items' in structure_type and isinstance(structure_type['items'], dict):
            structure_type['items'] = self._validate_and_fix_json_structure_type(structure_type['items'])
        elif 'values' in structure_type and isinstance(structure_type['values'], dict):
            structure_type['values'] = self._validate_and_fix_json_structure_type(structure_type['values'])
        elif 'properties' in structure_type and isinstance(structure_type['properties'], dict):
            structure_type['properties'] = {
                k: self._validate_and_fix_json_structure_type(v)
                for k, v in structure_type['properties'].items()
            }
          # Handle additionalProperties  
        if 'additionalProperties' in structure_type and isinstance(structure_type['additionalProperties'], dict):
            structure_type['additionalProperties'] = self._validate_and_fix_json_structure_type(structure_type['additionalProperties'])
        
        # Handle definitions
        if 'definitions' in structure_type and isinstance(structure_type['definitions'], dict):
            structure_type['definitions'] = {
                k: self._validate_and_fix_json_structure_type(v)
                for k, v in structure_type['definitions'].items()
            }
        
        # Handle $defs (JSON Schema 2019-09+)
        if '$defs' in structure_type and isinstance(structure_type['$defs'], dict):
            structure_type['$defs'] = {
                k: self._validate_and_fix_json_structure_type(v)
                for k, v in structure_type['$defs'].items()
            }
    
        return structure_type
        
    def _sort_json_structure_properties(self, schema: Any) -> Any:
        """
        Recursively sort properties in a JSON Structure schema for consistent output.
        """
        if not isinstance(schema, dict):
            return schema
            
        result = {}
        
        # Sort keys, putting common keys first
       
        key_order = ['$schema', 'type', 'title', 'description', 'properties', 'required', 'items', 'values', 'anyOf', 'oneOf', 'allOf', '$ref', '$extends']
        sorted_keys = []
        
        # Add keys in preferred order
        for key in key_order:
            if key in schema:
                sorted_keys.append(key)
        
        # Add remaining keys alphabetically
        remaining_keys = sorted([k for k in schema.keys() if k not in key_order])
        sorted_keys.extend(remaining_keys)
        
        # Build result with sorted keys
        for key in sorted_keys:
            value = schema[key]
            if key == 'properties' and isinstance(value, dict):
                # Sort properties alphabetically
                result[key] = {k: self._sort_json_structure_properties(v) for k, v in sorted(value.items())}
            elif key in ['anyOf', 'oneOf', 'allOf'] and isinstance(value, list):
                # Recursively sort composition schemas
                result[key] = [self._sort_json_structure_properties(item) for item in value]
            elif key in ['items', 'values'] and isinstance(value, dict):
                # Recursively sort nested schemas
                result[key] = self._sort_json_structure_properties(value)
            else:
                result[key] = value
                
        return result
    
    def _convert_constraint_composition_to_required(self, json_type: dict) -> dict:
        """
        Convert constraint-only composition (anyOf with property requirements) to a simple required array.
        This is used when anyOf items only add constraints without changing the structure.
        """
        try:
            # Check if this is constraint-only anyOf
            if 'anyOf' not in json_type:
                return json_type
                
            # Gather all required properties from anyOf items
            all_required = set()
            base_properties = json_type.get('properties', {})
            
            for anyof_item in json_type['anyOf']:
                if isinstance(anyof_item, dict) and 'required' in anyof_item:
                    # Only consider it constraint-only if it doesn't define new properties
                    item_properties = anyof_item.get('properties', {})
                    if not item_properties or all(prop in base_properties for prop in item_properties):
                        all_required.update(anyof_item['required'])
            
            # Create simplified schema
            result = {k: v for k, v in json_type.items() if k != 'anyOf'}
            if all_required:
                result['required'] = sorted(list(all_required))
                
            return result
            
        except Exception as e:
            # If conversion fails, return original
            return json_type
    
    def _is_constraint_only_schema(self, json_type: dict) -> bool:
        """
        Check if a schema contains only constraints (no structural elements).
        Used to determine if anyOf items are constraint-only.
        """
        if not isinstance(json_type, dict):
            return False
            
        # Constraint-only keys
        constraint_keys = {'required', 'minProperties', 'maxProperties', 'dependencies', 'dependentRequired', 'dependentSchemas'}
        
        # Structural keys that would make it not constraint-only
        structural_keys = {'type', 'properties', 'additionalProperties', 'patternProperties', 'items', 'anyOf', 'oneOf', 'allOf'}
        
        schema_keys = set(json_type.keys()) - {'title', 'description', '$id', '$schema'}
        
        # It's constraint-only if it has only constraint keys and no structural keys
        return bool(schema_keys & constraint_keys) and not bool(schema_keys & structural_keys)
    
    def _convert_constraint_anyof_to_permutations(self, json_type: dict) -> dict:
        """
        Convert constraint-only anyOf to a schema with required properties that represent
        the union of all constraint requirements.
        """
        try:
            if 'anyOf' not in json_type:
                return json_type
                
            # Collect all required properties from constraint-only anyOf items
            all_required = set()
            
            for anyof_item in json_type['anyOf']:
                if isinstance(anyof_item, dict) and self._is_constraint_only_schema(anyof_item):
                    if 'required' in anyof_item:
                        all_required.update(anyof_item['required'])
              # Return schema with union of required properties
            result = {'required': sorted(list(all_required))} if all_required else {}
            return result
            
        except Exception as e:
            # If conversion fails, return empty schema
            return {}

    def has_composition_keywords(self, json_type: dict) -> bool:
        """
        Check if a JSON schema has composition keywords (anyOf, oneOf, allOf) or conditional keywords (if/then/else).
        """
        if not isinstance(json_type, dict):
            return False
        return any(keyword in json_type for keyword in ['anyOf', 'oneOf', 'allOf', 'if', 'then', 'else'])
    
    def resolve_composition_keywords(self, json_type: dict, record_name: str, field_name: str, namespace: str, dependencies: list, json_schema: dict, base_uri: str, structure_schema: dict, record_stack: list, recursion_depth: int) -> dict:
        """
        Resolve composition keywords in JSON schema by flattening them.
        This is a simple implementation that merges composition schemas.
        """
        try:
            if 'allOf' in json_type:
                # Merge all schemas in allOf
                merged = {}
                for schema in json_type['allOf']:
                    converted = self.json_type_to_structure_type(
                        schema, record_name, field_name, namespace, dependencies,
                        json_schema, base_uri, structure_schema, record_stack, recursion_depth + 1
                    )
                    if isinstance(converted, dict):
                        # Simple merge - in real scenarios this would be more complex
                        for key, value in converted.items():
                            if key == 'properties' and key in merged:
                                merged[key].update(value)
                            elif key == 'required' and key in merged:
                                merged[key] = list(set(merged[key] + value))
                            else:
                                merged[key] = value
                return merged
                
            elif 'anyOf' in json_type:
                # For anyOf, convert to JSON Structure type union
                anyof_schemas = []
                for anyof_item in json_type['anyOf']:
                    converted = self.json_type_to_structure_type(
                        anyof_item, record_name, field_name, namespace, dependencies,
                        json_schema, base_uri, structure_schema, record_stack, recursion_depth + 1
                    )
                    anyof_schemas.append(converted)
                
                return {
                    'type': anyof_schemas
                }
                    
            elif 'oneOf' in json_type:
                # First check if this is a discriminated union pattern
                choice_info = self.detect_discriminated_union_pattern(json_type)
                if choice_info:
                    # Convert to JSON Structure choice type
                    choice_result = {
                        'type': 'choice',
                        'choices': choice_info['choices']
                    }
                    
                    # Add selector if specified (for tagged unions)
                    if choice_info.get('selector'):
                        choice_result['selector'] = choice_info['selector']
                    
                    # Add name if we have one
                    if record_name:
                        choice_result['name'] = avro_name(record_name)
                        
                    return choice_result
                
                # For oneOf without discriminated union, return the first option as a fallback
                if json_type['oneOf']:
                    return self.json_type_to_structure_type(
                        json_type['oneOf'][0], record_name, field_name, namespace, dependencies,
                        json_schema, base_uri, structure_schema, record_stack, recursion_depth + 1
                    )# Fallback to map type with any values
            return {'type': 'map', 'values': {'type': 'any'}}
            
        except Exception as e:            # If resolution fails, return a basic map type
            return {'type': 'map', 'values': {'type': 'any'}}

    def detect_discriminated_union_pattern(self, json_type: dict) -> dict | None:
        """
        Detect discriminated union patterns in oneOf schemas that should be converted to choice type.
        
        A discriminated union pattern is identified when:
        1. Schema has oneOf with multiple object schemas
        2. Each schema has a distinct set of required properties (mutually exclusive)
        3. The schemas define object structures (have properties)
        
        Patterns supported:
        - Simple discriminated unions: Each schema has exactly one unique required property
        - Complex discriminated unions: Each schema has a unique combination of required properties
        - Tagged unions: Each schema has a discriminator property with different enum/const values
        
        Args:
            json_type (dict): The JSON schema object with oneOf
            
        Returns:
            dict | None: Choice type configuration or None if not a discriminated union
        """
        if 'oneOf' not in json_type:
            return None
            
        oneof_items = json_type['oneOf']
        if len(oneof_items) < 2:
            return None
            
        # Check if all items are object schemas with properties
        all_schemas_are_objects = True
        for item in oneof_items:
            if not isinstance(item, dict):
                return None
            if not (item.get('type') == 'object' or 'properties' in item):
                all_schemas_are_objects = False
                break
                
        if not all_schemas_are_objects:
            return None
            
        # Pattern 1: Check for tagged unions with discriminator property
        discriminator_result = self._detect_tagged_union_pattern(oneof_items)
        if discriminator_result:
            return discriminator_result
            
        # Pattern 2: Check for simple discriminated unions (each schema has exactly one unique required property)
        simple_result = self._detect_simple_discriminated_union(oneof_items)
        if simple_result:
            return simple_result
            
        # Pattern 3: Check for complex discriminated unions (unique combinations of required properties)
        complex_result = self._detect_complex_discriminated_union(oneof_items)
        if complex_result:
            return complex_result
            
        return None

    def _detect_tagged_union_pattern(self, oneof_items: list) -> dict | None:
        """
        Detect tagged union pattern where all schemas have the same discriminator property
        with different enum/const values.
        """
        discriminator_props = {}
        common_discriminator = None
        
        for item in oneof_items:
            properties = item.get('properties', {})
            
            # Look for a property with enum or const value
            discriminator_found = False
            for prop_name, prop_schema in properties.items():
                if 'enum' in prop_schema and len(prop_schema['enum']) == 1:
                    # Single enum value acts as discriminator
                    disc_value = prop_schema['enum'][0]
                    if common_discriminator is None:
                        common_discriminator = prop_name
                    elif common_discriminator != prop_name:
                        return None  # Different discriminator properties
                    discriminator_props[str(disc_value)] = {'type': 'object'}
                    discriminator_found = True
                    break
                elif 'const' in prop_schema:
                    # Const value acts as discriminator
                    disc_value = prop_schema['const']
                    if common_discriminator is None:
                        common_discriminator = prop_name
                    elif common_discriminator != prop_name:
                        return None  # Different discriminator properties
                    discriminator_props[str(disc_value)] = {'type': 'object'}
                    discriminator_found = True
                    break
                    
            if not discriminator_found:
                return None
                
        if common_discriminator and len(discriminator_props) == len(oneof_items):
            return {
                'type': 'choice',
                'choices': discriminator_props,
                'selector': common_discriminator  # Tagged union with explicit selector
            }
            
        return None

    def _detect_simple_discriminated_union(self, oneof_items: list) -> dict | None:
        """
        Detect simple discriminated union where each schema has exactly one unique required property.
        """
        choice_mapping = {}
        
        for item in oneof_items:
            if 'properties' not in item or 'required' not in item:
                return None
                
            required = item['required']
            if not isinstance(required, list) or len(required) != 1:
                return None  # Must have exactly one required property
                
            required_prop = required[0]
            
            # Check if this property name is already used by another choice
            if required_prop in choice_mapping:
                return None  # Properties must be mutually exclusive
                
            # Ensure the required property exists in the properties
            if required_prop not in item['properties']:
                return None  # Required property must exist in properties
                
            # Store the choice information - use 'any' type for discriminated unions
            choice_mapping[required_prop] = {
                'type': 'any',
                'description': f'Choice variant with {required_prop} property'
            }
        
        if len(choice_mapping) == len(oneof_items):
            return {
                'type': 'choice',
                'choices': choice_mapping,
                'selector': None  # Inline choice without explicit selector property
            }
            
        return None

    def _detect_complex_discriminated_union(self, oneof_items: list) -> dict | None:
        """
        Detect complex discriminated union where each schema has a unique combination of required properties.
        """
        required_sets = []
        choice_mapping = {}
        
        for i, item in enumerate(oneof_items):
            if 'properties' not in item:
                return None
                
            required = set(item.get('required', []))
            
            # Check if this combination of required properties is unique
            for existing_set in required_sets:
                if required == existing_set:
                    return None  # Non-unique required property combination
                # Check for overlap - if sets overlap significantly, it's not a clean discriminated union
                overlap = required & existing_set
                if len(overlap) > 0 and (len(overlap) / len(required | existing_set)) > 0.5:
                    return None  # Too much overlap
                    
            required_sets.append(required)
            
            # Create a choice name based on the required properties
            if len(required) == 0:
                choice_name = f'variant_{i}'
            elif len(required) == 1:
                choice_name = list(required)[0]
            else:
                # Sort for consistent naming
                sorted_props = sorted(required)
                choice_name = '_'.join(sorted_props[:2])  # Use first two properties for name
                if len(sorted_props) > 2:
                    choice_name += '_etc'
                    
            # Use 'any' type for discriminated unions
            choice_mapping[choice_name] = {
                'type': 'any',
                'description': f'Choice variant requiring: {", ".join(sorted(required))}'
            }
        
        if len(choice_mapping) == len(oneof_items) and len(choice_mapping) >= 2:
            return {
                'type': 'choice',
                'choices': choice_mapping,
                'selector': None  # Inline choice without explicit selector property
            }
            
        return None

    def _ensure_abstract_base_type(self, base_ref: str, abstract_base_name: str, structure_schema: dict, json_schema: dict, base_uri: str) -> None:
        """
        Ensure that an abstract base type exists for inheritance patterns.
        
        Args:
            base_ref (str): The original $ref to the base type
            abstract_base_name (str): The name for the abstract base type  
            structure_schema (dict): The structure schema being built
            json_schema (dict): The original JSON schema
            base_uri (str): The base URI for resolving references
        """
        # Ensure definitions section exists
        if 'definitions' not in structure_schema:
            structure_schema['definitions'] = {}
            
        # If abstract base type already exists, don't recreate it
        if abstract_base_name in structure_schema['definitions']:
            return
            
        # Guard against recursive abstract base type creation
        if not hasattr(self, '_creating_abstract_bases'):
            self._creating_abstract_bases = set()
            
        if abstract_base_name in self._creating_abstract_bases:
            print(f"WARNING: Circular reference detected while creating abstract base type {abstract_base_name}")
            # Create a minimal abstract type to break the cycle
            structure_schema['definitions'][abstract_base_name] = {
                'type': 'object',
                'abstract': True,
                'name': abstract_base_name,
                'properties': {}
            }
            return
            
        # Add to the guard set
        self._creating_abstract_bases.add(abstract_base_name)
        
        try:
            # Resolve the original base type reference
            base_schema, _ = self.resolve_reference({'$ref': base_ref}, base_uri, json_schema)
            
            # Convert the base type to structure format, but without triggering inheritance conversion
            # to avoid infinite recursion
            old_detect_inheritance = self.detect_inheritance
            old_preserve_composition = self.preserve_composition
            self.detect_inheritance = False  # Temporarily disable inheritance detection
            self.preserve_composition = False  # Force composition flattening for base types
            
            try:
                base_structure = self.json_type_to_structure_type(
                    base_schema, abstract_base_name, '', '', [], json_schema, base_uri, 
                    structure_schema, [], 1
                )
                
                # Mark it as abstract
                if isinstance(base_structure, dict):
                    base_structure['abstract'] = True
                    base_structure['name'] = abstract_base_name
                    
                    # Store the abstract base type
                    structure_schema['definitions'][abstract_base_name] = base_structure
                elif isinstance(base_structure, str):
                    # If the base resolves to a simple type, create an object wrapper
                    structure_schema['definitions'][abstract_base_name] = {
                        'type': 'object',
                        'abstract': True,
                        'name': abstract_base_name,
                        'properties': {}
                    }
                    
            finally:
                # Restore inheritance detection and composition settings
                self.detect_inheritance = old_detect_inheritance
                self.preserve_composition = old_preserve_composition
            
        except Exception as e:
            # If we can't resolve the base type, create a minimal abstract type
            print(f"WARNING: Failed to create abstract base type {abstract_base_name}: {e}")
            structure_schema['definitions'][abstract_base_name] = {
                'type': 'object',
                'abstract': True,
                'name': abstract_base_name,
                'properties': {}
            }
        finally:
            # Remove from the guard set
            self._creating_abstract_bases.discard(abstract_base_name)

    def create_pattern_union_maps(self, pattern_properties: dict, additional_props, record_name: str, namespace: str, dependencies: list, json_schema: dict, base_uri: str, structure_schema: dict, record_stack: list, recursion_depth: int = 1) -> dict:
        """
        Create a type union of maps for multiple patternProperties with optional additionalProperties.
        Each map in the union has a single pattern constraint.
        If additionalProperties is not False, creates an additional map for the fallback.
        Uses JSON Structure type union syntax: {"type": [map1, map2, ...]}
        All compound types are hoisted to /definitions and referenced via $ref.
        
        Args:
            pattern_properties (dict): The patternProperties object with patterns as keys
            additional_props: The additionalProperties value (False, True, or schema dict)
            record_name (str): Name of the record
            namespace (str): Namespace
            dependencies (list): Dependencies list
            json_schema (dict): The full JSON schema
            base_uri (str): Base URI
            structure_schema (dict): Structure schema list
            record_stack (list): Record stack for recursion detection
            recursion_depth (int): Current recursion depth
            
        Returns:
            dict: JSON Structure type union of maps using {"type": [...]} syntax
        """
        # Initialize definitions if it doesn't exist
        if 'definitions' not in structure_schema:
            structure_schema['definitions'] = {}
        
        # Create a map for each pattern and hoist to definitions
        map_refs = []
        for idx, (pattern, values_schema) in enumerate(pattern_properties.items()):
            # Create map with pattern validation
            map_result = self.create_structure_map(
                values_schema, record_name, namespace, dependencies,
                json_schema, base_uri, structure_schema, record_stack, recursion_depth
            )
            
            # Add keyNames validation for this specific pattern
            map_result['keyNames'] = {
                "type": "string",
                "pattern": pattern
            }
            
            # Hoist this map to definitions and get a $ref
            pattern_safe = re.sub(r'[^a-zA-Z0-9_]', '_', pattern)
            map_name_hint = f"{record_name}_PatternMap_{pattern_safe}_{idx}"
            map_ref = self._hoist_definition(map_result, structure_schema, map_name_hint)
            map_refs.append(map_ref)
        
        # If additionalProperties is not False, create an additional map for the fallback
        if additional_props is not False and additional_props is not None:
            if isinstance(additional_props, dict):
                # Create map for additionalProperties schema
                additional_map_result = self.create_structure_map(
                    additional_props, record_name, namespace, dependencies,
                    json_schema, base_uri, structure_schema, record_stack, recursion_depth
                )
                
                # Add keyNames validation for catch-all pattern (any string)
                additional_map_result['keyNames'] = {
                    "type": "string"
                }
                
                # Hoist this map to definitions and get a $ref
                additional_map_name_hint = f"{record_name}_AdditionalMap"
                additional_map_ref = self._hoist_definition(additional_map_result, structure_schema, additional_map_name_hint)
                map_refs.append(additional_map_ref)
            elif additional_props is True:
                # additionalProperties: true means any type - create a map with any values
                any_map_result = {
                    "type": "map",
                    "keyNames": {
                        "type": "string"
                    },
                    "values": "any"
                }
                
                # Hoist this map to definitions and get a $ref
                any_map_name_hint = f"{record_name}_AnyMap"
                any_map_ref = self._hoist_definition(any_map_result, structure_schema, any_map_name_hint)
                map_refs.append(any_map_ref)
        
        # Ensure $uses includes JSONStructureValidation
        self._ensure_validation_extension_in_structure_schema(structure_schema)
        
        # Return type union using JSON Structure type array syntax with hoisted references
        return {
            "type": map_refs
        }
    
def convert_json_schema_to_structure(input_data: str, root_namespace: str = 'example.com', base_uri: str = '') -> str:
    """
    Converts a JSON Schema document to JSON Structure format.
    
    Args:
        input_data (str): The JSON Schema document as a string.
        root_namespace (str): The namespace for the root schema. Defaults to 'example.com'.
        base_uri (str): The base URI for resolving references. Defaults to ''.
        
    Returns:
        str: The converted JSON Structure document as a string.
    """
    converter = JsonToStructureConverter()
    converter.root_namespace = root_namespace
    
    json_schema = json.loads(input_data)
    
    # Convert the JSON Schema to JSON Structure
    result = converter.jsons_to_structure(json_schema, root_namespace, base_uri)
    
    return json.dumps(result, indent=2)

def convert_json_schema_to_structure_files(
    json_schema_file_path: str, 
    structure_schema_path: str,
    root_namespace = None
) -> None:
    """
    Convert a JSON Schema file to JSON Structure format.
    
    Args:
        json_schema_file_path (str): Path to the input JSON Schema file
        structure_schema_path (str): Path to the output JSON Structure file
        root_namespace (str): The namespace for the root schema
    """
    # Use default namespace if None provided
    if root_namespace is None:
        root_namespace = 'example.com'
    
    # Read the JSON Schema file
    with open(json_schema_file_path, 'r', encoding='utf-8') as f:
        schema_content = f.read()
    
    # Convert to JSON Structure
    result = convert_json_schema_to_structure(schema_content, root_namespace)
    
    # Write the result
    with open(structure_schema_path, 'w', encoding='utf-8') as f:
        f.write(result)
