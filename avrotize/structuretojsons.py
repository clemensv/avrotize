""" JSON Structure to JSON Schema converter. """

# pylint: disable=too-many-lines, line-too-long, too-many-branches, too-many-statements, too-many-locals, too-many-nested-blocks, too-many-arguments, too-many-instance-attributes, too-many-public-methods, too-many-boolean-expressions

import json
import os
from typing import Any, Dict, List, Union, Optional


class StructureToJsonConverter:
    """
    Converts JSON Structure documents to JSON Schema format.
    
    JSON Structure is more constrained than JSON Schema, making this conversion
    straightforward with well-defined mappings.
    """

    def __init__(self) -> None:
        """Initialize the converter."""
        self.definitions: Dict[str, Any] = {}
        self.base_uri = ""
        self.structure_document: Optional[Dict[str, Any]] = None

    def convert_type(self, structure_type: str) -> Dict[str, Any]:
        """
        Convert a JSON Structure type to JSON Schema type.
        
        Args:
            structure_type (str): The JSON Structure type
            
        Returns:
            Dict[str, Any]: JSON Schema type definition
        """
        # Basic type mappings from JSON Structure to JSON Schema
        # Note: JSON Structure types have specific serialization rules:
        # - int64, uint64, decimal are serialized as strings in JSON due to precision/range limits
        # - binary32/binary64 are IEEE 754 binary formats
        # - timestamp is Unix epoch time as number
        type_mappings = {
            'null': {'type': 'null'},
            'string': {'type': 'string'},
            'boolean': {'type': 'boolean'},
            'bytes': {'type': 'string', 'format': 'byte'},
            'int8': {'type': 'integer', 'minimum': -128, 'maximum': 127},
            'int16': {'type': 'integer', 'minimum': -32768, 'maximum': 32767},
            'int32': {'type': 'integer', 'minimum': -2147483648, 'maximum': 2147483647},
            'int64': {'type': 'string', 'pattern': '^-?[0-9]+$'},  # Serialized as string
            'uint8': {'type': 'integer', 'minimum': 0, 'maximum': 255},
            'uint16': {'type': 'integer', 'minimum': 0, 'maximum': 65535},            'uint32': {'type': 'integer', 'minimum': 0, 'maximum': 4294967295},
            'uint64': {'type': 'string', 'pattern': '^[0-9]+$'},  # Serialized as string
            'int128': {'type': 'string', 'pattern': '^-?[0-9]+$'},  # Serialized as string
            'uint128': {'type': 'string', 'pattern': '^[0-9]+$'},  # Serialized as string
            'float8': {'type': 'number'},  # 8-bit float
            'float': {'type': 'number', 'format': 'float'},
            'double': {'type': 'number', 'format': 'double'},
            'float32': {'type': 'number', 'format': 'float'},  # 32-bit float
            'float64': {'type': 'number', 'format': 'double'},  # 64-bit float
            'binary32': {'type': 'number', 'format': 'float'},  # IEEE 754 binary32 (alias)
            'binary64': {'type': 'number', 'format': 'double'}, # IEEE 754 binary64 (alias)
            'decimal': {'type': 'string', 'pattern': '^-?[0-9]+(\\.[0-9]+)?$'},  # Serialized as string
            'binary': {'type': 'string', 'contentEncoding': 'base64'},  # Binary data
            'date': {'type': 'string', 'format': 'date'},
            'time': {'type': 'string', 'format': 'time'},
            'datetime': {'type': 'string', 'format': 'date-time'},
            'timestamp': {'type': 'number'},  # Unix epoch time
            'duration': {'type': 'string', 'format': 'duration'},
            'uuid': {'type': 'string', 'format': 'uuid'},
            'uri': {'type': 'string', 'format': 'uri'},
            'jsonpointer': {'type': 'string', 'format': 'json-pointer'},
        }
        
        return type_mappings.get(structure_type, {'type': 'string'})

    def convert_structure_schema(self, structure: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert a JSON Structure schema to JSON Schema.
        
        Args:
            structure (Dict[str, Any]): The JSON Structure schema
            
        Returns:
            Dict[str, Any]: The converted JSON Schema
        """
        # Store the structure document for reference resolution
        self.structure_document = structure
        
        schema: Dict[str, Any] = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
        }
          # Preserve $id if present
        if '$id' in structure:
            schema['$id'] = structure['$id']
            
        # Preserve $uses as extension if present
        if '$uses' in structure:
            schema['x-uses'] = structure['$uses']
            
        # Convert title and description
        if 'name' in structure:
            schema['title'] = structure['name']
        if 'description' in structure:
            schema['description'] = structure['description']
            
        # Convert the main type
        if 'type' in structure:
            schema_type = self._convert_type_definition(structure)
            schema.update(schema_type)
          # Add definitions if we collected any
        if self.definitions:
            schema['$defs'] = self.definitions
            
        return schema

    def _convert_type_definition(self, type_def: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert a JSON Structure type definition to JSON Schema.
        
        Args:
            type_def (Dict[str, Any]): The type definition
            
        Returns:
            Dict[str, Any]: JSON Schema type definition
        """
        if not isinstance(type_def, dict) or 'type' not in type_def:
            return {'type': 'string'}
            
        structure_type = type_def['type']
        
        # Handle type unions (arrays of types)
        if isinstance(structure_type, list):
            return self._convert_type_union(structure_type, type_def)
        
        if structure_type == 'object':
            return self._convert_object_type(type_def)
        elif structure_type == 'array':
            return self._convert_array_type(type_def)
        elif structure_type == 'set':
            # Sets in JSON Structure become arrays with uniqueItems in JSON Schema
            return self._convert_set_type(type_def)
        elif structure_type == 'map':
            return self._convert_map_type(type_def)
        elif structure_type == 'choice':
            return self._convert_choice_type(type_def)
        else:
            # Primitive type
            result = self.convert_type(structure_type)
            
            # Add validation constraints
            self._add_validation_constraints(result, type_def)
            
            return result

    def _convert_object_type(self, type_def: Dict[str, Any]) -> Dict[str, Any]:
        """Convert a JSON Structure object type to JSON Schema."""
        result = {'type': 'object'}
        
        if 'properties' in type_def:
            result['properties'] = {}
            for prop_name, prop_def in type_def['properties'].items():
                result['properties'][prop_name] = self._convert_type_definition(prop_def)
        
        if 'required' in type_def:
            result['required'] = type_def['required']
            
        if 'additionalProperties' in type_def:
            if isinstance(type_def['additionalProperties'], bool):
                result['additionalProperties'] = type_def['additionalProperties']
            else:
                result['additionalProperties'] = self._convert_type_definition(type_def['additionalProperties'])
        
        return result

    def _convert_array_type(self, type_def: Dict[str, Any]) -> Dict[str, Any]:
        """Convert a JSON Structure array type to JSON Schema."""
        result = {'type': 'array'}
        
        if 'items' in type_def:
            result['items'] = self._convert_type_definition(type_def['items'])
            
        # Add validation constraints
        if 'minItems' in type_def:
            result['minItems'] = type_def['minItems']
        if 'maxItems' in type_def:
            result['maxItems'] = type_def['maxItems']
            
        return result

    def _convert_set_type(self, type_def: Dict[str, Any]) -> Dict[str, Any]:
        """Convert a JSON Structure set type to JSON Schema array with uniqueItems."""
        result = {'type': 'array', 'uniqueItems': True}
        
        if 'items' in type_def:
            result['items'] = self._convert_type_definition(type_def['items'])
            
        # Add validation constraints
        if 'minItems' in type_def:
            result['minItems'] = type_def['minItems']
        if 'maxItems' in type_def:
            result['maxItems'] = type_def['maxItems']
            
        return result

    def _convert_map_type(self, type_def: Dict[str, Any]) -> Dict[str, Any]:
        """Convert a JSON Structure map type to JSON Schema object with additionalProperties."""
        result = {'type': 'object'}
        
        if 'values' in type_def:
            result['additionalProperties'] = self._convert_type_definition(type_def['values'])
        else:
            result['additionalProperties'] = True
            
        return result

    def _convert_choice_type(self, type_def: Dict[str, Any]) -> Dict[str, Any]:
        """Convert a JSON Structure choice type to JSON Schema oneOf."""
        result = {}
        
        if 'choices' in type_def:
            choices = type_def['choices']
            if isinstance(choices, dict):
                # Tagged union: choices is a dict mapping names to types
                result['oneOf'] = []
                for choice_name, choice_def in choices.items():
                    converted_choice = None
                    resolved_choice = None
                    
                    if isinstance(choice_def, dict):
                        # Handle $ref resolution
                        if '$ref' in choice_def:
                            ref_type = self._resolve_reference(choice_def['$ref'])
                            if ref_type:
                                resolved_choice = self._resolve_type_with_inheritance(ref_type)
                                converted_choice = self._convert_type_definition(resolved_choice)
                            else:
                                # Fallback if reference can't be resolved
                                converted_choice = {'type': 'string'}
                        else:
                            resolved_choice = choice_def
                            converted_choice = self._convert_type_definition(choice_def)
                    elif isinstance(choice_def, str):
                        # Simple type name
                        converted_choice = self.convert_type(choice_def)
                    else:
                        # Fallback to string type
                        converted_choice = {'type': 'string'}
                    
                    # For tagged unions, wrap in an object with the choice name
                    if not type_def.get('selector'):  # Tagged union
                        tagged_choice = {
                            'type': 'object',
                            'properties': {
                                choice_name: converted_choice
                            },
                            'required': [choice_name],
                            'additionalProperties': False
                        }
                        result['oneOf'].append(tagged_choice)
                    else:                        # Inline union with selector - resolve the base type and merge with choices
                        if '$extends' in type_def:
                            # Resolve the base type that provides common properties
                            base_type = self._resolve_reference(type_def['$extends'])
                            if base_type:
                                resolved_base = self._resolve_type_with_inheritance(base_type)
                                # Merge base properties with the choice-specific properties
                                if resolved_choice and 'properties' in resolved_base:
                                    choice_properties = {}
                                    base_props = resolved_base.get('properties', {})
                                    choice_props = resolved_choice.get('properties', {})
                                    
                                    # Merge properties from base and choice
                                    if isinstance(base_props, dict):
                                        choice_properties.update(base_props)
                                    if isinstance(choice_props, dict):
                                        choice_properties.update(choice_props)
                                      # Convert each property definition from JSON Structure to JSON Schema
                                    converted_properties = {}
                                    for prop_name, prop_def in choice_properties.items():
                                        converted_properties[prop_name] = self._convert_type_definition(prop_def)
                                    
                                    # Add selector constraint if this is an inline choice with selector
                                    if 'selector' in type_def:
                                        selector_field = type_def['selector']
                                        if selector_field in converted_properties:
                                            # Constrain the selector field to the specific choice name
                                            converted_properties[selector_field] = {'const': choice_name}
                                    
                                    merged_choice = {
                                        'type': 'object',
                                        'properties': converted_properties
                                    }
                                    
                                    # Add required fields from both base and choice
                                    required_fields = set()
                                    if 'required' in resolved_base and isinstance(resolved_base['required'], list):
                                        required_fields.update(resolved_base['required'])
                                    if 'required' in resolved_choice and isinstance(resolved_choice['required'], list):
                                        required_fields.update(resolved_choice['required'])
                                    if required_fields:
                                        merged_choice['required'] = sorted(list(required_fields))
                                        
                                    result['oneOf'].append(merged_choice)
                                else:
                                    result['oneOf'].append(converted_choice)
                            else:
                                result['oneOf'].append(converted_choice)
                        else:
                            # No inheritance, just use the converted choice
                            result['oneOf'].append(converted_choice)
            else:
                # Legacy format or error
                result['oneOf'] = []
                for choice in choices:
                    result['oneOf'].append(self._convert_type_definition(choice))
        
        return result

    def _convert_type_union(self, type_list: List[str], type_def: Dict[str, Any]) -> Dict[str, Any]:
        """Convert a JSON Structure type union to JSON Schema."""
        if len(type_list) == 1:
            # Single type, convert directly
            single_type_def = {'type': type_list[0]}
            single_type_def.update({k: v for k, v in type_def.items() if k != 'type'})
            return self._convert_type_definition(single_type_def)
        
        # Multiple types - create anyOf
        result = {
            'anyOf': []
        }
        
        for type_name in type_list:
            single_type_def = {'type': type_name}
            # Don't inherit other properties for union members
            converted = self.convert_type(type_name)
            result['anyOf'].append(converted)
        
        # Add any validation constraints to the union itself
        self._add_validation_constraints(result, type_def)
        
        return result

    def _add_validation_constraints(self, result: Dict[str, Any], type_def: Dict[str, Any]) -> None:
        """Add validation constraints from JSON Structure to JSON Schema."""        # String constraints
        if 'minLength' in type_def:
            result['minLength'] = type_def['minLength']
        if 'maxLength' in type_def:
            result['maxLength'] = type_def['maxLength']
        if 'pattern' in type_def:
            result['pattern'] = type_def['pattern']
        if 'format' in type_def:
            result['format'] = type_def['format']
        if 'enum' in type_def:
            result['enum'] = type_def['enum']
        if 'const' in type_def:
            result['const'] = type_def['const']
            
        # Numeric constraints
        if 'minimum' in type_def:
            result['minimum'] = type_def['minimum']
        if 'maximum' in type_def:
            result['maximum'] = type_def['maximum']
        if 'exclusiveMinimum' in type_def:
            result['exclusiveMinimum'] = type_def['exclusiveMinimum']
        if 'exclusiveMaximum' in type_def:
            result['exclusiveMaximum'] = type_def['exclusiveMaximum']
        if 'multipleOf' in type_def:
            result['multipleOf'] = type_def['multipleOf']
            
        # For decimal types, handle precision/scale
        if 'precision' in type_def or 'scale' in type_def:
            # Add custom keywords for precision/scale as JSON Schema doesn't have direct equivalents
            if 'precision' in type_def:
                result['x-precision'] = type_def['precision']
            if 'scale' in type_def:
                result['x-scale'] = type_def['scale']
        
        # Handle units and currencies as custom properties
        if 'unit' in type_def:
            result['x-unit'] = type_def['unit']
        if 'currency' in type_def:
            result['x-currency'] = type_def['currency']
            
        # Handle alternate names
        if 'altnames' in type_def:
            result['x-altnames'] = type_def['altnames']

    def _resolve_reference(self, ref_path: str) -> Optional[Dict[str, Any]]:
        """
        Resolve a $ref path to the actual type definition.
        
        Args:
            ref_path (str): The reference path (e.g., "#/definitions/Person")
            
        Returns:
            Optional[Dict[str, Any]]: The resolved type definition or None if not found
        """
        if not self.structure_document or not ref_path.startswith('#/'):
            return None
            
        # Remove the '#/' prefix and split the path
        path_parts = ref_path[2:].split('/')
        
        current = self.structure_document
        for part in path_parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None
                
        return current if isinstance(current, dict) else None

    def _resolve_type_with_inheritance(self, type_def: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve a type definition, handling $extends inheritance.
        
        Args:
            type_def (Dict[str, Any]): The type definition that may have $extends
            
        Returns:
            Dict[str, Any]: The resolved type definition with inheritance applied
        """
        if '$extends' not in type_def:
            return type_def
            
        # Resolve the base type
        base_ref = type_def['$extends']
        base_type = self._resolve_reference(base_ref)
        
        if not base_type:
            # If we can't resolve the base, return the original type
            return type_def
            
        # Recursively resolve the base type's inheritance
        resolved_base = self._resolve_type_with_inheritance(base_type)
        
        # Merge the base type with the current type
        # The current type's properties override the base type's properties
        merged = {}
        merged.update(resolved_base)
        merged.update(type_def)
        
        # Special handling for properties - merge them
        if 'properties' in resolved_base and 'properties' in type_def:
            merged_props = {}
            merged_props.update(resolved_base['properties'])
            merged_props.update(type_def['properties'])
            merged['properties'] = merged_props
            
        # Remove $extends from the final result as it's not part of JSON Schema
        if '$extends' in merged:
            del merged['$extends']
            
        return merged


def convert_structure_to_json_schema(
    structure_file_path: str, 
    json_schema_path: str
) -> None:
    """
    Convert a JSON Structure file to JSON Schema format.
    
    Args:
        structure_file_path (str): Path to the input JSON Structure file
        json_schema_path (str): Path to the output JSON Schema file
    """
    # Read the JSON Structure file
    with open(structure_file_path, 'r', encoding='utf-8') as f:
        structure_content = f.read()
    
    # Convert to JSON Schema
    result = convert_structure_to_json_schema_string(structure_content)
    
    # Write the result
    with open(json_schema_path, 'w', encoding='utf-8') as f:
        f.write(result)


def convert_structure_to_json_schema_string(structure_content: str) -> str:
    """
    Convert a JSON Structure string to JSON Schema format.
    
    Args:
        structure_content (str): The JSON Structure document as a string
        
    Returns:
        str: The converted JSON Schema document as a string
    """
    converter = StructureToJsonConverter()
    
    try:
        structure_schema = json.loads(structure_content)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON Structure document: {e}") from e
    
    # Convert the JSON Structure to JSON Schema
    result = converter.convert_structure_schema(structure_schema)
    
    return json.dumps(result, indent=2)
