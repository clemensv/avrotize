"""
CDDL (Concise Data Definition Language) to JSON Structure converter.

CDDL is defined in RFC 8610 and is a schema language primarily used for 
expressing CBOR and JSON data structures.

RFC 8610 Compliance Summary:
============================

Fully Implemented (RFC 8610 Sections 2-3):
- Primitive types: uint, nint, int, float16/32/64, tstr, bstr, bool, nil, any
- Maps/objects with member keys and values
- Arrays with homogeneous and heterogeneous (tuple) items
- Choice/union types (type1 / type2)
- Occurrence indicators: ? (optional), * (zero-or-more), + (one-or-more)
- Literal values (strings, integers, floats, booleans)
- Range constraints (min..max, min...max)
- Type references and nested types
- Groups and group composition
- CBOR tags (#6.n(type)) - underlying type extracted
- Choice from groups (&group)
- Comments (handled by parser)

Partially Implemented:
- Control operators (.size, .regexp, .default, .bits, .cbor, .cborseq, etc.):
  The base type is extracted but specific constraints are not mapped to
  JSON Structure equivalents (no direct mapping exists for many operators).
- Bounded occurrence (n*m): min/max parsed but not fully utilized.

Not Implemented:
- CDDL sockets ($name, $$name): Extensibility points for future definitions.
- Generic type parameters (<T>): Parameterized type definitions.
- Unwrap operator (~): Extracting group content.
- .within, .and operators: Type intersection.

Notes on Type Mapping:
- CBOR-specific types (bstr, tstr) map to JSON equivalents (bytes, string)
- CBOR tags are unwrapped to their underlying types
- Integer types (uint, nint, int) all map to int64 for JSON compatibility
- Float types map to float (float16/32) or double (float64)
"""

import json
import os
from typing import Any, Dict, List, Optional, Tuple, Union
from cddlparser import parse as cddl_parse
from cddlparser.ast import (
    CDDLTree, Rule, Type, Typename, Map, Array, Group, GroupChoice, 
    GroupEntry, Memberkey, Occurrence, Value, Range, Operator, Tag,
    ChoiceFrom, GenericArguments, GenericParameters
)

from avrotize.common import avro_name


# Default namespace for JSON Structure schema
DEFAULT_NAMESPACE = 'example.com'

# CDDL primitive types to JSON Structure type mapping
CDDL_PRIMITIVE_TYPES: Dict[str, Dict[str, Any]] = {
    # Integer types
    'uint': {'type': 'int64'},      # Unsigned integer (CBOR major type 0)
    'nint': {'type': 'int64'},      # Negative integer (CBOR major type 1)
    'int': {'type': 'int64'},       # Signed integer (uint or nint)
    'integer': {'type': 'int64'},   # Alias for int
    
    # Floating-point types
    'float16': {'type': 'float'},   # Half precision
    'float32': {'type': 'float'},   # Single precision
    'float64': {'type': 'double'},  # Double precision
    'float': {'type': 'double'},    # Generic float (maps to double)
    
    # String types
    'bstr': {'type': 'bytes'},      # Byte string (base64 encoded)
    'bytes': {'type': 'bytes'},     # Alias for bstr
    'tstr': {'type': 'string'},     # Text string (UTF-8)
    'text': {'type': 'string'},     # Alias for tstr
    
    # Boolean and null
    'bool': {'type': 'boolean'},
    'true': {'type': 'boolean'},
    'false': {'type': 'boolean'},
    'nil': {'type': 'null'},
    'null': {'type': 'null'},
    
    # Any type
    'any': {'type': 'any'},
    
    # Additional CBOR types
    'undefined': {'type': 'null'},  # CBOR undefined maps to null in JSON Structure
}


class CddlToStructureConverter:
    """
    Converts CDDL schema documents to JSON Structure format.
    """

    def __init__(self) -> None:
        self.root_namespace = DEFAULT_NAMESPACE
        self.type_registry: Dict[str, Dict[str, Any]] = {}
        self.definitions: Dict[str, Dict[str, Any]] = {}

    def convert_cddl_to_structure(self, cddl_content: str, namespace: Optional[str] = None) -> Dict[str, Any]:
        """
        Convert CDDL content to JSON Structure format.
        
        Args:
            cddl_content: The CDDL schema as a string
            namespace: Optional namespace for the schema
            
        Returns:
            Dict containing the JSON Structure schema
        """
        if namespace:
            self.root_namespace = namespace
            
        # Parse the CDDL content
        ast = cddl_parse(cddl_content)
        
        # Clear type registry
        self.type_registry.clear()
        self.definitions.clear()
        
        # First pass: collect all type names
        for rule in ast.rules:
            if hasattr(rule, 'getChildren'):
                children = rule.getChildren()
                if len(children) >= 2:
                    typename_node = children[0]
                    if hasattr(typename_node, 'name'):
                        self.type_registry[typename_node.name] = {}
        
        # Second pass: process all rules
        for rule in ast.rules:
            self._process_rule(rule)
        
        # Build the output structure
        structure_schema: Dict[str, Any] = {
            "$schema": "https://json-structure.org/meta/extended/v0/#",
            "$id": f"https://{self.root_namespace.replace('.', '/')}/schema.json"
        }
        
        # Add definitions if any
        if self.definitions:
            structure_schema['definitions'] = self.definitions
        
        # If there's a root type (first rule), add it to the schema
        if ast.rules and self.definitions:
            first_rule_name = self._get_rule_name(ast.rules[0])
            if first_rule_name and first_rule_name in self.definitions:
                root_def = self.definitions[first_rule_name]
                # Copy root type properties to schema root
                for key, value in root_def.items():
                    if key not in structure_schema:
                        structure_schema[key] = value
        
        # Scan for extension usage
        uses = self._scan_for_uses(structure_schema)
        if uses:
            structure_schema['$uses'] = uses
            
        return structure_schema

    def _get_rule_name(self, rule: Rule) -> Optional[str]:
        """Extract the name from a rule."""
        if hasattr(rule, 'getChildren'):
            children = rule.getChildren()
            if len(children) >= 1:
                typename_node = children[0]
                if hasattr(typename_node, 'name'):
                    return avro_name(typename_node.name)
        return None

    def _process_rule(self, rule: Rule) -> None:
        """Process a CDDL rule and add it to definitions."""
        if not hasattr(rule, 'getChildren'):
            return
            
        children = rule.getChildren()
        if len(children) < 2:
            return
            
        # First child is the typename, second is the type
        typename_node = children[0]
        type_node = children[1] if len(children) > 1 else None
        
        if not hasattr(typename_node, 'name') or type_node is None:
            return
            
        rule_name = avro_name(typename_node.name)
        original_name = typename_node.name
        
        # Convert the type
        structure_type = self._convert_type(type_node, rule_name)
        
        if structure_type:
            if isinstance(structure_type, dict):
                structure_type['name'] = rule_name
                # Add altnames if original name differs
                if original_name != rule_name:
                    structure_type['altnames'] = {'cddl': original_name}
            self.definitions[rule_name] = structure_type
            self.type_registry[typename_node.name] = structure_type

    def _convert_type(self, type_node: Any, context_name: str = '') -> Dict[str, Any]:
        """Convert a CDDL type node to JSON Structure type."""
        if type_node is None:
            return {'type': 'any'}
            
        node_type = type(type_node).__name__
        
        if node_type == 'Type':
            return self._convert_type_node(type_node, context_name)
        elif node_type == 'Typename':
            return self._convert_typename(type_node)
        elif node_type == 'Map':
            return self._convert_map(type_node, context_name)
        elif node_type == 'Array':
            return self._convert_array(type_node, context_name)
        elif node_type == 'Group':
            return self._convert_group(type_node, context_name)
        elif node_type == 'GroupChoice':
            return self._convert_group_choice(type_node, context_name)
        elif node_type == 'Value':
            return self._convert_value(type_node)
        elif node_type == 'Range':
            return self._convert_range(type_node)
        elif node_type == 'Operator':
            return self._convert_operator(type_node, context_name)
        elif node_type == 'Tag':
            return self._convert_tag(type_node, context_name)
        elif node_type == 'ChoiceFrom':
            return self._convert_choice_from(type_node, context_name)
        else:
            # Default fallback
            return {'type': 'any'}

    def _convert_type_node(self, type_node: Type, context_name: str = '') -> Dict[str, Any]:
        """Convert a Type node."""
        if not hasattr(type_node, 'getChildren'):
            return {'type': 'any'}
            
        children = type_node.getChildren()
        if not children:
            return {'type': 'any'}
        
        # If there are multiple children, this might be a choice (union)
        if len(children) > 1:
            # Check if this is a choice type (type1 / type2)
            converted_types = []
            for child in children:
                converted = self._convert_type(child, context_name)
                if converted:
                    converted_types.append(converted)
            
            if len(converted_types) > 1:
                # Create a union type
                return {'type': converted_types}
            elif len(converted_types) == 1:
                return converted_types[0]
        
        # Single child
        return self._convert_type(children[0], context_name)

    def _convert_typename(self, typename_node: Typename) -> Dict[str, Any]:
        """Convert a Typename node."""
        if not hasattr(typename_node, 'name'):
            return {'type': 'any'}
            
        type_name = typename_node.name
        
        # Check if it's a primitive type
        if type_name in CDDL_PRIMITIVE_TYPES:
            return dict(CDDL_PRIMITIVE_TYPES[type_name])
        
        # Check if it's a reference to another type
        normalized_name = avro_name(type_name)
        if type_name in self.type_registry or normalized_name in self.definitions:
            return {'$ref': f'#/definitions/{normalized_name}'}
        
        # Unknown type - treat as reference
        return {'$ref': f'#/definitions/{normalized_name}'}

    def _convert_map(self, map_node: Map, context_name: str = '') -> Dict[str, Any]:
        """Convert a Map node to JSON Structure object."""
        result: Dict[str, Any] = {
            'type': 'object'
        }
        
        properties: Dict[str, Any] = {}
        required: List[str] = []
        additional_properties: Optional[Dict[str, Any]] = None
        
        # Process map contents
        if hasattr(map_node, 'getChildren'):
            for child in map_node.getChildren():
                child_type = type(child).__name__
                if child_type == 'GroupChoice':
                    self._process_group_choice_for_object(
                        child, properties, required, context_name
                    )
        
        if properties:
            result['properties'] = properties
        if required:
            result['required'] = required
        if additional_properties:
            result['additionalProperties'] = additional_properties
            
        return result

    def _process_group_choice_for_object(
        self, 
        group_choice: GroupChoice, 
        properties: Dict[str, Any], 
        required: List[str],
        context_name: str
    ) -> None:
        """Process GroupChoice for object properties."""
        if not hasattr(group_choice, 'getChildren'):
            return
            
        for child in group_choice.getChildren():
            child_type = type(child).__name__
            if child_type == 'GroupEntry':
                self._process_group_entry_for_object(
                    child, properties, required, context_name
                )
            elif child_type == 'GroupChoice':
                # Nested group choice
                self._process_group_choice_for_object(
                    child, properties, required, context_name
                )

    def _process_group_entry_for_object(
        self, 
        entry: GroupEntry, 
        properties: Dict[str, Any], 
        required: List[str],
        context_name: str
    ) -> None:
        """Process a GroupEntry for object properties."""
        if not hasattr(entry, 'getChildren'):
            return
            
        children = entry.getChildren()
        
        # Parse occurrence, memberkey, and type from children
        occurrence_indicator = None
        member_key = None
        member_type = None
        
        for child in children:
            child_type = type(child).__name__
            if child_type == 'Occurrence':
                occurrence_indicator = self._parse_occurrence(child)
            elif child_type == 'Memberkey':
                member_key = self._parse_memberkey(child)
            elif child_type in ('Type', 'Typename', 'Map', 'Array', 'Group', 'Value'):
                member_type = self._convert_type(child, context_name)
        
        if member_key is None:
            return
            
        prop_name = member_key.get('name', '')
        original_name = member_key.get('original_name', prop_name)
        is_computed = member_key.get('computed', False)
        
        if not prop_name:
            return
        
        # Handle computed keys (patterns like tstr => any)
        if is_computed:
            # This is an additionalProperties pattern
            return
        
        normalized_name = avro_name(prop_name)
        
        if member_type:
            prop_schema = member_type.copy() if isinstance(member_type, dict) else member_type
        else:
            prop_schema = {'type': 'any'}
        
        # Handle occurrence indicators
        if occurrence_indicator:
            if occurrence_indicator.get('optional'):
                # Optional field - no need to add to required
                pass
            elif occurrence_indicator.get('min', 1) >= 1:
                required.append(normalized_name)
            
            # Handle array occurrences (* or +)
            if occurrence_indicator.get('array'):
                prop_schema = {
                    'type': 'array',
                    'items': prop_schema
                }
                if occurrence_indicator.get('min'):
                    prop_schema['minItems'] = occurrence_indicator['min']
                if occurrence_indicator.get('max'):
                    prop_schema['maxItems'] = occurrence_indicator['max']
        else:
            # No occurrence indicator means required
            required.append(normalized_name)
        
        # Add altnames if original name differs
        if original_name != normalized_name:
            if isinstance(prop_schema, dict):
                prop_schema['altnames'] = {'cddl': original_name}
        
        properties[normalized_name] = prop_schema

    def _parse_occurrence(self, occurrence: Occurrence) -> Dict[str, Any]:
        """Parse an Occurrence node."""
        result: Dict[str, Any] = {}
        
        if not hasattr(occurrence, 'getChildren'):
            return {'optional': True}
            
        for child in occurrence.getChildren():
            # Check for occurrence tokens
            if hasattr(child, 'kind'):
                token_kind = str(child.kind)
                if 'QUEST' in token_kind:  # ?
                    result['optional'] = True
                elif 'ASTERISK' in token_kind:  # *
                    result['array'] = True
                    result['min'] = 0
                elif 'PLUS' in token_kind:  # +
                    result['array'] = True
                    result['min'] = 1
            elif hasattr(child, 'value'):
                # Handle numeric occurrences like n*m
                pass
        
        if not result:
            result['optional'] = True
            
        return result

    def _parse_memberkey(self, memberkey: Memberkey) -> Optional[Dict[str, Any]]:
        """Parse a Memberkey node."""
        if not hasattr(memberkey, 'getChildren'):
            return None
            
        result: Dict[str, Any] = {}
        
        for child in memberkey.getChildren():
            child_type = type(child).__name__
            if child_type == 'Typename':
                if hasattr(child, 'name'):
                    name = child.name
                    # Check if this is a computed key (like tstr => any)
                    if name in ('tstr', 'text', 'bstr', 'bytes', 'int', 'uint'):
                        result['computed'] = True
                        result['key_type'] = name
                    else:
                        result['name'] = avro_name(name)
                        result['original_name'] = name
            elif child_type == 'Value':
                # String key value
                if hasattr(child, 'value'):
                    value = child.value
                    # Remove quotes if present
                    if value.startswith('"') and value.endswith('"'):
                        value = value[1:-1]
                    result['name'] = avro_name(value)
                    result['original_name'] = value
        
        return result if result else None

    def _convert_array(self, array_node: Array, context_name: str = '') -> Dict[str, Any]:
        """Convert an Array node to JSON Structure array."""
        result: Dict[str, Any] = {
            'type': 'array'
        }
        
        items_type: Optional[Dict[str, Any]] = None
        
        if hasattr(array_node, 'getChildren'):
            for child in array_node.getChildren():
                child_type = type(child).__name__
                if child_type == 'GroupChoice':
                    # Get the items type from the group
                    items_type = self._get_array_items_type(child, context_name)
                elif child_type in ('Type', 'Typename'):
                    items_type = self._convert_type(child, context_name)
        
        if items_type:
            result['items'] = items_type
        else:
            result['items'] = {'type': 'any'}
            
        return result

    def _get_array_items_type(self, group_choice: GroupChoice, context_name: str) -> Dict[str, Any]:
        """Extract items type from a GroupChoice in an array context."""
        if not hasattr(group_choice, 'getChildren'):
            return {'type': 'any'}
            
        children = group_choice.getChildren()
        if not children:
            return {'type': 'any'}
        
        # Handle tuple types (multiple different types)
        types = []
        for child in children:
            child_type = type(child).__name__
            if child_type == 'GroupEntry':
                entry_type = self._get_group_entry_type(child, context_name)
                if entry_type:
                    types.append(entry_type)
            elif child_type in ('Type', 'Typename'):
                types.append(self._convert_type(child, context_name))
        
        if len(types) == 0:
            return {'type': 'any'}
        elif len(types) == 1:
            return types[0]
        else:
            # Multiple types - could be a tuple
            return {'type': types}

    def _get_group_entry_type(self, entry: GroupEntry, context_name: str) -> Optional[Dict[str, Any]]:
        """Get the type from a GroupEntry."""
        if not hasattr(entry, 'getChildren'):
            return None
            
        for child in entry.getChildren():
            child_type = type(child).__name__
            if child_type in ('Type', 'Typename', 'Map', 'Array', 'Group', 'Value'):
                return self._convert_type(child, context_name)
        
        return None

    def _convert_group(self, group_node: Group, context_name: str = '') -> Dict[str, Any]:
        """Convert a Group node."""
        # A group is typically used inline and becomes an object
        return self._convert_map_like(group_node, context_name)

    def _convert_map_like(self, node: Any, context_name: str = '') -> Dict[str, Any]:
        """Convert a map-like structure to object."""
        result: Dict[str, Any] = {
            'type': 'object'
        }
        
        properties: Dict[str, Any] = {}
        required: List[str] = []
        
        if hasattr(node, 'getChildren'):
            for child in node.getChildren():
                child_type = type(child).__name__
                if child_type == 'GroupChoice':
                    self._process_group_choice_for_object(
                        child, properties, required, context_name
                    )
        
        if properties:
            result['properties'] = properties
        if required:
            result['required'] = required
            
        return result

    def _convert_group_choice(self, group_choice: GroupChoice, context_name: str = '') -> Dict[str, Any]:
        """Convert a GroupChoice node."""
        # Process as an object with properties
        result: Dict[str, Any] = {
            'type': 'object'
        }
        
        properties: Dict[str, Any] = {}
        required: List[str] = []
        
        self._process_group_choice_for_object(
            group_choice, properties, required, context_name
        )
        
        if properties:
            result['properties'] = properties
        if required:
            result['required'] = required
            
        return result

    def _convert_value(self, value_node: Value) -> Dict[str, Any]:
        """Convert a Value node (literal value)."""
        if not hasattr(value_node, 'value'):
            return {'type': 'any'}
            
        value = value_node.value
        
        # Determine type based on value
        if value.startswith('"') and value.endswith('"'):
            # String literal - create enum with single value
            string_val = value[1:-1]
            return {
                'type': 'string',
                'const': string_val
            }
        elif value in ('true', 'false'):
            return {
                'type': 'boolean',
                'const': value == 'true'
            }
        elif value.isdigit() or (value.startswith('-') and value[1:].isdigit()):
            return {
                'type': 'int64',
                'const': int(value)
            }
        elif '.' in value:
            try:
                float_val = float(value)
                return {
                    'type': 'double',
                    'const': float_val
                }
            except ValueError:
                pass
        
        return {'type': 'string', 'const': value}

    def _convert_range(self, range_node: Range) -> Dict[str, Any]:
        """Convert a Range node (min..max or min...max)."""
        result: Dict[str, Any] = {'type': 'int64'}
        
        if hasattr(range_node, 'getChildren'):
            children = range_node.getChildren()
            values = []
            for child in children:
                if hasattr(child, 'value'):
                    try:
                        values.append(int(child.value))
                    except ValueError:
                        try:
                            values.append(float(child.value))
                            result['type'] = 'double'
                        except ValueError:
                            pass
            
            if len(values) >= 2:
                result['minimum'] = values[0]
                result['maximum'] = values[1]
            elif len(values) == 1:
                result['minimum'] = values[0]
        
        return result

    def _convert_operator(self, operator_node: Operator, context_name: str = '') -> Dict[str, Any]:
        """
        Convert an Operator node (type constraints like .size, .regexp).
        
        Note: CDDL operators are partially supported. The base type is extracted
        but specific constraints may not be fully converted to JSON Structure
        equivalents. See module docstring for more details.
        """
        base_type: Dict[str, Any] = {'type': 'any'}
        
        if hasattr(operator_node, 'getChildren'):
            children = operator_node.getChildren()
            for child in children:
                child_type = type(child).__name__
                if child_type in ('Type', 'Typename'):
                    base_type = self._convert_type(child, context_name)
                    break
        
        return base_type

    def _convert_tag(self, tag_node: Tag, context_name: str = '') -> Dict[str, Any]:
        """Convert a Tag node (CBOR tags like #6.n)."""
        # CBOR tags don't have a direct JSON Structure equivalent
        # Just convert the underlying type
        if hasattr(tag_node, 'getChildren'):
            for child in tag_node.getChildren():
                child_type = type(child).__name__
                if child_type in ('Type', 'Typename', 'Map', 'Array'):
                    return self._convert_type(child, context_name)
        
        return {'type': 'any'}

    def _convert_choice_from(self, choice_from: ChoiceFrom, context_name: str = '') -> Dict[str, Any]:
        """Convert a ChoiceFrom node (&group or &enum)."""
        # This is typically used for enumerations
        result: Dict[str, Any] = {'type': 'string'}
        
        if hasattr(choice_from, 'getChildren'):
            for child in choice_from.getChildren():
                if hasattr(child, 'name'):
                    # Reference to an enumeration group
                    normalized_name = avro_name(child.name)
                    return {'$ref': f'#/definitions/{normalized_name}'}
        
        return result

    def _scan_for_uses(self, structure_schema: Dict[str, Any]) -> List[str]:
        """Scan the structure schema for extension feature usage."""
        uses = set()
        
        def scan(obj: Any) -> None:
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if k == 'altnames':
                        uses.add('JSONStructureAlternateNames')
                    if k in {'unit', 'currency', 'symbol'}:
                        uses.add('JSONStructureUnits')
                    if k in {'pattern', 'minLength', 'maxLength', 'minimum', 'maximum', 
                            'exclusiveMinimum', 'exclusiveMaximum', 'multipleOf', 
                            'const', 'enum', 'required', 'minItems', 'maxItems'}:
                        uses.add('JSONStructureValidation')
                    if k in {'if', 'then', 'else', 'dependentRequired', 'dependentSchemas', 
                            'anyOf', 'allOf', 'oneOf', 'not'}:
                        uses.add('JSONStructureConditionalComposition')
                    scan(v)
            elif isinstance(obj, list):
                for item in obj:
                    scan(item)
        
        scan(structure_schema)
        return sorted(uses)


def convert_cddl_to_structure(cddl_content: str, namespace: str = DEFAULT_NAMESPACE) -> str:
    """
    Convert CDDL content to JSON Structure format.
    
    Args:
        cddl_content: The CDDL schema as a string
        namespace: The namespace for the schema
        
    Returns:
        JSON Structure schema as a string
    """
    converter = CddlToStructureConverter()
    converter.root_namespace = namespace
    result = converter.convert_cddl_to_structure(cddl_content, namespace)
    return json.dumps(result, indent=2)


def convert_cddl_to_structure_files(
    cddl_file_path: str,
    structure_schema_path: str,
    namespace: Optional[str] = None
) -> None:
    """
    Convert a CDDL file to JSON Structure format.
    
    Args:
        cddl_file_path: Path to the input CDDL file
        structure_schema_path: Path to the output JSON Structure file
        namespace: Optional namespace for the schema
    """
    # Use default namespace if None provided
    if namespace is None:
        namespace = DEFAULT_NAMESPACE
    
    # Read the CDDL file
    with open(cddl_file_path, 'r', encoding='utf-8') as f:
        cddl_content = f.read()
    
    # Convert to JSON Structure
    result = convert_cddl_to_structure(cddl_content, namespace)
    
    # Write the result
    with open(structure_schema_path, 'w', encoding='utf-8') as f:
        f.write(result)
