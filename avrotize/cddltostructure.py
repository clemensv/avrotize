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
- Generic type parameters (<T>): Parameterized type definitions with instantiation
- Unwrap operator (~): Extracting group/array content

Control Operators (RFC 8610 Section 3.8):
- .size: Maps to minLength/maxLength for strings, minItems/maxItems for arrays
- .regexp: Maps to pattern validation (ECMAScript regex)
- .default: Maps to default value
- .lt, .le, .gt, .ge: Maps to minimum/maximum/exclusiveMinimum/exclusiveMaximum
- .eq: Maps to const value
- .bits, .cbor, .cborseq: Base type extracted (CBOR-specific, no JSON equivalent)
- .within, .and: Type intersection (limited support, base type used)

Not Implemented:
- CDDL sockets ($name, $$name): Extensibility points (parser limitation)
- .ne operator: No direct JSON Structure equivalent

Notes on Type Mapping:
- CBOR-specific types (bstr, tstr) map to JSON equivalents (bytes, string)
- CBOR tags are unwrapped to their underlying types
- Integer types (uint, nint, int) all map to int64 for JSON compatibility
- Float types map to float (float16/32) or double (float64)
"""

import json
import os
from typing import Any, Dict, List, Optional, Set, Tuple, Union
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
        self.generic_types: Dict[str, Dict[str, Any]] = {}  # Maps type name to generic info
        self.current_generic_bindings: Dict[str, Dict[str, Any]] = {}  # Current type parameter bindings
        self.generic_template_names: Set[str] = set()  # Names of generic templates (not concrete types)

    def _extract_description(self, node: Any) -> Optional[str]:
        """
        Extract description from comments attached to a node.
        
        CDDL comments start with ';' and are attached to AST nodes by the parser.
        This method extracts and cleans up those comments to use as descriptions.
        
        Args:
            node: Any AST node that might have comments attached
            
        Returns:
            A cleaned description string, or None if no comments
        """
        if not hasattr(node, 'comments') or not node.comments:
            return None
        
        # Extract comment text from each comment token
        comment_lines = []
        for comment in node.comments:
            if hasattr(comment, 'literal'):
                # Remove the leading semicolon and whitespace
                text = comment.literal.strip()
                if text.startswith(';'):
                    text = text[1:].strip()
                if text:
                    comment_lines.append(text)
        
        if not comment_lines:
            return None
        
        # Join multiple comment lines with spaces
        return ' '.join(comment_lines)

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
        self.generic_types.clear()
        self.current_generic_bindings.clear()
        self.generic_template_names.clear()
        
        # First pass: collect all type names and generic definitions
        for rule in ast.rules:
            if hasattr(rule, 'getChildren'):
                children = rule.getChildren()
                if len(children) >= 2:
                    typename_node = children[0]
                    if hasattr(typename_node, 'name'):
                        self.type_registry[typename_node.name] = {}
                        # Check for generic parameters
                        generic_params = self._extract_generic_parameters(typename_node)
                        if generic_params:
                            self.generic_types[typename_node.name] = {
                                'params': generic_params,
                                'type_node': children[1] if len(children) > 1 else None
                            }
                            # Mark this as a generic template (not a concrete type)
                            self.generic_template_names.add(avro_name(typename_node.name))
        
        # Second pass: process all rules
        for rule in ast.rules:
            self._process_rule(rule)
        
        # Build the output structure
        structure_schema: Dict[str, Any] = {
            "$schema": "https://json-structure.org/meta/extended/v0/#",
            "$id": f"https://{self.root_namespace.replace('.', '/')}/schema.json"
        }
        
        # Add definitions if any (excluding generic templates)
        concrete_definitions = {
            name: defn for name, defn in self.definitions.items()
            if name not in self.generic_template_names
        }
        if concrete_definitions:
            structure_schema['definitions'] = concrete_definitions
        
        # If there's a root type (first rule that's not a generic template), add it to the schema
        if ast.rules and concrete_definitions:
            first_rule_name = self._get_rule_name(ast.rules[0])
            # Skip generic templates to find first concrete type
            for rule in ast.rules:
                first_rule_name = self._get_rule_name(rule)
                if first_rule_name and first_rule_name not in self.generic_template_names:
                    break
            if first_rule_name and first_rule_name in concrete_definitions:
                root_def = concrete_definitions[first_rule_name]
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
        
        # Extract description from comments on the typename
        description = self._extract_description(typename_node)
        
        # Convert the type
        structure_type = self._convert_type(type_node, rule_name)
        
        if structure_type:
            if isinstance(structure_type, dict):
                structure_type['name'] = rule_name
                # Add description if present
                if description:
                    structure_type['description'] = description
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
        
        # Check for unwrap operator (~)
        has_unwrap = self._has_unwrap_operator(typename_node)
        
        # Check if this is a type parameter that's currently bound
        if type_name in self.current_generic_bindings:
            result = dict(self.current_generic_bindings[type_name])
            if has_unwrap:
                result = self._apply_unwrap(result)
            return result
        
        # Check if it's a primitive type
        if type_name in CDDL_PRIMITIVE_TYPES:
            return dict(CDDL_PRIMITIVE_TYPES[type_name])
        
        # Check for generic arguments
        generic_args = self._extract_generic_arguments(typename_node)
        
        # Check if this is a reference to a generic type with arguments
        if generic_args and type_name in self.generic_types:
            result = self._instantiate_generic_type(type_name, generic_args)
            if has_unwrap:
                result = self._apply_unwrap(result)
            return result
        
        # Check if it's a reference to another type
        normalized_name = avro_name(type_name)
        result: Dict[str, Any]
        if type_name in self.type_registry or normalized_name in self.definitions:
            result = {'$ref': f'#/definitions/{normalized_name}'}
        else:
            # Unknown type - treat as reference
            result = {'$ref': f'#/definitions/{normalized_name}'}
        
        if has_unwrap:
            result = self._apply_unwrap(result)
        return result
    
    def _has_unwrap_operator(self, typename_node: Any) -> bool:
        """Check if a typename node has the unwrap operator (~)."""
        if hasattr(typename_node, 'getChildren'):
            for child in typename_node.getChildren():
                if hasattr(child, 'kind') and 'TILDE' in str(child.kind):
                    return True
        return False
    
    def _apply_unwrap(self, type_def: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply the unwrap operator (~) to a type.
        
        The unwrap operator extracts the content from a group or array.
        In JSON Structure, we mark this with a special annotation.
        """
        # For referenced types, we need to resolve and unwrap
        if '$ref' in type_def:
            ref_name = type_def['$ref'].split('/')[-1]
            if ref_name in self.definitions:
                resolved = self.definitions[ref_name]
                # If it's an object, return its properties inline
                if resolved.get('type') == 'object' and 'properties' in resolved:
                    return dict(resolved)
                # If it's an array, return items type
                if resolved.get('type') == 'array' and 'items' in resolved:
                    return resolved['items']
        
        # For inline types, just return as-is (unwrap is contextual)
        return type_def
    
    def _extract_generic_parameters(self, typename_node: Any) -> List[str]:
        """Extract generic type parameters from a typename node (e.g., optional<T> -> ['T'])."""
        params = []
        if hasattr(typename_node, 'getChildren'):
            for child in typename_node.getChildren():
                if type(child).__name__ == 'GenericParameters':
                    for pc in child.getChildren():
                        if type(pc).__name__ == 'Typename' and hasattr(pc, 'name'):
                            params.append(pc.name)
        return params
    
    def _extract_generic_arguments(self, typename_node: Any) -> List[Any]:
        """Extract generic type arguments from a typename node.
        
        Returns AST nodes for the arguments so they can be properly converted
        with current bindings in _convert_typename.
        """
        args = []
        if hasattr(typename_node, 'getChildren'):
            for child in typename_node.getChildren():
                if type(child).__name__ == 'GenericArguments':
                    for ac in child.getChildren():
                        # Return the AST node itself for proper conversion
                        args.append(ac)
        return args
    
    def _resolve_generic_argument(self, arg_node: Any) -> Dict[str, Any]:
        """Resolve a generic argument node to a JSON Structure type."""
        if type(arg_node).__name__ == 'Typename':
            if hasattr(arg_node, 'name'):
                arg_name = arg_node.name
                # Check if it's a bound type parameter
                if arg_name in self.current_generic_bindings:
                    return dict(self.current_generic_bindings[arg_name])
                # Check for primitives
                if arg_name in CDDL_PRIMITIVE_TYPES:
                    return dict(CDDL_PRIMITIVE_TYPES[arg_name])
                # Otherwise it's a reference to a defined type
                normalized_name = avro_name(arg_name)
                return {'$ref': f'#/definitions/{normalized_name}'}
        # Fallback: try to convert it
        return self._convert_type(arg_node, '')
    
    def _instantiate_generic_type(self, type_name: str, type_arg_nodes: List[Any]) -> Dict[str, Any]:
        """Instantiate a generic type with concrete type arguments.
        
        Args:
            type_name: Name of the generic type (e.g., 'optional', 'pair')
            type_arg_nodes: AST nodes for the type arguments
        """
        generic_info = self.generic_types.get(type_name)
        if not generic_info:
            return {'type': 'any'}
        
        params = generic_info.get('params', [])
        type_node = generic_info.get('type_node')
        
        if not type_node or len(params) != len(type_arg_nodes):
            # Parameter count mismatch - return reference
            normalized_name = avro_name(type_name)
            return {'$ref': f'#/definitions/{normalized_name}'}
        
        # Resolve type arguments with current bindings before creating new bindings
        resolved_args = [self._resolve_generic_argument(arg) for arg in type_arg_nodes]
        
        # Create bindings for type parameters
        old_bindings = self.current_generic_bindings.copy()
        for param, arg in zip(params, resolved_args):
            self.current_generic_bindings[param] = arg
        
        try:
            # Convert the type with the current bindings
            result = self._convert_type(type_node, type_name)
            return result
        finally:
            # Restore previous bindings
            self.current_generic_bindings = old_bindings

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
        
        # Add description if present in member_key or occurrence indicator
        if isinstance(prop_schema, dict):
            description = member_key.get('description') or (occurrence_indicator.get('description') if occurrence_indicator else None)
            if description:
                prop_schema['description'] = description
        
        properties[normalized_name] = prop_schema

    def _parse_occurrence(self, occurrence: Occurrence) -> Dict[str, Any]:
        """Parse an Occurrence node."""
        result: Dict[str, Any] = {}
        
        if not hasattr(occurrence, 'getChildren'):
            return {'optional': True}
        
        # Check for comments on the occurrence tokens
        if hasattr(occurrence, 'tokens') and occurrence.tokens:
            for token in occurrence.tokens:
                if hasattr(token, 'comments') and token.comments:
                    description = self._extract_description_from_comments(token.comments)
                    if description:
                        result['description'] = description
                        break
            
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
        
        if not result or ('optional' not in result and 'array' not in result):
            result['optional'] = True
            
        return result
    
    def _extract_description_from_comments(self, comments: List[Any]) -> Optional[str]:
        """Extract description from a list of comment tokens."""
        if not comments:
            return None
        
        comment_lines = []
        for comment in comments:
            if hasattr(comment, 'literal'):
                text = comment.literal.strip()
                if text.startswith(';'):
                    text = text[1:].strip()
                if text:
                    comment_lines.append(text)
        
        return ' '.join(comment_lines) if comment_lines else None

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
                        # Extract description from comments on the typename
                        description = self._extract_description(child)
                        if description:
                            result['description'] = description
            elif child_type == 'Value':
                # Could be string key or integer key
                if hasattr(child, 'value'):
                    value = child.value
                    # Remove quotes if present (string key)
                    if value.startswith('"') and value.endswith('"'):
                        value = value[1:-1]
                        result['name'] = avro_name(value)
                        result['original_name'] = value
                    else:
                        # Try to parse as integer key
                        try:
                            int_key = int(value)
                            result['name'] = f'_{value}'
                            result['original_name'] = value
                            result['integer_key'] = int_key
                        except ValueError:
                            result['name'] = avro_name(value)
                            result['original_name'] = value
        
        return result if result else None

    def _convert_array(self, array_node: Array, context_name: str = '') -> Dict[str, Any]:
        """Convert an Array node to JSON Structure array or tuple."""
        items_types: List[Dict[str, Any]] = []
        is_tuple = False
        
        if hasattr(array_node, 'getChildren'):
            for child in array_node.getChildren():
                child_type = type(child).__name__
                if child_type == 'GroupChoice':
                    # Get the items types from the group
                    items_types, is_tuple = self._get_array_items_types(child, context_name)
                elif child_type in ('Type', 'Typename'):
                    items_types = [self._convert_type(child, context_name)]
        
        if is_tuple and len(items_types) > 1:
            # Fixed-length array with different types = tuple
            # Use the JSON Structure tuple format
            properties: Dict[str, Any] = {}
            tuple_order: List[str] = []
            for idx, item_type in enumerate(items_types):
                prop_name = f"_{idx}"
                properties[prop_name] = item_type
                tuple_order.append(prop_name)
            return {
                'type': 'tuple',
                'properties': properties,
                'tuple': tuple_order
            }
        elif items_types:
            # Regular array
            if len(items_types) == 1:
                items_type = items_types[0]
            else:
                # Multiple types that aren't a tuple - use union for items
                items_type = {'type': items_types}
            return {
                'type': 'array',
                'items': items_type
            }
        else:
            return {
                'type': 'array',
                'items': {'type': 'any'}
            }

    def _get_array_items_types(self, group_choice: GroupChoice, context_name: str) -> Tuple[List[Dict[str, Any]], bool]:
        """Extract items types from a GroupChoice in an array context.
        
        Returns:
            Tuple of (list of item types, is_tuple flag)
            is_tuple is True if this looks like a fixed-size tuple definition
        """
        if not hasattr(group_choice, 'getChildren'):
            return [{'type': 'any'}], False
            
        children = group_choice.getChildren()
        if not children:
            return [{'type': 'any'}], False
        
        # Collect all types from group entries
        types = []
        all_entries_are_single = True
        
        for child in children:
            child_type = type(child).__name__
            if child_type == 'GroupEntry':
                entry_type = self._get_group_entry_type(child, context_name)
                if entry_type:
                    types.append(entry_type)
                # Check if entry has occurrence markers (*, +, ?)
                if self._has_occurrence_marker(child):
                    all_entries_are_single = False
            elif child_type in ('Type', 'Typename'):
                types.append(self._convert_type(child, context_name))
        
        # It's a tuple if we have multiple distinct types AND no occurrence markers
        is_tuple = len(types) > 1 and all_entries_are_single
        
        return types, is_tuple
    
    def _has_occurrence_marker(self, entry: GroupEntry) -> bool:
        """Check if a GroupEntry has occurrence markers (*, +, ?)."""
        if hasattr(entry, 'getChildren'):
            for child in entry.getChildren():
                if type(child).__name__ == 'Occurrence':
                    return True
        return False

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
        # Check if this is an integer-keyed group (should become a tuple)
        int_key_entries = self._extract_integer_key_entries(group_node)
        if int_key_entries:
            return self._convert_to_tuple(int_key_entries, context_name)
        
        # Otherwise, treat as an object
        return self._convert_map_like(group_node, context_name)
    
    def _extract_integer_key_entries(self, node: Any) -> Optional[List[Tuple[int, bool, Any]]]:
        """
        Extract integer-keyed entries from a group.
        Returns list of (key, is_optional, type_node) tuples, or None if not integer-keyed.
        """
        entries: List[Tuple[int, bool, Any]] = []
        
        if not hasattr(node, 'getChildren'):
            return None
        
        for child in node.getChildren():
            if type(child).__name__ == 'GroupChoice':
                result = self._extract_int_keys_from_group_choice(child)
                if result is None:
                    return None  # Mixed keys, not a pure integer-keyed group
                entries.extend(result)
        
        if not entries:
            return None
        
        # Sort by key and check for sequential keys starting from 1
        entries.sort(key=lambda x: x[0])
        
        return entries
    
    def _extract_int_keys_from_group_choice(self, group_choice: Any) -> Optional[List[Tuple[int, bool, Any]]]:
        """Extract integer keys from a GroupChoice node."""
        entries: List[Tuple[int, bool, Any]] = []
        
        if not hasattr(group_choice, 'getChildren'):
            return None
        
        for child in group_choice.getChildren():
            if type(child).__name__ == 'GroupEntry':
                result = self._extract_int_key_from_entry(child)
                if result is None:
                    return None  # Not an integer key
                entries.append(result)
            elif type(child).__name__ == 'GroupChoice':
                nested = self._extract_int_keys_from_group_choice(child)
                if nested is None:
                    return None
                entries.extend(nested)
        
        return entries
    
    def _extract_int_key_from_entry(self, entry: Any) -> Optional[Tuple[int, bool, Any]]:
        """Extract integer key, optionality, and type from a GroupEntry."""
        if not hasattr(entry, 'getChildren'):
            return None
        
        children = entry.getChildren()
        is_optional = False
        int_key = None
        type_node = None
        
        for child in children:
            child_type = type(child).__name__
            if child_type == 'Occurrence':
                # Check for optional marker
                is_optional = self._is_optional_occurrence(child)
            elif child_type == 'Memberkey':
                int_key = self._get_integer_key(child)
                if int_key is None:
                    return None  # Not an integer key
            elif child_type in ('Type', 'Typename', 'Map', 'Array', 'Group', 'Value'):
                type_node = child
        
        if int_key is not None and type_node is not None:
            return (int_key, is_optional, type_node)
        
        return None
    
    def _is_optional_occurrence(self, occurrence: Any) -> bool:
        """Check if an occurrence node indicates optional (?)."""
        # The ? marker means n=0, m=1 (zero or one)
        # Check the n attribute - if n=0, it's optional
        if hasattr(occurrence, 'n') and occurrence.n == 0:
            return True
        # Fallback: check tokens for QUEST
        if hasattr(occurrence, 'tokens'):
            for token in occurrence.tokens:
                if 'QUEST' in repr(token):
                    return True
        return False
    
    def _get_integer_key(self, memberkey: Any) -> Optional[int]:
        """Get integer key from a Memberkey node, or None if not an integer key."""
        if not hasattr(memberkey, 'getChildren'):
            return None
        
        for child in memberkey.getChildren():
            if type(child).__name__ == 'Value' and hasattr(child, 'value'):
                try:
                    return int(child.value)
                except ValueError:
                    return None
        
        return None
    
    def _convert_to_tuple(self, entries: List[Tuple[int, bool, Any]], context_name: str) -> Dict[str, Any]:
        """Convert integer-keyed entries to a tuple type per JSON Structure spec.
        
        JSON Structure tuples use:
        - 'properties': named properties with schemas
        - 'tuple': array of property names defining the order
        - All declared properties are implicitly REQUIRED
        
        For optional elements, we omit them from the 'tuple' array but keep them in properties,
        or we can include them and note that JSON Structure tuples don't support optional elements directly.
        """
        # Sort entries by key
        entries.sort(key=lambda x: x[0])
        
        # Build properties map and tuple order array
        properties: Dict[str, Any] = {}
        tuple_order: List[str] = []
        
        for key, is_optional, type_node in entries:
            # Generate property name from the integer key
            prop_name = f"_{key}"
            item_type = self._convert_type(type_node, context_name)
            
            # Store the original CDDL key as altname
            if isinstance(item_type, dict):
                item_type['altnames'] = {'cddl': str(key)}
            
            properties[prop_name] = item_type
            
            # Only include non-optional items in tuple order
            # (optional items can still be present but are not required)
            if not is_optional:
                tuple_order.append(prop_name)
        
        result: Dict[str, Any] = {
            'type': 'tuple',
            'properties': properties,
            'tuple': [f"_{e[0]}" for e in entries]  # Include all in order for the tuple layout
        }
        
        # If some items are optional, add required to indicate which are mandatory
        if tuple_order and len(tuple_order) < len(entries):
            result['required'] = tuple_order
        
        return result

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
        Convert an Operator node (type constraints like .size, .regexp, .default).
        
        Supported control operators:
        - .size: Maps to minLength/maxLength for strings, minItems/maxItems for arrays
        - .regexp: Maps to pattern (ECMAScript regex)
        - .default: Maps to default value
        - .bits, .cbor, .cborseq: Base type extracted (CBOR-specific, no JSON Structure equivalent)
        - .within, .and: Type intersection (limited support)
        
        See RFC 8610 Section 3.8 for full control operator specification.
        """
        base_type: Dict[str, Any] = {'type': 'any'}
        operator_name: Optional[str] = None
        controller_value: Any = None
        
        # Extract operator name and base type
        if hasattr(operator_node, 'name') and hasattr(operator_node.name, 'literal'):
            operator_name = operator_node.name.literal
        
        if hasattr(operator_node, 'type'):
            base_type = self._convert_typename(operator_node.type) if hasattr(operator_node.type, 'name') else {'type': 'any'}
        
        # Extract controller (constraint argument)
        if hasattr(operator_node, 'controller'):
            controller_value = self._extract_controller_value(operator_node.controller)
        
        # Apply operator-specific mappings
        if operator_name == 'size':
            base_type = self._apply_size_constraint(base_type, controller_value)
        elif operator_name == 'regexp':
            base_type = self._apply_regexp_constraint(base_type, controller_value)
        elif operator_name == 'default':
            base_type = self._apply_default_value(base_type, controller_value)
        elif operator_name == 'lt':
            # .lt (less than) -> exclusiveMaximum
            if isinstance(controller_value, (int, float)):
                base_type['exclusiveMaximum'] = controller_value
        elif operator_name == 'le':
            # .le (less or equal) -> maximum
            if isinstance(controller_value, (int, float)):
                base_type['maximum'] = controller_value
        elif operator_name == 'gt':
            # .gt (greater than) -> exclusiveMinimum
            if isinstance(controller_value, (int, float)):
                base_type['exclusiveMinimum'] = controller_value
        elif operator_name == 'ge':
            # .ge (greater or equal) -> minimum
            if isinstance(controller_value, (int, float)):
                base_type['minimum'] = controller_value
        elif operator_name == 'eq':
            # .eq (equals) -> const
            base_type['const'] = controller_value
        elif operator_name == 'ne':
            # .ne (not equals) - no direct JSON Structure equivalent, add as comment
            pass
        elif operator_name in ('within', 'and'):
            # Type intersection - limited support, just use base type
            pass
        elif operator_name in ('bits', 'cbor', 'cborseq'):
            # CBOR-specific operators - just use base type
            pass
        
        return base_type
    
    def _extract_controller_value(self, controller: Any) -> Any:
        """Extract the value from an operator's controller (argument)."""
        if controller is None:
            return None
        
        controller_type = type(controller).__name__
        
        # Handle direct Value node (e.g., .size 32, .ge 0)
        if controller_type == 'Value':
            return self._parse_value_literal(controller)
        
        # Handle direct Range node
        if controller_type == 'Range':
            return self._extract_range_values(controller)
        
        # Handle direct Typename node
        if controller_type == 'Typename' and hasattr(controller, 'name'):
            return controller.name
            
        # Handle Type node containing children (e.g., .size (1..100))
        if hasattr(controller, 'getChildren'):
            children = controller.getChildren()
            for child in children:
                child_type = type(child).__name__
                if child_type == 'Range':
                    # Extract min/max from range
                    return self._extract_range_values(child)
                elif child_type == 'Value':
                    return self._parse_value_literal(child)
                elif child_type == 'Typename':
                    if hasattr(child, 'name'):
                        return child.name
        
        return None
    
    def _extract_range_values(self, range_node: Any) -> Dict[str, Any]:
        """Extract min and max from a Range node."""
        result: Dict[str, Any] = {}
        values = []
        is_exclusive = False
        
        if hasattr(range_node, 'getChildren'):
            for child in range_node.getChildren():
                child_type = type(child).__name__
                if child_type == 'Value':
                    values.append(self._parse_value_literal(child))
                elif hasattr(child, 'kind') and 'EXCLRANGE' in str(child.kind):
                    is_exclusive = True
        
        if len(values) >= 2:
            result['min'] = values[0]
            result['max'] = values[1]
            result['exclusive'] = is_exclusive
        elif len(values) == 1:
            result['min'] = values[0]
            result['exclusive'] = is_exclusive
        
        return result
    
    def _parse_value_literal(self, value_node: Any) -> Any:
        """Parse a Value node and return the Python value."""
        if not hasattr(value_node, 'value'):
            return None
            
        value = value_node.value
        
        # String value
        if value.startswith('"') and value.endswith('"'):
            return value[1:-1]
        
        # Boolean
        if value == 'true':
            return True
        if value == 'false':
            return False
        
        # Integer
        try:
            return int(value)
        except ValueError:
            pass
        
        # Float
        try:
            return float(value)
        except ValueError:
            pass
        
        return value
    
    def _apply_size_constraint(self, base_type: Dict[str, Any], constraint: Any) -> Dict[str, Any]:
        """Apply .size constraint to a type."""
        if constraint is None:
            return base_type
        
        type_name = base_type.get('type', 'any')
        
        # Determine if this is a string or array type
        is_string_type = type_name in ('string', 'bytes')
        is_array_type = type_name == 'array'
        
        if isinstance(constraint, dict):
            # Range constraint
            min_val = constraint.get('min')
            max_val = constraint.get('max')
            
            if is_string_type:
                if min_val is not None:
                    base_type['minLength'] = min_val
                if max_val is not None:
                    base_type['maxLength'] = max_val
            elif is_array_type:
                if min_val is not None:
                    base_type['minItems'] = min_val
                if max_val is not None:
                    base_type['maxItems'] = max_val
            else:
                # For other types, might be constraining bit/byte size
                # Map to general min/max
                if min_val is not None:
                    base_type['minimum'] = min_val
                if max_val is not None:
                    base_type['maximum'] = max_val
        elif isinstance(constraint, int):
            # Exact size constraint
            if is_string_type:
                base_type['minLength'] = constraint
                base_type['maxLength'] = constraint
            elif is_array_type:
                base_type['minItems'] = constraint
                base_type['maxItems'] = constraint
        
        return base_type
    
    def _apply_regexp_constraint(self, base_type: Dict[str, Any], pattern: Any) -> Dict[str, Any]:
        """Apply .regexp constraint to a string type."""
        if pattern and isinstance(pattern, str):
            base_type['pattern'] = pattern
        return base_type
    
    def _apply_default_value(self, base_type: Dict[str, Any], default_val: Any) -> Dict[str, Any]:
        """Apply .default value to a type."""
        if default_val is not None:
            base_type['default'] = default_val
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
