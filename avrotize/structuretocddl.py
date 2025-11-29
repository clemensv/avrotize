"""
JSON Structure to CDDL (Concise Data Definition Language) converter.

CDDL is defined in RFC 8610 and is a schema language primarily used for
expressing CBOR and JSON data structures.

RFC 8610 Compliance Summary:
============================

Generated CDDL Features:
- Primitive types: uint, int, float16/32/64, tstr, bstr, bool, nil, any
- Maps/objects with member keys and values
- Arrays with homogeneous and heterogeneous (tuple) items
- Choice/union types (type1 / type2)
- Occurrence indicators: ? (optional), * (zero-or-more), + (one-or-more)
- Literal values (strings, integers, floats, booleans)
- Range constraints (min..max)
- Type references
- Groups and group composition
- Comments from descriptions
- Unwrap operator (~) for $extends references

Control Operators Generated:
- .size: From minLength/maxLength for strings, minItems/maxItems for arrays
- .regexp: From pattern validation
- .default: From default value
- .lt, .le, .gt, .ge: From minimum/maximum/exclusiveMinimum/exclusiveMaximum

Notes on Type Mapping:
- JSON Structure binary maps to bstr
- JSON Structure string maps to tstr
- Integer types map to int or uint based on constraints
- Float types map to float16/32/64 based on precision
"""

import json
import logging
import os
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from avrotize.common import avro_name


# Configure module logger
logger = logging.getLogger(__name__)

# Default indentation for CDDL output
INDENT = '  '

# Maximum recursion depth for type conversion (prevents stack overflow)
MAX_CONVERSION_DEPTH = 100


class StructureToCddlError(Exception):
    """
    Exception raised when JSON Structure to CDDL conversion fails.

    Attributes:
        message: Human-readable error description
        context: Optional context about where the error occurred
        cause: Optional underlying exception that caused this error
    """

    def __init__(self, message: str, context: Optional[str] = None,
                 cause: Optional[Exception] = None) -> None:
        self.message = message
        self.context = context
        self.cause = cause
        full_message = message
        if context:
            full_message = f"{message} (context: {context})"
        super().__init__(full_message)


class StructureToCddlCycleError(StructureToCddlError):
    """
    Exception raised when a circular type reference is detected.

    Attributes:
        cycle_path: List of type names forming the cycle
    """

    def __init__(self, cycle_path: List[str]) -> None:
        self.cycle_path = cycle_path
        cycle_str = ' -> '.join(cycle_path)
        super().__init__(f"Circular type reference detected: {cycle_str}")


# JSON Structure primitive types to CDDL type mapping
STRUCTURE_TO_CDDL_TYPES: Dict[str, str] = {
    # Integer types
    'int8': 'int',
    'uint8': 'uint',
    'int16': 'int',
    'uint16': 'uint',
    'int32': 'int',
    'uint32': 'uint',
    'int64': 'int',
    'uint64': 'uint',
    'int128': 'int',
    'uint128': 'uint',
    'integer': 'int',

    # Floating-point types
    'float': 'float32',
    'float8': 'float16',
    'float16': 'float16',
    'float32': 'float32',
    'float64': 'float64',
    'double': 'float64',
    'binary32': 'float32',
    'binary64': 'float64',
    'decimal': 'float64',  # No direct CDDL equivalent
    'number': 'float64',

    # String types
    'string': 'tstr',
    'binary': 'bstr',
    'bytes': 'bstr',

    # Boolean and null
    'boolean': 'bool',
    'null': 'nil',

    # Any type
    'any': 'any',

    # Date/time types (no direct CDDL equivalents, use tstr)
    'date': 'tstr',
    'time': 'tstr',
    'datetime': 'tstr',
    'timestamp': 'tstr',
    'duration': 'tstr',

    # Other types
    'uuid': 'tstr',
    'uri': 'tstr',
    'jsonpointer': 'tstr',
}


class StructureToCddlConverter:
    """
    Converts JSON Structure schema documents to CDDL format.
    """

    def __init__(self) -> None:
        self.definitions: Dict[str, Dict[str, Any]] = {}
        self.schema_registry: Dict[str, Dict] = {}
        self.converted_types: Set[str] = set()
        self.output_lines: List[str] = []
        self._conversion_stack: List[str] = []
        self._conversion_depth: int = 0

    def _to_cddl_name(self, name: str) -> str:
        """
        Convert a name to a valid CDDL identifier.
        
        CDDL identifiers can contain hyphens, so we prefer hyphenated names.
        If the name has altnames.cddl, use that instead.
        """
        # Replace underscores with hyphens for CDDL style
        return name.replace('_', '-')

    def _get_original_name(self, schema: Dict[str, Any], default_name: str) -> str:
        """Get the original CDDL name from altnames if available."""
        altnames = schema.get('altnames', {})
        if isinstance(altnames, dict) and 'cddl' in altnames:
            return altnames['cddl']
        return self._to_cddl_name(default_name)

    def _format_comment(self, description: Optional[str], indent_level: int = 0) -> str:
        """Format a description as a CDDL comment."""
        if not description:
            return ''
        
        indent = INDENT * indent_level
        lines = description.split('\n')
        return '\n'.join(f"{indent}; {line.strip()}" for line in lines if line.strip())

    def convert_structure_to_cddl(self, structure_content: str) -> str:
        """
        Convert JSON Structure content to CDDL format.
        
        Args:
            structure_content: The JSON Structure schema as a string
            
        Returns:
            CDDL schema as a string
        """
        schema = json.loads(structure_content)
        return self.convert_structure_schema_to_cddl(schema)

    def convert_structure_schema_to_cddl(self, schema: Dict[str, Any]) -> str:
        """
        Convert a JSON Structure schema dict to CDDL format.
        
        Args:
            schema: The JSON Structure schema as a dict
            
        Returns:
            CDDL schema as a string
        """
        # Clear state
        self.definitions = schema.get('definitions', {})
        self.schema_registry.clear()
        self.converted_types.clear()
        self.output_lines.clear()
        self._conversion_stack.clear()
        self._conversion_depth = 0

        # Add header comment
        schema_id = schema.get('$id', '')
        if schema_id:
            self.output_lines.append(f"; Generated from {schema_id}")
            self.output_lines.append("")

        # Process root type if present
        root_name = schema.get('name')
        root_type = schema.get('type')
        
        if root_name and root_type:
            # Root schema has inline type definition
            self._convert_definition(root_name, schema)

        # Process all definitions
        for def_name, def_schema in self.definitions.items():
            if def_name not in self.converted_types:
                self._convert_definition(def_name, def_schema)

        return '\n'.join(self.output_lines)

    def _convert_definition(self, name: str, schema: Dict[str, Any]) -> None:
        """
        Convert a single type definition to CDDL.
        
        Args:
            name: The name of the type
            schema: The JSON Structure schema for the type
        """
        if name in self.converted_types:
            return
        
        self.converted_types.add(name)
        
        # Get original CDDL name
        cddl_name = self._get_original_name(schema, name)
        
        # Add description as comment
        description = schema.get('description')
        if description:
            comment = self._format_comment(description)
            if comment:
                self.output_lines.append(comment)
        
        # Convert the type
        type_def = self._convert_type(schema, cddl_name)
        
        # Write the rule
        self.output_lines.append(f"{cddl_name} = {type_def}")
        self.output_lines.append("")

    def _convert_type(self, schema: Dict[str, Any], context_name: str = '') -> str:
        """
        Convert a JSON Structure type to CDDL type expression.
        
        Args:
            schema: The JSON Structure type schema
            context_name: Name context for nested types
            
        Returns:
            CDDL type expression as a string
        """
        if not isinstance(schema, dict):
            if isinstance(schema, str):
                # Simple type reference
                return self._map_primitive_type(schema)
            return 'any'

        # Check recursion depth
        self._conversion_depth += 1
        if self._conversion_depth > MAX_CONVERSION_DEPTH:
            self._conversion_depth -= 1
            logger.warning("Maximum conversion depth exceeded for context: %s", context_name)
            raise StructureToCddlError(
                f"Maximum conversion depth ({MAX_CONVERSION_DEPTH}) exceeded",
                context=context_name
            )

        try:
            # Handle $ref
            if '$ref' in schema:
                return self._convert_ref(schema['$ref'])

            # Check for enum first (can have both type and enum)
            if 'enum' in schema:
                return self._convert_enum(schema['enum'])

            type_value = schema.get('type')
            
            # Handle union types (type is a list)
            if isinstance(type_value, list):
                return self._convert_union(type_value, schema, context_name)

            # Handle specific types
            if type_value == 'object':
                return self._convert_object(schema, context_name)
            elif type_value == 'array':
                return self._convert_array(schema, context_name)
            elif type_value == 'tuple':
                return self._convert_tuple(schema, context_name)
            elif type_value == 'map':
                return self._convert_map(schema, context_name)
            elif type_value is not None:
                return self._convert_primitive(type_value, schema)
            
            # Check for const
            if 'const' in schema:
                return self._convert_const(schema['const'])

            return 'any'
        finally:
            self._conversion_depth -= 1

    def _map_primitive_type(self, type_name: str) -> str:
        """Map a JSON Structure primitive type to CDDL type."""
        return STRUCTURE_TO_CDDL_TYPES.get(type_name, type_name)

    def _convert_ref(self, ref: str) -> str:
        """Convert a $ref to a CDDL type reference."""
        if ref.startswith('#/definitions/'):
            type_name = ref[len('#/definitions/'):]
            # Get original name if available
            if type_name in self.definitions:
                return self._get_original_name(self.definitions[type_name], type_name)
            return self._to_cddl_name(type_name)
        elif ref.startswith('#/'):
            # Local reference
            return self._to_cddl_name(ref.split('/')[-1])
        else:
            # External reference - use as-is
            return self._to_cddl_name(ref)

    def _convert_union(self, types: List[Any], schema: Dict[str, Any], context_name: str) -> str:
        """Convert a union type to CDDL choice."""
        cddl_types = []
        for t in types:
            if isinstance(t, str):
                cddl_types.append(self._map_primitive_type(t))
            elif isinstance(t, dict):
                cddl_types.append(self._convert_type(t, context_name))
            else:
                cddl_types.append(str(t))
        
        return ' / '.join(cddl_types)

    def _convert_object(self, schema: Dict[str, Any], context_name: str) -> str:
        """Convert an object type to CDDL map."""
        properties = schema.get('properties', {})
        required = set(schema.get('required', []))
        extends = schema.get('$extends')
        
        if not properties and not extends:
            # Empty object
            return '{ }'

        lines = ['{']
        
        # Handle $extends (unwrap operator in CDDL)
        if extends:
            extends_refs = extends if isinstance(extends, list) else [extends]
            for ext_ref in extends_refs:
                if isinstance(ext_ref, str):
                    ref_name = self._convert_ref(ext_ref)
                elif isinstance(ext_ref, dict) and '$ref' in ext_ref:
                    ref_name = self._convert_ref(ext_ref['$ref'])
                else:
                    continue
                lines.append(f"{INDENT}~{ref_name},")

        # Convert properties
        prop_lines = []
        for prop_name, prop_schema in properties.items():
            original_name = self._get_original_name(prop_schema, prop_name) if isinstance(prop_schema, dict) else self._to_cddl_name(prop_name)
            prop_type = self._convert_type(prop_schema, f"{context_name}.{prop_name}")
            
            # Add optional marker if not required
            optional_marker = '' if prop_name in required else '? '
            
            # Add constraints
            constraints = self._get_constraints(prop_schema) if isinstance(prop_schema, dict) else ''
            
            prop_lines.append(f"{INDENT}{optional_marker}{original_name}: {prop_type}{constraints}")

        lines.extend([f"{line}," for line in prop_lines[:-1]] if len(prop_lines) > 1 else [])
        if prop_lines:
            lines.append(f"{INDENT}{prop_lines[-1].strip()}" if len(prop_lines) == 1 else prop_lines[-1])
        
        lines.append('}')
        
        return '\n'.join(lines)

    def _convert_array(self, schema: Dict[str, Any], context_name: str) -> str:
        """Convert an array type to CDDL array."""
        items = schema.get('items', {'type': 'any'})
        items_type = self._convert_type(items, f"{context_name}[]")
        
        # Get cardinality constraints
        min_items = schema.get('minItems')
        max_items = schema.get('maxItems')
        
        if min_items is not None and max_items is not None:
            if min_items == max_items:
                return f"[{min_items}*{min_items} {items_type}]"
            else:
                return f"[{min_items}*{max_items} {items_type}]"
        elif min_items is not None and min_items > 0:
            if min_items == 1:
                return f"[+ {items_type}]"
            return f"[{min_items}* {items_type}]"
        elif max_items is not None:
            return f"[*{max_items} {items_type}]"
        else:
            return f"[* {items_type}]"

    def _convert_tuple(self, schema: Dict[str, Any], context_name: str) -> str:
        """Convert a tuple type to CDDL array with positional elements."""
        tuple_order = schema.get('tuple', [])
        properties = schema.get('properties', {})
        
        if not tuple_order:
            return '[ ]'

        elements = []
        for idx, prop_name in enumerate(tuple_order):
            prop_schema = properties.get(prop_name, {'type': 'any'})
            elem_type = self._convert_type(prop_schema, f"{context_name}[{idx}]")
            elements.append(elem_type)
        
        return f"[{', '.join(elements)}]"

    def _convert_map(self, schema: Dict[str, Any], context_name: str) -> str:
        """Convert a map type to CDDL map with computed key."""
        keys = schema.get('keys', {'type': 'string'})
        values = schema.get('values', {'type': 'any'})
        
        key_type = self._convert_type(keys, f"{context_name}.keys")
        value_type = self._convert_type(values, f"{context_name}.values")
        
        return f"{{ * {key_type} => {value_type} }}"

    def _convert_primitive(self, type_name: str, schema: Dict[str, Any]) -> str:
        """Convert a primitive type with optional constraints."""
        cddl_type = self._map_primitive_type(type_name)
        constraints = self._get_constraints(schema)
        return f"{cddl_type}{constraints}"

    def _get_constraints(self, schema: Dict[str, Any]) -> str:
        """Get CDDL control operators for constraints."""
        constraints = []
        
        # String constraints
        if 'minLength' in schema or 'maxLength' in schema:
            min_len = schema.get('minLength', 0)
            max_len = schema.get('maxLength')
            if min_len == max_len and max_len is not None:
                constraints.append(f".size {max_len}")
            elif max_len is not None:
                constraints.append(f".size ({min_len}..{max_len})")
            elif min_len > 0:
                constraints.append(f".size ({min_len}..)")
        
        # Pattern constraint
        if 'pattern' in schema:
            pattern = schema['pattern'].replace('"', '\\"')
            constraints.append(f'.regexp "{pattern}"')
        
        # Numeric constraints
        if 'minimum' in schema:
            constraints.append(f".ge {schema['minimum']}")
        if 'maximum' in schema:
            constraints.append(f".le {schema['maximum']}")
        if 'exclusiveMinimum' in schema:
            constraints.append(f".gt {schema['exclusiveMinimum']}")
        if 'exclusiveMaximum' in schema:
            constraints.append(f".lt {schema['exclusiveMaximum']}")
        
        # Default value
        if 'default' in schema:
            default = schema['default']
            if isinstance(default, str):
                constraints.append(f'.default "{default}"')
            elif isinstance(default, bool):
                constraints.append(f".default {'true' if default else 'false'}")
            elif default is None:
                constraints.append(".default nil")
            else:
                constraints.append(f".default {default}")
        
        # Const value (use .eq)
        if 'const' in schema:
            const = schema['const']
            if isinstance(const, str):
                constraints.append(f'.eq "{const}"')
            elif isinstance(const, bool):
                constraints.append(f".eq {'true' if const else 'false'}")
            elif const is None:
                constraints.append(".eq nil")
            else:
                constraints.append(f".eq {const}")

        return ' '.join(constraints)

    def _convert_const(self, value: Any) -> str:
        """Convert a const value to CDDL literal."""
        if isinstance(value, str):
            return f'"{value}"'
        elif isinstance(value, bool):
            return 'true' if value else 'false'
        elif value is None:
            return 'nil'
        elif isinstance(value, (int, float)):
            return str(value)
        else:
            return f'"{value}"'

    def _convert_enum(self, values: List[Any]) -> str:
        """Convert an enum to CDDL choice of literals."""
        cddl_values = [self._convert_const(v) for v in values]
        return ' / '.join(cddl_values)


def convert_structure_to_cddl(structure_content: str) -> str:
    """
    Convert JSON Structure content to CDDL.
    
    Args:
        structure_content: JSON Structure schema as a string
        
    Returns:
        CDDL schema as a string
    """
    converter = StructureToCddlConverter()
    return converter.convert_structure_to_cddl(structure_content)


def convert_structure_to_cddl_files(
    structure_schema_path: str,
    cddl_file_path: Optional[str] = None
) -> str:
    """
    Convert a JSON Structure schema file to CDDL.
    
    Args:
        structure_schema_path: Path to the JSON Structure schema file
        cddl_file_path: Optional path for the output CDDL file
        
    Returns:
        CDDL schema as a string
    """
    with open(structure_schema_path, 'r', encoding='utf-8') as f:
        structure_content = f.read()
    
    cddl_content = convert_structure_to_cddl(structure_content)
    
    if cddl_file_path:
        os.makedirs(os.path.dirname(cddl_file_path) or '.', exist_ok=True)
        with open(cddl_file_path, 'w', encoding='utf-8') as f:
            f.write(cddl_content)
    
    return cddl_content
