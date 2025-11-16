"""
JSON Structure to Avro Schema Converter

Converts JSON Structure documents to Apache Avro schema format.
This is the reverse operation of avrotojstruct.py.
"""

import json
from typing import Any, Dict, List, Union, Optional


class JsonStructureToAvro:
    """
    Convert JSON Structure documents to Avro schema format.
    """

    def __init__(self) -> None:
        """Initialize the converter."""
        self.structure_doc: Optional[Dict[str, Any]] = None
        self.converted_types: Dict[str, Dict[str, Any]] = {}
        
    def convert(self, structure_schema: Dict[str, Any]) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Convert a JSON Structure document to Avro schema.
        
        Args:
            structure_schema: The JSON Structure document
            
        Returns:
            Avro schema (dict or list of dicts)
        """
        self.structure_doc = structure_schema
        self.converted_types.clear()
        
        # Check if this is an inline type (type at root) or uses $root
        root_ref = structure_schema.get('$root')
        has_inline_type = 'type' in structure_schema
        
        if has_inline_type:
            # Inline type at root - convert directly
            name = structure_schema.get('name', 'Root')
            namespace = None  # Root level doesn't have namespace
            return self._convert_type_from_schema(structure_schema, namespace, name)
        
        if not root_ref:
            raise ValueError("JSON Structure document must have either 'type' or '$root' property")
        
        # Extract definitions
        definitions = structure_schema.get('definitions', {})
        if not definitions:
            raise ValueError("JSON Structure document with $root must have definitions")
        
        # Convert all definitions first
        for def_path, def_schema in self._flatten_definitions(definitions).items():
            self._convert_definition(def_path, def_schema)
        
        # Get the root schema
        root_path = root_ref.replace('#/definitions/', '')
        root_schema = self.converted_types.get(root_path)
        
        if not root_schema:
            raise ValueError(f"Root type {root_path} not found in converted types")
        
        # Return single schema or list depending on how many types were defined
        if len(self.converted_types) == 1:
            return root_schema
        else:
            # Return all schemas as a list
            return list(self.converted_types.values())
    
    def _flatten_definitions(self, definitions: Dict[str, Any], prefix: str = '') -> Dict[str, Dict[str, Any]]:
        """
        Flatten nested definitions into a flat dictionary with paths as keys.
        
        Args:
            definitions: Nested definitions dictionary
            prefix: Current path prefix
            
        Returns:
            Flattened dictionary {path: definition_schema}
        """
        flattened = {}
        
        for key, value in definitions.items():
            path = f"{prefix}/{key}" if prefix else key
            
            if isinstance(value, dict):
                # Check if this is a type definition (has 'type' or other schema properties)
                if 'type' in value or 'oneOf' in value or 'allOf' in value:
                    flattened[path] = value
                else:
                    # It's a namespace, recurse
                    flattened.update(self._flatten_definitions(value, path))
            
        return flattened
    
    def _convert_definition(self, def_path: str, def_schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert a single type definition from JSON Structure to Avro.
        
        Args:
            def_path: The definition path (used as type name)
            def_schema: The JSON Structure type definition
            
        Returns:
            Avro schema for this type
        """
        # Parse namespace and name from path
        if '/' in def_path:
            parts = def_path.split('/')
            namespace = '.'.join(parts[:-1])
            name = parts[-1]
        else:
            namespace = None
            name = def_path
        
        avro_schema = self._convert_type_from_schema(def_schema, namespace, name)
        self.converted_types[def_path] = avro_schema
        return avro_schema
    
    def _convert_type_from_schema(self, def_schema: Dict[str, Any], namespace: Optional[str], name: str) -> Dict[str, Any]:
        """
        Convert a type definition based on its schema properties.
        
        Args:
            def_schema: The JSON Structure type definition
            namespace: The namespace for the type
            name: The name of the type
            
        Returns:
            Avro schema for this type
        """
        avro_schema: Dict[str, Any] = {}
        
        # Handle different JSON Structure types
        type_value = def_schema.get('type')
        
        if type_value == 'object':
            avro_schema = self._convert_object(def_schema, namespace, name)
        elif type_value == 'string' and 'enum' in def_schema:
            avro_schema = self._convert_enum(def_schema, namespace, name)
        elif type_value == 'binary' and 'byteLength' in def_schema:
            avro_schema = self._convert_fixed(def_schema, namespace, name)
        elif 'oneOf' in def_schema:
            avro_schema = self._convert_union(def_schema, namespace, name)
        elif type_value == 'choice':
            avro_schema = self._convert_choice(def_schema, namespace, name)
        elif type_value == 'set':
            avro_schema = self._convert_set(def_schema, namespace, name)
        elif type_value == 'tuple':
            avro_schema = self._convert_tuple(def_schema, namespace, name)
        elif type_value == 'any':
            avro_schema = self._convert_any(def_schema, namespace, name)
        elif type_value == 'array':
            # Array as top-level type needs wrapping in a record
            avro_schema = {
                'type': 'record',
                'name': name,
                'fields': [{
                    'name': 'items',
                    'type': {
                        'type': 'array',
                        'items': self._convert_type_reference(def_schema.get('items', 'string'))
                    }
                }]
            }
            if namespace:
                avro_schema['namespace'] = namespace
        elif type_value == 'map':
            # Map as top-level type needs wrapping in a record
            avro_schema = {
                'type': 'record',
                'name': name,
                'fields': [{
                    'name': 'values',
                    'type': {
                        'type': 'map',
                        'values': self._convert_type_reference(def_schema.get('values', 'string'))
                    }
                }]
            }
            if namespace:
                avro_schema['namespace'] = namespace
        else:
            # It might be a simple type alias or logical type
            avro_schema = self._convert_simple_type(def_schema, namespace, name)
        
        return avro_schema
    
    def _convert_object(self, schema: Dict[str, Any], namespace: Optional[str], name: str) -> Dict[str, Any]:
        """Convert JSON Structure object to Avro record."""
        avro_record: Dict[str, Any] = {
            'type': 'record',
            'name': name
        }
        
        if namespace:
            avro_record['namespace'] = namespace
        
        if 'description' in schema:
            avro_record['doc'] = schema['description']
        
        # Convert properties to fields
        properties = schema.get('properties', {})
        required = schema.get('required', [])
        
        fields = []
        for prop_name, prop_schema in properties.items():
            field = {
                'name': prop_name,
                'type': self._convert_type_reference(prop_schema)
            }
            
            if 'description' in prop_schema:
                field['doc'] = prop_schema['description']
            
            # Handle default values
            if 'default' in prop_schema:
                field['default'] = prop_schema['default']
            elif prop_name not in required:
                # Optional field - make it nullable with null default
                if isinstance(field['type'], list):
                    if 'null' not in field['type']:
                        field['type'] = ['null'] + field['type']
                else:
                    field['type'] = ['null', field['type']]
                field['default'] = None
            
            fields.append(field)
        
        avro_record['fields'] = fields
        return avro_record
    
    def _convert_enum(self, schema: Dict[str, Any], namespace: Optional[str], name: str) -> Dict[str, Any]:
        """Convert JSON Structure enum to Avro enum."""
        avro_enum: Dict[str, Any] = {
            'type': 'enum',
            'name': name,
            'symbols': schema['enum']
        }
        
        if namespace:
            avro_enum['namespace'] = namespace
        
        if 'description' in schema:
            avro_enum['doc'] = schema['description']
        
        if 'default' in schema:
            avro_enum['default'] = schema['default']
        
        return avro_enum
    
    def _convert_fixed(self, schema: Dict[str, Any], namespace: Optional[str], name: str) -> Dict[str, Any]:
        """Convert JSON Structure fixed-length binary to Avro fixed."""
        avro_fixed: Dict[str, Any] = {
            'type': 'fixed',
            'name': name,
            'size': schema['byteLength']
        }
        
        if namespace:
            avro_fixed['namespace'] = namespace
        
        if 'description' in schema:
            avro_fixed['doc'] = schema['description']
        
        return avro_fixed
    
    def _convert_union(self, schema: Dict[str, Any], namespace: Optional[str], name: str) -> Dict[str, Any]:
        """Convert JSON Structure oneOf to Avro union or record."""
        # Check if this is a proper union with $extends (discriminated union)
        one_of = schema.get('oneOf', [])
        
        # For now, create a simple record that can hold the union
        # TODO: Implement proper discriminated union mapping
        avro_record: Dict[str, Any] = {
            'type': 'record',
            'name': name
        }
        
        if namespace:
            avro_record['namespace'] = namespace
        
        if 'description' in schema:
            avro_record['doc'] = schema['description']
        
        # Create a union field
        union_types = [self._convert_type_reference(choice) for choice in one_of]
        
        avro_record['fields'] = [{
            'name': 'value',
            'type': union_types
        }]
        
        return avro_record
    
    def _convert_choice(self, schema: Dict[str, Any], namespace: Optional[str], name: str) -> Dict[str, Any]:
        """Convert JSON Structure choice to Avro union."""
        choices = schema.get('choices', {})
        selector = schema.get('selector')
        extends_ref = schema.get('$extends')
        
        if extends_ref and selector:
            # Inline union - create record with discriminator field
            # This is a complex case that requires resolving the base type
            avro_record: Dict[str, Any] = {
                'type': 'record',
                'name': name
            }
            
            if namespace:
                avro_record['namespace'] = namespace
            
            if 'description' in schema:
                avro_record['doc'] = schema['description']
            
            # For inline unions, create union of choice types
            union_types = []
            for choice_name, choice_schema in choices.items():
                choice_type = self._convert_type_reference(choice_schema)
                union_types.append(choice_type)
            
            # Create a single field with the union type
            avro_record['fields'] = [{
                'name': 'value',
                'type': union_types
            }]
            
            return avro_record
        else:
            # Tagged union - create a union of types
            # In Avro, this becomes a union type
            union_types = []
            for choice_name, choice_schema in choices.items():
                choice_type = self._convert_type_reference(choice_schema)
                union_types.append(choice_type)
            
            # Return a record wrapping the union
            avro_record: Dict[str, Any] = {
                'type': 'record',
                'name': name
            }
            
            if namespace:
                avro_record['namespace'] = namespace
            
            avro_record['fields'] = [{
                'name': 'value',
                'type': union_types
            }]
            
            return avro_record
    
    def _convert_set(self, schema: Dict[str, Any], namespace: Optional[str], name: str) -> Dict[str, Any]:
        """Convert JSON Structure set to Avro array (sets are represented as arrays in Avro)."""
        avro_record: Dict[str, Any] = {
            'type': 'record',
            'name': name
        }
        
        if namespace:
            avro_record['namespace'] = namespace
        
        if 'description' in schema:
            avro_record['doc'] = schema['description'] + ' (Set - unique unordered elements)'
        else:
            avro_record['doc'] = 'Set - unique unordered elements'
        
        # Sets are represented as arrays in Avro
        items_type = schema.get('items', 'string')
        avro_record['fields'] = [{
            'name': 'items',
            'type': {
                'type': 'array',
                'items': self._convert_type_reference(items_type)
            }
        }]
        
        return avro_record
    
    def _convert_tuple(self, schema: Dict[str, Any], namespace: Optional[str], name: str) -> Dict[str, Any]:
        """Convert JSON Structure tuple to Avro record with ordered fields."""
        avro_record: Dict[str, Any] = {
            'type': 'record',
            'name': name
        }
        
        if namespace:
            avro_record['namespace'] = namespace
        
        if 'description' in schema:
            avro_record['doc'] = schema['description']
        
        # Tuples have a fixed set of items with specific types
        tuple_items = schema.get('tuple', [])
        fields = []
        
        for idx, item_schema in enumerate(tuple_items):
            fields.append({
                'name': f'item{idx}',
                'type': self._convert_type_reference(item_schema)
            })
        
        avro_record['fields'] = fields
        return avro_record
    
    def _convert_any(self, schema: Dict[str, Any], namespace: Optional[str], name: str) -> Dict[str, Any]:
        """Convert JSON Structure 'any' type to Avro union of all basic types."""
        avro_record: Dict[str, Any] = {
            'type': 'record',
            'name': name
        }
        
        if namespace:
            avro_record['namespace'] = namespace
        
        if 'description' in schema:
            avro_record['doc'] = schema['description'] + ' (Any type)'
        else:
            avro_record['doc'] = 'Any type'
        
        # In Avro, 'any' can be represented as a union of all basic types
        # or as a string containing JSON
        avro_record['fields'] = [{
            'name': 'value',
            'type': ['null', 'boolean', 'int', 'long', 'float', 'double', 'string', 'bytes']
        }]
        
        return avro_record
    
    def _convert_simple_type(self, schema: Dict[str, Any], namespace: Optional[str], name: str) -> Dict[str, Any]:
        """Convert a simple type (possibly with logical type) to an Avro record with a single field."""
        type_value = schema.get('type')
        logical_type = schema.get('logicalType')
        
        # For simple types, we create a record wrapper with a 'value' field
        avro_record: Dict[str, Any] = {
            'type': 'record',
            'name': name
        }
        
        if namespace:
            avro_record['namespace'] = namespace
        
        if 'description' in schema:
            avro_record['doc'] = schema['description']
        
        # Determine the field type
        if logical_type:
            field_type = self._map_logical_type(logical_type, type_value)
        else:
            field_type = self._convert_type_reference(schema)
        
        avro_record['fields'] = [{
            'name': 'value',
            'type': field_type
        }]
        
        return avro_record
    
    def _convert_type_reference(self, schema: Union[Dict[str, Any], str]) -> Union[str, Dict[str, Any], List]:
        """
        Convert a type reference or inline type definition.
        
        Args:
            schema: Type schema or reference
            
        Returns:
            Avro type (string, dict, or list for union)
        """
        if isinstance(schema, str):
            return self._map_primitive_type(schema)
        
        if not isinstance(schema, dict):
            raise ValueError(f"Invalid type schema: {schema}")
        
        # Handle $ref
        if '$ref' in schema:
            ref = schema['$ref']
            if ref.startswith('#/definitions/'):
                ref_path = ref.replace('#/definitions/', '')
                # Convert path format back to Avro namespace.name format
                return ref_path.replace('/', '.')
            raise ValueError(f"Unsupported reference format: {ref}")
        
        # Handle inline types
        type_value = schema.get('type')
        
        # Handle union types (type is an array like ["string", "null"])
        if isinstance(type_value, list):
            return [self._map_primitive_type(t) if isinstance(t, str) else self._convert_type_reference(t) for t in type_value]
        
        # Handle choice types
        if type_value == 'choice':
            # For nested choice types, we need to convert them fully
            # Generate a unique name based on the choices
            choices = schema.get('choices', {})
            choice_name = f"Choice_{'_'.join(choices.keys())}" if choices else "Choice"
            return self._convert_choice(schema, None, choice_name)
        
        # Handle set types
        if type_value == 'set':
            # Sets are represented as arrays in Avro
            return {
                'type': 'array',
                'items': self._convert_type_reference(schema.get('items', 'string'))
            }
        
        # Handle tuple types
        if type_value == 'tuple':
            # Tuples need to be records - generate unique name
            tuple_name = f"Tuple_{len(schema.get('tuple', []))}_items"
            return self._convert_tuple(schema, None, tuple_name)
        
        # Handle any types  
        if type_value == 'any':
            # Return union of all basic types
            return ['null', 'boolean', 'int', 'long', 'float', 'double', 'string', 'bytes']
        
        if type_value == 'array':
            return {
                'type': 'array',
                'items': self._convert_type_reference(schema['items'])
            }
        
        if type_value == 'map':
            return {
                'type': 'map',
                'values': self._convert_type_reference(schema['values'])
            }
        
        # Handle logical types
        logical_type = schema.get('logicalType')
        if logical_type:
            return self._map_logical_type(logical_type, type_value)
        
        # Primitive type
        if type_value:
            return self._map_primitive_type(type_value)
        
        raise ValueError(f"Cannot convert type schema: {schema}")
    
    def _map_primitive_type(self, struct_type: str) -> str:
        """Map JSON Structure primitive type to Avro primitive type."""
        type_mapping = {
            'null': 'null',
            'boolean': 'boolean',
            # Integer types
            'int8': 'int',
            'int16': 'int',
            'int32': 'int',
            'int64': 'long',
            'uint8': 'int',
            'uint16': 'int',
            'uint32': 'long',
            'uint64': 'long',
            'int128': 'string',  # Too large for Avro numeric types
            'uint128': 'string',
            # Floating point types
            'float8': 'float',
            'float16': 'float',
            'float32': 'float',
            'float': 'float',
            'float64': 'double',
            'double': 'double',
            'number': 'double',  # Generic number → double
            'binary32': 'float',  # IEEE 754 binary32
            'binary64': 'double',  # IEEE 754 binary64
            # String and binary types
            'string': 'string',
            'binary': 'bytes',
            'bytes': 'bytes',
            # Temporal types (as strings with logical type hints)
            'date': 'string',
            'datetime': 'string',
            'time': 'string',
            'duration': 'string',
            'timestamp': 'long',  # Typically timestamp-millis
            # Other types
            'uuid': 'string',
            'uri': 'string',
            'decimal': 'bytes',  # Will be enhanced with logical type
            'jsonpointer': 'string',
        }
        
        return type_mapping.get(struct_type, struct_type)
    
    def _map_logical_type(self, logical_type: str, base_type: Optional[str]) -> Dict[str, Any]:
        """Map JSON Structure logical type to Avro logical type."""
        logical_mapping = {
            'timestampMicros': {'type': 'long', 'logicalType': 'timestamp-micros'},
            'timestampMillis': {'type': 'long', 'logicalType': 'timestamp-millis'},
            'date': {'type': 'int', 'logicalType': 'date'},
            'uuid': {'type': 'string', 'logicalType': 'uuid'},
            'decimal': {'type': 'bytes', 'logicalType': 'decimal'},
        }
        
        if logical_type in logical_mapping:
            return logical_mapping[logical_type]
        
        # Fallback to base type
        if base_type:
            return {'type': self._map_primitive_type(base_type)}
        
        return {'type': 'string'}


def convert_json_structure_to_avro(
    structure_file: str,
    avro_file: str
) -> None:
    """
    Convert a JSON Structure file to Avro schema file.
    
    Args:
        structure_file: Path to input JSON Structure file
        avro_file: Path to output Avro schema file
    """
    converter = JsonStructureToAvro()
    
    with open(structure_file, 'r', encoding='utf-8') as f:
        structure_schema = json.load(f)
    
    avro_schema = converter.convert(structure_schema)
    
    with open(avro_file, 'w', encoding='utf-8') as f:
        json.dump(avro_schema, f, indent=2)
