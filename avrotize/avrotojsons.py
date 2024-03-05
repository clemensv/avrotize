import json
from typing import Dict, Any, Union, List
from avrotize.common import is_generic_type

class AvroToJsonSchemaConverter:
    
    def __init__(self, naming_mode: str = 'snake') -> None:
        self.naming_mode = naming_mode
        self.defined_types: Dict[str, Any] = {}

    def avro_primitive_to_json_type(self, avro_type: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Map Avro primitive types to JSON types with appropriate format annotations.
        """
        json_type = {}
        if isinstance(avro_type, dict):
            avro_type = avro_type.get('type', avro_type)
            if isinstance(avro_type, dict) and 'logicalType' in avro_type:
                logical_type = avro_type['logicalType']
                if logical_type in ['date', 'timestamp-millis', 'timestamp-micros']:
                    json_type['type'] = 'string'
                    json_type['format'] = 'date-time'
                    return json_type
                elif logical_type == 'time-millis' or logical_type == 'time-micros':
                    json_type['type'] = 'string'
                    json_type['format'] = 'time'
                    return json_type
                elif logical_type == 'decimal':
                    json_type['type'] = 'number'
                    return json_type
                elif logical_type == 'uuid':
                    json_type['type'] = 'string'
                    json_type['format'] = 'uuid'
                    return json_type
        mapping = {
            'null': {'type': 'null'},
            'boolean': {'type': 'boolean'},
            'int': {'type': 'integer', 'format': 'int32'},
            'long': {'type': 'integer', 'format': 'int64'},
            'float': {'type': 'number', 'format': 'float'},
            'double': {'type': 'number', 'format': 'double'},
            'bytes': {'type': 'string', 'format': 'byte'},
            'string': {'type': 'string'},
            'fixed': {'type': 'string'}  # Could specify length in a format or a separate attribute
        }
        return mapping.get(avro_type, {'type': 'string'})  # Defaulting to string type for any unknown types


    def convert_name(self, name: str) -> str:
        """
        Convert names according to the specified naming mode.
        """
        if self.naming_mode == 'snake':
            return self.to_snake_case(name)
        elif self.naming_mode == 'camel':
            return self.to_camel_case(name)
        elif self.naming_mode == 'pascal':
            return self.to_pascal_case(name)
        return name

    @staticmethod
    def to_snake_case(name: str) -> str:
        return ''.join(['_'+c.lower() if c.isupper() else c for c in name]).lstrip('_')

    @staticmethod
    def to_camel_case(name: str) -> str:
        return ''.join(word.capitalize() if i else word for i, word in enumerate(name.split('_')))

    @staticmethod
    def to_pascal_case(name: str) -> str:
        return ''.join(word.capitalize() for word in name.split('_'))

    def is_nullable(self, avro_type: Union[str, Dict[str, Any]]) -> bool:
        """
        Check if a given Avro type is nullable.
        """
        if isinstance(avro_type, list):
            return 'null' in avro_type
        return avro_type == 'null'
    
    def handle_type_union(self, types: List[Union[str, Dict[str, Any]]]) -> Dict[str, Any] | List[Dict[str, Any]| str] | str:
        """
        Handle Avro type unions, returning a JSON schema that validates against any of the types.
        """
        non_null_types = [t for t in types if t != 'null']
        if len(non_null_types) == 1:
            # Single non-null type
            return self.parse_avro_schema(non_null_types[0])
        else:
            # Multiple non-null types
            union_types = [self.avro_primitive_to_json_type(t)
                            if isinstance(t, str) else self.parse_avro_schema(t)
                                for t in non_null_types]
            return {
                'oneOf': union_types
            }

    def parse_avro_schema(self, avro_schema: Union[Dict[str, Any], str, List[Union[str, Dict[str, Any]]]], is_root = False) -> Dict[str, Any] | List[Dict[str, Any]| str] | str:
        """
        Parse an Avro schema structure and return the corresponding JSON schema.
        """
        if isinstance(avro_schema, list):
            # Type union
            union = self.handle_type_union(avro_schema)
            if is_root:
                # all the definitions go into 'definitions'
                return {
                    "$schema": "http://json-schema.org/draft-07/schema#"
                }
            if is_generic_type(union):
                return { "type": "object" }
            else:
                return union
        elif isinstance(avro_schema, dict):
            if avro_schema['type'] == 'record':
                return self.convert_record(avro_schema, is_root)
            elif avro_schema['type'] == 'enum':
                return self.convert_enum(avro_schema, is_root)
            elif avro_schema['type'] == 'array':
                return self.convert_array(avro_schema)
            elif avro_schema['type'] == 'map':
                return self.convert_map(avro_schema)
            elif avro_schema['type'] in self.defined_types:
                # Type reference
                return {"$ref": f"#/definitions/{avro_schema['type']}"}
            else:
                # Nested type or a direct type definition
                return self.parse_avro_schema(avro_schema['type'])
        elif isinstance(avro_schema, str):
            # Primitive type or a reference to a defined type
            if avro_schema in self.defined_types:
                return {"$ref": f"#/definitions/{avro_schema}"}
            else:
                return self.avro_primitive_to_json_type(avro_schema)

    def convert_record(self, avro_schema: Dict[str, Any], is_root=False) -> Dict[str, Any]:
        """
        Convert an Avro record type to a JSON schema object, handling nested types and type definitions.
        """
        record_name = self.convert_name(avro_schema['name'])
        properties = {}
        required = []

        for field in avro_schema['fields']:
            field_name = self.convert_name(field['name'])
            properties[field_name] = self.parse_avro_schema(field['type'])
            if self.is_nullable(field['type']):
                required.append(field_name)

        json_schema = {
            "type": "object",
            "properties": properties,
            "title": record_name
        }

        if required:
            json_schema['required'] = required

        if 'doc' in avro_schema:
            json_schema['description'] = avro_schema['doc']

        # Add to defined types
        if not is_root:
            self.defined_types[record_name] = json_schema
        return json_schema

    def convert_enum(self, avro_schema: Dict[str, Any], is_root=False) -> Dict[str, Any]:
        """
        Convert an Avro enum type to a JSON schema enum, adding the definition to the schema.
        """
        enum_name = self.convert_name(avro_schema['name'])
        json_schema = {
            "type": "string",
            "enum": avro_schema['symbols'],
            "title": enum_name
        }

        if 'doc' in avro_schema:
            json_schema['description'] = avro_schema['doc']

        # Add to defined types
        if not is_root:
            self.defined_types[enum_name] = json_schema
        return json_schema

    def convert_array(self, avro_schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert an Avro array type to a JSON schema array.
        """
        return {
            "type": "array",
            "items": self.parse_avro_schema(avro_schema['items'])
        }

    def convert_map(self, avro_schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert an Avro map type to a JSON schema object with additionalProperties.
        """
        return {
            "type": "object",
            "additionalProperties": self.parse_avro_schema(avro_schema['values'])
        }

    def convert(self, avro_schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert the root Avro schema to a JSON schema.
        """
        json_schema = self.parse_avro_schema(avro_schema, is_root = True)
        
        if self.defined_types:
            json_schema['definitions'] = {}
            for name, definition in self.defined_types.items():
                json_schema['definitions'][name] = definition

        return json_schema

def convert_avro_to_json_schema(avro_schema_file: str, json_schema_file: str, naming_mode: str = 'default') -> None:
    """
    Convert an Avro schema file to a JSON schema file.

    :param avro_schema_file: The path to the input Avro schema file.
    :param json_schema_file: The path to the output JSON schema file.
    :param naming_mode: The naming mode for converting names ('snake', 'camel', 'pascal').
    """
    converter = AvroToJsonSchemaConverter(naming_mode)

    # Read the Avro schema file
    with open(avro_schema_file, 'r') as file:
        avro_schema = json.load(file)

    # Convert the Avro schema to JSON schema
    json_schema = converter.convert(avro_schema)

    # Write the JSON schema to the output file
    with open(json_schema_file, 'w') as file:
        json.dump(json_schema, file, indent=2)

