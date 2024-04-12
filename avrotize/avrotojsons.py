import copy
import json
from typing import Dict, Any, Union, List
from avrotize.common import build_tree_hash_list, group_by_hash, is_generic_json_type, NodeHashReference
from functools import reduce
import jsonpath_ng 

class AvroToJsonSchemaConverter:
    
    def __init__(self, naming_mode: str = 'snake') -> None:
        self.naming_mode = naming_mode
        self.defined_types: Dict[str, Any] = {}
        self.common_namespace = ''

    def find_common_namespace(self, namespaces: List[str]) -> str:
        """
        Find the common namespace prefix from a list of namespaces.
        """
        if not namespaces:
            return ''

        def common_prefix(a, b):
            prefix = ''
            for a_char, b_char in zip(a.split('.'), b.split('.')):
                if a_char == b_char:
                    prefix += a_char + '.'
                else:
                    break
            return prefix.rstrip('.')

        return reduce(common_prefix, namespaces)

    def update_common_namespace(self, namespace: str) -> None:
        """
        Update the common namespace based on the provided namespace.
        """
        if not self.common_namespace:
            self.common_namespace = namespace
        else:
            self.common_namespace = self.find_common_namespace([self.common_namespace, namespace])

    def get_definition_ref(self, name: str) -> str:
        """
        Construct the reference string based on the namespace and name.
        """
        
        if '.' in name:
            namespace, name = name.rsplit('.', 1)
        else:
            namespace = self.common_namespace
            
        if not self.common_namespace:
            return f"#/definitions/{name}"

        # Remove the common namespace and replace '.' with '/'
        namespace_suffix = namespace[len(self.common_namespace):].lstrip('.')
        path = namespace_suffix.replace('.', '/') if namespace_suffix else ''
        ref = f"#/definitions/{path}/{name}" if path else f"#/definitions/{name}"
        return ref
    
    def get_qualified_name(self, avro_type: Dict[str, Any]) -> str:
        """
        Construct the qualified name based on the namespace and name.
        """
        return avro_type['name'] if 'namespace' not in avro_type else f"{avro_type['namespace']}.{avro_type['name']}"

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
                else:
                    return self.avro_primitive_to_json_type(avro_type)
            else:
                if isinstance(avro_type, dict):
                    raise ValueError(f"Avro schema contains unexpected construct {avro_type}")
                return self.avro_primitive_to_json_type(avro_type)
            
        mapping = {
            'null': {'type': 'null'},
            'boolean': {'type': 'boolean'},
            'int': {'type': 'integer', 'format': 'int32'},
            'long': {'type': 'integer', 'format': 'int64'},
            'float': {'type': 'number', 'format': 'float'},
            'double': {'type': 'number', 'format': 'double'},
            'bytes': {'type': 'string', 'contentEncoding': 'base64'},
            'string': {'type': 'string'},
            'fixed': {'type': 'string'}  # Could specify length in a format or a separate attribute
        }
        type_ref = mapping.get(avro_type, '')  # Defaulting to string type for any unknown types
        if not type_ref:
            raise ValueError(f"Avro schema contains unexpected type {avro_type}")
        return type_ref


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
            union_types = [self.convert_reference(t) if isinstance(t,str) and t in self.defined_types else self.avro_primitive_to_json_type(t)
                            if isinstance(t, str) else self.parse_avro_schema(t)
                                for t in non_null_types]
            return {
                'oneOf': union_types
            }

    def parse_avro_schema(self, avro_schema: Dict[str, Any] | List[Dict[str, Any]| str] | str, is_root = False) -> Dict[str, Any] | List[Dict[str, Any]| str] | str:
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
            if is_generic_json_type(union):
                return { "type": "object" }
            else:
                return union
        elif isinstance(avro_schema, dict):
            if 'namespace' in avro_schema:
                namespace = avro_schema['namespace']
                self.update_common_namespace(namespace)
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
                return self.convert_reference(avro_schema)
            else:
                # Nested type or a direct type definition
                return self.parse_avro_schema(avro_schema['type'])
        elif isinstance(avro_schema, str):
            # Primitive type or a reference to a defined type
            if avro_schema in self.defined_types:
                return self.convert_reference(avro_schema)
            elif '.' in avro_schema:
                raise ValueError(f"Unknown type reference {avro_schema}")
            else:
                return self.avro_primitive_to_json_type(avro_schema)

    def convert_reference(self, avro_schema: Dict[str, Any] | str) -> Dict[str, Any]:
        """
        Convert a reference to a defined type to a JSON schema object with a reference to the definition.
        """
        key = avro_schema['type'] if isinstance(avro_schema, dict) else avro_schema
        json_type = self.defined_types[key]
        if 'enum' in json_type:
            return copy.deepcopy(json_type)
        else:
            return {"$ref": self.get_definition_ref(key)}
    
    def convert_record(self, avro_schema: Dict[str, Any], is_root=False) -> Dict[str, Any]:
        """
        Convert an Avro record type to a JSON schema object, handling nested types and type definitions.
        """
        record_name = self.convert_name(avro_schema['name'])
        properties = {}
        required = []
        
        json_schema: Dict[str, Any] = {
            "type": "object",
            "title": record_name
        }        
        if not is_root:
            self.defined_types[self.get_qualified_name(avro_schema)] = json_schema
        
        for field in avro_schema['fields']:
            field_name = self.convert_name(field['name'])
            prop = self.parse_avro_schema(field['type'])
            if 'doc' in field:
                if isinstance(prop, dict):
                    prop['description'] = field['doc']
                elif isinstance(prop, list) or isinstance(prop, str):
                    prop = {
                        'allOf': [
                            prop,
                            {'description': field['doc']}
                        ]}
            properties[field_name] = prop
            if not self.is_nullable(field['type']):
                required.append(field_name)

        if 'doc' in avro_schema:
            json_schema['description'] = avro_schema['doc']
        if properties:
            json_schema['properties'] = properties
        
        if required:
            json_schema['required'] = required
            
        if not is_root:
            return {"$ref": self.get_definition_ref(self.get_qualified_name(avro_schema))}
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
            self.defined_types[self.get_qualified_name(avro_schema)] = json_schema
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

    def convert(self, avro_schema: Dict[str, Any] | List[Dict[str, Any]| str] | str) -> Dict[str, Any] | List[Dict[str, Any]| str] | str:
        """
        Convert the root Avro schema to a JSON schema.
        """
        json_schema: Dict[str, Any] | List[Dict[str, Any]| str] | str = self.parse_avro_schema(avro_schema, is_root = True)
        
        if self.defined_types and isinstance(json_schema, dict):
            for name, definition in self.defined_types.items():
                if isinstance(definition, dict) and 'enum' in definition:
                    # enums are inlined
                    continue
                current_level = json_schema.setdefault('definitions', {})
                if '.' in name:
                    definition_namespace, definition_name = name.rsplit('.',1)
                    if not self.common_namespace or (self.common_namespace and definition_namespace == self.common_namespace):
                        definition_namespace = ''
                    else:
                        definition_namespace = definition_namespace[len(self.common_namespace):].lstrip('.')
                    # Split the definition_namespace into path segments
                    path_segments = definition_namespace.split('.')
                    if definition_namespace and len(path_segments) > 0:
                        # Traverse through all but the last segment, creating nested dictionaries as needed
                        for segment in path_segments:
                            # If the segment does not exist, create a new dictionary at that level
                            if segment not in current_level:
                                current_level[segment] = {}
                            # Move deeper into the nested structure
                            current_level = current_level[segment]
                else:
                    definition_name = name
                current_level[definition_name] = copy.deepcopy(definition)
                
        return json_schema

def compact_tree(json_schema):
    shared_def_counter = 1
    ignored_hashes = []
    while True:
        thl = build_tree_hash_list(json_schema)
        ghl = group_by_hash(thl)
        if len(ghl) == 0:
            return
        # sort ghl by the count in of the first item in each group
        ghl = dict(sorted(ghl.items(), key=lambda item: -item[1][0].count))
        repeat = True
        while repeat:
            repeat = False
            first_group_key = next((key for key in ghl.keys() if key not in ignored_hashes), None)
            if first_group_key is None:
                return
            ghl_top_item_entries = ghl[first_group_key]
            # sort the items by the shortest .path value
            ghl_top_item_entries = sorted(ghl_top_item_entries, key=lambda item: len(item.path.split('.')))
            top_item_entry: NodeHashReference = ghl_top_item_entries[0]
            top_item_path_segments = top_item_entry.path.split('.')
            if top_item_path_segments[1] == 'definitions' and len(top_item_path_segments) == 3:
                # the top item sits right under definitions, we will merge into that one
                def_key = top_item_path_segments[2]
                ghl_top_item_entries.remove(top_item_entry)
            elif ((top_item_path_segments[-1] == 'options' and top_item_path_segments[-2] == 'properties' and len(top_item_path_segments) > 4) and 'oneOf' in top_item_entry.value):
                # the first case is likely a union we created in j2a that we had to create a top-level item for. We will undo that here.
                json_item = json_schema
                def_key = ''
                for seg in top_item_path_segments[1:-2]:
                    def_key += '/' + seg if def_key else seg
                    json_item = json_item[seg]
                json_item.clear()
                json_item.update(copy.deepcopy(top_item_entry.value))
                ghl_top_item_entries.remove(top_item_entry)    
            elif top_item_path_segments[-2] == 'properties' or top_item_path_segments[-1] == 'properties':
                # the top item is a property of an object, which means that we would create direct
                # links into that object and therefore we will drop that hash
                ignored_hashes.append(first_group_key)
                repeat = True
                continue
            else:
                # the second is indeed a proper type declaration, so we will use the first as the one all other occurrences refer to
                json_item = json_schema
                def_key = ''
                for seg in top_item_path_segments[1:]:
                    def_key += '/' + seg if def_key else seg
                ghl_top_item_entries.remove(top_item_entry)        
        
        
            for ghl_item in ghl_top_item_entries:
                node = ghl_item.value
                if isinstance(node,dict):
                    node.clear()
                    node.update({
                        '$ref': f"#/{def_key}"
                    })
            break



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
    
    compact_tree(json_schema)
    # Write the JSON schema to the output file
    with open(json_schema_file, 'w') as file:
        json.dump(json_schema, file, indent=4)

