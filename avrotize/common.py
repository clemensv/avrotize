from collections import defaultdict
import os
import re
from typing import Dict, Tuple, Union, Any, List
import uuid
from jsoncomparison import NO_DIFF, Compare
import hashlib
import json    
import jinja2
from jinja2.ext import Extension
from jinja2 import Template, nodes

def avro_name(name):
    """Convert a name into an Avro name."""
    if isinstance(name, int):
        name = '_'+str(name)
    val = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    if re.match(r'^[0-9]', val):
        val = '_' + val
    return val

def avro_namespace(name):
    """Convert a name into an Avro name."""
    val = re.sub(r'[^a-zA-Z0-9_\.]', '_', name)
    if re.match(r'^[0-9]', val):
        val = '_' + val
    return val

def generic_type() -> list[str | dict]:
    simple_type_union: list[str | dict] = ["null", "boolean", "int", "long", "float", "double", "bytes", "string"]
    l2 = simple_type_union.copy()
    l2.extend([
        {
            "type": "array",
            "items": simple_type_union
        },
        {
            "type": "map",
            "values": simple_type_union
        }])
    l1 = simple_type_union.copy()
    l1.extend([
        {
            "type": "array",
            "items": l2
        },
        {
            "type": "map",
            "values": l2
        }])
    return l1

def is_generic_avro_type(avro_type: list) -> bool:
    """
    Check if the given Avro type is a generic type.

    Args:
        avro_type (Union[str, Dict[str, Any]]): The Avro type to check.

    Returns:
        bool: True if the Avro type is a generic type, False otherwise.
    """
    if isinstance(avro_type, str) or isinstance(avro_type, dict):	    
        return False
    compare_type = generic_type()
    return Compare().check(avro_type, compare_type) == NO_DIFF

def is_generic_json_type(json_type: Dict[str, Any] | List[Dict[str, Any]| str] | str)  -> bool:
    """
    Check if the given JSON type is a generic type.

    Args:
        json_type (Union[Dict[str, Any], str, List[Union[str, Dict[str, Any]]]]): The JSON type to check.

    Returns:
        bool: True if the JSON type is a generic type, False otherwise.
    """
    if isinstance(json_type, str) or isinstance(json_type, list):
        return False
    compare_type = generic_type_json()
    return Compare().check(json_type, compare_type) == NO_DIFF

def generic_type_json() -> dict:
    """
    Returns a dictionary representing a generic JSON schema for various types.

    The schema includes support for boolean, integer, number, string, array, and object types.
    Each type can have different formats such as int32, int64, float, double, and byte.

    Returns:
        dict: A dictionary representing the generic JSON schema.
    """
    return {
        "oneOf": [
            {"type": "boolean"},
            {"type": "integer", "format": "int32"},
            {"type": "integer", "format": "int64"},
            {"type": "number", "format": "float"},
            {"type": "number", "format": "double"},
            {"type": "string", "format": "byte"},
            {"type": "string"},
            {
                "type": "array",
                "items": {
                    "oneOf": [
                        {"type": "boolean"},
                        {"type": "integer", "format": "int32"},
                        {"type": "integer", "format": "int64"},
                        {"type": "number", "format": "float"},
                        {"type": "number", "format": "double"},
                        {"type": "string", "format": "byte"},
                        {"type": "string"},
                        {
                            "type": "array",
                            "items": {
                                "oneOf": [
                                    {"type": "boolean"},
                                    {"type": "integer", "format": "int32"},
                                    {"type": "integer", "format": "int64"},
                                    {"type": "number", "format": "float"},
                                    {"type": "number", "format": "double"},
                                    {"type": "string", "format": "byte"},
                                    {"type": "string"}
                                ]
                            }
                        },
                        {
                            "type": "object",
                            "additionalProperties": {
                                "oneOf": [
                                    {"type": "boolean"},
                                    {"type": "integer", "format": "int32"},
                                    {"type": "integer", "format": "int64"},
                                    {"type": "number", "format": "float"},
                                    {"type": "number", "format": "double"},
                                    {"type": "string", "format": "byte"},
                                    {"type": "string"}
                                ]
                            }
                        }
                    ]
                }
            },
            {
                "type": "object",
                "additionalProperties": {
                    "oneOf": [
                        {"type": "boolean"},
                        {"type": "integer", "format": "int32"},
                        {"type": "integer", "format": "int64"},
                        {"type": "number", "format": "float"},
                        {"type": "number", "format": "double"},
                        {"type": "string", "format": "byte"},
                        {"type": "string"},
                        {
                            "type": "array",
                            "items": {
                                "oneOf": [
                                    {"type": "boolean"},
                                    {"type": "integer", "format": "int32"},
                                    {"type": "integer", "format": "int64"},
                                    {"type": "number", "format": "float"},
                                    {"type": "number", "format": "double"},
                                    {"type": "string", "format": "byte"},
                                    {"type": "string"}
                                ]
                            }
                        },
                        {
                            "type": "object",
                            "additionalProperties": {
                                "oneOf": [
                                    {"type": "boolean"},
                                    {"type": "integer", "format": "int32"},
                                    {"type": "integer", "format": "int64"},
                                    {"type": "number", "format": "float"},
                                    {"type": "number", "format": "double"},
                                    {"type": "string", "format": "byte"},
                                    {"type": "string"}
                                ]
                            }
                        }
                    ]
                }
            }
        ]
    }

def find_schema_node(test, avro_schema, recursion_stack = []):    
    """Find the first schema node in the avro_schema matching the test"""
    for recursion_item in recursion_stack:
        if avro_schema is recursion_item:
            raise ValueError('Cyclical reference detected in schema')
        if len(recursion_stack) > 50:
            raise ValueError('Maximum recursion depth 50 exceeded in schema')
    try:
        recursion_stack.append(avro_schema)
        if isinstance(avro_schema, dict):
            test_node = test(avro_schema)
            if test_node:
                return avro_schema
            for k, v in avro_schema.items():
                if isinstance(v, (dict,list)):
                    node = find_schema_node(test, v, recursion_stack)
                    if node:
                        return node
        elif isinstance(avro_schema, list):
            for item in avro_schema:
                if isinstance(item, (dict,list)):
                    node = find_schema_node(test, item, recursion_stack)
                    if node:
                        return node
        return None
    finally:
        recursion_stack.pop()

def set_schema_node(test, replacement, avro_schema):
    """Set the first schema node in the avro_schema matching the test to the replacement"""
    if isinstance(avro_schema, dict):
        test_node = test(avro_schema)
        if test_node:
            avro_schema.clear()
            avro_schema.update(replacement)
            return
        for k, v in avro_schema.items():
            if isinstance(v, (dict,list)):
                set_schema_node(test, replacement, v)
    elif isinstance(avro_schema, list):
        for item in avro_schema:
            set_schema_node(test, replacement, item)

class NodeHash:
    def __init__(self: 'NodeHash', hash: bytes, count: int):
        self.hash: bytes = hash
        self.count: int = count
        
class NodeHashReference:
    def __init__(self, hash_and_count: NodeHash, value, path):
        self.hash: bytes = hash_and_count.hash
        self.count: int = hash_and_count.count
        self.value: Any = value            
        self.path: str = path
    
            
def get_tree_hash(json_obj: Union[dict, list]) -> NodeHash:
    """
    Generate a hash from a JSON object (dict or list).
    """
    if isinstance(json_obj, dict) or isinstance(json_obj, list):
        s = json.dumps(json_obj, sort_keys=True).encode('utf-8')
        return NodeHash(hashlib.sha256(s).digest(), len(s))
    else:
        s = json.dumps(json_obj).encode('utf-8')
        return NodeHash(hashlib.sha256(s).digest(), len(s))

def build_tree_hash_list(json_obj: Union[dict, list], path: str = '') -> Dict[str, NodeHashReference]:
    """
    Build a flat dictionary of hashes for a JSON object.
    The keys are JSON Path expressions, and the values are the hashes.
    """
    
    def has_nested_structure(obj: Union[dict, list]) -> bool:
        """
        Check if the object (list or dict) contains any nested lists or dicts.
        """
        if isinstance(obj, dict):
            return any(isinstance(value, (dict, list)) for value in obj.values())
        elif isinstance(obj, list):
            return any(isinstance(item, (dict, list)) for item in obj)
        return False
    
    tree_hash = {}
    if isinstance(json_obj, dict):
        for key, value in json_obj.items():
            new_path = f'{path}.{key}' if path else f'$.{key}'
            if isinstance(value, dict) and has_nested_structure(value):
                inner_hashes = build_tree_hash_list(value, new_path)
                for inner_path, hash_reference in inner_hashes.items():
                    tree_hash[inner_path] = hash_reference
                hash = get_tree_hash(value)
                tree_hash[new_path] = NodeHashReference(hash, value, new_path)
    elif isinstance(json_obj, list):
        for index, item in enumerate(json_obj):
            new_path = f"{path}[{index}]"
            if isinstance(item, (dict, list)) and has_nested_structure(item):
                inner_hashes = build_tree_hash_list(item, new_path)
                for inner_path, hash_reference in inner_hashes.items():
                    tree_hash[inner_path] = hash_reference
    return tree_hash

def group_by_hash(tree_hash_list: Dict[str, NodeHashReference]) -> Dict[bytes, list]:
    """
    Group JSON Path expressions by their hash values.
    
    :param tree_hash_list: A dictionary with JSON Path expressions as keys and hashes as values.
    :return: A dictionary where each key is a hash and each value is a list of JSON Path expressions that share that hash.
    """
    hash_groups = defaultdict(list)
    for _, hash_reference in tree_hash_list.items():
        hash_groups[hash_reference.hash].append(hash_reference)

    # Filter out unique hashes to only return groups with more than one path
    for k in list(hash_groups.keys()):
        if len(hash_groups[k]) == 1:
            del hash_groups[k]
    return hash_groups

def pascal(string):
    """ Convert a string to PascalCase """
    if '::' in string:
        strings = string.split('::')
        return strings[0] + '::' + '::'.join(pascal(s) for s in strings[1:])
    if '.' in string:
        strings = string.split('.')
        return '.'.join(pascal(s) for s in strings)
    if not string or len(string) == 0:
        return string
    words = []
    startswith_under = string[0] == '_'
    if '_' in string:
        # snake_case
        words = re.split(r'_', string)
    elif string[0].isupper():
        # PascalCase
        words = re.findall(r'[A-Z][a-z0-9_]*\.?', string)
    else:
        # camelCase
        words = re.findall(r'[a-z0-9]+\.?|[A-Z][a-z0-9_]*\.?', string)
    result = ''.join(word.capitalize() for word in words)
    if startswith_under:
        result = '_' + result
    return result

def camel(string):
    """ Convert a string to camelCase """
    if '::' in string:
        strings = string.split('::')
        return strings[0] + '::' + '::'.join(camel(s) for s in strings[1:])
    if '.' in string:
        strings = string.split('.')
        return '.'.join(camel(s) for s in strings)
    if not string or len(string) == 0:
        return string
    words = []
    if '_' in string:
        # snake_case
        words = re.split(r'_', string)
    elif string[0].isupper():
        # PascalCase
        words = re.findall(r'[A-Z][a-z0-9_]*\.?', string)
    else:
        # camelCase
        words = re.findall(r'[a-z0-9]+\.?|[A-Z][a-z0-9_]*\.?', string)
    result = words[0].lower() + ''.join(word.capitalize() for word in words[1:])
    return result

def snake(string):
    """ Convert a string to snake_case """
    if '::' in string:
        strings = string.split('::')
        return strings[0] + '::' + '::'.join(snake(s) for s in strings[1:])
    if '.' in string:
        strings = string.split('.')
        return '.'.join(snake(s) for s in strings)
    if not string or len(string) == 0:
        return string
    words = []
    if '_' in string:
        # snake_case
        words = re.split(r'_', string)
    elif string[0].isupper():
        # PascalCase
        words = re.findall(r'[A-Z][a-z0-9_]*\.?', string)
    else:
        # camelCase
        words = re.findall(r'[a-z0-9]+\.?|[A-Z][a-z0-9_]*\.?', string)
    result = '_'.join(word.lower() for word in words)
    return result

def fullname(avro_schema: dict):
    name = avro_schema.get("name", "")
    namespace = avro_schema.get("namespace", "")
    return namespace + "." + name if namespace else name

def altname(schema_obj: dict, purpose: str):
    """
    Retrieves the alternative name for a given purpose from the schema object.

    Args:
        schema_obj (dict): The schema object (record or field).
        default_name (str): The default name.
        purpose (str): The purpose for the alternative name (e.g., 'sql').

    Returns:
        str: The alternative name if present, otherwise the default name.
    """
    if "altnames" in schema_obj and purpose in schema_obj["altnames"]:
        return schema_obj["altnames"][purpose]
    return schema_obj["name"]

def process_template(file_path: str, **kvargs) -> str:
    """
    Process a file as a Jinja2 template with the given object as input.

    Args:
        file_path (str): The path to the file.
        obj (Any): The object to use as input for the template.

    Returns:
        str: The processed template as a string.
    """
    # Load the template environment
    file_dir = os.path.dirname(__file__)
    template_loader = jinja2.FileSystemLoader(searchpath=file_dir)
    template_env = jinja2.Environment(loader=template_loader)
    template_env.filters['pascal'] = pascal
    template_env.filters['camel'] = camel

    # Load the template from the file
    template = template_env.get_template(file_path)

    # Render the template with the object as input
    output = template.render(**kvargs)

    return output

def render_template(template: str, output: str, **kvargs):
    """ Render a template and write it to a file """
    out = process_template(template, **kvargs)
    # make sure the directory exists
    os.makedirs(os.path.dirname(output), exist_ok=True)
    with open(output, 'w', encoding='utf-8') as f:
        f.write(out)
        
        
def get_longest_namespace_prefix(schema):
    """ Get the longest common prefix for the namespace of all types in the schema. """
    namespaces = set(collect_namespaces(schema))
    longest_common_prefix = ''
    # find longest common prefix of the namespaces (not with os.path!!!)
    for ns in namespaces:
        if not longest_common_prefix:
            longest_common_prefix = ns
        else:
            for i in range(min(len(longest_common_prefix), len(ns))):
                if longest_common_prefix[i] != ns[i]:
                    longest_common_prefix = longest_common_prefix[:i]
                    break
    return longest_common_prefix.strip('.')

def collect_namespaces(schema: Any, parent_namespace: str = '') -> List[str]:
    """ Performs a deep search of the schema to collect all namespaces """
    namespaces = []
    if isinstance(schema, dict):
        namespace = str(schema.get('namespace', parent_namespace))
        if namespace:
            namespaces.append(namespace)
        if 'fields' in schema and isinstance(schema['fields'], list):
            for field in schema['fields']:
                if isinstance(field, dict) and 'type' in field and isinstance(field['type'], dict):
                    namespaces.extend(collect_namespaces(field['type'], namespace))
                namespaces.extend(collect_namespaces(field, namespace))
        if 'items' in schema and isinstance(schema['items'], dict):
            namespaces.extend(collect_namespaces(schema['items'], namespace))
        if 'values' in schema and isinstance(schema['values'], dict):
            namespaces.extend(collect_namespaces(schema['values'], namespace))
    elif isinstance(schema, list):
        for item in schema:
            namespaces.extend(collect_namespaces(item, parent_namespace))
    return namespaces
