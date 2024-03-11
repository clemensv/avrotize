from collections import defaultdict
import re
from typing import Dict, Tuple, Union, Any, List
from jsoncomparison import NO_DIFF, Compare
import hashlib
import json    

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

