"""
Common utility functions for Avrotize.
"""

# pylint: disable=too-many-arguments, too-many-locals, too-many-branches, too-many-statements, line-too-long

from collections import defaultdict
import os
import re
import hashlib
import json
from typing import Dict, Union, Any, List
from jsoncomparison import NO_DIFF, Compare
import jinja2


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
    """ 
    Constructs a generic Avro type for simple types, arrays, and maps.
    
    Returns:
        list[str | dict]: A list of simple types, arrays, and maps.
    """
    simple_type_union: list[str | dict] = [
        "null", "boolean", "int", "long", "float", "double", "bytes", "string"]
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


def is_generic_json_type(json_type: Dict[str, Any] | List[Dict[str, Any] | str] | str) -> bool:
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


def find_schema_node(test, avro_schema, recursion_stack=None):
    """
    Find the first schema node in the avro_schema matching the test
    
    Args:
        test (Callable): The test function.
        avro_schema (Union[Dict[str, Any], List[Dict[str, Any]]]): The Avro schema to search.
        recursion_stack (List[Union[Dict[str, Any], List[Dict[str, Any]]], optional): The recursion stack. Defaults to None.
        
    Returns:
        Union[Dict[str, Any], None]: The schema node if found, otherwise None.    
    """
    if recursion_stack is None:
        recursion_stack = []
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
            for _, v in avro_schema.items():
                if isinstance(v, (dict, list)):
                    node = find_schema_node(test, v, recursion_stack)
                    if node:
                        return node
        elif isinstance(avro_schema, list):
            for item in avro_schema:
                if isinstance(item, (dict, list)):
                    node = find_schema_node(test, item, recursion_stack)
                    if node:
                        return node
        return None
    finally:
        recursion_stack.pop()


def set_schema_node(test, replacement, avro_schema):
    """
    Set the first schema node in the avro_schema matching the test to the replacement
    
    Args:
        test (Callable): The test function.
        replacement (Dict[str, Any]): The replacement schema.
        avro_schema (Union[Dict[str, Any], List[Dict[str, Any]]]): The Avro schema to search.
        
    Returns:
        None    
    """
    if isinstance(avro_schema, dict):
        test_node = test(avro_schema)
        if test_node:
            avro_schema.clear()
            avro_schema.update(replacement)
            return
        for k, v in avro_schema.items():
            if isinstance(v, (dict, list)):
                set_schema_node(test, replacement, v)
    elif isinstance(avro_schema, list):
        for item in avro_schema:
            set_schema_node(test, replacement, item)


class NodeHash:
    """ A hash value and count for a JSON object. """
    def __init__(self: 'NodeHash', hash_value: bytes, count: int):
        self.hash_value: bytes = hash_value
        self.count: int = count


class NodeHashReference:
    """ A reference to a JSON object with a hash value and count."""
    def __init__(self, hash_and_count: NodeHash, value, path):
        self.hash_value: bytes = hash_and_count.hash_value
        self.count: int = hash_and_count.count
        self.value: Any = value
        self.path: str = path


def get_tree_hash(json_obj: Union[dict, list]) -> NodeHash:
    """
    Generate a hash from a JSON object (dict or list).
    
    Args:
        json_obj (Union[dict, list]): The JSON object to hash.
        
    Returns:
        NodeHash: The hash value and count.
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
    
    Args:
        json_obj (Union[dict, list]): The JSON object to hash.
        path (str): The current JSON Path expression. Defaults to ''.
        
    Returns:
        Dict[str, NodeHashReference]: A dictionary of JSON Path expressions and hashes.
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
                hash_value = get_tree_hash(value)
                tree_hash[new_path] = NodeHashReference(hash_value, value, new_path)
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

    Args:
        tree_hash_list (Dict[str, NodeHashReference]): A dictionary of JSON Path expressions and hashes.
        
    Returns:
        Dict[bytes, list]: A dictionary of hash values and lists of JSON Path expressions.    
    """
    hash_groups = defaultdict(list)
    for _, hash_reference in tree_hash_list.items():
        hash_groups[hash_reference.hash_value].append(hash_reference)

    # Filter out unique hashes to only return groups with more than one path
    for k in list(hash_groups.keys()):
        if len(hash_groups[k]) == 1:
            del hash_groups[k]
    return hash_groups


def pascal(string):
    """ 
    Convert a string to PascalCase from snake_case, camelCase, or PascalCase. 
    The string can contain dots or double colons, which are preserved in the output.
    Underscores at the beginning of the string are preserved in the output, but
    underscores in the middle of the string are removed.     
    
    Args:
        string (str): The string to convert.
        
    Returns:
        str: The string in PascalCase.    
    """
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
    """ 
    Convert a string to camelCase from snake_case, camelCase, or PascalCase.
    The string can contain dots or double colons, which are preserved in the output.
    Underscores at the beginning of the string are preserved in the output, but
    underscores in the middle of the string are removed. 
    
    Args:
        string (str): The string to convert.
        
    Returns:
        str: The string in camelCase.
    """
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
    result = words[0].lower() + ''.join(word.capitalize()
                                        for word in words[1:])
    return result


def snake(string):
    """ 
    Convert a string to snake_case from snake_case, camelCase, or PascalCase.
    The string can contain dots or double colons, which are preserved in the output.
    Underscores at the beginning of the string are preserved in the output, but
    underscores in the middle of the string are removed. 
    
    Args:
        string (str): The string to convert.
        
    Returns:
        str: The string in snake_case.    
    """
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


def fullname(avro_schema: dict| str, parent_namespace: str = '') -> str:
    """
    Constructs the full name of the Avro schema.
    
    Args:
        avro_schema (dict): The Avro schema.
        
    Returns:
        str: The full name of the Avro schema.
    """
    if isinstance(avro_schema, str):
        if not '.' in avro_schema and parent_namespace:
            return parent_namespace + '.' + avro_schema
        return avro_schema
    name = avro_schema.get("name", "")
    namespace = avro_schema.get("namespace", parent_namespace)
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
    """ 
    Render a template and write it to a file
    
    Args:
        template (str): The template to render.
        output (str): The output file path.
        **kvargs: The keyword arguments to pass to the template.
        
    Returns:
        None 
    """
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
                    namespaces.extend(collect_namespaces(
                        field['type'], namespace))
                namespaces.extend(collect_namespaces(field, namespace))
        if 'items' in schema and isinstance(schema['items'], dict):
            namespaces.extend(collect_namespaces(schema['items'], namespace))
        if 'values' in schema and isinstance(schema['values'], dict):
            namespaces.extend(collect_namespaces(schema['values'], namespace))
    elif isinstance(schema, list):
        for item in schema:
            namespaces.extend(collect_namespaces(item, parent_namespace))
    return namespaces


def build_flat_type_dict(avro_schema) -> Dict[str, Dict]:
    """Builds a flat dictionary of all named types in the main schema."""
    type_dict = {}

    def add_to_dict(schema, namespace):
        if isinstance(schema, dict):
            schema_type = schema.get('type')
            name = schema.get('name')
            namespace = schema.get('namespace', namespace)
            if schema_type in ['record', 'enum', 'fixed'] and name:
                qualified_name = f"{namespace}.{name}" if namespace else name
                type_dict[qualified_name] = schema
            if schema_type == 'record':
                for field in schema.get('fields', []):
                    field_type = field.get('type')
                    add_to_dict(field_type, namespace)
            elif schema_type == 'array':
                add_to_dict(schema.get('items'), namespace)
            elif schema_type == 'map':
                add_to_dict(schema.get('values'), namespace)
        elif isinstance(schema, list):
            for item in schema:
                add_to_dict(item, namespace)

    if isinstance(avro_schema, dict):
        add_to_dict(avro_schema, avro_schema.get('namespace', ''))
    elif isinstance(avro_schema, list):
        for schema in avro_schema:
            schema_namespace = schema.get('namespace', '')
            add_to_dict(schema, schema_namespace)
    return type_dict


def evict_tracked_references(avro_schema, parent_namespace, tracker):
    """ Evicts all tracked references in the Avro schema. """
    if isinstance(avro_schema, dict):
        if 'type' in avro_schema and (avro_schema['type'] == 'record' or avro_schema['type'] == 'enum' or avro_schema['type'] == 'fixed'):
            namespace = avro_schema.get('namespace', parent_namespace)
            qualified_name = (
                namespace + '.' if namespace else '') + avro_schema['name']
            if not qualified_name in tracker:
                if 'fields' in avro_schema:
                    for field in avro_schema['fields']:
                        field['type'] = evict_tracked_references(
                            field['type'], namespace, tracker)
                return avro_schema
            else:
                return qualified_name
        # Handling array types
        elif 'type' in avro_schema and avro_schema['type'] == 'array' and 'items' in avro_schema:
            avro_schema['items'] = evict_tracked_references(
                avro_schema['items'], parent_namespace, tracker)
        # Handling map types
        elif 'type' in avro_schema and avro_schema['type'] == 'map' and 'values' in avro_schema:
            avro_schema['values'] = evict_tracked_references(
                avro_schema['values'], parent_namespace, tracker)
    elif isinstance(avro_schema, list):
        return [evict_tracked_references(item, parent_namespace, tracker) for item in avro_schema]
    return avro_schema


def inline_avro_references(avro_schema, type_dict, current_namespace, tracker=None, defined_types=None):
    """ Inlines the first reference to a type in the Avro schema. """
    if tracker is None:
        tracker = set()
    if defined_types is None:
        defined_types = set()

    if isinstance(avro_schema, dict):
        # Register the type if it's a record, enum, or fixed and is inlined in the same schema
        if 'type' in avro_schema and avro_schema['type'] in ['record', 'enum', 'fixed']:
            namespace = avro_schema.get('namespace', current_namespace)
            qualified_name = (namespace + '.' if namespace else '') + avro_schema['name']
            defined_types.add(qualified_name)

        # Process record types
        if 'type' in avro_schema and avro_schema['type'] == 'record' and 'fields' in avro_schema:
            namespace = avro_schema.get('namespace', current_namespace)
            qualified_name = (namespace + '.' if namespace else '') + avro_schema['name']
            if qualified_name in tracker:
                return qualified_name
            tracker.add(qualified_name)
            for field in avro_schema['fields']:
                field['type'] = inline_avro_references(
                    field['type'], type_dict, namespace, tracker, defined_types)

        # Handling array types
        elif 'type' in avro_schema and avro_schema['type'] == 'array' and 'items' in avro_schema:
            avro_schema['items'] = inline_avro_references(
                avro_schema['items'], type_dict, current_namespace, tracker, defined_types)

        # Handling map types
        elif 'type' in avro_schema and avro_schema['type'] == 'map' and 'values' in avro_schema:
            avro_schema['values'] = inline_avro_references(
                avro_schema['values'], type_dict, current_namespace, tracker, defined_types)

        # Inline other types, except enum and fixed
        elif 'type' in avro_schema and avro_schema['type'] not in ['enum', 'fixed']:
            avro_schema['type'] = inline_avro_references(
                avro_schema['type'], type_dict, current_namespace, tracker, defined_types)

    elif isinstance(avro_schema, list):
        return [inline_avro_references(item, type_dict, current_namespace, tracker, defined_types) for item in avro_schema]

    elif avro_schema in type_dict and avro_schema not in tracker and avro_schema not in defined_types:
        # Inline the referenced schema if not already tracked and not defined in the current schema
        inlined_schema = type_dict[avro_schema].copy()
        if isinstance(inlined_schema, dict) and not inlined_schema.get('namespace', None):
            inlined_schema['namespace'] = '.'.join(avro_schema.split('.')[:-1])
        inlined_schema = inline_avro_references(
            inlined_schema, type_dict, inlined_schema['namespace'], tracker, defined_types)
        tracker.add(avro_schema)
        return inlined_schema

    return avro_schema

def strip_first_doc(schema) -> bool:
    """ strip the first doc field anywhere in the schema"""
    if isinstance(schema, dict):
        if "doc" in schema:
            del schema["doc"]
            return True
        for key in schema:
            if strip_first_doc(schema[key]):
                return True
    elif isinstance(schema, list):
        for item in schema:
            if strip_first_doc(item):
                return True
    return False


def is_type_with_alternate(avro_schema: List[Dict[str, Any]]) -> bool:
    """
    Check if the Avro schema union contains a type with a trailing alternate type.
    Alternate types are maps that mimic the structure of the original type, but
    allow for additional fields. Alternate types are labeled with an 'alternateof'
    attribute extension that points to the original type.

    Args:
        avro_schema (List[Dict[str, Any]]): The Avro schema to check.

    Returns:
        bool: True if the Avro schema contains a type with an alternate name, False otherwise.
    """
    avro_schema = avro_schema.copy()
    if not isinstance(avro_schema, list):
        return False
    if 'null' in avro_schema:
        avro_schema.remove('null')
    if len(avro_schema) != 2:
        return False
    original_type = any(t for t in avro_schema if isinstance(t, dict) and not 'alternateof' in t)
    alternate_type = any(t for t in avro_schema if isinstance(t, dict) and 'alternateof' in t)
    if original_type and alternate_type:
        return True
    return False

def strip_alternate_type(avro_schema: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Strips the alternate type from the Avro schema union.

    Args:
        avro_schema (List[Dict[str, Any]]): The Avro schema to strip.

    Returns:
        List[Dict[str, Any]]: The Avro schema without the alternate type.
    """
    original_type = next((t for t in avro_schema if isinstance(t, dict) and not 'alternateof' in t), None)
    alternate_type = next((t for t in avro_schema if isinstance(t, dict) and 'alternateof' in t), None)
    if original_type and alternate_type:
        avro_schema.remove(alternate_type)
    return avro_schema


def get_typing_args_from_string(type_str: str) -> List[str]:
    """ gets the list of generic arguments of a type. """
    # This regex captures the main type and its generic arguments
    pattern = re.compile(r'([\w\.]+)\[(.+)\]')
    match = pattern.match(type_str)
    
    if not match:
        return []

    _, args_str = match.groups()
    # Splitting the arguments while considering nested generic types
    args = []
    depth = 0
    current_arg:List[str] = []    
    for char in args_str:
        if char == ',' and depth == 0:
            args.append(''.join(current_arg).strip())
            current_arg = []
        else:
            if char == '[':
                depth += 1
            elif char == ']':
                depth -= 1
            current_arg.append(char)
    if current_arg:
        args.append(''.join(current_arg).strip())    
    return args