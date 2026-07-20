"""
Common utility functions for Avrotize.
"""

# pylint: disable=too-many-arguments, too-many-locals, too-many-branches, too-many-statements, line-too-long

from collections import defaultdict
import copy
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
    # Ensure the name starts with a letter or underscore (required for valid identifiers)
    if re.match(r'^[0-9]', val):
        val = '_' + val
    # Additional check to ensure we always have a valid identifier
    if not val or not re.match(r'^[a-zA-Z_]', val):
        val = '_' + val
    return val


def avro_name_with_altname(name):
    """
    Convert a name into an Avro name and return both the normalized name and alternate name info.
    
    Args:
        name (str): The original name to convert
        
    Returns:
        tuple: (normalized_name, original_name_if_different_or_None)
    """
    if isinstance(name, int):
        name = str(name)
    
    original_name = name
    normalized_name = avro_name(name)
    
    # If the normalized name is different from the original, return the original as alt name
    if normalized_name != original_name:
        return normalized_name, original_name
    else:
        return normalized_name, None


def avro_namespace(name):
    """Convert a name into an Avro name."""
    val = re.sub(r'[^a-zA-Z0-9_\.]', '_', name)
    if re.match(r'^[0-9]', val):
        val = '_' + val
    return val


def unique_name(candidate: str, used: set) -> str:
    """Return a name unique within ``used``, suffixing ``_1``, ``_2``, ... on collision.

    This disambiguates distinct source names that collapse to the same identifier
    after sanitization (e.g. ``a-b`` and ``a_b`` both become ``a_b``). The chosen
    name is added to ``used`` so repeated calls keep producing fresh names.
    """
    if candidate not in used:
        used.add(candidate)
        return candidate
    index = 1
    while f"{candidate}_{index}" in used:
        index += 1
    result = f"{candidate}_{index}"
    used.add(result)
    return result


ANY_VALUE_RECORD: dict = {
    "type": "record",
    "name": "AnyValue",
    "namespace": "avrotize",
    "doc": "Extensible record placeholder for the 'any' type. Add fields via schema evolution.",
    "fields": []
}
"""Avro record definition for the extensible 'any' type. Defined once, referenced by name thereafter."""

ANY_VALUE_NAME = "avrotize.AnyValue"
"""Fully-qualified name reference for the AnyValue record."""

ANY_VALUE_NAMESPACE = "avrotize"
"""Namespace used for all AnyValue record variants."""


def any_value_name(record_name: str = "", field_name: str = "") -> str:
    """Generate a qualified AnyValue record name from parent record and field context.
    
    Produces a unique name like 'Order_data_AnyValue' so that different
    any-typed fields can evolve their AnyValue records independently.
    
    Args:
        record_name: Name of the parent record (e.g., 'Order').
        field_name: Name of the field (e.g., 'data').
    
    Returns:
        str: A qualified name like 'Order_data_AnyValue', or just 'AnyValue' if no context.
    """
    parts = []
    if record_name:
        parts.append(avro_name(record_name))
    if field_name and field_name != record_name:
        parts.append(avro_name(field_name))
    if parts:
        return "_".join(parts) + "_AnyValue"
    return "AnyValue"


def is_any_value_type(avro_type: str) -> bool:
    """Check if a type name refers to an AnyValue variant (any record in the avrotize namespace)."""
    if not isinstance(avro_type, str):
        return False
    return (avro_type.startswith('avrotize.') or 
            avro_type == 'AnyValue' or 
            avro_type.endswith('AnyValue'))


def generic_type(*, define_any_value: bool = True, name: str = "AnyValue") -> list[str | dict]:
    """ 
    Constructs a generic Avro type as a union of all primitive types, an extensible
    empty record, and recursive array/map types.

    The record is an empty record that can be extended via Avro schema
    evolution (adding fields with defaults). Arrays and maps reference it
    by name, enabling infinite nesting.
    
    Args:
        define_any_value: If True (default), includes the full record definition.
            Set to False for subsequent uses in the same schema to avoid redefinition errors.
        name: Name for the extensible record. Defaults to "AnyValue".
            Use a unique name per field (e.g., "PayloadAnyValue") to enable
            independent schema evolution of different any-typed fields.
    
    Returns:
        list[str | dict]: A union type representing 'any'.
    """
    fqn = f"{ANY_VALUE_NAMESPACE}.{name}"
    record_def: dict = {
        "type": "record",
        "name": name,
        "namespace": ANY_VALUE_NAMESPACE,
        "doc": "Extensible record placeholder for the 'any' type. Add fields via schema evolution.",
        "fields": []
    }
    any_value_entry: str | dict = record_def if define_any_value else fqn
    # Inner union used in array items and map values — references record by name.
    # Record ref comes AFTER array/map so serializers try map before the empty record.
    inner_union: list[str | dict] = [
        "null", "boolean", "int", "long", "float", "double", "bytes", "string",
        {"type": "array", "items": ["null", "boolean", "int", "long", "float", "double", "bytes", "string", fqn]},
        {"type": "map", "values": ["null", "boolean", "int", "long", "float", "double", "bytes", "string", fqn]},
        fqn
    ]
    # Outer union — defines record (must come before array/map for schema parsing),
    # then array/map use inner_union which references it by name
    outer_union: list[str | dict] = [
        "null", "boolean", "int", "long", "float", "double", "bytes", "string",
        any_value_entry,
        {"type": "array", "items": inner_union},
        {"type": "map", "values": inner_union}
    ]
    return outer_union


def deduplicate_any_value_record(schema) -> None:
    """
    Post-process an Avro schema to ensure each AnyValue variant is defined only once.
    
    Handles both the default 'AnyValue' and per-field named variants (any record
    in the 'avrotize' namespace). Each unique name is kept at first occurrence;
    subsequent occurrences are replaced with name references.
    
    Also repairs schemas where definitions were lost during union
    merging/flattening by re-inserting definitions at the first reference point.
    
    Args:
        schema: The Avro schema (dict, list, or str) to deduplicate in place.
    """
    import json
    
    if not _has_any_value_record(schema):
        # Break aliasing: serialize/deserialize to get independent objects
        if isinstance(schema, list):
            fresh = json.loads(json.dumps(schema))
            schema.clear()
            schema.extend(fresh)
        _repair_missing_definitions(schema)
        return
    
    # Track which names have been seen (first definition kept)
    seen_names: set = set()
    _deduplicate_any_value_walk(schema, seen_names)
    
    # Break aliasing: serialize/deserialize to get independent objects at each path.
    # This ensures the repair won't accidentally modify shared structures.
    if isinstance(schema, list):
        fresh = json.loads(json.dumps(schema))
        schema.clear()
        schema.extend(fresh)
    elif isinstance(schema, dict):
        fresh = json.loads(json.dumps(schema))
        schema.clear()
        schema.update(fresh)
    
    # Repair: find refs without matching defs and re-insert definitions
    _repair_missing_definitions(schema)
    
    # Final dedup pass to clean up any duplicates introduced by repair
    seen_names2: set = set()
    _deduplicate_any_value_walk(schema, seen_names2)


def _is_any_value_record_node(node) -> bool:
    """Check if a dict node is an AnyValue record definition (any record in avrotize namespace)."""
    return (isinstance(node, dict) and 
            node.get("type") == "record" and 
            node.get("namespace") == ANY_VALUE_NAMESPACE)


def _has_any_value_record(node) -> bool:
    """Check if any AnyValue record definition or reference exists in the schema."""
    if isinstance(node, str):
        return is_any_value_type(node)
    elif isinstance(node, list):
        return any(_has_any_value_record(item) for item in node)
    elif isinstance(node, dict):
        if _is_any_value_record_node(node):
            return True
        return any(_has_any_value_record(v) for v in node.values() if isinstance(v, (list, dict)))
    return False


def _replace_all_any_value_defs(node) -> None:
    """Replace ALL AnyValue record definitions with name references."""
    if isinstance(node, list):
        for i, item in enumerate(node):
            if _is_any_value_record_node(item):
                node[i] = f"{ANY_VALUE_NAMESPACE}.{item['name']}"
            else:
                _replace_all_any_value_defs(item)
    elif isinstance(node, dict):
        for key, value in node.items():
            if isinstance(value, (list, dict)):
                _replace_all_any_value_defs(value)


def _deduplicate_any_value_walk(node, seen_names: set) -> None:
    """Recursively walk and deduplicate AnyValue record definitions.
    
    When replacing an inline definition with a name reference, moves the
    reference to the end of the containing union so that map/array types
    are tried first during serialization union resolution.
    """
    if isinstance(node, list):
        # Use index-based iteration to safely handle list mutations
        i = 0
        while i < len(node):
            item = node[i]
            if _is_any_value_record_node(item):
                fqn = f"{ANY_VALUE_NAMESPACE}.{item['name']}"
                if fqn in seen_names:
                    # Replace with name reference and move to end of the union
                    node.pop(i)
                    node.append(fqn)
                    # Don't increment i - next item shifted into current position
                else:
                    seen_names.add(fqn)
                    i += 1
            else:
                _deduplicate_any_value_walk(item, seen_names)
                i += 1
    elif isinstance(node, dict):
        for key, value in node.items():
            if isinstance(value, (list, dict)):
                _deduplicate_any_value_walk(value, seen_names)


def _repair_missing_definitions(schema) -> None:
    """Re-insert AnyValue definitions where refs exist but defs were lost during merging.
    
    Scans the schema for all AnyValue name references (strings like 'avrotize.XxxAnyValue')
    and all AnyValue record definitions. For any name that has references but no definition,
    replaces the FIRST reference with a full record definition inline.
    """
    # Collect all defined names and all referenced names
    defined_names: set = set()
    referenced_names: set = set()
    _collect_any_value_names(schema, defined_names, referenced_names)
    
    # Find names that are referenced but not defined
    missing = referenced_names - defined_names
    if not missing:
        return
    
    # For each missing name, replace the first reference with a definition
    for fqn in missing:
        name = fqn.replace(f"{ANY_VALUE_NAMESPACE}.", "", 1)
        record_def = {
            "type": "record",
            "name": name,
            "namespace": ANY_VALUE_NAMESPACE,
            "doc": "Extensible record placeholder for the 'any' type. Add fields via schema evolution.",
            "fields": []
        }
        _replace_first_ref_with_def(schema, fqn, record_def)


def _collect_any_value_names(node, defined: set, referenced: set) -> None:
    """Collect all AnyValue definition names and reference names in the schema."""
    if isinstance(node, str):
        if node.startswith(f"{ANY_VALUE_NAMESPACE}.") and node.endswith("AnyValue"):
            referenced.add(node)
    elif isinstance(node, list):
        for item in node:
            _collect_any_value_names(item, defined, referenced)
    elif isinstance(node, dict):
        if _is_any_value_record_node(node) and node.get("name", "").endswith("AnyValue"):
            defined.add(f"{ANY_VALUE_NAMESPACE}.{node['name']}")
        for v in node.values():
            if isinstance(v, (list, dict, str)):
                _collect_any_value_names(v, defined, referenced)


def _replace_first_ref_with_def(node, fqn: str, record_def: dict) -> bool:
    """Replace the first occurrence of a name reference string with a record definition.
    
    Returns True if replacement was made, False otherwise.
    """
    if isinstance(node, list):
        for i, item in enumerate(node):
            if item == fqn:
                node[i] = record_def
                return True
            elif isinstance(item, (list, dict)):
                if _replace_first_ref_with_def(item, fqn, record_def):
                    return True
    elif isinstance(node, dict):
        for key, value in node.items():
            if value == fqn:
                node[key] = record_def
                return True
            elif isinstance(value, (list, dict)):
                if _replace_first_ref_with_def(value, fqn, record_def):
                    return True
    return False



def is_generic_avro_type(avro_type: list) -> bool:
    """
    Check if the given Avro type is a generic type.

    Recognizes the current AnyValue-based format (with any name in the avrotize
    namespace), the default AnyValue format, and the legacy 2-level nested 
    primitives union for backward compatibility.

    Args:
        avro_type (Union[str, Dict[str, Any]]): The Avro type to check.

    Returns:
        bool: True if the Avro type is a generic type, False otherwise.
    """
    if isinstance(avro_type, str) or isinstance(avro_type, dict):
        return False
    # Check current default format (with full definition and with name reference)
    if Compare().check(avro_type, generic_type(define_any_value=True)) == NO_DIFF:
        return True
    if Compare().check(avro_type, generic_type(define_any_value=False)) == NO_DIFF:
        return True
    # Check for per-field named variant: look for any avrotize.* record in the union
    if _is_any_value_union_structure(avro_type):
        return True
    # Check legacy format (2-level nested primitives union without AnyValue)
    if Compare().check(avro_type, _legacy_generic_type()) == NO_DIFF:
        return True
    return False


def _is_any_value_union_structure(avro_type: list) -> bool:
    """Check if a union has the generic_type structure with any avrotize.* record."""
    # Must have at least 11 elements (8 primitives + record + array + map)
    if len(avro_type) < 11:
        return False
    # Check primitives prefix
    expected_primitives = ["null", "boolean", "int", "long", "float", "double", "bytes", "string"]
    if avro_type[:8] != expected_primitives:
        return False
    # Look for an avrotize namespace record (inline def or name ref) in remaining elements
    for item in avro_type[8:]:
        if isinstance(item, dict) and _is_any_value_record_node(item):
            return True
        if isinstance(item, str) and item.startswith(f"{ANY_VALUE_NAMESPACE}."):
            return True
    return False


def _legacy_generic_type() -> list[str | dict]:
    """Construct the legacy generic Avro type (2-level nested primitives without AnyValue)."""
    simple = ["null", "boolean", "int", "long", "float", "double", "bytes", "string"]
    l2 = simple.copy()
    l2.extend([{"type": "array", "items": simple}, {"type": "map", "values": simple}])
    l1 = simple.copy()
    l1.extend([{"type": "array", "items": l2}, {"type": "map", "values": l2}])
    return l1


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


def json_wire_name(prop_name: str, prop_schema: Any) -> str:
    """
    Resolve the JSON wire key for a property, honoring JSON Structure ``altnames.json``.

    JSON Structure requires property keys to be identifiers. A non-identifier wire key
    (e.g. ``dr-type``) is modeled as an identifier property name (``dr_type``) carrying
    ``altnames: {"json": "dr-type"}``. The ``json`` purpose is the canonical wire name.

    Args:
        prop_name (str): The property name (identifier) used as the schema key.
        prop_schema (Any): The property schema; may carry an ``altnames`` map.

    Returns:
        str: ``altnames.json`` when present, otherwise ``prop_name``.
    """
    if isinstance(prop_schema, dict):
        altnames = prop_schema.get("altnames")
        if isinstance(altnames, dict) and "json" in altnames:
            return altnames["json"]
    return prop_name


def xml_wire_name(name: str, schema: Any) -> str:
    """Resolve an XML local name, honoring ``altnames.xml``."""
    if isinstance(schema, dict):
        altnames = schema.get("altnames")
        if isinstance(altnames, dict) and isinstance(altnames.get("xml"), str):
            return altnames["xml"]
    return name


def xml_enum_wire_value(value: Any, enum_schema: Any) -> str:
    """Resolve an XML enum value, honoring XML alternate-symbol maps."""
    if isinstance(enum_schema, dict):
        # JSON Structure calls this map ``altenums`` while Avrotize/Avro
        # schemas historically use ``altsymbols``. Accept both spellings.
        for key in ("altenums", "altsymbols"):
            alternates = enum_schema.get(key)
            if isinstance(alternates, dict):
                xml_values = alternates.get("xml")
                if isinstance(xml_values, dict) and str(value) in xml_values:
                    return str(xml_values[str(value)])
    return str(value)


def json_enum_wire_value(value: Any, enum_schema: Any) -> str:
    """
    Resolve the JSON wire string for an enum value, honoring JSON Structure ``altenums.json``.

    ``altenums.json`` is a map keyed by the original enum value; the language member name
    stays derived from the original value, while the wire value uses the mapping when present.
    Unmapped values serialize verbatim (identity fallback).

    Args:
        value: The original enum value (schema key).
        enum_schema (Any): The enum schema; may carry an ``altenums`` map.

    Returns:
        str: ``altenums.json[value]`` when present, otherwise ``str(value)``.
    """
    if isinstance(enum_schema, dict):
        altenums = enum_schema.get("altenums")
        if isinstance(altenums, dict):
            j = altenums.get("json")
            if isinstance(j, dict) and str(value) in j:
                return j[str(value)]
    return str(value)


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


def format_go_files(directory: str) -> bool:
    """Run ``gofmt`` over every ``.go`` file under ``directory`` to produce
    canonical, gofmt-clean output.

    This normalizes indentation (tabs), sorts imports, aligns struct fields and
    ``const`` blocks, fixes blank-line placement, and guarantees a trailing
    newline at EOF -- everything the Go emitters cannot easily express in Jinja
    templates. Struct-tag spacing is fixed in the templates instead, because
    ``gofmt`` never rewrites the contents of raw string literals.

    Best-effort: if the Go toolchain is not installed, the generated files are
    left untouched and ``False`` is returned (the emitter still produced valid,
    compilable Go).

    Args:
        directory (str): Directory containing generated ``.go`` files. ``gofmt``
            processes it recursively.

    Returns:
        bool: ``True`` if ``gofmt`` (or ``go fmt``) ran, ``False`` otherwise.
    """
    import shutil
    import subprocess
    gofmt = shutil.which('gofmt')
    try:
        if gofmt:
            subprocess.run(
                [gofmt, '-w', directory], check=False,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return True
        go = shutil.which('go')
        if go:
            subprocess.run(
                [go, 'fmt', './...'], cwd=directory, check=False,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return True
    except OSError:
        return False
    return False


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
            # If this named type is already defined, return just the reference string
            if qualified_name in defined_types:
                return qualified_name
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
        # Use deepcopy to avoid mutating the original type_dict entries when modifying nested structures
        inlined_schema = copy.deepcopy(type_dict[avro_schema])
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