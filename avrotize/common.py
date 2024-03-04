import re

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

def find_schema_node(test, avro_schema, recursion_stack = []):    
    """Find the first schema node in the avro_schema matching the test"""
    for recursion_item in recursion_stack:
        if avro_schema is recursion_item:
            raise ValueError('Cyclical reference detected in schema')
        if len(recursion_stack) > 30:
            raise ValueError('Maximum recursion depth 30 exceeded in schema')
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