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