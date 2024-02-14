import json
import argparse

indent = '  '

def avro_primitive_to_proto_type(avro_type):
    """Map Avro primitive types to Protobuf types."""
    mapping = {
        'null': 'google.protobuf.Empty',  # Special handling may be required
        'boolean': 'bool',
        'int': 'int32',
        'long': 'int64',
        'float': 'float',
        'double': 'double',
        'bytes': 'bytes',
        'string': 'string',
    }
    # logical types require special handling
    if isinstance(avro_type, dict) and 'logicalType' in avro_type:
        logical_type = avro_type['logicalType']
        if logical_type == 'date':
            return 'string'
        elif logical_type == 'time-millis':
            return 'string'
        elif logical_type == 'timestamp-millis':
            return 'string'
        elif logical_type == 'decimal':
            precision = avro_type['precision']
            scale = avro_type['scale']
            return 'string'
        elif logical_type == 'duration':
            return 'string'
        elif logical_type == 'uuid':
            return 'string'
        
    return mapping.get(avro_type, avro_type)  # Default to string for unmapped types

def convert_field(avro_field, index):
    """Convert an Avro field to a Protobuf field."""
    field_type = avro_field['type']
    comment = ''
    if 'doc' in avro_field:
        comment = f'  // {avro_field["doc"]}\n{indent}'

    label = ''
    if isinstance(field_type, list) and 'null' in field_type:
        label = 'optional '
    if isinstance(field_type, list):
        # Handling union types (including nullable fields)
        non_null_types = [t for t in field_type if t != 'null']
        if len(non_null_types) == 1:
            field_type = non_null_types[0]
        else:
            # More complex unions may require manual handling or decisions
            field_type = 'string'  # Simplification for example purposes

    if isinstance(field_type, dict):
        # Nested types (e.g., records, enums) require special handling
        if field_type['type'] == 'record':
            return f'{indent}{comment}{label}message {field_type["name"]} {{\n' + "\n".join([convert_field(f, i+1) for i, f in enumerate(field_type['fields'])]) + '\n}\n'
        elif field_type['type'] == 'enum':
            return f'{indent}{comment}{label}enum {field_type["name"]} {{\n' + "\n".join([f'  {symbol} = {i};' for i, symbol in enumerate(field_type['symbols'])]) + '\n}\n'
        elif field_type['type'] == 'array':
            item_type = avro_primitive_to_proto_type(field_type['items'])
            return f'{indent}{comment}{label}repeated {item_type} {avro_field["name"]} = {index};'
        elif field_type['type'] == 'map':
            value_type = avro_primitive_to_proto_type(field_type['values'])
            return f'{indent}{comment}{label}map<string, {value_type}> {avro_field["name"]} = {index};'
        
    else:
        proto_type = avro_primitive_to_proto_type(field_type)
        return f'{indent}{comment}{label}{proto_type} {avro_field["name"]} = {index};'

def avro_schema_to_proto_messages(avro_schema_input):
    """Convert an Avro schema to Protobuf message definitions."""
    if not isinstance(avro_schema_input, list):
        avro_schema_list = [avro_schema_input]
    else:
        avro_schema_list = avro_schema_input
    
    proto_messages = []
    for avro_schema in avro_schema_list:
        comment = ''
        if 'doc' in avro_schema:
            comment = f'// {avro_schema["doc"]}\n'
        if avro_schema['type'] == 'record':
            message_name = avro_schema['name']
            proto_fields = [convert_field(field, i+1) for i, field in enumerate(avro_schema['fields'])]
            proto_messages.append(f'{comment}message {message_name} {{\n' + "\n".join(proto_fields) + '\n}')
        elif avro_schema['type'] == 'enum':
            enum_name = avro_schema['name']
            enum_values = "\n".join([f'  {symbol} = {i};' for i, symbol in enumerate(avro_schema['symbols'])])
            proto_messages.append(f'{comment}enum {enum_name} {{\n' + enum_values + '\n}')
        elif avro_schema['type'] == 'array':
            item_type = avro_primitive_to_proto_type(avro_schema['items'])
            proto_messages.append(f'{comment}repeated {item_type} {avro_schema["name"]} = 1;')
        elif avro_schema['type'] == 'map':
            value_type = avro_primitive_to_proto_type(avro_schema['values'])
            proto_messages.append(f'{comment}map<string, {value_type}> {avro_schema["name"]} = 1;')
    return "\n".join(proto_messages)

def convert_avro_to_proto(avro_schema_path, proto_file_path):
    """Convert Avro schema file to Protobuf .proto file."""
    with open(avro_schema_path, 'r') as avro_file:
        avro_schema = json.load(avro_file)
    proto_schema = 'syntax = "proto3";\n\n' + avro_schema_to_proto_messages(avro_schema)
    with open(proto_file_path, 'w') as proto_file:
        proto_file.write(proto_schema)

