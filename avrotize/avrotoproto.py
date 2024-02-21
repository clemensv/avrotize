import copy
import json
import argparse
import os
import typing

indent = '  '

Comment = typing.NamedTuple('Comment', [('content', str), ('tags', typing.Dict[str, typing.Any])])
Oneof = typing.NamedTuple('Oneof', [('comment', 'Comment'), ('name', str), ('fields', typing.List['Field'])])
Field = typing.NamedTuple('Field', [('comment', 'Comment'), ('label', str), ('type', str), ('key_type', str), ('val_type', str), ('name', str), ('number', int)])
Enum = typing.NamedTuple('Enum', [('comment', 'Comment'), ('name', str), ('fields', typing.Dict[str, 'Field'])])
Message = typing.NamedTuple('Message', [('comment', 'Comment'), ('name', str), ('fields', typing.List['Field']), ('oneofs', typing.List['Oneof']),
                                        ('messages', typing.Dict[str, 'Message']), ('enums', typing.Dict[str, 'Enum'])])
Service = typing.NamedTuple('Service', [('name', str), ('functions', typing.Dict[str, 'RpcFunc'])])
RpcFunc = typing.NamedTuple('RpcFunc', [('name', str), ('in_type', str), ('out_type', str), ('uri', str)])
ProtoFile = typing.NamedTuple('ProtoFile',
                              [('messages', typing.Dict[str, 'Message']), ('enums', typing.Dict[str, 'Enum']),
                               ('services', typing.Dict[str, 'Service']), ('imports', typing.List[str]),
                               ('options', typing.Dict[str, str]), ('package', str)])
ProtoFiles = typing.NamedTuple('ProtoFiles', [('files', typing.List['ProtoFile'])])


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

def convert_field(message: Message, avro_field: dict, index: int, proto_files: ProtoFiles) -> Field | Oneof | Enum:
    """Convert an Avro field to a Protobuf field."""
    field_type = avro_field['type']
    field_name = avro_field['name'] if 'name' in avro_field else field_type.split('.')[-1]+'Value' if isinstance(field_type, str) else f"{index}Value"   
    if 'doc' in avro_field:
        comment = Comment(avro_field["doc"], {})
    else:
        comment = Comment('',{})
    
    return convert_field_type(message, field_name, field_type, comment, index, proto_files)
    
def convert_record_type(avro_record: dict, comment: Comment, proto_files: ProtoFiles) -> Message:
    local_message = Message(comment, avro_record['name'], [], [], {}, {})
    for i, f in enumerate(avro_record['fields']):
        field = convert_field(local_message, f, i+1, proto_files)
        if isinstance(field, Oneof):
            local_message.oneofs.append(field)
        elif isinstance(field, Enum):
            enum = Enum(field.comment, field.name+"_enum", field.fields)
            local_message.enums[enum.name] = enum
            local_message.fields.append(Field(field.comment, '', enum.name, '', '', field.name.split('.')[-1], i+1))
        elif isinstance(field, Message):
            inner_message = Message(field.comment, field.name+"_type", field.fields, field.oneofs, field.messages, field.enums)
            local_message.messages[inner_message.name] = inner_message
            local_message.fields.append(Field(field.comment, '', inner_message.name, '', '', field.name.split('.')[-1], i+1))
        else:
            local_message.fields.append(field)
    return local_message

def convert_field_type(message: Message, field_name: str, field_type: str | dict | list, comment: Comment, index: int, proto_files: ProtoFiles) -> Field | Oneof | Enum:
    """Convert an Avro field type to a Protobuf field type."""
    label = ''
    
    if isinstance(field_type, list):
        # Handling union types (including nullable fields)
        non_null_types = [t for t in field_type if t != 'null']
        if len(non_null_types) == 1:
            label = 'optional'
            field_type = non_null_types[0]
        elif len(non_null_types) > 0:
            oneof_fields = []
            for i, t in enumerate(non_null_types):
                field = convert_field_type(message, field_name, t, comment, i+1, proto_files)
                if isinstance(field, Field):
                    field = Field(field.comment, field.label, field.type, field.key_type, field.val_type, field.type.split('.')[-1], i+1)
                    oneof_fields.append(field)
                elif isinstance(field, Oneof):
                    local_message = Message(comment, field.name, [], [], {}, {})
                    local_message.oneofs.append(field)
                    message.messages[local_message.name] = local_message
                    field = Field(field.comment, '', local_message.name, '', '', field.name.split('.')[-1], i+1)
                    oneof_fields.append(field)
                elif isinstance(field, Enum):
                    enum = Enum(field.comment, field.name+"_enum", field.fields)
                    message.enums[enum.name] = enum
                    field = Field(field.comment, '', enum.name, '', '', field.name.split('.')[-1], i+1)
                    oneof_fields.append(field)
                elif isinstance(field, Message):
                    local_message = Message(field.comment, field.name+"_type", field.fields, field.oneofs, field.messages, field.enums)
                    message.messages[local_message.name] = local_message
                    field = Field(field.comment, '', local_message.name, '', '', field.name.split('.')[-1], i+1)
                    oneof_fields.append(field)
            oneof = Oneof(comment, field_name, copy.deepcopy(oneof_fields))
            return oneof
        else:
            raise ValueError(f"Field {field_name} is a union type without any non-null types")

    if isinstance(field_type, dict):
        # Nested types (e.g., records, enums) require special handling
        if field_type['type'] == 'record':
            return convert_record_type(field_type, comment, proto_files)
        elif field_type['type'] == 'enum':
            return Enum(comment, field_type['name'], {symbol: i for i, symbol in enumerate(field_type['symbols'])})
        elif field_type['type'] == 'array':
            return convert_field_type(message, field_name, field_type['items'], comment, 1, proto_files)
        elif field_type['type'] == 'map':
            return convert_field_type(message, field_name, field_type['values'], 1, comment, proto_files)
        elif field_type['type'] == "fixed":
            return Field(comment, label, 'fixed','string', 'string', field_name, index)
        else:
            proto_type = avro_primitive_to_proto_type(field_type['type'])
            return Field(comment, label, proto_type, '', '', field_name, index)
    else:
        proto_type = avro_primitive_to_proto_type(field_type)
        return Field(comment, label, proto_type, '', '', field_name, index)

def avro_schema_to_proto_message(avro_schema: dict, proto_files: ProtoFiles) -> str:
    comment = Comment('',{})
    if 'doc' in avro_schema:
        comment = Comment(avro_schema["doc"], {})
    if avro_schema['type'] == 'record':
        message = convert_record_type(avro_schema, comment, proto_files)
        file = next((f for f in proto_files.files if f.package == avro_schema["namespace"]), None)
        if not file:
            file = ProtoFile({}, {}, {}, [], {}, avro_schema["namespace"])
            proto_files.files.append(file)
        file.messages[message.name] = message
    elif avro_schema['type'] == 'enum':
        enum_name = avro_schema['name']
        enum = Enum(comment, enum_name, {symbol: i for i, symbol in enumerate(avro_schema['symbols'])})
        file = next((f for f in proto_files.files if f.package == avro_schema["namespace"]), None)
        if not file:
            file = ProtoFile({}, {}, {}, [], {}, avro_schema["namespace"])
            proto_files.files.append(file)
        file.enums[enum_name] = enum
    return avro_schema["name"]
    
def avro_schema_to_proto_messages(avro_schema_input, proto_files: ProtoFiles):
    """Convert an Avro schema to Protobuf message definitions."""
    if not isinstance(avro_schema_input, list):
        avro_schema_list = [avro_schema_input]
    else:
        avro_schema_list = avro_schema_input    
    for avro_schema in avro_schema_list:
        avro_schema_to_proto_message(avro_schema, proto_files)

def save_proto_to_file(proto_files: ProtoFiles, proto_path):
    """Save the Protobuf schema to a file."""
    for proto in proto_files.files:

        proto.imports.extend([f.package[len(proto.package)+1:] for f in proto_files.files if f.package.startswith(proto.package) and f.package != proto.package])
        proto_file_path = os.path.join(proto_path, f"{proto.package}.proto")
        # create the directory for the proto file if it doesn't exist
        proto_dir = os.path.dirname(proto_file_path)
        if not os.path.exists(proto_dir):
            os.makedirs(proto_dir)
        with open(proto_file_path, 'w') as proto_file:
            # dump the ProtoFile structure in proto syntax
            proto_str = f'syntax = "proto3";\n\n'
            proto_str += f'package {proto.package};\n\n'

            for import_package in proto.imports:
                proto_str += f"import \"{proto.package}.{import_package}.proto\";\n"
            if (len(proto.imports)):
                proto_str += "\n"
            for enum in proto.enums.values():
                proto_str += f"enum {enum.name} {{\n"
                for field_name, val in enum.fields.items():
                    proto_str += f"{indent}{field_name} = {val};\n"
                proto_str += "}\n\n"
            for message in proto.messages.values():
                proto_str += render_message(message)
            for service in proto.services.values():
                proto_str += f"service {service.name} {{\n"
                for function_name, func in service.functions.items():
                    proto_str += f"{indent}rpc {func.name} ({func.in_type}) returns ({func.out_type}) {{\n"
                    proto_str += f"{indent}{indent}option (google.api.http) = {{\n"
                    proto_str += f"{indent}{indent}{indent}post: \"{func.uri}\"\n"
                    proto_str += f"{indent}{indent}}};\n"
                    proto_str += f"{indent}}};\n"
                proto_str += "}\n\n"
            proto_file.write(proto_str)

def render_message(message, level=0) -> str:
    proto_str = f"{indent*level}message {message.name} {{\n"
    for field in message.fields:
        proto_str += f"{indent*level}{indent}{field.type} {field.name} = {field.number};\n"
    for oneof in message.oneofs:
        proto_str += f"{indent*level}{indent}oneof {oneof.name} {{\n"
        for field in oneof.fields:
            proto_str += f"{indent*level}{indent}{indent}{field.type} {field.name} = {field.number};\n"
        proto_str += f"{indent*level}{indent}}}\n"
    for enum in message.enums.values():
        proto_str += f"{indent*level}{indent}enum {enum.name} {{\n"
        for field_name, val in enum.fields.items():
            proto_str += f"{indent*level}{indent}{indent}{field_name} = {val};\n"
        proto_str += f"{indent*level}{indent}}}\n"
    for local_message in message.messages.values():
        proto_str += render_message(local_message, level+1)
    proto_str += f"{indent*level}}}\n\n"
    return proto_str
        

def convert_avro_to_proto(avro_schema_path, proto_file_path):
    """Convert Avro schema file to Protobuf .proto file."""
    with open(avro_schema_path, 'r') as avro_file:
        avro_schema = json.load(avro_file)
    proto_files = ProtoFiles([])
    avro_schema_to_proto_messages(avro_schema, proto_files)
    save_proto_to_file(proto_files, proto_file_path)

