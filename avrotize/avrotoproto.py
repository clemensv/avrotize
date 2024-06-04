import copy
import json
import argparse
import os
from typing import Literal, NamedTuple, Dict, Any, List

indent = '  '

Comment = NamedTuple('Comment', [('content', str), ('tags', Dict[str, Any])])
Oneof = NamedTuple('Oneof', [('comment', 'Comment'), ('name', str), ('fields', List['Field'])])
Field = NamedTuple('Field', [('comment', 'Comment'), ('label', str), ('type', str), ('key_type', str), ('val_type', str), ('name', str), ('number', int), ('dependencies', List[str])])
Enum = NamedTuple('Enum', [('comment', 'Comment'), ('name', str), ('fields', Dict[str, 'Field'])])
Message = NamedTuple('Message', [('comment', 'Comment'), ('name', str), ('fields', List['Field']), ('oneofs', List['Oneof']),
                                        ('messages', Dict[str, 'Message']), ('enums', Dict[str, 'Enum']), ('dependencies', List[str])])
Service = NamedTuple('Service', [('name', str), ('functions', Dict[str, 'RpcFunc'])])
RpcFunc = NamedTuple('RpcFunc', [('name', str), ('in_type', str), ('out_type', str), ('uri', str)])
ProtoFile = NamedTuple('ProtoFile',
                            [('messages', Dict[str, 'Message']), ('enums', Dict[str, 'Enum']),
                            ('services', Dict[str, 'Service']), ('imports', List[str]),
                            ('options', Dict[str, str]), ('package', str)])
ProtoFiles = NamedTuple('ProtoFiles', [('files', List['ProtoFile'])])

class AvroToProto:
    
    def __init__(self) -> None:
        self.naming_mode: Literal['snake', 'pascal', 'camel'] = 'pascal'
        self.allow_optional: bool = False
        self.default_namespace: str = ''

    def avro_primitive_to_proto_type(self, avro_type: str, dependencies: List[str]) -> str:
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
            
        type = mapping.get(avro_type, '')
        if not type:
            dependencies.append(avro_type)
            type = avro_type
        return type

    def compose_name(self, prefix: str, name: str, naming_mode: Literal['pascal', 'camel', 'snake', 'default', 'field'] = 'default') -> str:
        if naming_mode == 'default':
            naming_mode = self.naming_mode
        if naming_mode == 'field':
            if self.naming_mode == 'pascal':
                naming_mode = 'camel'
            else:
                naming_mode = self.naming_mode
        if naming_mode == 'snake':
            return f"{prefix}_{name}"
        if naming_mode == 'pascal':
            return f"{prefix[0].upper()+prefix[1:] if prefix else ''}{name[0].upper()+name[1:] if name else ''}"
        if naming_mode == 'camel':
            return f"{prefix[0].lower()+prefix[1:] if prefix else ''}{name[0].upper()+name[1:] if name else ''}"
        return prefix+name    

    def convert_field(self, message: Message, avro_field: dict, index: int, proto_files: ProtoFiles) -> Field | Oneof | Enum | Message:
        """Convert an Avro field to a Protobuf field."""
        field_type = avro_field['type']
        field_name = avro_field['name'] if 'name' in avro_field else self.compose_name(field_type.split('.')[-1],'value', 'field') if isinstance(field_type, str) else self.compose_name(f"_{index}", 'value', 'field')
        if 'doc' in avro_field:
            comment = Comment(avro_field["doc"], {})
        else:
            comment = Comment('',{})
        
        return self.convert_field_type(message, field_name, field_type, comment, index, proto_files)
        
    def convert_record_type(self, avro_record: dict, comment: Comment, proto_files: ProtoFiles) -> Message:
        """Convert an Avro record to a Protobuf message."""
        local_message = Message(comment, avro_record['name'], [], [], {}, {}, [])
        offs = 1
        for i, f in enumerate(avro_record['fields']):
            field = self.convert_field(local_message, f, i+offs, proto_files)
            if isinstance(field, Oneof):
                for f in field.fields:
                    local_message.dependencies.extend(f.dependencies)
                local_message.oneofs.append(field)
                offs += len(field.fields)-1
            elif isinstance(field, Enum):
                enum = Enum(field.comment, self.compose_name(field.name,'enum'), field.fields)
                local_message.enums[enum.name] = enum
                local_message.fields.append(Field(field.comment, '', enum.name, '', '', field.name.split('.')[-1], i+offs, []))
            elif isinstance(field, Message):
                inner_message = Message(field.comment, self.compose_name(field.name,'type'), field.fields, field.oneofs, field.messages, field.enums, [])
                local_message.messages[inner_message.name] = inner_message
                local_message.fields.append(Field(field.comment, '', inner_message.name, '', '', field.name.split('.')[-1], i+offs, []))
                local_message.dependencies.extend(field.dependencies)
            else:
                local_message.dependencies.extend(field.dependencies)
                local_message.fields.append(field)
        return local_message

    def convert_field_type(self, message: Message, field_name: str, field_type: str | dict | list, comment: Comment, index: int, proto_files: ProtoFiles) -> Field | Oneof | Enum | Message:
        """Convert an Avro field type to a Protobuf field type."""
        label = ''
        
        if isinstance(field_type, list):
            # Handling union types (including nullable fields)
            non_null_types = [t for t in field_type if t != 'null']
            if len(non_null_types) == 1:
                if self.allow_optional:
                    label = 'optional'
                field_type = non_null_types[0]
            elif len(non_null_types) > 0:
                oneof_fields = []
                for i, t in enumerate(non_null_types):
                    field = self.convert_field_type(message, self.compose_name(field_name,'choice', 'field'), t, comment, i+index, proto_files)
                    if isinstance(field, Field):
                        if field.type == 'map' or field.type == 'array':
                            local_message = Message(comment, self.compose_name(field.name,field.type), [], [], {}, {}, field.dependencies)
                            local_message.fields.append(field)
                            new_field = Field(field.comment, '', local_message.name, '', '', self.compose_name(field.name.split('.')[-1],field.type, 'field'), i+index, field.dependencies)
                            message.messages[local_message.name] = local_message
                            oneof_fields.append(new_field)
                        else:
                            field = Field(field.comment, field.label, field.type, field.key_type, field.val_type, self.compose_name(field_name, (field.type.split('.')[-1]), 'field'), i+index, field.dependencies)
                            oneof_fields.append(field)
                    elif isinstance(field, Oneof):
                        deps: List[str] = []
                        oneof = field
                        for f in oneof.fields:
                            deps.extend(f.dependencies)
                        local_message = Message(comment, self.compose_name(field.name,'choice'), [], [], {}, {}, deps)
                        index += len(field.fields)
                        local_message.oneofs.append(field)
                        new_field = Field(field.comment, '', local_message.name, '', '', field.name.split('.')[-1], i+index, deps)
                        message.messages[local_message.name] = local_message
                        oneof_fields.append(new_field)
                    elif isinstance(field, Enum):
                        enum = Enum(field.comment, self.compose_name(field.name,"options"), field.fields)
                        message.enums[enum.name] = enum
                        field = Field(field.comment, '', enum.name, '', '', field.name.split('.')[-1], i+index, [])
                        oneof_fields.append(field)
                    elif isinstance(field, Message):
                        local_message = Message(field.comment, self.compose_name(field.name,'type'), field.fields, field.oneofs, field.messages, field.enums, field.dependencies)
                        message.messages[local_message.name] = local_message
                        field = Field(field.comment, '', local_message.name, '', '', field.name.split('.')[-1], i+index, field.dependencies)
                        oneof_fields.append(field)
                oneof = Oneof(comment, field_name, copy.deepcopy(oneof_fields))
                return oneof
            else:
                raise ValueError(f"Field {field_name} is a union type without any non-null types")

        if isinstance(field_type, dict):
            # Nested types (e.g., records, enums) require special handling
            if field_type['type'] == 'record':
                return self.convert_record_type(field_type, comment, proto_files)
            elif field_type['type'] == 'enum':
                enum_symbols = {symbol: Field(comment, '', symbol, '', '', symbol, s, []) for s, symbol in enumerate(field_type['symbols'])}
                return Enum(comment, field_type['name'], enum_symbols)
            elif field_type['type'] == 'array':
                converted_field_type = self.convert_field_type(message, self.compose_name(field_name, "item"), field_type['items'], comment, index, proto_files)
                if isinstance(converted_field_type, Field):
                    return Field(comment, 'repeated', 'array', '', converted_field_type.type, field_name, index, converted_field_type.dependencies)
                elif isinstance(converted_field_type, Enum):
                    enum = Enum(converted_field_type.comment, self.compose_name(converted_field_type.name,'enum'), converted_field_type.fields)
                    message.enums[enum.name] = enum
                    return Field(comment, 'repeated', 'array', '', enum.name, field_name, index, [])
                elif isinstance(converted_field_type, Message):
                    local_message = Message(converted_field_type.comment, self.compose_name(converted_field_type.name,'type'), converted_field_type.fields, converted_field_type.oneofs, converted_field_type.messages, converted_field_type.enums, converted_field_type.dependencies)
                    message.messages[local_message.name] = local_message
                    return Field(comment, 'repeated', 'array', '', local_message.name, field_name, index, [])
                elif isinstance(converted_field_type, Oneof):
                    deps3: List[str] = []
                    fl = []
                    for i, f in enumerate(converted_field_type.fields):
                        fl.append(Field(Comment('',{}), '', f.type, '', '', f.name, i+1, []))
                        deps3.extend(f.dependencies)
                    oneof = Oneof(converted_field_type.comment, 'item', fl)
                    local_message = Message(comment, self.compose_name(field_name,'type'), [], [], {}, {}, deps3)
                    local_message.oneofs.append(oneof)
                    new_field = Field(Comment('',{}), 'repeated', 'array', '', local_message.name, field_name.split('.')[-1], index, local_message.dependencies)
                    message.messages[local_message.name] = local_message
                    return new_field
            elif field_type['type'] == 'map':
                converted_field_type = self.convert_field_type(message, self.compose_name(field_name,'item', 'field'), field_type['values'], comment, index, proto_files)
                if isinstance(converted_field_type, Field):
                    return Field(comment, label, 'map', 'string', converted_field_type.type, field_name, index, converted_field_type.dependencies)
                elif isinstance(converted_field_type, Enum):
                    enum = Enum(converted_field_type.comment, self.compose_name(converted_field_type.name,'enum'), converted_field_type.fields)
                    message.enums[enum.name] = enum
                    return Field(comment, label, 'map', 'string', enum.name, field_name, index, [])
                elif isinstance(converted_field_type, Message):
                    local_message = Message(converted_field_type.comment, self.compose_name(converted_field_type.name,'type'), converted_field_type.fields, converted_field_type.oneofs, converted_field_type.messages, converted_field_type.enums, [])
                    message.messages[local_message.name] = local_message
                    return Field(comment, label, 'map', 'string', local_message.name, field_name, index, local_message.dependencies)
                elif isinstance(converted_field_type, Oneof):
                    deps4: List[str] = []
                    fl = []
                    for i, f in enumerate(converted_field_type.fields):
                        fl.append(Field(Comment('',{}), '', f.type, '', '', f.name, i+1, []))
                        deps4.extend(f.dependencies)
                    oneof = Oneof(converted_field_type.comment, 'item', fl)
                    local_message = Message(comment, self.compose_name(field_name, 'type'), [], [], {}, {}, deps4)
                    local_message.oneofs.append(oneof)
                    new_field = Field(Comment('',{}), label, 'map', 'string', local_message.name, field_name.split('.')[-1], index, local_message.dependencies)
                    message.messages[local_message.name] = local_message
                    return new_field
            elif field_type['type'] == "fixed":
                return Field(comment, label, 'fixed','string', 'string', field_name, index, [])
            else:
                deps1: List[str] = []
                proto_type = self.avro_primitive_to_proto_type(field_type['type'], deps1)
                return Field(comment, label, proto_type, '', '', field_name, index, deps1)
        elif isinstance(field_type, str):
            deps2: List[str] = []
            proto_type = self.avro_primitive_to_proto_type(field_type, deps2)
            return Field(comment, label, proto_type, '', '', field_name, index, deps2)
        raise ValueError(f"Unknown field type {field_type}")

    def avro_schema_to_proto_message(self, avro_schema: dict, proto_files: ProtoFiles) -> str:
        """Convert an Avro schema to a Protobuf message definition."""
        comment = Comment('',{})
        if 'doc' in avro_schema:
            comment = Comment(avro_schema["doc"], {})
        namespace = avro_schema.get("namespace", '')
        if not namespace:
            namespace = self.default_namespace
        if avro_schema['type'] == 'record':
            message = self.convert_record_type(avro_schema, comment, proto_files)
            file = next((f for f in proto_files.files if f.package == namespace), None)
            if not file:
                file = ProtoFile({}, {}, {}, [], {}, namespace)
                proto_files.files.append(file)
            file.messages[message.name] = message
        elif avro_schema['type'] == 'enum':
            enum_name = avro_schema['name']
            enum_symbols = {symbol: Field(comment, '', symbol, '', '', symbol, s, []) for s, symbol in enumerate(avro_schema['symbols'])}
            enum = Enum(comment, enum_name, enum_symbols)
            file = next((f for f in proto_files.files if f.package == namespace), None)
            if not file:
                file = ProtoFile({}, {}, {}, [], {}, namespace)
                proto_files.files.append(file)
            file.enums[enum_name] = enum
        return avro_schema["name"]
        
    def avro_schema_to_proto_messages(self, avro_schema_input, proto_files: ProtoFiles):
        """Convert an Avro schema to Protobuf message definitions."""
        if not isinstance(avro_schema_input, list):
            avro_schema_list = [avro_schema_input]
        else:
            avro_schema_list = avro_schema_input    
        for avro_schema in avro_schema_list:
            self.avro_schema_to_proto_message(avro_schema, proto_files)

    def save_proto_to_file(self, proto_files: ProtoFiles, proto_path):
        """Save the Protobuf schema to a file."""
        for proto in proto_files.files:
            # gather dependencies that are within the package
            deps: List[str] = []
            for message in proto.messages.values():
                for dep in message.dependencies:
                    if '.' in dep:
                        deps.append(dep.rsplit('.',1)[0])
            deps = list(set(deps))

            #proto.imports.extend([f.package[len(proto.package)+1:] for f in proto_files.files if f.package.startswith(proto.package) and f.package != proto.package])
            proto.imports.extend([d for d in deps if d != proto.package])
            proto_file_path = os.path.join(proto_path, f"{proto.package}.proto")
            # create the directory for the proto file if it doesn't exist
            proto_dir = os.path.dirname(proto_file_path)
            if not os.path.exists(proto_dir):
                os.makedirs(proto_dir, exist_ok=True)
            with open(proto_file_path, 'w') as proto_file:
                # dump the ProtoFile structure in proto syntax
                proto_str = f'syntax = "proto3";\n\n'
                proto_str += f'package {proto.package};\n\n'

                for import_package in proto.imports:
                    proto_str += f"import \"{import_package}.proto\";\n"
                if (len(proto.imports)):
                    proto_str += "\n"
                for enum_name, enum in proto.enums.items():
                    proto_str += f"enum {enum_name} {{\n"
                    for _, field in enum.fields.items():
                        proto_str += f"{indent}{field.name} = {field.number};\n"
                    proto_str += "}\n\n"
                for message in proto.messages.values():
                    proto_str += self.render_message(message)
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

    def render_message(self, message, level=0) -> str:
        proto_str = f"{indent*level}message {message.name} {{\n"
        fieldsAndOneofs = message.fields+message.oneofs
        fieldsAndOneofs.sort(key=lambda f: f.number if isinstance(f, Field) else f.fields[0].number)
        for fo in fieldsAndOneofs:
            if isinstance(fo, Field):
                field = fo
                if field.type == "map":
                    proto_str += f"{indent*level}{indent}{field.label}{' ' if field.label else ''}map<{field.key_type}, {field.val_type}> {field.name} = {field.number};\n"
                elif field.type == "array":
                    proto_str += f"{indent*level}{indent}{field.label}{' ' if field.label else ''}{field.val_type} {field.name} = {field.number};\n"
                else:
                    proto_str += f"{indent*level}{indent}{field.label}{' ' if field.label else ''}{field.type} {field.name} = {field.number};\n"
            else:
                oneof = fo
                proto_str += f"{indent*level}{indent}oneof {oneof.name} {{\n"
                for field in oneof.fields:
                    proto_str += f"{indent*level}{indent}{indent}{field.label}{' ' if field.label else ''}{field.type} {field.name} = {field.number};\n"
                proto_str += f"{indent*level}{indent}}}\n"
        for enum in message.enums.values():
            proto_str += f"{indent*level}{indent}enum {enum.name} {{\n"
            for _, field in enum.fields.items():
                proto_str += f"{indent*level}{indent}{indent}{field.label}{' ' if field.label else ''}{field.name} = {field.number};\n"
            proto_str += f"{indent*level}{indent}}}\n"
        for local_message in message.messages.values():
            proto_str += self.render_message(local_message, level+1)
        proto_str += f"{indent*level}}}\n"
        return proto_str
            

    def convert_avro_to_proto(self, avro_schema_path, proto_file_path):
        """Convert Avro schema file to Protobuf .proto file."""
        with open(avro_schema_path, 'r') as avro_file:
            avro_schema = json.load(avro_file)
        proto_files = ProtoFiles([])
        self.avro_schema_to_proto_messages(avro_schema, proto_files)
        self.save_proto_to_file(proto_files, proto_file_path)

def convert_avro_to_proto(avro_schema_path, proto_file_path, naming_mode: Literal['snake', 'pascal', 'camel'] = 'pascal', allow_optional: bool = False):
    avrotoproto = AvroToProto()
    avrotoproto.naming_mode = naming_mode
    avrotoproto.allow_optional = allow_optional
    avrotoproto.default_namespace = os.path.splitext(os.path.basename(proto_file_path))[0].replace('-','_')
    avrotoproto.convert_avro_to_proto(avro_schema_path, proto_file_path)