# pylint: disable=line-too-long

""" StructureToProto class for converting JSON Structure schemas to Protocol Buffers """

import copy
import json
import os
from typing import Literal, NamedTuple, Dict, Any, List, Optional

# Data structures for Protocol Buffers schema representation
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

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | None

indent = '  '


def pascal(name: str) -> str:
    """Convert a name to PascalCase"""
    if not name:
        return name
    # Handle snake_case and kebab-case
    parts = name.replace('-', '_').split('_')
    return ''.join(word.capitalize() for word in parts if word)


def camel(name: str) -> str:
    """Convert a name to camelCase"""
    pascal_name = pascal(name)
    if not pascal_name:
        return pascal_name
    return pascal_name[0].lower() + pascal_name[1:]


class StructureToProto:
    """Converts JSON Structure schemas to Protocol Buffers .proto files"""

    def __init__(self) -> None:
        self.naming_mode: Literal['snake', 'pascal', 'camel'] = 'pascal'
        self.allow_optional: bool = False
        self.default_namespace: str = ''
        self.schema_doc: JsonNode = None
        self.schema_registry: Dict[str, Dict] = {}
        self.definitions: Dict[str, Any] = {}
        self.generated_types: Dict[str, str] = {}

    def map_primitive_to_proto(self, structure_type: str, dependencies: List[str]) -> str:
        """Map JSON Structure primitive types to Protobuf types"""
        mapping = {
            'null': 'google.protobuf.Empty',
            'boolean': 'bool',
            'string': 'string',
            'integer': 'int32',
            'number': 'double',
            'int8': 'int32',
            'uint8': 'uint32',
            'int16': 'int32',
            'uint16': 'uint32',
            'int32': 'int32',
            'uint32': 'uint32',
            'int64': 'int64',
            'uint64': 'uint64',
            'int128': 'string',  # No native 128-bit int in proto3
            'uint128': 'string',
            'float8': 'float',
            'float': 'float',
            'double': 'double',
            'binary32': 'float',
            'binary64': 'double',
            'decimal': 'string',  # Decimal as string or use google.type.Decimal
            'binary': 'bytes',
            'date': 'string',  # Use google.type.Date or ISO 8601 string
            'time': 'string',  # Use google.type.TimeOfDay or ISO 8601 string
            'datetime': 'string',  # Use google.protobuf.Timestamp or ISO 8601 string
            'timestamp': 'string',
            'duration': 'string',  # Use google.protobuf.Duration or ISO 8601 string
            'uuid': 'string',
            'uri': 'string',
            'jsonpointer': 'string',
            'any': 'google.protobuf.Any'
        }
        
        proto_type = mapping.get(structure_type, '')
        if not proto_type:
            # It's a custom type reference
            dependencies.append(structure_type)
            proto_type = structure_type
        return proto_type

    def compose_name(self, prefix: str, name: str, naming_mode: Literal['pascal', 'camel', 'snake', 'default', 'field'] = 'default') -> str:
        """Compose a name with a prefix according to naming convention"""
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
            return f"{pascal(prefix) if prefix else ''}{pascal(name) if name else ''}"
        if naming_mode == 'camel':
            return camel(f"{prefix}{pascal(name) if name else ''}")
        return prefix + name

    def resolve_ref(self, ref: str, context_schema: Optional[Dict] = None) -> Optional[Dict]:
        """Resolves a $ref to the actual schema definition"""
        # Check if it's an absolute URI reference
        if not ref.startswith('#/'):
            if ref in self.schema_registry:
                return self.schema_registry[ref]
            return None
        
        # Handle fragment-only references (internal to document)
        path = ref[2:].split('/')
        schema = context_schema if context_schema else self.schema_doc
        
        for part in path:
            if not isinstance(schema, dict) or part not in schema:
                return None
            schema = schema[part]
        
        return schema

    def register_schema_ids(self, schema: Dict, base_uri: str = '') -> None:
        """Recursively registers schemas with $id keywords"""
        if not isinstance(schema, dict):
            return
        
        # Register this schema if it has an $id
        if '$id' in schema:
            schema_id = schema['$id']
            # Handle relative URIs
            if base_uri and not schema_id.startswith(('http://', 'https://', 'urn:')):
                from urllib.parse import urljoin
                schema_id = urljoin(base_uri, schema_id)
            self.schema_registry[schema_id] = schema
            base_uri = schema_id
        
        # Recursively process definitions
        if 'definitions' in schema:
            for def_name, def_schema in schema['definitions'].items():
                if isinstance(def_schema, dict):
                    self.register_schema_ids(def_schema, base_uri)
        
        # Recursively process properties
        if 'properties' in schema:
            for prop_name, prop_schema in schema['properties'].items():
                if isinstance(prop_schema, dict):
                    self.register_schema_ids(prop_schema, base_uri)
        
        # Recursively process items, values, etc.
        for key in ['items', 'values', 'additionalProperties']:
            if key in schema and isinstance(schema[key], dict):
                self.register_schema_ids(schema[key], base_uri)

    def convert_structure_type_to_proto(self, message: Message, field_name: str, 
                                       field_type: JsonNode, comment: Comment, 
                                       index: int, proto_files: ProtoFiles) -> Field | Oneof | Enum | Message:
        """Convert a JSON Structure type to a Protobuf field type"""
        label = ''
        
        if isinstance(field_type, list):
            # Handle union types (including nullable fields)
            non_null_types = [t for t in field_type if t != 'null']
            if len(non_null_types) == 1:
                if self.allow_optional:
                    label = 'optional'
                field_type = non_null_types[0]
            elif len(non_null_types) > 0:
                # Multiple non-null types - create a oneof
                oneof_fields = []
                for i, t in enumerate(non_null_types):
                    field = self.convert_structure_type_to_proto(
                        message, self.compose_name(field_name, 'choice', 'field'), 
                        t, comment, i+index, proto_files)
                    if isinstance(field, Field):
                        if field.type == 'map' or field.type == 'array':
                            local_message = Message(comment, self.compose_name(field.name, field.type), 
                                                   [], [], {}, {}, field.dependencies)
                            local_message.fields.append(field)
                            new_field = Field(field.comment, '', local_message.name, '', '', 
                                            self.compose_name(field.name.split('.')[-1], field.type, 'field'), 
                                            i+index, field.dependencies)
                            message.messages[local_message.name] = local_message
                            oneof_fields.append(new_field)
                        else:
                            field = Field(field.comment, field.label, field.type, field.key_type, 
                                        field.val_type, self.compose_name(field_name, field.type.split('.')[-1], 'field'), 
                                        i+index, field.dependencies)
                            oneof_fields.append(field)
                    elif isinstance(field, Enum):
                        enum = Enum(field.comment, self.compose_name(field.name, "options"), field.fields)
                        message.enums[enum.name] = enum
                        field = Field(field.comment, '', enum.name, '', '', field.name.split('.')[-1], i+index, [])
                        oneof_fields.append(field)
                    elif isinstance(field, Message):
                        local_message = Message(field.comment, self.compose_name(field.name, 'type'), 
                                              field.fields, field.oneofs, field.messages, field.enums, field.dependencies)
                        message.messages[local_message.name] = local_message
                        field = Field(field.comment, '', local_message.name, '', '', field.name.split('.')[-1], i+index, field.dependencies)
                        oneof_fields.append(field)
                oneof = Oneof(comment, field_name, copy.deepcopy(oneof_fields))
                return oneof
            else:
                raise ValueError(f"Field {field_name} is a union type without any non-null types")

        if isinstance(field_type, dict):
            # Handle $ref
            if '$ref' in field_type:
                ref_schema = self.resolve_ref(field_type['$ref'], self.schema_doc)
                if ref_schema:
                    ref_path = field_type['$ref'].split('/')
                    type_name = ref_path[-1]
                    ref_namespace = '.'.join(ref_path[2:-1]) if len(ref_path) > 3 else ''
                    return self.convert_record_or_enum(ref_schema, comment, proto_files, explicit_name=type_name)
                deps: List[str] = []
                return Field(comment, label, 'google.protobuf.Any', '', '', field_name, index, deps)
            
            # Handle enum keyword
            if 'enum' in field_type:
                enum_values = field_type.get('enum', [])
                enum_name = pascal(field_name) + 'Enum' if field_name else 'UnnamedEnum'
                enum_symbols = {str(symbol): Field(comment, '', str(symbol), '', '', str(symbol), s, []) 
                               for s, symbol in enumerate(enum_values)}
                return Enum(comment, enum_name, enum_symbols)
            
            # Handle type keyword
            if 'type' not in field_type:
                deps1: List[str] = []
                return Field(comment, label, 'google.protobuf.Any', '', '', field_name, index, deps1)
            
            struct_type = field_type['type']
            
            # If struct_type is itself a dict with $ref, resolve it recursively
            if isinstance(struct_type, dict):
                return self.convert_structure_type_to_proto(message, field_name, struct_type, comment, index, proto_files)
            
            # Handle complex types
            if struct_type == 'object':
                return self.convert_object_type(field_type, comment, proto_files, explicit_name=field_name)
            elif struct_type == 'array':
                converted_field_type = self.convert_structure_type_to_proto(
                    message, self.compose_name(field_name, "item"), 
                    field_type.get('items', {'type': 'any'}), comment, index, proto_files)
                if isinstance(converted_field_type, Field):
                    return Field(comment, 'repeated', 'array', '', converted_field_type.type, 
                               field_name, index, converted_field_type.dependencies)
                elif isinstance(converted_field_type, Enum):
                    enum = Enum(converted_field_type.comment, 
                              self.compose_name(converted_field_type.name, 'enum'), 
                              converted_field_type.fields)
                    message.enums[enum.name] = enum
                    return Field(comment, 'repeated', 'array', '', enum.name, field_name, index, [])
                elif isinstance(converted_field_type, Message):
                    local_message = Message(converted_field_type.comment, 
                                          self.compose_name(converted_field_type.name, 'type'), 
                                          converted_field_type.fields, converted_field_type.oneofs, 
                                          converted_field_type.messages, converted_field_type.enums, 
                                          converted_field_type.dependencies)
                    message.messages[local_message.name] = local_message
                    return Field(comment, 'repeated', 'array', '', local_message.name, field_name, index, [])
            elif struct_type == 'set':
                # Sets are represented as repeated fields in protobuf
                converted_field_type = self.convert_structure_type_to_proto(
                    message, self.compose_name(field_name, "item"), 
                    field_type.get('items', {'type': 'any'}), comment, index, proto_files)
                if isinstance(converted_field_type, Field):
                    return Field(comment, 'repeated', 'array', '', converted_field_type.type, 
                               field_name, index, converted_field_type.dependencies)
                elif isinstance(converted_field_type, Enum):
                    enum = Enum(converted_field_type.comment, 
                              self.compose_name(converted_field_type.name, 'enum'), 
                              converted_field_type.fields)
                    message.enums[enum.name] = enum
                    return Field(comment, 'repeated', 'array', '', enum.name, field_name, index, [])
                elif isinstance(converted_field_type, Message):
                    local_message = Message(converted_field_type.comment, 
                                          self.compose_name(converted_field_type.name, 'type'), 
                                          converted_field_type.fields, converted_field_type.oneofs, 
                                          converted_field_type.messages, converted_field_type.enums, 
                                          converted_field_type.dependencies)
                    message.messages[local_message.name] = local_message
                    return Field(comment, 'repeated', 'array', '', local_message.name, field_name, index, [])
            elif struct_type == 'map':
                converted_field_type = self.convert_structure_type_to_proto(
                    message, self.compose_name(field_name, 'item', 'field'), 
                    field_type.get('values', {'type': 'any'}), comment, index, proto_files)
                if isinstance(converted_field_type, Field):
                    return Field(comment, label, 'map', 'string', converted_field_type.type, 
                               field_name, index, converted_field_type.dependencies)
                elif isinstance(converted_field_type, Enum):
                    enum = Enum(converted_field_type.comment, 
                              self.compose_name(converted_field_type.name, 'enum'), 
                              converted_field_type.fields)
                    message.enums[enum.name] = enum
                    return Field(comment, label, 'map', 'string', enum.name, field_name, index, [])
                elif isinstance(converted_field_type, Message):
                    local_message = Message(converted_field_type.comment, 
                                          self.compose_name(converted_field_type.name, 'type'), 
                                          converted_field_type.fields, converted_field_type.oneofs, 
                                          converted_field_type.messages, converted_field_type.enums, [])
                    message.messages[local_message.name] = local_message
                    return Field(comment, label, 'map', 'string', local_message.name, field_name, index, 
                               local_message.dependencies)
            elif struct_type == 'tuple':
                # Tuples are converted to messages with ordered fields
                return self.convert_tuple_type(field_type, comment, proto_files, explicit_name=field_name)
            elif struct_type == 'choice':
                # Choices are converted to oneofs
                return self.convert_choice_type(field_type, field_name, comment, index, message, proto_files)
            else:
                # Primitive type
                deps2: List[str] = []
                proto_type = self.map_primitive_to_proto(struct_type, deps2)
                return Field(comment, label, proto_type, '', '', field_name, index, deps2)
        elif isinstance(field_type, str):
            # Primitive type as string
            deps3: List[str] = []
            proto_type = self.map_primitive_to_proto(field_type, deps3)
            return Field(comment, label, proto_type, '', '', field_name, index, deps3)
        
        # Default case
        deps4: List[str] = []
        return Field(comment, label, 'google.protobuf.Any', '', '', field_name, index, deps4)

    def convert_object_type(self, structure_schema: Dict, comment: Comment, 
                           proto_files: ProtoFiles, explicit_name: str = '') -> Message:
        """Convert a JSON Structure object type to a Protobuf message"""
        class_name = pascal(explicit_name if explicit_name else structure_schema.get('name', 'UnnamedMessage'))
        
        local_message = Message(comment, class_name, [], [], {}, {}, [])
        
        properties = structure_schema.get('properties', {})
        offs = 1
        for i, (prop_name, prop_schema) in enumerate(properties.items()):
            prop_comment = Comment(prop_schema.get('description', prop_schema.get('doc', '')), {})
            field = self.convert_structure_type_to_proto(
                local_message, prop_name, prop_schema, prop_comment, i+offs, proto_files)
            
            if isinstance(field, Oneof):
                for f in field.fields:
                    local_message.dependencies.extend(f.dependencies)
                local_message.oneofs.append(field)
                offs += len(field.fields) - 1
            elif isinstance(field, Enum):
                enum = Enum(field.comment, self.compose_name(field.name, 'enum'), field.fields)
                local_message.enums[enum.name] = enum
                local_message.fields.append(Field(field.comment, '', enum.name, '', '', 
                                                 field.name.split('.')[-1], i+offs, []))
            elif isinstance(field, Message):
                inner_message = Message(field.comment, self.compose_name(field.name, 'type'), 
                                       field.fields, field.oneofs, field.messages, field.enums, [])
                local_message.messages[inner_message.name] = inner_message
                local_message.fields.append(Field(field.comment, '', inner_message.name, '', '', 
                                                 field.name.split('.')[-1], i+offs, []))
                local_message.dependencies.extend(field.dependencies)
            else:
                local_message.dependencies.extend(field.dependencies)
                local_message.fields.append(field)
        
        return local_message

    def convert_tuple_type(self, structure_schema: Dict, comment: Comment, 
                          proto_files: ProtoFiles, explicit_name: str = '') -> Message:
        """Convert a JSON Structure tuple type to a Protobuf message"""
        class_name = pascal(explicit_name if explicit_name else structure_schema.get('name', 'UnnamedTuple'))
        
        local_message = Message(comment, class_name, [], [], {}, {}, [])
        
        properties = structure_schema.get('properties', {})
        tuple_order = structure_schema.get('tuple', [])
        
        for i, prop_name in enumerate(tuple_order):
            if prop_name in properties:
                prop_schema = properties[prop_name]
                prop_comment = Comment(prop_schema.get('description', prop_schema.get('doc', '')), {})
                field = self.convert_structure_type_to_proto(
                    local_message, prop_name, prop_schema, prop_comment, i+1, proto_files)
                
                if isinstance(field, Field):
                    local_message.dependencies.extend(field.dependencies)
                    local_message.fields.append(field)
                elif isinstance(field, Message):
                    inner_message = Message(field.comment, self.compose_name(field.name, 'type'), 
                                           field.fields, field.oneofs, field.messages, field.enums, [])
                    local_message.messages[inner_message.name] = inner_message
                    local_message.fields.append(Field(field.comment, '', inner_message.name, '', '', 
                                                     field.name.split('.')[-1], i+1, []))
        
        return local_message

    def convert_choice_type(self, structure_schema: Dict, field_name: str, comment: Comment, 
                           index: int, parent_message: Message, proto_files: ProtoFiles) -> Oneof:
        """Convert a JSON Structure choice type to a Protobuf oneof"""
        choices = structure_schema.get('choices', {})
        oneof_fields = []
        
        for i, (choice_name, choice_schema) in enumerate(choices.items()):
            choice_comment = Comment(choice_schema.get('description', choice_schema.get('doc', '')) if isinstance(choice_schema, dict) else '', {})
            field = self.convert_structure_type_to_proto(
                parent_message, choice_name, choice_schema, choice_comment, i+index, proto_files)
            
            if isinstance(field, Field):
                oneof_fields.append(field)
            elif isinstance(field, Message):
                local_message = Message(field.comment, self.compose_name(field.name, 'type'), 
                                       field.fields, field.oneofs, field.messages, field.enums, field.dependencies)
                parent_message.messages[local_message.name] = local_message
                oneof_fields.append(Field(field.comment, '', local_message.name, '', '', 
                                         field.name.split('.')[-1], i+index, field.dependencies))
        
        return Oneof(comment, field_name, oneof_fields)

    def convert_record_or_enum(self, structure_schema: Dict, comment: Comment, 
                              proto_files: ProtoFiles, explicit_name: str = '') -> Message | Enum | Field:
        """Convert a JSON Structure schema to a Protobuf message, enum, or field"""
        if 'enum' in structure_schema:
            # It's an enum
            enum_name = pascal(explicit_name if explicit_name else structure_schema.get('name', 'UnnamedEnum'))
            enum_values = structure_schema.get('enum', [])
            enum_symbols = {str(symbol): Field(comment, '', str(symbol), '', '', str(symbol), s, []) 
                           for s, symbol in enumerate(enum_values)}
            return Enum(comment, enum_name, enum_symbols)
        elif structure_schema.get('type') == 'array':
            # It's an array type - handle specially
            # Create a temporary message to hold the conversion context
            temp_message = Message(comment, 'TempMessage', [], [], {}, {}, [])
            return self.convert_structure_type_to_proto(
                temp_message, explicit_name, structure_schema, comment, 1, proto_files)
        elif structure_schema.get('type') == 'map':
            # It's a map type - handle specially
            temp_message = Message(comment, 'TempMessage', [], [], {}, {}, [])
            return self.convert_structure_type_to_proto(
                temp_message, explicit_name, structure_schema, comment, 1, proto_files)
        else:
            # It's an object/message
            return self.convert_object_type(structure_schema, comment, proto_files, explicit_name=explicit_name)

    def structure_schema_to_proto_message(self, structure_schema: dict, proto_files: ProtoFiles) -> str:
        """Convert a JSON Structure schema to a Protobuf message definition"""
        comment = Comment('', {})
        if 'description' in structure_schema:
            comment = Comment(structure_schema["description"], {})
        elif 'doc' in structure_schema:
            comment = Comment(structure_schema["doc"], {})
        
        namespace = structure_schema.get("namespace", '')
        if not namespace:
            namespace = self.default_namespace
        
        name = structure_schema.get('name', 'Document')
        
        if 'enum' in structure_schema:
            # It's an enum
            enum = self.convert_record_or_enum(structure_schema, comment, proto_files, explicit_name=name)
            if isinstance(enum, Enum):
                file = next((f for f in proto_files.files if f.package == namespace), None)
                if not file:
                    file = ProtoFile({}, {}, {}, [], {}, namespace)
                    proto_files.files.append(file)
                file.enums[enum.name] = enum
        elif structure_schema.get('type') == 'object':
            # It's an object/message
            message = self.convert_object_type(structure_schema, comment, proto_files, explicit_name=name)
            file = next((f for f in proto_files.files if f.package == namespace), None)
            if not file:
                file = ProtoFile({}, {}, {}, [], {}, namespace)
                proto_files.files.append(file)
            file.messages[message.name] = message
        
        return name

    def structure_schemas_to_proto_messages(self, structure_schemas, proto_files: ProtoFiles):
        """Convert multiple JSON Structure schemas to Protobuf message definitions"""
        if not isinstance(structure_schemas, list):
            structure_schemas = [structure_schemas]
        
        for structure_schema in structure_schemas:
            if isinstance(structure_schema, dict):
                self.structure_schema_to_proto_message(structure_schema, proto_files)

    def save_proto_to_file(self, proto_files: ProtoFiles, proto_path):
        """Save the Protobuf schema to a file"""
        for proto in proto_files.files:
            # Gather dependencies that are within the package
            deps: List[str] = []
            for message in proto.messages.values():
                for dep in message.dependencies:
                    if '.' in dep:
                        deps.append(dep.rsplit('.', 1)[0])
            deps = list(set(deps))

            proto.imports.extend([d for d in deps if d != proto.package])
            proto_file_path = os.path.join(proto_path, f"{proto.package}.proto")
            
            # Create the directory for the proto file if it doesn't exist
            proto_dir = os.path.dirname(proto_file_path)
            if not os.path.exists(proto_dir):
                os.makedirs(proto_dir, exist_ok=True)
            
            with open(proto_file_path, 'w', encoding='utf-8') as proto_file:
                # Dump the ProtoFile structure in proto syntax
                proto_str = 'syntax = "proto3";\n\n'
                proto_str += f'package {proto.package};\n\n'

                for import_package in proto.imports:
                    proto_str += f'import "{import_package}.proto";\n'
                if len(proto.imports):
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
        """Render a Protobuf message to string"""
        proto_str = f"{indent*level}message {message.name} {{\n"
        fields_and_oneofs = message.fields + message.oneofs
        fields_and_oneofs.sort(key=lambda f: f.number if isinstance(f, Field) else f.fields[0].number)
        
        for fo in fields_and_oneofs:
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

    def convert_structure_to_proto(self, structure_schema_path, proto_file_path):
        """Convert JSON Structure schema file to Protobuf .proto file"""
        with open(structure_schema_path, 'r', encoding='utf-8') as structure_file:
            structure_schema = json.load(structure_file)
        
        proto_files = ProtoFiles([])
        
        # Register schema IDs for $ref resolution
        if isinstance(structure_schema, list):
            for schema in structure_schema:
                if isinstance(schema, dict):
                    self.schema_doc = schema
                    self.register_schema_ids(schema)
        else:
            self.schema_doc = structure_schema
            self.register_schema_ids(structure_schema)
        
        self.structure_schemas_to_proto_messages(structure_schema, proto_files)
        self.save_proto_to_file(proto_files, proto_file_path)


def convert_structure_to_proto(structure_schema_path, proto_file_path, 
                               naming_mode: Literal['snake', 'pascal', 'camel'] = 'pascal', 
                               allow_optional: bool = False):
    """Convert JSON Structure schema to Protocol Buffers .proto file
    
    Args:
        structure_schema_path: Path to the JSON Structure schema file
        proto_file_path: Output path for the .proto file
        naming_mode: Naming convention for types ('snake', 'pascal', 'camel')
        allow_optional: Whether to use optional keyword in proto3
    """
    structuretoproto = StructureToProto()
    structuretoproto.naming_mode = naming_mode
    structuretoproto.allow_optional = allow_optional
    structuretoproto.default_namespace = os.path.splitext(os.path.basename(proto_file_path))[0].replace('-', '_')
    structuretoproto.convert_structure_to_proto(structure_schema_path, proto_file_path)
