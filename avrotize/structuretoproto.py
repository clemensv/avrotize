# pylint: disable=line-too-long

""" StructureToProto class for converting JSON Structure schema to Protocol Buffers (.proto files) """

import json
import os
import argparse
from typing import Literal, NamedTuple, Dict, Any, List, Optional

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

class StructureToProto:
    
    def __init__(self) -> None:
        self.naming_mode: Literal['snake', 'pascal', 'camel'] = 'pascal'
        self.allow_optional: bool = False
        self.default_namespace: str = ''
        self.schema_registry: Dict[str, Dict] = {}
        self.definitions: Dict[str, Any] = {}

    def structure_primitive_to_proto_type(self, structure_type: str, dependencies: List[str]) -> str:
        """Map JSON Structure primitive types to Protobuf types."""
        mapping = {
            'null': 'google.protobuf.Empty',
            'boolean': 'bool',
            'string': 'string',
            'integer': 'int32',
            'number': 'double',
            'int8': 'int32',  # Proto doesn't have int8, use int32
            'uint8': 'uint32',
            'int16': 'int32',  # Proto doesn't have int16, use int32
            'uint16': 'uint32',
            'int32': 'int32',
            'uint32': 'uint32',
            'int64': 'int64',
            'uint64': 'uint64',
            'int128': 'string',  # Proto doesn't have int128, use string
            'uint128': 'string',
            'float8': 'float',
            'float': 'float',
            'float32': 'float',
            'float64': 'double',
            'double': 'double',
            'binary32': 'float',
            'binary64': 'double',
            'decimal': 'string',  # Proto doesn't have native decimal, use string
            'binary': 'bytes',
            'bytes': 'bytes',
            'date': 'string',  # Or use google.type.Date
            'time': 'string',  # Or use google.type.TimeOfDay
            'datetime': 'string',  # Or use google.protobuf.Timestamp
            'timestamp': 'string',
            'duration': 'string',  # Or use google.protobuf.Duration
            'uuid': 'string',
            'uri': 'string',
            'jsonpointer': 'string',
            'any': 'google.protobuf.Any'
        }
        
        type_result = mapping.get(structure_type, '')
        if not type_result:
            dependencies.append(structure_type)
            type_result = structure_type
        return type_result

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

    def resolve_ref(self, ref: str, context_schema: Optional[Dict] = None) -> Optional[Dict]:
        """ Resolves a $ref to the actual schema definition """
        # Check if it's an absolute URI reference (schema with $id)
        if not ref.startswith('#/'):
            # Try to resolve from schema registry
            if ref in self.schema_registry:
                return self.schema_registry[ref]
            return None
        
        # Handle fragment-only references (internal to document)
        path = ref[2:].split('/')
        schema = context_schema if context_schema else None
        
        if schema is None:
            return None
            
        for part in path:
            if not isinstance(schema, dict) or part not in schema:
                return None
            schema = schema[part]
        
        return schema

    def register_schema_ids(self, schema: Dict, base_uri: str = '') -> None:
        """ Recursively registers schemas with $id keywords """
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
            base_uri = schema_id  # Update base URI for nested schemas
        
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

    def convert_field(self, message: Message, structure_field: dict, index: int, proto_files: ProtoFiles, context_schema: Dict) -> Field | Oneof | Enum | Message:
        """Convert a JSON Structure property to a Protobuf field."""
        field_name = structure_field.get('name', f'field{index}')
        
        if 'doc' in structure_field or 'description' in structure_field:
            comment = Comment(structure_field.get('description', structure_field.get('doc', '')), {})
        else:
            comment = Comment('', {})
        
        return self.convert_field_type(message, field_name, structure_field, comment, index, proto_files, context_schema)
        
    def convert_record_type(self, structure_record: dict, comment: Comment, proto_files: ProtoFiles, context_schema: Dict) -> Message:
        """Convert a JSON Structure object to a Protobuf message."""
        local_message = Message(comment, structure_record.get('name', 'UnnamedMessage'), [], [], {}, {}, [])
        properties = structure_record.get('properties', {})
        required_props = structure_record.get('required', [])
        
        offs = 1
        for i, (prop_name, prop_schema) in enumerate(properties.items()):
            field_dict = {'name': prop_name}
            if isinstance(prop_schema, dict):
                field_dict.update(prop_schema)
            else:
                field_dict['type'] = prop_schema
                
            field = self.convert_field(local_message, field_dict, i+offs, proto_files, context_schema)
            if isinstance(field, Oneof):
                for f in field.fields:
                    local_message.dependencies.extend(f.dependencies)
                local_message.oneofs.append(field)
                offs += len(field.fields)-1
            elif isinstance(field, Enum):
                enum = Enum(field.comment, self.compose_name(field.name, 'enum'), field.fields)
                local_message.enums[enum.name] = enum
                local_message.fields.append(Field(field.comment, '', enum.name, '', '', prop_name, i+offs, []))
            elif isinstance(field, Message):
                inner_message = Message(field.comment, self.compose_name(prop_name, 'type'), field.fields, field.oneofs, field.messages, field.enums, [])
                local_message.messages[inner_message.name] = inner_message
                local_message.fields.append(Field(field.comment, '', inner_message.name, '', '', prop_name, i+offs, []))
                local_message.dependencies.extend(field.dependencies)
            else:
                local_message.dependencies.extend(field.dependencies)
                local_message.fields.append(field)
        return local_message

    def convert_field_type(self, message: Message, field_name: str, field_type_schema: Dict | str | list, comment: Comment, index: int, proto_files: ProtoFiles, context_schema: Dict) -> Field | Oneof | Enum | Message:
        """Convert a JSON Structure field type to a Protobuf field type."""
        label = ''
        
        # Handle list of types (union)
        if isinstance(field_type_schema, list):
            # Handling union types (including nullable fields)
            non_null_types = [t for t in field_type_schema if t != 'null']
            if len(non_null_types) == 1:
                if self.allow_optional:
                    label = 'optional'
                field_type_schema = non_null_types[0]
            elif len(non_null_types) > 0:
                oneof_fields = []
                for i, t in enumerate(non_null_types):
                    field = self.convert_field_type(message, self.compose_name(field_name, 'choice', 'field'), t, comment, i+index, proto_files, context_schema)
                    if isinstance(field, Field):
                        if field.type == 'map' or field.type == 'array':
                            local_message = Message(comment, self.compose_name(field.name, field.type), [], [], {}, {}, field.dependencies)
                            local_message.fields.append(field)
                            new_field = Field(field.comment, '', local_message.name, '', '', self.compose_name(field.name, field.type, 'field'), i+index, field.dependencies)
                            message.messages[local_message.name] = local_message
                            oneof_fields.append(new_field)
                        else:
                            field = Field(field.comment, field.label, field.type, field.key_type, field.val_type, self.compose_name(field_name, (field.type.split('.')[-1]), 'field'), i+index, field.dependencies)
                            oneof_fields.append(field)
                    elif isinstance(field, Enum):
                        enum = Enum(field.comment, self.compose_name(field.name, "options"), field.fields)
                        message.enums[enum.name] = enum
                        field = Field(field.comment, '', enum.name, '', '', field.name, i+index, [])
                        oneof_fields.append(field)
                    elif isinstance(field, Message):
                        local_message = Message(field.comment, self.compose_name(field.name, 'type'), field.fields, field.oneofs, field.messages, field.enums, field.dependencies)
                        message.messages[local_message.name] = local_message
                        field = Field(field.comment, '', local_message.name, '', '', field.name, i+index, field.dependencies)
                        oneof_fields.append(field)
                oneof = Oneof(comment, field_name, oneof_fields)
                return oneof
            else:
                raise ValueError(f"Field {field_name} is a union type without any non-null types")

        # Handle dict types (complex structures)
        if isinstance(field_type_schema, dict):
            # Handle $ref
            if '$ref' in field_type_schema:
                ref_schema = self.resolve_ref(field_type_schema['$ref'], context_schema)
                if ref_schema:
                    return self.convert_field_type(message, field_name, ref_schema, comment, index, proto_files, context_schema)
                else:
                    # Reference not found, use string as fallback
                    deps: List[str] = []
                    return Field(comment, label, 'string', '', '', field_name, index, deps)
            
            # Handle enum keyword
            if 'enum' in field_type_schema:
                enum_values = field_type_schema['enum']
                enum_name = self.compose_name(field_name, 'Enum')
                enum_fields = {str(val): Field(comment, '', str(val), '', '', str(val), i, []) for i, val in enumerate(enum_values)}
                return Enum(comment, enum_name, enum_fields)
            
            # Get the type from the schema
            if 'type' not in field_type_schema:
                # No type specified, use Any
                deps1: List[str] = []
                return Field(comment, label, 'google.protobuf.Any', '', '', field_name, index, deps1)
            
            struct_type = field_type_schema['type']
            
            # Handle type as a list (union type declared inline in "type" field)
            if isinstance(struct_type, list):
                # This is a union type specified as "type": ["string", "null"]
                # Recursively call convert_field_type with the list
                return self.convert_field_type(message, field_name, struct_type, comment, index, proto_files, context_schema)
            
            # Handle object type
            if struct_type == 'object':
                return self.convert_record_type(field_type_schema, comment, proto_files, context_schema)
            
            # Handle array type
            elif struct_type == 'array':
                items_schema = field_type_schema.get('items', {'type': 'any'})
                converted_field_type = self.convert_field_type(message, self.compose_name(field_name, "item"), items_schema, comment, 1, proto_files, context_schema)
                if isinstance(converted_field_type, Field):
                    # If the item is an array or map, we need to wrap it in a message
                    if converted_field_type.type in ('array', 'map'):
                        item_message = Message(comment, self.compose_name(field_name, 'ItemType'), [converted_field_type], [], {}, {}, converted_field_type.dependencies)
                        message.messages[item_message.name] = item_message
                        return Field(comment, 'repeated', 'array', '', item_message.name, field_name, index, [])
                    else:
                        return Field(comment, 'repeated', 'array', '', converted_field_type.type, field_name, index, converted_field_type.dependencies)
                elif isinstance(converted_field_type, Enum):
                    enum = Enum(converted_field_type.comment, self.compose_name(converted_field_type.name, 'enum'), converted_field_type.fields)
                    message.enums[enum.name] = enum
                    return Field(comment, 'repeated', 'array', '', enum.name, field_name, index, [])
                elif isinstance(converted_field_type, Message):
                    local_message = Message(converted_field_type.comment, self.compose_name(converted_field_type.name, 'type'), converted_field_type.fields, converted_field_type.oneofs, converted_field_type.messages, converted_field_type.enums, converted_field_type.dependencies)
                    message.messages[local_message.name] = local_message
                    return Field(comment, 'repeated', 'array', '', local_message.name, field_name, index, [])
            
            # Handle set type (same as array in proto)
            elif struct_type == 'set':
                items_schema = field_type_schema.get('items', {'type': 'any'})
                converted_field_type = self.convert_field_type(message, self.compose_name(field_name, "item"), items_schema, comment, index, proto_files, context_schema)
                if isinstance(converted_field_type, Field):
                    return Field(comment, 'repeated', 'array', '', converted_field_type.type, field_name, index, converted_field_type.dependencies)
                elif isinstance(converted_field_type, Enum):
                    enum = Enum(converted_field_type.comment, self.compose_name(converted_field_type.name, 'enum'), converted_field_type.fields)
                    message.enums[enum.name] = enum
                    return Field(comment, 'repeated', 'array', '', enum.name, field_name, index, [])
                elif isinstance(converted_field_type, Message):
                    local_message = Message(converted_field_type.comment, self.compose_name(converted_field_type.name, 'type'), converted_field_type.fields, converted_field_type.oneofs, converted_field_type.messages, converted_field_type.enums, converted_field_type.dependencies)
                    message.messages[local_message.name] = local_message
                    return Field(comment, 'repeated', 'array', '', local_message.name, field_name, index, [])
            
            # Handle map type
            elif struct_type == 'map':
                values_schema = field_type_schema.get('values', {'type': 'any'})
                converted_field_type = self.convert_field_type(message, self.compose_name(field_name, 'item', 'field'), values_schema, comment, index, proto_files, context_schema)
                if isinstance(converted_field_type, Field):
                    return Field(comment, label, 'map', 'string', converted_field_type.type, field_name, index, converted_field_type.dependencies)
                elif isinstance(converted_field_type, Enum):
                    enum = Enum(converted_field_type.comment, self.compose_name(converted_field_type.name, 'enum'), converted_field_type.fields)
                    message.enums[enum.name] = enum
                    return Field(comment, label, 'map', 'string', enum.name, field_name, index, [])
                elif isinstance(converted_field_type, Message):
                    local_message = Message(converted_field_type.comment, self.compose_name(converted_field_type.name, 'type'), converted_field_type.fields, converted_field_type.oneofs, converted_field_type.messages, converted_field_type.enums, [])
                    message.messages[local_message.name] = local_message
                    return Field(comment, label, 'map', 'string', local_message.name, field_name, index, local_message.dependencies)
            
            # Handle choice type (discriminated union)
            elif struct_type == 'choice':
                # Choice type becomes a oneof in protobuf
                choices = field_type_schema.get('choices', {})
                oneof_fields = []
                for i, (choice_name, choice_schema) in enumerate(choices.items()):
                    choice_field = self.convert_field_type(message, choice_name, choice_schema, comment, i+index, proto_files, context_schema)
                    if isinstance(choice_field, Field):
                        oneof_fields.append(Field(choice_field.comment, '', choice_field.type, '', '', choice_name, i+index, choice_field.dependencies))
                    elif isinstance(choice_field, Message):
                        local_message = Message(choice_field.comment, self.compose_name(choice_name, 'type'), choice_field.fields, choice_field.oneofs, choice_field.messages, choice_field.enums, choice_field.dependencies)
                        message.messages[local_message.name] = local_message
                        oneof_fields.append(Field(choice_field.comment, '', local_message.name, '', '', choice_name, i+index, []))
                return Oneof(comment, field_name, oneof_fields)
            
            # Handle tuple type
            elif struct_type == 'tuple':
                # Tuple becomes a message with numbered fields
                tuple_order = field_type_schema.get('tuple', [])
                properties = field_type_schema.get('properties', {})
                tuple_message = Message(comment, self.compose_name(field_name, 'Tuple'), [], [], {}, {}, [])
                for i, prop_name in enumerate(tuple_order):
                    if prop_name in properties:
                        prop_schema = properties[prop_name]
                        tuple_field = self.convert_field_type(tuple_message, prop_name, prop_schema, Comment('', {}), i+1, proto_files, context_schema)
                        if isinstance(tuple_field, Field):
                            tuple_message.fields.append(tuple_field)
                return tuple_message
            
            # Handle primitive types with format specifications
            else:
                deps2: List[str] = []
                proto_type = self.structure_primitive_to_proto_type(struct_type, deps2)
                return Field(comment, label, proto_type, '', '', field_name, index, deps2)
        
        # Handle string types (primitive type names)
        elif isinstance(field_type_schema, str):
            deps3: List[str] = []
            proto_type = self.structure_primitive_to_proto_type(field_type_schema, deps3)
            return Field(comment, label, proto_type, '', '', field_name, index, deps3)
        
        raise ValueError(f"Unknown field type {field_type_schema}")

    def structure_schema_to_proto_message(self, structure_schema: dict, proto_files: ProtoFiles) -> str:
        """Convert a JSON Structure schema to a Protobuf message definition."""
        comment = Comment('', {})
        if 'doc' in structure_schema or 'description' in structure_schema:
            comment = Comment(structure_schema.get('description', structure_schema.get('doc', '')), {})
        
        namespace = structure_schema.get("namespace", '')
        if not namespace:
            namespace = self.default_namespace
        
        struct_type = structure_schema.get('type', 'object')
        
        if struct_type == 'object':
            message = self.convert_record_type(structure_schema, comment, proto_files, structure_schema)
            file = next((f for f in proto_files.files if f.package == namespace), None)
            if not file:
                file = ProtoFile({}, {}, {}, [], {}, namespace)
                proto_files.files.append(file)
            file.messages[message.name] = message
        elif struct_type == 'enum' or 'enum' in structure_schema:
            enum_name = structure_schema.get('name', 'UnnamedEnum')
            enum_values = structure_schema.get('enum', [])
            enum_fields = {str(val): Field(comment, '', str(val), '', '', str(val), i, []) for i, val in enumerate(enum_values)}
            enum = Enum(comment, enum_name, enum_fields)
            file = next((f for f in proto_files.files if f.package == namespace), None)
            if not file:
                file = ProtoFile({}, {}, {}, [], {}, namespace)
                proto_files.files.append(file)
            file.enums[enum_name] = enum
        
        return structure_schema.get("name", "UnnamedSchema")
        
    def structure_schema_to_proto_messages(self, structure_schema_input, proto_files: ProtoFiles):
        """Convert JSON Structure schema(s) to Protobuf message definitions."""
        if not isinstance(structure_schema_input, list):
            structure_schema_list = [structure_schema_input]
        else:
            structure_schema_list = structure_schema_input
        
        # Register all schemas first
        for structure_schema in structure_schema_list:
            if isinstance(structure_schema, dict):
                self.register_schema_ids(structure_schema)
        
        # Then convert them
        for structure_schema in structure_schema_list:
            if isinstance(structure_schema, dict):
                # Store definitions for later use
                if 'definitions' in structure_schema:
                    self.definitions = structure_schema['definitions']
                    # Process definitions
                    for def_name, def_schema in structure_schema['definitions'].items():
                        if isinstance(def_schema, dict):
                            def_schema_copy = def_schema.copy()
                            if 'name' not in def_schema_copy:
                                def_schema_copy['name'] = def_name
                            if 'namespace' not in def_schema_copy:
                                def_schema_copy['namespace'] = structure_schema.get('namespace', self.default_namespace)
                            self.structure_schema_to_proto_message(def_schema_copy, proto_files)
                
                # Process root schema
                self.structure_schema_to_proto_message(structure_schema, proto_files)

    def save_proto_to_file(self, proto_files: ProtoFiles, proto_path):
        """Save the Protobuf schema to a file."""
        for proto in proto_files.files:
            # gather dependencies that are within the package
            deps: List[str] = []
            for message in proto.messages.values():
                for dep in message.dependencies:
                    if '.' in dep:
                        deps.append(dep.rsplit('.', 1)[0])
            deps = list(set(deps))

            proto.imports.extend([d for d in deps if d != proto.package])
            proto_file_path = os.path.join(proto_path, f"{proto.package if proto.package else 'default'}.proto")
            # create the directory for the proto file if it doesn't exist
            proto_dir = os.path.dirname(proto_file_path)
            if not os.path.exists(proto_dir):
                os.makedirs(proto_dir, exist_ok=True)
            with open(proto_file_path, 'w', encoding='utf-8') as proto_file:
                # dump the ProtoFile structure in proto syntax
                proto_str = 'syntax = "proto3";\n\n'
                if proto.package:
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
        proto_str = f"{indent*level}message {message.name} {{\n"
        
        # Render nested messages and enums FIRST (protobuf convention)
        for local_message in message.messages.values():
            proto_str += self.render_message(local_message, level+1)
        for enum in message.enums.values():
            proto_str += f"{indent*level}{indent}enum {enum.name} {{\n"
            for _, field in enum.fields.items():
                proto_str += f"{indent*level}{indent}{indent}{field.label}{' ' if field.label else ''}{field.name} = {field.number};\n"
            proto_str += f"{indent*level}{indent}}}\n"
        
        # Then render fields and oneofs
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
        
        proto_str += f"{indent*level}}}\n"
        if level == 0:
            proto_str += "\n"
        return proto_str
            

    def convert_structure_to_proto(self, structure_schema_path, proto_file_path):
        """Convert JSON Structure schema file to Protobuf .proto file."""
        with open(structure_schema_path, 'r', encoding='utf-8') as structure_file:
            structure_schema = json.load(structure_file)
        proto_files = ProtoFiles([])
        self.structure_schema_to_proto_messages(structure_schema, proto_files)
        self.save_proto_to_file(proto_files, proto_file_path)

def convert_structure_to_proto(structure_schema_path, proto_file_path, naming_mode: Literal['snake', 'pascal', 'camel'] = 'pascal', allow_optional: bool = False):
    structuretoproto = StructureToProto()
    structuretoproto.naming_mode = naming_mode
    structuretoproto.allow_optional = allow_optional
    structuretoproto.default_namespace = os.path.splitext(os.path.basename(proto_file_path))[0].replace('-', '_')
    structuretoproto.convert_structure_to_proto(structure_schema_path, proto_file_path)
