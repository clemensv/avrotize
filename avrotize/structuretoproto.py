# pylint: disable=line-too-long

""" StructureToProto class for converting JSON Structure schema to Protocol Buffers (.proto files) """

import json
import os
from typing import Any, Dict, List, Optional, Set, NamedTuple

# Named tuples for proto structure (following avrotoproto.py pattern)
Comment = NamedTuple('Comment', [('content', str), ('tags', Dict[str, Any])])
Oneof = NamedTuple('Oneof', [('comment', 'Comment'), ('name', str), ('fields', List['Field'])])
Field = NamedTuple('Field', [('comment', 'Comment'), ('label', str), ('type', str), 
                             ('key_type', str), ('val_type', str), ('name', str), 
                             ('number', int), ('dependencies', List[str])])
Enum = NamedTuple('Enum', [('comment', 'Comment'), ('name', str), ('fields', Dict[str, 'Field'])])
Message = NamedTuple('Message', [('comment', 'Comment'), ('name', str), ('fields', List['Field']), 
                                 ('oneofs', List['Oneof']), ('messages', Dict[str, 'Message']), 
                                 ('enums', Dict[str, 'Enum']), ('dependencies', List[str])])
Service = NamedTuple('Service', [('name', str), ('functions', Dict[str, 'RpcFunc'])])
RpcFunc = NamedTuple('RpcFunc', [('name', str), ('in_type', str), ('out_type', str), ('uri', str)])
ProtoFile = NamedTuple('ProtoFile', [('messages', Dict[str, 'Message']), ('enums', Dict[str, 'Enum']),
                                     ('services', Dict[str, 'Service']), ('imports', List[str]),
                                     ('options', Dict[str, str]), ('package', str)])

INDENT = '  '


class StructureToProto:
    """ Converts JSON Structure schema to Protocol Buffers (.proto files) """

    def __init__(self) -> None:
        self.default_namespace: str = ''
        self.schema_registry: Dict[str, Dict] = {}  # Maps $id URIs to schemas
        self.generated_types: Set[str] = set()  # Track generated types to avoid duplicates

    def map_primitive_to_proto_type(self, structure_type: str, dependencies: List[str]) -> str:
        """Map JSON Structure primitive types to Protobuf types."""
        mapping = {
            # JSON Primitive Types
            'null': 'google.protobuf.Empty',
            'boolean': 'bool',
            'string': 'string',
            'integer': 'int32',  # Generic integer defaults to int32
            'number': 'double',  # Generic number defaults to double
            
            # Extended Primitive Types - Integer types
            'int8': 'int32',     # No int8 in proto, use int32
            'uint8': 'uint32',   # No uint8 in proto, use uint32
            'int16': 'int32',    # No int16 in proto, use int32
            'uint16': 'uint32',  # No uint16 in proto, use uint32
            'int32': 'int32',
            'uint32': 'uint32',
            'int64': 'int64',
            'uint64': 'uint64',
            'int128': 'string',  # No int128 in proto, use string
            'uint128': 'string', # No uint128 in proto, use string
            
            # Extended Primitive Types - Floating point types
            'float8': 'float',   # No float8 in proto, use float
            'float': 'float',
            'double': 'double',
            'binary32': 'float',  # IEEE 754 binary32
            'binary64': 'double', # IEEE 754 binary64
            'decimal': 'string',  # Decimal as string in proto (with precision/scale in docs)
            
            # Extended Primitive Types - Binary and temporal types
            'binary': 'bytes',
            'date': 'string',      # ISO 8601 date string
            'time': 'string',      # ISO 8601 time string
            'datetime': 'string',  # ISO 8601 datetime string
            'timestamp': 'string', # ISO 8601 timestamp string
            'duration': 'string',  # ISO 8601 duration string
            
            # Extended Primitive Types - Other types
            'uuid': 'string',
            'uri': 'string',
            'jsonpointer': 'string',
            'any': 'google.protobuf.Any',
        }
        
        proto_type = mapping.get(structure_type, '')
        if not proto_type:
            # Not a primitive type - must be a reference to another type
            dependencies.append(structure_type)
            proto_type = structure_type
        return proto_type

    def compose_name(self, prefix: str, name: str, naming_mode: str = 'pascal') -> str:
        """Compose a name with appropriate casing."""
        if naming_mode == 'pascal':
            return f"{prefix[0].upper() + prefix[1:] if prefix else ''}{name[0].upper() + name[1:] if name else ''}"
        return prefix + name

    def resolve_ref(self, ref: str, context_schema: Optional[Dict] = None) -> Optional[Dict]:
        """Resolves a $ref to the actual schema definition."""
        # Check if it's an absolute URI reference (schema with $id)
        if not ref.startswith('#/'):
            # Try to resolve from schema registry
            if ref in self.schema_registry:
                return self.schema_registry[ref]
            return None
        
        # Handle fragment-only references (internal to document)
        path = ref[2:].split('/')
        schema = context_schema if context_schema else {}
        
        for part in path:
            if not isinstance(schema, dict) or part not in schema:
                return None
            schema = schema[part]
        
        return schema

    def register_schema_ids(self, schema: Dict, base_uri: str = '') -> None:
        """Recursively registers schemas with $id keywords."""
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

    def convert_field_type(self, message: Message, field_name: str, field_type: Any, 
                          comment: Comment, index: int, context_schema: Dict) -> Field | Oneof | Enum | Message:
        """Convert a JSON Structure field type to a Protobuf field type."""
        label = ''
        
        if isinstance(field_type, list):
            # Handling union types (including nullable fields)
            non_null_types = [t for t in field_type if t != 'null']
            if len(non_null_types) == 1:
                # Nullable field - use optional label
                label = 'optional'
                field_type = non_null_types[0]
            elif len(non_null_types) > 0:
                # Multiple non-null types - use oneof
                oneof_fields = []
                for i, t in enumerate(non_null_types):
                    field = self.convert_field_type(message, self.compose_name(field_name, 'choice'),
                                                   t, comment, i + index, context_schema)
                    if isinstance(field, Field):
                        if field.type == 'map' or field.type == 'array' or field.type == 'set':
                            # Wrap collection types in a message
                            local_message = Message(comment, self.compose_name(field.name, field.type),
                                                  [], [], {}, {}, field.dependencies)
                            local_message.fields.append(field)
                            new_field = Field(field.comment, '', local_message.name, '', '',
                                            self.compose_name(field.name.split('.')[-1], field.type),
                                            i + index, field.dependencies)
                            message.messages[local_message.name] = local_message
                            oneof_fields.append(new_field)
                        else:
                            field = Field(field.comment, field.label, field.type, field.key_type,
                                        field.val_type, self.compose_name(field_name, field.type.split('.')[-1]),
                                        i + index, field.dependencies)
                            oneof_fields.append(field)
                    elif isinstance(field, Enum):
                        enum = Enum(field.comment, self.compose_name(field.name, "options"), field.fields)
                        message.enums[enum.name] = enum
                        field = Field(field.comment, '', enum.name, '', '', field.name.split('.')[-1],
                                    i + index, [])
                        oneof_fields.append(field)
                    elif isinstance(field, Message):
                        local_message = Message(field.comment, self.compose_name(field.name, 'type'),
                                              field.fields, field.oneofs, field.messages, field.enums,
                                              field.dependencies)
                        message.messages[local_message.name] = local_message
                        field = Field(field.comment, '', local_message.name, '', '', field.name.split('.')[-1],
                                    i + index, field.dependencies)
                        oneof_fields.append(field)
                oneof = Oneof(comment, field_name, oneof_fields)
                return oneof
            else:
                raise ValueError(f"Field {field_name} is a union type without any non-null types")

        if isinstance(field_type, dict):
            # Handle $ref
            if '$ref' in field_type:
                ref_schema = self.resolve_ref(field_type['$ref'], context_schema)
                if ref_schema:
                    return self.convert_field_type(message, field_name, ref_schema, comment, 
                                                  index, context_schema)
                dependencies: List[str] = []
                return Field(comment, label, 'google.protobuf.Any', '', '', field_name, index, dependencies)
            
            # Handle enum keyword
            if 'enum' in field_type:
                enum_values = field_type['enum']
                enum_symbols = {str(symbol): Field(comment, '', str(symbol), '', '', str(symbol), s, [])
                               for s, symbol in enumerate(enum_values)}
                enum_name = self.compose_name(field_name, 'Enum')
                return Enum(comment, enum_name, enum_symbols)
            
            # Handle type keyword
            if 'type' not in field_type:
                deps: List[str] = []
                return Field(comment, label, 'google.protobuf.Any', '', '', field_name, index, deps)
            
            struct_type = field_type['type']
            
            # Handle object type
            if struct_type == 'object':
                return self.convert_record_type(field_type, comment, context_schema, field_name)
            
            # Handle array type
            elif struct_type == 'array':
                items_schema = field_type.get('items', {'type': 'any'})
                converted_field_type = self.convert_field_type(message, self.compose_name(field_name, "item"),
                                                               items_schema, comment, index, context_schema)
                if isinstance(converted_field_type, Field):
                    return Field(comment, 'repeated', 'array', '', converted_field_type.type, field_name,
                               index, converted_field_type.dependencies)
                elif isinstance(converted_field_type, Enum):
                    enum = Enum(converted_field_type.comment, self.compose_name(converted_field_type.name, 'enum'),
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
                elif isinstance(converted_field_type, Oneof):
                    fl = []
                    deps3: List[str] = []
                    for i, f in enumerate(converted_field_type.fields):
                        fl.append(Field(Comment('', {}), '', f.type, '', '', f.name, i + 1, []))
                        deps3.extend(f.dependencies)
                    oneof = Oneof(converted_field_type.comment, 'item', fl)
                    local_message = Message(comment, self.compose_name(field_name, 'type'), [], [], {}, {}, deps3)
                    local_message.oneofs.append(oneof)
                    new_field = Field(Comment('', {}), 'repeated', 'array', '', local_message.name,
                                    field_name.split('.')[-1], index, local_message.dependencies)
                    message.messages[local_message.name] = local_message
                    return new_field
            
            # Handle set type (treat as repeated array)
            elif struct_type == 'set':
                items_schema = field_type.get('items', {'type': 'any'})
                converted_field_type = self.convert_field_type(message, self.compose_name(field_name, "item"),
                                                               items_schema, comment, index, context_schema)
                if isinstance(converted_field_type, Field):
                    return Field(comment, 'repeated', 'array', '', converted_field_type.type, field_name,
                               index, converted_field_type.dependencies)
                elif isinstance(converted_field_type, Enum):
                    enum = Enum(converted_field_type.comment, self.compose_name(converted_field_type.name, 'enum'),
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
            
            # Handle map type
            elif struct_type == 'map':
                values_schema = field_type.get('values', {'type': 'any'})
                converted_field_type = self.convert_field_type(message, self.compose_name(field_name, 'item'),
                                                               values_schema, comment, index, context_schema)
                if isinstance(converted_field_type, Field):
                    return Field(comment, label, 'map', 'string', converted_field_type.type, field_name,
                               index, converted_field_type.dependencies)
                elif isinstance(converted_field_type, Enum):
                    enum = Enum(converted_field_type.comment, self.compose_name(converted_field_type.name, 'enum'),
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
                elif isinstance(converted_field_type, Oneof):
                    fl = []
                    deps4: List[str] = []
                    for i, f in enumerate(converted_field_type.fields):
                        fl.append(Field(Comment('', {}), '', f.type, '', '', f.name, i + 1, []))
                        deps4.extend(f.dependencies)
                    oneof = Oneof(converted_field_type.comment, 'item', fl)
                    local_message = Message(comment, self.compose_name(field_name, 'type'), [], [], {}, {}, deps4)
                    local_message.oneofs.append(oneof)
                    new_field = Field(Comment('', {}), label, 'map', 'string', local_message.name,
                                    field_name.split('.')[-1], index, local_message.dependencies)
                    message.messages[local_message.name] = local_message
                    return new_field
            
            # Handle choice type (discriminated union)
            elif struct_type == 'choice':
                # Generate a oneof for choices
                choices = field_type.get('choices', {})
                oneof_fields = []
                field_idx = index
                for choice_name, choice_schema in choices.items():
                    if isinstance(choice_schema, dict) and '$ref' in choice_schema:
                        ref_schema = self.resolve_ref(choice_schema['$ref'], context_schema)
                        if ref_schema:
                            choice_schema = ref_schema
                    field = self.convert_field_type(message, choice_name, choice_schema, comment,
                                                   field_idx, context_schema)
                    if isinstance(field, Field):
                        oneof_fields.append(field)
                    elif isinstance(field, Message):
                        local_message = Message(field.comment, self.compose_name(field.name, 'type'),
                                              field.fields, field.oneofs, field.messages, field.enums,
                                              field.dependencies)
                        message.messages[local_message.name] = local_message
                        new_field = Field(field.comment, '', local_message.name, '', '', choice_name,
                                        field_idx, field.dependencies)
                        oneof_fields.append(new_field)
                    field_idx += 1
                return Oneof(comment, field_name, oneof_fields)
            
            # Handle tuple type (as repeated message with ordered fields)
            elif struct_type == 'tuple':
                # Tuples in protobuf are represented as repeated fields or nested messages
                # For simplicity, treat as repeated Any
                deps_tuple: List[str] = []
                return Field(comment, 'repeated', 'google.protobuf.Any', '', '', field_name, index, deps_tuple)
            
            # Handle primitive type
            else:
                deps1: List[str] = []
                proto_type = self.map_primitive_to_proto_type(struct_type, deps1)
                return Field(comment, label, proto_type, '', '', field_name, index, deps1)
        
        elif isinstance(field_type, str):
            deps2: List[str] = []
            proto_type = self.map_primitive_to_proto_type(field_type, deps2)
            return Field(comment, label, proto_type, '', '', field_name, index, deps2)
        
        raise ValueError(f"Unknown field type {field_type}")

    def convert_record_type(self, structure_record: Dict, comment: Comment, 
                           context_schema: Dict, name_hint: str = '') -> Message:
        """Convert a JSON Structure object to a Protobuf message."""
        message_name = structure_record.get('name', name_hint or 'UnnamedMessage')
        message_name = self.compose_name('', message_name)
        
        local_message = Message(comment, message_name, [], [], {}, {}, [])
        
        properties = structure_record.get('properties', {})
        required_props = structure_record.get('required', [])
        
        offs = 1
        for i, (prop_name, prop_schema) in enumerate(properties.items()):
            # Build comment from description
            prop_comment = Comment(prop_schema.get('description', prop_schema.get('doc', '')), {})
            
            field = self.convert_field_type(local_message, prop_name, prop_schema, prop_comment,
                                           i + offs, context_schema)
            
            # Determine if field should be optional
            if isinstance(field, Field) and prop_name not in required_props and not field.label:
                # Make non-required fields optional
                field = Field(field.comment, 'optional', field.type, field.key_type, field.val_type,
                            field.name, field.number, field.dependencies)
            
            if isinstance(field, Oneof):
                for f in field.fields:
                    local_message.dependencies.extend(f.dependencies)
                local_message.oneofs.append(field)
                offs += len(field.fields) - 1
            elif isinstance(field, Enum):
                enum = Enum(field.comment, self.compose_name(field.name, 'enum'), field.fields)
                local_message.enums[enum.name] = enum
                local_message.fields.append(Field(field.comment, '', enum.name, '', '',
                                                 field.name.split('.')[-1], i + offs, []))
            elif isinstance(field, Message):
                inner_message = Message(field.comment, self.compose_name(field.name, 'type'),
                                      field.fields, field.oneofs, field.messages, field.enums, [])
                local_message.messages[inner_message.name] = inner_message
                local_message.fields.append(Field(field.comment, '', inner_message.name, '', '',
                                                 field.name.split('.')[-1], i + offs, []))
                local_message.dependencies.extend(field.dependencies)
            else:
                local_message.dependencies.extend(field.dependencies)
                local_message.fields.append(field)
        
        return local_message

    def structure_schema_to_proto_message(self, structure_schema: Dict, proto_files: Dict[str, ProtoFile],
                                         context_schema: Dict) -> str:
        """Convert a JSON Structure schema to a Protobuf message definition."""
        comment = Comment(structure_schema.get('description', structure_schema.get('doc', '')), {})
        namespace = structure_schema.get('namespace', '')
        if not namespace:
            namespace = self.default_namespace
        
        # Get name
        type_name = structure_schema.get('name', 'UnnamedType')
        
        # Handle different root types
        struct_type = structure_schema.get('type', 'object')
        
        if struct_type == 'object':
            message = self.convert_record_type(structure_schema, comment, context_schema)
            
            # Get or create proto file for this namespace
            if namespace not in proto_files:
                proto_files[namespace] = ProtoFile({}, {}, {}, [], {}, namespace)
            
            proto_files[namespace].messages[message.name] = message
        
        elif struct_type == 'enum' or 'enum' in structure_schema:
            # Handle enum type
            enum_name = type_name
            enum_values = structure_schema.get('enum', [])
            enum_symbols = {str(symbol): Field(comment, '', str(symbol), '', '', str(symbol), s, [])
                           for s, symbol in enumerate(enum_values)}
            enum = Enum(comment, enum_name, enum_symbols)
            
            if namespace not in proto_files:
                proto_files[namespace] = ProtoFile({}, {}, {}, [], {}, namespace)
            
            proto_files[namespace].enums[enum_name] = enum
        
        elif struct_type == 'choice':
            # Handle choice as a message with oneof
            message_name = type_name
            choices = structure_schema.get('choices', {})
            
            # Create a message with a single oneof containing all choices
            local_message = Message(comment, message_name, [], [], {}, {}, [])
            oneof_fields = []
            
            for i, (choice_name, choice_schema) in enumerate(choices.items()):
                if isinstance(choice_schema, dict) and '$ref' in choice_schema:
                    ref_schema = self.resolve_ref(choice_schema['$ref'], context_schema)
                    if ref_schema:
                        choice_schema = ref_schema
                
                field = self.convert_field_type(local_message, choice_name, choice_schema, comment,
                                               i + 1, context_schema)
                if isinstance(field, Field):
                    oneof_fields.append(field)
                elif isinstance(field, Message):
                    inner_message = Message(field.comment, self.compose_name(field.name, 'type'),
                                          field.fields, field.oneofs, field.messages, field.enums,
                                          field.dependencies)
                    local_message.messages[inner_message.name] = inner_message
                    new_field = Field(field.comment, '', inner_message.name, '', '', choice_name,
                                    i + 1, field.dependencies)
                    oneof_fields.append(new_field)
            
            local_message.oneofs.append(Oneof(comment, 'value', oneof_fields))
            
            if namespace not in proto_files:
                proto_files[namespace] = ProtoFile({}, {}, {}, [], {}, namespace)
            
            proto_files[namespace].messages[message_name] = local_message
        
        return type_name

    def process_definitions(self, definitions: Dict[str, Any], namespace: str, 
                           proto_files: Dict[str, ProtoFile], context_schema: Dict) -> None:
        """Process definitions section recursively."""
        for name, definition in definitions.items():
            if isinstance(definition, dict):
                if 'type' in definition or 'enum' in definition:
                    # This is a type definition
                    def_schema = definition.copy()
                    if 'name' not in def_schema:
                        def_schema['name'] = name
                    if 'namespace' not in def_schema:
                        def_schema['namespace'] = namespace
                    self.structure_schema_to_proto_message(def_schema, proto_files, context_schema)
                elif any(k in definition for k in ['properties', '$ref', '$extends']):
                    # Might be an object without explicit type
                    def_schema = definition.copy()
                    if 'type' not in def_schema:
                        def_schema['type'] = 'object'
                    if 'name' not in def_schema:
                        def_schema['name'] = name
                    if 'namespace' not in def_schema:
                        def_schema['namespace'] = namespace
                    self.structure_schema_to_proto_message(def_schema, proto_files, context_schema)
                else:
                    # This might be a nested namespace
                    new_namespace = f"{namespace}.{name}" if namespace else name
                    self.process_definitions(definition, new_namespace, proto_files, context_schema)

    def render_message(self, message: Message, level: int = 0) -> str:
        """Render a proto message to string."""
        proto_str = f"{INDENT * level}message {message.name} {{\n"
        
        # Combine fields and oneofs, sort by field number
        fields_and_oneofs = list(message.fields) + list(message.oneofs)
        fields_and_oneofs.sort(key=lambda f: f.number if isinstance(f, Field) else f.fields[0].number)
        
        for fo in fields_and_oneofs:
            if isinstance(fo, Field):
                field = fo
                if field.type == "map":
                    proto_str += f"{INDENT * level}{INDENT}{field.label}{' ' if field.label else ''}map<{field.key_type}, {field.val_type}> {field.name} = {field.number};\n"
                elif field.type == "array" or field.type == "set":
                    proto_str += f"{INDENT * level}{INDENT}{field.label}{' ' if field.label else ''}{field.val_type} {field.name} = {field.number};\n"
                else:
                    proto_str += f"{INDENT * level}{INDENT}{field.label}{' ' if field.label else ''}{field.type} {field.name} = {field.number};\n"
            else:
                oneof = fo
                proto_str += f"{INDENT * level}{INDENT}oneof {oneof.name} {{\n"
                for field in oneof.fields:
                    proto_str += f"{INDENT * level}{INDENT}{INDENT}{field.label}{' ' if field.label else ''}{field.type} {field.name} = {field.number};\n"
                proto_str += f"{INDENT * level}{INDENT}}}\n"
        
        # Render nested enums
        for enum in message.enums.values():
            proto_str += f"{INDENT * level}{INDENT}enum {enum.name} {{\n"
            for _, field in enum.fields.items():
                proto_str += f"{INDENT * level}{INDENT}{INDENT}{field.name} = {field.number};\n"
            proto_str += f"{INDENT * level}{INDENT}}}\n"
        
        # Render nested messages
        for local_message in message.messages.values():
            proto_str += self.render_message(local_message, level + 1)
        
        proto_str += f"{INDENT * level}}}\n"
        return proto_str

    def save_proto_to_file(self, proto_files: Dict[str, ProtoFile], proto_path: str):
        """Save the Protobuf schema to files."""
        for namespace, proto in proto_files.items():
            # Gather dependencies that are within the package
            deps: List[str] = []
            for message in proto.messages.values():
                for dep in message.dependencies:
                    if '.' in dep:
                        deps.append(dep.rsplit('.', 1)[0])
            deps = list(set(deps))
            
            # Add imports
            proto.imports.extend([d for d in deps if d != proto.package])
            
            # Create proto file path
            proto_file_path = os.path.join(proto_path, f"{proto.package if proto.package else 'schema'}.proto")
            
            # Create the directory for the proto file if it doesn't exist
            proto_dir = os.path.dirname(proto_file_path)
            if proto_dir and not os.path.exists(proto_dir):
                os.makedirs(proto_dir, exist_ok=True)
            
            with open(proto_file_path, 'w', encoding='utf-8') as proto_file:
                # Write proto syntax and package
                proto_str = 'syntax = "proto3";\n\n'
                if proto.package:
                    proto_str += f'package {proto.package};\n\n'
                
                # Write imports
                for import_package in proto.imports:
                    proto_str += f'import "{import_package}.proto";\n'
                if proto.imports:
                    proto_str += "\n"
                
                # Write enums
                for enum_name, enum in proto.enums.items():
                    proto_str += f"enum {enum_name} {{\n"
                    for _, field in enum.fields.items():
                        proto_str += f"{INDENT}{field.name} = {field.number};\n"
                    proto_str += "}\n\n"
                
                # Write messages
                for message in proto.messages.values():
                    proto_str += self.render_message(message)
                    proto_str += "\n"
                
                # Write services (if any)
                for service in proto.services.values():
                    proto_str += f"service {service.name} {{\n"
                    for function_name, func in service.functions.items():
                        proto_str += f"{INDENT}rpc {func.name} ({func.in_type}) returns ({func.out_type});\n"
                    proto_str += "}\n\n"
                
                proto_file.write(proto_str)

    def convert_structure_to_proto(self, structure_schema_path: str, proto_file_path: str):
        """Convert JSON Structure schema file to Protobuf .proto file."""
        with open(structure_schema_path, 'r', encoding='utf-8') as structure_file:
            structure_schema = json.load(structure_file)
        
        self.convert_structure_schema_to_proto(structure_schema, proto_file_path)

    def convert_structure_schema_to_proto(self, structure_schema_input: Any, proto_file_path: str):
        """Convert JSON Structure schema to Protobuf message definitions."""
        if not isinstance(structure_schema_input, list):
            structure_schema_list = [structure_schema_input]
        else:
            structure_schema_list = structure_schema_input
        
        proto_files: Dict[str, ProtoFile] = {}
        
        # Register all schemas with $id keywords for cross-references
        for structure_schema in structure_schema_list:
            if isinstance(structure_schema, dict):
                self.register_schema_ids(structure_schema)
        
        # Process each schema
        for structure_schema in structure_schema_list:
            if not isinstance(structure_schema, dict):
                continue
            
            # Process root type
            if 'type' in structure_schema or 'enum' in structure_schema:
                self.structure_schema_to_proto_message(structure_schema, proto_files, structure_schema)
            
            # Process definitions
            if 'definitions' in structure_schema:
                namespace = structure_schema.get('namespace', '')
                self.process_definitions(structure_schema['definitions'], namespace, proto_files, structure_schema)
        
        # Save proto files
        self.save_proto_to_file(proto_files, proto_file_path)


def convert_structure_to_proto(structure_schema_path: str, proto_file_path: str):
    """Convert JSON Structure schema to Protocol Buffers (.proto files).
    
    Args:
        structure_schema_path: Path to the JSON Structure schema file
        proto_file_path: Output path for the .proto file(s)
    """
    converter = StructureToProto()
    converter.default_namespace = os.path.splitext(os.path.basename(proto_file_path))[0].replace('-', '_')
    converter.convert_structure_to_proto(structure_schema_path, proto_file_path)


def convert_structure_schema_to_proto(structure_schema: Any, proto_file_path: str):
    """Convert JSON Structure schema to Protocol Buffers (.proto files).
    
    Args:
        structure_schema: JSON Structure schema (dict or list of dicts)
        proto_file_path: Output path for the .proto file(s)
    """
    converter = StructureToProto()
    converter.default_namespace = os.path.splitext(os.path.basename(proto_file_path))[0].replace('-', '_')
    converter.convert_structure_schema_to_proto(structure_schema, proto_file_path)
