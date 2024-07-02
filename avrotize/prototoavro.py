"""
Module to convert Protobuf .proto files to Avro schema.
"""

import json
import os
import re
from typing import Dict
from avrotize.dependency_resolver import sort_messages_by_dependencies, inline_dependencies_of
from . import proto2parser
from . import proto3parser

class ProtoToAvroConverter:
    """Class to convert Protobuf .proto files to Avro schema."""

    isomorphic_types = ['float', 'double', 'bytes', 'string']
    imported_types: Dict[str, str] = {}

    def __init__(self):
        """Initialize ProtoToAvroConverter."""
        pass

    def proto_type_to_avro_primitive(self, proto_type):
        """
        Map Protobuf types to Avro primitive types.

        Args:
            proto_type (str): Protobuf type to convert.

        Returns:
            str or dict: Corresponding Avro type.
        """
        mapping = {
            'google.protobuf.Empty': 'null',  # Special handling may be required
            'bool': 'boolean',
            'int32': 'int',
            'uint32': 'int',
            'sint32': 'int',
            'int64': 'long',
            'uint64': 'long',
            'sint64': 'long',
            'fixed32': 'int',
            'fixed64': 'long',
            'sfixed32': 'int',
            'sfixed64': 'long',
            'google.protobuf.Timestamp': {
                "type": "long",
                "logicalType": "timestamp-micros"
            }
        }
        if proto_type in self.isomorphic_types:
            return proto_type
        return mapping.get(proto_type, proto_type)

    def convert_proto_to_avro_schema(self, proto_file_path: str, avro_namespace: str, message_type: str) -> list:
        """
        Convert .proto file to Avro schema.

        Args:
            proto_file_path (str): Path to the Protobuf .proto file.

        Returns:
            list: Avro schema as a list of dictionaries.
        """
        with open(proto_file_path, 'r', encoding='utf-8') as proto_file:
            proto_schema = proto_file.read()

        # Determine whether we have proto3 or proto2 and parse the data
        if re.search(r'syntax\s*=\s*"proto3"', proto_schema):
            data = proto3parser.parse(proto_schema)
        else:
            data = proto2parser.parse(proto_schema)

        # Get the namespace
        namespace = avro_namespace if avro_namespace else data.package.value if data.package else ''

        # Avro schema header
        avro_schema = []

        for import_ in data.imports:
            # Handle protobuf imports
            if import_.startswith('google/protobuf/'):
                script_path = os.path.dirname(os.path.abspath(__file__))
                avsc_dir = os.path.join(script_path, 'prototypes')

                # Load the corresponding avsc file from ./prototypes at this script's path into avro_schema
                avsc = f'{avsc_dir}/{import_.replace("google/protobuf/", "").replace(".proto", ".avsc")}'
                with open(avsc, 'r', encoding='utf-8') as avsc_file:
                    types = json.load(avsc_file)
                    for t in types:
                        self.imported_types[t["namespace"] + "." + t["name"]] = t
            else:
                # Find the path relative to the current directory
                cwd = os.path.join(os.getcwd(), os.path.dirname(proto_file_path))
                import_path = os.path.join(cwd, import_)
                # Raise an exception if the imported file does not exist
                if not os.path.exists(import_path):
                    raise FileNotFoundError(f'Import file {import_path} does not exist.')

                avro_schema.extend(self.convert_proto_to_avro_schema(import_path, avro_namespace, message_type))

        # Convert message fields
        for _, m in data.messages.items():
            self.handle_message(m, avro_schema, namespace)

        # Convert enum fields
        for _, enum_type in data.enums.items():
            self.handle_enum(enum_type, avro_schema, namespace)

        # Sort the messages in avro_schema by dependencies
        avro_schema = sort_messages_by_dependencies(avro_schema)
        if message_type:
            message_schema = next((message for message in avro_schema if message['type'] == "record" and message['name'] == message_type), None)
            if not message_schema:
                raise ValueError(f'Message type {message_type} not found in the Avro schema.')
            else:
               inline_dependencies_of(avro_schema, message_schema)
               return message_schema
        return avro_schema

    @staticmethod
    def clean_comment(comment):
        """
        Clean comments by stripping slashes, newlines, linefeeds, and extra whitespace.

        Args:
            comment (str): The comment to clean.

        Returns:
            str: Cleaned comment.
        """
        if comment:
            return comment.replace('//', '').replace('\n', '').lstrip().rstrip()
        return None

    def handle_enum(self, enum_type, avro_schema, namespace):
        """
        Convert enum fields to avro schema.

        Args:
            enum_type: The enum type from the parsed proto file.
            avro_schema (list): The list to append the converted enum schema.
            namespace (str): The namespace for the enum.
        """
        comment = self.clean_comment(enum_type.comment.content if enum_type.comment and enum_type.comment.content else None)

        # Create avro schema
        avro_enum = {
            'name': enum_type.name,
            'type': 'enum',
            'namespace': namespace,
            'symbols': [],
            'dependencies': []
        }

        if comment:
            avro_enum['doc'] = comment

        for value in enum_type.fields:
            avro_enum['symbols'].append(value.name)

        avro_schema.append(avro_enum)

    def handle_message(self, m, avro_schema, namespace):
        """
        Convert protobuf messages to avro records.

        Args:
            m: The message type from the parsed proto file.
            avro_schema (list): The list to append the converted message schema.
            namespace (str): The namespace for the message.
        """
        dependencies = []

        comment = self.clean_comment(m.comment.content if m.comment and m.comment.content else None)

        avro_record = {
            'type': 'record',
            'name': m.name,
            'namespace': namespace,
            'fields': []
        }

        if comment:
            avro_record['doc'] = comment

        for f in m.fields:
            avro_type = self.get_avro_type_for_field(m, namespace, dependencies, f)
            comment = self.clean_comment(f.comment.content if f.comment and f.comment.content else None)

            avro_field = {
                'name': f.name,
                'type': avro_type,
            }

            if comment:
                avro_field['doc'] = comment

            avro_record['fields'].append(avro_field)

        for f in m.oneofs:
            avro_oneof = {
                'name': f.name,
                'type': []
            }
            comment = self.clean_comment(f.comment.content if f.comment and f.comment.content else None)

            if comment:
                avro_oneof['doc'] = comment

            for o in f.fields:
                avro_type = self.get_avro_type_for_field(m, namespace, dependencies, o)
                comment = self.clean_comment(o.comment.content if o.comment and o.comment.content else None)

                if comment:
                    o['doc'] = comment

                avro_oneof['type'].append(avro_type)

            avro_record['fields'].append(avro_oneof)

        if dependencies:
            avro_record['dependencies'] = dependencies
        avro_schema.append(avro_record)

        for _, mi in m.messages.items():
            self.handle_message(mi, avro_schema, namespace)

        # Convert enum fields
        for _, enum_type in m.enums.items():
            self.handle_enum(enum_type, avro_schema, namespace)

    def get_avro_type_for_field(self, m, namespace, dependencies, f):
        """
        Get Avro type for a Protobuf field.

        Args:
            m: The message type from the parsed proto file.
            namespace (str): The namespace for the message.
            dependencies (list): The list to append the dependencies.
            f: The field from the parsed proto file.

        Returns:
            str or dict: Corresponding Avro type.
        """
        field_type = None
        is_custom = False
        if f.label == 'repeated' or f.type == 'map':
            field_type = self.proto_type_to_avro_primitive(f.val_type)
            is_custom = field_type == f.val_type and field_type not in self.isomorphic_types
        else:
            field_type = self.proto_type_to_avro_primitive(f.type)
            is_custom = field_type == f.type and field_type not in self.isomorphic_types

        if is_custom:
            if f.type in self.imported_types:
                field_type = self.imported_types[f.type]
                self.imported_types[f.type] = f.type
            else:
                found = False
                for k, mi in m.messages.items():
                    if mi.name == field_type:
                        schema = []
                        self.handle_message(mi, schema, namespace)
                        del m.messages[k]
                        field_type = schema[0]
                        if 'dependencies' in field_type:
                            dependencies.extend(field_type['dependencies'])
                            del field_type['dependencies']
                        found = True
                        break
                if not found:
                    dependencies.append(field_type)

        if f.label == 'optional':
            field_type = ["null", field_type]
        if f.label == 'repeated':
            avro_type = {
                "type": "array",
                "items": field_type
            }
        elif f.type == 'map':
            avro_type = {
                "type": "map",
                "values": field_type,
            }
        else:
            avro_type = field_type
        return avro_type

def convert_proto_to_avro(proto_file_path: str, avro_schema_path: str, namespace: str = None, message_type: str = None):
    """
    Convert Protobuf .proto file to Avro schema.

    Args:
        proto_file_path (str): Path to the Protobuf .proto file.
        avro_schema_path (str): Path to save the Avro schema .avsc file.

    Raises:
        FileNotFoundError: If the proto file does not exist.
        ValueError: If the file extensions are incorrect.
    """
    if not os.path.exists(proto_file_path):
        raise FileNotFoundError(f'Proto file {proto_file_path} does not exist.')

    converter = ProtoToAvroConverter()
    avro_schema = converter.convert_proto_to_avro_schema(proto_file_path, namespace, message_type)

    # Convert the Avro schema to JSON and write it to the file
    with open(avro_schema_path, 'w', encoding='utf-8') as avro_file:
        avro_file.write(json.dumps(avro_schema, indent=2))

    print(f'Converted {proto_file_path} to {avro_schema_path}')
