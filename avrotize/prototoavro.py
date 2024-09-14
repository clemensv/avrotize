"""
Module to convert Protobuf .proto files to Avro schema.
"""

import json
import os
import re
from typing import Dict, List, Tuple
from avrotize.common import pascal
from avrotize.dependency_resolver import sort_messages_by_dependencies, inline_dependencies_of
from . import proto2parser
from . import proto3parser

AvroSchema = Dict[str, 'AvroSchema'] | List['AvroSchema'] | str | None

class ProtoToAvroConverter:
    """Class to convert Protobuf .proto files to Avro schema."""

    isomorphic_types = ['float', 'double', 'bytes', 'string']

    def __init__(self):
        """Initialize ProtoToAvroConverter."""
        self.imported_types: Dict[str, str] = {}
        self.generated_types: Dict[str, str] = {}
        self.forward_references: Dict[str, str] = {} # table for resolvbing forward references

    def proto_type_to_avro_primitive(self, proto_type: str)-> Tuple[bool, str]:
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
            return True, proto_type
        mapped = mapping.get(proto_type, None)
        if mapped:
            return True, mapped
        return False, proto_type

    def build_forward_references_from_message(self, proto_message_type: proto2parser.Message | proto3parser.Message, avro_namespace: str):
        """
        Build forward references from a Protobuf message.

        Args:
            proto_message_type: The message type from the parsed proto file.
            avro_namespace (str): The namespace for the message.
        """
        for _, nested_message in proto_message_type.messages.items():
            nested_namespace = avro_namespace + '.' + proto_message_type.name + '_types'
            self.build_forward_references_from_message(nested_message, nested_namespace)
        for _, enum_type in proto_message_type.enums.items():
            nested_namespace = avro_namespace + '.' + proto_message_type.name + '_types'
            self.forward_references[nested_namespace+'.'+enum_type.name] = "enum"
        self.forward_references[avro_namespace+'.'+proto_message_type.name] = "record"

    def build_forward_references_from_file(self, proto_file:  proto3parser.ProtoFile| proto2parser.ProtoFile, avro_namespace: str):
        """
        Build forward references from a Protobuf file.

        Args:
            proto_file: The parsed proto file.
            avro_namespace (str): The namespace for the message.
        """
        for _, enum_type in proto_file.enums.items():
             self.forward_references[avro_namespace+'.'+enum_type.name] = "enum"
        for _, message in proto_file.messages.items():
            self.build_forward_references_from_message(message, avro_namespace)

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
            data: proto3parser.ProtoFile = proto3parser.parse(proto_schema)
        else:
            data: proto2parser.ProtoFile = proto2parser.parse(proto_schema)

        # Build forward references
        self.build_forward_references_from_file(data, avro_namespace)
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
                        qualified_name = t["namespace"] + "." + t["name"]
                        self.imported_types[qualified_name] = t
            else:
                # Find the path relative to the current directory
                cwd = os.path.join(os.getcwd(), os.path.dirname(proto_file_path))
                import_path = os.path.join(cwd, import_)
                # Raise an exception if the imported file does not exist
                if not os.path.exists(import_path):
                    raise FileNotFoundError(f'Import file {import_path} does not exist.')
                package_name = pascal(import_.replace('.proto', ''))
                import_namespace = (avro_namespace + '.' + package_name) if avro_namespace else package_name
                avro_schema.extend(self.convert_proto_to_avro_schema(import_path, import_namespace, message_type))


        # Convert enum fields
        for _, enum_type in data.enums.items():
            self.handle_enum(enum_type, avro_schema, avro_namespace)

        # Convert message fields
        for _, m in data.messages.items():
            self.handle_message(m, avro_schema, avro_namespace)


        # Sort the messages in avro_schema by dependencies
        if message_type:
            message_schema = next(
                (message for message in avro_schema if message['type'] == "record" and message['name'] == message_type), None)
            if not message_schema:
                raise ValueError(f'Message type {message_type} not found in the Avro schema.')
            else:
                inline_dependencies_of(avro_schema, message_schema)
                return message_schema
        else:
            avro_schema = sort_messages_by_dependencies(avro_schema)
        return avro_schema

    @staticmethod
    def clean_comment(comment: str):
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

    def handle_enum(self, enum_type: proto2parser.Enum | proto3parser.Enum, avro_schema: AvroSchema, avro_namespace: str) -> AvroSchema:
        """
        Convert enum fields to avro schema.

        Args:
            enum_type: The enum type from the parsed proto file.
            avro_schema (list): The list to append the converted enum schema.
            namespace (str): The namespace for the enum.
        """
        comment = self.clean_comment(
            enum_type.comment.content if enum_type.comment and enum_type.comment.content else None)

        # Create avro schema
        avro_enum: AvroSchema = {
            'name': enum_type.name,
            'type': 'enum',
            'namespace': avro_namespace,
            'symbols': [],
            'ordinals': {}
        }

        if comment:
            avro_enum['doc'] = comment
        for value in enum_type.fields:
            avro_enum['symbols'].append(value.name)
            avro_enum['ordinals'][value.name] = int(value.number)
        avro_schema.append(avro_enum)
        self.generated_types[avro_enum['namespace']+'.'+avro_enum['name']] = "enum"
        return avro_enum

    def handle_message(self, proto_message_type: proto2parser.Message | proto3parser.Message, avro_schema: AvroSchema, avro_namespace: str)-> AvroSchema:
        """
        Convert protobuf messages to avro records.

        Args:
            m: The message type from the parsed proto file.
            avro_schema (list): The list to append the converted message schema.
            namespace (str): The namespace for the message.
        """
        dependencies = []

        comment = self.clean_comment(proto_message_type.comment.content if proto_message_type.comment and proto_message_type.comment.content else None)
        avro_record: AvroSchema = {
            'type': 'record',
            'name': proto_message_type.name,
            'namespace': avro_namespace,
            'fields': []
        }
        if comment:
            avro_record['doc'] = comment
        for proto_field in proto_message_type.fields:
            avro_type = self.get_avro_type_for_field(proto_message_type, avro_namespace, avro_schema, dependencies, proto_field)
            comment = self.clean_comment(proto_field.comment.content if proto_field.comment and proto_field.comment.content else None)

            avro_field = {
                'name': proto_field.name,
                'type': avro_type,
            }

            if comment:
                avro_field['doc'] = comment

            avro_record['fields'].append(avro_field)

        for proto_field in proto_message_type.oneofs:
            avro_oneof: AvroSchema = {
                'name': proto_field.name,
                'type': []
            }
            comment = self.clean_comment(proto_field.comment.content if proto_field.comment and proto_field.comment.content else None)
            if comment:
                avro_oneof['doc'] = comment
            for oneof_field in proto_field.fields:
                avro_type = self.get_avro_type_for_field(proto_message_type, avro_namespace, avro_schema, dependencies, oneof_field)
                comment = self.clean_comment(oneof_field.comment.content if oneof_field.comment and oneof_field.comment.content else None)
                if comment:
                    oneof_field['doc'] = comment
                avro_oneof['type'].append(avro_type)
            avro_record['fields'].append(avro_oneof)

        if dependencies:
            avro_record['dependencies'] = dependencies
        avro_schema.append(avro_record)
        for _, nested_message in proto_message_type.messages.items():
            nested_namespace = avro_namespace + '.' + proto_message_type.name + '_types'
            self.handle_message(nested_message, avro_schema, nested_namespace)
        # Convert enum fields
        for _, enum_type in proto_message_type.enums.items():
            nested_namespace = avro_namespace + '.' + proto_message_type.name + '_types'
            self.handle_enum(enum_type, avro_schema, nested_namespace)
        self.generated_types[avro_record['namespace']+'.'+avro_record['name']] = "record"
        return avro_record

    def get_avro_type_for_field(self, proto_message_type: proto2parser.Message | proto3parser.Message, avro_namespace: str, avro_schema: AvroSchema, dependencies: List[str], proto_field: proto2parser.Field | proto3parser.Field):
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
        avro_field_type: AvroSchema = None
        proto_field_type = proto_field.val_type if proto_field.label == 'repeated' or proto_field.type == 'map' else proto_field.type
        is_primitive, avro_field_type = self.proto_type_to_avro_primitive(proto_field_type)

        if not is_primitive:
            if proto_field.type in self.imported_types:
                avro_field_type = self.imported_types[proto_field.type]
            else:
                avro_field_type = avro_namespace + '.' + avro_field_type
                found_in_nested_definitions = False
                for k, nested_proto_message_type in proto_message_type.messages.items():
                    nested_namespace = avro_namespace + '.' + proto_message_type.name + '_types'
                    if nested_proto_message_type.name == proto_field_type:
                        avro_field_type = self.handle_message(nested_proto_message_type, avro_schema, nested_namespace)
                        del proto_message_type.messages[k]
                        if 'dependencies' in avro_field_type:
                            dependencies.extend(avro_field_type['dependencies'])
                            del avro_field_type['dependencies']
                        found_in_nested_definitions = True
                        break
                if not found_in_nested_definitions:
                    for k, nested_proto_enum_type in proto_message_type.enums.items():
                        nested_namespace = avro_namespace + '.' + proto_message_type.name + '_types'
                        if nested_proto_enum_type.name == proto_field_type:
                            avro_field_type = self.handle_enum(nested_proto_enum_type, avro_schema, nested_namespace)
                            del proto_message_type.enums[k]
                            found_in_nested_definitions = True
                            break
                if not found_in_nested_definitions:
                    dependency_avro_field_type = avro_field_type
                    while '.' in dependency_avro_field_type:
                        if dependency_avro_field_type in self.forward_references:
                            dependencies.append(dependency_avro_field_type)
                            break
                        n = dependency_avro_field_type.split('.')
                        dependency_avro_field_type = '.'.join(n[:-2]+[n[-1]])

        if proto_field.label == 'optional':
            avro_field_type = ["null", avro_field_type]
        if proto_field.label == 'repeated':
            avro_type: AvroSchema = {
                "type": "array",
                "items": avro_field_type
            }
        elif proto_field.type == 'map':
            avro_type: AvroSchema = {
                "type": "map",
                "values": avro_field_type,
            }
        else:
            avro_type = avro_field_type
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
    if not namespace:
        namespace = pascal(os.path.basename(proto_file_path).replace('.proto', ''))
    avro_schema = converter.convert_proto_to_avro_schema(proto_file_path, namespace, message_type)

    # Convert the Avro schema to JSON and write it to the file
    with open(avro_schema_path, 'w', encoding='utf-8') as avro_file:
        avro_file.write(json.dumps(avro_schema, indent=2))

