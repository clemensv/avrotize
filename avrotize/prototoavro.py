import argparse
import json
import os

from avrotize.dependency_resolver import sort_messages_by_dependencies

from . import proto2parser
from . import proto3parser
import re
    
isomorphic_types = ['float', 'double', 'bytes', 'string']
imported_types = {}

def proto_type_to_avro_primitive(proto_type):
    """Map Protobuf types to Avro primitive types."""
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
        #'NullValue': 'null', 
        'google.protobuf.Timestamp' : {
            "type": "long",
            "logicalType": "timestamp-micros"
        }
    }
    if proto_type in isomorphic_types:
        return proto_type
    return mapping.get(proto_type, proto_type)  


def convert_proto_to_avro(proto_file_path: str, avro_schema_path: str):
    """Convert Protobuf .proto file to Avro schema."""

    def convert_proto_to_avro_schema(proto_file_path: str) -> list:
        """Convert .proto file to Avro schema."""
        with open(proto_file_path, 'r') as proto_file:
            proto_schema = proto_file.read()

        # determine whether we have proto3 or proto2 and parse the data
        if re.search(r'syntax\s*=\s*"proto3"', proto_schema):
            data = proto3parser.parse(proto_schema)
        else:
            data = proto2parser.parse(proto_schema)

        # get the namespace
        if data.package:
            namespace = data.package.value
        else:
            namespace = ''
        # Avro schema header
        avro_schema = []
        
        for import_ in data.imports:
            # handle protobuf imports
            if import_.startswith('google/protobuf/'):
                script_path = os.path.dirname(os.path.abspath(__file__))
                avsc_dir = os.path.join(script_path, 'prototypes')            

                # load the corresponding avsc file from ./prototypes at this script's path into avro_schema
                avsc = f'{avsc_dir}/{import_.replace("google/protobuf/", "").replace(".proto", ".avsc")}'
                with open(avsc, 'r') as avsc_file:
                    types = json.load(avsc_file)
                    for t in types:
                        imported_types[t["namespace"]+"."+t["name"]] = t                
            else:
                # find the path relative to the current directory
                cwd = os.path.join(os.getcwd(),os.path.dirname(proto_file_path))
                import_path = os.path.join(cwd, import_)
                # raise an exception if the imported file does not exist
                if not os.path.exists(import_path):
                    raise FileNotFoundError(f'Import file {import_path} does not exist.')
                                
                avro_schema.extend(convert_proto_to_avro_schema(import_path))
                    
        
        ## Convert message fields
        for _, m in data.messages.items():
            handle_message(m, avro_schema, namespace)        

        ## Convert enum fields
        for _, enum_type in data.enums.items():
            handle_enum(enum_type, avro_schema, namespace)

        # Sort the messages in avro_schema by dependencies
        return sort_messages_by_dependencies(avro_schema)
        
    
    avro_schema = convert_proto_to_avro_schema(proto_file_path)
    ## Convert the Avro schema to JSON and write it to the file
    with open(avro_schema_path, 'w') as avro_file:
        avro_file.write(json.dumps(avro_schema, indent=2))

    print(f'Converted {proto_file_path} to {avro_schema_path}')


def handle_enum(enum_type, avro_schema, namespace):
    """Convert enum fields to avro schema."""
    comment = enum_type.comment.content if enum_type.comment and enum_type.comment.content else None
    if comment:
        # strip slashes, newlines, linefeeds from the comment and then extra whitespace
        comment = comment.replace('//', '').replace('\n','').lstrip().rstrip()

    # create avro schema
    avro_enum = {
            'name': enum_type.name,
            'type': 'enum',
            'namespace' : namespace,
            'symbols': [],
            'dependencies': []
        }

    if comment:
        avro_enum['doc'] = comment

    for value in enum_type.fields:
        avro_enum['symbols'].append(value.name)

    avro_schema.append(avro_enum)


def handle_message(m, avro_schema, namespace):
    """Convert protobuf messages to avro records."""
    dependencies = []

    comment = m.comment.content if m.comment and m.comment.content else None
    if comment:
           # strip slashes, newlines, linefeeds from the comment and then extra whitespace
       comment = comment.replace('//', '').replace('\n','').lstrip().rstrip()
    avro_record = {
            'type': 'record',
            'name': m.name,
            'namespace' : namespace,
            'fields': []
        }
    if comment:
        avro_record['doc'] = comment

        
    for f in m.fields:
        avro_type = get_avro_type_for_field(m, namespace, dependencies, f)
        comment = f.comment.content if f.comment and f.comment.content else None
        if comment:
                # strip slashes, newlines, linefeeds from the comment and then extra whitespace
            comment = comment.replace('//', '').replace('\n','').lstrip().rstrip()

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
        comment = f.comment.content if f.comment and f.comment.content else None
        if comment:
                # strip slashes, newlines, linefeeds from the comment and then extra whitespace
            comment = comment.replace('//', '').replace('\n','').lstrip().rstrip()
            avro_oneof['doc'] = comment

        for o in f.fields:
            avro_type = get_avro_type_for_field(m, namespace, dependencies, o)
            comment = o.comment.content if o.comment and o.comment.content else None
            if comment:
                # strip slashes, newlines, linefeeds from the comment and then extra whitespace
                comment = comment.replace('//', '').replace('\n','').lstrip().rstrip()
            avro_oneof['type'].append(avro_type)            

        avro_record['fields'].append(avro_oneof)

    if dependencies:
       avro_record['dependencies'] = dependencies    
    avro_schema.append(avro_record)

    for _, mi in m.messages.items():
        handle_message(mi, avro_schema, namespace)        

    ## Convert enum fields
    for _, enum_type in m.enums.items():
        handle_enum(enum_type, avro_schema, namespace)

def get_avro_type_for_field(m, namespace, dependencies, f):
    field_type = None
    is_custom = False
    if f.label == 'repeated' or f.type == 'map':
        field_type = proto_type_to_avro_primitive(f.val_type)
        is_custom = field_type == f.val_type and field_type not in isomorphic_types
    else:
        field_type = proto_type_to_avro_primitive(f.type)
        is_custom = field_type == f.type and field_type not in isomorphic_types

    if is_custom:
        if f.type in imported_types:
            field_type = imported_types[f.type]
            imported_types[f.type] = f.type
        else:
            found = False
            for k, mi in m.messages.items():
                if mi.name == field_type:
                    schema = []
                    handle_message(mi, schema, namespace)
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
                "items" : field_type                    
                }
    elif f.type == 'map':
        avro_type = {
                "type": "map",
                "values" : field_type,
                }
    else:
        avro_type = field_type
    return avro_type


