import asn1tools
from asn1tools.codecs.ber import Sequence, SequenceOf, Integer, Boolean, Enumerated, OctetString, IA5String, UTF8String
import json

def asn1_type_to_avro_type(asn1_type, avro_schema):
    """Convert an ASN.1 type to an Avro type."""
    avro_type = None
    if isinstance(asn1_type,Integer):
        avro_type = 'int'
    elif isinstance(asn1_type, Boolean):
        avro_type = 'boolean'
    elif isinstance(asn1_type, Enumerated):
        symbols = [member['name'] for member in asn1_type['members']]
        avro_type = {'type': 'enum', 'symbols': symbols, 'name': 'EnumType'}
    elif isinstance(asn1_type, Sequence):
        if avro_schema and next((s for s in avro_schema if s['name'] == asn1_type.type_name), None):
            return asn1_type.type_name
        else:
            fields = []
            for member in asn1_type.root_members:
                fields.append({
                    'name': member.name,
                    'type': asn1_type_to_avro_type(member, avro_schema)
                })
            avro_type = {'type': 'record', 'name': asn1_type.name, 'fields': fields}
    elif isinstance(asn1_type, SequenceOf):
        item_type = asn1_type_to_avro_type(asn1_type['element'])
        avro_type = {'type': 'array', 'items': item_type}
    elif isinstance(asn1_type, OctetString) and asn1_type.type == 'OCTET STRING':
        avro_type = 'bytes'
    elif isinstance(asn1_type, IA5String) or isinstance(asn1_type, UTF8String):
        avro_type = 'string'
    return avro_type

def convert_asn1_to_avro_schema(asn1_spec_path):
    """Convert ASN.1 specification to Avro schema."""
    spec = asn1tools.compile_files(asn1_spec_path)
    avro_schema = []
    for module_name, module in spec.modules.items():
        for type_name, asn1_type in module.items():
            avro_type = asn1_type_to_avro_type(asn1_type.type, avro_schema)
            if avro_type:
                avro_schema.append({
                    'namespace': module_name,
                    'type': avro_type if isinstance(avro_type, str) else avro_type['type'],
                    'name': type_name,
                    **({'fields': avro_type['fields']} if isinstance(avro_type, dict) and 'fields' in avro_type else {})
                })
    
    if len(avro_schema) == 1:
        return avro_schema[0]
    return avro_schema

def convert_asn1_to_avro(asn1_spec_path, avro_file_path):
    """Convert ASN.1 specification to Avro schema and save it to a file."""
    avro_schema = convert_asn1_to_avro_schema(asn1_spec_path)
    with open(avro_file_path, 'w') as file:
        json.dump(avro_schema, file, indent=4)

# Example usage:
# asn1_spec_path = 'path/to/your/asn1_spec.asn'
# output_file_path = 'path/to/save/avro_schema.json'
# avro_schema = convert_asn1_to_avro(asn1_spec_path)
# save_avro_schema_to_file(avro_schema, output_file_path)
