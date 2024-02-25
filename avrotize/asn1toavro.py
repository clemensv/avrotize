import json
import asn1tools
from asn1tools.codecs.ber import Sequence, SequenceOf, Integer, Boolean, Enumerated, OctetString, IA5String, UTF8String, Date, Real, Choice, Null, SetOf, Recursive, ExplicitTag

from avrotize.common import avro_name

def asn1_type_to_avro_type(asn1_type, avro_schema, namespace):
    """Convert an ASN.1 type to an Avro type."""
    avro_type = None
    if isinstance(asn1_type,Integer):
        avro_type = 'int'
    elif isinstance(asn1_type, Boolean):
        avro_type = 'boolean'
    elif isinstance(asn1_type, Enumerated):
        symbols = [member for member in asn1_type.data_to_value.keys()]
        avro_type = { 
            'type': 'enum', 
            'name': asn1_type.name,
            'namespace': namespace, 
            'symbols': symbols
            }
    elif isinstance(asn1_type, Sequence):
        if avro_schema and next((s for s in avro_schema if s.get('name') == asn1_type.type_name), None):
            return asn1_type.type_name
        else:
            fields = []
            for member in asn1_type.root_members:
                fields.append({
                    'name': member.name,
                    'type': asn1_type_to_avro_type(member, avro_schema, namespace)
                })
            avro_type = {
                'type': 'record', 
                'name': asn1_type.name if asn1_type.name else asn1_type.type_name, 
                'namespace': namespace,
                'fields': fields}
    elif isinstance(asn1_type, Choice):
        # translate to Avro record with only one field, the choice field
        choices = []
        for i, member in enumerate(asn1_type.members):
            choice = asn1_type_to_avro_type(member, avro_schema, namespace)
            if isinstance(choice, dict) and not choice.get('name'):
                choice['name'] = asn1_type.name + str(i+1)
            choices.append(asn1_type_to_avro_type(member, avro_schema, namespace))
        avro_type = {
            'type': 'record', 
            'namespace': namespace, 
            'name': asn1_type.name if asn1_type.name else asn1_type.type_name,
            'fields': [{'name': 'choice', 'type': choices}]
            }
    elif isinstance(asn1_type, SequenceOf):
        item_type = asn1_type_to_avro_type(asn1_type.element_type, avro_schema, namespace)
        if isinstance(item_type, dict) and not item_type.get('name'):
                item_type['name'] = asn1_type.name + 'Item'
        avro_type = {
            'type': 'array',
            'namespace': namespace, 
            'name': asn1_type.name, 
            'items': item_type
            }
    elif isinstance(asn1_type, SetOf):
        item_type = asn1_type_to_avro_type(asn1_type.element_type, avro_schema, namespace)
        if isinstance(item_type, dict) and not item_type.get('name'):
            item_type['name'] = asn1_type.name + 'Item'
        avro_type = {
            'type': 'array',
            'namespace': namespace, 
            'name': asn1_type.name, 
            'items': item_type
            }
    elif isinstance(asn1_type, OctetString):
        avro_type = 'bytes'
    elif isinstance(asn1_type, IA5String) or isinstance(asn1_type, UTF8String):
        avro_type = 'string'
    elif isinstance(asn1_type, Date):
        avro_type = {'type': 'int', 'logicalType': 'date'}
    elif isinstance(asn1_type, Real):
        avro_type = 'double'
    elif isinstance(asn1_type, Null):
        avro_type = 'null'
    elif isinstance(asn1_type, Recursive):
        avro_type = asn1_type.type_name
    elif isinstance(asn1_type, ExplicitTag):
        avro_type = asn1_type_to_avro_type(asn1_type.inner, avro_schema, namespace)
        if isinstance(avro_type, dict):
            avro_type['name'] = asn1_type.name if asn1_type.name else asn1_type.type_name
    else:
        raise ValueError(f"Don't know how to translate ASN.1 type '{type(asn1_type)}' to Avro")

    if len(avro_schema) > 0 and 'name' in avro_type:
        existing_type = next((t for t in avro_schema if (isinstance(t,dict) and t.get('name') == avro_type['name'] and t.get('namespace') == avro_type.get('namespace')) ), None)
        if existing_type:
            return existing_type.get('name')
    
    return avro_type

def convert_asn1_to_avro_schema(asn1_spec_path):
    """Convert ASN.1 specification to Avro schema."""
    
    spec = asn1tools.compile_files(asn1_spec_path)
    
    avro_schema = []
    for module_name, module in spec.modules.items():
        for type_name, asn1_type in module.items():
            avro_type = asn1_type_to_avro_type(asn1_type.type, avro_schema, avro_name(module_name))
            if avro_type and not isinstance(avro_type, str):
                avro_schema.append(avro_type)
    
    if len(avro_schema) == 1:
        return avro_schema[0]
    return avro_schema

def convert_asn1_to_avro(asn1_spec_path, avro_file_path):
    """Convert ASN.1 specification to Avro schema and save it to a file."""
    avro_schema = convert_asn1_to_avro_schema(asn1_spec_path)
    with open(avro_file_path, 'w') as file:
        json.dump(avro_schema, file, indent=4)
