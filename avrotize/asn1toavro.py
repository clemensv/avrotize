import json
from typing import List
import asn1tools
from asn1tools.codecs.ber import Sequence, SequenceOf, Integer, Boolean, Enumerated, OctetString, IA5String, UTF8String, Date, Real, Choice, Null, SetOf, Recursive, ExplicitTag
from asn1tools.codecs.ber import BitString, VisibleString, BMPString, GeneralString, GraphicString, NumericString, PrintableString, TeletexString, UniversalString, ObjectIdentifier, Set

from avrotize.common import avro_name
from avrotize.dependency_resolver import sort_messages_by_dependencies

def asn1_type_to_avro_type(asn1_type: dict, avro_schema: list, namespace: str, parent_type_name: str | None, parent_member_name: str | None, dependencies: list)-> str | dict | None:
    """Convert an ASN.1 type to an Avro type."""
    avro_type: str | dict | list | None = None
    deps: List[str] = []

    if isinstance(asn1_type,Integer):
        avro_type = 'int'
    elif isinstance(asn1_type, Boolean):
        avro_type = 'boolean'
    elif isinstance(asn1_type, Enumerated):
        symbols = [avro_name(member) for member in asn1_type.data_to_value.keys()]
        avro_type = { 
            'type': 'enum', 
            'name': avro_name(parent_member_name if parent_member_name else asn1_type.name),
            'namespace': namespace, 
            'symbols': symbols
            }
    elif isinstance(asn1_type, Sequence) or isinstance(asn1_type, Choice) or isinstance(asn1_type, Set):
        if avro_schema and next((s for s in avro_schema if s.get('name') == asn1_type.type_name), []):
            return namespace + '.' + asn1_type.type_name
        else:
            record_name = asn1_type.type_name if asn1_type.type_name else asn1_type.name
            if record_name == 'CHOICE' or record_name == 'SEQUENCE' or record_name == 'SET':
                if parent_member_name and parent_type_name:
                    record_name = parent_type_name + parent_member_name         
                elif parent_type_name:
                    record_name = parent_type_name 
                else:
                    raise ValueError(f"Can't name record without a type name and member name")      

            fields = []
            for member in asn1_type.members if isinstance(asn1_type, Choice) else asn1_type.root_members:
                field_type = asn1_type_to_avro_type(member, avro_schema, namespace, record_name, member.name, deps)
                if isinstance(field_type, dict) and (field_type.get('type') == 'record' or field_type.get('type') == 'enum'):
                    existing_type = next((t for t in avro_schema if (isinstance(t,dict) and t.get('name') == field_type['name'] and t.get('namespace') == field_type.get('namespace')) ), None)
                    if not existing_type:
                        field_type['dependencies'] = [dep for dep in deps if dep != field_type['namespace']+'.'+field_type['name']]
                        avro_schema.append(field_type)
                    field_type = namespace + '.' + field_type.get('name','')
                    dependencies.append(field_type)
                if isinstance(asn1_type, Choice):
                    field_type = [field_type, 'null']
                fields.append({
                    'name': avro_name(member.name),
                    'type': field_type
                })
            
            avro_type = {
                'type': 'record', 
                'name': avro_name(record_name),
                'namespace': namespace,
                'fields': fields
                }
    elif isinstance(asn1_type, BitString):
        avro_type = {
            'type': 'bytes',
            'logicalType': 'bitstring'
        }
    elif isinstance(asn1_type, ObjectIdentifier):
        avro_type = 'string'
    elif isinstance(asn1_type, SequenceOf):
        record_name = asn1_type.name if asn1_type.name else parent_member_name
        if record_name == 'SEQUENCE OF':
            record_name = parent_member_name if parent_member_name else ''
        if parent_type_name:
            record_name = parent_type_name + record_name
        item_type = asn1_type_to_avro_type(asn1_type.element_type, avro_schema, namespace, record_name, 'Item', deps)
        if isinstance(item_type, dict) and not item_type.get('name'):
            item_type['name'] = asn1_type.name + 'Item'
        avro_type = {
            'type': 'array',
            'namespace': namespace, 
            'name': avro_name(record_name), 
            'items': item_type
            }
    elif isinstance(asn1_type, SetOf):
        record_name = asn1_type.name if asn1_type.name else parent_member_name
        if record_name == 'SET OF':
            record_name = parent_member_name if parent_member_name else ''                
        if parent_type_name:
            record_name = parent_type_name + record_name
        item_type = asn1_type_to_avro_type(asn1_type.element_type, avro_schema, namespace, record_name, 'Item', deps)
        if isinstance(item_type, dict) and not item_type.get('name'):
            item_type['name'] = asn1_type.name + 'Item'               
        avro_type = {
            'type': 'array',
            'namespace': namespace, 
            'name': avro_name(record_name),
            'items': item_type
            }
    elif isinstance(asn1_type, OctetString):
        avro_type = 'bytes'
    elif isinstance(asn1_type, IA5String) or isinstance(asn1_type, UTF8String) or isinstance(asn1_type, VisibleString) or isinstance(asn1_type, BMPString) or isinstance(asn1_type, GeneralString) or isinstance(asn1_type, GraphicString) or isinstance(asn1_type, NumericString) or isinstance(asn1_type, PrintableString) or isinstance(asn1_type, TeletexString) or isinstance(asn1_type, UniversalString):
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
        avro_type = asn1_type_to_avro_type(asn1_type.inner, avro_schema, namespace, parent_type_name, parent_member_name, dependencies)
        if isinstance(avro_type, dict):
            avro_type['name'] = asn1_type.name if asn1_type.name else asn1_type.type_name
    else:
        raise ValueError(f"Don't know how to translate ASN.1 type '{type(asn1_type)}' to Avro")

    if len(avro_schema) > 0 and isinstance(avro_type, dict) and 'name' in avro_type:
        existing_type = next((t for t in avro_schema if (isinstance(t,dict) and t.get('name') == avro_type['name'] and t.get('namespace') == avro_type.get('namespace')) ), None)
        if existing_type:
            qualified_name = namespace + '.' + existing_type.get('name','')
            dependencies.append(qualified_name)
            return qualified_name
            
    return avro_type

def convert_asn1_to_avro_schema(asn1_spec_list: List[str]):
    """Convert ASN.1 specification to Avro schema."""
    
    try:
        spec = asn1tools.compile_files(asn1_spec_list)
    except asn1tools.ParseError as e:
        print(f"Error parsing ASN.1 files: {e.args[0]}")
        for file in asn1_spec_list:
            print(f"  {file}")
        return None
    
    avro_schema: List[dict] = []
    for module_name, module in spec.modules.items():
        for type_name, asn1_type in module.items():
            dependencies: List [str] = []
            avro_type = asn1_type_to_avro_type(asn1_type.type, avro_schema, avro_name(module_name), type_name, None, dependencies)
            if avro_type and not isinstance(avro_type, str):
                avro_type['dependencies'] = [dep for dep in dependencies if dep != avro_type['namespace'] + '.' + avro_type['name']]
                avro_schema.append(avro_type)
    
    avro_schema = sort_messages_by_dependencies(avro_schema)

    if len(avro_schema) == 1:
        return avro_schema[0]
    return avro_schema

def convert_asn1_to_avro(asn1_spec_list: List[str], avro_file_path: str):
    """Convert ASN.1 specification to Avro schema and save it to a file."""
    avro_schema = convert_asn1_to_avro_schema(asn1_spec_list)
    if avro_schema is None:
        return
    with open(avro_file_path, 'w') as file:
        json.dump(avro_schema, file, indent=4)
