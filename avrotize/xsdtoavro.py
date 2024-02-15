import re
import xml.etree.ElementTree as ET
import json
import re
from urllib.parse import urlparse

from avrotize.dependency_resolver import inline_dependencies_of, sort_messages_by_dependencies

xsd_namespace = 'http://www.w3.org/2001/XMLSchema'

def xsd_targetnamespace_to_avro_namespace(targetnamespace: str) -> str:
    """Convert a XSD namespace to Avro Namespace."""
    parsed_url = urlparse(targetnamespace)
    path_segments = parsed_url.path.strip('/').split('/')
    reversed_path_segments = reversed(path_segments)
    namespace_prefix = '.'.join(reversed_path_segments)
    namespace_suffix = parsed_url.hostname
    namespace = f"{namespace_prefix}.{namespace_suffix}"
    return namespace

def xsd_to_avro_type(xsd_type: str, namespaces: dict):
    # split the type on the first colon
    prefix, type = xsd_type.split(':', 1)
    if not type:
        type = prefix
        prefix = ''
    # find the namespace for the prefix
    ns = namespaces.get(xsd_namespace, '')
    if ns == prefix:
        base_type_map = {
            'string': 'string',
            'int': 'int',
            'integer': 'int',
            'long': 'long',
            'short': 'int',
            'decimal': 'float',
            'float': 'float',
            'double': 'double',
            'boolean': 'boolean',
            'date': {'type': 'int', 'logicalType': 'date'},
            'dateTime': {'type': 'long', 'logicalType': 'timestamp-millis'},
        }
        return base_type_map.get(type, type)
    else:
        return type



def process_element(element: ET.Element, namespaces: dict, avro_schema: list, avro_namespace: str, dependencies: list):
    name = element.get('name')
    type = element.get('type','')
    minOccurs = element.get('minOccurs')
    maxOccurs = element.get('maxOccurs')
    
    if type.startswith(f'{namespaces[xsd_namespace]}:'):
        avro_type = xsd_to_avro_type(type, namespaces)
    else:
        avro_type = type    
        dependencies.append(type)

    if maxOccurs is not None and maxOccurs != '1':
        avro_type = {'type' : 'array', 'items': avro_type}
    if minOccurs is not None and minOccurs == '0':
        avro_type = ['null', avro_type]    

    return {'name': name, 'type': avro_type}  

def process_complex_type(complex_type: ET.Element, namespaces: dict, avro_schema: list, avro_namespace: str):
    dependencies = []
    avro_type = {
        'type': 'record', 
        'name': complex_type.attrib.get('name'),
        'namespace': avro_namespace,
        'fields': []
        }
    fields = []
    for el in complex_type.findall(f'.//{{{xsd_namespace}}}element', namespaces):
        fields.append(process_element(el, namespaces, avro_schema, avro_namespace, dependencies))
    avro_type['fields'] = fields
    if dependencies:
        avro_type['dependencies'] = dependencies
    return avro_type

def process_top_level_element(element: ET.Element, namespaces: dict, avro_schema: list, avro_namespace: str):
    dependencies = []
    avro_type = {
        'type': 'record', 
        'name': element.attrib.get('name'), 
        'namespace': avro_namespace,
        'fields': []
        }
    
    fields = []
    for el in element.findall(f'.//{{{xsd_namespace}}}element', namespaces):
        fields.append(process_element(el, namespaces, avro_schema, avro_namespace, dependencies))
    avro_type['fields'] = fields
    if dependencies:
        avro_type['dependencies'] = dependencies
    return avro_type

def extract_xml_namespaces(xml_str: str):
    # This regex finds all xmlns:prefix="uri" declarations
    pattern = re.compile(r'xmlns:([\w]+)="([^"]+)"')
    namespaces = {m.group(2): m.group(1) for m in pattern.finditer(xml_str)}
    return namespaces

def xsd_to_avro(xsd_path: str):
    # load the XSD file into a string
    with open(xsd_path, 'r') as f:
        xsd = f.read()

    namespaces = extract_xml_namespaces(xsd)
    root = ET.fromstring(xsd)
    targetNamespace = root.get('targetNamespace')
    if targetNamespace is None:
        raise ValueError('targetNamespace not found')
    avro_namespace = xsd_targetnamespace_to_avro_namespace(targetNamespace)
    ET.register_namespace(namespaces[xsd_namespace], xsd_namespace) 
    avro_schema = []
    
    for complex_type in root.findall(f'./{{{xsd_namespace}}}complexType', namespaces):
        avro_schema.append(process_complex_type(complex_type, namespaces, avro_schema, avro_namespace))

    top_level_elements = root.findall(f'./{{{xsd_namespace}}}element', namespaces)
    if len(top_level_elements) == 1:
        record = process_top_level_element(top_level_elements[0], namespaces, avro_schema, avro_namespace)
        inline_dependencies_of(avro_schema, record)
        return record
    for element in top_level_elements:
        avro_schema.append(process_top_level_element(element, namespaces, avro_schema, avro_namespace))    
    
    avro_schema = sort_messages_by_dependencies(avro_schema)
    if len(avro_schema) == 1:
        return avro_schema[0]
    else:
        return avro_schema

def convert_xsd_to_avro(xsd_path: str, avro_path: str):
    avro_schema = xsd_to_avro(xsd_path)
    with open(avro_path, 'w') as f:
        json.dump(avro_schema, f, indent=4)

# This approach dynamically handles namespaces by extracting the XSD namespace URI from the document
# and then uses it to correctly identify and process elements, types, etc.
