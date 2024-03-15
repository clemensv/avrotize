import re
from typing import Dict, List, Tuple
import xml.etree.ElementTree as ET
import json
import re
from urllib.parse import urlparse
from avrotize.common import avro_name, avro_namespace, generic_type

from avrotize.dependency_resolver import inline_dependencies_of, sort_messages_by_dependencies

xsd_namespace = 'http://www.w3.org/2001/XMLSchema'

class XSDToAvro:

    def __init__(self) -> None:
        self.simple_type_map: Dict[str,str | dict] = {}
        self.avro_namespace = ''

    def xsd_targetnamespace_to_avro_namespace(self, targetnamespace: str) -> str:
        """Convert a XSD namespace to Avro Namespace."""
        parsed_url = urlparse(targetnamespace)
        if parsed_url.scheme == 'urn':
            path_segments = parsed_url.path.strip(':').replace('.','-').split(':')
            # join all path segments that start with a number with the previous one
            new_path_segments: List[str] = []
            n = len(path_segments)
            for i in range(n):
                if path_segments[i][0].isdigit():
                    if i == 0:
                        new_path_segments.append('_'+path_segments[i])
                    else:
                        new_path_segments[-1] = f"{new_path_segments[-1]}-{path_segments[i]}"
                else:
                    new_path_segments.append(path_segments[i])
            path_segments = new_path_segments
        else:   
            path_segments = parsed_url.path.strip('/').split('/')
            path_segments = list(reversed(path_segments))
        namespace_prefix = '.'.join(path_segments)
        if parsed_url.hostname:
            namespace_suffix = parsed_url.hostname
            namespace = f"{namespace_prefix}.{namespace_suffix}"
        else:
            namespace = namespace_prefix
        return avro_namespace(namespace)

    def xsd_to_avro_type(self, xsd_type: str, namespaces: dict):
        """Convert a XSD type to an Avro type."""
        if xsd_type in self.simple_type_map:
            return self.simple_type_map[xsd_type]
        
        # split the type on the first colon
        if ':' not in xsd_type:
            type = xsd_type
            prefix = ''
        else:
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
                'decimal': {'type': 'bytes', 'logicalType': 'decimal', 'precision': 32, 'scale': 6},
                'float': 'float',
                'double': 'double',
                'boolean': 'boolean',
                'byte': 'int',
                'date': {'type': 'int', 'logicalType': 'date'},
                'dateTime': {'type': 'long', 'logicalType': 'timestamp-millis'},
                'time': {'type': 'int', 'logicalType': 'time-millis'},
                'duration': {'type': 'int', 'logicalType': 'duration'},
                'gYear': {'type': 'string'},
                'gYearMonth': {'type': 'string'},
                'gMonth': {'type': 'string'},
                'gMonthDay': {'type': 'string'},
                'gDay': {'type': 'string'},
                'nonNegativeInteger': 'int',
                'positiveInteger': 'int',
                'unsignedInt': 'int',
                'unsignedShort': 'int',
                'unsignedByte': 'int',
                'unsignedLong': 'long',
                'yearMonthDuration': {'type': 'string', 'logicalType': 'duration'},
                'dayTimeDuration': {'type': 'string', 'logicalType': 'duration'},
                'dateTimeStamp': {'type': 'long', 'logicalType': 'timestamp-millis'},
                'hexBinary': 'bytes',
                'base64Binary': 'bytes',
                'anyURI': 'string',
                'normalizedString': 'string',
                'token': 'string',
                'language': 'string',
                'Name': 'string',
                'NCName': 'string',
                'ENTITY': 'string',
                'ENTITIES': 'string',
                'ID': 'string',
                'IDREF': 'string',
                'IDREFS': 'string',
                'NMTOKEN': 'string',
                'NMTOKENS': 'string',
                'QName': 'string',
                'NOTATION': 'string'
            }
            return base_type_map.get(type, self.avro_namespace+'.'+type)
        else:
            return self.avro_namespace+'.'+type



    def process_element(self, element: ET.Element, namespaces: dict, avro_schema: list, dependencies: list):
        name = element.get('name')
        type = element.get('type','')
        avro_type = self.xsd_to_avro_type(type, namespaces)  
        if not type.startswith(f'{namespaces[xsd_namespace]}:') and type not in self.simple_type_map.keys():           
            dependencies.append(avro_type if isinstance(avro_type, str) else avro_type.get('namespace')+'.'+avro_type.get('name'))
            dependencies = list(set(dependencies))
        
        maxOccurs = element.get('maxOccurs')
        if maxOccurs is not None and maxOccurs != '1':
            avro_type = {'type' : 'array', 'items': avro_type}
        minOccurs = element.get('minOccurs')
        if minOccurs is not None and minOccurs == '0':
            avro_type = ['null', avro_type]    
        return {'name': name, 'type': avro_type}  


    def process_complex_type(self, complex_type: ET.Element, namespaces: dict, avro_schema: list) -> dict | str:
        dependencies: List[str] = []
        avro_type: dict  = {
            'type': 'record', 
            'name': complex_type.attrib.get('name'),
            'namespace': self.avro_namespace,
            'fields': []
            }
        avro_doc = ''
        annotation = complex_type.find(f'{{{xsd_namespace}}}annotation', namespaces)
        if annotation is not None:
            documentation = annotation.find(f'{{{xsd_namespace}}}documentation', namespaces)
            if documentation is not None and documentation.text is not None:
                avro_doc = documentation.text.strip()
                avro_type['doc'] = avro_doc
        fields = []
        for sequence in complex_type.findall(f'{{{xsd_namespace}}}sequence', namespaces):
            for el in sequence.findall(f'{{{xsd_namespace}}}element', namespaces):
                fields.append(self.process_element(el, namespaces, avro_schema, dependencies))
            if sequence.findall(f'{{{xsd_namespace}}}any', namespaces):
                fields.append({ "name": "any", "type": generic_type() })
        for all in complex_type.findall(f'{{{xsd_namespace}}}all', namespaces):
            for el in all.findall(f'{{{xsd_namespace}}}element', namespaces):
                fields.append(self.process_element(el, namespaces, avro_schema, dependencies))
        for choice in complex_type.findall(f'{{{xsd_namespace}}}choice', namespaces):
            choices: list = []
            for el in choice.findall(f'{{{xsd_namespace}}}element', namespaces):
                deps: List [str] = []
                choice_field = self.process_element(el, namespaces, avro_schema, deps)
                choice_record = {
                    'type': 'record',
                    'name': f'{complex_type.attrib.get("name")}_{choice_field["name"]}',
                    'fields': [choice_field],
                    'namespace': self.avro_namespace
                }
                if avro_doc:
                    choice_record['doc'] = avro_doc
                choices.append(choice_record)
                dependencies.extend(deps)
                dependencies = list(set(dependencies))
            choices_field = {
                'name': f'{complex_type.attrib.get("name")}',
                'type': choices
            }
            fields.append(choices_field)
        for attribute in complex_type.findall(f'.{{{xsd_namespace}}}attribute', namespaces):
            fields.append(self.process_element(attribute, namespaces, avro_schema, dependencies))
        for el in complex_type.findall(f'{{{xsd_namespace}}}simpleContent', namespaces):
            simple_content = el.find(f'{{{xsd_namespace}}}extension', namespaces)
            if simple_content is not None:
                baseType = simple_content.attrib.get('base')
                if baseType:
                    fields.append({"name": "value", "type": self.xsd_to_avro_type(baseType, namespaces)})
                    for se in simple_content.findall(f'{{{xsd_namespace}}}attribute', namespaces):
                        fields.append(self.process_element(se, namespaces, avro_schema, dependencies))
                else:
                    raise ValueError("No base found in simpleContent")

        avro_type['fields'] = fields
        if dependencies:
            avro_type['dependencies'] = dependencies
        return avro_type

    def process_simple_type(self, simple_type: ET.Element, namespaces: dict, avro_schema: list) -> Tuple[bool, dict | str]:
        type_name = simple_type.attrib.get('name')
        if not type_name:
            raise ValueError("SimpleType must have a name")
        avro_doc = ''
        annotation = simple_type.find(f'{{{xsd_namespace}}}annotation', namespaces)
        if annotation is not None:
            documentation = annotation.find(f'{{{xsd_namespace}}}documentation', namespaces)
            if documentation is not None and documentation.text is not None:
                avro_doc = documentation.text.strip()
        
        for restriction in simple_type.findall(f'{{{xsd_namespace}}}restriction', namespaces):
            baseType = restriction.get('base')
            enums: List[str] = [el.attrib.get('value','Empty') for el in restriction.findall(f'{{{xsd_namespace}}}enumeration', namespaces)]
            # if any of the enum entries start with a digit, we need to prefix the entry with _
            if enums:
                for i,enum in enumerate(enums):
                    if enums[i][0].isdigit():
                        enums[i] = '_'+enum
                enum_type = {
                    'type': 'enum', 
                    'name': simple_type.attrib.get('name'), 
                    'namespace': self.avro_namespace,
                    'symbols': enums
                    }
                if avro_doc:
                    enum_type['doc'] = avro_doc
                return True, enum_type
            elif baseType:
                # if the baseType is a decimal, get the precision and scale sub-element value attributes to set the logicalType
                if baseType == namespaces [xsd_namespace]+':'+'decimal':
                    precision = restriction.find(f'{{{xsd_namespace}}}totalDigits', namespaces)
                    scale = restriction.find(f'{{{xsd_namespace}}}fractionDigits', namespaces)
                    logicalType = {
                        'type': 'bytes', 
                        'logicalType': 'decimal', 
                        'precision': int(precision.attrib.get('value', 32)) if isinstance(precision, ET.Element) else 32, 
                        'scale': int(scale.attrib.get('value', 6)) if isinstance(scale, ET.Element) else 6,
                        }
                    if avro_doc:
                        logicalType['doc'] = avro_doc
                    self.simple_type_map[type_name] = logicalType
                    return False, logicalType                    
                else:
                    self.simple_type_map[type_name] = self.xsd_to_avro_type(baseType, namespaces)
                    return False, self.simple_type_map[type_name]
        raise ValueError("No content found in simple type")

    def process_top_level_element(self, element: ET.Element, namespaces: dict, avro_schema: list):
        dependencies: List[str] = []
        avro_type: dict = {
            'type': 'record', 
            'name': 'Root', 
            'namespace': self.avro_namespace,
            'fields': []
            }
        annotation = element.find(f'{{{xsd_namespace}}}annotation', namespaces)
        if annotation is not None:
            documentation = annotation.find(f'{{{xsd_namespace}}}documentation', namespaces)
            if documentation is not None and documentation.text is not None:
                avro_type['doc'] = documentation.text.strip()
        
        if 'type' in element.attrib:
            avro_type['fields'].append(self.process_element(element, namespaces, avro_schema, dependencies))
            if dependencies:
                avro_type['dependencies'] = dependencies
            return avro_type
        else:
            complex_type = element.find(f'{{{xsd_namespace}}}complexType', namespaces)
            if complex_type is None:
                raise ValueError('top level element must have a type or be complexType')
            complex_type.set('name', element.get('name',''))
            avro_complex_type = self.process_complex_type(complex_type, namespaces, avro_schema)
            return avro_complex_type

    def extract_xml_namespaces(self, xml_str: str):
        # This regex finds all xmlns:prefix="uri" declarations
        pattern = re.compile(r'xmlns:([\w]+)="([^"]+)"')
        namespaces = {m.group(2): m.group(1) for m in pattern.finditer(xml_str)}
        return namespaces

    def xsd_to_avro(self, xsd_path: str):
        # load the XSD file into a string
        with open(xsd_path, 'r') as f:
            xsd = f.read()

        namespaces = self.extract_xml_namespaces(xsd)
        root = ET.fromstring(xsd)
        targetNamespace = root.get('targetNamespace')
        if targetNamespace is None:
            raise ValueError('targetNamespace not found')
        self.avro_namespace = self.xsd_targetnamespace_to_avro_namespace(targetNamespace)
        ET.register_namespace(namespaces[xsd_namespace], xsd_namespace) 
        avro_schema: List[dict|list|str] = []
        
        for simple_type in root.findall(f'{{{xsd_namespace}}}simpleType', namespaces):
            add_to_schema, simple_type_type = self.process_simple_type(simple_type, namespaces, avro_schema)
            # we only want to append simple types if they are not resolved to one of the base types
            if add_to_schema:
                avro_schema.append(simple_type_type)
        for complex_type in root.findall(f'{{{xsd_namespace}}}complexType', namespaces):
            avro_schema.append(self.process_complex_type(complex_type, namespaces, avro_schema))    

        top_level_elements = root.findall(f'{{{xsd_namespace}}}element', namespaces)
        if len(top_level_elements) == 1:
             record = self.process_top_level_element(top_level_elements[0], namespaces, avro_schema)
             inline_dependencies_of(avro_schema, record)
             return record
        for element in top_level_elements:
            avro_schema.append(self.process_top_level_element(element, namespaces, avro_schema))    
        
        avro_schema = sort_messages_by_dependencies(avro_schema)
        if len(avro_schema) == 1:
            return avro_schema[0]
        else:
            return avro_schema

    def convert_xsd_to_avro(self, xsd_path: str, avro_path: str, namespace: str | None = None):
        avro_schema = self.xsd_to_avro(xsd_path)
        with open(avro_path, 'w') as f:
            json.dump(avro_schema, f, indent=4)

def convert_xsd_to_avro(xsd_path: str, avro_path: str, namespace: str | None = None):
    xsd_to_avro = XSDToAvro()
    xsd_to_avro.convert_xsd_to_avro(xsd_path, avro_path, namespace)