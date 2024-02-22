import re
from typing import Dict, List, Tuple
import xml.etree.ElementTree as ET
import json
import re
from urllib.parse import urlparse
from avrotize.common import avro_name, generic_type

from avrotize.dependency_resolver import inline_dependencies_of, sort_messages_by_dependencies

xsd_namespace = 'http://www.w3.org/2001/XMLSchema'

class XSDToAvro:

    def __init__(self) -> None:
        self.simple_type_map: Dict[str,str] = {}
        self.avro_namespace = ''

    def xsd_targetnamespace_to_avro_namespace(self, targetnamespace: str) -> str:
        """Convert a XSD namespace to Avro Namespace."""
        parsed_url = urlparse(targetnamespace)
        path_segments = parsed_url.path.strip('/').split('/')
        reversed_path_segments = reversed(path_segments)
        namespace_prefix = '.'.join(reversed_path_segments)
        namespace_suffix = parsed_url.hostname
        namespace = f"{namespace_prefix}.{namespace_suffix}"
        return avro_name(namespace)

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
                'decimal': 'float',
                'float': 'float',
                'double': 'double',
                'boolean': 'boolean',
                'date': {'type': 'int', 'logicalType': 'date'},
                'dateTime': {'type': 'long', 'logicalType': 'timestamp-millis'},
            }
            return base_type_map.get(type, self.avro_namespace+'.'+type)
        else:
            return self.avro_namespace+'.'+type



    def process_element(self, element: ET.Element, namespaces: dict, avro_schema: list, dependencies: list):
        name = element.get('name')
        type = element.get('type','')
        minOccurs = element.get('minOccurs')
        maxOccurs = element.get('maxOccurs')
        if type.startswith(f'{namespaces[xsd_namespace]}:'):
            avro_type = self.xsd_to_avro_type(type, namespaces)
        else:
            avro_type = self.xsd_to_avro_type(type, namespaces)  
            if not type.startswith(f'{namespaces[xsd_namespace]}:') and type not in self.simple_type_map.keys():           
                dependencies.append(avro_type if isinstance(avro_type, str) else avro_type.get('namespace')+'.'+avro_type.get('name'))
        if maxOccurs is not None and maxOccurs != '1':
            avro_type = {'type' : 'array', 'items': avro_type}
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
                choices.append(choice_record)
                dependencies.extend(deps)
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
        
        for restriction in simple_type.findall(f'{{{xsd_namespace}}}restriction', namespaces):
            baseType = restriction.get('base')
            enums = [el.attrib.get('value') for el in restriction.findall(f'{{{xsd_namespace}}}enumeration', namespaces)]
            if enums:
                return True, {
                    'type': 'enum', 
                    'name': simple_type.attrib.get('name'), 
                    'namespace': self.avro_namespace,
                    'symbols': enums
                    }
            elif baseType:
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
        
        if 'type' in element.attrib:
            avro_type['fields'].append(self.process_element(element, namespaces, avro_schema, dependencies))
            if dependencies:
                avro_type['dependencies'] = dependencies
            return avro_type
        else:
            fields = []
            for el in element.findall(f'.//{{{xsd_namespace}}}complexType', namespaces):
                fields.append(self.process_element(el, namespaces, avro_schema, dependencies))
            avro_type['fields'] = fields
            if dependencies:
                avro_type['dependencies'] = dependencies
            return avro_type

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