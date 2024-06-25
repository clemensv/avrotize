# pylint: disable=line-too-long, consider-iterating-dictionary, too-many-locals, too-many-branches

"""Converts XSD to Avro schema."""

import os
import re
from typing import Dict, List, Tuple
import xml.etree.ElementTree as ET
import json
from urllib.parse import urlparse
from avrotize.common import avro_namespace, generic_type

from avrotize.dependency_resolver import inline_dependencies_of, sort_messages_by_dependencies

XSD_NAMESPACE = 'http://www.w3.org/2001/XMLSchema'


class XSDToAvro:
    """ Convert XSD to Avro schema."""

    def __init__(self) -> None:
        """ Initialize the class. """
        self.simple_type_map: Dict[str, str | dict] = {}
        self.avro_namespace = ''

    def xsd_targetnamespace_to_avro_namespace(self, targetnamespace: str) -> str:
        """Convert a XSD namespace to Avro Namespace."""
        parsed_url = urlparse(targetnamespace)
        if parsed_url.scheme == 'urn':
            path_segments = parsed_url.path.strip(
                ':').replace('.', '-').split(':')
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
            type_name = xsd_type
            prefix = ''
        else:
            prefix, type_name = xsd_type.split(':', 1)
            if not type_name:
                type_name = prefix
                prefix = ''
        # find the namespace for the prefix
        ns = namespaces.get(XSD_NAMESPACE, '')
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
            return base_type_map.get(type_name, self.avro_namespace+'.'+type_name)
        else:
            return self.avro_namespace+'.'+type_name

    def process_element(self, element: ET.Element, namespaces: dict, dependencies: list):
        """Process an element in the XSD schema."""
        name = element.get('name')
        type_value = element.get('type', '')
        if type_value:
            avro_type = self.xsd_to_avro_type(type_value, namespaces)
            if not type_value.startswith(f'{namespaces[XSD_NAMESPACE]}:') and type_value not in self.simple_type_map.keys():
                dependencies.append(avro_type if isinstance(
                    avro_type, str) else avro_type.get('namespace')+'.'+avro_type.get('name'))
                dependencies = list(set(dependencies))
        else:
            complex_type = element.find(
                f'{{{XSD_NAMESPACE}}}complexType', namespaces)
            if complex_type is not None:
                complex_type.set('name', name)
                avro_type = self.process_complex_type(complex_type, namespaces)
            else:
                simple_type = element.find(
                    f'{{{XSD_NAMESPACE}}}simpleType', namespaces)
                if simple_type is not None:
                    add_to_schema, simple_type_type = self.process_simple_type(
                        simple_type, namespaces)
                    if add_to_schema:
                        avro_type = simple_type_type
                    else:
                        avro_type = self.simple_type_map[name]
                else:
                    raise ValueError('element must have a type or complexType')

        max_occurs = element.get('maxOccurs')
        if max_occurs is not None and max_occurs != '1':
            avro_type = {'type': 'array', 'items': avro_type}
        min_occurs = element.get('minOccurs')
        if min_occurs is not None and min_occurs == '0':
            avro_type = ['null', avro_type]
        avro_field = {'name': name, 'type': avro_type}
        annotation = element.find(f'{{{XSD_NAMESPACE}}}annotation', namespaces)
        if annotation is not None:
            documentation = annotation.find(
                f'{{{XSD_NAMESPACE}}}documentation', namespaces)
            if documentation is not None and documentation.text is not None:
                avro_field['doc'] = documentation.text.strip()
        return avro_field

    def process_complex_type(self, complex_type: ET.Element, namespaces: dict) -> dict | str:
        """ Process a complex type in the XSD schema."""
        dependencies: List[str] = []
        avro_type: dict = {
            'type': 'record',
            'name': complex_type.attrib.get('name'),
            'namespace': self.avro_namespace,
            'fields': []
        }
        avro_doc = ''
        annotation = complex_type.find(
            f'{{{XSD_NAMESPACE}}}annotation', namespaces)
        if annotation is not None:
            documentation = annotation.find(
                f'{{{XSD_NAMESPACE}}}documentation', namespaces)
            if documentation is not None and documentation.text is not None:
                avro_doc = documentation.text.strip()
                avro_type['doc'] = avro_doc
        fields = []
        for sequence in complex_type.findall(f'{{{XSD_NAMESPACE}}}sequence', namespaces):
            for el in sequence.findall(f'{{{XSD_NAMESPACE}}}element', namespaces):
                field = self.process_element(el, namespaces, dependencies)
                field['xmlkind'] = 'element'
                fields.append(field)
            if sequence.findall(f'{{{XSD_NAMESPACE}}}any', namespaces):
                fields.append({"name": "any", "xmlkind": "any", "type": generic_type()})
        for all_types in complex_type.findall(f'{{{XSD_NAMESPACE}}}all', namespaces):
            for el in all_types.findall(f'{{{XSD_NAMESPACE}}}element', namespaces):
                field = self.process_element(el, namespaces, dependencies)
                field['xmlkind'] = 'element'
                fields.append(field)
        for choice in complex_type.findall(f'{{{XSD_NAMESPACE}}}choice', namespaces):
            choices: list = []
            for el in choice.findall(f'{{{XSD_NAMESPACE}}}element', namespaces):
                deps: List[str] = []
                choice_field = self.process_element(el, namespaces, deps)
                choice_field['xmlkind'] = 'element'
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
        for attribute in complex_type.findall(f'.{{{XSD_NAMESPACE}}}attribute', namespaces):
            field = self.process_element(attribute, namespaces, dependencies)
            field['xmlkind'] = 'attribute'
            fields.append(field)
        for el in complex_type.findall(f'{{{XSD_NAMESPACE}}}simpleContent', namespaces):
            simple_content = el.find(
                f'{{{XSD_NAMESPACE}}}extension', namespaces)
            if simple_content is not None:
                base_type = simple_content.attrib.get('base')
                if base_type:
                    fields.append(
                        {"name": "value", "type": self.xsd_to_avro_type(base_type, namespaces)})
                    for se in simple_content.findall(f'{{{XSD_NAMESPACE}}}attribute', namespaces):
                        field = self.process_element(se, namespaces, dependencies)
                        field['xmlkind'] = 'attribute'
                        fields.append(field)
                else:
                    raise ValueError("No base found in simpleContent")

        avro_type['fields'] = fields
        if dependencies:
            avro_type['dependencies'] = dependencies
        return avro_type

    def process_simple_type(self, simple_type: ET.Element, namespaces: dict) -> Tuple[bool, dict | str]:
        """ Process a simple type in the XSD schema. """
        type_name = simple_type.attrib.get('name')
        if not type_name:
            raise ValueError("SimpleType must have a name")
        avro_doc = ''
        annotation = simple_type.find(
            f'{{{XSD_NAMESPACE}}}annotation', namespaces)
        if annotation is not None:
            documentation = annotation.find(
                f'{{{XSD_NAMESPACE}}}documentation', namespaces)
            if documentation is not None and documentation.text is not None:
                avro_doc = documentation.text.strip()

        for restriction in simple_type.findall(f'{{{XSD_NAMESPACE}}}restriction', namespaces):
            base_type = restriction.get('base')
            enums: List[str] = [el.attrib.get('value', 'Empty') for el in restriction.findall(
                f'{{{XSD_NAMESPACE}}}enumeration', namespaces)]
            # if any of the enum entries start with a digit, we need to prefix the entry with _
            if enums:
                for i, enum in enumerate(enums):
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
            elif base_type:
                # if the baseType is a decimal, get the precision and scale sub-element value attributes to set the logicalType
                if base_type == namespaces[XSD_NAMESPACE]+':'+'decimal':
                    precision = restriction.find(
                        f'{{{XSD_NAMESPACE}}}totalDigits', namespaces)
                    scale = restriction.find(
                        f'{{{XSD_NAMESPACE}}}fractionDigits', namespaces)
                    logical_type = {
                        'type': 'bytes',
                        'logicalType': 'decimal',
                        'precision': int(precision.attrib.get('value', 32)) if isinstance(precision, ET.Element) else 32,
                        'scale': int(scale.attrib.get('value', 6)) if isinstance(scale, ET.Element) else 6,
                    }
                    if avro_doc:
                        logical_type['doc'] = avro_doc
                    self.simple_type_map[type_name] = logical_type
                    return False, logical_type
                else:
                    self.simple_type_map[type_name] = self.xsd_to_avro_type(
                        base_type, namespaces)
                    return False, self.simple_type_map[type_name]
        raise ValueError("No content found in simple type")

    def process_top_level_element(self, element: ET.Element, namespaces: dict):
        """ Process a top level element in the XSD schema. """
        dependencies: List[str] = []
        avro_type: dict = {
            'type': 'record',
            'name': 'Root',
            'namespace': self.avro_namespace,
            'fields': []
        }
        annotation = element.find(f'{{{XSD_NAMESPACE}}}annotation', namespaces)
        if annotation is not None:
            documentation = annotation.find(
                f'{{{XSD_NAMESPACE}}}documentation', namespaces)
            if documentation is not None and documentation.text is not None:
                avro_type['doc'] = documentation.text.strip()

        if 'type' in element.attrib:
            field = self.process_element(element, namespaces, dependencies)
            field['xmlkind'] = 'element'
            avro_type['fields'].append(field)
            if dependencies:
                avro_type['dependencies'] = dependencies
            return avro_type
        else:
            complex_type = element.find(
                f'{{{XSD_NAMESPACE}}}complexType', namespaces)
            if complex_type is None:
                raise ValueError(
                    'top level element must have a type or be complexType')
            complex_type.set('name', element.get('name', ''))
            avro_complex_type = self.process_complex_type(
                complex_type, namespaces)
            return avro_complex_type

    def extract_xml_namespaces(self, xml_str: str):
        """ Extract XML namespaces from an XML string."""
        # This regex finds all xmlns:prefix="uri" declarations
        pattern = re.compile(r'xmlns:([\w]+)="([^"]+)"')
        namespaces = {m.group(2): m.group(1)
                      for m in pattern.finditer(xml_str)}
        return namespaces

    def xsd_to_avro(self, xsd_path: str, code_namespace: str | None = None):
        """ Convert XSD to Avro schema. """
        # load the XSD file into a string
        with open(xsd_path, 'r', encoding='utf-8') as f:
            xsd = f.read()

        namespaces = self.extract_xml_namespaces(xsd)
        root = ET.fromstring(xsd)
        target_namespace = root.get('targetNamespace')
        if target_namespace is None:
            raise ValueError('targetNamespace not found')
        if not code_namespace:
            self.avro_namespace = self.xsd_targetnamespace_to_avro_namespace(target_namespace)
        else:
            self.avro_namespace = code_namespace
        ET.register_namespace(namespaces[XSD_NAMESPACE], XSD_NAMESPACE)
        avro_schema: List[dict | list | str] = []

        for simple_type in root.findall(f'{{{XSD_NAMESPACE}}}simpleType', namespaces):
            add_to_schema, simple_type_type = self.process_simple_type(
                simple_type, namespaces)
            # we only want to append simple types if they are not resolved to one of the base types
            if add_to_schema:
                avro_schema.append(simple_type_type)
        for complex_type in root.findall(f'{{{XSD_NAMESPACE}}}complexType', namespaces):
            avro_schema.append(self.process_complex_type(
                complex_type, namespaces))

        top_level_elements = root.findall(
            f'{{{XSD_NAMESPACE}}}element', namespaces)
        if len(top_level_elements) == 1:
            record = self.process_top_level_element(
                top_level_elements[0], namespaces)
            inline_dependencies_of(avro_schema, record)
            return record
        for element in top_level_elements:
            avro_schema.append(self.process_top_level_element(
                element, namespaces))

        avro_schema = sort_messages_by_dependencies(avro_schema)
        if len(avro_schema) == 1:
            return avro_schema[0]
        else:
            return avro_schema

    def convert_xsd_to_avro(self, xsd_path: str, avro_path: str, namespace: str | None = None):
        """Convert XSD to Avro schema and write to a file."""
        
        
        avro_schema = self.xsd_to_avro(xsd_path, code_namespace=namespace)
        with open(avro_path, 'w', encoding='utf-8') as f:
            json.dump(avro_schema, f, indent=4)


def convert_xsd_to_avro(xsd_path: str, avro_path: str, namespace: str | None = None):
    """ 
    Convert XSD to Avro schema and write to a file. 
    
    Params:
    xsd_path: str - Path to the XSD file.
    avro_path: str - Path to the Avro file.
    namespace: str | None - Namespace of the Avro schema.    
    """
    
    if not os.path.exists(xsd_path):
        raise FileNotFoundError(f"XSD file not found at {xsd_path}")    
    if not namespace:
        namespace = os.path.splitext(os.path.basename(xsd_path))[0].lower().replace('-', '_')        
    xsd_to_avro = XSDToAvro()
    xsd_to_avro.convert_xsd_to_avro(xsd_path, avro_path, namespace)
