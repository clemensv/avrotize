from functools import reduce
import json
from typing import Dict, List
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom

from avrotize.common import is_generic_avro_type

class AvroToXSD:
    def __init__(self, target_namespace: str = ''):
        self.xmlns = {"xs": "http://www.w3.org/2001/XMLSchema"}
        self.union_types: Dict[str, str] = {}
        self.known_types: List[str] = []
        self.common_namespace = ''
        self.target_namespace = target_namespace

    def find_common_namespace(self, namespaces: List[str]) -> str:
        """
        Find the common namespace prefix from a list of namespaces.
        """
        if not namespaces:
            return ''

        def common_prefix(a, b):
            prefix = ''
            for a_char, b_char in zip(a.split('.'), b.split('.')):
                if a_char == b_char:
                    prefix += a_char + '.'
                else:
                    break
            return prefix.rstrip('.')

        return reduce(common_prefix, namespaces)

    def update_common_namespace(self, namespace: str) -> None:
        """
        Update the common namespace based on the provided namespace.
        """
        if not self.common_namespace:
            self.common_namespace = namespace
        else:
            self.common_namespace = self.find_common_namespace([self.common_namespace, namespace])

    def convert_avro_primitive(self, avro_type: str | dict) -> str:
        """Map Avro primitive types to XML schema (XSD) data types."""
        
        if isinstance(avro_type, dict) and 'logicalType' in avro_type:
            type = avro_type['type']
            logical_type = avro_type.get('logicalType')
            
            if logical_type == 'decimal':
                return f"decimal"
            if logical_type == 'timestamp-millis':
                return f"dateTime"
            if logical_type == 'date':
                return f"date"
            if logical_type in {'time-millis', 'time-micros'}:
                return f"time"
            if logical_type == 'uuid':
                return f"string"
        elif isinstance(avro_type, str):
            mapping = {
                'null': 'string',  # Defaulting to string for nullables
                'boolean': 'boolean',
                'int': 'integer',
                'long': 'long',
                'float': 'float',
                'double': 'double',
                'bytes': 'hexBinary',
                'string': 'string',
            }
            
            type = mapping.get(avro_type, '')  # Fallback to string
            if type:
                return f"xs:{type}"
            else:
                return avro_type.split('.')[-1]
        return f"xs:string"
    
    def is_avro_primitive(self, avro_type: str) -> bool:
        """Check if the Avro type is a primitive type."""
        if isinstance(avro_type, dict) and 'logicalType' in avro_type and 'type' in avro_type: 
            return avro_type['type'] in {'int', 'long', 'float', 'double', 'bytes', 'string'}
        elif isinstance(avro_type, str):
            return avro_type in {'null', 'boolean', 'int', 'long', 'float', 'double', 'bytes', 'string'}
        else:
            return False

    def create_element(self, parent: Element, tag: str, **attributes) -> Element:
        """Create an XML element with the proper namespace."""
        return SubElement(parent, f"{{{self.xmlns['xs']}}}{tag}", **attributes)
    
    def create_complex_type(self, parent: Element, **attributes) -> Element:
        """Create an XML complexType element."""
        return self.create_element(parent, "complexType", **attributes)

    def create_fixed(self, schema_root, field_type):
        """ handle Avro 'fixed' type"""
        simple_type = self.create_element(schema_root, "simpleType", name=field_type['name'])
        restriction = self.create_element(simple_type, "restriction", base="xs:hexBinary")
        restriction.set("fixed", "true")
        restriction.set("value", field_type['size'])        

    def create_map(self, schema_root: ET.Element, record_name: str, parent: ET.Element, map_schema: dict):
        """ handle Avro 'map' type"""
        complex_type = self.create_element(parent, "complexType")
        sequence = self.create_element(complex_type, "sequence")
        item = self.create_element(sequence, "element", name="item", minOccurs="0", maxOccurs="unbounded")
        inner_complex_type = self.create_element(item, "complexType")
        inner_sequence = self.create_element(inner_complex_type, "sequence")
        self.create_element(inner_sequence, "element", name="key", type="xs:string")
        map_values = map_schema['values']
        if isinstance(map_values, list):
            item_value = self.create_union(schema_root, record_name, inner_sequence, "value", map_values)
        else:
            item_value = self.create_element(inner_sequence, "element", name="value")
            self.set_field_type(schema_root, record_name, item_value, map_schema['values'])

    def create_union(self, schema_root: ET.Element, record_name: str, parent: ET.Element, field_name: str, field_type: list, insert_annotation: lambda e: None | None = None) -> ET.Element:
        """Create an XML element for union types."""

        def create_or_get_union_simple_type(self, schema_root: ET.Element, parent:Element, types: List[str], **attributes) -> str:
            """Create an XML simpleType element for union types."""

            type_key_list = types.copy()
            type_key_list.sort()
            type_key = ''.join(type_key_list)
            if type_key in self.union_types:
                return self.union_types[type_key]

            name = "And".join([t.capitalize() for t in types])
            simple_type = self.create_element(schema_root, "simpleType", **attributes)
            simple_type.set("name", name)
            union = self.create_element(simple_type, "union")
            union.set("memberTypes", ' '.join([self.convert_avro_primitive(t) for t in types if t != 'null']))
            self.union_types[type_key] = name
            return name

        if isinstance(field_type, list) and is_generic_avro_type(field_type):
            element = self.create_element(parent, "any", minOccurs="0", maxOccurs="unbounded")
            if insert_annotation: 
                insert_annotation(element)
            return element

        non_null_types = [t for t in field_type if t != 'null']
        if len(non_null_types) == 1:
            element = self.create_element(parent, "element", name=field_name)
            if insert_annotation: 
                insert_annotation(element)
            self.set_field_type(schema_root, record_name, element, non_null_types[0])
            return element
        else:
            element = self.create_element(parent, "element", name=field_name)
            if insert_annotation: 
                insert_annotation(element)
            primitives = [t for t in non_null_types if self.is_avro_primitive(t)]
            for primitive in primitives:
                non_null_types.remove(primitive)
            union_type_ref = ''
            if len(primitives) > 0:
                union_type_ref = create_or_get_union_simple_type(self, schema_root, parent, primitives)
                if len(non_null_types) == 0:
                    element.set('type', union_type_ref)
            if len(non_null_types) > 0:
                abstract_complex_type_name = record_name+field_name.capitalize()
                element.set('type', abstract_complex_type_name)
                if not abstract_complex_type_name in self.known_types:
                    self.known_types.append(abstract_complex_type_name)
                    self.create_element(schema_root, "complexType", name=abstract_complex_type_name, abstract="true")
                    if union_type_ref:
                        complex_content_option = self.create_element(schema_root, "complexType", name=abstract_complex_type_name+'1')
                        complex_content = self.create_element(complex_content_option, "complexContent")
                        complex_extension = self.create_element(complex_content, "extension", base=abstract_complex_type_name)
                        complex_sequence = self.create_element(complex_extension, "sequence")
                        complex_element = self.create_element(complex_sequence, "element", name='value', type=union_type_ref)
                    for i, union_type in enumerate(non_null_types):
                        complex_content_option = self.create_element(schema_root, "complexType", name=abstract_complex_type_name+str(i+2))
                        complex_content = self.create_element(complex_content_option, "complexContent")
                        complex_extension = self.create_element(complex_content, "extension", base=abstract_complex_type_name)
                        complex_sequence = self.create_element(complex_extension, "sequence")
                        complex_element = self.create_element(complex_sequence, "element", name=field_name)
                        self.set_field_type(schema_root, record_name, complex_element, union_type)
            return element  

    def create_array(self, schema_root: ET.Element, record_name: str, parent: ET.Element, array_schema: dict):
        """ handle Avro 'array' type """
        complex_type = self.create_element(parent, "complexType")
        sequence = self.create_element(complex_type, "sequence")
        item_type = array_schema['items']
        if isinstance(item_type, list):
            item = self.create_union(schema_root, record_name, sequence, "item", item_type)
            item.set('minOccurs', '0')
            item.set('maxOccurs', 'unbounded')
        else:
            item = self.create_element(sequence, "element", name="item", minOccurs="0", maxOccurs="unbounded")  
            self.set_field_type(schema_root, record_name, item, item_type)

    def create_enum(self, schema_root: ET.Element, enum_schema: dict) -> str:
        """Convert an Avro enum to an XML simpleType."""
        name = enum_schema['name']
        doc = enum_schema.get('doc', '')
        if name in self.known_types:
            return name
        simple_type = self.create_element(schema_root, "simpleType")
        if doc:
            annotation = self.create_element(simple_type, "annotation")
            documentation = self.create_element(annotation, "documentation")
            documentation.text = doc
        simple_type.set('name', name)
        restriction = self.create_element(simple_type, "restriction", base="xs:string")
        for enum_symbol in enum_schema['symbols']:
            self.create_element(restriction, "enumeration", value=enum_symbol)
        self.known_types.append(name)
        return name
    
    def set_field_type(self, schema_root: ET.Element, record_name: str, element: ET.Element, field_type: dict|str):
        """ set the type or create a subtype on the element for the given avro field type"""
        if isinstance(field_type, dict):
            if 'type' in field_type:
                if field_type['type'] == 'record':
                    if 'namespace' in field_type:
                        self.update_common_namespace(field_type['namespace'])
                    type = self.create_record(schema_root, field_type)
                    element.set('type', type)
                elif field_type['type'] == 'enum':
                    if 'namespace' in field_type:
                        self.update_common_namespace(field_type['namespace'])
                    type = self.create_enum(schema_root, field_type)
                    element.set('type', type)
                elif field_type['type'] == 'array':
                    self.create_array(schema_root, record_name, element, field_type)
                elif field_type['type'] == 'map':
                    self.create_map(schema_root, record_name, element, field_type)
                elif field_type['type'] == 'fixed':
                    self.create_fixed(schema_root, field_type)
                else:
                    return self.set_field_type(schema_root, record_name, element, field_type['type'])
            else:
                raise ValueError(f"Invalid field type")
        else:
            element.set('type', self.convert_avro_primitive(field_type))       

    def create_field(self, schema_root: Element, record_name: str, parent: Element, field: dict, attributes_parent: Element) -> ET.Element:
        """Convert an Avro field to an XML element."""
        field_name = field['name']
        field_type = field['type']
        field_doc = field.get('doc', '')
        xmlkind = field.get('xmlkind', 'element')
        if isinstance(field_type,list):
            def ia(e) -> None:
                if field_doc:
                    annotation = self.create_element(e, "annotation")
                    documentation = self.create_element(annotation, "documentation")
                    documentation.text = field_doc    
            element = self.create_union(schema_root, record_name, parent, field_name, field_type, ia)
        else:
            if xmlkind == 'attribute':
                element = self.create_element(attributes_parent, "attribute", name=field_name)
            else:
                element = self.create_element(parent, "element", name=field_name)
            if field_doc:
                annotation = self.create_element(element, "annotation")
                documentation = self.create_element(annotation, "documentation")
                documentation.text = field_doc
            self.set_field_type(schema_root, record_name, element, field_type)
        
        return element
    
    def create_record(self, schema_root: Element, record: dict) -> str:
        """Convert an Avro record to an XML complex type."""
        name = record['name']
        doc = record.get('doc', '')
        if name in self.known_types:
            return name
        complex_type = self.create_complex_type(schema_root, name=name)
        if doc:
            annotation = self.create_element(complex_type, "annotation")
            documentation = self.create_element(annotation, "documentation")
            documentation.text = doc
        sequence = self.create_element(complex_type, "sequence")
        attributes_parent = complex_type  # Attributes should be direct children of the complexType, not inside the sequence
        for field in record['fields']:
            self.create_field(schema_root, name, sequence, field, attributes_parent)
        self.known_types.append(name)
        return name

    def xsd_namespace_from_avro_namespace(self, namespace: str):
        """Convert an Avro namespace to an XML schema namespace."""
        if not self.target_namespace:
            return "urn:"+namespace.replace('.', ':')
        else:
            return self.target_namespace

    def avro_schema_to_xsd(self, avro_schema: dict) -> Element:
        """Convert the top-level Avro schema to an XML schema."""
        ET.register_namespace('xs', self.xmlns['xs'])
        schema = Element(f"{{{self.xmlns['xs']}}}schema")
        if isinstance(avro_schema, list):
            for record in avro_schema:
                if record['type'] == 'record':
                    if 'namespace' in record:
                        self.update_common_namespace(record['namespace'])
                    self.create_record(schema, record)
                elif record['type'] == 'enum':
                    if 'namespace' in record:
                        self.update_common_namespace(record['namespace'])
                    self.create_enum(schema, record)
        else:
            if avro_schema['type'] == 'record':
                if 'namespace' in avro_schema:
                    self.update_common_namespace(avro_schema['namespace'])
                self.create_record(schema, avro_schema)
            elif avro_schema['type'] == 'enum':
                if 'namespace' in avro_schema:
                    self.update_common_namespace(avro_schema['namespace'])
                self.create_enum(schema, avro_schema)
            elif avro_schema['type'] == 'fixed':
                if 'namespace' in avro_schema:
                    self.update_common_namespace(avro_schema['namespace'])
                self.create_fixed(schema, avro_schema)
        schema.set('targetNamespace', self.xsd_namespace_from_avro_namespace(self.common_namespace))
        schema.set('xmlns', self.xsd_namespace_from_avro_namespace(self.common_namespace))
        ET.register_namespace('', self.xsd_namespace_from_avro_namespace(self.common_namespace))
        return schema

    def save_xsd_to_file(self, schema: Element, xml_path: str) -> None:
        """Save the XML schema to a file."""
        tree_str = tostring(schema, 'utf-8')
        pretty_tree = minidom.parseString(tree_str).toprettyxml(indent="  ")
        with open(xml_path, 'w', encoding='utf-8') as xml_file:
            xml_file.write(pretty_tree)

    def convert_avro_to_xsd(self, avro_schema_path: str, xml_file_path: str) -> None:
        """Convert Avro schema file to XML schema file."""
        with open(avro_schema_path, 'r', encoding='utf-8') as avro_file:
            avro_schema = json.load(avro_file)
            
        xml_schema = self.avro_schema_to_xsd(avro_schema)
        self.save_xsd_to_file(xml_schema, xml_file_path)

def convert_avro_to_xsd(avro_schema_path: str, xml_file_path: str, target_namespace: str = '') -> None:
    avrotoxml = AvroToXSD(target_namespace)
    avrotoxml.convert_avro_to_xsd(avro_schema_path, xml_file_path)
