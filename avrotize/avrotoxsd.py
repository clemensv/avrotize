import json
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom

class AvroToXSD:
    def __init__(self):
        self.xmlns = {"xs": "http://www.w3.org/2001/XMLSchema"}

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
            
            type = mapping.get(avro_type, 'string')  # Fallback to string
            return f"xs:{type}"
        return f"xs:string"

    def create_element(self, parent: Element, tag: str, **attributes) -> Element:
        """Create an XML element with the proper namespace."""
        return SubElement(parent, f"{{{self.xmlns['xs']}}}{tag}", **attributes)
    
    def create_complex_type(self, parent: Element, **attributes) -> Element:
        """Create an XML complexType element."""
        return self.create_element(parent, "complexType", **attributes)

    def convert_field(self, parent: Element, field: dict) -> ET.Element:
        """Convert an Avro field to an XML element."""
        field_name = field['name']
        field_type = field['type']
        element = self.create_element(parent, "element", name=field_name)

        if isinstance(field_type, dict):
            # Handling complex types (record, enum, array)
            if field_type['type'] == 'record':
                complex_type = self.create_complex_type(element)
                sequence = self.create_element(complex_type, "sequence")
                for subfield in field_type['fields']:
                    self.convert_field(sequence, subfield)
            elif field_type['type'] == 'enum':
                simple_type = self.create_element(element, "simpleType")
                restriction = self.create_element(simple_type, "restriction", base="xs:string")
                for enum_symbol in field_type['symbols']:
                    self.create_element(restriction, "enumeration", value=enum_symbol)
            elif field_type['type'] == 'array':
                complex_type = self.create_element(element, "complexType")
                sequence = self.create_element(complex_type, "sequence")
                item_type = field_type['items']
                if isinstance(item_type, str): 
                    item_element = self.create_element(sequence, "element", name="item", type=self.convert_avro_primitive(item_type), minOccurs="0", maxOccurs="unbounded")
                else:
                    item_element = self.create_element(sequence, "element", name="item", minOccurs="0", maxOccurs="unbounded")
                    self.convert_field(item_element, item_type)
        elif isinstance(field_type, list):
            # Handling union types, simplistically picking the first non-null type
            non_null_types = [t for t in field_type if t != 'null']
            if len(non_null_types) == 1:
                chosen_type = non_null_types[0] if non_null_types else 'string'
                if isinstance(chosen_type, str) or (isinstance(chosen_type, dict) and 'logicalType' in chosen_type):
                    element.set('type', self.convert_avro_primitive(chosen_type))
                else:
                    self.convert_field(element, {"name": field_name, "type": chosen_type})
            else:
                complex_union_type = self.create_complex_type(element)
                choice_union = self.create_element(complex_union_type, "choice", minOccurs="0", maxOccurs="unbounded")
                for union_type in field_type:
                    if union_type != 'null':
                        self.convert_field(choice_union, {"name": field_name, "type": union_type})
        else:
            # Handling primitive types
            xsd_type = self.convert_avro_primitive(field_type)
            element.set('type', xsd_type)
        return element

    def convert_record(self, parent: Element, record: dict) -> None:
        """Convert an Avro record to an XML complex type."""
        name = record['name']
        complex_type = self.create_complex_type(parent, name=name)
        sequence = self.create_element(complex_type, "sequence")
        for field in record['fields']:
            self.convert_field(sequence, field)

    def avro_schema_to_xsd(self, avro_schema: dict) -> Element:
        """Convert the top-level Avro schema to an XML schema."""
        ET.register_namespace('xs', self.xmlns['xs'])
        schema = Element(f"{{{self.xmlns['xs']}}}schema")
        if isinstance(avro_schema, list):
            for record in avro_schema:
                if record['type'] == 'record':
                    self.convert_record(schema, record)
        else:
            if avro_schema['type'] == 'record':
                self.convert_record(schema, avro_schema)
        return schema

    def save_xsd_to_file(self, schema: Element, xml_path: str) -> None:
        """Save the XML schema to a file."""
        tree_str = tostring(schema, 'utf-8')
        pretty_tree = minidom.parseString(tree_str).toprettyxml(indent="  ")
        with open(xml_path, 'w') as xml_file:
            xml_file.write(pretty_tree)

    def convert_avro_to_xsd(self, avro_schema_path: str, xml_file_path: str) -> None:
        """Convert Avro schema file to XML schema file."""
        with open(avro_schema_path, 'r') as avro_file:
            avro_schema = json.load(avro_file)
        xml_schema = self.avro_schema_to_xsd(avro_schema)
        self.save_xsd_to_file(xml_schema, xml_file_path)

def convert_avro_to_xsd(avro_schema_path: str, xml_file_path: str) -> None:
    avrotoxml = AvroToXSD()
    avrotoxml.convert_avro_to_xsd(avro_schema_path, xml_file_path)
