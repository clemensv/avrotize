# pylint: disable=line-too-long

""" StructureToXSD class for converting JSON Structure schema to XSD """

from functools import reduce
import json
from typing import Dict, List, Optional, Any
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom


class StructureToXSD:
    """Converts JSON Structure schema to XML Schema Definition (XSD)"""
    
    def __init__(self, target_namespace: str = ''):
        self.xmlns = {"xs": "http://www.w3.org/2001/XMLSchema"}
        self.union_types: Dict[str, str] = {}
        self.known_types: List[str] = []
        self.common_namespace = ''
        self.target_namespace = target_namespace
        self.schema_registry: Dict[str, Dict] = {}
        self.definitions: Dict[str, Any] = {}

    def find_common_namespace(self, namespaces: List[str]) -> str:
        """Find the common namespace prefix from a list of namespaces."""
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
        """Update the common namespace based on the provided namespace."""
        if not self.common_namespace:
            self.common_namespace = namespace
        else:
            self.common_namespace = self.find_common_namespace([self.common_namespace, namespace])

    def register_schema_ids(self, schema: Dict, base_uri: str = '') -> None:
        """Recursively registers schemas with $id keywords"""
        if not isinstance(schema, dict):
            return
        
        if '$id' in schema:
            schema_id = schema['$id']
            if base_uri and not schema_id.startswith(('http://', 'https://', 'urn:')):
                from urllib.parse import urljoin
                schema_id = urljoin(base_uri, schema_id)
            self.schema_registry[schema_id] = schema
            base_uri = schema_id
        
        if 'definitions' in schema:
            for def_name, def_schema in schema['definitions'].items():
                if isinstance(def_schema, dict):
                    self.register_schema_ids(def_schema, base_uri)
        
        if 'properties' in schema:
            for prop_name, prop_schema in schema['properties'].items():
                if isinstance(prop_schema, dict):
                    self.register_schema_ids(prop_schema, base_uri)
        
        for key in ['items', 'values', 'additionalProperties']:
            if key in schema and isinstance(schema[key], dict):
                self.register_schema_ids(schema[key], base_uri)

    def resolve_ref(self, ref: str, context_schema: Optional[Dict] = None) -> Optional[Dict]:
        """Resolves a $ref to the actual schema definition"""
        if not ref.startswith('#/'):
            if ref in self.schema_registry:
                return self.schema_registry[ref]
            return None
        
        path = ref[2:].split('/')
        schema = context_schema if context_schema else self.definitions
        for part in path:
            if not isinstance(schema, dict) or part not in schema:
                return None
            schema = schema[part]
        return schema

    def convert_structure_primitive(self, structure_type: str | dict) -> str:
        """Map JSON Structure primitive types to XSD data types."""
        
        if isinstance(structure_type, dict):
            # Handle objects with type property
            if 'type' in structure_type:
                base_type = structure_type['type']
                
                # Handle decimal with precision/scale
                if base_type == 'decimal':
                    return 'xs:decimal'
                
                # Recurse with the base type
                return self.convert_structure_primitive(base_type)
            return 'xs:string'
        
        elif isinstance(structure_type, str):
            mapping = {
                'null': 'string',
                'boolean': 'boolean',
                'string': 'string',
                'integer': 'integer',
                'number': 'decimal',
                'int8': 'byte',
                'uint8': 'unsignedByte',
                'int16': 'short',
                'uint16': 'unsignedShort',
                'int32': 'int',
                'uint32': 'unsignedInt',
                'int64': 'long',
                'uint64': 'unsignedLong',
                'int128': 'integer',
                'uint128': 'integer',
                'float8': 'float',
                'float': 'float',
                'double': 'double',
                'binary32': 'float',
                'binary64': 'double',
                'decimal': 'decimal',
                'binary': 'hexBinary',
                'date': 'date',
                'time': 'time',
                'datetime': 'dateTime',
                'timestamp': 'dateTime',
                'duration': 'duration',
                'uuid': 'string',
                'uri': 'anyURI',
                'jsonpointer': 'string',
                'any': 'anyType'
            }
            
            xsd_type = mapping.get(structure_type, '')
            if xsd_type:
                return f"xs:{xsd_type}"
            else:
                # It's a reference to a complex type
                return structure_type.split('.')[-1]
        
        return 'xs:string'

    def is_structure_primitive(self, structure_type: str | dict) -> bool:
        """Check if the Structure type is a primitive type."""
        if isinstance(structure_type, dict):
            if 'type' in structure_type:
                return self.is_structure_primitive(structure_type['type'])
            return False
        elif isinstance(structure_type, str):
            return structure_type in {
                'null', 'boolean', 'string', 'integer', 'number',
                'int8', 'uint8', 'int16', 'uint16', 'int32', 'uint32',
                'int64', 'uint64', 'int128', 'uint128',
                'float8', 'float', 'double', 'binary32', 'binary64',
                'decimal', 'binary', 'date', 'time', 'datetime', 'timestamp',
                'duration', 'uuid', 'uri', 'jsonpointer', 'any'
            }
        return False

    def create_element(self, parent: Element, tag: str, **attributes) -> Element:
        """Create an XML element with the proper namespace."""
        return SubElement(parent, f"{{{self.xmlns['xs']}}}{tag}", **attributes)

    def create_complex_type(self, parent: Element, **attributes) -> Element:
        """Create an XML complexType element."""
        return self.create_element(parent, "complexType", **attributes)

    def create_simple_type(self, parent: Element, **attributes) -> Element:
        """Create an XML simpleType element."""
        return self.create_element(parent, "simpleType", **attributes)

    def create_map(self, schema_root: ET.Element, record_name: str, parent: ET.Element, map_schema: dict):
        """Handle Structure 'map' type"""
        complex_type = self.create_element(parent, "complexType")
        sequence = self.create_element(complex_type, "sequence")
        item = self.create_element(sequence, "element", name="item", minOccurs="0", maxOccurs="unbounded")
        inner_complex_type = self.create_element(item, "complexType")
        inner_sequence = self.create_element(inner_complex_type, "sequence")
        self.create_element(inner_sequence, "element", name="key", type="xs:string")
        
        map_values = map_schema.get('values', {'type': 'any'})
        if isinstance(map_values, list):
            item_value = self.create_union(schema_root, record_name, inner_sequence, "value", map_values)
        else:
            item_value = self.create_element(inner_sequence, "element", name="value")
            self.set_field_type(schema_root, record_name, item_value, map_values)

    def create_union(self, schema_root: ET.Element, record_name: str, parent: ET.Element, 
                     field_name: str, field_type: list, insert_annotation=None) -> ET.Element:
        """Create an XML element for union types (type arrays)."""
        
        def create_or_get_union_simple_type(types: List[str], **attributes) -> str:
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
            union.set("memberTypes", ' '.join([self.convert_structure_primitive(t) for t in types if t != 'null']))
            self.union_types[type_key] = name
            return name

        # Handle union with null (nullable types)
        non_null_types = [t for t in field_type if t != 'null']
        is_nullable = 'null' in field_type
        
        if len(non_null_types) == 0:
            # Only null - treat as optional string
            element = self.create_element(parent, "element", name=field_name, type="xs:string", minOccurs="0")
            if insert_annotation:
                insert_annotation(element)
            return element
        
        if len(non_null_types) == 1:
            # Single non-null type (possibly nullable)
            element = self.create_element(parent, "element", name=field_name)
            if is_nullable:
                element.set("minOccurs", "0")
            if insert_annotation:
                insert_annotation(element)
            self.set_field_type(schema_root, record_name, element, non_null_types[0])
            return element
        
        # Multiple non-null types - create choice group
        element = self.create_element(parent, "element", name=field_name)
        if is_nullable:
            element.set("minOccurs", "0")
        if insert_annotation:
            insert_annotation(element)
        
        # Separate primitives from complex types
        primitives = [t for t in non_null_types if self.is_structure_primitive(t)]
        complex_types = [t for t in non_null_types if not self.is_structure_primitive(t)]
        
        if len(complex_types) == 0 and len(primitives) > 0:
            # All primitives - create union type
            union_type_ref = create_or_get_union_simple_type(primitives)
            element.set('type', union_type_ref)
        elif len(complex_types) > 0:
            # Has complex types - create abstract base with derived types
            abstract_complex_type_name = record_name + field_name.capitalize()
            element.set('type', abstract_complex_type_name)
            
            if not abstract_complex_type_name in self.known_types:
                self.known_types.append(abstract_complex_type_name)
                self.create_element(schema_root, "complexType", name=abstract_complex_type_name, abstract="true")
                
                # If there are primitives, create a derived type for them
                if primitives:
                    union_type_ref = create_or_get_union_simple_type(primitives)
                    complex_content_option = self.create_element(schema_root, "complexType", 
                                                                 name=abstract_complex_type_name + '1')
                    complex_content = self.create_element(complex_content_option, "complexContent")
                    complex_extension = self.create_element(complex_content, "extension", 
                                                          base=abstract_complex_type_name)
                    complex_sequence = self.create_element(complex_extension, "sequence")
                    self.create_element(complex_sequence, "element", name='value', type=union_type_ref)
                
                # Create derived types for each complex type
                for i, union_type in enumerate(complex_types):
                    start_index = 2 if primitives else 1
                    complex_content_option = self.create_element(schema_root, "complexType", 
                                                                name=abstract_complex_type_name + str(i + start_index))
                    complex_content = self.create_element(complex_content_option, "complexContent")
                    complex_extension = self.create_element(complex_content, "extension", 
                                                          base=abstract_complex_type_name)
                    complex_sequence = self.create_element(complex_extension, "sequence")
                    complex_element = self.create_element(complex_sequence, "element", name=field_name)
                    self.set_field_type(schema_root, record_name, complex_element, union_type)
        
        return element

    def create_array(self, schema_root: ET.Element, record_name: str, parent: ET.Element, array_schema: dict):
        """Handle Structure 'array' type"""
        complex_type = self.create_element(parent, "complexType")
        sequence = self.create_element(complex_type, "sequence")
        item_type = array_schema.get('items', {'type': 'any'})
        
        if isinstance(item_type, list):
            item = self.create_union(schema_root, record_name, sequence, "item", item_type)
            item.set('minOccurs', '0')
            item.set('maxOccurs', 'unbounded')
        else:
            item = self.create_element(sequence, "element", name="item", minOccurs="0", maxOccurs="unbounded")
            self.set_field_type(schema_root, record_name, item, item_type)

    def create_set(self, schema_root: ET.Element, record_name: str, parent: ET.Element, set_schema: dict):
        """Handle Structure 'set' type - similar to array but semantically a set"""
        # XSD doesn't have a native set type, so we use array/sequence
        self.create_array(schema_root, record_name, parent, {'items': set_schema.get('items', {'type': 'any'})})

    def create_tuple(self, schema_root: ET.Element, record_name: str, parent: ET.Element, tuple_schema: dict):
        """Handle Structure 'tuple' type"""
        complex_type = self.create_element(parent, "complexType")
        sequence = self.create_element(complex_type, "sequence")
        
        properties = tuple_schema.get('properties', {})
        tuple_order = tuple_schema.get('tuple', [])
        
        # Generate elements in tuple order
        for prop_name in tuple_order:
            if prop_name in properties:
                prop_schema = properties[prop_name]
                element = self.create_element(sequence, "element", name=prop_name)
                self.set_field_type(schema_root, record_name, element, prop_schema)

    def create_enum(self, schema_root: ET.Element, enum_schema: dict) -> str:
        """Convert a Structure enum to an XML simpleType."""
        name = enum_schema.get('name', 'UnnamedEnum')
        doc = enum_schema.get('description', enum_schema.get('doc', ''))
        
        if name in self.known_types:
            return name
        
        simple_type = self.create_element(schema_root, "simpleType")
        if doc:
            annotation = self.create_element(simple_type, "annotation")
            documentation = self.create_element(annotation, "documentation")
            documentation.text = doc
        
        simple_type.set('name', name)
        restriction = self.create_element(simple_type, "restriction", base="xs:string")
        
        for enum_symbol in enum_schema.get('enum', []):
            self.create_element(restriction, "enumeration", value=str(enum_symbol))
        
        self.known_types.append(name)
        return name

    def create_choice(self, schema_root: ET.Element, choice_schema: dict) -> str:
        """Convert a Structure choice to an XML type hierarchy."""
        name = choice_schema.get('name', 'UnnamedChoice')
        doc = choice_schema.get('description', choice_schema.get('doc', ''))
        
        if name in self.known_types:
            return name
        
        # Create abstract base type
        complex_type = self.create_element(schema_root, "complexType", name=name, abstract="true")
        if doc:
            annotation = self.create_element(complex_type, "annotation")
            documentation = self.create_element(annotation, "documentation")
            documentation.text = doc
        
        self.known_types.append(name)
        
        # Create derived types for each choice
        choices = choice_schema.get('choices', {})
        for choice_name, choice_def in choices.items():
            derived_name = name + '_' + choice_name
            derived_type = self.create_element(schema_root, "complexType", name=derived_name)
            complex_content = self.create_element(derived_type, "complexContent")
            extension = self.create_element(complex_content, "extension", base=name)
            
            # Add choice-specific properties
            if isinstance(choice_def, dict):
                if '$ref' in choice_def:
                    ref_schema = self.resolve_ref(choice_def['$ref'])
                    if ref_schema and 'properties' in ref_schema:
                        sequence = self.create_element(extension, "sequence")
                        for prop_name, prop_schema in ref_schema['properties'].items():
                            self.create_field(schema_root, derived_name, sequence, prop_name, prop_schema, extension)
                elif 'properties' in choice_def:
                    sequence = self.create_element(extension, "sequence")
                    for prop_name, prop_schema in choice_def['properties'].items():
                        self.create_field(schema_root, derived_name, sequence, prop_name, prop_schema, extension)
        
        return name

    def set_field_type(self, schema_root: ET.Element, record_name: str, element: ET.Element, 
                       field_type: dict | str | list):
        """Set the type or create a subtype on the element for the given Structure field type"""
        if isinstance(field_type, list):
            # Type array (union)
            self.create_union(schema_root, record_name, element.getparent(), element.get('name'), field_type)
            element.getparent().remove(element)
            return
        
        if isinstance(field_type, dict):
            if '$ref' in field_type:
                # Resolve reference
                ref_schema = self.resolve_ref(field_type['$ref'])
                if ref_schema:
                    ref_name = field_type['$ref'].split('/')[-1]
                    if 'type' in ref_schema:
                        return self.set_field_type(schema_root, record_name, element, ref_schema)
                    else:
                        element.set('type', ref_name)
                        return
                element.set('type', 'xs:string')
                return
            
            if 'enum' in field_type:
                # Inline enum
                enum_name = record_name + element.get('name', 'Field').capitalize() + 'Enum'
                field_type['name'] = enum_name
                self.create_enum(schema_root, field_type)
                element.set('type', enum_name)
                return
            
            if 'type' in field_type:
                type_val = field_type['type']
                
                if type_val == 'object':
                    if 'namespace' in field_type:
                        self.update_common_namespace(field_type['namespace'])
                    type_name = self.create_record(schema_root, field_type)
                    element.set('type', type_name)
                elif type_val == 'array':
                    self.create_array(schema_root, record_name, element, field_type)
                elif type_val == 'set':
                    self.create_set(schema_root, record_name, element, field_type)
                elif type_val == 'map':
                    self.create_map(schema_root, record_name, element, field_type)
                elif type_val == 'tuple':
                    self.create_tuple(schema_root, record_name, element, field_type)
                elif type_val == 'choice':
                    choice_name = self.create_choice(schema_root, field_type)
                    element.set('type', choice_name)
                else:
                    # Primitive type or reference
                    element.set('type', self.convert_structure_primitive(type_val))
            else:
                # No type specified
                element.set('type', 'xs:anyType')
        else:
            # String type reference
            if self.is_structure_primitive(field_type):
                element.set('type', self.convert_structure_primitive(field_type))
            else:
                element.set('type', field_type)

    def create_field(self, schema_root: Element, record_name: str, parent: Element, 
                     field_name: str, field_schema: dict, attributes_parent: Element) -> ET.Element:
        """Convert a Structure field to an XML element."""
        field_type = field_schema.get('type') if isinstance(field_schema, dict) else field_schema
        field_doc = field_schema.get('description', field_schema.get('doc', '')) if isinstance(field_schema, dict) else ''
        
        # Determine if field is required
        is_required = True  # Default to required unless parent specifies otherwise
        
        # Check if this is an XML attribute
        xmlkind = field_schema.get('xmlkind', 'element') if isinstance(field_schema, dict) else 'element'
        
        if isinstance(field_type, list):
            # Union type
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
            
            self.set_field_type(schema_root, record_name, element, field_type if field_type else field_schema)
        
        return element

    def create_record(self, schema_root: Element, record: dict) -> str:
        """Convert a Structure object to an XML complex type."""
        name = record.get('name', 'UnnamedRecord')
        doc = record.get('description', record.get('doc', ''))
        
        if name in self.known_types:
            return name
        
        complex_type = self.create_complex_type(schema_root, name=name)
        if doc:
            annotation = self.create_element(complex_type, "annotation")
            documentation = self.create_element(annotation, "documentation")
            documentation.text = doc
        
        sequence = self.create_element(complex_type, "sequence")
        attributes_parent = complex_type
        
        properties = record.get('properties', {})
        required_props = record.get('required', [])
        
        for prop_name, prop_schema in properties.items():
            # Determine if field is required
            is_required = prop_name in required_props if isinstance(required_props, list) else False
            
            field_element = self.create_field(schema_root, name, sequence, prop_name, prop_schema, attributes_parent)
            
            # Set minOccurs if not required
            if not is_required and field_element.get('name') == prop_name:
                if field_element.tag.endswith('element'):
                    field_element.set('minOccurs', '0')
                elif field_element.tag.endswith('attribute'):
                    field_element.set('use', 'optional')
        
        self.known_types.append(name)
        return name

    def xsd_namespace_from_structure_namespace(self, namespace: str):
        """Convert a Structure namespace to an XML schema namespace."""
        if not self.target_namespace:
            return "urn:" + namespace.replace('.', ':')
        else:
            return self.target_namespace

    def structure_schema_to_xsd(self, structure_schema: dict | list) -> Element:
        """Convert the top-level Structure schema to an XML schema."""
        ET.register_namespace('xs', self.xmlns['xs'])
        schema = Element(f"{{{self.xmlns['xs']}}}schema")
        
        # Register schema IDs first
        if isinstance(structure_schema, list):
            for schema_item in structure_schema:
                if isinstance(schema_item, dict):
                    self.register_schema_ids(schema_item)
        else:
            self.register_schema_ids(structure_schema)
        
        # Process schemas
        if isinstance(structure_schema, list):
            for record in structure_schema:
                if isinstance(record, dict):
                    self.process_schema_item(schema, record)
        else:
            self.process_schema_item(schema, structure_schema)
        
        # Set target namespace
        schema.set('targetNamespace', self.xsd_namespace_from_structure_namespace(self.common_namespace))
        schema.set('xmlns', self.xsd_namespace_from_structure_namespace(self.common_namespace))
        ET.register_namespace('', self.xsd_namespace_from_structure_namespace(self.common_namespace))
        
        return schema

    def process_schema_item(self, schema_root: Element, schema_item: dict):
        """Process a single schema item (could be object, enum, choice, etc.)"""
        if 'namespace' in schema_item:
            self.update_common_namespace(schema_item['namespace'])
        
        if 'definitions' in schema_item:
            self.definitions = schema_item['definitions']
            self.process_definitions(schema_root, self.definitions, '')
        
        # Process root type
        if 'type' in schema_item:
            type_val = schema_item['type']
            if type_val == 'object':
                self.create_record(schema_root, schema_item)
            elif type_val == 'choice':
                self.create_choice(schema_root, schema_item)
        elif 'enum' in schema_item:
            self.create_enum(schema_root, schema_item)
        elif '$root' in schema_item:
            root_ref = schema_item['$root']
            root_schema = self.resolve_ref(root_ref, schema_item)
            if root_schema:
                self.process_schema_item(schema_root, root_schema)

    def process_definitions(self, schema_root: Element, definitions: Dict, namespace_path: str):
        """Processes the definitions section recursively"""
        for name, definition in definitions.items():
            if isinstance(definition, dict):
                if 'type' in definition or 'enum' in definition:
                    # This is a type definition
                    definition['name'] = name
                    if 'namespace' in definition:
                        self.update_common_namespace(definition['namespace'])
                    self.process_schema_item(schema_root, definition)
                else:
                    # This is a namespace
                    new_namespace = f"{namespace_path}.{name}" if namespace_path else name
                    self.process_definitions(schema_root, definition, new_namespace)

    def save_xsd_to_file(self, schema: Element, xml_path: str) -> None:
        """Save the XML schema to a file."""
        tree_str = tostring(schema, 'utf-8')
        pretty_tree = minidom.parseString(tree_str).toprettyxml(indent="  ")
        with open(xml_path, 'w', encoding='utf-8') as xml_file:
            xml_file.write(pretty_tree)

    def convert_structure_to_xsd(self, structure_schema_path: str, xml_file_path: str) -> None:
        """Convert Structure schema file to XML schema file."""
        with open(structure_schema_path, 'r', encoding='utf-8') as structure_file:
            structure_schema = json.load(structure_file)
        
        xml_schema = self.structure_schema_to_xsd(structure_schema)
        self.save_xsd_to_file(xml_schema, xml_file_path)


def convert_structure_to_xsd(structure_schema_path: str, xml_file_path: str, target_namespace: str = '') -> None:
    """Convert Structure schema to XSD.
    
    Args:
        structure_schema_path: Path to JSON Structure schema file
        xml_file_path: Path to output XSD file
        target_namespace: Target XML namespace (optional)
    """
    converter = StructureToXSD(target_namespace)
    converter.convert_structure_to_xsd(structure_schema_path, xml_file_path)
