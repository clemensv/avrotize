# pylint: disable=line-too-long

""" StructureToXSD class for converting JSON Structure schema to XML Schema (XSD) """

import json
import os
from typing import Any, Dict, List, Optional, Union
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom
from functools import reduce

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | None


class StructureToXSD:
    """ Converts JSON Structure schema to XSD """

    def __init__(self, target_namespace: str = ''):
        self.xmlns = {"xs": "http://www.w3.org/2001/XMLSchema"}
        self.union_types: Dict[str, str] = {}
        self.known_types: List[str] = []
        self.common_namespace = ''
        self.target_namespace = target_namespace
        self.schema_doc: JsonNode = None
        self.definitions: Dict[str, Any] = {}
        self.schema_registry: Dict[str, Dict] = {}
        self.offers: Dict[str, Any] = {}
        self.type_dict: Dict[str, Dict] = {}

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

    def map_primitive_to_xsd(self, structure_type: str | dict) -> str:
        """Maps JSON Structure primitive types to XSD types."""
        
        # Handle type as dict (for annotations)
        if isinstance(structure_type, dict):
            type_name = structure_type.get('type', 'string')
            return self.map_primitive_to_xsd(type_name)
        
        mapping = {
            'null': 'string',  # Nullable types handled separately
            'boolean': 'boolean',
            'string': 'string',
            'integer': 'integer',
            'number': 'double',
            'int8': 'byte',
            'uint8': 'unsignedByte',
            'int16': 'short',
            'uint16': 'unsignedShort',
            'int32': 'int',
            'uint32': 'unsignedInt',
            'int64': 'long',
            'uint64': 'unsignedLong',
            'int128': 'integer',  # XSD doesn't have 128-bit, use arbitrary precision
            'uint128': 'integer',
            'float8': 'float',
            'float': 'float',
            'float32': 'float',  # IEEE 754 single precision
            'float64': 'double',  # IEEE 754 double precision
            'double': 'double',
            'binary32': 'float',
            'binary64': 'double',
            'decimal': 'decimal',
            'binary': 'base64Binary',
            'bytes': 'base64Binary',
            'date': 'date',
            'time': 'time',
            'datetime': 'dateTime',
            'timestamp': 'dateTime',
            'duration': 'duration',
            'uuid': 'string',  # UUID pattern can be added
            'uri': 'anyURI',
            'jsonpointer': 'string',
            'any': 'anyType'
        }
        
        xsd_type = mapping.get(structure_type, 'string')
        return f"xs:{xsd_type}"

    def is_primitive_type(self, structure_type: str | dict) -> bool:
        """Check if the type is a primitive type."""
        if isinstance(structure_type, dict):
            type_name = structure_type.get('type', '')
            return self.is_primitive_type(type_name)
        
        primitives = {
            'null', 'boolean', 'string', 'integer', 'number',
            'int8', 'uint8', 'int16', 'uint16', 'int32', 'uint32',
            'int64', 'uint64', 'int128', 'uint128',
            'float8', 'float', 'float32', 'float64', 'double', 'binary32', 'binary64',
            'decimal', 'binary', 'bytes', 'date', 'time', 'datetime',
            'timestamp', 'duration', 'uuid', 'uri', 'jsonpointer'
        }
        return structure_type in primitives

    def create_element(self, parent: Element, tag: str, **attributes) -> Element:
        """Create an XML element with the proper namespace."""
        return SubElement(parent, f"{{{self.xmlns['xs']}}}{tag}", **attributes)

    def create_complex_type(self, parent: Element, **attributes) -> Element:
        """Create an XML complexType element."""
        return self.create_element(parent, "complexType", **attributes)

    def create_simple_type_with_restriction(self, parent: Element, name: str, base_type: str, facets: Dict[str, Any]) -> Element:
        """Create a simple type with restrictions (facets)."""
        simple_type = self.create_element(parent, "simpleType", name=name)
        restriction = self.create_element(simple_type, "restriction", base=base_type)
        
        # Add facets
        if 'minLength' in facets:
            self.create_element(restriction, "minLength", value=str(facets['minLength']))
        if 'maxLength' in facets:
            self.create_element(restriction, "maxLength", value=str(facets['maxLength']))
        if 'pattern' in facets:
            self.create_element(restriction, "pattern", value=facets['pattern'])
        if 'minimum' in facets:
            self.create_element(restriction, "minInclusive", value=str(facets['minimum']))
        if 'maximum' in facets:
            self.create_element(restriction, "maxInclusive", value=str(facets['maximum']))
        if 'exclusiveMinimum' in facets:
            self.create_element(restriction, "minExclusive", value=str(facets['exclusiveMinimum']))
        if 'exclusiveMaximum' in facets:
            self.create_element(restriction, "maxExclusive", value=str(facets['exclusiveMaximum']))
        
        return simple_type

    def resolve_type_reference(self, ref: str) -> Optional[Dict]:
        """Resolve a $ref to its definition."""
        # Handle local references (#/definitions/TypeName)
        if ref.startswith('#/'):
            parts = ref.split('/')
            if len(parts) >= 3 and parts[1] == 'definitions':
                type_name = parts[2]
                return self.definitions.get(type_name)
        
        # Handle URI references
        if ref.startswith('http://') or ref.startswith('https://'):
            return self.schema_registry.get(ref)
        
        return None

    def get_type_name_from_ref(self, ref: str) -> str:
        """Extract type name from a $ref."""
        if ref.startswith('#/definitions/'):
            return ref.split('/')[-1]
        return ref.split('/')[-1]

    def convert_array_type(self, schema_root: Element, type_name: str, parent: Element, array_def: Dict):
        """Handle array type conversion."""
        complex_type = self.create_element(parent, "complexType")
        sequence = self.create_element(complex_type, "sequence")
        
        item_type = array_def.get('items', {'type': 'any'})
        min_items = array_def.get('minItems', 0)
        max_items = array_def.get('maxItems')
        
        item_element = self.create_element(
            sequence, "element", name="item",
            minOccurs=str(min_items),
            maxOccurs=str(max_items) if max_items is not None else "unbounded"
        )
        
        self.set_element_type(schema_root, type_name, item_element, item_type)

    def convert_set_type(self, schema_root: Element, type_name: str, parent: Element, set_def: Dict):
        """Handle set type conversion (like array but with unique items)."""
        # XSD doesn't have native set, use array with unique constraint
        complex_type = self.create_element(parent, "complexType")
        sequence = self.create_element(complex_type, "sequence")
        
        item_type = set_def.get('items', {'type': 'any'})
        min_items = set_def.get('minItems', 0)
        max_items = set_def.get('maxItems')
        
        item_element = self.create_element(
            sequence, "element", name="item",
            minOccurs=str(min_items),
            maxOccurs=str(max_items) if max_items is not None else "unbounded"
        )
        
        self.set_element_type(schema_root, type_name, item_element, item_type)
        
        # Add unique constraint (though this is more semantic than enforced)
        # Could add xs:unique if there's a key to reference

    def convert_map_type(self, schema_root: Element, type_name: str, parent: Element, map_def: Dict):
        """Handle map type conversion."""
        complex_type = self.create_element(parent, "complexType")
        sequence = self.create_element(complex_type, "sequence")
        
        value_type = map_def.get('values', {'type': 'any'})
        
        entry_element = self.create_element(
            sequence, "element", name="entry",
            minOccurs="0", maxOccurs="unbounded"
        )
        
        entry_complex_type = self.create_element(entry_element, "complexType")
        entry_sequence = self.create_element(entry_complex_type, "sequence")
        
        # Map key is always string
        self.create_element(entry_sequence, "element", name="key", type="xs:string")
        
        # Map value
        value_element = self.create_element(entry_sequence, "element", name="value")
        self.set_element_type(schema_root, type_name, value_element, value_type)

    def convert_tuple_type(self, schema_root: Element, type_name: str, parent: Element, tuple_def: Dict):
        """Handle tuple type conversion."""
        complex_type = self.create_element(parent, "complexType")
        sequence = self.create_element(complex_type, "sequence")
        
        items = tuple_def.get('items', [])
        for i, item_type in enumerate(items):
            item_element = self.create_element(
                sequence, "element", name=f"item{i}",
                minOccurs="1", maxOccurs="1"
            )
            self.set_element_type(schema_root, type_name, item_element, item_type)

    def convert_choice_type(self, schema_root: Element, type_name: str, parent: Element, choice_def: Dict):
        """Handle choice (union) type conversion."""
        choices = choice_def.get('choices', [])
        discriminator = choice_def.get('discriminator') or choice_def.get('selector')
        
        # Handle choices as dict (tagged) or list (inline)
        if isinstance(choices, dict):
            choices_list = [(name, typedef) for name, typedef in choices.items()]
        else:
            choices_list = [(f"option{i+1}", choice) for i, choice in enumerate(choices)]
        
        if discriminator:
            # Tagged union - use xs:choice with different elements
            complex_type = self.create_element(parent, "complexType")
            choice_element = self.create_element(complex_type, "choice")
            
            for choice_name, choice_type in choices_list:
                option_element = self.create_element(choice_element, "element", name=choice_name)
                self.set_element_type(schema_root, type_name, option_element, choice_type)
        else:
            # Inline union - create abstract base type with extensions
            abstract_type_name = type_name if type_name else "Choice"
            if not abstract_type_name in self.known_types:
                self.create_element(schema_root, "complexType", name=abstract_type_name, abstract="true")
                self.known_types.append(abstract_type_name)
            
            # Create concrete types for each choice
            for i, (choice_name, choice_type) in enumerate(choices_list):
                option_type_name = f"{abstract_type_name}Option{i+1}"
                if option_type_name not in self.known_types:
                    option_complex_type = self.create_element(schema_root, "complexType", name=option_type_name)
                    complex_content = self.create_element(option_complex_type, "complexContent")
                    extension = self.create_element(complex_content, "extension", base=abstract_type_name)
                    sequence = self.create_element(extension, "sequence")
                    
                    value_element = self.create_element(sequence, "element", name="value")
                    # Mark this type as known BEFORE processing to avoid recursion issues
                    self.known_types.append(option_type_name)
                    self.set_element_type(schema_root, option_type_name, value_element, choice_type)

    def set_element_type(self, schema_root: Element, record_name: str, element: Element, type_def: Any):
        """Set the type of an element based on the type definition."""
        
        # Handle type as list (union of types, e.g., ["string", "null"])
        if isinstance(type_def, list):
            # This is a union type
            non_null_types = [t for t in type_def if t != 'null']
            is_nullable = 'null' in type_def
            
            if len(non_null_types) == 1:
                # Simple nullable type
                self.set_element_type(schema_root, record_name, element, non_null_types[0])
                if is_nullable and 'minOccurs' not in element.attrib:
                    element.set('minOccurs', '0')
            elif len(non_null_types) == 0:
                # Just null - use string
                element.set('type', 'xs:string')
                element.set('minOccurs', '0')
            else:
                # Multiple non-null types
                # Check if all are primitives
                all_primitives = all(self.is_primitive_type(t) if isinstance(t, str) else False for t in non_null_types)
                
                if all_primitives:
                    # Can create a simple union type - use element name to make it unique
                    element_name = element.get('name', 'value')
                    unique_type_name = f"{record_name}_{element_name}" if element_name != record_name else record_name
                    choice_def = {'type': 'choice', 'choices': non_null_types}
                    self.convert_choice_type(schema_root, unique_type_name, element, choice_def)
                else:
                    # Has complex types - use xs:anyType for now
                    # XSD doesn't have a good way to represent unions with mixed simple/complex types
                    element.set('type', 'xs:anyType')
                
                if is_nullable and 'minOccurs' not in element.attrib:
                    element.set('minOccurs', '0')
            return
        
        # Handle $ref
        if isinstance(type_def, dict) and '$ref' in type_def:
            ref_type_name = self.get_type_name_from_ref(type_def['$ref'])
            resolved_type = self.resolve_type_reference(type_def['$ref'])
            if resolved_type:
                # Ensure the referenced type is created
                self.process_type_definition(schema_root, ref_type_name, resolved_type)
            element.set('type', ref_type_name)
            return
        
        # Handle object type
        if isinstance(type_def, dict):
            type_name = type_def.get('type', 'object')
            
            # Handle case where type is a list within the dict
            if isinstance(type_name, list):
                self.set_element_type(schema_root, record_name, element, type_name)
                return
            
            if type_name == 'object':
                # Inline object definition
                self.convert_object_properties(schema_root, record_name, element, type_def)
            elif type_name == 'array':
                self.convert_array_type(schema_root, record_name, element, type_def)
            elif type_name == 'set':
                self.convert_set_type(schema_root, record_name, element, type_def)
            elif type_name == 'map':
                self.convert_map_type(schema_root, record_name, element, type_def)
            elif type_name == 'tuple':
                self.convert_tuple_type(schema_root, record_name, element, type_def)
            elif type_name == 'choice':
                # Check if this choice should be a named type
                choice_name = type_def.get('name')
                if choice_name and choice_name not in self.known_types:
                    # Create a named complex type for this choice
                    self.process_type_definition(schema_root, choice_name, type_def)
                    element.set('type', choice_name)
                else:
                    # Inline choice
                    self.convert_choice_type(schema_root, record_name, element, type_def)
            elif self.is_primitive_type(type_name):
                xsd_type = self.map_primitive_to_xsd(type_name)
                element.set('type', xsd_type)
                
                # Apply constraints as restrictions
                facets = {}
                for key in ['minLength', 'maxLength', 'pattern', 'minimum', 'maximum', 
                           'exclusiveMinimum', 'exclusiveMaximum', 'precision', 'scale']:
                    if key in type_def:
                        facets[key] = type_def[key]
                
                if facets:
                    # Create an anonymous simple type with restriction
                    simple_type = self.create_element(element, "simpleType")
                    restriction = self.create_element(simple_type, "restriction", base=xsd_type)
                    element.attrib.pop('type', None)  # Remove type attribute
                    
                    for facet_name, facet_value in facets.items():
                        if facet_name == 'minLength':
                            self.create_element(restriction, "minLength", value=str(facet_value))
                        elif facet_name == 'maxLength':
                            self.create_element(restriction, "maxLength", value=str(facet_value))
                        elif facet_name == 'pattern':
                            self.create_element(restriction, "pattern", value=facet_value)
                        elif facet_name == 'minimum':
                            self.create_element(restriction, "minInclusive", value=str(facet_value))
                        elif facet_name == 'maximum':
                            self.create_element(restriction, "maxInclusive", value=str(facet_value))
                        elif facet_name == 'exclusiveMinimum':
                            self.create_element(restriction, "minExclusive", value=str(facet_value))
                        elif facet_name == 'exclusiveMaximum':
                            self.create_element(restriction, "maxExclusive", value=str(facet_value))
                        elif facet_name == 'precision' and 'scale' in type_def:
                            # For decimal types
                            self.create_element(restriction, "totalDigits", value=str(facet_value))
                            self.create_element(restriction, "fractionDigits", value=str(type_def['scale']))
            else:
                # Named type reference
                element.set('type', type_name)
        elif isinstance(type_def, str):
            # Simple type reference
            if self.is_primitive_type(type_def):
                element.set('type', self.map_primitive_to_xsd(type_def))
            else:
                element.set('type', type_def)
        else:
            # Default to string
            element.set('type', 'xs:string')

    def convert_object_properties(self, schema_root: Element, type_name: str, parent: Element, obj_def: Dict):
        """Convert object properties to XSD elements."""
        complex_type = self.create_element(parent, "complexType")
        
        # Add documentation if present
        description = obj_def.get('description')
        if description:
            annotation = self.create_element(complex_type, "annotation")
            documentation = self.create_element(annotation, "documentation")
            documentation.text = description
        
        sequence = self.create_element(complex_type, "sequence")
        
        properties = obj_def.get('properties', {})
        required = obj_def.get('required', [])
        
        for prop_name, prop_def in properties.items():
            min_occurs = "1" if prop_name in required else "0"
            prop_element = self.create_element(
                sequence, "element", name=prop_name,
                minOccurs=min_occurs, maxOccurs="1"
            )
            
            # Add property description
            if isinstance(prop_def, dict) and 'description' in prop_def:
                annotation = self.create_element(prop_element, "annotation")
                documentation = self.create_element(annotation, "documentation")
                documentation.text = prop_def['description']
            
            self.set_element_type(schema_root, type_name, prop_element, prop_def)

    def process_type_definition(self, schema_root: Element, type_name: str, type_def: Dict):
        """Process a type definition and create corresponding XSD type."""
        
        if type_name in self.known_types:
            return
        
        type_type = type_def.get('type', 'object')
        namespace = type_def.get('namespace', '')
        
        if namespace:
            self.update_common_namespace(namespace)
        
        # Handle abstract types
        is_abstract = type_def.get('abstract', False)
        
        if type_type == 'object':
            complex_type = self.create_complex_type(schema_root, name=type_name)
            if is_abstract:
                complex_type.set('abstract', 'true')
            
            # Add documentation
            description = type_def.get('description')
            if description:
                annotation = self.create_element(complex_type, "annotation")
                documentation = self.create_element(annotation, "documentation")
                documentation.text = description
            
            # Handle extensions ($extends)
            extends = type_def.get('$extends')
            if extends:
                complex_content = self.create_element(complex_type, "complexContent")
                base_type = self.get_type_name_from_ref(extends) if isinstance(extends, str) and extends.startswith('#') else extends
                extension = self.create_element(complex_content, "extension", base=base_type)
                sequence = self.create_element(extension, "sequence")
                
                # Add properties to the extension
                properties = type_def.get('properties', {})
                required = type_def.get('required', [])
                for prop_name, prop_def in properties.items():
                    min_occurs = "1" if prop_name in required else "0"
                    prop_element = self.create_element(
                        sequence, "element", name=prop_name,
                        minOccurs=min_occurs, maxOccurs="1"
                    )
                    self.set_element_type(schema_root, type_name, prop_element, prop_def)
            else:
                # Regular object
                sequence = self.create_element(complex_type, "sequence")
                properties = type_def.get('properties', {})
                required = type_def.get('required', [])
                
                for prop_name, prop_def in properties.items():
                    min_occurs = "1" if prop_name in required else "0"
                    prop_element = self.create_element(
                        sequence, "element", name=prop_name,
                        minOccurs=min_occurs, maxOccurs="1"
                    )
                    
                    # Add property description
                    if isinstance(prop_def, dict) and 'description' in prop_def:
                        annotation = self.create_element(prop_element, "annotation")
                        documentation = self.create_element(annotation, "documentation")
                        documentation.text = prop_def['description']
                    
                    self.set_element_type(schema_root, type_name, prop_element, prop_def)
            
            self.known_types.append(type_name)
        
        elif type_type == 'array':
            # Create a named complex type for the array
            complex_type = self.create_complex_type(schema_root, name=type_name)
            sequence = self.create_element(complex_type, "sequence")
            
            item_type = type_def.get('items', {'type': 'any'})
            min_items = type_def.get('minItems', 0)
            max_items = type_def.get('maxItems')
            
            item_element = self.create_element(
                sequence, "element", name="item",
                minOccurs=str(min_items),
                maxOccurs=str(max_items) if max_items is not None else "unbounded"
            )
            
            self.set_element_type(schema_root, type_name, item_element, item_type)
            self.known_types.append(type_name)
        
        elif type_type == 'choice':
            # Handle choice as complex type with choice element
            choices = type_def.get('choices', [])
            discriminator = type_def.get('discriminator') or type_def.get('selector')
            
            # Handle choices as dict (tagged) or list (inline)
            if isinstance(choices, dict):
                choices_list = [(name, typedef) for name, typedef in choices.items()]
            else:
                choices_list = [(f"option{i+1}", choice) for i, choice in enumerate(choices)]
            
            complex_type = self.create_complex_type(schema_root, name=type_name)
            choice_element = self.create_element(complex_type, "choice")
            
            for choice_name, choice_type in choices_list:
                option_element = self.create_element(choice_element, "element", name=choice_name)
                self.set_element_type(schema_root, type_name, option_element, choice_type)
            
            self.known_types.append(type_name)
        
        elif self.is_primitive_type(type_type):
            # Create a simple type with restrictions
            xsd_type = self.map_primitive_to_xsd(type_type)
            facets = {}
            for key in ['minLength', 'maxLength', 'pattern', 'minimum', 'maximum']:
                if key in type_def:
                    facets[key] = type_def[key]
            
            if facets:
                self.create_simple_type_with_restriction(schema_root, type_name, xsd_type, facets)
            else:
                # Simple typedef
                simple_type = self.create_element(schema_root, "simpleType", name=type_name)
                restriction = self.create_element(simple_type, "restriction", base=xsd_type)
            
            self.known_types.append(type_name)

    def structure_schema_to_xsd(self, structure_schema: Dict) -> Element:
        """Convert JSON Structure schema to XSD."""
        ET.register_namespace('xs', self.xmlns['xs'])
        schema = Element(f"{{{self.xmlns['xs']}}}schema")
        
        # Extract definitions
        self.definitions = structure_schema.get('definitions', {})
        
        # Process top-level type
        if structure_schema.get('type'):
            type_name = structure_schema.get('name', 'Root')
            namespace = structure_schema.get('namespace', '')
            
            if namespace:
                self.update_common_namespace(namespace)
            
            # Create root element
            root_element = self.create_element(schema, "element", name=type_name)
            
            # Add description
            description = structure_schema.get('description')
            if description:
                annotation = self.create_element(root_element, "annotation")
                documentation = self.create_element(annotation, "documentation")
                documentation.text = description
            
            # Set the type
            if structure_schema.get('type') == 'object':
                self.convert_object_properties(schema, type_name, root_element, structure_schema)
            else:
                self.set_element_type(schema, type_name, root_element, structure_schema)
        
        # Process all definitions
        for def_name, def_value in self.definitions.items():
            self.process_type_definition(schema, def_name, def_value)
        
        # Set namespace
        target_ns = self.target_namespace if self.target_namespace else \
                     f"urn:{self.common_namespace.replace('.', ':')}" if self.common_namespace else \
                     "urn:example:schema"
        
        schema.set('targetNamespace', target_ns)
        schema.set('xmlns', target_ns)
        schema.set('elementFormDefault', 'qualified')
        ET.register_namespace('', target_ns)
        
        return schema

    def save_xsd_to_file(self, schema: Element, xml_path: str) -> None:
        """Save the XML schema to a file."""
        os.makedirs(os.path.dirname(xml_path) or '.', exist_ok=True)
        tree_str = tostring(schema, 'utf-8')
        pretty_tree = minidom.parseString(tree_str).toprettyxml(indent="  ")
        with open(xml_path, 'w', encoding='utf-8') as xml_file:
            xml_file.write(pretty_tree)

    def convert_structure_to_xsd(self, structure_schema_path: str, xml_file_path: str) -> None:
        """Convert JSON Structure schema file to XML schema file."""
        with open(structure_schema_path, 'r', encoding='utf-8') as structure_file:
            structure_schema = json.load(structure_file)
        
        xml_schema = self.structure_schema_to_xsd(structure_schema)
        self.save_xsd_to_file(xml_schema, xml_file_path)


def convert_structure_to_xsd(structure_schema_path: str, xml_file_path: str, target_namespace: str = '') -> None:
    """Convert JSON Structure schema to XSD."""
    converter = StructureToXSD(target_namespace)
    converter.convert_structure_to_xsd(structure_schema_path, xml_file_path)
