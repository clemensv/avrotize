# pylint: disable=line-too-long

""" StructureToXSD class for converting JSON Structure schema to XML Schema Definition (XSD) """

import json
import os
from typing import Dict, List, Optional, Set, Any, Union
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import Element, SubElement
from xml.dom import minidom

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | None


class StructureToXSD:
    """ Converts JSON Structure schema to XSD """

    def __init__(self, target_namespace: str = ''):
        self.xmlns = {"xs": "http://www.w3.org/2001/XMLSchema"}
        self.union_types: Dict[str, str] = {}
        self.known_types: List[str] = []
        self.target_namespace = target_namespace
        self.common_namespace = ''
        self.schema_doc: JsonNode = None
        self.schema_registry: Dict[str, Dict] = {}
        self.generated_types: Dict[str, str] = {}

    def create_element(self, parent: Element, tag: str, **attributes) -> Element:
        """Create an XML element with the proper namespace."""
        return SubElement(parent, f"{{{self.xmlns['xs']}}}{tag}", **attributes)

    def create_complex_type(self, parent: Element, **attributes) -> Element:
        """Create an XML complexType element."""
        return self.create_element(parent, "complexType", **attributes)

    def create_simple_type(self, parent: Element, **attributes) -> Element:
        """Create an XML simpleType element."""
        return self.create_element(parent, "simpleType", **attributes)

    def register_schema_ids(self, schema: Dict, base_uri: str = '') -> None:
        """ Recursively registers schemas with $id keywords """
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
        """ Resolves a $ref to the actual schema definition """
        if not ref.startswith('#/'):
            if ref in self.schema_registry:
                return self.schema_registry[ref]
            return None
        
        path = ref[2:].split('/')
        schema = context_schema if context_schema else self.schema_doc
        for part in path:
            if not isinstance(schema, dict) or part not in schema:
                return None
            schema = schema[part]
        return schema

    def map_primitive_to_xsd(self, structure_type: str | dict) -> str:
        """Map JSON Structure primitive types to XSD data types."""
        
        # Handle dict with type keyword
        if isinstance(structure_type, dict):
            if 'type' in structure_type:
                structure_type = structure_type['type']
            else:
                return 'xs:string'
        
        if not isinstance(structure_type, str):
            return 'xs:string'
        
        mapping = {
            'null': 'xs:string',
            'boolean': 'xs:boolean',
            'string': 'xs:string',
            'integer': 'xs:integer',
            'number': 'xs:double',
            'int8': 'xs:byte',
            'uint8': 'xs:unsignedByte',
            'int16': 'xs:short',
            'uint16': 'xs:unsignedShort',
            'int32': 'xs:int',
            'uint32': 'xs:unsignedInt',
            'int64': 'xs:long',
            'uint64': 'xs:unsignedLong',
            'int128': 'xs:integer',
            'uint128': 'xs:integer',
            'float8': 'xs:float',
            'float': 'xs:float',
            'double': 'xs:double',
            'binary32': 'xs:float',
            'binary64': 'xs:double',
            'decimal': 'xs:decimal',
            'binary': 'xs:hexBinary',
            'date': 'xs:date',
            'time': 'xs:time',
            'datetime': 'xs:dateTime',
            'timestamp': 'xs:dateTime',
            'duration': 'xs:duration',
            'uuid': 'xs:string',
            'uri': 'xs:anyURI',
            'jsonpointer': 'xs:string',
            'any': 'xs:anyType'
        }
        
        return mapping.get(structure_type, 'xs:string')

    def is_primitive(self, structure_type: str | dict) -> bool:
        """Check if the type is a primitive type."""
        if isinstance(structure_type, dict):
            if 'type' in structure_type:
                structure_type = structure_type['type']
            else:
                return False
        
        if not isinstance(structure_type, str):
            return False
        
        primitives = [
            'null', 'boolean', 'string', 'integer', 'number',
            'int8', 'uint8', 'int16', 'uint16', 'int32', 'uint32',
            'int64', 'uint64', 'int128', 'uint128',
            'float8', 'float', 'double', 'binary32', 'binary64',
            'decimal', 'binary', 'date', 'time', 'datetime', 'timestamp',
            'duration', 'uuid', 'uri', 'jsonpointer', 'any'
        ]
        
        return structure_type in primitives

    def create_array(self, schema_root: ET.Element, parent: ET.Element, array_schema: dict):
        """ Handle JSON Structure 'array' type """
        complex_type = self.create_element(parent, "complexType")
        sequence = self.create_element(complex_type, "sequence")
        item_type = array_schema.get('items', {'type': 'any'})
        
        if isinstance(item_type, list):
            # Union type in array items
            item = self.create_union(schema_root, sequence, "item", item_type)
            item.set('minOccurs', '0')
            item.set('maxOccurs', 'unbounded')
        else:
            item = self.create_element(sequence, "element", name="item", minOccurs="0", maxOccurs="unbounded")
            self.set_field_type(schema_root, item, item_type)

    def create_set(self, schema_root: ET.Element, parent: ET.Element, set_schema: dict):
        """ Handle JSON Structure 'set' type - similar to array but semantically a set """
        # XSD doesn't have a built-in set type, so we model it as an array with unique items
        complex_type = self.create_element(parent, "complexType")
        sequence = self.create_element(complex_type, "sequence")
        item_type = set_schema.get('items', {'type': 'any'})
        
        item = self.create_element(sequence, "element", name="item", minOccurs="0", maxOccurs="unbounded")
        
        # Add annotation to indicate this is a set
        annotation = self.create_element(item, "annotation")
        documentation = self.create_element(annotation, "documentation")
        documentation.text = "Set type - items should be unique"
        
        self.set_field_type(schema_root, item, item_type)

    def create_map(self, schema_root: ET.Element, parent: ET.Element, map_schema: dict):
        """ Handle JSON Structure 'map' type """
        complex_type = self.create_element(parent, "complexType")
        sequence = self.create_element(complex_type, "sequence")
        item = self.create_element(sequence, "element", name="entry", minOccurs="0", maxOccurs="unbounded")
        inner_complex_type = self.create_element(item, "complexType")
        inner_sequence = self.create_element(inner_complex_type, "sequence")
        self.create_element(inner_sequence, "element", name="key", type="xs:string")
        
        map_values = map_schema.get('values', {'type': 'any'})
        if isinstance(map_values, list):
            value_element = self.create_union(schema_root, inner_sequence, "value", map_values)
        else:
            value_element = self.create_element(inner_sequence, "element", name="value")
            self.set_field_type(schema_root, value_element, map_values)

    def create_tuple(self, schema_root: ET.Element, parent: ET.Element, tuple_schema: dict):
        """ Handle JSON Structure 'tuple' type """
        complex_type = self.create_element(parent, "complexType")
        sequence = self.create_element(complex_type, "sequence")
        
        # Add annotation
        annotation = self.create_element(complex_type, "annotation")
        documentation = self.create_element(annotation, "documentation")
        documentation.text = "Tuple type - ordered fixed-length array"
        
        # Get tuple ordering and properties
        tuple_order = tuple_schema.get('tuple', [])
        properties = tuple_schema.get('properties', {})
        
        # Create elements in tuple order
        for i, prop_name in enumerate(tuple_order):
            if prop_name in properties:
                prop_schema = properties[prop_name]
                element = self.create_element(sequence, "element", name=prop_name)
                
                # Add documentation if available
                if 'description' in prop_schema or 'doc' in prop_schema:
                    doc_text = prop_schema.get('description', prop_schema.get('doc', ''))
                    if doc_text:
                        elem_annotation = self.create_element(element, "annotation")
                        elem_doc = self.create_element(elem_annotation, "documentation")
                        elem_doc.text = doc_text
                
                self.set_field_type(schema_root, element, prop_schema)

    def create_union(self, schema_root: ET.Element, parent: ET.Element, field_name: str, 
                    field_types: list) -> ET.Element:
        """Create an XML element for union types (anyOf)."""
        
        # Filter out null types
        non_null_types = [t for t in field_types if t != 'null']
        has_null = 'null' in field_types
        
        if len(non_null_types) == 0:
            # Only null - create optional string element
            element = self.create_element(parent, "element", name=field_name, type="xs:string", minOccurs="0")
            return element
        
        if len(non_null_types) == 1:
            # Single non-null type - just make it optional if null is included
            element = self.create_element(parent, "element", name=field_name)
            if has_null:
                element.set('minOccurs', '0')
            self.set_field_type(schema_root, element, non_null_types[0])
            return element
        
        # Multiple non-null types - create a choice element
        element = self.create_element(parent, "element", name=field_name)
        if has_null:
            element.set('minOccurs', '0')
        
        # Check if all types are primitives
        all_primitives = all(self.is_primitive(t) for t in non_null_types)
        
        if all_primitives:
            # Create a union of simple types
            union_type_name = f"{field_name}Union"
            
            # Check if union type already exists
            if union_type_name not in self.known_types:
                simple_type = self.create_simple_type(schema_root, name=union_type_name)
                union = self.create_element(simple_type, "union")
                member_types = [self.map_primitive_to_xsd(t) for t in non_null_types]
                union.set("memberTypes", ' '.join(member_types))
                self.known_types.append(union_type_name)
            
            element.set('type', union_type_name)
        else:
            # Create an abstract base type with substitution group
            abstract_type_name = f"{field_name}ChoiceType"
            
            if abstract_type_name not in self.known_types:
                # Create abstract base type
                self.create_complex_type(schema_root, name=abstract_type_name, abstract="true")
                self.known_types.append(abstract_type_name)
                
                # Create derived types for each option
                for i, union_type in enumerate(non_null_types):
                    derived_type_name = f"{abstract_type_name}Option{i+1}"
                    if derived_type_name not in self.known_types:
                        derived_complex = self.create_complex_type(schema_root, name=derived_type_name)
                        complex_content = self.create_element(derived_complex, "complexContent")
                        extension = self.create_element(complex_content, "extension", base=abstract_type_name)
                        sequence = self.create_element(extension, "sequence")
                        value_element = self.create_element(sequence, "element", name="value")
                        self.set_field_type(schema_root, value_element, union_type)
                        self.known_types.append(derived_type_name)
            
            element.set('type', abstract_type_name)
        
        return element

    def create_enum(self, schema_root: ET.Element, enum_schema: dict, type_name: str) -> str:
        """Convert a JSON Structure enum to an XML simpleType."""
        enum_values = enum_schema.get('enum', [])
        if not enum_values:
            return 'xs:string'
        
        name = enum_schema.get('name', type_name)
        doc = enum_schema.get('description', enum_schema.get('doc', ''))
        
        if name in self.known_types:
            return name
        
        simple_type = self.create_element(schema_root, "simpleType", name=name)
        
        if doc:
            annotation = self.create_element(simple_type, "annotation")
            documentation = self.create_element(annotation, "documentation")
            documentation.text = doc
        
        restriction = self.create_element(simple_type, "restriction", base="xs:string")
        
        for enum_symbol in enum_values:
            self.create_element(restriction, "enumeration", value=str(enum_symbol))
        
        self.known_types.append(name)
        return name

    def create_choice(self, schema_root: ET.Element, choice_schema: dict, type_name: str) -> str:
        """Handle JSON Structure 'choice' type - discriminated union."""
        name = choice_schema.get('name', type_name)
        doc = choice_schema.get('description', choice_schema.get('doc', ''))
        
        if name in self.known_types:
            return name
        
        # Check if this is a tagged union (has choices) or inline union (has $extends and selector)
        choices = choice_schema.get('choices', {})
        extends = choice_schema.get('$extends')
        selector = choice_schema.get('selector', 'type')
        
        if extends and selector:
            # Inline union with inheritance
            return self.create_inline_union(schema_root, choice_schema, type_name)
        else:
            # Tagged union
            return self.create_tagged_union(schema_root, choice_schema, type_name)

    def create_tagged_union(self, schema_root: ET.Element, choice_schema: dict, type_name: str) -> str:
        """Create a tagged union type."""
        name = choice_schema.get('name', type_name)
        choices = choice_schema.get('choices', {})
        
        if name in self.known_types:
            return name
        
        # Create an abstract base type
        abstract_type = self.create_complex_type(schema_root, name=name, abstract="true")
        
        doc = choice_schema.get('description', choice_schema.get('doc', ''))
        if doc:
            annotation = self.create_element(abstract_type, "annotation")
            documentation = self.create_element(annotation, "documentation")
            documentation.text = doc
        
        self.known_types.append(name)
        
        # Create derived types for each choice
        for choice_name, choice_schema_ref in choices.items():
            # Resolve the choice schema
            if isinstance(choice_schema_ref, dict) and '$ref' in choice_schema_ref:
                resolved_choice = self.resolve_ref(choice_schema_ref['$ref'], self.schema_doc)
            else:
                resolved_choice = choice_schema_ref
            
            if resolved_choice:
                derived_name = f"{name}_{choice_name}"
                if derived_name not in self.known_types:
                    derived_complex = self.create_complex_type(schema_root, name=derived_name)
                    complex_content = self.create_element(derived_complex, "complexContent")
                    extension = self.create_element(complex_content, "extension", base=name)
                    
                    # Add choice-specific fields
                    if isinstance(resolved_choice, dict) and 'properties' in resolved_choice:
                        sequence = self.create_element(extension, "sequence")
                        for prop_name, prop_schema in resolved_choice['properties'].items():
                            self.create_field(schema_root, sequence, prop_name, prop_schema, 
                                            resolved_choice.get('required', []))
                    
                    self.known_types.append(derived_name)
        
        return name

    def create_inline_union(self, schema_root: ET.Element, choice_schema: dict, type_name: str) -> str:
        """Create an inline union with base class inheritance."""
        name = choice_schema.get('name', type_name)
        
        if name in self.known_types:
            return name
        
        # Resolve base type
        extends_ref = choice_schema.get('$extends', '')
        base_schema = self.resolve_ref(extends_ref, self.schema_doc) if extends_ref else None
        
        # Create the abstract base
        abstract_type = self.create_complex_type(schema_root, name=name, abstract="true")
        
        doc = choice_schema.get('description', choice_schema.get('doc', ''))
        if doc:
            annotation = self.create_element(abstract_type, "annotation")
            documentation = self.create_element(annotation, "documentation")
            documentation.text = doc
        
        # If extends a base, use extension
        if base_schema and 'name' in base_schema:
            base_type_name = base_schema['name']
            # Ensure base type is generated
            if base_schema.get('type') == 'object':
                self.create_object(schema_root, base_schema, base_type_name)
            
            complex_content = self.create_element(abstract_type, "complexContent")
            extension = self.create_element(complex_content, "extension", base=base_type_name)
            
            # Add selector field
            sequence = self.create_element(extension, "sequence")
            selector = choice_schema.get('selector', 'type')
            self.create_element(sequence, "element", name=selector, type="xs:string")
        else:
            # No base, just add selector
            sequence = self.create_element(abstract_type, "sequence")
            selector = choice_schema.get('selector', 'type')
            self.create_element(sequence, "element", name=selector, type="xs:string")
        
        self.known_types.append(name)
        
        # Generate derived types for each choice
        choices = choice_schema.get('choices', {})
        for choice_name, choice_schema_ref in choices.items():
            if isinstance(choice_schema_ref, dict) and '$ref' in choice_schema_ref:
                resolved_choice = self.resolve_ref(choice_schema_ref['$ref'], self.schema_doc)
            else:
                resolved_choice = choice_schema_ref
            
            if resolved_choice:
                derived_name = f"{name}_{choice_name}"
                if derived_name not in self.known_types:
                    derived_complex = self.create_complex_type(schema_root, name=derived_name)
                    complex_content = self.create_element(derived_complex, "complexContent")
                    extension = self.create_element(complex_content, "extension", base=name)
                    
                    # Add choice-specific properties
                    if isinstance(resolved_choice, dict) and 'properties' in resolved_choice:
                        sequence = self.create_element(extension, "sequence")
                        for prop_name, prop_schema in resolved_choice['properties'].items():
                            self.create_field(schema_root, sequence, prop_name, prop_schema,
                                            resolved_choice.get('required', []))
                    
                    self.known_types.append(derived_name)
        
        return name

    def create_object(self, schema_root: ET.Element, object_schema: dict, type_name: str) -> str:
        """Convert a JSON Structure object to an XML complex type."""
        name = object_schema.get('name', type_name)
        doc = object_schema.get('description', object_schema.get('doc', ''))
        
        if name in self.known_types:
            return name
        
        # Check if abstract
        is_abstract = object_schema.get('abstract', False)
        
        complex_type_attrs = {"name": name}
        if is_abstract:
            complex_type_attrs["abstract"] = "true"
        
        complex_type = self.create_complex_type(schema_root, **complex_type_attrs)
        
        if doc:
            annotation = self.create_element(complex_type, "annotation")
            documentation = self.create_element(annotation, "documentation")
            documentation.text = doc
        
        # Handle inheritance
        extends_ref = object_schema.get('$extends')
        if extends_ref:
            base_schema = self.resolve_ref(extends_ref, self.schema_doc)
            if base_schema and 'name' in base_schema:
                base_type_name = base_schema['name']
                # Ensure base type is generated
                if base_schema.get('type') == 'object':
                    self.create_object(schema_root, base_schema, base_type_name)
                
                complex_content = self.create_element(complex_type, "complexContent")
                extension = self.create_element(complex_content, "extension", base=base_type_name)
                sequence = self.create_element(extension, "sequence")
            else:
                sequence = self.create_element(complex_type, "sequence")
        else:
            sequence = self.create_element(complex_type, "sequence")
        
        # Generate properties
        properties = object_schema.get('properties', {})
        required_props = object_schema.get('required', [])
        
        for prop_name, prop_schema in properties.items():
            self.create_field(schema_root, sequence, prop_name, prop_schema, required_props)
        
        self.known_types.append(name)
        return name

    def create_field(self, schema_root: ET.Element, parent: ET.Element, 
                    field_name: str, field_schema: dict, required_props: list):
        """Create an XML element for an object field."""
        
        # Handle const fields
        if 'const' in field_schema:
            # XSD doesn't have const, so we use fixed value with restriction
            element = self.create_element(parent, "element", name=field_name)
            const_value = field_schema['const']
            element.set('fixed', str(const_value))
            
            # Set type based on const value type
            if isinstance(const_value, bool):
                element.set('type', 'xs:boolean')
            elif isinstance(const_value, int):
                element.set('type', 'xs:integer')
            elif isinstance(const_value, float):
                element.set('type', 'xs:double')
            else:
                element.set('type', 'xs:string')
            return
        
        # Check if field is required
        is_required = field_name in required_props if isinstance(required_props, list) else False
        
        # Handle documentation
        doc = field_schema.get('description', field_schema.get('doc', ''))
        
        # Handle anyOf (union types)
        if 'anyOf' in field_schema:
            types_list = field_schema['anyOf']
            element = self.create_union(schema_root, parent, field_name, types_list)
            if not is_required:
                element.set('minOccurs', '0')
            
            if doc:
                annotation = self.create_element(element, "annotation")
                documentation = self.create_element(annotation, "documentation")
                documentation.text = doc
        else:
            # Create regular element
            element = self.create_element(parent, "element", name=field_name)
            
            if not is_required:
                element.set('minOccurs', '0')
            
            if doc:
                annotation = self.create_element(element, "annotation")
                documentation = self.create_element(annotation, "documentation")
                documentation.text = doc
            
            self.set_field_type(schema_root, element, field_schema)

    def set_field_type(self, schema_root: ET.Element, element: ET.Element, field_type: dict | str):
        """ Set the type or create a subtype on the element for the given field type """
        
        if isinstance(field_type, str):
            # Primitive type
            element.set('type', self.map_primitive_to_xsd(field_type))
            return
        
        if not isinstance(field_type, dict):
            element.set('type', 'xs:string')
            return
        
        # Handle $ref
        if '$ref' in field_type:
            ref_schema = self.resolve_ref(field_type['$ref'], self.schema_doc)
            if ref_schema:
                ref_path = field_type['$ref'].split('/')
                type_name = ref_path[-1]
                
                # Generate the referenced type
                if 'enum' in ref_schema:
                    xsd_type = self.create_enum(schema_root, ref_schema, type_name)
                elif ref_schema.get('type') == 'object':
                    xsd_type = self.create_object(schema_root, ref_schema, type_name)
                elif ref_schema.get('type') == 'choice':
                    xsd_type = self.create_choice(schema_root, ref_schema, type_name)
                else:
                    xsd_type = 'xs:string'
                
                element.set('type', xsd_type)
                return
            element.set('type', 'xs:string')
            return
        
        # Handle enum
        if 'enum' in field_type:
            type_name = field_type.get('name', element.get('name', 'UnnamedEnum'))
            xsd_type = self.create_enum(schema_root, field_type, type_name)
            element.set('type', xsd_type)
            return
        
        # Handle type keyword
        if 'type' not in field_type:
            element.set('type', 'xs:string')
            return
        
        struct_type = field_type['type']
        
        # Handle primitive types
        if self.is_primitive(struct_type):
            element.set('type', self.map_primitive_to_xsd(struct_type))
            return
        
        # Handle complex types
        if struct_type == 'object':
            type_name = field_type.get('name', element.get('name', 'UnnamedObject'))
            xsd_type = self.create_object(schema_root, field_type, type_name)
            element.set('type', xsd_type)
        elif struct_type == 'array':
            self.create_array(schema_root, element, field_type)
        elif struct_type == 'set':
            self.create_set(schema_root, element, field_type)
        elif struct_type == 'map':
            self.create_map(schema_root, element, field_type)
        elif struct_type == 'choice':
            type_name = field_type.get('name', element.get('name', 'UnnamedChoice'))
            xsd_type = self.create_choice(schema_root, field_type, type_name)
            element.set('type', xsd_type)
        elif struct_type == 'tuple':
            self.create_tuple(schema_root, element, field_type)
        else:
            element.set('type', 'xs:string')

    def xsd_namespace_from_structure_namespace(self, namespace: str) -> str:
        """Convert a JSON Structure namespace to an XML schema namespace."""
        if not self.target_namespace:
            return f"urn:{namespace.replace('.', ':')}" if namespace else "urn:default"
        else:
            return self.target_namespace

    def structure_schema_to_xsd(self, structure_schema: dict | list) -> Element:
        """Convert the top-level JSON Structure schema to an XML schema."""
        ET.register_namespace('xs', self.xmlns['xs'])
        schema = Element(f"{{{self.xmlns['xs']}}}schema")
        
        # Handle list of schemas
        schemas_to_process = structure_schema if isinstance(structure_schema, list) else [structure_schema]
        
        # Register all schema IDs first
        for schema_item in schemas_to_process:
            if isinstance(schema_item, dict):
                self.schema_doc = schema_item
                self.register_schema_ids(schema_item)
        
        # Extract namespace from first schema
        target_ns = ''
        for schema_item in schemas_to_process:
            if isinstance(schema_item, dict):
                ns = schema_item.get('namespace', '')
                if ns:
                    target_ns = ns
                    break
        
        # Process each schema
        for schema_item in schemas_to_process:
            if not isinstance(schema_item, dict):
                continue
            
            self.schema_doc = schema_item
            
            # Check for enum at root level
            if 'enum' in schema_item:
                type_name = schema_item.get('name', 'RootEnum')
                self.create_enum(schema, schema_item, type_name)
            
            # Check for root type
            elif 'type' in schema_item:
                struct_type = schema_item['type']
                type_name = schema_item.get('name', 'Root')
                
                if struct_type == 'object':
                    self.create_object(schema, schema_item, type_name)
                elif struct_type == 'choice':
                    self.create_choice(schema, schema_item, type_name)
                elif struct_type == 'array':
                    # Create a root element for the array
                    root_element = self.create_element(schema, "element", name=type_name)
                    self.create_array(schema, root_element, schema_item)
                elif struct_type == 'map':
                    # Create a root element for the map
                    root_element = self.create_element(schema, "element", name=type_name)
                    self.create_map(schema, root_element, schema_item)
                elif struct_type == 'tuple':
                    # Create a root element for the tuple
                    root_element = self.create_element(schema, "element", name=type_name)
                    self.create_tuple(schema, root_element, schema_item)
            
            # Process definitions
            if 'definitions' in schema_item:
                self.process_definitions(schema, schema_item['definitions'], '')
        
        # Set namespace
        xsd_ns = self.xsd_namespace_from_structure_namespace(target_ns)
        schema.set('targetNamespace', xsd_ns)
        schema.set('xmlns', xsd_ns)
        ET.register_namespace('', xsd_ns)
        
        return schema

    def process_definitions(self, schema_root: Element, definitions: Dict, namespace_path: str):
        """ Processes the definitions section recursively """
        for name, definition in definitions.items():
            if isinstance(definition, dict):
                if 'type' in definition:
                    # This is a type definition
                    if 'enum' in definition:
                        self.create_enum(schema_root, definition, name)
                    elif definition['type'] == 'object':
                        self.create_object(schema_root, definition, name)
                    elif definition['type'] == 'choice':
                        self.create_choice(schema_root, definition, name)
                elif 'enum' in definition:
                    # Enum without explicit type
                    self.create_enum(schema_root, definition, name)
                else:
                    # This might be a namespace - process recursively
                    new_namespace = f"{namespace_path}.{name}" if namespace_path else name
                    self.process_definitions(schema_root, definition, new_namespace)

    def save_xsd_to_file(self, schema: Element, xsd_path: str) -> None:
        """Save the XML schema to a file."""
        tree_str = ET.tostring(schema, 'utf-8')
        pretty_tree = minidom.parseString(tree_str).toprettyxml(indent="  ")
        with open(xsd_path, 'w', encoding='utf-8') as xsd_file:
            xsd_file.write(pretty_tree)

    def convert_structure_to_xsd(self, structure_schema_path: str, xsd_file_path: str) -> None:
        """Convert JSON Structure schema file to XML schema file."""
        with open(structure_schema_path, 'r', encoding='utf-8') as structure_file:
            structure_schema = json.load(structure_file)
        
        xsd_schema = self.structure_schema_to_xsd(structure_schema)
        self.save_xsd_to_file(xsd_schema, xsd_file_path)


def convert_structure_to_xsd(structure_schema_path: str, xsd_file_path: str, target_namespace: str = '') -> None:
    """Convert JSON Structure schema to XSD schema.
    
    Args:
        structure_schema_path: Path to the JSON Structure schema file
        xsd_file_path: Path to the output XSD file
        target_namespace: Optional target namespace for the XSD schema
    """
    converter = StructureToXSD(target_namespace)
    converter.convert_structure_to_xsd(structure_schema_path, xsd_file_path)
