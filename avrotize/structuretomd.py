# coding: utf-8
"""
Module to convert JSON Structure schema to Markdown documentation.
"""

import json
import os
from typing import Any, Dict, List, Union

from avrotize.common import render_template

class StructureToMarkdownConverter:
    """
    Class to convert JSON Structure schema to Markdown documentation.
    """

    def __init__(self, structure_schema_path: str, markdown_path: str):
        """
        Initialize the converter with file paths.

        :param structure_schema_path: Path to the JSON Structure schema file.
        :param markdown_path: Path to save the Markdown file.
        """
        self.structure_schema_path = structure_schema_path
        self.markdown_path = markdown_path
        self.objects = {}
        self.choices = {}
        self.enums = {}
        self.definitions = {}

    def convert(self):
        """
        Convert JSON Structure schema to Markdown and save to file.
        """
        with open(self.structure_schema_path, 'r', encoding='utf-8') as file:
            structure_schema = json.load(file)

        schema_name = os.path.splitext(os.path.basename(self.structure_schema_path))[0]
        
        # Process the schema
        self.process_schema(structure_schema)
        
        # Generate markdown
        self.generate_markdown(schema_name, structure_schema)

    def process_schema(self, schema: Dict[str, Any], parent_namespace: str = ''):
        """
        Process the schema to extract all named types.
        
        :param schema: The schema or sub-schema to process
        :param parent_namespace: The parent namespace
        """
        # Store definitions separately
        if 'definitions' in schema:
            self.definitions = schema['definitions']
        
        # Process root schema (but not definitions)
        if 'type' in schema:
            name = schema.get('name', 'Root')
            self.extract_named_types(schema, parent_namespace, name, skip_definitions=True)

    def extract_named_types(self, schema: Union[Dict, List, str], parent_namespace: str = '', type_name: str = '', skip_definitions: bool = False):
        """
        Extract all named types (objects, choices, enums) from the schema.
        
        :param schema: Schema element to process
        :param parent_namespace: Parent namespace
        :param type_name: Name of this type
        :param skip_definitions: Skip processing definitions (they're handled separately)
        """
        if isinstance(schema, dict):
            # Skip definitions section when processing root schema
            if skip_definitions and 'definitions' in schema:
                # Create a copy without definitions for processing
                schema = {k: v for k, v in schema.items() if k != 'definitions'}
            
            schema_type = schema.get('type')
            ns = schema.get('namespace', parent_namespace)
            name = schema.get('name', type_name)
            
            # Only add named objects and choices to the respective lists
            # Inline/anonymous objects should not be listed separately
            if schema_type == 'object' and 'name' in schema:
                self.objects.setdefault(ns, []).append(schema)
            elif schema_type == 'choice' and 'name' in schema:
                self.choices.setdefault(ns, []).append(schema)
            elif 'enum' in schema:
                # Handle enum constraint
                self.enums.setdefault(ns, []).append({
                    'name': name,
                    'values': schema['enum'],
                    'doc': schema.get('description', '')
                })
            
            # Recursively process properties
            if 'properties' in schema:
                for prop_name, prop_schema in schema['properties'].items():
                    self.extract_named_types(prop_schema, ns, prop_name)
            
            # Process items in arrays/sets
            if 'items' in schema:
                self.extract_named_types(schema['items'], ns)
            
            # Process values in maps
            if 'values' in schema:
                self.extract_named_types(schema['values'], ns)
            
            # Process choice alternatives
            if 'choices' in schema:
                choices = schema['choices']
                if isinstance(choices, dict):
                    for choice_name, choice_schema in choices.items():
                        self.extract_named_types(choice_schema, ns, choice_name)
                elif isinstance(choices, list):
                    for choice_schema in choices:
                        self.extract_named_types(choice_schema, ns)
                        
        elif isinstance(schema, list):
            # Union type
            for sub_schema in schema:
                self.extract_named_types(sub_schema, parent_namespace)

    def generate_markdown(self, schema_name: str, root_schema: Dict[str, Any]):
        """
        Generate markdown content from the extracted types using Jinja2 template.

        :param schema_name: The name of the schema file.
        :param root_schema: The root schema object
        """
        render_template("structuretomd/README.md.jinja", self.markdown_path,
            schema_name=schema_name,
            schema_description=root_schema.get('description', ''),
            schema_id=root_schema.get('$id', ''),
            schema_uses=root_schema.get('$uses', []),
            schema_offers=root_schema.get('$offers', {}),
            objects=self.objects,
            choices=self.choices,
            enums=self.enums,
            definitions=self.definitions,
            generate_property_markdown=self.generate_property_markdown,
            get_type_string=self.get_type_string
        )

    def generate_property_markdown(self, prop_name: str, prop_schema: Union[Dict, List, str], required: bool = False) -> str:
        """
        Generate markdown content for a single property.

        :param prop_name: Property name
        :param prop_schema: Property schema
        :param required: Whether the property is required
        :return: Markdown content as a string
        """
        lines = []
        type_str = self.get_type_string(prop_schema)
        required_str = " (required)" if required else ""
        
        lines.append(f"- **{prop_name}**{required_str}: {type_str}")
        
        # Add description if present
        if isinstance(prop_schema, dict):
            if 'description' in prop_schema:
                lines.append(f"  - Description: {prop_schema['description']}")
            
            # Add extension properties (from various JSON Structure extension specs)
            extensions = []
            
            # Alternate names (JSONStructureAlternateNames extension)
            if 'altnames' in prop_schema:
                altnames = prop_schema['altnames']
                if isinstance(altnames, dict):
                    altnames_list = [f"{k}: {v}" for k, v in altnames.items()]
                    extensions.append(f"altnames: {{{', '.join(altnames_list)}}}")
            
            # Units (JSONStructureUnits extension)
            if 'unit' in prop_schema:
                extensions.append(f"unit: {prop_schema['unit']}")
            
            # Currency (for decimal types)
            if 'currency' in prop_schema:
                extensions.append(f"currency: {prop_schema['currency']}")
            
            # Symbol (from JSONStructureSymbol extension)
            if 'symbol' in prop_schema:
                extensions.append(f"symbol: {prop_schema['symbol']}")
            
            # Content encoding/media type (JSONStructureContent extension)
            if 'contentEncoding' in prop_schema:
                extensions.append(f"contentEncoding: {prop_schema['contentEncoding']}")
            if 'contentMediaType' in prop_schema:
                extensions.append(f"contentMediaType: {prop_schema['contentMediaType']}")
            
            # Format
            if 'format' in prop_schema:
                extensions.append(f"format: {prop_schema['format']}")
            
            # Examples
            if 'examples' in prop_schema:
                examples_str = ', '.join(str(ex) for ex in prop_schema['examples'])
                extensions.append(f"examples: [{examples_str}]")
            
            # Default value
            if 'default' in prop_schema:
                extensions.append(f"default: {prop_schema['default']}")
            
            if extensions:
                lines.append(f"  - Extensions: {', '.join(extensions)}")
            
            # Add constraints
            constraints = []
            if 'minLength' in prop_schema:
                constraints.append(f"minLength: {prop_schema['minLength']}")
            if 'maxLength' in prop_schema:
                constraints.append(f"maxLength: {prop_schema['maxLength']}")
            if 'minimum' in prop_schema:
                constraints.append(f"minimum: {prop_schema['minimum']}")
            if 'maximum' in prop_schema:
                constraints.append(f"maximum: {prop_schema['maximum']}")
            if 'exclusiveMinimum' in prop_schema:
                constraints.append(f"exclusiveMinimum: {prop_schema['exclusiveMinimum']}")
            if 'exclusiveMaximum' in prop_schema:
                constraints.append(f"exclusiveMaximum: {prop_schema['exclusiveMaximum']}")
            if 'multipleOf' in prop_schema:
                constraints.append(f"multipleOf: {prop_schema['multipleOf']}")
            if 'pattern' in prop_schema:
                constraints.append(f"pattern: `{prop_schema['pattern']}`")
            if 'minItems' in prop_schema:
                constraints.append(f"minItems: {prop_schema['minItems']}")
            if 'maxItems' in prop_schema:
                constraints.append(f"maxItems: {prop_schema['maxItems']}")
            if 'precision' in prop_schema:
                constraints.append(f"precision: {prop_schema['precision']}")
            if 'scale' in prop_schema:
                constraints.append(f"scale: {prop_schema['scale']}")
            if 'enum' in prop_schema:
                constraints.append(f"enum: {', '.join(str(v) for v in prop_schema['enum'])}")
            if 'const' in prop_schema:
                constraints.append(f"const: {prop_schema['const']}")
            
            if constraints:
                lines.append(f"  - Constraints: {', '.join(constraints)}")
        
        return '\n'.join(lines)

    def get_type_string(self, schema: Union[Dict, List, str]) -> str:
        """
        Get a string representation of a type.

        :param schema: Type schema
        :return: Type string
        """
        if isinstance(schema, str):
            # Simple type reference or primitive
            if schema.startswith('#/'):
                # Reference
                ref_parts = schema.split('/')
                return f"[{ref_parts[-1]}](#{ref_parts[-1].lower()})"
            return f"`{schema}`"
        
        elif isinstance(schema, list):
            # Union type
            type_strs = [self.get_type_string(t) for t in schema]
            return ' | '.join(type_strs)
        
        elif isinstance(schema, dict):
            schema_type = schema.get('type')
            
            if '$ref' in schema:
                # Reference
                ref = schema['$ref']
                ref_parts = ref.split('/')
                ref_name = ref_parts[-1]
                return f"[{ref_name}](#{ref_name.lower()})"
            
            if schema_type == 'array':
                items_type = self.get_type_string(schema.get('items', 'any'))
                return f"array&lt;{items_type}&gt;"
            
            elif schema_type == 'set':
                items_type = self.get_type_string(schema.get('items', 'any'))
                return f"set&lt;{items_type}&gt;"
            
            elif schema_type == 'map':
                keys_type = self.get_type_string(schema.get('keys', 'string'))
                values_type = self.get_type_string(schema.get('values', 'any'))
                return f"map&lt;{keys_type}, {values_type}&gt;"
            
            elif schema_type == 'tuple':
                items = schema.get('items', [])
                if isinstance(items, list):
                    item_types = [self.get_type_string(item) for item in items]
                    return f"tuple&lt;{', '.join(item_types)}&gt;"
                return "tuple"
            
            elif schema_type == 'choice':
                name = schema.get('name', 'Choice')
                return f"[{name}](#{name.lower()})"
            
            elif schema_type == 'object':
                name = schema.get('name', 'object')
                if name != 'object':
                    return f"[{name}](#{name.lower()})"
                return "`object`"
            
            elif schema_type:
                return f"`{schema_type}`"
            
            # If no type specified but has properties, it's an inline object
            if 'properties' in schema:
                return "`object` (inline)"
        
        return "`any`"


def convert_structure_to_markdown(structure_schema_path: str, markdown_path: str):
    """
    Convert a JSON Structure schema file to a Markdown file.

    :param structure_schema_path: Path to the JSON Structure schema file.
    :param markdown_path: Path to save the Markdown file.
    """
    converter = StructureToMarkdownConverter(structure_schema_path, markdown_path)
    converter.convert()
