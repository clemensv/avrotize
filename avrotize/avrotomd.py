# coding: utf-8
"""
Module to convert Avro schema to a comprehensive README.md.
"""

import json
import os
from jinja2 import Environment, FileSystemLoader

from avrotize.common import render_template

class AvroToMarkdownConverter:
    """
    Class to convert Avro schema to a comprehensive README.md.
    """

    def __init__(self, avro_schema_path, markdown_path):
        """
        Initialize the converter with file paths.

        :param avro_schema_path: Path to the Avro schema file.
        :param markdown_path: Path to save the README.md file.
        """
        self.avro_schema_path = avro_schema_path
        self.markdown_path = markdown_path
        self.records = {}
        self.enums = {}
        self.fixeds = {}

    def convert(self):
        """
        Convert Avro schema to Markdown and save to file.
        """
        with open(self.avro_schema_path, 'r', encoding='utf-8') as file:
            avro_schemas = json.load(file)

        schema_name = os.path.splitext(os.path.basename(self.avro_schema_path))[0]
        if isinstance(avro_schemas, dict):
            self.extract_named_types(avro_schemas)
        elif isinstance(avro_schemas, list):
            for schema in avro_schemas:
                self.extract_named_types(schema)

        self.generate_markdown(schema_name)

    def extract_named_types(self, schema, parent_namespace: str = ''):
        """
        Extract all named types (records, enums, fixed) from the schema.
        """
        
        if isinstance(schema, dict):
            ns = schema.get('namespace', parent_namespace)
            if schema['type'] == 'record':
                self.records.setdefault(ns, []).append(schema)
            elif schema['type'] == 'enum':
                self.enums.setdefault(ns, []).append(schema)
            elif schema['type'] == 'fixed':
                self.fixeds.setdefault(ns, []).append(schema)
            if 'fields' in schema:
                for field in schema['fields']:
                    self.extract_named_types(field['type'], ns)
            if 'items' in schema:
                self.extract_named_types(schema['items'], ns)
            if 'values' in schema:
                self.extract_named_types(schema['values'], ns)
        elif isinstance(schema, list):
            for sub_schema in schema:
                self.extract_named_types(sub_schema, '')
                
    def generate_markdown(self, schema_name: str):
        """
        Generate markdown content from the extracted types using Jinja2 template.

        :param schema_name: The name of the schema file.
        :return: Markdown content as a string.
        """
        render_template("avrotomd/README.md.jinja", self.markdown_path,
            schema_name = schema_name,
            records  = self.records,
            enums = self.enums,
            fixeds = self.fixeds,
            generate_field_markdown = self.generate_field_markdown
        )

    def generate_field_markdown(self, field, level):
        """
        Generate markdown content for a single field.

        :param field: Avro field as a dictionary.
        :param level: The current level of nesting.
        :return: Markdown content as a string.
        """
        field_md = []
        heading = "#" * level
        field_md.append(f"{heading} {field['name']}\n")
        field_md.append(f"- **Type:** {self.get_avro_type(field['type'])}")
        if 'doc' in field:
            field_md.append(f"- **Description:** {field['doc']}")
        if 'default' in field:
            field_md.append(f"- **Default:** {field['default']}")
        if isinstance(field['type'], dict) and field['type'].get('logicalType'):
            field_md.append(f"- **Logical Type:** {field['type']['logicalType']}")
        if 'symbols' in field.get('type', {}):
            field_md.append(f"- **Symbols:** {', '.join(field['type']['symbols'])}")
        field_md.append("\n")
        return "\n".join(field_md)

    def get_avro_type(self, avro_type):
        """
        Get Avro type as a string.

        :param avro_type: Avro type as a string or dictionary.
        :return: Avro type as a string.
        """
        if isinstance(avro_type, list):
            return " | ".join([self.get_avro_type(t) for t in avro_type])
        if isinstance(avro_type, dict):
            type_name = avro_type.get('type', 'unknown')
            namespace = avro_type.get('namespace', '')
            if type_name in [r['name'] for r in self.records.get(namespace, [])]:
                return f"[{type_name}](#record-{namespace.lower()}-{type_name.lower()})"
            if type_name in [e['name'] for e in self.enums.get(namespace, [])]:
                return f"[{type_name}](#enum-{namespace.lower()}-{type_name.lower()})"
            if type_name in [f['name'] for f in self.fixeds.get(namespace, [])]:
                return f"[{type_name}](#fixed-{namespace.lower()}-{type_name.lower()})"
            return type_name
        return avro_type

def convert_avro_to_markdown(avro_schema_path, markdown_path):
    """
    Convert an Avro schema file to a README.md file.

    :param avro_schema_path: Path to the Avro schema file.
    :param markdown_path: Path to save the README.md file.
    """
    converter = AvroToMarkdownConverter(avro_schema_path, markdown_path)
    converter.convert()
