# coding: utf-8
"""
Module to convert Avro schema to a comprehensive README.md.
"""

import json
import os

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
        for schema in avro_schemas:
            self.extract_named_types(schema)

        markdown_content = self.generate_markdown(schema_name)

        with open(self.markdown_path, "w", encoding="utf-8") as file:
            file.write(markdown_content)

    def extract_named_types(self, schema):
        """
        Extract all named types (records, enums, fixed) from the schema.
        """
        if isinstance(schema, dict):
            if schema['type'] == 'record':
                self.records.setdefault(schema['namespace'], []).append(schema)
            elif schema['type'] == 'enum':
                self.enums.setdefault(schema['namespace'], []).append(schema)
            elif schema['type'] == 'fixed':
                self.fixeds.setdefault(schema['namespace'], []).append(schema)
            if 'fields' in schema:
                for field in schema['fields']:
                    self.extract_named_types(field['type'])
            if 'items' in schema:
                self.extract_named_types(schema['items'])
            if 'values' in schema:
                self.extract_named_types(schema['values'])
        elif isinstance(schema, list):
            for sub_schema in schema:
                self.extract_named_types(sub_schema)

    def generate_markdown(self, schema_name):
        """
        Generate markdown content from the extracted types.

        :param schema_name: The name of the schema file.
        :return: Markdown content as a string.
        """
        markdown = []
        markdown.append(f"# {schema_name} Schemas\n")

        if self.records:
            markdown.append("\n## Records\n")
            for namespace, records in self.records.items():
                markdown.append(f"### Namespace: {namespace}\n")
                for record in records:
                    markdown.append(self.generate_record_markdown(record))

        if self.enums:
            markdown.append("\n## Enums\n")
            for namespace, enums in self.enums.items():
                markdown.append(f"### Namespace: {namespace}\n")
                for enum in enums:
                    markdown.append(self.generate_enum_markdown(enum))

        if self.fixeds:
            markdown.append("\n## Fixed Types\n")
            for namespace, fixeds in self.fixeds.items():
                markdown.append(f"### Namespace: {namespace}\n")
                for fixed in fixeds:
                    markdown.append(self.generate_fixed_markdown(fixed))

        return "\n".join(markdown)

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

    def generate_record_markdown(self, record):
        """
        Generate markdown content for a record.

        :param record: Record schema as a dictionary.
        :return: Markdown content as a string.
        """
        record_md = []
        record_md.append(f"#### {record['name']}\n")
        if 'doc' in record:
            record_md.append(f"{record['doc']}\n")
        record_md.append("- **Type:** record\n")
        record_md.append("##### Fields\n")
        for field in record['fields']:
            record_md.append(self.generate_field_markdown(field, 5))
        return "\n".join(record_md)

    def generate_enum_markdown(self, enum):
        """
        Generate markdown content for an enum.

        :param enum: Enum schema as a dictionary.
        :return: Markdown content as a string.
        """
        enum_md = []
        enum_md.append(f"#### {enum['name']}\n")
        if 'doc' in enum:
            enum_md.append(f"{enum['doc']}\n")
        enum_md.append("- **Type:** enum\n")
        enum_md.append(f"- **Symbols:** {', '.join(enum['symbols'])}\n")
        return "\n".join(enum_md)

    def generate_fixed_markdown(self, fixed):
        """
        Generate markdown content for a fixed type.

        :param fixed: Fixed schema as a dictionary.
        :return: Markdown content as a string.
        """
        fixed_md = []
        fixed_md.append(f"#### {fixed['name']}\n")
        if 'doc' in fixed:
            fixed_md.append(f"{fixed['doc']}\n")
        fixed_md.append("- **Type:** fixed\n")
        fixed_md.append(f"- **Size:** {fixed['size']}\n")
        return "\n".join(fixed_md)

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
