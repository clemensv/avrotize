# coding: utf-8
"""
Module to convert Avro schema to CSV schema.
"""

import json
import os

class AvroToCSVSchemaConverter:
    """
    Class to convert Avro schema to CSV schema.
    """

    def __init__(self, avro_schema_path, csv_schema_path):
        """
        Initialize the converter with file paths.

        :param avro_schema_path: Path to the Avro schema file.
        :param csv_schema_path: Path to save the CSV schema file.
        """
        self.avro_schema_path = avro_schema_path
        self.csv_schema_path = csv_schema_path

    def convert(self):
        """
        Convert Avro schema to CSV schema and save to file.
        """
        with open(self.avro_schema_path, 'r', encoding='utf-8') as file:
            avro_schema = json.load(file)

        csv_schema = self.convert_avro_to_csv_schema(avro_schema)

        with open(self.csv_schema_path, 'w', encoding='utf-8') as file:
            json.dump(csv_schema, file, indent=2)

    def convert_avro_to_csv_schema(self, avro_schema):
        """
        Convert an Avro schema to a CSV schema.

        :param avro_schema: Avro schema as a dictionary.
        :return: CSV schema as a dictionary.
        """
        csv_schema = {
            "fields": []
        }

        for field in avro_schema['fields']:
            csv_field = self.convert_avro_field_to_csv_field(field)
            csv_schema['fields'].append(csv_field)

        return csv_schema

    def convert_avro_field_to_csv_field(self, field):
        """
        Convert an Avro field to a CSV field.

        :param field: Avro field as a dictionary.
        :return: CSV field as a dictionary.
        """
        csv_field = {
            "name": field['name'],
            "type": self.convert_avro_type_to_csv_type(field['type'])
        }

        if isinstance(field['type'], dict) and field['type'].get('logicalType') == 'timestamp-millis':
            csv_field['type'] = 'string'
            csv_field['format'] = 'datetime'

        if field.get('default') is not None:
            csv_field['default'] = field['default']

        if isinstance(field['type'], list) and "null" in field['type']:
            csv_field['nullable'] = True

        if 'enum' in field['type']:
            csv_field['enum'] = field['type']['symbols']

        return csv_field

    def convert_avro_type_to_csv_type(self, avro_type):
        """
        Convert an Avro type to a CSV type.

        :param avro_type: Avro type as a string or dictionary.
        :return: CSV type as a string.
        """
        if isinstance(avro_type, list):
            avro_type = [t for t in avro_type if t != "null"][0]

        type_mapping = {
            "string": "string",
            "int": "integer",
            "long": "integer",
            "float": "number",
            "double": "number",
            "boolean": "boolean",
            "bytes": "string",
            "record": "object",
            "array": "array",
            "map": "object",
            "enum": "string"
        }

        if isinstance(avro_type, dict):
            return type_mapping.get(avro_type['type'], "string")

        return type_mapping.get(avro_type, "string")

def convert_avro_to_csv_schema(avro_schema_path, csv_schema_path):
    """
    Convert an Avro schema file to a CSV schema file.

    :param avro_schema_path: Path to the Avro schema file.
    :param csv_schema_path: Path to save the CSV schema file.
    """
    
    if not os.path.exists(avro_schema_path):
        raise FileNotFoundError(f"Avro schema file not found: {avro_schema_path}")
    
    converter = AvroToCSVSchemaConverter(avro_schema_path, csv_schema_path)
    converter.convert()
