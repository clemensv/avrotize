# coding: utf-8
"""
Module to convert CSV schema to Avro schema.
"""

import re
import json
import os
import pandas as pd

class CSVToAvroConverter:
    """
    Class to convert CSV schema to Avro schema.
    """

    def __init__(self, csv_file_path, avro_schema_path, csv_schema_path=None, namespace=""):
        """
        Initialize the converter with file paths and namespace.

        :param csv_file_path: Path to the CSV file.
        :param avro_schema_path: Path to save the Avro schema file.
        :param csv_schema_path: Optional path to CSV schema file.
        :param namespace: Namespace for Avro records.
        """
        self.csv_file_path = csv_file_path
        self.avro_schema_path = avro_schema_path
        self.csv_schema_path = csv_schema_path
        self.namespace = namespace

    def convert(self):
        """
        Convert CSV schema to Avro schema and save to file.
        """
        if self.csv_schema_path:
            schema = self.read_csv_schema()
        else:
            schema = self.infer_schema()

        # Infer the name of the schema from the CSV file name
        schema_name = os.path.basename(self.csv_file_path).split(".")[0]

        avro_schema = {
            "type": "record",
            "name": schema_name,
            "namespace": self.namespace,
            "fields": schema
        }

        with open(self.avro_schema_path, "w", encoding="utf-8") as file:
            json.dump(avro_schema, file, indent=2)

    def read_csv_schema(self):
        """
        Read CSV schema from the provided JSON schema file.
        :return: List of fields in Avro schema format.
        """
        with open(self.csv_schema_path, 'r', encoding='utf-8') as schema_file:
            csv_schema = json.load(schema_file)

        avro_schema = []
        df = pd.read_csv(self.csv_file_path)

        field_names = set(df.columns)
        processed_fields = set()

        if 'patternFields' in csv_schema:
            pattern_fields = csv_schema['patternFields']
            for pattern, field_schema in pattern_fields.items():
                regex = re.compile(pattern)
                matching_fields = [
                    field for field in field_names if regex.match(field)]
                for field in matching_fields:
                    if field not in processed_fields:
                        avro_field = self.create_avro_field(
                            field, field_schema)
                        avro_schema.append(avro_field)
                        processed_fields.add(field)

        for field in csv_schema.get('fields', []):
            if field['name'] not in processed_fields:
                avro_field = self.create_avro_field(field['name'], field)
                avro_schema.append(avro_field)
                processed_fields.add(field['name'])

        return avro_schema

    def create_avro_field(self, name, field_schema):
        """
        Create an Avro field from a CSV field schema.
        :param name: Field name.
        :param field_schema: CSV field schema.
        :return: Avro field schema.
        """
        avro_field = {
            "name": name,
            "type": self.map_csv_type_to_avro(field_schema['type'])
        }

        if field_schema['type'] == 'string' and 'format' in field_schema:
            avro_field['type'] = self.handle_string_format(
                field_schema['format'])

        if field_schema.get('nullable', False):
            avro_field['type'] = ["null", avro_field['type']]
            avro_field['default'] = None

        if 'enum' in field_schema:
            avro_field['type'] = {
                "type": avro_field['type'], "symbols": field_schema['enum']}

        if 'default' in field_schema:
            avro_field['default'] = field_schema['default']

        return avro_field

    def infer_schema(self):
        """
        Infer the schema from CSV headers or data.
        :return: List of fields in Avro schema format.
        """
        df = pd.read_csv(self.csv_file_path)
        schema = []
        for column in df.columns:
            avro_field = {
                "name": column,
                "type": self.infer_avro_type(df[column])
            }
            schema.append(avro_field)
        return schema

    def infer_avro_type(self, series):
        """
        Infer Avro type from pandas series.
        :param series: Pandas series to infer type from.
        :return: Avro type as string.
        """
        if pd.api.types.is_integer_dtype(series):
            return "int"
        if pd.api.types.is_float_dtype(series):
            return "double"
        if pd.api.types.is_bool_dtype(series):
            return "boolean"
        if pd.api.types.is_datetime64_any_dtype(series):
            return {"type": "long", "logicalType": "timestamp-millis"}
        if pd.api.types.is_object_dtype(series):
            return "string"
        return "string"

    def map_csv_type_to_avro(self, csv_type):
        """
        Map CSV type to Avro type.
        :param csv_type: CSV type as string.
        :return: Avro type as string.
        """
        type_mapping = {
            "string": "string",
            "number": "double",
            "integer": "int",
            "boolean": "boolean",
            "date": {"type": "int", "logicalType": "date"},
            "timestamp": {"type": "long", "logicalType": "timestamp-millis"}
        }
        return type_mapping.get(csv_type.lower(), "string")

    def handle_string_format(self, format_type):
        """
        Handle string format types.
        :param format_type: Format type as string.
        :return: Avro type as string or dict.
        """
        format_mapping = {
            "email": "string",
            "uri": "string",
            "uuid": "string",
            "ipv4": "string",
            "ipv6": "string",
            "hostname": "string",
            "datetime": {"type": "long", "logicalType": "timestamp-millis"}
        }
        return format_mapping.get(format_type.lower(), "string")


def convert_csv_to_avro(csv_file_path, avro_file_path, csv_schema_path=None, namespace=""):
    """
    Convert a CSV file to an Avro schema file.

    :param csv_file_path: Path to the CSV file.
    :param avro_file_path: Path to save the Avro schema file.
    :param csv_schema_path: Optional path to CSV schema file.
    :param namespace: Namespace for Avro records.
    """
    converter = CSVToAvroConverter(
        csv_file_path, avro_file_path, csv_schema_path, namespace)
    converter.convert()
