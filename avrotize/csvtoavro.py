# coding: utf-8
"""
Module to convert CSV schema to Avro schema.
"""

import json
import os
import pandas as pd

from avrotize.common import pascal

class CSVToAvroConverter:
    """
    Class to convert CSV schema to Avro schema.
    """

    def __init__(self, csv_file_path, avro_schema_path, namespace=""):
        """
        Initialize the converter with file paths and namespace.

        :param csv_file_path: Path to the CSV file.
        :param avro_schema_path: Path to save the Avro schema file.
        :param csv_schema_path: Optional path to CSV schema file.
        :param namespace: Namespace for Avro records.
        """
        self.csv_file_path = csv_file_path
        self.avro_schema_path = avro_schema_path
        self.namespace = namespace

    def convert(self):
        """
        Convert CSV schema to Avro schema and save to file.
        """
        schema = self.infer_schema()

        # Infer the name of the schema from the CSV file name
        schema_name = os.path.splitext(os.path.basename(self.csv_file_path))[0].replace(" ", "_")

        avro_schema = {}
        avro_schema["type"] = "record"
        avro_schema["name"] = schema_name
        if self.namespace:
            avro_schema["namespace"] = self.namespace
        avro_schema["fields"] = schema

        with open(self.avro_schema_path, "w", encoding="utf-8") as file:
            json.dump(avro_schema, file, indent=2)

    def infer_schema(self):
        """
        Infer the schema from CSV headers or data.
        :return: List of fields in Avro schema format.
        """
        df = pd.read_csv(self.csv_file_path)
        schema = []
        for column in df.columns:
            avro_field = {
                "name": pascal(column),
                "type": self.infer_avro_type(df[column])
            }
            if avro_field["name"] != column:
                avro_field["altnames"] = { "csv": column}
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


def convert_csv_to_avro(csv_file_path, avro_file_path, namespace=""):
    """
    Convert a CSV file to an Avro schema file.

    :param csv_file_path: Path to the CSV file.
    :param avro_file_path: Path to save the Avro schema file.
    :param namespace: Namespace for Avro records.
    """
    
    if not os.path.exists(csv_file_path):
        raise FileNotFoundError(f"CSV file not found at: {csv_file_path}")
    
    converter = CSVToAvroConverter(
        csv_file_path, avro_file_path, namespace)
    converter.convert()