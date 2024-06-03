# coding: utf-8
"""
Module to convert Parquet schema to Avro schema.
"""

import json
import os
import pyarrow as pa
import pyarrow.parquet as pq

from avrotize.common import avro_name

class ParquetToAvroConverter:
    """
    Class to convert Parquet schema to Avro schema.
    """

    def __init__(self, parquet_file_path, avro_schema_path, namespace=""):
        """
        Initialize the converter with file paths and namespace.

        :param parquet_file_path: Path to the Parquet file.
        :param avro_schema_path: Path to save the Avro schema file.
        :param namespace: Namespace for Avro records.
        """
        self.parquet_file_path = parquet_file_path
        self.avro_schema_path = avro_schema_path
        self.namespace = namespace

    def convert(self):
        """
        Convert Parquet schema to Avro schema and save to file.
        """
        parquet_table = pq.read_table(self.parquet_file_path)
        schema = parquet_table.schema

        # Infer the name of the schema from the parquet file name
        schema_name = avro_name(os.path.basename(self.parquet_file_path).split(".")[0])

        # Update the avro_schema dictionary
        avro_schema = {
            "type": "record",
            "name": schema_name,
            "namespace": self.namespace,
            "fields": []
        }

        for field in schema:
            avro_field = self.convert_parquet_field_to_avro_field(field)
            avro_schema["fields"].append(avro_field)

        with open(self.avro_schema_path, "w", encoding="utf-8") as file:
            json.dump(avro_schema, file, indent=2)

    def convert_parquet_field_to_avro_field(self, field):
        """
        Convert a Parquet field to an Avro field.

        :param field: Parquet field to convert.
        :return: Avro field as a dictionary.
        """
        avro_type = self.convert_parquet_type_to_avro_type(field.type, field.name)
        avro_field = {
            "name": field.name,
            "type": avro_type
        }
        if field.metadata and b'description' in field.metadata:
            avro_field["doc"] = field.metadata[b'description'].decode("utf-8")
        return avro_field

    def convert_parquet_type_to_avro_type(self, parquet_type, field_name):
        """
        Convert a Parquet type to an Avro type.

        :param parquet_type: Parquet type to convert.
        :param field_name: Name of the field being converted.
        :return: Avro type as a string or dictionary.
        """
        if pa.types.is_int8(parquet_type):
            return "int"
        if pa.types.is_int16(parquet_type):
            return "int"
        if pa.types.is_int32(parquet_type):
            return "int"
        if pa.types.is_int64(parquet_type):
            return "long"
        if pa.types.is_uint8(parquet_type):
            return "int"
        if pa.types.is_uint16(parquet_type):
            return "int"
        if pa.types.is_uint32(parquet_type):
            return "long"
        if pa.types.is_uint64(parquet_type):
            return "long"
        if pa.types.is_float32(parquet_type):
            return "float"
        if pa.types.is_float64(parquet_type):
            return "double"
        if pa.types.is_boolean(parquet_type):
            return "boolean"
        if pa.types.is_binary(parquet_type):
            return "bytes"
        if pa.types.is_string(parquet_type):
            return "string"
        if pa.types.is_timestamp(parquet_type):
            return {"type": "long", "logicalType": "timestamp-millis"}
        if pa.types.is_date32(parquet_type):
            return {"type": "int", "logicalType": "date"}
        if pa.types.is_date64(parquet_type):
            return {"type": "long", "logicalType": "timestamp-millis"}
        if pa.types.is_list(parquet_type):
            return {
                "type": "array",
                "items": self.convert_parquet_type_to_avro_type(parquet_type.value_type, field_name)
            }
        if pa.types.is_map(parquet_type):
            return {
                "type": "map",
                "values": self.convert_parquet_type_to_avro_type(parquet_type.item_type, field_name)
            }
        if pa.types.is_struct(parquet_type):
            fields = [
                {
                    "name": nested_field.name,
                    "type": self.convert_parquet_type_to_avro_type(nested_field.type, nested_field.name)
                } for nested_field in parquet_type
            ]
            return {
                "type": "record",
                "name": f"{field_name}Type",
                "namespace": self.namespace,
                "fields": fields
            }
        if pa.types.is_decimal(parquet_type):
            return {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": parquet_type.precision,
                "scale": parquet_type.scale
            }
        return "string"

def convert_parquet_to_avro(parquet_file_path, avro_file_path, namespace=""):
    """
    Convert a Parquet file to an Avro schema file.

    :param parquet_file_path: Path to the Parquet file.
    :param avro_file_path: Path to save the Avro schema file.
    :param namespace: Namespace for Avro records.
    """
    
    if not os.path.exists(parquet_file_path):
        raise FileNotFoundError(f"Parquet file not found: {parquet_file_path}")
    
    converter = ParquetToAvroConverter(parquet_file_path, avro_file_path, namespace)
    converter.convert()

