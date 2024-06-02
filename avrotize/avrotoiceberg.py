"""Convert an Avro schema to an Iceberg schema."""

import json
import sys
from typing import Dict, List
import pyarrow as pa
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import (
    BooleanType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    StringType,
    BinaryType,
    DateType,
    TimestampType,
    DecimalType,
    FixedType,
    ListType,
    MapType,
    StructType
)
from pyiceberg.io.pyarrow import PyArrowFileIO, schema_to_pyarrow

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | bool | int | None


class AvroToIcebergConverter:
    """Class to convert Avro schema to Iceberg schema."""

    def __init__(self: 'AvroToIcebergConverter'):
        self.named_type_cache: Dict[str, JsonNode] = {}
        self.id_counter = 0

    def get_id(self) -> int:
        """Get a unique ID for a record type."""
        self.id_counter += 1
        return self.id_counter
    
    def get_fullname(self, namespace: str, name: str) -> str:
        """Get the full name of a record type."""
        return f"{namespace}.{name}" if namespace else name

    def convert_avro_to_iceberg(self, avro_schema_path: str, avro_record_type: str, output_path: str, emit_cloudevents_columns: bool=False):
        """Convert an Avro schema to an Iceberg schema."""
        schema_file = avro_schema_path
        if not schema_file:
            print("Please specify the avro schema file")
            sys.exit(1)
        with open(schema_file, "r", encoding="utf-8") as f:
            schema_json = f.read()

        # Parse the schema as a JSON object
        schema = json.loads(schema_json)
        self.cache_named_types(schema)

        if isinstance(schema, list) and avro_record_type:
            schema = next(
                (x for x in schema if x["name"] == avro_record_type or x["namespace"] + "." + x["name"] == avro_record_type), None)
            if schema is None:
                print(
                    f"No top-level record type {avro_record_type} found in the Avro schema")
                sys.exit(1)
        elif not isinstance(schema, dict):
            print(
                "Expected a single Avro schema as a JSON object, or a list of schema records")
            sys.exit(1)

        # Get the name and fields of the top-level record
        table_name = schema["name"]
        fields = schema["fields"]

        # Create a list to store the iceberg schema
        iceberg_fields: List[NestedField] = []

        # Append the iceberg schema with the column names and types
        for i, field in enumerate(fields):
            column_name = field["name"]
            column_type = self.convert_avro_type_to_iceberg_type(field["type"])
            iceberg_fields.append(
                NestedField(field_id=self.get_id(), name=column_name, type=column_type))

        if emit_cloudevents_columns:
            iceberg_fields.extend([
                NestedField(field_id=self.get_id(),
                      name="___type", type=StringType()),
                NestedField(field_id=self.get_id(),
                      name="___source", type=StringType()),
                NestedField(field_id=self.get_id(),
                      name="___id", type=StringType()),
                NestedField(field_id=self.get_id(),
                      name="___time", type=TimestampType()),
                NestedField(field_id=self.get_id(),
                      name="___subject", type=StringType())
            ])

        iceberg_schema = Schema(*iceberg_fields)
        arrow_schema = schema_to_pyarrow(iceberg_schema)
        print(f"Iceberg schema created: {arrow_schema}")

        # Write to Iceberg table (for demonstration, using local file system)
        file_io = PyArrowFileIO()
        output_file = file_io.new_output("file://"+output_path)
        with output_file.create(overwrite=True) as f:
            pa.output_stream(f).write(arrow_schema.serialize().to_pybytes())

    def convert_avro_type_to_iceberg_type(self, avro_type):
        """Convert an Avro type to an Iceberg type."""
        if isinstance(avro_type, list):
            item_count = len(avro_type)
            if item_count == 1:
                return self.convert_avro_type_to_iceberg_type(avro_type[0])
            elif item_count == 2:
                first, second = avro_type[0], avro_type[1]
                if first == "null":
                    return self.convert_avro_type_to_iceberg_type(second)
                elif second == "null":
                    return self.convert_avro_type_to_iceberg_type(first)
                else:
                    return StructType(fields=[NestedField(field_id=self.get_id(), name=f'field_{i}', type=self.convert_avro_type_to_iceberg_type(t)) for i, t in enumerate(avro_type)])
            elif item_count > 0:
                return StructType(fields=[NestedField(field_id=self.get_id(), name=f'field_{i}', type=self.convert_avro_type_to_iceberg_type(t)) for i, t in enumerate(avro_type)])
            else:
                print(f"WARNING: Empty union type {avro_type}")
                return StringType()
        elif isinstance(avro_type, dict):
            type_name = avro_type.get("type")
            if type_name == "array":
                return ListType(element_id=self.get_id(), element=self.convert_avro_type_to_iceberg_type(avro_type.get("items")))
            elif type_name == "map":
                return MapType(key_id=self.get_id(), key_type=StringType(), value_id=self.get_id(), value_type=self.convert_avro_type_to_iceberg_type(avro_type.get("values")))
            elif type_name == "record":
                fields = avro_type.get("fields")
                return StructType(fields=[NestedField(field_id=self.get_id(), name=field["name"], type=self.convert_avro_type_to_iceberg_type(field["type"])) for i, field in enumerate(fields)])
            if type_name == "enum":
                return StringType()
            elif type_name == "fixed":
                return FixedType(avro_type.get("size"))
            elif type_name == "string":
                logical_type = avro_type.get("logicalType")
                if logical_type == "uuid":
                    return StringType()
                return StringType()
            elif type_name == "bytes":
                logical_type = avro_type.get("logicalType")
                if logical_type == "decimal":
                    return DecimalType(38, 18)
                return BinaryType()
            elif type_name == "long":
                logical_type = avro_type.get("logicalType")
                if logical_type in ["timestamp-millis", "timestamp-micros"]:
                    return TimestampType()
                if logical_type in ["time-millis", "time-micros"]:
                    return LongType()
                return LongType()
            elif type_name == "int":
                logical_type = avro_type.get("logicalType")
                if logical_type == "date":
                    return DateType()
                return IntegerType()
            else:
                return self.map_iceberg_scalar_type(type_name)
        elif isinstance(avro_type, str):
            if avro_type in self.named_type_cache:
                return self.convert_avro_type_to_iceberg_type(self.named_type_cache[avro_type])
            return self.map_iceberg_scalar_type(avro_type)

        return StringType()

    def cache_named_types(self, avro_type):
        """Add an encountered type to the list of types."""
        if isinstance(avro_type, list):
            for item in avro_type:
                self.cache_named_types(item)
        if isinstance(avro_type, dict) and avro_type.get("name"):
            self.named_type_cache[self.get_fullname(avro_type.get(
                "namespace"), avro_type.get("name"))] = avro_type
            if "fields" in avro_type:
                for field in avro_type.get("fields"):
                    if "type" in field:
                        self.cache_named_types(field.get("type"))

    def map_iceberg_scalar_type(self, type_name: str):
        """Map an Avro scalar type to an Iceberg scalar type."""
        if type_name == "null":
            return StringType()
        elif type_name == "int":
            return IntegerType()
        elif type_name == "long":
            return LongType()
        elif type_name == "float":
            return FloatType()
        elif type_name == "double":
            return DoubleType()
        elif type_name == "boolean":
            return BooleanType()
        elif type_name == "bytes":
            return BinaryType()
        elif type_name == "string":
            return StringType()
        else:
            return StringType()


def convert_avro_to_iceberg(avro_schema_path, avro_record_type, output_path, emit_cloudevents_columns=False):
    """Convert an Avro schema to an Iceberg schema."""
    converter = AvroToIcebergConverter()
    converter.convert_avro_to_iceberg(
        avro_schema_path, avro_record_type, output_path, emit_cloudevents_columns)
