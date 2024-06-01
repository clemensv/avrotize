""" Convert an Avro schema to a Parquet schema. """

import json
import sys
from typing import Dict, List
import pyarrow as pa
import pyarrow.parquet as pq

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | bool | int | None


class AvroToParquetConverter:
    """ Class to convert Avro schema to Parquet schema."""

    def __init__(self: 'AvroToParquetConverter'):
        self.named_type_cache: Dict[str, JsonNode] = {}

    def get_fullname(self, namespace: str, name: str) -> str:
        """ Get the full name of a record type."""
        return f"{namespace}.{name}" if namespace else name

    def convert_avro_to_parquet(self, avro_schema_path, avro_record_type, parquet_file_path, emit_cloudevents_columns=False):
        """ Convert an Avro schema to a Parquet schema."""
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
                (x for x in schema if x["name"] == avro_record_type or x["namespace"]+"."+x["name"] == avro_record_type), None)
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

        # Create a list to store the parquet schema
        parquet_schema = []

        # Append the parquet schema with the column names and types
        for field in fields:
            column_name = field["name"]
            column_type = self.convert_avro_type_to_parquet_type(field["type"])
            parquet_schema.append((column_name, column_type))

        if emit_cloudevents_columns:
            parquet_schema.extend([
                ("___type", pa.string()),
                ("___source", pa.string()),
                ("___id", pa.string()),
                ("___time", pa.timestamp('ns')),
                ("___subject", pa.string())
            ])

        # Create an empty table with the schema
        table = pa.Table.from_batches([], schema=pa.schema(parquet_schema))
        pq.write_table(table, parquet_file_path)

    def convert_avro_type_to_parquet_type(self, avro_type):
        """ Convert an Avro type to a Parquet type."""
        if isinstance(avro_type, list):
            # If the type is an array, then it is a union type. Look whether it's a pair of a scalar type and null:
            item_count = len(avro_type)
            if item_count == 1:
                return self.convert_avro_type_to_parquet_type(avro_type[0])
            elif item_count == 2:
                first = avro_type[0]
                second = avro_type[1]
                if isinstance(first, str) and first == "null":
                    return self.convert_avro_type_to_parquet_type(second)
                elif isinstance(second, str) and second == "null":
                    return self.convert_avro_type_to_parquet_type(first)
                else:
                    struct_fields = self.map_union_fields(avro_type)
                    return pa.struct(struct_fields)
            elif item_count > 0:
                struct_fields = self.map_union_fields(avro_type)
                return pa.struct(struct_fields)
            else:
                print(f"WARNING: Empty union type {avro_type}")
                return pa.string()
        elif isinstance(avro_type, dict):
            type_name = avro_type.get("type")
            if type_name == "array":
                return pa.list_(self.convert_avro_type_to_parquet_type(avro_type.get("items")))
            elif type_name == "map":
                return pa.map_(pa.string(), self.convert_avro_type_to_parquet_type(avro_type.get("values")))
            elif type_name == "record":
                fields = avro_type.get("fields")
                if len(fields) == 0:
                    print(
                        f"WARNING: No fields in record type {avro_type.get('name')}")
                    return pa.string()
                return pa.struct({field.get("name"): self.convert_avro_type_to_parquet_type(field.get("type")) for field in fields})
            if type_name == "enum":
                return pa.string()
            elif type_name == "fixed":
                return pa.string()
            elif type_name == "string":
                logical_type = avro_type.get("logicalType")
                if logical_type == "uuid":
                    return pa.string()
                return pa.string()
            elif type_name == "bytes":
                logical_type = avro_type.get("logicalType")
                if logical_type == "decimal":
                    return pa.decimal128(38, 18)
                return pa.binary()
            elif type_name == "long":
                logical_type = avro_type.get("logicalType")
                if logical_type in ["timestamp-millis", "timestamp-micros"]:
                    return pa.timestamp('ns')
                if logical_type in ["time-millis", "time-micros"]:
                    return pa.time64('ns')
                return pa.int64()
            elif type_name == "int":
                logical_type = avro_type.get("logicalType")
                if logical_type == "date":
                    return pa.date32()
                return pa.int32()
            else:
                return self.map_scalar_type(type_name)
        elif isinstance(avro_type, str):
            if avro_type in self.named_type_cache:
                return self.convert_avro_type_to_parquet_type(self.named_type_cache[avro_type])
            return self.map_scalar_type(avro_type)

        return pa.string()

    def cache_named_types(self, avro_type):
        """ Add an encountered type to the list of types."""
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

    def map_union_fields(self, avro_type):
        """ Map the fields of a union type to Parquet fields."""
        struct_fields = []
        for i, avro_union_type in enumerate(avro_type):
            field_type = self.convert_avro_type_to_parquet_type(
                avro_union_type)
            if isinstance(avro_union_type, str):
                if "null" == avro_union_type:
                    continue
                if avro_union_type in self.named_type_cache:
                    avro_union_type = self.named_type_cache[avro_union_type]
            if isinstance(avro_union_type, str):
                field_name = f'{avro_union_type}Value'
            elif isinstance(avro_union_type, dict):
                if "type" in avro_union_type and "array" == avro_union_type["type"]:
                    field_name = 'ArrayValue'
                elif "type" in avro_union_type and "map" == avro_union_type["type"]:
                    field_name = 'MapValue'
                elif "name" in avro_union_type:
                    field_name = f'{avro_union_type.get("name")}Value'
            else:
                field_name = f'_{i}'
            struct_fields.append(pa.field(field_name, field_type))
        return struct_fields

    def map_scalar_type(self, type_name: str):
        """ Map an Avro scalar type to a Parquet scalar type."""
        if type_name == "null":
            return pa.string()
        elif type_name == "int":
            return pa.int32()
        elif type_name == "long":
            return pa.int64()
        elif type_name == "float":
            return pa.float32()
        elif type_name == "double":
            return pa.float64()
        elif type_name == "boolean":
            return pa.bool_()
        elif type_name == "bytes":
            return pa.binary()
        elif type_name == "string":
            return pa.string()
        else:
            return pa.string()


def convert_avro_to_parquet(avro_schema_path, avro_record_type, parquet_file_path, emit_cloudevents_columns=False):
    """ Convert an Avro schema to a Parquet schema."""
    converter = AvroToParquetConverter()
    converter.convert_avro_to_parquet(
        avro_schema_path, avro_record_type, parquet_file_path, emit_cloudevents_columns)
