import json
import sys
import pyarrow as pa
import pyarrow.parquet as pq

def convert_avro_to_parquet(avro_schema_path, avro_record_type, parquet_file_path, emit_cloud_events_columns=False):
    schema_file = avro_schema_path
    if not schema_file:
        print("Please specify the avro schema file")
        sys.exit(1)
    with open(schema_file, "r") as f:
        schema_json = f.read()

    # Parse the schema as a JSON object
    schema = json.loads(schema_json)

    if isinstance(schema, list) and avro_record_type:
        schema = next((x for x in schema if x["name"] == avro_record_type or x["namespace"]+"."+x["name"] == avro_record_type), None)
        if schema is None:
            print(f"No top-level record type {avro_record_type} found in the Avro schema")
            sys.exit(1)
    elif not isinstance(schema, dict):
        print("Expected a single Avro schema as a JSON object, or a list of schema records")
        sys.exit(1)

    # Get the name and fields of the top-level record
    table_name = schema["name"]
    fields = schema["fields"]

    # Create a list to store the parquet schema
    parquet_schema = []

    # Append the parquet schema with the column names and types
    for field in fields:
        column_name = field["name"]
        column_type = convert_avro_type_to_parquet_type(field["type"])
        parquet_schema.append((column_name, column_type))

    if emit_cloud_events_columns:
        parquet_schema.extend([
            ("__type", pa.string()),
            ("__source", pa.string()),
            ("__id", pa.string()),
            ("__time", pa.timestamp('ns')),
            ("__subject", pa.string())
        ])

    # Create an empty table with the schema
    table = pa.Table.from_batches([], schema=pa.schema(parquet_schema))
    pq.write_table(table, parquet_file_path)

def convert_avro_type_to_parquet_type(avroType):
    if isinstance(avroType, list):
        # If the type is an array, then it is a union type. Look whether it's a pair of a scalar type and null:
        itemCount = len(avroType)
        if itemCount == 1:
            return convert_avro_type_to_parquet_type(avroType[0])
        elif itemCount == 2:
            first = avroType[0]
            second = avroType[1]
            if isinstance(first, str) and first == "null":
                return convert_avro_type_to_parquet_type(second)
            elif isinstance(second, str) and second == "null":
                return convert_avro_type_to_parquet_type(first)
            else:
                return pa.struct( [pa.field(f'{x}Value' if isinstance(x,str) else f'{x.get("name")}Value' if isinstance(x,dict) else f'_{i}', 
                                    convert_avro_type_to_parquet_type(x)) for i, x in enumerate(avroType)])
        elif itemCount > 0:
            return pa.struct( [pa.field(f'{x}Value' if isinstance(x,str) else f'{x.get("name")}Value' if isinstance(x,dict) else f'_{i}', 
                                    convert_avro_type_to_parquet_type(x)) for i, x in enumerate(avroType)])
        else:
            print(f"WARNING: Empty union type {avroType}")
            return pa.string()
    elif isinstance(avroType, dict):
        type = avroType.get("type")
        if type == "array":
            return pa.list_(convert_avro_type_to_parquet_type(avroType.get("items")))
        elif type == "map":
            return pa.map_(pa.string(), convert_avro_type_to_parquet_type(avroType.get("values")))
        elif type == "record":
            fields = avroType.get("fields")
            if len(fields) == 0:
                print(f"WARNING: No fields in record type {avroType.get('name')}")
                return pa.string()
            return pa.struct({field.get("name"): convert_avro_type_to_parquet_type(field.get("type")) for field in fields})
        if type == "enum":
            return pa.string()
        elif type == "fixed":
            return pa.string()
        elif type == "string":
            logicalType = avroType.get("logicalType")
            if logicalType == "uuid":
                return pa.string()
            return pa.string()
        elif type == "bytes":
            logicalType = avroType.get("logicalType")
            if logicalType == "decimal":
                return pa.decimal128(38, 18)
            return pa.string()
        elif type == "long":
            logicalType = avroType.get("logicalType")
            if logicalType in ["timestamp-millis", "timestamp-micros"]:
                return pa.timestamp('ns')
            if logicalType in ["time-millis", "time-micros"]:
                return pa.time64('ns')
            return pa.int64()
        elif type == "int":
            logicalType = avroType.get("logicalType")
            if logicalType == "date":
                return pa.date32()
            return pa.int32()
        else:
            return map_scalar_type(type)
    elif isinstance(avroType, str):
        return map_scalar_type(avroType)
    
    return pa.string()

def map_scalar_type(type):
    if type == "null":
        return pa.string()
    elif type == "int":
        return pa.int32()
    elif type == "long":
        return pa.int64()
    elif type == "float":
        return pa.float32()
    elif type == "double":
        return pa.float64()
    elif type == "boolean":
        return pa.bool_()
    elif type == "bytes":
        return pa.string()
    elif type == "string":
        return pa.string()
    else:
        return pa.string()
