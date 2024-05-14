import json
import sys
from typing import List


def convert_record_to_kusto(schema: dict, emit_cloud_events_columns: bool) -> List[str]:
    """Converts an Avro record schema to a Kusto table schema."""
    # Get the name and fields of the top-level record
    table_name = schema["name"]
    fields = schema["fields"]

    # Create a StringBuilder to store the kusto statements
    kusto = []

    # Append the create table statement with the column names and types
    kusto.append(f".create table [{table_name}] (")
    columns = []
    for field in fields:
        column_name = field["name"]
        column_type = convert_avro_type_to_kusto_type(field["type"])
        columns.append(f"    [{column_name}]: {column_type}")
    if emit_cloud_events_columns:
        columns.append("    [__type]: string")
        columns.append("    [__source]: string")
        columns.append("    [__id]: string")
        columns.append("    [__time]: datetime")
        columns.append("    [__subject]: string")
    kusto.append(",\n".join(columns))
    kusto.append(");")
    kusto.append("")

    # Add the doc string as table metadata
    if "doc" in schema:
        doc_string = schema["doc"]
        kusto.append(f".alter table [{table_name}] docstring '{doc_string}';")
        kusto.append("")

    doc_string_statement = []
    for field in fields:
        column_name = field["name"]
        if "doc" in field:
            doc = field["doc"]
            doc_string_statement.append(f"   [{column_name}]: '{doc}'")
    if doc_string_statement and emit_cloud_events_columns:
        doc_string_statement.extend([
            "  [__type] : 'Event type'",
            "  [__source]: 'Context origin/source of the event'",
            "  [__id]: 'Event identifier'",
            "  [__time]: 'Event generation time'",
            "  [__subject]: 'Context subject of the event'"
        ])
    if doc_string_statement:
        kusto.append(f".alter table [{table_name}] column-docstrings (")
        kusto.append(",\n".join(doc_string_statement))
        kusto.append(");")
        kusto.append("")

    # add the JSON mapping for the table
    # .create-or-alter table dfl_data_events ingestion json mapping
    kusto.append(
        f".create-or-alter table [{table_name}] ingestion json mapping \"{table_name}_json_mapping\"")
    kusto.append("```\n[")
    for field in fields:
        json_name = column_name = field["name"]
        if 'altnames' in field:
            if 'kql' in field['altnames']:
                column_name = field['altnames']['kql']
            if 'json' in field['altnames']:
                json_name = field['altnames']['json']
        kusto.append(
            f"  {{\"column\": \"{column_name}\", \"path\": \"$.{json_name}\"}},")
    kusto.append("]\n```\n\n")
    return kusto


def convert_avro_to_kusto(avro_schema_path, avro_record_type, kusto_file_path, emit_cloud_events_columns=False):
    schema_file = avro_schema_path
    if not schema_file:
        print("Please specify the avro schema file")
        sys.exit(1)
    with open(schema_file, "r") as f:
        schema_json = f.read()

    # Parse the schema as a JSON object
    schema = json.loads(schema_json)

    if isinstance(schema, list):
        if avro_record_type:
            schema = next(
                (x for x in schema if x["name"] == avro_record_type), None)
            if schema is None:
                print(
                    f"No record type {avro_record_type} found in the Avro schema")
                sys.exit(1)
    elif not isinstance(schema, dict) or "type" not in schema or schema["type"] != "record":
        print("Expected a single Avro schema as a JSON object, or a list of schema records")
        sys.exit(1)

    if not isinstance(schema, list):
        schema = [schema]

    kusto = []
    for record in schema:
        kusto.extend(convert_record_to_kusto(
            record, emit_cloud_events_columns))

    with open(kusto_file_path, "w", encoding="utf-8") as kusto_file:
        kusto_file.write("\n".join(kusto))


def convert_avro_type_to_kusto_type(avroType):
    """Converts an Avro type to a Kusto type."""
    if isinstance(avroType, list):
        # If the type is an array, then it is a union type. Look whether it's a pair of a scalar type and null:
        itemCount = len(avroType)
        if itemCount > 2:
            return "dynamic"
        if itemCount == 1:
            return convert_avro_type_to_kusto_type(avroType[0])
        else:
            first = avroType[0]
            second = avroType[1]
            if isinstance(first, str) and first == "null":
                return convert_avro_type_to_kusto_type(second)
            elif isinstance(second, str) and second == "null":
                return convert_avro_type_to_kusto_type(first)
            else:
                return "dynamic"
    elif isinstance(avroType, dict):
        type = avroType.get("type")
        if type == "enum":
            return "string"
        elif type == "fixed":
            return "dynamic"
        elif type == "string":
            logicalType = avroType.get("logicalType")
            if logicalType == "uuid":
                return "guid"
            return "string"
        elif type == "bytes":
            logicalType = avroType.get("logicalType")
            if logicalType == "decimal":
                return "decimal"
            return "dynamic"
        elif type == "long":
            logicalType = avroType.get("logicalType")
            if logicalType in ["timestamp-millis", "timestamp-micros"]:
                return "datetime"
            if logicalType in ["time-millis", "time-micros"]:
                return "timespan"
            return "long"
        elif type == "int":
            logicalType = avroType.get("logicalType")
            if logicalType == "date":
                return "datetime"
            return "int"
        else:
            return map_scalar_type(type)
    elif isinstance(avroType, str):
        return map_scalar_type(avroType)
    return "dynamic"


def map_scalar_type(type):
    if type == "null":
        return "dynamic"
    elif type == "int":
        return "int"
    elif type == "long":
        return "long"
    elif type == "float":
        return "real"
    elif type == "double":
        return "real"
    elif type == "boolean":
        return "bool"
    elif type == "bytes":
        return "dynamic"
    elif type == "string":
        return "string"
    else:
        return "dynamic"
