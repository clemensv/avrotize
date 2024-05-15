"""Converts an Avro schema to a Kusto table schema."""

import json
import sys
from typing import Any, List
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder, ClientRequestProperties


class AvroToKusto:
    """Converts an Avro schema to a Kusto table schema."""

    def __init__(self):
        """Initializes a new instance of the AvroToKusto class."""
        pass

    def convert_record_to_kusto(self, schema: dict, emit_cloud_events_columns: bool) -> List[str]:
        """Converts an Avro record schema to a Kusto table schema."""
        # Get the name and fields of the top-level record
        table_name = schema["name"]
        fields = schema["fields"]

        # Create a StringBuilder to store the kusto statements
        kusto = []

        # Append the create table statement with the column names and types
        kusto.append(f".create-merge table [{table_name}] (")
        columns = []
        for field in fields:
            column_name = field["name"]
            column_type = self.convert_avro_type_to_kusto_type(field["type"])
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
            doc_string = json.dumps(json.dumps({
                "description": schema["doc"]
            }))
            kusto.append(
                f".alter table [{table_name}] docstring {doc_string};")
            kusto.append("")

        doc_string_statement = []
        for field in fields:
            column_name = field["name"]
            if "doc" in field:
                doc_content = {
                    "description": field["doc"]
                }
                if isinstance(field["type"], list) or isinstance(field["type"], dict):
                    doc_content["type"] = field["type"]
                doc = json.dumps(json.dumps(doc_content))
                doc_string_statement.append(f"   [{column_name}]: {doc}")
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
            f".create-or-alter table [{table_name}] ingestion json mapping \"{table_name}_json_flat\"")
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

    def convert_avro_to_kusto_script(self, avro_schema_path, avro_record_type, emit_cloud_events_columns=False) -> str:
        """Converts an Avro schema to a Kusto table schema."""
        schema_file = avro_schema_path
        if not schema_file:
            print("Please specify the avro schema file")
            sys.exit(1)
        with open(schema_file, "r", encoding="utf-8") as f:
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
            print(
                "Expected a single Avro schema as a JSON object, or a list of schema records")
            sys.exit(1)

        if not isinstance(schema, list):
            schema = [schema]

        kusto = []
        for record in schema:
            kusto.extend(self.convert_record_to_kusto(
                record, emit_cloud_events_columns))
        return "\n".join(kusto)


    def convert_avro_to_kusto_file(self, avro_schema_path, avro_record_type, kusto_file_path, emit_cloud_events_columns=False):
        """Converts an Avro schema to a Kusto table schema."""
        script = self.convert_avro_to_kusto_script(avro_schema_path, avro_record_type, emit_cloud_events_columns)
        with open(kusto_file_path, "w", encoding="utf-8") as kusto_file:
            kusto_file.write(script)

    def convert_avro_type_to_kusto_type(self, avro_type: str | dict | list):
        """Converts an Avro type to a Kusto type."""
        if isinstance(avro_type, list):
            # If the type is an array, then it is a union type. Look whether it's a pair of a scalar type and null:
            itemCount = len(avro_type)
            if itemCount > 2:
                return "dynamic"
            if itemCount == 1:
                return self.convert_avro_type_to_kusto_type(avro_type[0])
            else:
                first = avro_type[0]
                second = avro_type[1]
                if isinstance(first, str) and first == "null":
                    return self.convert_avro_type_to_kusto_type(second)
                elif isinstance(second, str) and second == "null":
                    return self.convert_avro_type_to_kusto_type(first)
                else:
                    return "dynamic"
        elif isinstance(avro_type, dict):
            type_value = avro_type.get("type")
            if type_value == "enum":
                return "string"
            elif type_value == "fixed":
                return "dynamic"
            elif type_value == "string":
                logical_type = avro_type.get("logicalType")
                if logical_type == "uuid":
                    return "guid"
                return "string"
            elif type_value == "bytes":
                logical_type = avro_type.get("logicalType")
                if logical_type == "decimal":
                    return "decimal"
                return "dynamic"
            elif type_value == "long":
                logical_type = avro_type.get("logicalType")
                if logical_type in ["timestamp-millis", "timestamp-micros"]:
                    return "datetime"
                if logical_type in ["time-millis", "time-micros"]:
                    return "timespan"
                return "long"
            elif type_value == "int":
                logical_type = avro_type.get("logicalType")
                if logical_type == "date":
                    return "datetime"
                return "int"
            else:
                return self.map_scalar_type(type_value)
        elif isinstance(avro_type, str):
            return self.map_scalar_type(avro_type)

    def map_scalar_type(self, type_value: Any):
        """Maps an Avro scalar type to a Kusto scalar type."""
        if type_value == "null":
            return "dynamic"
        elif type_value == "int":
            return "int"
        elif type_value == "long":
            return "long"
        elif type_value == "float":
            return "real"
        elif type_value == "double":
            return "real"
        elif type_value == "boolean":
            return "bool"
        elif type_value == "bytes":
            return "dynamic"
        elif type_value == "string":
            return "string"
        else:
            return "dynamic"


def convert_avro_to_kusto_file(avro_schema_path, avro_record_type, kusto_file_path, emit_cloud_events_columns=False):
    """Converts an Avro schema to a Kusto table schema."""
    avro_to_kusto = AvroToKusto()
    avro_to_kusto.convert_avro_to_kusto_file(
        avro_schema_path, avro_record_type, kusto_file_path, emit_cloud_events_columns)

def convert_avro_to_kusto_db(avro_schema_path, avro_record_type, kusto_uri, kusto_database, emit_cloud_events_columns=False, token_provider=None):
    """Converts an Avro schema to a Kusto table schema."""
    avro_to_kusto = AvroToKusto()
    script = avro_to_kusto.convert_avro_to_kusto_script(
        avro_schema_path, avro_record_type, emit_cloud_events_columns)
    kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(kusto_uri) if not token_provider else KustoConnectionStringBuilder.with_token_provider(kusto_uri, token_provider)
    client = KustoClient(kcsb)
    for statement in script.split("\n\n"):
        if statement.strip():
            try:
                client.execute_mgmt(kusto_database, statement)
            except Exception as e:
                print(e)
                sys.exit(1)      