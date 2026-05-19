"""Converts an Avro schema to a Kusto table schema."""

import json
import sys
from typing import Any, List
from avrotize.common import build_flat_type_dict, inline_avro_references, strip_first_doc
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder, ClientRequestProperties


class AvroToKusto:
    """Converts an Avro schema to a Kusto table schema."""

    def ___init___(self):
        """Initializes a new instance of the AvroToKusto class."""
        pass

    def convert_record_to_kusto(self, type_dict:dict, recordschema: dict, emit_cloudevents_columns: bool, emit_cloudevents_dispatch_table: bool, qualified_table_names: bool = False, namespace_override: str | None = None) -> List[str]:
        """Converts an Avro record schema to a Kusto table schema."""
        # Get the name and fields of the top-level record
        simple_name = recordschema["name"]
        record_namespace = namespace_override if namespace_override else recordschema.get("namespace")
        if qualified_table_names and record_namespace:
            table_name = f"{record_namespace}.{simple_name}"
        else:
            table_name = simple_name
        # Kusto identifiers containing dots must be quoted as ['name.with.dots'].
        if '.' in table_name:
            table_ref = f"['{table_name}']"
            table_in_query = f"['{table_name}']"
            mv_ref = f"['{table_name}Latest']"
        else:
            table_ref = f"[{table_name}]"
            table_in_query = table_name
            mv_ref = f"{table_name}Latest"
        # The JSON mapping name is a string literal, so dots are always fine.
        mapping_base = table_name
        fields = recordschema["fields"]
        
        # Create a StringBuilder to store the kusto statements
        kusto = []

        # Append the create table statement with the column names and types
        kusto.append(f".create-merge table {table_ref} (")
        columns = []
        for field in fields:
            column_name = field["name"]
            column_type = self.convert_avro_type_to_kusto_type(field["type"])
            columns.append(f"   [{column_name}]: {column_type}")
        if emit_cloudevents_columns:
            columns.append("   [___type]: string")
            columns.append("   [___source]: string")
            columns.append("   [___id]: string")
            columns.append("   [___time]: datetime")
            columns.append("   [___subject]: string")
        kusto.append(",\n".join(columns))
        kusto.append(");")
        kusto.append("")

        # Add the doc string as table metadata
        if "doc" in recordschema:
            doc_data = recordschema["doc"]
            doc_data = (doc_data[:997] + "...") if len(doc_data) > 1000 else doc_data
            doc_string = json.dumps(json.dumps({
                "description": doc_data
            }))
            kusto.append(
                f".alter table {table_ref} docstring {doc_string};")
            kusto.append("")

        doc_string_statement = []
        for field in fields:
            column_name = field["name"]
            if "doc" in field:
                doc_data = field["doc"]
                if len(doc_data) > 900:
                    doc_data = (doc_data[:897] + "...")
                doc_content = {
                    "description": doc_data
                }
                if isinstance(field["type"], list) or isinstance(field["type"], dict):
                    inline_schema = inline_avro_references(field["type"].copy(), type_dict.copy(), '')
                    if (len(json.dumps(inline_schema)) + len(doc_data)) > 900:
                        while strip_first_doc(inline_schema):
                            if (len(json.dumps(inline_schema)) + len(doc_data)) < 900:
                                break
                        if (len(json.dumps(inline_schema)) + len(doc_data)) > 900:
                            doc_content["schema"] = '{ "doc": "Schema too large to inline. Please refer to the Avro schema for more details." }'
                        else:
                            doc_content["schema"] = inline_schema
                    else:
                        doc_content["schema"] = inline_schema
                doc = json.dumps(json.dumps(doc_content))
                doc_string_statement.append(f"   [{column_name}]: {doc}")
        if doc_string_statement and emit_cloudevents_columns:
            doc_string_statement.extend([
                "   [___type] : 'Event type'",
                "   [___source]: 'Context origin/source of the event'",
                "   [___id]: 'Event identifier'",
                "   [___time]: 'Event generation time'",
                "   [___subject]: 'Context subject of the event'"
            ])
        if doc_string_statement:
            kusto.append(f".alter table {table_ref} column-docstrings (")
            kusto.append(",\n".join(doc_string_statement))
            kusto.append(");")
            kusto.append("")

        # add the JSON mapping for the table
        # .create-or-alter table dfl_data_events ingestion json mapping
        kusto.append(
            f".create-or-alter table {table_ref} ingestion json mapping \"{mapping_base}_json_flat\"")
        kusto.append("```\n[")
        if emit_cloudevents_columns:
            kusto.append("  {\"column\": \"___type\", \"path\": \"$.type\"},")
            kusto.append(
                "  {\"column\": \"___source\", \"path\": \"$.source\"},")
            kusto.append("  {\"column\": \"___id\", \"path\": \"$.id\"},")
            kusto.append("  {\"column\": \"___time\", \"path\": \"$.time\"},")
            kusto.append(
                "  {\"column\": \"___subject\", \"path\": \"$.subject\"},")
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

        if emit_cloudevents_columns:
            kusto.append(
                f".create-or-alter table {table_ref} ingestion json mapping \"{mapping_base}_json_ce_structured\"")
            kusto.append("```\n[")
            kusto.append("  {\"column\": \"___type\", \"path\": \"$.type\"},")
            kusto.append(
                "  {\"column\": \"___source\", \"path\": \"$.source\"},")
            kusto.append("  {\"column\": \"___id\", \"path\": \"$.id\"},")
            kusto.append("  {\"column\": \"___time\", \"path\": \"$.time\"},")
            kusto.append(
                "  {\"column\": \"___subject\", \"path\": \"$.subject\"},")
            for field in fields:
                json_name = column_name = field["name"]
                if 'altnames' in field:
                    if 'kql' in field['altnames']:
                        column_name = field['altnames']['kql']
                    if 'json' in field['altnames']:
                        json_name = field['altnames']['json']
                kusto.append(
                    f"  {{\"column\": \"{column_name}\", \"path\": \"$.data.{json_name}\"}},")
            kusto.append("]\n```\n\n")

        if emit_cloudevents_columns:
            kusto.append(
                f".drop materialized-view {mv_ref} ifexists;")
            kusto.append("")
            kusto.append(
                f".create materialized-view with (backfill=true) {mv_ref} on table {table_in_query} {{")
            kusto.append(
                f"    {table_in_query} | summarize arg_max(___time, *) by ___type, ___source, ___subject")
            kusto.append("}")
            kusto.append("")

        if emit_cloudevents_dispatch_table:
            event_type = record_namespace + "." + \
                simple_name if record_namespace else simple_name

            query = f"_cloudevents_dispatch | where (specversion == '1.0' and type == '{event_type}') | " + \
                    "project"                        
            for field in fields:
                column_name = field["name"]
                if "altnames" in field and "kql" in field["altnames"]:
                    column_name = field["altnames"]["kql"]
                column_type = self.convert_avro_type_to_kusto_type(
                    field["type"])
                query += f"['{column_name}'] = to{column_type}(data.['{column_name}']),"
            query += "___type = type,___source = source,___id = ['id'],___time = ['time'],___subject = subject"

            # build an update policy for the table that gets triggered by updates to the dispatch table and extracts the event
            kusto.append(f".alter table {table_ref} policy update")
            kusto.append("```")
            kusto.append("[{")
            kusto.append("  \"IsEnabled\": true,")
            kusto.append("  \"Source\": \"_cloudevents_dispatch\",")
            kusto.append(
                f"  \"Query\": \"{query}\",")
            kusto.append("  \"IsTransactional\": false,")
            kusto.append("  \"PropagateIngestionProperties\": true,")
            kusto.append("}]")
            kusto.append("```\n")

        return kusto

    def convert_avro_to_kusto_script(self, avro_schema_path, avro_record_type, emit_cloudevents_columns=False, emit_cloudevents_dispatch_table=False, qualified_table_names: bool = False, namespace_override: str | None = None) -> str:
        """Converts an Avro schema to a Kusto table schema."""
        if emit_cloudevents_dispatch_table:
            emit_cloudevents_columns = True
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

        kusto_script = []

        if emit_cloudevents_dispatch_table:
            kusto_script.append(
                ".create-merge table [_cloudevents_dispatch] (")
            kusto_script.append("    [specversion]: string,")
            kusto_script.append("    [type]: string,")
            kusto_script.append("    [source]: string,")
            kusto_script.append("    [id]: string,")
            kusto_script.append("    [time]: datetime,")
            kusto_script.append("    [subject]: string,")
            kusto_script.append("    [datacontenttype]: string,")
            kusto_script.append("    [dataschema]: string,")
            kusto_script.append("    [data]: dynamic")
            kusto_script.append(");\n\n")
            kusto_script.append(
                ".create-or-alter table [_cloudevents_dispatch] ingestion json mapping \"_cloudevents_dispatch_json\"")
            kusto_script.append("```\n[")
            kusto_script.append(
                "  {\"column\": \"specversion\", \"path\": \"$.specversion\"},")
            kusto_script.append(
                "  {\"column\": \"type\", \"path\": \"$.type\"},")
            kusto_script.append(
                "  {\"column\": \"source\", \"path\": \"$.source\"},")
            kusto_script.append("  {\"column\": \"id\", \"path\": \"$.id\"},")
            kusto_script.append(
                "  {\"column\": \"time\", \"path\": \"$.time\"},")
            kusto_script.append(
                "  {\"column\": \"subject\", \"path\": \"$.subject\"},")
            kusto_script.append(
                "  {\"column\": \"datacontenttype\", \"path\": \"$.datacontenttype\"},")
            kusto_script.append(
                "  {\"column\": \"dataschema\", \"path\": \"$.dataschema\"},")
            kusto_script.append(
                "  {\"column\": \"data\", \"path\": \"$.data\"}")
            kusto_script.append("]\n```\n\n")

        type_dict =  build_flat_type_dict(schema)
        for record in schema:
            if not isinstance(record, dict) or "type" not in record or record["type"] != "record":
                continue
            kusto_script.extend(self.convert_record_to_kusto(type_dict,
                record, emit_cloudevents_columns, emit_cloudevents_dispatch_table, qualified_table_names, namespace_override))
        return "\n".join(kusto_script)

    def convert_avro_to_kusto_file(self, avro_schema_path, avro_record_type, kusto_file_path, emit_cloudevents_columns=False, emit_cloudevents_dispatch_table=False, qualified_table_names: bool = False, namespace_override: str | None = None):
        """Converts an Avro schema to a Kusto table schema."""
        script = self.convert_avro_to_kusto_script(
            avro_schema_path, avro_record_type, emit_cloudevents_columns, emit_cloudevents_dispatch_table, qualified_table_names, namespace_override)
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


def convert_avro_to_kusto_file(avro_schema_path, avro_record_type, kusto_file_path, emit_cloudevents_columns=False, emit_cloudevents_dispatch_table=False, qualified_table_names: bool = False, namespace: str | None = None):
    """Converts an Avro schema to a Kusto table schema."""
    avro_to_kusto = AvroToKusto()
    avro_to_kusto.convert_avro_to_kusto_file(
        avro_schema_path, avro_record_type, kusto_file_path, emit_cloudevents_columns, emit_cloudevents_dispatch_table, qualified_table_names, namespace)


def convert_avro_to_kusto_db(avro_schema_path, avro_record_type, kusto_uri, kusto_database, emit_cloudevents_columns=False, emit_cloudevents_dispatch_table=False, token_provider=None, qualified_table_names: bool = False, namespace: str | None = None):
    """Converts an Avro schema to a Kusto table schema."""
    avro_to_kusto = AvroToKusto()
    script = avro_to_kusto.convert_avro_to_kusto_script(
        avro_schema_path, avro_record_type, emit_cloudevents_columns, emit_cloudevents_dispatch_table, qualified_table_names, namespace)
    kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(
        kusto_uri) if not token_provider else KustoConnectionStringBuilder.with_token_provider(kusto_uri, token_provider)
    client = KustoClient(kcsb)
    for statement in script.split("\n\n"):
        if statement.strip():
            try:
                client.execute_mgmt(kusto_database, statement)
            except Exception as e:
                print(e)
                sys.exit(1)

def convert_avro_to_kusto(avro_schema_path, avro_record_type, kusto_file_path, kusto_uri, kusto_database, emit_cloudevents_columns=False, emit_cloudevents_dispatch_table=False, token_provider=None, qualified_table_names: bool = False, namespace: str | None = None):
    """Converts an Avro schema to a Kusto table schema."""
    if not kusto_uri and not kusto_database:
        convert_avro_to_kusto_file(
            avro_schema_path, avro_record_type, kusto_file_path, emit_cloudevents_columns, emit_cloudevents_dispatch_table, qualified_table_names, namespace)
    else:
        convert_avro_to_kusto_db(
            avro_schema_path, avro_record_type, kusto_uri, kusto_database, emit_cloudevents_columns, emit_cloudevents_dispatch_table, token_provider, qualified_table_names, namespace)