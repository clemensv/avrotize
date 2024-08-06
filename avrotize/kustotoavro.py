""" Converts Kusto table schemas to Avro schema format. """

import copy
import os
import json
from typing import Any, Dict, List, Tuple
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from avrotize.common import get_tree_hash
from avrotize.constants import AVRO_VERSION

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | bool | int | None


class KustoToAvro:
    """ Converts Kusto table schemas to Avro schema format."""

    def __init__(self, kusto_uri, kusto_database, table_name: str | None, avro_namespace: str, avro_schema_path, emit_cloudevents: bool, emit_cloudevents_xregistry: bool, token_provider=None):
        """ Initializes the KustoToAvro class with the Kusto URI and database name. """
        kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(kusto_uri) if not token_provider else KustoConnectionStringBuilder.with_token_provider(kusto_uri, token_provider)
        self.client = KustoClient(kcsb)
        self.kusto_database = kusto_database
        self.single_table_name = table_name
        self.avro_namespace = avro_namespace
        self.avro_schema_path = avro_schema_path
        self.emit_xregistry = emit_cloudevents_xregistry
        self.emit_cloudevents = emit_cloudevents or emit_cloudevents_xregistry
        if self.emit_xregistry:
            if not self.avro_namespace:
                raise ValueError(
                    "The avro_namespace must be specified when emit_cloudevents_xregistry is True")
        self.generated_types: List[str] = []

    def fetch_table_schema_and_docs(self, table_name: str):
        """ Fetches the schema and docstrings for a given table."""
        query = f".show table {table_name} schema as json"
        response = self.client.execute(self.kusto_database, query)
        schema_json = response.primary_results[0][0]['Schema']
        schema = json.loads(schema_json)
        return schema

    def fold_record_types(self, base_record: dict, new_record: dict) -> Tuple[bool, dict]:
        """ Merges two record types."""
        base_fields = copy.deepcopy(base_record).get("fields", [])
        new_fields = new_record.get("fields", [])

        for field in new_fields:
            base_field = next(
                (f for f in base_fields if f["name"] == field["name"]), None)
            if not base_field:
                base_fields.append(field)
            else:
                if isinstance(base_field["type"], str) and base_field["type"] != field["type"]:
                    # If the field already exists, but the types are different, don't fold
                    return False, new_record
                elif isinstance(base_field["type"], dict) and isinstance(field["type"], dict) and base_field["type"]["type"] == field["type"]["type"]:
                    result, record_type = self.fold_record_types(
                        base_field["type"], field["type"])
                    if not result:
                        return False, new_record
                    base_field["type"] = record_type
        return True, base_record

    def python_type_to_avro_type(self, type_name: str, python_value: Any):
        """ Maps Python types to Avro types."""
        simple_types = {
            int: "int", float: "double", str: "string",
            bool: "boolean", bytes: {"type": "bytes", "logicalType": "decimal"}
        }
        if isinstance(python_value, dict):
            type_name_name = type_name.rsplit('.', 1)[-1]
            type_name_namespace = (type_name.rsplit('.', 1)[0])+"Types" if '.' in type_name else ''
            type_namespace = self.avro_namespace + ('.' if self.avro_namespace and type_name_namespace else '') + type_name_namespace
            record: Dict[str, JsonNode] = {
                "type": "record",
                "name": f"{type_name_name}",
            }
            if type_name_namespace:
                record["namespace"] = type_namespace
            fields: List[JsonNode] = []
            for key, value in python_value.items():
                altname = key
                # replace anything not in [a-zA-Z0-9_] with '_'
                key = ''.join(c if c.isalnum() else '_' for c in key)
                field = {
                    "name": key,
                    "type": self.python_type_to_avro_type(type_name+key, value)
                }
                if altname != key:
                    field["altnames"] = {
                        "kql": altname
                    }
                fields.append(field)
            record["fields"] = fields
            return record
        if isinstance(python_value, list):
            if len(python_value) > 0:
                item_types = self.consolidated_type_list(
                    type_name, python_value)
            else:
                item_types = ["string"]
            if len(item_types) == 1:
                return {"type": "array", "items": item_types[0]}
            else:
                return {"type": "array", "items": item_types}
        return simple_types.get(type(python_value), "string")

    def consolidated_type_list(self, type_name: str, python_value: list):
        """ Consolidates a list of types."""
        list_types = [self.python_type_to_avro_type(
            type_name, item) for item in python_value]

        tree_hashes = {}
        # consolidate the list types by eliminating duplicates
        for item in list_types:
            tree_hash = get_tree_hash(item)
            if tree_hash.hash_value not in tree_hashes:
                tree_hashes[tree_hash.hash_value] = item
        list_types = list(tree_hashes.values())
        unique_types = []
        prior_record = None
        for item in list_types:
            if isinstance(item, dict) and "type" in item and item["type"] == "record":
                if prior_record is None:
                    prior_record = item
                else:
                    folded, record = self.fold_record_types(prior_record, item)
                    if not folded:
                        unique_types.append(item)
                    else:
                        prior_record = record
            else:
                unique_types.append(item)
        if prior_record is not None:
            unique_types.append(prior_record)

        array_types = [item["items"] for item in unique_types if isinstance(
            item, dict) and "type" in item and item["type"] == "array"]
        map_types = [item["values"] for item in unique_types if isinstance(
            item, dict) and "type" in item and item["type"] == "map"]
        list_types = [item for item in unique_types if not isinstance(
            item, dict) or "type" not in item or item["type"] not in ["array", "map"]]

        item_types = []
        for item2 in array_types:
            if isinstance(item2, list):
                item_types.extend(item2)
            else:
                item_types.append(item2)
        if len(item_types) > 0:
            list_types.append({"type": "array", "items": item_types})

        value_types = []
        for item3 in map_types:
            if isinstance(item3, list):
                value_types.extend(item3)
            else:
                value_types.append(item3)
        if len(value_types) > 0:
            list_types.append({"type": "map", "values": value_types})

        return list_types

    def infer_dynamic_schema(self, table_name: str, column_name: str, type_column: dict | None, type_value: str | None) -> JsonNode:
        """ 
        Infers the schema for a dynamic column. If a type column is provided, it will infer the schema based 
        on constraining the result set by the type column.

        Args:
            table_name: The name of the table.
            column_name: The name of the column.
            type_column: The type column (if any)
            type_value: The value of the type column (if any)
        """
        type_column_name = type_column['Name'] if type_column else None
        query = f"{table_name}"+(f' | where {type_column_name}=="{type_value}"' if type_column_name and type_value else '') + f" | project {column_name} | take 100"
        rows = self.client.execute(self.kusto_database, query)
        values = [row[column_name] for row in rows.primary_results[0]]
        type_name = type_value if type_value else f"{table_name}.{column_name}"
        unique_types = self.consolidated_type_list(type_name, values)
        if len(unique_types) > 1:
            # Using a union of inferred types
            return unique_types
        elif len(unique_types) == 1:
            # Single type, no need for union
            return unique_types[0]
        else:
            # No values, default to string
            return "string"

    type_map : Dict[str, JsonNode] = {
            "int": "int", 
            "long": "long", 
            "string": "string",
            "real": "double", 
            "bool": "boolean",
            "datetime": {"type": "long", "logicalType": "timestamp-millis"},
            "timespan": {"type": "fixed", "size": 12, "logicalType": "duration"},
            "decimal": {"type": "fixed", "size": 16, "precision": 38, "logicalType": "decimal"},
            "dynamic": "bytes"
        }
    
    def map_kusto_type_to_avro_type(self, kusto_type, table_name, column_name, type_column: dict | None, type_value: str | None) -> JsonNode:
        """ Maps Kusto types to Avro types."""
        if kusto_type == "dynamic":
            return self.infer_dynamic_schema(table_name, column_name, type_column, type_value)
        return self.type_map.get(kusto_type, "string")

    def kusto_to_avro_schema(self, kusto_schema: dict, table_name: str) -> JsonNode:
        """ Converts a Kusto schema to Avro schema."""
        column_names = set([column['Name'].lstrip('_')
                           for column in kusto_schema['OrderedColumns']])
        type_values: List[str|None] = []
        type_column: Dict[str, JsonNode] = {}
        is_cloudevent = False
        if self.emit_cloudevents:
            is_cloudevent = 'type' in column_names and 'source' in column_names and 'data' in column_names and 'id' in column_names
            if is_cloudevent:
                type_column = next(
                    (column for column in kusto_schema['OrderedColumns'] if column['Name'].lstrip('_') == 'type'), {})
                type_sampling_query = f"{table_name} | distinct {type_column['Name']}"
                type_sampling_rows = self.client.execute(
                    self.kusto_database, type_sampling_query)
                type_values.extend([row[type_column['Name']]
                                   for row in type_sampling_rows.primary_results[0]])

        if len(type_values) == 0:
            type_values.append(None)

        schemas: List[JsonNode] = []
        for type_value in type_values:
            schema: JsonNode = {}
            fields: List[JsonNode] = []
            if type_value and isinstance(type_value, str):
                type_name_name = type_value.rsplit('.', 1)[-1]
                type_name_namespace = type_value.rsplit('.', 1)[0] if '.' in type_value else ''
                type_namespace = self.avro_namespace + ('.' if self.avro_namespace and type_name_namespace else '') + type_name_namespace
            else:
                type_name_name = table_name
                type_namespace = self.avro_namespace
                
            if is_cloudevent:
                # get just the 'data' column and infer the schema
                column = next(col for col in kusto_schema['OrderedColumns'] if col['Name'].lstrip('_') == 'data')
                data_schemas: JsonNode = self.map_kusto_type_to_avro_type(
                        column['CslType'], table_name, column['Name'], type_column, type_value)           
                if isinstance(data_schemas, dict):
                    data_schemas = [data_schemas]
                if isinstance(data_schemas, list):
                    for schema in data_schemas:
                        if not isinstance(schema, dict) or "type" not in schema or schema["type"] != "record":
                            schema = self.wrap_schema_in_root_record(schema, type_name_name, type_namespace)
                        if self.emit_xregistry:
                            ce_attribs: Dict[str, JsonNode] ={}
                            for col in [col for col in kusto_schema['OrderedColumns'] if col['Name'].lstrip('_') != 'data']:
                                ce_attribs[col['Name'].lstrip('_')] = "string"
                            if isinstance(schema, dict):
                                schema["ce_attribs"] = ce_attribs
                        self.apply_schema_attributes(schema, kusto_schema, table_name, type_value, type_namespace)
                        schemas.append(schema)
            else:
                for column in kusto_schema['OrderedColumns']:
                    avro_type = self.map_kusto_type_to_avro_type(
                        column['CslType'], table_name, column['Name'], type_column, type_value)
                    field: Dict[str, JsonNode] = {"name": column['Name'], "type": avro_type}
                    doc: JsonNode = column.get('DocString', '')
                    if doc:
                        field["doc"] = doc
                    fields.append(field)
                schema = {
                    "type": "record",
                    "name": f"{type_name_name}",
                    "fields": fields
                }
                self.apply_schema_attributes(schema, kusto_schema, table_name, type_value, type_namespace)
                schemas.append(schema)

        return schemas if len(schemas) > 1 else schemas[0]


    def wrap_schema_in_root_record(self, schema: JsonNode, type_name: str, type_namespace: str):
        """ Wraps a schema in a root record."""
        record: Dict[str, JsonNode] = {
            "type": "record",
            "name": type_name,
            "fields": [
                {
                    "name": "data",
                    "type": schema,
                    "root": True
                }
            ]
        }
        if type_namespace:
            record["namespace"] = type_namespace
        return record
    
    def apply_schema_attributes(self, schema, kusto_schema, table_name, type_value, type_namespace):
        """ Applies schema attributes to the schema."""
        if isinstance(schema, dict):
            schema["altnames"] = {
                    "kql": table_name
                }
            if self.emit_cloudevents and type_value:
                schema["ce_type"] = type_value
            if type_namespace:
                schema["namespace"] = type_namespace
            doc = kusto_schema.get('DocString', '')
            if doc:
                schema["doc"] = doc

    def make_type_names_unique(self, item_types: list):
        """ Makes the type names unique."""
        for item in item_types:
            if isinstance(item, dict) and "type" in item and item["type"] == "array":
                if "type" in item["items"] and item["items"]["type"] == "record":
                    self.make_type_names_unique([item["items"]])
                elif isinstance(item["items"], list):
                    self.make_type_names_unique(item["items"])
            if isinstance(item, dict) and "type" in item and item["type"] == "map":
                if "type" in item["values"] and item["values"]["type"] == "record":
                    self.make_type_names_unique([item["values"]])
                elif isinstance(item["values"], list):
                    self.make_type_names_unique(item["values"])
            elif isinstance(item, dict) and "type" in item and item["type"] == "record":
                namespace = item.get("namespace", '')
                type_name = base_name = item["name"]
                record_name = item["namespace"]+"." + \
                    item["name"] if namespace else item["name"]
                if record_name in self.generated_types:
                    i = 0
                    while record_name in self.generated_types:
                        i += 1
                        type_name = f"{base_name}{i}"
                        record_name = item["namespace"]+"." + \
                            type_name if namespace else type_name
                    self.generated_types.append(record_name)
                else:
                    self.generated_types.append(record_name)
                item["name"] = type_name
                for field in item.get("fields", []):
                    if "type" in field and isinstance(field["type"], dict):
                        if field["type"]["type"] in ["record", "array", "map"]:
                            self.make_type_names_unique([field["type"]])
                    elif "type" in field and isinstance(field["type"], list):
                        self.make_type_names_unique(field["type"])

    def process_all_tables(self):
        """ Processes all tables in the database and returns a union schema."""
        union_schema = []
        tables_query = ".show tables | project TableName"
        if self.single_table_name:
            tables_query += f" | where TableName == '{self.single_table_name}'"
        tables = self.client.execute(self.kusto_database, tables_query)
        for row in tables.primary_results[0]:
            table_name = row['TableName']
            print(f"Processing table: {table_name}")
            kusto_schema = self.fetch_table_schema_and_docs(table_name)
            if kusto_schema:
                avro_schema = self.kusto_to_avro_schema(
                    kusto_schema, table_name)
                if isinstance(avro_schema, list):
                    union_schema.extend(avro_schema)
                else:
                    union_schema.append(avro_schema)

        output = None
        if self.emit_xregistry:
            xregistry_messages = {}
            xregistry_schemas = {}
            groupname = self.avro_namespace
            for schema in union_schema:
                self.generated_types = []
                self.make_type_names_unique([schema])
                ce_attribs: Dict[str, JsonNode] = {}
                if "ce_attribs" in schema:
                    ce_attribs = schema.get("ce_attribs", {})
                    del schema["ce_attribs"]                
                schemaid = schema['ce_type'] if 'ce_type' in schema else f"{self.avro_namespace}.{schema['name']}"
                schema_name = schemaid.rsplit('.', 1)[-1] 
                xregistry_schemas[schemaid] = {
                    "id": schemaid,
                    "name": schema_name,
                    "format": f"Avro/{AVRO_VERSION}",
                    "defaultversionid": "1",
                    "versions": {
                        "1": {
                            "id": "1",
                            "format": f"Avro/{AVRO_VERSION}",
                            "schema": schema
                        }
                    }
                }
                xregistry_messages[schemaid] = {
                    "id": schemaid,
                    "name": schema_name,
                    "format": "CloudEvents/1.0",
                    "metadata": {
                        "type": {
                            "value": schemaid
                        },
                        "source": {
                            "value": "{source}"
                        },
                    },
                    "schemaformat": f"Avro/{AVRO_VERSION}",
                    "schemaurl": f"#/schemagroups/{groupname}/schemas/{schemaid}"
                }
                for key, value in ce_attribs.items():
                    # skip the required attributes
                    if key == "type" or key == "source" or key == "id" or key == "specversion":
                        continue
                    xregistry_messages[schemaid]["metadata"][key] = {
                        "type": value,
                        "required": True
                    }
            output = {
                "messagegroups": {
                    groupname: {
                        "id": groupname,
                        "messages": xregistry_messages
                    }
                },
                "schemagroups": {
                    groupname: {
                        "id": groupname,
                        "schemas": xregistry_schemas
                    }
                }
            }
        else:
            self.generated_types = []
            self.make_type_names_unique(union_schema)
            output = union_schema

        # create the directory if it doesn't exist
        base_dir = os.path.dirname(self.avro_schema_path)
        if base_dir and not os.path.exists(base_dir):
            os.makedirs(base_dir)
        with open(self.avro_schema_path, 'w', encoding='utf-8') as avro_file:
            json.dump(output, avro_file, indent=4)


def convert_kusto_to_avro(kusto_uri: str, kusto_database: str, table_name: str | None, avro_namespace: str, avro_schema_file: str, emit_cloudevents:bool, emit_cloudevents_xregistry: bool, token_provider=None):
    """ Converts Kusto table schemas to Avro schema format."""
    
    if not kusto_uri:
        raise ValueError("kusto_uri is required")
    if not kusto_database:
        raise ValueError("kusto_database is required")
    if not avro_namespace:
        avro_namespace = kusto_database
    
    kusto_to_avro = KustoToAvro(
        kusto_uri, kusto_database, table_name, avro_namespace, avro_schema_file,emit_cloudevents, emit_cloudevents_xregistry, token_provider=token_provider)
    return kusto_to_avro.process_all_tables()
