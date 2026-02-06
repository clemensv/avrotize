""" Converts Kusto table schemas to JSON Structure schema format. """

import os
import json
from typing import Dict, List
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from avrotize.schema_inference import JsonStructureSchemaInferrer

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | bool | int | None


class KustoToJsonStructure:
    """ Converts Kusto table schemas to JSON Structure schema format."""

    def __init__(self, kusto_uri, kusto_database, table_name: str | None, base_id: str, jstruct_schema_path, emit_cloudevents: bool, emit_cloudevents_xregistry: bool, token_provider=None, sample_size: int = 100, infer_choices: bool = False, choice_depth: int = 1, infer_enums: bool = False):
        """ Initializes the KustoToJsonStructure class with the Kusto URI and database name. """
        kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(kusto_uri) if not token_provider else KustoConnectionStringBuilder.with_token_provider(kusto_uri, token_provider)
        self.client = KustoClient(kcsb)
        self.kusto_database = kusto_database
        self.single_table_name = table_name
        self.base_id = base_id if base_id else f"https://{kusto_database}.example.com/"
        self.jstruct_schema_path = jstruct_schema_path
        self.emit_xregistry = emit_cloudevents_xregistry
        self.emit_cloudevents = emit_cloudevents or emit_cloudevents_xregistry
        self.sample_size = sample_size if sample_size > 0 else 100
        self.infer_choices = infer_choices
        self.choice_depth = choice_depth
        self.infer_enums = infer_enums
        if self.emit_xregistry:
            if not self.base_id:
                raise ValueError(
                    "The base_id must be specified when emit_cloudevents_xregistry is True")
        self.generated_types: List[str] = []

    def fetch_table_schema_and_docs(self, table_name: str):
        """ Fetches the schema and docstrings for a given table."""
        query = f".show table {table_name} schema as json"
        response = self.client.execute(self.kusto_database, query)
        schema_json = response.primary_results[0][0]['Schema']
        schema = json.loads(schema_json)
        return schema

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
        query = f"{table_name}"+(f' | where {type_column_name}=="{type_value}"' if type_column_name and type_value else '') + f" | project {column_name} | take {self.sample_size}"
        rows = self.client.execute(self.kusto_database, query)
        values = [row[column_name] for row in rows.primary_results[0]]
        type_name = type_value if type_value else f"{table_name}.{column_name}"
        
        # Use the JsonStructureSchemaInferrer for consistent inference
        inferrer = JsonStructureSchemaInferrer(
            base_id=self.base_id, 
            infer_choices=self.infer_choices,
            choice_depth=self.choice_depth,
            infer_enums=self.infer_enums
        )
        return inferrer.infer_from_json_values(type_name, values)

    type_map : Dict[str, JsonNode] = {
            "int": "int32", 
            "long": "int64", 
            "string": "string",
            "real": "double", 
            "bool": "boolean",
            "datetime": "datetime",
            "timespan": "duration",
            "decimal": "decimal",
            "dynamic": "object"
        }
    
    def map_kusto_type_to_jstruct_type(self, kusto_type, table_name, column_name, type_column: dict | None, type_value: str | None) -> JsonNode:
        """ Maps Kusto types to JSON Structure types."""
        if kusto_type == "dynamic":
            return self.infer_dynamic_schema(table_name, column_name, type_column, type_value)
        return self.type_map.get(kusto_type, "string")

    def kusto_to_jstruct_schema(self, kusto_schema: dict, table_name: str) -> JsonNode:
        """ Converts a Kusto schema to JSON Structure schema."""
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
            properties: Dict[str, JsonNode] = {}
            type_name = type_value if type_value and isinstance(type_value, str) else table_name
                
            if is_cloudevent:
                # get just the 'data' column and infer the schema
                column = next(col for col in kusto_schema['OrderedColumns'] if col['Name'].lstrip('_') == 'data')
                data_schemas: JsonNode = self.map_kusto_type_to_jstruct_type(
                        column['CslType'], table_name, column['Name'], type_column, type_value)           
                if isinstance(data_schemas, dict):
                    data_schemas = [data_schemas]
                if isinstance(data_schemas, list):
                    for schema in data_schemas:
                        if not isinstance(schema, dict) or "type" not in schema or schema["type"] != "object":
                            schema = self.wrap_schema_in_root_record(schema, type_name)
                        self.apply_schema_attributes(schema, kusto_schema, table_name, type_value)
                        schemas.append(schema)
            else:
                for column in kusto_schema['OrderedColumns']:
                    jstruct_type = self.map_kusto_type_to_jstruct_type(
                        column['CslType'], table_name, column['Name'], type_column, type_value)
                    
                    # For dynamic columns, jstruct_type is a full schema object
                    # For static columns, it's a type string
                    if isinstance(jstruct_type, dict):
                        # Full schema object from inference - use directly as property schema
                        prop = jstruct_type
                    else:
                        prop = {"type": jstruct_type}
                    
                    doc: JsonNode = column.get('DocString', '')
                    if doc:
                        prop["description"] = doc
                    properties[column['Name']] = prop
                    
                schema = {
                    "type": "object",
                    "properties": properties
                }
                self.apply_schema_attributes(schema, kusto_schema, table_name, type_value)
                schemas.append(schema)

        return schemas if len(schemas) > 1 else schemas[0]


    def wrap_schema_in_root_record(self, schema: JsonNode, type_name: str):
        """ Wraps a schema in a root object."""
        # If schema is already a complete type object, use it directly in properties
        # Otherwise treat it as a type string
        if isinstance(schema, dict) and "type" in schema:
            data_prop = schema
        else:
            data_prop = {"type": schema}
        
        data_prop["root"] = True
        
        record: Dict[str, JsonNode] = {
            "type": "object",
            "properties": {
                "data": data_prop
            }
        }
        return record
    
    def apply_schema_attributes(self, schema, kusto_schema, table_name, type_value):
        """ Applies schema attributes to the schema."""
        if isinstance(schema, dict):
            type_name = type_value if type_value and isinstance(type_value, str) else table_name
            schema["$id"] = f"{self.base_id}{type_name}"
            
            # Add description from table doc string if available
            if 'DocString' in kusto_schema:
                schema["description"] = kusto_schema['DocString']

    def fetch_all_table_names(self) -> List[str]:
        """ Fetches all table names from the Kusto database."""
        query = ".show tables"
        rows = self.client.execute(self.kusto_database, query)
        table_names = [row['TableName'] for row in rows.primary_results[0]]
        return table_names

    def process_all_tables(self):
        """ Processes all tables in the Kusto database."""
        if self.single_table_name:
            table_names = [self.single_table_name]
        else:
            table_names = self.fetch_all_table_names()
        
        all_schemas = []
        for table_name in table_names:
            kusto_schema = self.fetch_table_schema_and_docs(table_name)
            jstruct_schema = self.kusto_to_jstruct_schema(kusto_schema, table_name)
            
            if isinstance(jstruct_schema, list):
                all_schemas.extend(jstruct_schema)
            else:
                all_schemas.append(jstruct_schema)
        
        # Write output
        output_dir = os.path.dirname(self.jstruct_schema_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        if self.emit_xregistry:
            # Create xRegistry manifest
            output = {
                "$schema": "https://cloudevents.io/schemas/registry",
                "specversion": "0.5-wip",
                "endpoints": {},
                "messagegroups": {},
                "schemagroups": {
                    f"{self.kusto_database}": {
                        "schemas": {schema.get("$id", f"schema_{i}"): {"format": "JSONStructure/1.0", "schema": schema} 
                                   for i, schema in enumerate(all_schemas)}
                    }
                }
            }
        else:
            # Single schema or array of schemas
            output = all_schemas if len(all_schemas) > 1 else all_schemas[0] if all_schemas else {}
        
        with open(self.jstruct_schema_path, 'w', encoding='utf-8') as jstruct_file:
            json.dump(output, jstruct_file, indent=4)


def convert_kusto_to_jstruct(kusto_uri: str, kusto_database: str, table_name: str | None, base_id: str, jstruct_schema_file: str, emit_cloudevents: bool, emit_cloudevents_xregistry: bool, token_provider=None, sample_size: int = 100, infer_choices: bool = False, choice_depth: int = 1, infer_enums: bool = False):
    """ Converts Kusto table schemas to JSON Structure schema format."""
    
    if not kusto_uri:
        raise ValueError("kusto_uri is required")
    if not kusto_database:
        raise ValueError("kusto_database is required")
    if not base_id:
        base_id = f"https://{kusto_database}.example.com/"
    
    kusto_to_jstruct = KustoToJsonStructure(
        kusto_uri, kusto_database, table_name, base_id, jstruct_schema_file, emit_cloudevents, emit_cloudevents_xregistry, token_provider=token_provider, sample_size=sample_size, infer_choices=infer_choices, choice_depth=choice_depth, infer_enums=infer_enums)
    return kusto_to_jstruct.process_all_tables()
