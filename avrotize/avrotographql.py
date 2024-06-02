# coding: utf-8
"""
Module to convert Avro schema to a GraphQL schema.
"""

import json
import os
from typing import Dict, List

from avrotize.common import get_longest_namespace_prefix

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | bool | int | None

class AvroToGraphQLConverter:
    """
    Class to convert Avro schema to GraphQL schema.
    """

    def __init__(self, avro_schema_path, graphql_schema_path):
        """
        Initialize the converter with file paths.

        :param avro_schema_path: Path to the Avro schema file.
        :param graphql_schema_path: Path to save the GraphQL schema file.
        """
        self.avro_schema_path = avro_schema_path
        self.graphql_schema_path = graphql_schema_path
        self.records = {}
        self.enums = {}
        self.fixeds = {}
        self.longest_namespace_prefix = ""

    def convert(self: 'AvroToGraphQLConverter'):
        """
        Convert Avro schema to GraphQL schema and save to file.
        """
        with open(self.avro_schema_path, 'r', encoding='utf-8') as file:
            avro_schemas: JsonNode = json.load(file)
            
        self.longest_namespace_prefix = get_longest_namespace_prefix(avro_schemas)

        if isinstance(avro_schemas, dict):
            self.extract_named_types(avro_schemas)
        elif isinstance(avro_schemas, list):
            for schema in avro_schemas:
                if isinstance(schema, dict):
                    self.extract_named_types(schema)
        else:
            raise ValueError("Expected a single Avro schema as a JSON object, or a list of schema records")

        graphql_content = self.generate_graphql()

        with open(self.graphql_schema_path, "w", encoding="utf-8") as file:
            file.write(graphql_content)
            
    def qualified_name(self, schema: Dict[str, JsonNode]) -> str:
        """
        Get the full name of a record type.
        """
        name = str(schema['name'])
        namespace = str(schema.get('namespace', ''))
        if namespace.startswith(self.longest_namespace_prefix):
            namespace = namespace[len(self.longest_namespace_prefix):].strip('.')
        return f"{namespace}_{name}" if namespace else name
    
    def extract_named_types(self, schema: Dict[str, JsonNode]):
        """
        Extract all named types (records, enums, fixed) from the schema.
        """
        if isinstance(schema, dict):
            if schema['type'] == 'record':
                self.records[self.qualified_name(schema)] = schema
            elif schema['type'] == 'enum':
                self.enums[self.qualified_name(schema)] = schema
            elif schema['type'] == 'fixed':
                self.fixeds[self.qualified_name(schema)] = schema
            if 'fields' in schema and isinstance(schema['fields'], list):
                for field in schema['fields']:
                    if isinstance(field, dict) and 'type' in field and isinstance(field['type'], dict):
                        self.extract_named_types(field['type'])
            if 'items' in schema and isinstance(schema['items'], dict):
                self.extract_named_types(schema['items'])
            if 'values' in schema and isinstance(schema['values'], dict):
                self.extract_named_types(schema['values'])
        elif isinstance(schema, list):
            for sub_schema in schema:
                self.extract_named_types(sub_schema)

    def generate_graphql(self):
        """
        Generate GraphQL content from the extracted types.

        :return: GraphQL content as a string.
        """
        graphql = []

        for record in self.records.values():
            graphql.append(self.generate_graphql_record(record))

        for enum in self.enums.values():
            graphql.append(self.generate_graphql_enum(enum))

        for fixed in self.fixeds.values():
            graphql.append(self.generate_graphql_fixed(fixed))

        return "\n".join(graphql)

    def generate_graphql_record(self, record):
        """
        Generate GraphQL content for a record.

        :param record: Record schema as a dictionary.
        :return: GraphQL content as a string.
        """
        fields = []
        for field in record['fields']:
            field_type = self.get_graphql_type(field['type'])
            if 'null' not in field['type'] and field.get('default') is not None:
                field_type = f"{field_type}!"
            fields.append(f"  {field['name']}: {field_type}")
        return f"type {record['name']} {{\n" + "\n".join(fields) + "\n}"

    def generate_graphql_enum(self, enum):
        """
        Generate GraphQL content for an enum.

        :param enum: Enum schema as a dictionary.
        :return: GraphQL content as a string.
        """
        symbols = [f"  {symbol}" for symbol in enum['symbols']]
        return f"enum {enum['name']} {{\n" + "\n".join(symbols) + "\n}"

    def generate_graphql_fixed(self, fixed):
        """
        Generate GraphQL content for a fixed type.

        :param fixed: Fixed schema as a dictionary.
        :return: GraphQL content as a string.
        """
        return f"scalar {fixed['name']}"

    def get_graphql_type(self, avro_type):
        """
        Get GraphQL type as a string.

        :param avro_type: Avro type as a string or dictionary.
        :return: GraphQL type as a string.
        """
        if isinstance(avro_type, list):
            non_null_type = next(t for t in avro_type if t != "null")
            return self.get_graphql_type(non_null_type)
        if isinstance(avro_type, dict):
            if avro_type['type'] == 'array':
                return f"[{self.get_graphql_type(avro_type['items'])}]"
            if avro_type['type'] == 'map':
                return "JSON"
            if avro_type['type'] == "record" and self.qualified_name(avro_type) in self.records:
                return self.qualified_name(avro_type)
            if avro_type['type'] == "enum" and self.qualified_name(avro_type) in self.enums:
                return self.qualified_name(avro_type)
            if avro_type['type'] == "fixed" and self.qualified_name(avro_type) in self.fixeds:
                return self.qualified_name(avro_type)
            return self.get_graphql_primitive_type(avro_type['type'])
        return self.get_graphql_primitive_type(avro_type)

    def get_graphql_primitive_type(self, avro_type):
        """
        Map Avro primitive types to GraphQL types.

        :param avro_type: Avro type as a string.
        :return: GraphQL type as a string.
        """
        
        type_mapping = {
            "string": "String",
            "bytes": "String",
            "int": "Int",
            "long": "Int",
            "float": "Float",
            "double": "Float",
            "boolean": "Boolean",
            "null": "String",
            "date": "Date",
            "timestamp-millis": "DateTime"
        }
        return type_mapping.get(avro_type, 'String')
        

def convert_avro_to_graphql(avro_schema_path, graphql_schema_path):
    """
    Convert an Avro schema file to a GraphQL schema file.

    :param avro_schema_path: Path to the Avro schema file.
    :param graphql_schema_path: Path to save the GraphQL schema file.
    """
    converter = AvroToGraphQLConverter(avro_schema_path, graphql_schema_path)
    converter.convert()
