# coding: utf-8
"""
Module to convert Avro schema to a GraphQL schema.
"""

import json
import os

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

    def convert(self):
        """
        Convert Avro schema to GraphQL schema and save to file.
        """
        with open(self.avro_schema_path, 'r', encoding='utf-8') as file:
            avro_schemas = json.load(file)

        for schema in avro_schemas:
            self.extract_named_types(schema)

        graphql_content = self.generate_graphql()

        with open(self.graphql_schema_path, "w", encoding="utf-8") as file:
            file.write(graphql_content)

    def extract_named_types(self, schema):
        """
        Extract all named types (records, enums, fixed) from the schema.
        """
        if isinstance(schema, dict):
            if schema['type'] == 'record':
                self.records[schema['name']] = schema
            elif schema['type'] == 'enum':
                self.enums[schema['name']] = schema
            elif schema['type'] == 'fixed':
                self.fixeds[schema['name']] = schema
            if 'fields' in schema:
                for field in schema['fields']:
                    self.extract_named_types(field['type'])
            if 'items' in schema:
                self.extract_named_types(schema['items'])
            if 'values' in schema:
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
                return f"JSON"
            if avro_type['type'] in self.records:
                return avro_type['type']
            if avro_type['type'] in self.enums:
                return avro_type['type']
            if avro_type['type'] in self.fixeds:
                return avro_type['type']
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
        return type_mapping.get(avro_type, "String")

def convert_avro_to_graphql(avro_schema_path, graphql_schema_path):
    """
    Convert an Avro schema file to a GraphQL schema file.

    :param avro_schema_path: Path to the Avro schema file.
    :param graphql_schema_path: Path to save the GraphQL schema file.
    """
    converter = AvroToGraphQLConverter(avro_schema_path, graphql_schema_path)
    converter.convert()
