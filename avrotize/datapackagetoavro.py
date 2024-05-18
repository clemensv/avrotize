"""Convert a Data Package to an Avro schema."""

import json
import sys
from typing import Dict, List
from datapackage import Package

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | bool | int | None


class DataPackageToAvroConverter:
    """Class to convert Data Package to Avro schema."""

    def convert_datapackage_to_avro(self, datapackage_path, avro_schema_path):
        """Convert a Data Package to an Avro schema."""
        package = Package(datapackage_path)
        resources = package.resources

        avro_schemas = []

        for resource in resources:
            table_name = resource.descriptor['name']
            fields = resource.descriptor['schema']['fields']

            avro_fields = []
            for field in fields:
                avro_field = {
                    "name": field["name"],
                    "type": self.convert_datapackage_type_to_avro_type(field["type"])
                }
                avro_fields.append(avro_field)

            avro_schema = {
                "type": "record",
                "name": table_name,
                "fields": avro_fields
            }
            avro_schemas.append(avro_schema)

        # If there's only one schema, write it directly
        if len(avro_schemas) == 1:
            avro_schema = avro_schemas[0]
        else:
            # If there are multiple schemas, create a union
            avro_schema = avro_schemas

        with open(avro_schema_path, "w", encoding="utf-8") as f:
            json.dump(avro_schema, f, indent=2)

    def convert_datapackage_type_to_avro_type(self, datapackage_type):
        """Convert a Data Package type to an Avro type."""
        if datapackage_type == "string":
            return "string"
        elif datapackage_type == "number":
            return "double"
        elif datapackage_type == "integer":
            return "int"
        elif datapackage_type == "boolean":
            return "boolean"
        elif datapackage_type == "array":
            return {"type": "array", "items": "string"}
        elif datapackage_type == "object":
            return {"type": "map", "values": "string"}
        else:
            print(f"WARNING: Unsupported data package type: {datapackage_type}")
            return "string"


def convert_datapackage_to_avro(datapackage_path, avro_schema_path):
    """Convert a Data Package to an Avro schema."""
    converter = DataPackageToAvroConverter()
    converter.convert_datapackage_to_avro(datapackage_path, avro_schema_path)


# Example usage:
# convert_datapackage_to_avro("datapackage.json", "schema.avsc")
