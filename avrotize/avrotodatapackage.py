import json
import sys
from typing import Dict, List
from datapackage import Package, Resource

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | bool | int | None


class AvroToDataPackageConverter:
    """Class to convert Avro schema to Data Package."""

    def __init__(self: 'AvroToDataPackageConverter'):
        self.named_type_cache: Dict[str, JsonNode] = {}

    def get_fullname(self, namespace: str, name: str) -> str:
        """Get the full name of a record type."""
        return f"{namespace}.{name}" if namespace else name

    def convert_avro_to_datapackage(self, avro_schema_path, avro_record_type, datapackage_path):
        """Convert an Avro schema to a Data Package."""
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
                (x for x in schema if x["name"] == avro_record_type or x["namespace"] + "." + x["name"] == avro_record_type), None)
            if schema is None:
                print(f"No top-level record type {avro_record_type} found in the Avro schema")
                sys.exit(1)
        elif isinstance(schema, dict) and "type" in schema and isinstance(schema["type"], list):
            # Handle the top-level union type
            for sub_type in schema["type"]:
                self.create_datapackage_for_type(sub_type, datapackage_path)
            return
        elif not isinstance(schema, dict):
            print("Expected a single Avro schema as a JSON object, or a list of schema records")
            sys.exit(1)

        # Create the Data Package for the main schema
        self.create_datapackage_for_type(schema, datapackage_path)

    def create_datapackage_for_type(self, schema, datapackage_path):
        """Create a Data Package for a given schema."""
        # Get the name and fields of the top-level record
        table_name = schema["name"]
        fields = schema["fields"]

        # Create a list to store the Data Package schema
        data_package_resources = []

        # Append the Data Package schema with the column names and types
        resource_schema = {
            "fields": []
        }

        for field in fields:
            column_name = field["name"]
            column_type = self.convert_avro_type_to_datapackage_type(field["type"])
            resource_schema["fields"].append({"name": column_name, "type": column_type})

        data_package_resources.append({
            "name": table_name,
            "path": f"{table_name}.csv",
            "schema": resource_schema
        })

        # Create the Data Package
        package = Package()
        for resource in data_package_resources:
            package.infer(resource["path"])
            resource_obj = Resource(resource)
            package.add_resource(resource_obj)

        # Save the Data Package
        package.descriptor["name"] = table_name
        package.descriptor["resources"] = data_package_resources
        package.commit()
        package.save(f"{datapackage_path}_{table_name}.json")

    def convert_avro_type_to_datapackage_type(self, avro_type):
        """Convert an Avro type to a Data Package type."""
        if isinstance(avro_type, list):
            # Handle union types
            item_count = len(avro_type)
            if item_count == 1:
                return self.convert_avro_type_to_datapackage_type(avro_type[0])
            elif item_count == 2:
                first = avro_type[0]
                second = avro_type[1]
                if isinstance(first, str) and first == "null":
                    return self.convert_avro_type_to_datapackage_type(second)
                elif isinstance(second, str) and second == "null":
                    return self.convert_avro_type_to_datapackage_type(first)
            print(f"WARNING: Complex union types are not fully supported: {avro_type}")
            return "string"
        elif isinstance(avro_type, dict):
            type_name = avro_type.get("type")
            if type_name == "array":
                return "array"
            elif type_name == "map":
                return "object"
            elif type_name == "record":
                return "object"
            elif type_name == "enum":
                return "string"
            elif type_name == "fixed":
                return "string"
            elif type_name == "string":
                return "string"
            elif type_name == "bytes":
                return "string"
            elif type_name == "long":
                return "integer"
            elif type_name == "int":
                return "integer"
            elif type_name == "float":
                return "number"
            elif type_name == "double":
                return "number"
            elif type_name == "boolean":
                return "boolean"
            else:
                return "string"
        elif isinstance(avro_type, str):
            if avro_type in self.named_type_cache:
                return self.convert_avro_type_to_datapackage_type(self.named_type_cache[avro_type])
            return self.map_scalar_type(avro_type)

        return "string"

    def cache_named_types(self, avro_type):
        """Add an encountered type to the list of types."""
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

    def map_scalar_type(self, type_name: str):
        """Map an Avro scalar type to a Data Package scalar type."""
        if type_name == "null":
            return "string"
        elif type_name == "int":
            return "integer"
        elif type_name == "long":
            return "integer"
        elif type_name == "float":
            return "number"
        elif type_name == "double":
            return "number"
        elif type_name == "boolean":
            return "boolean"
        elif type_name == "bytes":
            return "string"
        elif type_name == "string":
            return "string"
        else:
            return "string"


def convert_avro_to_datapackage(avro_schema_path, avro_record_type, datapackage_path):
    """Convert an Avro schema to a Data Package."""
    converter = AvroToDataPackageConverter()
    converter.convert_avro_to_datapackage(avro_schema_path, avro_record_type, datapackage_path)


# Example usage:
# convert_avro_to_datapackage("schema.avsc", "MyRecord", "datapackage.json")
