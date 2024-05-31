import json
import sys
from typing import Dict, List, cast, Optional
from datapackage import Package, Resource

from avrotize.common import get_longest_namespace_prefix


JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | bool | int | None

class AvroToDataPackageConverter:
    """Class to convert Avro schema to Data Package."""

    def __init__(self: 'AvroToDataPackageConverter'):
        self.named_type_cache: Dict[str, JsonNode] = {}

    def get_fullname(self, namespace: str, name: str) -> str:
        """Get the full name of a record type."""
        return f"{namespace}.{name}" if namespace else name

    def convert_avro_to_datapackage(self, avro_schema_path: str, avro_record_type: Optional[str], datapackage_path: str):
        """Convert an Avro schema to a Data Package."""
        with open(avro_schema_path, "r", encoding="utf-8") as f:
            schema_json = f.read()

        # Parse the schema as a JSON object
        schema = json.loads(schema_json)
        self.cache_named_types(schema)

        if isinstance(schema, list):
            if avro_record_type:
                schema = next(
                    (x for x in schema if x["name"] == avro_record_type or x["namespace"] + "." + x["name"] == avro_record_type), None)
                if schema is None:
                    print(f"No top-level record type {avro_record_type} found in the Avro schema")
                    sys.exit(1)
            schemas_to_convert = schema
        elif isinstance(schema, dict):
            schemas_to_convert = [schema]
        else:
            print("Expected a single Avro schema as a JSON object, or a list of schema records")
            sys.exit(1)

        longest_namespace_prefix = get_longest_namespace_prefix(schema)
        self.create_datapackage_for_schemas(schemas_to_convert, datapackage_path, longest_namespace_prefix)

    def create_datapackage_for_schemas(self, schemas: List[Dict[str, JsonNode]], datapackage_path: str, namespace_prefix: str):
        """Create a Data Package for given schemas."""
        package = Package()
        data_package_resources = []

        for schema in schemas:
            name = str(schema["name"])
            namespace = str(schema.get("namespace", ""))
            if namespace.startswith(namespace_prefix):
                namespace = namespace[len(namespace_prefix):].strip(".")
            table_name = f"{namespace}_{name}" if namespace else name
            fields = cast(List[Dict[str, JsonNode]], schema["fields"])

            # Create the Data Package schema
            resource_schema: Dict[str, List[JsonNode]] = {
                "fields": []
            }

            for field in fields:
                column_name = field["name"]
                column_type = self.convert_avro_type_to_datapackage_type(field["type"])
                field_schema = {"name": column_name, "type": column_type}
                if "doc" in field:
                    field_schema["description"] = field["doc"]
                resource_schema["fields"].append(field_schema)

            resource = {
                "name": table_name,
                "schema": resource_schema
            }
            data_package_resources.append(resource)

        # Add resources to the Data Package
        for resource in data_package_resources:
            package.add_resource(resource)

        # Save the Data Package
        package.descriptor["name"] = namespace_prefix
        package.commit()

        with open(datapackage_path, "w", encoding="utf-8") as f:
            f.write(json.dumps(package.descriptor, indent=2))

    def convert_avro_type_to_datapackage_type(self, avro_type: JsonNode) -> str:
        """Convert an Avro type to a Data Package type."""
        if isinstance(avro_type, list):
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

    def cache_named_types(self, avro_type: JsonNode):
        """Add an encountered type to the list of types."""
        if isinstance(avro_type, list):
            for item in avro_type:
                self.cache_named_types(item)
        if isinstance(avro_type, dict) and avro_type.get("name"):
            self.named_type_cache[self.get_fullname(str(avro_type.get(
                "namespace")), str(avro_type.get("name")))] = avro_type
            if "fields" in avro_type:
                for field in cast(List[Dict[str,JsonNode]],avro_type.get("fields")):
                    if "type" in field:
                        self.cache_named_types(field.get("type"))

    def map_scalar_type(self, type_name: str) -> str:
        """Map an Avro scalar type to a Data Package scalar type."""
        scalar_type_mapping = {
            "null": "string",
            "int": "integer",
            "long": "integer",
            "float": "number",
            "double": "number",
            "boolean": "boolean",
            "bytes": "string",
            "string": "string"
        }
        return scalar_type_mapping.get(type_name, "string")

def convert_avro_to_datapackage(avro_schema_path: str, avro_record_type: Optional[str], datapackage_path: str):
    """Convert an Avro schema to a Data Package."""
    converter = AvroToDataPackageConverter()
    converter.convert_avro_to_datapackage(avro_schema_path, avro_record_type, datapackage_path)

# Example usage:
# convert_avro_to_datapackage("schema.avsc", "MyRecord", "datapackage.json")
