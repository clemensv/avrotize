"""Convert Avrotize/Avro schema to RAML 1.0 Data Types."""

import json
import os
from typing import Any

import yaml

from avrotize.common import avro_name

AvroSchema = dict[str, Any] | list[Any] | str | None


class AvroToRamlConverter:
    """Converter for emitting a RAML 1.0 Library with Data Types only."""

    def __init__(self, namespace: str | None = None) -> None:
        self.namespace = namespace
        self.defined_names: set[str] = set()

    def convert_file(self, avro_schema_path: str) -> dict[str, Any]:
        with open(avro_schema_path, "r", encoding="utf-8") as avro_file:
            avro_schema = json.load(avro_file)
        return self.convert_schema(avro_schema)

    def convert_schema(self, avro_schema: AvroSchema) -> dict[str, Any]:
        types: dict[str, Any] = {}
        schemas = avro_schema if isinstance(avro_schema, list) else [avro_schema]
        for schema in schemas:
            if isinstance(schema, dict) and schema.get("type") in {"record", "enum"}:
                self.defined_names.add(schema["name"])
        for schema in schemas:
            if isinstance(schema, dict) and schema.get("type") in {"record", "enum"}:
                types[schema["name"]] = self.convert_named(schema)
        return {"types": types}

    def convert_named(self, schema: dict[str, Any]) -> dict[str, Any]:
        if schema.get("type") == "enum":
            result: dict[str, Any] = {"type": "string", "enum": schema.get("symbols", [])}
            if schema.get("doc"):
                result["description"] = schema["doc"]
            return result
        result = {"type": "object", "properties": {}}
        if schema.get("doc"):
            result["description"] = schema["doc"]
        for field in schema.get("fields", []):
            field_name = field.get("name", "field")
            field_type = field.get("type", "string")
            nullable, non_null_type = self.split_nullable(field_type)
            raml_name = f"{field_name}?" if nullable else field_name
            result["properties"][raml_name] = self.convert_type(non_null_type)
            if field.get("doc"):
                if isinstance(result["properties"][raml_name], dict):
                    result["properties"][raml_name]["description"] = field["doc"]
                else:
                    result["properties"][raml_name] = {"type": result["properties"][raml_name], "description": field["doc"]}
            if not nullable and "default" in field:
                if not isinstance(result["properties"][raml_name], dict):
                    result["properties"][raml_name] = {"type": result["properties"][raml_name]}
                result["properties"][raml_name]["default"] = field["default"]
        return result

    def convert_type(self, avro_type: AvroSchema) -> Any:
        nullable, non_null = self.split_nullable(avro_type)
        suffix = "?" if nullable else ""
        if isinstance(non_null, str):
            return f"{self.primitive_to_raml(non_null)}{suffix}"
        if isinstance(non_null, list):
            return " | ".join(self.type_name(item) for item in non_null)
        if isinstance(non_null, dict):
            logical = self.logical_to_raml(non_null)
            if logical:
                return f"{logical}{suffix}"
            type_name = non_null.get("type")
            if type_name == "array":
                return f"{self.type_name(non_null.get('items', 'string'))}[]{suffix}"
            if type_name == "map":
                return {"type": "object", "properties": {"//": self.convert_type(non_null.get("values", "string"))}}
            if type_name == "enum":
                result: dict[str, Any] = {"type": "string", "enum": non_null.get("symbols", [])}
                if non_null.get("doc"):
                    result["description"] = non_null["doc"]
                return result
            if type_name == "record":
                return self.convert_named(non_null)
            if isinstance(type_name, str):
                return f"{self.primitive_to_raml(type_name)}{suffix}"
        return "string"

    def type_name(self, avro_type: AvroSchema) -> str:
        converted = self.convert_type(avro_type)
        if isinstance(converted, str):
            return converted
        if isinstance(avro_type, dict) and avro_type.get("name"):
            return avro_type["name"]
        return "object"

    def primitive_to_raml(self, avro_type: str) -> str:
        mapping = {
            "null": "nil",
            "boolean": "boolean",
            "int": "integer",
            "long": "integer",
            "float": "number",
            "double": "number",
            "bytes": "file",
            "string": "string",
        }
        return self.strip_namespace(mapping.get(avro_type, avro_type))

    @staticmethod
    def logical_to_raml(avro_type: dict[str, Any]) -> str | None:
        logical = avro_type.get("logicalType")
        if logical == "date":
            return "date-only"
        if logical in {"time-millis", "time-micros"}:
            return "time-only"
        if logical in {"timestamp-millis", "timestamp-micros"}:
            return "datetime"
        return None

    def strip_namespace(self, name: str) -> str:
        if self.namespace and name.startswith(f"{self.namespace}."):
            return name[len(self.namespace) + 1:]
        return name.split(".")[-1] if name.split(".")[-1] in self.defined_names else name

    @staticmethod
    def split_nullable(avro_type: AvroSchema) -> tuple[bool, AvroSchema]:
        if isinstance(avro_type, list) and "null" in avro_type:
            non_null = [item for item in avro_type if item != "null"]
            if len(non_null) == 1:
                return True, non_null[0]
            return True, non_null
        return False, avro_type


def convert_avro_to_raml(avro_schema_path: str, raml_file_path: str, namespace: str | None = None) -> None:
    """Convert Avrotize/Avro schema to a RAML 1.0 Library containing Data Types only."""
    converter = AvroToRamlConverter(namespace)
    raml_doc = converter.convert_file(avro_schema_path)
    os.makedirs(os.path.dirname(os.path.abspath(raml_file_path)) or ".", exist_ok=True)
    with open(raml_file_path, "w", encoding="utf-8") as raml_file:
        raml_file.write("#%RAML 1.0 Library\n")
        yaml.safe_dump(raml_doc, raml_file, sort_keys=False, allow_unicode=True)
