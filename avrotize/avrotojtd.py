"""Avrotize Schema to JSON Type Definition (RFC 8927) converter."""

from __future__ import annotations

import json
from typing import Any

AvroSchema = dict[str, Any] | list[Any] | str
JtdSchema = dict[str, Any]


class AvroToJtdConverter:
    """Convert Avrotize/Avro schemas to JSON Type Definition schemas."""

    PRIMITIVE_TO_JTD: dict[str, str] = {
        "boolean": "boolean",
        "int": "int32",
        "long": "uint32",
        "float": "float32",
        "double": "float64",
        "string": "string",
    }

    def __init__(self, record_type: str | None = None) -> None:
        """Initialize the converter."""
        self.record_type = record_type
        self.named_types: dict[str, dict[str, Any]] = {}
        self.simple_names: dict[str, str] = {}
        self.definitions: dict[str, JtdSchema] = {}
        self.converting: set[str] = set()

    def convert(self, avro_schema: AvroSchema) -> JtdSchema:
        """Convert an Avro schema object to a JTD schema dictionary."""
        root = self._register_top_level(avro_schema)
        if self.record_type:
            root = self._find_named_type(self.record_type)
        root_jtd = self._convert_type(root)
        if self.definitions:
            if isinstance(root, dict) and root.get("type") in {"record", "enum"}:
                root_name = self._type_key(root)
                self.definitions[root_name] = root_jtd
                return {"definitions": self.definitions, "ref": root_name}
            root_jtd = dict(root_jtd)
            root_jtd["definitions"] = self.definitions
        return root_jtd

    def _register_top_level(self, avro_schema: AvroSchema) -> AvroSchema:
        if isinstance(avro_schema, list):
            if self._is_discriminator_union(avro_schema):
                for item in avro_schema:
                    self._register_named(item)
                return avro_schema
            if not all(isinstance(item, dict) and item.get("type") in {"record", "enum"} for item in avro_schema):
                return avro_schema
            root: AvroSchema | None = None
            for item in avro_schema:
                self._register_named(item)
                root = item
                if item.get("jtdRoot"):
                    root = item
                    break
            if root is None:
                raise ValueError("Avro schema list is empty")
            return root
        if isinstance(avro_schema, dict) and avro_schema.get("type") in {"record", "enum"}:
            self._register_named(avro_schema)
        return avro_schema

    def _register_named(self, schema: dict[str, Any]) -> None:
        key = self._type_key(schema)
        self.named_types[key] = schema
        self.simple_names[schema["name"]] = key

    def _find_named_type(self, record_type: str) -> dict[str, Any]:
        key = record_type if record_type in self.named_types else self.simple_names.get(record_type)
        if key and key in self.named_types:
            return self.named_types[key]
        raise ValueError(f"Avro record type '{record_type}' not found")

    @staticmethod
    def _type_key(schema: dict[str, Any]) -> str:
        namespace = schema.get("namespace")
        return f"{namespace}.{schema['name']}" if namespace else schema["name"]

    def _convert_type(self, avro_type: AvroSchema) -> JtdSchema:
        nullable, non_null = self._strip_nullable(avro_type)
        if self._is_discriminator_union(non_null):
            jtd = self._convert_discriminator(non_null)  # type: ignore[arg-type]
        else:
            jtd = self._convert_non_nullable(non_null)
        if nullable:
            jtd = dict(jtd)
            jtd["nullable"] = True
        return jtd

    def _convert_non_nullable(self, avro_type: AvroSchema) -> JtdSchema:
        if isinstance(avro_type, str):
            if avro_type == "null":
                return {"nullable": True}
            if avro_type in self.PRIMITIVE_TO_JTD:
                return {"type": self.PRIMITIVE_TO_JTD[avro_type]}
            ref_key = self.simple_names.get(avro_type, avro_type)
            if ref_key in self.named_types:
                self._ensure_definition(ref_key)
                return {"ref": ref_key}
            return {"type": "string"}
        if isinstance(avro_type, list):
            return self._convert_union(avro_type)
        if not isinstance(avro_type, dict):
            return {"type": "string"}

        if avro_type.get("jtdType"):
            return {"type": avro_type["jtdType"]}
        if avro_type.get("logicalType") == "timestamp-millis" and avro_type.get("type") == "long":
            return {"type": "timestamp"}

        schema_type = avro_type.get("type")
        if isinstance(schema_type, dict) or isinstance(schema_type, list):
            return self._convert_type(schema_type)
        if schema_type in self.PRIMITIVE_TO_JTD:
            return {"type": self.PRIMITIVE_TO_JTD[schema_type]}
        if schema_type == "record":
            return self._convert_record(avro_type)
        if schema_type == "enum":
            return self._convert_enum(avro_type)
        if schema_type == "array":
            return {"elements": self._convert_type(avro_type.get("items", "string"))}
        if schema_type == "map":
            return {"values": self._convert_type(avro_type.get("values", "string"))}
        if isinstance(schema_type, str) and schema_type in self.named_types:
            self._ensure_definition(schema_type)
            return {"ref": schema_type}
        return {"type": "string"}

    def _convert_record(self, schema: dict[str, Any]) -> JtdSchema:
        key = self._type_key(schema)
        if key in self.converting:
            return {"ref": key}
        self.converting.add(key)
        properties: dict[str, JtdSchema] = {}
        optional_properties: dict[str, JtdSchema] = {}
        for field in schema.get("fields", []):
            if field.get("name") == schema.get("jtdDiscriminator"):
                continue
            field_nullable, field_type = self._strip_nullable(field.get("type", "string"))
            converted = self._convert_type(field_type)
            if field_nullable and field.get("default") is None:
                optional_properties[field["name"]] = converted
            else:
                if field_nullable:
                    converted = dict(converted)
                    converted["nullable"] = True
                properties[field["name"]] = converted
        self.converting.remove(key)
        result: JtdSchema = {}
        if properties:
            result["properties"] = properties
        if optional_properties:
            result["optionalProperties"] = optional_properties
        if "jtdAdditionalProperties" in schema:
            result["additionalProperties"] = bool(schema["jtdAdditionalProperties"])
        return result

    @staticmethod
    def _convert_enum(schema: dict[str, Any]) -> JtdSchema:
        original_symbols = schema.get("jtdEnumSymbols", {})
        return {"enum": [original_symbols.get(symbol, symbol) for symbol in schema.get("symbols", [])]}

    def _convert_union(self, union: list[Any]) -> JtdSchema:
        non_null = [item for item in union if item != "null"]
        if len(non_null) == 1:
            return self._convert_type(non_null[0])
        if self._is_discriminator_union(non_null):
            return self._convert_discriminator(non_null)
        return {"metadata": {"avrotize-union": [self._convert_type(item) for item in non_null]}}

    def _convert_discriminator(self, union: list[Any]) -> JtdSchema:
        discriminator = union[0].get("jtdDiscriminator")
        mapping: dict[str, JtdSchema] = {}
        for branch in union:
            tag = branch.get("jtdMappingKey")
            if tag is None:
                tag = branch.get("name")
            mapping[str(tag)] = self._convert_record(branch)
        return {"discriminator": discriminator, "mapping": mapping}

    @staticmethod
    def _strip_nullable(avro_type: AvroSchema) -> tuple[bool, AvroSchema]:
        if isinstance(avro_type, list) and "null" in avro_type:
            non_null = [item for item in avro_type if item != "null"]
            if len(non_null) == 1:
                return True, non_null[0]
            return True, non_null
        return False, avro_type

    @staticmethod
    def _is_discriminator_union(avro_type: AvroSchema) -> bool:
        return (
            isinstance(avro_type, list)
            and bool(avro_type)
            and all(isinstance(item, dict) and item.get("type") == "record" and item.get("jtdDiscriminator") for item in avro_type)
            and len({item.get("jtdDiscriminator") for item in avro_type if isinstance(item, dict)}) == 1
        )

    def _ensure_definition(self, key: str) -> None:
        if key in self.definitions:
            return
        if key not in self.named_types:
            return
        if key in self.converting:
            self.definitions[key] = {"ref": key}
            return
        self.converting.add(key)
        self.definitions[key] = self._convert_non_nullable(self.named_types[key])
        self.converting.remove(key)


def convert_avro_to_jtd(avro_schema_path: str, jtd_file_path: str, record_type: str | None = None) -> None:
    """Convert an Avrotize Schema file to a JSON Type Definition file."""
    with open(avro_schema_path, "r", encoding="utf-8") as avro_file:
        avro_schema = json.load(avro_file)
    converter = AvroToJtdConverter(record_type=record_type)
    jtd_schema = converter.convert(avro_schema)
    with open(jtd_file_path, "w", encoding="utf-8") as jtd_file:
        json.dump(jtd_schema, jtd_file, indent=2)
