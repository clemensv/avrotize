"""JSON Type Definition (RFC 8927) to Avrotize Schema converter."""

from __future__ import annotations

import json
import os
from typing import Any

from avrotize.common import avro_name, avro_namespace

AvroSchema = dict[str, Any] | list[Any] | str
JtdSchema = dict[str, Any]


class JtdToAvroConverter:
    """Convert JSON Type Definition schemas to Avrotize/Avro schemas."""

    TYPE_MAPPING: dict[str, AvroSchema] = {
        "boolean": {"type": "boolean", "jtdType": "boolean"},
        "float32": {"type": "float", "jtdType": "float32"},
        "float64": {"type": "double", "jtdType": "float64"},
        "int8": {"type": "int", "jtdType": "int8"},
        "uint8": {"type": "int", "jtdType": "uint8"},
        "int16": {"type": "int", "jtdType": "int16"},
        "uint16": {"type": "int", "jtdType": "uint16"},
        "int32": {"type": "int", "jtdType": "int32"},
        "uint32": {"type": "long", "jtdType": "uint32"},
        "string": {"type": "string", "jtdType": "string"},
        "timestamp": {"type": "long", "logicalType": "timestamp-millis", "jtdType": "timestamp"},
    }

    def __init__(self, namespace: str | None = None) -> None:
        """Initialize the converter."""
        self.namespace = avro_namespace(namespace) if namespace else None
        self.definitions: dict[str, JtdSchema] = {}
        self.ref_names: dict[str, str] = {}
        self.generated: dict[str, dict[str, Any]] = {}
        self.in_progress: set[str] = set()
        self.output: list[dict[str, Any]] = []

    def convert(self, jtd_schema: JtdSchema, root_name: str = "Root") -> AvroSchema | list[AvroSchema]:
        """Convert a JTD schema dictionary to an Avro schema."""
        self.definitions = jtd_schema.get("definitions", {}) if isinstance(jtd_schema.get("definitions"), dict) else {}
        for ref_name in self.definitions:
            self.ref_names[ref_name] = self._unique_type_name(ref_name)

        root_schema = {key: value for key, value in jtd_schema.items() if key != "definitions"}
        if "ref" in root_schema and set(root_schema).issubset({"ref", "nullable", "metadata"}):
            root_ref = root_schema["ref"]
            root_type = self._convert_ref(root_ref)
            if root_ref in self.generated:
                self.generated[root_ref]["jtdRoot"] = True
            if root_schema.get("nullable"):
                return [*self.output, "null", root_type]
            return self.output if self.output else root_type

        converted = self._convert_schema(root_schema, self._unique_type_name(root_name))
        if self.output:
            if isinstance(converted, dict) and converted.get("type") in {"record", "enum"}:
                if converted not in self.output:
                    self.output.append(converted)
                return self.output
            return [*self.output, converted]
        return converted

    def _unique_type_name(self, name: str) -> str:
        candidate = avro_name(str(name).split("/")[-1].split(".")[-1] or "Type")
        if candidate == "_":
            candidate = "Type"
        used = set(self.ref_names.values())
        if candidate not in used:
            return candidate
        index = 2
        while f"{candidate}{index}" in used:
            index += 1
        return f"{candidate}{index}"

    def _full_name(self, name: str) -> str:
        return f"{self.namespace}.{name}" if self.namespace else name

    def _convert_ref(self, ref_name: str) -> str:
        if ref_name not in self.definitions:
            raise ValueError(f"JTD ref '{ref_name}' was not found in definitions")
        if ref_name not in self.ref_names:
            self.ref_names[ref_name] = self._unique_type_name(ref_name)
        if ref_name not in self.generated and ref_name not in self.in_progress:
            self._convert_definition(ref_name)
        return self._full_name(self.ref_names[ref_name])

    def _convert_definition(self, ref_name: str) -> dict[str, Any]:
        if ref_name in self.generated:
            return self.generated[ref_name]
        self.in_progress.add(ref_name)
        avro_type = self._convert_schema(self.definitions[ref_name], self.ref_names[ref_name])
        self.in_progress.remove(ref_name)
        if not isinstance(avro_type, dict) or avro_type.get("type") not in {"record", "enum"}:
            avro_type = {
                "type": "record",
                "name": self.ref_names[ref_name],
                "fields": [{"name": "value", "type": avro_type}],
                "jtdWrappedDefinition": True,
            }
            if self.namespace:
                avro_type["namespace"] = self.namespace
        self.generated[ref_name] = avro_type
        if avro_type not in self.output:
            self.output.append(avro_type)
        return avro_type

    def _convert_schema(self, schema: JtdSchema, suggested_name: str) -> AvroSchema:
        if not isinstance(schema, dict):
            raise ValueError("JTD schema nodes must be JSON objects")

        nullable = bool(schema.get("nullable"))
        base_schema = {key: value for key, value in schema.items() if key not in {"nullable", "metadata"}}
        avro_type = self._convert_non_nullable_schema(base_schema, suggested_name)
        if nullable:
            return self._nullable(avro_type)
        return avro_type

    def _convert_non_nullable_schema(self, schema: JtdSchema, suggested_name: str) -> AvroSchema:
        if not schema:
            return {"type": "string", "jtdEmptySchema": True}
        if "ref" in schema:
            return self._convert_ref(str(schema["ref"]))
        if "type" in schema:
            jtd_type = str(schema["type"])
            if jtd_type not in self.TYPE_MAPPING:
                raise ValueError(f"Unsupported JTD type '{jtd_type}'")
            return dict(self.TYPE_MAPPING[jtd_type])  # shallow copy
        if "enum" in schema:
            return self._convert_enum(schema, suggested_name)
        if "elements" in schema:
            return {"type": "array", "items": self._convert_schema(schema["elements"], f"{suggested_name}Item")}
        if "values" in schema:
            return {"type": "map", "values": self._convert_schema(schema["values"], f"{suggested_name}Value")}
        if "properties" in schema or "optionalProperties" in schema:
            return self._convert_record(schema, suggested_name)
        if "discriminator" in schema and "mapping" in schema:
            return self._convert_discriminator(schema, suggested_name)
        raise ValueError(f"Unsupported or invalid JTD schema form at {suggested_name}")

    def _convert_enum(self, schema: JtdSchema, suggested_name: str) -> dict[str, Any]:
        symbols: list[str] = []
        original_symbols: dict[str, str] = {}
        seen: set[str] = set()
        for raw_symbol in schema.get("enum", []):
            symbol = avro_name(str(raw_symbol)) or "Value"
            if symbol == "_":
                symbol = "Value"
            base_symbol = symbol
            index = 2
            while symbol in seen:
                symbol = f"{base_symbol}_{index}"
                index += 1
            seen.add(symbol)
            symbols.append(symbol)
            if symbol != raw_symbol:
                original_symbols[symbol] = str(raw_symbol)
        avro_enum: dict[str, Any] = {"type": "enum", "name": avro_name(suggested_name), "symbols": symbols}
        if self.namespace:
            avro_enum["namespace"] = self.namespace
        if original_symbols:
            avro_enum["jtdEnumSymbols"] = original_symbols
        return avro_enum

    def _convert_record(self, schema: JtdSchema, suggested_name: str) -> dict[str, Any]:
        record: dict[str, Any] = {"type": "record", "name": avro_name(suggested_name), "fields": []}
        if self.namespace:
            record["namespace"] = self.namespace
        if "additionalProperties" in schema:
            record["jtdAdditionalProperties"] = bool(schema["additionalProperties"])
        for prop_name, prop_schema in schema.get("properties", {}).items():
            record["fields"].append({"name": avro_name(prop_name), "type": self._convert_schema(prop_schema, f"{suggested_name}{avro_name(prop_name).title()}")})
        for prop_name, prop_schema in schema.get("optionalProperties", {}).items():
            record["fields"].append({
                "name": avro_name(prop_name),
                "type": self._nullable(self._convert_schema(prop_schema, f"{suggested_name}{avro_name(prop_name).title()}")),
                "default": None,
            })
        return record

    def _convert_discriminator(self, schema: JtdSchema, suggested_name: str) -> list[AvroSchema]:
        discriminator = str(schema["discriminator"])
        branches: list[AvroSchema] = []
        for tag, mapping_schema in schema.get("mapping", {}).items():
            branch_name = f"{suggested_name}{avro_name(str(tag)).title()}"
            branch = self._convert_schema(mapping_schema, branch_name)
            if not isinstance(branch, dict) or branch.get("type") != "record":
                branch = {"type": "record", "name": avro_name(branch_name), "fields": [{"name": "value", "type": branch}]}
                if self.namespace:
                    branch["namespace"] = self.namespace
            tag_symbol = avro_name(str(tag)) or "Tag"
            tag_enum = {"type": "enum", "name": avro_name(f"{branch['name']}{avro_name(discriminator).title()}"), "symbols": [tag_symbol]}
            if self.namespace:
                tag_enum["namespace"] = self.namespace
            if tag_symbol != tag:
                tag_enum["jtdEnumSymbols"] = {tag_symbol: str(tag)}
            branch = dict(branch)
            branch["jtdDiscriminator"] = discriminator
            branch["jtdMappingKey"] = str(tag)
            branch.setdefault("fields", [])
            branch["fields"] = [{"name": avro_name(discriminator), "type": tag_enum, "default": tag_symbol}, *branch["fields"]]
            branches.append(branch)
        return branches

    @staticmethod
    def _nullable(avro_type: AvroSchema) -> list[AvroSchema]:
        if isinstance(avro_type, list) and "null" in avro_type:
            return avro_type
        return ["null", avro_type]


def convert_jtd_to_avro(jtd_file_path: str, avro_schema_path: str, namespace: str | None = None) -> None:
    """Convert a JSON Type Definition file to an Avrotize Schema file."""
    with open(jtd_file_path, "r", encoding="utf-8") as jtd_file:
        jtd_schema = json.load(jtd_file)
    root_name = os.path.splitext(os.path.basename(jtd_file_path))[0] or "Root"
    converter = JtdToAvroConverter(namespace=namespace)
    avro_schema = converter.convert(jtd_schema, root_name=root_name)
    with open(avro_schema_path, "w", encoding="utf-8") as avro_file:
        json.dump(avro_schema, avro_file, indent=2)
