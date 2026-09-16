"""Convert RAML 1.0 Data Types to Avrotize/Avro schema."""

import json
import os
from typing import Any

import yaml

from avrotize.common import avro_name, avro_namespace, unique_name
from avrotize.dependency_resolver import sort_and_inline_dependencies

AvroSchema = dict[str, Any] | list[Any] | str | None


class _RamlLoader(yaml.SafeLoader):
    """YAML loader that keeps unsupported RAML tags such as !include inert."""


def _unknown_tag(loader: _RamlLoader, tag_suffix: str, node: yaml.Node) -> Any:
    if isinstance(node, yaml.ScalarNode):
        return loader.construct_scalar(node)
    if isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node)
    if isinstance(node, yaml.MappingNode):
        return loader.construct_mapping(node)
    return None


_RamlLoader.add_multi_constructor("!", _unknown_tag)


class RamlToAvroConverter:
    """Converter for the RAML 1.0 Data Types subset."""

    scalar_types: dict[str, AvroSchema] = {
        "string": "string",
        "number": "double",
        "integer": "long",
        "boolean": "boolean",
        "date-only": {"type": "int", "logicalType": "date"},
        "time-only": {"type": "int", "logicalType": "time-millis"},
        "datetime-only": {"type": "long", "logicalType": "timestamp-millis"},
        "datetime": {"type": "long", "logicalType": "timestamp-millis"},
        "file": "bytes",
        "nil": "null",
        "null": "null",
        "any": "string",
    }

    def __init__(self, namespace: str | None = None) -> None:
        self.namespace = avro_namespace(namespace) if namespace else None
        self.known_types: set[str] = set()
        self.type_name_map: dict[str, str] = {}

    def load_raml(self, raml_file_path: str) -> dict[str, Any]:
        with open(raml_file_path, "r", encoding="utf-8") as raml_file:
            data = yaml.load(raml_file, Loader=_RamlLoader)
        return data or {}

    def convert_file(self, raml_file_path: str) -> AvroSchema:
        data = self.load_raml(raml_file_path)
        if not isinstance(data, dict):
            raise ValueError("RAML document must be a mapping")
        types = data.get("types", {}) or {}
        if not isinstance(types, dict):
            raise ValueError("RAML types section must be a mapping")
        used_type_names: set[str] = set()
        self.type_name_map = {}
        for name in types:
            raw_name = str(name).rstrip("?")
            self.type_name_map[raw_name] = unique_name(avro_name(raw_name), used_type_names)
        self.known_types = set(self.type_name_map.values())
        avro_types: list[dict[str, Any]] = []
        for type_name, definition in types.items():
            avro_type = self.convert_named_type(str(type_name), definition)
            if isinstance(avro_type, dict) and avro_type.get("type") in {"record", "enum"}:
                avro_types.append(avro_type)
        return sort_and_inline_dependencies(avro_types) if len(avro_types) > 1 else (avro_types[0] if avro_types else [])

    def convert_named_type(self, type_name: str, definition: Any) -> AvroSchema:
        raw_name = type_name.rstrip("?")
        name = self.type_name_map.get(raw_name, avro_name(raw_name))
        definition = self.normalize_definition(definition)
        if "enum" in definition:
            return self.convert_enum(name, definition)
        raml_type = definition.get("type", "object" if "properties" in definition else "string")
        if self.is_map_definition(definition):
            return self.convert_record(name, {**definition, "type": "object"})
        if raml_type == "object" or "properties" in definition:
            return self.convert_record(name, definition)
        avro_type = self.convert_type(raml_type, name)
        if isinstance(avro_type, dict) and avro_type.get("type") in {"record", "enum"}:
            avro_type.setdefault("name", name)
            avro_type.setdefault("namespace", self.namespace)
            return avro_type
        return {"type": "record", "name": name, **({"namespace": self.namespace} if self.namespace else {}), "fields": [{"name": "value", "type": avro_type}]}

    def normalize_definition(self, definition: Any) -> dict[str, Any]:
        if definition is None:
            return {"type": "string"}
        if isinstance(definition, str):
            return {"type": definition}
        if isinstance(definition, list):
            return {"type": " | ".join(str(item) for item in definition)}
        if isinstance(definition, dict):
            return dict(definition)
        return {"type": str(definition)}

    def convert_record(self, name: str, definition: dict[str, Any]) -> dict[str, Any]:
        record: dict[str, Any] = {"type": "record", "name": avro_name(name), "fields": []}
        if self.namespace:
            record["namespace"] = self.namespace
        if definition.get("description"):
            record["doc"] = str(definition["description"])
        deps: list[str] = []
        used_field_names: set[str] = set()
        properties = definition.get("properties", {}) or {}
        if self.is_map_definition(definition):
            map_type = self.map_value_type(definition)
            record["fields"].append({"name": unique_name("additionalProperties", used_field_names), "type": {"type": "map", "values": map_type}})
            self.collect_dependencies(map_type, deps)
        elif isinstance(properties, dict):
            for prop_name, prop_def in properties.items():
                if self.is_out_of_scope_key(str(prop_name)):
                    continue
                field = self.convert_property(avro_name(name), str(prop_name), prop_def, used_field_names)
                self.collect_dependencies(field["type"], deps)
                record["fields"].append(field)
        if deps:
            record["dependencies"] = sorted(set(deps))
        return record

    @staticmethod
    def is_out_of_scope_key(name: str) -> bool:
        return name.startswith("/") or name.lower() in {"get", "post", "put", "patch", "delete", "head", "options", "trace"}

    def convert_property(self, record_name: str, prop_name: str, prop_def: Any, used_field_names: set[str] | None = None) -> dict[str, Any]:
        optional_by_name = prop_name.endswith("?")
        clean_name = prop_name.rstrip("?")
        definition = self.normalize_definition(prop_def)
        optional = optional_by_name or definition.get("required") is False
        field_name = unique_name(avro_name(clean_name), used_field_names) if used_field_names is not None else avro_name(clean_name)
        field_type = self.convert_type(definition.get("type", "object" if "properties" in definition else "string"), f"{record_name}_{field_name}", definition)
        if optional and not self.is_nullable(field_type):
            field_type = ["null", field_type]
        field: dict[str, Any] = {"name": field_name, "type": field_type}
        if definition.get("description"):
            field["doc"] = str(definition["description"])
        if optional:
            field["default"] = None
        elif "default" in definition:
            field["default"] = definition["default"]
        return field

    def convert_enum(self, name: str, definition: dict[str, Any]) -> dict[str, Any]:
        used_symbols: set[str] = set()
        enum_type: dict[str, Any] = {"type": "enum", "name": avro_name(name), "symbols": [unique_name(avro_name(str(symbol)), used_symbols) for symbol in definition.get("enum", [])]}
        if self.namespace:
            enum_type["namespace"] = self.namespace
        if definition.get("description"):
            enum_type["doc"] = str(definition["description"])
        return enum_type

    def convert_type(self, raml_type: Any, context_name: str, definition: dict[str, Any] | None = None) -> AvroSchema:
        definition = definition or {}
        if "enum" in definition:
            return self.convert_enum(context_name, definition)
        if self.is_map_definition(definition):
            return {"type": "map", "values": self.map_value_type(definition)}
        if isinstance(raml_type, list):
            return self.union([self.convert_type(item, context_name, definition) for item in raml_type])
        if isinstance(raml_type, dict):
            return self.convert_type(raml_type.get("type", "object"), context_name, raml_type)
        type_text = str(raml_type).strip() if raml_type is not None else "string"
        if type_text.endswith("?") and " | " not in type_text and "|" not in type_text:
            return self.union(["null", self.convert_type(type_text[:-1], context_name, definition)])
        if "|" in type_text:
            return self.union([self.convert_type(part.strip(), context_name, definition) for part in type_text.split("|")])
        if type_text.endswith("[]"):
            return {"type": "array", "items": self.convert_type(type_text[:-2].strip(), context_name, definition)}
        if type_text == "array":
            return {"type": "array", "items": self.convert_type(definition.get("items", "string"), context_name, {})}
        if type_text == "object" or "properties" in definition:
            nested = self.convert_record(context_name, {**definition, "type": "object"})
            nested.pop("dependencies", None)
            return nested
        if type_text in self.scalar_types:
            value = self.scalar_types[type_text]
            return dict(value) if isinstance(value, dict) else value
        return self.reference_name(type_text)

    def reference_name(self, type_name: str) -> str:
        raw_name = type_name.rstrip("?")
        name = self.type_name_map.get(raw_name, avro_name(raw_name))
        if self.namespace and name in self.known_types:
            return f"{self.namespace}.{name}"
        return name

    @staticmethod
    def union(items: list[AvroSchema]) -> list[AvroSchema]:
        result: list[AvroSchema] = []
        seen: set[str] = set()
        for item in items:
            if isinstance(item, list):
                nested = item
            else:
                nested = [item]
            for nested_item in nested:
                key = json.dumps(nested_item, sort_keys=True) if isinstance(nested_item, (dict, list)) else str(nested_item)
                if key not in seen:
                    seen.add(key)
                    result.append(nested_item)
        if "null" in result:
            result.insert(0, result.pop(result.index("null")))
        return result

    @staticmethod
    def is_nullable(avro_type: AvroSchema) -> bool:
        return avro_type == "null" or (isinstance(avro_type, list) and "null" in avro_type)

    def is_map_definition(self, definition: dict[str, Any]) -> bool:
        props = definition.get("properties")
        return "additionalProperties" in definition or (isinstance(props, dict) and set(props.keys()) == {"//"})

    def map_value_type(self, definition: dict[str, Any]) -> AvroSchema:
        if "additionalProperties" in definition:
            return self.convert_type(definition["additionalProperties"], "MapValue", {})
        props = definition.get("properties", {}) or {}
        return self.convert_type(props.get("//", "string"), "MapValue", {})

    def collect_dependencies(self, avro_type: AvroSchema, deps: list[str]) -> None:
        if isinstance(avro_type, str):
            if self.namespace and avro_type.startswith(f"{self.namespace}."):
                deps.append(avro_type)
        elif isinstance(avro_type, list):
            for item in avro_type:
                self.collect_dependencies(item, deps)
        elif isinstance(avro_type, dict):
            for key in ("items", "values", "type"):
                if key in avro_type and key != "type" or key == "type" and isinstance(avro_type.get(key), (dict, list, str)):
                    self.collect_dependencies(avro_type[key], deps)


def convert_raml_to_avro(raml_file_path: str, avro_schema_path: str, namespace: str | None = None) -> None:
    """Convert RAML 1.0 Data Types from ``raml_file_path`` to Avrotize/Avro schema."""
    converter = RamlToAvroConverter(namespace)
    avro_schema = converter.convert_file(raml_file_path)
    os.makedirs(os.path.dirname(os.path.abspath(avro_schema_path)) or ".", exist_ok=True)
    with open(avro_schema_path, "w", encoding="utf-8") as avro_file:
        json.dump(avro_schema, avro_file, indent=2)
        avro_file.write("\n")
