"""Avrotize Schema to JSON Type Definition (RFC 8927) converter."""

from __future__ import annotations

import json
from typing import Any

from avrotize.common import altname

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
        self.inherited_namespaces: dict[int, str] = {}
        self.inline_named_schema_ids: set[int] = set()
        self.definitions: dict[str, JtdSchema] = {}
        self.converting: set[str] = set()
        self.root_key: str | None = None

    def convert(self, avro_schema: AvroSchema) -> JtdSchema:
        """Convert an Avro schema object to a JTD schema dictionary."""
        self._register_named_types(avro_schema)
        root = self._register_top_level(avro_schema)
        if self.record_type:
            root = self._find_named_type(self.record_type)
        if isinstance(root, dict) and root.get("type") in {"record", "enum"}:
            self.root_key = self._type_key(root)
        root_namespace = self._schema_namespace(root) if isinstance(root, dict) else None
        root_jtd = self._convert_type(root, root_namespace)
        if isinstance(root, dict) and root.get("type") in {"record", "enum"} and root.get("jtdRoot"):
            root_name = self._type_key(root)
            self.definitions[root_name] = root_jtd
            return {"definitions": self.definitions, "ref": root_name}
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
            discriminator_branches = [
                item for item in avro_schema
                if isinstance(item, dict) and item.get("type") == "record" and item.get("jtdDiscriminator")
            ]
            if self._is_discriminator_union(discriminator_branches):
                for item in avro_schema:
                    if isinstance(item, dict) and item.get("type") in {"record", "enum"}:
                        self._register_named(item)
                helper_dependencies = self._named_dependencies_of(discriminator_branches)
                return [
                    item for item in avro_schema
                    if not (
                        isinstance(item, dict)
                        and item.get("type") in {"record", "enum"}
                        and not item.get("jtdDiscriminator")
                        and self._type_key(item) in helper_dependencies
                    )
                ]
            named_items = [
                item for item in avro_schema
                if isinstance(item, dict) and item.get("type") in {"record", "enum"}
            ]
            root_members = [item for item in avro_schema if item not in named_items]
            has_explicit_root = any(item.get("jtdRoot") for item in named_items) or any(
                isinstance(item, dict) and item.get("jtdRoot") for item in root_members
            )
            if named_items and root_members and has_explicit_root:
                for item in named_items:
                    self._register_named(item)
                if len(root_members) == 1:
                    return root_members[0]
                return root_members
            if not all(isinstance(item, dict) and item.get("type") in {"record", "enum"} for item in avro_schema):
                return avro_schema
            for item in avro_schema:
                self._register_named(item)
            root = next((item for item in avro_schema if item.get("jtdRoot")), avro_schema[-1] if avro_schema else None)
            if root is None:
                raise ValueError("Avro schema list is empty")
            return root
        if isinstance(avro_schema, dict) and avro_schema.get("type") in {"record", "enum"}:
            self._register_named(avro_schema)
        return avro_schema

    def _register_named_types(
        self,
        avro_type: AvroSchema,
        seen: set[int] | None = None,
        inherited_namespace: str | None = None,
        inline: bool = False,
    ) -> None:
        if seen is None:
            seen = set()
        if isinstance(avro_type, list):
            for member in avro_type:
                self._register_named_types(member, seen, inherited_namespace, inline)
            return
        if not isinstance(avro_type, dict) or id(avro_type) in seen:
            return
        seen.add(id(avro_type))

        schema_type = avro_type.get("type")
        if schema_type in {"record", "enum"} and avro_type.get("name"):
            namespace = avro_type.get("namespace", inherited_namespace)
            if namespace:
                self.inherited_namespaces[id(avro_type)] = str(namespace)
            if inline:
                self.inline_named_schema_ids.add(id(avro_type))
            self._register_named(avro_type)
            inherited_namespace = str(namespace) if namespace else None
        if schema_type == "record":
            for field in avro_type.get("fields", []):
                self._register_named_types(field.get("type", "string"), seen, inherited_namespace, True)
        elif schema_type == "array":
            self._register_named_types(avro_type.get("items", "string"), seen, inherited_namespace, True)
        elif schema_type == "map":
            self._register_named_types(avro_type.get("values", "string"), seen, inherited_namespace, True)
        elif isinstance(schema_type, (dict, list)):
            self._register_named_types(schema_type, seen, inherited_namespace, True)

    def _register_named(self, schema: dict[str, Any]) -> None:
        key = self._type_key(schema)
        self.named_types[key] = schema
        self.simple_names[schema["name"]] = key

    def _named_dependencies_of(self, roots: list[Any]) -> set[str]:
        root_keys = {
            self._type_key(root)
            for root in roots
            if isinstance(root, dict) and root.get("type") in {"record", "enum"}
        }
        dependencies: set[str] = set()
        pending = list(roots)
        while pending:
            for reference in self._named_references(pending.pop()):
                if reference in root_keys or reference in dependencies:
                    continue
                dependencies.add(reference)
                pending.append(self.named_types[reference])
        return dependencies

    def _named_references(self, avro_type: AvroSchema, namespace: str | None = None) -> set[str]:
        if isinstance(avro_type, str):
            key = self._resolve_named_key(avro_type, namespace)
            return {key} if key else set()
        if isinstance(avro_type, list):
            references: set[str] = set()
            for member in avro_type:
                references.update(self._named_references(member, namespace))
            return references
        if not isinstance(avro_type, dict):
            return set()

        schema_type = avro_type.get("type")
        if schema_type == "record":
            record_namespace = self._schema_namespace(avro_type, namespace)
            references: set[str] = set()
            for field in avro_type.get("fields", []):
                references.update(self._named_references(field.get("type", "string"), record_namespace))
            return references
        if schema_type == "array":
            return self._named_references(avro_type.get("items", "string"), namespace)
        if schema_type == "map":
            return self._named_references(avro_type.get("values", "string"), namespace)
        if isinstance(schema_type, (dict, list, str)):
            return self._named_references(schema_type, namespace)
        return set()

    def _find_named_type(self, record_type: str) -> dict[str, Any]:
        key = self._resolve_named_key(record_type)
        if key and key in self.named_types:
            return self.named_types[key]
        raise ValueError(f"Avro record type '{record_type}' not found")

    def _type_key(self, schema: dict[str, Any]) -> str:
        namespace = self._schema_namespace(schema)
        return f"{namespace}.{schema['name']}" if namespace else schema["name"]

    def _schema_namespace(self, schema: dict[str, Any], fallback: str | None = None) -> str | None:
        if "namespace" in schema:
            namespace = schema.get("namespace")
        else:
            namespace = self.inherited_namespaces.get(id(schema), fallback)
        return str(namespace) if namespace else None

    def _resolve_named_key(self, reference: str, namespace: str | None = None) -> str | None:
        if reference in self.named_types:
            return reference
        if namespace and "." not in reference:
            relative_key = f"{namespace}.{reference}"
            if relative_key in self.named_types:
                return relative_key
        return self.simple_names.get(reference)

    def _convert_type(self, avro_type: AvroSchema, namespace: str | None = None) -> JtdSchema:
        nullable, non_null = self._strip_nullable(avro_type)
        if self._is_discriminator_union(non_null):
            jtd = self._convert_discriminator(non_null, namespace)  # type: ignore[arg-type]
        elif isinstance(non_null, dict) and non_null.get("type") == "record" and non_null.get("jtdDiscriminator"):
            jtd = self._convert_discriminator([non_null], namespace)
        else:
            jtd = self._convert_non_nullable(non_null, namespace)
        if nullable:
            jtd = dict(jtd)
            jtd["nullable"] = True
        return jtd

    def _convert_non_nullable(self, avro_type: AvroSchema, namespace: str | None = None) -> JtdSchema:
        if isinstance(avro_type, str):
            if avro_type == "null":
                return {"nullable": True}
            if avro_type in self.PRIMITIVE_TO_JTD:
                return {"type": self.PRIMITIVE_TO_JTD[avro_type]}
            ref_key = self._resolve_named_key(avro_type, namespace)
            if ref_key:
                self._ensure_definition(ref_key)
                return {"ref": ref_key}
            return {"type": "string"}
        if isinstance(avro_type, list):
            return self._convert_union(avro_type, namespace)
        if not isinstance(avro_type, dict):
            return {"type": "string"}

        if avro_type.get("jtdWrappedDefinition"):
            fields = avro_type.get("fields", [])
            if len(fields) == 1:
                return self._convert_type(fields[0].get("type", "string"), namespace)
        if avro_type.get("jtdType"):
            return {"type": avro_type["jtdType"]}
        if avro_type.get("logicalType") == "timestamp-millis" and avro_type.get("type") == "long":
            return {"type": "timestamp"}

        schema_type = avro_type.get("type")
        if isinstance(schema_type, dict) or isinstance(schema_type, list):
            return self._convert_type(schema_type, namespace)
        if schema_type in self.PRIMITIVE_TO_JTD:
            return {"type": self.PRIMITIVE_TO_JTD[schema_type]}
        if schema_type == "record":
            key = self._type_key(avro_type)
            if id(avro_type) in self.inline_named_schema_ids and key != self.root_key:
                self._ensure_definition(key)
                return {"ref": key}
            return self._convert_record(avro_type, namespace)
        if schema_type == "enum":
            key = self._type_key(avro_type)
            if id(avro_type) in self.inline_named_schema_ids and key != self.root_key:
                self._ensure_definition(key)
                return {"ref": key}
            return self._convert_enum(avro_type)
        if schema_type == "array":
            return {"elements": self._convert_type(avro_type.get("items", "string"), namespace)}
        if schema_type == "map":
            return {"values": self._convert_type(avro_type.get("values", "string"), namespace)}
        if isinstance(schema_type, str):
            ref_key = self._resolve_named_key(schema_type, namespace)
            if ref_key:
                self._ensure_definition(ref_key)
                return {"ref": ref_key}
        return {"type": "string"}

    def _convert_record(self, schema: dict[str, Any], parent_namespace: str | None = None) -> JtdSchema:
        key = self._type_key(schema)
        if key in self.converting:
            return {"ref": key}
        namespace = self._schema_namespace(schema, parent_namespace)
        self.converting.add(key)
        properties: dict[str, JtdSchema] = {}
        optional_properties: dict[str, JtdSchema] = {}
        for field in schema.get("fields", []):
            if field.get("name") == schema.get("jtdDiscriminator"):
                continue
            property_name = altname(field, "jtd")
            field_nullable, field_type = self._strip_nullable(field.get("type", "string"))
            converted = self._convert_type(field_type, namespace)
            if field_nullable and field.get("default") is None:
                optional_properties[property_name] = converted
            else:
                if field_nullable:
                    converted = dict(converted)
                    converted["nullable"] = True
                properties[property_name] = converted
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

    def _convert_union(self, union: list[Any], namespace: str | None = None) -> JtdSchema:
        non_null = [item for item in union if item != "null"]
        if len(non_null) == 1:
            return self._convert_type(non_null[0], namespace)
        if self._is_discriminator_union(non_null):
            return self._convert_discriminator(non_null, namespace)
        discriminator_branches = [
            item for item in non_null
            if isinstance(item, dict) and item.get("type") == "record" and item.get("jtdDiscriminator")
        ]
        if self._is_discriminator_union(discriminator_branches):
            tagged_ids = {id(branch) for branch in discriminator_branches}
            converted: list[JtdSchema] = []
            discriminator_added = False
            for item in non_null:
                if id(item) in tagged_ids:
                    if not discriminator_added:
                        converted.append(self._convert_discriminator(discriminator_branches, namespace))
                        discriminator_added = True
                else:
                    converted.append(self._convert_type(item, namespace))
            return {"metadata": {"avrotize-union": converted}}
        return {"metadata": {"avrotize-union": [self._convert_type(item, namespace) for item in non_null]}}

    def _convert_discriminator(self, union: list[Any], namespace: str | None = None) -> JtdSchema:
        discriminator = union[0].get("jtdDiscriminator")
        mapping: dict[str, JtdSchema] = {}
        for branch in union:
            tag = branch.get("jtdMappingKey")
            if tag is None:
                tag = branch.get("name")
            mapping[str(tag)] = self._convert_record(branch, namespace)
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
        self.definitions[key] = {}
        try:
            schema = self.named_types[key]
            namespace = self._schema_namespace(schema)
            if id(schema) in self.inline_named_schema_ids and schema.get("type") == "record":
                self.definitions[key] = self._convert_record(schema, namespace)
            elif id(schema) in self.inline_named_schema_ids and schema.get("type") == "enum":
                self.definitions[key] = self._convert_enum(schema)
            else:
                self.definitions[key] = self._convert_non_nullable(schema, namespace)
        except Exception:
            self.definitions.pop(key, None)
            raise


def convert_avro_to_jtd(avro_schema_path: str, jtd_file_path: str, record_type: str | None = None) -> None:
    """Convert an Avrotize Schema file to a JSON Type Definition file."""
    with open(avro_schema_path, "r", encoding="utf-8") as avro_file:
        avro_schema = json.load(avro_file)
    converter = AvroToJtdConverter(record_type=record_type)
    jtd_schema = converter.convert(avro_schema)
    with open(jtd_file_path, "w", encoding="utf-8") as jtd_file:
        json.dump(jtd_schema, jtd_file, indent=2)
