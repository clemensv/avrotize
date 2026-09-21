"""JSON Type Definition (RFC 8927) to Avrotize Schema converter."""

from __future__ import annotations

import json
import os
import copy
from typing import Any

from avrotize.common import avro_name, avro_name_with_altname, avro_namespace

AvroSchema = dict[str, Any] | list[Any] | str
JtdSchema = dict[str, Any]


class JtdToAvroConverter:
    """Convert JSON Type Definition schemas to Avrotize/Avro schemas."""

    AVRO_PRIMITIVES = {"null", "boolean", "int", "long", "float", "double", "bytes", "string"}
    AVRO_SCHEMA_KINDS = {"record", "enum", "array", "map", "fixed", "error"}

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
        self.root_type: AvroSchema | None = None

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
                self.root_type = self._nullable(root_type)
                return [*self._finalize_named_types(self.output), "null", root_type]
            self.root_type = root_type
            return self._finalize_named_types(self.output) if self.output else root_type

        converted = self._convert_schema(root_schema, self._unique_type_name(root_name))
        self.root_type = copy.deepcopy(converted)
        if self.output:
            if isinstance(converted, dict) and converted.get("type") in {"record", "enum"}:
                if converted not in self.output:
                    self.output.append(converted)
                return self._finalize_named_types(self.output)
            root_members = converted if isinstance(converted, list) else [converted]
            for member in root_members:
                if isinstance(member, dict) and member.get("type") not in {"record", "enum"}:
                    member["jtdRoot"] = True
            named_members = [
                member for member in root_members
                if isinstance(member, dict) and member.get("type") in {"record", "enum", "fixed", "error"}
            ]
            sorted_named = self._finalize_named_types([*self.output, *named_members])
            other_members = [member for member in root_members if member not in named_members]
            return [*sorted_named, *other_members]
        if isinstance(converted, list):
            return self._finalize_named_types(converted)
        return converted

    def _unique_type_name(self, name: str) -> str:
        candidate = avro_name(str(name).split("/")[-1].split(".")[-1] or "Type")
        if candidate == "_":
            candidate = "Type"
        if candidate in self.AVRO_PRIMITIVES:
            candidate = f"{candidate}Type"
        used = set(self.ref_names.values())
        if candidate not in used:
            return candidate
        index = 2
        while f"{candidate}{index}" in used:
            index += 1
        return f"{candidate}{index}"

    def _full_name(self, name: str) -> str:
        return f"{self.namespace}.{name}" if self.namespace else name

    @staticmethod
    def _named_type_key(schema: dict[str, Any]) -> str:
        namespace = schema.get("namespace")
        return f"{namespace}.{schema['name']}" if namespace else schema["name"]

    def _finalize_named_types(self, schemas: list[AvroSchema]) -> list[AvroSchema]:
        named = [
            schema for schema in schemas
            if isinstance(schema, dict) and "name" in schema and schema.get("type") in {"record", "enum", "fixed", "error"}
        ]
        if len(named) < 2:
            return schemas

        by_key = {self._named_type_key(schema): schema for schema in named}
        simple_keys: dict[str, str] = {}
        ambiguous_names: set[str] = set()
        for key, schema in by_key.items():
            name = schema["name"]
            if name in simple_keys and simple_keys[name] != key:
                ambiguous_names.add(name)
            else:
                simple_keys[name] = key
        for name in ambiguous_names:
            simple_keys.pop(name, None)

        def resolve_key(reference: str, namespace: str | None = None) -> str | None:
            if reference in by_key:
                return reference
            if namespace and f"{namespace}.{reference}" in by_key:
                return f"{namespace}.{reference}"
            return simple_keys.get(reference)

        def collect_references(node: AvroSchema, namespace: str | None) -> set[str]:
            if isinstance(node, str):
                key = resolve_key(node, namespace)
                return {key} if key else set()
            if isinstance(node, list):
                references: set[str] = set()
                for member in node:
                    references.update(collect_references(member, namespace))
                return references
            if not isinstance(node, dict):
                return set()
            schema_type = node.get("type")
            if schema_type in {"record", "error"}:
                record_namespace = node.get("namespace", namespace)
                references: set[str] = set()
                for field in node.get("fields", []):
                    references.update(collect_references(field.get("type", "string"), record_namespace))
                return references
            if schema_type == "array":
                return collect_references(node.get("items", "string"), namespace)
            if schema_type == "map":
                return collect_references(node.get("values", "string"), namespace)
            if isinstance(schema_type, (dict, list)):
                return collect_references(schema_type, namespace)
            if isinstance(schema_type, str) and schema_type not in self.AVRO_SCHEMA_KINDS:
                return collect_references(schema_type, namespace)
            return set()

        graph = {
            key: collect_references(schema, schema.get("namespace"))
            for key, schema in by_key.items()
        }

        def reachable(start: str, target: str) -> bool:
            pending = list(graph.get(start, set()))
            visited: set[str] = set()
            while pending:
                current = pending.pop()
                if current == target:
                    return True
                if current not in visited:
                    visited.add(current)
                    pending.extend(graph.get(current, set()))
            return False

        ordered_keys: list[str] = []
        seen_components: set[frozenset[str]] = set()
        for schema in named:
            key = self._named_type_key(schema)
            component = frozenset(
                candidate for candidate in by_key
                if reachable(key, candidate) and reachable(candidate, key)
            )
            if len(component) > 1:
                if component in seen_components:
                    continue
                seen_components.add(component)
                root = next(
                    (candidate for candidate in component if by_key[candidate].get("jtdRoot")),
                    key,
                )
                ordered_keys.append(root)
                ordered_keys.extend(
                    candidate for candidate in by_key
                    if candidate in component and candidate != root
                )
            elif key not in ordered_keys:
                ordered_keys.append(key)

        declared: set[str] = set()
        active: set[str] = set()

        def materialize_type(node: AvroSchema, namespace: str | None) -> AvroSchema:
            if isinstance(node, str):
                key = resolve_key(node, namespace)
                if key and key not in declared and key not in active:
                    return materialize_named(key)
                return node
            if isinstance(node, list):
                return [materialize_type(member, namespace) for member in node]
            if not isinstance(node, dict):
                return node

            materialized = copy.deepcopy(node)
            schema_type = materialized.get("type")
            if schema_type in {"record", "error"}:
                record_namespace = materialized.get("namespace", namespace)
                for field in materialized.get("fields", []):
                    field["type"] = materialize_type(field.get("type", "string"), record_namespace)
            elif schema_type == "array":
                materialized["items"] = materialize_type(materialized.get("items", "string"), namespace)
            elif schema_type == "map":
                materialized["values"] = materialize_type(materialized.get("values", "string"), namespace)
            elif isinstance(schema_type, (dict, list)):
                materialized["type"] = materialize_type(schema_type, namespace)
            elif isinstance(schema_type, str) and schema_type not in self.AVRO_SCHEMA_KINDS:
                materialized["type"] = materialize_type(schema_type, namespace)
            return materialized

        def materialize_named(key: str) -> dict[str, Any]:
            active.add(key)
            schema = by_key[key]
            materialized = materialize_type(schema, schema.get("namespace"))
            active.remove(key)
            declared.add(key)
            return materialized  # type: ignore[return-value]

        finalized: list[AvroSchema] = []
        for key in ordered_keys:
            if key not in declared:
                finalized.append(materialize_named(key))
        finalized.extend(schema for schema in schemas if schema not in named)
        return finalized

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
            avro_field_name, original_name = avro_name_with_altname(prop_name)
            field = {"name": avro_field_name, "type": self._convert_schema(prop_schema, f"{suggested_name}{avro_field_name.title()}")}
            if original_name is not None:
                field["altnames"] = {"jtd": original_name}
            record["fields"].append(field)
        for prop_name, prop_schema in schema.get("optionalProperties", {}).items():
            avro_field_name, original_name = avro_name_with_altname(prop_name)
            field = {
                "name": avro_field_name,
                "type": self._nullable(self._convert_schema(prop_schema, f"{suggested_name}{avro_field_name.title()}")),
                "default": None,
            }
            if original_name is not None:
                field["altnames"] = {"jtd": original_name}
            record["fields"].append(field)
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
        if isinstance(avro_type, list):
            if "null" in avro_type:
                return avro_type
            return ["null", *avro_type]
        return ["null", avro_type]


def _jtd_root_name(jtd_file_path: str, strip_jtd_suffix: bool) -> str:
    file_name = os.path.basename(jtd_file_path)
    if strip_jtd_suffix and file_name.lower().endswith(".jtd.json"):
        file_name = file_name[:-9]
    else:
        file_name = os.path.splitext(file_name)[0]
    name = avro_name(file_name or "Root")
    return "Root" if name == "_" else name


def jtd_root_name(jtd_file_path: str) -> str:
    """Derive the legacy direct-converter root name from the final file suffix."""
    return _jtd_root_name(jtd_file_path, strip_jtd_suffix=False)


def jtd_structure_root_name(jtd_file_path: str) -> str:
    """Derive a stable bridge root name while treating .jtd.json as one suffix."""
    return _jtd_root_name(jtd_file_path, strip_jtd_suffix=True)


def convert_jtd_to_avro(jtd_file_path: str, avro_schema_path: str, namespace: str | None = None) -> None:
    """Convert a JSON Type Definition file to an Avrotize Schema file."""
    with open(jtd_file_path, "r", encoding="utf-8") as jtd_file:
        jtd_schema = json.load(jtd_file)
    root_name = jtd_root_name(jtd_file_path)
    converter = JtdToAvroConverter(namespace=namespace)
    avro_schema = converter.convert(jtd_schema, root_name=root_name)
    with open(avro_schema_path, "w", encoding="utf-8") as avro_file:
        json.dump(avro_schema, avro_file, indent=2)
