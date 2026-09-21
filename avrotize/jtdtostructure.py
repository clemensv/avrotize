"""JSON Type Definition to JSON Structure converter."""

from __future__ import annotations

import os
import tempfile
import json
import copy

from avrotize.avrotojstruct import AvroToJsonStructure
from avrotize.jtdtoavro import JtdToAvroConverter, jtd_structure_root_name


def _set_definition(definitions: dict, path: str, value: dict) -> None:
    parts = path.split("/")
    current = definitions
    for part in parts[:-1]:
        current = current.setdefault(part, {})
    current[parts[-1]] = value


def _get_definition(definitions: dict, path: str) -> dict | None:
    current = definitions
    for part in path.split("/"):
        value = current.get(part)
        if not isinstance(value, dict):
            return None
        current = value
    return current


def _rewrite_ref(node: object, old_ref: str, new_ref: str) -> None:
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "$ref" and value == old_ref:
                node[key] = new_ref
            else:
                _rewrite_ref(value, old_ref, new_ref)
    elif isinstance(node, list):
        for value in node:
            _rewrite_ref(value, old_ref, new_ref)


def convert_jtd_to_structure(jtd_file_path: str, json_structure_file: str, namespace: str | None = None) -> None:
    """Convert a JTD file to JSON Structure by bridging through Avrotize Schema."""
    temp_dir = os.path.dirname(os.path.abspath(json_structure_file)) or os.getcwd()
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".avsc", dir=temp_dir)
    temp_file.close()
    try:
        with open(jtd_file_path, "r", encoding="utf-8") as jtd_file:
            jtd_schema = json.load(jtd_file)
        root_name = jtd_structure_root_name(jtd_file_path)
        jtd_converter = JtdToAvroConverter(namespace=namespace)
        avro_schema = jtd_converter.convert(jtd_schema, root_name=root_name)

        converter = AvroToJsonStructure()
        structure = converter.convert(avro_schema)
        root_type = jtd_converter.root_type
        if root_type is None:
            raise ValueError("JTD conversion did not produce a root type")

        non_null_root = root_type
        nullable = False
        seen_wrappers: set[str] = set()
        while True:
            nullable_members = isinstance(non_null_root, list) and "null" in non_null_root
            nullable = nullable or nullable_members
            if nullable_members:
                members = [item for item in non_null_root if item != "null"]
                non_null_root = members[0] if len(members) == 1 else members
            if not isinstance(non_null_root, str) or non_null_root in seen_wrappers:
                break
            seen_wrappers.add(non_null_root)
            wrapped = next(
                (
                    schema for schema in jtd_converter.generated.values()
                    if schema.get("jtdWrappedDefinition")
                    and non_null_root in {schema.get("name"), f"{schema.get('namespace')}.{schema.get('name')}"}
                ),
                None,
            )
            if wrapped is None:
                break
            non_null_root = wrapped["fields"][0]["type"]
        if isinstance(non_null_root, list) and "null" in non_null_root:
            members = [item for item in non_null_root if item != "null"]
            non_null_root = members[0] if len(members) == 1 else members
        root_node = converter.resolve_avro_type(non_null_root, jtd_converter.namespace, structure["definitions"])

        structure.pop("type", None)
        structure.pop("$root", None)
        root_namespace, resolved_root_name = converter.resolve_full_name(root_name, jtd_converter.namespace)
        root_path = converter.get_fqn(root_namespace, resolved_root_name)
        if root_path == converter.get_raw_fqn(root_namespace, resolved_root_name) and resolved_root_name in {"type", "definitions"}:
            root_path += "_"
        structure["name"] = resolved_root_name

        root_reference = root_node.get("type") if set(root_node) == {"type"} else None
        referenced_path = None
        if isinstance(root_reference, dict) and "$ref" in root_reference:
            referenced_path = root_reference["$ref"].removeprefix("#/definitions/")
        if _get_definition(structure["definitions"], root_path) is not None and referenced_path != root_path:
            base_path = f"{root_path}Root"
            root_path = base_path
            index = 2
            while _get_definition(structure["definitions"], root_path) is not None:
                root_path = f"{base_path}{index}"
                index += 1
        structure["$id"] = f"https://example.com/schemas/{root_path}"

        if isinstance(root_reference, dict) and "$ref" in root_reference and not nullable:
            structure["$root"] = root_reference["$ref"]
        elif nullable:
            base_value_path = f"{root_path}Value"
            value_path = base_value_path
            index = 2
            while _get_definition(structure["definitions"], value_path) is not None:
                value_path = f"{base_value_path}{index}"
                index += 1
            referenced_definition = _get_definition(structure["definitions"], referenced_path) if referenced_path else None
            value_definition = copy.deepcopy(referenced_definition or root_node)
            if referenced_path:
                _rewrite_ref(
                    value_definition,
                    f"#/definitions/{referenced_path}",
                    f"#/definitions/{value_path}",
                )
                _rewrite_ref(
                    structure["definitions"],
                    f"#/definitions/{referenced_path}",
                    f"#/definitions/{value_path}",
                )
            _set_definition(structure["definitions"], value_path, value_definition)
            _set_definition(
                structure["definitions"],
                root_path,
                {"name": resolved_root_name, "type": [{"$ref": f"#/definitions/{value_path}"}, "null"]},
            )
            structure["$root"] = f"#/definitions/{root_path}"
        else:
            root_node.setdefault("name", resolved_root_name)
            _set_definition(structure["definitions"], root_path, root_node)
            structure["$root"] = f"#/definitions/{root_path}"

        with open(json_structure_file, "w", encoding="utf-8") as structure_file:
            json.dump(structure, structure_file, indent=4)
    finally:
        try:
            os.remove(temp_file.name)
        except OSError:
            pass
