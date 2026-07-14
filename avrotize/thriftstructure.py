
"""Apache Thrift IDL and JSON Structure bridge converters."""

from __future__ import annotations

import json
import os
import tempfile

from avrotize.avrotojstruct import AvroToJsonStructure
from avrotize.avrotothrift import convert_avro_to_thrift
from avrotize.jstructtoavro import convert_json_structure_to_avro
from avrotize.thrifttoavro import convert_thrift_to_avro


def convert_thrift_to_structure(thrift_file_path: str, json_structure_file: str, namespace: str | None = None):
    """Convert Apache Thrift IDL to JSON Structure through Avrotize Schema."""
    with tempfile.TemporaryDirectory() as temp_dir:
        avro_path = os.path.join(temp_dir, "schema.avsc")
        convert_thrift_to_avro(thrift_file_path, avro_path, namespace)
        with open(avro_path, "r", encoding="utf-8") as avro_file:
            avro_schema = json.load(avro_file)
        schemas = avro_schema if isinstance(avro_schema, list) else [avro_schema]
        root_schema = next((s for s in reversed(schemas) if isinstance(s, dict) and s.get("type") == "record"), schemas[0] if schemas else {})
        converter = AvroToJsonStructure()
        root_namespace = root_schema.get("namespace") if isinstance(root_schema, dict) else None
        root_name = converter.clean_name(root_schema.get("name", "Root") if isinstance(root_schema, dict) else "Root")
        root_fqn = converter.get_fqn(root_namespace, root_name)
        structure = {
            "$schema": "https://json-structure.org/meta/core/v0/#",
            "$id": f"https://example.com/schemas/{root_fqn}",
            "name": root_name,
            "$root": f"#/definitions/{root_fqn}",
            "definitions": {},
        }
        for schema in schemas:
            if isinstance(schema, dict) and schema.get("type") in {"record", "enum", "fixed"}:
                converter.register_definition(schema, schema.get("namespace"), structure["definitions"])
        with open(json_structure_file, "w", encoding="utf-8") as structure_file:
            json.dump(structure, structure_file, indent=4)


def convert_structure_to_thrift(structure_file: str, thrift_file_path: str, namespace: str | None = None):
    """Convert JSON Structure to Apache Thrift IDL through Avrotize Schema."""
    with tempfile.TemporaryDirectory() as temp_dir:
        avro_path = os.path.join(temp_dir, "schema.avsc")
        convert_json_structure_to_avro(structure_file, avro_path)
        convert_avro_to_thrift(avro_path, thrift_file_path, namespace)
