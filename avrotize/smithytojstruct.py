"""Bridge Smithy IDL to JSON Structure through Avrotize Schema."""

from __future__ import annotations

import os
import tempfile

from avrotize.avrotojstruct import convert_avro_to_json_structure
from avrotize.smithytoavro import convert_smithy_to_avro


def convert_smithy_to_json_structure(smithy_file_path: str, json_structure_file: str, namespace: str | None = None) -> None:
    """Convert Smithy data shapes to JSON Structure via a temporary Avrotize Schema file."""
    temp_dir = os.path.dirname(os.path.abspath(json_structure_file)) or os.getcwd()
    os.makedirs(temp_dir, exist_ok=True)
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".avsc", delete=False, dir=temp_dir) as temp_file:
            temp_path = temp_file.name
        convert_smithy_to_avro(smithy_file_path, temp_path, namespace=namespace)
        convert_avro_to_json_structure(temp_path, json_structure_file)
    finally:
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)
