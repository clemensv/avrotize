"""Bridge JSON Structure to Smithy IDL through Avrotize Schema."""

from __future__ import annotations

import os
import tempfile

from avrotize.avrotosmithy import convert_avro_to_smithy
from avrotize.jstructtoavro import convert_json_structure_to_avro


def convert_json_structure_to_smithy(structure_file: str, smithy_file_path: str, namespace: str | None = None) -> None:
    """Convert JSON Structure data shapes to Smithy IDL via a temporary Avrotize Schema file."""
    temp_dir = os.path.dirname(os.path.abspath(smithy_file_path)) or os.getcwd()
    os.makedirs(temp_dir, exist_ok=True)
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".avsc", delete=False, dir=temp_dir) as temp_file:
            temp_path = temp_file.name
        convert_json_structure_to_avro(structure_file, temp_path)
        convert_avro_to_smithy(temp_path, smithy_file_path, namespace=namespace)
    finally:
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)
