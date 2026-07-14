"""JSON Type Definition to JSON Structure converter."""

from __future__ import annotations

import os
import tempfile

from avrotize.avrotojstruct import convert_avro_to_json_structure
from avrotize.jtdtoavro import convert_jtd_to_avro


def convert_jtd_to_structure(jtd_file_path: str, json_structure_file: str, namespace: str | None = None) -> None:
    """Convert a JTD file to JSON Structure by bridging through Avrotize Schema."""
    temp_dir = os.path.dirname(os.path.abspath(json_structure_file)) or os.getcwd()
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".avsc", dir=temp_dir)
    temp_file.close()
    try:
        convert_jtd_to_avro(jtd_file_path, temp_file.name, namespace=namespace)
        convert_avro_to_json_structure(temp_file.name, json_structure_file)
    finally:
        try:
            os.remove(temp_file.name)
        except OSError:
            pass
