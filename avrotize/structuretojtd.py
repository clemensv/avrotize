"""JSON Structure to JSON Type Definition converter."""

from __future__ import annotations

import os
import tempfile

from avrotize.avrotojtd import convert_avro_to_jtd
from avrotize.jstructtoavro import convert_json_structure_to_avro


def convert_structure_to_jtd(structure_file: str, jtd_file_path: str, record_type: str | None = None) -> None:
    """Convert JSON Structure to JTD by bridging through Avrotize Schema."""
    temp_dir = os.path.dirname(os.path.abspath(jtd_file_path)) or os.getcwd()
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".avsc", dir=temp_dir)
    temp_file.close()
    try:
        convert_json_structure_to_avro(structure_file, temp_file.name)
        convert_avro_to_jtd(temp_file.name, jtd_file_path, record_type=record_type)
    finally:
        try:
            os.remove(temp_file.name)
        except OSError:
            pass
