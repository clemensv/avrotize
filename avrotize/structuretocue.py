"""JSON Structure to CUE subset bridge via Avrotize Schema."""

from __future__ import annotations

import os
import tempfile

from avrotize.avrotocue import convert_avro_to_cue
from avrotize.jstructtoavro import convert_json_structure_to_avro


def convert_json_structure_to_cue(
    structure_file: str,
    cue_file_path: str,
    namespace: str | None = None,
) -> None:
    """Convert JSON Structure schema to CUE subset through an Avro temporary file."""
    output_dir = os.path.dirname(os.path.abspath(cue_file_path)) or os.getcwd()
    with tempfile.NamedTemporaryFile("w", suffix=".avsc", delete=False, dir=output_dir, encoding="utf-8") as temp_file:
        temp_avro = temp_file.name
    try:
        convert_json_structure_to_avro(structure_file, temp_avro)
        convert_avro_to_cue(temp_avro, cue_file_path, namespace=namespace)
    finally:
        if os.path.exists(temp_avro):
            os.remove(temp_avro)
