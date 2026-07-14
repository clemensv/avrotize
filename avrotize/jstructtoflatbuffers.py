"""Convert JSON Structure schemas to FlatBuffers .fbs via Avrotize Schema."""

from __future__ import annotations

import os
import tempfile

from avrotize.avrotoflatbuffers import convert_avro_to_flatbuffers
from avrotize.jstructtoavro import convert_json_structure_to_avro


def convert_json_structure_to_flatbuffers(structure_file: str, fbs_file_path: str, namespace: str | None = None):
    temp_dir = os.path.dirname(os.path.abspath(fbs_file_path)) or os.getcwd()
    with tempfile.NamedTemporaryFile(delete=False, suffix=".avsc", dir=temp_dir) as temp:
        temp_path = temp.name
    try:
        convert_json_structure_to_avro(structure_file, temp_path)
        convert_avro_to_flatbuffers(temp_path, fbs_file_path, namespace=namespace)
    finally:
        try:
            os.remove(temp_path)
        except OSError:
            pass
