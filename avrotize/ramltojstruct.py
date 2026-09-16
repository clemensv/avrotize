"""Bridge RAML Data Types to JSON Structure through Avrotize/Avro schema."""

import os
import uuid

from avrotize.avrotojstruct import convert_avro_to_json_structure
from avrotize.ramltoavro import convert_raml_to_avro


def convert_raml_to_json_structure(raml_file_path: str, json_structure_file: str, namespace: str | None = None) -> None:
    """Convert RAML 1.0 Data Types to JSON Structure via a temporary Avro schema."""
    out_dir = os.path.dirname(os.path.abspath(json_structure_file)) or os.getcwd()
    os.makedirs(out_dir, exist_ok=True)
    temp_avro = os.path.join(out_dir, f".raml2s-{uuid.uuid4().hex}.avsc")
    try:
        convert_raml_to_avro(raml_file_path, temp_avro, namespace=namespace)
        convert_avro_to_json_structure(temp_avro, json_structure_file)
    finally:
        if os.path.exists(temp_avro):
            os.remove(temp_avro)
