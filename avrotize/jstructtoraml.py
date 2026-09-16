"""Bridge JSON Structure to RAML Data Types through Avrotize/Avro schema."""

import os
import uuid

from avrotize.avrotoraml import convert_avro_to_raml
from avrotize.jstructtoavro import convert_json_structure_to_avro


def convert_json_structure_to_raml(structure_file: str, raml_file_path: str, namespace: str | None = None) -> None:
    """Convert JSON Structure to RAML 1.0 Data Types via a temporary Avro schema."""
    out_dir = os.path.dirname(os.path.abspath(raml_file_path)) or os.getcwd()
    os.makedirs(out_dir, exist_ok=True)
    temp_avro = os.path.join(out_dir, f".s2raml-{uuid.uuid4().hex}.avsc")
    try:
        convert_json_structure_to_avro(structure_file, temp_avro)
        convert_avro_to_raml(temp_avro, raml_file_path, namespace=namespace)
    finally:
        if os.path.exists(temp_avro):
            os.remove(temp_avro)
