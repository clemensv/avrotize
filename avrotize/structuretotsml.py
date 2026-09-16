"""Convert a JSON Structure schema to a Tabular Model Scripting Language (TMSL) schema."""

import json
from typing import Optional

from avrotize.avrototsml import AvroToTmslConverter
from avrotize.jstructtoavro import JsonStructureToAvro


def convert_structure_to_tmsl(
    structure_schema_path: str,
    tmsl_file_path: str,
    structure_record_type: Optional[str] = None,
    database_name: str = "",
    compatibility_level: int = 1605,
    emit_cloudevents_columns: bool = False,
) -> None:
    """Convert a JSON Structure schema file to a TMSL JSON file."""
    with open(structure_schema_path, "r", encoding="utf-8") as f:
        structure_schema = json.load(f)

    avro_schema = JsonStructureToAvro().convert(structure_schema)

    converter = AvroToTmslConverter()
    tmsl_schema = converter.build_tmsl_schema(
        avro_schema,
        avro_record_type=structure_record_type,
        database_name=database_name,
        compatibility_level=compatibility_level,
        emit_cloudevents_columns=emit_cloudevents_columns,
    )

    with open(tmsl_file_path, "w", encoding="utf-8") as f:
        json.dump(tmsl_schema, f, indent=2)
