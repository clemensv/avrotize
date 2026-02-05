"""Infers schema from JSON files and converts to Avro or JSON Structure format.

This module provides:
- json2a: Infer Avro schema from JSON files
- json2s: Infer JSON Structure schema from JSON files
"""

import json
import os
from typing import Any, Dict, List

from avrotize.schema_inference import (
    AvroSchemaInferrer,
    JsonStructureSchemaInferrer,
    JsonNode
)


def convert_json_to_avro(
    input_files: List[str],
    avro_schema_file: str,
    type_name: str = 'Document',
    avro_namespace: str = '',
    sample_size: int = 0,
    infer_choices: bool = False,
    choice_depth: int = 1
) -> None:
    """Infers Avro schema from JSON files.

    Reads JSON files, analyzes their structure, and generates an Avro schema
    that can represent all the data. Multiple files are analyzed together to
    produce a unified schema.

    Args:
        input_files: List of JSON file paths to analyze
        avro_schema_file: Output path for the Avro schema
        type_name: Name for the root type
        avro_namespace: Namespace for generated Avro types
        sample_size: Maximum number of records to sample (0 = all)
        infer_choices: Detect discriminated unions and emit as Avro unions with discriminator defaults
        choice_depth: Maximum nesting depth for recursive choice inference (1 = root only)
    """
    if not input_files:
        raise ValueError("At least one input file is required")

    values = _load_json_values(input_files, sample_size)

    if not values:
        raise ValueError("No valid JSON data found in input files")

    inferrer = AvroSchemaInferrer(namespace=avro_namespace, infer_choices=infer_choices, choice_depth=choice_depth)
    schema = inferrer.infer_from_json_values(type_name, values)

    # Ensure output directory exists
    output_dir = os.path.dirname(avro_schema_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open(avro_schema_file, 'w', encoding='utf-8') as f:
        json.dump(schema, f, indent=2)


def convert_json_to_jstruct(
    input_files: List[str],
    jstruct_schema_file: str,
    type_name: str = 'Document',
    base_id: str = 'https://example.com/',
    sample_size: int = 0,
    infer_choices: bool = False,
    choice_depth: int = 1
) -> None:
    """Infers JSON Structure schema from JSON files.

    Reads JSON files, analyzes their structure, and generates a JSON Structure
    schema that validates with the official JSON Structure SDK.

    Args:
        input_files: List of JSON file paths to analyze
        jstruct_schema_file: Output path for the JSON Structure schema
        type_name: Name for the root type
        base_id: Base URI for $id generation
        sample_size: Maximum number of records to sample (0 = all)
        infer_choices: Detect discriminated unions and emit as choice types with discriminator defaults
        choice_depth: Maximum nesting depth for recursive choice inference (1 = root only)
    """
    if not input_files:
        raise ValueError("At least one input file is required")

    values = _load_json_values(input_files, sample_size)

    if not values:
        raise ValueError("No valid JSON data found in input files")

    inferrer = JsonStructureSchemaInferrer(base_id=base_id, infer_choices=infer_choices, choice_depth=choice_depth)
    schema = inferrer.infer_from_json_values(type_name, values)

    # Ensure output directory exists
    output_dir = os.path.dirname(jstruct_schema_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open(jstruct_schema_file, 'w', encoding='utf-8') as f:
        json.dump(schema, f, indent=2)


def _load_json_values(input_files: List[str], sample_size: int) -> List[Any]:
    """Loads JSON values from files.

    Handles both single JSON documents and JSON Lines (JSONL) files.
    Arrays at the root level are flattened into individual values.

    Args:
        input_files: List of file paths
        sample_size: Maximum values to load (0 = all)

    Returns:
        List of parsed JSON values
    """
    values: List[Any] = []

    for file_path in input_files:
        if sample_size > 0 and len(values) >= sample_size:
            break

        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()

        if not content:
            continue

        # Try parsing as a single JSON document first
        try:
            data = json.loads(content)
            if isinstance(data, list):
                # Root-level array: each element is a separate value
                for item in data:
                    values.append(item)
                    if sample_size > 0 and len(values) >= sample_size:
                        break
            else:
                values.append(data)
            continue
        except json.JSONDecodeError:
            pass

        # Try parsing as JSON Lines (JSONL)
        for line in content.split('\n'):
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                values.append(data)
                if sample_size > 0 and len(values) >= sample_size:
                    break
            except json.JSONDecodeError:
                pass

    return values
