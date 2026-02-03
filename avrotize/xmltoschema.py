"""Infers schema from XML files and converts to Avro or JSON Structure format.

This module provides:
- xml2a: Infer Avro schema from XML files
- xml2s: Infer JSON Structure schema from XML files
"""

import json
import os
from typing import List

from avrotize.schema_inference import (
    AvroSchemaInferrer,
    JsonStructureSchemaInferrer,
    JsonNode
)


def convert_xml_to_avro(
    input_files: List[str],
    avro_schema_file: str,
    type_name: str = 'Document',
    avro_namespace: str = '',
    sample_size: int = 0
) -> None:
    """Infers Avro schema from XML files.

    Reads XML files, analyzes their structure, and generates an Avro schema
    that can represent all the data. Multiple files are analyzed together to
    produce a unified schema.

    Args:
        input_files: List of XML file paths to analyze
        avro_schema_file: Output path for the Avro schema
        type_name: Name for the root type
        avro_namespace: Namespace for generated Avro types
        sample_size: Maximum number of documents to sample (0 = all)
    """
    if not input_files:
        raise ValueError("At least one input file is required")

    xml_strings = _load_xml_strings(input_files, sample_size)

    if not xml_strings:
        raise ValueError("No valid XML data found in input files")

    inferrer = AvroSchemaInferrer(namespace=avro_namespace)
    schema = inferrer.infer_from_xml_values(type_name, xml_strings)

    # Ensure output directory exists
    output_dir = os.path.dirname(avro_schema_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open(avro_schema_file, 'w', encoding='utf-8') as f:
        json.dump(schema, f, indent=2)


def convert_xml_to_jstruct(
    input_files: List[str],
    jstruct_schema_file: str,
    type_name: str = 'Document',
    base_id: str = 'https://example.com/',
    sample_size: int = 0
) -> None:
    """Infers JSON Structure schema from XML files.

    Reads XML files, analyzes their structure, and generates a JSON Structure
    schema that validates with the official JSON Structure SDK.

    Args:
        input_files: List of XML file paths to analyze
        jstruct_schema_file: Output path for the JSON Structure schema
        type_name: Name for the root type
        base_id: Base URI for $id generation
        sample_size: Maximum number of documents to sample (0 = all)
    """
    if not input_files:
        raise ValueError("At least one input file is required")

    xml_strings = _load_xml_strings(input_files, sample_size)

    if not xml_strings:
        raise ValueError("No valid XML data found in input files")

    inferrer = JsonStructureSchemaInferrer(base_id=base_id)
    schema = inferrer.infer_from_xml_values(type_name, xml_strings)

    # Ensure output directory exists
    output_dir = os.path.dirname(jstruct_schema_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open(jstruct_schema_file, 'w', encoding='utf-8') as f:
        json.dump(schema, f, indent=2)


def _load_xml_strings(input_files: List[str], sample_size: int) -> List[str]:
    """Loads XML content from files.

    Each file is treated as a single XML document.

    Args:
        input_files: List of file paths
        sample_size: Maximum documents to load (0 = all)

    Returns:
        List of XML strings
    """
    xml_strings: List[str] = []

    for file_path in input_files:
        if sample_size > 0 and len(xml_strings) >= sample_size:
            break

        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()

        if content:
            xml_strings.append(content)

    return xml_strings
