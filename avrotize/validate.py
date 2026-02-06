"""Validates JSON instances against Avro or JSON Structure schemas.

This module provides a unified interface for validating JSON data against
both Avro schemas and JSON Structure schemas.
"""

import json
import os
from typing import Any, Dict, List, Tuple

from avrotize.avrovalidator import AvroValidator, AvroValidationError, validate_json_against_avro

# JSON Structure SDK for validation
try:
    from json_structure import SchemaValidator as JStructSchemaValidator
    from json_structure import InstanceValidator as JStructInstanceValidator
    HAS_JSTRUCT_SDK = True
except ImportError:
    HAS_JSTRUCT_SDK = False


class ValidationResult:
    """Result of validating a JSON instance against a schema."""

    def __init__(self, is_valid: bool, errors: List[str] = None, instance_path: str = None):
        self.is_valid = is_valid
        self.errors = errors or []
        self.instance_path = instance_path

    def __str__(self) -> str:
        if self.is_valid:
            return f"✓ Valid" + (f": {self.instance_path}" if self.instance_path else "")
        else:
            prefix = f"{self.instance_path}: " if self.instance_path else ""
            return f"✗ Invalid: {prefix}" + "; ".join(self.errors)

    def __repr__(self) -> str:
        return f"ValidationResult(is_valid={self.is_valid}, errors={self.errors})"


def detect_schema_type(schema: Dict[str, Any]) -> str:
    """Detects whether a schema is Avro or JSON Structure.

    Args:
        schema: The parsed schema object

    Returns:
        'avro' or 'jstruct' or 'unknown'
    """
    # JSON Structure schemas have $schema and $id
    if '$schema' in schema and 'json-structure' in schema.get('$schema', ''):
        return 'jstruct'

    # Avro schemas have 'type' at root and may have 'namespace', 'fields', etc.
    if 'type' in schema:
        schema_type = schema.get('type')
        # Check for Avro record, enum, array, map, or primitive
        if schema_type in ('record', 'enum', 'fixed', 'array', 'map'):
            return 'avro'
        if schema_type in ('null', 'boolean', 'int', 'long', 'float', 'double', 'bytes', 'string'):
            return 'avro'
        # JSON Structure object type
        if schema_type == 'object' and 'properties' in schema:
            return 'jstruct'

    # Check if it's a union (list)
    if isinstance(schema, list):
        return 'avro'

    return 'unknown'


def validate_instance(
    instance: Any,
    schema: Dict[str, Any],
    schema_type: str = None
) -> ValidationResult:
    """Validates a JSON instance against a schema.

    Args:
        instance: The JSON value to validate
        schema: The schema (Avro or JSON Structure)
        schema_type: 'avro' or 'jstruct', auto-detected if not provided

    Returns:
        ValidationResult with validation status and any errors
    """
    if schema_type is None:
        schema_type = detect_schema_type(schema)

    if schema_type == 'avro':
        errors = validate_json_against_avro(instance, schema)
        return ValidationResult(is_valid=len(errors) == 0, errors=errors)

    elif schema_type == 'jstruct':
        if not HAS_JSTRUCT_SDK:
            return ValidationResult(
                is_valid=False,
                errors=["JSON Structure SDK not installed. Install with: pip install json-structure"]
            )
        try:
            validator = JStructInstanceValidator(schema)
            errors = validator.validate(instance)
            return ValidationResult(is_valid=len(errors) == 0, errors=errors if errors else [])
        except Exception as e:
            return ValidationResult(is_valid=False, errors=[str(e)])

    else:
        return ValidationResult(
            is_valid=False,
            errors=[f"Unknown schema type. Cannot auto-detect schema format."]
        )


def validate_file(
    instance_file: str,
    schema_file: str,
    schema_type: str = None
) -> List[ValidationResult]:
    """Validates JSON instance file(s) against a schema file.

    Args:
        instance_file: Path to JSON file (single object, array, or JSONL)
        schema_file: Path to schema file (.avsc or .jstruct.json)
        schema_type: 'avro' or 'jstruct', auto-detected if not provided

    Returns:
        List of ValidationResult for each instance in the file
    """
    # Load schema
    with open(schema_file, 'r', encoding='utf-8') as f:
        schema = json.load(f)

    # Auto-detect schema type from file extension if not provided
    if schema_type is None:
        if schema_file.endswith('.avsc'):
            schema_type = 'avro'
        elif schema_file.endswith('.jstruct.json') or schema_file.endswith('.jstruct'):
            schema_type = 'jstruct'
        else:
            schema_type = detect_schema_type(schema)

    # Load instances
    with open(instance_file, 'r', encoding='utf-8') as f:
        content = f.read().strip()

    instances = []
    instance_paths = []

    # Check if schema expects an array at the root
    schema_is_array = isinstance(schema, dict) and schema.get('type') == 'array'

    # Try as JSON array or object
    try:
        data = json.loads(content)
        if isinstance(data, list):
            if schema_is_array:
                # Schema expects an array, so validate the whole array as one instance
                instances = [data]
                instance_paths = [instance_file]
            else:
                # Schema expects individual items, validate each array element
                instances = data
                instance_paths = [f"{instance_file}[{i}]" for i in range(len(data))]
        else:
            instances = [data]
            instance_paths = [instance_file]
    except json.JSONDecodeError:
        # Try as JSONL
        for i, line in enumerate(content.split('\n')):
            line = line.strip()
            if line:
                try:
                    instances.append(json.loads(line))
                    instance_paths.append(f"{instance_file}:{i+1}")
                except json.JSONDecodeError:
                    pass

    # Validate each instance
    results = []
    for instance, path in zip(instances, instance_paths):
        result = validate_instance(instance, schema, schema_type)
        result.instance_path = path
        results.append(result)

    return results


def validate_json_instances(
    input_files: List[str],
    schema_file: str,
    schema_type: str = None,
    verbose: bool = False
) -> Tuple[int, int]:
    """Validates multiple JSON instance files against a schema.

    Args:
        input_files: List of JSON file paths to validate
        schema_file: Path to schema file
        schema_type: 'avro' or 'jstruct', auto-detected if not provided
        verbose: Whether to print validation results

    Returns:
        Tuple of (valid_count, invalid_count)
    """
    valid_count = 0
    invalid_count = 0

    for input_file in input_files:
        results = validate_file(input_file, schema_file, schema_type)
        for result in results:
            if result.is_valid:
                valid_count += 1
                if verbose:
                    print(result)
            else:
                invalid_count += 1
                if verbose:
                    print(result)

    return valid_count, invalid_count


# Command entry point for avrotize CLI
def validate(
    input: List[str],
    schema: str,
    schema_type: str = None,
    quiet: bool = False
) -> None:
    """Validates JSON instances against an Avro or JSON Structure schema.

    Args:
        input: List of JSON files to validate
        schema: Path to schema file (.avsc or .jstruct.json)
        schema_type: Schema type ('avro' or 'jstruct'), auto-detected if not provided
        quiet: Suppress output, exit with code 0 if valid, 1 if invalid
    """
    valid_count, invalid_count = validate_json_instances(
        input_files=input,
        schema_file=schema,
        schema_type=schema_type,
        verbose=not quiet
    )

    if not quiet:
        total = valid_count + invalid_count
        print(f"\nValidation summary: {valid_count}/{total} instances valid")

    if invalid_count > 0:
        exit(1)
