"""Tests for JSON Structure to Iceberg schema conversion."""

import os
import sys
import tempfile
import unittest
import pyarrow as pa

# Ensure the project root is in the system path for imports
current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

from avrotize.structuretoiceberg import convert_structure_to_iceberg


class TestStructureToIceberg(unittest.TestCase):
    """Test cases for JSON Structure to Iceberg conversion."""

    def _compare_iceberg_schemas(self, expected_path: str, actual_path: str) -> None:
        """
        Compare two Iceberg schema files.
        
        Args:
            expected_path: Path to the reference Iceberg schema file
            actual_path: Path to the generated Iceberg schema file
        """
        # Read both files as PyArrow schemas
        with open(expected_path, 'rb') as f:
            expected_schema = pa.ipc.read_schema(f)
        
        with open(actual_path, 'rb') as f:
            actual_schema = pa.ipc.read_schema(f)
        
        # Compare the schemas using PyArrow's equals method
        # Note: This compares field names, types, and nullability but ignores field metadata (like field IDs)
        self.assertTrue(
            expected_schema.equals(actual_schema),
            f"Schema mismatch:\nExpected: {expected_schema}\nActual: {actual_schema}"
        )

    def _convert_and_validate(
        self,
        struct_file: str,
        record_type: str | None = None,
        emit_cloudevents: bool = False
    ) -> None:
        """
        Helper that converts a JSON Structure schema to Iceberg, and compares
        it with a reference file if present.
        
        Args:
            struct_file: Name of the JSON Structure file in test/struct/
            record_type: Optional specific record type to convert
            emit_cloudevents: Whether to emit CloudEvents columns
        """
        cwd = os.getcwd()
        struct_full_path = os.path.join(cwd, "test", "struct", struct_file)
        iceberg_file = struct_file.replace(".struct.json", ".iceberg")
        iceberg_full_path = os.path.join(tempfile.gettempdir(), "avrotize", iceberg_file)

        # Ensure destination directory exists
        os.makedirs(os.path.dirname(iceberg_full_path), exist_ok=True)

        # Perform conversion with arrow format for binary comparison with reference files
        convert_structure_to_iceberg(
            struct_full_path,
            record_type,
            iceberg_full_path,
            emit_cloudevents,
            output_format="arrow"
        )

        # If a reference file exists, compare generated output against it
        ref_path = struct_full_path.replace(".struct.json", ".struct-ref.iceberg")
        if os.path.exists(ref_path):
            self._compare_iceberg_schemas(ref_path, iceberg_full_path)

    def test_basic_types(self):
        """Test conversion of basic JSON Structure types."""
        self._convert_and_validate("basic-types.struct.json")

    def test_choice_types(self):
        """Test conversion of choice types (unions)."""
        self._convert_and_validate("choice-types.struct.json")

    def test_collections(self):
        """Test conversion of collection types (array, set, map)."""
        self._convert_and_validate("collections.struct.json")

    def test_complex_scenario(self):
        """Test conversion of a complex schema with nested structures."""
        self._convert_and_validate("complex-scenario.struct.json")

    def test_edge_cases(self):
        """Test conversion of edge cases (empty strings, deep nesting, etc.)."""
        self._convert_and_validate("edge-cases.struct.json")

    def test_extensions(self):
        """Test conversion of schemas with $extends."""
        self._convert_and_validate("extensions.struct.json")

    def test_nested_objects(self):
        """Test conversion of nested object structures."""
        self._convert_and_validate("nested-objects.struct.json")

    def test_numeric_types(self):
        """Test conversion of various numeric types."""
        self._convert_and_validate("numeric-types.struct.json")

    def test_temporal_types(self):
        """Test conversion of temporal types (date, time, datetime, etc.)."""
        self._convert_and_validate("temporal-types.struct.json")

    def test_validation_constraints(self):
        """Test conversion of schemas with validation constraints."""
        self._convert_and_validate("validation-constraints.struct.json")

    def test_with_cloudevents_columns(self):
        """Test conversion with CloudEvents columns enabled."""
        cwd = os.getcwd()
        struct_full_path = os.path.join(cwd, "test", "struct", "basic-types.struct.json")
        iceberg_full_path = os.path.join(
            tempfile.gettempdir(), "avrotize", "basic-types-cloudevents.iceberg"
        )

        # Ensure destination directory exists
        os.makedirs(os.path.dirname(iceberg_full_path), exist_ok=True)

        # Perform conversion with CloudEvents columns (using arrow format for binary output)
        convert_structure_to_iceberg(struct_full_path, None, iceberg_full_path, True, output_format="arrow")

        # Read the generated schema
        with open(iceberg_full_path, 'rb') as f:
            schema = pa.ipc.read_schema(f)

        # Verify CloudEvents columns are present
        field_names = [field.name for field in schema]
        self.assertIn("___type", field_names)
        self.assertIn("___source", field_names)
        self.assertIn("___id", field_names)
        self.assertIn("___time", field_names)
        self.assertIn("___subject", field_names)


if __name__ == '__main__':
    unittest.main()
