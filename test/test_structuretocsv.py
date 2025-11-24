"""
Tests for JSON Structure to CSV schema conversion
"""
import json
import os
import tempfile
import unittest
from os import path, getcwd
from jsoncomparison import NO_DIFF, Compare

from avrotize.structuretocsv import convert_structure_to_csv_schema


class TestStructureToCSV(unittest.TestCase):
    """Test cases for JSON Structure to CSV schema conversion"""

    def _convert_and_compare(self, struct_file: str) -> None:
        """
        Helper that converts a JSON Structure schema to CSV schema and compares with reference.
        
        Args:
            struct_file: Name of the structure file (without extension) in test/struct
        """
        cwd = getcwd()
        struct_full_path = path.join(cwd, "test", "struct", f"{struct_file}.struct.json")
        csv_full_path = path.join(tempfile.gettempdir(), "avrotize", f"{struct_file}.csv.json")
        csv_ref_path = path.join(cwd, "test", "struct", f"{struct_file}-csv-ref.json")

        # Ensure destination directory exists
        os.makedirs(os.path.dirname(csv_full_path), exist_ok=True)

        # Perform conversion
        convert_structure_to_csv_schema(struct_full_path, csv_full_path)

        # Validate JSON is syntactically correct
        with open(csv_full_path, "r", encoding="utf-8") as file:
            csv_schema = json.load(file)

        # Basic validation
        self.assertIn("fields", csv_schema, "CSV schema must have 'fields' property")
        self.assertIsInstance(csv_schema["fields"], list, "'fields' must be a list")

        # Validate each field has required properties
        for field in csv_schema["fields"]:
            self.assertIn("name", field, "Each field must have a 'name'")
            self.assertIn("type", field, "Each field must have a 'type'")
            self.assertIsInstance(field["name"], str, "Field name must be a string")
            self.assertIsInstance(field["type"], str, "Field type must be a string")

        # Compare with reference file if it exists
        if os.path.exists(csv_ref_path):
            with open(csv_ref_path, "r", encoding="utf-8") as ref_file:
                expected = json.load(ref_file)
            
            diff = Compare().check(csv_schema, expected)
            self.assertEqual(diff, NO_DIFF, f"Differences found for {struct_file}")
        else:
            self.fail(f"Reference file not found: {csv_ref_path}")

    def test_basic_types(self):
        """Test conversion of basic JSON Structure types"""
        self._convert_and_compare("basic-types")

    def test_numeric_types(self):
        """Test conversion of numeric types"""
        self._convert_and_compare("numeric-types")

    def test_temporal_types(self):
        """Test conversion of temporal/date-time types"""
        self._convert_and_compare("temporal-types")

    def test_collections(self):
        """Test conversion of collection types (array, set, map)"""
        self._convert_and_compare("collections")

    def test_nested_objects(self):
        """Test conversion of nested object structures"""
        self._convert_and_compare("nested-objects")

    def test_choice_types(self):
        """Test conversion of choice/union types"""
        self._convert_and_compare("choice-types")

    def test_extensions(self):
        """Test conversion with $extends and inheritance"""
        self._convert_and_compare("extensions")

    def test_validation_constraints(self):
        """Test that validation constraints are preserved"""
        self._convert_and_compare("validation-constraints")

    def test_complex_scenario(self):
        """Test conversion of a complex scenario with multiple features"""
        self._convert_and_compare("complex-scenario")

    def test_edge_cases(self):
        """Test conversion of edge cases"""
        self._convert_and_compare("edge-cases")


if __name__ == '__main__':
    unittest.main()
