"""Tests for JSON Structure to Markdown conversion."""

import json
import os
import tempfile
import unittest
from os import path, getcwd

from avrotize.structuretomd import convert_structure_to_markdown


class TestStructureToMarkdown(unittest.TestCase):
    """Tests converting JSON Structure schemas to Markdown documentation."""

    def _convert_and_validate(self, struct_file: str, md_file: str | None = None) -> None:
        """
        Helper that converts a JSON Structure schema to Markdown and compares it
        with a reference file if present.
        """
        cwd = getcwd()
        if md_file is None:
            md_file = struct_file.replace(".struct.json", ".struct-ref.md")

        struct_full_path = path.join(cwd, "test", "struct", struct_file)
        md_full_path = path.join(tempfile.gettempdir(), "avrotize", md_file)

        # Ensure destination directory exists
        os.makedirs(os.path.dirname(md_full_path), exist_ok=True)

        # Perform conversion
        convert_structure_to_markdown(struct_full_path, md_full_path)

        # Verify the output file was created
        self.assertTrue(os.path.exists(md_full_path), f"Output file {md_full_path} was not created")

        # If a reference file exists, compare generated output against it
        ref_path = struct_full_path.replace(".struct.json", ".struct-ref.md")
        if os.path.exists(ref_path):
            with open(ref_path, "r", encoding="utf-8") as ref_file:
                expected = ref_file.read()
            with open(md_full_path, "r", encoding="utf-8") as gen_file:
                actual = gen_file.read()

            self.assertEqual(actual, expected, f"Differences found for {struct_file}")
        else:
            # If no reference file, just validate that we produced valid markdown
            with open(md_full_path, "r", encoding="utf-8") as gen_file:
                content = gen_file.read()
                self.assertGreater(len(content), 0, "Generated markdown is empty")

    def test_basic_types_conversion(self):
        """Test converting basic-types.struct.json to Markdown"""
        self._convert_and_validate("basic-types.struct.json")

    def test_choice_types_conversion(self):
        """Test converting choice-types.struct.json to Markdown"""
        self._convert_and_validate("choice-types.struct.json")

    def test_collections_conversion(self):
        """Test converting collections.struct.json to Markdown"""
        self._convert_and_validate("collections.struct.json")

    def test_complex_scenario_conversion(self):
        """Test converting complex-scenario.struct.json to Markdown"""
        self._convert_and_validate("complex-scenario.struct.json")

    def test_edge_cases_conversion(self):
        """Test converting edge-cases.struct.json to Markdown"""
        self._convert_and_validate("edge-cases.struct.json")

    def test_extensions_conversion(self):
        """Test converting extensions.struct.json to Markdown"""
        self._convert_and_validate("extensions.struct.json")

    def test_nested_objects_conversion(self):
        """Test converting nested-objects.struct.json to Markdown"""
        self._convert_and_validate("nested-objects.struct.json")

    def test_numeric_types_conversion(self):
        """Test converting numeric-types.struct.json to Markdown"""
        self._convert_and_validate("numeric-types.struct.json")

    def test_temporal_types_conversion(self):
        """Test converting temporal-types.struct.json to Markdown"""
        self._convert_and_validate("temporal-types.struct.json")

    def test_validation_constraints_conversion(self):
        """Test converting validation-constraints.struct.json to Markdown"""
        self._convert_and_validate("validation-constraints.struct.json")

    def test_descriptions_test_conversion(self):
        """Test converting descriptions-test.struct.json to Markdown - comprehensive description coverage"""
        self._convert_and_validate("descriptions-test.struct.json")


if __name__ == '__main__':
    unittest.main()
