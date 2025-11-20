"""Tests for JSON Structure to TypeScript conversion."""

import unittest
import os
import shutil
import sys
import tempfile
import json
from os import path, getcwd

import pytest

from avrotize.structuretots import convert_structure_to_typescript

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)


class TestStructureToTypeScript(unittest.TestCase):
    """Test cases for JSON Structure to TypeScript conversion."""

    def run_convert_struct_to_ts(
        self,
        struct_name,
        typedjson_annotation=False,
        avro_annotation=False,
        package_name='',
    ):
        """Helper method to convert a JSON Structure file to TypeScript"""
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "struct", struct_name + ".struct.json")
        ts_path = os.path.join(tempfile.gettempdir(), "avrotize", struct_name + "-ts")
        if os.path.exists(ts_path):
            shutil.rmtree(ts_path, ignore_errors=True)
        os.makedirs(ts_path, exist_ok=True)

        convert_structure_to_typescript(
            struct_path,
            ts_path,
            package_name=package_name or struct_name,
            typed_json_annotation=typedjson_annotation,
            avro_annotation=avro_annotation
        )

        # Verify basic structure
        assert os.path.exists(os.path.join(ts_path, 'src'))
        assert os.path.exists(os.path.join(ts_path, 'tsconfig.json'))
        assert os.path.exists(os.path.join(ts_path, 'package.json'))
        
        # Verify index file was created
        assert os.path.exists(os.path.join(ts_path, 'src', 'index.ts'))

    def test_convert_basic_types_struct_to_typescript(self):
        """Test converting basic-types.struct.json to TypeScript"""
        self.run_convert_struct_to_ts("basic-types")

    def test_convert_basic_types_struct_to_typescript_typedjson(self):
        """Test converting basic-types.struct.json to TypeScript with TypedJSON annotations"""
        self.run_convert_struct_to_ts("basic-types", typedjson_annotation=True)

    def test_convert_collections_struct_to_typescript(self):
        """Test converting collections.struct.json to TypeScript"""
        self.run_convert_struct_to_ts("collections")

    def test_convert_nested_objects_struct_to_typescript(self):
        """Test converting nested-objects.struct.json to TypeScript"""
        self.run_convert_struct_to_ts("nested-objects")

    def test_convert_numeric_types_struct_to_typescript(self):
        """Test converting numeric-types.struct.json to TypeScript"""
        self.run_convert_struct_to_ts("numeric-types")

    def test_convert_temporal_types_struct_to_typescript(self):
        """Test converting temporal-types.struct.json to TypeScript"""
        self.run_convert_struct_to_ts("temporal-types")

    def test_convert_choice_types_struct_to_typescript(self):
        """Test converting choice-types.struct.json to TypeScript"""
        self.run_convert_struct_to_ts("choice-types")

    def test_convert_complex_scenario_struct_to_typescript(self):
        """Test converting complex-scenario.struct.json to TypeScript"""
        self.run_convert_struct_to_ts("complex-scenario")

    def test_convert_edge_cases_struct_to_typescript(self):
        """Test converting edge-cases.struct.json to TypeScript"""
        self.run_convert_struct_to_ts("edge-cases")

    def test_convert_validation_constraints_struct_to_typescript(self):
        """Test converting validation-constraints.struct.json to TypeScript"""
        self.run_convert_struct_to_ts("validation-constraints")


if __name__ == '__main__':
    unittest.main()
