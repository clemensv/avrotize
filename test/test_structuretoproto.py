"""Tests for JSON Structure to Protocol Buffers conversion."""

import unittest
import os
import shutil
import sys
import tempfile
import json

import pytest

from avrotize.structuretoproto import convert_structure_to_proto

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)


class TestStructureToProto(unittest.TestCase):
    """Test cases for JSON Structure to Protocol Buffers conversion."""

    def run_convert_struct_to_proto(self, struct_name):
        """Helper method to convert a JSON Structure file to Proto"""
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "struct", struct_name + ".struct.json")
        proto_path = os.path.join(tempfile.gettempdir(), "avrotize", struct_name + "-proto")
        
        if os.path.exists(proto_path):
            shutil.rmtree(proto_path, ignore_errors=True)
        os.makedirs(proto_path, exist_ok=True)

        convert_structure_to_proto(struct_path, proto_path)

        # Verify that proto files were created
        proto_files = [f for f in os.listdir(proto_path) if f.endswith('.proto')]
        assert len(proto_files) > 0, "No proto files were generated"

        # Read and verify proto file content
        for proto_file in proto_files:
            proto_file_path = os.path.join(proto_path, proto_file)
            with open(proto_file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                # Verify basic proto structure
                assert 'syntax = "proto3";' in content, "Missing proto3 syntax declaration"
                print(f"Generated proto file: {proto_file}")
                print(content)
                print("-" * 80)

        return proto_path

    def test_convert_basic_types_to_proto(self):
        """Test converting basic types from JSON Structure to Proto"""
        self.run_convert_struct_to_proto("basic-types")

    def test_convert_collections_to_proto(self):
        """Test converting collection types from JSON Structure to Proto"""
        self.run_convert_struct_to_proto("collections")

    def test_convert_choice_types_to_proto(self):
        """Test converting choice (union) types from JSON Structure to Proto"""
        self.run_convert_struct_to_proto("choice-types")

    def test_convert_complex_scenario_to_proto(self):
        """Test converting complex scenario from JSON Structure to Proto"""
        # Skip if file doesn't exist
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "struct", "complex-scenario.struct.json")
        if os.path.exists(struct_path):
            self.run_convert_struct_to_proto("complex-scenario")
        else:
            print(f"Skipping test_convert_complex_scenario_to_proto - file not found")

    def test_convert_nested_objects_to_proto(self):
        """Test converting nested objects from JSON Structure to Proto"""
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "struct", "nested-objects.struct.json")
        if os.path.exists(struct_path):
            self.run_convert_struct_to_proto("nested-objects")
        else:
            print(f"Skipping test_convert_nested_objects_to_proto - file not found")


if __name__ == '__main__':
    unittest.main()
