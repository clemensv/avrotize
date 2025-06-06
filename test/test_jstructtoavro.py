"""
SPDX-FileCopyrightText: 2025-present Avrotize contributors
SPDX-License-Identifier: Apache-2.0
"""

import os
import sys
import json
import tempfile
from os import path, getcwd
from fastavro.schema import load_schema
from jsoncomparison import NO_DIFF, Compare
import unittest

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

# Import directly from the module to avoid dependency on package integration
from avrotize.jstructtoavro import convert_json_structure_to_avro

class TestJStructToAvro(unittest.TestCase):
    def validate_avro_schema(self, avro_file_path):
        load_schema(avro_file_path)

    def create_avro_from_jstruct(self, jstruct_path, avro_path='', avro_ref_path='', namespace="com.test.example"):
        cwd = getcwd()
        if jstruct_path.startswith("http"):
            jsons_full_path = jstruct_path
        else:
            jsons_full_path = path.join(cwd, "test", "jstruct", jstruct_path)
        
        if not avro_path:
            avro_path = jstruct_path.replace(".struct.json", ".avsc")
        
        if not avro_ref_path:
            # '-ref' appended to the avsc base file name
            avro_ref_full_path = path.join(cwd, "test", "jstruct", avro_path.replace(".avsc", "-ref.avsc"))
        else:
            avro_ref_full_path = path.join(cwd, "test", "jstruct", avro_ref_path)
        
        avro_full_path = path.join(tempfile.gettempdir(), "avrotize", avro_path)
        dir_path = os.path.dirname(avro_full_path)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)

        convert_json_structure_to_avro(jsons_full_path, avro_full_path)
        self.validate_avro_schema(avro_full_path)

        # Create reference file if it doesn't exist
        if not os.path.exists(avro_ref_full_path):
            with open(avro_full_path, "r") as src:
                content = src.read()
            with open(avro_ref_full_path, "w") as dest:
                dest.write(content)
            print(f"Created reference file: {avro_ref_full_path}")
        else:
            with open(avro_ref_full_path, "r") as ref:
                expected = json.loads(ref.read())
            with open(avro_full_path, "r") as result:
                actual = json.loads(result.read())
            diff = Compare().check(actual, expected)
            assert diff == NO_DIFF
            
    def test_minimal_object(self):
        """Test basic object conversion"""
        self.create_avro_from_jstruct("minimal.struct.json")
        
    def test_primitive_types(self):
        """Test all primitive type mappings"""
        self.create_avro_from_jstruct("primitive_types.struct.json")
        
    def test_array_type(self):
        """Test array type conversion"""
        self.create_avro_from_jstruct("array_type.struct.json")

    def test_map_type(self):
        """Test map type conversion"""
        self.create_avro_from_jstruct("map_type.struct.json")
        
    def test_enum_type(self):
        """Test enum type conversion"""
        self.create_avro_from_jstruct("enum_type.struct.json")

    def test_logical_type_decimal(self):
        """Test decimal logical type conversion"""
        self.create_avro_from_jstruct("logical_decimal.struct.json")

    def test_choice_type_union(self):
        """Test choice type to union conversion"""
        self.create_avro_from_jstruct("choice_type.struct.json")

    def test_union_type_direct(self):
        """Test direct union array conversion"""
        self.create_avro_from_jstruct("union_type.struct.json")

    def test_optional_fields(self):
        """Test optional field handling with null unions"""
        self.create_avro_from_jstruct("optional_fields.struct.json")

    def test_nested_records(self):
        """Test nested record structures"""
        self.create_avro_from_jstruct("nested_records.struct.json")

    def test_references_with_definitions(self):
        """Test $ref resolution with definitions"""
        self.create_avro_from_jstruct("reference_types.struct.json")

    def test_name_sanitization(self):
        """Test invalid name sanitization"""
        self.create_avro_from_jstruct("name_sanitization.struct.json")

    def test_cli_command_integration(self):
        """Test the CLI command integration"""
        self.create_avro_from_jstruct("cli_test.struct.json")

if __name__ == "__main__":
    unittest.main()
