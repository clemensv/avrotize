"""
Tests for JSON Structure to C++ conversion
"""
import unittest
import os
import shutil
import subprocess
import sys
import tempfile

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

from avrotize.structuretocpp import convert_structure_to_cpp


class TestStructureToCpp(unittest.TestCase):
    """Test cases for JSON Structure to C++ conversion"""

    def run_convert_struct_to_cpp(
        self,
        struct_name,
        json_annotation=True,
        namespace=None,
    ):
        """Test converting a JSON Structure file to C++"""
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "jsons", struct_name + ".struct.json")
        cpp_path = os.path.join(tempfile.gettempdir(), "avrotize", struct_name + "-cpp")
        if os.path.exists(cpp_path):
            shutil.rmtree(cpp_path, ignore_errors=True)
        os.makedirs(cpp_path, exist_ok=True)

        kwargs = {
            "json_annotation": json_annotation,
        }
        if namespace is not None:
            kwargs["namespace"] = namespace

        convert_structure_to_cpp(struct_path, cpp_path, **kwargs)
        
        # Verify files were generated
        include_dir = os.path.join(cpp_path, "include")
        self.assertTrue(os.path.exists(include_dir), f"Include directory not found at {include_dir}")
        
        # Check for CMakeLists.txt
        cmake_file = os.path.join(cpp_path, "CMakeLists.txt")
        self.assertTrue(os.path.exists(cmake_file), f"CMakeLists.txt not found at {cmake_file}")
        
        # Check for vcpkg.json
        vcpkg_file = os.path.join(cpp_path, "vcpkg.json")
        self.assertTrue(os.path.exists(vcpkg_file), f"vcpkg.json not found at {vcpkg_file}")

    def test_convert_address_struct_to_cpp(self):
        """Test converting address-ref.struct.json to C++"""
        self.run_convert_struct_to_cpp("address-ref", json_annotation=True, namespace="address")

    def test_convert_address_struct_to_cpp_no_json(self):
        """Test converting address-ref.struct.json to C++ without JSON annotation"""
        self.run_convert_struct_to_cpp("address-ref", json_annotation=False, namespace="address")

    def test_convert_arraydef_struct_to_cpp(self):
        """Test converting arraydef-ref.struct.json to C++"""
        self.run_convert_struct_to_cpp("arraydef-ref", json_annotation=True, namespace="arraydef")

    def test_convert_addlprops1_struct_to_cpp(self):
        """Test converting addlprops1-ref.struct.json to C++"""
        self.run_convert_struct_to_cpp("addlprops1-ref", json_annotation=True, namespace="addlprops1")

    def test_convert_multiple_types_struct_to_cpp(self):
        """Test converting a schema with multiple types"""
        # Create a simple test schema
        import json
        test_schema = {
            "$schema": "https://json-structure.org/meta/extended/v0/#",
            "type": "object",
            "name": "TestTypes",
            "properties": {
                "stringField": {"type": "string"},
                "intField": {"type": "int32"},
                "boolField": {"type": "boolean"},
                "floatField": {"type": "float"},
                "dateField": {"type": "date"},
                "uuidField": {"type": "uuid"},
                "binaryField": {"type": "binary"},
                "arrayField": {"type": "array", "items": {"type": "string"}},
                "mapField": {"type": "map", "values": {"type": "int32"}}
            },
            "required": ["stringField", "intField"]
        }
        
        cwd = os.getcwd()
        struct_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_types.struct.json")
        cpp_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_types-cpp")
        
        # Write test schema
        os.makedirs(os.path.dirname(struct_path), exist_ok=True)
        with open(struct_path, 'w') as f:
            json.dump(test_schema, f)
        
        # Clean up output directory
        if os.path.exists(cpp_path):
            shutil.rmtree(cpp_path, ignore_errors=True)
        os.makedirs(cpp_path, exist_ok=True)
        
        # Convert
        convert_structure_to_cpp(struct_path, cpp_path, namespace="testtypes", json_annotation=True)
        
        # Verify files were generated
        include_dir = os.path.join(cpp_path, "include")
        self.assertTrue(os.path.exists(include_dir), f"Include directory not found at {include_dir}")


if __name__ == '__main__':
    unittest.main()
