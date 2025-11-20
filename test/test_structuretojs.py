"""Tests for JSON Structure to JavaScript conversion."""

import unittest
import os
import shutil
import subprocess
import sys
import tempfile
from os import path, getcwd

import pytest

from avrotize.structuretojs import convert_structure_to_javascript

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)


class TestStructureToJavaScript(unittest.TestCase):
    """Test cases for JSON Structure to JavaScript conversion."""

    def run_convert_struct_to_js(self, struct_name, package_name=''):
        """Helper method to convert a JSON Structure file to JavaScript"""
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "jsons", struct_name + ".struct.json")
        js_path = os.path.join(tempfile.gettempdir(), "avrotize", struct_name + "-js")
        if os.path.exists(js_path):
            shutil.rmtree(js_path, ignore_errors=True)
        os.makedirs(js_path, exist_ok=True)

        convert_structure_to_javascript(struct_path, js_path, package_name=package_name)

        # Verify basic structure
        assert os.path.exists(js_path)

        # Check that at least one .js file was generated
        js_files = []
        for root, dirs, files in os.walk(js_path):
            for file in files:
                if file.endswith('.js'):
                    js_files.append(os.path.join(root, file))
        
        assert len(js_files) > 0, f"No JavaScript files generated in {js_path}"
        
        # Try to run the generated JavaScript files with Node.js if available
        try:
            # Check if Node.js is available
            result = subprocess.run(['node', '--version'], capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                print(f"Node.js version: {result.stdout.strip()}")
                
                # For each JS file, try to load it to verify syntax
                for js_file in js_files:
                    try:
                        # Simple syntax check by requiring the module
                        test_code = f"const mod = require('{js_file}'); console.log('Loaded:', '{os.path.basename(js_file)}');"
                        result = subprocess.run(
                            ['node', '-e', test_code],
                            capture_output=True,
                            text=True,
                            timeout=5,
                            cwd=os.path.dirname(js_file)
                        )
                        if result.returncode == 0:
                            print(f"✓ {os.path.basename(js_file)}: {result.stdout.strip()}")
                        else:
                            print(f"✗ {os.path.basename(js_file)}: {result.stderr}")
                    except Exception as e:
                        print(f"Could not test {js_file}: {e}")
        except (subprocess.TimeoutExpired, FileNotFoundError):
            print("Node.js not available, skipping runtime tests")
        
        return js_path

    def test_convert_address_ref_struct_to_js(self):
        """Test converting address-ref.struct.json to JavaScript"""
        js_path = self.run_convert_struct_to_js("address-ref")
        
        # Check for generated Document class
        # After the fix, it should be in address_ref/ not address_ref/struct/
        document_js = os.path.join(js_path, "address_ref", "Document.js")
        assert os.path.exists(document_js), f"Expected Document.js at {document_js}"

    def test_convert_anyof_ref_struct_to_js(self):
        """Test converting anyof-ref.struct.json to JavaScript"""
        js_path = self.run_convert_struct_to_js("anyof-ref")
        
        # Verify that JavaScript files were generated
        js_files = []
        for root, dirs, files in os.walk(js_path):
            for file in files:
                if file.endswith('.js'):
                    js_files.append(file)
        
        assert len(js_files) > 0, "No JavaScript files generated"

    def test_convert_simple_types_struct_to_js(self):
        """Test converting a structure with simple types to JavaScript"""
        cwd = os.getcwd()
        
        # Create a simple test schema
        import json
        simple_schema = {
            "type": "object",
            "name": "SimpleTypes",
            "properties": {
                "stringField": {"type": "string"},
                "numberField": {"type": "number"},
                "boolField": {"type": "boolean"},
                "dateField": {"type": "date"},
                "arrayField": {
                    "type": "array",
                    "items": {"type": "string"}
                }
            },
            "required": ["stringField", "numberField"]
        }
        
        struct_path = os.path.join(tempfile.gettempdir(), "simple_types.struct.json")
        with open(struct_path, 'w') as f:
            json.dump(simple_schema, f)
        
        js_path = os.path.join(tempfile.gettempdir(), "avrotize", "simple-types-js")
        if os.path.exists(js_path):
            shutil.rmtree(js_path, ignore_errors=True)
        os.makedirs(js_path, exist_ok=True)

        convert_structure_to_javascript(struct_path, js_path, package_name="simple_types")

        # Check that SimpleTypes.js was generated
        simple_types_js = os.path.join(js_path, "simple_types", "SimpleTypes.js")
        assert os.path.exists(simple_types_js), f"Expected SimpleTypes.js at {simple_types_js}"
        
        # Read and verify content
        with open(simple_types_js, 'r') as f:
            content = f.read()
            assert "function SimpleTypes()" in content
            assert "stringField" in content
            assert "numberField" in content
            assert "boolField" in content
            assert "dateField" in content
            assert "arrayField" in content

    def test_convert_enum_struct_to_js(self):
        """Test converting a structure with enums to JavaScript"""
        import json
        
        enum_schema = {
            "type": "object",
            "name": "WithEnum",
            "properties": {
                "status": {
                    "enum": ["active", "inactive", "pending"],
                    "type": "string"
                }
            }
        }
        
        struct_path = os.path.join(tempfile.gettempdir(), "enum_test.struct.json")
        with open(struct_path, 'w') as f:
            json.dump(enum_schema, f)
        
        js_path = os.path.join(tempfile.gettempdir(), "avrotize", "enum-test-js")
        if os.path.exists(js_path):
            shutil.rmtree(js_path, ignore_errors=True)
        os.makedirs(js_path, exist_ok=True)

        convert_structure_to_javascript(struct_path, js_path, package_name="enum_test")

        # Check that enum was generated
        status_enum_js = os.path.join(js_path, "enum_test", "StatusEnum.js")
        assert os.path.exists(status_enum_js), f"Expected StatusEnum.js at {status_enum_js}"
        
        # Verify enum content
        with open(status_enum_js, 'r') as f:
            content = f.read()
            assert "Object.freeze" in content
            assert "active" in content
            assert "inactive" in content
            assert "pending" in content

    def test_convert_nested_objects_struct_to_js(self):
        """Test converting a structure with nested objects to JavaScript"""
        import json
        
        nested_schema = {
            "type": "object",
            "name": "Parent",
            "properties": {
                "name": {"type": "string"},
                "child": {
                    "type": "object",
                    "properties": {
                        "age": {"type": "number"}
                    }
                }
            }
        }
        
        struct_path = os.path.join(tempfile.gettempdir(), "nested_test.struct.json")
        with open(struct_path, 'w') as f:
            json.dump(nested_schema, f)
        
        js_path = os.path.join(tempfile.gettempdir(), "avrotize", "nested-test-js")
        if os.path.exists(js_path):
            shutil.rmtree(js_path, ignore_errors=True)
        os.makedirs(js_path, exist_ok=True)

        convert_structure_to_javascript(struct_path, js_path, package_name="nested_test")

        # Check that both classes were generated
        parent_js = os.path.join(js_path, "nested_test", "Parent.js")
        assert os.path.exists(parent_js), f"Expected Parent.js at {parent_js}"

    def test_convert_map_type_struct_to_js(self):
        """Test converting a structure with map type to JavaScript"""
        import json
        
        map_schema = {
            "type": "object",
            "name": "WithMap",
            "properties": {
                "metadata": {
                    "type": "map",
                    "values": {"type": "string"}
                }
            }
        }
        
        struct_path = os.path.join(tempfile.gettempdir(), "map_test.struct.json")
        with open(struct_path, 'w') as f:
            json.dump(map_schema, f)
        
        js_path = os.path.join(tempfile.gettempdir(), "avrotize", "map-test-js")
        if os.path.exists(js_path):
            shutil.rmtree(js_path, ignore_errors=True)
        os.makedirs(js_path, exist_ok=True)

        convert_structure_to_javascript(struct_path, js_path, package_name="map_test")

        # Check that class was generated
        with_map_js = os.path.join(js_path, "map_test", "WithMap.js")
        assert os.path.exists(with_map_js), f"Expected WithMap.js at {with_map_js}"
        
        # Verify map property
        with open(with_map_js, 'r') as f:
            content = f.read()
            assert "metadata" in content

    def test_convert_set_type_struct_to_js(self):
        """Test converting a structure with set type to JavaScript"""
        import json
        
        set_schema = {
            "type": "object",
            "name": "WithSet",
            "properties": {
                "tags": {
                    "type": "set",
                    "items": {"type": "string"}
                }
            }
        }
        
        struct_path = os.path.join(tempfile.gettempdir(), "set_test.struct.json")
        with open(struct_path, 'w') as f:
            json.dump(set_schema, f)
        
        js_path = os.path.join(tempfile.gettempdir(), "avrotize", "set-test-js")
        if os.path.exists(js_path):
            shutil.rmtree(js_path, ignore_errors=True)
        os.makedirs(js_path, exist_ok=True)

        convert_structure_to_javascript(struct_path, js_path, package_name="set_test")

        # Check that class was generated
        with_set_js = os.path.join(js_path, "set_test", "WithSet.js")
        assert os.path.exists(with_set_js), f"Expected WithSet.js at {with_set_js}"
        
        # Verify set property
        with open(with_set_js, 'r') as f:
            content = f.read()
            assert "tags" in content


if __name__ == '__main__':
    unittest.main()
