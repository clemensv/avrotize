"""
Tests for JSON Structure to JavaScript conversion
"""
import unittest
import os
import shutil
import sys
import tempfile
import json

import pytest

from avrotize.structuretojs import convert_structure_to_javascript

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)


class TestStructureToJavaScript(unittest.TestCase):
    """Test cases for JSON Structure to JavaScript conversion"""

    def run_convert_struct_to_js(self, struct_name, avro_annotation=False, base_package=None):
        """Test converting a JSON Structure file to JavaScript"""
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "jsons", struct_name + ".struct.json")
        js_path = os.path.join(tempfile.gettempdir(), "avrotize", struct_name + "-js")
        if os.path.exists(js_path):
            shutil.rmtree(js_path, ignore_errors=True)
        os.makedirs(js_path, exist_ok=True)

        kwargs = {
            "avro_annotation": avro_annotation,
        }
        if base_package:
            kwargs["package_name"] = base_package

        convert_structure_to_javascript(struct_path, js_path, **kwargs)
        
        # Verify files were created
        self.assertTrue(os.path.exists(js_path))
        js_files = []
        for root, dirs, files in os.walk(js_path):
            for file in files:
                if file.endswith('.js'):
                    js_files.append(os.path.join(root, file))
        self.assertGreater(len(js_files), 0, f"No JavaScript files generated for {struct_name}")
        
        return js_path

    def test_convert_address_struct_to_javascript(self):
        """Test converting address.struct.json to JavaScript"""
        self.run_convert_struct_to_js("address-ref")

    def test_convert_person_struct_to_javascript(self):
        """Test converting person.struct.json to JavaScript"""
        self.run_convert_struct_to_js("person-ref")

    def test_convert_product_struct_to_javascript(self):
        """Test converting a simple struct to JavaScript"""
        # Use a simple struct that exists
        self.run_convert_struct_to_js("anyof-ref")

    def test_convert_primitives_struct_to_javascript(self):
        """Test converting a schema with all primitive types to JavaScript"""
        # Create a test schema with all primitive types
        test_schema = {
            "type": "object",
            "name": "AllPrimitives",
            "properties": {
                "stringField": {"type": "string"},
                "intField": {"type": "int32"},
                "longField": {"type": "int64"},
                "floatField": {"type": "float"},
                "doubleField": {"type": "double"},
                "booleanField": {"type": "boolean"},
                "binaryField": {"type": "binary"},
                "dateField": {"type": "date"},
                "timeField": {"type": "time"},
                "datetimeField": {"type": "datetime"},
                "uuidField": {"type": "uuid"},
                "uriField": {"type": "uri"},
                "decimalField": {"type": "decimal"},
                "nullField": {"type": "null"}
            }
        }
        
        js_path = os.path.join(tempfile.gettempdir(), "avrotize", "primitives-js")
        if os.path.exists(js_path):
            shutil.rmtree(js_path, ignore_errors=True)
        os.makedirs(js_path, exist_ok=True)
        
        from avrotize.structuretojs import convert_structure_schema_to_javascript
        convert_structure_schema_to_javascript(test_schema, js_path, "test")
        
        # Verify the class file was created
        class_file = os.path.join(js_path, "test", "AllPrimitives.js")
        self.assertTrue(os.path.exists(class_file))
        
        # Read and verify the generated code
        with open(class_file, 'r') as f:
            content = f.read()
            self.assertIn("class AllPrimitives", content)
            self.assertIn("stringField", content)
            self.assertIn("module.exports = AllPrimitives", content)

    def test_convert_array_types_to_javascript(self):
        """Test converting array and collection types to JavaScript"""
        test_schema = {
            "type": "object",
            "name": "Collections",
            "properties": {
                "arrayField": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "setField": {
                    "type": "set",
                    "items": {"type": "int32"}
                },
                "mapField": {
                    "type": "map",
                    "values": {"type": "string"}
                }
            }
        }
        
        js_path = os.path.join(tempfile.gettempdir(), "avrotize", "collections-js")
        if os.path.exists(js_path):
            shutil.rmtree(js_path, ignore_errors=True)
        os.makedirs(js_path, exist_ok=True)
        
        from avrotize.structuretojs import convert_structure_schema_to_javascript
        convert_structure_schema_to_javascript(test_schema, js_path, "test")
        
        # Verify the class file was created
        class_file = os.path.join(js_path, "test", "Collections.js")
        self.assertTrue(os.path.exists(class_file))
        
        with open(class_file, 'r') as f:
            content = f.read()
            self.assertIn("arrayField", content)
            self.assertIn("setField", content)
            self.assertIn("mapField", content)

    def test_convert_enum_to_javascript(self):
        """Test converting enum types to JavaScript"""
        test_schema = {
            "type": "object",
            "name": "WithEnum",
            "properties": {
                "status": {
                    "enum": ["active", "inactive", "pending"],
                    "type": "string"
                }
            }
        }
        
        js_path = os.path.join(tempfile.gettempdir(), "avrotize", "enum-js")
        if os.path.exists(js_path):
            shutil.rmtree(js_path, ignore_errors=True)
        os.makedirs(js_path, exist_ok=True)
        
        from avrotize.structuretojs import convert_structure_schema_to_javascript
        convert_structure_schema_to_javascript(test_schema, js_path, "test")
        
        # Verify files were created
        self.assertTrue(os.path.exists(os.path.join(js_path, "test")))

    def test_convert_optional_fields_to_javascript(self):
        """Test converting optional/required fields to JavaScript"""
        test_schema = {
            "type": "object",
            "name": "OptionalFields",
            "properties": {
                "requiredField": {"type": "string"},
                "optionalField": {"type": "string"}
            },
            "required": ["requiredField"]
        }
        
        js_path = os.path.join(tempfile.gettempdir(), "avrotize", "optional-js")
        if os.path.exists(js_path):
            shutil.rmtree(js_path, ignore_errors=True)
        os.makedirs(js_path, exist_ok=True)
        
        from avrotize.structuretojs import convert_structure_schema_to_javascript
        convert_structure_schema_to_javascript(test_schema, js_path, "test")
        
        class_file = os.path.join(js_path, "test", "OptionalFields.js")
        self.assertTrue(os.path.exists(class_file))

    def test_convert_nested_objects_to_javascript(self):
        """Test converting nested object types to JavaScript"""
        test_schema = {
            "type": "object",
            "name": "Outer",
            "properties": {
                "inner": {
                    "type": "object",
                    "name": "Inner",
                    "properties": {
                        "value": {"type": "string"}
                    }
                }
            }
        }
        
        js_path = os.path.join(tempfile.gettempdir(), "avrotize", "nested-js")
        if os.path.exists(js_path):
            shutil.rmtree(js_path, ignore_errors=True)
        os.makedirs(js_path, exist_ok=True)
        
        from avrotize.structuretojs import convert_structure_schema_to_javascript
        convert_structure_schema_to_javascript(test_schema, js_path, "test")
        
        # Verify both files were created
        self.assertTrue(os.path.exists(os.path.join(js_path, "test")))

    def test_convert_with_defaults_to_javascript(self):
        """Test converting fields with default values to JavaScript"""
        test_schema = {
            "type": "object",
            "name": "WithDefaults",
            "properties": {
                "stringWithDefault": {
                    "type": "string",
                    "default": "default_value"
                },
                "intWithDefault": {
                    "type": "int32",
                    "default": 42
                },
                "boolWithDefault": {
                    "type": "boolean",
                    "default": True
                }
            }
        }
        
        js_path = os.path.join(tempfile.gettempdir(), "avrotize", "defaults-js")
        if os.path.exists(js_path):
            shutil.rmtree(js_path, ignore_errors=True)
        os.makedirs(js_path, exist_ok=True)
        
        from avrotize.structuretojs import convert_structure_schema_to_javascript
        convert_structure_schema_to_javascript(test_schema, js_path, "test")
        
        class_file = os.path.join(js_path, "test", "WithDefaults.js")
        self.assertTrue(os.path.exists(class_file))
        
        with open(class_file, 'r') as f:
            content = f.read()
            self.assertIn('"default_value"', content)
            self.assertIn('42', content)
            self.assertIn('true', content)

    def test_convert_with_const_fields_to_javascript(self):
        """Test converting const fields to JavaScript"""
        test_schema = {
            "type": "object",
            "name": "WithConst",
            "properties": {
                "version": {
                    "type": "string",
                    "const": "1.0.0"
                }
            }
        }
        
        js_path = os.path.join(tempfile.gettempdir(), "avrotize", "const-js")
        if os.path.exists(js_path):
            shutil.rmtree(js_path, ignore_errors=True)
        os.makedirs(js_path, exist_ok=True)
        
        from avrotize.structuretojs import convert_structure_schema_to_javascript
        convert_structure_schema_to_javascript(test_schema, js_path, "test")
        
        class_file = os.path.join(js_path, "test", "WithConst.js")
        self.assertTrue(os.path.exists(class_file))
        
        with open(class_file, 'r') as f:
            content = f.read()
            self.assertIn('static version', content)
            self.assertIn('"1.0.0"', content)


if __name__ == '__main__':
    unittest.main()
