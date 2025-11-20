"""
Tests for JSON Structure to CSV schema conversion
"""
import json
import os
import tempfile
import unittest
from os import path, getcwd

from avrotize.structuretocsv import convert_structure_to_csv_schema


class TestStructureToCSV(unittest.TestCase):
    """Test cases for JSON Structure to CSV schema conversion"""

    def _convert_and_check(self, struct_file: str, expected_fields: list = None) -> dict:
        """
        Helper that converts a JSON Structure schema to CSV schema and validates the output.
        
        Args:
            struct_file: Name of the structure file in test/jsons
            expected_fields: Optional list of expected field names
            
        Returns:
            The generated CSV schema as a dict
        """
        cwd = getcwd()
        struct_full_path = path.join(cwd, "test", "jsons", struct_file)
        csv_full_path = path.join(tempfile.gettempdir(), "avrotize", struct_file.replace(".struct.json", ".csv.json"))

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

        # If expected fields provided, validate them
        if expected_fields:
            field_names = [field["name"] for field in csv_schema["fields"]]
            for expected_field in expected_fields:
                self.assertIn(expected_field, field_names, f"Expected field '{expected_field}' not found")

        # Validate each field has required properties
        for field in csv_schema["fields"]:
            self.assertIn("name", field, "Each field must have a 'name'")
            self.assertIn("type", field, "Each field must have a 'type'")
            self.assertIsInstance(field["name"], str, "Field name must be a string")
            self.assertIsInstance(field["type"], str, "Field type must be a string")

        return csv_schema

    def test_userprofile(self):
        """Test converting userprofile schema"""
        csv_schema = self._convert_and_check(
            "userprofile-ref.struct.json",
            expected_fields=["username", "email", "age", "fullName", "interests", "location"]
        )
        
        # Verify specific field types
        fields_by_name = {field["name"]: field for field in csv_schema["fields"]}
        
        # Check username (required string)
        self.assertEqual(fields_by_name["username"]["type"], "string")
        self.assertNotIn("nullable", fields_by_name["username"])  # Required fields shouldn't be marked nullable
        
        # Check email (required string)
        self.assertEqual(fields_by_name["email"]["type"], "string")
        
        # Check age (optional int32 with minimum constraint)
        self.assertEqual(fields_by_name["age"]["type"], "integer")
        self.assertTrue(fields_by_name["age"].get("nullable", False), "Optional field should be nullable")
        self.assertEqual(fields_by_name["age"].get("minimum"), 0)
        
        # Check interests (optional array - becomes string in CSV)
        self.assertEqual(fields_by_name["interests"]["type"], "string")
        self.assertTrue(fields_by_name["interests"].get("nullable", False))

    def test_enum_const(self):
        """Test converting schema with enum and const"""
        csv_schema = self._convert_and_check(
            "test-enum-const.struct.json",
            expected_fields=["statusEnum", "priorityEnum", "versionConst"]
        )
        
        fields_by_name = {field["name"]: field for field in csv_schema["fields"]}
        
        # Check statusEnum (required string enum)
        self.assertEqual(fields_by_name["statusEnum"]["type"], "string")
        self.assertIn("enum", fields_by_name["statusEnum"])
        self.assertEqual(fields_by_name["statusEnum"]["enum"], ["active", "inactive", "pending"])
        
        # Check priorityEnum (optional integer enum)
        self.assertEqual(fields_by_name["priorityEnum"]["type"], "integer")
        self.assertIn("enum", fields_by_name["priorityEnum"])
        self.assertEqual(fields_by_name["priorityEnum"]["enum"], [1, 2, 3, 4, 5])
        
        # Check versionConst (const field)
        self.assertEqual(fields_by_name["versionConst"]["type"], "string")
        self.assertIn("const", fields_by_name["versionConst"])
        self.assertEqual(fields_by_name["versionConst"]["const"], "1.0.0")

    def test_validation_attributes(self):
        """Test converting schema with validation attributes"""
        csv_schema = self._convert_and_check("test-validation-attributes.struct.json")
        
        # Check that constraint attributes are preserved
        for field in csv_schema["fields"]:
            # Each field should have name and type
            self.assertIn("name", field)
            self.assertIn("type", field)

    def test_empty_schema(self):
        """Test converting empty object schema"""
        # Create a minimal schema
        minimal_schema = {
            "type": "object",
            "properties": {}
        }
        
        # Write to temp file
        temp_input = path.join(tempfile.gettempdir(), "avrotize", "minimal.struct.json")
        temp_output = path.join(tempfile.gettempdir(), "avrotize", "minimal.csv.json")
        os.makedirs(os.path.dirname(temp_input), exist_ok=True)
        
        with open(temp_input, "w") as f:
            json.dump(minimal_schema, f)
        
        # Convert
        convert_structure_to_csv_schema(temp_input, temp_output)
        
        # Validate
        with open(temp_output, "r") as f:
            csv_schema = json.load(f)
        
        self.assertIn("fields", csv_schema)
        self.assertEqual(len(csv_schema["fields"]), 0)

    def test_all_primitive_types(self):
        """Test conversion of all JSON Structure primitive types"""
        # Create a schema with various primitive types
        all_types_schema = {
            "type": "object",
            "properties": {
                "stringField": {"type": "string"},
                "intField": {"type": "integer"},
                "int32Field": {"type": "int32"},
                "int64Field": {"type": "int64"},
                "numberField": {"type": "number"},
                "floatField": {"type": "float"},
                "doubleField": {"type": "double"},
                "booleanField": {"type": "boolean"},
                "binaryField": {"type": "binary"},
                "dateField": {"type": "date"},
                "timeField": {"type": "time"},
                "datetimeField": {"type": "datetime"},
                "uuidField": {"type": "uuid"},
                "uriField": {"type": "uri"},
                "decimalField": {"type": "decimal"}
            },
            "required": ["stringField", "intField"]
        }
        
        # Write to temp file
        temp_input = path.join(tempfile.gettempdir(), "avrotize", "alltypes.struct.json")
        temp_output = path.join(tempfile.gettempdir(), "avrotize", "alltypes.csv.json")
        os.makedirs(os.path.dirname(temp_input), exist_ok=True)
        
        with open(temp_input, "w") as f:
            json.dump(all_types_schema, f)
        
        # Convert
        convert_structure_to_csv_schema(temp_input, temp_output)
        
        # Validate
        with open(temp_output, "r") as f:
            csv_schema = json.load(f)
        
        fields_by_name = {field["name"]: field for field in csv_schema["fields"]}
        
        # Verify type mappings
        self.assertEqual(fields_by_name["stringField"]["type"], "string")
        self.assertEqual(fields_by_name["intField"]["type"], "integer")
        self.assertEqual(fields_by_name["int32Field"]["type"], "integer")
        self.assertEqual(fields_by_name["int64Field"]["type"], "integer")
        self.assertEqual(fields_by_name["numberField"]["type"], "number")
        self.assertEqual(fields_by_name["floatField"]["type"], "number")
        self.assertEqual(fields_by_name["doubleField"]["type"], "number")
        self.assertEqual(fields_by_name["booleanField"]["type"], "boolean")
        self.assertEqual(fields_by_name["binaryField"]["type"], "string")  # Binary becomes string (base64)
        self.assertEqual(fields_by_name["dateField"]["type"], "string")
        self.assertEqual(fields_by_name["timeField"]["type"], "string")
        self.assertEqual(fields_by_name["datetimeField"]["type"], "string")
        self.assertEqual(fields_by_name["uuidField"]["type"], "string")
        self.assertEqual(fields_by_name["uriField"]["type"], "string")
        self.assertEqual(fields_by_name["decimalField"]["type"], "number")
        
        # Check nullable status
        self.assertNotIn("nullable", fields_by_name["stringField"])  # Required
        self.assertNotIn("nullable", fields_by_name["intField"])  # Required
        self.assertTrue(fields_by_name["floatField"].get("nullable", False))  # Optional

    def test_compound_types(self):
        """Test conversion of compound types (array, object, map)"""
        compound_schema = {
            "type": "object",
            "properties": {
                "arrayField": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "mapField": {
                    "type": "map",
                    "values": {"type": "integer"}
                },
                "objectField": {
                    "type": "object",
                    "properties": {
                        "nested": {"type": "string"}
                    }
                }
            }
        }
        
        # Write to temp file
        temp_input = path.join(tempfile.gettempdir(), "avrotize", "compound.struct.json")
        temp_output = path.join(tempfile.gettempdir(), "avrotize", "compound.csv.json")
        os.makedirs(os.path.dirname(temp_input), exist_ok=True)
        
        with open(temp_input, "w") as f:
            json.dump(compound_schema, f)
        
        # Convert
        convert_structure_to_csv_schema(temp_input, temp_output)
        
        # Validate
        with open(temp_output, "r") as f:
            csv_schema = json.load(f)
        
        fields_by_name = {field["name"]: field for field in csv_schema["fields"]}
        
        # All compound types should become strings in CSV
        self.assertEqual(fields_by_name["arrayField"]["type"], "string")
        self.assertEqual(fields_by_name["mapField"]["type"], "string")
        self.assertEqual(fields_by_name["objectField"]["type"], "string")

    def test_constraints(self):
        """Test that validation constraints are preserved"""
        constrained_schema = {
            "type": "object",
            "properties": {
                "limitedString": {
                    "type": "string",
                    "maxLength": 100,
                    "minLength": 5,
                    "pattern": "^[A-Za-z]+$"
                },
                "limitedNumber": {
                    "type": "number",
                    "minimum": 0,
                    "maximum": 100
                },
                "preciseDecimal": {
                    "type": "decimal",
                    "precision": 10,
                    "scale": 2
                }
            }
        }
        
        # Write to temp file
        temp_input = path.join(tempfile.gettempdir(), "avrotize", "constrained.struct.json")
        temp_output = path.join(tempfile.gettempdir(), "avrotize", "constrained.csv.json")
        os.makedirs(os.path.dirname(temp_input), exist_ok=True)
        
        with open(temp_input, "w") as f:
            json.dump(constrained_schema, f)
        
        # Convert
        convert_structure_to_csv_schema(temp_input, temp_output)
        
        # Validate
        with open(temp_output, "r") as f:
            csv_schema = json.load(f)
        
        fields_by_name = {field["name"]: field for field in csv_schema["fields"]}
        
        # Check string constraints
        self.assertEqual(fields_by_name["limitedString"].get("maxLength"), 100)
        self.assertEqual(fields_by_name["limitedString"].get("minLength"), 5)
        self.assertEqual(fields_by_name["limitedString"].get("pattern"), "^[A-Za-z]+$")
        
        # Check number constraints
        self.assertEqual(fields_by_name["limitedNumber"].get("minimum"), 0)
        self.assertEqual(fields_by_name["limitedNumber"].get("maximum"), 100)
        
        # Check decimal precision
        self.assertEqual(fields_by_name["preciseDecimal"].get("precision"), 10)
        self.assertEqual(fields_by_name["preciseDecimal"].get("scale"), 2)


if __name__ == '__main__':
    unittest.main()
