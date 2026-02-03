"""Tests for the validate command."""

import json
import os
import sys
import tempfile
import unittest

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

from avrotize.avrovalidator import AvroValidator, AvroValidationError, validate_json_against_avro
from avrotize.validate import validate_instance, validate_file, detect_schema_type, ValidationResult

# JSON Structure SDK for validation
try:
    from json_structure import SchemaValidator, InstanceValidator
    HAS_JSTRUCT_SDK = True
except ImportError:
    HAS_JSTRUCT_SDK = False


class TestAvroValidatorPrimitives(unittest.TestCase):
    """Test Avro validation for primitive types."""

    def test_null(self):
        """Test null type validation."""
        schema = "null"
        errors = validate_json_against_avro(None, schema)
        self.assertEqual(errors, [])
        
        errors = validate_json_against_avro("not null", schema)
        self.assertNotEqual(errors, [])

    def test_boolean(self):
        """Test boolean type validation."""
        schema = "boolean"
        self.assertEqual(validate_json_against_avro(True, schema), [])
        self.assertEqual(validate_json_against_avro(False, schema), [])
        self.assertNotEqual(validate_json_against_avro("true", schema), [])
        self.assertNotEqual(validate_json_against_avro(1, schema), [])

    def test_int(self):
        """Test int (32-bit) type validation."""
        schema = "int"
        self.assertEqual(validate_json_against_avro(42, schema), [])
        self.assertEqual(validate_json_against_avro(-42, schema), [])
        self.assertEqual(validate_json_against_avro(0, schema), [])
        self.assertEqual(validate_json_against_avro(2147483647, schema), [])  # int32 max
        self.assertEqual(validate_json_against_avro(-2147483648, schema), [])  # int32 min
        
        # Out of range
        self.assertNotEqual(validate_json_against_avro(2147483648, schema), [])
        self.assertNotEqual(validate_json_against_avro(-2147483649, schema), [])
        
        # Wrong type
        self.assertNotEqual(validate_json_against_avro("42", schema), [])
        self.assertNotEqual(validate_json_against_avro(3.14, schema), [])

    def test_long(self):
        """Test long (64-bit) type validation."""
        schema = "long"
        self.assertEqual(validate_json_against_avro(42, schema), [])
        self.assertEqual(validate_json_against_avro(9223372036854775807, schema), [])  # int64 max
        self.assertEqual(validate_json_against_avro(-9223372036854775808, schema), [])  # int64 min

    def test_float(self):
        """Test float type validation."""
        schema = "float"
        self.assertEqual(validate_json_against_avro(3.14, schema), [])
        self.assertEqual(validate_json_against_avro(42, schema), [])  # int is valid as float
        self.assertNotEqual(validate_json_against_avro("3.14", schema), [])

    def test_double(self):
        """Test double type validation."""
        schema = "double"
        self.assertEqual(validate_json_against_avro(3.14159265359, schema), [])
        self.assertEqual(validate_json_against_avro(42, schema), [])

    def test_string(self):
        """Test string type validation."""
        schema = "string"
        self.assertEqual(validate_json_against_avro("hello", schema), [])
        self.assertEqual(validate_json_against_avro("", schema), [])
        self.assertEqual(validate_json_against_avro("unicode: 日本語 🎉", schema), [])
        self.assertNotEqual(validate_json_against_avro(42, schema), [])

    def test_bytes(self):
        """Test bytes type validation (encoded as string in JSON)."""
        schema = "bytes"
        self.assertEqual(validate_json_against_avro("binary data", schema), [])
        self.assertNotEqual(validate_json_against_avro(42, schema), [])


class TestAvroValidatorComplex(unittest.TestCase):
    """Test Avro validation for complex types."""

    def test_record(self):
        """Test record type validation."""
        schema = {
            "type": "record",
            "name": "Person",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "age", "type": "int"}
            ]
        }
        
        valid = {"name": "Alice", "age": 30}
        self.assertEqual(validate_json_against_avro(valid, schema), [])
        
        # Missing required field
        missing_field = {"name": "Alice"}
        self.assertNotEqual(validate_json_against_avro(missing_field, schema), [])
        
        # Wrong type for field
        wrong_type = {"name": "Alice", "age": "thirty"}
        self.assertNotEqual(validate_json_against_avro(wrong_type, schema), [])

    def test_record_with_optional_field(self):
        """Test record with optional (nullable) field."""
        schema = {
            "type": "record",
            "name": "Person",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "nickname", "type": ["null", "string"]}
            ]
        }
        
        # With nickname
        self.assertEqual(validate_json_against_avro({"name": "Alice", "nickname": "Ali"}, schema), [])
        
        # With null nickname
        self.assertEqual(validate_json_against_avro({"name": "Alice", "nickname": None}, schema), [])
        
        # Without nickname (defaults to null in union)
        self.assertEqual(validate_json_against_avro({"name": "Alice"}, schema), [])

    def test_record_with_default(self):
        """Test record with default value."""
        schema = {
            "type": "record",
            "name": "Config",
            "fields": [
                {"name": "setting", "type": "string", "default": "default_value"}
            ]
        }
        
        # Field present
        self.assertEqual(validate_json_against_avro({"setting": "custom"}, schema), [])
        
        # Field missing - has default so valid
        self.assertEqual(validate_json_against_avro({}, schema), [])

    def test_record_with_altnames(self):
        """Test record field with JSON alternate name."""
        schema = {
            "type": "record",
            "name": "Contact",
            "fields": [
                {"name": "firstName", "type": "string", "altnames": {"json": "first-name"}},
                {"name": "lastName", "type": "string", "altnames": {"json": "last-name"}}
            ]
        }
        
        # Using altnames
        self.assertEqual(validate_json_against_avro({"first-name": "Alice", "last-name": "Smith"}, schema), [])
        
        # Using regular names also works
        self.assertEqual(validate_json_against_avro({"firstName": "Alice", "lastName": "Smith"}, schema), [])

    def test_enum(self):
        """Test enum type validation."""
        schema = {
            "type": "enum",
            "name": "Color",
            "symbols": ["RED", "GREEN", "BLUE"]
        }
        
        self.assertEqual(validate_json_against_avro("RED", schema), [])
        self.assertEqual(validate_json_against_avro("GREEN", schema), [])
        self.assertEqual(validate_json_against_avro("BLUE", schema), [])
        
        self.assertNotEqual(validate_json_against_avro("YELLOW", schema), [])
        self.assertNotEqual(validate_json_against_avro(1, schema), [])

    def test_enum_with_altsymbols(self):
        """Test enum with JSON alternate symbols."""
        schema = {
            "type": "enum",
            "name": "Color",
            "symbols": ["RED", "GREEN", "BLUE"],
            "altsymbols": {
                "json": {
                    "RED": "#FF0000",
                    "GREEN": "#00FF00",
                    "BLUE": "#0000FF"
                }
            }
        }
        
        # Regular symbols
        self.assertEqual(validate_json_against_avro("RED", schema), [])
        
        # Alt symbols
        self.assertEqual(validate_json_against_avro("#FF0000", schema), [])
        self.assertEqual(validate_json_against_avro("#00FF00", schema), [])

    def test_array(self):
        """Test array type validation."""
        schema = {
            "type": "array",
            "items": "string"
        }
        
        self.assertEqual(validate_json_against_avro(["a", "b", "c"], schema), [])
        self.assertEqual(validate_json_against_avro([], schema), [])
        
        self.assertNotEqual(validate_json_against_avro([1, 2, 3], schema), [])
        self.assertNotEqual(validate_json_against_avro("not an array", schema), [])

    def test_array_of_records(self):
        """Test array of records."""
        schema = {
            "type": "array",
            "items": {
                "type": "record",
                "name": "Item",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "name", "type": "string"}
                ]
            }
        }
        
        valid = [
            {"id": 1, "name": "Item1"},
            {"id": 2, "name": "Item2"}
        ]
        self.assertEqual(validate_json_against_avro(valid, schema), [])
        
        # Invalid item
        invalid = [{"id": 1, "name": "Item1"}, {"id": "two", "name": "Item2"}]
        self.assertNotEqual(validate_json_against_avro(invalid, schema), [])

    def test_map(self):
        """Test map type validation."""
        schema = {
            "type": "map",
            "values": "int"
        }
        
        self.assertEqual(validate_json_against_avro({"a": 1, "b": 2}, schema), [])
        self.assertEqual(validate_json_against_avro({}, schema), [])
        
        self.assertNotEqual(validate_json_against_avro({"a": "not int"}, schema), [])
        self.assertNotEqual(validate_json_against_avro([1, 2], schema), [])

    def test_fixed(self):
        """Test fixed type validation."""
        schema = {
            "type": "fixed",
            "name": "FourBytes",
            "size": 4
        }
        
        self.assertEqual(validate_json_against_avro("1234", schema), [])
        self.assertNotEqual(validate_json_against_avro("12345", schema), [])
        self.assertNotEqual(validate_json_against_avro("123", schema), [])


class TestAvroValidatorUnion(unittest.TestCase):
    """Test Avro validation for type unions."""

    def test_simple_union(self):
        """Test simple union type."""
        schema = ["null", "string"]
        
        self.assertEqual(validate_json_against_avro(None, schema), [])
        self.assertEqual(validate_json_against_avro("hello", schema), [])
        self.assertNotEqual(validate_json_against_avro(42, schema), [])

    def test_complex_union(self):
        """Test union with multiple types."""
        schema = ["null", "string", "int"]
        
        self.assertEqual(validate_json_against_avro(None, schema), [])
        self.assertEqual(validate_json_against_avro("hello", schema), [])
        self.assertEqual(validate_json_against_avro(42, schema), [])
        self.assertNotEqual(validate_json_against_avro(3.14, schema), [])

    def test_record_union(self):
        """Test union of record types."""
        schema = [
            {
                "type": "record",
                "name": "Cat",
                "fields": [{"name": "meow", "type": "string"}]
            },
            {
                "type": "record",
                "name": "Dog",
                "fields": [{"name": "bark", "type": "string"}]
            }
        ]
        
        self.assertEqual(validate_json_against_avro({"meow": "meow!"}, schema), [])
        self.assertEqual(validate_json_against_avro({"bark": "woof!"}, schema), [])


class TestAvroValidatorLogicalTypes(unittest.TestCase):
    """Test Avro validation for logical types."""

    def test_uuid(self):
        """Test uuid logical type."""
        schema = {"type": "string", "logicalType": "uuid"}
        
        self.assertEqual(validate_json_against_avro("550e8400-e29b-41d4-a716-446655440000", schema), [])
        self.assertNotEqual(validate_json_against_avro("not-a-uuid", schema), [])
        self.assertNotEqual(validate_json_against_avro(123, schema), [])

    def test_date_int(self):
        """Test date logical type on int."""
        schema = {"type": "int", "logicalType": "date"}
        
        self.assertEqual(validate_json_against_avro(19000, schema), [])  # Days since epoch
        self.assertNotEqual(validate_json_against_avro("2024-01-01", schema), [])

    def test_date_string(self):
        """Test date logical type on string (Avrotize extension)."""
        schema = {"type": "string", "logicalType": "date"}
        
        self.assertEqual(validate_json_against_avro("2024-01-15", schema), [])
        self.assertEqual(validate_json_against_avro("2024-12-31", schema), [])
        self.assertNotEqual(validate_json_against_avro("01-15-2024", schema), [])
        self.assertNotEqual(validate_json_against_avro("not a date", schema), [])

    def test_timestamp_millis(self):
        """Test timestamp-millis logical type."""
        schema = {"type": "long", "logicalType": "timestamp-millis"}
        
        self.assertEqual(validate_json_against_avro(1705330800000, schema), [])

    def test_timestamp_string(self):
        """Test timestamp logical type on string (Avrotize extension)."""
        schema = {"type": "string", "logicalType": "timestamp-millis"}
        
        self.assertEqual(validate_json_against_avro("2024-01-15T10:30:00Z", schema), [])
        self.assertEqual(validate_json_against_avro("2024-01-15T10:30:00.123Z", schema), [])
        self.assertEqual(validate_json_against_avro("2024-01-15T10:30:00+05:00", schema), [])

    def test_decimal_string(self):
        """Test decimal logical type on string."""
        schema = {"type": "string", "logicalType": "decimal", "precision": 10, "scale": 2}
        
        self.assertEqual(validate_json_against_avro("123.45", schema), [])
        self.assertEqual(validate_json_against_avro("-123.45", schema), [])
        self.assertEqual(validate_json_against_avro("0.01", schema), [])
        self.assertNotEqual(validate_json_against_avro("not a number", schema), [])


class TestSchemaTypeDetection(unittest.TestCase):
    """Test schema type auto-detection."""

    def test_detect_avro_record(self):
        """Detect Avro record schema."""
        schema = {"type": "record", "name": "Test", "fields": []}
        self.assertEqual(detect_schema_type(schema), 'avro')

    def test_detect_avro_enum(self):
        """Detect Avro enum schema."""
        schema = {"type": "enum", "name": "Color", "symbols": ["RED"]}
        self.assertEqual(detect_schema_type(schema), 'avro')

    def test_detect_avro_array(self):
        """Detect Avro array schema."""
        schema = {"type": "array", "items": "string"}
        self.assertEqual(detect_schema_type(schema), 'avro')

    def test_detect_jstruct(self):
        """Detect JSON Structure schema."""
        schema = {
            "$schema": "https://json-structure.org/meta/core/v0/#",
            "$id": "https://example.com/Test",
            "type": "object",
            "properties": {}
        }
        self.assertEqual(detect_schema_type(schema), 'jstruct')


@unittest.skipUnless(HAS_JSTRUCT_SDK, "json-structure SDK not available")
class TestJStructValidation(unittest.TestCase):
    """Test JSON Structure validation via SDK."""

    def test_simple_object(self):
        """Test simple object validation."""
        schema = {
            "$schema": "https://json-structure.org/meta/core/v0/#",
            "$id": "https://example.com/Person",
            "name": "Person",
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"}
            }
        }
        
        # Valid instance
        result = validate_instance({"name": "Alice", "age": 30}, schema, "jstruct")
        self.assertTrue(result.is_valid, result.errors)
        
        # Wrong type for field
        result = validate_instance({"name": "Alice", "age": "thirty"}, schema, "jstruct")
        self.assertFalse(result.is_valid)
        self.assertIn("age", str(result.errors))


class TestValidateFile(unittest.TestCase):
    """Test file-based validation."""

    def test_validate_json_array(self):
        """Test validation of JSON array file."""
        # Create schema
        schema = {
            "type": "record",
            "name": "Item",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"}
            ]
        }
        
        # Create instance file
        instances = [
            {"id": 1, "name": "Item1"},
            {"id": 2, "name": "Item2"}
        ]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.avsc', delete=False) as f:
            json.dump(schema, f)
            schema_path = f.name
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(instances, f)
            instance_path = f.name
        
        try:
            results = validate_file(instance_path, schema_path)
            self.assertEqual(len(results), 2)
            self.assertTrue(all(r.is_valid for r in results))
        finally:
            os.unlink(schema_path)
            os.unlink(instance_path)

    def test_validate_jsonl(self):
        """Test validation of JSONL file."""
        schema = {
            "type": "record",
            "name": "Record",
            "fields": [{"name": "value", "type": "int"}]
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.avsc', delete=False) as f:
            json.dump(schema, f)
            schema_path = f.name
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            f.write('{"value": 1}\n')
            f.write('{"value": 2}\n')
            f.write('{"value": 3}\n')
            instance_path = f.name
        
        try:
            results = validate_file(instance_path, schema_path)
            self.assertEqual(len(results), 3)
            self.assertTrue(all(r.is_valid for r in results))
        finally:
            os.unlink(schema_path)
            os.unlink(instance_path)


if __name__ == '__main__':
    unittest.main()
