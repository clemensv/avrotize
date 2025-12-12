"""Quick targeted tests to verify bug fixes."""

import json
import os
import sys
import tempfile
import subprocess
import unittest

# Add the parent directory to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from avrotize.avrotopython import convert_avro_to_python
from avrotize.avrotojava import convert_avro_to_java
from avrotize.structuretopython import convert_structure_to_python
from avrotize.structuretojava import convert_structure_to_java
from avrotize.avrotogo import convert_avro_to_go
from avrotize.structuretogo import convert_structure_to_go
from avrotize.avrotocsharp import convert_avro_to_csharp
from avrotize.structuretocsharp import convert_structure_to_csharp


class TestDecimalImportFix(unittest.TestCase):
    """Test that decimal.Decimal import is correctly added."""

    def test_avro_python_decimal_import(self):
        """Bug fix: Python code should import decimal module for decimal logical type."""
        schema = {
            "type": "record",
            "name": "DecimalRecord",
            "namespace": "mytest.decimal",
            "fields": [
                {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}},
            ]
        }
        
        with tempfile.TemporaryDirectory() as temp_dir:
            schema_path = os.path.join(temp_dir, "decimal.avsc")
            with open(schema_path, 'w') as f:
                json.dump(schema, f)
            
            output_dir = os.path.join(temp_dir, "output")
            convert_avro_to_python(schema_path, output_dir)
            
            # Check that the generated code imports decimal
            py_file = os.path.join(output_dir, "mytest", "decimal", "decimal_record.py")
            self.assertTrue(os.path.exists(py_file), f"Expected {py_file} to exist")
            
            with open(py_file, 'r') as f:
                content = f.read()
            
            self.assertIn("import decimal", content, "Missing 'import decimal' statement")
            
            # Verify it can be imported
            result = subprocess.run(
                [sys.executable, "-c", f"import sys; sys.path.insert(0, '{output_dir}'); from mytest.decimal.decimal_record import DecimalRecord"],
                capture_output=True, text=True
            )
            self.assertEqual(result.returncode, 0, f"Import failed: {result.stderr}")
            print("✓ Python decimal import fix works")


class TestIdentifierSanitization(unittest.TestCase):
    """Test that identifiers with special characters are properly sanitized."""

    def test_python_numeric_prefix_field(self):
        """Bug fix: Fields starting with numbers should be prefixed with underscore."""
        schema = {
            "type": "record",
            "name": "NumericFieldRecord",
            "namespace": "mytest",
            "fields": [
                {"name": "123field", "type": "string"},
                {"name": "456another", "type": "int"},
            ]
        }
        
        with tempfile.TemporaryDirectory() as temp_dir:
            schema_path = os.path.join(temp_dir, "numeric.avsc")
            with open(schema_path, 'w') as f:
                json.dump(schema, f)
            
            output_dir = os.path.join(temp_dir, "output")
            convert_avro_to_python(schema_path, output_dir)
            
            py_file = os.path.join(output_dir, "mytest", "numeric_field_record.py")
            self.assertTrue(os.path.exists(py_file), f"Expected {py_file} to exist")
            
            # Verify it can be imported
            result = subprocess.run(
                [sys.executable, "-c", f"import sys; sys.path.insert(0, '{output_dir}'); from mytest.numeric_field_record import NumericFieldRecord"],
                capture_output=True, text=True
            )
            self.assertEqual(result.returncode, 0, f"Import failed: {result.stderr}")
            print("✓ Python numeric prefix field sanitization works")

    def test_python_hyphen_field(self):
        """Bug fix: Fields with hyphens should be converted to underscores."""
        schema = {
            "type": "record",
            "name": "HyphenFieldRecord",
            "namespace": "mytest",
            "fields": [
                {"name": "my-field", "type": "string"},
                {"name": "another-field", "type": "int"},
            ]
        }
        
        with tempfile.TemporaryDirectory() as temp_dir:
            schema_path = os.path.join(temp_dir, "hyphen.avsc")
            with open(schema_path, 'w') as f:
                json.dump(schema, f)
            
            output_dir = os.path.join(temp_dir, "output")
            convert_avro_to_python(schema_path, output_dir)
            
            py_file = os.path.join(output_dir, "mytest", "hyphen_field_record.py")
            self.assertTrue(os.path.exists(py_file), f"Expected {py_file} to exist")
            
            # Verify it can be imported
            result = subprocess.run(
                [sys.executable, "-c", f"import sys; sys.path.insert(0, '{output_dir}'); from mytest.hyphen_field_record import HyphenFieldRecord"],
                capture_output=True, text=True
            )
            self.assertEqual(result.returncode, 0, f"Import failed: {result.stderr}")
            print("✓ Python hyphen field sanitization works")


class TestGoTimeImport(unittest.TestCase):
    """Test that Go code correctly imports time package for date/time types."""

    def test_go_time_import_for_date(self):
        """Bug fix: Go code should import time package for date logical type."""
        schema = {
            "type": "record",
            "name": "DateRecord",
            "namespace": "mytest",
            "fields": [
                {"name": "created_at", "type": {"type": "int", "logicalType": "date"}},
            ]
        }
        
        with tempfile.TemporaryDirectory() as temp_dir:
            schema_path = os.path.join(temp_dir, "date.avsc")
            with open(schema_path, 'w') as f:
                json.dump(schema, f)
            
            output_dir = os.path.join(temp_dir, "output")
            convert_avro_to_go(schema_path, output_dir)
            
            # Find the generated Go file
            go_files = []
            for root, dirs, files in os.walk(output_dir):
                for f in files:
                    if f.endswith('.go'):
                        go_files.append(os.path.join(root, f))
            
            self.assertTrue(len(go_files) > 0, "No Go files generated")
            
            # Check that time is imported
            found_time_import = False
            for go_file in go_files:
                with open(go_file, 'r') as f:
                    content = f.read()
                if '"time"' in content:
                    found_time_import = True
                    break
            
            self.assertTrue(found_time_import, "Go code should import 'time' package for date type")
            print("✓ Go time import fix works")


class TestJavaLogicalTypeTestValues(unittest.TestCase):
    """Test that Java test classes have correct test values for logical types."""

    def test_java_instant_test_value(self):
        """Bug fix: Java test class should have valid Instant.now() for timestamp types."""
        schema = {
            "type": "record",
            "name": "TimestampRecord",
            "namespace": "mytest",
            "fields": [
                {"name": "created_at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
            ]
        }
        
        with tempfile.TemporaryDirectory() as temp_dir:
            schema_path = os.path.join(temp_dir, "timestamp.avsc")
            with open(schema_path, 'w') as f:
                json.dump(schema, f)
            
            output_dir = os.path.join(temp_dir, "output")
            convert_avro_to_java(schema_path, output_dir)
            
            # Find test files
            test_files = []
            for root, dirs, files in os.walk(output_dir):
                for f in files:
                    if f.endswith('Test.java'):
                        test_files.append(os.path.join(root, f))
            
            self.assertTrue(len(test_files) > 0, "No test files generated")
            
            # Check that test files have Instant.now() not null
            for test_file in test_files:
                with open(test_file, 'r') as f:
                    content = f.read()
                if 'Instant' in content:
                    self.assertIn("Instant.now()", content, "Should use Instant.now() for timestamp test value")
                    print("✓ Java Instant test value fix works")
                    return
            
            print("✓ No Instant types in generated code (test passes)")


if __name__ == '__main__':
    unittest.main(verbosity=2)
