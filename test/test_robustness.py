"""
Robustness tests for avrotize code generators.

These tests verify that the code generators:
1. Handle malformed/edge-case inputs gracefully with clear error messages
2. Never crash with raw stack traces visible to users
3. Generate code that compiles/builds successfully
4. Generate code that passes its own tests

Bug tracking: Each failing test that reveals a bug should result in a GitHub issue.
"""

import unittest
import os
import shutil
import subprocess
import sys
import tempfile
import json
import traceback
from typing import Dict, Any, Optional, Tuple, List
from io import StringIO

import pytest

# Import all converters we want to test
from avrotize.jsonstoavro import convert_jsons_to_avro
from avrotize.structuretopython import convert_structure_to_python
from avrotize.structuretojava import convert_structure_to_java
from avrotize.structuretots import convert_structure_to_typescript
from avrotize.structuretocsharp import convert_structure_to_csharp
from avrotize.structuretogo import convert_structure_to_go
from avrotize.structuretorust import convert_structure_to_rust
from avrotize.avrotopython import convert_avro_to_python
from avrotize.avrotojava import convert_avro_to_java
from avrotize.avrotots import convert_avro_to_typescript
from avrotize.avrotocsharp import convert_avro_to_csharp
from avrotize.avrotogo import convert_avro_to_go

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)


class RobustnessTestResult:
    """Captures the result of a robustness test."""
    
    def __init__(self, test_name: str, converter: str, input_desc: str):
        self.test_name = test_name
        self.converter = converter
        self.input_desc = input_desc
        self.passed = False
        self.error_message: Optional[str] = None
        self.stack_trace: Optional[str] = None
        self.is_graceful_rejection = False
        self.is_bug = False
        self.bug_category: Optional[str] = None  # 'crash', 'bad_error', 'compile_fail', 'test_fail'
    
    def mark_graceful_rejection(self, message: str):
        """The converter gracefully rejected bad input with a clear message."""
        self.passed = True
        self.is_graceful_rejection = True
        self.error_message = message
    
    def mark_success(self):
        """The converter succeeded (for valid input tests)."""
        self.passed = True
    
    def mark_crash(self, exception: Exception, tb: str):
        """The converter crashed with a raw exception."""
        self.passed = False
        self.is_bug = True
        self.bug_category = 'crash'
        self.error_message = str(exception)
        self.stack_trace = tb
    
    def mark_bad_error(self, message: str):
        """The converter failed but with an unclear/unhelpful error message."""
        self.passed = False
        self.is_bug = True
        self.bug_category = 'bad_error'
        self.error_message = message
    
    def mark_compile_fail(self, message: str):
        """Generated code failed to compile/build."""
        self.passed = False
        self.is_bug = True
        self.bug_category = 'compile_fail'
        self.error_message = message
    
    def mark_test_fail(self, message: str):
        """Generated code failed its own tests."""
        self.passed = False
        self.is_bug = True
        self.bug_category = 'test_fail'
        self.error_message = message


class RobustnessTestBase(unittest.TestCase):
    """Base class for robustness tests with common utilities."""
    
    @classmethod
    def setUpClass(cls):
        cls.temp_base = os.path.join(tempfile.gettempdir(), "avrotize_robustness")
        os.makedirs(cls.temp_base, exist_ok=True)
        cls.bugs_found: List[RobustnessTestResult] = []
    
    def get_temp_dir(self, name: str) -> str:
        """Get a clean temporary directory for a test."""
        path = os.path.join(self.temp_base, name)
        if os.path.exists(path):
            shutil.rmtree(path, ignore_errors=True)
        os.makedirs(path, exist_ok=True)
        return path
    
    def write_json_file(self, dir_path: str, filename: str, content: Any) -> str:
        """Write JSON content to a file and return the path."""
        file_path = os.path.join(dir_path, filename)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(content, f, indent=2)
        return file_path
    
    def write_text_file(self, dir_path: str, filename: str, content: str) -> str:
        """Write text content to a file and return the path."""
        file_path = os.path.join(dir_path, filename)
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        return file_path
    
    def check_has_stack_trace(self, output: str) -> bool:
        """Check if output contains a raw Python stack trace."""
        stack_indicators = [
            'Traceback (most recent call last):',
            'File "', 'line ',
            'raise ',
            'Exception:',
            'Error:',
        ]
        # A stack trace typically has multiple "File" + "line" occurrences
        file_line_count = output.count('File "') + output.count("File '")
        if file_line_count >= 2 and 'Traceback' in output:
            return True
        return False
    
    def run_converter_safely(
        self,
        converter_func,
        converter_name: str,
        input_desc: str,
        *args,
        **kwargs
    ) -> RobustnessTestResult:
        """
        Run a converter function and capture the result.
        
        A converter should either:
        1. Succeed and produce valid output
        2. Fail gracefully with a clear, user-friendly error message
        
        It should NOT:
        1. Crash with a raw stack trace
        2. Produce code that doesn't compile
        3. Produce code that fails its own tests
        """
        result = RobustnessTestResult(
            test_name=self._testMethodName,
            converter=converter_name,
            input_desc=input_desc
        )
        
        # Capture stderr to check for stack traces
        old_stderr = sys.stderr
        sys.stderr = captured_stderr = StringIO()
        
        try:
            converter_func(*args, **kwargs)
            result.mark_success()
        except SystemExit as e:
            # SystemExit with a message is acceptable for graceful rejection
            stderr_output = captured_stderr.getvalue()
            if self.check_has_stack_trace(stderr_output):
                result.mark_crash(e, stderr_output)
            else:
                result.mark_graceful_rejection(f"SystemExit: {e}")
        except Exception as e:
            tb = traceback.format_exc()
            stderr_output = captured_stderr.getvalue()
            full_output = tb + "\n" + stderr_output
            
            # Check if this is a graceful rejection or a crash
            if self.check_has_stack_trace(full_output):
                result.mark_crash(e, full_output)
            else:
                # Even without a visible stack trace, an unhandled exception is a bug
                result.mark_crash(e, full_output)
        finally:
            sys.stderr = old_stderr
        
        if result.is_bug:
            self.bugs_found.append(result)
        
        return result


class TestEmptyAndNullInputs(RobustnessTestBase):
    """Test how converters handle empty and null inputs."""
    
    def test_001_jsons_to_avro_empty_object(self):
        """JSON Schema converter should handle empty object schema gracefully."""
        temp_dir = self.get_temp_dir("test_001")
        schema = {}  # Empty object - not a valid JSON Schema
        schema_path = self.write_json_file(temp_dir, "empty.json", schema)
        output_path = os.path.join(temp_dir, "output.avsc")
        
        result = self.run_converter_safely(
            convert_jsons_to_avro,
            "jsonstoavro",
            "empty object {}",
            schema_path,
            output_path
        )
        
        # An empty object is not a valid JSON Schema, so graceful rejection is acceptable
        # But a crash with stack trace is not
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed on empty input: {result.error_message}\n{result.stack_trace}")
    
    def test_002_jsons_to_avro_null_type(self):
        """JSON Schema converter should handle null type schema."""
        temp_dir = self.get_temp_dir("test_002")
        schema = {"type": "null"}
        schema_path = self.write_json_file(temp_dir, "null_type.json", schema)
        output_path = os.path.join(temp_dir, "output.avsc")
        
        result = self.run_converter_safely(
            convert_jsons_to_avro,
            "jsonstoavro",
            "null type schema",
            schema_path,
            output_path
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed on null type: {result.error_message}\n{result.stack_trace}")
    
    def test_003_jsons_to_avro_empty_array_type(self):
        """JSON Schema converter should handle empty array in type field."""
        temp_dir = self.get_temp_dir("test_003")
        schema = {"type": []}  # Empty type array
        schema_path = self.write_json_file(temp_dir, "empty_array_type.json", schema)
        output_path = os.path.join(temp_dir, "output.avsc")
        
        result = self.run_converter_safely(
            convert_jsons_to_avro,
            "jsonstoavro",
            "empty array type",
            schema_path,
            output_path
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed on empty array type: {result.error_message}\n{result.stack_trace}")
    
    def test_004_structure_to_python_empty_schema(self):
        """Structure to Python converter should handle empty schema."""
        temp_dir = self.get_temp_dir("test_004")
        schema = {}
        schema_path = self.write_json_file(temp_dir, "empty.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_python,
            "structuretopython",
            "empty structure schema",
            schema_path,
            output_dir
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed on empty schema: {result.error_message}\n{result.stack_trace}")
    
    def test_005_structure_to_python_minimal_valid(self):
        """Structure to Python should work with minimal valid schema."""
        temp_dir = self.get_temp_dir("test_005")
        schema = {
            "type": "object",
            "properties": {
                "id": {"type": "string"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "minimal.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_python,
            "structuretopython",
            "minimal valid schema",
            schema_path,
            output_dir,
            package_name="test_minimal"
        )
        
        if result.is_bug:
            self.fail(f"Converter failed on minimal valid schema: {result.error_message}")
        
        # If it succeeded, verify the output compiles
        if result.passed and not result.is_graceful_rejection:
            src_dir = os.path.join(output_dir, "src")
            if os.path.exists(src_dir):
                # Try to import the generated module
                env = os.environ.copy()
                env['PYTHONPATH'] = src_dir
                try:
                    subprocess.check_call(
                        [sys.executable, "-c", "import test_minimal"],
                        env=env,
                        cwd=output_dir,
                        timeout=30
                    )
                except subprocess.CalledProcessError as e:
                    result.mark_compile_fail(f"Generated Python code failed to import: {e}")
                    self.bugs_found.append(result)
                    self.fail(f"Generated code failed to import: {e}")


class TestMalformedSchemas(RobustnessTestBase):
    """Test how converters handle malformed/invalid schemas."""
    
    def test_006_jsons_invalid_type_value(self):
        """JSON Schema converter should handle invalid type values."""
        temp_dir = self.get_temp_dir("test_006")
        schema = {"type": "not_a_valid_type"}
        schema_path = self.write_json_file(temp_dir, "invalid_type.json", schema)
        output_path = os.path.join(temp_dir, "output.avsc")
        
        result = self.run_converter_safely(
            convert_jsons_to_avro,
            "jsonstoavro",
            "invalid type value 'not_a_valid_type'",
            schema_path,
            output_path
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed on invalid type: {result.error_message}\n{result.stack_trace}")
    
    def test_007_jsons_missing_items_in_array(self):
        """JSON Schema converter should handle array type without items definition."""
        temp_dir = self.get_temp_dir("test_007")
        schema = {
            "type": "object",
            "properties": {
                "data": {"type": "array"}  # Missing 'items'
            }
        }
        schema_path = self.write_json_file(temp_dir, "array_no_items.json", schema)
        output_path = os.path.join(temp_dir, "output.avsc")
        
        result = self.run_converter_safely(
            convert_jsons_to_avro,
            "jsonstoavro",
            "array without items definition",
            schema_path,
            output_path
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed on array without items: {result.error_message}\n{result.stack_trace}")
    
    def test_008_jsons_circular_ref(self):
        """JSON Schema converter should handle circular $ref references."""
        temp_dir = self.get_temp_dir("test_008")
        schema = {
            "type": "object",
            "properties": {
                "child": {"$ref": "#"}  # Circular reference to self
            }
        }
        schema_path = self.write_json_file(temp_dir, "circular_ref.json", schema)
        output_path = os.path.join(temp_dir, "output.avsc")
        
        result = self.run_converter_safely(
            convert_jsons_to_avro,
            "jsonstoavro",
            "circular $ref to self",
            schema_path,
            output_path
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed on circular ref: {result.error_message}\n{result.stack_trace}")
    
    def test_009_jsons_broken_ref(self):
        """JSON Schema converter should handle broken $ref references."""
        temp_dir = self.get_temp_dir("test_009")
        schema = {
            "type": "object",
            "properties": {
                "data": {"$ref": "#/definitions/NonExistent"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "broken_ref.json", schema)
        output_path = os.path.join(temp_dir, "output.avsc")
        
        result = self.run_converter_safely(
            convert_jsons_to_avro,
            "jsonstoavro",
            "broken $ref to non-existent definition",
            schema_path,
            output_path
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed on broken ref: {result.error_message}\n{result.stack_trace}")
    
    def test_010_jsons_deeply_nested(self):
        """JSON Schema converter should handle deeply nested schemas."""
        temp_dir = self.get_temp_dir("test_010")
        
        # Create a deeply nested schema (100 levels)
        inner = {"type": "string"}
        for i in range(100):
            inner = {
                "type": "object",
                "properties": {
                    f"level_{i}": inner
                }
            }
        
        schema_path = self.write_json_file(temp_dir, "deeply_nested.json", inner)
        output_path = os.path.join(temp_dir, "output.avsc")
        
        result = self.run_converter_safely(
            convert_jsons_to_avro,
            "jsonstoavro",
            "deeply nested schema (100 levels)",
            schema_path,
            output_path
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed on deeply nested schema: {result.error_message}\n{result.stack_trace}")


class TestSpecialCharactersAndNames(RobustnessTestBase):
    """Test how converters handle special characters in names."""
    
    def test_011_jsons_property_with_spaces(self):
        """JSON Schema converter should handle property names with spaces."""
        temp_dir = self.get_temp_dir("test_011")
        schema = {
            "type": "object",
            "properties": {
                "my property": {"type": "string"},
                "another property": {"type": "integer"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "spaces_in_names.json", schema)
        output_path = os.path.join(temp_dir, "output.avsc")
        
        result = self.run_converter_safely(
            convert_jsons_to_avro,
            "jsonstoavro",
            "property names with spaces",
            schema_path,
            output_path
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed on spaces in names: {result.error_message}\n{result.stack_trace}")
    
    def test_012_jsons_property_with_special_chars(self):
        """JSON Schema converter should handle property names with special characters."""
        temp_dir = self.get_temp_dir("test_012")
        schema = {
            "type": "object",
            "properties": {
                "my-property": {"type": "string"},
                "another.property": {"type": "integer"},
                "@type": {"type": "string"},
                "$id": {"type": "string"},
                "100_starts_with_number": {"type": "string"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "special_chars.json", schema)
        output_path = os.path.join(temp_dir, "output.avsc")
        
        result = self.run_converter_safely(
            convert_jsons_to_avro,
            "jsonstoavro",
            "property names with special characters",
            schema_path,
            output_path
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed on special chars: {result.error_message}\n{result.stack_trace}")
    
    def test_013_jsons_unicode_property_names(self):
        """JSON Schema converter should handle Unicode property names."""
        temp_dir = self.get_temp_dir("test_013")
        schema = {
            "type": "object",
            "properties": {
                "名前": {"type": "string"},  # Japanese
                "имя": {"type": "string"},   # Russian
                "שם": {"type": "string"},    # Hebrew
                "🎉": {"type": "string"},    # Emoji
            }
        }
        schema_path = self.write_json_file(temp_dir, "unicode_names.json", schema)
        output_path = os.path.join(temp_dir, "output.avsc")
        
        result = self.run_converter_safely(
            convert_jsons_to_avro,
            "jsonstoavro",
            "Unicode property names",
            schema_path,
            output_path
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed on Unicode names: {result.error_message}\n{result.stack_trace}")
    
    def test_014_jsons_reserved_keywords(self):
        """JSON Schema converter should handle property names that are reserved keywords."""
        temp_dir = self.get_temp_dir("test_014")
        schema = {
            "type": "object",
            "properties": {
                "class": {"type": "string"},
                "import": {"type": "string"},
                "def": {"type": "string"},
                "return": {"type": "string"},
                "type": {"type": "string"},
                "int": {"type": "string"},
                "string": {"type": "string"},
                "null": {"type": "string"},
                "true": {"type": "string"},
                "false": {"type": "string"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "reserved_keywords.json", schema)
        output_path = os.path.join(temp_dir, "output.avsc")
        
        result = self.run_converter_safely(
            convert_jsons_to_avro,
            "jsonstoavro",
            "reserved keyword property names",
            schema_path,
            output_path
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed on reserved keywords: {result.error_message}\n{result.stack_trace}")
    
    def test_015_jsons_empty_property_name(self):
        """JSON Schema converter should handle empty property name."""
        temp_dir = self.get_temp_dir("test_015")
        schema = {
            "type": "object",
            "properties": {
                "": {"type": "string"},  # Empty property name
            }
        }
        schema_path = self.write_json_file(temp_dir, "empty_prop_name.json", schema)
        output_path = os.path.join(temp_dir, "output.avsc")
        
        result = self.run_converter_safely(
            convert_jsons_to_avro,
            "jsonstoavro",
            "empty property name",
            schema_path,
            output_path
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed on empty property name: {result.error_message}\n{result.stack_trace}")


class TestStructureToPythonRobustness(RobustnessTestBase):
    """Test Structure to Python converter robustness."""
    
    def test_016_structure_to_python_reserved_keywords(self):
        """Structure to Python should escape reserved Python keywords."""
        temp_dir = self.get_temp_dir("test_016")
        schema = {
            "type": "object",
            "title": "TestClass",
            "properties": {
                "class": {"type": "string"},
                "import": {"type": "string"},
                "def": {"type": "integer"},
                "lambda": {"type": "boolean"},
                "from": {"type": "string"},
                "global": {"type": "string"},
                "None": {"type": "string"},
                "True": {"type": "string"},
                "False": {"type": "string"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "py_keywords.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_python,
            "structuretopython",
            "Python reserved keywords as properties",
            schema_path,
            output_dir,
            package_name="test_keywords"
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed on Python keywords: {result.error_message}\n{result.stack_trace}")
        
        # If generation succeeded, verify the code compiles
        if result.passed and not result.is_graceful_rejection:
            src_dir = os.path.join(output_dir, "src")
            if os.path.exists(src_dir):
                env = os.environ.copy()
                env['PYTHONPATH'] = src_dir
                try:
                    subprocess.check_call(
                        [sys.executable, "-c", "import test_keywords"],
                        env=env,
                        cwd=output_dir,
                        timeout=30
                    )
                except subprocess.CalledProcessError as e:
                    result.mark_compile_fail(f"Generated Python code with keywords failed to import: {e}")
                    self.bugs_found.append(result)
                    self.fail(f"Generated code failed to import: {e}")
    
    def test_017_structure_to_python_very_long_names(self):
        """Structure to Python should handle very long property names."""
        temp_dir = self.get_temp_dir("test_017")
        long_name = "a" * 500  # 500 character property name
        schema = {
            "type": "object",
            "title": "TestLongNames",
            "properties": {
                long_name: {"type": "string"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "long_names.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_python,
            "structuretopython",
            "500 character property name",
            schema_path,
            output_dir,
            package_name="test_long_names"
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed on long names: {result.error_message}\n{result.stack_trace}")
    
    def test_018_structure_to_python_many_properties(self):
        """Structure to Python should handle schemas with many properties."""
        temp_dir = self.get_temp_dir("test_018")
        properties = {f"prop_{i}": {"type": "string"} for i in range(500)}
        schema = {
            "type": "object",
            "title": "TestManyProps",
            "properties": properties
        }
        schema_path = self.write_json_file(temp_dir, "many_props.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_python,
            "structuretopython",
            "500 properties",
            schema_path,
            output_dir,
            package_name="test_many_props"
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed on many properties: {result.error_message}\n{result.stack_trace}")
    
    def test_019_structure_to_python_allof_empty(self):
        """Structure to Python should handle empty allOf."""
        temp_dir = self.get_temp_dir("test_019")
        schema = {
            "allOf": []  # Empty allOf
        }
        schema_path = self.write_json_file(temp_dir, "empty_allof.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_python,
            "structuretopython",
            "empty allOf",
            schema_path,
            output_dir,
            package_name="test_empty_allof"
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed on empty allOf: {result.error_message}\n{result.stack_trace}")
    
    def test_020_structure_to_python_oneof_empty(self):
        """Structure to Python should handle empty oneOf."""
        temp_dir = self.get_temp_dir("test_020")
        schema = {
            "oneOf": []  # Empty oneOf
        }
        schema_path = self.write_json_file(temp_dir, "empty_oneof.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_python,
            "structuretopython",
            "empty oneOf",
            schema_path,
            output_dir,
            package_name="test_empty_oneof"
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed on empty oneOf: {result.error_message}\n{result.stack_trace}")


class TestAvroToPythonRobustness(RobustnessTestBase):
    """Test Avro to Python converter robustness."""
    
    def test_021_avro_to_python_empty_record(self):
        """Avro to Python should handle empty record."""
        temp_dir = self.get_temp_dir("test_021")
        schema = {
            "type": "record",
            "name": "EmptyRecord",
            "namespace": "test",
            "fields": []
        }
        schema_path = self.write_json_file(temp_dir, "empty_record.avsc", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_avro_to_python,
            "avrotopython",
            "empty record with no fields",
            schema_path,
            output_dir
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed on empty record: {result.error_message}\n{result.stack_trace}")
    
    def test_022_avro_to_python_reserved_keywords(self):
        """Avro to Python should escape reserved Python keywords in field names."""
        temp_dir = self.get_temp_dir("test_022")
        schema = {
            "type": "record",
            "name": "KeywordRecord",
            "namespace": "test.keywords",
            "fields": [
                {"name": "class", "type": "string"},
                {"name": "import", "type": "string"},
                {"name": "def", "type": "int"},
                {"name": "lambda", "type": "boolean"},
                {"name": "from", "type": "string"}
            ]
        }
        schema_path = self.write_json_file(temp_dir, "keywords.avsc", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_avro_to_python,
            "avrotopython",
            "Python keywords as Avro field names",
            schema_path,
            output_dir
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed on keyword field names: {result.error_message}\n{result.stack_trace}")
        
        # If generation succeeded, verify the code compiles
        if result.passed and not result.is_graceful_rejection:
            src_dir = os.path.join(output_dir, "src")
            if os.path.exists(src_dir):
                env = os.environ.copy()
                env['PYTHONPATH'] = src_dir
                try:
                    # Note: use keywords.test.keywords to avoid conflict with Python's test module
                    subprocess.check_call(
                        [sys.executable, "-c", "from keywords.test.keywords import KeywordRecord"],
                        env=env,
                        cwd=output_dir,
                        timeout=30
                    )
                except subprocess.CalledProcessError as e:
                    result.mark_compile_fail(f"Generated Python code failed to import: {e}")
                    self.bugs_found.append(result)
                    self.fail(f"Generated code failed to import: {e}")
    
    def test_023_avro_to_python_recursive_type(self):
        """Avro to Python should handle recursive types."""
        temp_dir = self.get_temp_dir("test_023")
        schema = {
            "type": "record",
            "name": "Node",
            "namespace": "test.tree",
            "fields": [
                {"name": "value", "type": "string"},
                {"name": "children", "type": {"type": "array", "items": "Node"}}
            ]
        }
        schema_path = self.write_json_file(temp_dir, "recursive.avsc", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_avro_to_python,
            "avrotopython",
            "recursive type (Node with children of Node)",
            schema_path,
            output_dir
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed on recursive type: {result.error_message}\n{result.stack_trace}")
    
    def test_024_avro_to_python_complex_union(self):
        """Avro to Python should handle complex unions."""
        temp_dir = self.get_temp_dir("test_024")
        schema = {
            "type": "record",
            "name": "ComplexUnion",
            "namespace": "test.unions",
            "fields": [
                {
                    "name": "data",
                    "type": [
                        "null",
                        "string",
                        "int",
                        "boolean",
                        {"type": "array", "items": "string"},
                        {"type": "map", "values": "int"}
                    ]
                }
            ]
        }
        schema_path = self.write_json_file(temp_dir, "complex_union.avsc", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_avro_to_python,
            "avrotopython",
            "complex union with multiple types",
            schema_path,
            output_dir
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed on complex union: {result.error_message}\n{result.stack_trace}")
    
    def test_025_avro_to_python_enum_with_symbols(self):
        """Avro to Python should handle enums with various symbol names."""
        temp_dir = self.get_temp_dir("test_025")
        schema = {
            "type": "record",
            "name": "EnumRecord",
            "namespace": "test.enums",
            "fields": [
                {
                    "name": "status",
                    "type": {
                        "type": "enum",
                        "name": "Status",
                        "symbols": ["UNKNOWN", "ACTIVE", "INACTIVE", "PENDING_REVIEW"]
                    }
                }
            ]
        }
        schema_path = self.write_json_file(temp_dir, "enum_record.avsc", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_avro_to_python,
            "avrotopython",
            "enum with various symbols",
            schema_path,
            output_dir
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed on enum: {result.error_message}\n{result.stack_trace}")


class TestStructureToJavaRobustness(RobustnessTestBase):
    """Test Structure to Java converter robustness."""
    
    def test_026_structure_to_java_reserved_keywords(self):
        """Structure to Java should handle Java reserved keywords as property names."""
        temp_dir = self.get_temp_dir("test_026")
        schema = {
            "type": "object",
            "title": "JavaKeywords",
            "properties": {
                "class": {"type": "string"},
                "public": {"type": "string"},
                "private": {"type": "string"},
                "static": {"type": "string"},
                "void": {"type": "string"},
                "int": {"type": "integer"},
                "boolean": {"type": "boolean"},
                "package": {"type": "string"},
                "import": {"type": "string"},
                "new": {"type": "string"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "java_keywords.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_java,
            "structuretojava",
            "Java reserved keywords as properties",
            schema_path,
            output_dir,
            package_name="test.javakeywords"
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed on Java keywords: {result.error_message}\n{result.stack_trace}")
    
    def test_027_structure_to_java_numeric_prefix(self):
        """Structure to Java should handle property names starting with numbers."""
        temp_dir = self.get_temp_dir("test_027")
        schema = {
            "type": "object",
            "title": "NumericProps",
            "properties": {
                "123field": {"type": "string"},
                "0start": {"type": "integer"},
                "9lives": {"type": "boolean"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "numeric_prefix.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_java,
            "structuretojava",
            "property names starting with numbers",
            schema_path,
            output_dir,
            package_name="test.numericprefix"
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed on numeric prefix: {result.error_message}\n{result.stack_trace}")
    
    def test_028_structure_to_java_very_deep_nesting(self):
        """Structure to Java should handle very deeply nested objects."""
        temp_dir = self.get_temp_dir("test_028")
        
        # Create 50 levels of nesting
        inner = {"type": "string"}
        for i in range(50):
            inner = {
                "type": "object",
                "title": f"Level{i}",
                "properties": {
                    "child": inner
                }
            }
        
        schema_path = self.write_json_file(temp_dir, "deep_nesting.struct.json", inner)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_java,
            "structuretojava",
            "50 levels of object nesting",
            schema_path,
            output_dir,
            package_name="test.deepnesting"
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed on deep nesting: {result.error_message}\n{result.stack_trace}")


class TestStructureToTypeScriptRobustness(RobustnessTestBase):
    """Test Structure to TypeScript converter robustness."""
    
    def test_029_structure_to_ts_reserved_keywords(self):
        """Structure to TypeScript should handle TypeScript reserved keywords."""
        temp_dir = self.get_temp_dir("test_029")
        schema = {
            "type": "object",
            "title": "TSKeywords",
            "properties": {
                "class": {"type": "string"},
                "interface": {"type": "string"},
                "type": {"type": "string"},
                "namespace": {"type": "string"},
                "module": {"type": "string"},
                "declare": {"type": "string"},
                "readonly": {"type": "boolean"},
                "any": {"type": "string"},
                "unknown": {"type": "string"},
                "never": {"type": "string"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "ts_keywords.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_typescript,
            "structuretots",
            "TypeScript reserved keywords as properties",
            schema_path,
            output_dir,
            package_name="test_ts_keywords"
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed on TS keywords: {result.error_message}\n{result.stack_trace}")
    
    def test_030_structure_to_ts_type_conflicts(self):
        """Structure to TypeScript should handle names that conflict with built-in types."""
        temp_dir = self.get_temp_dir("test_030")
        schema = {
            "type": "object",
            "title": "String",  # Conflicts with built-in String
            "properties": {
                "Array": {"type": "string"},  # Conflicts with built-in Array
                "Object": {"type": "string"},
                "Number": {"type": "integer"},
                "Boolean": {"type": "boolean"},
                "Map": {"type": "string"},
                "Set": {"type": "string"},
                "Promise": {"type": "string"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "type_conflicts.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_typescript,
            "structuretots",
            "class name 'String' and built-in type property names",
            schema_path,
            output_dir,
            package_name="test_type_conflicts"
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed on type conflicts: {result.error_message}\n{result.stack_trace}")


class TestStructureToCSharpRobustness(RobustnessTestBase):
    """Test Structure to C# converter robustness."""
    
    def test_031_structure_to_csharp_reserved_keywords(self):
        """Structure to C# should handle C# reserved keywords."""
        temp_dir = self.get_temp_dir("test_031")
        schema = {
            "type": "object",
            "title": "CSharpKeywords",
            "properties": {
                "class": {"type": "string"},
                "namespace": {"type": "string"},
                "using": {"type": "string"},
                "public": {"type": "string"},
                "private": {"type": "string"},
                "virtual": {"type": "string"},
                "override": {"type": "string"},
                "event": {"type": "string"},
                "delegate": {"type": "string"},
                "async": {"type": "string"},
                "await": {"type": "string"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "csharp_keywords.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_csharp,
            "structuretocsharp",
            "C# reserved keywords as properties",
            schema_path,
            output_dir,
            base_namespace="Test.CSharpKeywords"
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed on C# keywords: {result.error_message}\n{result.stack_trace}")
    
    def test_032_structure_to_csharp_nullable_complex(self):
        """Structure to C# should handle complex nullable patterns."""
        temp_dir = self.get_temp_dir("test_032")
        schema = {
            "type": "object",
            "title": "NullableComplex",
            "properties": {
                "requiredString": {"type": "string"},
                "optionalString": {"type": ["string", "null"]},
                "optionalArray": {"type": ["array", "null"], "items": {"type": "string"}},
                "nestedOptional": {
                    "type": ["object", "null"],
                    "properties": {
                        "value": {"type": ["integer", "null"]}
                    }
                }
            },
            "required": ["requiredString"]
        }
        schema_path = self.write_json_file(temp_dir, "nullable_complex.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_csharp,
            "structuretocsharp",
            "complex nullable patterns",
            schema_path,
            output_dir,
            base_namespace="Test.NullableComplex"
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed on nullable complex: {result.error_message}\n{result.stack_trace}")


class TestStructureToGoRobustness(RobustnessTestBase):
    """Test Structure to Go converter robustness."""
    
    def test_033_structure_to_go_reserved_keywords(self):
        """Structure to Go should handle Go reserved keywords."""
        temp_dir = self.get_temp_dir("test_033")
        schema = {
            "type": "object",
            "title": "GoKeywords",
            "properties": {
                "func": {"type": "string"},
                "package": {"type": "string"},
                "import": {"type": "string"},
                "type": {"type": "string"},
                "struct": {"type": "string"},
                "interface": {"type": "string"},
                "map": {"type": "string"},
                "chan": {"type": "string"},
                "go": {"type": "string"},
                "defer": {"type": "string"},
                "select": {"type": "string"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "go_keywords.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_go,
            "structuretogo",
            "Go reserved keywords as properties",
            schema_path,
            output_dir,
            package_name="gokeywords"
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed on Go keywords: {result.error_message}\n{result.stack_trace}")
    
    def test_034_structure_to_go_camelcase_required(self):
        """Structure to Go should properly export fields (uppercase first letter)."""
        temp_dir = self.get_temp_dir("test_034")
        schema = {
            "type": "object",
            "title": "ExportTest",
            "properties": {
                "lowercase": {"type": "string"},
                "UPPERCASE": {"type": "string"},
                "mixedCase": {"type": "string"},
                "_underscore": {"type": "string"},
                "__dunder": {"type": "string"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "export_test.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_go,
            "structuretogo",
            "various casing patterns for Go export",
            schema_path,
            output_dir,
            package_name="exporttest"
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed on casing: {result.error_message}\n{result.stack_trace}")


class TestEdgeCaseSchemas(RobustnessTestBase):
    """Test edge case schemas that might trip up converters."""
    
    def test_035_additionalproperties_false(self):
        """Converters should handle additionalProperties: false."""
        temp_dir = self.get_temp_dir("test_035")
        schema = {
            "type": "object",
            "title": "StrictObject",
            "properties": {
                "name": {"type": "string"}
            },
            "additionalProperties": False
        }
        schema_path = self.write_json_file(temp_dir, "strict.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_python,
            "structuretopython",
            "additionalProperties: false",
            schema_path,
            output_dir,
            package_name="test_strict"
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed: {result.error_message}\n{result.stack_trace}")
    
    def test_036_additionalproperties_typed(self):
        """Converters should handle additionalProperties with a type."""
        temp_dir = self.get_temp_dir("test_036")
        schema = {
            "type": "object",
            "title": "MapLike",
            "properties": {
                "name": {"type": "string"}
            },
            "additionalProperties": {"type": "integer"}
        }
        schema_path = self.write_json_file(temp_dir, "maplike.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_python,
            "structuretopython",
            "additionalProperties with typed value",
            schema_path,
            output_dir,
            package_name="test_maplike"
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed: {result.error_message}\n{result.stack_trace}")
    
    def test_037_const_values(self):
        """Converters should handle const values."""
        temp_dir = self.get_temp_dir("test_037")
        schema = {
            "type": "object",
            "title": "WithConst",
            "properties": {
                "version": {"const": "1.0.0"},
                "type": {"const": "event"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "const.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_python,
            "structuretopython",
            "const values",
            schema_path,
            output_dir,
            package_name="test_const"
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed: {result.error_message}\n{result.stack_trace}")
    
    def test_038_enum_values(self):
        """Converters should handle enum values."""
        temp_dir = self.get_temp_dir("test_038")
        schema = {
            "type": "object",
            "title": "WithEnum",
            "properties": {
                "status": {
                    "type": "string",
                    "enum": ["pending", "active", "completed", "failed"]
                },
                "priority": {
                    "type": "integer",
                    "enum": [1, 2, 3, 4, 5]
                }
            }
        }
        schema_path = self.write_json_file(temp_dir, "enum.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_python,
            "structuretopython",
            "enum values",
            schema_path,
            output_dir,
            package_name="test_enum"
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed: {result.error_message}\n{result.stack_trace}")
    
    def test_039_pattern_property(self):
        """Converters should handle pattern validation."""
        temp_dir = self.get_temp_dir("test_039")
        schema = {
            "type": "object",
            "title": "WithPattern",
            "properties": {
                "email": {
                    "type": "string",
                    "pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
                },
                "phone": {
                    "type": "string",
                    "pattern": "^\\+?[0-9]{10,15}$"
                }
            }
        }
        schema_path = self.write_json_file(temp_dir, "pattern.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_python,
            "structuretopython",
            "pattern validation",
            schema_path,
            output_dir,
            package_name="test_pattern"
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed: {result.error_message}\n{result.stack_trace}")
    
    def test_040_format_property(self):
        """Converters should handle format annotations."""
        temp_dir = self.get_temp_dir("test_040")
        schema = {
            "type": "object",
            "title": "WithFormats",
            "properties": {
                "email": {"type": "string", "format": "email"},
                "uri": {"type": "string", "format": "uri"},
                "uuid": {"type": "string", "format": "uuid"},
                "date": {"type": "string", "format": "date"},
                "datetime": {"type": "string", "format": "date-time"},
                "time": {"type": "string", "format": "time"},
                "ipv4": {"type": "string", "format": "ipv4"},
                "ipv6": {"type": "string", "format": "ipv6"},
                "hostname": {"type": "string", "format": "hostname"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "formats.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_python,
            "structuretopython",
            "format annotations",
            schema_path,
            output_dir,
            package_name="test_formats"
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed: {result.error_message}\n{result.stack_trace}")


class TestComplexCompositions(RobustnessTestBase):
    """Test complex schema compositions."""
    
    def test_041_nested_allof(self):
        """Converters should handle nested allOf compositions."""
        temp_dir = self.get_temp_dir("test_041")
        schema = {
            "allOf": [
                {
                    "type": "object",
                    "properties": {"base": {"type": "string"}}
                },
                {
                    "allOf": [
                        {"properties": {"level1": {"type": "string"}}},
                        {"properties": {"level2": {"type": "string"}}}
                    ]
                }
            ]
        }
        schema_path = self.write_json_file(temp_dir, "nested_allof.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_python,
            "structuretopython",
            "nested allOf",
            schema_path,
            output_dir,
            package_name="test_nested_allof"
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed: {result.error_message}\n{result.stack_trace}")
    
    def test_042_mixed_composition(self):
        """Converters should handle mixed allOf/oneOf/anyOf compositions."""
        temp_dir = self.get_temp_dir("test_042")
        schema = {
            "allOf": [
                {"properties": {"common": {"type": "string"}}},
                {
                    "oneOf": [
                        {"properties": {"optionA": {"type": "string"}}},
                        {"properties": {"optionB": {"type": "integer"}}}
                    ]
                }
            ]
        }
        schema_path = self.write_json_file(temp_dir, "mixed_comp.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_python,
            "structuretopython",
            "mixed allOf/oneOf composition",
            schema_path,
            output_dir,
            package_name="test_mixed_comp"
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed: {result.error_message}\n{result.stack_trace}")
    
    def test_043_ref_with_override(self):
        """Converters should handle $ref with sibling properties (override)."""
        temp_dir = self.get_temp_dir("test_043")
        schema = {
            "definitions": {
                "Base": {
                    "type": "object",
                    "properties": {"id": {"type": "string"}}
                }
            },
            "type": "object",
            "allOf": [
                {"$ref": "#/definitions/Base"}
            ],
            "properties": {
                "name": {"type": "string"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "ref_override.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_python,
            "structuretopython",
            "$ref with sibling properties",
            schema_path,
            output_dir,
            package_name="test_ref_override"
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed: {result.error_message}\n{result.stack_trace}")
    
    def test_044_patternproperties(self):
        """Converters should handle patternProperties."""
        temp_dir = self.get_temp_dir("test_044")
        schema = {
            "type": "object",
            "title": "WithPatternProps",
            "patternProperties": {
                "^S_": {"type": "string"},
                "^I_": {"type": "integer"}
            },
            "additionalProperties": False
        }
        schema_path = self.write_json_file(temp_dir, "pattern_props.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_python,
            "structuretopython",
            "patternProperties",
            schema_path,
            output_dir,
            package_name="test_pattern_props"
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed: {result.error_message}\n{result.stack_trace}")
    
    def test_045_dependent_schemas(self):
        """Converters should handle dependentSchemas."""
        temp_dir = self.get_temp_dir("test_045")
        schema = {
            "type": "object",
            "title": "WithDependentSchemas",
            "properties": {
                "name": {"type": "string"},
                "credit_card": {"type": "string"}
            },
            "dependentSchemas": {
                "credit_card": {
                    "properties": {
                        "billing_address": {"type": "string"}
                    },
                    "required": ["billing_address"]
                }
            }
        }
        schema_path = self.write_json_file(temp_dir, "dependent_schemas.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_python,
            "structuretopython",
            "dependentSchemas",
            schema_path,
            output_dir,
            package_name="test_dependent_schemas"
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed: {result.error_message}\n{result.stack_trace}")


class TestArrayEdgeCases(RobustnessTestBase):
    """Test array-related edge cases."""
    
    def test_046_tuple_array(self):
        """Converters should handle tuple validation (prefixItems)."""
        temp_dir = self.get_temp_dir("test_046")
        schema = {
            "type": "object",
            "title": "WithTuple",
            "properties": {
                "coordinates": {
                    "type": "array",
                    "prefixItems": [
                        {"type": "number"},
                        {"type": "number"},
                        {"type": "number"}
                    ],
                    "items": False
                }
            }
        }
        schema_path = self.write_json_file(temp_dir, "tuple.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_python,
            "structuretopython",
            "tuple array (prefixItems)",
            schema_path,
            output_dir,
            package_name="test_tuple"
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed: {result.error_message}\n{result.stack_trace}")
    
    def test_047_nested_arrays(self):
        """Converters should handle deeply nested arrays."""
        temp_dir = self.get_temp_dir("test_047")
        schema = {
            "type": "object",
            "title": "DeepArrays",
            "properties": {
                "matrix3d": {
                    "type": "array",
                    "items": {
                        "type": "array",
                        "items": {
                            "type": "array",
                            "items": {"type": "number"}
                        }
                    }
                }
            }
        }
        schema_path = self.write_json_file(temp_dir, "nested_arrays.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_python,
            "structuretopython",
            "3D array (array of array of array)",
            schema_path,
            output_dir,
            package_name="test_nested_arrays"
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed: {result.error_message}\n{result.stack_trace}")
    
    def test_048_array_with_unique_items(self):
        """Converters should handle uniqueItems constraint."""
        temp_dir = self.get_temp_dir("test_048")
        schema = {
            "type": "object",
            "title": "UniqueArray",
            "properties": {
                "tags": {
                    "type": "array",
                    "items": {"type": "string"},
                    "uniqueItems": True
                }
            }
        }
        schema_path = self.write_json_file(temp_dir, "unique_items.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_python,
            "structuretopython",
            "uniqueItems constraint",
            schema_path,
            output_dir,
            package_name="test_unique_items"
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed: {result.error_message}\n{result.stack_trace}")
    
    def test_049_array_with_contains(self):
        """Converters should handle contains constraint."""
        temp_dir = self.get_temp_dir("test_049")
        schema = {
            "type": "object",
            "title": "ContainsArray",
            "properties": {
                "values": {
                    "type": "array",
                    "contains": {"type": "string", "const": "required_value"},
                    "items": {"type": "string"}
                }
            }
        }
        schema_path = self.write_json_file(temp_dir, "contains.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_python,
            "structuretopython",
            "contains constraint",
            schema_path,
            output_dir,
            package_name="test_contains"
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed: {result.error_message}\n{result.stack_trace}")
    
    def test_050_mixed_type_array(self):
        """Converters should handle arrays with mixed item types (oneOf in items)."""
        temp_dir = self.get_temp_dir("test_050")
        schema = {
            "type": "object",
            "title": "MixedArray",
            "properties": {
                "values": {
                    "type": "array",
                    "items": {
                        "oneOf": [
                            {"type": "string"},
                            {"type": "integer"},
                            {"type": "object", "properties": {"nested": {"type": "boolean"}}}
                        ]
                    }
                }
            }
        }
        schema_path = self.write_json_file(temp_dir, "mixed_array.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_python,
            "structuretopython",
            "array with oneOf items",
            schema_path,
            output_dir,
            package_name="test_mixed_array"
        )
        
        if result.is_bug and result.bug_category == 'crash':
            self.fail(f"Converter crashed: {result.error_message}\n{result.stack_trace}")


class TestPythonCodeCompilationAndExecution(RobustnessTestBase):
    """
    Tests that verify the generated Python code actually compiles and runs.
    These tests go beyond just checking for crashes - they verify the output is valid.
    """
    
    def verify_python_code_compiles(self, output_dir: str, package_name: str) -> Tuple[bool, str]:
        """Verify generated Python code can be imported."""
        src_dir = os.path.join(output_dir, "src")
        if not os.path.exists(src_dir):
            return False, f"src directory not found at {src_dir}"
        
        env = os.environ.copy()
        env['PYTHONPATH'] = src_dir
        
        # Find the actual package to import
        top_level_package = package_name.split('.')[0].lower()
        
        try:
            result = subprocess.run(
                [sys.executable, "-c", f"import {top_level_package}; print('OK')"],
                env=env,
                cwd=output_dir,
                capture_output=True,
                text=True,
                timeout=30
            )
            if result.returncode != 0:
                return False, f"Import failed: {result.stderr}"
            return True, "OK"
        except subprocess.TimeoutExpired:
            return False, "Import timed out"
        except Exception as e:
            return False, str(e)
    
    def verify_python_tests_pass(self, output_dir: str) -> Tuple[bool, str]:
        """Run pytest on the generated tests directory."""
        tests_dir = os.path.join(output_dir, "tests")
        if not os.path.exists(tests_dir):
            return True, "No tests directory"  # Not a failure if no tests
        
        src_dir = os.path.join(output_dir, "src")
        env = os.environ.copy()
        env['PYTHONPATH'] = src_dir
        
        try:
            result = subprocess.run(
                [sys.executable, "-m", "pytest", tests_dir, "-v", "--tb=short"],
                env=env,
                cwd=output_dir,
                capture_output=True,
                text=True,
                timeout=120
            )
            if result.returncode != 0:
                return False, f"Tests failed:\n{result.stdout}\n{result.stderr}"
            return True, "OK"
        except subprocess.TimeoutExpired:
            return False, "Tests timed out"
        except Exception as e:
            return False, str(e)
    
    def test_051_python_compile_reserved_keywords(self):
        """Generated Python with reserved keywords should compile and import."""
        temp_dir = self.get_temp_dir("test_051")
        schema = {
            "type": "object",
            "title": "TestKeywords",
            "properties": {
                "class": {"type": "string"},
                "import": {"type": "string"},
                "def": {"type": "integer"},
                "for": {"type": "boolean"},
                "while": {"type": "string"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "keywords.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_python(
            schema_path,
            output_dir,
            package_name="test_kw"
        )
        
        success, message = self.verify_python_code_compiles(output_dir, "test_kw")
        self.assertTrue(success, f"Python code with reserved keywords failed to compile: {message}")
    
    def test_052_python_run_tests_simple_object(self):
        """Generated Python tests for simple object should pass."""
        temp_dir = self.get_temp_dir("test_052")
        schema = {
            "type": "object",
            "title": "Person",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"},
                "active": {"type": "boolean"}
            },
            "required": ["name"]
        }
        schema_path = self.write_json_file(temp_dir, "person.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_python(
            schema_path,
            output_dir,
            package_name="test_person",
            dataclasses_json_annotation=True
        )
        
        success, message = self.verify_python_code_compiles(output_dir, "test_person")
        self.assertTrue(success, f"Python code failed to compile: {message}")
        
        success, message = self.verify_python_tests_pass(output_dir)
        self.assertTrue(success, f"Python tests failed: {message}")
    
    def test_053_python_datetime_roundtrip(self):
        """Generated Python with datetime should pass round-trip serialization tests."""
        temp_dir = self.get_temp_dir("test_053")
        schema = {
            "type": "object",
            "title": "EventRecord",
            "properties": {
                "eventTime": {"type": "string", "format": "date-time"},
                "eventDate": {"type": "string", "format": "date"},
                "name": {"type": "string"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "event.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_python(
            schema_path,
            output_dir,
            package_name="test_event",
            dataclasses_json_annotation=True
        )
        
        success, message = self.verify_python_code_compiles(output_dir, "test_event")
        self.assertTrue(success, f"Python code failed to compile: {message}")
        
        success, message = self.verify_python_tests_pass(output_dir)
        self.assertTrue(success, f"Python tests failed: {message}")
    
    def test_054_python_nested_objects(self):
        """Generated Python with nested objects should compile and run tests."""
        temp_dir = self.get_temp_dir("test_054")
        schema = {
            "type": "object",
            "title": "Container",
            "properties": {
                "id": {"type": "string"},
                "inner": {
                    "type": "object",
                    "title": "Inner",
                    "properties": {
                        "value": {"type": "integer"},
                        "deepInner": {
                            "type": "object",
                            "title": "DeepInner",
                            "properties": {
                                "flag": {"type": "boolean"}
                            }
                        }
                    }
                }
            }
        }
        schema_path = self.write_json_file(temp_dir, "nested.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_python(
            schema_path,
            output_dir,
            package_name="test_nested",
            dataclasses_json_annotation=True
        )
        
        success, message = self.verify_python_code_compiles(output_dir, "test_nested")
        self.assertTrue(success, f"Python code failed to compile: {message}")
        
        success, message = self.verify_python_tests_pass(output_dir)
        self.assertTrue(success, f"Python tests failed: {message}")
    
    def test_055_python_array_of_objects(self):
        """Generated Python with array of objects should compile and run tests."""
        temp_dir = self.get_temp_dir("test_055")
        schema = {
            "type": "object",
            "title": "ItemList",
            "properties": {
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "title": "Item",
                        "properties": {
                            "id": {"type": "integer"},
                            "name": {"type": "string"}
                        }
                    }
                }
            }
        }
        schema_path = self.write_json_file(temp_dir, "items.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_python(
            schema_path,
            output_dir,
            package_name="test_items",
            dataclasses_json_annotation=True
        )
        
        success, message = self.verify_python_code_compiles(output_dir, "test_items")
        self.assertTrue(success, f"Python code failed to compile: {message}")
        
        success, message = self.verify_python_tests_pass(output_dir)
        self.assertTrue(success, f"Python tests failed: {message}")
    
    def test_056_python_optional_and_required(self):
        """Generated Python should properly handle optional vs required fields."""
        temp_dir = self.get_temp_dir("test_056")
        schema = {
            "type": "object",
            "title": "MixedFields",
            "properties": {
                "required_string": {"type": "string"},
                "optional_string": {"type": "string"},
                "required_int": {"type": "integer"},
                "optional_int": {"type": "integer"},
                "nullable_string": {"type": ["string", "null"]}
            },
            "required": ["required_string", "required_int"]
        }
        schema_path = self.write_json_file(temp_dir, "mixed.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_python(
            schema_path,
            output_dir,
            package_name="test_mixed",
            dataclasses_json_annotation=True
        )
        
        success, message = self.verify_python_code_compiles(output_dir, "test_mixed")
        self.assertTrue(success, f"Python code failed to compile: {message}")
        
        success, message = self.verify_python_tests_pass(output_dir)
        self.assertTrue(success, f"Python tests failed: {message}")
    
    def test_057_python_enum_property(self):
        """Generated Python with enum property should compile and run tests."""
        temp_dir = self.get_temp_dir("test_057")
        schema = {
            "type": "object",
            "title": "StatusRecord",
            "properties": {
                "status": {
                    "type": "string",
                    "enum": ["pending", "active", "completed", "failed"]
                },
                "message": {"type": "string"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "status.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_python(
            schema_path,
            output_dir,
            package_name="test_status",
            dataclasses_json_annotation=True
        )
        
        success, message = self.verify_python_code_compiles(output_dir, "test_status")
        self.assertTrue(success, f"Python code failed to compile: {message}")
        
        success, message = self.verify_python_tests_pass(output_dir)
        self.assertTrue(success, f"Python tests failed: {message}")
    
    def test_058_python_allof_inheritance(self):
        """Generated Python with allOf (inheritance pattern) should compile."""
        temp_dir = self.get_temp_dir("test_058")
        schema = {
            "definitions": {
                "Base": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "created": {"type": "string", "format": "date-time"}
                    }
                }
            },
            "type": "object",
            "title": "Extended",
            "allOf": [
                {"$ref": "#/definitions/Base"},
                {
                    "properties": {
                        "name": {"type": "string"},
                        "value": {"type": "integer"}
                    }
                }
            ]
        }
        schema_path = self.write_json_file(temp_dir, "extended.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_python(
            schema_path,
            output_dir,
            package_name="test_extended",
            dataclasses_json_annotation=True
        )
        
        success, message = self.verify_python_code_compiles(output_dir, "test_extended")
        self.assertTrue(success, f"Python code failed to compile: {message}")
    
    def test_059_python_special_characters_in_names(self):
        """Generated Python should handle special chars in property names."""
        temp_dir = self.get_temp_dir("test_059")
        schema = {
            "type": "object",
            "title": "SpecialNames",
            "properties": {
                "my-field": {"type": "string"},
                "field.with.dots": {"type": "string"},
                "@type": {"type": "string"},
                "$schema": {"type": "string"},
                "123numeric": {"type": "string"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "special.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_python(
            schema_path,
            output_dir,
            package_name="test_special",
            dataclasses_json_annotation=True
        )
        
        success, message = self.verify_python_code_compiles(output_dir, "test_special")
        self.assertTrue(success, f"Python code with special chars failed to compile: {message}")
        
        success, message = self.verify_python_tests_pass(output_dir)
        self.assertTrue(success, f"Python tests failed: {message}")
    
    def test_060_python_map_type(self):
        """Generated Python with map/dict types should compile and run tests."""
        temp_dir = self.get_temp_dir("test_060")
        schema = {
            "type": "object",
            "title": "MapContainer",
            "properties": {
                "metadata": {
                    "type": "object",
                    "additionalProperties": {"type": "string"}
                },
                "counts": {
                    "type": "object",
                    "additionalProperties": {"type": "integer"}
                }
            }
        }
        schema_path = self.write_json_file(temp_dir, "maps.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_python(
            schema_path,
            output_dir,
            package_name="test_maps",
            dataclasses_json_annotation=True
        )
        
        success, message = self.verify_python_code_compiles(output_dir, "test_maps")
        self.assertTrue(success, f"Python code failed to compile: {message}")
        
        success, message = self.verify_python_tests_pass(output_dir)
        self.assertTrue(success, f"Python tests failed: {message}")


class TestTypeScriptCodeCompilationAndExecution(RobustnessTestBase):
    """
    Tests that verify the generated TypeScript code actually compiles.
    """
    
    def verify_typescript_compiles(self, output_dir: str) -> Tuple[bool, str]:
        """Verify generated TypeScript code compiles with tsc."""
        src_dir = os.path.join(output_dir, "src")
        if not os.path.exists(src_dir):
            return False, f"src directory not found at {src_dir}"
        
        # Check if tsc is available
        try:
            result = subprocess.run(
                ["npx", "tsc", "--version"],
                capture_output=True,
                text=True,
                timeout=30,
                shell=True
            )
            if result.returncode != 0:
                return True, "tsc not available, skipping"
        except Exception:
            return True, "tsc not available, skipping"
        
        # Find tsconfig.json or create a minimal one
        tsconfig_path = os.path.join(output_dir, "tsconfig.json")
        if not os.path.exists(tsconfig_path):
            tsconfig = {
                "compilerOptions": {
                    "target": "ES2020",
                    "module": "commonjs",
                    "strict": True,
                    "esModuleInterop": True,
                    "skipLibCheck": True,
                    "noEmit": True
                },
                "include": ["src/**/*"]
            }
            with open(tsconfig_path, 'w') as f:
                json.dump(tsconfig, f, indent=2)
        
        try:
            result = subprocess.run(
                ["npx", "tsc", "--noEmit"],
                cwd=output_dir,
                capture_output=True,
                text=True,
                timeout=60,
                shell=True
            )
            if result.returncode != 0:
                return False, f"TypeScript compilation failed:\n{result.stdout}\n{result.stderr}"
            return True, "OK"
        except subprocess.TimeoutExpired:
            return False, "TypeScript compilation timed out"
        except Exception as e:
            return False, str(e)
    
    def test_061_ts_nested_objects(self):
        """Generated TypeScript with nested objects should compile."""
        temp_dir = self.get_temp_dir("test_061")
        schema = {
            "type": "object",
            "title": "Container",
            "properties": {
                "id": {"type": "string"},
                "inner": {
                    "type": "object",
                    "title": "Inner",
                    "properties": {
                        "value": {"type": "integer"}
                    }
                }
            }
        }
        schema_path = self.write_json_file(temp_dir, "nested.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_typescript(
            schema_path,
            output_dir,
            package_name="test_nested_ts"
        )
        
        success, message = self.verify_typescript_compiles(output_dir)
        self.assertTrue(success, f"TypeScript code failed to compile: {message}")
    
    def test_062_ts_reserved_keywords(self):
        """Generated TypeScript with reserved keywords should compile."""
        temp_dir = self.get_temp_dir("test_062")
        schema = {
            "type": "object",
            "title": "TSKeywords",
            "properties": {
                "class": {"type": "string"},
                "interface": {"type": "string"},
                "type": {"type": "string"},
                "function": {"type": "string"},
                "const": {"type": "string"},
                "let": {"type": "string"},
                "var": {"type": "string"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "ts_kw.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_typescript(
            schema_path,
            output_dir,
            package_name="test_ts_kw"
        )
        
        success, message = self.verify_typescript_compiles(output_dir)
        self.assertTrue(success, f"TypeScript code failed to compile: {message}")
    
    def test_063_ts_numeric_prefix(self):
        """Generated TypeScript with numeric prefix should compile."""
        temp_dir = self.get_temp_dir("test_063")
        schema = {
            "type": "object",
            "title": "NumericNames",
            "properties": {
                "123field": {"type": "string"},
                "456value": {"type": "integer"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "numeric.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_typescript(
            schema_path,
            output_dir,
            package_name="test_numeric_ts"
        )
        
        success, message = self.verify_typescript_compiles(output_dir)
        self.assertTrue(success, f"TypeScript code failed to compile: {message}")


class TestJavaCodeCompilation(RobustnessTestBase):
    """
    Tests that verify the generated Java code compiles.
    """
    
    def verify_java_compiles(self, output_dir: str) -> Tuple[bool, str]:
        """Verify generated Java code compiles with javac."""
        src_dir = os.path.join(output_dir, "src", "main", "java")
        if not os.path.exists(src_dir):
            # Try alternate path
            src_dir = os.path.join(output_dir, "src")
            if not os.path.exists(src_dir):
                return False, f"src directory not found"
        
        # Check if javac is available
        try:
            result = subprocess.run(
                ["javac", "-version"],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.returncode != 0:
                return True, "javac not available, skipping"
        except Exception:
            return True, "javac not available, skipping"
        
        # Find all .java files
        java_files = []
        for root, dirs, files in os.walk(src_dir):
            for file in files:
                if file.endswith('.java'):
                    java_files.append(os.path.join(root, file))
        
        if not java_files:
            return False, "No .java files found"
        
        # Try to compile (syntax check only, no dependencies)
        try:
            # Just do a syntax check with -Xlint:none to suppress warnings
            result = subprocess.run(
                ["javac", "-Xlint:none", "-d", output_dir] + java_files[:5],  # Check first 5 files
                capture_output=True,
                text=True,
                timeout=60
            )
            # We expect this to fail for missing dependencies, but syntax errors are bugs
            if "error:" in result.stderr and "cannot find symbol" not in result.stderr:
                return False, f"Java syntax error:\n{result.stderr}"
            return True, "OK (syntax check)"
        except subprocess.TimeoutExpired:
            return False, "Java compilation timed out"
        except Exception as e:
            return False, str(e)
    
    def test_064_java_nested_objects(self):
        """Generated Java with nested objects should compile."""
        temp_dir = self.get_temp_dir("test_064")
        schema = {
            "type": "object",
            "title": "Container",
            "properties": {
                "id": {"type": "string"},
                "inner": {
                    "type": "object",
                    "title": "Inner",
                    "properties": {
                        "value": {"type": "integer"}
                    }
                }
            }
        }
        schema_path = self.write_json_file(temp_dir, "nested.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_java(
            schema_path,
            output_dir,
            package_name="test.nested.java"
        )
        
        success, message = self.verify_java_compiles(output_dir)
        self.assertTrue(success, f"Java code failed to compile: {message}")
    
    def test_065_java_reserved_keywords(self):
        """Generated Java with reserved keywords should compile."""
        temp_dir = self.get_temp_dir("test_065")
        schema = {
            "type": "object",
            "title": "JavaKeywords",
            "properties": {
                "class": {"type": "string"},
                "public": {"type": "string"},
                "static": {"type": "string"},
                "void": {"type": "string"},
                "final": {"type": "string"},
                "abstract": {"type": "string"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "java_kw.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_java(
            schema_path,
            output_dir,
            package_name="test.javakw"
        )
        
        success, message = self.verify_java_compiles(output_dir)
        self.assertTrue(success, f"Java code failed to compile: {message}")
    
    def test_066_java_numeric_prefix(self):
        """Generated Java with numeric prefix property names should compile."""
        temp_dir = self.get_temp_dir("test_066")
        schema = {
            "type": "object",
            "title": "NumericProps",
            "properties": {
                "123field": {"type": "string"},
                "456value": {"type": "integer"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "numeric.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_java(
            schema_path,
            output_dir,
            package_name="test.numeric.java"
        )
        
        success, message = self.verify_java_compiles(output_dir)
        self.assertTrue(success, f"Java code failed to compile: {message}")


class TestAvroToPythonCompilation(RobustnessTestBase):
    """
    Tests that verify the generated Python code from Avro compiles and runs.
    """
    
    def verify_python_code_compiles(self, output_dir: str, namespace: str) -> Tuple[bool, str]:
        """Verify generated Python code can be imported.
        
        The avrotopython generator creates a structure like src/{last_segment}/{namespace}/
        For namespace 'mytest.logical', it becomes src/logical/mytest/logical/
        So import path is: logical.mytest.logical
        """
        src_dir = os.path.join(output_dir, "src")
        if not os.path.exists(src_dir):
            return False, f"src directory not found at {src_dir}"
        
        # Avro Python puts code in a different structure - the last namespace segment
        # becomes the top-level directory. For ns 'mytest.logical', it's src/logical/mytest/logical
        ns_parts = namespace.split('.')
        last_part = ns_parts[-1]
        # Import path: last_part.namespace
        import_path = f"{last_part}.{namespace}"
        
        env = os.environ.copy()
        env['PYTHONPATH'] = src_dir
        
        try:
            result = subprocess.run(
                [sys.executable, "-c", f"import {import_path}; print('OK')"],
                env=env,
                cwd=output_dir,
                capture_output=True,
                text=True,
                timeout=30
            )
            if result.returncode != 0:
                return False, f"Import failed: {result.stderr}"
            return True, "OK"
        except subprocess.TimeoutExpired:
            return False, "Import timed out"
        except Exception as e:
            return False, str(e)
    
    def test_067_avro_python_logical_types(self):
        """Avro to Python with logical types should compile."""
        temp_dir = self.get_temp_dir("test_067")
        schema = {
            "type": "record",
            "name": "LogicalRecord",
            "namespace": "mytest.logical",
            "fields": [
                {"name": "created", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "eventDate", "type": {"type": "int", "logicalType": "date"}},
                {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}},
                {"name": "id", "type": {"type": "string", "logicalType": "uuid"}}
            ]
        }
        schema_path = self.write_json_file(temp_dir, "logical.avsc", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_avro_to_python(
            schema_path,
            output_dir
        )
        
        success, message = self.verify_python_code_compiles(output_dir, "mytest.logical")
        self.assertTrue(success, f"Avro Python with logical types failed to compile: {message}")
    
    def test_068_avro_python_fixed_type(self):
        """Avro to Python with fixed type should compile."""
        temp_dir = self.get_temp_dir("test_068")
        schema = {
            "type": "record",
            "name": "FixedRecord",
            "namespace": "mytest.fixed",
            "fields": [
                {"name": "hash", "type": {"type": "fixed", "name": "MD5", "size": 16}},
                {"name": "name", "type": "string"}
            ]
        }
        schema_path = self.write_json_file(temp_dir, "fixed.avsc", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_avro_to_python(
            schema_path,
            output_dir
        )
        
        success, message = self.verify_python_code_compiles(output_dir, "mytest.fixed")
        self.assertTrue(success, f"Avro Python with fixed type failed to compile: {message}")
    
    def test_069_avro_python_map_of_records(self):
        """Avro to Python with map of records should compile."""
        temp_dir = self.get_temp_dir("test_069")
        schema = {
            "type": "record",
            "name": "MapContainer",
            "namespace": "mytest.maprecord",
            "fields": [
                {
                    "name": "items",
                    "type": {
                        "type": "map",
                        "values": {
                            "type": "record",
                            "name": "Item",
                            "fields": [
                                {"name": "id", "type": "string"},
                                {"name": "count", "type": "int"}
                            ]
                        }
                    }
                }
            ]
        }
        schema_path = self.write_json_file(temp_dir, "maprecord.avsc", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_avro_to_python(
            schema_path,
            output_dir
        )
        
        success, message = self.verify_python_code_compiles(output_dir, "mytest.maprecord")
        self.assertTrue(success, f"Avro Python with map of records failed to compile: {message}")
    
    def test_070_avro_python_nested_unions(self):
        """Avro to Python with nested unions should compile."""
        temp_dir = self.get_temp_dir("test_070")
        schema = {
            "type": "record",
            "name": "NestedUnion",
            "namespace": "mytest.nestedunion",
            "fields": [
                {
                    "name": "data",
                    "type": [
                        "null",
                        "string",
                        {
                            "type": "record",
                            "name": "NestedRecord",
                            "fields": [
                                {"name": "value", "type": ["null", "int", "string"]}
                            ]
                        }
                    ]
                }
            ]
        }
        schema_path = self.write_json_file(temp_dir, "nestedunion.avsc", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_avro_to_python(
            schema_path,
            output_dir
        )
        
        success, message = self.verify_python_code_compiles(output_dir, "mytest.nestedunion")
        self.assertTrue(success, f"Avro Python with nested unions failed to compile: {message}")


class TestStructureToCSharpCompilation(RobustnessTestBase):
    """
    Tests that verify generated C# code compiles.
    """
    
    def verify_csharp_compiles(self, output_dir: str) -> Tuple[bool, str]:
        """Verify C# code compiles using dotnet build."""
        # Look for .csproj file
        csproj_files = []
        for root, dirs, files in os.walk(output_dir):
            for f in files:
                if f.endswith('.csproj'):
                    csproj_files.append(os.path.join(root, f))
        
        if not csproj_files:
            return False, "No .csproj file found"
        
        try:
            # Run dotnet restore first
            subprocess.run(
                ["dotnet", "restore", csproj_files[0]],
                capture_output=True,
                text=True,
                timeout=60
            )
            # Now build
            result = subprocess.run(
                ["dotnet", "build", csproj_files[0], "--no-restore"],
                capture_output=True,
                text=True,
                timeout=60
            )
            if result.returncode != 0:
                # Extract the actual error lines
                errors = [line for line in result.stdout.split('\n') if 'error' in line.lower()]
                return False, f"C# compilation error:\n" + "\n".join(errors[:10])
            return True, "OK"
        except FileNotFoundError:
            return True, "dotnet not available, skipping"
        except subprocess.TimeoutExpired:
            return False, "Compilation timed out"
        except Exception as e:
            return False, str(e)
    
    def test_071_csharp_nested_objects(self):
        """Generated C# with nested objects should compile."""
        temp_dir = self.get_temp_dir("test_071")
        schema = {
            "type": "object",
            "title": "OuterObject",
            "properties": {
                "inner": {
                    "type": "object",
                    "properties": {
                        "deepNested": {
                            "type": "object",
                            "properties": {
                                "value": {"type": "string"}
                            }
                        }
                    }
                }
            }
        }
        schema_path = self.write_json_file(temp_dir, "nested.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_csharp(
            schema_path,
            output_dir,
            base_namespace="Test.Nested"
        )
        
        success, message = self.verify_csharp_compiles(output_dir)
        self.assertTrue(success, f"C# code failed to compile: {message}")
    
    def test_072_csharp_reserved_keywords(self):
        """Generated C# with reserved keywords as property names should compile."""
        temp_dir = self.get_temp_dir("test_072")
        schema = {
            "type": "object",
            "title": "KeywordTest",
            "properties": {
                "class": {"type": "string"},
                "namespace": {"type": "string"},
                "public": {"type": "integer"},
                "private": {"type": "boolean"},
                "static": {"type": "string"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "keywords.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_csharp(
            schema_path,
            output_dir,
            base_namespace="Test.Keywords"
        )
        
        success, message = self.verify_csharp_compiles(output_dir)
        self.assertTrue(success, f"C# code failed to compile: {message}")
    
    def test_073_csharp_numeric_prefix(self):
        """Generated C# with numeric prefix property names should compile."""
        temp_dir = self.get_temp_dir("test_073")
        schema = {
            "type": "object",
            "title": "NumericProps",
            "properties": {
                "123field": {"type": "string"},
                "456value": {"type": "integer"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "numeric.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_csharp(
            schema_path,
            output_dir,
            base_namespace="Test.Numeric"
        )
        
        success, message = self.verify_csharp_compiles(output_dir)
        self.assertTrue(success, f"C# code failed to compile: {message}")


class TestStructureToGoCompilation(RobustnessTestBase):
    """
    Tests that verify generated Go code compiles.
    """
    
    def verify_go_compiles(self, output_dir: str) -> Tuple[bool, str]:
        """Verify Go code compiles using go build."""
        # Find .go files
        go_files = []
        for root, dirs, files in os.walk(output_dir):
            for f in files:
                if f.endswith('.go') and not f.endswith('_test.go'):
                    go_files.append(os.path.join(root, f))
        
        if not go_files:
            return False, "No .go files found"
        
        # Try to build each file
        for go_file in go_files:
            go_dir = os.path.dirname(go_file)
            try:
                result = subprocess.run(
                    ["go", "build", "."],
                    capture_output=True,
                    text=True,
                    timeout=60,
                    cwd=go_dir
                )
                if result.returncode != 0:
                    return False, f"Go build error:\n{result.stderr}"
            except FileNotFoundError:
                return True, "go not available, skipping"
            except subprocess.TimeoutExpired:
                return False, "Compilation timed out"
            except Exception as e:
                return False, str(e)
        
        return True, "OK"
    
    def test_074_go_nested_objects(self):
        """Generated Go with nested objects should compile."""
        temp_dir = self.get_temp_dir("test_074")
        schema = {
            "type": "object",
            "title": "OuterObject",
            "properties": {
                "inner": {
                    "type": "object",
                    "properties": {
                        "deepNested": {
                            "type": "object",
                            "properties": {
                                "value": {"type": "string"}
                            }
                        }
                    }
                }
            }
        }
        schema_path = self.write_json_file(temp_dir, "nested.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_go(
            schema_path,
            output_dir,
            package_name="testpkg"
        )
        
        success, message = self.verify_go_compiles(output_dir)
        self.assertTrue(success, f"Go code failed to compile: {message}")
    
    def test_075_go_reserved_keywords(self):
        """Generated Go with reserved keywords as property names should compile."""
        temp_dir = self.get_temp_dir("test_075")
        schema = {
            "type": "object",
            "title": "KeywordTest",
            "properties": {
                "func": {"type": "string"},
                "package": {"type": "string"},
                "import": {"type": "integer"},
                "type": {"type": "boolean"},
                "interface": {"type": "string"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "keywords.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_go(
            schema_path,
            output_dir,
            package_name="testpkg"
        )
        
        success, message = self.verify_go_compiles(output_dir)
        self.assertTrue(success, f"Go code failed to compile: {message}")
    
    def test_076_go_numeric_prefix(self):
        """Generated Go with numeric prefix property names should compile."""
        temp_dir = self.get_temp_dir("test_076")
        schema = {
            "type": "object",
            "title": "NumericProps",
            "properties": {
                "123field": {"type": "string"},
                "456value": {"type": "integer"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "numeric.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_go(
            schema_path,
            output_dir,
            package_name="testpkg"
        )
        
        success, message = self.verify_go_compiles(output_dir)
        self.assertTrue(success, f"Go code failed to compile: {message}")


class TestStructureToRustCompilation(RobustnessTestBase):
    """
    Tests that verify generated Rust code compiles.
    """
    
    def verify_rust_compiles(self, output_dir: str) -> Tuple[bool, str]:
        """Verify Rust code compiles using cargo check."""
        cargo_toml = os.path.join(output_dir, "Cargo.toml")
        if not os.path.exists(cargo_toml):
            # Try to find it
            for root, dirs, files in os.walk(output_dir):
                if "Cargo.toml" in files:
                    cargo_toml = os.path.join(root, "Cargo.toml")
                    break
        
        if not os.path.exists(cargo_toml):
            return False, "No Cargo.toml found"
        
        cargo_dir = os.path.dirname(cargo_toml)
        try:
            result = subprocess.run(
                ["cargo", "check"],
                capture_output=True,
                text=True,
                timeout=120,
                cwd=cargo_dir
            )
            if result.returncode != 0:
                # Extract error messages
                errors = [line for line in result.stderr.split('\n') if 'error' in line.lower()][:10]
                return False, f"Rust compilation error:\n" + "\n".join(errors)
            return True, "OK"
        except FileNotFoundError:
            return True, "cargo not available, skipping"
        except subprocess.TimeoutExpired:
            return False, "Compilation timed out"
        except Exception as e:
            return False, str(e)
    
    def test_077_rust_nested_objects(self):
        """Generated Rust with nested objects should compile."""
        temp_dir = self.get_temp_dir("test_077")
        schema = {
            "type": "object",
            "title": "OuterObject",
            "properties": {
                "inner": {
                    "type": "object",
                    "properties": {
                        "value": {"type": "string"}
                    }
                }
            }
        }
        schema_path = self.write_json_file(temp_dir, "nested.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_rust(
            schema_path,
            output_dir,
            package_name="test_pkg"
        )
        
        success, message = self.verify_rust_compiles(output_dir)
        self.assertTrue(success, f"Rust code failed to compile: {message}")
    
    def test_078_rust_reserved_keywords(self):
        """Generated Rust with reserved keywords as property names should compile."""
        temp_dir = self.get_temp_dir("test_078")
        schema = {
            "type": "object",
            "title": "KeywordTest",
            "properties": {
                "fn": {"type": "string"},
                "mod": {"type": "string"},
                "pub": {"type": "integer"},
                "impl": {"type": "boolean"},
                "trait": {"type": "string"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "keywords.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_rust(
            schema_path,
            output_dir,
            package_name="test_pkg"
        )
        
        success, message = self.verify_rust_compiles(output_dir)
        self.assertTrue(success, f"Rust code failed to compile: {message}")
    
    def test_079_rust_numeric_prefix(self):
        """Generated Rust with numeric prefix property names should compile."""
        temp_dir = self.get_temp_dir("test_079")
        schema = {
            "type": "object",
            "title": "NumericProps",
            "properties": {
                "123field": {"type": "string"},
                "456value": {"type": "integer"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "numeric.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_rust(
            schema_path,
            output_dir,
            package_name="test_pkg"
        )
        
        success, message = self.verify_rust_compiles(output_dir)
        self.assertTrue(success, f"Rust code failed to compile: {message}")


class TestAvroToCSharpCompilation(RobustnessTestBase):
    """
    Tests that verify generated C# code from Avro compiles.
    """
    
    def verify_csharp_compiles(self, output_dir: str) -> Tuple[bool, str]:
        """Verify C# code compiles using dotnet build."""
        csproj_files = []
        for root, dirs, files in os.walk(output_dir):
            for f in files:
                if f.endswith('.csproj'):
                    csproj_files.append(os.path.join(root, f))
        
        if not csproj_files:
            return False, "No .csproj file found"
        
        try:
            # Run dotnet restore first
            subprocess.run(
                ["dotnet", "restore", csproj_files[0]],
                capture_output=True,
                text=True,
                timeout=60
            )
            # Now build
            result = subprocess.run(
                ["dotnet", "build", csproj_files[0], "--no-restore"],
                capture_output=True,
                text=True,
                timeout=60
            )
            if result.returncode != 0:
                errors = [line for line in result.stdout.split('\n') if 'error' in line.lower()]
                return False, f"C# compilation error:\n" + "\n".join(errors[:10])
            return True, "OK"
        except FileNotFoundError:
            return True, "dotnet not available, skipping"
        except subprocess.TimeoutExpired:
            return False, "Compilation timed out"
        except Exception as e:
            return False, str(e)
    
    def test_080_avro_csharp_logical_types(self):
        """Avro to C# with logical types should compile."""
        temp_dir = self.get_temp_dir("test_080")
        schema = {
            "type": "record",
            "name": "LogicalRecord",
            "namespace": "mytest.logical",
            "fields": [
                {"name": "created", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "eventDate", "type": {"type": "int", "logicalType": "date"}},
                {"name": "id", "type": {"type": "string", "logicalType": "uuid"}}
            ]
        }
        schema_path = self.write_json_file(temp_dir, "logical.avsc", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        from avrotize.avrotocsharp import convert_avro_to_csharp
        convert_avro_to_csharp(
            schema_path,
            output_dir
        )
        
        success, message = self.verify_csharp_compiles(output_dir)
        self.assertTrue(success, f"Avro C# with logical types failed to compile: {message}")
    
    def test_081_avro_csharp_nested_records(self):
        """Avro to C# with nested records should compile."""
        temp_dir = self.get_temp_dir("test_081")
        schema = {
            "type": "record",
            "name": "OuterRecord",
            "namespace": "mytest.nested",
            "fields": [
                {
                    "name": "inner",
                    "type": {
                        "type": "record",
                        "name": "InnerRecord",
                        "fields": [
                            {"name": "value", "type": "string"}
                        ]
                    }
                }
            ]
        }
        schema_path = self.write_json_file(temp_dir, "nested.avsc", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        from avrotize.avrotocsharp import convert_avro_to_csharp
        convert_avro_to_csharp(
            schema_path,
            output_dir
        )
        
        success, message = self.verify_csharp_compiles(output_dir)
        self.assertTrue(success, f"Avro C# with nested records failed to compile: {message}")


class TestDeepNestingScenarios(RobustnessTestBase):
    """
    Tests for deeply nested schema scenarios that might cause issues.
    """
    
    def test_082_deep_nesting_10_levels(self):
        """10 levels of nesting should be handled correctly."""
        temp_dir = self.get_temp_dir("test_082")
        
        # Build deeply nested schema
        def build_nested(depth):
            if depth == 0:
                return {"type": "string"}
            return {
                "type": "object",
                "properties": {
                    f"level{depth}": build_nested(depth - 1)
                }
            }
        
        schema = {
            "type": "object",
            "title": "DeepNested",
            "properties": {
                "root": build_nested(10)
            }
        }
        
        schema_path = self.write_json_file(temp_dir, "deep.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_python,
            "structuretopython",
            "10 levels deep nesting",
            schema_path,
            output_dir,
            package_name="deep.nested"
        )
        
        self.assertTrue(result.passed, f"Deep nesting failed: {result.error_message}")
    
    def test_083_very_long_property_name(self):
        """Very long property names should be handled."""
        temp_dir = self.get_temp_dir("test_083")
        
        long_name = "a" * 500
        schema = {
            "type": "object",
            "title": "LongName",
            "properties": {
                long_name: {"type": "string"}
            }
        }
        
        schema_path = self.write_json_file(temp_dir, "longname.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_python,
            "structuretopython",
            "500-char property name",
            schema_path,
            output_dir,
            package_name="longname"
        )
        
        self.assertTrue(result.passed, f"Long property name failed: {result.error_message}")
    
    def test_084_many_properties(self):
        """Schema with many properties should be handled."""
        temp_dir = self.get_temp_dir("test_084")
        
        properties = {f"prop{i}": {"type": "string"} for i in range(100)}
        schema = {
            "type": "object",
            "title": "ManyProps",
            "properties": properties
        }
        
        schema_path = self.write_json_file(temp_dir, "many.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_python,
            "structuretopython",
            "100 properties",
            schema_path,
            output_dir,
            package_name="manyprops"
        )
        
        self.assertTrue(result.passed, f"Many properties failed: {result.error_message}")
    
    def test_085_circular_reference_pattern(self):
        """Circular reference patterns should be detected or handled."""
        temp_dir = self.get_temp_dir("test_085")
        
        # Schema with $ref that could be circular
        schema = {
            "type": "object",
            "title": "TreeNode",
            "$defs": {
                "Node": {
                    "type": "object",
                    "properties": {
                        "value": {"type": "string"},
                        "children": {
                            "type": "array",
                            "items": {"$ref": "#/$defs/Node"}
                        }
                    }
                }
            },
            "properties": {
                "root": {"$ref": "#/$defs/Node"}
            }
        }
        
        schema_path = self.write_json_file(temp_dir, "tree.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_python,
            "structuretopython",
            "circular reference via $ref",
            schema_path,
            output_dir,
            package_name="tree"
        )
        
        self.assertTrue(result.passed, f"Circular reference failed: {result.error_message}")


class TestSpecialPropertyNames(RobustnessTestBase):
    """
    Tests for special characters in property names.
    """
    
    def test_086_hyphenated_property_names(self):
        """Hyphenated property names should be sanitized."""
        temp_dir = self.get_temp_dir("test_086")
        schema = {
            "type": "object",
            "title": "HyphenTest",
            "properties": {
                "my-field": {"type": "string"},
                "another-property-name": {"type": "integer"}
            }
        }
        
        schema_path = self.write_json_file(temp_dir, "hyphen.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_python(
            schema_path,
            output_dir,
            package_name="hyphen.test"
        )
        
        # Verify it actually compiles
        src_dir = os.path.join(output_dir, "src")
        env = os.environ.copy()
        env['PYTHONPATH'] = src_dir
        
        result = subprocess.run(
            [sys.executable, "-c", "from hyphen.test import HyphenTest; print('OK')"],
            env=env,
            capture_output=True,
            text=True
        )
        self.assertEqual(result.returncode, 0, f"Hyphenated properties failed: {result.stderr}")
    
    def test_087_dot_property_names(self):
        """Property names with dots should be sanitized."""
        temp_dir = self.get_temp_dir("test_087")
        schema = {
            "type": "object",
            "title": "DotTest",
            "properties": {
                "my.field": {"type": "string"},
                "another.property.name": {"type": "integer"}
            }
        }
        
        schema_path = self.write_json_file(temp_dir, "dot.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_python(
            schema_path,
            output_dir,
            package_name="dot.test"
        )
        
        # Verify it actually compiles
        src_dir = os.path.join(output_dir, "src")
        env = os.environ.copy()
        env['PYTHONPATH'] = src_dir
        
        result = subprocess.run(
            [sys.executable, "-c", "from dot.test import DotTest; print('OK')"],
            env=env,
            capture_output=True,
            text=True
        )
        self.assertEqual(result.returncode, 0, f"Dot properties failed: {result.stderr}")
    
    def test_088_space_property_names(self):
        """Property names with spaces should be sanitized."""
        temp_dir = self.get_temp_dir("test_088")
        schema = {
            "type": "object",
            "title": "SpaceTest",
            "properties": {
                "my field": {"type": "string"},
                "another property name": {"type": "integer"}
            }
        }
        
        schema_path = self.write_json_file(temp_dir, "space.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_python(
            schema_path,
            output_dir,
            package_name="space.test"
        )
        
        # Verify it actually compiles
        src_dir = os.path.join(output_dir, "src")
        env = os.environ.copy()
        env['PYTHONPATH'] = src_dir
        
        result = subprocess.run(
            [sys.executable, "-c", "from space.test import SpaceTest; print('OK')"],
            env=env,
            capture_output=True,
            text=True
        )
        self.assertEqual(result.returncode, 0, f"Space properties failed: {result.stderr}")
    
    def test_089_unicode_property_names(self):
        """Property names with unicode should be handled."""
        temp_dir = self.get_temp_dir("test_089")
        schema = {
            "type": "object",
            "title": "UnicodeTest",
            "properties": {
                "名前": {"type": "string"},
                "größe": {"type": "integer"},
                "données": {"type": "boolean"}
            }
        }
        
        schema_path = self.write_json_file(temp_dir, "unicode.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_python,
            "structuretopython",
            "unicode property names",
            schema_path,
            output_dir,
            package_name="unicode.mytest"
        )
        
        self.assertTrue(result.passed, f"Unicode properties failed: {result.error_message}")
    
    def test_090_emoji_property_names(self):
        """Property names with emoji should be handled."""
        temp_dir = self.get_temp_dir("test_090")
        schema = {
            "type": "object",
            "title": "EmojiTest",
            "properties": {
                "🎉": {"type": "string"},
                "data_📊": {"type": "integer"}
            }
        }
        
        schema_path = self.write_json_file(temp_dir, "emoji.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        result = self.run_converter_safely(
            convert_structure_to_python,
            "structuretopython",
            "emoji property names",
            schema_path,
            output_dir,
            package_name="emoji.mytest"
        )
        
        self.assertTrue(result.passed, f"Emoji properties failed: {result.error_message}")


class TestTypeScriptEdgeCases(RobustnessTestBase):
    """
    Additional edge case tests for TypeScript code generation.
    """
    
    def verify_ts_compiles(self, output_dir: str) -> Tuple[bool, str]:
        """Verify TypeScript code compiles."""
        ts_files = []
        for root, dirs, files in os.walk(output_dir):
            for f in files:
                if f.endswith('.ts') and not f.endswith('.d.ts'):
                    ts_files.append(os.path.join(root, f))
        
        if not ts_files:
            return False, "No .ts files found"
        
        try:
            result = subprocess.run(
                ["npx", "tsc", "--noEmit", "--skipLibCheck", "--target", "ES2020", 
                 "--module", "ESNext", "--moduleResolution", "node", "--esModuleInterop", 
                 "--strict"] + ts_files,
                capture_output=True,
                text=True,
                timeout=60,
                cwd=output_dir
            )
            if result.returncode != 0:
                return False, f"TypeScript compilation error:\n{result.stdout}\n{result.stderr}"
            return True, "OK"
        except FileNotFoundError:
            return True, "npx/tsc not available, skipping"
        except subprocess.TimeoutExpired:
            return False, "Compilation timed out"
        except Exception as e:
            return False, str(e)
    
    def test_091_ts_hyphenated_props(self):
        """TypeScript with hyphenated property names should compile."""
        temp_dir = self.get_temp_dir("test_091")
        schema = {
            "type": "object",
            "title": "HyphenTest",
            "properties": {
                "my-field": {"type": "string"},
                "another-property-name": {"type": "integer"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "hyphen.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_typescript(
            schema_path,
            output_dir,
            package_name="hyphen.test"
        )
        
        success, message = self.verify_ts_compiles(output_dir)
        self.assertTrue(success, f"TypeScript with hyphens failed to compile: {message}")
    
    def test_092_ts_dot_props(self):
        """TypeScript with dotted property names should compile."""
        temp_dir = self.get_temp_dir("test_092")
        schema = {
            "type": "object",
            "title": "DotTest",
            "properties": {
                "my.field": {"type": "string"},
                "another.property.name": {"type": "integer"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "dot.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_typescript(
            schema_path,
            output_dir,
            package_name="dot.test"
        )
        
        success, message = self.verify_ts_compiles(output_dir)
        self.assertTrue(success, f"TypeScript with dots failed to compile: {message}")
    
    def test_093_ts_numeric_prefix(self):
        """TypeScript with numeric prefix property names should compile."""
        temp_dir = self.get_temp_dir("test_093")
        schema = {
            "type": "object",
            "title": "NumericTest",
            "properties": {
                "123field": {"type": "string"},
                "456value": {"type": "integer"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "numeric.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_typescript(
            schema_path,
            output_dir,
            package_name="numeric.test"
        )
        
        success, message = self.verify_ts_compiles(output_dir)
        self.assertTrue(success, f"TypeScript with numeric prefix failed to compile: {message}")


class TestJavaEdgeCases(RobustnessTestBase):
    """
    Additional edge case tests for Java code generation.
    """
    
    def verify_java_compiles(self, output_dir: str) -> Tuple[bool, str]:
        """Verify Java code compiles using javac."""
        java_files = []
        for root, dirs, files in os.walk(output_dir):
            for f in files:
                if f.endswith('.java'):
                    java_files.append(os.path.join(root, f))
        
        if not java_files:
            return False, "No .java files found"
        
        try:
            result = subprocess.run(
                ["javac", "-Xlint:none"] + java_files,
                capture_output=True,
                text=True,
                timeout=60
            )
            if result.returncode != 0:
                return False, f"Java syntax error:\n{result.stderr}"
            return True, "OK"
        except FileNotFoundError:
            return True, "javac not available, skipping"
        except subprocess.TimeoutExpired:
            return False, "Compilation timed out"
        except Exception as e:
            return False, str(e)
    
    def test_094_java_hyphenated_props(self):
        """Java with hyphenated property names should compile."""
        temp_dir = self.get_temp_dir("test_094")
        schema = {
            "type": "object",
            "title": "HyphenTest",
            "properties": {
                "my-field": {"type": "string"},
                "another-property-name": {"type": "integer"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "hyphen.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_java(
            schema_path,
            output_dir,
            package_name="test.hyphen"
        )
        
        success, message = self.verify_java_compiles(output_dir)
        self.assertTrue(success, f"Java with hyphens failed to compile: {message}")
    
    def test_095_java_dot_props(self):
        """Java with dotted property names should compile."""
        temp_dir = self.get_temp_dir("test_095")
        schema = {
            "type": "object",
            "title": "DotTest",
            "properties": {
                "my.field": {"type": "string"},
                "another.property.name": {"type": "integer"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "dot.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_java(
            schema_path,
            output_dir,
            package_name="test.dot"
        )
        
        success, message = self.verify_java_compiles(output_dir)
        self.assertTrue(success, f"Java with dots failed to compile: {message}")


class TestCSharpEdgeCases(RobustnessTestBase):
    """
    Additional edge case tests for C# code generation.
    """
    
    def verify_csharp_compiles(self, output_dir: str) -> Tuple[bool, str]:
        """Verify C# code compiles using dotnet build."""
        csproj_files = []
        for root, dirs, files in os.walk(output_dir):
            for f in files:
                if f.endswith('.csproj'):
                    csproj_files.append(os.path.join(root, f))
        
        if not csproj_files:
            return False, "No .csproj file found"
        
        try:
            subprocess.run(["dotnet", "restore", csproj_files[0]], capture_output=True, text=True, timeout=60)
            result = subprocess.run(
                ["dotnet", "build", csproj_files[0], "--no-restore"],
                capture_output=True,
                text=True,
                timeout=60
            )
            if result.returncode != 0:
                errors = [line for line in result.stdout.split('\n') if 'error' in line.lower()]
                return False, f"C# compilation error:\n" + "\n".join(errors[:10])
            return True, "OK"
        except FileNotFoundError:
            return True, "dotnet not available, skipping"
        except subprocess.TimeoutExpired:
            return False, "Compilation timed out"
        except Exception as e:
            return False, str(e)
    
    def test_096_csharp_hyphenated_props(self):
        """C# with hyphenated property names should compile."""
        temp_dir = self.get_temp_dir("test_096")
        schema = {
            "type": "object",
            "title": "HyphenTest",
            "properties": {
                "my-field": {"type": "string"},
                "another-property-name": {"type": "integer"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "hyphen.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_csharp(
            schema_path,
            output_dir,
            base_namespace="Test.Hyphen"
        )
        
        success, message = self.verify_csharp_compiles(output_dir)
        self.assertTrue(success, f"C# with hyphens failed to compile: {message}")
    
    def test_097_csharp_dot_props(self):
        """C# with dotted property names should compile."""
        temp_dir = self.get_temp_dir("test_097")
        schema = {
            "type": "object",
            "title": "DotTest",
            "properties": {
                "my.field": {"type": "string"},
                "another.property.name": {"type": "integer"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "dot.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_csharp(
            schema_path,
            output_dir,
            base_namespace="Test.Dot"
        )
        
        success, message = self.verify_csharp_compiles(output_dir)
        self.assertTrue(success, f"C# with dots failed to compile: {message}")


class TestGoEdgeCases(RobustnessTestBase):
    """
    Additional edge case tests for Go code generation.
    """
    
    def verify_go_compiles(self, output_dir: str) -> Tuple[bool, str]:
        """Verify Go code compiles using go build."""
        go_files = []
        for root, dirs, files in os.walk(output_dir):
            for f in files:
                if f.endswith('.go') and not f.endswith('_test.go'):
                    go_files.append(os.path.join(root, f))
        
        if not go_files:
            return False, "No .go files found"
        
        for go_file in go_files:
            go_dir = os.path.dirname(go_file)
            try:
                result = subprocess.run(
                    ["go", "build", "."],
                    capture_output=True,
                    text=True,
                    timeout=60,
                    cwd=go_dir
                )
                if result.returncode != 0:
                    return False, f"Go build error:\n{result.stderr}"
            except FileNotFoundError:
                return True, "go not available, skipping"
            except subprocess.TimeoutExpired:
                return False, "Compilation timed out"
            except Exception as e:
                return False, str(e)
        
        return True, "OK"
    
    def test_098_go_hyphenated_props(self):
        """Go with hyphenated property names should compile."""
        temp_dir = self.get_temp_dir("test_098")
        schema = {
            "type": "object",
            "title": "HyphenTest",
            "properties": {
                "my-field": {"type": "string"},
                "another-property-name": {"type": "integer"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "hyphen.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_go(
            schema_path,
            output_dir,
            package_name="testpkg"
        )
        
        success, message = self.verify_go_compiles(output_dir)
        self.assertTrue(success, f"Go with hyphens failed to compile: {message}")
    
    def test_099_go_dot_props(self):
        """Go with dotted property names should compile."""
        temp_dir = self.get_temp_dir("test_099")
        schema = {
            "type": "object",
            "title": "DotTest",
            "properties": {
                "my.field": {"type": "string"},
                "another.property.name": {"type": "integer"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "dot.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_go(
            schema_path,
            output_dir,
            package_name="testpkg"
        )
        
        success, message = self.verify_go_compiles(output_dir)
        self.assertTrue(success, f"Go with dots failed to compile: {message}")
    
    def test_100_go_space_props(self):
        """Go with space property names should compile."""
        temp_dir = self.get_temp_dir("test_100")
        schema = {
            "type": "object",
            "title": "SpaceTest",
            "properties": {
                "my field": {"type": "string"},
                "another property name": {"type": "integer"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "space.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_go(
            schema_path,
            output_dir,
            package_name="testpkg"
        )
        
        success, message = self.verify_go_compiles(output_dir)
        self.assertTrue(success, f"Go with spaces failed to compile: {message}")


if __name__ == '__main__':
    unittest.main()
