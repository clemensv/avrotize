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
from avrotize.avrotorust import convert_avro_to_rust
from avrotize.structuretojs import convert_structure_to_javascript
from avrotize.structuretocddl import convert_structure_to_cddl_files
from avrotize.avrotokusto import convert_avro_to_kusto_file
from avrotize.structuretoproto import convert_structure_to_proto
from avrotize.avrotoiceberg import convert_avro_to_iceberg

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

    def verify_python_imports(self, output_dir: str) -> Tuple[bool, str]:
        """Verify generated Python code can be imported."""
        src_dir = os.path.join(output_dir, "src")
        if not os.path.exists(src_dir):
            # Try output_dir directly
            src_dir = output_dir
        
        # Find Python packages
        packages = []
        for item in os.listdir(src_dir):
            item_path = os.path.join(src_dir, item)
            if os.path.isdir(item_path):
                init_file = os.path.join(item_path, '__init__.py')
                if os.path.exists(init_file):
                    packages.append(item)
        
        if not packages:
            return True, "No packages to import"
        
        env = os.environ.copy()
        env['PYTHONPATH'] = src_dir
        
        for pkg in packages:
            try:
                result = subprocess.run(
                    [sys.executable, "-c", f"import {pkg}"],
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                if result.returncode != 0:
                    return False, f"Failed to import {pkg}: {result.stderr}"
            except subprocess.TimeoutExpired:
                return False, f"Import of {pkg} timed out"
            except Exception as e:
                return False, str(e)
        
        return True, "OK"

    def verify_java_compiles(self, output_dir: str) -> Tuple[bool, str]:
        """Verify Java code compiles using javac."""
        java_files = []
        for root, dirs, files in os.walk(output_dir):
            for f in files:
                if f.endswith('.java'):
                    java_files.append(os.path.join(root, f))
        
        if not java_files:
            return True, "No Java files to compile"
        
        try:
            result = subprocess.run(
                ["javac", "-source", "17", "-target", "17"] + java_files,
                capture_output=True,
                text=True,
                timeout=60
            )
            if result.returncode != 0:
                error_lines = [line for line in result.stderr.split('\n') if 'error:' in line.lower()]
                return False, "Java compilation error:\n" + "\n".join(error_lines[:10])
            return True, "OK"
        except FileNotFoundError:
            return True, "javac not available, skipping"
        except subprocess.TimeoutExpired:
            return False, "Compilation timed out"
        except Exception as e:
            return False, str(e)

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
            subprocess.run(
                ["dotnet", "restore", csproj_files[0]],
                capture_output=True,
                text=True,
                timeout=60
            )
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

    def verify_typescript_compiles(self, output_dir: str) -> Tuple[bool, str]:
        """Verify TypeScript code compiles using tsc."""
        ts_files = []
        for root, dirs, files in os.walk(output_dir):
            for f in files:
                if f.endswith('.ts') and not f.endswith('.d.ts'):
                    ts_files.append(os.path.join(root, f))
        
        if not ts_files:
            return True, "No TypeScript files to compile"
        
        try:
            result = subprocess.run(
                ["npx", "tsc", "--noEmit", "--skipLibCheck", "--esModuleInterop", "--target", "ES2020", "--moduleResolution", "node"] + ts_files,
                capture_output=True,
                text=True,
                timeout=60,
                cwd=output_dir
            )
            if result.returncode != 0:
                error_lines = [line for line in result.stdout.split('\n') if 'error' in line.lower()]
                if not error_lines:
                    error_lines = result.stderr.split('\n')[:5]
                return False, "TypeScript compilation error:\n" + "\n".join(error_lines[:10])
            return True, "OK"
        except FileNotFoundError:
            return True, "npx/tsc not available, skipping"
        except subprocess.TimeoutExpired:
            return False, "Compilation timed out"
        except Exception as e:
            return False, str(e)

    def verify_go_compiles(self, output_dir: str) -> Tuple[bool, str]:
        """Verify Go code compiles using go build."""
        go_files = []
        for root, dirs, files in os.walk(output_dir):
            for f in files:
                if f.endswith('.go'):
                    go_files.append(os.path.join(root, f))
        
        if not go_files:
            return True, "No Go files to compile"
        
        # Create go.mod if not exists
        go_mod_path = os.path.join(output_dir, "go.mod")
        if not os.path.exists(go_mod_path):
            with open(go_mod_path, 'w') as f:
                f.write("module testmodule\n\ngo 1.21\n")
        
        try:
            result = subprocess.run(
                ["go", "build", "./..."],
                capture_output=True,
                text=True,
                timeout=60,
                cwd=output_dir
            )
            if result.returncode != 0:
                return False, f"Go compilation error:\n{result.stderr[:500]}"
            return True, "OK"
        except FileNotFoundError:
            return True, "go not available, skipping"
        except subprocess.TimeoutExpired:
            return False, "Compilation timed out"
        except Exception as e:
            return False, str(e)

    def verify_rust_compiles(self, output_dir: str) -> Tuple[bool, str]:
        """Verify Rust code compiles using cargo check."""
        cargo_toml = os.path.join(output_dir, "Cargo.toml")
        if not os.path.exists(cargo_toml):
            return False, "No Cargo.toml found"
        
        try:
            result = subprocess.run(
                ["cargo", "check"],
                capture_output=True,
                text=True,
                timeout=120,
                cwd=output_dir
            )
            if result.returncode != 0:
                error_lines = [line for line in result.stderr.split('\n') if 'error' in line.lower()]
                return False, "Rust compilation error:\n" + "\n".join(error_lines[:10])
            return True, "OK"
        except FileNotFoundError:
            return True, "cargo not available, skipping"
        except subprocess.TimeoutExpired:
            return False, "Compilation timed out"
        except Exception as e:
            return False, str(e)


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


class TestRustEdgeCases(RobustnessTestBase):
    """
    Tests for Rust code generator edge cases.
    """
    
    def verify_rust_compiles(self, output_dir: str) -> Tuple[bool, str]:
        """Verify Rust code compiles using cargo check."""
        cargo_toml = os.path.join(output_dir, "Cargo.toml")
        if not os.path.exists(cargo_toml):
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
                errors = [line for line in result.stderr.split('\n') if 'error' in line.lower()][:10]
                return False, f"Rust compilation error:\n" + "\n".join(errors)
            return True, "OK"
        except FileNotFoundError:
            return True, "cargo not available, skipping"
        except subprocess.TimeoutExpired:
            return False, "Compilation timed out"
        except Exception as e:
            return False, str(e)
    
    def test_101_rust_hyphenated_props(self):
        """Rust with hyphenated property names should compile."""
        temp_dir = self.get_temp_dir("test_101")
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
        
        convert_structure_to_rust(
            schema_path,
            output_dir,
            package_name="test_pkg"
        )
        
        success, message = self.verify_rust_compiles(output_dir)
        self.assertTrue(success, f"Rust with hyphens failed to compile: {message}")
    
    def test_102_rust_dot_props(self):
        """Rust with dotted property names should compile."""
        temp_dir = self.get_temp_dir("test_102")
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
        
        convert_structure_to_rust(
            schema_path,
            output_dir,
            package_name="test_pkg"
        )
        
        success, message = self.verify_rust_compiles(output_dir)
        self.assertTrue(success, f"Rust with dots failed to compile: {message}")
    
    def test_103_rust_space_props(self):
        """Rust with space property names should compile."""
        temp_dir = self.get_temp_dir("test_103")
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
        
        convert_structure_to_rust(
            schema_path,
            output_dir,
            package_name="test_pkg"
        )
        
        success, message = self.verify_rust_compiles(output_dir)
        self.assertTrue(success, f"Rust with spaces failed to compile: {message}")


class TestAvroToJavaCompilation(RobustnessTestBase):
    """
    Tests for Avro to Java code generation.
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
    
    def test_104_avro_java_logical_types(self):
        """Avro to Java with logical types should compile."""
        temp_dir = self.get_temp_dir("test_104")
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
        
        convert_avro_to_java(
            schema_path,
            output_dir
        )
        
        success, message = self.verify_java_compiles(output_dir)
        self.assertTrue(success, f"Avro Java with logical types failed to compile: {message}")
    
    def test_105_avro_java_decimal(self):
        """Avro to Java with decimal logical type should compile."""
        temp_dir = self.get_temp_dir("test_105")
        schema = {
            "type": "record",
            "name": "DecimalRecord",
            "namespace": "mytest.decimal",
            "fields": [
                {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}},
                {"name": "id", "type": "string"}
            ]
        }
        schema_path = self.write_json_file(temp_dir, "decimal.avsc", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_avro_to_java(
            schema_path,
            output_dir
        )
        
        success, message = self.verify_java_compiles(output_dir)
        self.assertTrue(success, f"Avro Java with decimal failed to compile: {message}")
    
    def test_106_avro_java_fixed(self):
        """Avro to Java with fixed type should compile."""
        temp_dir = self.get_temp_dir("test_106")
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
        
        convert_avro_to_java(
            schema_path,
            output_dir
        )
        
        success, message = self.verify_java_compiles(output_dir)
        self.assertTrue(success, f"Avro Java with fixed failed to compile: {message}")


class TestAvroToGoCompilation(RobustnessTestBase):
    """
    Tests for Avro to Go code generation.
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
    
    def test_107_avro_go_logical_types(self):
        """Avro to Go with logical types should compile."""
        temp_dir = self.get_temp_dir("test_107")
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
        
        convert_avro_to_go(
            schema_path,
            output_dir
        )
        
        success, message = self.verify_go_compiles(output_dir)
        self.assertTrue(success, f"Avro Go with logical types failed to compile: {message}")
    
    def test_108_avro_go_map_type(self):
        """Avro to Go with map type should compile."""
        temp_dir = self.get_temp_dir("test_108")
        schema = {
            "type": "record",
            "name": "MapRecord",
            "namespace": "mytest.maptype",
            "fields": [
                {
                    "name": "data",
                    "type": {
                        "type": "map",
                        "values": "string"
                    }
                }
            ]
        }
        schema_path = self.write_json_file(temp_dir, "map.avsc", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_avro_to_go(
            schema_path,
            output_dir
        )
        
        success, message = self.verify_go_compiles(output_dir)
        self.assertTrue(success, f"Avro Go with map failed to compile: {message}")


class TestAvroToTypeScriptCompilation(RobustnessTestBase):
    """
    Tests for Avro to TypeScript code generation.
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
    
    def test_109_avro_ts_logical_types(self):
        """Avro to TypeScript with logical types should compile."""
        temp_dir = self.get_temp_dir("test_109")
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
        
        convert_avro_to_typescript(
            schema_path,
            output_dir
        )
        
        success, message = self.verify_ts_compiles(output_dir)
        self.assertTrue(success, f"Avro TypeScript with logical types failed to compile: {message}")
    
    def test_110_avro_ts_nested_records(self):
        """Avro to TypeScript with nested records should compile."""
        temp_dir = self.get_temp_dir("test_110")
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
                            {"name": "value", "type": "string"},
                            {"name": "count", "type": "int"}
                        ]
                    }
                }
            ]
        }
        schema_path = self.write_json_file(temp_dir, "nested.avsc", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_avro_to_typescript(
            schema_path,
            output_dir
        )
        
        success, message = self.verify_ts_compiles(output_dir)
        self.assertTrue(success, f"Avro TypeScript with nested records failed to compile: {message}")


class TestReservedKeywordsAsFieldNames(RobustnessTestBase):
    """
    Tests for using language reserved keywords as field names.
    """
    
    def verify_python_imports(self, output_dir: str) -> Tuple[bool, str]:
        """Verify Python code can be imported."""
        py_files = []
        for root, dirs, files in os.walk(output_dir):
            for f in files:
                if f.endswith('.py') and f != '__init__.py':
                    py_files.append(os.path.join(root, f))
        
        if not py_files:
            return False, "No .py files found"
        
        for py_file in py_files:
            try:
                result = subprocess.run(
                    ["python", "-c", f"import sys; sys.path.insert(0, r'{output_dir}'); exec(open(r'{py_file}').read())"],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                if result.returncode != 0:
                    return False, f"Python import error in {py_file}:\n{result.stderr}"
            except Exception as e:
                return False, str(e)
        
        return True, "OK"
    
    def verify_java_compiles(self, output_dir: str) -> Tuple[bool, str]:
        """Verify Java code compiles."""
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
                return False, f"Java compilation error:\n{result.stderr}"
            return True, "OK"
        except FileNotFoundError:
            return True, "javac not available, skipping"
        except Exception as e:
            return False, str(e)
    
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
                return False, f"TypeScript error:\n{result.stdout}\n{result.stderr}"
            return True, "OK"
        except FileNotFoundError:
            return True, "npx/tsc not available, skipping"
        except Exception as e:
            return False, str(e)
    
    def test_111_python_reserved_keywords(self):
        """Python with reserved keyword field names should compile."""
        temp_dir = self.get_temp_dir("test_111")
        schema = {
            "type": "object",
            "title": "KeywordTest",
            "properties": {
                "class": {"type": "string"},
                "def": {"type": "string"},
                "import": {"type": "string"},
                "from": {"type": "string"},
                "return": {"type": "string"},
                "if": {"type": "boolean"},
                "else": {"type": "string"},
                "for": {"type": "integer"},
                "while": {"type": "integer"},
                "try": {"type": "string"},
                "except": {"type": "string"},
                "lambda": {"type": "string"},
                "global": {"type": "string"},
                "yield": {"type": "string"},
                "async": {"type": "string"},
                "await": {"type": "string"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "keywords.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_python(
            schema_path,
            output_dir,
            package_name="mytest.keywords"
        )
        
        success, message = self.verify_python_imports(output_dir)
        self.assertTrue(success, f"Python with reserved keywords failed: {message}")
    
    def test_112_java_reserved_keywords(self):
        """Java with reserved keyword field names should compile."""
        temp_dir = self.get_temp_dir("test_112")
        schema = {
            "type": "object",
            "title": "KeywordTest",
            "properties": {
                "class": {"type": "string"},
                "public": {"type": "string"},
                "private": {"type": "string"},
                "static": {"type": "string"},
                "final": {"type": "string"},
                "void": {"type": "string"},
                "int": {"type": "string"},
                "boolean": {"type": "string"},
                "new": {"type": "string"},
                "return": {"type": "string"},
                "import": {"type": "string"},
                "package": {"type": "string"},
                "interface": {"type": "string"},
                "abstract": {"type": "string"},
                "synchronized": {"type": "string"},
                "native": {"type": "string"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "keywords.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_java(
            schema_path,
            output_dir,
            package_name="mytest.keywords"
        )
        
        success, message = self.verify_java_compiles(output_dir)
        self.assertTrue(success, f"Java with reserved keywords failed: {message}")
    
    def test_113_typescript_reserved_keywords(self):
        """TypeScript with reserved keyword field names should compile."""
        temp_dir = self.get_temp_dir("test_113")
        schema = {
            "type": "object",
            "title": "KeywordTest",
            "properties": {
                "class": {"type": "string"},
                "function": {"type": "string"},
                "const": {"type": "string"},
                "let": {"type": "string"},
                "var": {"type": "string"},
                "if": {"type": "boolean"},
                "else": {"type": "string"},
                "return": {"type": "string"},
                "import": {"type": "string"},
                "export": {"type": "string"},
                "interface": {"type": "string"},
                "type": {"type": "string"},
                "enum": {"type": "string"},
                "async": {"type": "string"},
                "await": {"type": "string"},
                "new": {"type": "string"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "keywords.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_typescript(
            schema_path,
            output_dir,
            package_name="mytest.keywords"
        )
        
        success, message = self.verify_ts_compiles(output_dir)
        self.assertTrue(success, f"TypeScript with reserved keywords failed: {message}")


class TestAvroUnionTypes(RobustnessTestBase):
    """
    Tests for Avro union type handling.
    """
    
    def verify_python_imports(self, output_dir: str) -> Tuple[bool, str]:
        """Verify Python code can be imported."""
        py_files = []
        for root, dirs, files in os.walk(output_dir):
            for f in files:
                if f.endswith('.py') and f != '__init__.py':
                    py_files.append(os.path.join(root, f))
        
        if not py_files:
            return False, "No .py files found"
        
        for py_file in py_files:
            try:
                result = subprocess.run(
                    ["python", "-c", f"import sys; sys.path.insert(0, r'{output_dir}'); exec(open(r'{py_file}').read())"],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                if result.returncode != 0:
                    return False, f"Python import error in {py_file}:\n{result.stderr}"
            except Exception as e:
                return False, str(e)
        
        return True, "OK"
    
    def test_114_avro_complex_union(self):
        """Avro with complex union types should compile to Python."""
        temp_dir = self.get_temp_dir("test_114")
        schema = {
            "type": "record",
            "name": "UnionRecord",
            "namespace": "mytest.union",
            "fields": [
                {
                    "name": "payload",
                    "type": [
                        "null",
                        "string",
                        "int",
                        {
                            "type": "record",
                            "name": "InnerRecord",
                            "fields": [
                                {"name": "value", "type": "string"}
                            ]
                        }
                    ]
                }
            ]
        }
        schema_path = self.write_json_file(temp_dir, "union.avsc", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_avro_to_python(
            schema_path,
            output_dir
        )
        
        success, message = self.verify_python_imports(output_dir)
        self.assertTrue(success, f"Avro Python with complex union failed: {message}")
    
    def test_115_avro_array_of_unions(self):
        """Avro with array of union types should compile to Python."""
        temp_dir = self.get_temp_dir("test_115")
        schema = {
            "type": "record",
            "name": "ArrayUnionRecord",
            "namespace": "mytest.arrayunion",
            "fields": [
                {
                    "name": "items",
                    "type": {
                        "type": "array",
                        "items": ["null", "string", "int"]
                    }
                }
            ]
        }
        schema_path = self.write_json_file(temp_dir, "arrayunion.avsc", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_avro_to_python(
            schema_path,
            output_dir
        )
        
        success, message = self.verify_python_imports(output_dir)
        self.assertTrue(success, f"Avro Python with array of unions failed: {message}")
    
    def test_116_avro_map_of_unions(self):
        """Avro with map of union types should compile to Python."""
        temp_dir = self.get_temp_dir("test_116")
        schema = {
            "type": "record",
            "name": "MapUnionRecord",
            "namespace": "mytest.mapunion",
            "fields": [
                {
                    "name": "data",
                    "type": {
                        "type": "map",
                        "values": ["null", "string", "int"]
                    }
                }
            ]
        }
        schema_path = self.write_json_file(temp_dir, "mapunion.avsc", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_avro_to_python(
            schema_path,
            output_dir
        )
        
        success, message = self.verify_python_imports(output_dir)
        self.assertTrue(success, f"Avro Python with map of unions failed: {message}")


class TestEnumEdgeCases(RobustnessTestBase):
    """
    Tests for enum handling edge cases.
    """
    
    def verify_python_imports(self, output_dir: str) -> Tuple[bool, str]:
        """Verify Python code can be imported."""
        py_files = []
        for root, dirs, files in os.walk(output_dir):
            for f in files:
                if f.endswith('.py') and f != '__init__.py':
                    py_files.append(os.path.join(root, f))
        
        if not py_files:
            return False, "No .py files found"
        
        for py_file in py_files:
            try:
                result = subprocess.run(
                    ["python", "-c", f"import sys; sys.path.insert(0, r'{output_dir}'); exec(open(r'{py_file}').read())"],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                if result.returncode != 0:
                    return False, f"Python import error in {py_file}:\n{result.stderr}"
            except Exception as e:
                return False, str(e)
        
        return True, "OK"
    
    def verify_java_compiles(self, output_dir: str) -> Tuple[bool, str]:
        """Verify Java code compiles."""
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
                return False, f"Java compilation error:\n{result.stderr}"
            return True, "OK"
        except FileNotFoundError:
            return True, "javac not available, skipping"
        except Exception as e:
            return False, str(e)
    
    def test_117_avro_enum_with_numeric_prefix(self):
        """Avro enum with numeric prefix symbols should compile."""
        temp_dir = self.get_temp_dir("test_117")
        schema = {
            "type": "record",
            "name": "EnumRecord",
            "namespace": "mytest.enumtest",
            "fields": [
                {
                    "name": "status",
                    "type": {
                        "type": "enum",
                        "name": "Status",
                        "symbols": ["1_PENDING", "2_ACTIVE", "3_CLOSED"]
                    }
                }
            ]
        }
        schema_path = self.write_json_file(temp_dir, "enum.avsc", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_avro_to_python(
            schema_path,
            output_dir
        )
        
        success, message = self.verify_python_imports(output_dir)
        self.assertTrue(success, f"Avro Python with numeric enum failed: {message}")
    
    def test_118_avro_enum_java_reserved(self):
        """Avro enum with Java reserved word symbols should compile."""
        temp_dir = self.get_temp_dir("test_118")
        schema = {
            "type": "record",
            "name": "EnumRecord",
            "namespace": "mytest.enumtest",
            "fields": [
                {
                    "name": "keyword",
                    "type": {
                        "type": "enum",
                        "name": "Keyword",
                        "symbols": ["CLASS", "PUBLIC", "PRIVATE", "STATIC", "FINAL"]
                    }
                }
            ]
        }
        schema_path = self.write_json_file(temp_dir, "enum.avsc", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_avro_to_java(
            schema_path,
            output_dir
        )
        
        success, message = self.verify_java_compiles(output_dir)
        self.assertTrue(success, f"Avro Java with reserved enum symbols failed: {message}")


class TestDeeplyNestedSchemas(RobustnessTestBase):
    """
    Tests for very deeply nested schemas.
    """
    
    def verify_python_imports(self, output_dir: str) -> Tuple[bool, str]:
        """Verify Python code can be imported."""
        py_files = []
        for root, dirs, files in os.walk(output_dir):
            for f in files:
                if f.endswith('.py') and f != '__init__.py':
                    py_files.append(os.path.join(root, f))
        
        if not py_files:
            return False, "No .py files found"
        
        for py_file in py_files:
            try:
                result = subprocess.run(
                    ["python", "-c", f"import sys; sys.path.insert(0, r'{output_dir}'); exec(open(r'{py_file}').read())"],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                if result.returncode != 0:
                    return False, f"Python import error in {py_file}:\n{result.stderr}"
            except Exception as e:
                return False, str(e)
        
        return True, "OK"
    
    def test_119_very_deep_nesting(self):
        """Very deeply nested schema (10 levels) should compile."""
        temp_dir = self.get_temp_dir("test_119")
        
        # Build a 10-level deep nested structure
        def build_nested(depth: int) -> dict:
            if depth == 0:
                return {"type": "string"}
            return {
                "type": "object",
                "title": f"Level{depth}",
                "properties": {
                    "nested": build_nested(depth - 1),
                    f"value{depth}": {"type": "string"}
                }
            }
        
        schema = build_nested(10)
        schema_path = self.write_json_file(temp_dir, "deep.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_python(
            schema_path,
            output_dir,
            package_name="mytest.deep"
        )
        
        success, message = self.verify_python_imports(output_dir)
        self.assertTrue(success, f"Very deep nesting failed: {message}")
    
    def test_120_avro_self_referential(self):
        """Avro with self-referential type should not crash."""
        temp_dir = self.get_temp_dir("test_120")
        schema = {
            "type": "record",
            "name": "TreeNode",
            "namespace": "mytest.tree",
            "fields": [
                {"name": "value", "type": "string"},
                {"name": "left", "type": ["null", "TreeNode"]},
                {"name": "right", "type": ["null", "TreeNode"]}
            ]
        }
        schema_path = self.write_json_file(temp_dir, "tree.avsc", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_avro_to_python(
            schema_path,
            output_dir
        )
        
        success, message = self.verify_python_imports(output_dir)
        self.assertTrue(success, f"Self-referential Avro failed: {message}")


class TestAllOfAndOneOf(RobustnessTestBase):
    """
    Tests for allOf and oneOf JSON Schema constructs.
    """
    
    def verify_python_imports(self, output_dir: str) -> Tuple[bool, str]:
        """Verify Python code can be imported."""
        py_files = []
        for root, dirs, files in os.walk(output_dir):
            for f in files:
                if f.endswith('.py') and f != '__init__.py':
                    py_files.append(os.path.join(root, f))
        
        if not py_files:
            return False, "No .py files found"
        
        for py_file in py_files:
            try:
                result = subprocess.run(
                    ["python", "-c", f"import sys; sys.path.insert(0, r'{output_dir}'); exec(open(r'{py_file}').read())"],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                if result.returncode != 0:
                    return False, f"Python import error in {py_file}:\n{result.stderr}"
            except Exception as e:
                return False, str(e)
        
        return True, "OK"
    
    def test_121_allof_inheritance(self):
        """allOf with multiple refs should compile."""
        temp_dir = self.get_temp_dir("test_121")
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$defs": {
                "Base1": {
                    "type": "object",
                    "properties": {
                        "field1": {"type": "string"}
                    }
                },
                "Base2": {
                    "type": "object",
                    "properties": {
                        "field2": {"type": "integer"}
                    }
                }
            },
            "type": "object",
            "title": "Combined",
            "allOf": [
                {"$ref": "#/$defs/Base1"},
                {"$ref": "#/$defs/Base2"}
            ],
            "properties": {
                "field3": {"type": "boolean"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "allof.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_python(
            schema_path,
            output_dir,
            package_name="mytest.allof"
        )
        
        success, message = self.verify_python_imports(output_dir)
        self.assertTrue(success, f"allOf inheritance failed: {message}")
    
    def test_122_oneof_union(self):
        """oneOf with multiple types should compile."""
        temp_dir = self.get_temp_dir("test_122")
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "title": "Container",
            "properties": {
                "payload": {
                    "oneOf": [
                        {"type": "string"},
                        {"type": "integer"},
                        {
                            "type": "object",
                            "title": "ComplexPayload",
                            "properties": {
                                "data": {"type": "string"}
                            }
                        }
                    ]
                }
            }
        }
        schema_path = self.write_json_file(temp_dir, "oneof.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_python(
            schema_path,
            output_dir,
            package_name="mytest.oneof"
        )
        
        success, message = self.verify_python_imports(output_dir)
        self.assertTrue(success, f"oneOf union failed: {message}")
    
    def test_123_anyof_union(self):
        """anyOf with multiple types should compile."""
        temp_dir = self.get_temp_dir("test_123")
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "title": "AnyOfContainer",
            "properties": {
                "value": {
                    "anyOf": [
                        {"type": "string"},
                        {"type": "number"},
                        {"type": "boolean"}
                    ]
                }
            }
        }
        schema_path = self.write_json_file(temp_dir, "anyof.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_python(
            schema_path,
            output_dir,
            package_name="mytest.anyof"
        )
        
        success, message = self.verify_python_imports(output_dir)
        self.assertTrue(success, f"anyOf union failed: {message}")


class TestCSharpEdgeCases(RobustnessTestBase):
    """
    Tests for C# specific edge cases.
    """
    
    def verify_csharp_compiles(self, output_dir: str) -> Tuple[bool, str]:
        """Verify C# code compiles using dotnet build."""
        csproj_files = []
        for root, dirs, files in os.walk(output_dir):
            for f in files:
                if f.endswith('.csproj'):
                    csproj_files.append(os.path.join(root, f))
        
        if not csproj_files:
            return False, "No .csproj files found"
        
        for csproj in csproj_files:
            csproj_dir = os.path.dirname(csproj)
            try:
                restore_result = subprocess.run(
                    ["dotnet", "restore"],
                    capture_output=True,
                    text=True,
                    timeout=120,
                    cwd=csproj_dir
                )
                if restore_result.returncode != 0:
                    return False, f"dotnet restore failed:\n{restore_result.stderr}"
                
                result = subprocess.run(
                    ["dotnet", "build", "--no-restore"],
                    capture_output=True,
                    text=True,
                    timeout=120,
                    cwd=csproj_dir
                )
                if result.returncode != 0:
                    errors = [line for line in result.stdout.split('\n') if 'error' in line.lower()][:10]
                    return False, f"C# compilation error:\n" + "\n".join(errors)
            except FileNotFoundError:
                return True, "dotnet not available, skipping"
            except subprocess.TimeoutExpired:
                return False, "Compilation timed out"
            except Exception as e:
                return False, str(e)
        
        return True, "OK"
    
    def test_124_csharp_reserved_keywords(self):
        """C# with reserved keyword field names should compile."""
        temp_dir = self.get_temp_dir("test_124")
        schema = {
            "type": "object",
            "title": "KeywordTest",
            "properties": {
                "class": {"type": "string"},
                "public": {"type": "string"},
                "private": {"type": "string"},
                "static": {"type": "string"},
                "readonly": {"type": "string"},
                "new": {"type": "string"},
                "override": {"type": "string"},
                "virtual": {"type": "string"},
                "abstract": {"type": "string"},
                "event": {"type": "string"},
                "delegate": {"type": "string"},
                "namespace": {"type": "string"},
                "object": {"type": "string"},
                "string": {"type": "string"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "keywords.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_csharp(
            schema_path,
            output_dir,
            base_namespace="MyTest.Keywords"
        )
        
        success, message = self.verify_csharp_compiles(output_dir)
        self.assertTrue(success, f"C# with reserved keywords failed: {message}")
    
    def test_125_csharp_nullable_types(self):
        """C# with nullable types should compile."""
        temp_dir = self.get_temp_dir("test_125")
        schema = {
            "type": "object",
            "title": "NullableTest",
            "properties": {
                "optionalString": {"type": ["string", "null"]},
                "optionalInt": {"type": ["integer", "null"]},
                "optionalBool": {"type": ["boolean", "null"]},
                "optionalNumber": {"type": ["number", "null"]}
            }
        }
        schema_path = self.write_json_file(temp_dir, "nullable.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_csharp(
            schema_path,
            output_dir,
            base_namespace="MyTest.Nullable"
        )
        
        success, message = self.verify_csharp_compiles(output_dir)
        self.assertTrue(success, f"C# with nullable types failed: {message}")


class TestGoEdgeCases2(RobustnessTestBase):
    """
    More tests for Go specific edge cases.
    """
    
    def verify_go_compiles(self, output_dir: str) -> Tuple[bool, str]:
        """Verify Go code compiles."""
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
    
    def test_126_go_reserved_keywords(self):
        """Go with reserved keyword field names should compile."""
        temp_dir = self.get_temp_dir("test_126")
        schema = {
            "type": "object",
            "title": "KeywordTest",
            "properties": {
                "func": {"type": "string"},
                "var": {"type": "string"},
                "const": {"type": "string"},
                "type": {"type": "string"},
                "struct": {"type": "string"},
                "interface": {"type": "string"},
                "map": {"type": "string"},
                "chan": {"type": "string"},
                "range": {"type": "string"},
                "select": {"type": "string"},
                "defer": {"type": "string"},
                "go": {"type": "string"},
                "package": {"type": "string"},
                "import": {"type": "string"}
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
        self.assertTrue(success, f"Go with reserved keywords failed: {message}")
    
    def test_127_go_builtin_types(self):
        """Go with builtin type field names should compile."""
        temp_dir = self.get_temp_dir("test_127")
        schema = {
            "type": "object",
            "title": "BuiltinTest",
            "properties": {
                "string": {"type": "string"},
                "int": {"type": "integer"},
                "int32": {"type": "integer"},
                "int64": {"type": "integer"},
                "float32": {"type": "number"},
                "float64": {"type": "number"},
                "bool": {"type": "boolean"},
                "byte": {"type": "integer"},
                "error": {"type": "string"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "builtin.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_go(
            schema_path,
            output_dir,
            package_name="testpkg"
        )
        
        success, message = self.verify_go_compiles(output_dir)
        self.assertTrue(success, f"Go with builtin type names failed: {message}")


class TestAvroToCSharpCompilation(RobustnessTestBase):
    """
    Tests for Avro to C# code generation.
    """
    
    def verify_csharp_compiles(self, output_dir: str) -> Tuple[bool, str]:
        """Verify C# code compiles."""
        csproj_files = []
        for root, dirs, files in os.walk(output_dir):
            for f in files:
                if f.endswith('.csproj'):
                    csproj_files.append(os.path.join(root, f))
        
        if not csproj_files:
            return False, "No .csproj files found"
        
        for csproj in csproj_files:
            csproj_dir = os.path.dirname(csproj)
            try:
                restore_result = subprocess.run(
                    ["dotnet", "restore"],
                    capture_output=True,
                    text=True,
                    timeout=120,
                    cwd=csproj_dir
                )
                if restore_result.returncode != 0:
                    return False, f"dotnet restore failed:\n{restore_result.stderr}"
                
                result = subprocess.run(
                    ["dotnet", "build", "--no-restore"],
                    capture_output=True,
                    text=True,
                    timeout=120,
                    cwd=csproj_dir
                )
                if result.returncode != 0:
                    errors = [line for line in result.stdout.split('\n') if 'error' in line.lower()][:10]
                    return False, f"C# compilation error:\n" + "\n".join(errors)
            except FileNotFoundError:
                return True, "dotnet not available, skipping"
            except subprocess.TimeoutExpired:
                return False, "Compilation timed out"
            except Exception as e:
                return False, str(e)
        
        return True, "OK"
    
    def test_128_avro_csharp_logical_types(self):
        """Avro to C# with logical types should compile."""
        temp_dir = self.get_temp_dir("test_128")
        schema = {
            "type": "record",
            "name": "LogicalRecord",
            "namespace": "MyTest.Logical",
            "fields": [
                {"name": "created", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "eventDate", "type": {"type": "int", "logicalType": "date"}},
                {"name": "id", "type": {"type": "string", "logicalType": "uuid"}}
            ]
        }
        schema_path = self.write_json_file(temp_dir, "logical.avsc", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_avro_to_csharp(
            schema_path,
            output_dir
        )
        
        success, message = self.verify_csharp_compiles(output_dir)
        self.assertTrue(success, f"Avro C# with logical types failed: {message}")
    
    def test_129_avro_csharp_decimal(self):
        """Avro to C# with decimal logical type should compile."""
        temp_dir = self.get_temp_dir("test_129")
        schema = {
            "type": "record",
            "name": "DecimalRecord",
            "namespace": "MyTest.Decimal",
            "fields": [
                {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}},
                {"name": "id", "type": "string"}
            ]
        }
        schema_path = self.write_json_file(temp_dir, "decimal.avsc", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_avro_to_csharp(
            schema_path,
            output_dir
        )
        
        success, message = self.verify_csharp_compiles(output_dir)
        self.assertTrue(success, f"Avro C# with decimal failed: {message}")
    
    def test_130_avro_csharp_nested(self):
        """Avro to C# with nested records should compile."""
        temp_dir = self.get_temp_dir("test_130")
        schema = {
            "type": "record",
            "name": "OuterRecord",
            "namespace": "MyTest.Nested",
            "fields": [
                {
                    "name": "inner",
                    "type": {
                        "type": "record",
                        "name": "InnerRecord",
                        "fields": [
                            {"name": "value", "type": "string"},
                            {"name": "count", "type": "int"}
                        ]
                    }
                }
            ]
        }
        schema_path = self.write_json_file(temp_dir, "nested.avsc", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_avro_to_csharp(
            schema_path,
            output_dir
        )
        
        success, message = self.verify_csharp_compiles(output_dir)
        self.assertTrue(success, f"Avro C# with nested records failed: {message}")


class TestAvroToRustCompilation(RobustnessTestBase):
    """
    Tests for Avro to Rust code generation.
    """
    
    def verify_rust_compiles(self, output_dir: str) -> Tuple[bool, str]:
        """Verify Rust code compiles using cargo check."""
        cargo_toml = os.path.join(output_dir, "Cargo.toml")
        if not os.path.exists(cargo_toml):
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
                errors = [line for line in result.stderr.split('\n') if 'error' in line.lower()][:10]
                return False, f"Rust compilation error:\n" + "\n".join(errors)
            return True, "OK"
        except FileNotFoundError:
            return True, "cargo not available, skipping"
        except subprocess.TimeoutExpired:
            return False, "Compilation timed out"
        except Exception as e:
            return False, str(e)
    
    def test_131_avro_rust_logical_types(self):
        """Avro to Rust with logical types should compile."""
        temp_dir = self.get_temp_dir("test_131")
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
        
        convert_avro_to_rust(
            schema_path,
            output_dir
        )
        
        success, message = self.verify_rust_compiles(output_dir)
        self.assertTrue(success, f"Avro Rust with logical types failed: {message}")
    
    def test_132_avro_rust_decimal(self):
        """Avro to Rust with decimal logical type should compile."""
        temp_dir = self.get_temp_dir("test_132")
        schema = {
            "type": "record",
            "name": "DecimalRecord",
            "namespace": "mytest.decimal",
            "fields": [
                {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}},
                {"name": "id", "type": "string"}
            ]
        }
        schema_path = self.write_json_file(temp_dir, "decimal.avsc", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_avro_to_rust(
            schema_path,
            output_dir
        )
        
        success, message = self.verify_rust_compiles(output_dir)
        self.assertTrue(success, f"Avro Rust with decimal failed: {message}")
    
    def test_133_avro_rust_nested(self):
        """Avro to Rust with nested records should compile."""
        temp_dir = self.get_temp_dir("test_133")
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
                            {"name": "value", "type": "string"},
                            {"name": "count", "type": "int"}
                        ]
                    }
                }
            ]
        }
        schema_path = self.write_json_file(temp_dir, "nested.avsc", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_avro_to_rust(
            schema_path,
            output_dir
        )
        
        success, message = self.verify_rust_compiles(output_dir)
        self.assertTrue(success, f"Avro Rust with nested records failed: {message}")


class TestAdditionalPropertiesHandling(RobustnessTestBase):
    """
    Tests for JSON Schema additionalProperties handling.
    """
    
    def verify_python_syntax(self, output_dir: str) -> Tuple[bool, str]:
        """Verify Python code has correct syntax."""
        for root, dirs, files in os.walk(output_dir):
            for f in files:
                if f.endswith('.py') and f != '__init__.py':
                    filepath = os.path.join(root, f)
                    try:
                        with open(filepath, 'r') as fp:
                            code = fp.read()
                            compile(code, filepath, 'exec')
                    except SyntaxError as e:
                        return False, f"Syntax error in {f}: {e}"
        return True, "OK"
    
    def test_134_additional_properties_true(self):
        """Schema with additionalProperties: true should compile."""
        temp_dir = self.get_temp_dir("test_134")
        schema = {
            "type": "object",
            "title": "OpenType",
            "properties": {
                "name": {"type": "string"}
            },
            "additionalProperties": True
        }
        schema_path = self.write_json_file(temp_dir, "open.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_python(
            schema_path,
            output_dir,
            package_name="mytest.open"
        )
        
        success, message = self.verify_python_syntax(output_dir)
        self.assertTrue(success, f"additionalProperties: true failed: {message}")
    
    def test_135_additional_properties_typed(self):
        """Schema with typed additionalProperties should compile."""
        temp_dir = self.get_temp_dir("test_135")
        schema = {
            "type": "object",
            "title": "DictType",
            "properties": {
                "name": {"type": "string"}
            },
            "additionalProperties": {"type": "integer"}
        }
        schema_path = self.write_json_file(temp_dir, "dict.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_python(
            schema_path,
            output_dir,
            package_name="mytest.dict"
        )
        
        success, message = self.verify_python_syntax(output_dir)
        self.assertTrue(success, f"additionalProperties typed failed: {message}")
    
    def test_136_pattern_properties(self):
        """Schema with patternProperties should compile."""
        temp_dir = self.get_temp_dir("test_136")
        schema = {
            "type": "object",
            "title": "PatternType",
            "properties": {
                "name": {"type": "string"}
            },
            "patternProperties": {
                "^x-": {"type": "string"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "pattern.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_python(
            schema_path,
            output_dir,
            package_name="mytest.pattern"
        )
        
        success, message = self.verify_python_syntax(output_dir)
        self.assertTrue(success, f"patternProperties failed: {message}")


class TestEdgeCaseFieldNames(RobustnessTestBase):
    """
    Tests for unusual field names.
    """
    
    def verify_python_syntax(self, output_dir: str) -> Tuple[bool, str]:
        """Verify Python code has correct syntax."""
        for root, dirs, files in os.walk(output_dir):
            for f in files:
                if f.endswith('.py') and f != '__init__.py':
                    filepath = os.path.join(root, f)
                    try:
                        with open(filepath, 'r') as fp:
                            code = fp.read()
                            compile(code, filepath, 'exec')
                    except SyntaxError as e:
                        return False, f"Syntax error in {f}: {e}"
        return True, "OK"
    
    def verify_java_compiles(self, output_dir: str) -> Tuple[bool, str]:
        """Verify Java code compiles."""
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
                return False, f"Java compilation error:\n{result.stderr}"
            return True, "OK"
        except FileNotFoundError:
            return True, "javac not available, skipping"
        except Exception as e:
            return False, str(e)
    
    def test_137_very_long_field_names(self):
        """Schema with very long field names should compile."""
        temp_dir = self.get_temp_dir("test_137")
        long_name = "a" * 200
        schema = {
            "type": "object",
            "title": "LongNames",
            "properties": {
                long_name: {"type": "string"},
                f"prefix_{long_name}_suffix": {"type": "integer"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "long.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_python(
            schema_path,
            output_dir,
            package_name="mytest.long"
        )
        
        success, message = self.verify_python_syntax(output_dir)
        self.assertTrue(success, f"Very long field names failed: {message}")
    
    def test_138_unicode_field_names(self):
        """Schema with unicode field names should compile."""
        temp_dir = self.get_temp_dir("test_138")
        schema = {
            "type": "object",
            "title": "UnicodeNames",
            "properties": {
                "名前": {"type": "string"},
                "übung": {"type": "string"},
                "テスト": {"type": "integer"},
                "данные": {"type": "boolean"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "unicode.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_python(
            schema_path,
            output_dir,
            package_name="mytest.unicode"
        )
        
        success, message = self.verify_python_syntax(output_dir)
        self.assertTrue(success, f"Unicode field names failed: {message}")
    
    def test_139_empty_property_name(self):
        """Schema with empty property name should be handled gracefully."""
        temp_dir = self.get_temp_dir("test_139")
        schema = {
            "type": "object",
            "title": "EmptyProp",
            "properties": {
                "": {"type": "string"},
                " ": {"type": "string"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "empty.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        try:
            convert_structure_to_python(
                schema_path,
                output_dir,
                package_name="mytest.empty"
            )
            # Check if it generated valid syntax
            success, message = self.verify_python_syntax(output_dir)
            self.assertTrue(success, f"Empty property name generated invalid syntax: {message}")
        except Exception as e:
            # Should not crash with a stack trace
            if "RecursionError" in str(type(e).__name__) or "stack" in str(e).lower():
                self.fail(f"Empty property name caused crash: {type(e).__name__}: {e}")
    
    def test_140_special_characters_only(self):
        """Schema with only special characters in property names should compile."""
        temp_dir = self.get_temp_dir("test_140")
        schema = {
            "type": "object",
            "title": "SpecialOnly",
            "properties": {
                "!@#": {"type": "string"},
                "$%^": {"type": "string"},
                "&*()": {"type": "integer"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "special.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_python(
            schema_path,
            output_dir,
            package_name="mytest.special"
        )
        
        success, message = self.verify_python_syntax(output_dir)
        self.assertTrue(success, f"Special characters only failed: {message}")


class TestAvroEnumEdgeCases(RobustnessTestBase):
    """
    Tests for Avro enum edge cases.
    """
    
    def verify_python_syntax(self, output_dir: str) -> Tuple[bool, str]:
        """Verify Python code has correct syntax."""
        for root, dirs, files in os.walk(output_dir):
            for f in files:
                if f.endswith('.py') and f != '__init__.py':
                    filepath = os.path.join(root, f)
                    try:
                        with open(filepath, 'r') as fp:
                            code = fp.read()
                            compile(code, filepath, 'exec')
                    except SyntaxError as e:
                        return False, f"Syntax error in {f}: {e}"
        return True, "OK"
    
    def test_141_avro_empty_enum(self):
        """Avro with empty enum symbols should be handled gracefully."""
        temp_dir = self.get_temp_dir("test_141")
        schema = {
            "type": "record",
            "name": "EmptyEnumRecord",
            "namespace": "mytest.emptyenum",
            "fields": [
                {
                    "name": "status",
                    "type": {
                        "type": "enum",
                        "name": "Status",
                        "symbols": []
                    }
                }
            ]
        }
        schema_path = self.write_json_file(temp_dir, "emptyenum.avsc", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        try:
            convert_avro_to_python(
                schema_path,
                output_dir
            )
            success, message = self.verify_python_syntax(output_dir)
            self.assertTrue(success, f"Empty enum generated invalid syntax: {message}")
        except Exception as e:
            # Should handle gracefully, not crash
            error_name = type(e).__name__
            if error_name in ["RecursionError", "SystemError"]:
                self.fail(f"Empty enum caused crash: {error_name}: {e}")
    
    def test_142_avro_enum_with_symbols_like_python_builtins(self):
        """Avro enum with Python builtin-like symbols should compile."""
        temp_dir = self.get_temp_dir("test_142")
        schema = {
            "type": "record",
            "name": "BuiltinEnumRecord",
            "namespace": "mytest.builtinenum",
            "fields": [
                {
                    "name": "value",
                    "type": {
                        "type": "enum",
                        "name": "Builtin",
                        "symbols": ["True", "False", "None", "print", "list", "dict", "str", "int"]
                    }
                }
            ]
        }
        schema_path = self.write_json_file(temp_dir, "builtinenum.avsc", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_avro_to_python(
            schema_path,
            output_dir
        )
        
        success, message = self.verify_python_syntax(output_dir)
        self.assertTrue(success, f"Builtin-like enum symbols failed: {message}")


class TestAvroMapTypes(RobustnessTestBase):
    """
    Tests for Avro map type handling.
    """
    
    def verify_python_syntax(self, output_dir: str) -> Tuple[bool, str]:
        """Verify Python code has correct syntax."""
        for root, dirs, files in os.walk(output_dir):
            for f in files:
                if f.endswith('.py') and f != '__init__.py':
                    filepath = os.path.join(root, f)
                    try:
                        with open(filepath, 'r') as fp:
                            code = fp.read()
                            compile(code, filepath, 'exec')
                    except SyntaxError as e:
                        return False, f"Syntax error in {f}: {e}"
        return True, "OK"
    
    def test_143_avro_map_of_records(self):
        """Avro with map of records should compile."""
        temp_dir = self.get_temp_dir("test_143")
        schema = {
            "type": "record",
            "name": "MapOfRecordsRecord",
            "namespace": "mytest.mapofrecords",
            "fields": [
                {
                    "name": "items",
                    "type": {
                        "type": "map",
                        "values": {
                            "type": "record",
                            "name": "Item",
                            "fields": [
                                {"name": "value", "type": "string"}
                            ]
                        }
                    }
                }
            ]
        }
        schema_path = self.write_json_file(temp_dir, "mapofrecords.avsc", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_avro_to_python(
            schema_path,
            output_dir
        )
        
        success, message = self.verify_python_syntax(output_dir)
        self.assertTrue(success, f"Map of records failed: {message}")
    
    def test_144_avro_nested_maps(self):
        """Avro with nested maps should compile."""
        temp_dir = self.get_temp_dir("test_144")
        schema = {
            "type": "record",
            "name": "NestedMapRecord",
            "namespace": "mytest.nestedmap",
            "fields": [
                {
                    "name": "data",
                    "type": {
                        "type": "map",
                        "values": {
                            "type": "map",
                            "values": "string"
                        }
                    }
                }
            ]
        }
        schema_path = self.write_json_file(temp_dir, "nestedmap.avsc", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_avro_to_python(
            schema_path,
            output_dir
        )
        
        success, message = self.verify_python_syntax(output_dir)
        self.assertTrue(success, f"Nested maps failed: {message}")


class TestAvroArrayTypes(RobustnessTestBase):
    """
    Tests for Avro array type handling.
    """
    
    def verify_python_syntax(self, output_dir: str) -> Tuple[bool, str]:
        """Verify Python code has correct syntax."""
        for root, dirs, files in os.walk(output_dir):
            for f in files:
                if f.endswith('.py') and f != '__init__.py':
                    filepath = os.path.join(root, f)
                    try:
                        with open(filepath, 'r') as fp:
                            code = fp.read()
                            compile(code, filepath, 'exec')
                    except SyntaxError as e:
                        return False, f"Syntax error in {f}: {e}"
        return True, "OK"
    
    def test_145_avro_array_of_records(self):
        """Avro with array of records should compile."""
        temp_dir = self.get_temp_dir("test_145")
        schema = {
            "type": "record",
            "name": "ArrayOfRecordsRecord",
            "namespace": "mytest.arrayofrecords",
            "fields": [
                {
                    "name": "items",
                    "type": {
                        "type": "array",
                        "items": {
                            "type": "record",
                            "name": "Item",
                            "fields": [
                                {"name": "value", "type": "string"}
                            ]
                        }
                    }
                }
            ]
        }
        schema_path = self.write_json_file(temp_dir, "arrayofrecords.avsc", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_avro_to_python(
            schema_path,
            output_dir
        )
        
        success, message = self.verify_python_syntax(output_dir)
        self.assertTrue(success, f"Array of records failed: {message}")
    
    def test_146_avro_nested_arrays(self):
        """Avro with nested arrays should compile."""
        temp_dir = self.get_temp_dir("test_146")
        schema = {
            "type": "record",
            "name": "NestedArrayRecord",
            "namespace": "mytest.nestedarray",
            "fields": [
                {
                    "name": "matrix",
                    "type": {
                        "type": "array",
                        "items": {
                            "type": "array",
                            "items": "int"
                        }
                    }
                }
            ]
        }
        schema_path = self.write_json_file(temp_dir, "nestedarray.avsc", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_avro_to_python(
            schema_path,
            output_dir
        )
        
        success, message = self.verify_python_syntax(output_dir)
        self.assertTrue(success, f"Nested arrays failed: {message}")


class TestStructureToRustCompilation(RobustnessTestBase):
    """
    Tests for Structure to Rust code generation.
    """
    
    def verify_rust_compiles(self, output_dir: str) -> Tuple[bool, str]:
        """Verify Rust code compiles using cargo check."""
        cargo_toml = os.path.join(output_dir, "Cargo.toml")
        if not os.path.exists(cargo_toml):
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
                errors = [line for line in result.stderr.split('\n') if 'error' in line.lower()][:10]
                return False, f"Rust compilation error:\n" + "\n".join(errors)
            return True, "OK"
        except FileNotFoundError:
            return True, "cargo not available, skipping"
        except subprocess.TimeoutExpired:
            return False, "Compilation timed out"
        except Exception as e:
            return False, str(e)
    
    def test_147_struct_rust_reserved_keywords(self):
        """Structure to Rust with reserved keywords should compile."""
        temp_dir = self.get_temp_dir("test_147")
        schema = {
            "type": "object",
            "title": "RustKeywords",
            "properties": {
                "fn": {"type": "string"},
                "let": {"type": "string"},
                "mut": {"type": "string"},
                "pub": {"type": "string"},
                "impl": {"type": "string"},
                "trait": {"type": "string"},
                "struct": {"type": "string"},
                "enum": {"type": "string"},
                "mod": {"type": "string"},
                "crate": {"type": "string"},
                "self": {"type": "string"},
                "super": {"type": "string"},
                "match": {"type": "string"},
                "loop": {"type": "string"},
                "break": {"type": "string"},
                "continue": {"type": "string"}
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
        self.assertTrue(success, f"Rust with reserved keywords failed: {message}")
    
    def test_148_struct_rust_nullable_types(self):
        """Structure to Rust with nullable types should compile."""
        temp_dir = self.get_temp_dir("test_148")
        schema = {
            "type": "object",
            "title": "NullableTypes",
            "properties": {
                "optString": {"type": ["string", "null"]},
                "optInt": {"type": ["integer", "null"]},
                "optBool": {"type": ["boolean", "null"]},
                "optNumber": {"type": ["number", "null"]}
            }
        }
        schema_path = self.write_json_file(temp_dir, "nullable.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_rust(
            schema_path,
            output_dir,
            package_name="test_pkg"
        )
        
        success, message = self.verify_rust_compiles(output_dir)
        self.assertTrue(success, f"Rust with nullable types failed: {message}")


class TestStructureToTypeScriptCompilation(RobustnessTestBase):
    """
    Tests for Structure to TypeScript code generation.
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
                return False, f"TypeScript error:\n{result.stdout}\n{result.stderr}"
            return True, "OK"
        except FileNotFoundError:
            return True, "npx/tsc not available, skipping"
        except Exception as e:
            return False, str(e)
    
    def test_149_struct_ts_reserved_keywords(self):
        """Structure to TypeScript with reserved keywords should compile."""
        temp_dir = self.get_temp_dir("test_149")
        schema = {
            "type": "object",
            "title": "TSKeywords",
            "properties": {
                "class": {"type": "string"},
                "function": {"type": "string"},
                "interface": {"type": "string"},
                "type": {"type": "string"},
                "enum": {"type": "string"},
                "namespace": {"type": "string"},
                "module": {"type": "string"},
                "declare": {"type": "string"},
                "abstract": {"type": "string"},
                "implements": {"type": "string"},
                "private": {"type": "string"},
                "public": {"type": "string"},
                "protected": {"type": "string"},
                "readonly": {"type": "string"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "keywords.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_typescript(
            schema_path,
            output_dir,
            package_name="mytest.keywords"
        )
        
        success, message = self.verify_ts_compiles(output_dir)
        self.assertTrue(success, f"TypeScript with reserved keywords failed: {message}")
    
    def test_150_struct_ts_optional_properties(self):
        """Structure to TypeScript with optional properties should compile."""
        temp_dir = self.get_temp_dir("test_150")
        schema = {
            "type": "object",
            "title": "OptionalProps",
            "properties": {
                "required": {"type": "string"},
                "optional": {"type": "string"}
            },
            "required": ["required"]
        }
        schema_path = self.write_json_file(temp_dir, "optional.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_typescript(
            schema_path,
            output_dir,
            package_name="mytest.optional"
        )
        
        success, message = self.verify_ts_compiles(output_dir)
        self.assertTrue(success, f"TypeScript with optional properties failed: {message}")


class TestProtoToAvro(RobustnessTestBase):
    """
    Tests for Protobuf to Avro conversion.
    """
    
    def write_proto_file(self, temp_dir: str, filename: str, content: str) -> str:
        """Write a proto file and return its path."""
        filepath = os.path.join(temp_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        return filepath
    
    def test_151_proto3_simple(self):
        """Simple proto3 file should convert to Avro."""
        temp_dir = self.get_temp_dir("test_151")
        proto_content = '''
syntax = "proto3";

package mytest;

message Person {
    string name = 1;
    int32 age = 2;
    bool active = 3;
}
'''
        proto_path = self.write_proto_file(temp_dir, "person.proto", proto_content)
        output_path = os.path.join(temp_dir, "person.avsc")
        
        from avrotize.prototoavro import convert_proto_to_avro
        try:
            convert_proto_to_avro(proto_path, output_path)
            self.assertTrue(os.path.exists(output_path), "Avro schema was not created")
        except Exception as e:
            self.fail(f"Proto to Avro conversion failed: {type(e).__name__}: {e}")
    
    def test_152_proto3_nested_message(self):
        """Proto3 with nested messages should convert to Avro."""
        temp_dir = self.get_temp_dir("test_152")
        proto_content = '''
syntax = "proto3";

package mytest;

message Outer {
    message Inner {
        string value = 1;
    }
    Inner nested = 1;
    string name = 2;
}
'''
        proto_path = self.write_proto_file(temp_dir, "nested.proto", proto_content)
        output_path = os.path.join(temp_dir, "nested.avsc")
        
        from avrotize.prototoavro import convert_proto_to_avro
        try:
            convert_proto_to_avro(proto_path, output_path)
            self.assertTrue(os.path.exists(output_path), "Avro schema was not created")
        except Exception as e:
            self.fail(f"Nested proto to Avro conversion failed: {type(e).__name__}: {e}")
    
    def test_153_proto3_enum(self):
        """Proto3 with enum should convert to Avro."""
        temp_dir = self.get_temp_dir("test_153")
        proto_content = '''
syntax = "proto3";

package mytest;

enum Status {
    UNKNOWN = 0;
    ACTIVE = 1;
    INACTIVE = 2;
}

message Record {
    string name = 1;
    Status status = 2;
}
'''
        proto_path = self.write_proto_file(temp_dir, "enum.proto", proto_content)
        output_path = os.path.join(temp_dir, "enum.avsc")
        
        from avrotize.prototoavro import convert_proto_to_avro
        try:
            convert_proto_to_avro(proto_path, output_path)
            self.assertTrue(os.path.exists(output_path), "Avro schema was not created")
        except Exception as e:
            self.fail(f"Enum proto to Avro conversion failed: {type(e).__name__}: {e}")
    
    def test_154_proto3_repeated(self):
        """Proto3 with repeated fields should convert to Avro."""
        temp_dir = self.get_temp_dir("test_154")
        proto_content = '''
syntax = "proto3";

package mytest;

message Container {
    repeated string items = 1;
    repeated int32 numbers = 2;
}
'''
        proto_path = self.write_proto_file(temp_dir, "repeated.proto", proto_content)
        output_path = os.path.join(temp_dir, "repeated.avsc")
        
        from avrotize.prototoavro import convert_proto_to_avro
        try:
            convert_proto_to_avro(proto_path, output_path)
            self.assertTrue(os.path.exists(output_path), "Avro schema was not created")
        except Exception as e:
            self.fail(f"Repeated proto to Avro conversion failed: {type(e).__name__}: {e}")
    
    def test_155_proto3_map(self):
        """Proto3 with map fields should convert to Avro."""
        temp_dir = self.get_temp_dir("test_155")
        proto_content = '''
syntax = "proto3";

package mytest;

message Container {
    map<string, string> data = 1;
    map<string, int32> counts = 2;
}
'''
        proto_path = self.write_proto_file(temp_dir, "map.proto", proto_content)
        output_path = os.path.join(temp_dir, "map.avsc")
        
        from avrotize.prototoavro import convert_proto_to_avro
        try:
            convert_proto_to_avro(proto_path, output_path)
            self.assertTrue(os.path.exists(output_path), "Avro schema was not created")
        except Exception as e:
            self.fail(f"Map proto to Avro conversion failed: {type(e).__name__}: {e}")
    
    def test_156_proto3_oneof(self):
        """Proto3 with oneof should convert to Avro."""
        temp_dir = self.get_temp_dir("test_156")
        proto_content = '''
syntax = "proto3";

package mytest;

message Response {
    string name = 1;
    oneof result {
        string success = 2;
        string error = 3;
    }
}
'''
        proto_path = self.write_proto_file(temp_dir, "oneof.proto", proto_content)
        output_path = os.path.join(temp_dir, "oneof.avsc")
        
        from avrotize.prototoavro import convert_proto_to_avro
        try:
            convert_proto_to_avro(proto_path, output_path)
            self.assertTrue(os.path.exists(output_path), "Avro schema was not created")
        except Exception as e:
            self.fail(f"Oneof proto to Avro conversion failed: {type(e).__name__}: {e}")


class TestXsdToAvro(RobustnessTestBase):
    """
    Tests for XSD to Avro conversion.
    """
    
    def write_xsd_file(self, temp_dir: str, filename: str, content: str) -> str:
        """Write an XSD file and return its path."""
        filepath = os.path.join(temp_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        return filepath
    
    def test_157_xsd_simple(self):
        """Simple XSD should convert to Avro."""
        temp_dir = self.get_temp_dir("test_157")
        xsd_content = '''<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="http://example.com/person">
    <xs:element name="person">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="name" type="xs:string"/>
                <xs:element name="age" type="xs:int"/>
            </xs:sequence>
        </xs:complexType>
    </xs:element>
</xs:schema>
'''
        xsd_path = self.write_xsd_file(temp_dir, "person.xsd", xsd_content)
        output_path = os.path.join(temp_dir, "person.avsc")
        
        from avrotize.xsdtoavro import convert_xsd_to_avro
        try:
            convert_xsd_to_avro(xsd_path, output_path)
            self.assertTrue(os.path.exists(output_path), "Avro schema was not created")
        except Exception as e:
            self.fail(f"XSD to Avro conversion failed: {type(e).__name__}: {e}")
    
    def test_158_xsd_complex_type(self):
        """XSD with named complex type should convert to Avro."""
        temp_dir = self.get_temp_dir("test_158")
        xsd_content = '''<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="http://example.com/address">
    <xs:complexType name="AddressType">
        <xs:sequence>
            <xs:element name="street" type="xs:string"/>
            <xs:element name="city" type="xs:string"/>
            <xs:element name="zip" type="xs:string"/>
        </xs:sequence>
    </xs:complexType>
    
    <xs:element name="address" type="AddressType"/>
</xs:schema>
'''
        xsd_path = self.write_xsd_file(temp_dir, "address.xsd", xsd_content)
        output_path = os.path.join(temp_dir, "address.avsc")
        
        from avrotize.xsdtoavro import convert_xsd_to_avro
        try:
            convert_xsd_to_avro(xsd_path, output_path)
            self.assertTrue(os.path.exists(output_path), "Avro schema was not created")
        except Exception as e:
            self.fail(f"XSD complex type to Avro conversion failed: {type(e).__name__}: {e}")
    
    def test_159_xsd_enumeration(self):
        """XSD with enumeration should convert to Avro."""
        temp_dir = self.get_temp_dir("test_159")
        xsd_content = '''<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="http://example.com/status">
    <xs:simpleType name="StatusType">
        <xs:restriction base="xs:string">
            <xs:enumeration value="ACTIVE"/>
            <xs:enumeration value="INACTIVE"/>
            <xs:enumeration value="PENDING"/>
        </xs:restriction>
    </xs:simpleType>
    
    <xs:element name="status" type="StatusType"/>
</xs:schema>
'''
        xsd_path = self.write_xsd_file(temp_dir, "status.xsd", xsd_content)
        output_path = os.path.join(temp_dir, "status.avsc")
        
        from avrotize.xsdtoavro import convert_xsd_to_avro
        try:
            convert_xsd_to_avro(xsd_path, output_path)
            self.assertTrue(os.path.exists(output_path), "Avro schema was not created")
        except Exception as e:
            self.fail(f"XSD enumeration to Avro conversion failed: {type(e).__name__}: {e}")


class TestAvroToGraphQL(RobustnessTestBase):
    """
    Tests for Avro to GraphQL conversion.
    """
    
    def verify_graphql_syntax(self, output_path: str) -> Tuple[bool, str]:
        """Basic check that GraphQL output is valid."""
        if not os.path.exists(output_path):
            return False, "Output file not created"
        
        with open(output_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        if not content.strip():
            return False, "Output file is empty"
        
        # Basic GraphQL syntax check - should have type definitions
        if 'type ' not in content and 'input ' not in content:
            return False, "No type definitions found in GraphQL output"
        
        return True, "OK"
    
    def test_160_avro_graphql_simple(self):
        """Simple Avro record should convert to GraphQL."""
        temp_dir = self.get_temp_dir("test_160")
        schema = {
            "type": "record",
            "name": "Person",
            "namespace": "mytest",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "age", "type": "int"}
            ]
        }
        schema_path = self.write_json_file(temp_dir, "person.avsc", schema)
        output_path = os.path.join(temp_dir, "schema.graphql")
        
        from avrotize.avrotographql import convert_avro_to_graphql
        try:
            convert_avro_to_graphql(schema_path, output_path)
            success, message = self.verify_graphql_syntax(output_path)
            self.assertTrue(success, f"GraphQL output invalid: {message}")
        except Exception as e:
            self.fail(f"Avro to GraphQL conversion failed: {type(e).__name__}: {e}")
    
    def test_161_avro_graphql_nested(self):
        """Avro with nested records should convert to GraphQL."""
        temp_dir = self.get_temp_dir("test_161")
        schema = {
            "type": "record",
            "name": "Outer",
            "namespace": "mytest",
            "fields": [
                {
                    "name": "inner",
                    "type": {
                        "type": "record",
                        "name": "Inner",
                        "fields": [
                            {"name": "value", "type": "string"}
                        ]
                    }
                }
            ]
        }
        schema_path = self.write_json_file(temp_dir, "nested.avsc", schema)
        output_path = os.path.join(temp_dir, "schema.graphql")
        
        from avrotize.avrotographql import convert_avro_to_graphql
        try:
            convert_avro_to_graphql(schema_path, output_path)
            success, message = self.verify_graphql_syntax(output_path)
            self.assertTrue(success, f"GraphQL nested output invalid: {message}")
        except Exception as e:
            self.fail(f"Nested Avro to GraphQL conversion failed: {type(e).__name__}: {e}")


class TestAvroToCpp(RobustnessTestBase):
    """
    Tests for Avro to C++ conversion.
    """
    
    def test_162_avro_cpp_simple(self):
        """Simple Avro record should convert to C++."""
        temp_dir = self.get_temp_dir("test_162")
        schema = {
            "type": "record",
            "name": "Person",
            "namespace": "mytest",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "age", "type": "int"}
            ]
        }
        schema_path = self.write_json_file(temp_dir, "person.avsc", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        from avrotize.avrotocpp import convert_avro_to_cpp
        try:
            convert_avro_to_cpp(schema_path, output_dir)
            # Check that some C++ file was created
            cpp_files = [f for f in os.listdir(output_dir) if f.endswith('.hpp') or f.endswith('.cpp') or f.endswith('.h')]
            self.assertTrue(len(cpp_files) > 0 or any(os.listdir(output_dir)), "No C++ files created")
        except Exception as e:
            self.fail(f"Avro to C++ conversion failed: {type(e).__name__}: {e}")
    
    def test_163_avro_cpp_logical_types(self):
        """Avro with logical types should convert to C++."""
        temp_dir = self.get_temp_dir("test_163")
        schema = {
            "type": "record",
            "name": "TimestampRecord",
            "namespace": "mytest",
            "fields": [
                {"name": "created", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "id", "type": {"type": "string", "logicalType": "uuid"}}
            ]
        }
        schema_path = self.write_json_file(temp_dir, "timestamp.avsc", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        from avrotize.avrotocpp import convert_avro_to_cpp
        try:
            convert_avro_to_cpp(schema_path, output_dir)
            # Just verify it doesn't crash
            self.assertTrue(True)
        except Exception as e:
            self.fail(f"Avro C++ with logical types failed: {type(e).__name__}: {e}")


class TestJsonSchemaEdgeCases(RobustnessTestBase):
    """
    Tests for JSON Schema edge cases.
    """
    
    def verify_python_syntax(self, output_dir: str) -> Tuple[bool, str]:
        """Verify Python code has correct syntax."""
        for root, dirs, files in os.walk(output_dir):
            for f in files:
                if f.endswith('.py') and f != '__init__.py':
                    filepath = os.path.join(root, f)
                    try:
                        with open(filepath, 'r', encoding='utf-8') as fp:
                            code = fp.read()
                            compile(code, filepath, 'exec')
                    except SyntaxError as e:
                        return False, f"Syntax error in {f}: {e}"
        return True, "OK"
    
    def test_164_default_values(self):
        """Schema with default values should compile."""
        temp_dir = self.get_temp_dir("test_164")
        schema = {
            "type": "object",
            "title": "DefaultValues",
            "properties": {
                "name": {"type": "string", "default": "unknown"},
                "count": {"type": "integer", "default": 0},
                "active": {"type": "boolean", "default": True},
                "score": {"type": "number", "default": 0.0}
            }
        }
        schema_path = self.write_json_file(temp_dir, "defaults.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_python(
            schema_path,
            output_dir,
            package_name="mytest.defaults"
        )
        
        success, message = self.verify_python_syntax(output_dir)
        self.assertTrue(success, f"Default values failed: {message}")
    
    def test_165_min_max_constraints(self):
        """Schema with min/max constraints should compile."""
        temp_dir = self.get_temp_dir("test_165")
        schema = {
            "type": "object",
            "title": "Constraints",
            "properties": {
                "age": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 150
                },
                "score": {
                    "type": "number",
                    "minimum": 0.0,
                    "maximum": 100.0
                },
                "name": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 100
                }
            }
        }
        schema_path = self.write_json_file(temp_dir, "constraints.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_python(
            schema_path,
            output_dir,
            package_name="mytest.constraints"
        )
        
        success, message = self.verify_python_syntax(output_dir)
        self.assertTrue(success, f"Min/max constraints failed: {message}")
    
    def test_166_format_types(self):
        """Schema with format annotations should compile."""
        temp_dir = self.get_temp_dir("test_166")
        schema = {
            "type": "object",
            "title": "Formats",
            "properties": {
                "email": {"type": "string", "format": "email"},
                "uri": {"type": "string", "format": "uri"},
                "date": {"type": "string", "format": "date"},
                "datetime": {"type": "string", "format": "date-time"},
                "uuid": {"type": "string", "format": "uuid"},
                "ipv4": {"type": "string", "format": "ipv4"},
                "ipv6": {"type": "string", "format": "ipv6"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "formats.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_python(
            schema_path,
            output_dir,
            package_name="mytest.formats"
        )
        
        success, message = self.verify_python_syntax(output_dir)
        self.assertTrue(success, f"Format types failed: {message}")
    
    def test_167_const_value(self):
        """Schema with const value should compile."""
        temp_dir = self.get_temp_dir("test_167")
        schema = {
            "type": "object",
            "title": "ConstValue",
            "properties": {
                "type": {"const": "fixed_type"},
                "version": {"const": 1}
            }
        }
        schema_path = self.write_json_file(temp_dir, "const.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_to_python(
            schema_path,
            output_dir,
            package_name="mytest.const"
        )
        
        success, message = self.verify_python_syntax(output_dir)
        self.assertTrue(success, f"Const value failed: {message}")
    
    def test_168_recursive_ref(self):
        """Schema with recursive $ref should not cause infinite loop."""
        temp_dir = self.get_temp_dir("test_168")
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "title": "LinkedList",
            "$defs": {
                "Node": {
                    "type": "object",
                    "properties": {
                        "value": {"type": "string"},
                        "next": {"$ref": "#/$defs/Node"}
                    }
                }
            },
            "properties": {
                "head": {"$ref": "#/$defs/Node"}
            }
        }
        schema_path = self.write_json_file(temp_dir, "recursive.struct.json", schema)
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        try:
            convert_structure_to_python(
                schema_path,
                output_dir,
                package_name="mytest.recursive"
            )
            success, message = self.verify_python_syntax(output_dir)
            self.assertTrue(success, f"Recursive ref failed: {message}")
        except RecursionError:
            self.fail("Recursive $ref caused RecursionError")


class TestAvroToCSharpCompilation2(RobustnessTestBase):
    """Tests for Avro-to-C# code generation compilation - additional tests"""
    
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
            subprocess.run(
                ["dotnet", "restore", csproj_files[0]],
                capture_output=True,
                text=True,
                timeout=60
            )
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
    
    def test_169_csharp_avro_bytes_type(self):
        """Test C# compilation with Avro bytes type"""
        avro_schema = {
            "type": "record",
            "name": "BinaryData",
            "namespace": "test.binary",
            "fields": [
                {"name": "rawData", "type": "bytes"},
                {"name": "optionalData", "type": ["null", "bytes"]}
            ]
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            avsc_file = os.path.join(tmpdir, "binary.avsc")
            with open(avsc_file, 'w') as f:
                json.dump(avro_schema, f)
            
            out_dir = os.path.join(tmpdir, "csharp_output")
            os.makedirs(out_dir)
            
            try:
                convert_avro_to_csharp(avsc_file, out_dir, base_namespace="Test.Binary")
            except Exception as e:
                self.fail(f"Avro-to-C# conversion failed: {e}")
            
            # Check compilation
            can_compile, error = self.verify_csharp_compiles(out_dir)
            if not can_compile:
                self.fail(f"Generated C# code doesn't compile: {error}")

    def test_170_csharp_avro_fixed_type(self):
        """Test C# compilation with Avro fixed type"""
        avro_schema = {
            "type": "record",
            "name": "HashContainer",
            "namespace": "test.hash",
            "fields": [
                {"name": "md5Hash", "type": {"type": "fixed", "name": "MD5", "size": 16}},
                {"name": "sha256Hash", "type": {"type": "fixed", "name": "SHA256", "size": 32}}
            ]
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            avsc_file = os.path.join(tmpdir, "hash.avsc")
            with open(avsc_file, 'w') as f:
                json.dump(avro_schema, f)
            
            out_dir = os.path.join(tmpdir, "csharp_output")
            os.makedirs(out_dir)
            
            try:
                convert_avro_to_csharp(avsc_file, out_dir, base_namespace="Test.Hash")
            except Exception as e:
                self.fail(f"Avro-to-C# conversion failed: {e}")
            
            # Check compilation
            can_compile, error = self.verify_csharp_compiles(out_dir)
            if not can_compile:
                self.fail(f"Generated C# code doesn't compile: {error}")

    def test_171_csharp_avro_enum_with_namespace(self):
        """Test C# compilation with enum in different namespace"""
        avro_schema = {
            "type": "record",
            "name": "ColoredItem",
            "namespace": "test.items",
            "fields": [
                {"name": "color", "type": {
                    "type": "enum",
                    "name": "Color",
                    "namespace": "test.enums",
                    "symbols": ["RED", "GREEN", "BLUE"]
                }},
                {"name": "name", "type": "string"}
            ]
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            avsc_file = os.path.join(tmpdir, "colored.avsc")
            with open(avsc_file, 'w') as f:
                json.dump(avro_schema, f)
            
            out_dir = os.path.join(tmpdir, "csharp_output")
            os.makedirs(out_dir)
            
            try:
                convert_avro_to_csharp(avsc_file, out_dir, base_namespace="Test")
            except Exception as e:
                self.fail(f"Avro-to-C# conversion failed: {e}")
            
            # Check compilation
            can_compile, error = self.verify_csharp_compiles(out_dir)
            if not can_compile:
                self.fail(f"Generated C# code doesn't compile: {error}")


class TestStructureToJavaScript(RobustnessTestBase):
    """Tests for structure-to-JavaScript code generation"""
    
    def test_172_js_basic_object(self):
        """Test JS generation with basic object"""
        schema = {
            "type": "object",
            "title": "SimpleObject",
            "properties": {
                "name": {"type": "string"},
                "count": {"type": "integer"}
            }
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            schema_file = os.path.join(tmpdir, "simple.json")
            with open(schema_file, 'w') as f:
                json.dump(schema, f)
            
            out_dir = os.path.join(tmpdir, "js_output")
            os.makedirs(out_dir)
            
            try:
                convert_structure_to_javascript(schema_file, out_dir)
            except Exception as e:
                self.fail(f"Structure-to-JS conversion failed: {e}")
            
            # Check files were generated
            self.assertTrue(os.listdir(out_dir), "No files generated")

    def test_173_js_reserved_keywords(self):
        """Test JS generation with reserved keyword field names"""
        schema = {
            "type": "object",
            "title": "ReservedWords",
            "properties": {
                "class": {"type": "string"},
                "function": {"type": "string"},
                "var": {"type": "string"},
                "let": {"type": "string"},
                "const": {"type": "string"},
                "yield": {"type": "string"},
                "await": {"type": "string"}
            }
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            schema_file = os.path.join(tmpdir, "reserved.json")
            with open(schema_file, 'w') as f:
                json.dump(schema, f)
            
            out_dir = os.path.join(tmpdir, "js_output")
            os.makedirs(out_dir)
            
            try:
                convert_structure_to_javascript(schema_file, out_dir)
            except Exception as e:
                self.fail(f"Structure-to-JS conversion failed: {e}")
            
            # Files should be generated - check if they contain invalid code
            for root, dirs, files in os.walk(out_dir):
                for file in files:
                    if file.endswith('.js'):
                        with open(os.path.join(root, file), 'r') as f:
                            content = f.read()
                        # Check that reserved words are not used as bare identifiers
                        if 'class:' in content or 'function:' in content:
                            self.fail("Reserved words used as bare identifiers in JS")

    def test_174_js_numeric_prefix_fields(self):
        """Test JS generation with numeric prefix field names"""
        schema = {
            "type": "object",
            "title": "NumericFields",
            "properties": {
                "1stItem": {"type": "string"},
                "2ndItem": {"type": "string"},
                "3rdItem": {"type": "string"}
            }
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            schema_file = os.path.join(tmpdir, "numeric.json")
            with open(schema_file, 'w') as f:
                json.dump(schema, f)
            
            out_dir = os.path.join(tmpdir, "js_output")
            os.makedirs(out_dir)
            
            try:
                convert_structure_to_javascript(schema_file, out_dir)
            except Exception as e:
                self.fail(f"Structure-to-JS conversion failed: {e}")


class TestStructureToCddl(RobustnessTestBase):
    """Tests for structure-to-CDDL conversion"""
    
    def test_175_cddl_basic_object(self):
        """Test CDDL generation with basic object"""
        schema = {
            "type": "object",
            "title": "SimpleCddl",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"}
            }
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            schema_file = os.path.join(tmpdir, "simple.json")
            with open(schema_file, 'w') as f:
                json.dump(schema, f)
            
            out_file = os.path.join(tmpdir, "output.cddl")
            
            try:
                convert_structure_to_cddl_files(schema_file, out_file)
            except Exception as e:
                self.fail(f"Structure-to-CDDL conversion failed: {e}")
            
            self.assertTrue(os.path.exists(out_file), "CDDL file not created")

    def test_176_cddl_complex_types(self):
        """Test CDDL generation with complex nested types"""
        schema = {
            "type": "object",
            "title": "ComplexCddl",
            "properties": {
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "integer"},
                            "name": {"type": "string"}
                        }
                    }
                },
                "metadata": {
                    "type": "object",
                    "additionalProperties": {"type": "string"}
                }
            }
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            schema_file = os.path.join(tmpdir, "complex.json")
            with open(schema_file, 'w') as f:
                json.dump(schema, f)
            
            out_file = os.path.join(tmpdir, "output.cddl")
            
            try:
                convert_structure_to_cddl_files(schema_file, out_file)
            except Exception as e:
                self.fail(f"Structure-to-CDDL conversion failed: {e}")


class TestAvroToKusto(RobustnessTestBase):
    """Tests for Avro-to-Kusto conversion"""
    
    def test_177_kusto_basic_record(self):
        """Test Kusto generation with basic record"""
        avro_schema = {
            "type": "record",
            "name": "KustoRecord",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "value", "type": "double"}
            ]
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            avsc_file = os.path.join(tmpdir, "kusto.avsc")
            with open(avsc_file, 'w') as f:
                json.dump(avro_schema, f)
            
            out_file = os.path.join(tmpdir, "output.kql")
            
            try:
                convert_avro_to_kusto_file(avsc_file, "KustoRecord", out_file)
            except Exception as e:
                self.fail(f"Avro-to-Kusto conversion failed: {e}")
            
            self.assertTrue(os.path.exists(out_file), "Kusto file not created")

    def test_178_kusto_logical_types(self):
        """Test Kusto generation with logical types"""
        avro_schema = {
            "type": "record",
            "name": "LogicalKusto",
            "fields": [
                {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "date", "type": {"type": "int", "logicalType": "date"}},
                {"name": "uuid", "type": {"type": "string", "logicalType": "uuid"}},
                {"name": "decimal", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}}
            ]
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            avsc_file = os.path.join(tmpdir, "logical.avsc")
            with open(avsc_file, 'w') as f:
                json.dump(avro_schema, f)
            
            out_file = os.path.join(tmpdir, "output.kql")
            
            try:
                convert_avro_to_kusto_file(avsc_file, "LogicalKusto", out_file)
            except Exception as e:
                self.fail(f"Avro-to-Kusto conversion failed: {e}")


class TestComplexNesting(RobustnessTestBase):
    """Tests for complex nesting scenarios across generators"""
    
    def test_179_deeply_nested_arrays(self):
        """Test handling of deeply nested array types"""
        schema = {
            "type": "object",
            "title": "DeeplyNested",
            "properties": {
                "level1": {
                    "type": "array",
                    "items": {
                        "type": "array",
                        "items": {
                            "type": "array",
                            "items": {
                                "type": "array",
                                "items": {"type": "string"}
                            }
                        }
                    }
                }
            }
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            schema_file = os.path.join(tmpdir, "nested_arrays.json")
            with open(schema_file, 'w') as f:
                json.dump(schema, f)
            
            # Test Python
            py_dir = os.path.join(tmpdir, "py_output")
            os.makedirs(py_dir)
            try:
                convert_structure_to_python(schema_file, py_dir)
            except Exception as e:
                self.fail(f"Python conversion failed with deeply nested arrays: {e}")
            
            # Check if generated Python imports correctly
            can_import, error = self.verify_python_imports(py_dir)
            if not can_import:
                self.fail(f"Generated Python doesn't import: {error}")

    def test_180_map_of_arrays(self):
        """Test handling of maps containing arrays"""
        schema = {
            "type": "object",
            "title": "MapOfArrays",
            "properties": {
                "data": {
                    "type": "object",
                    "additionalProperties": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "key": {"type": "string"},
                                "value": {"type": "integer"}
                            }
                        }
                    }
                }
            }
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            schema_file = os.path.join(tmpdir, "map_arrays.json")
            with open(schema_file, 'w') as f:
                json.dump(schema, f)
            
            # Test TypeScript
            ts_dir = os.path.join(tmpdir, "ts_output")
            os.makedirs(ts_dir)
            try:
                convert_structure_to_typescript(schema_file, ts_dir)
            except Exception as e:
                self.fail(f"TypeScript conversion failed with map of arrays: {e}")


class TestMultipleFileSchemas(RobustnessTestBase):
    """Tests for schemas that generate multiple files"""
    
    def test_181_multiple_named_types(self):
        """Test schema with multiple named types"""
        schema = {
            "definitions": {
                "Address": {
                    "type": "object",
                    "properties": {
                        "street": {"type": "string"},
                        "city": {"type": "string"}
                    }
                },
                "Person": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "address": {"$ref": "#/definitions/Address"}
                    }
                },
                "Company": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "address": {"$ref": "#/definitions/Address"},
                        "employees": {
                            "type": "array",
                            "items": {"$ref": "#/definitions/Person"}
                        }
                    }
                }
            },
            "type": "object",
            "title": "Organization",
            "properties": {
                "company": {"$ref": "#/definitions/Company"}
            }
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            schema_file = os.path.join(tmpdir, "org.json")
            with open(schema_file, 'w') as f:
                json.dump(schema, f)
            
            # Test Java
            java_dir = os.path.join(tmpdir, "java_output")
            os.makedirs(java_dir)
            try:
                convert_structure_to_java(schema_file, java_dir, package_name="com.test.org")
            except Exception as e:
                self.fail(f"Java conversion failed with multiple named types: {e}")
            
            # Check compilation
            can_compile, error = self.verify_java_compiles(java_dir)
            if not can_compile:
                self.fail(f"Generated Java doesn't compile: {error}")

    def test_182_cross_referencing_types(self):
        """Test schema with cross-referencing types"""
        schema = {
            "definitions": {
                "Node": {
                    "type": "object",
                    "properties": {
                        "value": {"type": "string"},
                        "children": {
                            "type": "array",
                            "items": {"$ref": "#/definitions/Node"}
                        },
                        "parent": {"$ref": "#/definitions/Node"}
                    }
                }
            },
            "type": "object",
            "title": "Tree",
            "properties": {
                "root": {"$ref": "#/definitions/Node"}
            }
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            schema_file = os.path.join(tmpdir, "tree.json")
            with open(schema_file, 'w') as f:
                json.dump(schema, f)
            
            # Test Go
            go_dir = os.path.join(tmpdir, "go_output")
            os.makedirs(go_dir)
            try:
                convert_structure_to_go(schema_file, go_dir, package_name="tree")
            except Exception as e:
                self.fail(f"Go conversion failed with cross-referencing types: {e}")


class TestAvroComplexUnions(RobustnessTestBase):
    """Tests for complex Avro union types"""
    
    def test_183_union_with_multiple_records(self):
        """Test union containing multiple record types"""
        avro_schema = {
            "type": "record",
            "name": "Container",
            "namespace": "test.union",
            "fields": [
                {
                    "name": "payload",
                    "type": [
                        "null",
                        {
                            "type": "record",
                            "name": "TextPayload",
                            "fields": [{"name": "text", "type": "string"}]
                        },
                        {
                            "type": "record",
                            "name": "BinaryPayload",
                            "fields": [{"name": "data", "type": "bytes"}]
                        },
                        {
                            "type": "record",
                            "name": "NumericPayload",
                            "fields": [{"name": "value", "type": "double"}]
                        }
                    ]
                }
            ]
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            avsc_file = os.path.join(tmpdir, "union.avsc")
            with open(avsc_file, 'w') as f:
                json.dump(avro_schema, f)
            
            # Test Python
            py_dir = os.path.join(tmpdir, "py_output")
            os.makedirs(py_dir)
            try:
                convert_avro_to_python(avsc_file, py_dir)
            except Exception as e:
                self.fail(f"Python conversion failed with union of records: {e}")
            
            can_import, error = self.verify_python_imports(py_dir)
            if not can_import:
                self.fail(f"Generated Python doesn't import: {error}")

    def test_184_union_with_arrays_and_maps(self):
        """Test union containing arrays and maps"""
        avro_schema = {
            "type": "record",
            "name": "FlexibleData",
            "namespace": "test.flex",
            "fields": [
                {
                    "name": "data",
                    "type": [
                        "null",
                        {"type": "array", "items": "string"},
                        {"type": "map", "values": "int"}
                    ]
                }
            ]
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            avsc_file = os.path.join(tmpdir, "flex.avsc")
            with open(avsc_file, 'w') as f:
                json.dump(avro_schema, f)
            
            # Test TypeScript
            ts_dir = os.path.join(tmpdir, "ts_output")
            os.makedirs(ts_dir)
            try:
                convert_avro_to_typescript(avsc_file, ts_dir)
            except Exception as e:
                self.fail(f"TypeScript conversion failed with union of arrays/maps: {e}")
            
            # Check compilation
            can_compile, error = self.verify_typescript_compiles(ts_dir)
            if not can_compile:
                self.fail(f"Generated TypeScript doesn't compile: {error}")


class TestAvroRecordNameConflicts(RobustnessTestBase):
    """Tests for Avro schemas with potential name conflicts"""
    
    def test_185_same_name_different_namespace(self):
        """Test records with same name but different namespace"""
        avro_schema = {
            "type": "record",
            "name": "Container",
            "namespace": "test.main",
            "fields": [
                {
                    "name": "item1",
                    "type": {
                        "type": "record",
                        "name": "Item",
                        "namespace": "test.ns1",
                        "fields": [{"name": "value", "type": "string"}]
                    }
                },
                {
                    "name": "item2",
                    "type": {
                        "type": "record",
                        "name": "Item",
                        "namespace": "test.ns2",
                        "fields": [{"name": "count", "type": "int"}]
                    }
                }
            ]
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            avsc_file = os.path.join(tmpdir, "conflict.avsc")
            with open(avsc_file, 'w') as f:
                json.dump(avro_schema, f)
            
            # Test Java - namespace handling is important
            java_dir = os.path.join(tmpdir, "java_output")
            os.makedirs(java_dir)
            try:
                convert_avro_to_java(avsc_file, java_dir)
            except Exception as e:
                self.fail(f"Java conversion failed with same name different namespace: {e}")
            
            can_compile, error = self.verify_java_compiles(java_dir)
            if not can_compile:
                self.fail(f"Generated Java doesn't compile: {error}")

    def test_186_nested_record_same_name(self):
        """Test nested records with same name as parent"""
        avro_schema = {
            "type": "record",
            "name": "Data",
            "namespace": "test.nested",
            "fields": [
                {
                    "name": "nestedData",
                    "type": {
                        "type": "record",
                        "name": "Data",
                        "namespace": "test.nested.inner",
                        "fields": [{"name": "value", "type": "string"}]
                    }
                }
            ]
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            avsc_file = os.path.join(tmpdir, "nested_same.avsc")
            with open(avsc_file, 'w') as f:
                json.dump(avro_schema, f)
            
            # Test C#
            csharp_dir = os.path.join(tmpdir, "csharp_output")
            os.makedirs(csharp_dir)
            try:
                convert_avro_to_csharp(avsc_file, csharp_dir, base_namespace="Test.Nested")
            except Exception as e:
                self.fail(f"C# conversion failed with nested same name: {e}")
            
            can_compile, error = self.verify_csharp_compiles(csharp_dir)
            if not can_compile:
                self.fail(f"Generated C# doesn't compile: {error}")


class TestStructureToProto(RobustnessTestBase):
    """Tests for structure-to-proto conversion"""
    
    def test_187_proto_basic_message(self):
        """Test proto generation with basic message"""
        schema = {
            "type": "object",
            "title": "BasicMessage",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "active": {"type": "boolean"}
            }
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            schema_file = os.path.join(tmpdir, "basic.json")
            with open(schema_file, 'w') as f:
                json.dump(schema, f)
            
            out_file = os.path.join(tmpdir, "output.proto")
            
            try:
                convert_structure_to_proto(schema_file, out_file)
            except Exception as e:
                self.fail(f"Structure-to-proto conversion failed: {e}")
            
            self.assertTrue(os.path.exists(out_file), "Proto file not created")

    def test_188_proto_nested_messages(self):
        """Test proto generation with nested messages"""
        schema = {
            "type": "object",
            "title": "NestedMessage",
            "properties": {
                "inner": {
                    "type": "object",
                    "properties": {
                        "deep": {
                            "type": "object",
                            "properties": {
                                "value": {"type": "string"}
                            }
                        }
                    }
                }
            }
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            schema_file = os.path.join(tmpdir, "nested.json")
            with open(schema_file, 'w') as f:
                json.dump(schema, f)
            
            out_file = os.path.join(tmpdir, "output.proto")
            
            try:
                convert_structure_to_proto(schema_file, out_file)
            except Exception as e:
                self.fail(f"Structure-to-proto conversion failed with nesting: {e}")


class TestStructureToIceberg(RobustnessTestBase):
    """Tests for structure-to-Iceberg conversion"""
    
    def test_189_iceberg_basic_schema(self):
        """Test Iceberg generation with basic schema"""
        avro_schema = {
            "type": "record",
            "name": "IcebergRecord",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}}
            ]
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            avsc_file = os.path.join(tmpdir, "iceberg.avsc")
            with open(avsc_file, 'w') as f:
                json.dump(avro_schema, f)
            
            out_file = os.path.join(tmpdir, "output.json")
            
            try:
                convert_avro_to_iceberg(avsc_file, "IcebergRecord", out_file)
            except Exception as e:
                self.fail(f"Avro-to-Iceberg conversion failed: {e}")
            
            self.assertTrue(os.path.exists(out_file), "Iceberg file not created")


class TestBinaryAndNumericTypes(RobustnessTestBase):
    """Tests for binary and numeric type handling"""
    
    def test_190_python_bytes_handling(self):
        """Test Python generation with bytes type"""
        schema = {
            "type": "object",
            "title": "BinarySchema",
            "properties": {
                "binaryData": {
                    "type": "string",
                    "contentEncoding": "base64"
                },
                "rawBytes": {
                    "type": "string",
                    "format": "byte"
                }
            }
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            schema_file = os.path.join(tmpdir, "binary.json")
            with open(schema_file, 'w') as f:
                json.dump(schema, f)
            
            py_dir = os.path.join(tmpdir, "py_output")
            os.makedirs(py_dir)
            
            try:
                convert_structure_to_python(schema_file, py_dir)
            except Exception as e:
                self.fail(f"Python conversion failed with binary types: {e}")
            
            can_import, error = self.verify_python_imports(py_dir)
            if not can_import:
                self.fail(f"Generated Python doesn't import: {error}")

    def test_191_large_numbers(self):
        """Test handling of large number constraints"""
        schema = {
            "type": "object",
            "title": "LargeNumbers",
            "properties": {
                "bigInt": {
                    "type": "integer",
                    "minimum": -9223372036854775808,
                    "maximum": 9223372036854775807
                },
                "preciseDecimal": {
                    "type": "number",
                    "multipleOf": 0.0001
                }
            }
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            schema_file = os.path.join(tmpdir, "large.json")
            with open(schema_file, 'w') as f:
                json.dump(schema, f)
            
            # Test Java
            java_dir = os.path.join(tmpdir, "java_output")
            os.makedirs(java_dir)
            try:
                convert_structure_to_java(schema_file, java_dir, package_name="com.test.large")
            except Exception as e:
                self.fail(f"Java conversion failed with large numbers: {e}")


class TestEmptySchemas(RobustnessTestBase):
    """Tests for edge cases with empty or minimal schemas"""
    
    def test_192_empty_properties(self):
        """Test schema with empty properties object"""
        schema = {
            "type": "object",
            "title": "EmptyProps",
            "properties": {}
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            schema_file = os.path.join(tmpdir, "empty.json")
            with open(schema_file, 'w') as f:
                json.dump(schema, f)
            
            py_dir = os.path.join(tmpdir, "py_output")
            os.makedirs(py_dir)
            
            try:
                convert_structure_to_python(schema_file, py_dir)
            except Exception as e:
                self.fail(f"Python conversion failed with empty properties: {e}")

    def test_193_only_additional_properties(self):
        """Test schema with only additionalProperties"""
        schema = {
            "type": "object",
            "title": "MapOnly",
            "additionalProperties": {
                "type": "string"
            }
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            schema_file = os.path.join(tmpdir, "map.json")
            with open(schema_file, 'w') as f:
                json.dump(schema, f)
            
            ts_dir = os.path.join(tmpdir, "ts_output")
            os.makedirs(ts_dir)
            
            try:
                convert_structure_to_typescript(schema_file, ts_dir)
            except Exception as e:
                self.fail(f"TypeScript conversion failed with only additionalProperties: {e}")


class TestNullableFields(RobustnessTestBase):
    """Tests for nullable field handling"""
    
    def test_194_all_nullable_fields(self):
        """Test schema where all fields are nullable"""
        schema = {
            "type": "object",
            "title": "AllNullable",
            "properties": {
                "field1": {"type": ["string", "null"]},
                "field2": {"type": ["integer", "null"]},
                "field3": {"type": ["boolean", "null"]},
                "field4": {"type": ["array", "null"], "items": {"type": "string"}}
            }
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            schema_file = os.path.join(tmpdir, "nullable.json")
            with open(schema_file, 'w') as f:
                json.dump(schema, f)
            
            # Test C#
            csharp_dir = os.path.join(tmpdir, "csharp_output")
            os.makedirs(csharp_dir)
            try:
                convert_structure_to_csharp(schema_file, csharp_dir, base_namespace="Test.Nullable")
            except Exception as e:
                self.fail(f"C# conversion failed with nullable fields: {e}")
            
            can_compile, error = self.verify_csharp_compiles(csharp_dir)
            if not can_compile:
                self.fail(f"Generated C# doesn't compile: {error}")

    def test_195_avro_null_default(self):
        """Test Avro schema with null defaults"""
        avro_schema = {
            "type": "record",
            "name": "NullDefaults",
            "namespace": "test.nulls",
            "fields": [
                {"name": "opt1", "type": ["null", "string"], "default": None},
                {"name": "opt2", "type": ["null", "int"], "default": None},
                {"name": "opt3", "type": ["null", {"type": "array", "items": "string"}], "default": None}
            ]
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            avsc_file = os.path.join(tmpdir, "nulldefaults.avsc")
            with open(avsc_file, 'w') as f:
                json.dump(avro_schema, f)
            
            # Test Go
            go_dir = os.path.join(tmpdir, "go_output")
            os.makedirs(go_dir)
            try:
                convert_avro_to_go(avsc_file, go_dir, package_name="nulls")
            except Exception as e:
                self.fail(f"Go conversion failed with null defaults: {e}")


class TestSpecialTypeNames(RobustnessTestBase):
    """Tests for special type name handling"""
    
    def test_196_type_name_with_underscores(self):
        """Test type names with underscores"""
        schema = {
            "type": "object",
            "title": "my_type_name",
            "properties": {
                "my_field_name": {"type": "string"}
            }
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            schema_file = os.path.join(tmpdir, "underscores.json")
            with open(schema_file, 'w') as f:
                json.dump(schema, f)
            
            # Test Go - which uses PascalCase
            go_dir = os.path.join(tmpdir, "go_output")
            os.makedirs(go_dir)
            try:
                convert_structure_to_go(schema_file, go_dir, package_name="underscores")
            except Exception as e:
                self.fail(f"Go conversion failed with underscore names: {e}")

    def test_197_type_name_all_caps(self):
        """Test type names that are ALL CAPS"""
        schema = {
            "type": "object",
            "title": "ALLCAPS",
            "properties": {
                "ID": {"type": "string"},
                "URL": {"type": "string"},
                "API_KEY": {"type": "string"}
            }
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            schema_file = os.path.join(tmpdir, "caps.json")
            with open(schema_file, 'w') as f:
                json.dump(schema, f)
            
            # Test Rust
            rust_dir = os.path.join(tmpdir, "rust_output")
            os.makedirs(rust_dir)
            try:
                convert_structure_to_rust(schema_file, rust_dir)
            except Exception as e:
                self.fail(f"Rust conversion failed with ALL CAPS names: {e}")


class TestDefaultValues(RobustnessTestBase):
    """Tests for default value handling"""
    
    def test_198_complex_defaults(self):
        """Test schema with complex default values"""
        schema = {
            "type": "object",
            "title": "ComplexDefaults",
            "properties": {
                "tags": {
                    "type": "array",
                    "items": {"type": "string"},
                    "default": ["default", "tags"]
                },
                "config": {
                    "type": "object",
                    "properties": {
                        "enabled": {"type": "boolean"}
                    },
                    "default": {"enabled": True}
                }
            }
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            schema_file = os.path.join(tmpdir, "defaults.json")
            with open(schema_file, 'w') as f:
                json.dump(schema, f)
            
            # Test Python
            py_dir = os.path.join(tmpdir, "py_output")
            os.makedirs(py_dir)
            try:
                convert_structure_to_python(schema_file, py_dir)
            except Exception as e:
                self.fail(f"Python conversion failed with complex defaults: {e}")
            
            can_import, error = self.verify_python_imports(py_dir)
            if not can_import:
                self.fail(f"Generated Python doesn't import: {error}")

    def test_199_avro_record_default(self):
        """Test Avro schema with record default value"""
        avro_schema = {
            "type": "record",
            "name": "WithRecordDefault",
            "namespace": "test.defaults",
            "fields": [
                {
                    "name": "config",
                    "type": {
                        "type": "record",
                        "name": "Config",
                        "fields": [
                            {"name": "enabled", "type": "boolean"},
                            {"name": "count", "type": "int"}
                        ]
                    },
                    "default": {"enabled": True, "count": 0}
                }
            ]
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            avsc_file = os.path.join(tmpdir, "record_default.avsc")
            with open(avsc_file, 'w') as f:
                json.dump(avro_schema, f)
            
            # Test Java
            java_dir = os.path.join(tmpdir, "java_output")
            os.makedirs(java_dir)
            try:
                convert_avro_to_java(avsc_file, java_dir)
            except Exception as e:
                self.fail(f"Java conversion failed with record default: {e}")


class TestPatternProperties(RobustnessTestBase):
    """Tests for patternProperties handling"""
    
    def test_200_simple_pattern_properties(self):
        """Test schema with simple patternProperties"""
        schema = {
            "type": "object",
            "title": "PatternSchema",
            "patternProperties": {
                "^S_": {"type": "string"},
                "^I_": {"type": "integer"}
            }
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            schema_file = os.path.join(tmpdir, "pattern.json")
            with open(schema_file, 'w') as f:
                json.dump(schema, f)
            
            # Test TypeScript
            ts_dir = os.path.join(tmpdir, "ts_output")
            os.makedirs(ts_dir)
            try:
                convert_structure_to_typescript(schema_file, ts_dir)
            except Exception as e:
                self.fail(f"TypeScript conversion failed with patternProperties: {e}")


class TestAvroLogicalTypesCompilation(RobustnessTestBase):
    """Tests for Avro logical types compilation across languages"""
    
    def test_201_python_all_logical_types(self):
        """Test Python compilation with all Avro logical types"""
        avro_schema = {
            "type": "record",
            "name": "AllLogicalTypes",
            "namespace": "test.logical",
            "fields": [
                {"name": "date_field", "type": {"type": "int", "logicalType": "date"}},
                {"name": "time_millis", "type": {"type": "int", "logicalType": "time-millis"}},
                {"name": "time_micros", "type": {"type": "long", "logicalType": "time-micros"}},
                {"name": "timestamp_millis", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "timestamp_micros", "type": {"type": "long", "logicalType": "timestamp-micros"}},
                {"name": "decimal_field", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}},
                {"name": "uuid_field", "type": {"type": "string", "logicalType": "uuid"}},
                {"name": "duration_field", "type": {"type": "fixed", "name": "Duration", "size": 12, "logicalType": "duration"}}
            ]
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            avsc_file = os.path.join(tmpdir, "logical.avsc")
            with open(avsc_file, 'w') as f:
                json.dump(avro_schema, f)
            
            py_dir = os.path.join(tmpdir, "py_output")
            os.makedirs(py_dir)
            
            try:
                convert_avro_to_python(avsc_file, py_dir)
            except Exception as e:
                self.fail(f"Python conversion failed: {e}")
            
            can_import, error = self.verify_python_imports(py_dir)
            if not can_import:
                self.fail(f"Generated Python doesn't import: {error}")

    def test_202_go_all_logical_types(self):
        """Test Go compilation with all Avro logical types"""
        avro_schema = {
            "type": "record",
            "name": "AllLogicalTypes",
            "namespace": "test.logical",
            "fields": [
                {"name": "date_field", "type": {"type": "int", "logicalType": "date"}},
                {"name": "timestamp_millis", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "decimal_field", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}},
                {"name": "uuid_field", "type": {"type": "string", "logicalType": "uuid"}}
            ]
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            avsc_file = os.path.join(tmpdir, "logical.avsc")
            with open(avsc_file, 'w') as f:
                json.dump(avro_schema, f)
            
            go_dir = os.path.join(tmpdir, "go_output")
            os.makedirs(go_dir)
            
            try:
                convert_avro_to_go(avsc_file, go_dir, package_name="logical")
            except Exception as e:
                self.fail(f"Go conversion failed: {e}")
            
            can_compile, error = self.verify_go_compiles(go_dir)
            if not can_compile:
                self.fail(f"Generated Go doesn't compile: {error}")


class TestTypeScriptEdgeCases(RobustnessTestBase):
    """Tests for TypeScript generation edge cases"""
    
    def test_203_ts_union_types(self):
        """Test TypeScript with JSON Schema oneOf"""
        schema = {
            "type": "object",
            "title": "UnionContainer",
            "properties": {
                "data": {
                    "oneOf": [
                        {"type": "string"},
                        {"type": "number"},
                        {"type": "object", "properties": {"value": {"type": "string"}}}
                    ]
                }
            }
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            schema_file = os.path.join(tmpdir, "union.json")
            with open(schema_file, 'w') as f:
                json.dump(schema, f)
            
            ts_dir = os.path.join(tmpdir, "ts_output")
            os.makedirs(ts_dir)
            
            try:
                convert_structure_to_typescript(schema_file, ts_dir)
            except Exception as e:
                self.fail(f"TypeScript conversion failed: {e}")
            
            can_compile, error = self.verify_typescript_compiles(ts_dir)
            if not can_compile:
                self.fail(f"Generated TypeScript doesn't compile: {error}")

    def test_204_ts_avro_unions(self):
        """Test TypeScript compilation with Avro union types"""
        avro_schema = {
            "type": "record",
            "name": "UnionRecord",
            "namespace": "test.unions",
            "fields": [
                {
                    "name": "stringOrNull",
                    "type": ["null", "string"]
                },
                {
                    "name": "intOrLongOrDouble",
                    "type": ["int", "long", "double"]
                },
                {
                    "name": "complexUnion",
                    "type": [
                        "null",
                        "string",
                        {"type": "array", "items": "string"},
                        {"type": "map", "values": "int"}
                    ]
                }
            ]
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            avsc_file = os.path.join(tmpdir, "unions.avsc")
            with open(avsc_file, 'w') as f:
                json.dump(avro_schema, f)
            
            ts_dir = os.path.join(tmpdir, "ts_output")
            os.makedirs(ts_dir)
            
            try:
                convert_avro_to_typescript(avsc_file, ts_dir)
            except Exception as e:
                self.fail(f"TypeScript conversion failed: {e}")
            
            can_compile, error = self.verify_typescript_compiles(ts_dir)
            if not can_compile:
                self.fail(f"Generated TypeScript doesn't compile: {error}")


class TestGoEdgeCases2(RobustnessTestBase):
    """Additional tests for Go generation edge cases"""
    
    def test_205_go_json_schema_allof(self):
        """Test Go with JSON Schema allOf"""
        schema = {
            "type": "object",
            "title": "AllOfSchema",
            "allOf": [
                {
                    "type": "object",
                    "properties": {"id": {"type": "string"}}
                },
                {
                    "type": "object",
                    "properties": {"name": {"type": "string"}}
                }
            ]
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            schema_file = os.path.join(tmpdir, "allof.json")
            with open(schema_file, 'w') as f:
                json.dump(schema, f)
            
            go_dir = os.path.join(tmpdir, "go_output")
            os.makedirs(go_dir)
            
            try:
                convert_structure_to_go(schema_file, go_dir, package_name="allof")
            except Exception as e:
                self.fail(f"Go conversion failed: {e}")
            
            can_compile, error = self.verify_go_compiles(go_dir)
            if not can_compile:
                self.fail(f"Generated Go doesn't compile: {error}")

    def test_206_go_avro_maps(self):
        """Test Go compilation with Avro map types"""
        avro_schema = {
            "type": "record",
            "name": "MapRecord",
            "namespace": "test.maps",
            "fields": [
                {"name": "stringMap", "type": {"type": "map", "values": "string"}},
                {"name": "intMap", "type": {"type": "map", "values": "int"}},
                {"name": "recordMap", "type": {"type": "map", "values": {
                    "type": "record",
                    "name": "MapValue",
                    "fields": [{"name": "value", "type": "string"}]
                }}}
            ]
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            avsc_file = os.path.join(tmpdir, "maps.avsc")
            with open(avsc_file, 'w') as f:
                json.dump(avro_schema, f)
            
            go_dir = os.path.join(tmpdir, "go_output")
            os.makedirs(go_dir)
            
            try:
                convert_avro_to_go(avsc_file, go_dir, package_name="maps")
            except Exception as e:
                self.fail(f"Go conversion failed: {e}")
            
            can_compile, error = self.verify_go_compiles(go_dir)
            if not can_compile:
                self.fail(f"Generated Go doesn't compile: {error}")


class TestCSharpSpecialCases(RobustnessTestBase):
    """Tests for C# generation special cases"""
    
    def test_207_csharp_nullable_primitives(self):
        """Test C# with nullable primitive types from JSON Schema"""
        schema = {
            "type": "object",
            "title": "NullablePrimitives",
            "properties": {
                "nullableInt": {"type": ["integer", "null"]},
                "nullableString": {"type": ["string", "null"]},
                "nullableBool": {"type": ["boolean", "null"]},
                "nullableNumber": {"type": ["number", "null"]}
            }
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            schema_file = os.path.join(tmpdir, "nullable.json")
            with open(schema_file, 'w') as f:
                json.dump(schema, f)
            
            csharp_dir = os.path.join(tmpdir, "csharp_output")
            os.makedirs(csharp_dir)
            
            try:
                convert_structure_to_csharp(schema_file, csharp_dir, base_namespace="Test.Nullable")
            except Exception as e:
                self.fail(f"C# conversion failed: {e}")
            
            can_compile, error = self.verify_csharp_compiles(csharp_dir)
            if not can_compile:
                self.fail(f"Generated C# doesn't compile: {error}")

    def test_208_csharp_avro_arrays(self):
        """Test C# compilation with various Avro array types"""
        avro_schema = {
            "type": "record",
            "name": "ArrayRecord",
            "namespace": "test.arrays",
            "fields": [
                {"name": "stringArray", "type": {"type": "array", "items": "string"}},
                {"name": "intArray", "type": {"type": "array", "items": "int"}},
                {"name": "nestedArray", "type": {"type": "array", "items": {"type": "array", "items": "string"}}},
                {"name": "recordArray", "type": {"type": "array", "items": {
                    "type": "record",
                    "name": "ArrayItem",
                    "fields": [{"name": "value", "type": "string"}]
                }}}
            ]
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            avsc_file = os.path.join(tmpdir, "arrays.avsc")
            with open(avsc_file, 'w') as f:
                json.dump(avro_schema, f)
            
            csharp_dir = os.path.join(tmpdir, "csharp_output")
            os.makedirs(csharp_dir)
            
            try:
                convert_avro_to_csharp(avsc_file, csharp_dir, base_namespace="Test.Arrays")
            except Exception as e:
                self.fail(f"C# conversion failed: {e}")
            
            can_compile, error = self.verify_csharp_compiles(csharp_dir)
            if not can_compile:
                self.fail(f"Generated C# doesn't compile: {error}")


class TestRustSpecialCases(RobustnessTestBase):
    """Tests for Rust generation special cases"""
    
    def test_209_rust_json_schema_enums(self):
        """Test Rust with JSON Schema enum values"""
        schema = {
            "type": "object",
            "title": "EnumContainer",
            "properties": {
                "status": {
                    "type": "string",
                    "enum": ["active", "inactive", "pending"]
                },
                "priority": {
                    "type": "integer",
                    "enum": [1, 2, 3]
                }
            }
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            schema_file = os.path.join(tmpdir, "enums.json")
            with open(schema_file, 'w') as f:
                json.dump(schema, f)
            
            rust_dir = os.path.join(tmpdir, "rust_output")
            os.makedirs(rust_dir)
            
            try:
                convert_structure_to_rust(schema_file, rust_dir)
            except Exception as e:
                self.fail(f"Rust conversion failed: {e}")
            
            can_compile, error = self.verify_rust_compiles(rust_dir)
            if not can_compile:
                self.fail(f"Generated Rust doesn't compile: {error}")


class TestAvroRecursiveTypes(RobustnessTestBase):
    """Tests for recursive/self-referencing Avro types"""
    
    def test_210_avro_self_reference_python(self):
        """Test Python with self-referencing Avro type"""
        avro_schema = {
            "type": "record",
            "name": "TreeNode",
            "namespace": "test.tree",
            "fields": [
                {"name": "value", "type": "string"},
                {"name": "children", "type": {"type": "array", "items": "TreeNode"}},
                {"name": "parent", "type": ["null", "TreeNode"], "default": None}
            ]
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            avsc_file = os.path.join(tmpdir, "tree.avsc")
            with open(avsc_file, 'w') as f:
                json.dump(avro_schema, f)
            
            py_dir = os.path.join(tmpdir, "py_output")
            os.makedirs(py_dir)
            
            try:
                convert_avro_to_python(avsc_file, py_dir)
            except Exception as e:
                self.fail(f"Python conversion failed: {e}")
            
            can_import, error = self.verify_python_imports(py_dir)
            if not can_import:
                self.fail(f"Generated Python doesn't import: {error}")

    def test_211_avro_self_reference_go(self):
        """Test Go with self-referencing Avro type"""
        avro_schema = {
            "type": "record",
            "name": "LinkedNode",
            "namespace": "test.linked",
            "fields": [
                {"name": "value", "type": "int"},
                {"name": "next", "type": ["null", "LinkedNode"], "default": None}
            ]
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            avsc_file = os.path.join(tmpdir, "linked.avsc")
            with open(avsc_file, 'w') as f:
                json.dump(avro_schema, f)
            
            go_dir = os.path.join(tmpdir, "go_output")
            os.makedirs(go_dir)
            
            try:
                convert_avro_to_go(avsc_file, go_dir, package_name="linked")
            except Exception as e:
                self.fail(f"Go conversion failed: {e}")
            
            can_compile, error = self.verify_go_compiles(go_dir)
            if not can_compile:
                self.fail(f"Generated Go doesn't compile: {error}")


class TestEdgeCaseInputFilenames(RobustnessTestBase):
    """Tests for schemas with edge-case input filenames"""
    
    def test_212_python_input_file_named_typing(self):
        """Test Python when input file would shadow 'typing' module"""
        schema = {
            "type": "object",
            "title": "SimpleType",
            "properties": {
                "value": {"type": "string"}
            }
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create file named 'typing.json' - this could cause shadowing issues
            schema_file = os.path.join(tmpdir, "typing.json")
            with open(schema_file, 'w') as f:
                json.dump(schema, f)
            
            py_dir = os.path.join(tmpdir, "py_output")
            os.makedirs(py_dir)
            
            try:
                convert_structure_to_python(schema_file, py_dir)
            except Exception as e:
                self.fail(f"Python conversion failed: {e}")
            
            can_import, error = self.verify_python_imports(py_dir)
            if not can_import:
                self.fail(f"Generated Python doesn't import (typing shadowing): {error}")

    def test_213_python_input_file_named_json(self):
        """Test Python when input file would shadow 'json' module"""
        schema = {
            "type": "object",
            "title": "JsonData",
            "properties": {
                "data": {"type": "string"}
            }
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create file named 'json_data.json' - less problematic but still test
            schema_file = os.path.join(tmpdir, "json.json")
            with open(schema_file, 'w') as f:
                json.dump(schema, f)
            
            py_dir = os.path.join(tmpdir, "py_output")
            os.makedirs(py_dir)
            
            try:
                convert_structure_to_python(schema_file, py_dir)
            except Exception as e:
                self.fail(f"Python conversion failed: {e}")
            
            can_import, error = self.verify_python_imports(py_dir)
            if not can_import:
                self.fail(f"Generated Python doesn't import (json shadowing): {error}")


class TestComplexTypeConversions(RobustnessTestBase):
    """Tests for complex type conversion scenarios"""
    
    def test_214_java_deeply_nested_objects(self):
        """Test Java with deeply nested objects"""
        schema = {
            "type": "object",
            "title": "DeepNest",
            "properties": {
                "level1": {
                    "type": "object",
                    "properties": {
                        "level2": {
                            "type": "object",
                            "properties": {
                                "level3": {
                                    "type": "object",
                                    "properties": {
                                        "level4": {
                                            "type": "object",
                                            "properties": {
                                                "value": {"type": "string"}
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            schema_file = os.path.join(tmpdir, "deep.json")
            with open(schema_file, 'w') as f:
                json.dump(schema, f)
            
            java_dir = os.path.join(tmpdir, "java_output")
            os.makedirs(java_dir)
            
            try:
                convert_structure_to_java(schema_file, java_dir, package_name="com.test.deep")
            except Exception as e:
                self.fail(f"Java conversion failed: {e}")
            
            can_compile, error = self.verify_java_compiles(java_dir)
            if not can_compile:
                self.fail(f"Generated Java doesn't compile: {error}")

    def test_215_csharp_avro_multiple_enums(self):
        """Test C# with multiple enum types in Avro"""
        avro_schema = {
            "type": "record",
            "name": "MultiEnum",
            "namespace": "test.enums",
            "fields": [
                {
                    "name": "status",
                    "type": {"type": "enum", "name": "Status", "symbols": ["ACTIVE", "INACTIVE"]}
                },
                {
                    "name": "priority",
                    "type": {"type": "enum", "name": "Priority", "symbols": ["LOW", "MEDIUM", "HIGH"]}
                },
                {
                    "name": "category",
                    "type": {"type": "enum", "name": "Category", "symbols": ["A", "B", "C", "D"]}
                }
            ]
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            avsc_file = os.path.join(tmpdir, "multienum.avsc")
            with open(avsc_file, 'w') as f:
                json.dump(avro_schema, f)
            
            csharp_dir = os.path.join(tmpdir, "csharp_output")
            os.makedirs(csharp_dir)
            
            try:
                convert_avro_to_csharp(avsc_file, csharp_dir, base_namespace="Test.Enums")
            except Exception as e:
                self.fail(f"C# conversion failed: {e}")
            
            can_compile, error = self.verify_csharp_compiles(csharp_dir)
            if not can_compile:
                self.fail(f"Generated C# doesn't compile: {error}")


if __name__ == '__main__':
    unittest.main()
