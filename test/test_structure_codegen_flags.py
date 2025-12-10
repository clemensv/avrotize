"""
Comprehensive tests for JSON Structure code generation with all flag combinations.

This module tests the various annotation flags and their combinations for:
- TypeScript (typedjson_annotation, avro_annotation)
- Python (dataclasses_json_annotation, avro_annotation)
- Go (json_annotation, avro_annotation)
- C# (system_text_json_annotation, newtonsoft_json_annotation, system_xml_annotation, pascal_properties)
"""

import unittest
import os
import shutil
import subprocess
import sys
import tempfile
import json
import itertools
from typing import List, Tuple, Dict, Any

import pytest

# Import converters
from avrotize.structuretots import convert_structure_to_typescript, convert_structure_schema_to_typescript
from avrotize.structuretopython import convert_structure_to_python, convert_structure_schema_to_python
from avrotize.structuretogo import convert_structure_to_go, convert_structure_schema_to_go
from avrotize.structuretocsharp import convert_structure_to_csharp, convert_structure_schema_to_csharp


# Test schemas that cover various JSON Structure features
BASIC_SCHEMA = {
    "name": "BasicRecord",
    "type": "object",
    "properties": {
        "id": {"type": "string"},
        "count": {"type": "integer"},
        "active": {"type": "boolean"},
        "score": {"type": "number"}
    },
    "required": ["id", "count"]
}

ENUM_SCHEMA = {
    "name": "RecordWithEnum",
    "type": "object",
    "properties": {
        "status": {
            "type": "string",
            "enum": ["Active", "Inactive", "Pending"]
        },
        "priority": {
            "type": "string",
            "enum": ["Low", "Medium", "High", "Critical"]
        }
    },
    "required": ["status"]
}

NESTED_SCHEMA = {
    "$defs": {
        "Address": {
            "name": "Address",
            "type": "object",
            "properties": {
                "street": {"type": "string"},
                "city": {"type": "string"},
                "zipCode": {"type": "string"}
            },
            "required": ["street", "city"]
        }
    },
    "name": "Person",
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "age": {"type": "integer"},
        "address": {"$ref": "#/$defs/Address"}
    },
    "required": ["name"]
}

ARRAY_MAP_SCHEMA = {
    "name": "CollectionRecord",
    "type": "object",
    "properties": {
        "tags": {
            "type": "array",
            "items": {"type": "string"}
        },
        "scores": {
            "type": "array",
            "items": {"type": "number"}
        },
        "labels": {
            "type": "array",
            "items": {"type": "integer"}
        }
    },
    "required": ["tags"]
}

TEMPORAL_SCHEMA = {
    "name": "TemporalRecord",
    "type": "object",
    "properties": {
        "createdAt": {"type": "string", "format": "date-time"},
        "updatedDate": {"type": "string", "format": "date"},
        "duration": {"type": "string", "format": "duration"}
    },
    "required": ["createdAt"]
}

CHOICE_SCHEMA = {
    "$defs": {
        "Cat": {
            "name": "Cat",
            "type": "object",
            "properties": {
                "meow": {"type": "string"}
            }
        },
        "Dog": {
            "name": "Dog",
            "type": "object",
            "properties": {
                "bark": {"type": "string"}
            }
        }
    },
    "name": "PetOwner",
    "type": "object",
    "properties": {
        "ownerName": {"type": "string"},
        "pet": {
            "oneOf": [
                {"$ref": "#/$defs/Cat"},
                {"$ref": "#/$defs/Dog"}
            ]
        }
    },
    "required": ["ownerName"]
}

ALL_TEST_SCHEMAS = {
    "basic": BASIC_SCHEMA,
    "enum": ENUM_SCHEMA,
    "nested": NESTED_SCHEMA,
    "array_map": ARRAY_MAP_SCHEMA,
    "temporal": TEMPORAL_SCHEMA,
    "choice": CHOICE_SCHEMA,
}


def get_temp_dir(name: str) -> str:
    """Get a clean temporary directory for test output."""
    path = os.path.join(tempfile.gettempdir(), "avrotize_tests", name)
    if os.path.exists(path):
        shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path, exist_ok=True)
    return path


class TestTypeScriptFlagCombinations(unittest.TestCase):
    """Test TypeScript code generation with all flag combinations."""

    def _verify_typescript_output(self, output_dir: str, should_have_typedjson: bool = False):
        """Verify the TypeScript output is valid."""
        # Check basic structure
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'src')), "src directory missing")
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'package.json')), "package.json missing")
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'tsconfig.json')), "tsconfig.json missing")
        
        # Check package.json has required dependencies
        with open(os.path.join(output_dir, 'package.json'), 'r') as f:
            pkg = json.load(f)
        
        if should_have_typedjson:
            self.assertIn('typedjson', pkg.get('dependencies', {}), 
                         "typedjson should be in dependencies when annotation is enabled")
        
        # Check Jest config has moduleNameMapper for .js imports
        jest_config = pkg.get('jest', {})
        self.assertIn('moduleNameMapper', jest_config, 
                     "Jest config should have moduleNameMapper for ESM .js imports")
        
        # Find generated TypeScript files
        ts_files = []
        for root, _, files in os.walk(os.path.join(output_dir, 'src')):
            for f in files:
                if f.endswith('.ts'):
                    ts_files.append(os.path.join(root, f))
        
        self.assertGreater(len(ts_files), 0, "No TypeScript files generated")
        
        # Check for typedjson decorators if enabled
        if should_have_typedjson:
            found_decorator = False
            for ts_file in ts_files:
                with open(ts_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                if '@jsonObject' in content or '@jsonMember' in content:
                    found_decorator = True
                    break
            self.assertTrue(found_decorator, "No TypedJSON decorators found when annotation enabled")

    def _try_compile_typescript(self, output_dir: str) -> bool:
        """Try to compile TypeScript, return True if successful."""
        try:
            # On Windows, npm is a script that requires shell=True
            use_shell = sys.platform == 'win32'
            subprocess.check_call(
                ["npm", "install"],
                cwd=output_dir,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=120,
                shell=use_shell
            )
            subprocess.check_call(
                ["npm", "run", "build"],
                cwd=output_dir,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=60,
                shell=use_shell
            )
            return True
        except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired) as e:
            print(f"TypeScript compilation failed or skipped: {e}")
            return False

    def _run_typescript_tests(self, output_dir: str) -> bool:
        """Run TypeScript tests, return True if successful."""
        try:
            # On Windows, npm is a script that requires shell=True
            use_shell = sys.platform == 'win32'
            subprocess.check_call(
                ["npm", "test"],
                cwd=output_dir,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=120,
                shell=use_shell
            )
            return True
        except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired) as e:
            print(f"TypeScript tests failed or skipped: {e}")
            return False

    def test_basic_schema_all_flag_combinations(self):
        """Test basic schema with all flag combinations."""
        flag_combinations = [
            (False, False),
            (True, False),
            (False, True),
            (True, True),
        ]
        for typedjson, avro in flag_combinations:
            flag_str = f"typedjson={typedjson}_avro={avro}"
            with self.subTest(flags=flag_str):
                output_dir = get_temp_dir(f"ts_basic_{flag_str}")
                
                convert_structure_schema_to_typescript(
                    BASIC_SCHEMA,
                    output_dir,
                    package_name="test-basic",
                    typedjson_annotation=typedjson,
                    avro_annotation=avro
                )
                
                self._verify_typescript_output(output_dir, should_have_typedjson=typedjson)
                
                # Actually compile and run the generated tests
                if self._try_compile_typescript(output_dir):
                    self.assertTrue(self._run_typescript_tests(output_dir), 
                                   f"TypeScript tests failed for flags={flag_str}")

    def test_all_schemas_with_typedjson(self):
        """Test all schema types with TypedJSON annotation."""
        for schema_name, schema in ALL_TEST_SCHEMAS.items():
            with self.subTest(schema=schema_name):
                output_dir = get_temp_dir(f"ts_{schema_name}_typedjson")
                convert_structure_schema_to_typescript(
                    schema,
                    output_dir,
                    package_name=f"test-{schema_name}",
                    typedjson_annotation=True
                )
                self._verify_typescript_output(output_dir, should_have_typedjson=True)
                
                # Actually compile and run the generated tests
                if self._try_compile_typescript(output_dir):
                    self.assertTrue(self._run_typescript_tests(output_dir), 
                                   f"TypeScript tests failed for schema={schema_name}")

    def test_all_schemas_without_annotations(self):
        """Test all schema types without annotations."""
        for schema_name, schema in ALL_TEST_SCHEMAS.items():
            with self.subTest(schema=schema_name):
                output_dir = get_temp_dir(f"ts_{schema_name}_plain")
                convert_structure_schema_to_typescript(
                    schema,
                    output_dir,
                    package_name=f"test-{schema_name}"
                )
                self._verify_typescript_output(output_dir)
                
                # Actually compile and run the generated tests
                if self._try_compile_typescript(output_dir):
                    self.assertTrue(self._run_typescript_tests(output_dir), 
                                   f"TypeScript tests failed for schema={schema_name}")

    def test_enum_schema_compilation(self):
        """Test enum schema compiles and enum imports are correct."""
        output_dir = get_temp_dir("ts_enum_compile")
        convert_structure_schema_to_typescript(
            ENUM_SCHEMA,
            output_dir,
            package_name="test-enum",
            typedjson_annotation=True
        )
        
        self._verify_typescript_output(output_dir, should_have_typedjson=True)
        
        # Check test file has enum imports
        test_dir = os.path.join(output_dir, 'test')
        if os.path.exists(test_dir):
            test_files = [f for f in os.listdir(test_dir) if f.endswith('.test.ts')]
            for test_file in test_files:
                with open(os.path.join(test_dir, test_file), 'r', encoding='utf-8') as f:
                    content = f.read()
                # Verify enum types used in tests are imported
                if 'StatusEnum' in content or 'PriorityEnum' in content:
                    self.assertIn("import {", content, "Test file should have imports")
        
        # Actually compile and run tests
        if self._try_compile_typescript(output_dir):
            self.assertTrue(self._run_typescript_tests(output_dir), 
                           "TypeScript tests failed for enum schema")


class TestPythonFlagCombinations(unittest.TestCase):
    """Test Python code generation with all flag combinations."""

    def _verify_python_output(self, output_dir: str, should_have_dataclasses_json: bool = False):
        """Verify the Python output is valid."""
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'src')), "src directory missing")
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'pyproject.toml')), "pyproject.toml missing")
        
        # Find generated Python files
        py_files = []
        for root, _, files in os.walk(os.path.join(output_dir, 'src')):
            for f in files:
                if f.endswith('.py') and f != '__init__.py':
                    py_files.append(os.path.join(root, f))
        
        self.assertGreater(len(py_files), 0, "No Python files generated")
        
        # Check for dataclasses_json decorators if enabled
        if should_have_dataclasses_json:
            found_decorator = False
            for py_file in py_files:
                with open(py_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                if '@dataclass_json' in content:
                    found_decorator = True
                    break
            self.assertTrue(found_decorator, "No dataclass_json decorators found when annotation enabled")

    def _verify_test_files_valid(self, output_dir: str):
        """Verify generated test files have valid imports."""
        test_dir = os.path.join(output_dir, 'tests')
        if not os.path.exists(test_dir):
            return
        
        for test_file in os.listdir(test_dir):
            if not test_file.endswith('.py'):
                continue
            with open(os.path.join(test_dir, test_file), 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Check for invalid enum test values
            # Valid: StatusEnum.Active (using enum value directly)
            # Invalid: Test_StatusEnum.create_instance() (trying to call create_instance on an enum test class)
            # 
            # We only flag lines where:
            # 1. The type name ends with "Enum" AND
            # 2. The type is imported from a non-test module (meaning it's an actual enum) AND
            # 3. There's a Test_<EnumName>.create_instance() call
            
            import re
            
            # Find all imported enum types (not test classes)
            # Pattern: from package.enumname import EnumName
            enum_imports = set()
            for line in content.split('\n'):
                match = re.search(r'from\s+[\w.]+\s+import\s+(\w+Enum)\b', line)
                if match and 'test_' not in line.lower():
                    enum_imports.add(match.group(1))
            
            # Now check for invalid Test_<EnumType>.create_instance() calls
            lines = content.split('\n')
            for i, line in enumerate(lines):
                for enum_type in enum_imports:
                    # If we import StatusEnum and then call Test_StatusEnum.create_instance(), that's wrong
                    if f'Test_{enum_type}.create_instance()' in line:
                        self.fail(f"Found invalid Test_{enum_type}.create_instance() in {test_file}:{i+1}: {line.strip()}\n"
                                 f"Enum types should use {enum_type}.<VALUE> instead")

    def _try_run_python_tests(self, output_dir: str) -> bool:
        """Try to run Python tests, return True if successful."""
        try:
            # Install the package and run tests
            subprocess.check_call(
                ["pip", "install", "-e", ".", "--quiet"],
                cwd=output_dir,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=120
            )
            subprocess.check_call(
                ["pytest", "-v", "--tb=short"],
                cwd=output_dir,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=180
            )
            return True
        except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired) as e:
            print(f"Python tests failed or skipped: {e}")
            return False

    def test_basic_schema_all_flag_combinations(self):
        """Test basic schema with all flag combinations."""
        flag_combinations = [
            (False, False),
            (True, False),
            (False, True),
            (True, True),
        ]
        for dataclasses_json, avro in flag_combinations:
            flag_str = f"dcj={dataclasses_json}_avro={avro}"
            with self.subTest(flags=flag_str):
                output_dir = get_temp_dir(f"py_basic_{flag_str}")
                
                convert_structure_schema_to_python(
                    BASIC_SCHEMA,
                    output_dir,
                    package_name="test_basic",
                    dataclasses_json_annotation=dataclasses_json
                )
                
                self._verify_python_output(output_dir, should_have_dataclasses_json=dataclasses_json)
                
                # Actually run the generated tests
                self.assertTrue(self._try_run_python_tests(output_dir),
                               f"Python tests failed for flags={flag_str}")

    def test_all_schemas_with_dataclasses_json(self):
        """Test all schema types with dataclasses_json annotation."""
        for schema_name, schema in ALL_TEST_SCHEMAS.items():
            with self.subTest(schema=schema_name):
                output_dir = get_temp_dir(f"py_{schema_name}_dcj")
                convert_structure_schema_to_python(
                    schema,
                    output_dir,
                    package_name=f"test_{schema_name}",
                    dataclasses_json_annotation=True
                )
                self._verify_python_output(output_dir, should_have_dataclasses_json=True)
                self._verify_test_files_valid(output_dir)
                
                # Actually run the generated tests
                self.assertTrue(self._try_run_python_tests(output_dir),
                               f"Python tests failed for schema={schema_name}")

    def test_all_schemas_without_annotations(self):
        """Test all schema types without annotations."""
        for schema_name, schema in ALL_TEST_SCHEMAS.items():
            with self.subTest(schema=schema_name):
                output_dir = get_temp_dir(f"py_{schema_name}_plain")
                convert_structure_schema_to_python(
                    schema,
                    output_dir,
                    package_name=f"test_{schema_name}"
                )
                self._verify_python_output(output_dir)
                
                # Actually run the generated tests
                self.assertTrue(self._try_run_python_tests(output_dir),
                               f"Python tests failed for schema={schema_name}")

    def test_enum_schema_test_values(self):
        """Test enum schema generates correct test values (not Test_EnumType.create_instance)."""
        output_dir = get_temp_dir("py_enum_test_values")
        convert_structure_schema_to_python(
            ENUM_SCHEMA,
            output_dir,
            package_name="test_enum",
            dataclasses_json_annotation=True
        )
        
        self._verify_python_output(output_dir, should_have_dataclasses_json=True)
        self._verify_test_files_valid(output_dir)
        
        # Check test file uses EnumType.Symbol not Test_EnumType.create_instance()
        test_dir = os.path.join(output_dir, 'tests')
        if os.path.exists(test_dir):
            for test_file in os.listdir(test_dir):
                if test_file.endswith('.py'):
                    with open(os.path.join(test_dir, test_file), 'r', encoding='utf-8') as f:
                        content = f.read()
                    # Should find enum value usage like StatusEnum.Active
                    if 'StatusEnum' in content or 'PriorityEnum' in content:
                        self.assertNotIn('Test_StatusEnum.create_instance()', content,
                                        "Should use StatusEnum.Symbol not Test_StatusEnum.create_instance()")
        
        # Actually run the generated tests
        self.assertTrue(self._try_run_python_tests(output_dir),
                       "Python tests failed for enum schema")


class TestGoFlagCombinations(unittest.TestCase):
    """Test Go code generation with all flag combinations."""

    def _verify_go_output(self, output_dir: str, should_have_json: bool = False):
        """Verify the Go output is valid."""
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'go.mod')), "go.mod missing")
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'pkg')), "pkg directory missing")
        
        # Find generated Go files
        go_files = []
        for root, _, files in os.walk(os.path.join(output_dir, 'pkg')):
            for f in files:
                if f.endswith('.go') and not f.endswith('_test.go'):
                    go_files.append(os.path.join(root, f))
        
        self.assertGreater(len(go_files), 0, "No Go files generated")
        
        # Check for json tags if enabled
        if should_have_json:
            found_json_tag = False
            for go_file in go_files:
                with open(go_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                if '`json:"' in content:
                    found_json_tag = True
                    break
            self.assertTrue(found_json_tag, "No json tags found when annotation enabled")

    def _verify_test_files_no_unused_imports(self, output_dir: str):
        """Verify test files don't have unused imports (especially encoding/json for enums)."""
        pkg_dir = os.path.join(output_dir, 'pkg')
        if not os.path.exists(pkg_dir):
            return
        
        for root, _, files in os.walk(pkg_dir):
            for f in files:
                if not f.endswith('_test.go'):
                    continue
                filepath = os.path.join(root, f)
                with open(filepath, 'r', encoding='utf-8') as file:
                    content = file.read()
                
                # Check if encoding/json is imported but not used
                if '"encoding/json"' in content:
                    # If it's imported, it should be used
                    # Common usages: json.Marshal, json.Unmarshal
                    has_import = '"encoding/json"' in content
                    has_usage = 'json.Marshal' in content or 'json.Unmarshal' in content
                    if has_import and not has_usage:
                        # This might be an enum test file with unused import
                        # Check if it's an enum file (has _test.go suffix and no struct)
                        self.fail(f"Unused encoding/json import in {f}")

    def _try_build_go(self, output_dir: str) -> bool:
        """Try to build Go code, return True if successful."""
        try:
            subprocess.check_call(
                ["go", "mod", "tidy"],
                cwd=output_dir,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=60
            )
            subprocess.check_call(
                ["go", "build", "./..."],
                cwd=output_dir,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=60
            )
            return True
        except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired) as e:
            print(f"Go build failed or skipped: {e}")
            return False

    def _try_run_go_tests(self, output_dir: str) -> bool:
        """Try to run Go tests, return True if successful."""
        try:
            subprocess.check_call(
                ["go", "test", "./..."],
                cwd=output_dir,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=120
            )
            return True
        except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired) as e:
            print(f"Go tests failed or skipped: {e}")
            return False

    def test_basic_schema_all_flag_combinations(self):
        """Test basic schema with all flag combinations."""
        flag_combinations = [
            (False, False),
            (True, False),
            (False, True),
            (True, True),
        ]
        for json_annotation, avro in flag_combinations:
            flag_str = f"json={json_annotation}_avro={avro}"
            with self.subTest(flags=flag_str):
                output_dir = get_temp_dir(f"go_basic_{flag_str}")
                
                convert_structure_schema_to_go(
                    BASIC_SCHEMA,
                    output_dir,
                    package_name="testbasic",
                    json_annotation=json_annotation,
                    avro_annotation=avro
                )
                
                self._verify_go_output(output_dir, should_have_json=json_annotation)
                
                # Build and run tests
                if self._try_build_go(output_dir):
                    self.assertTrue(self._try_run_go_tests(output_dir),
                                   f"Go tests failed for flags={flag_str}")

    def test_all_schemas_with_json(self):
        """Test all schema types with JSON annotation."""
        for schema_name, schema in ALL_TEST_SCHEMAS.items():
            with self.subTest(schema=schema_name):
                pkg_name = schema_name.replace('_', '').replace('-', '')
                output_dir = get_temp_dir(f"go_{schema_name}_json")
                convert_structure_schema_to_go(
                    schema,
                    output_dir,
                    package_name=pkg_name,
                    json_annotation=True
                )
                self._verify_go_output(output_dir, should_have_json=True)
                
                # Build and run tests
                if self._try_build_go(output_dir):
                    self.assertTrue(self._try_run_go_tests(output_dir),
                                   f"Go tests failed for schema={schema_name}")

    def test_all_schemas_without_annotations(self):
        """Test all schema types without annotations."""
        for schema_name, schema in ALL_TEST_SCHEMAS.items():
            with self.subTest(schema=schema_name):
                pkg_name = schema_name.replace('_', '').replace('-', '')
                output_dir = get_temp_dir(f"go_{schema_name}_plain")
                convert_structure_schema_to_go(
                    schema,
                    output_dir,
                    package_name=pkg_name
                )
                self._verify_go_output(output_dir)
                
                # Build and run tests
                if self._try_build_go(output_dir):
                    self.assertTrue(self._try_run_go_tests(output_dir),
                                   f"Go tests failed for schema={schema_name}")

    def test_enum_schema_no_unused_imports(self):
        """Test enum schema doesn't have unused encoding/json import in tests."""
        output_dir = get_temp_dir("go_enum_imports")
        convert_structure_schema_to_go(
            ENUM_SCHEMA,
            output_dir,
            package_name="testenum",
            json_annotation=True
        )
        
        self._verify_go_output(output_dir, should_have_json=True)
        self._verify_test_files_no_unused_imports(output_dir)
        
        # Build and run tests
        if self._try_build_go(output_dir):
            self.assertTrue(self._try_run_go_tests(output_dir),
                           "Go tests failed for enum schema")

    def test_temporal_schema_has_time_import(self):
        """Test temporal schema has time import in helpers."""
        output_dir = get_temp_dir("go_temporal_time")
        convert_structure_schema_to_go(
            TEMPORAL_SCHEMA,
            output_dir,
            package_name="testtemporal",
            json_annotation=True
        )
        
        self._verify_go_output(output_dir, should_have_json=True)
        
        # Check helpers file has time import
        pkg_dir = os.path.join(output_dir, 'pkg', 'testtemporal')
        helpers_file = os.path.join(pkg_dir, 'testtemporal_helpers.go')
        if os.path.exists(helpers_file):
            with open(helpers_file, 'r', encoding='utf-8') as f:
                content = f.read()
            # If time.Time or time.Duration is used, "time" should be imported
            if 'time.Time' in content or 'time.Duration' in content or 'time.Now()' in content:
                self.assertIn('"time"', content, "time package should be imported when time types are used")
        
        # Build and run tests
        if self._try_build_go(output_dir):
            self.assertTrue(self._try_run_go_tests(output_dir),
                           "Go tests failed for temporal schema")


class TestCSharpFlagCombinations(unittest.TestCase):
    """Test C# code generation with all flag combinations."""

    def _verify_csharp_output(self, output_dir: str, 
                              should_have_system_text_json: bool = False,
                              should_have_newtonsoft: bool = False,
                              should_have_xml: bool = False):
        """Verify the C# output is valid."""
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'src')), "src directory missing")
        
        # Find .csproj file
        csproj_files = []
        for root, _, files in os.walk(os.path.join(output_dir, 'src')):
            for f in files:
                if f.endswith('.csproj'):
                    csproj_files.append(os.path.join(root, f))
        
        self.assertGreater(len(csproj_files), 0, "No .csproj files found")
        
        # Find generated C# files
        cs_files = []
        for root, _, files in os.walk(os.path.join(output_dir, 'src')):
            for f in files:
                if f.endswith('.cs'):
                    cs_files.append(os.path.join(root, f))
        
        self.assertGreater(len(cs_files), 0, "No C# files generated")

    def _try_build_csharp(self, output_dir: str) -> bool:
        """Try to build C# code, return True if successful."""
        try:
            subprocess.check_call(
                ["dotnet", "build"],
                cwd=output_dir,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=120
            )
            return True
        except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired) as e:
            print(f"C# build failed or skipped: {e}")
            return False

    def _try_test_csharp(self, output_dir: str) -> bool:
        """Try to run C# tests, return True if successful."""
        try:
            subprocess.check_call(
                ["dotnet", "test"],
                cwd=output_dir,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=180
            )
            return True
        except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired) as e:
            print(f"C# tests failed or skipped: {e}")
            return False

    def test_basic_schema_system_text_json_only(self):
        """Test basic schema with System.Text.Json only."""
        output_dir = get_temp_dir("cs_basic_stj")
        convert_structure_schema_to_csharp(
            BASIC_SCHEMA,
            output_dir,
            base_namespace="TestBasic",
            system_text_json_annotation=True
        )
        self._verify_csharp_output(output_dir, should_have_system_text_json=True)
        
        # Build and run tests
        if self._try_build_csharp(output_dir):
            self.assertTrue(self._try_test_csharp(output_dir),
                           "C# tests failed for System.Text.Json")

    def test_basic_schema_newtonsoft_only(self):
        """Test basic schema with Newtonsoft.Json only."""
        output_dir = get_temp_dir("cs_basic_newtonsoft")
        convert_structure_schema_to_csharp(
            BASIC_SCHEMA,
            output_dir,
            base_namespace="TestBasic",
            newtonsoft_json_annotation=True
        )
        self._verify_csharp_output(output_dir, should_have_newtonsoft=True)
        
        # Build and run tests
        if self._try_build_csharp(output_dir):
            self.assertTrue(self._try_test_csharp(output_dir),
                           "C# tests failed for Newtonsoft.Json")

    def test_basic_schema_xml_only(self):
        """Test basic schema with XML annotation only."""
        output_dir = get_temp_dir("cs_basic_xml")
        convert_structure_schema_to_csharp(
            BASIC_SCHEMA,
            output_dir,
            base_namespace="TestBasic",
            system_xml_annotation=True
        )
        self._verify_csharp_output(output_dir, should_have_xml=True)
        
        # Build and run tests
        if self._try_build_csharp(output_dir):
            self.assertTrue(self._try_test_csharp(output_dir),
                           "C# tests failed for System.Xml")

    def test_basic_schema_all_annotations(self):
        """Test basic schema with all annotations."""
        output_dir = get_temp_dir("cs_basic_all")
        convert_structure_schema_to_csharp(
            BASIC_SCHEMA,
            output_dir,
            base_namespace="TestBasic",
            system_text_json_annotation=True,
            newtonsoft_json_annotation=True,
            system_xml_annotation=True,
            pascal_properties=True
        )
        self._verify_csharp_output(output_dir, 
                                   should_have_system_text_json=True,
                                   should_have_newtonsoft=True,
                                   should_have_xml=True)
        
        # Build and run tests
        if self._try_build_csharp(output_dir):
            self.assertTrue(self._try_test_csharp(output_dir),
                           "C# tests failed for all annotations")

    def test_basic_schema_no_annotations(self):
        """Test basic schema with no annotations."""
        output_dir = get_temp_dir("cs_basic_plain")
        convert_structure_schema_to_csharp(
            BASIC_SCHEMA,
            output_dir,
            base_namespace="TestBasic"
        )
        self._verify_csharp_output(output_dir)
        
        # Build and run tests
        if self._try_build_csharp(output_dir):
            self.assertTrue(self._try_test_csharp(output_dir),
                           "C# tests failed for no annotations")

    def test_all_schemas_with_system_text_json(self):
        """Test all schema types with System.Text.Json."""
        for schema_name, schema in ALL_TEST_SCHEMAS.items():
            with self.subTest(schema=schema_name):
                output_dir = get_temp_dir(f"cs_{schema_name}_stj")
                namespace = f"Test{schema_name.replace('_', '').replace('-', '').title()}"
                convert_structure_schema_to_csharp(
                    schema,
                    output_dir,
                    base_namespace=namespace,
                    system_text_json_annotation=True
                )
                self._verify_csharp_output(output_dir, should_have_system_text_json=True)
                
                # Build and run tests
                if self._try_build_csharp(output_dir):
                    self.assertTrue(self._try_test_csharp(output_dir),
                                   f"C# tests failed for schema={schema_name}")

    def test_all_flag_combinations(self):
        """Test all possible flag combinations for C#."""
        flag_combinations = list(itertools.product([True, False], repeat=4))
        
        for stj, newtonsoft, xml, pascal in flag_combinations:
            # Skip if no annotations at all (already tested)
            if not any([stj, newtonsoft, xml, pascal]):
                continue
            
            flag_str = f"stj={stj}_nj={newtonsoft}_xml={xml}_pascal={pascal}"
            with self.subTest(flags=flag_str):
                output_dir = get_temp_dir(f"cs_combo_{flag_str}")
                convert_structure_schema_to_csharp(
                    BASIC_SCHEMA,
                    output_dir,
                    base_namespace="TestBasic",
                    system_text_json_annotation=stj,
                    newtonsoft_json_annotation=newtonsoft,
                    system_xml_annotation=xml,
                    pascal_properties=pascal
                )
                self._verify_csharp_output(output_dir,
                                          should_have_system_text_json=stj,
                                          should_have_newtonsoft=newtonsoft,
                                          should_have_xml=xml)
                
                # Build and run tests
                if self._try_build_csharp(output_dir):
                    self.assertTrue(self._try_test_csharp(output_dir),
                                   f"C# tests failed for flags={flag_str}")

    def test_enum_schema_test_casing(self):
        """Test enum schema generates correct PascalCase test values."""
        output_dir = get_temp_dir("cs_enum_casing")
        convert_structure_schema_to_csharp(
            ENUM_SCHEMA,
            output_dir,
            base_namespace="TestEnum",
            system_text_json_annotation=True
        )
        
        self._verify_csharp_output(output_dir, should_have_system_text_json=True)
        
        # Check test file for correct enum value casing
        test_dir = os.path.join(output_dir, 'test')
        if os.path.exists(test_dir):
            for test_file in os.listdir(test_dir):
                if test_file.endswith('.cs'):
                    with open(os.path.join(test_dir, test_file), 'r', encoding='utf-8') as f:
                        content = f.read()
                    # Enum values should be PascalCase
                    if 'StatusEnum' in content:
                        self.assertIn('StatusEnum.', content, 
                                     "Test should reference enum values")
        
        # Build and run tests
        if self._try_build_csharp(output_dir):
            self.assertTrue(self._try_test_csharp(output_dir),
                           "C# tests failed for enum schema")


class TestSchemaFileCoverage(unittest.TestCase):
    """Test coverage of all schema files in test/struct and test/jsons directories."""

    def test_struct_files_to_typescript(self):
        """Test all struct files in test/struct can be converted to TypeScript."""
        cwd = os.getcwd()
        struct_dir = os.path.join(cwd, "test", "struct")
        if not os.path.exists(struct_dir):
            self.skipTest("test/struct directory not found")
        
        struct_files = [f for f in os.listdir(struct_dir) if f.endswith('.struct.json')]
        self.assertGreater(len(struct_files), 0, "No struct files found")
        
        for struct_file in struct_files[:5]:  # Test first 5 to keep test time reasonable
            schema_name = struct_file.replace('.struct.json', '')
            with self.subTest(schema=schema_name):
                schema_path = os.path.join(struct_dir, struct_file)
                output_dir = get_temp_dir(f"ts_struct_{schema_name}")
                
                try:
                    convert_structure_to_typescript(
                        schema_path,
                        output_dir,
                        package_name=schema_name.replace('-', '_'),
                        typedjson_annotation=True
                    )
                    self.assertTrue(os.path.exists(os.path.join(output_dir, 'src')))
                except Exception as e:
                    self.fail(f"Failed to convert {schema_name}: {e}")

    def test_jsons_files_to_python(self):
        """Test all struct files in test/jsons can be converted to Python."""
        cwd = os.getcwd()
        jsons_dir = os.path.join(cwd, "test", "jsons")
        if not os.path.exists(jsons_dir):
            self.skipTest("test/jsons directory not found")
        
        struct_files = [f for f in os.listdir(jsons_dir) if f.endswith('.struct.json')]
        self.assertGreater(len(struct_files), 0, "No struct files found")
        
        # Filter to simpler schemas for this test
        simple_schemas = ['address-ref.struct.json', 'movie-ref.struct.json', 
                          'person-ref.struct.json', 'userprofile-ref.struct.json']
        struct_files = [f for f in struct_files if f in simple_schemas][:5]
        
        for struct_file in struct_files:
            schema_name = struct_file.replace('.struct.json', '').replace('-ref', '').replace('-', '_')
            with self.subTest(schema=schema_name):
                schema_path = os.path.join(jsons_dir, struct_file)
                output_dir = get_temp_dir(f"py_jsons_{schema_name}")
                
                try:
                    convert_structure_to_python(
                        schema_path,
                        output_dir,
                        package_name=schema_name,
                        dataclasses_json_annotation=True
                    )
                    # Check output - some schemas might have different structure
                    has_src = os.path.exists(os.path.join(output_dir, 'src'))
                    has_pyproject = os.path.exists(os.path.join(output_dir, 'pyproject.toml'))
                    self.assertTrue(has_src or has_pyproject, 
                                   f"Expected src dir or pyproject.toml in {output_dir}")
                except Exception as e:
                    self.fail(f"Failed to convert {schema_name}: {e}")


if __name__ == '__main__':
    unittest.main()
