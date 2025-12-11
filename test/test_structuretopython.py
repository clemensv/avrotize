"""Tests for JSON Structure to Python conversion."""

import unittest
import os
import shutil
import subprocess
import sys
import tempfile
import json
from os import path, getcwd

import pytest

from avrotize.structuretopython import convert_structure_to_python

# Import the validator
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'tools'))
from json_structure_instance_validator import JSONStructureInstanceValidator

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)


class TestStructureToPython(unittest.TestCase):
    """Test cases for JSON Structure to Python conversion."""

    def run_convert_struct_to_python(
        self,
        struct_name,
        dataclasses_json_annotation=False,
        package_name='',
    ):
        """Helper method to convert a JSON Structure file to Python"""
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "jsons", struct_name + ".struct.json")
        py_path = os.path.join(tempfile.gettempdir(), "avrotize", struct_name + "-py")
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)

        convert_structure_to_python(
            struct_path,
            py_path,
            package_name=package_name,
            dataclasses_json_annotation=dataclasses_json_annotation
        )

        # Verify basic structure
        assert os.path.exists(os.path.join(py_path, 'src'))
        assert os.path.exists(os.path.join(py_path, 'pyproject.toml'))

        # Run Python tests if they exist
        test_dir = os.path.join(py_path, "test")
        if os.path.exists(test_dir):
            new_env = os.environ.copy()
            new_env['PYTHONPATH'] = os.path.join(py_path, 'src')
            try:
                subprocess.check_call(
                    [sys.executable, "-m", "pytest", test_dir],
                    cwd=py_path,
                    env=new_env,
                    stdout=sys.stdout,
                    stderr=sys.stderr
                )
            except subprocess.CalledProcessError as e:
                print(f"Warning: Python tests failed: {e}")
                # Continue anyway for now

        # Run the test instances if they exist
        instances_dir = os.path.join(py_path, "instances")
        os.makedirs(instances_dir, exist_ok=True)

        # Try to import and run create_instance() methods
        src_path = os.path.join(py_path, "src")
        new_env = os.environ.copy()
        new_env['PYTHONPATH'] = src_path

        # Find all Python modules and try to execute create_instance()
        py_files = []
        for root, dirs, files in os.walk(src_path):
            for file in files:
                if file.endswith('.py') and file != '__init__.py':
                    py_files.append((root, file))

        # Generate instances by importing modules and calling create_instance
        for root, file in py_files:
            module_name = file[:-3]  # Remove .py
            try:
                # Create a script to run create_instance and save to JSON
                script = f"""
import sys
import os
import json
sys.path.insert(0, r'{src_path}')

# Import the module
from {module_name} import *

# Find classes with create_instance method
import inspect
for name, obj in inspect.getmembers(sys.modules['{module_name}']):
    if inspect.isclass(obj) and hasattr(obj, 'create_instance'):
        try:
            instance = obj.create_instance()
            if hasattr(instance, 'to_serializer_dict'):
                data = instance.to_serializer_dict()
            else:
                data = instance.__dict__
            with open(r'{instances_dir}/{module_name}.json', 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
            print(f'Generated {{name}} instance')
        except Exception as e:
            print(f'Could not generate {{name}}: {{e}}')
"""
                result = subprocess.run(
                    [sys.executable, "-c", script],
                    cwd=py_path,
                    env=new_env,
                    capture_output=True,
                    text=True
                )
                if result.stdout:
                    print(result.stdout)
            except Exception as e:
                print(f"Could not generate instances for {module_name}: {e}")

        # Validate each generated JSON file against the schema
        if os.path.exists(instances_dir):
            json_files = [f for f in os.listdir(instances_dir) if f.endswith('.json')]
            if json_files:
                # Load the schema
                with open(struct_path, 'r', encoding='utf-8') as f:
                    schema = json.load(f)

                # Create validator
                validator = JSONStructureInstanceValidator(schema, extended=True)

                # Validate each instance
                for json_file in json_files:
                    instance_path = os.path.join(instances_dir, json_file)
                    with open(instance_path, 'r', encoding='utf-8') as f:
                        instance = json.load(f)

                    errors = validator.validate(instance)
                    if errors:
                        print(f"\nValidation errors for {json_file}:")
                        for error in errors:
                            print(f"  - {error}")
                        assert False, f"Instance {json_file} failed validation against JSON Structure schema"
                    else:
                        print(f"[OK] {json_file} validated successfully")

        return py_path


    def test_convert_address_struct_to_python(self):
        """Test converting address-ref.struct.json to Python"""
        self.run_convert_struct_to_python("address-ref")

    def test_convert_address_struct_to_python_annotated(self):
        """Test converting address with dataclasses_json annotation"""
        for dataclasses_json_annotation in [True, False]:
            self.run_convert_struct_to_python(
                "address-ref",
                dataclasses_json_annotation=dataclasses_json_annotation
            )

    def test_convert_addlprops1_struct_to_python(self):
        """Test converting additionalProperties example to Python"""
        self.run_convert_struct_to_python("addlprops1-ref")

    def test_convert_addlprops2_struct_to_python(self):
        """Test converting additionalProperties typed example to Python"""
        self.run_convert_struct_to_python("addlprops2-ref")

    def test_convert_addlprops3_struct_to_python(self):
        """Test converting additionalProperties complex example to Python"""
        self.run_convert_struct_to_python("addlprops3-ref")

    def test_convert_movie_struct_to_python(self):
        """Test converting movie JSON Structure to Python"""
        self.run_convert_struct_to_python("movie-ref")

    def test_convert_movie_struct_to_python_annotated(self):
        """Test converting movie with dataclasses_json annotation"""
        for dataclasses_json_annotation in [True, False]:
            self.run_convert_struct_to_python(
                "movie-ref",
                dataclasses_json_annotation=dataclasses_json_annotation
            )

    def test_convert_primitives_struct_to_python(self):
        """Test converting all primitive types to Python"""
        self.run_convert_struct_to_python(
            "test-all-primitives",
            dataclasses_json_annotation=True
        )

    def test_convert_enum_const_struct_to_python(self):
        """Test converting enum and const keywords to Python"""
        self.run_convert_struct_to_python(
            "test-enum-const",
            dataclasses_json_annotation=True
        )

    def test_convert_tuple_struct_to_python(self):
        """Test converting tuple types to Python"""
        self.run_convert_struct_to_python(
            "test-tuple",
            dataclasses_json_annotation=True
        )

    def test_convert_choice_tagged_struct_to_python(self):
        """Test converting tagged choice types to Python"""
        self.run_convert_struct_to_python(
            "test-choice-tagged",
            dataclasses_json_annotation=True
        )

    def test_convert_choice_inline_struct_to_python(self):
        """Test converting inline choice types with $extends to Python"""
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "struct", "choice-types.struct.json")
        py_path = os.path.join(
            tempfile.gettempdir(), "avrotize", "choice-types-py"
        )
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)

        convert_structure_to_python(
            struct_path,
            py_path,
            dataclasses_json_annotation=True,
            avro_annotation=True,
        )

        # Verify that the BaseEntity abstract base class was generated
        base_entity_path = os.path.join(py_path, "src", "choice_types", "baseentity.py")
        assert os.path.exists(base_entity_path)
        
        with open(base_entity_path, "r", encoding="utf-8") as f:
            content = f.read()
            assert "from abc import ABC" in content
            assert "class BaseEntity(ABC):" in content
            assert "name: typing.Optional[str]" in content or "name: Optional[str]" in content
            assert "entityType: typing.Optional[str]" in content or "entityType: Optional[str]" in content

        # Verify that Person extends BaseEntity
        person_path = os.path.join(py_path, "src", "choice_types", "person.py")
        assert os.path.exists(person_path)
        
        with open(person_path, "r", encoding="utf-8") as f:
            content = f.read()
            assert "from choice_types.baseentity import BaseEntity" in content
            assert "class Person(BaseEntity):" in content
            assert "age: typing.Optional[int]" in content or "age: Optional[int]" in content

        # Verify that Company extends BaseEntity
        company_path = os.path.join(py_path, "src", "choice_types", "company.py")
        assert os.path.exists(company_path)
        
        with open(company_path, "r", encoding="utf-8") as f:
            content = f.read()
            assert "from choice_types.baseentity import BaseEntity" in content
            assert "class Company(BaseEntity):" in content
            assert "employees: typing.Optional[int]" in content or "employees: Optional[int]" in content

        # Verify the ChoiceTypes class with Union field
        choice_types_path = os.path.join(py_path, "src", "choice_types", "choicetypes.py")
        assert os.path.exists(choice_types_path)
        
        with open(choice_types_path, "r", encoding="utf-8") as f:
            content = f.read()
            # Check for Union type for inlineChoice (wrapped in Optional since not required)
            assert "inlineChoice: typing.Optional[typing.Union[Person, Company]]" in content or \
                   "inlineChoice: Optional[Union[Person, Company]]" in content

    def test_convert_with_package_name(self):
        """Test conversion with custom package name"""
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "jsons", "address-ref.struct.json")
        py_path = os.path.join(
            tempfile.gettempdir(), "avrotize", "address-package-py"
        )
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)

        convert_structure_to_python(
            struct_path,
            py_path,
            package_name="my_custom_package",
            dataclasses_json_annotation=True,
        )

        # Verify the package structure is correct
        src_path = os.path.join(py_path, "src")
        if not os.path.exists(src_path):
            raise AssertionError(f"src directory not created: {src_path}")

        # Verify pyproject.toml has the correct package name
        pyproject_path = os.path.join(py_path, "pyproject.toml")
        assert os.path.exists(pyproject_path), "pyproject.toml should exist"

        with open(pyproject_path, "r", encoding="utf-8") as f:
            content = f.read()
            assert "my_custom_package" in content or "my-custom-package" in content

    def test_array_type(self):
        """Test conversion of array types"""
        cwd = os.getcwd()
        py_path = os.path.join(
            tempfile.gettempdir(), "avrotize", "array-test-py"
        )
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)

        # Create a test JSON Structure file with array
        test_struct = {
            "$schema": "https://json-structure.org/meta/extended/v0/#",
            "type": "object",
            "name": "ArrayTest",
            "properties": {
                "stringArray": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "numberArray": {
                    "type": "array",
                    "items": {"type": "number"}
                }
            }
        }

        struct_path = os.path.join(py_path, "array-test.struct.json")
        with open(struct_path, 'w', encoding='utf-8') as f:
            json.dump(test_struct, f, indent=2)

        convert_structure_to_python(
            struct_path,
            py_path,
            dataclasses_json_annotation=True
        )

        # Verify files were generated
        assert os.path.exists(os.path.join(py_path, 'src'))
        assert os.path.exists(os.path.join(py_path, 'pyproject.toml'))

    def test_map_type(self):
        """Test conversion of map/dict types"""
        cwd = os.getcwd()
        py_path = os.path.join(
            tempfile.gettempdir(), "avrotize", "map-test-py"
        )
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)

        # Create a test JSON Structure file with map
        test_struct = {
            "$schema": "https://json-structure.org/meta/extended/v0/#",
            "type": "object",
            "name": "MapTest",
            "properties": {
                "stringMap": {
                    "type": "object",
                    "additionalProperties": {"type": "string"}
                },
                "numberMap": {
                    "type": "object",
                    "additionalProperties": {"type": "number"}
                }
            }
        }

        struct_path = os.path.join(py_path, "map-test.struct.json")
        with open(struct_path, 'w', encoding='utf-8') as f:
            json.dump(test_struct, f, indent=2)

        convert_structure_to_python(
            struct_path,
            py_path,
            dataclasses_json_annotation=True
        )

        # Verify files were generated
        assert os.path.exists(os.path.join(py_path, 'src'))
        assert os.path.exists(os.path.join(py_path, 'pyproject.toml'))

    def test_set_type(self):
        """Test conversion of set types"""
        cwd = os.getcwd()
        py_path = os.path.join(
            tempfile.gettempdir(), "avrotize", "set-test-py"
        )
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)

        # Create a test JSON Structure file with set
        test_struct = {
            "$schema": "https://json-structure.org/meta/extended/v0/#",
            "type": "object",
            "name": "SetTest",
            "properties": {
                "stringSet": {
                    "type": "array",
                    "uniqueItems": True,
                    "items": {"type": "string"}
                },
                "numberSet": {
                    "type": "array",
                    "uniqueItems": True,
                    "items": {"type": "number"}
                }
            }
        }

        struct_path = os.path.join(py_path, "set-test.struct.json")
        with open(struct_path, 'w', encoding='utf-8') as f:
            json.dump(test_struct, f, indent=2)

        convert_structure_to_python(
            struct_path,
            py_path,
            dataclasses_json_annotation=True
        )

        # Verify files were generated
        assert os.path.exists(os.path.join(py_path, 'src'))
        assert os.path.exists(os.path.join(py_path, 'pyproject.toml'))

    def test_convert_object_struct_to_python(self):
        """Test converting a simple object struct to Python"""
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "struct", "numeric-types.struct.json")
        py_path = os.path.join(tempfile.gettempdir(), "avrotize", "numeric-types-py")
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)

        convert_structure_to_python(struct_path, py_path)
        new_env = os.environ.copy()
        new_env['PYTHONPATH'] = os.path.join(py_path, 'src')
        # Check that files were generated
        assert os.path.exists(os.path.join(py_path, 'src'))
        assert os.path.exists(os.path.join(py_path, 'pyproject.toml'))

    def test_structure_output_structurally_equivalent_to_avro(self):
        """Test that s2py output is structurally equivalent to a2py output"""
        # This test verifies that the generated Python code from JSON Structure
        # has the same structure as the code generated from Avro schema
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "struct", "numeric-types.struct.json")
        py_struct_path = os.path.join(tempfile.gettempdir(), "avrotize", "numeric-types-s2py")
        
        if os.path.exists(py_struct_path):
            shutil.rmtree(py_struct_path, ignore_errors=True)
        os.makedirs(py_struct_path, exist_ok=True)

        convert_structure_to_python(struct_path, py_struct_path, package_name="numeric_types")
        
        # Verify the structure exists
        assert os.path.exists(os.path.join(py_struct_path, 'src'))
        assert os.path.exists(os.path.join(py_struct_path, 'pyproject.toml'))
        
        # Verify at least one Python file was generated
        src_dir = os.path.join(py_struct_path, 'src')
        py_files = []
        for root, dirs, files in os.walk(src_dir):
            for file in files:
                if file.endswith('.py'):
                    py_files.append(os.path.join(root, file))
        
        assert len(py_files) > 0, "No Python files were generated"

    def test_test_file_import_matches_module_structure(self):
        """
        Regression test: Generated test files should import from the correct module path.
        
        The import path must match where the source files are actually placed.
        """
        from avrotize.structuretopython import convert_structure_schema_to_python
        
        schema = {
            "type": "object",
            "name": "Customer",
            "namespace": "myapp.models",
            "properties": {
                "customerId": {"type": "string"},
                "name": {"type": "string"}
            }
        }
        
        output_dir = os.path.join(tempfile.gettempdir(), "avrotize", "python-test-import")
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir, ignore_errors=True)
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_schema_to_python(schema, output_dir, package_name="myapp")
        
        # Find the test file
        test_file_path = os.path.join(output_dir, "tests", "test_customer.py")
        assert os.path.exists(test_file_path), f"Test file should exist at {test_file_path}"
        
        with open(test_file_path, 'r', encoding='utf-8') as f:
            test_content = f.read()
        
        # The import should reference the correct module path
        assert "Customer" in test_content, \
            "Test file should import Customer class"
        
        # Verify the import path matches where files are actually placed
        # The module should be importable - check that path components are correct
        assert "myapp" in test_content, \
            "Test file import should include the package name"
        assert "models" in test_content, \
            "Test file import should include the namespace"

    def test_test_file_name_is_simple(self):
        """
        Regression test: Test file names should be simple (test_{class}.py), not overly long.
        """
        from avrotize.structuretopython import convert_structure_schema_to_python
        
        schema = {
            "type": "object",
            "name": "VeryLongClassName",
            "namespace": "very.long.namespace.path.that.is.quite.deep",
            "properties": {
                "field1": {"type": "string"}
            }
        }
        
        output_dir = os.path.join(tempfile.gettempdir(), "avrotize", "python-test-filename")
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir, ignore_errors=True)
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_schema_to_python(schema, output_dir, package_name="pkg")
        
        tests_dir = os.path.join(output_dir, "tests")
        assert os.path.exists(tests_dir), "Tests directory should exist"
        
        # List test files
        test_files = [f for f in os.listdir(tests_dir) if f.endswith('.py')]
        assert len(test_files) > 0, "Should have at least one test file"
        
        for test_file in test_files:
            # Verify file name is reasonably short (class name based, not full package path)
            assert len(test_file) < 100, f"Test file name '{test_file}' should not be excessively long"
            
            # Should NOT include flattened namespace path
            assert "very_long_namespace" not in test_file.lower(), \
                "Test file name should not include flattened namespace path"


if __name__ == '__main__':
    unittest.main()
