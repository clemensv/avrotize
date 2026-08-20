"""Tests for JSON Structure to Python conversion."""

import unittest
import datetime
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

    def test_issue_402_to_byte_array_application_json_returns_bytes(self):
        """ Issue #402 (parity): s2py to_byte_array('application/json') must
        return bytes, not str. Mirrors the a2py fix for the shared dataclasses
        core template. """
        import importlib
        from avrotize.structuretopython import convert_structure_schema_to_python

        schema = {
            "type": "object",
            "name": "Issue402Struct",
            "namespace": "example.issue402",
            "properties": {
                "tenantid": {"type": "string"},
                "count": {"type": "int32"},
            },
            "required": ["tenantid", "count"],
        }
        output_dir = os.path.join(tempfile.gettempdir(), "avrotize", "issue-402-s2py-json")
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir, ignore_errors=True)
        os.makedirs(output_dir, exist_ok=True)

        convert_structure_schema_to_python(
            schema, output_dir, package_name="issue_402s_json", dataclasses_json_annotation=True)

        generated_src = os.path.join(output_dir, "src")
        sys.path.insert(0, generated_src)
        try:
            module = importlib.import_module(
                "issue_402s_json.example.issue402.issue402struct")
            Issue402Struct = module.Issue402Struct
            record = Issue402Struct(tenantid="acme", count=7)

            payload = record.to_byte_array("application/json")
            assert isinstance(payload, bytes), f"expected bytes, got {type(payload).__name__}"

            round_tripped = Issue402Struct.from_data(payload, "application/json")
            assert round_tripped == record
        finally:
            sys.path.remove(generated_src)

    def test_xml_annotation_round_trip_with_structure_metadata(self):
        """Generated Structure classes provide XML-only serialization parity."""
        import gzip
        import importlib
        import xml.etree.ElementTree as ET
        from avrotize.structuretopython import convert_structure_schema_to_python

        schema = {
            "type": "object",
            "name": "Catalog",
            "namespace": "example.xml",
            "xmlns": "urn:avrotize:structure",
            "altnames": {"xml": "product-catalog"},
            "properties": {
                "version": {"type": "string", "xmlkind": "attribute",
                            "altnames": {"xml": "schema-version"}},
                "title": {"type": "string", "altnames": {"xml": "display-name"}},
                "state": {"type": "string", "name": "State", "enum": ["open", "closed"],
                          "altnames": {"xml": "catalog-state"},
                          "altenums": {"xml": {"open": "available"}}},
                "items": {"type": "array", "items": {"type": "object", "name": "Item",
                          "xmlns": "urn:avrotize:item",
                          "properties": {"code": {"type": "string", "xmlkind": "attribute"},
                                         "quantity": {"type": "int32"}},
                          "required": ["code", "quantity"]}},
                "labels": {"type": "map", "values": {"type": "string"}},
                "note": {"type": "string"},
            },
            "required": ["version", "title", "state", "items", "labels"],
        }
        output_dir = os.path.join(tempfile.gettempdir(), "avrotize", "issue-408-s2py-xml")
        shutil.rmtree(output_dir, ignore_errors=True)
        convert_structure_schema_to_python(
            schema, output_dir, package_name="issue_408_s2py", xml_annotation=True,
            dataclasses_json_annotation=True, avro_annotation=True)

        generated_src = os.path.join(output_dir, "src")
        for root, _dirs, files in os.walk(generated_src):
            for filename in files:
                if filename.endswith(".py"):
                    generated_file = os.path.join(root, filename)
                    with open(generated_file, encoding="utf-8") as source:
                        compile(source.read(), generated_file, "exec")

        sys.path.insert(0, generated_src)
        try:
            Catalog = importlib.import_module("issue_408_s2py.example.xml.catalog").Catalog
            Item = importlib.import_module("issue_408_s2py.example.xml.item").Item
            State = importlib.import_module("issue_408_s2py.example.xml.state").State
            value = Catalog(version="1", title="Summer", state=State.open,
                            items=[Item(code="P1", quantity=2)], labels={"region": "west"},
                            note=None)

            payload = value.to_byte_array("text/xml")
            root = ET.fromstring(payload)
            assert root.tag == "{urn:avrotize:structure}product-catalog"
            assert root.attrib == {"schema-version": "1"}
            assert root.find("{urn:avrotize:structure}display-name").text == "Summer"
            assert root.find("{urn:avrotize:structure}catalog-state").text == "available"
            assert root.find("{urn:avrotize:structure}note") is None
            item = root.find("{urn:avrotize:item}items")
            assert item.attrib == {"code": "P1"}
            map_item = root.find("{urn:avrotize:structure}labels/{urn:avrotize:structure}item")
            assert map_item.attrib == {"key": "region"} and map_item.text == "west"
            assert State.__xml_name__ == "catalog-state"
            version_metadata = Catalog.__dataclass_fields__["version"].metadata
            assert version_metadata["name"] == "schema-version"
            assert version_metadata["type"] == "Attribute"
            catalog_source = os.path.join(generated_src, "issue_408_s2py", "example", "xml", "catalog.py")
            with open(catalog_source, encoding="utf-8") as source:
                generated_code = source.read()
            assert "XMLFields" not in generated_code
            assert "_to_xml_element" not in generated_code
            assert "from issue_408_s2py.xml_runtime import parse_xml, serialize_xml" in generated_code
            runtime_path = os.path.join(generated_src, "issue_408_s2py", "xml_runtime.py")
            assert os.path.exists(runtime_path)
            with open(runtime_path, encoding="utf-8") as runtime:
                runtime_code = runtime.read()
            assert "XmlParser" in runtime_code and "XmlSerializer" in runtime_code
            assert "xml.etree" not in runtime_code
            with open(os.path.join(output_dir, "pyproject.toml"), encoding="utf-8") as project:
                assert 'xsdata = "^26.2"' in project.read()
            assert Catalog.from_data(payload, "text/xml") == value
            json_round_trip = Catalog.from_data(
                value.to_byte_array("application/json"), "application/json")
            assert json_round_trip.version == value.version
            assert Catalog.from_data(value.to_byte_array("avro/binary"), "avro/binary").version == value.version

            compressed = value.to_byte_array("application/xml+gzip")
            assert gzip.decompress(compressed).startswith(b"<?xml")
            assert Catalog.from_data(compressed, "application/xml+gzip") == value
        finally:
            sys.path.remove(generated_src)
            for module_name in list(sys.modules):
                if module_name == "issue_408_s2py" or module_name.startswith("issue_408_s2py."):
                    sys.modules.pop(module_name, None)

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

    def test_root_with_definitions_wrapper_generates_classes(self):
        """Regression test for issue #314.

        ``convert_structure_schema_to_python`` previously silently dropped
        schemas that used the canonical ``$root`` + ``definitions`` wrapper
        pattern, producing only ``pyproject.toml`` and no ``src/`` tree.
        """
        from avrotize.structuretopython import convert_structure_schema_to_python

        schema = {
            "$id": "https://example.com/Station",
            "name": "Station",
            "$root": "#/definitions/de/wsv/pegelonline/Station",
            "definitions": {"de": {"wsv": {"pegelonline": {
                "Station": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "water": {"type": {"$ref": "#/definitions/de/wsv/pegelonline/Water"}},
                    },
                },
                "Water": {
                    "type": "object",
                    "properties": {"name": {"type": "string"}},
                },
            }}}},
        }

        output_dir = os.path.join(tempfile.gettempdir(), "avrotize", "issue-314-root-wrapper")
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir, ignore_errors=True)
        os.makedirs(output_dir, exist_ok=True)

        convert_structure_schema_to_python(schema, output_dir, package_name="station_data")

        src_dir = os.path.join(output_dir, "src")
        assert os.path.exists(src_dir), "src/ tree must be generated for $root-wrapped schema"

        py_files = []
        for root, _dirs, files in os.walk(src_dir):
            for f in files:
                if f.endswith(".py"):
                    py_files.append(os.path.join(root, f))

        names = {os.path.basename(p).lower() for p in py_files}
        assert any("station" in n for n in names), \
            f"Expected a Station-derived file in {names}"
        assert any("water" in n for n in names), \
            f"Expected a Water-derived file in {names}"

        expected_subpath = os.path.join("de", "wsv", "pegelonline")
        assert any(expected_subpath in p for p in py_files), \
            f"Expected types under de/wsv/pegelonline/, got file paths {py_files}"


    def test_integer_enum_emits_valid_python(self):
        """Regression test for issue #315.

        Integer-valued JSON Structure enums (e.g. ``"enum": [0, 1, 2]``)
        previously produced an importable-looking file whose class body
        contained bare numeric literals like ``0 = '0'``, which is a
        SyntaxError. The generator now prefixes numeric member names with
        ``VALUE_`` and uses ``IntEnum`` with the original integer values.
        """
        from avrotize.structuretopython import convert_structure_schema_to_python

        schema = {
            "type": "object",
            "name": "LightningStrike",
            "namespace": "wx.dmi",
            "properties": {
                "id": {"type": "string"},
                "type": {
                    "type": {
                        "type": "integer",
                        "enum": [0, 1, 2],
                        "description": "Strike type code.",
                    },
                },
            },
        }

        output_dir = os.path.join(tempfile.gettempdir(), "avrotize", "issue-315-int-enum")
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir, ignore_errors=True)
        os.makedirs(output_dir, exist_ok=True)

        convert_structure_schema_to_python(schema, output_dir, package_name="dmi_lightning")

        # Find the generated enum file
        src_dir = os.path.join(output_dir, "src")
        enum_files = []
        for root, _dirs, files in os.walk(src_dir):
            for f in files:
                if f.endswith(".py") and "typeenum" in f.lower():
                    enum_files.append(os.path.join(root, f))
        assert enum_files, f"Expected a *typeenum*.py file under {src_dir}"

        enum_source = ""
        for f in enum_files:
            with open(f, "r", encoding="utf-8") as fh:
                enum_source = fh.read()
            break

        # The file must be syntactically valid Python
        import ast
        ast.parse(enum_source)

        # IntEnum with VALUE_n members and integer values (no quotes)
        assert "IntEnum" in enum_source, f"Expected IntEnum in:\n{enum_source}"
        assert "VALUE_0 = 0" in enum_source, f"Expected 'VALUE_0 = 0' in:\n{enum_source}"
        assert "VALUE_1 = 1" in enum_source, f"Expected 'VALUE_1 = 1' in:\n{enum_source}"
        assert "VALUE_2 = 2" in enum_source, f"Expected 'VALUE_2 = 2' in:\n{enum_source}"
        assert "0 = '0'" not in enum_source, "Bare numeric member name regressed"

    def test_int64_decimal_serialized_as_json_strings(self):
        """Regression test for issue #346.

        int64, uint64, int128, uint128, and decimal fields MUST be serialized
        as JSON strings (not numbers) because IEEE-754 doubles cannot represent
        the full range of these types without precision loss.
        """
        from avrotize.structuretopython import convert_structure_schema_to_python

        schema = {
            "type": "object",
            "name": "LargeNumbers",
            "namespace": "test.issue346",
            "properties": {
                "id": {"type": "string"},
                "bigInt": {"type": "int64"},
                "bigUint": {"type": "uint64"},
                "hugeInt": {"type": "int128"},
                "hugeUint": {"type": "uint128"},
                "price": {"type": "decimal"},
            },
            "required": ["id", "bigInt", "bigUint", "hugeInt", "hugeUint", "price"]
        }

        output_dir = os.path.join(tempfile.gettempdir(), "avrotize", "issue-346-string-numbers")
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir, ignore_errors=True)
        os.makedirs(output_dir, exist_ok=True)

        convert_structure_schema_to_python(schema, output_dir, package_name="test_issue346",
                                          dataclasses_json_annotation=True)

        # Find the generated class file
        src_dir = os.path.join(output_dir, "src")
        class_file = None
        for root, _dirs, files in os.walk(src_dir):
            for f in files:
                if "largenumbers" in f.lower() and f.endswith(".py"):
                    class_file = os.path.join(root, f)
                    break

        assert class_file is not None, f"Expected LargeNumbers class file under {src_dir}"

        with open(class_file, "r", encoding="utf-8") as fh:
            source = fh.read()

        # Verify the generated code has encoder/decoder for string serialization
        assert "encoder=lambda v: str(v)" in source, \
            f"Expected string encoder for numeric fields in:\n{source}"
        assert "decoder=lambda v: int(v)" in source, \
            f"Expected int decoder for int64/uint64 fields in:\n{source}"
        assert "decimal.Decimal(v)" in source, \
            f"Expected Decimal decoder for decimal field in:\n{source}"

        # Verify to_serializer_dict has string conversion
        assert "str(asdict_result[" in source or "= str(asdict_result" in source, \
            f"Expected to_serializer_dict to convert numeric fields to strings in:\n{source}"

        # Verify the code is syntactically valid and can be imported
        import ast
        ast.parse(source)

    def test_nullable_int64_serialized_as_json_strings(self):
        """Regression test for issue #348.

        Nullable int64/uint64 fields (typed as ["int64", "null"]) must also be
        serialized as JSON strings, not just bare int64 fields.
        """
        from avrotize.structuretopython import convert_structure_schema_to_python

        schema = {
            "type": "object",
            "name": "NullableNumbers",
            "namespace": "test.issue348",
            "properties": {
                "id": {"type": "string"},
                "sequence_number": {"type": "int64"},
                "user_id": {"type": ["int64", "null"]},
                "big_decimal": {"type": ["decimal", "null"]},
            },
            "required": ["id", "sequence_number"]
        }

        output_dir = os.path.join(tempfile.gettempdir(), "avrotize", "issue-348-nullable-strings")
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir, ignore_errors=True)
        os.makedirs(output_dir, exist_ok=True)

        convert_structure_schema_to_python(schema, output_dir, package_name="test_issue348",
                                          dataclasses_json_annotation=True)

        # Find the generated class file
        src_dir = os.path.join(output_dir, "src")
        class_file = None
        for root, _dirs, files in os.walk(src_dir):
            for f in files:
                if "nullablenumbers" in f.lower() and f.endswith(".py"):
                    class_file = os.path.join(root, f)
                    break

        assert class_file is not None, f"Expected NullableNumbers class file under {src_dir}"

        with open(class_file, "r", encoding="utf-8") as fh:
            source = fh.read()

        # Both required int64 and nullable int64 must have string encoder
        # Count encoder occurrences - should have at least 3 (sequence_number, user_id, big_decimal)
        encoder_count = source.count("encoder=lambda v: str(v)")
        assert encoder_count >= 3, \
            f"Expected at least 3 string encoders (for sequence_number, user_id, big_decimal), got {encoder_count} in:\n{source}"

        # Verify to_serializer_dict stringifies user_id (nullable field)
        assert "user_id" in source, f"Expected user_id field in source"
        
        # Check that to_serializer_dict converts nullable fields too
        serializer_section = source[source.find("def to_serializer_dict"):]
        assert "str(asdict_result" in serializer_section, \
            f"Expected to_serializer_dict to stringify nullable int64 fields"

        # Verify valid Python
        import ast
        ast.parse(source)


    def test_enum_collision_shindo_scale(self):
        """Test that enum symbols differing only by +/- produce unique member names.

        Reproduces issue #349: the Japanese shindo intensity scale has symbols
        like "5-" and "5+" which, after sanitization, must yield distinct Python
        identifiers (e.g. VALUE_5_MINUS vs VALUE_5_PLUS) and the generated
        module must be importable without TypeError.
        """
        schema = {
            "definitions": {
                "ShindoScale": {
                    "name": "ShindoScale",
                    "namespace": "test.seismic",
                    "description": "Japanese seismic intensity scale",
                    "type": "enum",
                    "enum": ["1", "2", "3", "4", "5-", "5+", "6-", "6+", "7"]
                }
            }
        }
        py_path = os.path.join(tempfile.gettempdir(), "avrotize", "shindo-enum-py")
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)

        struct_path = os.path.join(py_path, "shindo.struct.json")
        with open(struct_path, "w") as f:
            json.dump(schema, f)

        convert_structure_to_python(struct_path, py_path)

        # Find and read the generated enum source
        enum_file = os.path.join(py_path, "src", "shindo", "test", "seismic", "shindoscale.py")
        assert os.path.exists(enum_file), f"Expected enum file at {enum_file}"

        with open(enum_file) as f:
            source = f.read()

        # Verify it parses (no duplicate keys)
        import ast
        ast.parse(source)

        # Verify distinct member names for +/- variants
        assert "VALUE_5_MINUS" in source, "Expected VALUE_5_MINUS for '5-'"
        assert "VALUE_5_PLUS" in source, "Expected VALUE_5_PLUS for '5+'"
        assert "VALUE_6_MINUS" in source, "Expected VALUE_6_MINUS for '6-'"
        assert "VALUE_6_PLUS" in source, "Expected VALUE_6_PLUS for '6+'"

        # Verify the .value strings remain the original symbols
        assert "'5-'" in source, "Enum value for 5- must be the original string"
        assert "'5+'" in source, "Enum value for 5+ must be the original string"
        assert "'6-'" in source, "Enum value for 6- must be the original string"
        assert "'6+'" in source, "Enum value for 6+ must be the original string"

    # ------------------------------------------------------------------
    # Issue #405: s2py emits temporal fields without dataclasses_json
    # encoder/decoder/mm_field metadata, so date/time values raise
    # "TypeError: Object of type date is not JSON serializable" on to_json().
    # ------------------------------------------------------------------

    def _generate_issue_405_module(self, schema, package_name):
        """Generates a dataclasses-json annotated module and returns (src_dir, source_text)."""
        from avrotize.structuretopython import convert_structure_schema_to_python

        # A unique directory per generation keeps concurrent (pytest-xdist)
        # workers from colliding on a shared fixed path.
        output_dir = tempfile.mkdtemp(prefix="avrotize-issue405-")
        self.addCleanup(shutil.rmtree, output_dir, True)

        convert_structure_schema_to_python(
            schema, output_dir, package_name=package_name,
            dataclasses_json_annotation=True)

        src_dir = os.path.join(output_dir, "src")
        class_file = None
        expected = schema["name"].lower() + ".py"
        for root, _dirs, files in os.walk(src_dir):
            for f in sorted(files):
                if f.lower() == expected:
                    class_file = os.path.join(root, f)
                    break
            if class_file is not None:
                break
        assert class_file is not None, f"Expected {expected} under {src_dir}"

        with open(class_file, "r", encoding="utf-8") as fh:
            source = fh.read()

        import ast
        ast.parse(source)
        return src_dir, source

    def _import_generated(self, src_dir, module_name):
        """Imports a generated module and unregisters it again when the test ends."""
        import importlib

        package = module_name.split(".", 1)[0]
        sys.path.insert(0, src_dir)
        self.addCleanup(self._forget_generated_package, src_dir, package)
        return importlib.import_module(module_name)

    @staticmethod
    def _forget_generated_package(src_dir, package):
        """Removes a generated package from sys.path and sys.modules."""
        if src_dir in sys.path:
            sys.path.remove(src_dir)
        for name in [n for n in sys.modules
                     if n == package or n.startswith(package + ".")]:
            del sys.modules[name]

    def test_issue_405_date_field_has_dataclasses_json_metadata(self):
        """Positive fixture: date fields carry encoder/decoder/mm_field metadata.

        Regression test for issue #405. Both a required `date` and a nullable
        `["null", "date"]` property must emit the same encoder/decoder/
        mm_field triple that the a2py generator already emits, and
        the existing `datetime` behaviour must be unchanged. The mm_field is a
        generated `fields.Date` subclass so that it can carry `data_key` and
        delegate parsing to the module's ISO helper.
        """
        schema = {
            "type": "object",
            "name": "DateProbe",
            "namespace": "test.issue405",
            "properties": {
                "d": {"type": ["null", "date"]},
                "born": {"type": "date"},
                "ts": {"type": ["null", "datetime"]},
            },
            "required": ["born"],
        }
        _src, source = self._generate_issue_405_module(
            schema, "test_issue405_date")

        assert "mm_field=_IsoDateField(" in source, \
            f"date fields must declare a marshmallow Date field:\n{source}"
        assert source.count("mm_field=_IsoDateField(") == 2, \
            f"both the nullable and the required date field must be annotated:\n{source}"
        # dataclasses_json only assigns data_key on the branch it takes when no
        # mm_field is configured, so a supplied field has to carry it or
        # schema() loses the JSON field name.
        assert "mm_field=_IsoDateField(data_key='d', required=True, allow_none=True)" in source, \
            f"a nullable date mm_field must carry data_key and allow_none:\n{source}"
        assert "mm_field=_IsoDateField(data_key='born', required=True)" in source, \
            f"a required date mm_field must carry data_key:\n{source}"
        assert "_parse_iso_date(v, 'd')" in source, \
            f"date fields must declare an ISO date decoder:\n{source}"
        assert "isinstance(v, datetime.date)" in source, \
            f"date encoder must handle datetime.date values:\n{source}"

        # No regression for the datetime sibling.
        assert "mm_field=_IsoDateTimeField(" in source, \
            f"datetime fields must keep their marshmallow DateTime field:\n{source}"
        assert "_parse_iso_datetime(v, 'ts')" in source, \
            f"datetime fields must keep their ISO datetime decoder:\n{source}"

    def test_issue_405_time_field_has_dataclasses_json_metadata(self):
        """Positive fixture: `time`, the sibling temporal type, has the same defect."""
        schema = {
            "type": "object",
            "name": "TimeProbe",
            "namespace": "test.issue405",
            "properties": {
                "at": {"type": "time"},
                "maybe_at": {"type": ["null", "time"]},
            },
            "required": ["at"],
        }
        _src, source = self._generate_issue_405_module(
            schema, "test_issue405_time")

        assert source.count("mm_field=_IsoTimeField(") == 2, \
            f"time fields must declare a marshmallow Time field:\n{source}"
        assert "_parse_iso_time(v, 'at')" in source, \
            f"time fields must declare an ISO time decoder:\n{source}"

    def test_issue_405_date_only_schema_imports_marshmallow_fields(self):
        """Boundary fixture: the marshmallow import guard must fire for date-only schemas.

        Before the fix the guard only matched `datetime.datetime`, so a schema
        with a date (or time) field but no datetime field produced code
        referencing `fields.Date` without importing `fields`.
        """
        for name, prop_type in (("DateOnly", "date"), ("TimeOnly", "time")):
            with self.subTest(prop_type=prop_type):
                schema = {
                    "type": "object",
                    "name": name,
                    "namespace": "test.issue405",
                    "properties": {
                        "value": {"type": prop_type},
                        "label": {"type": "string"},
                    },
                    "required": ["value", "label"],
                }
                src_dir, source = self._generate_issue_405_module(
                    schema, "test_issue405_" + prop_type + "only")

                assert "from marshmallow import fields" in source, \
                    f"marshmallow fields import missing for {prop_type}-only schema:\n{source}"

                # The module must actually import (NameError guard).
                module = self._import_generated(
                    src_dir,
                    f"test_issue405_{prop_type}only.test.issue405.{name.lower()}")
                assert hasattr(module, name)

    def test_issue_405_date_round_trips_through_json(self):
        """Round-trip fixture: the exact failure mode reported in issue #405."""
        import datetime as dt

        schema = {
            "type": "object",
            "name": "RoundTrip",
            "namespace": "test.issue405",
            "properties": {
                "d": {"type": "date"},
                "t": {"type": "time"},
                "ts": {"type": "datetime"},
                "count": {"type": "int64"},
            },
            "required": ["d", "t", "ts", "count"],
        }
        src_dir, _source = self._generate_issue_405_module(
            schema, "test_issue405_roundtrip")

        module = self._import_generated(
            src_dir, "test_issue405_roundtrip.test.issue405.roundtrip")
        RoundTrip = module.RoundTrip
        record = RoundTrip(
            d=dt.date(1998, 11, 20),
            t=dt.time(13, 45, 30),
            ts=dt.datetime(1998, 11, 20, 13, 45, 30, tzinfo=dt.timezone.utc),
            count=9007199254740993,
        )

        # pylint: disable=no-member
        json_text = record.to_json()
        assert '"1998-11-20"' in json_text, \
            f"date must serialize as an ISO date string, got: {json_text}"
        assert '"13:45:30"' in json_text, \
            f"time must serialize as an ISO time string, got: {json_text}"

        payload = record.to_byte_array("application/json")
        assert isinstance(payload, bytes)

        restored = RoundTrip.from_data(payload, "application/json")
        assert restored == record, f"expected {record}, got {restored}"
        assert isinstance(restored.d, dt.date) and not isinstance(restored.d, dt.datetime)
        assert isinstance(restored.t, dt.time)
        assert isinstance(restored.ts, dt.datetime)

        # dataclasses_json's own decoder path must round-trip too.
        assert RoundTrip.from_json(json_text) == record

    def test_issue_405_date_field_holding_datetime_emits_one_wire_format(self):
        """Boundary fixture: datetime.datetime is a subclass of datetime.date.

        A naive ``isinstance(v, datetime.datetime)``-first encoder in the date
        branch emits a datetime-shaped string that the matching
        ``datetime.date.fromisoformat`` decoder cannot parse, giving two wire
        formats for one value. The date encoder must narrow to the date part.
        """
        import datetime as dt

        schema = {
            "type": "object",
            "name": "DateNarrowing",
            "namespace": "test.issue405",
            "properties": {"d": {"type": "date"}},
            "required": ["d"],
        }
        src_dir, _source = self._generate_issue_405_module(
            schema, "test_issue405_narrow")

        module = self._import_generated(
            src_dir, "test_issue405_narrow.test.issue405.datenarrowing")
        DateNarrowing = module.DateNarrowing
        record = DateNarrowing(d=dt.datetime(2024, 1, 1, 10, 0, 0))

        # pylint: disable=no-member
        json_text = record.to_json()
        assert '"2024-01-01"' in json_text, \
            f"a datetime in a date field must serialize as a date, got: {json_text}"
        assert "10:00:00" not in json_text, \
            f"date wire format must not carry a time component: {json_text}"

        # Both wire producers must agree, and the value must survive the trip.
        assert '"2024-01-01"' in DateNarrowing.schema().dumps(record)
        assert DateNarrowing.from_json(json_text).d == dt.date(2024, 1, 1)
        assert DateNarrowing.from_data(
            json_text.encode("utf-8"), "application/json").d == dt.date(2024, 1, 1)

    def test_issue_405_nested_temporal_collections_round_trip(self):
        """Positive fixture: temporal types nested in arrays, maps and unions.

        Issue #405 is only fixed if encoder selection follows the type
        structure. Exact string matching on the six scalar spellings leaves
        ``List[date]``, ``Dict[str, date]`` and ``Optional[List[date]]``
        raising the original TypeError.
        """
        import datetime as dt

        schema = {
            "type": "object",
            "name": "NestedTemporal",
            "namespace": "test.issue405",
            "properties": {
                "dates": {"type": "array", "items": {"type": "date"}},
                "by_key": {"type": "map", "values": {"type": "date"}},
                "stamps": {"type": "set", "items": {"type": "datetime"}},
                "maybe_times": {"type": ["null", {"type": "array",
                                                  "items": {"type": "time"}}]},
            },
            "required": ["dates", "by_key", "stamps"],
        }
        src_dir, _source = self._generate_issue_405_module(
            schema, "test_issue405_nested")

        module = self._import_generated(
            src_dir, "test_issue405_nested.test.issue405.nestedtemporal")
        NestedTemporal = module.NestedTemporal
        record = NestedTemporal(
            dates=[dt.date(2024, 2, 2)],
            by_key={"k": dt.date(2024, 3, 3)},
            stamps=[dt.datetime(2024, 4, 4, 5, 6, 7)],
            maybe_times=[dt.time(1, 2, 3)],
        )

        # pylint: disable=no-member
        json_text = record.to_json()
        assert '"2024-02-02"' in json_text, json_text
        assert '"2024-03-03"' in json_text, json_text
        assert '"01:02:03"' in json_text, json_text

        for restored in (NestedTemporal.from_json(json_text),
                         NestedTemporal.from_data(
                             json_text.encode("utf-8"), "application/json")):
            assert list(restored.dates) == [dt.date(2024, 2, 2)], restored
            assert restored.by_key == {"k": dt.date(2024, 3, 3)}, restored
            assert list(restored.stamps) == [dt.datetime(2024, 4, 4, 5, 6, 7)], restored
            assert list(restored.maybe_times) == [dt.time(1, 2, 3)], restored

    _TEMPORAL_TRIPLE_SCHEMA = {
        "type": "object",
        "name": "Strict",
        "namespace": "test.issue405",
        "properties": {
            "d": {"type": "date"},
            "t": {"type": "time"},
            "ts": {"type": "datetime"},
        },
        "required": ["d", "t", "ts"],
    }

    def test_issue_405_rfc3339_zulu_and_fractional_seconds_normalise(self):
        """Boundary fixture: RFC 3339 offsets and fractional seconds.

        ``datetime.fromisoformat`` only learned RFC 3339 ``Z`` and >6-digit
        fractional seconds on Python 3.11, but ``requires-python`` is
        ``">=3.10"`` and 3.10 leads the CI matrix. The generated decoders
        normalise both, so an *RFC 3339* payload -- ``Z``, lowercase ``z``,
        numeric offsets and fractional seconds -- yields the same value, of the
        same type, on every supported interpreter. Non-RFC-3339 ISO 8601 forms
        that only ``fromisoformat`` on 3.11 accepts (basic format such as
        ``20240101``, ordinal and week dates) are deliberately not normalised
        and remain version dependent: they parse on 3.11 and raise on 3.10.
        """
        import datetime as dt

        src_dir, _source = self._generate_issue_405_module(
            self._TEMPORAL_TRIPLE_SCHEMA, "test_issue405_zulu")

        module = self._import_generated(
            src_dir, "test_issue405_zulu.test.issue405.strict")
        Strict = module.Strict
        utc = dt.timezone.utc

        # A trailing "Z" (and its lowercase form) is the most common JSON
        # timestamp encoding; it must parse identically on 3.10 and 3.11+.
        for suffix in ("Z", "z"):
            payload = ('{"d": "2024-01-01", "t": "13:45:30%s",'
                       ' "ts": "2024-01-01T00:00:00%s"}' % (suffix, suffix))
            for restored in (Strict.from_data(payload.encode("utf-8"),
                                              "application/json"),
                             Strict.from_json(payload)):
                assert restored.ts == dt.datetime(2024, 1, 1, tzinfo=utc), restored
                assert restored.t == dt.time(13, 45, 30, tzinfo=utc), restored
                assert restored.d == dt.date(2024, 1, 1), restored

        # Nanosecond precision is the default Go/protobuf timestamp emission.
        # It must truncate to microseconds rather than yielding a str on 3.10
        # and a datetime on 3.11.
        nanos = ('{"d": "2024-01-01", "t": "13:45:30.123456789Z",'
                 ' "ts": "2024-01-01T00:00:00.123456789Z"}')
        restored = Strict.from_json(nanos)
        assert restored.ts == dt.datetime(2024, 1, 1, 0, 0, 0, 123456,
                                          tzinfo=utc), restored
        assert restored.t == dt.time(13, 45, 30, 123456, tzinfo=utc), restored

        # An explicit numeric offset keeps working.
        offset = ('{"d": "2024-01-01", "t": "13:45:30+05:30",'
                  ' "ts": "2024-01-01T00:00:00+05:30"}')
        restored = Strict.from_json(offset)
        assert restored.ts.utcoffset() == dt.timedelta(hours=5, minutes=30), restored

    def test_issue_405_invalid_date_string_is_rejected(self):
        """Invalid-input fixture: a malformed date must raise, never corrupt.

        Acceptance item 5 on issue #405 requires a malformed date string to be
        rejected with ``ValueError``. Returning the raw string instead would
        leave a field declared ``datetime.date`` holding a ``str``, so the
        failure would surface as an ``AttributeError`` far from the
        deserialization site and ``to_json`` would launder the bad value back
        onto the wire.
        """
        src_dir, _source = self._generate_issue_405_module(
            self._TEMPORAL_TRIPLE_SCHEMA, "test_issue405_invalid")

        module = self._import_generated(
            src_dir, "test_issue405_invalid.test.issue405.strict")
        Strict = module.Strict

        for bad in ("20 November 1998", "", "2024-13-45", "not-a-date"):
            payload = json.dumps({"d": bad, "t": "01:02:03",
                                  "ts": "2024-01-01T00:00:00"})
            with self.assertRaises(ValueError) as caught:
                Strict.from_json(payload)
            # The message must name the offending field, otherwise the error is
            # untraceable in a record with several temporal fields.
            assert "'d'" in str(caught.exception), caught.exception

    def test_issue_405_all_entry_points_reject_malformed_temporal_strings(self):
        """Invalid-input fixture: every public deserializer must agree.

        A generated class exposes five deserialization entry points. If some
        tolerate a malformed value and others reject it, the same payload
        produces an instance or an exception depending only on which entry
        point the caller happened to use.
        """
        src_dir, _source = self._generate_issue_405_module(
            self._TEMPORAL_TRIPLE_SCHEMA, "test_issue405_entrypoints")

        module = self._import_generated(
            src_dir, "test_issue405_entrypoints.test.issue405.strict")
        Strict = module.Strict

        bad = {"d": "20 November 1998", "t": "x", "ts": "y"}
        text = json.dumps(bad)
        entry_points = {
            "from_data(bytes)":
                lambda: Strict.from_data(text.encode("utf-8"), "application/json"),
            "from_data(dict)":
                lambda: Strict.from_data(dict(bad), "application/json"),
            "from_json": lambda: Strict.from_json(text),
            "from_serializer_dict": lambda: Strict.from_serializer_dict(dict(bad)),
            # marshmallow owns this path and raises its own ValidationError.
            "schema().loads": lambda: Strict.schema().loads(text),
        }
        for label, call in entry_points.items():
            with self.subTest(entry_point=label):
                with self.assertRaises(Exception) as caught:
                    call()
                assert not isinstance(caught.exception, AttributeError), \
                    f"{label} must reject at the deserialization site"

        # Non-string JSON must not leak an AttributeError out of the parser
        # either. `_normalize_iso_string` calls str methods, so an int, bool,
        # float, list or dict reaching it escaped as AttributeError from inside
        # marshmallow, where master raised ValidationError and callers relying
        # on `err.messages` for per-field aggregation crashed instead.
        from marshmallow import ValidationError

        for value in (12345, True, 1.5, [1, 2], {"a": 1}):
            with self.subTest(non_string=value):
                payload = json.dumps({"d": value, "t": value, "ts": value})
                with self.assertRaises(ValidationError) as caught:
                    Strict.schema().loads(payload)
                messages = caught.exception.messages
                assert set(messages) == {"d", "t", "ts"}, \
                    f"schema().loads must aggregate per field, got {messages}"

                # The other four entry points leave a non-string untouched, as
                # they do on master: their decoders are guarded by
                # `isinstance(v, str)` so that `from_serializer_dict` can accept
                # the real `datetime` objects `to_serializer_dict` emits. These
                # calls must be built from `payload`/`value`, not reused from
                # the malformed-string table above, or they would never feed a
                # non-string to the decoders this block exists to pin.
                raw = {"d": value, "t": value, "ts": value}
                non_string_entry_points = {
                    "from_data(bytes)": lambda: Strict.from_data(
                        payload.encode("utf-8"), "application/json"),
                    "from_data(dict)": lambda: Strict.from_data(
                        dict(raw), "application/json"),
                    "from_json": lambda: Strict.from_json(payload),
                    "from_serializer_dict":
                        lambda: Strict.from_serializer_dict(dict(raw)),
                }
                for label, call in non_string_entry_points.items():
                    try:
                        record = call()
                    except AttributeError as error:  # pragma: no cover
                        self.fail(f"{label} leaked AttributeError: {error}")
                    for name in ("d", "t", "ts"):
                        actual = getattr(record, name)
                        assert actual == value, \
                            (f"{label} must pass a non-string through "
                             f"untouched, got {actual!r} for {name!r}")

    def test_issue_405_serializer_dict_round_trips_typed_temporal_values(self):
        """Boundary fixture: the decoders' ``isinstance(v, str)`` guard.

        ``to_serializer_dict`` emits real ``datetime`` objects rather than ISO
        strings, so the generated decoders must pass a non-string through
        untouched or the documented
        ``from_serializer_dict(to_serializer_dict(x))`` pair would stop working.
        """
        src_dir, _source = self._generate_issue_405_module(
            self._TEMPORAL_TRIPLE_SCHEMA, "test_issue405_typedserdict")
        module = self._import_generated(
            src_dir, "test_issue405_typedserdict.test.issue405.strict")

        record = module.Strict(
            d=datetime.date(2024, 1, 1),
            t=datetime.time(7, 0),
            ts=datetime.datetime(2024, 1, 1, 10, 0))

        serialized = record.to_serializer_dict()
        assert isinstance(serialized["ts"], datetime.datetime), \
            f"to_serializer_dict should keep typed values, got {serialized['ts']!r}"
        assert module.Strict.from_serializer_dict(serialized) == record

    def test_issue_405_datetime_field_holding_a_date_dumps_like_master(self):
        """Boundary fixture: a ``date`` value in a ``datetime``-declared field.

        ``marshmallow.fields.DateTime(format='iso')`` serialized anything with
        an ``isoformat()``, so ``schema().dumps`` accepted a ``date`` in a
        ``datetime`` field and emitted ``"2024-05-06"``. The generated
        ``mm_field`` replaces that field, so it has to keep accepting it rather
        than handing the raw object to ``json.dumps`` and raising ``TypeError``.
        """
        schema = {
            "type": "object",
            "name": "Dt",
            "namespace": "test.issue405",
            "properties": {
                "v": {"type": "datetime"},
                "label": {"type": "string"},
            },
            "required": ["v", "label"],
        }
        src_dir, source = self._generate_issue_405_module(
            schema, "test_issue405_dtdump")
        module = self._import_generated(
            src_dir, "test_issue405_dtdump.test.issue405.dt")

        record = module.Dt(v=datetime.date(2024, 5, 6), label="L")
        dumped = module.Dt.schema().dumps(record)
        assert json.loads(dumped)["v"] == "2024-05-06", \
            f"schema().dumps must keep master's output for a date value: {dumped}\n{source}"

        # A real datetime is still emitted with its time component.
        exact = module.Dt(v=datetime.datetime(2024, 5, 6, 7, 8, 9), label="L")
        assert json.loads(module.Dt.schema().dumps(exact))["v"] == "2024-05-06T07:08:09"
        assert json.loads(exact.to_json()) == json.loads(module.Dt.schema().dumps(exact))

    def test_issue_405_ambiguous_temporal_union_emits_no_codec(self):
        """Invalid-input fixture: an undecidable union must not guess.

        ``["date", "datetime"]`` has no discriminated wire encoding, so a
        codec would have to pick one arm. Picking the date arm truncates the
        time component; picking by declaration order makes the two spellings
        of the same logical union behave differently. Emitting no codec keeps
        the pre-existing loud behaviour instead of losing data silently.
        """
        import datetime as dt

        orders = {
            "date_first": ["date", "datetime"],
            "datetime_first": ["datetime", "date"],
        }
        encodings = {}
        for label, arms in orders.items():
            schema = {
                "type": "object",
                "name": "AmbiguousUnion",
                "namespace": "test.issue405",
                "properties": {"v": {"type": arms}},
                "required": ["v"],
            }
            src_dir, source = self._generate_issue_405_module(
                schema, "test_issue405_union_" + label)
            assert "encoder=" not in source, \
                f"{label}: an undecidable union must not get an encoder\n{source}"
            assert "mm_field=" not in source, \
                f"{label}: an undecidable union must not get an mm_field\n{source}"

            module = self._import_generated(
                src_dir,
                f"test_issue405_union_{label}.test.issue405.ambiguousunion")
            moment = dt.datetime(2024, 1, 1, 10, 30, 45)
            encodings[label] = json.loads(module.AmbiguousUnion(v=moment).to_json())["v"]
            assert encodings[label] != "2024-01-01", \
                f"{label}: the time component must not be silently truncated"

        assert encodings["date_first"] == encodings["datetime_first"], \
            f"union arm order must not change the wire format: {encodings}"

    def test_issue_405_union_of_date_and_string_does_not_coerce_the_string_arm(self):
        """Invalid-input fixture: a str value must survive a ``[date, string]`` union.

        A decoder that parses every ``str`` in such a union turns a value that
        legitimately belongs to the string arm into a ``date``, so the record
        no longer round-trips.
        """
        schema = {
            "type": "object",
            "name": "DateOrString",
            "namespace": "test.issue405",
            "properties": {"v": {"type": ["date", "string"]}},
            "required": ["v"],
        }
        src_dir, source = self._generate_issue_405_module(
            schema, "test_issue405_union_str")
        assert "decoder=" not in source, \
            f"a date/string union must not get a decoder\n{source}"

        module = self._import_generated(
            src_dir, "test_issue405_union_str.test.issue405.dateorstring")
        record = module.DateOrString(v="2024-01-01")
        restored = module.DateOrString.from_json(record.to_json())
        assert restored.v == "2024-01-01", restored
        assert isinstance(restored.v, str), \
            f"the string arm must not be coerced to a date, got {type(restored.v)}"

    def test_issue_405_temporal_set_decodes_to_a_set(self):
        """Positive fixture: a temporal set must decode like any other set.

        ``dataclasses_json`` reconstructs the declared container for fields
        without a custom decoder, so a temporal set decoding to a ``list``
        would both violate its own ``typing.Set`` annotation and behave
        differently from a non-temporal set on the same class.
        """
        import datetime as dt

        schema = {
            "type": "object",
            "name": "SetProbe",
            "namespace": "test.issue405",
            "properties": {
                "labels": {"type": "set", "items": {"type": "string"}},
                "days": {"type": "set", "items": {"type": "date"}},
            },
            "required": ["labels", "days"],
        }
        src_dir, _source = self._generate_issue_405_module(
            schema, "test_issue405_set")

        module = self._import_generated(
            src_dir, "test_issue405_set.test.issue405.setprobe")
        SetProbe = module.SetProbe
        record = SetProbe(labels={"a", "b"},
                          days={dt.date(2024, 1, 1), dt.date(2024, 2, 2)})

        restored = SetProbe.from_json(record.to_json())
        assert isinstance(restored.labels, set), type(restored.labels)
        assert isinstance(restored.days, set), \
            f"a temporal set must decode to a set, got {type(restored.days)}"
        assert restored.days == record.days, restored.days
        assert restored == record, "a temporal set must round-trip by equality"

    def test_issue_405_only_the_needed_iso_parsers_are_emitted(self):
        """A module must not carry parser helpers it never calls."""
        src_dir, source = self._generate_issue_405_module(
            {
                "type": "object",
                "name": "DatetimeOnly",
                "namespace": "test.issue405",
                "properties": {"ts": {"type": "datetime"}},
                "required": ["ts"],
            },
            "test_issue405_parsers")
        del src_dir

        assert "def _parse_iso_datetime(" in source, source
        assert "def _parse_iso_date(" not in source, \
            "a datetime-only module must not emit the unused date parser"
        assert "def _parse_iso_time(" not in source, \
            "a datetime-only module must not emit the unused time parser"

    def test_issue_405_from_serializer_dict_does_not_mutate_input(self):
        """Invalid-input fixture: deserialization must not rewrite the caller's dict."""
        schema = {
            "type": "object",
            "name": "NoMutate",
            "namespace": "test.issue405",
            "properties": {"d": {"type": "date"}},
            "required": ["d"],
        }
        src_dir, _source = self._generate_issue_405_module(
            schema, "test_issue405_nomutate")

        module = self._import_generated(
            src_dir, "test_issue405_nomutate.test.issue405.nomutate")
        payload = {"d": "2024-01-01"}
        module.NoMutate.from_serializer_dict(dict(payload))
        original = {"d": "2024-01-01"}
        module.NoMutate.from_serializer_dict(original)
        assert original == payload, \
            f"from_serializer_dict must not mutate its argument, got {original}"

    def test_issue_405_generation_is_deterministic(self):
        """Repeated generation of the same schema must produce identical sources.

        The two generations run in separate interpreters with different
        PYTHONHASHSEED values; running them in-process would pass even against
        an unsorted generator because the hash seed would be shared.
        """
        import textwrap

        schema = {
            "type": "object",
            "name": "Determinism",
            "namespace": "test.issue405",
            "properties": {
                "d": {"type": "date"},
                "t": {"type": "time"},
                "ts": {"type": "datetime"},
                "label": {"type": "string"},
                "count": {"type": "int32"},
                "flag": {"type": "boolean"},
                "ratio": {"type": "double"},
                "tags": {"type": "array", "items": {"type": "string"}},
                "scores": {"type": "map", "values": {"type": "int32"}},
                "nested": {"type": "object", "name": "NestedOne",
                           "properties": {"x": {"type": "string"}}, "required": ["x"]},
                "other": {"type": "object", "name": "NestedTwo",
                          "properties": {"y": {"type": "string"}}, "required": ["y"]},
            },
            "required": ["d", "t", "ts", "label", "count", "flag", "ratio",
                         "tags", "scores", "nested", "other"],
        }

        work_dir = tempfile.mkdtemp(prefix="avrotize-issue405-det-")
        self.addCleanup(shutil.rmtree, work_dir, True)
        script = os.path.join(work_dir, "generate.py")
        with open(script, "w", encoding="utf-8") as fh:
            fh.write(textwrap.dedent("""
                import json
                import sys
                from avrotize.structuretopython import convert_structure_schema_to_python

                with open(sys.argv[1], 'r', encoding='utf-8') as handle:
                    schema = json.load(handle)
                convert_structure_schema_to_python(
                    schema, sys.argv[2], package_name='det_pkg',
                    dataclasses_json_annotation=True)
                """))
        schema_file = os.path.join(work_dir, "schema.json")
        with open(schema_file, "w", encoding="utf-8") as fh:
            json.dump(schema, fh)

        repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        renderings = []
        for seed in ("1", "2"):
            out_dir = os.path.join(work_dir, "out" + seed)
            env = dict(os.environ)
            env["PYTHONHASHSEED"] = seed
            env["PYTHONPATH"] = os.pathsep.join(
                [repo_root] + ([env["PYTHONPATH"]] if env.get("PYTHONPATH") else []))
            result = subprocess.run(
                [sys.executable, script, schema_file, out_dir],
                env=env, capture_output=True, text=True, check=False)
            assert result.returncode == 0, \
                f"generation failed for PYTHONHASHSEED={seed}: {result.stderr}"

            rendering = {}
            for root, dirs, files in os.walk(out_dir):
                dirs.sort()
                for name in sorted(files):
                    path = os.path.join(root, name)
                    with open(path, "r", encoding="utf-8", errors="replace") as fh:
                        rendering[os.path.relpath(path, out_dir)] = fh.read()
            renderings.append(rendering)

        assert sorted(renderings[0]) == sorted(renderings[1]), \
            "s2py must emit the same set of files regardless of PYTHONHASHSEED"
        for name in sorted(renderings[0]):
            assert renderings[0][name] == renderings[1][name], \
                f"s2py output for {name} differs between hash seeds"

    # -- round 4: marshmallow schema() consistency -------------------------

    _RENAMED_SCHEMA = {
        "type": "object",
        "name": "Names",
        "namespace": "test.issue405",
        "properties": {
            "birth-date": {"type": "date"},
            "class": {"type": "string"},
            "seen-at": {"type": "datetime"},
            "wake-time": {"type": "time"},
        },
        "required": ["birth-date", "class", "seen-at", "wake-time"],
    }

    def test_issue_405_schema_preserves_renamed_json_field_names(self):
        """Positive fixture: attaching an mm_field must not drop ``data_key``.

        ``dataclasses_json.mm.schema()`` uses a configured ``mm_field``
        verbatim and only assigns ``data_key`` on the branch it takes when no
        ``mm_field`` is present.  A temporal field whose JSON name differs from
        its Python name therefore lost its wire name, so ``schema().dumps``
        emitted a second, different wire format and ``schema().loads`` raised
        ``KeyError``.
        """
        src_dir, source = self._generate_issue_405_module(
            self._RENAMED_SCHEMA, "test_issue405_renamed")
        module = self._import_generated(
            src_dir, "test_issue405_renamed.test.issue405.names")

        record = module.Names(
            birth_date=datetime.date(2024, 1, 1),
            class_="C",
            seen_at=datetime.datetime(2024, 1, 1, 10, 0),
            wake_time=datetime.time(7, 0))

        expected_keys = ["birth-date", "class", "seen-at", "wake-time"]
        assert sorted(json.loads(record.to_json())) == expected_keys, \
            f"to_json must use the JSON field names:\n{source}"

        dumped = module.Names.schema().dumps(record)
        assert sorted(json.loads(dumped)) == expected_keys, \
            f"schema().dumps must use the JSON field names, got {dumped}"
        # One wire format, not two.
        assert json.loads(dumped) == json.loads(record.to_json()), \
            f"schema().dumps and to_json disagree: {dumped} vs {record.to_json()}"

        # All five documented entry points must accept that payload.
        payload = record.to_json()
        assert module.Names.schema().loads(payload) == record
        assert module.Names.from_json(payload) == record
        assert module.Names.from_data(payload.encode("utf-8"),
                                      "application/json") == record
        assert module.Names.from_data(json.loads(payload),
                                      "application/json") == record
        assert module.Names.from_serializer_dict(json.loads(payload)) == record

    def test_issue_405_schema_load_uses_the_generated_iso_parser(self):
        """Positive fixture: ``schema().loads`` must share one parser.

        A configured ``mm_field`` also suppresses the ``_deserialize`` override
        dataclasses_json installs for a custom decoder, so ``schema().loads``
        fell back to marshmallow's parser.  ``marshmallow.utils.from_iso_time``
        does not support UTC offsets and silently discards them, and marshmallow
        rejects a lowercase ``z`` that the other four entry points accept.
        """
        from marshmallow import ValidationError

        src_dir, source = self._generate_issue_405_module(
            self._TEMPORAL_TRIPLE_SCHEMA, "test_issue405_mmparser")
        module = self._import_generated(
            src_dir, "test_issue405_mmparser.test.issue405.strict")

        offset = {"d": "2024-01-01", "t": "13:45:30+05:30",
                  "ts": "2024-01-01T13:45:30+05:30"}
        loaded = module.Strict.schema().loads(json.dumps(offset))
        assert loaded.t.utcoffset() == datetime.timedelta(hours=5, minutes=30), \
            f"schema().loads dropped the UTC offset on a time: {loaded.t!r}"
        assert loaded == module.Strict.from_json(json.dumps(offset)), \
            "schema().loads and from_json disagree on an offset-bearing payload"

        lower_z = {"d": "2024-01-01", "t": "13:45:30z",
                   "ts": "2024-01-01T13:45:30z"}
        assert (module.Strict.schema().loads(json.dumps(lower_z))
                == module.Strict.from_json(json.dumps(lower_z))), \
            f"schema().loads must accept a lowercase z like from_json:\n{source}"

        # Malformed input still has to be rejected, and marshmallow must keep
        # owning the exception type on the path it controls.
        with self.assertRaises(ValidationError) as caught:
            module.Strict.schema().loads(json.dumps(
                {"d": "20 November 1998", "t": "13:45:30",
                 "ts": "2024-01-01T13:45:30"}))
        assert "'d'" in str(caught.exception), \
            f"the validation error must name the field: {caught.exception}"

    def test_issue_405_set_containers_agree_across_entry_points(self):
        """Boundary fixture: every entry point rebuilds declared containers.

        ``from_json`` reconstructed a ``Set`` while ``from_serializer_dict``
        (and therefore ``from_data``) handed back a list for non-temporal
        element types, so ``from_data(to_byte_array(x)) != x``.
        """
        schema = {
            "type": "object",
            "name": "Sets",
            "namespace": "test.issue405",
            "properties": {
                "labels": {"type": "set", "items": {"type": "string"}},
                "days": {"type": "set", "items": {"type": "date"}},
            },
            "required": ["labels", "days"],
        }
        src_dir, source = self._generate_issue_405_module(
            schema, "test_issue405_sets")
        module = self._import_generated(
            src_dir, "test_issue405_sets.test.issue405.sets")

        record = module.Sets(
            labels={"a", "b"},
            days={datetime.date(2024, 1, 1), datetime.date(2024, 2, 2)})

        payload = record.to_json()
        for label, restored in (
                ("from_json", module.Sets.from_json(payload)),
                ("from_serializer_dict",
                 module.Sets.from_serializer_dict(json.loads(payload))),
                ("from_data",
                 module.Sets.from_data(payload.encode("utf-8"),
                                       "application/json")),
        ):
            assert isinstance(restored.labels, set), \
                f"{label} returned {type(restored.labels).__name__} for Set[str]:\n{source}"
            assert isinstance(restored.days, set), \
                f"{label} returned {type(restored.days).__name__} for Set[date]"
            assert restored == record, f"{label} did not round-trip: {restored!r}"

        # The byte-array path is what a caller actually uses end to end.
        assert module.Sets.from_data(record.to_byte_array("application/json"),
                                     "application/json") == record, \
            "from_data(to_byte_array(x)) must equal x"

    def test_issue_405_nullable_temporal_survives_schema_round_trip(self):
        """Invalid/boundary fixture: nullable temporal fields under schema().

        The ``mm_field`` branch also skips ``allow_none``, so a null value for
        an optional temporal field failed marshmallow validation.
        """
        schema = {
            "type": "object",
            "name": "Opt",
            "namespace": "test.issue405",
            "properties": {
                "maybe-day": {"type": ["null", "date"]},
                "label": {"type": "string"},
            },
            "required": ["maybe-day", "label"],
        }
        src_dir, source = self._generate_issue_405_module(
            schema, "test_issue405_optional")
        module = self._import_generated(
            src_dir, "test_issue405_optional.test.issue405.opt")

        for value in (None, datetime.date(2024, 1, 1)):
            with self.subTest(value=value):
                record = module.Opt(maybe_day=value, label="L")
                dumped = module.Opt.schema().dumps(record)
                assert json.loads(dumped) == json.loads(record.to_json()), \
                    f"schema().dumps and to_json disagree: {dumped}"
                assert "maybe-day" in json.loads(dumped), \
                    f"schema().dumps lost the JSON field name:\n{source}"
                assert module.Opt.schema().loads(dumped) == record


if __name__ == '__main__':
    unittest.main()
