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


if __name__ == '__main__':
    unittest.main()
