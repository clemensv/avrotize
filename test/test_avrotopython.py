from unittest.mock import patch
import unittest
import os
import shutil
import subprocess
import sys
import tempfile
from os import path, getcwd

import pytest

from avrotize.avrotopython import convert_avro_to_python
from avrotize.jsonstoavro import convert_jsons_to_avro

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)


class TestAvroToPython(unittest.TestCase):
    def test_convert_address_avsc_to_python(self):
        """ Test converting an address.avsc file to Python """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        py_path = os.path.join(tempfile.gettempdir(), "avrotize", "address-py")
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)

        convert_avro_to_python(avro_path, py_path)
        new_env = os.environ.copy()
        new_env['PYTHONPATH'] = py_path
        assert subprocess.check_call(
            ['python', '-m', 'pytest'], cwd=py_path, env=new_env, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0

    def test_convert_address_avsc_to_python_json(self):
        """ Test converting an address.avsc file to Python """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        py_path = os.path.join(tempfile.gettempdir(), "avrotize", "address-py-json")
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)

        convert_avro_to_python(
            avro_path, py_path, dataclasses_json_annotation=True)
        new_env = os.environ.copy()
        new_env['PYTHONPATH'] = py_path
        assert subprocess.check_call(
            ['python', '-m', 'pytest'], cwd=py_path, env=new_env, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0
        
    def test_convert_address_avsc_to_python_json_avro(self):
        """ Test converting an address.avsc file to Python """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        py_path = os.path.join(tempfile.gettempdir(), "avrotize", "address-py-jsonavro")
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)

        convert_avro_to_python(
            avro_path, py_path, dataclasses_json_annotation=True, avro_annotation=True)
        new_env = os.environ.copy()
        new_env['PYTHONPATH'] = py_path
        assert subprocess.check_call(
            ['python', '-m', 'pytest'], cwd=py_path, env=new_env, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0

    def test_convert_address_avsc_to_python_avro(self):
        """ Test converting an address.avsc file to Python with avro annotation """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        py_path = os.path.join(tempfile.gettempdir(), "avrotize", "address-py-avro")
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)

        convert_avro_to_python(avro_path, py_path, avro_annotation=True)
        new_env = os.environ.copy()
        new_env['PYTHONPATH'] = py_path
        assert subprocess.check_call(
            ['python', '-m', 'pytest'], cwd=py_path, env=new_env, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0

    def test_convert_telemetry_avsc_to_python(self):
        """ Test converting a telemetry.avsc file to Python """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "telemetry.avsc")
        py_path = os.path.join(tempfile.gettempdir(), "avrotize", "telemetry-py")
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)

        convert_avro_to_python(avro_path, py_path, dataclasses_json_annotation=True, avro_annotation=True)
        new_env = os.environ.copy()
        new_env['PYTHONPATH'] = py_path
        assert subprocess.check_call(
            ['python', '-m', 'pytest'], cwd=py_path, env=new_env, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0
        
    def test_convert_enumfield_avsc_to_python(self):
        """ Test converting a enumfield.avsc file to Python """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "enumfield.avsc")
        py_path = os.path.join(tempfile.gettempdir(), "avrotize", "enumfield-py")
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)

        convert_avro_to_python(avro_path, py_path, dataclasses_json_annotation=True, avro_annotation=True)
        new_env = os.environ.copy()
        new_env['PYTHONPATH'] = py_path
        assert subprocess.check_call(
            ['python', '-m', 'pytest'], cwd=py_path, env=new_env, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0
        
    def test_convert_enumfield_ordinals_avsc_to_python(self):
        """ Test converting a enumfield-ordinal.avsc file to Python """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "enumfield-ordinals.avsc")
        py_path = os.path.join(tempfile.gettempdir(), "avrotize", "enumfield-ordinals-py")
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)

        convert_avro_to_python(avro_path, py_path, dataclasses_json_annotation=True, avro_annotation=True)
        new_env = os.environ.copy()
        new_env['PYTHONPATH'] = py_path
        assert subprocess.check_call(
            ['python', '-m', 'pytest'], cwd=py_path, env=new_env, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0
        
    def test_convert_feeditem_avsc_to_python(self):
        """ Test converting a enumfield-ordinal.avsc file to Python """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "feeditem.avsc")
        py_path = os.path.join(tempfile.gettempdir(), "avrotize", "feeditem-py")
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)

        convert_avro_to_python(avro_path, py_path, dataclasses_json_annotation=True, avro_annotation=True)
        new_env = os.environ.copy()
        new_env['PYTHONPATH'] = py_path
        assert subprocess.check_call(
            ['python', '-m', 'pytest'], cwd=py_path, env=new_env, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0

    def test_convert_jfrog_pipelines_jsons_to_avro_to_python(self):
        """ Test converting a jfrog-pipelines.json file to Python """
        cwd = getcwd()
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        py_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines-py")
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)

        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_python(avro_path, py_path, dataclasses_json_annotation=True, avro_annotation=True)
        new_env = os.environ.copy()
        new_env['PYTHONPATH'] = py_path
        
        assert subprocess.check_call(
            ['python', '-m', 'pytest'], cwd=py_path, env=new_env, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0

    def test_init_py_exports_class_names(self):
        """ Test that __init__.py files correctly export PascalCase class names, not just module names """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        py_path = os.path.join(tempfile.gettempdir(), "avrotize", "address-py-init-test")
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)

        convert_avro_to_python(avro_path, py_path)
        
        # Test 1: Verify __init__.py exists and contains correct imports
        init_file = os.path.join(py_path, "src", "address", "example", "com", "__init__.py")
        assert os.path.exists(init_file), f"__init__.py not found at {init_file}"
        
        with open(init_file, 'r', encoding='utf-8') as f:
            init_content = f.read()
            # Should import PascalCase class name, not lowercase module name
            assert "from .record import Record" in init_content, \
                f"Expected 'from .record import Record' in __init__.py, got: {init_content}"
            assert '__all__ = ["Record"]' in init_content, \
                f"Expected '__all__ = [\"Record\"]' in __init__.py, got: {init_content}"
        
        # Test 2: Verify no erroneous subdirectory was created
        record_dir = os.path.join(py_path, "src", "address", "example", "com", "record")
        assert not os.path.exists(record_dir), \
            f"Erroneous 'record' directory should not exist at {record_dir}"
        
        # Test 3: Verify the module file exists
        record_file = os.path.join(py_path, "src", "address", "example", "com", "record.py")
        assert os.path.exists(record_file), f"Module file not found at {record_file}"
        
        # Test 4: Verify we can actually import the class using the package
        # This is the critical test - it must work for end users
        new_env = os.environ.copy()
        new_env['PYTHONPATH'] = os.path.join(py_path, 'src')
        
        test_import_script = os.path.join(py_path, "test_import.py")
        with open(test_import_script, 'w', encoding='utf-8') as f:
            f.write("""
# Test direct class import from package
from address.example.com import Record

# Verify it's the actual class, not a module
assert hasattr(Record, '__dataclass_fields__'), "Record should be a dataclass"
assert Record.__name__ == 'Record', f"Expected class name 'Record', got {Record.__name__}"

# Test instantiation
try:
    instance = Record(
        type='address',
        postOfficeBox=None,
        extendedAddress=None,
        streetAddress='123 Main St',
        locality='Springfield',
        region='IL',
        postalCode='62701',
        countryName='USA'
    )
    assert instance.locality == 'Springfield', "Instance creation failed"
    print("SUCCESS: All import and class tests passed!")
except Exception as e:
    print(f"FAILED: {e}")
    raise
""")
        
        result = subprocess.run(
            [sys.executable, test_import_script],
            cwd=py_path,
            env=new_env,
            capture_output=True,
            text=True
        )
        
        assert result.returncode == 0, \
            f"Import test failed with code {result.returncode}.\nStdout: {result.stdout}\nStderr: {result.stderr}"
        assert "SUCCESS" in result.stdout, \
            f"Expected success message in output, got: {result.stdout}"
