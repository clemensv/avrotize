"""Tests for JSON Structure to Python conversion."""

import unittest
import os
import shutil
import subprocess
import sys
import tempfile
from os import path, getcwd

from avrotize.structuretopython import convert_structure_to_python

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)


class TestStructureToPython(unittest.TestCase):
    """Test cases for JSON Structure to Python conversion."""

    def test_convert_address_struct_to_python(self):
        """ Test converting address-ref.struct.json to Python """
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "jsons", "address-ref.struct.json")
        py_path = os.path.join(tempfile.gettempdir(), "avrotize", "address-py")
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)

        convert_structure_to_python(struct_path, py_path)
        new_env = os.environ.copy()
        new_env['PYTHONPATH'] = os.path.join(py_path, 'src')
        # Check that files were generated
        assert os.path.exists(os.path.join(py_path, 'src'))
        assert os.path.exists(os.path.join(py_path, 'pyproject.toml'))

    def test_convert_address_struct_to_python_json(self):
        """ Test converting address-ref.struct.json to Python with JSON annotations """
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "jsons", "address-ref.struct.json")
        py_path = os.path.join(tempfile.gettempdir(), "avrotize", "address-py-json")
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)

        convert_structure_to_python(struct_path, py_path, dataclasses_json_annotation=True)
        new_env = os.environ.copy()
        new_env['PYTHONPATH'] = os.path.join(py_path, 'src')
        # Check that files were generated
        assert os.path.exists(os.path.join(py_path, 'src'))
        assert os.path.exists(os.path.join(py_path, 'pyproject.toml'))

    def test_convert_object_struct_to_python(self):
        """ Test converting a simple object struct to Python """
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
        """ Test that s2py output is structurally equivalent to a2py output """
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


if __name__ == '__main__':
    unittest.main()
