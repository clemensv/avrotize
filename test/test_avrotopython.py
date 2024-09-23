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
