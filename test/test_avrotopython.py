import os
import shutil
import sys
from os import path, getcwd

import pytest

from avrotize.avrotopython import convert_avro_to_python
from avrotize.jsonstoavro import convert_jsons_to_avro

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

import unittest
from unittest.mock import patch

class TestAvroToPython(unittest.TestCase):
    def test_convert_address_avsc_to_python(self):
        """ Test converting an address.avsc file to C# """
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        py_path = os.path.join(cwd, "test", "tmp", "address-py")
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)
        
        convert_avro_to_python(avro_path, py_path)
        
    def test_convert_address_avsc_to_python_json(self):
        """ Test converting an address.avsc file to C# """
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        py_path = os.path.join(cwd, "test", "tmp", "address-py-json")
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)
        
        convert_avro_to_python(avro_path, py_path, dataclasses_json_annotation=True)
        
    def test_convert_address_avsc_to_python_avro(self):
        """ Test converting an address.avsc file to Python with avro annotation """
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        py_path = os.path.join(cwd, "test", "tmp", "address-py-avro")
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)
        
        convert_avro_to_python(avro_path, py_path, avro_annotation=True)

    def test_convert_telemetry_avsc_to_python(self):
        """ Test converting a telemetry.avsc file to C# """
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", "telemetry.avsc")
        py_path = os.path.join(cwd, "test", "tmp", "telemetry-py")
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)
        
        convert_avro_to_python(avro_path, py_path)

    def test_convert_jfrog_pipelines_jsons_to_avro_to_python(self):
        """ Test converting a jfrog-pipelines.json file to C# """
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(cwd, "test", "tmp", "jfrog-pipelines.avsc")
        py_path = path.join(cwd, "test", "tmp", "jfrog-pipelines-py")
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)
            
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_python(avro_path, py_path)
        