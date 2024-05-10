import os
import shutil
import sys
import tempfile
from os import path, getcwd

import pytest

from avrotize.avrotots import convert_avro_to_typescript
from avrotize.jsonstoavro import convert_jsons_to_avro

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

import unittest
from unittest.mock import patch

class TestAvroToTypeScript(unittest.TestCase):
    def test_convert_address_avsc_to_typescript(self):
        """ Test converting an address.avsc file to C# """
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        ts_path = os.path.join(tempfile.gettempdir(), "avrotize", "address-ts")
        if os.path.exists(ts_path):
            shutil.rmtree(ts_path, ignore_errors=True)
        os.makedirs(ts_path, exist_ok=True)
        
        convert_avro_to_typescript(avro_path, ts_path)

    def test_convert_telemetry_avsc_to_typescript(self):
        """ Test converting a telemetry.avsc file to C# """
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", "telemetry.avsc")
        ts_path = os.path.join(tempfile.gettempdir(), "avrotize", "telemetry-ts")
        if os.path.exists(ts_path):
            shutil.rmtree(ts_path, ignore_errors=True)
        os.makedirs(ts_path, exist_ok=True)
        
        convert_avro_to_typescript(avro_path, ts_path)

    def test_convert_jfrog_pipelines_jsons_to_avro_to_typescript(self):
        """ Test converting a jfrog-pipelines.json file to C# """
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        ts_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines-ts")
        if os.path.exists(ts_path):
            shutil.rmtree(ts_path, ignore_errors=True)
        os.makedirs(ts_path, exist_ok=True)
            
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_typescript(avro_path, ts_path)
        
    def test_convert_jfrog_pipelines_jsons_to_avro_to_typescript_typed_json(self):
        """ Test converting a jfrog-pipelines.json file to C# """
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        ts_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines-ts-typed-json")
        if os.path.exists(ts_path):
            shutil.rmtree(ts_path, ignore_errors=True)
        os.makedirs(ts_path, exist_ok=True)
            
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_typescript(avro_path, ts_path, typedjson_annotation=True)
        
    def test_convert_jfrog_pipelines_jsons_to_avro_to_typescript_avro_annotations(self):
        """ Test converting a jfrog-pipelines.json file to C# """
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        ts_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines-ts-avro")
        if os.path.exists(ts_path):
            shutil.rmtree(ts_path, ignore_errors=True)
        os.makedirs(ts_path, exist_ok=True)
            
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_typescript(avro_path, ts_path, avro_annotation=True)
        