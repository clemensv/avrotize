import os
import shutil
import sys
from os import path, getcwd

import pytest

from avrotize.avrotojs import convert_avro_to_javascript
from avrotize.jsonstoavro import convert_jsons_to_avro

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

import unittest
from unittest.mock import patch

class TestAvroToJavaScript(unittest.TestCase):
    def test_convert_address_avsc_to_javascript(self):
        """ Test converting an address.avsc file to C# """
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        js_path = os.path.join(cwd, "test", "tmp", "address-js")
        if os.path.exists(js_path):
            shutil.rmtree(js_path, ignore_errors=True)
        os.makedirs(js_path, exist_ok=True)
        
        convert_avro_to_javascript(avro_path, js_path)

    def test_convert_telemetry_avsc_to_javascript(self):
        """ Test converting a telemetry.avsc file to C# """
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", "telemetry.avsc")
        js_path = os.path.join(cwd, "test", "tmp", "telemetry-js")
        if os.path.exists(js_path):
            shutil.rmtree(js_path, ignore_errors=True)
        os.makedirs(js_path, exist_ok=True)
        
        convert_avro_to_javascript(avro_path, js_path)

    def test_convert_jfrog_pipelines_jsons_to_avro_to_javascript(self):
        """ Test converting a jfrog-pipelines.json file to C# """
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(cwd, "test", "tmp", "jfrog-pipelines.avsc")
        js_path = path.join(cwd, "test", "tmp", "jfrog-pipelines-js")
        if os.path.exists(js_path):
            shutil.rmtree(js_path, ignore_errors=True)
        os.makedirs(js_path, exist_ok=True)
            
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_javascript(avro_path, js_path)
                
    def test_convert_jfrog_pipelines_jsons_to_avro_to_javascript_avro_annotations(self):
        """ Test converting a jfrog-pipelines.json file to C# """
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(cwd, "test", "tmp", "jfrog-pipelines.avsc")
        js_path = path.join(cwd, "test", "tmp", "jfrog-pipelines-js-avro")
        if os.path.exists(js_path):
            shutil.rmtree(js_path, ignore_errors=True)
        os.makedirs(js_path, exist_ok=True)
            
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_javascript(avro_path, js_path, avro_annotation=True)
        