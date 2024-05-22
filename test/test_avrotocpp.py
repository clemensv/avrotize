import os
import shutil
import sys
import tempfile
from os import path, getcwd
import pytest
import unittest
from unittest.mock import patch

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)


from avrotize.avrotocpp import convert_avro_to_cpp
from avrotize.jsonstoavro import convert_jsons_to_avro


class TestAvroToCpp(unittest.TestCase):


    def run_convert_avsc_to_cpp(self, avro_name, avro_annotation=False, json_annotation=False):
        """ Test converting an.avsc file to cpp """
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", f"{avro_name}.avsc")
        cpp_path = os.path.join(tempfile.gettempdir(), "avrotize", f"{avro_name}-cpp{'-avro' if avro_annotation else ''}{'-json' if json_annotation else ''}")
        if os.path.exists(cpp_path):
            shutil.rmtree(cpp_path, ignore_errors=True)
        os.makedirs(cpp_path, exist_ok=True)        
        convert_avro_to_cpp(avro_path, cpp_path, namespace=avro_name, json_annotation=json_annotation, avro_annotation=avro_annotation)

    def test_convert_address_avsc_to_cpp(self):
        """ Test converting an address.avsc file to cpp """
        self.run_convert_avsc_to_cpp("address", False, False)
        self.run_convert_avsc_to_cpp("address", True, False)
        self.run_convert_avsc_to_cpp("address", False, True)
        self.run_convert_avsc_to_cpp("address", True, True)

    def test_convert_telemetry_avsc_to_cpp(self):
        """ Test converting a telemetry.avsc file to cpp """
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", "telemetry.avsc")
        cpp_path = os.path.join(tempfile.gettempdir(), "avrotize", "telemetry-cpp")
        if os.path.exists(cpp_path):
            shutil.rmtree(cpp_path, ignore_errors=True)
        os.makedirs(cpp_path, exist_ok=True)
        
        convert_avro_to_cpp(avro_path, cpp_path, namespace="telemetry")

    def test_convert_jfrog_pipelines_jsons_to_avro_to_cpp(self):
        """ Test converting a jfrog-pipelines.json file to cpp """
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        cpp_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines-cpp")
        if os.path.exists(cpp_path):
            shutil.rmtree(cpp_path, ignore_errors=True)
        os.makedirs(cpp_path, exist_ok=True)
            
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_cpp(avro_path, cpp_path, namespace="jfrog_pipelines")
        
    def test_convert_jfrog_pipelines_jsons_to_avro_to_cpp_typed_json(self):
        """ Test converting a jfrog-pipelines.json file to cpp """
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        cpp_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines-cpp-typed-json")
        if os.path.exists(cpp_path):
            shutil.rmtree(cpp_path, ignore_errors=True)
        os.makedirs(cpp_path, exist_ok=True)
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_cpp(avro_path, cpp_path, namespace="jfrog_pipelines", json_annotation=True)
        
    def test_convert_jfrog_pipelines_jsons_to_avro_to_cpp_avro_annotations(self):
        """ Test converting a jfrog-pipelines.json file to cpp """
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        cpp_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines-cpp-avro")
        if os.path.exists(cpp_path):
            shutil.rmtree(cpp_path, ignore_errors=True)
        os.makedirs(cpp_path, exist_ok=True)
            
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_cpp(avro_path, cpp_path, namespace="jfrog_pipelines", avro_annotation=True)
