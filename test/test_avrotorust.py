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


from avrotize.avrotorust import convert_avro_to_rust
from avrotize.jsonstoavro import convert_jsons_to_avro


class TestAvroToRust(unittest.TestCase):
    def test_convert_address_avsc_to_rust(self):
        """ Test converting an address.avsc file to Rust """
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        rust_path = os.path.join(tempfile.gettempdir(), "avrotize", "address-rs")
        if os.path.exists(rust_path):
            shutil.rmtree(rust_path, ignore_errors=True)
        os.makedirs(rust_path, exist_ok=True)
        
        convert_avro_to_rust(avro_path, rust_path, package_name="address")

    def test_convert_telemetry_avsc_to_rust(self):
        """ Test converting a telemetry.avsc file to Rust """
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", "telemetry.avsc")
        rust_path = os.path.join(tempfile.gettempdir(), "avrotize", "telemetry-rs")
        if os.path.exists(rust_path):
            shutil.rmtree(rust_path, ignore_errors=True)
        os.makedirs(rust_path, exist_ok=True)
        
        convert_avro_to_rust(avro_path, rust_path, package_name="telemetry")

    def test_convert_jfrog_pipelines_jsons_to_avro_to_rust(self):
        """ Test converting a jfrog-pipelines.json file to Rust """
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        rust_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines-rs")
        if os.path.exists(rust_path):
            shutil.rmtree(rust_path, ignore_errors=True)
        os.makedirs(rust_path, exist_ok=True)
            
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_rust(avro_path, rust_path, package_name="jfrog_pipelines")
        
    def test_convert_jfrog_pipelines_jsons_to_avro_to_rust_typed_json(self):
        """ Test converting a jfrog-pipelines.json file to Rust """
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        rust_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines-rs-typed-json")
        if os.path.exists(rust_path):
            shutil.rmtree(rust_path, ignore_errors=True)
        os.makedirs(rust_path, exist_ok=True)
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_rust(avro_path, rust_path, package_name="jfrog_pipelines", serde_annotation=True)
        
    def test_convert_jfrog_pipelines_jsons_to_avro_to_rust_avro_annotations(self):
        """ Test converting a jfrog-pipelines.json file to Rust """
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        rust_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines-rs-avro")
        if os.path.exists(rust_path):
            shutil.rmtree(rust_path, ignore_errors=True)
        os.makedirs(rust_path, exist_ok=True)
            
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_rust(avro_path, rust_path, package_name="jfrog_pipelines", avro_annotation=True)
