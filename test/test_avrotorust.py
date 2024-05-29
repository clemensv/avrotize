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
import pytest


class TestAvroToRust(unittest.TestCase):
    
    def run_convert_to_rust(self, name:str, avro_annotation:bool=False, serde_annotation:bool=False):
        """ Test converting an address.avsc file to Rust """
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", f"{name}.avsc")
        rust_path = os.path.join(tempfile.gettempdir(), "avrotize", f"{name}-rs{'' if not avro_annotation else '-avro'}{'' if not serde_annotation else '-serde'}")
        if os.path.exists(rust_path):
            shutil.rmtree(rust_path, ignore_errors=True)
        os.makedirs(rust_path, exist_ok=True)        
        convert_avro_to_rust(avro_path, rust_path, package_name=name, avro_annotation=avro_annotation, serde_annotation=serde_annotation)
        
    def test_convert_address_avsc_to_rust(self):
        """ Test converting an address.avsc file to Rust """
        self.run_convert_to_rust("address", True, True)
        self.run_convert_to_rust("address", True, False)
        self.run_convert_to_rust("address", False, True)
        self.run_convert_to_rust("address", False, False)
        
    @pytest.mark.skip(reason="Rust library for Apache Avro has broken suppport for unions")
    def test_convert_twotypeunion_avsc_to_rust(self):
        """ Test converting an twotypeunion.avsc file to Rust """
        self.run_convert_to_rust("twotypeunion", True, True)
        self.run_convert_to_rust("twotypeunion", True, False)
        self.run_convert_to_rust("twotypeunion", False, True)
        self.run_convert_to_rust("twotypeunion", False, False)
    
    @pytest.mark.skip(reason="Rust library for Apache Avro has broken suppport for unions")
    def test_convert_typemapunion_avsc_to_rust(self):
        """ Test converting an twotypeunion.avsc file to Rust """
        self.run_convert_to_rust("typemapunion", True, True)
        self.run_convert_to_rust("typemapunion", True, False)
        self.run_convert_to_rust("typemapunion", False, True)
        self.run_convert_to_rust("typemapunion", False, False)
    

    def test_convert_telemetry_avsc_to_rust(self):
        """ Test converting a telemetry.avsc file to Rust """
        self.run_convert_to_rust("telemetry", True, True)
        self.run_convert_to_rust("telemetry", True, False)
        self.run_convert_to_rust("telemetry", False, True)
        self.run_convert_to_rust("telemetry", False, False)

    @pytest.mark.skip(reason="Rust library for Apache Avro has broken suppport for unions")
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

    @pytest.mark.skip(reason="Rust library for Apache Avro has broken suppport for unions")        
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
