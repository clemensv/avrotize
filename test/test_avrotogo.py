import os
import shutil
import subprocess
import sys
import tempfile
from os import path, getcwd
import pytest
import unittest
from unittest.mock import patch

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)


from avrotize.avrotogo import convert_avro_to_go
from avrotize.jsonstoavro import convert_jsons_to_avro


class TestAvroToGo(unittest.TestCase):


    def verify_go_output(self, go_path, package_name: str):
        """ Verify the Go output """
         # Check if Go is installed
        go_installed = False
        try:
            subprocess.run(["go", "version"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            go_installed = True
        except (subprocess.CalledProcessError, FileNotFoundError):
            pass
        
        if go_installed:
            try:
                # Initialize a new Go module if needed
                subprocess.run(["go", "mod", "init", package_name], cwd=go_path, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            except subprocess.CalledProcessError:
                pass  # Ignore if the module is already initialized

            try:
                # Download all dependencies
                subprocess.run(["go", "mod", "tidy"], cwd=go_path, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                
                # Run the Go tests
                result = subprocess.run(["go", "build", "./..."], cwd=go_path, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                output = result.stdout.decode()
                if output:
                    print("\nGo build output:\n", output)
            except subprocess.CalledProcessError as e:
                print("Go tests failed:\n", e.stderr.decode())
        else:
            print("Go tools are not installed on this machine.")
        
    def run_convert_avsc_to_go(self, avro_name, avro_annotation=False, json_annotation=False):
        """ Test converting an.avsc file to go """
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", f"{avro_name}.avsc")
        go_path = os.path.join(tempfile.gettempdir(), "avrotize", f"{avro_name}-go{'-avro' if avro_annotation else ''}{'-json' if json_annotation else ''}")
        if os.path.exists(go_path):
            shutil.rmtree(go_path, ignore_errors=True)
        os.makedirs(go_path, exist_ok=True)        
        convert_avro_to_go(avro_path, go_path, package_name=avro_name, json_annotation=json_annotation, avro_annotation=avro_annotation)
        self.verify_go_output(go_path, avro_name)

    def test_convert_address_avsc_to_go(self):
        """ Test converting an address.avsc file to go """
        self.run_convert_avsc_to_go("address", False, False)
        self.run_convert_avsc_to_go("address", True, False)
        self.run_convert_avsc_to_go("address", False, True)
        self.run_convert_avsc_to_go("address", True, True)

    def test_convert_telemetry_avsc_to_go(self):
        """ Test converting a telemetry.avsc file to go """
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", "telemetry.avsc")
        go_path = os.path.join(tempfile.gettempdir(), "avrotize", "telemetry-go")
        if os.path.exists(go_path):
            shutil.rmtree(go_path, ignore_errors=True)
        os.makedirs(go_path, exist_ok=True)
        
        convert_avro_to_go(avro_path, go_path, package_name="telemetry")
        self.verify_go_output(go_path, "telemetry")
        
        
        

    def test_convert_jfrog_pipelines_jsons_to_avro_to_go(self):
        """ Test converting a jfrog-pipelines.json file to go """
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        go_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines-go")
        if os.path.exists(go_path):
            shutil.rmtree(go_path, ignore_errors=True)
        os.makedirs(go_path, exist_ok=True)
            
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_go(avro_path, go_path, package_name="jfrog_pipelines")
        self.verify_go_output(go_path, "jfrog_pipelines")
        
    def test_convert_jfrog_pipelines_jsons_to_avro_to_go_typed_json(self):
        """ Test converting a jfrog-pipelines.json file to go """
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        go_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines-go-typed-json")
        if os.path.exists(go_path):
            shutil.rmtree(go_path, ignore_errors=True)
        os.makedirs(go_path, exist_ok=True)
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_go(avro_path, go_path, package_name="jfrog_pipelines", json_annotation=True)
        self.verify_go_output(go_path, "jfrog_pipelines")
        
    def test_convert_jfrog_pipelines_jsons_to_avro_to_go_avro_annotations(self):
        """ Test converting a jfrog-pipelines.json file to go """
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        go_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines-go-avro")
        if os.path.exists(go_path):
            shutil.rmtree(go_path, ignore_errors=True)
        os.makedirs(go_path, exist_ok=True)            
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_go(avro_path, go_path, package_name="jfrog_pipelines", avro_annotation=True)
        self.verify_go_output(go_path, "jfrog_pipelines")
