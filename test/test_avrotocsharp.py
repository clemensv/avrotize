import os
import shutil
import sys
from os import path, getcwd

import pytest

from avrotize.avrotocsharp import convert_avro_to_csharp
from avrotize.jsonstoavro import convert_jsons_to_avro

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

import unittest
from unittest.mock import patch

class TestAvroToCSharp(unittest.TestCase):
    def test_convert_address_avsc_to_csharp(self):
        """ Test converting an address.avsc file to C# """
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        cs_path = os.path.join(cwd, "test", "tmp", "address-cs")
        if os.path.exists(cs_path):
            shutil.rmtree(cs_path)
        os.makedirs(cs_path)
        
        convert_avro_to_csharp(avro_path, cs_path)
        
    def test_convert_address_avsc_to_csharp_avro_annotation(self):
        """ Test converting an address.avsc file to C# """
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        cs_path = os.path.join(cwd, "test", "tmp", "address-cs-avro")
        if os.path.exists(cs_path):
            shutil.rmtree(cs_path)
        os.makedirs(cs_path)
        
        convert_avro_to_csharp(avro_path, cs_path, avro_annotation=True)
        
    def test_convert_address_avsc_to_csharp_system_text_json_annotation(self):
        """ Test converting an address.avsc file to C# """
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        cs_path = os.path.join(cwd, "test", "tmp", "address-cs-stj")
        if os.path.exists(cs_path):
            shutil.rmtree(cs_path)
        os.makedirs(cs_path)
                
        convert_avro_to_csharp(avro_path, cs_path, system_text_json_annotation=True, pascal_properties=True)
        
    def test_convert_address_avsc_to_csharp_newtonsoft_json_annotation(self):
        """ Test converting an address.avsc file to C# """
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        cs_path = os.path.join(cwd, "test", "tmp", "address-cs-nj")
        if os.path.exists(cs_path):
            shutil.rmtree(cs_path)
        os.makedirs(cs_path)
                
        convert_avro_to_csharp(avro_path, cs_path, newtonsoft_json_annotation=True, pascal_properties=True)
        

    def test_convert_telemetry_avsc_to_csharp(self):
        """ Test converting a telemetry.avsc file to C# """
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", "telemetry.avsc")
        cs_path = os.path.join(cwd, "test", "tmp", "telemetry-cs")
        if os.path.exists(cs_path):
            shutil.rmtree(cs_path)
        os.makedirs(cs_path)
        
        convert_avro_to_csharp(avro_path, cs_path)

    def test_convert_jfrog_pipelines_jsons_to_avro_to_csharp(self):
        """ Test converting a jfrog-pipelines.json file to C# """
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(cwd, "test", "tmp", "jfrog-pipelines.avsc")
        cs_path = path.join(cwd, "test", "tmp", "jfrog-pipelines-cs")
        if os.path.exists(cs_path):
            shutil.rmtree(cs_path)
        os.makedirs(cs_path)
            
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_csharp(avro_path, cs_path)
        
    def test_convert_jfrog_pipelines_jsons_to_avro_to_csharp_annotated(self):
        """ Test converting a jfrog-pipelines.json file to C# """
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(cwd, "test", "tmp", "jfrog-pipelines.avsc")
        cs_path = path.join(cwd, "test", "tmp", "jfrog-pipelines-cs-ann")
        if os.path.exists(cs_path):
            shutil.rmtree(cs_path)
        os.makedirs(cs_path)
            
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_csharp(avro_path, cs_path, pascal_properties=True, avro_annotation=True, system_text_json_annotation=True, newtonsoft_json_annotation=True)