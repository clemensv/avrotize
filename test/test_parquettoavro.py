#  pylint: disable=missing-function-docstring, wrong-import-order, wrong-import-position, missing-module-docstring, missing-class-docstring

import os
import sys
import tempfile
from os import path, getcwd

from avrotize.jsonstoavro import convert_jsons_to_avro

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

import unittest
from avrotize.avrotoparquet import convert_avro_to_parquet
from avrotize.parquettoavro import convert_parquet_to_avro

class TestParquetToAvro(unittest.TestCase):
    def test_convert_address_avsc_to_parquet_and_back(self):
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        parquet_path = os.path.join(tempfile.gettempdir(), "avrotize", "address.parquet")
        avro_output_path = os.path.join(tempfile.gettempdir(), "avrotize", "address.avsc")
        dir_path = os.path.dirname(parquet_path)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
        
        convert_avro_to_parquet(avro_path, None, parquet_path, False)           
        convert_parquet_to_avro(parquet_path, avro_output_path)

    def test_convert_telemetry_avsc_to_parquet_and_back(self):
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", "telemetry.avsc")
        parquet_path = os.path.join(tempfile.gettempdir(), "avrotize", "telemetry.parquet")
        avro_output_path = os.path.join(tempfile.gettempdir(), "avrotize", "telemetry.avsc")
        dir_path = os.path.dirname(parquet_path)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
        
        convert_avro_to_parquet(avro_path, None, parquet_path, True)
        convert_parquet_to_avro(parquet_path, avro_output_path)
        
    def test_convert_fileblob_to_parquet_and_back(self):
        """ Test converting a fileblob.avsc file to Parquet"""
        cwd = getcwd()        
        avro_path = path.join(cwd, "test", "avsc", "fileblob.avsc")
        parquet_path = path.join(tempfile.gettempdir(), "avrotize", "fileblob.parquet")
        avro_output_path = path.join(tempfile.gettempdir(), "avrotize", "fileblob.avsc")
        dir_path = os.path.dirname(parquet_path)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
        
        convert_avro_to_parquet(avro_path, None, parquet_path, False)
        convert_parquet_to_avro(parquet_path, avro_output_path)
        
    
    def test_convert_jfrog_pipelines_jsons_to_avro_to_parquet_and_back(self):
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        parquet_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.parquet")
        avro_output_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines-from-parquet.avsc")
        dir_path = os.path.dirname(avro_path)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_parquet(avro_path, "HelmBlueGreenDeploy", parquet_path, True)
        convert_parquet_to_avro(parquet_path, avro_output_path)
