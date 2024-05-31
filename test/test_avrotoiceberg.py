"""Test cases for converting Avro to Iceberg schemas"""

import os
import sys
import tempfile
from os import path, getcwd

import pytest
import unittest
from unittest.mock import patch

# Ensure the project root is in the system path for imports
current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

from avrotize.jsonstoavro import convert_jsons_to_avro
from avrotize.avrotoiceberg import convert_avro_to_iceberg

class TestAvroToIceberg(unittest.TestCase):
    """Test cases for converting Avro to Iceberg schemas"""
    def test_convert_address_avsc_to_iceberg(self):
        """Test converting an Avro schema to Iceberg"""
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        iceberg_path = os.path.join(tempfile.gettempdir(), "avrotize", "address.iceberg")
        dir = os.path.dirname(iceberg_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_avro_to_iceberg(avro_path, None, iceberg_path, False)

    def test_convert_telemetry_avsc_to_iceberg(self):
        """Test converting a telemetry.avsc file to Iceberg"""
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "telemetry.avsc")
        iceberg_path = os.path.join(tempfile.gettempdir(), "avrotize", "telemetry.iceberg")
        dir = os.path.dirname(iceberg_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_avro_to_iceberg(avro_path, None, iceberg_path, True)
        
    def test_convert_fileblob_to_iceberg(self):
        """Test converting a fileblob.avsc file to Iceberg"""
        cwd = getcwd()
        avro_path = path.join(cwd, "test", "avsc", "fileblob.avsc")
        iceberg_path = path.join(tempfile.gettempdir(), "avrotize", "fileblob.iceberg")
        dir = os.path.dirname(iceberg_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_avro_to_iceberg(avro_path, None, iceberg_path, False)
    
    def test_convert_jfrog_pipelines_jsons_to_avro_to_iceberg(self):
        """Test converting JFrog Pipelines JSONs to Avro and Iceberg schemas"""
        cwd = getcwd()
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        iceberg_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.iceberg")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_iceberg(avro_path, "HelmBlueGreenDeploy", iceberg_path, True)

if __name__ == '__main__':
    unittest.main()
