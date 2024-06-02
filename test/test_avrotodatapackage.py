from unittest.mock import patch
import unittest
import os
import sys
import tempfile
from os import path, getcwd
import time

import pytest

from avrotize.avrotodatapackage import convert_avro_to_datapackage

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)


def test_convert_address_avsc_to_datapackage():
    """Test converting address.avsc to address.datapackage"""
    convert_case("address")

def test_convert_telemetry_avsc_to_datapackage():
    """Test converting address.avsc to address.datapackage"""
    convert_case("telemetry")
    
def test_convert_northwind_avsc_to_datapackage():
    """Test converting address.avsc to address.datapackage"""
    convert_case("northwind")

def convert_case(file_base_name: str):
    """Convert an Avro schema to datapackage query language"""
    cwd = os.getcwd()
    avro_path = os.path.join(cwd, "test", "avsc", file_base_name+".avsc")
    datapackage_path = os.path.join(tempfile.gettempdir(
    ), "avrotize", file_base_name+".datapackage")
    datapackage_ref_path = os.path.join(cwd, "test", "avsc", file_base_name+"-ref.datapackage")
    dir_name = os.path.dirname(datapackage_path)
    if not os.path.exists(dir_name):
        os.makedirs(dir_name, exist_ok=True)

    convert_avro_to_datapackage(avro_path, None, datapackage_path)
    if os.path.exists(datapackage_ref_path):
        with open(datapackage_path, 'r', encoding="utf-8") as file1, open(datapackage_ref_path, 'r', encoding="utf-8") as file2:
            content1 = file1.read()
            content2 = file2.read()
        assert content1 == content2
