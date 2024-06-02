from unittest.mock import patch
import unittest
import os
import sys
import tempfile
from os import path, getcwd
import time

import pytest

from avrotize.avrotomd import convert_avro_to_markdown

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)


def test_convert_address_avsc_to_md():
    """Test converting address.avsc to address.md"""
    convert_case("address")

def test_convert_telemetry_avsc_to_md():
    """Test converting address.avsc to address.md"""
    convert_case("telemetry")
    
def test_convert_northwind_avsc_to_md():
    """Test converting address.avsc to address.md"""
    convert_case("northwind")

def convert_case(file_base_name: str):
    """Convert an Avro schema to Markdown """
    cwd = os.getcwd()
    avro_path = os.path.join(cwd, "test", "avsc", file_base_name+".avsc")
    md_path = os.path.join(tempfile.gettempdir(
    ), "avrotize", file_base_name+".md")
    md_ref_path = os.path.join(cwd, "test", "avsc", file_base_name+"-ref.md")
    dir_name = os.path.dirname(md_path)
    if not os.path.exists(dir_name):
        os.makedirs(dir_name, exist_ok=True)

    convert_avro_to_markdown(
        avro_path, md_path)
    if os.path.exists(md_ref_path):
        with open(md_path, 'r', encoding="utf-8") as file1, open(md_ref_path, 'r', encoding="utf-8") as file2:
            content1 = file1.read()
            content2 = file2.read()
        assert content1 == content2
