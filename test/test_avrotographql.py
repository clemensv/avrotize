from unittest.mock import patch
import unittest
import os
import sys
import tempfile
from os import path, getcwd
import time

import pytest

from avrotize.avrotographql import convert_avro_to_graphql

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)


def test_convert_address_avsc_to_graphql():
    """Test converting address.avsc to address.graphql"""
    convert_case("address")

def test_convert_telemetry_avsc_to_graphql():
    """Test converting address.avsc to address.graphql"""
    convert_case("telemetry")
    
def test_convert_northwind_avsc_to_graphql():
    """Test converting address.avsc to address.graphql"""
    convert_case("northwind")

def convert_case(file_base_name: str):
    """Convert an Avro schema to graphql query language"""
    cwd = os.getcwd()
    avro_path = os.path.join(cwd, "test", "avsc", file_base_name+".avsc")
    graphql_path = os.path.join(tempfile.gettempdir(
    ), "avrotize", file_base_name+".graphql")
    graphql_ref_path = os.path.join(cwd, "test", "avsc", file_base_name+"-avro-ref.graphql")
    dir_name = os.path.dirname(graphql_path)
    if not os.path.exists(dir_name):
        os.makedirs(dir_name, exist_ok=True)

    convert_avro_to_graphql(
        avro_path, graphql_path)
    if os.path.exists(graphql_ref_path):
        with open(graphql_path, 'r', encoding="utf-8") as file1, open(graphql_ref_path, 'r', encoding="utf-8") as file2:
            content1 = file1.read()
            content2 = file2.read()
        assert content1 == content2
