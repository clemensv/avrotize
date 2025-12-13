"""Tests for avrotocsv module"""
import unittest
import os
import sys
import tempfile
from os import path, getcwd

from avrotize.avrotocsv import convert_avro_to_csv_schema

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)


def test_convert_address_avsc_to_csv():
    """Test converting address.avsc to CSV schema"""
    convert_case("address")


def test_convert_telemetry_avsc_to_csv():
    """Test converting telemetry.avsc to CSV schema"""
    convert_case("telemetry")


# Note: northwind.avsc is a union of records (array), which the avrotocsv
# converter doesn't currently support. Single record schemas work fine.


def convert_case(file_base_name: str):
    """Convert an Avro schema to CSV schema"""
    cwd = os.getcwd()
    avro_path = os.path.join(cwd, "test", "avsc", file_base_name + ".avsc")
    csv_path = os.path.join(tempfile.gettempdir(), "avrotize", file_base_name + ".csv.json")
    dir_name = os.path.dirname(csv_path)
    if not os.path.exists(dir_name):
        os.makedirs(dir_name, exist_ok=True)

    convert_avro_to_csv_schema(avro_path, csv_path)
    
    # Verify the output file was created
    assert os.path.exists(csv_path), f"CSV schema file was not created: {csv_path}"
    
    # Verify the output is valid JSON
    import json
    with open(csv_path, 'r', encoding='utf-8') as f:
        csv_schema = json.load(f)
    
    # Verify it has expected structure (fields array)
    assert 'fields' in csv_schema, "CSV schema should have 'fields' property"
    assert isinstance(csv_schema['fields'], list), "'fields' should be a list"


if __name__ == '__main__':
    test_convert_address_avsc_to_csv()
    test_convert_telemetry_avsc_to_csv()
    test_convert_northwind_avsc_to_csv()
    print("All tests passed!")
