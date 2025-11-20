"""Tests for structuretokusto module"""
from avrotize.structuretokusto import convert_structure_to_kusto_file
import unittest
import os
import sys
import tempfile

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)


def test_convert_address_struct_to_kusto():
    """Test converting address.struct.json to address.kql"""
    convert_case("address", False, False)


def test_convert_address_struct_to_kusto_ce():
    """Test converting address.struct.json to address-ce.kql"""
    convert_case("address", True, False)


def test_convert_address_struct_to_kusto_ce_dt():
    """Test converting address.struct.json to address-ce-dt.kql"""
    convert_case("address", True, True)


def convert_case(file_base_name: str, emit_cloudevents_columns, emit_cloudevents_dispatch_table):
    """Convert a JSON Structure schema to Kusto query language"""
    cwd = os.getcwd()
    struct_path = os.path.join(cwd, "test", "avsc", file_base_name+"-ref.struct.json")
    kql_path = os.path.join(tempfile.gettempdir(
    ), "avrotize", file_base_name+('-ce' if emit_cloudevents_columns else '')+('-dt' if emit_cloudevents_dispatch_table else '')+"-struct.kql")
    kql_ref_path = os.path.join(cwd, "test", "avsc", file_base_name +
                                ('-ce' if emit_cloudevents_columns else '')+('-dt' if emit_cloudevents_dispatch_table else '')+"-struct-ref.kql")
    dir_name = os.path.dirname(kql_path)
    if not os.path.exists(dir_name):
        os.makedirs(dir_name, exist_ok=True)

    convert_structure_to_kusto_file(
        struct_path, None, kql_path, emit_cloudevents_columns, emit_cloudevents_dispatch_table)
    
    # Compare with reference file if it exists
    if os.path.exists(kql_ref_path):
        with open(kql_path, 'r', encoding="utf-8") as file1, open(kql_ref_path, 'r', encoding="utf-8") as file2:
            content1 = file1.read()
            content2 = file2.read()
        assert content1 == content2


if __name__ == '__main__':
    test_convert_address_struct_to_kusto()
    test_convert_address_struct_to_kusto_ce()
    test_convert_address_struct_to_kusto_ce_dt()
    print("All tests passed!")
