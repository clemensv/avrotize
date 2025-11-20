from unittest.mock import patch
import unittest
import os
import sys
import tempfile
from os import path, getcwd
import time

import pytest

from avrotize.structuretographql import convert_structure_to_graphql

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)


def test_convert_address_struct_to_graphql():
    """Test converting address.struct.json to address.graphql"""
    convert_case("address")


def test_convert_telemetry_struct_to_graphql():
    """Test converting telemetry.struct.json to telemetry.graphql"""
    convert_case("telemetry")


def test_convert_primitives_struct_to_graphql():
    """Test converting primitives.struct.json to primitives.graphql - tests all JSON primitive types"""
    convert_case("primitives")


def test_convert_extendedprimitives_struct_to_graphql():
    """Test converting extendedprimitives.struct.json to extendedprimitives.graphql - tests all extended primitive types"""
    convert_case("extendedprimitives")


def test_convert_compounds_struct_to_graphql():
    """Test converting compounds.struct.json to compounds.graphql - tests compound types (array, set, map, nested objects)"""
    convert_case("compounds")


def test_convert_enums_struct_to_graphql():
    """Test converting enums.struct.json to enums.graphql - tests enum types"""
    convert_case("enums")


def test_convert_enumfield_struct_to_graphql():
    """Test converting enumfield.struct.json to enumfield.graphql - tests enum field references"""
    convert_case("enumfield")


def test_convert_complexunion_struct_to_graphql():
    """Test converting complexunion.struct.json to complexunion.graphql - tests complex union types"""
    convert_case("complexunion")


def test_convert_northwind_struct_to_graphql():
    """Test converting northwind.struct.json to northwind.graphql - tests real-world complex schema"""
    convert_case("northwind")


def convert_case(file_base_name: str):
    """Convert a JSON Structure schema to GraphQL schema"""
    cwd = os.getcwd()
    struct_path = os.path.join(cwd, "test", "avsc", file_base_name+"-ref.struct.json")
    graphql_path = os.path.join(tempfile.gettempdir(
    ), "avrotize", file_base_name+".graphql")
    graphql_ref_path = os.path.join(cwd, "test", "avsc", file_base_name+"-ref.graphql")
    dir_name = os.path.dirname(graphql_path)
    if not os.path.exists(dir_name):
        os.makedirs(dir_name, exist_ok=True)

    convert_structure_to_graphql(
        struct_path, graphql_path)
    
    if os.path.exists(graphql_ref_path):
        with open(graphql_path, 'r', encoding="utf-8") as file1, open(graphql_ref_path, 'r', encoding="utf-8") as file2:
            content1 = file1.read()
            content2 = file2.read()
        assert content1 == content2, f"Generated GraphQL does not match reference.\nGenerated:\n{content1}\n\nExpected:\n{content2}"
