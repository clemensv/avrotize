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

# Import GraphQL validation library if available
try:
    from graphql import parse, build_schema, validate_schema
    GRAPHQL_VALIDATION_AVAILABLE = True
except ImportError:
    GRAPHQL_VALIDATION_AVAILABLE = False
    print("Warning: graphql-core not installed. GraphQL syntax validation will be skipped.")
    print("Install with: pip install graphql-core")


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


def validate_graphql_syntax(graphql_content: str, file_name: str):
    """
    Validate that the generated GraphQL is syntactically correct.
    
    Args:
        graphql_content: The GraphQL schema content to validate
        file_name: Name of the file being validated (for error messages)
    
    Raises:
        AssertionError: If GraphQL syntax is invalid
    """
    if not GRAPHQL_VALIDATION_AVAILABLE:
        return
    
    try:
        # Parse the GraphQL schema to verify syntax
        # This validates that the SDL (Schema Definition Language) is syntactically correct
        document = parse(graphql_content)
        
        # We don't use build_schema() or validate_schema() because:
        # 1. Our generated schemas are type definitions only (no Query/Mutation root types)
        # 2. They're meant to be valid SDL that can be combined/extended, not standalone executable schemas
        # 3. Parsing alone validates syntax which is what we need
        
        # Verify we got a valid document with definitions
        if not document or not document.definitions:
            raise AssertionError(f"GraphQL document for {file_name} has no definitions")
            
    except Exception as e:
        if isinstance(e, AssertionError):
            raise
        raise AssertionError(f"GraphQL syntax validation failed for {file_name}: {str(e)}")


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
    
    # Read the generated GraphQL content
    with open(graphql_path, 'r', encoding="utf-8") as f:
        generated_content = f.read()
    
    # Validate GraphQL syntax
    validate_graphql_syntax(generated_content, file_base_name + ".graphql")
    
    # Compare with reference if it exists
    if os.path.exists(graphql_ref_path):
        with open(graphql_ref_path, 'r', encoding="utf-8") as file2:
            reference_content = file2.read()
        
        # Also validate reference GraphQL
        validate_graphql_syntax(reference_content, file_base_name + "-ref.graphql")
        
        assert generated_content == reference_content, f"Generated GraphQL does not match reference.\nGenerated:\n{generated_content}\n\nExpected:\n{reference_content}"
