"""Tests for JSON Structure to Data Package conversion."""
from unittest.mock import patch
import unittest
import os
import sys
import tempfile
import json
from os import path, getcwd

import pytest
from datapackage import Package

from avrotize.structuretodatapackage import convert_structure_to_datapackage

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)


def test_convert_address_struct_to_datapackage():
    """Test converting address JSON Structure to datapackage"""
    convert_case("address-ref")


def test_convert_employee_struct_to_datapackage():
    """Test converting employee JSON Structure to datapackage"""
    convert_case("employee-ref")


def test_convert_arraydef_struct_to_datapackage():
    """Test converting arraydef JSON Structure to datapackage"""
    convert_case("arraydef-ref")


def test_convert_simple_types_struct_to_datapackage():
    """Test converting schema with various simple types"""
    # Create a test schema with various JSON Structure types
    test_schema = {
        "type": "object",
        "name": "TestTypes",
        "properties": {
            "stringField": {"type": "string"},
            "intField": {"type": "int32"},
            "floatField": {"type": "float"},
            "boolField": {"type": "boolean"},
            "dateField": {"type": "date"},
            "datetimeField": {"type": "datetime"},
            "uuidField": {"type": "uuid"},
            "binaryField": {"type": "binary"}
        },
        "required": ["stringField", "intField"]
    }
    
    # Write test schema
    cwd = os.getcwd()
    temp_dir = tempfile.gettempdir()
    test_schema_path = os.path.join(temp_dir, "avrotize", "test_types.struct.json")
    datapackage_path = os.path.join(temp_dir, "avrotize", "test_types.datapackage.json")
    
    os.makedirs(os.path.dirname(test_schema_path), exist_ok=True)
    
    with open(test_schema_path, 'w', encoding='utf-8') as f:
        json.dump(test_schema, f, indent=2)
    
    # Convert
    convert_structure_to_datapackage(test_schema_path, None, datapackage_path)
    
    # Verify output
    assert os.path.exists(datapackage_path)
    
    with open(datapackage_path, 'r', encoding='utf-8') as f:
        result = json.load(f)
    
    # Check basic structure
    assert 'resources' in result
    assert len(result['resources']) > 0
    
    # Check first resource
    resource = result['resources'][0]
    assert 'schema' in resource
    assert 'fields' in resource['schema']
    
    # Check field types
    fields = {f['name']: f['type'] for f in resource['schema']['fields']}
    assert fields['stringField'] == 'string'
    assert fields['intField'] == 'integer'
    assert fields['floatField'] == 'number'
    assert fields['boolField'] == 'boolean'
    assert fields['dateField'] == 'date'
    assert fields['datetimeField'] == 'datetime'
    assert fields['uuidField'] == 'string'
    assert fields['binaryField'] == 'string'


def test_convert_with_constraints():
    """Test converting schema with constraints and annotations"""
    test_schema = {
        "type": "object",
        "name": "ConstrainedType",
        "properties": {
            "limitedString": {
                "type": "string",
                "maxLength": 100,
                "minLength": 5,
                "pattern": "^[A-Z].*"
            },
            "rangedNumber": {
                "type": "int32",
                "minimum": 0,
                "maximum": 100
            },
            "enumField": {
                "type": "string",
                "enum": ["option1", "option2", "option3"]
            }
        }
    }
    
    # Write and convert
    temp_dir = tempfile.gettempdir()
    test_schema_path = os.path.join(temp_dir, "avrotize", "constrained.struct.json")
    datapackage_path = os.path.join(temp_dir, "avrotize", "constrained.datapackage.json")
    
    os.makedirs(os.path.dirname(test_schema_path), exist_ok=True)
    
    with open(test_schema_path, 'w', encoding='utf-8') as f:
        json.dump(test_schema, f, indent=2)
    
    convert_structure_to_datapackage(test_schema_path, None, datapackage_path)
    
    # Verify constraints
    with open(datapackage_path, 'r', encoding='utf-8') as f:
        result = json.load(f)
    
    fields = {f['name']: f for f in result['resources'][0]['schema']['fields']}
    
    # Check string constraints
    limited_string = fields['limitedString']
    assert 'constraints' in limited_string
    assert limited_string['constraints']['maxLength'] == 100
    assert limited_string['constraints']['minLength'] == 5
    assert limited_string['constraints']['pattern'] == "^[A-Z].*"
    
    # Check numeric constraints
    ranged_number = fields['rangedNumber']
    assert 'constraints' in ranged_number
    assert ranged_number['constraints']['minimum'] == 0
    assert ranged_number['constraints']['maximum'] == 100
    
    # Check enum
    enum_field = fields['enumField']
    assert 'constraints' in enum_field
    assert enum_field['constraints']['enum'] == ["option1", "option2", "option3"]


def test_convert_with_nested_object():
    """Test converting schema with nested objects"""
    test_schema = {
        "type": "object",
        "name": "Parent",
        "properties": {
            "name": {"type": "string"},
            "nested": {
                "type": "object",
                "properties": {
                    "field1": {"type": "string"},
                    "field2": {"type": "int32"}
                }
            }
        }
    }
    
    temp_dir = tempfile.gettempdir()
    test_schema_path = os.path.join(temp_dir, "avrotize", "nested.struct.json")
    datapackage_path = os.path.join(temp_dir, "avrotize", "nested.datapackage.json")
    
    os.makedirs(os.path.dirname(test_schema_path), exist_ok=True)
    
    with open(test_schema_path, 'w', encoding='utf-8') as f:
        json.dump(test_schema, f, indent=2)
    
    convert_structure_to_datapackage(test_schema_path, None, datapackage_path)
    
    # Verify output - nested objects become 'object' type
    with open(datapackage_path, 'r', encoding='utf-8') as f:
        result = json.load(f)
    
    fields = {f['name']: f for f in result['resources'][0]['schema']['fields']}
    assert fields['nested']['type'] == 'object'


def test_convert_with_array():
    """Test converting schema with array types"""
    test_schema = {
        "type": "object",
        "name": "WithArray",
        "properties": {
            "stringArray": {
                "type": "array",
                "items": {"type": "string"}
            },
            "numberSet": {
                "type": "set",
                "items": {"type": "int32"}
            }
        }
    }
    
    temp_dir = tempfile.gettempdir()
    test_schema_path = os.path.join(temp_dir, "avrotize", "array.struct.json")
    datapackage_path = os.path.join(temp_dir, "avrotize", "array.datapackage.json")
    
    os.makedirs(os.path.dirname(test_schema_path), exist_ok=True)
    
    with open(test_schema_path, 'w', encoding='utf-8') as f:
        json.dump(test_schema, f, indent=2)
    
    convert_structure_to_datapackage(test_schema_path, None, datapackage_path)
    
    # Verify arrays
    with open(datapackage_path, 'r', encoding='utf-8') as f:
        result = json.load(f)
    
    fields = {f['name']: f for f in result['resources'][0]['schema']['fields']}
    assert fields['stringArray']['type'] == 'array'
    assert fields['numberSet']['type'] == 'array'  # Sets map to arrays


def test_convert_with_union_types():
    """Test converting schema with union types (nullable)"""
    test_schema = {
        "type": "object",
        "name": "WithUnion",
        "properties": {
            "nullableString": ["null", "string"],
            "nullableInt": ["int32", "null"]
        }
    }
    
    temp_dir = tempfile.gettempdir()
    test_schema_path = os.path.join(temp_dir, "avrotize", "union.struct.json")
    datapackage_path = os.path.join(temp_dir, "avrotize", "union.datapackage.json")
    
    os.makedirs(os.path.dirname(test_schema_path), exist_ok=True)
    
    with open(test_schema_path, 'w', encoding='utf-8') as f:
        json.dump(test_schema, f, indent=2)
    
    convert_structure_to_datapackage(test_schema_path, None, datapackage_path)
    
    # Verify unions resolve to base types
    with open(datapackage_path, 'r', encoding='utf-8') as f:
        result = json.load(f)
    
    fields = {f['name']: f for f in result['resources'][0]['schema']['fields']}
    assert fields['nullableString']['type'] == 'string'
    assert fields['nullableInt']['type'] == 'integer'


def test_convert_with_definitions():
    """Test converting schema with definitions"""
    test_schema = {
        "definitions": {
            "Address": {
                "type": "object",
                "name": "Address",
                "properties": {
                    "street": {"type": "string"},
                    "city": {"type": "string"}
                }
            },
            "Person": {
                "type": "object",
                "name": "Person",
                "properties": {
                    "name": {"type": "string"},
                    "age": {"type": "int32"}
                }
            }
        }
    }
    
    temp_dir = tempfile.gettempdir()
    test_schema_path = os.path.join(temp_dir, "avrotize", "definitions.struct.json")
    datapackage_path = os.path.join(temp_dir, "avrotize", "definitions.datapackage.json")
    
    os.makedirs(os.path.dirname(test_schema_path), exist_ok=True)
    
    with open(test_schema_path, 'w', encoding='utf-8') as f:
        json.dump(test_schema, f, indent=2)
    
    convert_structure_to_datapackage(test_schema_path, None, datapackage_path)
    
    # Verify both types are extracted
    with open(datapackage_path, 'r', encoding='utf-8') as f:
        result = json.load(f)
    
    assert 'resources' in result
    assert len(result['resources']) == 2
    resource_names = {r['name'] for r in result['resources']}
    assert 'Address' in resource_names
    assert 'Person' in resource_names


def test_convert_with_namespace():
    """Test converting schema with namespace"""
    test_schema = {
        "type": "object",
        "name": "Document",
        "namespace": "com.example.schema",
        "properties": {
            "title": {"type": "string"},
            "content": {"type": "string"}
        }
    }
    
    temp_dir = tempfile.gettempdir()
    test_schema_path = os.path.join(temp_dir, "avrotize", "namespace.struct.json")
    datapackage_path = os.path.join(temp_dir, "avrotize", "namespace.datapackage.json")
    
    os.makedirs(os.path.dirname(test_schema_path), exist_ok=True)
    
    with open(test_schema_path, 'w', encoding='utf-8') as f:
        json.dump(test_schema, f, indent=2)
    
    convert_structure_to_datapackage(test_schema_path, None, datapackage_path)
    
    # Verify namespace handling
    with open(datapackage_path, 'r', encoding='utf-8') as f:
        result = json.load(f)
    
    assert result['name'] == 'com.example.schema'


def convert_case(file_base_name: str):
    """Convert a JSON Structure schema to datapackage"""
    cwd = os.getcwd()
    struct_path = os.path.join(cwd, "test", "jsons", file_base_name + ".struct.json")
    datapackage_path = os.path.join(
        tempfile.gettempdir(), "avrotize", file_base_name + ".datapackage")
    datapackage_ref_path = os.path.join(cwd, "test", "jsons", file_base_name + "-ref.datapackage")
    dir_name = os.path.dirname(datapackage_path)
    if not os.path.exists(dir_name):
        os.makedirs(dir_name, exist_ok=True)

    convert_structure_to_datapackage(struct_path, None, datapackage_path)
    
    # Verify file was created
    assert os.path.exists(datapackage_path)
    
    # Verify it's valid JSON
    with open(datapackage_path, 'r', encoding='utf-8') as f:
        result = json.load(f)
    
    # Validate using datapackage library
    try:
        package = Package(result)
        assert package.valid, f"Data Package validation failed: {package.errors if hasattr(package, 'errors') else 'Unknown error'}"
    except Exception as e:
        pytest.fail(f"Data Package validation error: {e}")
    
    # Compare with reference file if it exists
    if os.path.exists(datapackage_ref_path):
        with open(datapackage_path, 'r', encoding="utf-8") as file1, open(datapackage_ref_path, 'r', encoding="utf-8") as file2:
            content1 = file1.read()
            content2 = file2.read()
        assert content1 == content2, f"Generated output does not match reference file for {file_base_name}"
    else:
        # Basic structure checks if no reference file
        assert 'resources' in result
        if result['resources']:  # If there are resources
            assert 'schema' in result['resources'][0]
            assert 'fields' in result['resources'][0]['schema']


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
