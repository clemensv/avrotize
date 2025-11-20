import os
import sys
import tempfile
from os import path, getcwd

import pytest

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

import unittest
from avrotize.structuretoparquet import convert_structure_to_parquet


class TestStructureToParquet(unittest.TestCase):
    def test_convert_address_struct_to_parquet(self):
        """ Test converting a JSON Structure schema to Parquet"""
        cwd = os.getcwd()
        structure_path = os.path.join(cwd, "test", "jsons", "address-ref.struct.json")
        parquet_path = os.path.join(tempfile.gettempdir(), "avrotize", "address-struct.parquet")
        dir = os.path.dirname(parquet_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_structure_to_parquet(structure_path, None, parquet_path, False)
        assert os.path.exists(parquet_path)

    def test_convert_telemetry_struct_to_parquet(self):
        """ Test converting a telemetry JSON Structure schema to Parquet with CloudEvents columns"""
        cwd = os.getcwd()
        # First, let's check if there's a telemetry structure file, otherwise skip
        structure_path = os.path.join(cwd, "test", "jsons", "telemetry.struct.json")
        if not os.path.exists(structure_path):
            self.skipTest(f"Test file {structure_path} does not exist")
        
        parquet_path = os.path.join(tempfile.gettempdir(), "avrotize", "telemetry-struct.parquet")
        dir = os.path.dirname(parquet_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_structure_to_parquet(structure_path, None, parquet_path, True)
        assert os.path.exists(parquet_path)

    def test_convert_simple_types_struct_to_parquet(self):
        """ Test converting a JSON Structure schema with various primitive types"""
        cwd = os.getcwd()
        
        # Create a simple test structure with various types
        test_structure = {
            "type": "object",
            "name": "TestTypes",
            "properties": {
                "stringField": {"type": "string"},
                "intField": {"type": "int32"},
                "longField": {"type": "int64"},
                "floatField": {"type": "float"},
                "doubleField": {"type": "double"},
                "boolField": {"type": "boolean"},
                "binaryField": {"type": "binary"},
                "dateField": {"type": "date"},
                "datetimeField": {"type": "datetime"},
                "uuidField": {"type": "uuid"},
                "decimalField": {"type": "decimal", "precision": 10, "scale": 2}
            },
            "required": ["stringField", "intField"]
        }
        
        # Write test structure to temp file
        import json
        structure_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_types.struct.json")
        os.makedirs(os.path.dirname(structure_path), exist_ok=True)
        with open(structure_path, 'w') as f:
            json.dump(test_structure, f)
        
        parquet_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_types.parquet")
        
        convert_structure_to_parquet(structure_path, None, parquet_path, False)
        assert os.path.exists(parquet_path)

    def test_convert_nested_object_struct_to_parquet(self):
        """ Test converting a JSON Structure schema with nested objects"""
        cwd = os.getcwd()
        
        # Create a test structure with nested objects
        test_structure = {
            "type": "object",
            "name": "Person",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "int32"},
                "address": {
                    "type": "object",
                    "properties": {
                        "street": {"type": "string"},
                        "city": {"type": "string"},
                        "zipCode": {"type": "string"}
                    },
                    "required": ["city"]
                }
            },
            "required": ["name"]
        }
        
        # Write test structure to temp file
        import json
        structure_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_nested.struct.json")
        os.makedirs(os.path.dirname(structure_path), exist_ok=True)
        with open(structure_path, 'w') as f:
            json.dump(test_structure, f)
        
        parquet_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_nested.parquet")
        
        convert_structure_to_parquet(structure_path, None, parquet_path, False)
        assert os.path.exists(parquet_path)

    def test_convert_array_struct_to_parquet(self):
        """ Test converting a JSON Structure schema with arrays"""
        cwd = os.getcwd()
        
        # Create a test structure with arrays
        test_structure = {
            "type": "object",
            "name": "ArrayTest",
            "properties": {
                "tags": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "scores": {
                    "type": "array",
                    "items": {"type": "int32"}
                }
            },
            "required": []
        }
        
        # Write test structure to temp file
        import json
        structure_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_array.struct.json")
        os.makedirs(os.path.dirname(structure_path), exist_ok=True)
        with open(structure_path, 'w') as f:
            json.dump(test_structure, f)
        
        parquet_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_array.parquet")
        
        convert_structure_to_parquet(structure_path, None, parquet_path, False)
        assert os.path.exists(parquet_path)

    def test_convert_map_struct_to_parquet(self):
        """ Test converting a JSON Structure schema with maps"""
        cwd = os.getcwd()
        
        # Create a test structure with maps
        test_structure = {
            "type": "object",
            "name": "MapTest",
            "properties": {
                "metadata": {
                    "type": "map",
                    "values": {"type": "string"}
                },
                "counts": {
                    "type": "map",
                    "values": {"type": "int32"}
                }
            },
            "required": []
        }
        
        # Write test structure to temp file
        import json
        structure_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_map.struct.json")
        os.makedirs(os.path.dirname(structure_path), exist_ok=True)
        with open(structure_path, 'w') as f:
            json.dump(test_structure, f)
        
        parquet_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_map.parquet")
        
        convert_structure_to_parquet(structure_path, None, parquet_path, False)
        assert os.path.exists(parquet_path)

    def test_convert_union_struct_to_parquet(self):
        """ Test converting a JSON Structure schema with union types"""
        cwd = os.getcwd()
        
        # Create a test structure with union types
        test_structure = {
            "type": "object",
            "name": "UnionTest",
            "properties": {
                "optionalString": ["null", "string"],
                "stringOrInt": ["string", "int32"]
            },
            "required": []
        }
        
        # Write test structure to temp file
        import json
        structure_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_union.struct.json")
        os.makedirs(os.path.dirname(structure_path), exist_ok=True)
        with open(structure_path, 'w') as f:
            json.dump(test_structure, f)
        
        parquet_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_union.parquet")
        
        convert_structure_to_parquet(structure_path, None, parquet_path, False)
        assert os.path.exists(parquet_path)


if __name__ == '__main__':
    unittest.main()
