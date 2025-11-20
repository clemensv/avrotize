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
        struct_path = os.path.join(cwd, "test", "avsc", "address-ref.struct.json")
        parquet_path = os.path.join(tempfile.gettempdir(), "avrotize", "address-struct.parquet")
        dir = os.path.dirname(parquet_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_structure_to_parquet(struct_path, None, parquet_path, False)       

    def test_convert_primitives_struct_to_parquet(self):
        """ Test converting a JSON Structure schema with primitive types to Parquet"""
        # Create a test schema with various primitive types
        test_schema = {
            "type": "object",
            "name": "PrimitiveTypes",
            "properties": {
                "stringField": {"type": "string"},
                "intField": {"type": "int32"},
                "longField": {"type": "int64"},
                "floatField": {"type": "float"},
                "doubleField": {"type": "double"},
                "booleanField": {"type": "boolean"},
                "binaryField": {"type": "binary"},
                "dateField": {"type": "date"},
                "timeField": {"type": "time"},
                "datetimeField": {"type": "datetime"},
                "uuidField": {"type": "uuid"},
                "decimalField": {"type": "decimal", "precision": 10, "scale": 2}
            },
            "required": ["stringField", "intField"]
        }
        
        # Write schema to temp file
        import json
        cwd = os.getcwd()
        schema_path = os.path.join(tempfile.gettempdir(), "avrotize", "primitives.struct.json")
        parquet_path = os.path.join(tempfile.gettempdir(), "avrotize", "primitives.parquet")
        dir = os.path.dirname(schema_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        with open(schema_path, 'w') as f:
            json.dump(test_schema, f)
        
        convert_structure_to_parquet(schema_path, None, parquet_path, False)

    def test_convert_nested_object_struct_to_parquet(self):
        """ Test converting a JSON Structure schema with nested objects to Parquet"""
        test_schema = {
            "type": "object",
            "name": "NestedObject",
            "properties": {
                "id": {"type": "string"},
                "address": {
                    "type": "object",
                    "properties": {
                        "street": {"type": "string"},
                        "city": {"type": "string"},
                        "zipCode": {"type": "string"}
                    }
                }
            }
        }
        
        import json
        schema_path = os.path.join(tempfile.gettempdir(), "avrotize", "nested.struct.json")
        parquet_path = os.path.join(tempfile.gettempdir(), "avrotize", "nested.parquet")
        dir = os.path.dirname(schema_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        with open(schema_path, 'w') as f:
            json.dump(test_schema, f)
        
        convert_structure_to_parquet(schema_path, None, parquet_path, False)

    def test_convert_array_struct_to_parquet(self):
        """ Test converting a JSON Structure schema with arrays to Parquet"""
        test_schema = {
            "type": "object",
            "name": "ArrayTypes",
            "properties": {
                "tags": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "numbers": {
                    "type": "array",
                    "items": {"type": "int32"}
                }
            }
        }
        
        import json
        schema_path = os.path.join(tempfile.gettempdir(), "avrotize", "arrays.struct.json")
        parquet_path = os.path.join(tempfile.gettempdir(), "avrotize", "arrays.parquet")
        dir = os.path.dirname(schema_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        with open(schema_path, 'w') as f:
            json.dump(test_schema, f)
        
        convert_structure_to_parquet(schema_path, None, parquet_path, False)

    def test_convert_map_struct_to_parquet(self):
        """ Test converting a JSON Structure schema with maps to Parquet"""
        test_schema = {
            "type": "object",
            "name": "MapTypes",
            "properties": {
                "metadata": {
                    "type": "map",
                    "values": {"type": "string"}
                },
                "counts": {
                    "type": "map",
                    "values": {"type": "int32"}
                }
            }
        }
        
        import json
        schema_path = os.path.join(tempfile.gettempdir(), "avrotize", "maps.struct.json")
        parquet_path = os.path.join(tempfile.gettempdir(), "avrotize", "maps.parquet")
        dir = os.path.dirname(schema_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        with open(schema_path, 'w') as f:
            json.dump(test_schema, f)
        
        convert_structure_to_parquet(schema_path, None, parquet_path, False)

    def test_convert_optional_fields_struct_to_parquet(self):
        """ Test converting a JSON Structure schema with optional fields to Parquet"""
        test_schema = {
            "type": "object",
            "name": "OptionalFields",
            "properties": {
                "requiredField": {"type": "string"},
                "optionalField": {"type": ["null", "string"]},
                "optionalIntField": {"type": ["null", "int32"]}
            },
            "required": ["requiredField"]
        }
        
        import json
        schema_path = os.path.join(tempfile.gettempdir(), "avrotize", "optional.struct.json")
        parquet_path = os.path.join(tempfile.gettempdir(), "avrotize", "optional.parquet")
        dir = os.path.dirname(schema_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        with open(schema_path, 'w') as f:
            json.dump(test_schema, f)
        
        convert_structure_to_parquet(schema_path, None, parquet_path, False)

    def test_convert_with_cloudevents_columns(self):
        """ Test converting with CloudEvents columns"""
        test_schema = {
            "type": "object",
            "name": "SimpleRecord",
            "properties": {
                "id": {"type": "string"},
                "value": {"type": "int32"}
            }
        }
        
        import json
        schema_path = os.path.join(tempfile.gettempdir(), "avrotize", "cloudevents.struct.json")
        parquet_path = os.path.join(tempfile.gettempdir(), "avrotize", "cloudevents.parquet")
        dir = os.path.dirname(schema_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        with open(schema_path, 'w') as f:
            json.dump(test_schema, f)
        
        convert_structure_to_parquet(schema_path, None, parquet_path, True)

    def test_convert_with_root_reference(self):
        """ Test converting with $root reference"""
        test_schema = {
            "$root": "#/definitions/MyRecord",
            "definitions": {
                "MyRecord": {
                    "type": "object",
                    "name": "MyRecord",
                    "properties": {
                        "field1": {"type": "string"},
                        "field2": {"type": "int32"}
                    }
                }
            }
        }
        
        import json
        schema_path = os.path.join(tempfile.gettempdir(), "avrotize", "root_ref.struct.json")
        parquet_path = os.path.join(tempfile.gettempdir(), "avrotize", "root_ref.parquet")
        dir = os.path.dirname(schema_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        with open(schema_path, 'w') as f:
            json.dump(test_schema, f)
        
        convert_structure_to_parquet(schema_path, None, parquet_path, False)

    def test_convert_set_type_struct_to_parquet(self):
        """ Test converting a JSON Structure schema with set type to Parquet"""
        test_schema = {
            "type": "object",
            "name": "SetTypes",
            "properties": {
                "uniqueTags": {
                    "type": "set",
                    "items": {"type": "string"}
                }
            }
        }
        
        import json
        schema_path = os.path.join(tempfile.gettempdir(), "avrotize", "sets.struct.json")
        parquet_path = os.path.join(tempfile.gettempdir(), "avrotize", "sets.parquet")
        dir = os.path.dirname(schema_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        with open(schema_path, 'w') as f:
            json.dump(test_schema, f)
        
        convert_structure_to_parquet(schema_path, None, parquet_path, False)

    def test_convert_tuple_type_struct_to_parquet(self):
        """ Test converting a JSON Structure schema with tuple type to Parquet"""
        test_schema = {
            "type": "object",
            "name": "TupleTypes",
            "properties": {
                "coordinate": {
                    "type": "tuple",
                    "tuple": ["x", "y"],
                    "properties": {
                        "x": {"type": "double"},
                        "y": {"type": "double"}
                    }
                }
            }
        }
        
        import json
        schema_path = os.path.join(tempfile.gettempdir(), "avrotize", "tuples.struct.json")
        parquet_path = os.path.join(tempfile.gettempdir(), "avrotize", "tuples.parquet")
        dir = os.path.dirname(schema_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        with open(schema_path, 'w') as f:
            json.dump(test_schema, f)
        
        convert_structure_to_parquet(schema_path, None, parquet_path, False)

if __name__ == '__main__':
    unittest.main()
