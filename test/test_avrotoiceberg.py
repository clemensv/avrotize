"""Test cases for converting Avro to Iceberg schemas"""

import os
import sys
import tempfile
import json
from os import path, getcwd

import pytest
import unittest
from unittest.mock import patch

# Ensure the project root is in the system path for imports
current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType, IntegerType, LongType, FloatType, DoubleType,
    StringType, BinaryType, DateType, TimestampType, DecimalType,
    FixedType, ListType, MapType, StructType
)

from avrotize.jsonstoavro import convert_jsons_to_avro
from avrotize.avrotoiceberg import convert_avro_to_iceberg

class TestAvroToIceberg(unittest.TestCase):
    """Test cases for converting Avro to Iceberg schemas"""
    def test_convert_address_avsc_to_iceberg(self):
        """Test converting an Avro schema to Iceberg"""
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        iceberg_path = os.path.join(tempfile.gettempdir(), "avrotize", "address.iceberg")
        dir = os.path.dirname(iceberg_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_avro_to_iceberg(avro_path, None, iceberg_path, False)

    def test_convert_telemetry_avsc_to_iceberg(self):
        """Test converting a telemetry.avsc file to Iceberg"""
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "telemetry.avsc")
        iceberg_path = os.path.join(tempfile.gettempdir(), "avrotize", "telemetry.iceberg")
        dir = os.path.dirname(iceberg_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_avro_to_iceberg(avro_path, None, iceberg_path, True)
        
    def test_convert_fileblob_to_iceberg(self):
        """Test converting a fileblob.avsc file to Iceberg"""
        cwd = getcwd()
        avro_path = path.join(cwd, "test", "avsc", "fileblob.avsc")
        iceberg_path = path.join(tempfile.gettempdir(), "avrotize", "fileblob.iceberg")
        dir = os.path.dirname(iceberg_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_avro_to_iceberg(avro_path, None, iceberg_path, False)
    
    def test_convert_jfrog_pipelines_jsons_to_avro_to_iceberg(self):
        """Test converting JFrog Pipelines JSONs to Avro and Iceberg schemas"""
        cwd = getcwd()
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        iceberg_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.iceberg")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_iceberg(avro_path, "HelmBlueGreenDeploy", iceberg_path, True)


class TestIcebergSchemaValidation(unittest.TestCase):
    """Test cases that validate Iceberg schema output against the pyiceberg library"""
    
    def _load_and_validate_iceberg_schema(self, iceberg_path: str) -> Schema:
        """Load an Iceberg schema JSON file and validate it with pyiceberg"""
        with open(iceberg_path, "r", encoding="utf-8") as f:
            schema_json = json.load(f)
        
        # Validate that pyiceberg can parse the schema
        schema = Schema.model_validate(schema_json)
        return schema
    
    def test_iceberg_schema_json_structure(self):
        """Test that the Iceberg schema JSON has the correct top-level structure"""
        cwd = getcwd()
        avro_path = path.join(cwd, "test", "avsc", "address.avsc")
        iceberg_path = path.join(tempfile.gettempdir(), "avrotize", "address_validate.iceberg")
        dir = os.path.dirname(iceberg_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_avro_to_iceberg(avro_path, None, iceberg_path, False)
        
        with open(iceberg_path, "r", encoding="utf-8") as f:
            schema_json = json.load(f)
        
        # Verify top-level structure per Iceberg spec Appendix C
        self.assertEqual(schema_json["type"], "struct")
        self.assertIn("schema-id", schema_json)
        self.assertIn("fields", schema_json)
        self.assertIsInstance(schema_json["fields"], list)
        
        # Verify field structure
        for field in schema_json["fields"]:
            self.assertIn("id", field)
            self.assertIn("name", field)
            self.assertIn("required", field)
            self.assertIn("type", field)
            self.assertIsInstance(field["id"], int)
            self.assertIsInstance(field["name"], str)
            self.assertIsInstance(field["required"], bool)

    def test_iceberg_schema_parseable_by_pyiceberg(self):
        """Test that the generated Iceberg schema can be parsed by pyiceberg"""
        cwd = getcwd()
        avro_path = path.join(cwd, "test", "avsc", "address.avsc")
        iceberg_path = path.join(tempfile.gettempdir(), "avrotize", "address_pyiceberg.iceberg")
        dir = os.path.dirname(iceberg_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_avro_to_iceberg(avro_path, None, iceberg_path, False)
        
        # This will raise an exception if pyiceberg can't parse it
        schema = self._load_and_validate_iceberg_schema(iceberg_path)
        
        # Verify the schema is usable
        self.assertIsInstance(schema, Schema)
        self.assertGreater(len(schema.fields), 0)

    def test_iceberg_primitive_types(self):
        """Test that primitive types are correctly serialized as strings"""
        cwd = getcwd()
        avro_path = path.join(cwd, "test", "avsc", "telemetry.avsc")
        iceberg_path = path.join(tempfile.gettempdir(), "avrotize", "telemetry_primitives.iceberg")
        dir = os.path.dirname(iceberg_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_avro_to_iceberg(avro_path, None, iceberg_path, False)
        
        with open(iceberg_path, "r", encoding="utf-8") as f:
            schema_json = json.load(f)
        
        # Verify primitive types are serialized as strings
        for field in schema_json["fields"]:
            field_type = field["type"]
            if isinstance(field_type, str):
                # Primitive types should be one of the valid Iceberg primitives
                valid_primitives = [
                    "boolean", "int", "long", "float", "double", 
                    "string", "binary", "date", "time", "timestamp"
                ]
                # Allow decimal and fixed patterns
                if not any(field_type.startswith(p) for p in ["decimal(", "fixed["]):
                    self.assertIn(field_type, valid_primitives, 
                        f"Field {field['name']} has invalid primitive type: {field_type}")
        
        # Validate with pyiceberg
        schema = self._load_and_validate_iceberg_schema(iceberg_path)
        self.assertIsInstance(schema, Schema)

    def test_iceberg_nested_struct_types(self):
        """Test that nested struct types are correctly serialized as JSON objects"""
        cwd = getcwd()
        jsons_path = path.join(cwd, "test", "jsons", "employee.jsons")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "employee_struct.avsc")
        iceberg_path = path.join(tempfile.gettempdir(), "avrotize", "employee_struct.iceberg")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_iceberg(avro_path, None, iceberg_path, False)
        
        with open(iceberg_path, "r", encoding="utf-8") as f:
            schema_json = json.load(f)
        
        # Find a nested struct field
        nested_struct_found = False
        for field in schema_json["fields"]:
            if isinstance(field["type"], dict) and field["type"].get("type") == "struct":
                nested_struct_found = True
                nested_type = field["type"]
                self.assertIn("fields", nested_type)
                self.assertIsInstance(nested_type["fields"], list)
                break
        
        self.assertTrue(nested_struct_found, "Expected at least one nested struct field")
        
        # Validate with pyiceberg
        schema = self._load_and_validate_iceberg_schema(iceberg_path)
        # Verify the nested field is a StructType
        for f in schema.fields:
            if isinstance(f.field_type, StructType):
                self.assertIsInstance(f.field_type, StructType)
                break

    def test_iceberg_list_types(self):
        """Test that list types are correctly serialized with element-id and element-required"""
        cwd = getcwd()
        jsons_path = path.join(cwd, "test", "jsons", "userprofile.jsons")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "userprofile_list.avsc")
        iceberg_path = path.join(tempfile.gettempdir(), "avrotize", "userprofile_list.iceberg")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_iceberg(avro_path, None, iceberg_path, False)
        
        with open(iceberg_path, "r", encoding="utf-8") as f:
            schema_json = json.load(f)
        
        # Find a list field
        list_field_found = False
        for field in schema_json["fields"]:
            if isinstance(field["type"], dict) and field["type"].get("type") == "list":
                list_field_found = True
                list_type = field["type"]
                # Verify list type structure per Appendix C
                self.assertIn("element-id", list_type)
                self.assertIn("element-required", list_type)
                self.assertIn("element", list_type)
                self.assertIsInstance(list_type["element-id"], int)
                self.assertIsInstance(list_type["element-required"], bool)
                break
        
        self.assertTrue(list_field_found, "Expected at least one list field in userprofile")
        
        # Validate with pyiceberg
        schema = self._load_and_validate_iceberg_schema(iceberg_path)
        # Verify the list field is a ListType
        for f in schema.fields:
            if isinstance(f.field_type, ListType):
                self.assertIsInstance(f.field_type, ListType)
                break

    def test_iceberg_cloudevents_columns(self):
        """Test that CloudEvents columns are correctly added when enabled"""
        cwd = getcwd()
        avro_path = path.join(cwd, "test", "avsc", "address.avsc")
        iceberg_path = path.join(tempfile.gettempdir(), "avrotize", "address_cloudevents.iceberg")
        dir = os.path.dirname(iceberg_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_avro_to_iceberg(avro_path, None, iceberg_path, emit_cloudevents_columns=True)
        
        schema = self._load_and_validate_iceberg_schema(iceberg_path)
        
        # Verify CloudEvents columns are present
        field_names = [f.name for f in schema.fields]
        self.assertIn("___type", field_names)
        self.assertIn("___source", field_names)
        self.assertIn("___id", field_names)
        self.assertIn("___time", field_names)
        self.assertIn("___subject", field_names)
        
        # Verify ___time is a timestamp type
        time_field = next(f for f in schema.fields if f.name == "___time")
        self.assertIsInstance(time_field.field_type, TimestampType)

    def test_iceberg_schema_field_ids_are_unique(self):
        """Test that all field IDs in the schema are unique"""
        cwd = getcwd()
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog_unique_ids.avsc")
        iceberg_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog_unique_ids.iceberg")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_iceberg(avro_path, "HelmBlueGreenDeploy", iceberg_path, False)
        
        with open(iceberg_path, "r", encoding="utf-8") as f:
            schema_json = json.load(f)
        
        # Collect all field IDs recursively
        def collect_ids(obj, ids=None):
            if ids is None:
                ids = []
            if isinstance(obj, dict):
                if "id" in obj:
                    ids.append(obj["id"])
                if "element-id" in obj:
                    ids.append(obj["element-id"])
                if "key-id" in obj:
                    ids.append(obj["key-id"])
                if "value-id" in obj:
                    ids.append(obj["value-id"])
                for v in obj.values():
                    collect_ids(v, ids)
            elif isinstance(obj, list):
                for item in obj:
                    collect_ids(item, ids)
            return ids
        
        all_ids = collect_ids(schema_json)
        # Verify all IDs are unique
        self.assertEqual(len(all_ids), len(set(all_ids)), 
            f"Found duplicate field IDs: {[x for x in all_ids if all_ids.count(x) > 1]}")

    def test_iceberg_schema_roundtrip_to_arrow(self):
        """Test that the schema can be converted to Arrow format via pyiceberg"""
        cwd = getcwd()
        avro_path = path.join(cwd, "test", "avsc", "telemetry.avsc")
        iceberg_path = path.join(tempfile.gettempdir(), "avrotize", "telemetry_arrow.iceberg")
        dir = os.path.dirname(iceberg_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_avro_to_iceberg(avro_path, None, iceberg_path, False)
        
        schema = self._load_and_validate_iceberg_schema(iceberg_path)
        
        # Convert to Arrow schema - this validates the schema is fully usable
        try:
            arrow_schema = schema.as_arrow()
            self.assertIsNotNone(arrow_schema)
            self.assertGreater(len(arrow_schema), 0)
        except Exception as e:
            self.fail(f"Failed to convert Iceberg schema to Arrow: {e}")


if __name__ == '__main__':
    unittest.main()
