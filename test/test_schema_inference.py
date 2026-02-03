"""Tests for JSON and XML schema inference commands."""

import json
import os
import sys
import tempfile
import unittest

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

from avrotize.jsontoschema import convert_json_to_avro, convert_json_to_jstruct
from avrotize.xmltoschema import convert_xml_to_avro, convert_xml_to_jstruct
from avrotize.schema_inference import (
    infer_avro_schema_from_json,
    infer_avro_schema_from_xml,
    infer_jstruct_schema_from_json,
    infer_jstruct_schema_from_xml
)


class TestJsonToAvro(unittest.TestCase):
    """Test cases for JSON to Avro schema inference."""

    def test_simple_object(self):
        """Test inference from a simple JSON object."""
        values = [
            {"name": "Alice", "age": 30, "active": True},
            {"name": "Bob", "age": 25, "active": False}
        ]
        
        schema = infer_avro_schema_from_json(values, type_name='Person', namespace='com.example')
        
        self.assertEqual(schema['type'], 'record')
        self.assertEqual(schema['name'], 'Person')
        self.assertEqual(schema['namespace'], 'com.example')
        
        fields_by_name = {f['name']: f for f in schema['fields']}
        self.assertIn('name', fields_by_name)
        self.assertIn('age', fields_by_name)
        self.assertIn('active', fields_by_name)
        self.assertEqual(fields_by_name['name']['type'], 'string')
        self.assertEqual(fields_by_name['age']['type'], 'long')
        self.assertEqual(fields_by_name['active']['type'], 'boolean')

    def test_nested_object(self):
        """Test inference from nested JSON objects."""
        values = [
            {"user": {"name": "Alice", "email": "alice@example.com"}, "score": 100}
        ]
        
        schema = infer_avro_schema_from_json(values, type_name='Event')
        
        self.assertEqual(schema['type'], 'record')
        fields_by_name = {f['name']: f for f in schema['fields']}
        
        # user should be a nested record
        user_field = fields_by_name['user']
        self.assertEqual(user_field['type']['type'], 'record')
        self.assertEqual(user_field['type']['name'], 'user')

    def test_array_of_objects(self):
        """Test inference from arrays of objects."""
        values = [
            {"items": [{"id": 1, "name": "Item1"}, {"id": 2, "name": "Item2"}]}
        ]
        
        schema = infer_avro_schema_from_json(values, type_name='Container')
        
        fields_by_name = {f['name']: f for f in schema['fields']}
        items_field = fields_by_name['items']
        self.assertEqual(items_field['type']['type'], 'array')

    def test_sparse_data_folding(self):
        """Test that sparse data is folded into a single record type."""
        values = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "email": "bob@example.com"},
            {"name": "Charlie", "age": 35, "email": "charlie@example.com"}
        ]
        
        schema = infer_avro_schema_from_json(values, type_name='Person')
        
        self.assertEqual(schema['type'], 'record')
        fields_by_name = {f['name']: f for f in schema['fields']}
        
        # All fields should be present
        self.assertIn('name', fields_by_name)
        self.assertIn('age', fields_by_name)
        self.assertIn('email', fields_by_name)

    def test_convert_json_to_avro_file(self):
        """Test the full file conversion flow."""
        # Create a temp JSON file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump([
                {"id": 1, "name": "Test1"},
                {"id": 2, "name": "Test2"}
            ], f)
            json_file = f.name

        try:
            output_file = os.path.join(tempfile.gettempdir(), 'avrotize', 'test_json2a.avsc')
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            convert_json_to_avro(
                input_files=[json_file],
                avro_schema_file=output_file,
                type_name='TestRecord',
                avro_namespace='com.test'
            )
            
            with open(output_file, 'r') as f:
                schema = json.load(f)
            
            self.assertEqual(schema['type'], 'record')
            self.assertEqual(schema['name'], 'TestRecord')
            self.assertEqual(schema['namespace'], 'com.test')
        finally:
            os.unlink(json_file)


class TestJsonToJstruct(unittest.TestCase):
    """Test cases for JSON to JSON Structure schema inference."""

    def test_simple_object(self):
        """Test inference from a simple JSON object."""
        values = [
            {"name": "Alice", "age": 30, "active": True},
            {"name": "Bob", "age": 25, "active": False}
        ]
        
        schema = infer_jstruct_schema_from_json(values, type_name='Person', base_id='https://example.com/')
        
        self.assertEqual(schema['$schema'], 'https://json-structure.org/meta/core/v0/#')
        self.assertEqual(schema['$id'], 'https://example.com/Person')
        self.assertEqual(schema['type'], 'object')
        self.assertEqual(schema['name'], 'Person')
        
        self.assertIn('properties', schema)
        self.assertIn('name', schema['properties'])
        self.assertIn('age', schema['properties'])
        self.assertIn('active', schema['properties'])
        
        self.assertEqual(schema['properties']['name']['type'], 'string')
        self.assertEqual(schema['properties']['age']['type'], 'int64')
        self.assertEqual(schema['properties']['active']['type'], 'boolean')

    def test_nested_object(self):
        """Test inference from nested JSON objects."""
        values = [
            {"user": {"name": "Alice", "email": "alice@example.com"}, "score": 100}
        ]
        
        schema = infer_jstruct_schema_from_json(values, type_name='Event')
        
        self.assertEqual(schema['type'], 'object')
        self.assertIn('user', schema['properties'])
        
        # user should be a nested object
        user_prop = schema['properties']['user']
        self.assertEqual(user_prop['type'], 'object')
        self.assertIn('properties', user_prop)

    def test_array_of_primitives(self):
        """Test inference from arrays of primitives."""
        values = [
            {"tags": ["a", "b", "c"]}
        ]
        
        schema = infer_jstruct_schema_from_json(values, type_name='TaggedItem')
        
        tags_prop = schema['properties']['tags']
        self.assertEqual(tags_prop['type'], 'array')
        self.assertIn('items', tags_prop)

    def test_sparse_data_folding(self):
        """Test that sparse data is folded into a single object type."""
        values = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "email": "bob@example.com"},
            {"name": "Charlie", "age": 35, "email": "charlie@example.com"}
        ]
        
        schema = infer_jstruct_schema_from_json(values, type_name='Person')
        
        self.assertEqual(schema['type'], 'object')
        
        # All properties should be present
        self.assertIn('name', schema['properties'])
        self.assertIn('age', schema['properties'])
        self.assertIn('email', schema['properties'])
        
        # Only 'name' should be required (appears in all records)
        self.assertIn('required', schema)
        self.assertIn('name', schema['required'])

    def test_convert_json_to_jstruct_file(self):
        """Test the full file conversion flow."""
        # Create a temp JSON file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump([
                {"id": 1, "name": "Test1"},
                {"id": 2, "name": "Test2"}
            ], f)
            json_file = f.name

        try:
            output_file = os.path.join(tempfile.gettempdir(), 'avrotize', 'test_json2s.jstruct.json')
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            convert_json_to_jstruct(
                input_files=[json_file],
                jstruct_schema_file=output_file,
                type_name='TestRecord',
                base_id='https://test.example.com/'
            )
            
            with open(output_file, 'r') as f:
                schema = json.load(f)
            
            self.assertEqual(schema['$schema'], 'https://json-structure.org/meta/core/v0/#')
            self.assertEqual(schema['$id'], 'https://test.example.com/TestRecord')
            self.assertEqual(schema['type'], 'object')
            self.assertEqual(schema['name'], 'TestRecord')
        finally:
            os.unlink(json_file)

    def test_jstruct_schema_validates_with_sdk(self):
        """Test that generated JSON Structure schemas validate with the official SDK."""
        try:
            from json_structure import SchemaValidator
        except ImportError:
            self.skipTest("json-structure SDK not installed")

        values = [
            {"name": "Alice", "age": 30, "active": True},
            {"name": "Bob", "age": 25, "active": False}
        ]
        
        schema = infer_jstruct_schema_from_json(values, type_name='Person', base_id='https://example.com/')
        
        validator = SchemaValidator()
        errors = validator.validate(schema)
        
        self.assertEqual(errors, [], f"Schema validation failed: {errors}")


class TestXmlToAvro(unittest.TestCase):
    """Test cases for XML to Avro schema inference."""

    def test_simple_xml(self):
        """Test inference from simple XML."""
        xml_strings = [
            '<person><name>Alice</name><age>30</age></person>',
            '<person><name>Bob</name><age>25</age></person>'
        ]
        
        schema = infer_avro_schema_from_xml(xml_strings, type_name='Person', namespace='com.example')
        
        self.assertEqual(schema['type'], 'record')
        self.assertEqual(schema['name'], 'Person')
        
        fields_by_name = {f['name']: f for f in schema['fields']}
        self.assertIn('name', fields_by_name)
        self.assertIn('age', fields_by_name)

    def test_xml_with_attributes(self):
        """Test inference from XML with attributes."""
        xml_strings = [
            '<item id="1" status="active"><name>Item1</name></item>'
        ]
        
        schema = infer_avro_schema_from_xml(xml_strings, type_name='Item')
        
        fields_by_name = {f['name']: f for f in schema['fields']}
        # Attributes should be prefixed with @
        self.assertIn('_id', fields_by_name)  # @ converted to _ by avro_name
        self.assertIn('_status', fields_by_name)
        self.assertIn('name', fields_by_name)

    def test_xml_with_nested_elements(self):
        """Test inference from nested XML."""
        xml_strings = [
            '<order><customer><name>Alice</name></customer><total>100</total></order>'
        ]
        
        schema = infer_avro_schema_from_xml(xml_strings, type_name='Order')
        
        fields_by_name = {f['name']: f for f in schema['fields']}
        self.assertIn('customer', fields_by_name)
        
        # customer should be a nested record
        customer_field = fields_by_name['customer']
        self.assertEqual(customer_field['type']['type'], 'record')

    def test_convert_xml_to_avro_file(self):
        """Test the full file conversion flow."""
        # Create a temp XML file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
            f.write('<root><item><id>1</id><name>Test1</name></item></root>')
            xml_file = f.name

        try:
            output_file = os.path.join(tempfile.gettempdir(), 'avrotize', 'test_xml2a.avsc')
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            convert_xml_to_avro(
                input_files=[xml_file],
                avro_schema_file=output_file,
                type_name='TestRecord',
                avro_namespace='com.test'
            )
            
            with open(output_file, 'r') as f:
                schema = json.load(f)
            
            self.assertEqual(schema['type'], 'record')
            self.assertEqual(schema['name'], 'TestRecord')
        finally:
            os.unlink(xml_file)


class TestXmlToJstruct(unittest.TestCase):
    """Test cases for XML to JSON Structure schema inference."""

    def test_simple_xml(self):
        """Test inference from simple XML."""
        xml_strings = [
            '<person><name>Alice</name><age>30</age></person>',
            '<person><name>Bob</name><age>25</age></person>'
        ]
        
        schema = infer_jstruct_schema_from_xml(xml_strings, type_name='Person', base_id='https://example.com/')
        
        self.assertEqual(schema['$schema'], 'https://json-structure.org/meta/core/v0/#')
        self.assertEqual(schema['type'], 'object')
        
        self.assertIn('name', schema['properties'])
        self.assertIn('age', schema['properties'])

    def test_xml_with_attributes(self):
        """Test inference from XML with attributes."""
        xml_strings = [
            '<item id="1" status="active"><name>Item1</name></item>'
        ]
        
        schema = infer_jstruct_schema_from_xml(xml_strings, type_name='Item')
        
        # Attributes should be prefixed with @
        self.assertIn('_id', schema['properties'])
        self.assertIn('_status', schema['properties'])
        self.assertIn('name', schema['properties'])

    def test_convert_xml_to_jstruct_file(self):
        """Test the full file conversion flow."""
        # Create a temp XML file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
            f.write('<root><item><id>1</id><name>Test1</name></item></root>')
            xml_file = f.name

        try:
            output_file = os.path.join(tempfile.gettempdir(), 'avrotize', 'test_xml2s.jstruct.json')
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            convert_xml_to_jstruct(
                input_files=[xml_file],
                jstruct_schema_file=output_file,
                type_name='TestRecord',
                base_id='https://test.example.com/'
            )
            
            with open(output_file, 'r') as f:
                schema = json.load(f)
            
            self.assertEqual(schema['$schema'], 'https://json-structure.org/meta/core/v0/#')
            self.assertEqual(schema['type'], 'object')
        finally:
            os.unlink(xml_file)

    def test_jstruct_xml_schema_validates_with_sdk(self):
        """Test that generated JSON Structure schemas from XML validate with the official SDK."""
        try:
            from json_structure import SchemaValidator
        except ImportError:
            self.skipTest("json-structure SDK not installed")

        xml_strings = [
            '<person><name>Alice</name><age>30</age></person>'
        ]
        
        schema = infer_jstruct_schema_from_xml(xml_strings, type_name='Person', base_id='https://example.com/')
        
        validator = SchemaValidator()
        errors = validator.validate(schema)
        
        self.assertEqual(errors, [], f"Schema validation failed: {errors}")


class TestJsonLines(unittest.TestCase):
    """Test cases for JSONL (JSON Lines) file support."""

    def test_jsonl_file(self):
        """Test reading JSONL files."""
        # Create a temp JSONL file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            f.write('{"id": 1, "name": "Line1"}\n')
            f.write('{"id": 2, "name": "Line2"}\n')
            f.write('{"id": 3, "name": "Line3"}\n')
            jsonl_file = f.name

        try:
            output_file = os.path.join(tempfile.gettempdir(), 'avrotize', 'test_jsonl.avsc')
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            convert_json_to_avro(
                input_files=[jsonl_file],
                avro_schema_file=output_file,
                type_name='LineRecord'
            )
            
            with open(output_file, 'r') as f:
                schema = json.load(f)
            
            self.assertEqual(schema['type'], 'record')
            self.assertEqual(schema['name'], 'LineRecord')
            
            fields_by_name = {f['name']: f for f in schema['fields']}
            self.assertIn('id', fields_by_name)
            self.assertIn('name', fields_by_name)
        finally:
            os.unlink(jsonl_file)


class TestMultipleFiles(unittest.TestCase):
    """Test cases for processing multiple input files."""

    def test_multiple_json_files(self):
        """Test processing multiple JSON files together."""
        # Create temp JSON files with different structures
        files = []
        try:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                json.dump([{"name": "Alice", "age": 30}], f)
                files.append(f.name)
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                json.dump([{"name": "Bob", "email": "bob@example.com"}], f)
                files.append(f.name)
            
            output_file = os.path.join(tempfile.gettempdir(), 'avrotize', 'test_multi.avsc')
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            convert_json_to_avro(
                input_files=files,
                avro_schema_file=output_file,
                type_name='MergedRecord'
            )
            
            with open(output_file, 'r') as f:
                schema = json.load(f)
            
            fields_by_name = {f['name']: f for f in schema['fields']}
            # Schema should contain fields from all files
            self.assertIn('name', fields_by_name)
            self.assertIn('age', fields_by_name)
            self.assertIn('email', fields_by_name)
        finally:
            for f in files:
                os.unlink(f)


if __name__ == '__main__':
    unittest.main()
